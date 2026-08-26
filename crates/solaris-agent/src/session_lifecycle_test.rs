use std::fs;

use rusqlite::Connection;
use tempfile::tempdir;

use super::*;

#[test]
fn active_creation_persists_state_run_lease_and_reference_together() {
    let directory = tempdir().unwrap();
    let manager = SessionManager::new(directory.path().to_path_buf(), 20);

    let session = manager
        .create_active_session("provider", "model", "workspace", Some("atomic"), "shared-run")
        .unwrap();

    assert_eq!(session.run_id.as_deref(), Some("shared-run"));
    let connection = Connection::open(directory.path().join("session.sqlite3")).unwrap();
    let (state, run_id, revision): (Vec<u8>, Option<String>, i64) = connection
        .query_row(
            "SELECT state_json, run_id, revision FROM sessions WHERE session_id = 'atomic'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    let stored: Session = serde_json::from_slice(&state).unwrap();
    let owner: Option<String> = connection
        .query_row(
            "SELECT owner_id FROM session_leases WHERE session_id = 'atomic'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let references: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM session_run_references
             WHERE session_id = 'atomic' AND run_id = 'shared-run' AND reference_kind = 'session'",
            [],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(stored.run_id.as_deref(), Some("shared-run"));
    assert_eq!(run_id.as_deref(), Some("shared-run"));
    assert_eq!(revision, 0);
    assert!(owner.is_some_and(|owner| !owner.is_empty()));
    assert_eq!(references, 1);
    manager.release_active_session().unwrap();
}

#[test]
fn lease_takeover_fences_manager_save_and_future_activity() {
    let directory = tempdir().unwrap();
    let first = SessionManager::new(directory.path().to_path_buf(), 20);
    let mut session = first
        .create_active_session("provider", "model", "workspace", Some("fenced"), "shared-run")
        .unwrap();
    let connection = Connection::open(directory.path().join("session.sqlite3")).unwrap();
    connection
        .execute(
            "UPDATE session_leases SET heartbeat_at_ms = 0, expires_at_ms = 0
             WHERE session_id = 'fenced'",
            [],
        )
        .unwrap();
    drop(connection);

    let second = SessionManager::new(directory.path().to_path_buf(), 20);
    second.load_active_session("fenced").unwrap();
    session.model = "stale-write".to_owned();

    let ensure_error = first.ensure_active_session("fenced").unwrap_err();
    let save_error = first.save_active_session(&session).unwrap_err();

    assert!(ensure_error.is_lease_lost());
    assert!(save_error.is_lease_lost());
    assert_ne!(second.load("fenced").unwrap().model, "stale-write");
    second.release_active_session().unwrap();
}

#[test]
fn dropping_manager_releases_active_lease_without_async_cleanup() {
    let directory = tempdir().unwrap();
    {
        let manager = SessionManager::new(directory.path().to_path_buf(), 20);
        manager
            .create_active_session("provider", "model", "workspace", Some("released"), "run")
            .unwrap();
    }

    let resumed = SessionManager::new(directory.path().to_path_buf(), 20);
    let session = resumed.load_active_session("released").unwrap();

    assert_eq!(session.id, "released");
    resumed.release_active_session().unwrap();
}

#[test]
fn legacy_json_is_imported_once_and_never_rewritten() {
    let directory = tempdir().unwrap();
    let now = Utc::now();
    let legacy = Session {
        id: "legacy-read-only".to_owned(),
        run_id: Some("legacy-run".to_owned()),
        created_at: now,
        updated_at: now,
        provider: "provider".to_owned(),
        model: "old".to_owned(),
        cwd: "workspace".to_owned(),
        total_usage: TokenUsage::default(),
        messages: Vec::new(),
        runtime_state: None,
    };
    let path = directory.path().join("2026-08-23_legacy-read-only.json");
    let original = serde_json::to_vec_pretty(&legacy).unwrap();
    fs::write(&path, &original).unwrap();
    let manager = SessionManager::new(directory.path().to_path_buf(), 20);

    let mut loaded = manager.load("legacy-read-only").unwrap();
    loaded.model = "new".to_owned();
    manager.save(&loaded).unwrap();
    let reopened = SessionManager::new(directory.path().to_path_buf(), 20);

    assert_eq!(reopened.load("legacy-read-only").unwrap().model, "new");
    assert_eq!(fs::read(&path).unwrap(), original);
    assert!(!directory.path().join("sessions").exists());
}

#[test]
fn retention_tombstones_only_the_oldest_inactive_session_and_plans_delayed_gc() {
    let directory = tempdir().unwrap();
    let manager = SessionManager::new(directory.path().to_path_buf(), 2);
    for id in ["oldest", "middle", "newest"] {
        manager.create("provider", "model", "workspace", Some(id)).unwrap();
    }

    let visible: Vec<_> = manager.list().unwrap().into_iter().map(|session| session.id).collect();
    assert_eq!(visible, ["middle", "newest"]);
    assert!(manager.load_if_exists("oldest").unwrap().is_none());

    let connection = Connection::open(directory.path().join("session.sqlite3")).unwrap();
    let (phase, delay_ms, tree): (String, i64, Vec<u8>) = connection
        .query_row(
            "SELECT phase, eligible_at_ms - created_at_ms, exact_run_tree_json
             FROM session_gc_jobs WHERE session_id = 'oldest'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(phase, "planned");
    assert_eq!(delay_ms, 30 * 24 * 60 * 60 * 1_000);
    let tree: serde_json::Value = serde_json::from_slice(&tree).unwrap();
    assert_eq!(tree["session_id"], "oldest");
    assert_eq!(tree["run_ids"].as_array().unwrap().len(), 1);
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM sessions WHERE session_id = 'oldest'", [], |row| {
                row.get::<_, i64>(0)
            },)
            .unwrap(),
        1,
        "retention must preserve session state until delayed GC is safe"
    );
}

#[test]
fn retention_preserves_the_oldest_pending_host_delivery_and_tombstones_the_next_inactive_session() {
    let directory = tempdir().unwrap();
    let setup = SessionManager::new(directory.path().to_path_buf(), 20);
    let oldest = setup
        .create("provider", "model", "workspace", Some("oldest-pending"))
        .unwrap();
    setup
        .create("provider", "model", "workspace", Some("next-inactive"))
        .unwrap();

    let owner = SessionManager::new(directory.path().to_path_buf(), 20);
    owner.load_active_session(&oldest.id).unwrap();
    let outbox = owner
        .open_host_outbox(&oldest.id, oldest.run_id.as_deref().unwrap())
        .unwrap();
    outbox
        .enqueue("message", br#"{"type":"info","msg_id":"message","message":"pending"}"#)
        .unwrap();
    owner.release_active_session().unwrap();

    let retaining = SessionManager::new(directory.path().to_path_buf(), 1);
    let visible: Vec<_> = retaining
        .list()
        .unwrap()
        .into_iter()
        .map(|session| session.id)
        .collect();
    assert_eq!(visible, ["oldest-pending"]);

    let connection = Connection::open(directory.path().join("session.sqlite3")).unwrap();
    let tombstones: Vec<String> = connection
        .prepare("SELECT session_id FROM session_tombstones ORDER BY session_id")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(tombstones, ["next-inactive"]);
}
