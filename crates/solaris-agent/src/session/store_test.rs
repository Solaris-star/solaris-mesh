use std::env;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use rusqlite::{Connection, TransactionBehavior};
use solaris_types::message::TokenUsage;
use tempfile::tempdir;

use crate::session::{SessionMemorySnapshot, SessionRuntimeState};

use super::store_test_support::{create_directory_redirect, create_file_symlink, optional_symlink_created};
use super::*;

const FIXTURE_MODE: &str = "SOLARIS_SESSION_STORE_FIXTURE_MODE";
const FIXTURE_DIRECTORY: &str = "SOLARIS_SESSION_STORE_FIXTURE_DIRECTORY";
const FIXTURE_SESSION_ID: &str = "SOLARIS_SESSION_STORE_FIXTURE_SESSION_ID";
const FIXTURE_OWNER: &str = "SOLARIS_SESSION_STORE_FIXTURE_OWNER";
const FIXTURE_START: &str = "SOLARIS_SESSION_STORE_FIXTURE_START";
const FIXTURE_READY: &str = "SOLARIS_SESSION_STORE_FIXTURE_READY";
const FIXTURE_RESULT: &str = "SOLARIS_SESSION_STORE_FIXTURE_RESULT";
const FIXTURE_EPOCH: &str = "SOLARIS_SESSION_STORE_FIXTURE_EPOCH";
const FIXTURE_REVISION: &str = "SOLARIS_SESSION_STORE_FIXTURE_REVISION";

#[test]
fn open_configures_durable_versioned_schema() {
    let directory = tempdir().unwrap();

    let store = SessionStore::open(directory.path()).unwrap();

    assert_eq!(
        store.database_path().canonicalize().unwrap(),
        directory.path().join("session.sqlite3").canonicalize().unwrap()
    );
    assert!(store.database_path().is_file());
    let connection = store.open_connection().unwrap();
    let journal_mode: String = connection
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .unwrap();
    let synchronous: i64 = connection
        .query_row("PRAGMA synchronous", [], |row| row.get(0))
        .unwrap();
    let user_version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    let busy_timeout: i64 = connection
        .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
        .unwrap();
    let foreign_keys: i64 = connection
        .query_row("PRAGMA foreign_keys", [], |row| row.get(0))
        .unwrap();
    let tables = table_names(&connection);

    assert_eq!(journal_mode.to_ascii_lowercase(), "wal");
    assert_eq!(synchronous, 2);
    assert_eq!(user_version, SESSION_STORE_SCHEMA_VERSION);
    assert_eq!(busy_timeout, 30_000);
    assert_eq!(foreign_keys, 1);
    assert_eq!(DEFAULT_HEARTBEAT_SECONDS, 15);
    for expected in [
        "sessions",
        "session_leases",
        "session_tombstones",
        "session_migration_sources",
        "session_migration_imports",
        "session_gc_jobs",
        "session_run_references",
        "host_outbox",
        "durable_agent_tasks",
    ] {
        assert!(tables.iter().any(|table| table == expected), "missing table {expected}");
    }
}

#[test]
fn schema_enforces_foreign_keys_and_durable_identity_constraints() {
    let directory = tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    let connection = store.open_connection().unwrap();

    let missing_session_lease = connection.execute(
        "INSERT INTO session_leases
            (session_id, owner_id, epoch, heartbeat_at_ms, expires_at_ms)
         VALUES ('missing', 'owner', 1, 1, 2)",
        [],
    );
    assert!(missing_session_lease.is_err());
    let invalid_host_sequence = connection.execute(
        "INSERT INTO host_outbox
            (delivery_id, session_id, msg_id, run_epoch, sequence, digest, payload, created_at_ms)
         VALUES ('delivery', 'session', 'message', 0, -1, X'00', X'00', 1)",
        [],
    );
    assert!(invalid_host_sequence.is_err());
}

#[test]
fn heartbeat_extends_lease_and_read_only_queries_do_not_change_ownership() {
    let directory = tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    let start = timestamp(1_800_000_000_000);
    let first = sample_session("first-session", "first");
    let second = sample_session("second-session", "second");
    let lease = store.create_active_at(&first, "owner-a", start).unwrap();
    let second_lease = store.create_active_at(&second, "owner-b", start).unwrap();
    store.release_at(&second_lease, start).unwrap();

    store
        .heartbeat_at(&lease, start + ChronoDuration::seconds(DEFAULT_HEARTBEAT_SECONDS))
        .unwrap();
    let listed = store.list().unwrap();
    let loaded = store.load("first-session").unwrap().unwrap();

    assert_eq!(listed.len(), 2);
    assert_eq!(loaded.session.model, "first");
    let connection = Connection::open(store.database_path()).unwrap();
    let (owner, heartbeat, expiry): (Option<String>, Option<i64>, Option<i64>) = connection
        .query_row(
            "SELECT owner_id, heartbeat_at_ms, expires_at_ms
             FROM session_leases WHERE session_id = 'first-session'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(owner.as_deref(), Some("owner-a"));
    assert_eq!(heartbeat, Some(start.timestamp_millis() + 15_000));
    assert_eq!(expiry, Some(start.timestamp_millis() + 105_000));
}

#[test]
fn save_requires_owner_epoch_and_revision() {
    let directory = tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    let now = timestamp(1_800_000_000_000);
    let session = sample_session("cas-session", "before");
    let mut lease = store.create_active_at(&session, "owner-a", now).unwrap();
    let stale_revision = lease.clone();

    let mut changed = session.clone();
    changed.model = "after".into();
    store.save_at(&mut lease, &changed, now).unwrap();

    let error = store.save_at(&mut stale_revision.clone(), &changed, now).unwrap_err();
    assert!(matches!(
        error,
        SessionStoreError::RevisionConflict {
            expected: 0,
            actual: 1,
            ..
        }
    ));

    let mut wrong_owner = lease.clone();
    wrong_owner.owner_id = "owner-b".into();
    let error = store.save_at(&mut wrong_owner, &changed, now).unwrap_err();
    assert!(matches!(error, SessionStoreError::StaleLease { .. }));

    let mut wrong_epoch = lease.clone();
    wrong_epoch.epoch += 1;
    let error = store.save_at(&mut wrong_epoch, &changed, now).unwrap_err();
    assert!(matches!(error, SessionStoreError::StaleLease { .. }));

    let stored = store.load("cas-session").unwrap().unwrap();
    assert_eq!(stored.revision, 1);
    assert_eq!(stored.session.model, "after");
}

#[test]
fn save_sets_memory_snapshot_once_under_the_session_cas() {
    let directory = tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    let now = timestamp(1_800_000_000_000);
    let session = sample_session("memory-set-once", "model");
    let mut lease = store.create_active_at(&session, "owner", now).unwrap();
    let reference = SessionMemorySnapshot {
        format_version: 1,
        digest_sha256: "a".repeat(64),
        encoded_bytes: 17,
        captured_at_ms: now.timestamp_millis(),
    };
    let mut with_snapshot = session.clone();
    with_snapshot.runtime_state = Some(SessionRuntimeState {
        memory_snapshot: Some(reference.clone()),
        ..SessionRuntimeState::default()
    });

    store.save_at(&mut lease, &with_snapshot, now).unwrap();
    store.save_at(&mut lease, &with_snapshot, now).unwrap();

    let mut removed = with_snapshot.clone();
    removed.runtime_state.as_mut().unwrap().memory_snapshot = None;
    assert!(matches!(
        store.save_at(&mut lease, &removed, now),
        Err(SessionStoreError::MemorySnapshotConflict { .. })
    ));

    let mut replaced = with_snapshot;
    replaced.runtime_state.as_mut().unwrap().memory_snapshot = Some(SessionMemorySnapshot {
        digest_sha256: "b".repeat(64),
        ..reference.clone()
    });
    assert!(matches!(
        store.save_at(&mut lease, &replaced, now),
        Err(SessionStoreError::MemorySnapshotConflict { .. })
    ));
    let stored = store.load("memory-set-once").unwrap().unwrap();
    assert_eq!(
        stored
            .session
            .runtime_state
            .as_ref()
            .and_then(|state| state.memory_snapshot.as_ref()),
        Some(&reference)
    );
}

#[test]
fn expired_or_released_lease_is_taken_over_with_monotonic_epoch() {
    let directory = tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    let start = timestamp(1_800_000_000_000);
    let session = sample_session("lease-session", "model");
    let first = store.create_active_at(&session, "owner-a", start).unwrap();

    let held = store
        .load_active_at("lease-session", "owner-b", start + ChronoDuration::seconds(1))
        .unwrap_err();
    assert!(matches!(held, SessionStoreError::LeaseHeld { .. }));

    let mut second = store
        .load_active_at(
            "lease-session",
            "owner-b",
            start + ChronoDuration::seconds(DEFAULT_LEASE_SECONDS + 1),
        )
        .unwrap();
    assert_eq!(second.lease.epoch, first.epoch + 1);

    let stale = store
        .heartbeat_at(&first, start + ChronoDuration::seconds(2))
        .unwrap_err();
    assert!(matches!(stale, SessionStoreError::StaleLease { .. }));

    store
        .release_at(
            &second.lease,
            start + ChronoDuration::seconds(DEFAULT_LEASE_SECONDS + 2),
        )
        .unwrap();
    let third = store
        .load_active_at(
            "lease-session",
            "owner-c",
            start + ChronoDuration::seconds(DEFAULT_LEASE_SECONDS + 3),
        )
        .unwrap();
    assert_eq!(third.lease.epoch, second.lease.epoch + 1);

    second.session.model = "stale".into();
    let error = store
        .save_at(
            &mut second.lease,
            &second.session,
            start + ChronoDuration::seconds(DEFAULT_LEASE_SECONDS + 3),
        )
        .unwrap_err();
    assert!(matches!(error, SessionStoreError::StaleLease { .. }));
}

#[test]
fn same_owner_reacquire_fences_every_older_lease() {
    let directory = tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    let start = timestamp(1_800_000_000_000);
    let session = sample_session("same-owner-session", "initial");
    let mut first = store.create_active_at(&session, "same-owner", start).unwrap();

    let second = store
        .load_active_at("same-owner-session", "same-owner", start + ChronoDuration::seconds(1))
        .unwrap();

    assert_eq!(second.lease.epoch, first.epoch + 1);
    let error = store
        .save_at(&mut first, &session, start + ChronoDuration::seconds(2))
        .unwrap_err();
    assert!(matches!(error, SessionStoreError::StaleLease { .. }));
    let error = store
        .heartbeat_at(&first, start + ChronoDuration::seconds(2))
        .unwrap_err();
    assert!(matches!(error, SessionStoreError::StaleLease { .. }));
    let error = store
        .release_at(&first, start + ChronoDuration::seconds(2))
        .unwrap_err();
    assert!(matches!(error, SessionStoreError::StaleLease { .. }));
}

#[test]
fn clock_is_sampled_only_after_immediate_lock_and_expiry_is_rechecked() {
    let directory = tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    let start = timestamp(1_800_000_000_000);
    let session = sample_session("clock-session", "initial");
    let first = store.create_active_at(&session, "first-owner", start).unwrap();

    let mut blocker = Connection::open(store.database_path()).unwrap();
    blocker.busy_timeout(Duration::from_secs(5)).unwrap();
    let transaction = blocker
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let started = Arc::new(AtomicBool::new(false));
    let sampled = Arc::new(AtomicBool::new(false));
    let worker_store = store.clone();
    let worker_started = Arc::clone(&started);
    let worker_sampled = Arc::clone(&sampled);
    let late = start + ChronoDuration::seconds(DEFAULT_LEASE_SECONDS + 1);
    let worker = thread::spawn(move || {
        worker_started.store(true, Ordering::SeqCst);
        worker_store.load_active_with_clock("clock-session", "next-owner", || {
            worker_sampled.store(true, Ordering::SeqCst);
            late
        })
    });
    wait_for_flag(&started);
    thread::sleep(Duration::from_millis(100));
    assert!(!sampled.load(Ordering::SeqCst));

    transaction.commit().unwrap();
    let active = worker.join().unwrap().unwrap();

    assert!(sampled.load(Ordering::SeqCst));
    assert_eq!(active.lease.epoch, first.epoch + 1);
}

#[test]
fn tombstoned_live_session_is_invisible_and_rejects_every_mutation() {
    let directory = tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    let start = timestamp(1_800_000_000_000);
    let session = sample_session("late-tombstone", "initial");
    let mut lease = store.create_active_at(&session, "owner", start).unwrap();
    let connection = Connection::open(store.database_path()).unwrap();
    connection
        .execute(
            "INSERT INTO session_tombstones (session_id, deleted_at_ms, reason)
             VALUES (?1, ?2, ?3)",
            rusqlite::params!["late-tombstone", start.timestamp_millis(), "test"],
        )
        .unwrap();
    drop(connection);

    assert!(store.load("late-tombstone").unwrap().is_none());
    assert!(store.list().unwrap().is_empty());
    let error = store
        .load_active_at("late-tombstone", "new-owner", start + ChronoDuration::seconds(1))
        .unwrap_err();
    assert!(matches!(error, SessionStoreError::Tombstoned { .. }));
    let error = store
        .save_at(&mut lease, &session, start + ChronoDuration::seconds(1))
        .unwrap_err();
    assert!(matches!(error, SessionStoreError::Tombstoned { .. }));
    let error = store
        .heartbeat_at(&lease, start + ChronoDuration::seconds(1))
        .unwrap_err();
    assert!(matches!(error, SessionStoreError::Tombstoned { .. }));
    let error = store
        .release_at(&lease, start + ChronoDuration::seconds(1))
        .unwrap_err();
    assert!(matches!(error, SessionStoreError::Tombstoned { .. }));
}

#[test]
fn legacy_json_import_is_idempotent_and_preserves_sources() {
    let directory = tempdir().unwrap();
    let legacy = sample_session("legacy-session", "legacy-model");
    let legacy_path = directory.path().join("2026-08-23_legacy-session.json");
    fs::write(&legacy_path, serde_json::to_vec_pretty(&legacy).unwrap()).unwrap();

    let store = SessionStore::open(directory.path()).unwrap();
    let imported = store.load("legacy-session").unwrap().unwrap();
    assert_eq!(imported.session.model, "legacy-model");
    assert!(legacy_path.is_file());

    let mut changed_source = legacy;
    changed_source.model = "must-not-reimport".into();
    fs::write(&legacy_path, serde_json::to_vec_pretty(&changed_source).unwrap()).unwrap();
    drop(store);

    let reopened = SessionStore::open(directory.path()).unwrap();
    let imported = reopened.load("legacy-session").unwrap().unwrap();
    assert_eq!(imported.session.model, "legacy-model");
    let connection = Connection::open(reopened.database_path()).unwrap();
    let source_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM session_migration_sources", [], |row| row.get(0))
        .unwrap();
    assert_eq!(source_count, 1);
}

#[test]
fn current_json_layout_is_imported_without_rewriting_the_source() {
    let directory = tempdir().unwrap();
    let legacy = sample_session("current-layout", "current-json");
    let state_directory = directory.path().join("sessions").join("current-layout");
    fs::create_dir_all(&state_directory).unwrap();
    let state_path = state_directory.join("state.json");
    let original = serde_json::to_vec_pretty(&legacy).unwrap();
    fs::write(&state_path, &original).unwrap();

    let store = SessionStore::open(directory.path()).unwrap();

    let imported = store.load("current-layout").unwrap().unwrap();
    assert_eq!(imported.session.model, "current-json");
    assert_eq!(fs::read(state_path).unwrap(), original);
}

#[test]
fn retention_allows_visible_limit_to_be_exceeded_when_every_session_is_active() {
    let directory = tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    let mut leases = Vec::new();
    for id in ["active-a", "active-b", "active-c"] {
        leases.push(
            store
                .create_active(&sample_session(id, "model"), &format!("owner-{id}"))
                .unwrap(),
        );
    }

    store.enforce_retention(2).unwrap();

    assert_eq!(store.list().unwrap().len(), 3);
    let connection = Connection::open(store.database_path()).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM session_tombstones", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    for lease in leases {
        store.release(&lease).unwrap();
    }
}

#[test]
fn tombstone_prevents_legacy_session_from_being_imported_again() {
    let directory = tempdir().unwrap();
    let initial = SessionStore::open(directory.path()).unwrap();
    let connection = Connection::open(initial.database_path()).unwrap();
    connection
        .execute(
            "INSERT INTO session_tombstones (session_id, deleted_at_ms, reason) VALUES (?1, ?2, ?3)",
            rusqlite::params!["deleted-session", 1_i64, "test"],
        )
        .unwrap();
    drop(connection);
    drop(initial);

    let legacy = sample_session("deleted-session", "must-stay-deleted");
    let legacy_path = directory.path().join("2026-08-23_deleted-session.json");
    fs::write(&legacy_path, serde_json::to_vec_pretty(&legacy).unwrap()).unwrap();

    let reopened = SessionStore::open(directory.path()).unwrap();
    assert!(reopened.load("deleted-session").unwrap().is_none());
    assert!(legacy_path.is_file());
    let connection = Connection::open(reopened.database_path()).unwrap();
    let outcome: String = connection
        .query_row(
            "SELECT outcome FROM session_migration_imports WHERE session_id = 'deleted-session'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(outcome, "tombstoned");
}

#[test]
fn session_store_rejects_redirected_root_and_database_slots() {
    let target = tempdir().unwrap();
    let alias_parent = tempdir().unwrap();
    let root_alias = alias_parent.path().join("session-root-alias");
    create_directory_redirect(target.path(), &root_alias).unwrap();
    let error = SessionStore::open(&root_alias).unwrap_err();
    assert!(matches!(error, SessionStoreError::UnsafePath { .. }));

    let symlink_root = tempdir().unwrap();
    let outside_database = symlink_root.path().join("outside.sqlite3");
    fs::write(&outside_database, []).unwrap();
    let database_alias = symlink_root.path().join("store");
    fs::create_dir(&database_alias).unwrap();
    if optional_symlink_created(
        create_file_symlink(&outside_database, &database_alias.join("session.sqlite3")),
        "session database symlink",
    ) {
        let error = SessionStore::open(&database_alias).unwrap_err();
        assert!(matches!(error, SessionStoreError::UnsafePath { .. }));
    }

    let hardlink_root = tempdir().unwrap();
    let outside_database = hardlink_root.path().join("outside.sqlite3");
    fs::write(&outside_database, []).unwrap();
    let store_root = hardlink_root.path().join("store");
    fs::create_dir(&store_root).unwrap();
    fs::hard_link(&outside_database, store_root.join("session.sqlite3")).unwrap();
    let error = SessionStore::open(&store_root).unwrap_err();
    assert!(matches!(error, SessionStoreError::UnsafePath { .. }));

    let live_root = tempdir().unwrap();
    let live_store = SessionStore::open(live_root.path()).unwrap();
    fs::hard_link(
        live_store.database_path(),
        live_root.path().join("database-hardlink.sqlite3"),
    )
    .unwrap();
    let error = live_store.load("missing").unwrap_err();
    assert!(matches!(error, SessionStoreError::UnsafePath { .. }));
}

#[test]
fn legacy_import_rejects_external_aliases_and_oversized_sources() {
    let outside = tempdir().unwrap();
    let legacy = sample_session("outside-session", "outside");
    let outside_json = outside.path().join("outside.json");
    fs::write(&outside_json, serde_json::to_vec(&legacy).unwrap()).unwrap();

    let symlink_root = tempdir().unwrap();
    if optional_symlink_created(
        create_file_symlink(&outside_json, &symlink_root.path().join("linked.json")),
        "legacy session symlink",
    ) {
        let error = SessionStore::open(symlink_root.path()).unwrap_err();
        assert!(matches!(error, SessionStoreError::UnsafePath { .. }));
    }

    let hardlink_root = tempdir().unwrap();
    fs::hard_link(&outside_json, hardlink_root.path().join("linked.json")).unwrap();
    let error = SessionStore::open(hardlink_root.path()).unwrap_err();
    assert!(matches!(error, SessionStoreError::UnsafePath { .. }));

    let oversized_root = tempdir().unwrap();
    let oversized = oversized_root.path().join("oversized.json");
    let file = fs::File::create(&oversized).unwrap();
    file.set_len(MAX_LEGACY_SESSION_BYTES + 1).unwrap();
    let error = SessionStore::open(oversized_root.path()).unwrap_err();
    assert!(matches!(
        error,
        SessionStoreError::LegacySourceTooLarge {
            max_bytes: MAX_LEGACY_SESSION_BYTES,
        }
    ));
}

#[test]
fn completed_legacy_source_is_checked_before_file_content_is_read() {
    let directory = tempdir().unwrap();
    let legacy = sample_session("read-once", "original");
    let path = directory.path().join("2026-08-23_read-once.json");
    fs::write(&path, serde_json::to_vec(&legacy).unwrap()).unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    drop(store);

    let file = fs::OpenOptions::new().write(true).open(&path).unwrap();
    file.set_len(MAX_LEGACY_SESSION_BYTES + 1).unwrap();
    let reopened = SessionStore::open(directory.path()).unwrap();

    let stored = reopened.load("read-once").unwrap().unwrap();
    assert_eq!(stored.session.model, "original");
}

#[test]
fn metadata_helper_ignores_only_not_found() {
    let directory = tempdir().unwrap();
    let missing = directory.path().join("missing.json");
    assert!(metadata_if_exists(&missing).unwrap().is_none());

    let invalid_path = directory.path().join("invalid\0path.json");
    let error = metadata_if_exists(&invalid_path).unwrap_err();
    assert!(matches!(
        error,
        SessionStoreError::Io {
            operation: "inspect legacy session source",
            ..
        }
    ));
}

#[test]
fn real_processes_allow_exactly_one_active_owner() {
    let directory = tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    let session = sample_session("shared-session", "model");
    let lease = store.create_active(&session, "setup-owner").unwrap();
    store.release(&lease).unwrap();
    let start = directory.path().join("start");

    let mut children = Vec::new();
    let mut results = Vec::new();
    for index in 0..4 {
        let result = directory.path().join(format!("claim-{index}.txt"));
        children.push(spawn_fixture(
            "claim",
            directory.path(),
            "shared-session",
            &format!("owner-{index}"),
            &result,
            FixtureSynchronization::without_ready(&start),
            None,
        ));
        results.push(result);
    }
    fs::write(&start, b"go").unwrap();
    wait_for_children(children);

    let contents: Vec<_> = results.iter().map(|path| fs::read_to_string(path).unwrap()).collect();
    assert_eq!(contents.iter().filter(|value| value.starts_with("ok:")).count(), 1);
    assert_eq!(contents.iter().filter(|value| value.starts_with("held:")).count(), 3);
}

#[test]
fn real_processes_save_different_sessions_without_corruption() {
    let directory = tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    for id in ["session-a", "session-b"] {
        let lease = store
            .create_active(&sample_session(id, "initial"), "setup-owner")
            .unwrap();
        store.release(&lease).unwrap();
    }
    let start = directory.path().join("start-save");
    let result_a = directory.path().join("save-a.txt");
    let result_b = directory.path().join("save-b.txt");
    let ready_a = directory.path().join("save-a.ready");
    let ready_b = directory.path().join("save-b.ready");
    let mut children = vec![
        spawn_fixture(
            "save-loop",
            directory.path(),
            "session-a",
            "owner-a",
            &result_a,
            FixtureSynchronization::with_ready(&start, &ready_a),
            None,
        ),
        spawn_fixture(
            "save-loop",
            directory.path(),
            "session-b",
            "owner-b",
            &result_b,
            FixtureSynchronization::with_ready(&start, &ready_b),
            None,
        ),
    ];
    wait_for_fixture_readiness(&mut children, &[&ready_a, &ready_b]);
    fs::write(&start, b"go").unwrap();
    wait_for_children(children);

    assert_eq!(fs::read_to_string(result_a).unwrap(), "ok:40");
    assert_eq!(fs::read_to_string(result_b).unwrap(), "ok:40");
    let a = store.load("session-a").unwrap().unwrap();
    let b = store.load("session-b").unwrap().unwrap();
    assert_eq!(a.revision, 40);
    assert_eq!(b.revision, 40);
    assert_eq!(a.session.model, "owner-a-39");
    assert_eq!(b.session.model, "owner-b-39");
}

#[test]
fn real_processes_reject_stale_epoch_and_revision() {
    let directory = tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    let session = sample_session("stale-session", "initial");
    let mut current = store.create_active(&session, "shared-owner").unwrap();
    let stale_revision = current.clone();
    let mut changed = session.clone();
    changed.model = "parent-save".into();
    store.save(&mut current, &changed).unwrap();

    let start_revision = directory.path().join("start-revision");
    let revision_result = directory.path().join("stale-revision.txt");
    let revision_child = spawn_fixture(
        "save-token",
        directory.path(),
        "stale-session",
        "shared-owner",
        &revision_result,
        FixtureSynchronization::without_ready(&start_revision),
        Some((&stale_revision.epoch.to_string(), &stale_revision.revision.to_string())),
    );
    fs::write(&start_revision, b"go").unwrap();
    wait_for_children(vec![revision_child]);
    assert_eq!(fs::read_to_string(revision_result).unwrap(), "revision:0:1");

    store.release(&current).unwrap();
    let next = store.load_active("stale-session", "next-owner").unwrap();
    assert!(next.lease.epoch > current.epoch);
    let start_epoch = directory.path().join("start-epoch");
    let epoch_result = directory.path().join("stale-epoch.txt");
    let epoch_child = spawn_fixture(
        "save-token",
        directory.path(),
        "stale-session",
        "shared-owner",
        &epoch_result,
        FixtureSynchronization::without_ready(&start_epoch),
        Some((&current.epoch.to_string(), &current.revision.to_string())),
    );
    fs::write(&start_epoch, b"go").unwrap();
    wait_for_children(vec![epoch_child]);
    assert_eq!(fs::read_to_string(epoch_result).unwrap(), "stale");
}

#[test]
fn forced_process_exit_before_commit_preserves_previous_state() {
    let directory = tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    let session = sample_session("crash-session", "committed");
    let lease = store.create_active(&session, "crash-owner").unwrap();
    let start = directory.path().join("start-crash");
    let result = directory.path().join("unused.txt");

    let child = spawn_fixture(
        "crash-before-commit",
        directory.path(),
        "crash-session",
        "crash-owner",
        &result,
        FixtureSynchronization::without_ready(&start),
        Some((&lease.epoch.to_string(), &lease.revision.to_string())),
    );
    fs::write(&start, b"go").unwrap();
    let (status, stderr) = wait_for_child(child);
    assert_eq!(status.code(), Some(91));
    assert!(stderr.is_empty(), "unexpected crash fixture stderr: {stderr}");

    let loaded = store.load("crash-session").unwrap().unwrap();
    assert_eq!(loaded.revision, 0);
    assert_eq!(loaded.session.model, "committed");
}

#[test]
fn process_fixture() {
    let Ok(mode) = env::var(FIXTURE_MODE) else {
        return;
    };
    let directory = PathBuf::from(env::var_os(FIXTURE_DIRECTORY).unwrap());
    let session_id = env::var(FIXTURE_SESSION_ID).unwrap();
    let owner = env::var(FIXTURE_OWNER).unwrap();
    let start = PathBuf::from(env::var_os(FIXTURE_START).unwrap());
    let result = PathBuf::from(env::var_os(FIXTURE_RESULT).unwrap());
    let store = SessionStore::open(&directory).unwrap();
    let reports_stages = env::var_os(FIXTURE_READY);
    if let Some(ready) = &reports_stages {
        fs::write(ready, b"ready").unwrap();
        eprintln!("session-process-fixture stage=ready mode={mode} session={session_id}");
    }
    wait_for_start(&start);
    if reports_stages.is_some() {
        eprintln!("session-process-fixture stage=released mode={mode} session={session_id}");
    }

    match mode.as_str() {
        "claim" => match store.load_active(&session_id, &owner) {
            Ok(active) => {
                fs::write(&result, format!("ok:{}:{}", active.lease.epoch, active.lease.revision)).unwrap();
                thread::sleep(Duration::from_millis(500));
            }
            Err(SessionStoreError::LeaseHeld { owner_id, .. }) => {
                fs::write(&result, format!("held:{owner_id}")).unwrap();
            }
            Err(error) => panic!("unexpected claim error: {error}"),
        },
        "save-loop" => {
            let mut active = store.load_active(&session_id, &owner).unwrap();
            for index in 0..40 {
                active.session.model = format!("{owner}-{index}");
                active.session.updated_at = Utc::now();
                store.save(&mut active.lease, &active.session).unwrap();
            }
            fs::write(&result, format!("ok:{}", active.lease.revision)).unwrap();
            store.release(&active.lease).unwrap();
        }
        "save-token" => {
            let epoch = env::var(FIXTURE_EPOCH).unwrap().parse().unwrap();
            let revision = env::var(FIXTURE_REVISION).unwrap().parse().unwrap();
            let mut lease = SessionLease {
                session_id: session_id.clone(),
                owner_id: owner,
                epoch,
                revision,
            };
            let mut session = store.load(&session_id).unwrap().unwrap().session;
            session.model = "child-save".into();
            let outcome = match store.save(&mut lease, &session) {
                Ok(()) => "ok".to_string(),
                Err(SessionStoreError::StaleLease { .. }) => "stale".to_string(),
                Err(SessionStoreError::RevisionConflict { expected, actual, .. }) => {
                    format!("revision:{expected}:{actual}")
                }
                Err(error) => panic!("unexpected token save error: {error}"),
            };
            fs::write(&result, outcome).unwrap();
        }
        "crash-before-commit" => {
            let epoch = env::var(FIXTURE_EPOCH).unwrap().parse().unwrap();
            let revision = env::var(FIXTURE_REVISION).unwrap().parse().unwrap();
            let lease = SessionLease {
                session_id,
                owner_id: owner,
                epoch,
                revision,
            };
            store.test_exit_before_save_commit(&lease, 91);
        }
        other => panic!("unknown fixture mode {other}"),
    }
}

fn sample_session(id: &str, model: &str) -> Session {
    let now = timestamp(1_800_000_000_000);
    Session {
        id: id.into(),
        run_id: Some(format!("run-{id}")),
        created_at: now,
        updated_at: now,
        provider: "test-provider".into(),
        model: model.into(),
        cwd: "/workspace".into(),
        total_usage: TokenUsage::default(),
        messages: Vec::new(),
        runtime_state: None,
    }
}

fn timestamp(milliseconds: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_millis_opt(milliseconds).single().unwrap()
}

fn table_names(connection: &Connection) -> Vec<String> {
    let mut statement = connection
        .prepare("SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name")
        .unwrap();
    statement
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

#[derive(Clone, Copy)]
struct FixtureSynchronization<'a> {
    release: &'a Path,
    ready: Option<&'a Path>,
}

impl<'a> FixtureSynchronization<'a> {
    fn without_ready(release: &'a Path) -> Self {
        Self { release, ready: None }
    }

    fn with_ready(release: &'a Path, ready: &'a Path) -> Self {
        Self {
            release,
            ready: Some(ready),
        }
    }
}

fn spawn_fixture(
    mode: &str,
    directory: &Path,
    session_id: &str,
    owner: &str,
    result: &Path,
    synchronization: FixtureSynchronization<'_>,
    token: Option<(&str, &str)>,
) -> Child {
    let mut command = Command::new(env::current_exe().unwrap());
    command
        .arg("session::store::store_test::process_fixture")
        .arg("--exact")
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(FIXTURE_MODE, mode)
        .env(FIXTURE_DIRECTORY, directory)
        .env(FIXTURE_SESSION_ID, session_id)
        .env(FIXTURE_OWNER, owner)
        .env(FIXTURE_START, synchronization.release)
        .env(FIXTURE_RESULT, result)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    if let Some((epoch, revision)) = token {
        command.env(FIXTURE_EPOCH, epoch).env(FIXTURE_REVISION, revision);
    }
    if let Some(ready) = synchronization.ready {
        command.env(FIXTURE_READY, ready);
    }
    command.spawn().unwrap()
}

fn wait_for_fixture_readiness(children: &mut [Child], paths: &[&Path]) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if paths.iter().all(|path| path.is_file()) {
            return;
        }
        for child in children.iter_mut() {
            if let Some(status) = child.try_wait().unwrap() {
                let stderr = read_child_stderr(child);
                panic!("fixture exited before readiness with {status}; stderr: {stderr}");
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for process fixture readiness"
        );
        thread::yield_now();
    }
}

fn wait_for_children(children: Vec<Child>) {
    for child in children {
        let (status, stderr) = wait_for_child(child);
        assert!(status.success(), "fixture exited with {status}; stderr: {stderr}");
    }
}

fn wait_for_child(mut child: Child) -> (ExitStatus, String) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return (status, read_child_stderr(&mut child));
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let status = child.wait().unwrap();
            let stderr = read_child_stderr(&mut child);
            panic!("fixture timed out and was terminated with {status}; stderr: {stderr}");
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn read_child_stderr(child: &mut Child) -> String {
    let mut stderr = Vec::new();
    if let Some(mut stream) = child.stderr.take() {
        stream.read_to_end(&mut stderr).unwrap();
    }
    String::from_utf8_lossy(&stderr).into_owned()
}

fn wait_for_start(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if path.is_file() {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for process fixture start");
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_flag(flag: &AtomicBool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !flag.load(Ordering::SeqCst) {
        assert!(Instant::now() < deadline, "timed out waiting for worker start");
        thread::sleep(Duration::from_millis(10));
    }
}
