use std::fs;
use std::path::{Path, PathBuf};

use rusqlite::Connection;
use tempfile::tempdir;

use super::store_test_support::{create_directory_redirect, create_file_symlink, optional_symlink_created};
use super::*;

const SQLITE_SIDECAR_SUFFIXES: [&str; 3] = ["-wal", "-shm", "-journal"];

#[test]
fn redirected_intermediate_parent_is_rejected_before_descendants_are_created() {
    let outside = tempdir().unwrap();
    let requested_parent = tempdir().unwrap();
    let redirected_parent = requested_parent.path().join("redirected-parent");
    create_directory_redirect(outside.path(), &redirected_parent).unwrap();
    let external_descendant = outside.path().join("must-not-be-created");
    let requested_root = redirected_parent.join("must-not-be-created").join("session-root");

    let error = SessionStore::open(&requested_root).unwrap_err();

    assert!(matches!(error, SessionStoreError::UnsafePath { .. }));
    assert!(
        !external_descendant.exists(),
        "validation must happen before creating a descendant through a redirected parent"
    );
}

#[test]
fn preexisting_sqlite_sidecar_aliases_are_rejected_without_touching_targets() {
    for suffix in SQLITE_SIDECAR_SUFFIXES {
        assert_preexisting_sidecar_hardlink_is_rejected(suffix);
        assert_preexisting_sidecar_symlink_is_rejected_or_explicitly_skipped(suffix);
    }
}

#[test]
fn every_connection_revalidates_sqlite_sidecar_slots() {
    let directory = tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();

    for suffix in SQLITE_SIDECAR_SUFFIXES {
        let sidecar = sqlite_sidecar_path(directory.path(), suffix);
        if sidecar.exists() {
            fs::remove_file(&sidecar).unwrap();
        }
        let outside = directory.path().join(format!("outside-live{suffix}"));
        let sentinel = format!("outside live sidecar {suffix}").into_bytes();
        fs::write(&outside, &sentinel).unwrap();
        fs::hard_link(&outside, &sidecar).unwrap();

        let error = store.load("missing").unwrap_err();

        assert!(matches!(error, SessionStoreError::UnsafePath { .. }), "suffix {suffix}");
        assert_eq!(fs::read(&outside).unwrap(), sentinel, "suffix {suffix}");
        fs::remove_file(&sidecar).unwrap();
    }
}

#[test]
fn live_connection_keeps_and_revalidates_sqlite_sidecar_slots() {
    let directory = tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    let connection = store.open_connection().unwrap();
    let wal = sqlite_sidecar_path(directory.path(), "-wal");

    match fs::remove_file(&wal) {
        Ok(()) => {
            fs::write(&wal, b"replacement").unwrap();
            let error = connection.verify_storage_slots().unwrap_err();
            assert!(matches!(error, SessionStoreError::UnsafePath { .. }));
        }
        Err(error) => {
            #[cfg(not(windows))]
            panic!("unexpected sidecar removal failure: {error}");
            #[cfg(windows)]
            let _expected_open_handle_rejection = error;
            connection.verify_storage_slots().unwrap();
        }
    }
}

#[cfg(windows)]
#[test]
fn windows_junction_cannot_occupy_a_sqlite_sidecar_slot() {
    for suffix in SQLITE_SIDECAR_SUFFIXES {
        let directory = tempdir().unwrap();
        let store_root = directory.path().join("store");
        fs::create_dir(&store_root).unwrap();
        let outside = directory.path().join(format!("outside-directory{suffix}"));
        fs::create_dir(&outside).unwrap();
        let sentinel = outside.join("sentinel.txt");
        fs::write(&sentinel, b"must remain unchanged").unwrap();
        create_directory_redirect(&outside, &sqlite_sidecar_path(&store_root, suffix)).unwrap();

        let error = SessionStore::open(&store_root).unwrap_err();

        assert!(matches!(error, SessionStoreError::UnsafePath { .. }), "suffix {suffix}");
        assert_eq!(
            fs::read(&sentinel).unwrap(),
            b"must remain unchanged",
            "suffix {suffix}"
        );
    }
}

#[test]
fn run_references_require_a_session_and_restrict_session_deletion() {
    let directory = tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    let connection = store.open_connection().unwrap();

    let missing_session = connection.execute(
        "INSERT INTO session_run_references
            (session_id, run_id, reference_kind, created_at_ms)
         VALUES ('missing', 'run', 'owner', 1)",
        [],
    );
    assert!(missing_session.is_err());

    connection
        .execute(
            "INSERT INTO sessions
                (session_id, state_json, revision, run_id, created_at_ms, updated_at_ms)
             VALUES ('retained', X'7B7D', 0, NULL, 1, 1)",
            [],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO session_run_references
                (session_id, run_id, reference_kind, created_at_ms)
             VALUES ('retained', 'run', 'owner', 1)",
            [],
        )
        .unwrap();

    let delete = connection.execute("DELETE FROM sessions WHERE session_id = 'retained'", []);
    assert!(delete.is_err());
    let retained: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sessions WHERE session_id = 'retained'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(retained, 1);

    let on_delete = run_reference_on_delete_action(&connection);
    assert_eq!(on_delete, "RESTRICT");
}

fn assert_preexisting_sidecar_hardlink_is_rejected(suffix: &str) {
    let directory = tempdir().unwrap();
    let store_root = directory.path().join("store");
    fs::create_dir(&store_root).unwrap();
    let outside = directory.path().join(format!("outside{suffix}"));
    let sentinel = format!("outside sidecar {suffix}").into_bytes();
    fs::write(&outside, &sentinel).unwrap();
    fs::hard_link(&outside, sqlite_sidecar_path(&store_root, suffix)).unwrap();

    let error = SessionStore::open(&store_root).unwrap_err();

    assert!(matches!(error, SessionStoreError::UnsafePath { .. }), "suffix {suffix}");
    assert_eq!(fs::read(&outside).unwrap(), sentinel, "suffix {suffix}");
}

fn assert_preexisting_sidecar_symlink_is_rejected_or_explicitly_skipped(suffix: &str) {
    let directory = tempdir().unwrap();
    let store_root = directory.path().join("store");
    fs::create_dir(&store_root).unwrap();
    let outside = directory.path().join(format!("outside{suffix}"));
    let sentinel = format!("outside sidecar {suffix}").into_bytes();
    fs::write(&outside, &sentinel).unwrap();
    let sidecar = sqlite_sidecar_path(&store_root, suffix);
    if !optional_symlink_created(create_file_symlink(&outside, &sidecar), "SQLite sidecar symlink") {
        return;
    }

    let error = SessionStore::open(&store_root).unwrap_err();

    assert!(matches!(error, SessionStoreError::UnsafePath { .. }), "suffix {suffix}");
    assert_eq!(fs::read(&outside).unwrap(), sentinel, "suffix {suffix}");
}

fn sqlite_sidecar_path(directory: &Path, suffix: &str) -> PathBuf {
    directory.join(format!("{DATABASE_FILE_NAME}{suffix}"))
}

fn run_reference_on_delete_action(connection: &Connection) -> String {
    connection
        .query_row("PRAGMA foreign_key_list(session_run_references)", [], |row| row.get(6))
        .unwrap()
}
