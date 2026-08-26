use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::json;
use sha2::{Digest, Sha256};
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::RunId;

use super::*;

fn append_legacy_record(path: &Path, record: &LedgerRecord) {
    let mut file = OpenOptions::new().append(true).open(path).unwrap();
    serde_json::to_writer(&mut file, record).unwrap();
    file.write_all(b"\n").unwrap();
    file.sync_all().unwrap();
}

fn migration_record(run_id: &RunId, seq: u64, record_type: &str) -> LedgerRecord {
    LedgerRecord {
        schema_version: LEDGER_SCHEMA_VERSION,
        seq,
        run_id: run_id.clone(),
        timestamp_unix_ms: seq as i64,
        durability: DurabilityClass::SyncCritical,
        record_type: record_type.into(),
        payload: json!({"sequence": seq}),
    }
}

#[test]
fn completed_automatic_migration_does_not_reread_later_corruption() {
    let directory = tempfile::tempdir().unwrap();
    let jsonl_path = directory.path().join("legacy.jsonl");
    let sqlite_path = directory.path().join("runtime.sqlite3");
    let run = RunId::from("completed-source-run");
    {
        let legacy = JsonlRuntimeLedger::open(&jsonl_path).unwrap();
        legacy
            .append(&run, DurabilityClass::SyncCritical, "committed", json!({"value": 1}))
            .unwrap();
    }
    drop(SqliteRuntimeLedger::open_with_jsonl_migration(&sqlite_path, &jsonl_path).unwrap());
    let migrated_source = std::fs::read(&jsonl_path).unwrap();
    let connection = rusqlite::Connection::open(&sqlite_path).unwrap();
    let marker: (Vec<u8>, i64, i64) = connection
        .query_row(
            "SELECT content_digest, content_length, completed FROM runtime_ledger_import_sources",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(marker.0, Sha256::digest(&migrated_source).to_vec());
    assert_eq!(marker.1, i64::try_from(migrated_source.len()).unwrap());
    assert_eq!(marker.2, 1);
    drop(connection);
    OpenOptions::new()
        .append(true)
        .open(&jsonl_path)
        .unwrap()
        .write_all(b"{\"secret-corruption\":true}\n")
        .unwrap();

    let reopened = SqliteRuntimeLedger::open_with_jsonl_migration(&sqlite_path, &jsonl_path)
        .expect("completed automatic migration must not reread the source");
    let records = reopened.records_for_run(&run).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].record_type, "committed");
}

#[test]
fn automatic_migration_skips_append_but_explicit_import_adds_it() {
    let directory = tempfile::tempdir().unwrap();
    let jsonl_path = directory.path().join("legacy.jsonl");
    let sqlite_path = directory.path().join("runtime.sqlite3");
    let run = RunId::from("explicit-append-run");
    {
        let legacy = JsonlRuntimeLedger::open(&jsonl_path).unwrap();
        legacy
            .append(&run, DurabilityClass::SyncCritical, "first", json!({"value": 1}))
            .unwrap();
    }
    drop(SqliteRuntimeLedger::open_with_jsonl_migration(&sqlite_path, &jsonl_path).unwrap());
    append_legacy_record(&jsonl_path, &migration_record(&run, 2, "appended"));

    let reopened = SqliteRuntimeLedger::open_with_jsonl_migration(&sqlite_path, &jsonl_path).unwrap();
    assert_eq!(reopened.records_for_run(&run).unwrap().len(), 1);
    assert_eq!(reopened.import_jsonl(&jsonl_path).unwrap(), 1);
    let records = reopened.records_for_run(&run).unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[1].record_type, "appended");
}

#[test]
fn first_migration_rejects_a_complete_bad_line_without_writing_a_marker() {
    let directory = tempfile::tempdir().unwrap();
    let jsonl_path = directory.path().join("legacy.jsonl");
    let sqlite_path = directory.path().join("runtime.sqlite3");
    let run = RunId::from("bad-first-source-run");
    {
        let legacy = JsonlRuntimeLedger::open(&jsonl_path).unwrap();
        legacy
            .append(&run, DurabilityClass::SyncCritical, "valid", json!({}))
            .unwrap();
    }
    let valid_length = std::fs::metadata(&jsonl_path).unwrap().len();
    OpenOptions::new()
        .append(true)
        .open(&jsonl_path)
        .unwrap()
        .write_all(b"{\"secret-bad-line\":true}\n")
        .unwrap();

    let error = match SqliteRuntimeLedger::open_with_jsonl_migration(&sqlite_path, &jsonl_path) {
        Ok(_) => panic!("complete invalid source line must fail first migration"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("invalid complete record at line 2"));
    assert!(!error.contains("secret-bad-line"));
    let connection = rusqlite::Connection::open(&sqlite_path).unwrap();
    let marker_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM runtime_ledger_import_sources", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(marker_count, 0);
    let record_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM runtime_ledger_records", [], |row| row.get(0))
        .unwrap();
    assert_eq!(record_count, 0);
    drop(connection);

    OpenOptions::new()
        .write(true)
        .open(&jsonl_path)
        .unwrap()
        .set_len(valid_length)
        .unwrap();
    let recovered = SqliteRuntimeLedger::open_with_jsonl_migration(&sqlite_path, &jsonl_path).unwrap();
    assert_eq!(recovered.records_for_run(&run).unwrap().len(), 1);
}

#[test]
fn explicit_import_fingerprints_versioned_raw_line_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let jsonl_path = directory.path().join("legacy.jsonl");
    let sqlite_path = directory.path().join("runtime.sqlite3");
    std::fs::write(
        &jsonl_path,
        concat!(
            "{\"schema_version\":1,\"seq\":1,\"run_id\":\"raw-line-run\",\"timestamp_unix_ms\":1,\"durability\":\"sync_critical\",\"record_type\":\"same\",\"payload\":{\"a\":1,\"b\":2}}\n",
            "{\"payload\":{\"b\":2,\"a\":1},\"record_type\":\"same\",\"durability\":\"sync_critical\",\"timestamp_unix_ms\":1,\"run_id\":\"raw-line-run\",\"seq\":1,\"schema_version\":1}\n"
        ),
    )
    .unwrap();
    let ledger = SqliteRuntimeLedger::open(&sqlite_path).unwrap();

    assert_eq!(ledger.import_jsonl(&jsonl_path).unwrap(), 2);
    assert_eq!(ledger.import_jsonl(&jsonl_path).unwrap(), 0);
    assert_eq!(ledger.records_for_run(&RunId::from("raw-line-run")).unwrap().len(), 2);
}

#[test]
fn imported_line_fingerprint_survives_record_cleanup() {
    let directory = tempfile::tempdir().unwrap();
    let jsonl_path = directory.path().join("legacy.jsonl");
    let sqlite_path = directory.path().join("runtime.sqlite3");
    let run = RunId::from("cleanup-run");
    {
        let legacy = JsonlRuntimeLedger::open(&jsonl_path).unwrap();
        legacy
            .append(&run, DurabilityClass::SyncCritical, "imported", json!({}))
            .unwrap();
    }
    let ledger = SqliteRuntimeLedger::open(&sqlite_path).unwrap();
    assert_eq!(ledger.import_jsonl(&jsonl_path).unwrap(), 1);
    ledger
        .connection
        .lock()
        .unwrap()
        .connection
        .execute("DELETE FROM runtime_ledger_records", [])
        .unwrap();

    assert_eq!(ledger.import_jsonl(&jsonl_path).unwrap(), 0);
    assert!(ledger.records_for_run(&run).unwrap().is_empty());
}

#[test]
fn schema_v1_import_fingerprint_is_preserved_without_a_record_foreign_key() {
    let directory = tempfile::tempdir().unwrap();
    let jsonl_path = directory.path().join("legacy.jsonl");
    let sqlite_path = directory.path().join("runtime.sqlite3");
    let run = RunId::from("schema-v1-run");
    let record = migration_record(&run, 1, "already-imported");
    let encoded_record = serde_json::to_vec(&record).unwrap();
    let mut source = encoded_record.clone();
    source.push(b'\n');
    std::fs::write(&jsonl_path, source).unwrap();
    let legacy_fingerprint = Sha256::digest(&encoded_record).to_vec();
    let encoded_payload = serde_json::to_vec(&record.payload).unwrap();

    let connection = rusqlite::Connection::open(&sqlite_path).unwrap();
    connection
        .execute_batch(
            "PRAGMA foreign_keys = ON;
             CREATE TABLE runtime_ledger_meta (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                schema_version INTEGER NOT NULL,
                next_sequence INTEGER NOT NULL CHECK (next_sequence >= 0)
             );
             CREATE TABLE runtime_ledger_records (
                sequence INTEGER PRIMARY KEY,
                schema_version INTEGER NOT NULL,
                run_id TEXT NOT NULL,
                timestamp_unix_ms INTEGER NOT NULL,
                durability INTEGER NOT NULL CHECK (durability IN (1, 2)),
                record_type TEXT NOT NULL,
                payload BLOB NOT NULL
             );
             CREATE INDEX runtime_ledger_records_run_sequence
                ON runtime_ledger_records (run_id, sequence);
             CREATE TABLE runtime_ledger_imports (
                fingerprint BLOB PRIMARY KEY,
                imported_sequence INTEGER NOT NULL REFERENCES runtime_ledger_records(sequence) ON DELETE CASCADE
             );
             INSERT INTO runtime_ledger_meta (singleton, schema_version, next_sequence) VALUES (1, 1, 1);
             PRAGMA user_version = 1;",
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO runtime_ledger_records
                (sequence, schema_version, run_id, timestamp_unix_ms, durability, record_type, payload)
             VALUES (1, ?1, ?2, ?3, 1, ?4, ?5)",
            rusqlite::params![
                i64::from(record.schema_version),
                record.run_id.as_str(),
                record.timestamp_unix_ms,
                record.record_type,
                encoded_payload,
            ],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO runtime_ledger_imports (fingerprint, imported_sequence) VALUES (?1, 1)",
            rusqlite::params![legacy_fingerprint],
        )
        .unwrap();
    drop(connection);

    let ledger = SqliteRuntimeLedger::open(&sqlite_path).unwrap();
    assert_eq!(ledger.import_jsonl(&jsonl_path).unwrap(), 0);
    assert_eq!(ledger.records_for_run(&run).unwrap().len(), 1);
    let state = ledger.connection.lock().unwrap();
    state
        .connection
        .execute("DELETE FROM runtime_ledger_records", [])
        .unwrap();
    let fingerprint_count: i64 = state
        .connection
        .query_row("SELECT COUNT(*) FROM runtime_ledger_imports", [], |row| row.get(0))
        .unwrap();
    assert_eq!(fingerprint_count, 2);
    drop(state);

    assert_eq!(ledger.import_jsonl(&jsonl_path).unwrap(), 0);
    assert!(ledger.records_for_run(&run).unwrap().is_empty());
}

#[test]
fn automatic_migration_rejects_a_directory_source() {
    let directory = tempfile::tempdir().unwrap();
    let sqlite_path = directory.path().join("runtime.sqlite3");
    let error = match SqliteRuntimeLedger::open_with_jsonl_migration(&sqlite_path, directory.path()) {
        Ok(_) => panic!("legacy directory must be rejected"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("legacy runtime ledger source is not a file"));
}

#[test]
fn automatic_migration_propagates_non_not_found_metadata_errors() {
    let directory = tempfile::tempdir().unwrap();
    let sqlite_path = directory.path().join("runtime.sqlite3");
    let invalid_path = PathBuf::from("legacy\0runtime.jsonl");
    let error = match SqliteRuntimeLedger::open_with_jsonl_migration(&sqlite_path, &invalid_path) {
        Ok(_) => panic!("invalid legacy path must not be treated as missing"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("inspect legacy runtime ledger source"));
}

#[test]
fn automatic_migration_ignores_only_a_missing_source() {
    let directory = tempfile::tempdir().unwrap();
    let sqlite_path = directory.path().join("runtime.sqlite3");
    let missing_path = directory.path().join("missing.jsonl");

    let ledger = SqliteRuntimeLedger::open_with_jsonl_migration(&sqlite_path, &missing_path).unwrap();
    assert!(ledger.run_ids().unwrap().is_empty());
}

struct CurrentDirectoryGuard {
    original: PathBuf,
}

impl CurrentDirectoryGuard {
    fn change_to(path: &Path) -> Self {
        let original = std::env::current_dir().unwrap();
        std::env::set_current_dir(path).unwrap();
        Self { original }
    }
}

impl Drop for CurrentDirectoryGuard {
    fn drop(&mut self) {
        std::env::set_current_dir(&self.original).expect("restore test working directory");
    }
}

#[test]
#[serial_test::serial]
fn sqlite_export_protection_is_stable_after_working_directory_changes() {
    const CHILD_ENV: &str = "SOLARIS_SQLITE_CWD_PROTECTION_CHILD";
    const TEST_NAME: &str = "runtime_ledger::runtime_ledger_sqlite_review_test::sqlite_export_protection_is_stable_after_working_directory_changes";

    if std::env::var_os(CHILD_ENV).is_some() {
        let directory = tempfile::tempdir().unwrap();
        let open_directory = directory.path().join("open");
        let later_directory = directory.path().join("later");
        std::fs::create_dir_all(&open_directory).unwrap();
        std::fs::create_dir_all(&later_directory).unwrap();
        let cwd = CurrentDirectoryGuard::change_to(&open_directory);
        let run = RunId::from("cwd-protection-run");
        let ledger = SqliteRuntimeLedger::open("runtime.sqlite3").unwrap();
        ledger
            .append(&run, DurabilityClass::SyncCritical, "protected", json!({}))
            .unwrap();
        std::env::set_current_dir(&later_directory).unwrap();

        let error = ledger
            .export_jsonl(open_directory.join("runtime.sqlite3"))
            .expect_err("the open-time database path must remain protected")
            .to_string();
        assert!(error.contains("protected internal state"));
        assert_eq!(ledger.records_for_run(&run).unwrap().len(), 1);
        drop(ledger);
        drop(cwd);
        return;
    }

    let mut child = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(TEST_NAME)
        .arg("--nocapture")
        .env(CHILD_ENV, "1")
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "working-directory protection child failed: {status}");
            break;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("working-directory protection child timed out");
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[cfg(unix)]
#[test]
fn sqlite_export_rejects_the_open_database_after_its_path_is_renamed() {
    let directory = tempfile::tempdir().unwrap();
    let sqlite_path = directory.path().join("runtime.sqlite3");
    let renamed_path = directory.path().join("renamed-runtime.sqlite3");
    let run = RunId::from("renamed-open-database-run");
    let ledger = SqliteRuntimeLedger::open(&sqlite_path).unwrap();
    ledger
        .append(
            &run,
            DurabilityClass::SyncCritical,
            "protected-before-rename",
            json!({"value": 1}),
        )
        .unwrap();
    std::fs::rename(&sqlite_path, &renamed_path).unwrap();

    let error = ledger
        .export_jsonl(&renamed_path)
        .expect_err("an open SQLite object must stay protected after rename");

    assert!(error.to_string().contains("protected internal state"));
    let records = ledger.records_for_run(&run).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].record_type, "protected-before-rename");
}

fn terminate_children(children: &mut [Child]) {
    for child in children.iter_mut() {
        if child.try_wait().ok().flatten().is_none() {
            let _ = child.kill();
        }
    }
    for child in children.iter_mut() {
        let _ = child.wait();
    }
}

#[test]
fn sqlite_processes_allocate_one_global_sequence_without_loss() {
    const CHILD_ENV: &str = "SOLARIS_SQLITE_CONCURRENT_PROCESS_CHILD";
    const DATABASE_ENV: &str = "SOLARIS_SQLITE_CONCURRENT_PROCESS_DATABASE";
    const READY_ENV: &str = "SOLARIS_SQLITE_CONCURRENT_PROCESS_READY";
    const START_ENV: &str = "SOLARIS_SQLITE_CONCURRENT_PROCESS_START";
    const TEST_NAME: &str =
        "runtime_ledger::runtime_ledger_sqlite_review_test::sqlite_processes_allocate_one_global_sequence_without_loss";

    if let Some(producer) = std::env::var_os(CHILD_ENV) {
        let path = PathBuf::from(std::env::var_os(DATABASE_ENV).unwrap());
        let ready = PathBuf::from(std::env::var_os(READY_ENV).unwrap());
        let start = PathBuf::from(std::env::var_os(START_ENV).unwrap());
        let ledger = SqliteRuntimeLedger::open(path).unwrap();
        std::fs::write(&ready, b"ready").unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !start.is_file() {
            assert!(Instant::now() < deadline, "concurrent child start timed out");
            std::thread::sleep(Duration::from_millis(5));
        }
        let producer = producer.to_string_lossy();
        for index in 0..75 {
            ledger
                .append(
                    &RunId::from("process-concurrent-run"),
                    DurabilityClass::AsyncDurable,
                    "process-concurrent",
                    json!({"producer": producer, "index": index}),
                )
                .unwrap();
        }
        return;
    }

    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("runtime.sqlite3");
    drop(SqliteRuntimeLedger::open(&database).unwrap());
    let start = directory.path().join("start");
    let mut children = Vec::new();
    let mut ready_paths = Vec::new();
    for producer in ["first", "second"] {
        let ready = directory.path().join(format!("{producer}.ready"));
        ready_paths.push(ready.clone());
        children.push(
            Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg(TEST_NAME)
                .arg("--nocapture")
                .env(CHILD_ENV, producer)
                .env(DATABASE_ENV, &database)
                .env(READY_ENV, ready)
                .env(START_ENV, &start)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
    }
    let ready_deadline = Instant::now() + Duration::from_secs(10);
    while !ready_paths.iter().all(|path| path.is_file()) {
        if Instant::now() >= ready_deadline {
            terminate_children(&mut children);
            panic!("concurrent SQLite children did not become ready");
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    std::fs::write(&start, b"start").unwrap();

    let completion_deadline = Instant::now() + Duration::from_secs(15);
    let mut completed = vec![false; children.len()];
    while completed.iter().any(|completed| !completed) {
        for (index, child) in children.iter_mut().enumerate() {
            if completed[index] {
                continue;
            }
            if let Some(status) = child.try_wait().unwrap() {
                if !status.success() {
                    terminate_children(&mut children);
                    panic!("concurrent SQLite child failed: {status}");
                }
                completed[index] = true;
            }
        }
        if Instant::now() >= completion_deadline {
            terminate_children(&mut children);
            panic!("concurrent SQLite children timed out");
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    for child in &mut children {
        let _ = child.wait();
    }

    let ledger = SqliteRuntimeLedger::open(&database).unwrap();
    let records = ledger.records_for_run(&RunId::from("process-concurrent-run")).unwrap();
    assert_eq!(records.len(), 150);
    assert_eq!(
        records.iter().map(|record| record.seq).collect::<Vec<_>>(),
        (1..=150).collect::<Vec<_>>()
    );
    let identities = records
        .iter()
        .map(|record| {
            format!(
                "{}:{}",
                record.payload["producer"].as_str().unwrap(),
                record.payload["index"].as_u64().unwrap()
            )
        })
        .collect::<HashSet<_>>();
    assert_eq!(identities.len(), 150);
}
