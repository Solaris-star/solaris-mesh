use std::io::{self, ErrorKind};

use rusqlite::Connection;
use serde_json::json;
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::RunId;

use super::runtime_ledger_migration::initialize_sqlite_schema_with_before_commit;
use super::runtime_ledger_unique::compare_and_append_sqlite;
use super::{InMemoryRuntimeLedger, JsonlRuntimeLedger, LogicalAppendCapability, RuntimeLedger, SqliteRuntimeLedger};

const RECORD_TYPE: &str = "logical_test_record";
const IDENTITY_FIELDS: &[&str] = &["task_id", "round"];

fn payload(body: &str) -> serde_json::Value {
    json!({ "task_id": "task", "round": 1, "body": body })
}

#[test]
fn jsonl_logical_append_is_explicitly_unsupported() {
    let directory = tempfile::tempdir().unwrap();
    let ledger = JsonlRuntimeLedger::open(directory.path().join("ledger.jsonl")).unwrap();
    assert_eq!(ledger.logical_append_capability(), LogicalAppendCapability::Unsupported);

    let error = ledger
        .compare_and_append(
            &RunId::from("jsonl-logical"),
            DurabilityClass::SyncCritical,
            RECORD_TYPE,
            IDENTITY_FIELDS,
            payload("body"),
        )
        .unwrap_err();

    assert_eq!(error.kind(), ErrorKind::Unsupported);
    assert!(
        ledger
            .records_for_run(&RunId::from("jsonl-logical"))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn logical_append_capabilities_state_their_process_boundary() {
    let memory = InMemoryRuntimeLedger::default();
    let directory = tempfile::tempdir().unwrap();
    let sqlite = SqliteRuntimeLedger::open(directory.path().join("ledger.sqlite3")).unwrap();

    assert_eq!(
        memory.logical_append_capability(),
        LogicalAppendCapability::ProcessLocal
    );
    assert_eq!(
        sqlite.logical_append_capability(),
        LogicalAppendCapability::CrossProcess
    );
}

#[test]
fn sqlite_commit_success_error_recovers_the_original_sequence() {
    let directory = tempfile::tempdir().unwrap();
    let ledger = SqliteRuntimeLedger::open(directory.path().join("ledger.sqlite3")).unwrap();
    let run_id = RunId::from("logical-commit-recovery");
    let expected = payload("body");

    let recovered = compare_and_append_sqlite(
        &ledger,
        &run_id,
        DurabilityClass::SyncCritical,
        RECORD_TYPE,
        IDENTITY_FIELDS,
        expected.clone(),
        (|| Ok(()), || Err(io::Error::other("injected error after commit"))),
    )
    .unwrap();
    let replay = ledger
        .compare_and_append(
            &run_id,
            DurabilityClass::SyncCritical,
            RECORD_TYPE,
            IDENTITY_FIELDS,
            expected,
        )
        .unwrap();

    assert_eq!(recovered.seq, replay.seq);
    assert_eq!(ledger.records_for_run(&run_id).unwrap().len(), 1);
}

#[test]
fn sqlite_legacy_identity_backfill_rolls_back_and_retries_idempotently() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ledger.sqlite3");
    let ledger = SqliteRuntimeLedger::open(&path).unwrap();
    let run_id = RunId::from("logical-backfill-retry");
    let expected = payload("legacy");
    let legacy = ledger
        .append(&run_id, DurabilityClass::SyncCritical, RECORD_TYPE, expected.clone())
        .unwrap();

    let error = compare_and_append_sqlite(
        &ledger,
        &run_id,
        DurabilityClass::SyncCritical,
        RECORD_TYPE,
        IDENTITY_FIELDS,
        expected.clone(),
        (
            || Err(io::Error::other("injected crash before backfill commit")),
            || Ok(()),
        ),
    )
    .unwrap_err();
    assert_eq!(error.to_string(), "injected crash before backfill commit");
    assert_eq!(logical_mapping_count(&path), 0);

    let recovered = ledger
        .compare_and_append(
            &run_id,
            DurabilityClass::SyncCritical,
            RECORD_TYPE,
            IDENTITY_FIELDS,
            expected.clone(),
        )
        .unwrap();
    let replay = ledger
        .compare_and_append(
            &run_id,
            DurabilityClass::SyncCritical,
            RECORD_TYPE,
            IDENTITY_FIELDS,
            expected,
        )
        .unwrap();

    assert_eq!(recovered.seq, legacy.seq);
    assert_eq!(replay.seq, legacy.seq);
    assert_eq!(logical_mapping_count(&path), 1);
    assert_eq!(ledger.records_for_run(&run_id).unwrap().len(), 1);
}

#[test]
fn sqlite_legacy_duplicate_identity_fails_closed() {
    let directory = tempfile::tempdir().unwrap();
    let ledger = SqliteRuntimeLedger::open(directory.path().join("ledger.sqlite3")).unwrap();
    let run_id = RunId::from("logical-legacy-duplicate");
    for body in ["first", "second"] {
        ledger
            .append(&run_id, DurabilityClass::SyncCritical, RECORD_TYPE, payload(body))
            .unwrap();
    }

    let error = ledger
        .compare_and_append(
            &run_id,
            DurabilityClass::SyncCritical,
            RECORD_TYPE,
            IDENTITY_FIELDS,
            payload("first"),
        )
        .unwrap_err();

    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert!(error.to_string().contains("multiple durable records"));
}

#[test]
fn sqlite_schema_migration_rolls_back_and_is_idempotent() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ledger.sqlite3");
    let mut connection = Connection::open(&path).unwrap();

    let error = initialize_sqlite_schema_with_before_commit(&mut connection, || {
        Err(io::Error::other("injected migration crash"))
    })
    .unwrap_err();
    assert_eq!(error.to_string(), "injected migration crash");
    assert_eq!(schema_version(&connection), 0);
    assert!(!table_exists(&connection, "runtime_ledger_logical_records"));

    initialize_sqlite_schema_with_before_commit(&mut connection, || Ok(())).unwrap();
    initialize_sqlite_schema_with_before_commit(&mut connection, || Ok(())).unwrap();
    assert_eq!(schema_version(&connection), 4);
    assert!(table_exists(&connection, "runtime_ledger_logical_records"));
}

#[test]
fn sqlite_schema_v2_upgrades_to_logical_identity_schema() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ledger.sqlite3");
    let mut connection = Connection::open(&path).unwrap();
    initialize_sqlite_schema_with_before_commit(&mut connection, || Ok(())).unwrap();
    connection
        .execute_batch(
            "DROP TABLE runtime_ledger_logical_records;
             DROP TABLE runtime_ledger_workflow_mutation_leases;
             UPDATE runtime_ledger_meta SET schema_version = 2 WHERE singleton = 1;
             PRAGMA user_version = 2;",
        )
        .unwrap();

    initialize_sqlite_schema_with_before_commit(&mut connection, || Ok(())).unwrap();

    assert_eq!(schema_version(&connection), 4);
    assert!(table_exists(&connection, "runtime_ledger_logical_records"));
    assert!(table_exists(&connection, "runtime_ledger_workflow_mutation_leases"));
}

#[test]
fn sqlite_schema_v3_upgrades_to_workflow_mutation_lease_schema() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ledger.sqlite3");
    let mut connection = Connection::open(&path).unwrap();
    initialize_sqlite_schema_with_before_commit(&mut connection, || Ok(())).unwrap();
    connection
        .execute_batch(
            "DROP TABLE runtime_ledger_workflow_mutation_leases;
             UPDATE runtime_ledger_meta SET schema_version = 3 WHERE singleton = 1;
             PRAGMA user_version = 3;",
        )
        .unwrap();

    initialize_sqlite_schema_with_before_commit(&mut connection, || Ok(())).unwrap();

    assert_eq!(schema_version(&connection), 4);
    assert!(table_exists(&connection, "runtime_ledger_workflow_mutation_leases"));
}

fn logical_mapping_count(path: &std::path::Path) -> i64 {
    Connection::open(path)
        .unwrap()
        .query_row("SELECT COUNT(*) FROM runtime_ledger_logical_records", [], |row| {
            row.get(0)
        })
        .unwrap()
}

fn schema_version(connection: &Connection) -> i64 {
    connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap()
}

fn table_exists(connection: &Connection, table: &str) -> bool {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
            [table],
            |row| row.get(0),
        )
        .unwrap()
}
