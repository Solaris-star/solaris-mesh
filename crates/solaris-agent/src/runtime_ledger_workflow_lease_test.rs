use std::io::ErrorKind;

use serde_json::json;
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::RunId;

use super::{InMemoryRuntimeLedger, RuntimeLedger, SqliteRuntimeLedger, WorkflowRestoreCommit};

#[test]
fn sqlite_workflow_mutation_lease_takeover_fences_old_epoch() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ledger.sqlite3");
    let first = SqliteRuntimeLedger::open(&path).unwrap();
    let second = SqliteRuntimeLedger::open(&path).unwrap();
    let run_id = RunId::from("workflow-lease-takeover");
    let old = first.acquire_workflow_mutation_lease(&run_id, "first", 1_000).unwrap();

    assert_eq!(
        second
            .acquire_workflow_mutation_lease(&run_id, "second", 1_001)
            .unwrap_err()
            .kind(),
        ErrorKind::WouldBlock
    );
    let replacement = second
        .acquire_workflow_mutation_lease(&run_id, "second", 91_000)
        .unwrap();

    assert!(replacement.epoch > old.epoch);
    assert_eq!(
        first.release_workflow_mutation_lease(&old).unwrap_err().kind(),
        ErrorKind::PermissionDenied
    );
    assert_eq!(
        first.commit_workflow_restore(&old, 0, 91_001).unwrap_err().kind(),
        ErrorKind::PermissionDenied
    );
    second.release_workflow_mutation_lease(&replacement).unwrap();
}

#[test]
fn sqlite_workflow_mutation_lease_rejects_clock_rollback_and_lost_renewal() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ledger.sqlite3");
    let first = SqliteRuntimeLedger::open(&path).unwrap();
    let second = SqliteRuntimeLedger::open(&path).unwrap();
    let run_id = RunId::from("workflow-lease-clock");
    let old = first.acquire_workflow_mutation_lease(&run_id, "first", 10_000).unwrap();

    assert_eq!(
        first.renew_workflow_mutation_lease(&old, 9_999).unwrap_err().kind(),
        ErrorKind::InvalidData
    );
    let replacement = second
        .acquire_workflow_mutation_lease(&run_id, "second", 100_000)
        .unwrap();
    assert_eq!(
        first.renew_workflow_mutation_lease(&old, 100_001).unwrap_err().kind(),
        ErrorKind::PermissionDenied
    );
    second.release_workflow_mutation_lease(&replacement).unwrap();
}

#[test]
fn sqlite_workflow_restore_high_water_detects_change_and_replays_after_crash() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ledger.sqlite3");
    let first = SqliteRuntimeLedger::open(&path).unwrap();
    let run_id = RunId::from("workflow-lease-high-water");
    first
        .append(
            &run_id,
            DurabilityClass::SyncCritical,
            "workflow_started",
            json!({"run": "one"}),
        )
        .unwrap();
    let lease = first.acquire_workflow_mutation_lease(&run_id, "first", 1_000).unwrap();
    first
        .append(
            &run_id,
            DurabilityClass::SyncCritical,
            "workflow_node_completed",
            json!({"node": "work"}),
        )
        .unwrap();

    let stale = first
        .commit_workflow_restore(&lease, lease.observed_sequence, 1_001)
        .unwrap();
    let current_sequence = match stale {
        WorkflowRestoreCommit::Stale { current_sequence } => current_sequence,
        WorkflowRestoreCommit::Current => panic!("changed high-water was accepted"),
    };
    assert_eq!(
        first.commit_workflow_restore(&lease, current_sequence, 1_002).unwrap(),
        WorkflowRestoreCommit::Current
    );
    drop(first);

    let restarted = SqliteRuntimeLedger::open(&path).unwrap();
    let replay = restarted
        .acquire_workflow_mutation_lease(&run_id, "replacement", 91_003)
        .unwrap();
    assert_eq!(
        restarted
            .commit_workflow_restore(&replay, current_sequence, 91_004)
            .unwrap(),
        WorkflowRestoreCommit::Current
    );
    restarted.release_workflow_mutation_lease(&replay).unwrap();
}

#[test]
fn guarded_appends_advance_the_in_memory_lease_observed_sequence() {
    assert_guarded_appends_advance_observed_sequence(&InMemoryRuntimeLedger::default());
}

#[test]
fn guarded_appends_advance_the_sqlite_lease_observed_sequence_in_their_transaction() {
    let directory = tempfile::tempdir().unwrap();
    let ledger = SqliteRuntimeLedger::open(directory.path().join("ledger.sqlite3")).unwrap();
    assert_guarded_appends_advance_observed_sequence(&ledger);
}

fn assert_guarded_appends_advance_observed_sequence(ledger: &dyn RuntimeLedger) {
    let run_id = RunId::from("workflow-lease-append-high-water");
    let lease = ledger
        .acquire_workflow_mutation_lease(&run_id, "controller", 1_000)
        .unwrap();
    assert_eq!(lease.observed_sequence, 0);

    let appended = ledger
        .append_under_workflow_lease(
            &lease,
            1_001,
            DurabilityClass::SyncCritical,
            "workflow_started",
            json!({"run": "one"}),
        )
        .unwrap();
    let renewed = ledger.renew_workflow_mutation_lease(&lease, 1_002).unwrap();
    assert_eq!(renewed.observed_sequence, appended.seq);

    let logical = ledger
        .compare_and_append_under_workflow_lease(
            &renewed,
            1_003,
            DurabilityClass::SyncCritical,
            "workflow_logical",
            &["operation_id"],
            json!({"operation_id": "one", "value": 1}),
        )
        .unwrap();
    let renewed = ledger.renew_workflow_mutation_lease(&renewed, 1_004).unwrap();
    assert_eq!(renewed.observed_sequence, logical.seq);

    let replayed = ledger
        .compare_and_append_under_workflow_lease(
            &renewed,
            1_005,
            DurabilityClass::SyncCritical,
            "workflow_logical",
            &["operation_id"],
            json!({"operation_id": "one", "value": 1}),
        )
        .unwrap();
    let renewed = ledger.renew_workflow_mutation_lease(&renewed, 1_006).unwrap();
    assert_eq!(replayed.seq, logical.seq);
    assert_eq!(renewed.observed_sequence, logical.seq);
    ledger.release_workflow_mutation_lease(&renewed).unwrap();
}
