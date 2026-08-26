use std::io::ErrorKind;
use std::sync::{Arc, mpsc};
use std::time::Duration;

use serde_json::json;

use super::*;

fn append(ledger: &dyn RuntimeLedger, run_id: &RunId, marker: &str) -> u64 {
    ledger
        .append(
            run_id,
            DurabilityClass::SyncCritical,
            "marker",
            json!({"marker": marker}),
        )
        .unwrap()
        .seq
}

fn assert_exact_run_deletion(ledger: &dyn RuntimeLedger) {
    let target = RunId::from("run-a");
    let similar = RunId::from("run-ab");
    let descendant = RunId::from("run-a:child");

    assert_eq!(append(ledger, &target, "target-1"), 1);
    assert_eq!(append(ledger, &similar, "similar"), 2);
    assert_eq!(append(ledger, &descendant, "descendant"), 3);
    assert_eq!(append(ledger, &target, "target-2"), 4);

    assert_eq!(ledger.delete_run_exact(&target).unwrap(), 2);
    assert!(ledger.records_for_run(&target).unwrap().is_empty());
    assert_eq!(ledger.records_for_run(&similar).unwrap().len(), 1);
    assert_eq!(ledger.records_for_run(&descendant).unwrap().len(), 1);
    assert_eq!(ledger.delete_run_exact(&target).unwrap(), 0);

    assert_eq!(append(ledger, &target, "after-delete"), 5);
}

#[test]
fn in_memory_delete_run_is_exact_idempotent_and_keeps_global_sequence() {
    assert_exact_run_deletion(&InMemoryRuntimeLedger::default());
}

#[test]
fn sqlite_delete_run_is_exact_idempotent_and_keeps_global_sequence() {
    let directory = tempfile::tempdir().unwrap();
    let ledger = SqliteRuntimeLedger::open(directory.path().join("runtime.sqlite3")).unwrap();

    assert_exact_run_deletion(&ledger);

    drop(ledger);
    let reopened = SqliteRuntimeLedger::open(directory.path().join("runtime.sqlite3")).unwrap();
    assert_eq!(append(&reopened, &RunId::from("reopened"), "after-reopen"), 6);
}

#[test]
fn jsonl_delete_run_is_explicitly_unsupported_and_does_not_mutate_history() {
    let directory = tempfile::tempdir().unwrap();
    let ledger = JsonlRuntimeLedger::open(directory.path().join("runtime.jsonl")).unwrap();
    let run_id = RunId::from("jsonl-run");
    append(&ledger, &run_id, "retained");

    let error = ledger.delete_run_exact(&run_id).unwrap_err();

    assert_eq!(error.kind(), ErrorKind::Unsupported);
    assert_eq!(ledger.records_for_run(&run_id).unwrap().len(), 1);
}

#[test]
fn sqlite_delete_rolls_back_when_work_fails_before_commit() {
    let directory = tempfile::tempdir().unwrap();
    let ledger = SqliteRuntimeLedger::open(directory.path().join("runtime.sqlite3")).unwrap();
    let run_id = RunId::from("rollback-target");
    append(&ledger, &run_id, "retained");

    let error = ledger
        .delete_run_exact_with_before_commit(&run_id, || Err(std::io::Error::other("injected failure")))
        .unwrap_err();

    assert_eq!(error.to_string(), "injected failure");
    assert_eq!(ledger.records_for_run(&run_id).unwrap().len(), 1);
    assert_eq!(ledger.delete_run_exact(&run_id).unwrap(), 1);
}

#[test]
fn concurrent_sqlite_deleters_serialize_and_keep_unrelated_runs() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("runtime.sqlite3");
    let first = Arc::new(SqliteRuntimeLedger::open(&path).unwrap());
    let second = Arc::new(SqliteRuntimeLedger::open(&path).unwrap());
    let target = RunId::from("concurrent-target");
    let similar = RunId::from("concurrent-target-extra");
    append(first.as_ref(), &target, "one");
    append(first.as_ref(), &target, "two");
    append(first.as_ref(), &similar, "similar");

    let (paused_tx, paused_rx) = mpsc::sync_channel(1);
    let (resume_tx, resume_rx) = mpsc::sync_channel(1);
    let first_delete = std::thread::spawn({
        let first = Arc::clone(&first);
        let target = target.clone();
        move || {
            first.delete_run_exact_with_before_commit(&target, || {
                paused_tx.send(()).unwrap();
                resume_rx.recv().unwrap();
                Ok(())
            })
        }
    });
    paused_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let second_delete = std::thread::spawn({
        let second = Arc::clone(&second);
        let target = target.clone();
        move || second.delete_run_exact(&target)
    });

    resume_tx.send(()).unwrap();

    assert_eq!(first_delete.join().unwrap().unwrap(), 2);
    assert_eq!(second_delete.join().unwrap().unwrap(), 0);
    assert_eq!(first.records_for_run(&similar).unwrap().len(), 1);
}
