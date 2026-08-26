use std::sync::{Arc, mpsc};
use std::time::Duration;

use serde_json::json;
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::RunId;

use super::{RuntimeLedger, SqliteRuntimeLedger};

#[test]
fn sqlite_tree_query_treats_like_wildcards_as_literal_run_id_characters() {
    let directory = tempfile::tempdir().unwrap();
    let ledger = SqliteRuntimeLedger::open(directory.path().join("runtime.sqlite3")).unwrap();
    let root = RunId::from("root%_literal");
    let descendant = RunId::from("root%_literal:child");
    let like_false_positive = RunId::from("rootXXliteral:child");
    let underscore_false_positive = RunId::from("root%Xliteral:child");
    for (run_id, record_type) in [
        (&root, "root"),
        (&like_false_positive, "percent-false-positive"),
        (&descendant, "descendant-one"),
        (&underscore_false_positive, "underscore-false-positive"),
        (&descendant, "descendant-two"),
    ] {
        ledger
            .append(run_id, DurabilityClass::AsyncDurable, record_type, json!({}))
            .unwrap();
    }

    let page = ledger.records_after_tree(&root, 1, 2).unwrap();
    assert_eq!(page.iter().map(|record| record.seq).collect::<Vec<_>>(), vec![3, 5]);
    assert_eq!(ledger.last_sequence_tree(&root).unwrap(), 5);
}

#[test]
fn sqlite_tree_query_uses_one_read_snapshot_during_concurrent_append() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("runtime.sqlite3");
    let reader = Arc::new(SqliteRuntimeLedger::open(&path).unwrap());
    let writer = SqliteRuntimeLedger::open(&path).unwrap();
    let root = RunId::from("snapshot-root");
    for index in 0..300 {
        reader
            .append(
                &RunId::new(format!("snapshot-root:child:{index:03}")),
                DurabilityClass::AsyncDurable,
                "seed",
                json!({"index": index}),
            )
            .unwrap();
    }

    let (query_paused_tx, query_paused_rx) = mpsc::sync_channel(1);
    let (resume_query_tx, resume_query_rx) = mpsc::sync_channel(1);
    reader
        .connection
        .lock()
        .unwrap()
        .connection
        .progress_handler(
            100,
            Some({
                let mut paused = false;
                move || {
                    if !paused {
                        paused = true;
                        query_paused_tx.send(()).unwrap();
                        resume_query_rx.recv().unwrap();
                    }
                    false
                }
            }),
        )
        .unwrap();
    let query = std::thread::spawn({
        let reader = Arc::clone(&reader);
        let root = root.clone();
        move || reader.records_after_tree(&root, 0, 1_000).unwrap()
    });
    if let Err(error) = query_paused_rx.recv_timeout(Duration::from_secs(5)) {
        let _ = resume_query_tx.send(());
        let _ = query.join();
        panic!("tree query did not reach the controlled snapshot point: {error}");
    }
    let concurrent = writer
        .append(
            &RunId::from("snapshot-root:child:000"),
            DurabilityClass::SyncCritical,
            "concurrent",
            json!({}),
        )
        .unwrap();
    resume_query_tx.send(()).unwrap();
    let page = query.join().unwrap();

    assert_eq!(page.len(), 300);
    assert!(!page.iter().any(|record| record.seq == concurrent.seq));
}
