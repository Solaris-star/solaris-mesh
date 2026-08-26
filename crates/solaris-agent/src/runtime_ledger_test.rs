use serde_json::json;
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::RunId;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::{Arc, Mutex};

use super::*;

#[test]
fn in_memory_ledger_sequences_are_global_and_monotonic() {
    let ledger = InMemoryRuntimeLedger::default();
    let run_a = RunId::from("a");
    let run_b = RunId::from("b");
    assert_eq!(
        ledger
            .append(&run_a, DurabilityClass::SyncCritical, "one", json!({}))
            .unwrap()
            .seq,
        1
    );
    assert_eq!(
        ledger
            .append(&run_a, DurabilityClass::SyncCritical, "two", json!({}))
            .unwrap()
            .seq,
        2
    );
    assert_eq!(
        ledger
            .append(&run_b, DurabilityClass::SyncCritical, "one", json!({}))
            .unwrap()
            .seq,
        3
    );
}

#[test]
fn tree_journal_pages_interleaved_root_and_child_records_once() {
    let ledger = InMemoryRuntimeLedger::default();
    let root = RunId::from("root");
    let child_a = RunId::from("root:workflow:a");
    let child_b = RunId::from("root:workflow:b");
    let unrelated = RunId::from("other");
    for (run_id, record_type) in [
        (&root, "root-one"),
        (&child_a, "child-a"),
        (&unrelated, "other"),
        (&child_b, "child-b"),
        (&root, "root-two"),
    ] {
        ledger
            .append(run_id, DurabilityClass::SyncCritical, record_type, json!({}))
            .unwrap();
    }

    assert_eq!(ledger.last_sequence_tree(&root).unwrap(), 5);
    let page = ledger.records_after_tree(&root, 1, 10).unwrap();
    assert_eq!(page.iter().map(|record| record.seq).collect::<Vec<_>>(), vec![2, 4, 5]);
    assert_eq!(
        page.iter()
            .map(|record| record.record_type.as_str())
            .collect::<Vec<_>>(),
        vec!["child-a", "child-b", "root-two"]
    );
}

#[test]
fn ephemeral_jsonl_record_is_not_persisted() {
    let path = std::env::temp_dir().join(format!("solaris-ledger-{}.jsonl", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let ledger = JsonlRuntimeLedger::open(&path).unwrap();
    let run = RunId::from("run");
    ledger
        .append(&run, DurabilityClass::Ephemeral, "progress", json!({"n": 1}))
        .unwrap();
    assert!(ledger.records_for_run(&run).unwrap().is_empty());
    let _ = std::fs::remove_file(path);
}

#[test]
fn jsonl_recovery_discards_an_uncommitted_trailing_record() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("runtime.jsonl");
    let run = RunId::from("run");
    {
        let ledger = JsonlRuntimeLedger::open(&path).unwrap();
        ledger
            .append(&run, DurabilityClass::SyncCritical, "committed", json!({"n": 1}))
            .unwrap();
    }
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(br#"{"schema_version":1,"seq":2"#)
        .unwrap();

    let ledger = JsonlRuntimeLedger::open(&path).expect("a torn final record must not hide committed history");
    let records = ledger.records_for_run(&run).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].record_type, "committed");
    assert_eq!(ledger.last_sequence(&run).unwrap(), 1);
    assert!(std::fs::read(&path).unwrap().ends_with(b"\n"));

    assert_eq!(
        ledger
            .append(&run, DurabilityClass::SyncCritical, "after-recovery", json!({"n": 2}))
            .unwrap()
            .seq,
        2
    );
}

struct FailFirstSyncWriter {
    bytes: Vec<u8>,
    fail_next_sync: bool,
}

impl Write for FailFirstSyncWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl JsonlWriter for FailFirstSyncWriter {
    fn sync_data(&mut self) -> std::io::Result<()> {
        if self.fail_next_sync {
            self.fail_next_sync = false;
            return Err(std::io::Error::other("injected sync failure"));
        }
        Ok(())
    }
}

#[test]
fn jsonl_sync_failure_does_not_reuse_the_written_sequence() {
    let directory = tempfile::tempdir().unwrap();
    let run = RunId::from("run");
    let ledger = JsonlRuntimeLedger {
        path: directory.path().join("unused.jsonl"),
        state: Mutex::new(JsonlLedgerState {
            writer: Box::new(FailFirstSyncWriter {
                bytes: Vec::new(),
                fail_next_sync: true,
            }),
            sequences: HashMap::new(),
            next_sequence: 0,
            poisoned: false,
        }),
    };

    assert!(
        ledger
            .append(&run, DurabilityClass::SyncCritical, "uncertain", json!({}))
            .is_err()
    );
    assert_eq!(ledger.last_sequence(&run).unwrap(), 1);
    assert_eq!(
        ledger
            .append(&run, DurabilityClass::SyncCritical, "next", json!({}))
            .unwrap()
            .seq,
        2
    );
}

#[test]
fn mutation_coordinator_serializes_sequence_allocation() {
    let ledger = InMemoryRuntimeLedger::default();
    let coordinator = RunMutationCoordinator::default();
    let run = RunId::from("run");
    for index in 0..4 {
        coordinator
            .append_serialized(
                &ledger,
                &run,
                DurabilityClass::SyncCritical,
                "record",
                json!({"index": index}),
            )
            .unwrap();
    }
    let seqs: Vec<_> = ledger
        .records_for_run(&run)
        .unwrap()
        .into_iter()
        .map(|r| r.seq)
        .collect();
    assert_eq!(seqs, vec![1, 2, 3, 4]);
}

#[test]
fn mutation_coordinator_uses_one_line_for_root_and_workflow_descendants() {
    let coordinator = RunMutationCoordinator::default();
    let root = coordinator.line_for(&RunId::from("root-run"));
    let workflow = coordinator.line_for(&RunId::from("root-run:workflow:review"));
    let subworkflow = coordinator.line_for(&RunId::from("root-run:workflow:review:subworkflow:verify:attempt-1"));

    assert!(Arc::ptr_eq(&root, &workflow));
    assert!(Arc::ptr_eq(&root, &subworkflow));
}
#[test]
fn records_after_pages_from_exclusive_sequence() {
    let ledger = InMemoryRuntimeLedger::default();
    let run = RunId::from("journal");
    for index in 0..5 {
        ledger
            .append(&run, DurabilityClass::SyncCritical, "record", json!({"index": index}))
            .unwrap();
    }
    assert_eq!(ledger.last_sequence(&run).unwrap(), 5);
    let page = ledger.records_after(&run, 2, 2).unwrap();
    assert_eq!(page.iter().map(|record| record.seq).collect::<Vec<_>>(), vec![3, 4]);
    assert_eq!(page[0].payload["index"], 2);
}
#[test]
fn ephemeral_records_do_not_advance_durable_sequence() {
    for jsonl in [false, true] {
        let run = RunId::from(if jsonl { "jsonl-seq" } else { "memory-seq" });
        let temp = std::env::temp_dir().join(format!(
            "solaris-ledger-seq-{}-{}.jsonl",
            std::process::id(),
            if jsonl { "jsonl" } else { "memory" }
        ));
        let _ = std::fs::remove_file(&temp);
        let ledger: Box<dyn RuntimeLedger> = if jsonl {
            Box::new(JsonlRuntimeLedger::open(&temp).unwrap())
        } else {
            Box::new(InMemoryRuntimeLedger::default())
        };
        assert_eq!(
            ledger
                .append(&run, DurabilityClass::SyncCritical, "one", json!({}))
                .unwrap()
                .seq,
            1
        );
        assert_eq!(
            ledger
                .append(&run, DurabilityClass::Ephemeral, "progress", json!({}))
                .unwrap()
                .seq,
            2
        );
        assert_eq!(ledger.last_sequence(&run).unwrap(), 1);
        assert_eq!(
            ledger
                .append(&run, DurabilityClass::SyncCritical, "two", json!({}))
                .unwrap()
                .seq,
            2
        );
        assert_eq!(ledger.run_ids().unwrap(), vec![run]);
        drop(ledger);
        let _ = std::fs::remove_file(temp);
    }
}

#[test]
fn sqlite_instances_allocate_one_global_sequence_without_loss() {
    use std::collections::HashSet;
    use std::sync::Barrier;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("runtime.sqlite3");
    let first = Arc::new(SqliteRuntimeLedger::open(&path).unwrap());
    let second = Arc::new(SqliteRuntimeLedger::open(&path).unwrap());
    let barrier = Arc::new(Barrier::new(3));
    let mut workers = Vec::new();
    for (producer, ledger) in [("first", first), ("second", second)] {
        let barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            let run = RunId::from("concurrent-run");
            barrier.wait();
            for index in 0..100 {
                ledger
                    .append(
                        &run,
                        DurabilityClass::AsyncDurable,
                        "concurrent",
                        json!({"producer": producer, "index": index}),
                    )
                    .unwrap();
            }
        }));
    }
    barrier.wait();
    for worker in workers {
        worker.join().unwrap();
    }

    let reader = SqliteRuntimeLedger::open(&path).unwrap();
    let records = reader.records_for_run(&RunId::from("concurrent-run")).unwrap();
    assert_eq!(records.len(), 200);
    assert_eq!(
        records.iter().map(|record| record.seq).collect::<Vec<_>>(),
        (1..=200).collect::<Vec<_>>()
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
    assert_eq!(identities.len(), 200);
}

#[test]
fn sqlite_jsonl_migration_is_idempotent() {
    let directory = tempfile::tempdir().unwrap();
    let jsonl_path = directory.path().join("ledger.jsonl");
    let sqlite_path = directory.path().join("ledger.sqlite3");
    let run = RunId::from("migrated-run");
    {
        let legacy = JsonlRuntimeLedger::open(&jsonl_path).unwrap();
        legacy
            .append(&run, DurabilityClass::SyncCritical, "first", json!({"n": 1}))
            .unwrap();
        legacy
            .append(&run, DurabilityClass::AsyncDurable, "second", json!({"n": 2}))
            .unwrap();
    }

    let first = SqliteRuntimeLedger::open_with_jsonl_migration(&sqlite_path, &jsonl_path).unwrap();
    assert_eq!(first.records_for_run(&run).unwrap().len(), 2);
    drop(first);

    let reopened = SqliteRuntimeLedger::open_with_jsonl_migration(&sqlite_path, &jsonl_path).unwrap();
    let records = reopened.records_for_run(&run).unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records.iter().map(|record| record.seq).collect::<Vec<_>>(), vec![1, 2]);
    assert_eq!(
        reopened
            .append(&run, DurabilityClass::SyncCritical, "after-migration", json!({}))
            .unwrap()
            .seq,
        3
    );
}

#[test]
fn sqlite_recovers_after_process_exits_with_an_uncommitted_tail() {
    use std::process::{Command, Stdio};

    const CHILD_ENV: &str = "SOLARIS_SQLITE_CRASH_TAIL_CHILD";
    const DATABASE_ENV: &str = "SOLARIS_SQLITE_CRASH_TAIL_DATABASE";
    const MARKER_ENV: &str = "SOLARIS_SQLITE_CRASH_TAIL_MARKER";
    const TEST_NAME: &str =
        "runtime_ledger::runtime_ledger_test::sqlite_recovers_after_process_exits_with_an_uncommitted_tail";

    if std::env::var_os(CHILD_ENV).is_some() {
        let connection = rusqlite::Connection::open(std::env::var_os(DATABASE_ENV).unwrap()).unwrap();
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL;
                 BEGIN IMMEDIATE;
                 UPDATE runtime_ledger_meta SET next_sequence = next_sequence + 1 WHERE singleton = 1;
                 INSERT INTO runtime_ledger_records
                    (sequence, schema_version, run_id, timestamp_unix_ms, durability, record_type, payload)
                 VALUES
                    ((SELECT next_sequence FROM runtime_ledger_meta WHERE singleton = 1),
                     1, 'crash-run', 1, 1, 'uncommitted', X'7B7D');",
            )
            .unwrap();
        std::fs::write(std::env::var_os(MARKER_ENV).unwrap(), b"entered").unwrap();
        std::process::exit(0);
    }

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("runtime.sqlite3");
    let marker = directory.path().join("child-entered");
    let run = RunId::from("crash-run");
    {
        let ledger = SqliteRuntimeLedger::open(&path).unwrap();
        ledger
            .append(&run, DurabilityClass::SyncCritical, "committed", json!({"n": 1}))
            .unwrap();
    }

    let status = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(TEST_NAME)
        .arg("--nocapture")
        .env(CHILD_ENV, "1")
        .env(DATABASE_ENV, &path)
        .env(MARKER_ENV, &marker)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();
    assert!(status.success());
    assert!(marker.is_file(), "crash child test did not run");

    let recovered = SqliteRuntimeLedger::open(&path).unwrap();
    let records = recovered.records_for_run(&run).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].record_type, "committed");
    assert_eq!(
        recovered
            .append(&run, DurabilityClass::SyncCritical, "after-crash", json!({"n": 2}))
            .unwrap()
            .seq,
        2
    );
}

#[test]
fn sqlite_declares_database_wal_and_shm_as_protected_state() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("runtime.sqlite3");
    let ledger = SqliteRuntimeLedger::open(&path).unwrap();
    ledger
        .append(
            &RunId::from("protected-state"),
            DurabilityClass::AsyncDurable,
            "record",
            json!({}),
        )
        .unwrap();

    let path = path.canonicalize().unwrap();
    let mut wal = path.as_os_str().to_os_string();
    wal.push("-wal");
    let mut shm = path.as_os_str().to_os_string();
    shm.push("-shm");
    let expected = vec![path.clone(), PathBuf::from(wal), PathBuf::from(shm)];
    assert_eq!(ledger.protected_state_paths(), expected);
    assert!(ledger.protected_state_paths().iter().all(|path| path.exists()));
}

#[test]
fn sqlite_preserves_durability_class_semantics_across_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("runtime.sqlite3");
    let run = RunId::from("durability-run");
    {
        let ledger = SqliteRuntimeLedger::open(&path).unwrap();
        assert_eq!(
            ledger
                .append(&run, DurabilityClass::SyncCritical, "sync", json!({}))
                .unwrap()
                .seq,
            1
        );
        let sync_mode: i64 = ledger
            .connection
            .lock()
            .unwrap()
            .connection
            .query_row("PRAGMA synchronous", [], |row| row.get(0))
            .unwrap();
        assert_eq!(sync_mode, 2, "SyncCritical commits must use SQLite FULL sync");
        assert_eq!(
            ledger
                .append(&run, DurabilityClass::AsyncDurable, "async", json!({}))
                .unwrap()
                .seq,
            2
        );
        let async_mode: i64 = ledger
            .connection
            .lock()
            .unwrap()
            .connection
            .query_row("PRAGMA synchronous", [], |row| row.get(0))
            .unwrap();
        assert_eq!(async_mode, 1, "AsyncDurable commits must use SQLite NORMAL sync");
        assert_eq!(
            ledger
                .append(&run, DurabilityClass::Ephemeral, "ephemeral", json!({}))
                .unwrap()
                .seq,
            3
        );
    }

    let reopened = SqliteRuntimeLedger::open(&path).unwrap();
    let records = reopened.records_for_run(&run).unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].durability, DurabilityClass::SyncCritical);
    assert_eq!(records[1].durability, DurabilityClass::AsyncDurable);
    assert_eq!(
        reopened
            .append(&run, DurabilityClass::SyncCritical, "next", json!({}))
            .unwrap()
            .seq,
        3
    );
}

#[test]
fn sqlite_export_remains_readable_by_the_jsonl_backend() {
    let directory = tempfile::tempdir().unwrap();
    let sqlite_path = directory.path().join("runtime.sqlite3");
    let jsonl_path = directory.path().join("diagnostic.jsonl");
    let run = RunId::from("export-run");
    let ledger = SqliteRuntimeLedger::open(&sqlite_path).unwrap();
    ledger
        .append(&run, DurabilityClass::SyncCritical, "first", json!({"value": 1}))
        .unwrap();
    ledger
        .append(&run, DurabilityClass::AsyncDurable, "second", json!({"value": 2}))
        .unwrap();

    assert_eq!(ledger.export_jsonl(&jsonl_path).unwrap(), 2);
    let exported = JsonlRuntimeLedger::open(&jsonl_path).unwrap();
    let records = exported.records_for_run(&run).unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].record_type, "first");
    assert_eq!(records[1].durability, DurabilityClass::AsyncDurable);
}

#[test]
fn sqlite_export_replace_failure_preserves_the_previous_export() {
    let directory = tempfile::tempdir().unwrap();
    let sqlite_path = directory.path().join("runtime.sqlite3");
    let export_path = directory.path().join("diagnostic.jsonl");
    let run = RunId::from("failed-export-run");
    let ledger = SqliteRuntimeLedger::open(&sqlite_path).unwrap();
    ledger
        .append(&run, DurabilityClass::SyncCritical, "new", json!({}))
        .unwrap();
    std::fs::write(&export_path, b"previous export\n").unwrap();
    let records = ledger.records_for_run(&run).unwrap();

    let error = export_records_atomically_with(&records, &export_path, &[], |_, _| {
        Err(std::io::Error::other("injected atomic replace failure"))
    })
    .unwrap_err();

    assert!(error.to_string().contains("injected atomic replace failure"));
    assert_eq!(std::fs::read(&export_path).unwrap(), b"previous export\n");
    assert!(std::fs::read_dir(directory.path()).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".solaris-ledger-export-")
    }));
}

#[test]
fn sqlite_export_revalidation_blocks_a_barrier_controlled_target_swap_and_cleans_the_temp_file() {
    use std::sync::{Arc, Barrier};

    let directory = tempfile::tempdir().unwrap();
    let sqlite_path = directory.path().join("runtime.sqlite3");
    let export_path = directory.path().join("diagnostic.jsonl");
    let run = RunId::from("swapped-export-run");
    let ledger = SqliteRuntimeLedger::open(&sqlite_path).unwrap();
    ledger
        .append(&run, DurabilityClass::SyncCritical, "safe-record", json!({}))
        .unwrap();
    std::fs::write(&export_path, b"previous export\n").unwrap();
    let records = ledger.records_for_run(&run).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let thread_barrier = Arc::clone(&barrier);
    let thread_export = export_path.clone();
    let thread_database = sqlite_path.clone();
    let attacker = std::thread::spawn(move || {
        thread_barrier.wait();
        std::fs::remove_file(&thread_export).unwrap();
        std::fs::hard_link(&thread_database, &thread_export).unwrap();
        thread_barrier.wait();
    });

    let error = export_records_atomically_with_revalidation_hook(
        &records,
        &export_path,
        &ledger.protected_state_paths(),
        |_| {
            barrier.wait();
            barrier.wait();
            Ok(())
        },
    )
    .unwrap_err();
    attacker.join().unwrap();

    assert!(
        error.to_string().contains("protected internal state") || error.to_string().contains("changed during export")
    );
    assert_eq!(ledger.records_for_run(&run).unwrap().len(), 1);
    assert!(std::fs::read_dir(directory.path()).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".solaris-ledger-export-")
    }));
}

#[test]
fn sqlite_export_revalidation_blocks_a_temporary_slot_swap_and_cleans_the_alias() {
    let directory = tempfile::tempdir().unwrap();
    let sqlite_path = directory.path().join("runtime.sqlite3");
    let export_path = directory.path().join("diagnostic.jsonl");
    let (ledger, run) = seeded_sqlite_ledger(&sqlite_path);
    let records = ledger.records_for_run(&run).unwrap();

    let error = export_records_atomically_with_revalidation_hook(
        &records,
        &export_path,
        &ledger.protected_state_paths(),
        |parent| {
            let temporary = std::fs::read_dir(parent)?
                .find_map(|entry| {
                    let entry = entry.ok()?;
                    entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with(".solaris-ledger-export-")
                        .then(|| entry.path())
                })
                .ok_or_else(|| std::io::Error::other("export temporary file not found"))?;
            std::fs::remove_file(&temporary)?;
            std::fs::hard_link(&sqlite_path, temporary)?;
            Ok(())
        },
    )
    .expect_err("temporary slot replacement must be rejected");

    assert!(error.to_string().contains("temporary file changed"));
    assert!(!export_path.exists());
    assert_eq!(ledger.records_for_run(&run).unwrap().len(), 1);
    assert!(std::fs::read_dir(directory.path()).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".solaris-ledger-export-")
    }));
}

#[cfg(unix)]
#[test]
fn sqlite_export_keeps_using_the_validated_parent_handle_after_parent_path_replacement() {
    use std::os::unix::fs::symlink;

    let directory = tempfile::tempdir().unwrap();
    let export_parent = directory.path().join("export");
    let moved_parent = directory.path().join("moved-export");
    let runtime_parent = directory.path().join("runtime");
    std::fs::create_dir_all(&export_parent).unwrap();
    std::fs::create_dir_all(&runtime_parent).unwrap();
    let sqlite_path = runtime_parent.join("runtime.sqlite3");
    let export_path = export_parent.join("diagnostic.jsonl");
    let ledger = SqliteRuntimeLedger::open(&sqlite_path).unwrap();
    let run = RunId::from("parent-swap-export-run");
    ledger
        .append(&run, DurabilityClass::SyncCritical, "safe-record", json!({}))
        .unwrap();
    let records = ledger.records_for_run(&run).unwrap();

    let exported = export_records_atomically_with_revalidation_hook(
        &records,
        &export_path,
        &ledger.protected_state_paths(),
        |_| {
            std::fs::rename(&export_parent, &moved_parent)?;
            symlink(&runtime_parent, &export_parent)?;
            Ok(())
        },
    )
    .unwrap();

    assert_eq!(exported, 1);
    assert!(moved_parent.join("diagnostic.jsonl").is_file());
    assert!(!runtime_parent.join("diagnostic.jsonl").exists());
    assert_eq!(ledger.records_for_run(&run).unwrap().len(), 1);
}

#[cfg(windows)]
#[test]
fn sqlite_export_parent_handle_prevents_parent_path_replacement_on_windows() {
    let directory = tempfile::tempdir().unwrap();
    let export_parent = directory.path().join("export");
    let moved_parent = directory.path().join("moved-export");
    std::fs::create_dir_all(&export_parent).unwrap();
    let sqlite_path = directory.path().join("runtime.sqlite3");
    let export_path = export_parent.join("diagnostic.jsonl");
    let ledger = SqliteRuntimeLedger::open(&sqlite_path).unwrap();
    let run = RunId::from("windows-parent-guard-export-run");
    ledger
        .append(&run, DurabilityClass::SyncCritical, "safe-record", json!({}))
        .unwrap();
    let records = ledger.records_for_run(&run).unwrap();

    let exported = export_records_atomically_with_revalidation_hook(
        &records,
        &export_path,
        &ledger.protected_state_paths(),
        |_| {
            assert!(std::fs::rename(&export_parent, &moved_parent).is_err());
            Ok(())
        },
    )
    .unwrap();

    assert_eq!(exported, 1);
    assert!(export_path.is_file());
    assert_eq!(ledger.records_for_run(&run).unwrap().len(), 1);
}

#[test]
fn sqlite_export_syncs_the_parent_after_atomic_replace() {
    let directory = tempfile::tempdir().unwrap();
    let sqlite_path = directory.path().join("runtime.sqlite3");
    let export_path = directory.path().join("diagnostic.jsonl");
    let run = RunId::from("sync-export-run");
    let ledger = SqliteRuntimeLedger::open(&sqlite_path).unwrap();
    ledger
        .append(&run, DurabilityClass::SyncCritical, "synced", json!({}))
        .unwrap();
    let records = ledger.records_for_run(&run).unwrap();
    let steps = std::cell::RefCell::new(Vec::new());

    let exported = export_records_atomically_with_steps(
        &records,
        &export_path,
        &[],
        |source, target| {
            steps.borrow_mut().push("replace");
            std::fs::rename(source, target)
        },
        |parent| {
            steps.borrow_mut().push("sync-parent");
            assert_eq!(parent, directory.path().canonicalize().unwrap());
            Ok(())
        },
    )
    .unwrap();

    assert_eq!(exported, 1);
    assert_eq!(*steps.borrow(), ["replace", "sync-parent"]);
    assert!(export_path.is_file());
}

#[test]
fn sqlite_export_propagates_parent_sync_failure() {
    let directory = tempfile::tempdir().unwrap();
    let sqlite_path = directory.path().join("runtime.sqlite3");
    let export_path = directory.path().join("diagnostic.jsonl");
    let run = RunId::from("failed-sync-export-run");
    let ledger = SqliteRuntimeLedger::open(&sqlite_path).unwrap();
    ledger
        .append(&run, DurabilityClass::SyncCritical, "synced", json!({}))
        .unwrap();
    let records = ledger.records_for_run(&run).unwrap();

    let error = export_records_atomically_with_steps(
        &records,
        &export_path,
        &[],
        |source, target| std::fs::rename(source, target),
        |_| Err(std::io::Error::other("injected parent sync failure")),
    )
    .unwrap_err();

    assert!(error.to_string().contains("injected parent sync failure"));
    assert!(export_path.is_file());
}

#[cfg(unix)]
#[test]
fn sqlite_export_syncs_a_real_unix_parent_directory() {
    let directory = tempfile::tempdir().unwrap();
    let ledger = SqliteRuntimeLedger::open(directory.path().join("runtime.sqlite3")).unwrap();
    ledger
        .append(
            &RunId::from("unix-sync-export-run"),
            DurabilityClass::SyncCritical,
            "synced",
            json!({}),
        )
        .unwrap();

    assert_eq!(
        ledger.export_jsonl(directory.path().join("diagnostic.jsonl")).unwrap(),
        1
    );
}

#[test]
fn sqlite_jsonl_migration_ignores_a_torn_trailing_record() {
    let directory = tempfile::tempdir().unwrap();
    let jsonl_path = directory.path().join("ledger.jsonl");
    let sqlite_path = directory.path().join("ledger.sqlite3");
    let run = RunId::from("torn-jsonl-run");
    {
        let legacy = JsonlRuntimeLedger::open(&jsonl_path).unwrap();
        legacy
            .append(&run, DurabilityClass::SyncCritical, "committed", json!({"value": 1}))
            .unwrap();
    }
    OpenOptions::new()
        .append(true)
        .open(&jsonl_path)
        .unwrap()
        .write_all(br#"{"schema_version":1,"seq":2,"payload":"secret-tail""#)
        .unwrap();

    let ledger = SqliteRuntimeLedger::open_with_jsonl_migration(&sqlite_path, &jsonl_path).unwrap();
    let records = ledger.records_for_run(&run).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].record_type, "committed");
}

#[test]
fn sqlite_errors_do_not_expose_record_type_or_payload() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("runtime.sqlite3");
    let ledger = SqliteRuntimeLedger::open(&path).unwrap();
    ledger
        .connection
        .lock()
        .unwrap()
        .connection
        .execute_batch("DROP TABLE runtime_ledger_records")
        .unwrap();

    let secret_record_type = "secret-record-type-5821";
    let secret_payload = "secret-payload-9347";
    let error = ledger
        .append(
            &RunId::from("error-run"),
            DurabilityClass::SyncCritical,
            secret_record_type,
            json!({"token": secret_payload}),
        )
        .unwrap_err()
        .to_string();
    assert!(!error.contains(secret_record_type));
    assert!(!error.contains(secret_payload));
}

fn seeded_sqlite_ledger(path: &Path) -> (SqliteRuntimeLedger, RunId) {
    let ledger = SqliteRuntimeLedger::open(path).unwrap();
    let run = RunId::from("protected-export-run");
    ledger
        .append(
            &run,
            DurabilityClass::SyncCritical,
            "protected-record",
            json!({"value": 1}),
        )
        .unwrap();
    (ledger, run)
}

fn assert_export_alias_is_rejected(ledger: &SqliteRuntimeLedger, alias: &Path, run: &RunId) {
    let error = ledger
        .export_jsonl(alias)
        .expect_err("protected alias must be rejected");
    assert!(
        error.to_string().contains("protected internal state"),
        "unexpected export error: {error}"
    );
    let records = ledger.records_for_run(run).expect("active ledger must remain readable");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].record_type, "protected-record");
}

#[test]
fn sqlite_export_rejects_a_relative_alias_of_the_database() {
    let current = std::env::current_dir().unwrap();
    let directory = tempfile::Builder::new()
        .prefix("solaris-ledger-relative-")
        .tempdir_in(&current)
        .unwrap();
    let relative_directory = directory.path().strip_prefix(&current).unwrap();
    let database = relative_directory.join("runtime.sqlite3");
    let (ledger, run) = seeded_sqlite_ledger(&database);
    let alias = PathBuf::from(".").join(&database);

    assert_export_alias_is_rejected(&ledger, &alias, &run);
}

#[test]
fn sqlite_export_rejects_a_parent_component_alias_of_the_database() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("runtime.sqlite3");
    let alias_directory = directory.path().join("alias");
    std::fs::create_dir(&alias_directory).unwrap();
    let (ledger, run) = seeded_sqlite_ledger(&database);
    let alias = alias_directory.join("..").join("runtime.sqlite3");

    assert_export_alias_is_rejected(&ledger, &alias, &run);
}

#[cfg(unix)]
fn create_file_symlink(target: &Path, link: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(windows)]
fn create_file_symlink(target: &Path, link: &Path) -> std::io::Result<()> {
    std::os::windows::fs::symlink_file(target, link)
}

#[test]
fn sqlite_export_rejects_a_symlink_to_the_database() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("runtime.sqlite3");
    let alias = directory.path().join("database-link.jsonl");
    let (ledger, run) = seeded_sqlite_ledger(&database);
    if let Err(error) = create_file_symlink(&database, &alias) {
        #[cfg(windows)]
        if error.kind() == std::io::ErrorKind::PermissionDenied || error.raw_os_error() == Some(1314) {
            return;
        }
        panic!("create file symlink: {error}");
    }

    assert_export_alias_is_rejected(&ledger, &alias, &run);
}

#[test]
fn sqlite_export_rejects_a_hardlink_to_the_database() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("runtime.sqlite3");
    let alias = directory.path().join("database-hardlink.jsonl");
    let (ledger, run) = seeded_sqlite_ledger(&database);
    std::fs::hard_link(&database, &alias).unwrap();

    assert_export_alias_is_rejected(&ledger, &alias, &run);
}
