use std::io::{self, BufRead, BufReader, Write};

use serde::Serialize;
use serde::ser::Error as _;
use serde_json::json;
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::RunId;

use super::*;
use crate::runtime_ledger::{RuntimeLedger, SqliteRuntimeLedger};

struct FailingSerialize;

impl Serialize for FailingSerialize {
    fn serialize<S>(&self, _: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Err(S::Error::custom("secret serialization failure"))
    }
}

fn no_export_temporaries(parent: &Path) -> bool {
    std::fs::read_dir(parent).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".solaris-ledger-export-")
    })
}

#[test]
fn streaming_export_writes_earlier_rows_before_a_late_sqlite_decode_error() {
    let directory = tempfile::tempdir().unwrap();
    let ledger = SqliteRuntimeLedger::open(directory.path().join("runtime.sqlite3")).unwrap();
    let run = RunId::from("late-decode-run");
    ledger
        .append(&run, DurabilityClass::SyncCritical, "first", json!({"value": 1}))
        .unwrap();
    ledger
        .append(&run, DurabilityClass::SyncCritical, "second", json!({"value": 2}))
        .unwrap();
    let mut state = ledger.connection.lock().unwrap();
    state
        .connection
        .execute(
            "UPDATE runtime_ledger_records SET payload = ?1 WHERE sequence = 2",
            [b"not-json".as_slice()],
        )
        .unwrap();
    let mut output = Vec::new();

    let error = write_sqlite_snapshot(&mut state.connection, &mut output)
        .unwrap_err()
        .to_string();

    assert!(
        !output.is_empty(),
        "the first row must be serialized before the second row is decoded"
    );
    assert!(error.contains("decode runtime ledger export record"));
    assert!(!error.contains("first"));
    assert!(!error.contains("not-json"));
}

#[test]
fn sqlite_decode_failure_preserves_the_previous_export_and_retry_succeeds() {
    let directory = tempfile::tempdir().unwrap();
    let target = directory.path().join("diagnostic.jsonl");
    let ledger = SqliteRuntimeLedger::open(directory.path().join("runtime.sqlite3")).unwrap();
    let run = RunId::from("retry-export-run");
    ledger
        .append(&run, DurabilityClass::SyncCritical, "first", json!({"value": 1}))
        .unwrap();
    ledger
        .append(&run, DurabilityClass::SyncCritical, "second", json!({"value": 2}))
        .unwrap();
    std::fs::write(&target, b"previous export\n").unwrap();
    {
        let state = ledger.connection.lock().unwrap();
        state
            .connection
            .execute(
                "UPDATE runtime_ledger_records SET payload = ?1 WHERE sequence = 2",
                [b"secret-invalid-json".as_slice()],
            )
            .unwrap();
    }

    let error = ledger.export_jsonl(&target).unwrap_err().to_string();
    assert!(error.contains("decode runtime ledger export record"));
    assert!(!error.contains("secret-invalid-json"));
    assert!(!error.contains(&target.to_string_lossy().to_string()));
    assert_eq!(std::fs::read(&target).unwrap(), b"previous export\n");
    assert!(no_export_temporaries(directory.path()));

    {
        let state = ledger.connection.lock().unwrap();
        let repaired = serde_json::to_vec(&json!({"value": 2})).unwrap();
        state
            .connection
            .execute(
                "UPDATE runtime_ledger_records SET payload = ?1 WHERE sequence = 2",
                [repaired],
            )
            .unwrap();
    }
    assert_eq!(ledger.export_jsonl(&target).unwrap(), 2);
    assert_eq!(BufReader::new(std::fs::File::open(&target).unwrap()).lines().count(), 2);
}

#[test]
fn serialization_failure_cleans_the_temporary_and_does_not_expose_payload() {
    let directory = tempfile::tempdir().unwrap();
    let target = directory.path().join("diagnostic.jsonl");
    std::fs::write(&target, b"previous export\n").unwrap();

    let error = export_with_write_step_for_test(&target, |writer| {
        writer.write_all(b"partial record\n")?;
        write_jsonl_record(writer, &FailingSerialize)?;
        Ok(1)
    })
    .unwrap_err()
    .to_string();

    assert!(error.contains("encode runtime ledger export record"));
    assert!(!error.contains("secret serialization failure"));
    assert_eq!(std::fs::read(&target).unwrap(), b"previous export\n");
    assert!(no_export_temporaries(directory.path()));
}

#[test]
fn write_failure_cleans_the_temporary_and_a_retry_can_replace_the_target() {
    let directory = tempfile::tempdir().unwrap();
    let target = directory.path().join("diagnostic.jsonl");
    std::fs::write(&target, b"previous export\n").unwrap();

    let error = export_with_write_step_for_test(&target, |writer| {
        writer.write_all(b"partial record\n")?;
        Err(io::Error::other("secret injected write failure"))
    })
    .unwrap_err()
    .to_string();
    assert!(error.contains("write runtime ledger export"));
    assert!(!error.contains("secret injected write failure"));
    assert_eq!(std::fs::read(&target).unwrap(), b"previous export\n");
    assert!(no_export_temporaries(directory.path()));

    assert_eq!(
        export_with_write_step_for_test(&target, |writer| {
            writer.write_all(b"replacement\n")?;
            Ok(1)
        })
        .unwrap(),
        1
    );
    assert_eq!(std::fs::read(&target).unwrap(), b"replacement\n");
}

struct AppendOnFirstWrite<'a> {
    ledger: &'a SqliteRuntimeLedger,
    appended: bool,
    bytes: Vec<u8>,
}

impl Write for AppendOnFirstWrite<'_> {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        if !self.appended {
            self.ledger.append(
                &RunId::from("snapshot-run"),
                DurabilityClass::AsyncDurable,
                "concurrent",
                json!({}),
            )?;
            self.appended = true;
        }
        self.bytes.extend_from_slice(input);
        Ok(input.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn sqlite_export_cursor_uses_one_snapshot_while_another_connection_appends() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("runtime.sqlite3");
    let ledger = SqliteRuntimeLedger::open(&database).unwrap();
    let run = RunId::from("snapshot-run");
    for record_type in ["first", "second"] {
        ledger
            .append(&run, DurabilityClass::AsyncDurable, record_type, json!({}))
            .unwrap();
    }
    let concurrent = SqliteRuntimeLedger::open(&database).unwrap();
    let mut writer = AppendOnFirstWrite {
        ledger: &concurrent,
        appended: false,
        bytes: Vec::new(),
    };
    let mut state = ledger.connection.lock().unwrap();

    assert_eq!(write_sqlite_snapshot(&mut state.connection, &mut writer).unwrap(), 2);
    assert!(writer.appended);
    assert_eq!(writer.bytes.iter().filter(|byte| **byte == b'\n').count(), 2);
    drop(state);
    assert_eq!(ledger.records_for_run(&run).unwrap().len(), 3);
}

#[test]
fn sqlite_export_handles_many_records_without_collecting_them_first() {
    let directory = tempfile::tempdir().unwrap();
    let target = directory.path().join("diagnostic.jsonl");
    let ledger = SqliteRuntimeLedger::open(directory.path().join("runtime.sqlite3")).unwrap();
    {
        let mut state = ledger.connection.lock().unwrap();
        let transaction = state.connection.transaction().unwrap();
        let payload = serde_json::to_vec(&json!({"body": "x".repeat(1_024)})).unwrap();
        {
            let mut insert = transaction
                .prepare(
                    "INSERT INTO runtime_ledger_records
                        (sequence, schema_version, run_id, timestamp_unix_ms, durability, record_type, payload)
                     VALUES (?1, 1, 'many-export-run', ?1, 2, 'many', ?2)",
                )
                .unwrap();
            for sequence in 1_i64..=5_000 {
                insert.execute(rusqlite::params![sequence, &payload]).unwrap();
            }
        }
        transaction
            .execute(
                "UPDATE runtime_ledger_meta SET next_sequence = 5000 WHERE singleton = 1",
                [],
            )
            .unwrap();
        transaction.commit().unwrap();
    }

    assert_eq!(ledger.export_jsonl(&target).unwrap(), 5_000);
    assert_eq!(
        BufReader::new(std::fs::File::open(target).unwrap()).lines().count(),
        5_000
    );
}

fn assert_real_io_failure_is_atomic(failure: TestExportIoFailure, expected: &str) {
    let directory = tempfile::tempdir().unwrap();
    let target = directory.path().join("diagnostic.jsonl");
    std::fs::write(&target, b"previous export\n").unwrap();

    let error = export_with_io_failure_for_test(&target, failure, |writer| {
        writer.write_all(&[b'x'; 256])?;
        Ok(1)
    })
    .unwrap_err()
    .to_string();

    assert!(error.contains(expected), "unexpected export error: {error}");
    assert!(!error.contains("secret injected"));
    assert!(!error.contains(&target.to_string_lossy().to_string()));
    assert_eq!(std::fs::read(&target).unwrap(), b"previous export\n");
    assert!(no_export_temporaries(directory.path()));

    assert_eq!(
        export_with_write_step_for_test(&target, |writer| {
            writer.write_all(b"replacement\n")?;
            Ok(1)
        })
        .unwrap(),
        1
    );
    assert_eq!(std::fs::read(&target).unwrap(), b"replacement\n");
}

#[test]
fn actual_partial_write_failure_preserves_target_and_cleans_temporary() {
    assert_real_io_failure_is_atomic(
        TestExportIoFailure::PartialWrite { after_bytes: 19 },
        "write runtime ledger export",
    );
}

#[test]
fn actual_flush_failure_preserves_target_and_cleans_temporary() {
    assert_real_io_failure_is_atomic(TestExportIoFailure::Flush, "write runtime ledger export");
}

#[test]
fn injected_sync_all_failure_preserves_target_and_cleans_temporary() {
    assert_real_io_failure_is_atomic(
        TestExportIoFailure::SyncAll,
        "sync runtime ledger export temporary file",
    );
}

fn converted_windows_rename_target(input: &str) -> String {
    String::from_utf16(&windows_rename_target_name(Path::new(input))).unwrap()
}

#[test]
fn windows_rename_target_converts_verbatim_unc_to_standard_unc() {
    assert_eq!(
        converted_windows_rename_target(r"\\?\UNC\server\share\diagnostic.jsonl"),
        r"\\server\share\diagnostic.jsonl"
    );
}

#[test]
fn windows_rename_target_strips_verbatim_drive_prefix() {
    assert_eq!(
        converted_windows_rename_target(r"\\?\C:\runtime\diagnostic.jsonl"),
        r"C:\runtime\diagnostic.jsonl"
    );
}

#[test]
fn windows_rename_target_preserves_non_verbatim_paths() {
    for path in [
        r"\\server\share\diagnostic.jsonl",
        r"C:\runtime\diagnostic.jsonl",
        r"runtime\diagnostic.jsonl",
    ] {
        assert_eq!(converted_windows_rename_target(path), path);
    }
}
