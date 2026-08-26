use std::fs::OpenOptions;
use std::io::{self, BufReader, Cursor, Read, Write, repeat};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::json;
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::RunId;

use super::*;
use crate::runtime_ledger::RuntimeLedger;

struct GeneratedJsonlReader {
    next: usize,
    total: usize,
    payload_bytes: usize,
    current: Cursor<Vec<u8>>,
    largest_request: Arc<AtomicUsize>,
}

impl GeneratedJsonlReader {
    fn new(total: usize, payload_bytes: usize, largest_request: Arc<AtomicUsize>) -> Self {
        Self {
            next: 0,
            total,
            payload_bytes,
            current: Cursor::new(Vec::new()),
            largest_request,
        }
    }

    fn prepare_next_line(&mut self) -> io::Result<bool> {
        if self.next == self.total {
            return Ok(false);
        }
        let record = LedgerRecord {
            schema_version: 1,
            seq: u64::try_from(self.next + 1).unwrap(),
            run_id: RunId::from("generated-stream-run"),
            timestamp_unix_ms: i64::try_from(self.next).unwrap(),
            durability: DurabilityClass::AsyncDurable,
            record_type: "generated".to_owned(),
            payload: json!({
                "index": self.next,
                "body": "x".repeat(self.payload_bytes),
            }),
        };
        let mut line = serde_json::to_vec(&record).map_err(io::Error::other)?;
        line.push(b'\n');
        self.current = Cursor::new(line);
        self.next += 1;
        Ok(true)
    }
}

impl Read for GeneratedJsonlReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.largest_request.fetch_max(output.len(), Ordering::SeqCst);
        loop {
            let read = self.current.read(output)?;
            if read != 0 {
                return Ok(read);
            }
            if !self.prepare_next_line()? {
                return Ok(0);
            }
        }
    }
}

struct FailAfterReader {
    content: Cursor<Vec<u8>>,
    fail_after: u64,
}

impl Read for FailAfterReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let position = self.content.position();
        if position >= self.fail_after {
            return Err(io::Error::other("secret injected reader failure"));
        }
        let remaining = usize::try_from(self.fail_after - position).unwrap();
        let allowed = output.len().min(remaining).min(31);
        self.content.read(&mut output[..allowed])
    }
}

fn source_digest() -> Vec<u8> {
    vec![7; 32]
}

fn encoded_records(count: usize) -> Vec<u8> {
    let mut content = Vec::new();
    for index in 0..count {
        let record = LedgerRecord {
            schema_version: 1,
            seq: u64::try_from(index + 1).unwrap(),
            run_id: RunId::from("retry-stream-run"),
            timestamp_unix_ms: i64::try_from(index).unwrap(),
            durability: DurabilityClass::SyncCritical,
            record_type: "retry".to_owned(),
            payload: json!({"index": index, "secret": "source-secret"}),
        };
        serde_json::to_writer(&mut content, &record).unwrap();
        content.push(b'\n');
    }
    content
}

#[test]
fn import_reader_streams_many_large_records_with_bounded_read_requests() {
    let directory = tempfile::tempdir().unwrap();
    let ledger = SqliteRuntimeLedger::open(directory.path().join("runtime.sqlite3")).unwrap();
    let largest_request = Arc::new(AtomicUsize::new(0));
    let input = GeneratedJsonlReader::new(4_000, 2_048, Arc::clone(&largest_request));
    let mut reader = BufReader::with_capacity(JSONL_READ_BUFFER_BYTES, input);

    assert_eq!(
        import_jsonl_reader(&ledger, &source_digest(), &mut reader, false, || Ok(())).unwrap(),
        4_000
    );
    assert!(largest_request.load(Ordering::SeqCst) <= JSONL_READ_BUFFER_BYTES);
    let state = ledger.connection.lock().unwrap();
    let count: i64 = state
        .connection
        .query_row("SELECT COUNT(*) FROM runtime_ledger_records", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 4_000);
}

#[test]
fn import_reader_failure_rolls_back_and_the_source_can_be_retried() {
    let directory = tempfile::tempdir().unwrap();
    let ledger = SqliteRuntimeLedger::open(directory.path().join("runtime.sqlite3")).unwrap();
    let content = encoded_records(20);
    let first_newline = content.iter().position(|byte| *byte == b'\n').unwrap();
    let failing = FailAfterReader {
        content: Cursor::new(content.clone()),
        fail_after: u64::try_from(first_newline + 19).unwrap(),
    };
    let mut failing = BufReader::with_capacity(64, failing);

    let error = import_jsonl_reader(&ledger, &source_digest(), &mut failing, false, || Ok(()))
        .unwrap_err()
        .to_string();
    assert!(error.contains("read legacy runtime ledger source"));
    assert!(!error.contains("secret injected reader failure"));
    assert!(!error.contains("must-not-appear-in-errors"));
    assert!(!error.contains("source-secret"));
    {
        let state = ledger.connection.lock().unwrap();
        let counts: (i64, i64, i64) = state
            .connection
            .query_row(
                "SELECT
                    (SELECT COUNT(*) FROM runtime_ledger_records),
                    (SELECT COUNT(*) FROM runtime_ledger_import_sources),
                    (SELECT next_sequence FROM runtime_ledger_meta WHERE singleton = 1)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(counts, (0, 0, 0));
    }

    let mut retry = BufReader::with_capacity(64, Cursor::new(content));
    assert_eq!(
        import_jsonl_reader(&ledger, &source_digest(), &mut retry, false, || Ok(())).unwrap(),
        20
    );
    assert_eq!(
        ledger.records_for_run(&RunId::from("retry-stream-run")).unwrap().len(),
        20
    );
}

#[test]
fn complete_final_record_without_newline_is_imported_once() {
    let directory = tempfile::tempdir().unwrap();
    let source_path = directory.path().join("legacy.jsonl");
    let ledger = SqliteRuntimeLedger::open(directory.path().join("runtime.sqlite3")).unwrap();
    let record = LedgerRecord {
        schema_version: 1,
        seq: 1,
        run_id: RunId::from("no-final-newline"),
        timestamp_unix_ms: 1,
        durability: DurabilityClass::SyncCritical,
        record_type: "complete-tail".to_owned(),
        payload: json!({"complete": true}),
    };
    std::fs::write(&source_path, serde_json::to_vec(&record).unwrap()).unwrap();

    assert_eq!(ledger.import_jsonl(&source_path).unwrap(), 1);
    assert_eq!(ledger.import_jsonl(&source_path).unwrap(), 0);
    assert_eq!(ledger.records_for_run(&record.run_id).unwrap().len(), 1);
}

#[test]
fn raw_line_fingerprint_keeps_identical_records_at_distinct_offsets() {
    let directory = tempfile::tempdir().unwrap();
    let source_path = directory.path().join("legacy.jsonl");
    let ledger = SqliteRuntimeLedger::open(directory.path().join("runtime.sqlite3")).unwrap();
    let record = LedgerRecord {
        schema_version: 1,
        seq: 1,
        run_id: RunId::from("duplicate-offset-run"),
        timestamp_unix_ms: 1,
        durability: DurabilityClass::SyncCritical,
        record_type: "same-event".to_owned(),
        payload: json!({"same": true}),
    };
    let mut line = serde_json::to_vec(&record).unwrap();
    line.push(b'\n');
    let mut content = line.clone();
    content.extend_from_slice(&line);
    std::fs::write(&source_path, content).unwrap();

    assert_eq!(ledger.import_jsonl(&source_path).unwrap(), 2);
    assert_eq!(ledger.import_jsonl(&source_path).unwrap(), 0);
    assert_eq!(ledger.records_for_run(&record.run_id).unwrap().len(), 2);
}

#[test]
fn offset_fingerprint_aliases_a_preexisting_raw_line_v1_import() {
    let directory = tempfile::tempdir().unwrap();
    let source_path = directory.path().join("legacy.jsonl");
    let ledger = SqliteRuntimeLedger::open(directory.path().join("runtime.sqlite3")).unwrap();
    let run = RunId::from("raw-v1-compat-run");
    let imported = ledger
        .append(&run, DurabilityClass::SyncCritical, "already-imported", json!({}))
        .unwrap();
    let source_record = LedgerRecord {
        schema_version: 1,
        seq: 41,
        run_id: run.clone(),
        timestamp_unix_ms: 42,
        durability: DurabilityClass::SyncCritical,
        record_type: "legacy-source".to_owned(),
        payload: json!({"legacy": true}),
    };
    let mut raw_line = serde_json::to_vec(&source_record).unwrap();
    let legacy_fingerprint = legacy_raw_line_fingerprint(&raw_line);
    raw_line.push(b'\n');
    std::fs::write(&source_path, raw_line).unwrap();
    {
        let state = ledger.connection.lock().unwrap();
        state
            .connection
            .execute(
                "INSERT INTO runtime_ledger_imports
                    (fingerprint_version, fingerprint, imported_sequence)
                 VALUES (1, ?1, ?2)",
                rusqlite::params![legacy_fingerprint, i64::try_from(imported.seq).unwrap()],
            )
            .unwrap();
    }

    assert_eq!(ledger.import_jsonl(&source_path).unwrap(), 0);
    let state = ledger.connection.lock().unwrap();
    let mut statement = state
        .connection
        .prepare("SELECT fingerprint_version FROM runtime_ledger_imports ORDER BY fingerprint_version")
        .unwrap();
    let versions: Vec<i64> = statement
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(versions, [1, 2]);
}

fn sqlite_import_counts(ledger: &SqliteRuntimeLedger) -> (i64, i64, i64) {
    let state = ledger.connection.lock().unwrap();
    state
        .connection
        .query_row(
            "SELECT
                (SELECT COUNT(*) FROM runtime_ledger_records),
                (SELECT COUNT(*) FROM runtime_ledger_import_sources),
                (SELECT next_sequence FROM runtime_ledger_meta WHERE singleton = 1)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap()
}

#[test]
fn source_slot_swap_before_completed_marker_rolls_back_the_import() {
    let directory = tempfile::tempdir().unwrap();
    let source_path = directory.path().join("legacy.jsonl");
    let moved_path = directory.path().join("legacy-opened.jsonl");
    let database = directory.path().join("runtime.sqlite3");
    let original = encoded_records(4);
    let replacement = encoded_records(1);
    std::fs::write(&source_path, &original).unwrap();
    let ledger = SqliteRuntimeLedger::open(database).unwrap();
    let source = resolve_jsonl_source(&source_path, false).unwrap().unwrap();

    let error = import_jsonl_source_with_revalidation_hook(&ledger, &source, false, || {
        std::fs::rename(&source_path, &moved_path)?;
        std::fs::write(&source_path, &replacement)?;
        Ok(())
    })
    .unwrap_err()
    .to_string();

    assert!(error.contains("legacy runtime ledger source changed during import"));
    assert!(!error.contains(&source_path.to_string_lossy().to_string()));
    assert!(!error.contains("source-secret"));
    assert_eq!(sqlite_import_counts(&ledger), (0, 0, 0));

    let replacement_source = resolve_jsonl_source(&source_path, false).unwrap().unwrap();
    assert_eq!(
        import_jsonl_source_with_revalidation_hook(&ledger, &replacement_source, false, || Ok(())).unwrap(),
        1
    );
}

#[test]
fn source_length_change_before_completed_marker_rolls_back_the_import() {
    let directory = tempfile::tempdir().unwrap();
    let source_path = directory.path().join("legacy.jsonl");
    let database = directory.path().join("runtime.sqlite3");
    std::fs::write(&source_path, encoded_records(3)).unwrap();
    let ledger = SqliteRuntimeLedger::open(database).unwrap();
    let source = resolve_jsonl_source(&source_path, false).unwrap().unwrap();

    let error = import_jsonl_source_with_revalidation_hook(&ledger, &source, false, || {
        OpenOptions::new().append(true).open(&source_path)?.write_all(b" \n")
    })
    .unwrap_err()
    .to_string();

    assert!(error.contains("legacy runtime ledger source changed during import"));
    assert_eq!(sqlite_import_counts(&ledger), (0, 0, 0));
}

#[test]
fn oversized_complete_line_is_rejected_without_allocating_the_whole_line() {
    let directory = tempfile::tempdir().unwrap();
    let ledger = SqliteRuntimeLedger::open(directory.path().join("runtime.sqlite3")).unwrap();
    let bytes = u64::try_from(JSONL_RECORD_MAX_BYTES).unwrap() + 1;
    let input = repeat(b'x').take(bytes).chain(Cursor::new([b'\n']));
    let mut reader = BufReader::with_capacity(JSONL_READ_BUFFER_BYTES, input);

    let error = import_jsonl_reader(&ledger, &source_digest(), &mut reader, false, || Ok(())).unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        error.to_string(),
        "legacy runtime ledger complete record exceeds the size limit"
    );
    assert_eq!(sqlite_import_counts(&ledger), (0, 0, 0));
}

#[test]
fn oversized_torn_tail_is_discarded_incrementally_and_marked_complete() {
    let directory = tempfile::tempdir().unwrap();
    let ledger = SqliteRuntimeLedger::open(directory.path().join("runtime.sqlite3")).unwrap();
    let bytes = u64::try_from(JSONL_RECORD_MAX_BYTES).unwrap() * 2 + 17;
    let input = repeat(b'x').take(bytes);
    let mut reader = BufReader::with_capacity(JSONL_READ_BUFFER_BYTES, input);

    assert_eq!(
        import_jsonl_reader(&ledger, &source_digest(), &mut reader, false, || Ok(())).unwrap(),
        0
    );
    let state = ledger.connection.lock().unwrap();
    let marker: (i64, i64) = state
        .connection
        .query_row(
            "SELECT content_length, completed FROM runtime_ledger_import_sources",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(marker, (i64::try_from(bytes).unwrap(), 1));
}
