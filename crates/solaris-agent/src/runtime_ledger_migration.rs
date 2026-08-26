use std::ffi::OsString;
use std::io::{self, BufRead, BufReader, ErrorKind};
use std::path::Path;
use std::sync::Arc;

use cap_std::ambient_authority;
use cap_std::fs::{Dir, File, Metadata, OpenOptions};
use cap_std::time::SystemTime;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use sha2::{Digest, Sha256};
use solaris_config::file_identity::OpenedFileIdentity;
use solaris_types::effect::DurabilityClass;

use super::{
    LedgerRecord, SqliteRuntimeLedger, allocate_sqlite_sequence, durability_code, set_sqlite_synchronous, sqlite_error,
};

const SQLITE_LEDGER_SCHEMA_VERSION: i64 = 4;
const JSONL_FINGERPRINT_VERSION_LEGACY: i64 = 0;
const JSONL_FINGERPRINT_VERSION_RAW_LINE_LEGACY: i64 = 1;
const JSONL_FINGERPRINT_VERSION_RAW_LINE_OFFSET: i64 = 2;
const JSONL_READ_BUFFER_BYTES: usize = 64 * 1024;
const JSONL_RECORD_MAX_BYTES: usize = 8 * 1024 * 1024;

struct JsonlImportSource {
    parent: Dir,
    file_name: OsString,
    path_digest: Vec<u8>,
}

struct OpenedJsonlSource {
    file: File,
    identity: Arc<OpenedFileIdentity>,
    metadata: JsonlSourceMetadata,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct JsonlSourceMetadata {
    length: u64,
    modified: Option<SystemTime>,
    created: Option<SystemTime>,
    readonly: bool,
}

pub(super) fn initialize_sqlite_schema(connection: &mut Connection) -> io::Result<()> {
    initialize_sqlite_schema_with_before_commit_inner(connection, || Ok(()))
}

fn initialize_sqlite_schema_with_before_commit_inner(
    connection: &mut Connection,
    before_commit: impl FnOnce() -> io::Result<()>,
) -> io::Result<()> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| sqlite_error("begin runtime ledger schema migration", error))?;
    let schema_version: i64 = transaction
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(|error| sqlite_error("read runtime ledger schema version", error))?;
    match schema_version {
        0 => transaction
            .execute_batch(
                "CREATE TABLE runtime_ledger_meta (
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
                    fingerprint_version INTEGER NOT NULL,
                    fingerprint BLOB NOT NULL,
                    imported_sequence INTEGER NOT NULL,
                    PRIMARY KEY (fingerprint_version, fingerprint)
                 );
                 CREATE TABLE runtime_ledger_import_sources (
                    canonical_path_digest BLOB PRIMARY KEY,
                    content_digest BLOB NOT NULL,
                    content_length INTEGER NOT NULL CHECK (content_length >= 0),
                    completed INTEGER NOT NULL CHECK (completed IN (0, 1))
                 );
                 CREATE TABLE runtime_ledger_logical_records (
                    run_id TEXT NOT NULL,
                    record_type TEXT NOT NULL,
                    logical_identity BLOB NOT NULL,
                    sequence INTEGER NOT NULL UNIQUE
                        REFERENCES runtime_ledger_records(sequence) ON DELETE CASCADE,
                    payload_digest BLOB NOT NULL,
                    PRIMARY KEY (run_id, record_type, logical_identity)
                 );
                 CREATE TABLE runtime_ledger_workflow_mutation_leases (
                    run_id TEXT PRIMARY KEY,
                    owner_id TEXT,
                    epoch INTEGER NOT NULL CHECK (epoch >= 0),
                    heartbeat_at_ms INTEGER NOT NULL,
                    expires_at_ms INTEGER NOT NULL,
                    observed_sequence INTEGER NOT NULL CHECK (observed_sequence >= 0),
                    committed_sequence INTEGER NOT NULL CHECK (committed_sequence >= 0)
                 );
                 INSERT INTO runtime_ledger_meta (singleton, schema_version, next_sequence)
                    VALUES (1, 4, 0);
                 PRAGMA user_version = 4;",
            )
            .map_err(|error| sqlite_error("create runtime ledger schema", error))?,
        1 => transaction
            .execute_batch(
                "ALTER TABLE runtime_ledger_imports RENAME TO runtime_ledger_imports_v1;
                 CREATE TABLE runtime_ledger_imports (
                    fingerprint_version INTEGER NOT NULL,
                    fingerprint BLOB NOT NULL,
                    imported_sequence INTEGER NOT NULL,
                    PRIMARY KEY (fingerprint_version, fingerprint)
                 );
                 INSERT INTO runtime_ledger_imports
                    (fingerprint_version, fingerprint, imported_sequence)
                    SELECT 0, fingerprint, imported_sequence
                    FROM runtime_ledger_imports_v1;
                 DROP TABLE runtime_ledger_imports_v1;
                 CREATE TABLE runtime_ledger_import_sources (
                    canonical_path_digest BLOB PRIMARY KEY,
                    content_digest BLOB NOT NULL,
                    content_length INTEGER NOT NULL CHECK (content_length >= 0),
                    completed INTEGER NOT NULL CHECK (completed IN (0, 1))
                 );
                 CREATE TABLE runtime_ledger_logical_records (
                    run_id TEXT NOT NULL,
                    record_type TEXT NOT NULL,
                    logical_identity BLOB NOT NULL,
                    sequence INTEGER NOT NULL UNIQUE
                        REFERENCES runtime_ledger_records(sequence) ON DELETE CASCADE,
                    payload_digest BLOB NOT NULL,
                    PRIMARY KEY (run_id, record_type, logical_identity)
                 );
                 CREATE TABLE runtime_ledger_workflow_mutation_leases (
                    run_id TEXT PRIMARY KEY,
                    owner_id TEXT,
                    epoch INTEGER NOT NULL CHECK (epoch >= 0),
                    heartbeat_at_ms INTEGER NOT NULL,
                    expires_at_ms INTEGER NOT NULL,
                    observed_sequence INTEGER NOT NULL CHECK (observed_sequence >= 0),
                    committed_sequence INTEGER NOT NULL CHECK (committed_sequence >= 0)
                 );
                 UPDATE runtime_ledger_meta SET schema_version = 4 WHERE singleton = 1;
                 PRAGMA user_version = 4;",
            )
            .map_err(|error| sqlite_error("upgrade runtime ledger schema", error))?,
        2 => transaction
            .execute_batch(
                "CREATE TABLE runtime_ledger_logical_records (
                    run_id TEXT NOT NULL,
                    record_type TEXT NOT NULL,
                    logical_identity BLOB NOT NULL,
                    sequence INTEGER NOT NULL UNIQUE
                        REFERENCES runtime_ledger_records(sequence) ON DELETE CASCADE,
                    payload_digest BLOB NOT NULL,
                    PRIMARY KEY (run_id, record_type, logical_identity)
                 );
                 CREATE TABLE runtime_ledger_workflow_mutation_leases (
                    run_id TEXT PRIMARY KEY,
                    owner_id TEXT,
                    epoch INTEGER NOT NULL CHECK (epoch >= 0),
                    heartbeat_at_ms INTEGER NOT NULL,
                    expires_at_ms INTEGER NOT NULL,
                    observed_sequence INTEGER NOT NULL CHECK (observed_sequence >= 0),
                    committed_sequence INTEGER NOT NULL CHECK (committed_sequence >= 0)
                 );
                 UPDATE runtime_ledger_meta SET schema_version = 4 WHERE singleton = 1;
                 PRAGMA user_version = 4;",
            )
            .map_err(|error| sqlite_error("upgrade runtime ledger logical identity schema", error))?,
        3 => transaction
            .execute_batch(
                "CREATE TABLE runtime_ledger_workflow_mutation_leases (
                    run_id TEXT PRIMARY KEY,
                    owner_id TEXT,
                    epoch INTEGER NOT NULL CHECK (epoch >= 0),
                    heartbeat_at_ms INTEGER NOT NULL,
                    expires_at_ms INTEGER NOT NULL,
                    observed_sequence INTEGER NOT NULL CHECK (observed_sequence >= 0),
                    committed_sequence INTEGER NOT NULL CHECK (committed_sequence >= 0)
                 );
                 UPDATE runtime_ledger_meta SET schema_version = 4 WHERE singleton = 1;
                 PRAGMA user_version = 4;",
            )
            .map_err(|error| sqlite_error("upgrade runtime ledger Workflow mutation schema", error))?,
        SQLITE_LEDGER_SCHEMA_VERSION => {}
        _ => {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "runtime ledger schema version is newer than this build supports",
            ));
        }
    }
    transaction
        .execute(
            "UPDATE runtime_ledger_meta
             SET next_sequence = MAX(
                next_sequence,
                COALESCE((SELECT MAX(sequence) FROM runtime_ledger_records), 0)
             )
             WHERE singleton = 1",
            [],
        )
        .map_err(|error| sqlite_error("repair runtime ledger sequence", error))?;
    if let Err(error) = before_commit() {
        transaction
            .rollback()
            .map_err(|rollback_error| sqlite_error("roll back runtime ledger schema migration", rollback_error))?;
        return Err(error);
    }
    transaction
        .commit()
        .map_err(|error| sqlite_error("commit runtime ledger schema migration", error))
}

#[cfg(test)]
pub(super) fn initialize_sqlite_schema_with_before_commit(
    connection: &mut Connection,
    before_commit: impl FnOnce() -> io::Result<()>,
) -> io::Result<()> {
    initialize_sqlite_schema_with_before_commit_inner(connection, before_commit)
}

pub(super) fn import_jsonl_once(ledger: &SqliteRuntimeLedger, path: &Path) -> io::Result<usize> {
    let Some(source) = resolve_jsonl_source(path, true)? else {
        return Ok(0);
    };
    if source_completed(ledger, &source.path_digest)? {
        return Ok(0);
    }
    import_jsonl_source_with_revalidation_hook(ledger, &source, true, || Ok(()))
}

pub(super) fn import_jsonl_explicit(ledger: &SqliteRuntimeLedger, path: &Path) -> io::Result<usize> {
    let source = resolve_jsonl_source(path, false)?
        .ok_or_else(|| io::Error::new(ErrorKind::NotFound, "legacy runtime ledger source was not found"))?;
    import_jsonl_source_with_revalidation_hook(ledger, &source, false, || Ok(()))
}

fn resolve_jsonl_source(path: &Path, allow_missing: bool) -> io::Result<Option<JsonlImportSource>> {
    let canonical_path = match path.canonicalize() {
        Ok(path) => path,
        Err(error) if allow_missing && error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(io::Error::new(error.kind(), "inspect legacy runtime ledger source"));
        }
    };
    let parent_path = canonical_path
        .parent()
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "legacy runtime ledger source has no parent"))?;
    let file_name = canonical_path
        .file_name()
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "legacy runtime ledger source has no file name"))?
        .to_os_string();
    let parent = Dir::open_ambient_dir(parent_path, ambient_authority())
        .map_err(|error| io::Error::new(error.kind(), "open legacy runtime ledger source parent"))?;
    let path_digest = Sha256::digest(canonical_path_bytes(&canonical_path)).to_vec();
    Ok(Some(JsonlImportSource {
        parent,
        file_name,
        path_digest,
    }))
}

fn import_jsonl_source_with_revalidation_hook(
    ledger: &SqliteRuntimeLedger,
    source: &JsonlImportSource,
    skip_completed_source: bool,
    before_revalidation: impl FnOnce() -> io::Result<()>,
) -> io::Result<usize> {
    let opened = open_jsonl_source(source)?;
    let reader_file = opened
        .file
        .try_clone()
        .map_err(|error| io::Error::new(error.kind(), "retain legacy runtime ledger source"))?
        .into_std();
    let mut reader = BufReader::with_capacity(JSONL_READ_BUFFER_BYTES, reader_file);
    import_jsonl_reader(ledger, &source.path_digest, &mut reader, skip_completed_source, || {
        before_revalidation()
            .map_err(|error| io::Error::new(error.kind(), "prepare legacy runtime ledger source revalidation"))?;
        revalidate_jsonl_source(source, &opened)
    })
}

fn open_jsonl_source(source: &JsonlImportSource) -> io::Result<OpenedJsonlSource> {
    let slot_metadata = source
        .parent
        .symlink_metadata(&source.file_name)
        .map_err(|error| io::Error::new(error.kind(), "inspect legacy runtime ledger source"))?;
    require_jsonl_source_file(&slot_metadata)?;
    let options = jsonl_source_open_options();
    let file = source
        .parent
        .open_with(&source.file_name, &options)
        .map_err(|error| io::Error::new(error.kind(), "read legacy runtime ledger source"))?;
    let metadata = jsonl_source_metadata(&file)?;
    let identity_file = file
        .try_clone()
        .map_err(|error| io::Error::new(error.kind(), "retain legacy runtime ledger source identity"))?
        .into_std();
    let identity = OpenedFileIdentity::from_owned_file(identity_file)
        .map(Arc::new)
        .map_err(|error| io::Error::new(error.kind(), "identify legacy runtime ledger source"))?;
    Ok(OpenedJsonlSource {
        file,
        identity,
        metadata,
    })
}

fn revalidate_jsonl_source(source: &JsonlImportSource, opened: &OpenedJsonlSource) -> io::Result<()> {
    let opened_metadata = jsonl_source_metadata(&opened.file)?;
    if opened_metadata != opened.metadata {
        return Err(source_changed_error());
    }
    let options = jsonl_source_open_options();
    let current = source
        .parent
        .open_with(&source.file_name, &options)
        .map_err(|_| source_changed_error())?;
    let current_metadata = jsonl_source_metadata(&current).map_err(|_| source_changed_error())?;
    let current_identity =
        OpenedFileIdentity::from_owned_file(current.into_std()).map_err(|_| source_changed_error())?;
    if current_metadata != opened.metadata || !opened.identity.same_object(&current_identity) {
        return Err(source_changed_error());
    }
    Ok(())
}

fn jsonl_source_metadata(file: &File) -> io::Result<JsonlSourceMetadata> {
    let metadata = file
        .metadata()
        .map_err(|error| io::Error::new(error.kind(), "inspect legacy runtime ledger source"))?;
    require_jsonl_source_file(&metadata)?;
    Ok(JsonlSourceMetadata {
        length: metadata.len(),
        modified: metadata.modified().ok(),
        created: metadata.created().ok(),
        readonly: metadata.permissions().readonly(),
    })
}

fn require_jsonl_source_file(metadata: &Metadata) -> io::Result<()> {
    if metadata.is_file() {
        Ok(())
    } else {
        Err(io::Error::new(
            ErrorKind::InvalidInput,
            "legacy runtime ledger source is not a file",
        ))
    }
}

fn jsonl_source_open_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(windows)]
    {
        use cap_std::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::{FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE};

        options.share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE);
    }
    options
}

fn source_changed_error() -> io::Error {
    io::Error::other("legacy runtime ledger source changed during import")
}

fn source_completed(ledger: &SqliteRuntimeLedger, path_digest: &[u8]) -> io::Result<bool> {
    let state = ledger.connection.lock().unwrap_or_else(|error| error.into_inner());
    let completed: i64 = state
        .connection
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM runtime_ledger_import_sources
                WHERE canonical_path_digest = ?1 AND completed = 1
             )",
            params![path_digest],
            |row| row.get(0),
        )
        .map_err(|error| sqlite_error("check runtime ledger import source", error))?;
    Ok(completed != 0)
}

fn import_jsonl_reader(
    ledger: &SqliteRuntimeLedger,
    source_digest: &[u8],
    reader: &mut dyn BufRead,
    skip_completed_source: bool,
    revalidate_source: impl FnOnce() -> io::Result<()>,
) -> io::Result<usize> {
    let mut state = ledger.connection.lock().unwrap_or_else(|error| error.into_inner());
    set_sqlite_synchronous(&mut state, DurabilityClass::SyncCritical)?;
    let transaction = state
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| sqlite_error("begin runtime ledger import", error))?;
    if skip_completed_source {
        let completed: i64 = transaction
            .query_row(
                "SELECT EXISTS(
                SELECT 1 FROM runtime_ledger_import_sources
                WHERE canonical_path_digest = ?1 AND completed = 1
             )",
                params![source_digest],
                |row| row.get(0),
            )
            .map_err(|error| sqlite_error("recheck runtime ledger import source", error))?;
        if completed != 0 {
            return Ok(0);
        }
    }

    let mut content_digest = Sha256::new();
    let mut content_length = 0_u64;
    let mut line_number = 0_u64;
    let mut line_offset = 0_u64;
    let mut line = Vec::with_capacity(JSONL_READ_BUFFER_BYTES.min(JSONL_RECORD_MAX_BYTES));
    let mut discard_oversized_line = false;
    let mut imported = 0_usize;
    loop {
        let buffer = reader
            .fill_buf()
            .map_err(|error| io::Error::new(error.kind(), "read legacy runtime ledger source"))?;
        if buffer.is_empty() {
            break;
        }
        let newline = buffer.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(buffer.len(), |index| index + 1);
        let record_bytes = newline.map_or(consumed, |index| index);
        content_digest.update(&buffer[..consumed]);
        content_length = content_length
            .checked_add(
                u64::try_from(consumed)
                    .map_err(|_| io::Error::new(ErrorKind::InvalidData, "legacy runtime ledger source is too large"))?,
            )
            .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "legacy runtime ledger source is too large"))?;
        if !discard_oversized_line {
            let next_length = line.len().checked_add(record_bytes);
            if next_length.is_none_or(|length| length > JSONL_RECORD_MAX_BYTES) {
                discard_oversized_line = true;
                line = Vec::with_capacity(JSONL_READ_BUFFER_BYTES.min(JSONL_RECORD_MAX_BYTES));
            } else {
                line.extend_from_slice(&buffer[..record_bytes]);
            }
        }
        reader.consume(consumed);
        if newline.is_none() {
            continue;
        }
        line_number = line_number
            .checked_add(1)
            .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "legacy runtime ledger has too many lines"))?;
        if discard_oversized_line {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "legacy runtime ledger complete record exceeds the size limit",
            ));
        }
        let raw_line = line.as_slice();
        if raw_line.iter().all(|byte| byte.is_ascii_whitespace()) {
            line.clear();
            line_offset = content_length;
            continue;
        }
        if import_complete_jsonl_line(&transaction, source_digest, line_offset, line_number, raw_line)? {
            imported = imported
                .checked_add(1)
                .ok_or_else(|| io::Error::other("runtime ledger import count overflow"))?;
        }
        line.clear();
        line_offset = content_length;
    }
    if !discard_oversized_line && !line.iter().all(|byte| byte.is_ascii_whitespace()) {
        // A final newline is optional in JSONL. Import a complete final JSON
        // record, while continuing to tolerate an invalid tail left by a
        // crashed legacy writer.
        if serde_json::from_slice::<LedgerRecord>(&line).is_ok() {
            line_number = line_number
                .checked_add(1)
                .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "legacy runtime ledger has too many lines"))?;
            if import_complete_jsonl_line(&transaction, source_digest, line_offset, line_number, &line)? {
                imported = imported
                    .checked_add(1)
                    .ok_or_else(|| io::Error::other("runtime ledger import count overflow"))?;
            }
        }
    }
    revalidate_source()?;
    let content_length = i64::try_from(content_length)
        .map_err(|_| io::Error::new(ErrorKind::InvalidData, "legacy runtime ledger source is too large"))?;
    let content_digest = content_digest.finalize().to_vec();
    transaction
        .execute(
            "INSERT INTO runtime_ledger_import_sources
                (canonical_path_digest, content_digest, content_length, completed)
             VALUES (?1, ?2, ?3, 1)
             ON CONFLICT(canonical_path_digest) DO UPDATE SET
                content_digest = excluded.content_digest,
                content_length = excluded.content_length,
                completed = 1",
            params![source_digest, content_digest, content_length,],
        )
        .map_err(|error| sqlite_error("record runtime ledger import source", error))?;
    transaction
        .commit()
        .map_err(|error| sqlite_error("commit runtime ledger import", error))?;
    Ok(imported)
}

fn import_complete_jsonl_line(
    transaction: &rusqlite::Transaction<'_>,
    source_digest: &[u8],
    line_offset: u64,
    line_number: u64,
    raw_line: &[u8],
) -> io::Result<bool> {
    let record: LedgerRecord = serde_json::from_slice(raw_line).map_err(|_| {
        io::Error::new(
            ErrorKind::InvalidData,
            format!("legacy runtime ledger has an invalid complete record at line {line_number}"),
        )
    })?;
    let Some(durability) = durability_code(record.durability) else {
        return Ok(false);
    };
    let raw_fingerprint = raw_line_offset_fingerprint(source_digest, line_offset, raw_line);
    if imported_sequence(transaction, JSONL_FINGERPRINT_VERSION_RAW_LINE_OFFSET, &raw_fingerprint)?.is_some() {
        return Ok(false);
    }

    let legacy_raw_fingerprint = legacy_raw_line_fingerprint(raw_line);
    if let Some(sequence) = imported_sequence(
        transaction,
        JSONL_FINGERPRINT_VERSION_RAW_LINE_LEGACY,
        &legacy_raw_fingerprint,
    )? {
        record_import_fingerprint(
            transaction,
            JSONL_FINGERPRINT_VERSION_RAW_LINE_OFFSET,
            &raw_fingerprint,
            sequence,
        )?;
        return Ok(false);
    }

    let legacy_fingerprint = legacy_record_fingerprint(&record)?;
    if let Some(sequence) = imported_sequence(transaction, JSONL_FINGERPRINT_VERSION_LEGACY, &legacy_fingerprint)? {
        record_import_fingerprint(
            transaction,
            JSONL_FINGERPRINT_VERSION_RAW_LINE_OFFSET,
            &raw_fingerprint,
            sequence,
        )?;
        return Ok(false);
    }

    let sequence = allocate_sqlite_sequence(transaction)?;
    let encoded_payload =
        serde_json::to_vec(&record.payload).map_err(|_| io::Error::other("encode legacy runtime ledger payload"))?;
    transaction
        .execute(
            "INSERT INTO runtime_ledger_records
                (sequence, schema_version, run_id, timestamp_unix_ms, durability, record_type, payload)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                sequence,
                i64::from(record.schema_version),
                record.run_id.as_str(),
                record.timestamp_unix_ms,
                durability,
                record.record_type,
                encoded_payload,
            ],
        )
        .map_err(|error| sqlite_error("insert imported runtime ledger record", error))?;
    record_import_fingerprint(
        transaction,
        JSONL_FINGERPRINT_VERSION_RAW_LINE_OFFSET,
        &raw_fingerprint,
        sequence,
    )?;
    Ok(true)
}

fn imported_sequence(
    transaction: &rusqlite::Transaction<'_>,
    fingerprint_version: i64,
    fingerprint: &[u8],
) -> io::Result<Option<i64>> {
    transaction
        .query_row(
            "SELECT imported_sequence FROM runtime_ledger_imports
             WHERE fingerprint_version = ?1 AND fingerprint = ?2",
            params![fingerprint_version, fingerprint],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| sqlite_error("check runtime ledger import", error))
}

fn record_import_fingerprint(
    transaction: &rusqlite::Transaction<'_>,
    fingerprint_version: i64,
    fingerprint: &[u8],
    imported_sequence: i64,
) -> io::Result<()> {
    transaction
        .execute(
            "INSERT INTO runtime_ledger_imports
                (fingerprint_version, fingerprint, imported_sequence)
             VALUES (?1, ?2, ?3)",
            params![fingerprint_version, fingerprint, imported_sequence,],
        )
        .map_err(|error| sqlite_error("record runtime ledger import", error))?;
    Ok(())
}

fn raw_line_offset_fingerprint(source_digest: &[u8], offset: u64, raw_line: &[u8]) -> Vec<u8> {
    let mut digest = Sha256::new();
    digest.update(b"solaris-runtime-ledger-jsonl-line-offset\0");
    digest.update(JSONL_FINGERPRINT_VERSION_RAW_LINE_OFFSET.to_le_bytes());
    digest.update(u64::try_from(source_digest.len()).unwrap_or(u64::MAX).to_le_bytes());
    digest.update(source_digest);
    digest.update(offset.to_le_bytes());
    digest.update(u64::try_from(raw_line.len()).unwrap_or(u64::MAX).to_le_bytes());
    digest.update(raw_line);
    digest.finalize().to_vec()
}

fn legacy_raw_line_fingerprint(raw_line: &[u8]) -> Vec<u8> {
    let mut digest = Sha256::new();
    digest.update(b"solaris-runtime-ledger-jsonl-line\0");
    digest.update(JSONL_FINGERPRINT_VERSION_RAW_LINE_LEGACY.to_le_bytes());
    digest.update(u64::try_from(raw_line.len()).unwrap_or(u64::MAX).to_le_bytes());
    digest.update(raw_line);
    digest.finalize().to_vec()
}

fn legacy_record_fingerprint(record: &LedgerRecord) -> io::Result<Vec<u8>> {
    let encoded =
        serde_json::to_vec(record).map_err(|_| io::Error::other("encode legacy runtime ledger record fingerprint"))?;
    Ok(Sha256::digest(encoded).to_vec())
}

#[cfg(unix)]
fn canonical_path_bytes(path: &Path) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;

    path.as_os_str().as_bytes().to_vec()
}

#[cfg(windows)]
fn canonical_path_bytes(path: &Path) -> Vec<u8> {
    use std::os::windows::ffi::OsStrExt;

    path.as_os_str().encode_wide().flat_map(u16::to_le_bytes).collect()
}

#[cfg(not(any(unix, windows)))]
fn canonical_path_bytes(path: &Path) -> Vec<u8> {
    path.to_string_lossy().into_owned().into_bytes()
}

#[cfg(test)]
#[path = "runtime_ledger_migration_test.rs"]
mod runtime_ledger_migration_test;
