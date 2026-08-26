use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::types::MemoryType;

#[path = "service_import.rs"]
mod service_import;
#[path = "service_schema.rs"]
mod service_schema;

use service_schema::initialize_schema;

const BUSY_TIMEOUT_SECONDS: u64 = 30;
const MAX_NAME_BYTES: usize = 256;
const MAX_DESCRIPTION_BYTES: usize = 4 * 1024;
const MAX_CONTENT_BYTES: usize = 1024 * 1024;
const MAX_QUERY_BYTES: usize = 1024;
const SEARCH_RESULT_LIMIT: usize = 8;
const SEARCH_CONTENT_BUDGET: usize = 32 * 1024;
const SNAPSHOT_RECORD_LIMIT: usize = 10_000;
const SNAPSHOT_CONTENT_BUDGET: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemoryScope {
    #[serde(rename = "USER")]
    User,
    #[serde(rename = "MEMORY")]
    Memory,
}

impl MemoryScope {
    fn as_str(self) -> &'static str {
        match self {
            Self::User => "USER",
            Self::Memory => "MEMORY",
        }
    }

    fn parse(value: &str) -> Result<Self, MemoryServiceError> {
        match value {
            "USER" => Ok(Self::User),
            "MEMORY" => Ok(Self::Memory),
            _ => Err(MemoryServiceError::CorruptRecord),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum MemoryMutation {
    Create {
        scope: MemoryScope,
        memory_type: MemoryType,
        name: String,
        description: String,
        content: String,
    },
    Edit {
        id: String,
        expected_version: u64,
        name: String,
        description: String,
        content: String,
    },
    Delete {
        id: String,
        expected_version: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryRecord {
    pub id: String,
    pub scope: MemoryScope,
    pub memory_type: MemoryType,
    pub name: String,
    pub description: String,
    pub content: String,
    pub version: u64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    #[serde(default)]
    pub content_truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryVersionOperation {
    Create,
    Edit,
    Delete,
}

impl MemoryVersionOperation {
    fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Edit => "edit",
            Self::Delete => "delete",
        }
    }

    fn parse(value: &str) -> Result<Self, MemoryServiceError> {
        match value {
            "create" => Ok(Self::Create),
            "edit" => Ok(Self::Edit),
            "delete" => Ok(Self::Delete),
            _ => Err(MemoryServiceError::CorruptRecord),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryVersion {
    pub record: MemoryRecord,
    pub operation: MemoryVersionOperation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryProposalDecision {
    Approve,
    Reject,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryProposal {
    pub id: String,
    pub mutation: MemoryMutation,
    pub created_at_ms: i64,
}

#[derive(Clone)]
pub struct MemorySnapshot {
    captured_at_ms: i64,
    records: Arc<[MemoryRecord]>,
    search_connection: Arc<Mutex<Connection>>,
}

impl std::fmt::Debug for MemorySnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MemorySnapshot")
            .field("captured_at_ms", &self.captured_at_ms)
            .field("record_count", &self.records.len())
            .finish()
    }
}

impl MemorySnapshot {
    pub fn captured_at_ms(&self) -> i64 {
        self.captured_at_ms
    }

    pub fn records(&self) -> &[MemoryRecord] {
        &self.records
    }

    pub fn search(&self, query: &str) -> Result<Vec<MemoryRecord>, MemoryServiceError> {
        let expression = fts_expression(query)?;
        if expression.is_empty() {
            return Ok(Vec::new());
        }
        let connection = self
            .search_connection
            .lock()
            .map_err(|_| MemoryServiceError::LockUnavailable)?;
        let mut statement = connection
            .prepare(
                "SELECT memory_id FROM snapshot_fts
                 WHERE snapshot_fts MATCH ?1
                 ORDER BY bm25(snapshot_fts), memory_id
                 LIMIT 64",
            )
            .map_err(|source| database_error("prepare frozen memory search", source))?;
        let ids = statement
            .query_map(params![expression], |row| row.get::<_, String>(0))
            .map_err(|source| database_error("search frozen memory snapshot", source))?
            .map(|row| row.map_err(|source| database_error("decode frozen memory search", source)))
            .collect::<Result<Vec<_>, _>>()?;
        let mut records = Vec::with_capacity(ids.len());
        for id in ids {
            let record = self
                .records
                .iter()
                .find(|record| record.id == id)
                .ok_or(MemoryServiceError::CorruptRecord)?;
            records.push(record.clone());
        }
        Ok(bound_search_results(records))
    }

    /// Restore a previously frozen session snapshot without consulting the
    /// current memory database. Callers must authenticate the persisted bytes
    /// before decoding them and passing the records here.
    pub fn restore(captured_at_ms: i64, records: Vec<MemoryRecord>) -> Result<Self, MemoryServiceError> {
        validate_snapshot_records(&records)?;
        let search_connection = build_snapshot_search_connection(&records)?;
        Ok(Self {
            captured_at_ms,
            records: records.into(),
            search_connection: Arc::new(Mutex::new(search_connection)),
        })
    }
}

#[derive(Debug, Error)]
pub enum MemoryServiceError {
    #[error("memory service input is invalid: {field}")]
    InvalidInput { field: &'static str },
    #[error("memory record was not found")]
    NotFound,
    #[error("memory record version changed from {expected} to {actual}")]
    VersionConflict { expected: u64, actual: u64 },
    #[error("memory proposal is no longer pending")]
    ProposalNotPending,
    #[error("memory service data is corrupt")]
    CorruptRecord,
    #[error("memory snapshot exceeds its safety limit")]
    SnapshotTooLarge,
    #[error("memory service lock is unavailable")]
    LockUnavailable,
    #[error("memory service database error during {operation}")]
    Database {
        operation: &'static str,
        #[source]
        source: rusqlite::Error,
    },
    #[error("memory service filesystem error during {operation}")]
    Io {
        operation: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("memory service payload encoding failed")]
    Json(#[source] serde_json::Error),
}

#[derive(Clone)]
pub struct MemoryService {
    database_path: Arc<PathBuf>,
    connection: Arc<Mutex<Connection>>,
}

impl std::fmt::Debug for MemoryService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("MemoryService").finish_non_exhaustive()
    }
}

impl MemoryService {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, MemoryServiceError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent).map_err(|source| MemoryServiceError::Io {
                operation: "create memory service directory",
                source,
            })?;
        }
        let mut connection = Connection::open(path).map_err(|source| database_error("open memory service", source))?;
        connection
            .busy_timeout(Duration::from_secs(BUSY_TIMEOUT_SECONDS))
            .map_err(|source| database_error("configure memory service busy timeout", source))?;
        connection
            .pragma_update(None, "foreign_keys", true)
            .map_err(|source| database_error("enable memory service foreign keys", source))?;
        let journal_mode: String = connection
            .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
            .map_err(|source| database_error("enable memory service WAL", source))?;
        if !journal_mode.eq_ignore_ascii_case("wal") {
            return Err(MemoryServiceError::CorruptRecord);
        }
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(|source| database_error("configure memory service durability", source))?;
        initialize_schema(&mut connection)?;
        Ok(Self {
            database_path: Arc::new(path.to_path_buf()),
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    pub fn database_path(&self) -> &Path {
        self.database_path.as_path()
    }

    pub fn apply(&self, mutation: MemoryMutation) -> Result<MemoryRecord, MemoryServiceError> {
        validate_mutation(&mutation)?;
        let mut connection = self.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| database_error("begin memory mutation", source))?;
        let record = apply_mutation(&transaction, &mutation)?;
        transaction
            .commit()
            .map_err(|source| database_error("commit memory mutation", source))?;
        Ok(record)
    }

    pub fn get(&self, id: &str) -> Result<Option<MemoryRecord>, MemoryServiceError> {
        validate_id(id)?;
        let connection = self.connection()?;
        query_record(&connection, id, false)
    }

    pub fn list(&self) -> Result<Vec<MemoryRecord>, MemoryServiceError> {
        let connection = self.connection()?;
        query_active_records(&connection, SNAPSHOT_RECORD_LIMIT + 1)
    }

    pub fn versions(&self, id: &str) -> Result<Vec<MemoryVersion>, MemoryServiceError> {
        validate_id(id)?;
        let connection = self.connection()?;
        let mut statement = connection
            .prepare(
                "SELECT record_id, scope, memory_type, name, description, content, version,
                        created_at_ms, updated_at_ms, operation
                 FROM memory_versions WHERE record_id = ?1 ORDER BY version",
            )
            .map_err(|source| database_error("prepare memory version query", source))?;
        let rows = statement
            .query_map(params![id], decode_version_row)
            .map_err(|source| database_error("query memory versions", source))?;
        rows.map(|row| row.map_err(|source| database_error("decode memory version", source)))
            .collect()
    }

    pub fn submit_proposal(&self, mutation: MemoryMutation) -> Result<MemoryProposal, MemoryServiceError> {
        validate_mutation(&mutation)?;
        let proposal = MemoryProposal {
            id: Uuid::now_v7().to_string(),
            mutation,
            created_at_ms: Utc::now().timestamp_millis(),
        };
        let encoded = serde_json::to_vec(&proposal.mutation).map_err(MemoryServiceError::Json)?;
        let connection = self.connection()?;
        connection
            .execute(
                "INSERT INTO memory_proposals (proposal_id, mutation_json, state, created_at_ms)
                 VALUES (?1, ?2, 'pending', ?3)",
                params![proposal.id, encoded, proposal.created_at_ms],
            )
            .map_err(|source| database_error("insert memory proposal", source))?;
        Ok(proposal)
    }

    pub fn pending_proposals(&self) -> Result<Vec<MemoryProposal>, MemoryServiceError> {
        let connection = self.connection()?;
        let mut statement = connection
            .prepare(
                "SELECT proposal_id, mutation_json, created_at_ms
                 FROM memory_proposals WHERE state = 'pending'
                 ORDER BY created_at_ms, proposal_id LIMIT 256",
            )
            .map_err(|source| database_error("prepare pending memory proposal query", source))?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .map_err(|source| database_error("query pending memory proposals", source))?;
        rows.map(|row| {
            let (id, encoded, created_at_ms) =
                row.map_err(|source| database_error("decode pending memory proposal", source))?;
            let mutation = serde_json::from_slice(&encoded).map_err(MemoryServiceError::Json)?;
            validate_mutation(&mutation)?;
            Ok(MemoryProposal {
                id,
                mutation,
                created_at_ms,
            })
        })
        .collect()
    }

    pub fn review_proposal(
        &self,
        proposal_id: &str,
        decision: MemoryProposalDecision,
    ) -> Result<Option<MemoryRecord>, MemoryServiceError> {
        validate_id(proposal_id)?;
        let mut connection = self.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| database_error("begin memory proposal review", source))?;
        let proposal = transaction
            .query_row(
                "SELECT mutation_json, state FROM memory_proposals WHERE proposal_id = ?1",
                params![proposal_id],
                |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(|source| database_error("read memory proposal", source))?
            .ok_or(MemoryServiceError::NotFound)?;
        if proposal.1 != "pending" {
            return Err(MemoryServiceError::ProposalNotPending);
        }
        let reviewed_at_ms = Utc::now().timestamp_millis();
        let record = match decision {
            MemoryProposalDecision::Approve => {
                let mutation =
                    serde_json::from_slice::<MemoryMutation>(&proposal.0).map_err(MemoryServiceError::Json)?;
                validate_mutation(&mutation)?;
                let record = apply_mutation(&transaction, &mutation)?;
                transaction
                    .execute(
                        "UPDATE memory_proposals SET state = 'approved', reviewed_at_ms = ?2
                         WHERE proposal_id = ?1 AND state = 'pending'",
                        params![proposal_id, reviewed_at_ms],
                    )
                    .map_err(|source| database_error("approve memory proposal", source))?;
                Some(record)
            }
            MemoryProposalDecision::Reject => {
                transaction
                    .execute(
                        "UPDATE memory_proposals SET state = 'rejected', reviewed_at_ms = ?2
                         WHERE proposal_id = ?1 AND state = 'pending'",
                        params![proposal_id, reviewed_at_ms],
                    )
                    .map_err(|source| database_error("reject memory proposal", source))?;
                None
            }
        };
        transaction
            .commit()
            .map_err(|source| database_error("commit memory proposal review", source))?;
        Ok(record)
    }

    pub fn search(&self, query: &str) -> Result<Vec<MemoryRecord>, MemoryServiceError> {
        let expression = fts_expression(query)?;
        if expression.is_empty() {
            return Ok(Vec::new());
        }
        let connection = self.connection()?;
        let mut statement = connection
            .prepare(
                "SELECT records.memory_id, records.scope, records.memory_type, records.name,
                        records.description, records.content, records.version,
                        records.created_at_ms, records.updated_at_ms
                 FROM memory_fts
                 JOIN memory_records AS records ON records.memory_id = memory_fts.memory_id
                 WHERE memory_fts MATCH ?1 AND records.deleted_at_ms IS NULL
                 ORDER BY bm25(memory_fts), records.updated_at_ms DESC, records.memory_id
                 LIMIT 64",
            )
            .map_err(|source| database_error("prepare memory search", source))?;
        let rows = statement
            .query_map(params![expression], decode_record_row)
            .map_err(|source| database_error("search memory", source))?;
        let records = rows
            .map(|row| row.map_err(|source| database_error("decode memory search result", source)))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(bound_search_results(records))
    }

    pub fn snapshot(&self) -> Result<MemorySnapshot, MemoryServiceError> {
        let records = self.list()?;
        MemorySnapshot::restore(Utc::now().timestamp_millis(), records)
    }

    fn connection(&self) -> Result<MutexGuard<'_, Connection>, MemoryServiceError> {
        self.connection.lock().map_err(|_| MemoryServiceError::LockUnavailable)
    }
}

fn validate_snapshot_records(records: &[MemoryRecord]) -> Result<(), MemoryServiceError> {
    if records.len() > SNAPSHOT_RECORD_LIMIT
        || records
            .iter()
            .try_fold(0usize, |total, record| total.checked_add(record.content.len()))
            .is_none_or(|bytes| bytes > SNAPSHOT_CONTENT_BUDGET)
    {
        return Err(MemoryServiceError::SnapshotTooLarge);
    }

    let mut ids = HashSet::with_capacity(records.len());
    for record in records {
        if validate_id(&record.id).is_err()
            || validate_text("name", &record.name, MAX_NAME_BYTES, false).is_err()
            || validate_text("description", &record.description, MAX_DESCRIPTION_BYTES, true).is_err()
            || validate_text("content", &record.content, MAX_CONTENT_BYTES, false).is_err()
            || record.version == 0
            || record.created_at_ms > record.updated_at_ms
            || record.content_truncated
            || !ids.insert(record.id.as_str())
        {
            return Err(MemoryServiceError::CorruptRecord);
        }
    }
    Ok(())
}

fn build_snapshot_search_connection(records: &[MemoryRecord]) -> Result<Connection, MemoryServiceError> {
    let mut connection =
        Connection::open_in_memory().map_err(|source| database_error("open frozen memory search index", source))?;
    connection
        .execute_batch(
            "CREATE VIRTUAL TABLE snapshot_fts USING fts5(
                 memory_id UNINDEXED,
                 name,
                 description,
                 content,
                 tokenize = 'unicode61'
             );",
        )
        .map_err(|source| database_error("create frozen memory search index", source))?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|source| database_error("begin frozen memory index build", source))?;
    {
        let mut insert = transaction
            .prepare(
                "INSERT INTO snapshot_fts (memory_id, name, description, content)
                 VALUES (?1, ?2, ?3, ?4)",
            )
            .map_err(|source| database_error("prepare frozen memory index insert", source))?;
        for record in records {
            insert
                .execute(params![record.id, record.name, record.description, record.content])
                .map_err(|source| database_error("insert frozen memory index record", source))?;
        }
    }
    transaction
        .commit()
        .map_err(|source| database_error("commit frozen memory index build", source))?;
    Ok(connection)
}

fn apply_mutation(
    transaction: &Transaction<'_>,
    mutation: &MemoryMutation,
) -> Result<MemoryRecord, MemoryServiceError> {
    let now = Utc::now().timestamp_millis();
    match mutation {
        MemoryMutation::Create {
            scope,
            memory_type,
            name,
            description,
            content,
        } => {
            let record = MemoryRecord {
                id: Uuid::now_v7().to_string(),
                scope: *scope,
                memory_type: *memory_type,
                name: name.trim().to_owned(),
                description: description.trim().to_owned(),
                content: content.to_owned(),
                version: 1,
                created_at_ms: now,
                updated_at_ms: now,
                content_truncated: false,
            };
            insert_record(transaction, &record)?;
            insert_version(transaction, &record, MemoryVersionOperation::Create)?;
            index_record(transaction, &record)?;
            Ok(record)
        }
        MemoryMutation::Edit {
            id,
            expected_version,
            name,
            description,
            content,
        } => {
            let current = query_record(transaction, id, false)?.ok_or(MemoryServiceError::NotFound)?;
            ensure_version(&current, *expected_version)?;
            let mut record = current;
            record.version = record.version.checked_add(1).ok_or(MemoryServiceError::CorruptRecord)?;
            record.name = name.trim().to_owned();
            record.description = description.trim().to_owned();
            record.content = content.to_owned();
            record.updated_at_ms = now;
            update_record(transaction, &record, *expected_version)?;
            insert_version(transaction, &record, MemoryVersionOperation::Edit)?;
            index_record(transaction, &record)?;
            Ok(record)
        }
        MemoryMutation::Delete { id, expected_version } => {
            let current = query_record(transaction, id, false)?.ok_or(MemoryServiceError::NotFound)?;
            ensure_version(&current, *expected_version)?;
            let mut record = current;
            record.version = record.version.checked_add(1).ok_or(MemoryServiceError::CorruptRecord)?;
            record.updated_at_ms = now;
            transaction
                .execute(
                    "UPDATE memory_records
                     SET version = ?2, updated_at_ms = ?3, deleted_at_ms = ?3
                     WHERE memory_id = ?1 AND version = ?4 AND deleted_at_ms IS NULL",
                    params![
                        record.id,
                        sql_version(record.version)?,
                        now,
                        sql_version(*expected_version)?
                    ],
                )
                .map_err(|source| database_error("delete memory record", source))?;
            insert_version(transaction, &record, MemoryVersionOperation::Delete)?;
            delete_index_record(transaction, &record.id)?;
            Ok(record)
        }
    }
}

fn insert_record(transaction: &Transaction<'_>, record: &MemoryRecord) -> Result<(), MemoryServiceError> {
    transaction
        .execute(
            "INSERT INTO memory_records
                (memory_id, scope, memory_type, name, description, content, version, created_at_ms, updated_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                record.id,
                record.scope.as_str(),
                record.memory_type.as_str(),
                record.name,
                record.description,
                record.content,
                sql_version(record.version)?,
                record.created_at_ms,
                record.updated_at_ms,
            ],
        )
        .map_err(|source| database_error("insert memory record", source))?;
    Ok(())
}

fn update_record(
    transaction: &Transaction<'_>,
    record: &MemoryRecord,
    expected_version: u64,
) -> Result<(), MemoryServiceError> {
    let changed = transaction
        .execute(
            "UPDATE memory_records
             SET name = ?2, description = ?3, content = ?4, version = ?5, updated_at_ms = ?6
             WHERE memory_id = ?1 AND version = ?7 AND deleted_at_ms IS NULL",
            params![
                record.id,
                record.name,
                record.description,
                record.content,
                sql_version(record.version)?,
                record.updated_at_ms,
                sql_version(expected_version)?,
            ],
        )
        .map_err(|source| database_error("update memory record", source))?;
    if changed != 1 {
        return Err(MemoryServiceError::VersionConflict {
            expected: expected_version,
            actual: record.version.saturating_sub(1),
        });
    }
    Ok(())
}

fn insert_version(
    transaction: &Transaction<'_>,
    record: &MemoryRecord,
    operation: MemoryVersionOperation,
) -> Result<(), MemoryServiceError> {
    transaction
        .execute(
            "INSERT INTO memory_versions
                (record_id, version, operation, scope, memory_type, name, description, content, created_at_ms, updated_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                record.id,
                sql_version(record.version)?,
                operation.as_str(),
                record.scope.as_str(),
                record.memory_type.as_str(),
                record.name,
                record.description,
                record.content,
                record.created_at_ms,
                record.updated_at_ms,
            ],
        )
        .map_err(|source| database_error("insert memory version", source))?;
    Ok(())
}

fn index_record(transaction: &Transaction<'_>, record: &MemoryRecord) -> Result<(), MemoryServiceError> {
    delete_index_record(transaction, &record.id)?;
    transaction
        .execute(
            "INSERT INTO memory_fts (memory_id, name, description, content) VALUES (?1, ?2, ?3, ?4)",
            params![record.id, record.name, record.description, record.content],
        )
        .map_err(|source| database_error("index memory record", source))?;
    Ok(())
}

fn delete_index_record(transaction: &Transaction<'_>, id: &str) -> Result<(), MemoryServiceError> {
    transaction
        .execute("DELETE FROM memory_fts WHERE memory_id = ?1", params![id])
        .map_err(|source| database_error("remove memory search record", source))?;
    Ok(())
}

fn query_record(
    connection: &Connection,
    id: &str,
    include_deleted: bool,
) -> Result<Option<MemoryRecord>, MemoryServiceError> {
    let sql = if include_deleted {
        "SELECT memory_id, scope, memory_type, name, description, content, version, created_at_ms, updated_at_ms
         FROM memory_records WHERE memory_id = ?1"
    } else {
        "SELECT memory_id, scope, memory_type, name, description, content, version, created_at_ms, updated_at_ms
         FROM memory_records WHERE memory_id = ?1 AND deleted_at_ms IS NULL"
    };
    connection
        .query_row(sql, params![id], decode_record_row)
        .optional()
        .map_err(|source| database_error("read memory record", source))
}

fn query_active_records(connection: &Connection, limit: usize) -> Result<Vec<MemoryRecord>, MemoryServiceError> {
    let limit = i64::try_from(limit).map_err(|_| MemoryServiceError::SnapshotTooLarge)?;
    let mut statement = connection
        .prepare(
            "SELECT memory_id, scope, memory_type, name, description, content, version, created_at_ms, updated_at_ms
             FROM memory_records WHERE deleted_at_ms IS NULL ORDER BY updated_at_ms DESC, memory_id LIMIT ?1",
        )
        .map_err(|source| database_error("prepare memory list", source))?;
    let rows = statement
        .query_map(params![limit], decode_record_row)
        .map_err(|source| database_error("list memory records", source))?;
    rows.map(|row| row.map_err(|source| database_error("decode memory record", source)))
        .collect()
}

fn decode_record_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<MemoryRecord> {
    let scope = row.get::<_, String>(1)?;
    let memory_type = row.get::<_, String>(2)?;
    let version = row.get::<_, i64>(6)?;
    Ok(MemoryRecord {
        id: row.get(0)?,
        scope: MemoryScope::parse(&scope).map_err(to_sql_decode_error)?,
        memory_type: MemoryType::parse(&memory_type)
            .ok_or_else(|| to_sql_decode_error(MemoryServiceError::CorruptRecord))?,
        name: row.get(3)?,
        description: row.get(4)?,
        content: row.get(5)?,
        version: u64::try_from(version).map_err(|_| to_sql_decode_error(MemoryServiceError::CorruptRecord))?,
        created_at_ms: row.get(7)?,
        updated_at_ms: row.get(8)?,
        content_truncated: false,
    })
}

fn decode_version_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<MemoryVersion> {
    let operation = row.get::<_, String>(9)?;
    Ok(MemoryVersion {
        record: decode_record_row(row)?,
        operation: MemoryVersionOperation::parse(&operation).map_err(to_sql_decode_error)?,
    })
}

fn to_sql_decode_error(error: MemoryServiceError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
}

fn validate_mutation(mutation: &MemoryMutation) -> Result<(), MemoryServiceError> {
    match mutation {
        MemoryMutation::Create {
            name,
            description,
            content,
            ..
        }
        | MemoryMutation::Edit {
            name,
            description,
            content,
            ..
        } => {
            validate_text("name", name, MAX_NAME_BYTES, false)?;
            validate_text("description", description, MAX_DESCRIPTION_BYTES, true)?;
            validate_text("content", content, MAX_CONTENT_BYTES, false)
        }
        MemoryMutation::Delete { id, .. } => validate_id(id),
    }
}

fn validate_id(id: &str) -> Result<(), MemoryServiceError> {
    validate_text("id", id, 128, false)
}

fn validate_text(
    field: &'static str,
    value: &str,
    max_bytes: usize,
    allow_empty: bool,
) -> Result<(), MemoryServiceError> {
    if (!allow_empty && value.trim().is_empty()) || value.len() > max_bytes || value.contains('\0') {
        return Err(MemoryServiceError::InvalidInput { field });
    }
    Ok(())
}

fn ensure_version(record: &MemoryRecord, expected: u64) -> Result<(), MemoryServiceError> {
    if record.version != expected {
        return Err(MemoryServiceError::VersionConflict {
            expected,
            actual: record.version,
        });
    }
    Ok(())
}

fn sql_version(version: u64) -> Result<i64, MemoryServiceError> {
    i64::try_from(version).map_err(|_| MemoryServiceError::CorruptRecord)
}

fn fts_expression(query: &str) -> Result<String, MemoryServiceError> {
    validate_text("query", query, MAX_QUERY_BYTES, true)?;
    Ok(normalized_terms(query)
        .into_iter()
        .take(16)
        .map(|term| format!("\"{}\"", term.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(" AND "))
}

fn normalized_terms(query: &str) -> Vec<String> {
    query
        .split_whitespace()
        .map(str::trim)
        .filter(|term| !term.is_empty())
        .map(str::to_lowercase)
        .collect()
}

fn bound_search_results(records: impl IntoIterator<Item = MemoryRecord>) -> Vec<MemoryRecord> {
    let mut remaining = SEARCH_CONTENT_BUDGET;
    let mut bounded = Vec::new();
    for mut record in records.into_iter().take(SEARCH_RESULT_LIMIT) {
        if remaining == 0 {
            break;
        }
        if record.content.len() > remaining {
            let boundary = floor_char_boundary(&record.content, remaining);
            record.content.truncate(boundary);
            record.content_truncated = true;
        }
        remaining = remaining.saturating_sub(record.content.len());
        bounded.push(record);
    }
    bounded
}

fn floor_char_boundary(value: &str, mut index: usize) -> usize {
    index = index.min(value.len());
    while index > 0 && !value.is_char_boundary(index) {
        index -= 1;
    }
    index
}

fn database_error(operation: &'static str, source: rusqlite::Error) -> MemoryServiceError {
    MemoryServiceError::Database { operation, source }
}

#[cfg(test)]
#[path = "service_test.rs"]
mod service_test;
