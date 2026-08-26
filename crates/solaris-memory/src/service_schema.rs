use rusqlite::{Connection, TransactionBehavior};

use super::{MemoryServiceError, database_error};

const SCHEMA_VERSION: i64 = 2;

pub(super) fn initialize_schema(connection: &mut Connection) -> Result<(), MemoryServiceError> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|source| database_error("begin memory schema migration", source))?;
    let found = transaction
        .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
        .map_err(|source| database_error("read memory schema version", source))?;
    if found == 0 {
        transaction
            .execute_batch(
                "CREATE TABLE memory_records (
                    memory_id TEXT PRIMARY KEY,
                    scope TEXT NOT NULL CHECK (scope IN ('USER', 'MEMORY')),
                    memory_type TEXT NOT NULL CHECK (memory_type IN ('user', 'feedback', 'project', 'reference')),
                    name TEXT NOT NULL,
                    description TEXT NOT NULL,
                    content TEXT NOT NULL,
                    version INTEGER NOT NULL CHECK (version >= 1),
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL,
                    deleted_at_ms INTEGER
                 );
                 CREATE INDEX memory_records_updated ON memory_records (updated_at_ms, memory_id);
                 CREATE TABLE memory_versions (
                    record_id TEXT NOT NULL REFERENCES memory_records(memory_id) ON DELETE RESTRICT,
                    version INTEGER NOT NULL CHECK (version >= 1),
                    operation TEXT NOT NULL CHECK (operation IN ('create', 'edit', 'delete')),
                    scope TEXT NOT NULL CHECK (scope IN ('USER', 'MEMORY')),
                    memory_type TEXT NOT NULL CHECK (memory_type IN ('user', 'feedback', 'project', 'reference')),
                    name TEXT NOT NULL,
                    description TEXT NOT NULL,
                    content TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL,
                    PRIMARY KEY (record_id, version)
                 );
                 CREATE TABLE memory_proposals (
                    proposal_id TEXT PRIMARY KEY,
                    mutation_json BLOB NOT NULL,
                    state TEXT NOT NULL CHECK (state IN ('pending', 'approved', 'rejected')),
                    created_at_ms INTEGER NOT NULL,
                    reviewed_at_ms INTEGER
                 );
                 CREATE INDEX memory_proposals_pending
                    ON memory_proposals (created_at_ms, proposal_id) WHERE state = 'pending';
                 CREATE TABLE memory_import_sources (
                    path_digest BLOB PRIMARY KEY,
                    content_digest BLOB NOT NULL,
                    record_id TEXT NOT NULL REFERENCES memory_records(memory_id) ON DELETE RESTRICT,
                    completed INTEGER NOT NULL CHECK (completed = 1)
                 );
                 CREATE VIRTUAL TABLE memory_fts USING fts5(
                    memory_id UNINDEXED,
                    name,
                    description,
                    content,
                    tokenize = 'unicode61 remove_diacritics 2'
                 );",
            )
            .map_err(|source| database_error("create memory schema", source))?;
        transaction
            .pragma_update(None, "user_version", SCHEMA_VERSION)
            .map_err(|source| database_error("set memory schema version", source))?;
    } else if found == 1 {
        transaction
            .execute_batch(
                "CREATE TABLE memory_import_sources (
                    path_digest BLOB PRIMARY KEY,
                    content_digest BLOB NOT NULL,
                    record_id TEXT NOT NULL REFERENCES memory_records(memory_id) ON DELETE RESTRICT,
                    completed INTEGER NOT NULL CHECK (completed = 1)
                 );",
            )
            .map_err(|source| database_error("upgrade memory schema", source))?;
        transaction
            .pragma_update(None, "user_version", SCHEMA_VERSION)
            .map_err(|source| database_error("set memory schema version", source))?;
    } else if found != SCHEMA_VERSION {
        return Err(MemoryServiceError::CorruptRecord);
    }
    transaction
        .commit()
        .map_err(|source| database_error("commit memory schema migration", source))
}
