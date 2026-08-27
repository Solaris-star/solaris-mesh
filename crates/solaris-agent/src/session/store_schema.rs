use rusqlite::{Connection, Transaction, TransactionBehavior, params};

use super::{SESSION_STORE_SCHEMA_VERSION, SessionStoreError, db_error};

pub(super) fn initialize_schema(connection: &mut Connection) -> Result<(), SessionStoreError> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|source| db_error("begin session store schema migration", source))?;
    let found: i64 = transaction
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(|source| db_error("read session store schema version", source))?;
    match found {
        0 => create_schema(&transaction)?,
        1 => {
            migrate_v1_to_v2(&transaction)?;
            migrate_v2_to_v3(&transaction)?;
            migrate_v3_to_v4(&transaction)?;
            migrate_v4_to_v5(&transaction)?;
            migrate_v5_to_v6(&transaction)?;
            migrate_v6_to_v7(&transaction)?;
            migrate_v7_to_v8(&transaction)?;
            migrate_v8_to_v9(&transaction)?;
        }
        2 => {
            migrate_v2_to_v3(&transaction)?;
            migrate_v3_to_v4(&transaction)?;
            migrate_v4_to_v5(&transaction)?;
            migrate_v5_to_v6(&transaction)?;
            migrate_v6_to_v7(&transaction)?;
            migrate_v7_to_v8(&transaction)?;
            migrate_v8_to_v9(&transaction)?;
        }
        3 => {
            migrate_v3_to_v4(&transaction)?;
            migrate_v4_to_v5(&transaction)?;
            migrate_v5_to_v6(&transaction)?;
            migrate_v6_to_v7(&transaction)?;
            migrate_v7_to_v8(&transaction)?;
            migrate_v8_to_v9(&transaction)?;
        }
        4 => {
            migrate_v4_to_v5(&transaction)?;
            migrate_v5_to_v6(&transaction)?;
            migrate_v6_to_v7(&transaction)?;
            migrate_v7_to_v8(&transaction)?;
            migrate_v8_to_v9(&transaction)?;
        }
        5 => {
            migrate_v5_to_v6(&transaction)?;
            migrate_v6_to_v7(&transaction)?;
            migrate_v7_to_v8(&transaction)?;
            migrate_v8_to_v9(&transaction)?;
        }
        6 => {
            migrate_v6_to_v7(&transaction)?;
            migrate_v7_to_v8(&transaction)?;
            migrate_v8_to_v9(&transaction)?;
        }
        7 => {
            migrate_v7_to_v8(&transaction)?;
            migrate_v8_to_v9(&transaction)?;
        }
        8 => migrate_v8_to_v9(&transaction)?,
        SESSION_STORE_SCHEMA_VERSION => verify_meta_version(&transaction)?,
        version if version > SESSION_STORE_SCHEMA_VERSION => {
            return Err(SessionStoreError::UnsupportedSchema {
                found: version,
                supported: SESSION_STORE_SCHEMA_VERSION,
            });
        }
        version => {
            return Err(SessionStoreError::UnsupportedSchema {
                found: version,
                supported: SESSION_STORE_SCHEMA_VERSION,
            });
        }
    }
    transaction
        .commit()
        .map_err(|source| db_error("commit session store schema migration", source))
}

fn create_schema(transaction: &Transaction<'_>) -> Result<(), SessionStoreError> {
    transaction
        .execute_batch(
            "CREATE TABLE session_store_meta (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                schema_version INTEGER NOT NULL
             );
             CREATE TABLE sessions (
                session_id TEXT PRIMARY KEY,
                state_json BLOB NOT NULL,
                revision INTEGER NOT NULL CHECK (revision >= 0),
                run_id TEXT,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL
             );
             CREATE INDEX sessions_updated_at ON sessions (updated_at_ms, session_id);
             CREATE TABLE session_leases (
                session_id TEXT PRIMARY KEY REFERENCES sessions(session_id) ON DELETE CASCADE,
                owner_id TEXT,
                epoch INTEGER NOT NULL CHECK (epoch >= 1),
                heartbeat_at_ms INTEGER,
                expires_at_ms INTEGER,
                CHECK (
                    (owner_id IS NULL AND heartbeat_at_ms IS NULL AND expires_at_ms IS NULL)
                    OR (owner_id IS NOT NULL AND heartbeat_at_ms IS NOT NULL AND expires_at_ms IS NOT NULL)
                )
             );
             CREATE INDEX session_leases_expiry ON session_leases (expires_at_ms) WHERE owner_id IS NOT NULL;
             CREATE TABLE session_tombstones (
                session_id TEXT PRIMARY KEY,
                deleted_at_ms INTEGER NOT NULL,
                reason TEXT NOT NULL
             );
             CREATE TABLE session_migration_sources (
                source_digest BLOB PRIMARY KEY,
                source_path TEXT NOT NULL,
                content_digest BLOB NOT NULL,
                content_length INTEGER NOT NULL CHECK (content_length >= 0),
                completed_at_ms INTEGER NOT NULL
             );
             CREATE TABLE session_migration_imports (
                source_digest BLOB NOT NULL REFERENCES session_migration_sources(source_digest),
                session_id TEXT NOT NULL,
                outcome TEXT NOT NULL CHECK (outcome IN ('inserted', 'duplicate', 'tombstoned')),
                PRIMARY KEY (source_digest, session_id)
             );
             CREATE INDEX session_migration_imports_session ON session_migration_imports (session_id);
             CREATE TABLE session_run_references (
                session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE RESTRICT,
                run_id TEXT NOT NULL,
                reference_kind TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                PRIMARY KEY (session_id, run_id, reference_kind)
             );
             CREATE TABLE session_gc_jobs (
                job_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                exact_run_tree_json BLOB NOT NULL,
                phase TEXT NOT NULL CHECK (phase IN ('planned', 'ledger_deleted', 'blobs_deleted', 'complete')),
                created_at_ms INTEGER NOT NULL,
                eligible_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                last_error TEXT
             );
             CREATE INDEX session_gc_jobs_phase ON session_gc_jobs (phase, updated_at_ms);",
        )
        .map_err(|source| db_error("create session store schema", source))?;
    create_gc_run_claims(transaction)?;
    create_host_outbox(transaction)?;
    create_durable_tasks(transaction)?;
    create_agent_conversations(transaction)?;
    transaction
        .execute(
            "INSERT INTO session_store_meta (singleton, schema_version) VALUES (1, ?1)",
            params![SESSION_STORE_SCHEMA_VERSION],
        )
        .map_err(|source| db_error("record session store schema version", source))?;
    transaction
        .pragma_update(None, "user_version", SESSION_STORE_SCHEMA_VERSION)
        .map_err(|source| db_error("set session store schema version", source))
}

fn migrate_v1_to_v2(transaction: &Transaction<'_>) -> Result<(), SessionStoreError> {
    transaction
        .execute(
            "ALTER TABLE session_gc_jobs ADD COLUMN eligible_at_ms INTEGER NOT NULL DEFAULT 0",
            [],
        )
        .map_err(|source| db_error("add session GC eligibility time", source))?;
    transaction
        .execute(
            "UPDATE session_gc_jobs SET eligible_at_ms = created_at_ms + 2592000000
             WHERE eligible_at_ms = 0",
            [],
        )
        .map_err(|source| db_error("migrate session GC eligibility time", source))?;
    transaction
        .execute(
            "UPDATE session_store_meta SET schema_version = 2 WHERE singleton = 1",
            [],
        )
        .map_err(|source| db_error("record migrated session store schema version", source))?;
    transaction
        .pragma_update(None, "user_version", 2)
        .map_err(|source| db_error("set migrated session store schema version", source))
}

fn migrate_v2_to_v3(transaction: &Transaction<'_>) -> Result<(), SessionStoreError> {
    create_host_outbox(transaction)?;
    transaction
        .execute(
            "UPDATE session_store_meta SET schema_version = ?1 WHERE singleton = 1",
            params![3],
        )
        .map_err(|source| db_error("record host outbox schema version", source))?;
    transaction
        .pragma_update(None, "user_version", 3)
        .map_err(|source| db_error("set host outbox schema version", source))
}

fn migrate_v3_to_v4(transaction: &Transaction<'_>) -> Result<(), SessionStoreError> {
    create_durable_tasks(transaction)?;
    transaction
        .execute(
            "UPDATE session_store_meta SET schema_version = ?1 WHERE singleton = 1",
            params![4],
        )
        .map_err(|source| db_error("record durable task schema version", source))?;
    transaction
        .pragma_update(None, "user_version", 4)
        .map_err(|source| db_error("set durable task schema version", source))
}

fn migrate_v4_to_v5(transaction: &Transaction<'_>) -> Result<(), SessionStoreError> {
    create_gc_run_claims(transaction)?;
    transaction
        .execute(
            "INSERT OR IGNORE INTO session_gc_run_claims (job_id, session_id, root_run_id)
             SELECT session_gc_jobs.job_id, session_gc_jobs.session_id, json_each.value
             FROM session_gc_jobs,
                  json_each(CAST(session_gc_jobs.exact_run_tree_json AS TEXT), '$.run_ids')
             WHERE session_gc_jobs.phase != 'complete'
               AND json_each.type = 'text'
               AND length(json_each.value) > 0",
            [],
        )
        .map_err(|source| db_error("backfill session GC Run claims", source))?;
    transaction
        .execute(
            "UPDATE session_store_meta SET schema_version = 5 WHERE singleton = 1",
            [],
        )
        .map_err(|source| db_error("record session GC claim schema version", source))?;
    transaction
        .pragma_update(None, "user_version", 5)
        .map_err(|source| db_error("set session GC claim schema version", source))
}

fn migrate_v5_to_v6(transaction: &Transaction<'_>) -> Result<(), SessionStoreError> {
    let has_durable_tasks: bool = transaction
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM sqlite_master
                WHERE type = 'table' AND name = 'durable_agent_tasks'
             )",
            [],
            |row| row.get(0),
        )
        .map_err(|source| db_error("inspect durable task schema before migration", source))?;
    let supports_user_checkpoint = if has_durable_tasks {
        transaction
            .query_row(
                "SELECT instr(sql, '''user_checkpointed''') > 0
                 FROM sqlite_master
                 WHERE type = 'table' AND name = 'durable_agent_tasks'",
                [],
                |row| row.get::<_, bool>(0),
            )
            .map_err(|source| db_error("inspect durable task phase constraint", source))?
    } else {
        false
    };
    if has_durable_tasks && !supports_user_checkpoint {
        transaction
            .execute_batch(
                "DROP INDEX IF EXISTS durable_agent_tasks_phase;
                 ALTER TABLE durable_agent_tasks RENAME TO durable_agent_tasks_v5;",
            )
            .map_err(|source| db_error("prepare durable task user checkpoint migration", source))?;
        create_durable_tasks(transaction)?;
        transaction
            .execute(
                "INSERT INTO durable_agent_tasks
                    (session_id, task_key, input_digest, phase, call_id, session_revision,
                     task_revision, terminal_result_json, updated_at_ms)
                 SELECT session_id, task_key, input_digest, phase, call_id, session_revision,
                        task_revision, terminal_result_json, updated_at_ms
                 FROM durable_agent_tasks_v5",
                [],
            )
            .map_err(|source| db_error("copy durable tasks into user checkpoint schema", source))?;
        transaction
            .execute("DROP TABLE durable_agent_tasks_v5", [])
            .map_err(|source| db_error("remove previous durable task schema", source))?;
    } else if !has_durable_tasks {
        create_durable_tasks(transaction)?;
    }
    transaction
        .execute(
            "UPDATE session_store_meta SET schema_version = 6 WHERE singleton = 1",
            [],
        )
        .map_err(|source| db_error("record durable task user checkpoint schema version", source))?;
    transaction
        .pragma_update(None, "user_version", 6)
        .map_err(|source| db_error("set durable task user checkpoint schema version", source))
}

fn migrate_v6_to_v7(transaction: &Transaction<'_>) -> Result<(), SessionStoreError> {
    create_agent_conversations(transaction)?;
    transaction
        .execute(
            "UPDATE session_store_meta SET schema_version = 7 WHERE singleton = 1",
            [],
        )
        .map_err(|source| db_error("record Agent conversation schema version", source))?;
    transaction
        .pragma_update(None, "user_version", 7)
        .map_err(|source| db_error("set Agent conversation schema version", source))
}

fn migrate_v7_to_v8(transaction: &Transaction<'_>) -> Result<(), SessionStoreError> {
    let has_opening_epoch: bool = transaction
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM pragma_table_info('agent_conversations')
                WHERE name = 'opening_epoch'
             )",
            [],
            |row| row.get(0),
        )
        .map_err(|source| db_error("inspect Agent conversation opening lease schema", source))?;
    if !has_opening_epoch {
        transaction
            .execute_batch(
                "ALTER TABLE agent_conversations
                    ADD COLUMN opening_epoch INTEGER NOT NULL DEFAULT 1 CHECK (opening_epoch >= 1);
                 ALTER TABLE agent_conversations
                    ADD COLUMN opening_expires_at_ms INTEGER;",
            )
            .map_err(|source| db_error("add Agent conversation opening lease", source))?;
    }
    transaction
        .execute(
            "UPDATE agent_conversations
             SET opening_expires_at_ms = updated_at_ms + (?1 * 1000)
             WHERE state = 'opening' AND opening_expires_at_ms IS NULL",
            params![super::DEFAULT_LEASE_SECONDS],
        )
        .map_err(|source| db_error("backfill Agent conversation opening lease", source))?;
    transaction
        .execute_batch(
            "CREATE INDEX IF NOT EXISTS agent_conversations_opening_lease
                ON agent_conversations (state, opening_expires_at_ms)
                WHERE state = 'opening';",
        )
        .map_err(|source| db_error("index Agent conversation opening lease", source))?;
    transaction
        .execute(
            "UPDATE session_store_meta SET schema_version = 8 WHERE singleton = 1",
            [],
        )
        .map_err(|source| db_error("record Agent conversation opening lease schema version", source))?;
    transaction
        .pragma_update(None, "user_version", 8)
        .map_err(|source| db_error("set Agent conversation opening lease schema version", source))
}

fn migrate_v8_to_v9(transaction: &Transaction<'_>) -> Result<(), SessionStoreError> {
    let has_durable_tasks: bool = transaction
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM sqlite_master
                WHERE type = 'table' AND name = 'durable_agent_tasks'
             )",
            [],
            |row| row.get(0),
        )
        .map_err(|source| db_error("inspect durable task schema before side-effect migration", source))?;
    let supports_side_effect_unknown = if has_durable_tasks {
        transaction
            .query_row(
                "SELECT instr(sql, '''side_effect_unknown''') > 0
                 FROM sqlite_master
                 WHERE type = 'table' AND name = 'durable_agent_tasks'",
                [],
                |row| row.get::<_, bool>(0),
            )
            .map_err(|source| db_error("inspect durable task side-effect phase constraint", source))?
    } else {
        false
    };
    if has_durable_tasks && !supports_side_effect_unknown {
        transaction
            .execute_batch(
                "DROP INDEX IF EXISTS durable_agent_tasks_phase;
                 ALTER TABLE durable_agent_tasks RENAME TO durable_agent_tasks_v8;",
            )
            .map_err(|source| db_error("prepare durable task side-effect migration", source))?;
        create_durable_tasks(transaction)?;
        transaction
            .execute(
                "INSERT INTO durable_agent_tasks
                    (session_id, task_key, input_digest, phase, call_id, session_revision,
                     task_revision, terminal_result_json, updated_at_ms)
                 SELECT session_id, task_key, input_digest, phase, call_id, session_revision,
                        task_revision, terminal_result_json, updated_at_ms
                 FROM durable_agent_tasks_v8",
                [],
            )
            .map_err(|source| db_error("copy durable tasks into side-effect schema", source))?;
        transaction
            .execute("DROP TABLE durable_agent_tasks_v8", [])
            .map_err(|source| db_error("remove previous durable task side-effect schema", source))?;
    } else if !has_durable_tasks {
        create_durable_tasks(transaction)?;
    }
    transaction
        .execute(
            "UPDATE session_store_meta SET schema_version = 9 WHERE singleton = 1",
            [],
        )
        .map_err(|source| db_error("record durable task side-effect schema version", source))?;
    transaction
        .pragma_update(None, "user_version", 9)
        .map_err(|source| db_error("set durable task side-effect schema version", source))
}

fn create_agent_conversations(transaction: &Transaction<'_>) -> Result<(), SessionStoreError> {
    transaction
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS agent_conversations (
                run_id TEXT NOT NULL,
                parent_agent_id TEXT NOT NULL,
                conversation_id TEXT NOT NULL,
                schema_version INTEGER NOT NULL CHECK (schema_version > 0),
                state TEXT NOT NULL CHECK (state IN ('opening', 'open', 'closing', 'closed')),
                revision INTEGER NOT NULL CHECK (revision >= 0),
                opening_owner TEXT,
                opening_epoch INTEGER NOT NULL CHECK (opening_epoch >= 1),
                opening_expires_at_ms INTEGER,
                agent_id TEXT NOT NULL UNIQUE,
                session_id TEXT NOT NULL UNIQUE REFERENCES sessions(session_id) ON DELETE RESTRICT,
                task_id TEXT NOT NULL,
                open_operation_id TEXT NOT NULL,
                spec_digest TEXT NOT NULL,
                handle_json BLOB,
                next_sequence INTEGER NOT NULL CHECK (next_sequence >= 0),
                next_to_run INTEGER NOT NULL CHECK (next_to_run >= 0 AND next_to_run <= next_sequence),
                active_sequence INTEGER CHECK (active_sequence >= 0),
                terminal_state TEXT,
                failure_class TEXT,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY (run_id, parent_agent_id, conversation_id),
                CHECK (
                    (state = 'opening' AND opening_owner IS NOT NULL
                        AND opening_expires_at_ms IS NOT NULL AND handle_json IS NULL)
                    OR (state != 'opening' AND opening_owner IS NULL
                        AND opening_expires_at_ms IS NULL AND handle_json IS NOT NULL)
                ),
                CHECK ((state = 'closed' AND terminal_state IS NOT NULL) OR state != 'closed')
             );
             CREATE INDEX IF NOT EXISTS agent_conversations_state
                ON agent_conversations (state, updated_at_ms);
             CREATE INDEX IF NOT EXISTS agent_conversations_opening_lease
                ON agent_conversations (state, opening_expires_at_ms)
                WHERE state = 'opening';
             CREATE TABLE IF NOT EXISTS agent_conversation_turns (
                run_id TEXT NOT NULL,
                parent_agent_id TEXT NOT NULL,
                conversation_id TEXT NOT NULL,
                sequence INTEGER NOT NULL CHECK (sequence >= 0),
                turn_id TEXT NOT NULL,
                operation_id TEXT NOT NULL,
                message_id TEXT NOT NULL,
                input_digest TEXT NOT NULL,
                state TEXT NOT NULL CHECK (state IN (
                    'queued', 'admitted', 'intent_committed', 'completed', 'failed',
                    'cancelled', 'outcome_unknown', 'reconciliation_required'
                )),
                revision INTEGER NOT NULL CHECK (revision >= 0),
                claim_owner TEXT,
                claim_expires_at_ms INTEGER,
                outcome_json BLOB,
                failure_class TEXT,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY (run_id, parent_agent_id, conversation_id, sequence),
                UNIQUE (run_id, parent_agent_id, conversation_id, turn_id),
                FOREIGN KEY (run_id, parent_agent_id, conversation_id)
                    REFERENCES agent_conversations(run_id, parent_agent_id, conversation_id)
                    ON DELETE RESTRICT,
                CHECK (
                    (state = 'queued' AND claim_owner IS NULL AND claim_expires_at_ms IS NULL AND outcome_json IS NULL)
                    OR (state IN ('admitted', 'intent_committed') AND claim_owner IS NOT NULL
                        AND claim_expires_at_ms IS NOT NULL AND outcome_json IS NULL)
                    OR (state IN ('completed', 'failed', 'cancelled', 'outcome_unknown', 'reconciliation_required')
                        AND claim_owner IS NULL AND claim_expires_at_ms IS NULL AND outcome_json IS NOT NULL)
                )
             );
             CREATE INDEX IF NOT EXISTS agent_conversation_turns_queue
                ON agent_conversation_turns (run_id, parent_agent_id, conversation_id, sequence, state);",
        )
        .map_err(|source| db_error("create Agent conversation schema", source))
}

fn create_gc_run_claims(transaction: &Transaction<'_>) -> Result<(), SessionStoreError> {
    transaction
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS session_gc_run_claims (
                job_id TEXT NOT NULL REFERENCES session_gc_jobs(job_id) ON DELETE CASCADE,
                session_id TEXT NOT NULL,
                root_run_id TEXT NOT NULL CHECK (length(root_run_id) > 0),
                PRIMARY KEY (job_id, root_run_id)
             );
             CREATE INDEX IF NOT EXISTS session_gc_run_claims_root
                ON session_gc_run_claims (root_run_id, session_id);",
        )
        .map_err(|source| db_error("create session GC Run claim schema", source))
}

fn create_durable_tasks(transaction: &Transaction<'_>) -> Result<(), SessionStoreError> {
    transaction
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS durable_agent_tasks (
                session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
                task_key TEXT NOT NULL,
                input_digest BLOB NOT NULL CHECK (length(input_digest) = 32),
                phase TEXT NOT NULL CHECK (phase IN (
                    'created', 'user_checkpointed', 'awaiting_provider',
                    'provider_in_flight', 'provider_completed',
                    'tools_in_flight', 'tools_completed', 'completed', 'outcome_unknown',
                    'side_effect_unknown', 'aborted'
                )),
                call_id TEXT,
                session_revision INTEGER NOT NULL CHECK (session_revision >= 0),
                task_revision INTEGER NOT NULL CHECK (task_revision >= 0),
                terminal_result_json BLOB,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY (session_id, task_key),
                CHECK (
                    (phase = 'completed' AND terminal_result_json IS NOT NULL)
                    OR (phase != 'completed' AND terminal_result_json IS NULL)
                )
             );
             CREATE INDEX IF NOT EXISTS durable_agent_tasks_phase
                ON durable_agent_tasks (phase, updated_at_ms);",
        )
        .map_err(|source| db_error("create durable task schema", source))
}

fn create_host_outbox(transaction: &Transaction<'_>) -> Result<(), SessionStoreError> {
    transaction
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS host_outbox (
                delivery_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                msg_id TEXT NOT NULL,
                run_epoch INTEGER NOT NULL CHECK (run_epoch >= 0),
                sequence INTEGER NOT NULL CHECK (sequence >= 0),
                digest BLOB NOT NULL,
                payload BLOB NOT NULL,
                created_at_ms INTEGER NOT NULL,
                acknowledged_at_ms INTEGER,
                UNIQUE (session_id, run_epoch, sequence)
             );
             CREATE INDEX IF NOT EXISTS host_outbox_pending
                ON host_outbox (session_id, run_epoch, sequence)
                WHERE acknowledged_at_ms IS NULL;",
        )
        .map_err(|source| db_error("create host outbox schema", source))
}

fn verify_meta_version(transaction: &Transaction<'_>) -> Result<(), SessionStoreError> {
    let version: i64 = transaction
        .query_row(
            "SELECT schema_version FROM session_store_meta WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .map_err(|source| db_error("verify session store schema metadata", source))?;
    if version != SESSION_STORE_SCHEMA_VERSION {
        return Err(SessionStoreError::UnsupportedSchema {
            found: version,
            supported: SESSION_STORE_SCHEMA_VERSION,
        });
    }
    Ok(())
}

#[cfg(test)]
#[path = "store_schema_test.rs"]
mod store_schema_test;
