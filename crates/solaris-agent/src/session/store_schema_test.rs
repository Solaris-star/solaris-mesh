use rusqlite::Connection;

use crate::session::store::DEFAULT_LEASE_SECONDS;

use super::*;

#[test]
fn version_one_gc_jobs_gain_a_delayed_eligibility_time() {
    let mut connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE session_store_meta (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                schema_version INTEGER NOT NULL
             );
             INSERT INTO session_store_meta (singleton, schema_version) VALUES (1, 1);
             CREATE TABLE session_gc_jobs (
                job_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                exact_run_tree_json BLOB NOT NULL,
                phase TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                last_error TEXT
             );
             INSERT INTO session_gc_jobs
                (job_id, session_id, exact_run_tree_json, phase, created_at_ms, updated_at_ms, last_error)
             VALUES ('job', 'session', X'7B7D', 'planned', 1000, 1000, NULL);
             PRAGMA user_version = 1;",
        )
        .unwrap();

    initialize_schema(&mut connection).unwrap();

    let (schema_version, user_version, eligible_at_ms, outbox_table_count): (i64, i64, i64, i64) = connection
        .query_row(
            "SELECT
                (SELECT schema_version FROM session_store_meta WHERE singleton = 1),
                (SELECT user_version FROM pragma_user_version),
                (SELECT eligible_at_ms FROM session_gc_jobs WHERE job_id = 'job'),
                (SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table' AND name = 'host_outbox')",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(schema_version, SESSION_STORE_SCHEMA_VERSION);
    assert_eq!(user_version, SESSION_STORE_SCHEMA_VERSION);
    assert_eq!(eligible_at_ms, 1000 + 30 * 24 * 60 * 60 * 1_000);
    assert_eq!(outbox_table_count, 1);
}

#[test]
fn version_two_store_gains_the_durable_host_outbox() {
    let mut connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE session_store_meta (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                schema_version INTEGER NOT NULL
             );
             INSERT INTO session_store_meta (singleton, schema_version) VALUES (1, 2);
             CREATE TABLE session_gc_jobs (
                job_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                exact_run_tree_json BLOB NOT NULL,
                phase TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                eligible_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                last_error TEXT
             );
             PRAGMA user_version = 2;",
        )
        .unwrap();

    initialize_schema(&mut connection).unwrap();

    let (schema_version, user_version): (i64, i64) = connection
        .query_row(
            "SELECT
                (SELECT schema_version FROM session_store_meta WHERE singleton = 1),
                (SELECT user_version FROM pragma_user_version)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(schema_version, SESSION_STORE_SCHEMA_VERSION);
    assert_eq!(user_version, SESSION_STORE_SCHEMA_VERSION);

    let table_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type = 'table' AND name = 'host_outbox'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(table_count, 1);
}

#[test]
fn version_three_store_gains_durable_agent_tasks() {
    let mut connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE session_store_meta (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                schema_version INTEGER NOT NULL
             );
             INSERT INTO session_store_meta (singleton, schema_version) VALUES (1, 3);
             CREATE TABLE sessions (session_id TEXT PRIMARY KEY);
             CREATE TABLE session_gc_jobs (
                job_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                exact_run_tree_json BLOB NOT NULL,
                phase TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                eligible_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                last_error TEXT
             );
             PRAGMA user_version = 3;",
        )
        .unwrap();

    initialize_schema(&mut connection).unwrap();

    let (schema_version, user_version, table_count): (i64, i64, i64) = connection
        .query_row(
            "SELECT
                (SELECT schema_version FROM session_store_meta WHERE singleton = 1),
                (SELECT user_version FROM pragma_user_version),
                (SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table' AND name = 'durable_agent_tasks')",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(schema_version, SESSION_STORE_SCHEMA_VERSION);
    assert_eq!(user_version, SESSION_STORE_SCHEMA_VERSION);
    assert_eq!(table_count, 1);
}

#[test]
fn version_four_store_backfills_incomplete_gc_run_claims() {
    let mut connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE session_store_meta (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                schema_version INTEGER NOT NULL
             );
             INSERT INTO session_store_meta (singleton, schema_version) VALUES (1, 4);
             CREATE TABLE session_gc_jobs (
                job_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                exact_run_tree_json BLOB NOT NULL,
                phase TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                eligible_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                last_error TEXT
             );
             INSERT INTO session_gc_jobs
                (job_id, session_id, exact_run_tree_json, phase, created_at_ms,
                 eligible_at_ms, updated_at_ms, last_error)
             VALUES
                ('pending', 'session-a',
                 '{\"schema\":\"solaris/session-run-tree/v1\",\"session_id\":\"session-a\",\"run_ids\":[\"run-a\",\"run-a:child\"]}',
                 'planned', 1, 2, 1, NULL),
                ('done', 'session-b',
                 '{\"schema\":\"solaris/session-run-tree/v1\",\"session_id\":\"session-b\",\"run_ids\":[\"run-b\"]}',
                 'complete', 1, 2, 1, NULL);
             PRAGMA user_version = 4;",
        )
        .unwrap();

    initialize_schema(&mut connection).unwrap();

    let claims = connection
        .prepare("SELECT job_id, session_id, root_run_id FROM session_gc_run_claims ORDER BY root_run_id")
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        claims,
        vec![
            ("pending".to_owned(), "session-a".to_owned(), "run-a".to_owned()),
            ("pending".to_owned(), "session-a".to_owned(), "run-a:child".to_owned()),
        ]
    );
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, SESSION_STORE_SCHEMA_VERSION);
}

#[test]
fn version_five_store_preserves_tasks_and_adds_user_checkpoint_phases() {
    let mut connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE session_store_meta (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                schema_version INTEGER NOT NULL
             );
             INSERT INTO session_store_meta (singleton, schema_version) VALUES (1, 5);
             CREATE TABLE sessions (
                session_id TEXT PRIMARY KEY,
                state_json BLOB NOT NULL,
                revision INTEGER NOT NULL,
                run_id TEXT,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL
             );
             INSERT INTO sessions VALUES ('session-a', X'7B7D', 0, 'run-a', 1, 1);
             CREATE TABLE durable_agent_tasks (
                session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
                task_key TEXT NOT NULL,
                input_digest BLOB NOT NULL CHECK (length(input_digest) = 32),
                phase TEXT NOT NULL CHECK (phase IN (
                    'awaiting_provider', 'provider_in_flight', 'provider_completed',
                    'tools_in_flight', 'tools_completed', 'completed', 'outcome_unknown', 'aborted'
                )),
                call_id TEXT,
                session_revision INTEGER NOT NULL,
                task_revision INTEGER NOT NULL,
                terminal_result_json BLOB,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY (session_id, task_key)
             );
             INSERT INTO durable_agent_tasks
                (session_id, task_key, input_digest, phase, call_id, session_revision,
                 task_revision, terminal_result_json, updated_at_ms)
             VALUES ('session-a', 'task-a', zeroblob(32), 'awaiting_provider',
                     'provider-call-v1:stable', 0, 1, NULL, 1);
             PRAGMA user_version = 5;",
        )
        .unwrap();

    initialize_schema(&mut connection).unwrap();

    let preserved: (String, Option<String>, i64) = connection
        .query_row(
            "SELECT phase, call_id, task_revision FROM durable_agent_tasks
             WHERE session_id = 'session-a' AND task_key = 'task-a'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        preserved,
        (
            "awaiting_provider".to_owned(),
            Some("provider-call-v1:stable".to_owned()),
            1
        )
    );
    let schema: String = connection
        .query_row(
            "SELECT sql FROM sqlite_master
             WHERE type = 'table' AND name = 'durable_agent_tasks'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(schema.contains("'created'"));
    assert!(schema.contains("'user_checkpointed'"));
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, SESSION_STORE_SCHEMA_VERSION);
}

#[test]
fn version_six_store_gains_durable_agent_conversations() {
    let mut connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE session_store_meta (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                schema_version INTEGER NOT NULL
             );
             INSERT INTO session_store_meta (singleton, schema_version) VALUES (1, 6);
             CREATE TABLE sessions (
                session_id TEXT PRIMARY KEY,
                state_json BLOB NOT NULL,
                revision INTEGER NOT NULL,
                run_id TEXT,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL
             );
             INSERT INTO sessions VALUES ('session-a', X'7B7D', 0, 'run-a', 1, 1);
             PRAGMA user_version = 6;",
        )
        .unwrap();

    initialize_schema(&mut connection).unwrap();

    let (meta_version, user_version, conversation_tables): (i64, i64, i64) = connection
        .query_row(
            "SELECT
                (SELECT schema_version FROM session_store_meta WHERE singleton = 1),
                (SELECT user_version FROM pragma_user_version),
                (SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table'
                   AND name IN ('agent_conversations', 'agent_conversation_turns'))",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(meta_version, SESSION_STORE_SCHEMA_VERSION);
    assert_eq!(user_version, SESSION_STORE_SCHEMA_VERSION);
    assert_eq!(conversation_tables, 2);
}

#[test]
fn version_seven_opening_claim_gains_epoch_and_expiry() {
    let mut connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE session_store_meta (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                schema_version INTEGER NOT NULL
             );
             INSERT INTO session_store_meta (singleton, schema_version) VALUES (1, 7);
             CREATE TABLE agent_conversations (
                run_id TEXT NOT NULL,
                parent_agent_id TEXT NOT NULL,
                conversation_id TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                state TEXT NOT NULL,
                revision INTEGER NOT NULL,
                opening_owner TEXT,
                agent_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                open_operation_id TEXT NOT NULL,
                spec_digest TEXT NOT NULL,
                handle_json BLOB,
                next_sequence INTEGER NOT NULL,
                next_to_run INTEGER NOT NULL,
                active_sequence INTEGER,
                terminal_state TEXT,
                failure_class TEXT,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY (run_id, parent_agent_id, conversation_id)
             );
             INSERT INTO agent_conversations VALUES (
                'run-a', 'parent-a', 'conversation-a', 1, 'opening', 0, 'owner-a',
                'agent-a', 'session-a', 'task-a', 'open-a', 'digest-a', NULL,
                0, 0, NULL, NULL, NULL, 1000
             );
             PRAGMA user_version = 7;",
        )
        .unwrap();

    initialize_schema(&mut connection).unwrap();

    let (meta_version, user_version, epoch, expires_at_ms): (i64, i64, i64, i64) = connection
        .query_row(
            "SELECT
                (SELECT schema_version FROM session_store_meta WHERE singleton = 1),
                (SELECT user_version FROM pragma_user_version),
                opening_epoch,
                opening_expires_at_ms
             FROM agent_conversations",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(meta_version, SESSION_STORE_SCHEMA_VERSION);
    assert_eq!(user_version, SESSION_STORE_SCHEMA_VERSION);
    assert_eq!(epoch, 1);
    assert_eq!(expires_at_ms, 1000 + DEFAULT_LEASE_SECONDS * 1000);
}

#[test]
fn version_eight_store_gains_side_effect_unknown_without_losing_tasks() {
    let mut connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE session_store_meta (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                schema_version INTEGER NOT NULL
             );
             INSERT INTO session_store_meta (singleton, schema_version) VALUES (1, 8);
             CREATE TABLE sessions (
                session_id TEXT PRIMARY KEY,
                state_json BLOB NOT NULL,
                revision INTEGER NOT NULL,
                run_id TEXT,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL
             );
             INSERT INTO sessions VALUES ('session-a', X'7B7D', 0, 'run-a', 1, 1);
             CREATE TABLE durable_agent_tasks (
                session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
                task_key TEXT NOT NULL,
                input_digest BLOB NOT NULL CHECK (length(input_digest) = 32),
                phase TEXT NOT NULL CHECK (phase IN (
                    'created', 'user_checkpointed', 'awaiting_provider',
                    'provider_in_flight', 'provider_completed',
                    'tools_in_flight', 'tools_completed', 'completed', 'outcome_unknown', 'aborted'
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
             CREATE INDEX durable_agent_tasks_phase
                ON durable_agent_tasks (phase, updated_at_ms);
             INSERT INTO durable_agent_tasks
                (session_id, task_key, input_digest, phase, call_id, session_revision,
                 task_revision, terminal_result_json, updated_at_ms)
             VALUES ('session-a', 'task-a', zeroblob(32), 'outcome_unknown',
                     'tool-call-v3:existing', 0, 1, NULL, 1);
             PRAGMA user_version = 8;",
        )
        .unwrap();

    initialize_schema(&mut connection).unwrap();

    let preserved: (String, Option<String>, i64) = connection
        .query_row(
            "SELECT phase, call_id, task_revision FROM durable_agent_tasks
             WHERE session_id = 'session-a' AND task_key = 'task-a'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        preserved,
        (
            "outcome_unknown".to_owned(),
            Some("tool-call-v3:existing".to_owned()),
            1
        )
    );
    let schema: String = connection
        .query_row(
            "SELECT sql FROM sqlite_master
             WHERE type = 'table' AND name = 'durable_agent_tasks'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(schema.contains("'side_effect_unknown'"));
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, SESSION_STORE_SCHEMA_VERSION);
}

#[test]
fn current_agent_conversation_schema_initialization_is_idempotent() {
    let mut connection = Connection::open_in_memory().unwrap();
    initialize_schema(&mut connection).unwrap();
    initialize_schema(&mut connection).unwrap();

    let (meta_version, user_version, conversation_tables): (i64, i64, i64) = connection
        .query_row(
            "SELECT
                (SELECT schema_version FROM session_store_meta WHERE singleton = 1),
                (SELECT user_version FROM pragma_user_version),
                (SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table'
                   AND name IN ('agent_conversations', 'agent_conversation_turns'))",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(meta_version, SESSION_STORE_SCHEMA_VERSION);
    assert_eq!(user_version, SESSION_STORE_SCHEMA_VERSION);
    assert_eq!(conversation_tables, 2);
}
