use chrono::Utc;
use rusqlite::{Transaction, params};
use std::collections::BTreeSet;
use uuid::Uuid;

use super::{SessionStore, SessionStoreError, begin_immediate, db_error};

const GC_DELAY_MILLISECONDS: i64 = 30 * 24 * 60 * 60 * 1_000;

impl SessionStore {
    pub(crate) fn enforce_retention(&self, max_sessions: usize) -> Result<(), SessionStoreError> {
        let mut connection = self.open_connection()?;
        let storage = connection.storage_guard();
        let transaction = begin_immediate(&mut connection, "begin session retention")?;
        let now_ms = Utc::now().timestamp_millis();
        let visible_count: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM sessions
                 WHERE NOT EXISTS (
                    SELECT 1 FROM session_tombstones
                    WHERE session_tombstones.session_id = sessions.session_id
                 )",
                [],
                |row| row.get(0),
            )
            .map_err(|source| db_error("count visible sessions for retention", source))?;
        let excess = usize::try_from(visible_count)
            .unwrap_or(usize::MAX)
            .saturating_sub(max_sessions);
        if excess == 0 {
            storage.verify()?;
            transaction
                .commit()
                .map_err(|source| db_error("commit empty session retention", source))?;
            connection.verify_storage_slots()?;
            return Ok(());
        }
        let limit = i64::try_from(excess).unwrap_or(i64::MAX);
        let candidates = retention_candidates(&transaction, now_ms, limit)?;
        for session_id in candidates {
            plan_retention_gc(&transaction, &session_id, now_ms)?;
        }
        storage.verify()?;
        transaction
            .commit()
            .map_err(|source| db_error("commit session retention", source))?;
        connection.verify_storage_slots()
    }
}

fn retention_candidates(
    transaction: &Transaction<'_>,
    now_ms: i64,
    limit: i64,
) -> Result<Vec<String>, SessionStoreError> {
    let mut statement = transaction
        .prepare(
            "SELECT sessions.session_id
             FROM sessions
             LEFT JOIN session_leases ON session_leases.session_id = sessions.session_id
             WHERE NOT EXISTS (
                SELECT 1 FROM session_tombstones
                WHERE session_tombstones.session_id = sessions.session_id
             )
               AND (session_leases.owner_id IS NULL OR session_leases.expires_at_ms <= ?1)
               AND NOT EXISTS (
                    SELECT 1 FROM host_outbox
                    WHERE host_outbox.session_id = sessions.session_id
                      AND host_outbox.acknowledged_at_ms IS NULL
               )
             ORDER BY sessions.updated_at_ms, sessions.created_at_ms, sessions.rowid
             LIMIT ?2",
        )
        .map_err(|source| db_error("prepare session retention candidates", source))?;
    let rows = statement
        .query_map(params![now_ms, limit], |row| row.get::<_, String>(0))
        .map_err(|source| db_error("query session retention candidates", source))?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|source| db_error("read session retention candidates", source))
}

fn plan_retention_gc(transaction: &Transaction<'_>, session_id: &str, now_ms: i64) -> Result<(), SessionStoreError> {
    if has_pending_host_deliveries(transaction, session_id)? {
        return Ok(());
    }
    let run_references = read_run_references(transaction, session_id)?;
    let run_ids = run_references
        .iter()
        .filter_map(|reference| {
            reference
                .get("run_id")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .filter(|run_id| !run_id.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let exact_run_tree_json = serde_json::to_vec(&serde_json::json!({
        "schema": "solaris/session-run-tree/v1",
        "session_id": session_id,
        "run_ids": run_ids,
        "references": run_references,
    }))
    .map_err(|source| SessionStoreError::Json {
        operation: "encode retained session run tree",
        source,
    })?;
    let job_id = Uuid::now_v7().to_string();
    transaction
        .execute(
            "INSERT INTO session_tombstones (session_id, deleted_at_ms, reason)
             VALUES (?1, ?2, 'retention')",
            params![session_id, now_ms],
        )
        .map_err(|source| db_error("tombstone retained session", source))?;
    transaction
        .execute(
            "INSERT INTO session_gc_jobs
                (job_id, session_id, exact_run_tree_json, phase, created_at_ms,
                 eligible_at_ms, updated_at_ms, last_error)
             VALUES (?1, ?2, ?3, 'planned', ?4, ?5, ?4, NULL)",
            params![
                &job_id,
                session_id,
                exact_run_tree_json,
                now_ms,
                now_ms.saturating_add(GC_DELAY_MILLISECONDS),
            ],
        )
        .map_err(|source| db_error("plan retained session GC", source))?;
    for run_id in run_ids {
        transaction
            .execute(
                "INSERT INTO session_gc_run_claims (job_id, session_id, root_run_id)
                 VALUES (?1, ?2, ?3)",
                params![&job_id, session_id, run_id],
            )
            .map_err(|source| db_error("claim retained session Run for GC", source))?;
    }
    Ok(())
}

fn has_pending_host_deliveries(transaction: &Transaction<'_>, session_id: &str) -> Result<bool, SessionStoreError> {
    transaction
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM host_outbox
                WHERE session_id = ?1 AND acknowledged_at_ms IS NULL
             )",
            params![session_id],
            |row| row.get(0),
        )
        .map_err(|source| db_error("check pending host deliveries before session GC", source))
}

fn read_run_references(
    transaction: &Transaction<'_>,
    session_id: &str,
) -> Result<Vec<serde_json::Value>, SessionStoreError> {
    let mut statement = transaction
        .prepare(
            "SELECT run_id, reference_kind FROM session_run_references
             WHERE session_id = ?1 ORDER BY run_id, reference_kind",
        )
        .map_err(|source| db_error("prepare retained session run tree", source))?;
    let rows = statement
        .query_map(params![session_id], |row| {
            Ok(serde_json::json!({
                "run_id": row.get::<_, String>(0)?,
                "reference_kind": row.get::<_, String>(1)?,
            }))
        })
        .map_err(|source| db_error("query retained session run tree", source))?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|source| db_error("read retained session run tree", source))
}
