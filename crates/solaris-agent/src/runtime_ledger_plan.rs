use std::io;

use rusqlite::{TransactionBehavior, params};
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::RunId;
use solaris_types::plan::PlanArtifact;

use super::{
    LEDGER_SCHEMA_VERSION, RuntimeLedger, SqliteRuntimeLedger, allocate_sqlite_sequence, durability_code,
    query_sqlite_records, set_sqlite_synchronous, sqlite_error,
};
use crate::plan::artifact::{PLAN_ARTIFACT_RECORD_TYPE, matching_plan_artifact, next_plan_artifact};

pub(super) fn record_plan_artifact_default<L: RuntimeLedger + ?Sized>(
    ledger: &L,
    run_id: &RunId,
    msg_id: &str,
    markdown: &str,
) -> io::Result<PlanArtifact> {
    let records = ledger.records_for_run(run_id)?;
    let digest = PlanArtifact::markdown_digest(markdown);
    if let Some(existing) = matching_plan_artifact(&records, msg_id, &digest).map_err(io::Error::other)? {
        return Ok(existing);
    }
    let artifact = next_plan_artifact(
        &records,
        run_id,
        msg_id,
        markdown,
        chrono::Utc::now().timestamp_millis(),
    )
    .map_err(io::Error::other)?;
    ledger.append(
        run_id,
        DurabilityClass::SyncCritical,
        PLAN_ARTIFACT_RECORD_TYPE,
        serde_json::to_value(&artifact).map_err(io::Error::other)?,
    )?;
    Ok(artifact)
}

pub(super) fn record_plan_artifact_sqlite(
    ledger: &SqliteRuntimeLedger,
    run_id: &RunId,
    msg_id: &str,
    markdown: &str,
) -> io::Result<PlanArtifact> {
    let mut state = ledger.connection.lock().unwrap_or_else(|error| error.into_inner());
    set_sqlite_synchronous(&mut state, DurabilityClass::SyncCritical)?;
    let transaction = state
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| sqlite_error("begin PlanArtifact append", error))?;
    let records = query_sqlite_records(
        &transaction,
        "SELECT sequence, schema_version, run_id, timestamp_unix_ms, durability, record_type, payload
         FROM runtime_ledger_records WHERE run_id = ?1 ORDER BY sequence",
        params![run_id.as_str()],
    )?;
    let digest = PlanArtifact::markdown_digest(markdown);
    if let Some(existing) = matching_plan_artifact(&records, msg_id, &digest).map_err(io::Error::other)? {
        transaction
            .commit()
            .map_err(|error| sqlite_error("commit PlanArtifact reuse", error))?;
        return Ok(existing);
    }
    let artifact = next_plan_artifact(
        &records,
        run_id,
        msg_id,
        markdown,
        chrono::Utc::now().timestamp_millis(),
    )
    .map_err(io::Error::other)?;
    let payload = serde_json::to_vec(&artifact).map_err(|_| io::Error::other("encode PlanArtifact payload"))?;
    let sequence = allocate_sqlite_sequence(&transaction)?;
    let durability = durability_code(DurabilityClass::SyncCritical)
        .ok_or_else(|| io::Error::other("PlanArtifact durability is not persistent"))?;
    transaction
        .execute(
            "INSERT INTO runtime_ledger_records
                (sequence, schema_version, run_id, timestamp_unix_ms, durability, record_type, payload)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                sequence,
                i64::from(LEDGER_SCHEMA_VERSION),
                run_id.as_str(),
                artifact.updated_at_unix_ms,
                durability,
                PLAN_ARTIFACT_RECORD_TYPE,
                payload,
            ],
        )
        .map_err(|error| sqlite_error("insert PlanArtifact record", error))?;
    transaction
        .commit()
        .map_err(|error| sqlite_error("commit PlanArtifact append", error))?;
    Ok(artifact)
}
