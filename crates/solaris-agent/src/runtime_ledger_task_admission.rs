use std::collections::{HashMap, HashSet};
use std::io;

use rusqlite::{Transaction, TransactionBehavior, params};
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::{RunId, TaskId};
use solaris_types::runtime::TaskRecord;

use super::runtime_ledger_workflow_lease::{
    advance_workflow_mutation_lease_in_memory, advance_workflow_mutation_lease_sqlite_transaction,
    validate_workflow_mutation_lease_in_memory, validate_workflow_mutation_lease_sqlite_transaction,
};
use super::{
    InMemoryLedgerState, LEDGER_SCHEMA_VERSION, LedgerRecord, SqliteRuntimeLedger, WorkflowMutationLease,
    allocate_sqlite_sequence, durability_code, set_sqlite_synchronous, sqlite_error, sqlite_sequence_to_u64,
};

fn decode_task(payload: &[u8]) -> io::Result<TaskRecord> {
    serde_json::from_slice(payload).map_err(|error| io::Error::other(format!("decode task_created payload: {error}")))
}

pub(super) fn validate_collaboration_batch(
    existing: &HashMap<TaskId, TaskRecord>,
    max_tasks: usize,
    tasks: &[TaskRecord],
) -> io::Result<Vec<TaskRecord>> {
    let mut batch_ids = HashSet::new();
    let mut admitted = Vec::new();
    for task in tasks {
        if let Some(previous) = existing.get(&task.task_id) {
            if previous != task {
                return Err(io::Error::other(format!(
                    "runtime task {} was durably created with different metadata",
                    task.task_id
                )));
            }
            continue;
        }
        if !batch_ids.insert(task.task_id.clone()) {
            return Err(io::Error::other(format!(
                "collaboration batch contains duplicate task {}",
                task.task_id
            )));
        }
        admitted.push(task.clone());
    }
    if existing.len().saturating_add(admitted.len()) > max_tasks {
        return Err(io::Error::other(format!(
            "Run accepts at most {max_tasks} collaboration tasks, including the independent reviewer"
        )));
    }
    Ok(admitted)
}

pub(super) fn admit_collaboration_tasks_in_memory(
    state: &mut InMemoryLedgerState,
    root_run_id: &RunId,
    run_id: &RunId,
    max_tasks: usize,
    tasks: &[TaskRecord],
) -> io::Result<Vec<LedgerRecord>> {
    admit_tasks_and_append_in_memory(state, root_run_id, run_id, max_tasks, tasks, &[])
}

/// Collects every `task_created` record charged to `root_run_id`, covering the
/// Root Run itself and all descendant Runs derived from its id.
fn existing_root_tasks_in_memory(
    state: &InMemoryLedgerState,
    root_run_id: &RunId,
) -> io::Result<HashMap<TaskId, TaskRecord>> {
    let prefix = format!("{}:", root_run_id.as_str());
    state
        .records
        .iter()
        .filter(|(candidate_run_id, _)| {
            *candidate_run_id == root_run_id || candidate_run_id.as_str().starts_with(&prefix)
        })
        .flat_map(|(_, records)| records)
        .filter(|record| record.record_type == "task_created")
        .map(|record| {
            serde_json::from_value::<TaskRecord>(record.payload.clone())
                .map(|task| (task.task_id.clone(), task))
                .map_err(|error| io::Error::other(format!("decode task_created payload: {error}")))
        })
        .collect()
}

pub(super) fn admit_collaboration_tasks_sqlite(
    ledger: &SqliteRuntimeLedger,
    root_run_id: &RunId,
    run_id: &RunId,
    max_tasks: usize,
    tasks: &[TaskRecord],
) -> io::Result<Vec<LedgerRecord>> {
    admit_tasks_and_append_sqlite(ledger, root_run_id, run_id, max_tasks, tasks, &[])
}

/// Reads every durable `task_created` record charged to `root_run_id`, which
/// covers the Root Run itself and all descendant Runs derived from its id.
fn existing_root_tasks_sqlite(
    transaction: &Transaction<'_>,
    root_run_id: &RunId,
) -> io::Result<HashMap<TaskId, TaskRecord>> {
    let mut statement = transaction
        .prepare(
            "SELECT payload FROM runtime_ledger_records
             WHERE (run_id = ?1 OR run_id LIKE ?2 ESCAPE '\\')
               AND record_type = 'task_created'
             ORDER BY sequence",
        )
        .map_err(|error| sqlite_error("prepare collaboration task admission query", error))?;
    let escaped_root = root_run_id
        .as_str()
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_");
    let descendant_pattern = format!("{escaped_root}:%");
    let rows = statement
        .query_map(params![root_run_id.as_str(), descendant_pattern], |row| {
            row.get::<_, Vec<u8>>(0)
        })
        .map_err(|error| sqlite_error("query collaboration task admission", error))?;
    let mut existing = HashMap::new();
    for row in rows {
        let task = decode_task(&row.map_err(|error| sqlite_error("read task_created payload", error))?)?;
        if let Some(previous) = existing.insert(task.task_id.clone(), task.clone())
            && previous != task
        {
            return Err(io::Error::other(format!(
                "runtime task {} is bound to multiple durable task_created records",
                task.task_id
            )));
        }
    }
    Ok(existing)
}

/// Inserts one durable record inside an open admission transaction. Ephemeral
/// records must be filtered out by the caller; they are never persisted.
fn insert_record_sqlite(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    durability: DurabilityClass,
    record_type: &str,
    payload: serde_json::Value,
) -> io::Result<LedgerRecord> {
    let durability_code = durability_code(durability)
        .ok_or_else(|| io::Error::other("ephemeral records are not persisted by task admission"))?;
    let sequence = allocate_sqlite_sequence(transaction)?;
    let timestamp_unix_ms = chrono::Utc::now().timestamp_millis();
    let encoded = serde_json::to_vec(&payload).map_err(|error| io::Error::other(error.to_string()))?;
    transaction
        .execute(
            "INSERT INTO runtime_ledger_records
                (sequence, schema_version, run_id, timestamp_unix_ms, durability, record_type, payload)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                sequence,
                i64::from(LEDGER_SCHEMA_VERSION),
                run_id.as_str(),
                timestamp_unix_ms,
                durability_code,
                record_type,
                encoded,
            ],
        )
        .map_err(|error| sqlite_error("insert admitted runtime ledger record", error))?;
    Ok(LedgerRecord {
        schema_version: LEDGER_SCHEMA_VERSION,
        seq: sqlite_sequence_to_u64(sequence)?,
        run_id: run_id.clone(),
        timestamp_unix_ms,
        durability,
        record_type: record_type.to_owned(),
        payload,
    })
}

/// Atomically admits `tasks` against the Root Run quota and appends
/// `additional` records under `run_id`. Every fallible step runs before the
/// first mutation, so a rejected batch leaves the ledger untouched.
pub(super) fn admit_tasks_and_append_in_memory(
    state: &mut InMemoryLedgerState,
    root_run_id: &RunId,
    run_id: &RunId,
    max_tasks: usize,
    tasks: &[TaskRecord],
    additional: &[(DurabilityClass, String, serde_json::Value)],
) -> io::Result<Vec<LedgerRecord>> {
    admit_tasks_and_append_in_memory_inner(state, root_run_id, run_id, max_tasks, tasks, additional, None)
}

pub(super) fn admit_tasks_and_append_under_workflow_lease_in_memory(
    state: &mut InMemoryLedgerState,
    lease: &WorkflowMutationLease,
    now_unix_ms: i64,
    root_run_id: &RunId,
    max_tasks: usize,
    tasks: &[TaskRecord],
    additional: &[(DurabilityClass, String, serde_json::Value)],
) -> io::Result<Vec<LedgerRecord>> {
    let run_id = lease.run_id.clone();
    admit_tasks_and_append_in_memory_inner(
        state,
        root_run_id,
        &run_id,
        max_tasks,
        tasks,
        additional,
        Some((lease, now_unix_ms)),
    )
}

fn admit_tasks_and_append_in_memory_inner(
    state: &mut InMemoryLedgerState,
    root_run_id: &RunId,
    run_id: &RunId,
    max_tasks: usize,
    tasks: &[TaskRecord],
    additional: &[(DurabilityClass, String, serde_json::Value)],
    workflow_lease: Option<(&WorkflowMutationLease, i64)>,
) -> io::Result<Vec<LedgerRecord>> {
    if let Some((lease, now_unix_ms)) = workflow_lease {
        validate_workflow_mutation_lease_in_memory(state, lease, now_unix_ms)?;
    }
    let existing = existing_root_tasks_in_memory(state, root_run_id)?;
    let admitted = validate_collaboration_batch(&existing, max_tasks, tasks)?;
    let mut pending: Vec<(DurabilityClass, String, serde_json::Value)> =
        Vec::with_capacity(admitted.len() + additional.len());
    for task in admitted {
        let payload = serde_json::to_value(task).map_err(|error| io::Error::other(error.to_string()))?;
        pending.push((DurabilityClass::SyncCritical, "task_created".to_owned(), payload));
    }
    for (durability, record_type, payload) in additional {
        if *durability != DurabilityClass::Ephemeral {
            pending.push((*durability, record_type.clone(), payload.clone()));
        }
    }
    let pending_len = u64::try_from(pending.len()).map_err(|_| io::Error::other("admission batch is too large"))?;
    state
        .next_sequence
        .checked_add(pending_len)
        .ok_or_else(|| io::Error::other("runtime ledger sequence exhausted"))?;
    let timestamp_unix_ms = workflow_lease.map_or_else(|| chrono::Utc::now().timestamp_millis(), |(_, now)| now);
    let mut records = Vec::with_capacity(pending.len());
    for (durability, record_type, payload) in pending {
        state.next_sequence += 1;
        let record = LedgerRecord {
            schema_version: LEDGER_SCHEMA_VERSION,
            seq: state.next_sequence,
            run_id: run_id.clone(),
            timestamp_unix_ms,
            durability,
            record_type,
            payload,
        };
        state.records.entry(run_id.clone()).or_default().push(record.clone());
        records.push(record);
    }
    if let Some((lease, now_unix_ms)) = workflow_lease {
        let observed_sequence = state
            .records
            .get(run_id)
            .and_then(|records| records.last())
            .map_or(lease.observed_sequence, |record| record.seq);
        advance_workflow_mutation_lease_in_memory(state, lease, now_unix_ms, observed_sequence);
    }
    Ok(records)
}

pub(super) fn admit_tasks_and_append_sqlite(
    ledger: &SqliteRuntimeLedger,
    root_run_id: &RunId,
    run_id: &RunId,
    max_tasks: usize,
    tasks: &[TaskRecord],
    additional: &[(DurabilityClass, String, serde_json::Value)],
) -> io::Result<Vec<LedgerRecord>> {
    admit_tasks_and_append_sqlite_inner(ledger, None, root_run_id, run_id, max_tasks, tasks, additional)
}

pub(super) fn admit_tasks_and_append_under_workflow_lease_sqlite(
    ledger: &SqliteRuntimeLedger,
    lease: &WorkflowMutationLease,
    now_unix_ms: i64,
    root_run_id: &RunId,
    max_tasks: usize,
    tasks: &[TaskRecord],
    additional: &[(DurabilityClass, String, serde_json::Value)],
) -> io::Result<Vec<LedgerRecord>> {
    let run_id = lease.run_id.clone();
    admit_tasks_and_append_sqlite_inner(
        ledger,
        Some((lease, now_unix_ms)),
        root_run_id,
        &run_id,
        max_tasks,
        tasks,
        additional,
    )
}

fn admit_tasks_and_append_sqlite_inner(
    ledger: &SqliteRuntimeLedger,
    workflow_lease: Option<(&WorkflowMutationLease, i64)>,
    root_run_id: &RunId,
    run_id: &RunId,
    max_tasks: usize,
    tasks: &[TaskRecord],
    additional: &[(DurabilityClass, String, serde_json::Value)],
) -> io::Result<Vec<LedgerRecord>> {
    let mut state = ledger.connection.lock().unwrap_or_else(|error| error.into_inner());
    set_sqlite_synchronous(&mut state, DurabilityClass::SyncCritical)?;
    let transaction = state
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| sqlite_error("begin task metadata admission", error))?;
    if let Some((lease, now_unix_ms)) = workflow_lease {
        validate_workflow_mutation_lease_sqlite_transaction(&transaction, lease, now_unix_ms)?;
    }
    let existing = existing_root_tasks_sqlite(&transaction, root_run_id)?;
    let admitted = validate_collaboration_batch(&existing, max_tasks, tasks)?;
    let mut records = Vec::with_capacity(admitted.len() + additional.len());
    for task in admitted {
        records.push(insert_record_sqlite(
            &transaction,
            run_id,
            DurabilityClass::SyncCritical,
            "task_created",
            serde_json::to_value(task).map_err(io::Error::other)?,
        )?);
    }
    for (durability, record_type, payload) in additional {
        if *durability != DurabilityClass::Ephemeral {
            records.push(insert_record_sqlite(
                &transaction,
                run_id,
                *durability,
                record_type,
                payload.clone(),
            )?);
        }
    }
    if let Some((lease, now_unix_ms)) = workflow_lease {
        let observed_sequence = records.last().map_or(lease.observed_sequence, |record| record.seq);
        advance_workflow_mutation_lease_sqlite_transaction(&transaction, lease, now_unix_ms, observed_sequence)?;
    }
    transaction
        .commit()
        .map_err(|error| sqlite_error("commit task metadata admission", error))?;
    Ok(records)
}
