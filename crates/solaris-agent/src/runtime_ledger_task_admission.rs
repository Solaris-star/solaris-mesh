use std::collections::{HashMap, HashSet};
use std::io;

use rusqlite::{TransactionBehavior, params};
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::{RunId, TaskId};
use solaris_types::runtime::TaskRecord;

use super::{
    InMemoryLedgerState, LEDGER_SCHEMA_VERSION, LedgerRecord, SqliteRuntimeLedger, allocate_sqlite_sequence,
    durability_code, set_sqlite_synchronous, sqlite_error, sqlite_sequence_to_u64,
};

fn decode_task(payload: &[u8]) -> io::Result<TaskRecord> {
    serde_json::from_slice(payload).map_err(|error| io::Error::other(format!("decode task_created payload: {error}")))
}

fn validate_batch(
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
    run_id: &RunId,
    max_tasks: usize,
    tasks: &[TaskRecord],
) -> io::Result<Vec<LedgerRecord>> {
    let existing = state
        .records
        .get(run_id)
        .into_iter()
        .flatten()
        .filter(|record| record.record_type == "task_created")
        .map(|record| {
            serde_json::from_value::<TaskRecord>(record.payload.clone())
                .map(|task| (task.task_id.clone(), task))
                .map_err(|error| io::Error::other(format!("decode task_created payload: {error}")))
        })
        .collect::<io::Result<HashMap<_, _>>>()?;
    let admitted = validate_batch(&existing, max_tasks, tasks)?;
    let mut records = Vec::with_capacity(admitted.len());
    for task in admitted {
        state.next_sequence = state
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| io::Error::other("runtime ledger sequence exhausted"))?;
        let record = LedgerRecord {
            schema_version: LEDGER_SCHEMA_VERSION,
            seq: state.next_sequence,
            run_id: run_id.clone(),
            timestamp_unix_ms: chrono::Utc::now().timestamp_millis(),
            durability: DurabilityClass::SyncCritical,
            record_type: "task_created".to_owned(),
            payload: serde_json::to_value(task).map_err(|error| io::Error::other(error.to_string()))?,
        };
        state.records.entry(run_id.clone()).or_default().push(record.clone());
        records.push(record);
    }
    Ok(records)
}

pub(super) fn admit_collaboration_tasks_sqlite(
    ledger: &SqliteRuntimeLedger,
    run_id: &RunId,
    max_tasks: usize,
    tasks: &[TaskRecord],
) -> io::Result<Vec<LedgerRecord>> {
    let mut state = ledger.connection.lock().unwrap_or_else(|error| error.into_inner());
    set_sqlite_synchronous(&mut state, DurabilityClass::SyncCritical)?;
    let transaction = state
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| sqlite_error("begin collaboration task admission", error))?;
    let existing = {
        let mut statement = transaction
            .prepare(
                "SELECT payload FROM runtime_ledger_records
                 WHERE run_id = ?1 AND record_type = 'task_created'
                 ORDER BY sequence",
            )
            .map_err(|error| sqlite_error("prepare collaboration task admission query", error))?;
        let rows = statement
            .query_map(params![run_id.as_str()], |row| row.get::<_, Vec<u8>>(0))
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
        existing
    };
    let admitted = validate_batch(&existing, max_tasks, tasks)?;
    let timestamp_unix_ms = chrono::Utc::now().timestamp_millis();
    let durability = DurabilityClass::SyncCritical;
    let durability_code = durability_code(durability).expect("sync critical is persisted");
    let mut records = Vec::with_capacity(admitted.len());
    for task in admitted {
        let sequence = allocate_sqlite_sequence(&transaction)?;
        let payload = serde_json::to_value(task).map_err(|error| io::Error::other(error.to_string()))?;
        let encoded = serde_json::to_vec(&payload).map_err(|error| io::Error::other(error.to_string()))?;
        transaction
            .execute(
                "INSERT INTO runtime_ledger_records
                    (sequence, schema_version, run_id, timestamp_unix_ms, durability, record_type, payload)
                 VALUES (?1, ?2, ?3, ?4, ?5, 'task_created', ?6)",
                params![
                    sequence,
                    i64::from(LEDGER_SCHEMA_VERSION),
                    run_id.as_str(),
                    timestamp_unix_ms,
                    durability_code,
                    encoded,
                ],
            )
            .map_err(|error| sqlite_error("insert admitted collaboration task", error))?;
        records.push(LedgerRecord {
            schema_version: LEDGER_SCHEMA_VERSION,
            seq: sqlite_sequence_to_u64(sequence)?,
            run_id: run_id.clone(),
            timestamp_unix_ms,
            durability,
            record_type: "task_created".to_owned(),
            payload,
        });
    }
    transaction
        .commit()
        .map_err(|error| sqlite_error("commit collaboration task admission", error))?;
    Ok(records)
}
