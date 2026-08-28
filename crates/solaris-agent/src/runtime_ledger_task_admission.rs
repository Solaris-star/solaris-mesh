use std::collections::{HashMap, HashSet};
use std::io;

use rusqlite::{Transaction, TransactionBehavior, params};
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::{AgentId, RunId, TaskId, TeamId};
use solaris_types::runtime::{TaskRecord, TaskState};

use crate::team_registry::TeamRecord;

type CollaborationBatchPayload = (Vec<TeamRecord>, Vec<(TeamId, AgentId)>, Vec<TaskRecord>);
type DurableCollaborationMetadata = (HashMap<TeamId, TeamRecord>, HashSet<(TeamId, AgentId)>);

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

fn decode_collaboration_batch(payload: &serde_json::Value) -> io::Result<CollaborationBatchPayload> {
    let teams = serde_json::from_value(payload.get("teams").cloned().unwrap_or_else(|| serde_json::json!([])))
        .map_err(|error| io::Error::other(format!("decode collaboration batch teams: {error}")))?;
    let memberships = serde_json::from_value(
        payload
            .get("memberships")
            .cloned()
            .unwrap_or_else(|| serde_json::json!([])),
    )
    .map_err(|error| io::Error::other(format!("decode collaboration batch memberships: {error}")))?;
    let tasks = serde_json::from_value(payload.get("tasks").cloned().unwrap_or_else(|| serde_json::json!([])))
        .map_err(|error| io::Error::other(format!("decode collaboration batch tasks: {error}")))?;
    Ok((teams, memberships, tasks))
}

fn same_team_metadata(left: &TeamRecord, right: &TeamRecord) -> bool {
    left.run_id == right.run_id
        && left.team_id == right.team_id
        && left.name == right.name
        && left.strategy == right.strategy
        && left.coordinator == right.coordinator
        && left.direct_peer_messaging == right.direct_peer_messaging
        && left.max_pending_messages == right.max_pending_messages
        && left.max_message_bytes == right.max_message_bytes
}

fn register_team_metadata(teams: &mut HashMap<TeamId, TeamRecord>, team: TeamRecord) -> io::Result<()> {
    team.validate_message_limits()?;
    if let Some(existing) = teams.get(&team.team_id) {
        if !same_team_metadata(existing, &team) {
            return Err(io::Error::other(format!(
                "team {} was durably created with different metadata",
                team.team_id
            )));
        }
        return Ok(());
    }
    teams.insert(team.team_id.clone(), team);
    Ok(())
}

fn collaboration_metadata_from_records(
    run_id: &RunId,
    records: &[(String, serde_json::Value)],
) -> io::Result<DurableCollaborationMetadata> {
    let mut teams = HashMap::new();
    let mut memberships = HashSet::new();
    for (record_type, payload) in records {
        match record_type.as_str() {
            "team_created" => {
                let team: TeamRecord = serde_json::from_value(payload.clone())
                    .map_err(|error| io::Error::other(format!("decode team_created payload: {error}")))?;
                if team.run_id != *run_id {
                    return Err(io::Error::other("durable Team belongs to a different Run"));
                }
                register_team_metadata(&mut teams, team)?;
            }
            "collaboration_team_prepared" | "collaboration_batch_prepared" => {
                let (batch_teams, batch_memberships, _) = decode_collaboration_batch(payload)?;
                for team in batch_teams {
                    if team.run_id != *run_id {
                        return Err(io::Error::other(
                            "durable collaboration Team belongs to a different Run",
                        ));
                    }
                    register_team_metadata(&mut teams, team)?;
                }
                memberships.extend(batch_memberships);
            }
            "team_member_joined" => {
                let team_id = payload
                    .get("team_id")
                    .and_then(serde_json::Value::as_str)
                    .map(TeamId::from)
                    .ok_or_else(|| io::Error::other("team_member_joined is missing team_id"))?;
                let agent_id = payload
                    .get("agent_id")
                    .and_then(serde_json::Value::as_str)
                    .map(AgentId::from)
                    .ok_or_else(|| io::Error::other("team_member_joined is missing agent_id"))?;
                memberships.insert((team_id, agent_id));
            }
            _ => {}
        }
    }
    Ok((teams, memberships))
}

fn validate_team_task_admission(
    task: &TaskRecord,
    teams: &HashMap<TeamId, TeamRecord>,
    memberships: &HashSet<(TeamId, AgentId)>,
) -> io::Result<()> {
    let Some(team_id) = task.team_id.as_ref() else {
        return Ok(());
    };
    let team = teams.get(team_id).ok_or_else(|| {
        io::Error::other(format!(
            "task {} references Team {} that is not durable in the same atomic batch",
            task.task_id, team_id
        ))
    })?;
    let coordinator = team.coordinator.as_ref().ok_or_else(|| {
        io::Error::other(format!(
            "team task {} references Team {} without a coordinator",
            task.task_id, team_id
        ))
    })?;
    if !memberships.contains(&(team_id.clone(), coordinator.clone())) {
        return Err(io::Error::other(format!(
            "team task {} coordinator {} is not durably a member of Team {}",
            task.task_id, coordinator, team_id
        )));
    }

    match task.state {
        TaskState::Created => {
            return Err(io::Error::other(format!(
                "team task {} cannot be durably admitted in Created state",
                task.task_id
            )));
        }
        TaskState::Queued => {
            if task.owner_agent_id.is_some() {
                return Err(io::Error::other(format!(
                    "queued team task {} must not have an assigned owner",
                    task.task_id
                )));
            }
        }
        TaskState::Assigned | TaskState::Running => {
            let owner = task.owner_agent_id.as_ref().ok_or_else(|| {
                io::Error::other(format!(
                    "team task {} in {:?} state has no assigned owner membership",
                    task.task_id, task.state
                ))
            })?;
            if !memberships.contains(&(team_id.clone(), owner.clone())) {
                return Err(io::Error::other(format!(
                    "task {} owner {} is not durably a member of Team {}",
                    task.task_id, owner, team_id
                )));
            }
        }
        TaskState::Completed | TaskState::Failed | TaskState::Cancelled | TaskState::Skipped => {
            if let Some(owner) = task.owner_agent_id.as_ref()
                && !memberships.contains(&(team_id.clone(), owner.clone()))
            {
                return Err(io::Error::other(format!(
                    "task {} historical owner {} is not durably a member of Team {}",
                    task.task_id, owner, team_id
                )));
            }
        }
    }
    Ok(())
}

fn prepare_additional_records(
    run_id: &RunId,
    existing_records: &[(String, serde_json::Value)],
    tasks: &[TaskRecord],
    additional: &[(DurabilityClass, String, serde_json::Value)],
) -> io::Result<Vec<(DurabilityClass, String, serde_json::Value)>> {
    let has_collaboration_batch = additional
        .iter()
        .any(|(_, record_type, _)| record_type == "collaboration_batch_prepared");
    if !has_collaboration_batch {
        return Ok(additional
            .iter()
            .filter(|(durability, _, _)| *durability != DurabilityClass::Ephemeral)
            .cloned()
            .collect());
    }

    let (mut teams, mut memberships) = collaboration_metadata_from_records(run_id, existing_records)?;
    for (_, record_type, payload) in additional {
        if record_type != "collaboration_batch_prepared" {
            continue;
        }
        let (batch_teams, batch_memberships, batch_tasks) = decode_collaboration_batch(payload)?;
        if batch_tasks != tasks {
            return Err(io::Error::other(
                "collaboration batch marker tasks do not match the atomically admitted task set",
            ));
        }
        for team in batch_teams {
            if team.run_id != *run_id {
                return Err(io::Error::other("collaboration batch Team belongs to a different Run"));
            }
            register_team_metadata(&mut teams, team)?;
        }
        for (team_id, agent_id) in batch_memberships {
            if !teams.contains_key(&team_id) {
                return Err(io::Error::other(format!(
                    "collaboration batch membership for Agent {agent_id} references unknown Team {team_id}"
                )));
            }
            memberships.insert((team_id, agent_id));
        }
    }
    for task in tasks {
        validate_team_task_admission(task, &teams, &memberships)?;
    }

    Ok(additional
        .iter()
        .filter(|(durability, record_type, payload)| {
            *durability != DurabilityClass::Ephemeral
                && !(record_type == "collaboration_batch_prepared"
                    && existing_records.iter().any(|(existing_type, existing_payload)| {
                        existing_type == record_type && existing_payload == payload
                    }))
        })
        .cloned()
        .collect())
}

fn existing_metadata_in_memory(state: &InMemoryLedgerState, run_id: &RunId) -> Vec<(String, serde_json::Value)> {
    state
        .records
        .get(run_id)
        .into_iter()
        .flatten()
        .map(|record| (record.record_type.clone(), record.payload.clone()))
        .collect()
}

fn existing_metadata_sqlite(
    transaction: &Transaction<'_>,
    run_id: &RunId,
) -> io::Result<Vec<(String, serde_json::Value)>> {
    let mut statement = transaction
        .prepare("SELECT record_type, payload FROM runtime_ledger_records WHERE run_id = ?1 ORDER BY sequence")
        .map_err(|error| sqlite_error("prepare collaboration metadata query", error))?;
    let rows = statement
        .query_map(params![run_id.as_str()], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
        })
        .map_err(|error| sqlite_error("query collaboration metadata", error))?;
    let mut records = Vec::new();
    for row in rows {
        let (record_type, payload) = row.map_err(|error| sqlite_error("read collaboration metadata", error))?;
        let payload = serde_json::from_slice(&payload)
            .map_err(|error| io::Error::other(format!("decode collaboration metadata payload: {error}")))?;
        records.push((record_type, payload));
    }
    Ok(records)
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
    let existing_metadata = existing_metadata_in_memory(state, run_id);
    let additional = prepare_additional_records(run_id, &existing_metadata, tasks, additional)?;
    let mut pending: Vec<(DurabilityClass, String, serde_json::Value)> =
        Vec::with_capacity(admitted.len() + additional.len());
    for task in admitted {
        let payload = serde_json::to_value(task).map_err(|error| io::Error::other(error.to_string()))?;
        pending.push((DurabilityClass::SyncCritical, "task_created".to_owned(), payload));
    }
    pending.extend(additional);
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
    let existing_metadata = existing_metadata_sqlite(&transaction, run_id)?;
    let additional = prepare_additional_records(run_id, &existing_metadata, tasks, additional)?;
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
        records.push(insert_record_sqlite(
            &transaction,
            run_id,
            durability,
            &record_type,
            payload,
        )?);
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
