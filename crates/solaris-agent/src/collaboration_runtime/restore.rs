//! Durable ledger restoration for the collaboration runtime.

use std::sync::Arc;

use serde_json::{Value, json};
use solaris_types::identity::{AgentId, OperationId, RunId, TaskId, TeamId};
use solaris_types::runtime::{AgentLifecycleState, AgentRecord, TaskFailureClass, TaskRecord, TaskState};
use solaris_types::spawner::SubAgentResult;

use crate::execution_context::stable_digest_value;
use crate::message_bus::AgentMessage;
use crate::relationship_store::{AgentRelationship, AgentRelationshipStore};
use crate::runtime_ledger::LedgerRecord;
use crate::task_registry::TaskCasMutation;
use crate::team_registry::TeamRecord;
use crate::team_state::{ArtifactRef, TeamFact};

use super::CollaborationRuntime;
use super::spawn::{agent_state_is_terminal, outcome_agent_state};

impl<T> CollaborationRuntime<T> {
    pub fn restore_projection(&self, run_id: &RunId) -> std::io::Result<()> {
        let line = self.mutation.line_for(run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        self.restore_projection_locked(run_id)
    }

    fn restore_projection_locked(&self, run_id: &RunId) -> std::io::Result<()> {
        for record in self.ledger.records_for_run(run_id)? {
            match record.record_type.as_str() {
                "agent_spawn_intent" => {
                    let parent = record
                        .payload
                        .get("parent_agent_id")
                        .and_then(serde_json::Value::as_str)
                        .map(AgentId::from);
                    let child = record
                        .payload
                        .get("child_agent_id")
                        .and_then(serde_json::Value::as_str)
                        .map(AgentId::from);
                    if let (Some(parent_agent_id), Some(child_agent_id)) = (parent, child) {
                        self.agents.upsert(AgentRecord {
                            run_id: run_id.clone(),
                            agent_id: child_agent_id,
                            team_id: None,
                            parent_agent_id: Some(parent_agent_id),
                            state: AgentLifecycleState::Reserved,
                        });
                    }
                }
                "agent_spawn_committed" => {
                    if let Ok(relationship) = serde_json::from_value::<AgentRelationship>(record.payload.clone()) {
                        self.relationships.put(relationship.clone());
                        if self.agents.get(&relationship.child_agent_id).is_none() {
                            self.agents.upsert(AgentRecord {
                                run_id: relationship.run_id.clone(),
                                agent_id: relationship.child_agent_id.clone(),
                                team_id: None,
                                parent_agent_id: Some(relationship.parent_agent_id.clone()),
                                state: AgentLifecycleState::Active,
                            });
                        } else if self
                            .agents
                            .get(&relationship.child_agent_id)
                            .is_some_and(|agent| !agent_state_is_terminal(agent.state))
                        {
                            self.agents
                                .set_state(&relationship.child_agent_id, AgentLifecycleState::Active);
                        }
                    }
                }
                "agent_spawn_retry_started" => {
                    if let Ok(relationship) = serde_json::from_value::<AgentRelationship>(record.payload.clone()) {
                        self.relationships.put(relationship.clone());
                        self.agents
                            .set_state(&relationship.child_agent_id, AgentLifecycleState::Active);
                    }
                }
                "agent_spawn_cancelled" => {
                    let child_id = record
                        .payload
                        .get("child_agent_id")
                        .and_then(serde_json::Value::as_str)
                        .map(AgentId::from);
                    let team_id = record
                        .payload
                        .get("team_id")
                        .and_then(serde_json::Value::as_str)
                        .map(TeamId::from);
                    if let Some(child_id) = child_id {
                        if let Some(team_id) = team_id {
                            self.teams.leave(&team_id, &child_id);
                            self.agents.set_team(&child_id, None);
                        }
                        self.agents.set_state(&child_id, AgentLifecycleState::Cancelled);
                    }
                }
                "agent_outcome" => {
                    let child_id = record
                        .payload
                        .get("child_agent_id")
                        .and_then(serde_json::Value::as_str)
                        .map(AgentId::from);
                    let result = record
                        .payload
                        .get("result")
                        .cloned()
                        .and_then(|value| serde_json::from_value::<SubAgentResult>(value).ok());
                    if let (Some(child_id), Some(result)) = (child_id, result) {
                        self.agents.set_state(&child_id, outcome_agent_state(&result));
                    }
                }
                "agent_state_changed" => {
                    let agent_id = record
                        .payload
                        .get("agent_id")
                        .and_then(serde_json::Value::as_str)
                        .map(AgentId::from);
                    let state = record
                        .payload
                        .get("state")
                        .cloned()
                        .and_then(|value| serde_json::from_value::<AgentLifecycleState>(value).ok());
                    if let (Some(agent_id), Some(state)) = (agent_id, state) {
                        self.agents.set_state(&agent_id, state);
                    }
                }
                "agent_spawn_aborted" => {
                    let removed = record
                        .payload
                        .get("disposition")
                        .and_then(serde_json::Value::as_str)
                        .is_none_or(|value| value == "removed");
                    if removed
                        && let Some(child) = record.payload.get("child_agent_id").and_then(serde_json::Value::as_str)
                    {
                        self.agents.remove(&AgentId::from(child));
                    }
                    let team_id = record
                        .payload
                        .get("team_id")
                        .and_then(serde_json::Value::as_str)
                        .map(TeamId::from);
                    let child_id = record
                        .payload
                        .get("child_agent_id")
                        .and_then(serde_json::Value::as_str)
                        .map(AgentId::from);
                    if let (Some(team_id), Some(child_id)) = (team_id, child_id) {
                        self.teams.leave(&team_id, &child_id);
                    }
                }
                "team_created" => {
                    if let Ok(team) = serde_json::from_value::<TeamRecord>(record.payload.clone()) {
                        team.validate_message_limits()?;
                        if let Some(existing) = self.teams.get(&team.team_id)
                            && (existing.run_id != team.run_id
                                || existing.strategy != team.strategy
                                || existing.coordinator != team.coordinator
                                || existing.max_pending_messages != team.max_pending_messages
                                || existing.max_message_bytes != team.max_message_bytes)
                        {
                            return Err(std::io::Error::other(format!(
                                "team {} has incompatible restored collaboration metadata",
                                team.team_id
                            )));
                        }
                        self.teams.create(team);
                    }
                }
                "collaboration_batch_prepared" => {
                    let teams: Vec<TeamRecord> = record
                        .payload
                        .get("teams")
                        .cloned()
                        .and_then(|value| serde_json::from_value(value).ok())
                        .unwrap_or_default();
                    let memberships: Vec<(TeamId, AgentId)> = record
                        .payload
                        .get("memberships")
                        .cloned()
                        .and_then(|value| serde_json::from_value(value).ok())
                        .unwrap_or_default();
                    let tasks: Vec<TaskRecord> = record
                        .payload
                        .get("tasks")
                        .cloned()
                        .and_then(|value| serde_json::from_value(value).ok())
                        .unwrap_or_default();
                    for team in teams {
                        team.validate_message_limits()?;
                        if let Some(existing) = self.teams.get(&team.team_id)
                            && (existing.run_id != team.run_id
                                || existing.strategy != team.strategy
                                || existing.coordinator != team.coordinator
                                || existing.max_pending_messages != team.max_pending_messages
                                || existing.max_message_bytes != team.max_message_bytes)
                        {
                            return Err(std::io::Error::other(format!(
                                "team {} has incompatible restored collaboration metadata",
                                team.team_id
                            )));
                        }
                        self.teams.create(team);
                    }
                    for (team_id, agent_id) in memberships {
                        self.teams.join(&team_id, agent_id.clone());
                        self.agents.set_team(&agent_id, Some(team_id));
                    }
                    for task in tasks {
                        self.tasks.upsert(task);
                    }
                }
                "team_member_joined" => {
                    let team_id = record
                        .payload
                        .get("team_id")
                        .and_then(serde_json::Value::as_str)
                        .map(TeamId::from);
                    let agent_id = record
                        .payload
                        .get("agent_id")
                        .and_then(serde_json::Value::as_str)
                        .map(AgentId::from);
                    if let (Some(team_id), Some(agent_id)) = (team_id, agent_id) {
                        self.teams.join(&team_id, agent_id.clone());
                        self.agents.set_team(&agent_id, Some(team_id));
                    }
                }
                "team_fact_set" => {
                    if let Ok(fact) = serde_json::from_value::<TeamFact>(record.payload.clone()) {
                        self.team_state.set_fact(fact);
                    }
                }
                "artifact_registered" => {
                    if let Ok(artifact) = serde_json::from_value::<ArtifactRef>(record.payload.clone()) {
                        self.team_state.put_artifact(artifact);
                    }
                }
                "message_delivered" => {
                    if let Ok(message) = serde_json::from_value::<AgentMessage>(record.payload.clone()) {
                        self.messages.restore_message(message);
                    }
                }
                "messages_broadcast" => {
                    self.messages.restore_broadcast(record.payload.clone())?;
                }
                "message_acknowledged" => {
                    let agent_id = record
                        .payload
                        .get("agent_id")
                        .and_then(serde_json::Value::as_str)
                        .map(AgentId::from);
                    let message_id = record.payload.get("message_id").and_then(serde_json::Value::as_str);
                    let timestamp = record
                        .payload
                        .get("acknowledged_at_unix_ms")
                        .and_then(serde_json::Value::as_i64);
                    if let (Some(agent_id), Some(message_id), Some(timestamp)) = (agent_id, message_id, timestamp) {
                        self.messages.restore_acknowledgement(&agent_id, message_id, timestamp);
                    }
                }
                "messages_claimed" => {
                    let claim_id = record.payload.get("claim_id").and_then(serde_json::Value::as_str);
                    let message_ids = record
                        .payload
                        .get("message_ids")
                        .and_then(serde_json::Value::as_array)
                        .map(|values| {
                            values
                                .iter()
                                .filter_map(serde_json::Value::as_str)
                                .map(str::to_owned)
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    if let Some(claim_id) = claim_id {
                        self.messages.restore_claim(claim_id, &message_ids);
                    }
                }
                "messages_dequeued" => {
                    let agent_id = record
                        .payload
                        .get("agent_id")
                        .and_then(serde_json::Value::as_str)
                        .map(AgentId::from);
                    let message_ids = record
                        .payload
                        .get("message_ids")
                        .and_then(serde_json::Value::as_array)
                        .map(|values| {
                            values
                                .iter()
                                .filter_map(serde_json::Value::as_str)
                                .map(str::to_owned)
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    if let Some(agent_id) = agent_id {
                        self.messages.restore_dequeue(&agent_id, &message_ids);
                    }
                }
                "task_created" => {
                    if let Ok(task) = serde_json::from_value::<TaskRecord>(record.payload.clone()) {
                        self.tasks.upsert(task);
                    }
                }
                "task_assigned" | "task_handoff" => {
                    self.restore_legacy_assignment_record(&record)?;
                }
                "task_settled" => {
                    let task_id = record
                        .payload
                        .get("task_id")
                        .and_then(serde_json::Value::as_str)
                        .map(TaskId::from);
                    let state = record
                        .payload
                        .get("state")
                        .cloned()
                        .and_then(|value| serde_json::from_value::<TaskState>(value).ok());
                    if let (Some(task_id), Some(state)) = (task_id, state) {
                        self.tasks.restore_legacy_state(&task_id, state);
                    }
                }
                "task_cas" => {
                    let _ = self.restore_task_cas_record(&record)?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub(crate) fn restore_task_projection_across_lineage_locked(&self, task_id: &TaskId) -> std::io::Result<bool> {
        self.restore_task_projection_and_operation_across_lineage_locked(task_id, None)
            .map(|(found, _)| found)
    }

    pub(crate) fn restore_task_operation_across_lineage_locked(
        &self,
        task_id: &TaskId,
        operation_id: &OperationId,
    ) -> std::io::Result<(bool, Option<TaskRecord>)> {
        self.restore_task_projection_and_operation_across_lineage_locked(task_id, Some(operation_id))
    }

    fn restore_task_projection_and_operation_across_lineage_locked(
        &self,
        task_id: &TaskId,
        operation_id: Option<&OperationId>,
    ) -> std::io::Result<(bool, Option<TaskRecord>)> {
        let task = self
            .tasks
            .get(task_id)
            .ok_or_else(|| std::io::Error::other(format!("unknown task: {task_id}")))?;
        let allowed_line = self.mutation.line_for(&task.run_id);
        let mut records = Vec::new();
        for run_id in self.ledger.run_ids()? {
            if !Arc::ptr_eq(&allowed_line, &self.mutation.line_for(&run_id)) {
                continue;
            }
            records.extend(self.ledger.records_for_run(&run_id)?.into_iter().filter(|record| {
                record.record_type == "task_cas"
                    && record.payload.get("task_id").and_then(Value::as_str) == Some(task_id.as_str())
            }));
        }
        records.sort_by_key(|record| record.seq);
        let found = !records.is_empty();
        let mut operation_result: Option<TaskRecord> = None;
        for record in records {
            let matches_operation = operation_id.is_some_and(|operation_id| {
                record.payload.get("operation_id").and_then(Value::as_str) == Some(operation_id.as_str())
            });
            let restored = self.restore_task_cas_record(&record)?;
            if matches_operation {
                if operation_result.as_ref().is_some_and(|existing| existing != &restored) {
                    return Err(std::io::Error::other(
                        "durable task operation has conflicting task results",
                    ));
                }
                operation_result = Some(restored);
            }
        }
        Ok((found, operation_result))
    }

    fn restore_task_cas_record(&self, record: &LedgerRecord) -> std::io::Result<TaskRecord> {
        let task: TaskRecord = record
            .payload
            .get("task")
            .cloned()
            .ok_or_else(|| std::io::Error::other("durable task_cas record is missing task payload"))
            .and_then(|value| serde_json::from_value(value).map_err(std::io::Error::other))?;
        let outer_task_id = record
            .payload
            .get("task_id")
            .and_then(Value::as_str)
            .map(TaskId::from)
            .ok_or_else(|| std::io::Error::other("durable task_cas record is missing task_id"))?;
        if outer_task_id != task.task_id {
            return Err(std::io::Error::other(
                "durable task_cas outer task_id does not match task payload",
            ));
        }
        let operation_id = record
            .payload
            .get("operation_id")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(OperationId::from)
            .ok_or_else(|| std::io::Error::other("durable task_cas record has invalid operation_id"))?;
        let transition = record
            .payload
            .get("transition")
            .and_then(Value::as_str)
            .ok_or_else(|| std::io::Error::other("durable task_cas record is missing transition"))?;
        let mutation_digest = record
            .payload
            .get("mutation_digest")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| std::io::Error::other("durable task_cas record has invalid mutation_digest"))?;
        let expected_revision = record
            .payload
            .get("expected_revision")
            .and_then(Value::as_u64)
            .ok_or_else(|| std::io::Error::other("durable task_cas record is missing expected_revision"))?;
        let new_revision = record
            .payload
            .get("new_revision")
            .and_then(Value::as_u64)
            .ok_or_else(|| std::io::Error::other("durable task_cas record is missing new_revision"))?;
        if task.revision != new_revision || expected_revision.checked_add(1) != Some(new_revision) {
            return Err(std::io::Error::other("invalid durable task CAS revision pair"));
        }
        if !Arc::ptr_eq(
            &self.mutation.line_for(&record.run_id),
            &self.mutation.line_for(&task.run_id),
        ) {
            return Err(std::io::Error::other(
                "durable task_cas record belongs to a different mutation lineage",
            ));
        }
        validate_intrinsic_task_identity(&task)?;
        let existing = self.tasks.get(&task.task_id);
        if let Some(existing) = existing.as_ref()
            && !task_immutable_metadata_matches(existing, &task)
        {
            return Err(std::io::Error::other(
                "durable task_cas record changes immutable task identity",
            ));
        }
        let durable_expected = record
            .payload
            .get("expected_task")
            .cloned()
            .map(serde_json::from_value::<TaskRecord>)
            .transpose()
            .map_err(std::io::Error::other)?;
        let durable_mutation = record.payload.get("mutation").cloned();
        if durable_expected.is_some() != durable_mutation.is_some() {
            return Err(std::io::Error::other(
                "durable task_cas expected_task and mutation must appear together",
            ));
        }
        if let Some(expected) = durable_expected.as_ref() {
            if expected.revision != expected_revision
                || expected.task_id != task.task_id
                || !task_immutable_metadata_matches(expected, &task)
            {
                return Err(std::io::Error::other(
                    "durable task_cas expected_task does not match its revision and immutable identity",
                ));
            }
            validate_intrinsic_task_identity(expected)?;
            if let Some(current) = existing.as_ref()
                && current.revision == expected_revision
                && current != expected
            {
                return Err(std::io::Error::other(
                    "durable task_cas expected_task conflicts with the current projection",
                ));
            }
        }
        let prior = durable_expected
            .as_ref()
            .or_else(|| existing.as_ref().filter(|record| record.revision == expected_revision));
        let mutation = match durable_mutation {
            Some(mutation) => mutation,
            None => reconstruct_legacy_mutation(transition, mutation_digest, prior, &task)?,
        };
        if stable_digest_value(&mutation) != mutation_digest {
            return Err(std::io::Error::other(
                "durable task_cas mutation digest does not match mutation payload",
            ));
        }
        if let Some(expected) = durable_expected.as_ref() {
            let typed_mutation: TaskCasMutation = serde_json::from_value(mutation).map_err(std::io::Error::other)?;
            if typed_mutation.name() != transition {
                return Err(std::io::Error::other(
                    "durable task_cas transition does not match typed mutation",
                ));
            }
            let expected_task = typed_mutation.apply(expected, new_revision)?;
            if expected_task != task {
                return Err(std::io::Error::other(
                    "durable task_cas task does not match mutation applied to expected_task",
                ));
            }
        } else {
            validate_task_cas_mutation(transition, &mutation, prior, &task)?;
        }
        self.tasks.restore_cas(
            task.clone(),
            operation_id,
            mutation_digest.to_owned(),
            expected_revision,
            new_revision,
        )?;
        Ok(task)
    }

    fn restore_legacy_assignment_record(&self, record: &LedgerRecord) -> std::io::Result<()> {
        let task_id = record
            .payload
            .get("task_id")
            .and_then(Value::as_str)
            .map(TaskId::from)
            .ok_or_else(|| std::io::Error::other("legacy task assignment is missing task_id"))?;
        let owner_field = if record.record_type == "task_assigned" {
            "agent_id"
        } else {
            "to"
        };
        let owner = record
            .payload
            .get(owner_field)
            .and_then(Value::as_str)
            .map(AgentId::from)
            .ok_or_else(|| std::io::Error::other("legacy task assignment is missing owner"))?;
        let current = self.tasks.get(&task_id).ok_or_else(|| {
            std::io::Error::other(format!("legacy task assignment references unknown task: {task_id}"))
        })?;
        if !Arc::ptr_eq(
            &self.mutation.line_for(&record.run_id),
            &self.mutation.line_for(&current.run_id),
        ) {
            return Err(std::io::Error::other(
                "legacy task assignment belongs to a different mutation lineage",
            ));
        }
        if current.state == TaskState::Assigned && current.owner_agent_id.as_ref() == Some(&owner) {
            return Ok(());
        }
        if record.record_type == "task_assigned" {
            if current.state != TaskState::Queued || current.owner_agent_id.is_some() {
                return Err(std::io::Error::other(
                    "legacy task assignment cannot overwrite an existing owner",
                ));
            }
        } else {
            let from = record
                .payload
                .get("from")
                .and_then(Value::as_str)
                .map(AgentId::from)
                .ok_or_else(|| std::io::Error::other("legacy task handoff is missing prior owner"))?;
            if !matches!(current.state, TaskState::Assigned | TaskState::Running)
                || current.owner_agent_id.as_ref() != Some(&from)
            {
                return Err(std::io::Error::other(
                    "legacy task handoff does not match the current owner",
                ));
            }
        }
        self.tasks.restore_legacy_assign(&task_id, owner);
        Ok(())
    }
}

fn task_immutable_metadata_matches(existing: &TaskRecord, restored: &TaskRecord) -> bool {
    existing.run_id == restored.run_id
        && existing.task_id == restored.task_id
        && existing.task_key == restored.task_key
        && existing.team_id == restored.team_id
        && existing.workflow_id == restored.workflow_id
        && existing.node_id == restored.node_id
        && existing.role == restored.role
        && existing.depends_on == restored.depends_on
        && existing.content == restored.content
        && existing.expected_write_scope == restored.expected_write_scope
}

fn validate_intrinsic_task_identity(task: &TaskRecord) -> std::io::Result<()> {
    match (task.workflow_id.as_deref(), task.node_id.as_deref()) {
        (Some(workflow_id), Some(node_id)) => {
            let expected_task_id = TaskId::new(format!("workflow:{}:{node_id}", task.run_id));
            if task.task_id != expected_task_id {
                return Err(std::io::Error::other(
                    "durable workflow task identity does not match its Run and node",
                ));
            }
            if let Some(task_key) = task.task_key.as_deref()
                && task_key != format!("workflow:{workflow_id}:{node_id}")
            {
                return Err(std::io::Error::other(
                    "durable workflow task_key does not match workflow identity",
                ));
            }
        }
        (Some(_), None) | (None, None) => {}
        (None, Some(_)) => {
            return Err(std::io::Error::other(
                "durable task_cas record has incomplete workflow identity",
            ));
        }
    }
    Ok(())
}

fn mutation_body<'a>(transition: &str, mutation: &'a Value) -> std::io::Result<&'a serde_json::Map<String, Value>> {
    let object = mutation
        .as_object()
        .filter(|object| object.len() == 1)
        .ok_or_else(|| std::io::Error::other("durable task_cas mutation must contain exactly one transition"))?;
    object
        .get(transition)
        .and_then(Value::as_object)
        .ok_or_else(|| std::io::Error::other("durable task_cas transition does not match mutation payload"))
}

fn mutation_agent(body: &serde_json::Map<String, Value>, field: &str) -> std::io::Result<AgentId> {
    body.get(field)
        .and_then(Value::as_str)
        .map(AgentId::from)
        .ok_or_else(|| std::io::Error::other(format!("durable task_cas mutation is missing {field}")))
}

fn validate_task_cas_mutation(
    transition: &str,
    mutation: &Value,
    prior: Option<&TaskRecord>,
    task: &TaskRecord,
) -> std::io::Result<()> {
    let body = mutation_body(transition, mutation)?;
    match transition {
        "assign" => {
            let owner = mutation_agent(body, "owner")?;
            if task.state != TaskState::Assigned || task.owner_agent_id.as_ref() != Some(&owner) {
                return Err(std::io::Error::other(
                    "durable assign mutation does not match restored owner and state",
                ));
            }
            if prior.is_some_and(|record| record.state != TaskState::Queued || record.owner_agent_id.is_some()) {
                return Err(std::io::Error::other(
                    "durable assign mutation does not follow a Queued unowned task",
                ));
            }
        }
        "mark_running" => {
            let owner = mutation_agent(body, "owner")?;
            if task.state != TaskState::Running || task.owner_agent_id.as_ref() != Some(&owner) {
                return Err(std::io::Error::other(
                    "durable mark_running mutation does not match restored owner and state",
                ));
            }
            if prior.is_some_and(|record| {
                record.state != TaskState::Assigned || record.owner_agent_id.as_ref() != Some(&owner)
            }) {
                return Err(std::io::Error::other(
                    "durable mark_running mutation does not follow the Assigned owner",
                ));
            }
        }
        "handoff" => {
            let from = mutation_agent(body, "from")?;
            let to = mutation_agent(body, "to")?;
            if task.state != TaskState::Assigned || task.owner_agent_id.as_ref() != Some(&to) {
                return Err(std::io::Error::other(
                    "durable handoff mutation does not match restored owner and state",
                ));
            }
            if prior.is_some_and(|record| {
                !matches!(record.state, TaskState::Assigned | TaskState::Running)
                    || record.owner_agent_id.as_ref() != Some(&from)
            }) {
                return Err(std::io::Error::other(
                    "durable handoff mutation does not follow the current owner",
                ));
            }
        }
        "settle" => {
            let owner = mutation_agent(body, "owner")?;
            let state: TaskState = body
                .get("state")
                .cloned()
                .ok_or_else(|| std::io::Error::other("durable settle mutation is missing state"))
                .and_then(|value| serde_json::from_value(value).map_err(std::io::Error::other))?;
            let outcome_ref: Option<String> = body
                .get("outcome_ref")
                .cloned()
                .map(serde_json::from_value)
                .transpose()
                .map_err(std::io::Error::other)?
                .flatten();
            let failure_class: Option<TaskFailureClass> = body
                .get("failure_class")
                .cloned()
                .map(serde_json::from_value)
                .transpose()
                .map_err(std::io::Error::other)?
                .flatten();
            if task.owner_agent_id.as_ref() != Some(&owner)
                || task.state != state
                || task.outcome_ref != outcome_ref
                || task.failure_class != failure_class
            {
                return Err(std::io::Error::other(
                    "durable settle mutation does not match restored task outcome",
                ));
            }
            if prior.is_some_and(|record| {
                !matches!(record.state, TaskState::Assigned | TaskState::Running)
                    || record.owner_agent_id.as_ref() != Some(&owner)
            }) {
                return Err(std::io::Error::other(
                    "durable settle mutation does not follow the current owner",
                ));
            }
        }
        "supervisor_retry" => {
            let reason: Option<String> = body
                .get("reason")
                .cloned()
                .map(serde_json::from_value)
                .transpose()
                .map_err(std::io::Error::other)?
                .flatten();
            if task.workflow_id.is_some()
                || task.state != TaskState::Queued
                || task.owner_agent_id.is_some()
                || task.outcome_ref != reason
                || task.failure_class.is_some()
            {
                return Err(std::io::Error::other(
                    "durable supervisor_retry mutation does not match restored direct task state",
                ));
            }
            if prior.is_some_and(|record| {
                record.workflow_id.is_some()
                    || record.state != TaskState::Failed
                    || record.owner_agent_id.is_none()
                    || record.failure_class != Some(TaskFailureClass::Retryable)
            }) {
                return Err(std::io::Error::other(
                    "durable supervisor_retry mutation does not follow a direct Retryable failure",
                ));
            }
        }
        "workflow_state" => {
            let state: TaskState = body
                .get("state")
                .cloned()
                .ok_or_else(|| std::io::Error::other("durable workflow_state mutation is missing state"))
                .and_then(|value| serde_json::from_value(value).map_err(std::io::Error::other))?;
            let clear_owner = body
                .get("clear_owner")
                .and_then(Value::as_bool)
                .ok_or_else(|| std::io::Error::other("durable workflow_state mutation is missing clear_owner"))?;
            let outcome_ref: Option<String> = body
                .get("outcome_ref")
                .cloned()
                .map(serde_json::from_value)
                .transpose()
                .map_err(std::io::Error::other)?
                .flatten();
            let failure_class: Option<TaskFailureClass> = body
                .get("failure_class")
                .cloned()
                .map(serde_json::from_value)
                .transpose()
                .map_err(std::io::Error::other)?
                .flatten();
            if task.workflow_id.is_none()
                || task.state != state
                || task.outcome_ref != outcome_ref
                || task.failure_class != failure_class
                || (clear_owner && task.owner_agent_id.is_some())
            {
                return Err(std::io::Error::other(
                    "durable workflow_state mutation does not match restored task state",
                ));
            }
            if let Some(prior) = prior {
                let allowed = matches!(
                    (prior.state, state),
                    (
                        TaskState::Queued,
                        TaskState::Completed | TaskState::Failed | TaskState::Skipped | TaskState::Cancelled
                    ) | (
                        TaskState::Assigned | TaskState::Running,
                        TaskState::Completed | TaskState::Failed | TaskState::Cancelled
                    ) | (TaskState::Failed, TaskState::Queued)
                );
                if !allowed || (prior.state == TaskState::Failed && !clear_owner) {
                    return Err(std::io::Error::other(
                        "durable workflow_state mutation contains an illegal transition",
                    ));
                }
                if !clear_owner && task.owner_agent_id != prior.owner_agent_id {
                    return Err(std::io::Error::other(
                        "durable workflow_state mutation changes owner without clear_owner",
                    ));
                }
            }
        }
        _ => {
            return Err(std::io::Error::other(
                "durable task_cas record has an unknown transition",
            ));
        }
    }
    Ok(())
}

fn reconstruct_legacy_mutation(
    transition: &str,
    mutation_digest: &str,
    prior: Option<&TaskRecord>,
    task: &TaskRecord,
) -> std::io::Result<Value> {
    let mutation = match transition {
        "assign" => json!({"assign": {"owner": task.owner_agent_id}}),
        "mark_running" => json!({"mark_running": {"owner": task.owner_agent_id}}),
        "handoff" => {
            let prior = prior
                .and_then(|record| record.owner_agent_id.clone())
                .ok_or_else(|| std::io::Error::other("legacy handoff CAS requires its prior owner projection"))?;
            json!({"handoff": {"from": prior, "to": task.owner_agent_id}})
        }
        "settle" => json!({
            "settle": {
                "owner": task.owner_agent_id,
                "state": task.state,
                "outcome_ref": task.outcome_ref,
                "failure_class": task.failure_class,
            }
        }),
        "workflow_state" => {
            let candidates = [false, true].map(|clear_owner| {
                json!({
                    "workflow_state": {
                        "state": task.state,
                        "clear_owner": clear_owner,
                        "outcome_ref": task.outcome_ref,
                        "failure_class": task.failure_class,
                    }
                })
            });
            candidates
                .into_iter()
                .find(|candidate| stable_digest_value(candidate) == mutation_digest)
                .ok_or_else(|| std::io::Error::other("legacy workflow_state CAS mutation digest is invalid"))?
        }
        _ => {
            return Err(std::io::Error::other(
                "legacy task_cas record has an unknown transition",
            ));
        }
    };
    Ok(mutation)
}
