use std::collections::BTreeSet;
use std::sync::Arc;

use serde_json::json;
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::{AgentId, OperationId, RunId, TaskId, TeamId};
use solaris_types::runtime::{TaskFailureClass, TaskRecord, TaskState};
use solaris_types::workflow::CollaborationStrategy;

use super::CollaborationRuntime;
use crate::execution_context::stable_digest_value;
use crate::runtime_ledger::LedgerRecord;
use crate::task_registry::TaskCasMutation;

pub(crate) struct TaskSettlement {
    pub state: TaskState,
    pub outcome_ref: Option<String>,
    pub failure_class: Option<TaskFailureClass>,
}

impl<T> CollaborationRuntime<T> {
    pub fn scoped_team_task_id(team_id: &TeamId, task_id: &str) -> TaskId {
        let prefix = format!("team:{team_id}:");
        if task_id.starts_with(&prefix) {
            TaskId::from(task_id)
        } else {
            TaskId::new(format!("{prefix}{task_id}"))
        }
    }

    pub fn create_collaboration_task(
        &self,
        run_id: &RunId,
        team_id: &TeamId,
        created_by: &AgentId,
        mut task: TaskRecord,
    ) -> std::io::Result<bool> {
        let team = self
            .teams
            .get(team_id)
            .ok_or_else(|| std::io::Error::other(format!("unknown team: {team_id}")))?;
        if &team.run_id != run_id || !team.members.contains(created_by) {
            return Err(std::io::Error::other(
                "collaboration task creator must belong to the requested team/run",
            ));
        }
        if team.strategy == CollaborationStrategy::Supervisor && team.coordinator.as_ref() != Some(created_by) {
            return Err(std::io::Error::other(
                "only the Supervisor coordinator may create Team tasks",
            ));
        }
        if task.team_id.as_ref() != Some(team_id) {
            return Err(std::io::Error::other(
                "collaboration task must carry the requested Team identity",
            ));
        }
        let owner = task.owner_agent_id.clone();
        if let Some(owner) = owner.as_ref()
            && !team.members.contains(owner)
        {
            return Err(std::io::Error::other("collaboration task owner is not a team member"));
        }
        task.state = if owner.is_some() {
            TaskState::Assigned
        } else {
            TaskState::Queued
        };
        task.revision = 0;
        task.outcome_ref = None;
        task.failure_class = None;
        self.register_runtime_task(run_id, task)
    }

    pub fn register_runtime_tasks_admitted(
        &self,
        run_id: &RunId,
        tasks: Vec<TaskRecord>,
        max_tasks: usize,
    ) -> std::io::Result<Vec<bool>> {
        if tasks.iter().any(|task| task.run_id != *run_id) {
            return Err(std::io::Error::other("runtime task belongs to a different run"));
        }
        let line = self.mutation.line_for(run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        let durable_ids = self
            .ledger
            .records_for_run(run_id)?
            .into_iter()
            .filter(|record| record.record_type == "task_created")
            .filter_map(|record| serde_json::from_value::<TaskRecord>(record.payload).ok())
            .map(|task| task.task_id)
            .collect::<BTreeSet<_>>();
        let new_ids = tasks
            .iter()
            .map(|task| task.task_id.clone())
            .filter(|task_id| !durable_ids.contains(task_id))
            .collect::<BTreeSet<_>>();
        let projected = durable_ids.len().saturating_add(new_ids.len());
        if projected > max_tasks {
            return Err(std::io::Error::other(format!(
                "Run accepts at most {max_tasks} collaboration tasks"
            )));
        }
        tasks
            .into_iter()
            .map(|task| self.register_runtime_task_locked(run_id, task))
            .collect()
    }

    pub fn register_runtime_task(&self, run_id: &RunId, task: TaskRecord) -> std::io::Result<bool> {
        let line = self.mutation.line_for(run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        self.register_runtime_task_locked(run_id, task)
    }

    fn register_runtime_task_locked(&self, run_id: &RunId, task: TaskRecord) -> std::io::Result<bool> {
        if task.run_id != *run_id {
            return Err(std::io::Error::other("runtime task belongs to a different run"));
        }
        if task.task_id.as_str().trim().is_empty() {
            return Err(std::io::Error::other("runtime task id cannot be empty"));
        }
        if let Some(owner) = task.owner_agent_id.as_ref() {
            let agent = self
                .agents
                .get(owner)
                .ok_or_else(|| std::io::Error::other(format!("unknown task owner: {owner}")))?;
            if !Arc::ptr_eq(&self.mutation.line_for(&agent.run_id), &self.mutation.line_for(run_id)) {
                return Err(std::io::Error::other(
                    "runtime task owner belongs to a different run lineage",
                ));
            }
        }
        let durable_created = self
            .ledger
            .records_for_run(run_id)?
            .into_iter()
            .filter(|record| record.record_type == "task_created")
            .filter_map(|record| serde_json::from_value::<TaskRecord>(record.payload).ok())
            .find(|record| record.task_id == task.task_id);
        if let Some(created) = durable_created {
            if created != task {
                return Err(std::io::Error::other(format!(
                    "runtime task {} was durably created with different metadata",
                    task.task_id
                )));
            }
            if self.tasks.get(&task.task_id).is_none() {
                self.tasks.upsert(created);
            }
            return Ok(false);
        }
        if let Some(existing) = self.tasks.get(&task.task_id) {
            return if existing == task {
                Ok(false)
            } else {
                Err(std::io::Error::other(format!(
                    "runtime task {} already exists with different metadata",
                    task.task_id
                )))
            };
        }
        let owner = task.owner_agent_id.clone();
        let record = self.ledger.append(
            run_id,
            DurabilityClass::SyncCritical,
            "task_created",
            serde_json::to_value(&task).map_err(std::io::Error::other)?,
        )?;
        self.tasks.upsert(task.clone());
        self.emit_durable_event(
            &record,
            owner,
            "task_created",
            serde_json::to_value(&task).unwrap_or_default(),
        );
        Ok(true)
    }

    pub(crate) fn settle_collaboration_task(
        &self,
        run_id: &RunId,
        task_id: &TaskId,
        agent_id: &AgentId,
        expected_revision: u64,
        operation_id: &OperationId,
        settlement: TaskSettlement,
    ) -> std::io::Result<u64> {
        if !matches!(
            settlement.state,
            TaskState::Completed | TaskState::Failed | TaskState::Cancelled
        ) {
            return Err(std::io::Error::other("collaboration task settlement must be terminal"));
        }
        let line = self.mutation.line_for(run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        let task = self
            .tasks
            .get(task_id)
            .ok_or_else(|| std::io::Error::other(format!("unknown task: {task_id}")))?;
        if task.run_id != *run_id || task.owner_agent_id.as_ref() != Some(agent_id) {
            return Err(std::io::Error::other(
                "only the assigned task owner may settle a collaboration task",
            ));
        }
        if let Some(team_id) = task.team_id.as_ref() {
            let team = self
                .teams
                .get(team_id)
                .ok_or_else(|| std::io::Error::other(format!("unknown task team: {team_id}")))?;
            if !team.members.contains(agent_id) {
                return Err(std::io::Error::other(
                    "collaboration task owner is no longer a member of its Team",
                ));
            }
        }
        let mutation = TaskCasMutation::Settle {
            owner: agent_id.clone(),
            state: settlement.state,
            outcome_ref: settlement.outcome_ref,
            failure_class: settlement.failure_class,
        };
        let updated = self.apply_task_cas_locked(
            run_id,
            task_id,
            expected_revision,
            operation_id,
            mutation,
            Some(agent_id.clone()),
        )?;
        Ok(updated.revision)
    }

    /// Mark a not-yet-assigned collaboration task as skipped because one of
    /// its prerequisites failed. The transition is durable and CAS-protected,
    /// so a resumed Run cannot accidentally execute the blocked task.
    pub(crate) fn skip_collaboration_task(
        &self,
        run_id: &RunId,
        task_id: &TaskId,
        expected_revision: u64,
        operation_id: &OperationId,
        reason: impl Into<String>,
    ) -> std::io::Result<u64> {
        let line = self.mutation.line_for(run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        let task = self
            .tasks
            .get(task_id)
            .ok_or_else(|| std::io::Error::other(format!("unknown task: {task_id}")))?;
        if task.run_id != *run_id {
            return Err(std::io::Error::other("collaboration task belongs to a different run"));
        }
        let updated = self.apply_task_cas_locked(
            run_id,
            task_id,
            expected_revision,
            operation_id,
            TaskCasMutation::Skip {
                reason: Some(reason.into()),
            },
            None,
        )?;
        Ok(updated.revision)
    }

    /// Requeue a failed Supervisor task for one explicit retry. The failed
    /// attempt remains in the ledger; only the task projection is moved back
    /// to `Queued`, with its owner cleared, so a new stable operation can be
    /// assigned without losing the prior outcome.
    pub(crate) fn requeue_supervisor_task(
        &self,
        run_id: &RunId,
        task_id: &TaskId,
        coordinator: &AgentId,
        expected_revision: u64,
        operation_id: &OperationId,
        reason: impl Into<String>,
    ) -> std::io::Result<u64> {
        let line = self.mutation.line_for(run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        let task = self
            .tasks
            .get(task_id)
            .ok_or_else(|| std::io::Error::other(format!("unknown task: {task_id}")))?;
        if task.run_id != *run_id
            || task.state != TaskState::Failed
            || task.failure_class != Some(TaskFailureClass::Retryable)
        {
            return Err(std::io::Error::other(
                "Supervisor retry requires a failed task with a Retryable failure",
            ));
        }
        let team_id = task
            .team_id
            .as_ref()
            .ok_or_else(|| std::io::Error::other("Supervisor retry task has no Team"))?;
        let team = self
            .teams
            .get(team_id)
            .ok_or_else(|| std::io::Error::other(format!("unknown task team: {team_id}")))?;
        if team.strategy != CollaborationStrategy::Supervisor || team.coordinator.as_ref() != Some(coordinator) {
            return Err(std::io::Error::other(
                "only the Supervisor coordinator may requeue this task",
            ));
        }
        let updated = self.apply_task_cas_locked(
            run_id,
            task_id,
            expected_revision,
            operation_id,
            TaskCasMutation::SupervisorRetry {
                reason: Some(reason.into()),
            },
            Some(coordinator.clone()),
        )?;
        Ok(updated.revision)
    }

    pub(crate) fn cancel_supervisor_workflow_task(
        &self,
        run_id: &RunId,
        task_id: &TaskId,
        coordinator: &AgentId,
        expected_revision: u64,
        operation_id: &OperationId,
    ) -> std::io::Result<u64> {
        let line = self.mutation.line_for(run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        let task = self
            .tasks
            .get(task_id)
            .ok_or_else(|| std::io::Error::other(format!("unknown task: {task_id}")))?;
        let team_id = task
            .team_id
            .as_ref()
            .ok_or_else(|| std::io::Error::other("Supervisor workflow task has no Team"))?;
        let team = self
            .teams
            .get(team_id)
            .ok_or_else(|| std::io::Error::other(format!("unknown task team: {team_id}")))?;
        if task.run_id != *run_id
            || task.workflow_id.is_none()
            || team.strategy != CollaborationStrategy::Supervisor
            || team.coordinator.as_ref() != Some(coordinator)
        {
            return Err(std::io::Error::other(
                "only the Supervisor coordinator may cancel its workflow Task",
            ));
        }
        let updated = self.apply_task_cas_locked(
            run_id,
            task_id,
            expected_revision,
            operation_id,
            TaskCasMutation::WorkflowState {
                state: TaskState::Cancelled,
                clear_owner: false,
                outcome_ref: Some(format!("supervisor-cancel:{operation_id}")),
                failure_class: None,
            },
            Some(coordinator.clone()),
        )?;
        Ok(updated.revision)
    }

    pub fn acknowledge_message(&self, run_id: &RunId, agent_id: &AgentId, message_id: &str) -> std::io::Result<bool> {
        let agent = self
            .agents
            .get(agent_id)
            .ok_or_else(|| std::io::Error::other(format!("unknown message agent: {agent_id}")))?;
        if &agent.run_id != run_id {
            return Err(std::io::Error::other(
                "message acknowledgement agent does not belong to the requested run",
            ));
        }
        if self
            .messages
            .inbox(agent_id)
            .iter()
            .find(|message| message.message_id == message_id)
            .is_some_and(|message| &message.run_id != run_id || &message.to != agent_id)
        {
            return Err(std::io::Error::other(
                "message acknowledgement does not belong to the requested run/agent",
            ));
        }
        let line = self.mutation.line_for(run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        let (acknowledged, record) = self
            .messages
            .acknowledge_within_mutation(run_id, agent_id, message_id)?;
        if let Some(record) = record {
            self.emit_durable_event(
                &record,
                Some(agent_id.clone()),
                "agent_message_acknowledged",
                json!({"agent_id": agent_id, "message_id": message_id}),
            );
        }
        Ok(acknowledged)
    }

    pub fn handoff_task(
        &self,
        run_id: &RunId,
        task_id: &TaskId,
        from: &AgentId,
        to: AgentId,
        expected_revision: u64,
        operation_id: &OperationId,
    ) -> std::io::Result<u64> {
        for agent_id in [from, &to] {
            let agent = self
                .agents
                .get(agent_id)
                .ok_or_else(|| std::io::Error::other(format!("unknown handoff agent: {agent_id}")))?;
            if !Arc::ptr_eq(&self.mutation.line_for(&agent.run_id), &self.mutation.line_for(run_id)) {
                return Err(std::io::Error::other(
                    "task handoff agents must belong to the requested run",
                ));
            }
        }
        let line = self.mutation.line_for(run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        let task = self
            .tasks
            .get(task_id)
            .ok_or_else(|| std::io::Error::other(format!("unknown task: {task_id}")))?;
        let task_run_id = task.run_id.clone();
        if !Arc::ptr_eq(&line, &self.mutation.line_for(&task_run_id)) {
            return Err(std::io::Error::other(
                "task does not belong to the requested run lineage",
            ));
        }
        let durable_from = if let Some(team_id) = task.team_id.as_ref() {
            let team = self
                .teams
                .get(team_id)
                .ok_or_else(|| std::io::Error::other(format!("unknown task team: {team_id}")))?;
            if !team.members.contains(from) || !team.members.contains(&to) {
                return Err(std::io::Error::other(
                    "task handoff agents must belong to the task Team",
                ));
            }
            if team.strategy == CollaborationStrategy::Supervisor {
                if team.coordinator.as_ref() != Some(from) {
                    return Err(std::io::Error::other(
                        "only the Supervisor coordinator may reassign Team tasks",
                    ));
                }
                task.owner_agent_id
                    .clone()
                    .ok_or_else(|| std::io::Error::other("Supervisor task has no current owner"))?
            } else {
                from.clone()
            }
        } else {
            from.clone()
        };
        let mutation = TaskCasMutation::Handoff {
            from: durable_from,
            to: to.clone(),
        };
        let updated = self.apply_task_cas_locked(
            &task_run_id,
            task_id,
            expected_revision,
            operation_id,
            mutation,
            Some(to),
        )?;
        Ok(updated.revision)
    }

    pub fn assign_task_owner(
        &self,
        run_id: &RunId,
        task_id: &TaskId,
        agent_id: &AgentId,
        expected_revision: u64,
        operation_id: &OperationId,
    ) -> std::io::Result<u64> {
        self.validate_task_agent(run_id, task_id, agent_id)?;
        let line = self.mutation.line_for(run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        let updated = self.apply_task_cas_locked(
            run_id,
            task_id,
            expected_revision,
            operation_id,
            TaskCasMutation::Assign {
                owner: agent_id.clone(),
            },
            Some(agent_id.clone()),
        )?;
        Ok(updated.revision)
    }

    pub fn mark_task_running(
        &self,
        run_id: &RunId,
        task_id: &TaskId,
        agent_id: &AgentId,
        expected_revision: u64,
        operation_id: &OperationId,
    ) -> std::io::Result<u64> {
        self.validate_task_agent(run_id, task_id, agent_id)?;
        let line = self.mutation.line_for(run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        let updated = self.apply_task_cas_locked(
            run_id,
            task_id,
            expected_revision,
            operation_id,
            TaskCasMutation::MarkRunning {
                owner: agent_id.clone(),
            },
            Some(agent_id.clone()),
        )?;
        Ok(updated.revision)
    }

    fn validate_task_agent(&self, run_id: &RunId, task_id: &TaskId, agent_id: &AgentId) -> std::io::Result<()> {
        let task = self
            .tasks
            .get(task_id)
            .ok_or_else(|| std::io::Error::other(format!("unknown task: {task_id}")))?;
        let agent = self
            .agents
            .get(agent_id)
            .ok_or_else(|| std::io::Error::other(format!("unknown Agent: {agent_id}")))?;
        let line = self.mutation.line_for(run_id);
        if !Arc::ptr_eq(&line, &self.mutation.line_for(&task.run_id))
            || !Arc::ptr_eq(&line, &self.mutation.line_for(&agent.run_id))
        {
            return Err(std::io::Error::other("task and Agent must belong to the requested run"));
        }
        Ok(())
    }

    fn apply_task_cas_locked(
        &self,
        run_id: &RunId,
        task_id: &TaskId,
        expected_revision: u64,
        operation_id: &OperationId,
        mutation: TaskCasMutation,
        event_agent_id: Option<AgentId>,
    ) -> std::io::Result<TaskRecord> {
        let transition = mutation.name();
        let mutation_digest = stable_digest_value(&serde_json::to_value(&mutation).map_err(std::io::Error::other)?);
        let expected_task = self
            .tasks
            .get(task_id)
            .ok_or_else(|| std::io::Error::other(format!("unknown task: {task_id}")))?;
        let mut durable: Option<LedgerRecord> = None;
        let updated = self.tasks.cas_durable(
            task_id,
            expected_revision,
            operation_id,
            &mutation_digest,
            &mutation,
            |next| {
                let payload = json!({
                    "task_id": task_id,
                    "operation_id": operation_id,
                    "transition": transition,
                    "mutation": &mutation,
                    "mutation_digest": mutation_digest,
                    "expected_revision": expected_revision,
                    "new_revision": next.revision,
                    "expected_task": expected_task,
                    "task": next,
                });
                if let Some(existing) = self.ledger.records_for_run(run_id)?.into_iter().find(|record| {
                    record.record_type == "task_cas"
                        && (record.payload.get("operation_id").and_then(serde_json::Value::as_str)
                            == Some(operation_id.as_str())
                            || (record.payload.get("task_id").and_then(serde_json::Value::as_str)
                                == Some(task_id.as_str())
                                && record
                                    .payload
                                    .get("expected_revision")
                                    .and_then(serde_json::Value::as_u64)
                                    == Some(expected_revision)))
                }) {
                    if existing.payload != payload {
                        return Err(std::io::Error::other(format!(
                            "task revision {expected_revision} is already bound to a different mutation"
                        )));
                    }
                    return Ok(());
                }
                tracing::debug!(
                    task_id = %task_id,
                    transition,
                    expected_revision,
                    new_revision = next.revision,
                    "persisting task CAS"
                );
                durable = Some(
                    self.ledger
                        .append(run_id, DurabilityClass::SyncCritical, "task_cas", payload)?,
                );
                Ok(())
            },
        )?;
        if let Some(record) = durable.as_ref() {
            self.emit_durable_event(
                record,
                event_agent_id,
                "task_cas",
                json!({
                    "task_id": task_id,
                    "operation_id": operation_id,
                    "transition": transition,
                    "mutation_digest": mutation_digest,
                    "expected_revision": expected_revision,
                    "new_revision": updated.revision,
                }),
            );
        }
        Ok(updated)
    }
}
