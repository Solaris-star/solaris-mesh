use std::collections::HashMap;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};
use solaris_types::identity::{AgentId, OperationId, TaskId};
use solaris_types::runtime::{TaskFailureClass, TaskRecord, TaskState};

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TaskCasMutation {
    Assign {
        owner: AgentId,
    },
    MarkRunning {
        owner: AgentId,
    },
    Handoff {
        from: AgentId,
        to: AgentId,
    },
    Settle {
        owner: AgentId,
        state: TaskState,
        outcome_ref: Option<String>,
        failure_class: Option<TaskFailureClass>,
    },
    Skip {
        reason: Option<String>,
    },
    SupervisorRetry {
        reason: Option<String>,
    },
    WorkflowState {
        state: TaskState,
        clear_owner: bool,
        outcome_ref: Option<String>,
        failure_class: Option<TaskFailureClass>,
    },
}

impl TaskCasMutation {
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Self::Assign { .. } => "assign",
            Self::MarkRunning { .. } => "mark_running",
            Self::Handoff { .. } => "handoff",
            Self::Settle { .. } => "settle",
            Self::Skip { .. } => "skip",
            Self::SupervisorRetry { .. } => "supervisor_retry",
            Self::WorkflowState { .. } => "workflow_state",
        }
    }

    pub(super) fn apply(&self, current: &TaskRecord, new_revision: u64) -> std::io::Result<TaskRecord> {
        let mut next = current.clone();
        match self {
            Self::Assign { owner } => {
                if current.state != TaskState::Queued || current.owner_agent_id.is_some() {
                    return Err(std::io::Error::other(
                        "task assignment requires a Queued task without an owner",
                    ));
                }
                next.owner_agent_id = Some(owner.clone());
                next.state = TaskState::Assigned;
            }
            Self::MarkRunning { owner } => {
                if current.state != TaskState::Assigned || current.owner_agent_id.as_ref() != Some(owner) {
                    return Err(std::io::Error::other("mark_running requires the Assigned task owner"));
                }
                next.state = TaskState::Running;
            }
            Self::Handoff { from, to } => {
                if !matches!(current.state, TaskState::Assigned | TaskState::Running)
                    || current.owner_agent_id.as_ref() != Some(from)
                {
                    return Err(std::io::Error::other(
                        "task handoff requires the current Assigned or Running owner",
                    ));
                }
                next.owner_agent_id = Some(to.clone());
                next.state = TaskState::Assigned;
            }
            Self::Settle {
                owner,
                state,
                outcome_ref,
                failure_class,
            } => {
                if !matches!(state, TaskState::Completed | TaskState::Failed | TaskState::Cancelled) {
                    return Err(std::io::Error::other("task settlement state must be terminal"));
                }
                if !matches!(current.state, TaskState::Assigned | TaskState::Running)
                    || current.owner_agent_id.as_ref() != Some(owner)
                {
                    return Err(std::io::Error::other(
                        "task settlement requires the current Assigned or Running owner",
                    ));
                }
                next.state = *state;
                next.outcome_ref.clone_from(outcome_ref);
                next.failure_class = *failure_class;
            }
            Self::Skip { reason } => {
                if current.state != TaskState::Queued || current.owner_agent_id.is_some() {
                    return Err(std::io::Error::other(
                        "task skip requires a Queued task without an owner",
                    ));
                }
                next.state = TaskState::Skipped;
                next.outcome_ref.clone_from(reason);
                next.failure_class = Some(TaskFailureClass::NonRetryable);
            }
            Self::SupervisorRetry { reason } => {
                if current.workflow_id.is_some()
                    || current.state != TaskState::Failed
                    || current.failure_class != Some(TaskFailureClass::Retryable)
                    || current.owner_agent_id.is_none()
                {
                    return Err(std::io::Error::other(
                        "Supervisor retry requires a direct failed task with a Retryable failure and an owner",
                    ));
                }
                next.state = TaskState::Queued;
                next.owner_agent_id = None;
                next.outcome_ref.clone_from(reason);
                next.failure_class = None;
            }
            Self::WorkflowState {
                state,
                clear_owner,
                outcome_ref,
                failure_class,
            } => {
                if current.workflow_id.is_none() {
                    return Err(std::io::Error::other(
                        "workflow state projection requires a workflow task",
                    ));
                }
                let allowed = matches!(
                    (current.state, *state),
                    (
                        TaskState::Queued,
                        TaskState::Completed | TaskState::Failed | TaskState::Skipped | TaskState::Cancelled
                    ) | (
                        TaskState::Assigned | TaskState::Running,
                        TaskState::Completed | TaskState::Failed | TaskState::Cancelled
                    ) | (TaskState::Failed, TaskState::Queued)
                );
                if !allowed || (current.state == TaskState::Failed && !clear_owner) {
                    return Err(std::io::Error::other(format!(
                        "illegal workflow task transition: {:?} -> {:?}",
                        current.state, state
                    )));
                }
                next.state = *state;
                if *clear_owner {
                    next.owner_agent_id = None;
                }
                next.outcome_ref.clone_from(outcome_ref);
                next.failure_class = *failure_class;
            }
        }
        next.revision = new_revision;
        Ok(next)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AppliedTaskCas {
    task_id: TaskId,
    mutation_digest: String,
    expected_revision: u64,
    new_revision: u64,
    result: TaskRecord,
}

#[derive(Default)]
pub struct TaskRegistry {
    state: RwLock<TaskRegistryState>,
}

#[derive(Default)]
struct TaskRegistryState {
    tasks: HashMap<TaskId, TaskRecord>,
    applied: HashMap<OperationId, AppliedTaskCas>,
}

impl TaskRegistry {
    pub(crate) fn upsert(&self, record: TaskRecord) {
        self.state
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .tasks
            .insert(record.task_id.clone(), record);
    }

    pub fn get(&self, task_id: &TaskId) -> Option<TaskRecord> {
        self.state
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .tasks
            .get(task_id)
            .cloned()
    }

    pub(crate) fn cas_durable(
        &self,
        task_id: &TaskId,
        expected_revision: u64,
        operation_id: &OperationId,
        mutation_digest: &str,
        mutation: &TaskCasMutation,
        persist: impl FnOnce(&TaskRecord) -> std::io::Result<()>,
    ) -> std::io::Result<TaskRecord> {
        let mut state = self.state.write().unwrap_or_else(|error| error.into_inner());
        if let Some(applied) = state.applied.get(operation_id) {
            return if applied.task_id == *task_id
                && applied.mutation_digest == mutation_digest
                && applied.expected_revision == expected_revision
                && expected_revision.checked_add(1) == Some(applied.new_revision)
            {
                Ok(applied.result.clone())
            } else {
                Err(std::io::Error::other(format!(
                    "task operation {operation_id} is already bound to a different mutation"
                )))
            };
        }
        let current = state
            .tasks
            .get(task_id)
            .ok_or_else(|| std::io::Error::other(format!("unknown task: {task_id}")))?;
        let new_revision = expected_revision
            .checked_add(1)
            .ok_or_else(|| std::io::Error::other("task revision overflow"))?;
        if current.revision != expected_revision {
            return Err(std::io::Error::other(format!(
                "stale task revision: expected {expected_revision}, found {}",
                current.revision
            )));
        }
        let next = mutation.apply(current, new_revision)?;
        persist(&next)?;
        state.tasks.insert(task_id.clone(), next.clone());
        state.applied.insert(
            operation_id.clone(),
            AppliedTaskCas {
                task_id: task_id.clone(),
                mutation_digest: mutation_digest.to_owned(),
                expected_revision,
                new_revision,
                result: next.clone(),
            },
        );
        Ok(next)
    }

    pub(crate) fn restore_cas(
        &self,
        record: TaskRecord,
        operation_id: OperationId,
        mutation_digest: String,
        expected_revision: u64,
        new_revision: u64,
    ) -> std::io::Result<()> {
        if record.revision != new_revision || expected_revision.checked_add(1) != Some(new_revision) {
            return Err(std::io::Error::other("invalid durable task CAS revision pair"));
        }
        let mut state = self.state.write().unwrap_or_else(|error| error.into_inner());
        let applied = AppliedTaskCas {
            task_id: record.task_id.clone(),
            mutation_digest,
            expected_revision,
            new_revision,
            result: record.clone(),
        };
        if let Some(existing) = state.applied.get(&operation_id) {
            if existing != &applied {
                return Err(std::io::Error::other(format!(
                    "task operation {operation_id} has conflicting durable results"
                )));
            }
        } else {
            state.applied.insert(operation_id, applied);
        }
        match state.tasks.get(&record.task_id) {
            Some(current) if current.revision > record.revision => Ok(()),
            Some(current) if current.revision == record.revision && current == &record => Ok(()),
            Some(current) if current.revision == record.revision => Err(std::io::Error::other(format!(
                "conflicting task projection at revision {}",
                record.revision
            ))),
            _ => {
                state.tasks.insert(record.task_id.clone(), record);
                Ok(())
            }
        }
    }

    pub(crate) fn restore_legacy_assign(&self, task_id: &TaskId, owner: AgentId) {
        let mut state = self.state.write().unwrap_or_else(|error| error.into_inner());
        if let Some(record) = state.tasks.get_mut(task_id) {
            record.owner_agent_id = Some(owner);
            record.state = TaskState::Assigned;
        }
    }

    pub(crate) fn restore_legacy_state(&self, task_id: &TaskId, task_state: TaskState) {
        let mut state = self.state.write().unwrap_or_else(|error| error.into_inner());
        if let Some(record) = state.tasks.get_mut(task_id) {
            record.state = task_state;
        }
    }

    pub fn snapshot(&self) -> Vec<TaskRecord> {
        self.state
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .tasks
            .values()
            .cloned()
            .collect()
    }
}

#[cfg(test)]
#[path = "task_registry_test.rs"]
mod task_registry_test;
