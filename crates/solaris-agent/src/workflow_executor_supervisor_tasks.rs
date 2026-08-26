use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Component, Path};

use solaris_types::identity::{OperationId, TaskId};
use solaris_types::permission::{ExecutionBoundary, PermissionMode};
use solaris_types::runtime::{TaskFailureClass, TaskState};
use solaris_types::spawner::{AgentHandle, AgentOutcomeStatus, AgentSpawnService, OutcomeBlobRef, SubAgentResult};
use solaris_types::workflow::CollaborationRuntimeConfig;

use crate::workflow_controller::WorkflowNodeError;

use super::records::DurableSupervisorWorkerOutcome;
use super::*;

impl AgentWorkflowExecutor {
    pub(super) fn find_supervisor_worker_handle(
        &self,
        task_id: &TaskId,
        operation_id: &OperationId,
    ) -> Result<Option<AgentHandle>, WorkflowNodeError> {
        let mut found = None;
        for record in self
            .spawner
            .lifecycle_runtime()
            .ledger()
            .records_for_run(self.spawner.run_id())
            .map_err(|error| WorkflowNodeError::reconciliation_required(error.to_string()))?
        {
            if record.record_type != "agent_handle_issued"
                || record.payload.get("task_id").and_then(Value::as_str) != Some(task_id.as_str())
                || record.payload.get("operation_id").and_then(Value::as_str) != Some(operation_id.as_str())
            {
                continue;
            }
            let handle: AgentHandle = serde_json::from_value(record.payload).map_err(|error| {
                WorkflowNodeError::reconciliation_required(format!("invalid worker handle: {error}"))
            })?;
            if found
                .as_ref()
                .is_some_and(|existing| serde_json::to_value(existing).ok() != serde_json::to_value(&handle).ok())
            {
                return Err(WorkflowNodeError::reconciliation_required(
                    "conflicting durable Supervisor worker handles",
                ));
            }
            found = Some(handle);
        }
        Ok(found)
    }

    pub(super) fn find_agent_outcome(
        &self,
        handle: &AgentHandle,
    ) -> Result<Option<(SubAgentResult, Option<OutcomeBlobRef>)>, WorkflowNodeError> {
        self.spawner
            .lifecycle_runtime()
            .agent_outcome_by_identity(self.spawner.run_id(), &handle.operation_id, &handle.agent_id)
            .map_err(|error| WorkflowNodeError::reconciliation_required(format!("invalid Agent outcome: {error}")))
    }

    pub(super) fn validate_supervisor_decision(
        &self,
        config: &CollaborationRuntimeConfig,
        known: &BTreeMap<String, SupervisorTaskProposal>,
        decision: &SupervisorDecision,
        durable: bool,
    ) -> Result<(), WorkflowNodeError> {
        let SupervisorDecision::Dispatch { tasks } = decision else {
            if let SupervisorDecision::Abort { reason, .. } = decision
                && reason.trim().is_empty()
            {
                return Err(self.invalid_supervisor_decision("abort reason must not be empty", durable));
            }
            return Ok(());
        };
        if tasks.is_empty() {
            return Err(self.invalid_supervisor_decision("dispatch must contain at least one Task", durable));
        }
        let max_tasks = usize::try_from(config.max_tasks)
            .unwrap_or(usize::MAX)
            .min(HARD_MAX_SUPERVISOR_TASKS);
        if known.len().saturating_add(tasks.len()) > max_tasks {
            return Err(self.invalid_supervisor_decision(format!("Supervisor Task count exceeds {max_tasks}"), durable));
        }
        let role_limits: HashMap<_, _> = config
            .worker_roles
            .iter()
            .map(|policy| (policy.role.as_str(), policy.max_total as usize))
            .collect();
        let mut keys = BTreeSet::new();
        let mut role_counts = HashMap::<&str, usize>::new();
        for proposal in known.values().chain(tasks) {
            *role_counts.entry(proposal.role.as_str()).or_default() += 1;
        }
        for proposal in tasks {
            if proposal.task_key.trim().is_empty()
                || proposal.role.trim().is_empty()
                || proposal.instruction.trim().is_empty()
            {
                return Err(
                    self.invalid_supervisor_decision("Task key, role, and instruction must not be empty", durable)
                );
            }
            if known.contains_key(&proposal.task_key) || !keys.insert(proposal.task_key.as_str()) {
                return Err(self.invalid_supervisor_decision(
                    format!("duplicate Supervisor task key: {}", proposal.task_key),
                    durable,
                ));
            }
            self.validate_supervisor_write_scope(proposal, durable)?;
            self.validate_supervisor_process_scope(proposal, durable)?;
            let Some(limit) = role_limits.get(proposal.role.as_str()) else {
                return Err(self.invalid_supervisor_decision(
                    format!("unknown configured Supervisor worker role: {}", proposal.role),
                    durable,
                ));
            };
            if role_counts.get(proposal.role.as_str()).copied().unwrap_or_default() > *limit {
                return Err(self.invalid_supervisor_decision(
                    format!("Supervisor role {} exceeds max_total {limit}", proposal.role),
                    durable,
                ));
            }
            let mut dependencies = BTreeSet::new();
            for dependency in &proposal.depends_on {
                if dependency == &proposal.task_key {
                    return Err(self.invalid_supervisor_decision(
                        format!("Supervisor task {} depends on itself", proposal.task_key),
                        durable,
                    ));
                }
                if !dependencies.insert(dependency) {
                    return Err(self.invalid_supervisor_decision(
                        format!("Supervisor task {} repeats dependency {dependency}", proposal.task_key),
                        durable,
                    ));
                }
                if !known.contains_key(dependency) && !tasks.iter().any(|task| &task.task_key == dependency) {
                    return Err(self.invalid_supervisor_decision(
                        format!(
                            "Supervisor task {} has unknown dependency {dependency}",
                            proposal.task_key
                        ),
                        durable,
                    ));
                }
            }
        }
        let mut graph = known.clone();
        graph.extend(tasks.iter().cloned().map(|task| (task.task_key.clone(), task)));
        if supervisor_graph_has_cycle(&graph) {
            return Err(self.invalid_supervisor_decision("Supervisor Task dependency graph contains a cycle", durable));
        }
        Ok(())
    }

    fn invalid_supervisor_decision(&self, message: impl Into<String>, durable: bool) -> WorkflowNodeError {
        if durable {
            WorkflowNodeError::reconciliation_required(message)
        } else {
            WorkflowNodeError::non_retryable(message)
        }
    }

    fn validate_supervisor_write_scope(
        &self,
        proposal: &SupervisorTaskProposal,
        durable: bool,
    ) -> Result<(), WorkflowNodeError> {
        for scope in &proposal.expected_write_scope {
            let path = Path::new(scope);
            if path.is_absolute() {
                return Err(self.invalid_supervisor_decision(
                    format!("Supervisor task {} has an absolute write scope", proposal.task_key),
                    durable,
                ));
            }
            if path.components().any(|component| component == Component::ParentDir) {
                return Err(self.invalid_supervisor_decision(
                    format!("Supervisor task {} write scope contains '..'", proposal.task_key),
                    durable,
                ));
            }
            if scope
                .chars()
                .any(|character| matches!(character, '*' | '?' | '[' | ']' | '{' | '}'))
            {
                return Err(self.invalid_supervisor_decision(
                    format!(
                        "Supervisor task {} write scope contains an unsupported glob",
                        proposal.task_key
                    ),
                    durable,
                ));
            }
        }
        Ok(())
    }

    fn validate_supervisor_process_scope(
        &self,
        proposal: &SupervisorTaskProposal,
        durable: bool,
    ) -> Result<(), WorkflowNodeError> {
        if self.spawner.permission_mode() != PermissionMode::Auto {
            return Ok(());
        }
        let role = self.roles.get(&proposal.role).ok_or_else(|| {
            self.invalid_supervisor_decision(
                format!("unknown configured Supervisor worker role: {}", proposal.role),
                durable,
            )
        })?;
        if !role
            .capability_scope
            .iter()
            .any(|capability| capability == "ExecCommand")
        {
            return Ok(());
        }
        let supported = proposal.expected_write_scope.len() == 1 && {
            let mut components = Path::new(&proposal.expected_write_scope[0]).components();
            matches!(components.next(), Some(Component::CurDir)) && components.next().is_none()
        };
        if supported {
            return Ok(());
        }
        Err(self.invalid_supervisor_decision(
            format!(
                "Supervisor task {} cannot enforce process write scope; Auto ExecCommand currently requires exactly the workspace root '.'",
                proposal.task_key
            ),
            durable,
        ))
    }

    pub(super) async fn terminate_supervisor_tasks(
        &self,
        runtime: &SupervisorRuntime<'_>,
        known: &BTreeMap<String, SupervisorTaskProposal>,
    ) -> Result<(), WorkflowNodeError> {
        let mut errors = Vec::new();
        for proposal in known.values() {
            if let Err(error) = self.terminate_supervisor_task(runtime, proposal).await {
                errors.push(format!("{}: {}", proposal.task_key, error.message));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(WorkflowNodeError::reconciliation_required(errors.join(" | ")))
        }
    }

    async fn terminate_supervisor_task(
        &self,
        runtime: &SupervisorRuntime<'_>,
        proposal: &SupervisorTaskProposal,
    ) -> Result<(), WorkflowNodeError> {
        let task_id = supervisor_task_id(runtime.context, &proposal.task_key);
        let operation_id = supervisor_worker_operation_id(runtime.context, &proposal.task_key);
        let task = self.spawner.lifecycle_runtime().tasks().get(&task_id).ok_or_else(|| {
            WorkflowNodeError::reconciliation_required("Supervisor Task disappeared during termination")
        })?;
        if matches!(
            task.failure_class,
            Some(TaskFailureClass::OutcomeUnknown | TaskFailureClass::ReconciliationRequired)
        ) {
            return Err(workflow_error(
                task.failure_class.unwrap_or(TaskFailureClass::ReconciliationRequired),
                "Supervisor Task requires reconciliation",
            ));
        }
        if matches!(
            task.state,
            TaskState::Completed | TaskState::Failed | TaskState::Skipped | TaskState::Cancelled
        ) {
            return Ok(());
        }
        if task.state == TaskState::Queued {
            return self.cancel_supervisor_task_record(runtime, &task_id, task.revision);
        }
        let handle = self
            .find_supervisor_worker_handle(&task_id, &operation_id)?
            .ok_or_else(|| {
                WorkflowNodeError::reconciliation_required(format!(
                    "active Supervisor Task {task_id} has no durable Agent handle"
                ))
            })?;
        if let Some((result, result_ref)) = self.find_agent_outcome(&handle)? {
            if matches!(
                result.status,
                AgentOutcomeStatus::OutcomeUnknown | AgentOutcomeStatus::ReconciliationRequired
            ) {
                return Err(workflow_error(
                    result
                        .failure_class
                        .unwrap_or_else(|| conservative_failure_class(result.status)),
                    "Supervisor Task has an unknown worker outcome",
                ));
            }
            let (outcome, body) =
                self.build_supervisor_worker_outcome(runtime.context, proposal, &handle, result, result_ref)?;
            self.persist_supervisor_worker_outcome(&outcome)?;
            self.record_usage_once(
                format!("workflow-supervisor-worker:{}", outcome.operation_id),
                body.result.turns,
                &body.result.usage,
            );
            return self.settle_supervisor_worker(&outcome);
        }
        self.spawner.cancel(&handle).await.map_err(|error| {
            WorkflowNodeError::reconciliation_required(format!("failed to cancel Supervisor worker: {error}"))
        })?;
        let current = self
            .spawner
            .lifecycle_runtime()
            .tasks()
            .get(&task_id)
            .ok_or_else(|| WorkflowNodeError::reconciliation_required("Supervisor Task disappeared"))?;
        if matches!(current.state, TaskState::Assigned | TaskState::Running) {
            self.cancel_supervisor_task_record(runtime, &task_id, current.revision)?;
        }
        Ok(())
    }

    fn cancel_supervisor_task_record(
        &self,
        runtime: &SupervisorRuntime<'_>,
        task_id: &TaskId,
        revision: u64,
    ) -> Result<(), WorkflowNodeError> {
        self.spawner
            .lifecycle_runtime()
            .cancel_supervisor_workflow_task(
                self.spawner.run_id(),
                task_id,
                &runtime.handle.agent_id,
                revision,
                &OperationId::new(format!("workflow-supervisor-cancel:{task_id}:{revision}")),
            )
            .map(|_| ())
            .map_err(|error| WorkflowNodeError::reconciliation_required(error.to_string()))
    }
}

fn supervisor_graph_has_cycle(tasks: &BTreeMap<String, SupervisorTaskProposal>) -> bool {
    fn visit(
        key: &str,
        tasks: &BTreeMap<String, SupervisorTaskProposal>,
        visiting: &mut BTreeSet<String>,
        visited: &mut BTreeSet<String>,
    ) -> bool {
        if visited.contains(key) {
            return false;
        }
        if !visiting.insert(key.to_owned()) {
            return true;
        }
        if let Some(task) = tasks.get(key) {
            for dependency in &task.depends_on {
                if visit(dependency, tasks, visiting, visited) {
                    return true;
                }
            }
        }
        visiting.remove(key);
        visited.insert(key.to_owned());
        false
    }

    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    tasks.keys().any(|key| visit(key, tasks, &mut visiting, &mut visited))
}

pub(super) fn supervisor_worker_boundary(
    mode: PermissionMode,
    proposal: &SupervisorTaskProposal,
) -> Option<ExecutionBoundary> {
    if mode == PermissionMode::Bypass {
        return None;
    }
    Some(ExecutionBoundary {
        readable_roots: Vec::new(),
        unrestricted_file_reads: true,
        writable_roots: proposal.expected_write_scope.clone(),
        unrestricted_file_writes: false,
        network_domains: Vec::new(),
        process_command_prefixes: Vec::new(),
        process_invocations: Vec::new(),
        unrestricted_process: true,
        external_resource_prefixes: Vec::new(),
        unrestricted_external_side_effects: true,
        unrestricted_network: true,
    })
}

pub(super) fn supervisor_task_state(outcome: &DurableSupervisorWorkerOutcome) -> TaskState {
    match outcome.status {
        AgentOutcomeStatus::Completed if outcome.failure_class.is_none() => TaskState::Completed,
        AgentOutcomeStatus::Cancelled => TaskState::Cancelled,
        AgentOutcomeStatus::Completed
        | AgentOutcomeStatus::Failed
        | AgentOutcomeStatus::OutcomeUnknown
        | AgentOutcomeStatus::ReconciliationRequired => TaskState::Failed,
    }
}
