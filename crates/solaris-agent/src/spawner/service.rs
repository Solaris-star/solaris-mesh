use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;
use tokio::sync::Notify;
use uuid::Uuid;

use solaris_tools::edit::EditTool;
use solaris_tools::exec_command::ExecCommandTool;
use solaris_tools::glob::GlobTool;
use solaris_tools::grep::GrepTool;
use solaris_tools::read::ReadTool;
use solaris_tools::read_only_evidence::ReadOnlyEvidenceIndex;
use solaris_tools::registry::ToolRegistry;
use solaris_tools::write::WriteTool;
use solaris_types::identity::{AgentId, ChildAgentKey, OperationId, TaskId};
use solaris_types::message::TokenUsage;
use solaris_types::permission::{PermissionCeiling, PermissionDecision};
use solaris_types::resource::ResourceBudget;
use solaris_types::runtime::{AgentLifecycleState, TaskFailureClass};
use solaris_types::spawner::{
    AgentHandle, AgentOutcome, AgentOutcomeStatus, AgentSpawnError, AgentSpawnService, AgentSpawnSpec, ForkOverrides,
    Spawner, SubAgentConfig, SubAgentResult,
};

use crate::collaboration_runtime::AgentSpawnReservation;
use crate::execution_context::{EffectRecoveryDecision, stable_digest_value};
use crate::permission_engine::PermissionContext;
use crate::runtime_ledger::LedgerRecord;

use super::AgentSpawner;

const DEFAULT_MAX_SPAWN_DEPTH: usize = 8;
const DEFAULT_MAX_TOTAL_DESCENDANTS_PER_RUN: usize = 256;

#[async_trait]
impl Spawner for AgentSpawner {
    async fn spawn_fork(&self, sub_config: SubAgentConfig, overrides: ForkOverrides) -> SubAgentResult {
        let operation_id = OperationId::new(format!("fork-{}", Uuid::now_v7()));
        let spec = self.fork_spec(sub_config, overrides, operation_id, PermissionCeiling::unrestricted());
        match self.spawn(spec).await {
            Ok(handle) => match self.join(&handle).await {
                Ok(outcome) => outcome_to_legacy(outcome),
                Err(error) => spawn_error(&handle.spec.config.name, error),
            },
            Err(error) => spawn_failure("fork", error),
        }
    }
}

#[async_trait]
impl AgentSpawnService for AgentSpawner {
    async fn spawn(&self, spec: AgentSpawnSpec) -> Result<AgentHandle, AgentSpawnError> {
        self.ensure_multi_agent_allowed()
            .map_err(AgentSpawnError::non_retryable)?;
        if spec.run_id != self.run_id || spec.parent_agent_id != self.parent_agent_id {
            return Err(AgentSpawnError::non_retryable(
                "AgentSpawnSpec does not belong to this AgentSpawner Run and parent",
            ));
        }
        if self.remaining_spawn_depth == Some(0) {
            return Err(AgentSpawnError::non_retryable(
                "Agent role recursion policy denies spawning another child Agent",
            ));
        }
        if spec.role_key.trim().is_empty() || spec.stable_task_key.trim().is_empty() {
            return Err(AgentSpawnError::non_retryable(
                "AgentSpawnSpec role_key and stable_task_key must not be empty",
            ));
        }
        let spec_value =
            serde_json::to_value(&spec).map_err(|error| AgentSpawnError::non_retryable(error.to_string()))?;
        let spec_digest = stable_digest_value(&spec_value);
        let key = ChildAgentKey {
            run_id: spec.run_id.clone(),
            parent_agent_id: spec.parent_agent_id.clone(),
            role_key: spec.role_key.clone(),
            stable_task_key: spec.stable_task_key.clone(),
            spawn_operation_id: spec.operation_id.clone(),
        };
        let (agent_id, identity_version) = self
            .lifecycle_runtime
            .existing_child_identity(&key)
            .unwrap_or_else(|| (key.agent_id(), ChildAgentKey::CURRENT_IDENTITY_VERSION));
        if key.identity_version_for_agent_id(&agent_id) != Some(identity_version) {
            return Err(AgentSpawnError::reconciliation_required(
                "durable child identity version does not match its AgentId",
            ));
        }
        if let Some(collaboration) = spec.overrides.collaboration.as_ref() {
            if !self.supports_atomic_collaboration_batch() {
                return Err(AgentSpawnError::non_retryable(
                    "runtime ledger cannot atomically persist collaboration Team membership",
                ));
            }
            self.lifecycle_runtime
                .prepare_collaboration_team(&self.run_id, &agent_id, collaboration)
                .map_err(|error| AgentSpawnError::reconciliation_required(error.to_string()))?;
        }
        let handle = AgentHandle {
            run_id: spec.run_id.clone(),
            agent_id,
            identity_version,
            task_id: spec.task_id.clone(),
            operation_id: spec.operation_id.clone(),
            role_key: spec.role_key.clone(),
            stable_task_key: spec.stable_task_key.clone(),
            spec_digest,
            spec: spec.clone(),
        };
        self.lifecycle_runtime
            .record_agent_handle(&handle)
            .map_err(|error| AgentSpawnError::reconciliation_required(error.to_string()))?;

        let _operation_guard = self.operation_locks.acquire(key).await;
        let context = self.lifecycle_effect_context();
        let request = self.lifecycle_effect_request(&spec);
        match context
            .recover_effect(&request)
            .map_err(AgentSpawnError::reconciliation_required)?
        {
            EffectRecoveryDecision::Reuse { .. } => return Ok(handle),
            EffectRecoveryDecision::Execute => {}
            EffectRecoveryDecision::Reconcile { reason } => {
                return Err(self.spawn_recovery_error(&request, reason));
            }
        }
        let evaluation = context.evaluate(&request);
        context
            .record_permission_decision(&request, &evaluation, "agent_spawn_reservation")
            .map_err(|error| AgentSpawnError::reconciliation_required(error.to_string()))?;
        if evaluation.decision != PermissionDecision::Allow {
            return Err(AgentSpawnError::non_retryable(format!(
                "spawn permission denied: {}",
                evaluation.reason
            )));
        }
        let _permit = context
            .acquire_effect_permit()
            .await
            .map_err(AgentSpawnError::non_retryable)?;
        let approved_environment = context.environment();
        context
            .revalidate_environment(&request, &approved_environment)
            .map_err(AgentSpawnError::non_retryable)?;
        // `spawn` only reserves a stable child handle. The child does not start
        // until `join`, where `execute_spawn_spec` writes the effect intent
        // immediately before execution. Writing it here would make the normal
        // spawn-then-join path look like an interrupted, outcome-unknown effect.
        let reservation = self
            .lifecycle_runtime
            .reserve_spawn_typed(
                spec.run_id.clone(),
                spec.parent_agent_id.clone(),
                spec.role_key.clone(),
                spec.stable_task_key.clone(),
                spec.operation_id.clone(),
                &self.permission_context,
                spec.permission_ceiling,
            )
            .map_err(|error| AgentSpawnError::reconciliation_required(error.to_string()))?;
        if reservation.child_agent_id != handle.agent_id {
            return Err(self.abort_reservation_after_spawn_failure(
                &reservation,
                "durable spawn reservation returned a different child identity".to_owned(),
            ));
        }
        if self.lifecycle_runtime.tasks().get(&handle.task_id).is_some()
            && let Some(collaboration) = spec.overrides.collaboration.as_ref()
            && let Err(error) =
                self.lifecycle_runtime
                    .prepare_collaboration_membership(&handle.run_id, &handle.agent_id, collaboration)
        {
            return Err(self.abort_reservation_after_spawn_failure(
                &reservation,
                format!("failed to persist reserved Agent Team membership before task assignment: {error}"),
            ));
        }
        if let Some(task) = self.lifecycle_runtime.tasks().get(&handle.task_id) {
            let expected_revision = spec.expected_task_revision.unwrap_or(task.revision);
            let task_operation_id = OperationId::new(format!("{}:task-assign", handle.operation_id));
            if let Err(first_error) = self.lifecycle_runtime.assign_task_owner(
                &handle.run_id,
                &handle.task_id,
                &handle.agent_id,
                expected_revision,
                &task_operation_id,
            ) {
                if let Err(replay_error) = self.lifecycle_runtime.assign_task_owner(
                    &handle.run_id,
                    &handle.task_id,
                    &handle.agent_id,
                    expected_revision,
                    &task_operation_id,
                ) {
                    let assignment_error = format!(
                        "failed to assign reserved Agent task: {first_error}; exact CAS replay failed: {replay_error}"
                    );
                    match self.task_assignment_durable_record(&handle, expected_revision, &task_operation_id) {
                        Ok(None) => {
                            return Err(self.abort_reservation_after_spawn_failure(&reservation, assignment_error));
                        }
                        Ok(Some(durable_record)) => {
                            if let Err(restore_error) =
                                self.lifecycle_runtime.restore_projection(&durable_record.run_id)
                            {
                                tracing::warn!(
                                    run_id = %handle.run_id,
                                    task_id = %handle.task_id,
                                    operation_id = %task_operation_id,
                                    "durable task assignment projection restore failed"
                                );
                                return Err(AgentSpawnError::reconciliation_required(format!(
                                    "{assignment_error}; durable assignment projection restore failed: {restore_error}"
                                )));
                            }
                            tracing::warn!(
                                run_id = %handle.run_id,
                                task_id = %handle.task_id,
                                operation_id = %task_operation_id,
                                durable_sequence = durable_record.seq,
                                "restored durable task assignment after exact replay failed"
                            );
                            return Err(AgentSpawnError::reconciliation_required(assignment_error));
                        }
                        Err(lookup_error) => {
                            tracing::warn!(
                                run_id = %handle.run_id,
                                task_id = %handle.task_id,
                                operation_id = %task_operation_id,
                                "task assignment durable lookup failed after exact replay"
                            );
                            return Err(AgentSpawnError::reconciliation_required(format!(
                                "{assignment_error}; durable assignment lookup failed: {lookup_error}"
                            )));
                        }
                    }
                }
                tracing::info!(
                    run_id = %handle.run_id,
                    task_id = %handle.task_id,
                    operation_id = %task_operation_id,
                    "recovered task assignment through exact durable replay"
                );
            }
        }
        Ok(handle)
    }

    async fn join(&self, handle: &AgentHandle) -> Result<AgentOutcome, String> {
        if handle.run_id != self.run_id || handle.spec.run_id != handle.run_id {
            return Err("AgentHandle does not belong to this Run".to_owned());
        }
        let actual_digest =
            stable_digest_value(&serde_json::to_value(&handle.spec).map_err(|error| error.to_string())?);
        if actual_digest != handle.spec_digest {
            return Err("AgentHandle AgentSpawnSpec digest changed; reconciliation is required".to_owned());
        }
        if let Some(task) = self.lifecycle_runtime.tasks().get(&handle.task_id) {
            match (task.state, task.owner_agent_id.as_ref()) {
                (solaris_types::runtime::TaskState::Assigned, Some(owner)) if owner == &handle.agent_id => {
                    let task_operation_id = OperationId::new(format!("{}:task-running", handle.operation_id));
                    self.lifecycle_runtime
                        .mark_task_running(
                            &handle.run_id,
                            &handle.task_id,
                            &handle.agent_id,
                            task.revision,
                            &task_operation_id,
                        )
                        .map_err(|error| error.to_string())?;
                }
                (solaris_types::runtime::TaskState::Running, Some(owner)) if owner == &handle.agent_id => {}
                _ => {
                    return Err(
                        "Agent task is not Assigned or Running for this owner; reconciliation is required".to_owned(),
                    );
                }
            }
        }
        let notify = Arc::new(Notify::new());
        {
            let cancelled = self
                .spawn_control
                .cancelled
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if cancelled.contains(&handle.operation_id) {
                return Ok(cancelled_outcome(handle));
            }
        }
        self.spawn_control
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(handle.operation_id.clone(), Arc::clone(&notify));
        let result = tokio::select! {
            result = self.execute_spawn_spec(handle.spec.clone()) => result,
            _ = notify.notified() => {
                spawn_cancelled(&handle.spec.config.name, &handle.agent_id, &handle.task_id)
            }
        };
        self.spawn_control
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&handle.operation_id);
        let status = result.status;
        let output = result.output.unwrap_or_else(|| json!({"text": result.text}));
        Ok(AgentOutcome {
            handle: handle.clone(),
            status,
            output,
            usage: result.usage,
            turns: result.turns,
            failure_class: result.failure_class,
            error: (status != AgentOutcomeStatus::Completed).then_some(result.text),
        })
    }

    async fn cancel(&self, handle: &AgentHandle) -> Result<(), String> {
        let actual_digest =
            stable_digest_value(&serde_json::to_value(&handle.spec).map_err(|error| error.to_string())?);
        if handle.run_id != self.run_id || actual_digest != handle.spec_digest {
            return Err("AgentHandle identity mismatch".to_owned());
        }
        let context = self.lifecycle_effect_context();
        let request = self.lifecycle_effect_request(&handle.spec);
        match context.recover_effect(&request)? {
            EffectRecoveryDecision::Reuse { .. } => {
                return match self.lifecycle_runtime.agents().get(&handle.agent_id) {
                    Some(agent) if agent.state == AgentLifecycleState::Cancelled => Ok(()),
                    None => Ok(()),
                    Some(agent) => Err(format!(
                        "durable cancel outcome conflicts with Agent state {:?}; reconciliation is required",
                        agent.state
                    )),
                };
            }
            EffectRecoveryDecision::Execute => {
                // A reserved handle has not started its child effect yet. Pair
                // the cancellation with a durable intent so a restart can
                // reuse the terminal cancelled outcome instead of executing it.
                context
                    .record_effect_intent(&request)
                    .map_err(|error| error.to_string())?;
            }
            EffectRecoveryDecision::Reconcile { reason } => return Err(reason),
        }
        let reservation = AgentSpawnReservation {
            key: ChildAgentKey {
                run_id: handle.run_id.clone(),
                parent_agent_id: handle.spec.parent_agent_id.clone(),
                role_key: handle.role_key.clone(),
                stable_task_key: handle.stable_task_key.clone(),
                spawn_operation_id: handle.operation_id.clone(),
            },
            child_agent_id: handle.agent_id.clone(),
            permission_ceiling: handle.spec.permission_ceiling,
            reattached: false,
        };
        self.lifecycle_runtime
            .cancel_spawn(&reservation, "AgentHandle was cancelled")
            .map_err(|error| error.to_string())?;
        let cancelled = spawn_cancelled(&handle.spec.config.name, &handle.agent_id, &handle.task_id);
        self.lifecycle_runtime
            .record_agent_outcome(&reservation, &cancelled)
            .map_err(|error| error.to_string())?;
        let output = serde_json::to_string(&cancelled).map_err(|error| error.to_string())?;
        context
            .record_effect_outcome(&request, true, &output)
            .map_err(|error| error.to_string())?;
        self.spawn_control
            .cancelled
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(handle.operation_id.clone());
        if let Some(notify) = self
            .spawn_control
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&handle.operation_id)
            .cloned()
        {
            notify.notify_waiters();
        }
        Ok(())
    }
}

impl AgentSpawner {
    fn task_assignment_durable_record(
        &self,
        handle: &AgentHandle,
        expected_revision: u64,
        operation_id: &OperationId,
    ) -> std::io::Result<Option<LedgerRecord>> {
        let Some(new_revision) = expected_revision.checked_add(1) else {
            return Ok(None);
        };
        Ok(self
            .lifecycle_runtime
            .ledger()
            .records_for_run(&handle.run_id)?
            .into_iter()
            .find(|record| {
                record.record_type == "task_cas"
                    && record.payload.get("operation_id").and_then(serde_json::Value::as_str)
                        == Some(operation_id.as_str())
                    && record.payload.get("task_id").and_then(serde_json::Value::as_str)
                        == Some(handle.task_id.as_str())
                    && record
                        .payload
                        .get("expected_revision")
                        .and_then(serde_json::Value::as_u64)
                        == Some(expected_revision)
                    && record.payload.get("new_revision").and_then(serde_json::Value::as_u64) == Some(new_revision)
                    && record.payload.get("transition").and_then(serde_json::Value::as_str) == Some("assign")
                    && record
                        .payload
                        .get("task")
                        .and_then(|task| task.get("owner_agent_id"))
                        .and_then(serde_json::Value::as_str)
                        == Some(handle.agent_id.as_str())
            }))
    }

    fn abort_reservation_after_spawn_failure(
        &self,
        reservation: &AgentSpawnReservation,
        original_error: String,
    ) -> AgentSpawnError {
        match self.lifecycle_runtime.abort_spawn(reservation, &original_error) {
            Ok(()) => AgentSpawnError::reconciliation_required(original_error),
            Err(cleanup_error) => AgentSpawnError::reconciliation_required(format!(
                "{original_error}; reservation cleanup failed: {cleanup_error}"
            )),
        }
    }
}

pub(super) fn resource_budget_from_env(runtime_env: &[(String, String)]) -> ResourceBudget {
    fn value<'a>(runtime_env: &'a [(String, String)], key: &str) -> Option<&'a str> {
        runtime_env
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.as_str())
    }
    ResourceBudget {
        max_active_agents: value(runtime_env, "SOLARIS_MAX_ACTIVE_AGENTS").and_then(|value| value.parse().ok()),
        max_concurrent_effects: value(runtime_env, "SOLARIS_MAX_CONCURRENT_EFFECTS")
            .and_then(|value| value.parse().ok()),
        max_spawn_depth: Some(
            value(runtime_env, "SOLARIS_MAX_SPAWN_DEPTH")
                .and_then(|value| value.parse().ok())
                .unwrap_or(DEFAULT_MAX_SPAWN_DEPTH),
        ),
        max_total_descendants_per_run: Some(
            value(runtime_env, "SOLARIS_MAX_TOTAL_DESCENDANTS")
                .and_then(|value| value.parse().ok())
                .unwrap_or(DEFAULT_MAX_TOTAL_DESCENDANTS_PER_RUN),
        ),
        max_turns: value(runtime_env, "SOLARIS_MAX_RUN_TURNS").and_then(|value| value.parse().ok()),
        max_tokens: value(runtime_env, "SOLARIS_MAX_RUN_TOKENS").and_then(|value| value.parse().ok()),
        max_wall_time_ms: value(runtime_env, "SOLARIS_MAX_RUN_WALL_TIME_MS").and_then(|value| value.parse().ok()),
        max_cost: value(runtime_env, "SOLARIS_MAX_RUN_COST").and_then(|value| value.parse().ok()),
        max_process_output_bytes: value(runtime_env, "SOLARIS_MAX_PROCESS_OUTPUT_BYTES")
            .and_then(|value| value.parse().ok()),
    }
}

pub(super) fn spawn_error(name: &str, text: String) -> SubAgentResult {
    SubAgentResult {
        name: name.to_owned(),
        agent_id: None,
        task_id: None,
        status: AgentOutcomeStatus::Failed,
        output: Some(json!({"error": text})),
        text,
        usage: TokenUsage::default(),
        turns: 0,
        failure_class: Some(TaskFailureClass::NonRetryable),
        is_error: true,
    }
}

pub(super) fn spawn_failure(name: &str, error: AgentSpawnError) -> SubAgentResult {
    let status = match error.failure_class {
        solaris_types::runtime::TaskFailureClass::OutcomeUnknown => AgentOutcomeStatus::OutcomeUnknown,
        solaris_types::runtime::TaskFailureClass::ReconciliationRequired => AgentOutcomeStatus::ReconciliationRequired,
        solaris_types::runtime::TaskFailureClass::Cancelled => AgentOutcomeStatus::Cancelled,
        solaris_types::runtime::TaskFailureClass::Retryable
        | solaris_types::runtime::TaskFailureClass::NonRetryable
        | solaris_types::runtime::TaskFailureClass::PermissionDenied
        | solaris_types::runtime::TaskFailureClass::MaxTurns
        | solaris_types::runtime::TaskFailureClass::NonConvergent
        | solaris_types::runtime::TaskFailureClass::SideEffectUnknown => AgentOutcomeStatus::Failed,
    };
    let message = error.message;
    SubAgentResult {
        name: name.to_owned(),
        agent_id: None,
        task_id: None,
        status,
        output: Some(json!({"error": &message, "failure_class": error.failure_class})),
        text: message,
        usage: TokenUsage::default(),
        turns: 0,
        failure_class: Some(error.failure_class),
        is_error: true,
    }
}

pub(super) fn spawn_reconciliation(name: &str, text: String) -> SubAgentResult {
    SubAgentResult {
        name: name.to_owned(),
        agent_id: None,
        task_id: None,
        status: AgentOutcomeStatus::ReconciliationRequired,
        output: Some(json!({"error": text, "reconciliation_required": true})),
        text,
        usage: TokenUsage::default(),
        turns: 0,
        failure_class: Some(TaskFailureClass::ReconciliationRequired),
        is_error: true,
    }
}

pub(super) fn outcome_to_legacy(outcome: AgentOutcome) -> SubAgentResult {
    let text = outcome
        .output
        .get("text")
        .and_then(serde_json::Value::as_str)
        .or(outcome.error.as_deref())
        .unwrap_or_default()
        .to_owned();
    SubAgentResult {
        name: outcome.handle.spec.config.name.clone(),
        agent_id: Some(outcome.handle.agent_id),
        task_id: Some(outcome.handle.task_id),
        status: outcome.status,
        output: Some(outcome.output),
        text,
        usage: outcome.usage,
        turns: outcome.turns,
        failure_class: outcome.failure_class,
        is_error: outcome.status != AgentOutcomeStatus::Completed,
    }
}

pub(super) fn spawn_cancelled(name: &str, agent_id: &AgentId, task_id: &TaskId) -> SubAgentResult {
    let text = "child Agent was cancelled through its stable handle".to_owned();
    SubAgentResult {
        name: name.to_owned(),
        agent_id: Some(agent_id.clone()),
        task_id: Some(task_id.clone()),
        status: AgentOutcomeStatus::Cancelled,
        output: Some(json!({"error": text})),
        text,
        usage: TokenUsage::default(),
        turns: 0,
        failure_class: Some(TaskFailureClass::Cancelled),
        is_error: true,
    }
}

fn cancelled_outcome(handle: &AgentHandle) -> AgentOutcome {
    AgentOutcome {
        handle: handle.clone(),
        status: AgentOutcomeStatus::Cancelled,
        output: json!({"error": "child Agent was cancelled before join"}),
        usage: TokenUsage::default(),
        turns: 0,
        failure_class: Some(TaskFailureClass::Cancelled),
        error: Some("child Agent was cancelled before join".to_owned()),
    }
}

#[cfg(test)]
pub(super) fn build_tool_registry(
    allowed: &[String],
    inherit_capabilities: bool,
    cwd: &Path,
    runtime_env: &[(String, String)],
    permissions: &PermissionContext,
) -> ToolRegistry {
    build_tool_registry_with_evidence(
        allowed,
        inherit_capabilities,
        cwd,
        runtime_env,
        permissions,
        Arc::new(ReadOnlyEvidenceIndex::default()),
    )
}

pub(super) fn build_tool_registry_with_evidence(
    allowed: &[String],
    inherit_capabilities: bool,
    cwd: &Path,
    runtime_env: &[(String, String)],
    permissions: &PermissionContext,
    read_only_evidence_index: Arc<ReadOnlyEvidenceIndex>,
) -> ToolRegistry {
    let search_policy = permissions.workspace_search_policy();
    let all_tools: Vec<(&str, Box<dyn solaris_tools::Tool>)> = vec![
        (
            "Read",
            Box::new(
                ReadTool::new_with_search_policy(None, cwd, Arc::clone(&search_policy))
                    .with_read_only_evidence_index(Arc::clone(&read_only_evidence_index)),
            ),
        ),
        (
            "Write",
            Box::new(WriteTool::new_with_search_policy(None, cwd, Arc::clone(&search_policy))),
        ),
        (
            "Edit",
            Box::new(EditTool::new_with_search_policy(None, cwd, Arc::clone(&search_policy))),
        ),
        (
            "ExecCommand",
            Box::new(ExecCommandTool::new_with_env(cwd.to_path_buf(), runtime_env.to_vec())),
        ),
        (
            "Grep",
            Box::new(
                GrepTool::new_with_search_policy(cwd.to_path_buf(), Arc::clone(&search_policy))
                    .with_read_only_evidence_index(Arc::clone(&read_only_evidence_index)),
            ),
        ),
        (
            "Glob",
            Box::new(
                GlobTool::new_with_search_policy(cwd.to_path_buf(), search_policy)
                    .with_read_only_evidence_index(read_only_evidence_index),
            ),
        ),
    ];

    let mut registry = ToolRegistry::new();
    for (name, tool) in all_tools {
        if inherit_capabilities || allowed.iter().any(|a| a.as_str() == name) {
            registry.register(tool);
        }
    }
    registry
}
