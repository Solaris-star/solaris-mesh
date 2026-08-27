use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use serde_json::json;

use solaris_types::identity::{ChildAgentKey, OperationId, RunId};
use solaris_types::permission::{ExecutionBoundary, PermissionCeiling, PermissionDecision, PermissionMode};
use solaris_types::resource::ResourceBudget;
use solaris_types::runtime::{OperationEnvironmentSnapshot, TaskFailureClass};

use crate::execution_context::{
    EffectExecutionContext, EffectOutcomeGuard, EffectRecoveryDecision, build_environment_snapshot_with_plugins,
};
use crate::output::OutputSink;
use crate::output::runtime_sink::RuntimeOutputSink;
use crate::permission_engine::boundary_path_within;
use crate::resource_manager::ResourceManager;

use super::service::{outcome_to_legacy, spawn_cancelled, spawn_error, spawn_failure};
use super::{
    AgentOutcomeStatus, AgentSpawnService, AgentSpawnSpec, AgentSpawner, ForkOverrides, SpawnExecutionGuard,
    SubAgentConfig, SubAgentResult, sub_agent_result_from_engine,
};

fn sub_agent_request_max_tokens(configured: Option<u32>, role_ceiling: u32) -> u32 {
    configured.map_or(role_ceiling, |max_tokens| max_tokens.min(role_ceiling))
}

impl AgentSpawner {
    pub(crate) fn permission_mode(&self) -> PermissionMode {
        self.permission_context.mode()
    }

    pub async fn spawn_fork_with_operation(
        &self,
        sub_config: SubAgentConfig,
        overrides: ForkOverrides,
        operation_id: OperationId,
        requested_ceiling: PermissionCeiling,
    ) -> SubAgentResult {
        let spec = self.fork_spec(sub_config, overrides, operation_id, requested_ceiling);
        match self.spawn(spec).await {
            Ok(handle) => match self.join(&handle).await {
                Ok(outcome) => outcome_to_legacy(outcome),
                Err(error) => spawn_error(&handle.spec.config.name, error),
            },
            Err(error) => spawn_failure("fork", error),
        }
    }

    pub(super) async fn execute_spawn_spec(&self, spec: AgentSpawnSpec) -> SubAgentResult {
        let sub_config = spec.config.clone();
        let overrides = spec.overrides.clone();
        let operation_id = spec.operation_id.clone();
        let requested_ceiling = spec.permission_ceiling;
        let key = ChildAgentKey {
            run_id: self.run_id.clone(),
            parent_agent_id: self.parent_agent_id.clone(),
            role_key: spec.role_key.clone(),
            stable_task_key: spec.stable_task_key.clone(),
            spawn_operation_id: operation_id.clone(),
        };
        let expected_agent_id = self
            .lifecycle_runtime
            .existing_child_identity(&key)
            .map_or_else(|| key.agent_id(), |(agent_id, _)| agent_id);
        let _operation_guard = self.operation_locks.acquire(key).await;
        let context = self.lifecycle_effect_context();
        let request = self.lifecycle_effect_request(&spec);
        match context.recover_effect(&request) {
            Ok(EffectRecoveryDecision::Reuse { output, .. }) => {
                return serde_json::from_str(&output).unwrap_or_else(|error| {
                    spawn_error(&sub_config.name, format!("invalid durable fork outcome: {error}"))
                });
            }
            Ok(EffectRecoveryDecision::Execute) => {}
            Ok(EffectRecoveryDecision::Reconcile { reason }) => {
                let error = self.spawn_recovery_error(&request, reason);
                return spawn_failure(&sub_config.name, error);
            }
            Err(reason) => {
                return spawn_failure(
                    &sub_config.name,
                    super::AgentSpawnError::reconciliation_required(reason),
                );
            }
        }
        let evaluation = context.evaluate(&request);
        if let Err(error) = context.record_permission_decision(&request, &evaluation, "agent_spawn_service") {
            return spawn_error(
                &sub_config.name,
                format!("failed to persist fork permission decision: {error}"),
            );
        }
        if evaluation.decision != PermissionDecision::Allow {
            return spawn_failure(
                &sub_config.name,
                super::AgentSpawnError {
                    failure_class: TaskFailureClass::PermissionDenied,
                    message: format!("fork permission denied: {}", evaluation.reason),
                },
            );
        }
        let _effect_permit = match context.acquire_effect_permit().await {
            Ok(permit) => permit,
            Err(error) => return spawn_error(&sub_config.name, format!("fork effect budget denied: {error}")),
        };
        let approved_environment = context.environment();
        if let Err(error) = context.revalidate_environment(&request, &approved_environment) {
            let _ = context.record_revalidation_failure(&request, &error);
            return spawn_error(&sub_config.name, error);
        }
        if let Err(error) = context.record_effect_intent(&request) {
            return spawn_error(
                &sub_config.name,
                format!("failed to persist fork effect intent: {error}"),
            );
        }
        let cancellation_outcome =
            serde_json::to_string(&spawn_cancelled(&sub_config.name, &expected_agent_id, &spec.task_id))
                .unwrap_or_else(|_| "{\"status\":\"cancelled\",\"is_error\":true}".to_owned());
        let mut outcome_guard = EffectOutcomeGuard::new(context, request, cancellation_outcome);
        let mut result = self
            .spawn_fork_authorized(
                sub_config,
                overrides,
                operation_id,
                requested_ceiling,
                spec.role_key.clone(),
                spec.stable_task_key.clone(),
                spec.resource_budget.clone(),
                spec.context_policy.clone(),
                spec.recursion_limit,
            )
            .await;
        result.agent_id = Some(expected_agent_id);
        result.task_id = Some(spec.task_id.clone());
        if matches!(
            result.status,
            AgentOutcomeStatus::OutcomeUnknown | AgentOutcomeStatus::ReconciliationRequired
        ) {
            outcome_guard.leave_for_reconciliation();
            return result;
        }
        result.is_error = result.status != AgentOutcomeStatus::Completed;
        result.output = Some(json!({"text": result.text}));
        let output = match serde_json::to_string(&result) {
            Ok(output) => output,
            Err(error) => return spawn_error(&result.name, format!("failed to serialize fork outcome: {error}")),
        };
        if let Err(error) = outcome_guard.complete(result.is_error, &output) {
            return spawn_error(&result.name, format!("failed to persist fork effect outcome: {error}"));
        }
        result
    }

    #[allow(clippy::too_many_arguments)]
    async fn spawn_fork_authorized(
        &self,
        sub_config: SubAgentConfig,
        overrides: ForkOverrides,
        operation_id: OperationId,
        requested_ceiling: PermissionCeiling,
        role_key: String,
        stable_task_key: String,
        resource_budget: ResourceBudget,
        context_policy: Option<String>,
        recursion_limit: Option<usize>,
    ) -> SubAgentResult {
        let name = sub_config.name.clone();
        let reservation = match self.lifecycle_runtime.reserve_spawn_typed(
            self.run_id.clone(),
            self.parent_agent_id.clone(),
            role_key,
            stable_task_key,
            operation_id,
            &self.permission_context,
            requested_ceiling,
        ) {
            Ok(value) => value,
            Err(error) => return spawn_error(&name, format!("fork reservation failed: {error}")),
        };
        if let Some(result) = self.reattached_outcome(&reservation, &name, "workflow operation") {
            return result;
        }
        let mut execution_guard =
            SpawnExecutionGuard::new(Arc::clone(&self.lifecycle_runtime), reservation.clone(), name.clone());
        let resource_permit = if reservation.reattached {
            self.resources
                .acquire_reattached_agent(self.spawn_depth.saturating_add(1))
                .await
        } else {
            self.resources.acquire_agent(self.spawn_depth.saturating_add(1)).await
        };
        let _resource_permit = match resource_permit {
            Ok(permit) => permit,
            Err(error) => {
                return execution_guard.fail(&name, &error, format!("fork resource budget denied: {error}"));
            }
        };
        if let Err(error) = self.attach_collaboration(&reservation, overrides.collaboration.as_ref()) {
            return execution_guard.fail(&name, &error, format!("collaboration setup failed: {error}"));
        }

        let role_wall_time_ms = resource_budget.max_wall_time_ms;
        let role_resources = ResourceManager::new(resource_budget);
        role_resources.set_provider_signals(self.resources.provider_signals());
        let role_resource_run_id = RunId::new(format!("{}:agent-resource:{}", self.run_id, reservation.child_agent_id));
        if let Err(error) = role_resources.attach_ledger(role_resource_run_id, self.lifecycle_runtime.ledger()) {
            return execution_guard.fail(&name, &error, format!("role resource budget restore failed: {error}"));
        }

        let mut config = self.base_config.clone();
        config.max_turns = Some(sub_config.max_turns);
        config.max_tokens = Some(sub_agent_request_max_tokens(config.max_tokens, sub_config.max_tokens));
        if let Some(system_prompt) = sub_config.system_prompt.clone() {
            config.system_prompt = Some(system_prompt);
        }
        if let Some(collaboration_prompt) = self.collaboration_prompt(&reservation, overrides.collaboration.as_ref()) {
            config.system_prompt = Some(match config.system_prompt.take() {
                Some(existing) if !existing.is_empty() => format!("{existing}\n\n{collaboration_prompt}"),
                _ => collaboration_prompt,
            });
        }
        if let Some(policy) = context_policy {
            let policy_prompt = match policy.as_str() {
                "isolated" => {
                    "Context policy: isolated. Use only the typed input in this task; parent transcript is unavailable."
                }
                "isolated_verification" => {
                    "Context policy: isolated verification. Verify only the supplied typed evidence; parent transcript and unstated claims are unavailable."
                }
                other => {
                    return execution_guard.fail(
                        &name,
                        "unsupported role context policy",
                        format!("unsupported role context policy: {other}"),
                    );
                }
            };
            config.system_prompt = Some(match config.system_prompt.take() {
                Some(existing) if !existing.is_empty() => format!("{existing}\n\n{policy_prompt}"),
                _ => policy_prompt.to_owned(),
            });
        }
        if let Some(model) = overrides.model.clone() {
            config.model = model;
        }

        let child_permissions = match overrides.execution_boundary.as_ref() {
            Some(requested) => {
                match intersect_execution_boundary(&self.permission_context.boundary(), requested, &self.cwd) {
                    Ok(boundary) => self
                        .permission_context
                        .narrowed_with_boundary(reservation.permission_ceiling, boundary),
                    Err(error) => {
                        return execution_guard.fail(
                            &name,
                            "invalid child execution boundary",
                            format!("invalid child execution boundary: {error}"),
                        );
                    }
                }
            }
            None => self.permission_context.narrowed(reservation.permission_ceiling),
        };
        let child_execution_context = EffectExecutionContext::new(
            self.run_id.clone(),
            reservation.child_agent_id.clone(),
            self.lifecycle_runtime.ledger(),
            child_permissions.clone(),
            OperationEnvironmentSnapshot::default(),
        )
        .with_mutation_coordinator(self.lifecycle_runtime.mutation_coordinator())
        .with_resource_manager(Arc::clone(&self.resources))
        .with_scoped_resource_manager(role_resources);
        let tools = self.child_registry(
            &overrides.allowed_tools,
            overrides.inherit_capabilities,
            reservation.child_agent_id.clone(),
            child_permissions.clone(),
            recursion_limit,
            child_execution_context.clone(),
        );
        let mut child_environment =
            build_environment_snapshot_with_plugins(&config, &tools, &child_permissions, self.plugin_identities());
        child_environment.workflow = overrides.workflow.clone();
        child_execution_context.set_environment(child_environment);
        let output: Arc<dyn OutputSink> = Arc::new(RuntimeOutputSink::new(
            Arc::clone(&self.lifecycle_runtime),
            self.run_id.clone(),
            reservation.child_agent_id.clone(),
        ));
        let mut engine = match self.build_child_engine(
            &reservation,
            config,
            tools,
            output,
            child_permissions,
            child_execution_context,
        ) {
            Ok(engine) => engine,
            Err(error) => {
                return execution_guard.fail(&name, &error, error.clone());
            }
        };
        engine.set_initial_reasoning_effort(overrides.effort.clone());

        if let Err(error) = self.lifecycle_runtime.commit_spawn(&reservation) {
            return execution_guard.fail(&name, &error.to_string(), format!("fork commit failed: {error}"));
        }
        execution_guard.committed();

        let run_result = if let Some(limit_ms) = role_wall_time_ms {
            match tokio::time::timeout(
                std::time::Duration::from_millis(limit_ms),
                engine.run(&sub_config.prompt, reservation.child_agent_id.as_str()),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => {
                    let outcome = spawn_error(
                        &name,
                        format!("Agent role wall-time budget exhausted after {limit_ms} ms"),
                    );
                    let result = self.persist_outcome(&reservation, outcome);
                    execution_guard.finish();
                    return result;
                }
            }
        } else {
            engine
                .run(&sub_config.prompt, reservation.child_agent_id.as_str())
                .await
        };
        let outcome = match run_result {
            Ok(result) => sub_agent_result_from_engine(name, reservation.child_agent_id.clone(), result),
            Err(error) => {
                let failure_class = error.failure_class();
                spawn_failure(
                    &name,
                    solaris_types::spawner::AgentSpawnError {
                        failure_class,
                        message: format!("Sub-agent error: {error}"),
                    },
                )
            }
        };
        let result = self.persist_outcome(&reservation, outcome);
        execution_guard.finish();
        result
    }
}

fn intersect_execution_boundary(
    parent: &ExecutionBoundary,
    requested: &ExecutionBoundary,
    cwd: &Path,
) -> Result<ExecutionBoundary, String> {
    let requested_reads = normalize_boundary_roots(&requested.readable_roots, cwd)?;
    let requested_writes = normalize_boundary_roots(&requested.writable_roots, cwd)?;
    Ok(ExecutionBoundary {
        readable_roots: intersect_path_roots(
            &parent.readable_roots,
            parent.unrestricted_file_reads,
            &requested_reads,
            requested.unrestricted_file_reads,
            "read",
        )?,
        unrestricted_file_reads: parent.unrestricted_file_reads && requested.unrestricted_file_reads,
        writable_roots: intersect_path_roots(
            &parent.writable_roots,
            parent.unrestricted_file_writes,
            &requested_writes,
            requested.unrestricted_file_writes,
            "write",
        )?,
        unrestricted_file_writes: parent.unrestricted_file_writes && requested.unrestricted_file_writes,
        network_domains: intersect_named_scope(
            &parent.network_domains,
            parent.unrestricted_network,
            &requested.network_domains,
            requested.unrestricted_network,
            |candidate, allowed| candidate == allowed,
        ),
        process_command_prefixes: intersect_named_scope(
            &parent.process_command_prefixes,
            parent.unrestricted_process,
            &requested.process_command_prefixes,
            requested.unrestricted_process,
            |candidate, allowed| candidate == allowed,
        ),
        process_invocations: if parent.unrestricted_process {
            requested.process_invocations.clone()
        } else if requested.unrestricted_process {
            parent.process_invocations.clone()
        } else {
            requested
                .process_invocations
                .iter()
                .filter(|value| parent.process_invocations.contains(value))
                .cloned()
                .collect()
        },
        unrestricted_process: parent.unrestricted_process && requested.unrestricted_process,
        external_resource_prefixes: intersect_named_scope(
            &parent.external_resource_prefixes,
            parent.unrestricted_external_side_effects,
            &requested.external_resource_prefixes,
            requested.unrestricted_external_side_effects,
            |candidate, allowed| candidate.starts_with(allowed),
        ),
        unrestricted_external_side_effects: parent.unrestricted_external_side_effects
            && requested.unrestricted_external_side_effects,
        unrestricted_network: parent.unrestricted_network && requested.unrestricted_network,
    })
}

fn normalize_boundary_roots(roots: &[String], cwd: &Path) -> Result<Vec<String>, String> {
    roots
        .iter()
        .map(|root| {
            if root
                .chars()
                .any(|character| matches!(character, '*' | '?' | '[' | ']' | '{' | '}'))
            {
                return Err(format!("boundary path cannot contain a glob: {root}"));
            }
            let path = Path::new(root);
            if path.components().any(|component| component == Component::ParentDir) {
                return Err(format!("boundary path cannot contain '..': {root}"));
            }
            let absolute = if path.is_absolute() {
                path.to_path_buf()
            } else {
                cwd.join(path)
            };
            Ok(normalize_lexical_absolute(&absolute)?.to_string_lossy().into_owned())
        })
        .collect()
}

fn normalize_lexical_absolute(path: &Path) -> Result<PathBuf, String> {
    if !path.is_absolute() {
        return Err(format!("boundary path is not absolute: {}", path.display()));
    }
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::Normal(value) => normalized.push(value),
            Component::ParentDir => return Err(format!("boundary path contains '..': {}", path.display())),
        }
    }
    Ok(normalized)
}

fn intersect_path_roots(
    parent: &[String],
    parent_unrestricted: bool,
    requested: &[String],
    requested_unrestricted: bool,
    label: &str,
) -> Result<Vec<String>, String> {
    if parent_unrestricted {
        return Ok(if requested_unrestricted {
            Vec::new()
        } else {
            requested.to_vec()
        });
    }
    if requested_unrestricted {
        return Ok(parent.to_vec());
    }
    let mut intersection = Vec::new();
    for requested_root in requested {
        let mut matched = false;
        for parent_root in parent {
            let root = if boundary_path_within(requested_root, parent_root) {
                Some(requested_root)
            } else if boundary_path_within(parent_root, requested_root) {
                Some(parent_root)
            } else {
                None
            };
            if let Some(root) = root {
                matched = true;
                if !intersection.contains(root) {
                    intersection.push(root.clone());
                }
            }
        }
        if !matched {
            return Err(format!(
                "requested {label} root is outside the parent boundary: {requested_root}"
            ));
        }
    }
    Ok(intersection)
}

fn intersect_named_scope<T: Clone + PartialEq>(
    parent: &[T],
    parent_unrestricted: bool,
    requested: &[T],
    requested_unrestricted: bool,
    covered_by: impl Fn(&T, &T) -> bool,
) -> Vec<T> {
    if parent_unrestricted {
        return if requested_unrestricted {
            Vec::new()
        } else {
            requested.to_vec()
        };
    }
    if requested_unrestricted {
        return parent.to_vec();
    }
    requested
        .iter()
        .filter(|candidate| parent.iter().any(|allowed| covered_by(candidate, allowed)))
        .cloned()
        .collect()
}

#[cfg(test)]
#[path = "fork_test.rs"]
mod fork_test;
