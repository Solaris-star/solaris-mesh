use std::collections::HashMap;
use std::sync::Arc;

use futures::stream::{self, StreamExt};
use serde_json::{Value, json};
use solaris_types::spawner::{AgentCollaborationContext, AgentHandle, AgentSpawnSpec, ForkOverrides, SubAgentConfig};
use solaris_types::workflow::{AgentRoleDefinition, CollaborationRuntimeConfig, CollaborationStrategy};
use tokio::sync::Semaphore;

use super::*;

pub(super) struct ConfiguredSpecRequest<'a> {
    pub(super) context: &'a WorkflowExecutionContext,
    pub(super) role: &'a AgentRoleDefinition,
    pub(super) role_input: &'a Value,
    pub(super) strategy: CollaborationStrategy,
    pub(super) collaboration: Option<AgentCollaborationContext>,
    pub(super) suffix: &'a str,
    pub(super) instruction: &'a str,
    pub(super) owns_workflow_task: bool,
}

impl AgentWorkflowExecutor {
    pub(super) async fn execute_configured_collaboration(
        &self,
        context: &WorkflowExecutionContext,
        config: &CollaborationRuntimeConfig,
        role_input: &Value,
    ) -> Result<Value, WorkflowNodeError> {
        self.validate_configured_role_inputs(context, config, role_input)?;
        match config.strategy {
            CollaborationStrategy::Supervisor => self.execute_configured_supervisor(context, config, role_input).await,
            CollaborationStrategy::Team => self.execute_configured_team(context, config, role_input).await,
            CollaborationStrategy::Fanout => self.execute_configured_fanout(context, config, role_input).await,
            CollaborationStrategy::IndependentReviewer => {
                self.execute_configured_reviewer(context, config, role_input).await
            }
            CollaborationStrategy::Single => Err(WorkflowNodeError::non_retryable(
                "configured single collaboration reached multi-agent runtime",
            )),
        }
    }

    pub(super) fn configured_role_spec(
        &self,
        request: ConfiguredSpecRequest<'_>,
    ) -> Result<AgentSpawnSpec, WorkflowNodeError> {
        let mut allowed_tools = if request.context.node.capability_scope.is_empty() {
            request.role.capability_scope.clone()
        } else {
            request.context.node.capability_scope.clone()
        };
        for capability in [
            "SendAgentMessage",
            "BroadcastTeamMessage",
            "ReadAgentMessages",
            "AcknowledgeAgentMessage",
            "GetTeamState",
            "CreateTeamTask",
            "HandoffTask",
            "SetTeamFact",
            "ReadTeamFacts",
            "RegisterArtifact",
            "ListArtifacts",
        ] {
            if !allowed_tools.iter().any(|value| value == capability) {
                allowed_tools.push(capability.to_owned());
            }
        }
        let may_manage_team = match request.strategy {
            CollaborationStrategy::Team => true,
            CollaborationStrategy::Supervisor => request.owns_workflow_task,
            CollaborationStrategy::Single
            | CollaborationStrategy::Fanout
            | CollaborationStrategy::IndependentReviewer => false,
        };
        if !may_manage_team {
            allowed_tools
                .retain(|tool| !matches!(tool.as_str(), "BroadcastTeamMessage" | "CreateTeamTask" | "HandoffTask"));
        }

        let base_task_id = format!("workflow:{}:{}", request.context.run_id, request.context.node.id);
        let base_stable_key = format!(
            "workflow:{}:{}:{}",
            request.context.run_id, request.context.node.id, request.context.attempt_id
        );
        let base_operation_id = format!(
            "workflow:{}:{}:{}",
            request.context.run_id, request.context.node.id, request.context.attempt_id
        );
        let (task_id, stable_task_key, operation_id, name) = if request.owns_workflow_task {
            let name = if request.strategy == CollaborationStrategy::IndependentReviewer {
                format!("{}:{}:{}", request.context.node.id, request.role.id, request.suffix)
            } else {
                format!("{}:{}", request.context.node.id, request.role.id)
            };
            (
                TaskId::new(base_task_id),
                base_stable_key,
                OperationId::new(base_operation_id),
                name,
            )
        } else {
            (
                TaskId::new(format!("{base_task_id}:{}", request.suffix)),
                format!("{base_stable_key}:{}", request.suffix),
                OperationId::new(format!("{base_operation_id}:{}", request.suffix)),
                format!("{}:{}:{}", request.context.node.id, request.role.id, request.suffix),
            )
        };
        let prompt = format!(
            "Role: {}\n\n{}\n\nTyped workflow input:\n{}\n\nExpected output schema:\n{}\n\n{}\n\nReturn only valid JSON matching the output schema when one is provided. Do not wrap JSON in prose.",
            request.role.id,
            request.role.description,
            serde_json::to_string_pretty(request.role_input).unwrap_or_default(),
            request
                .role
                .output_schema
                .as_ref()
                .and_then(|schema| serde_json::to_string_pretty(schema).ok())
                .unwrap_or_else(|| "null".to_owned()),
            request.instruction,
        );
        let max_tokens = request
            .role
            .budget
            .max_tokens
            .unwrap_or(DEFAULT_WORKFLOW_ROLE_TOKEN_BUDGET)
            .min(u32::MAX as u64) as u32;
        let recursion_limit = match request.role.recursion_policy.as_deref() {
            Some("none") => Some(0),
            Some("bounded") => Some(request.role.budget.max_spawn_depth.unwrap_or(1)),
            Some(other) => {
                return Err(WorkflowNodeError::non_retryable(format!(
                    "unsupported recursion policy for role {}: {other}",
                    request.role.id
                )));
            }
            None => None,
        };
        let expected_task_revision = if request.owns_workflow_task {
            self.spawner
                .lifecycle_runtime()
                .tasks()
                .get(&task_id)
                .map(|task| task.revision)
        } else {
            Some(0)
        };
        Ok(AgentSpawnSpec {
            run_id: self.spawner.run_id().clone(),
            parent_agent_id: self.spawner.parent_agent_id().clone(),
            task_id,
            role_key: request.role.id.clone(),
            stable_task_key,
            operation_id,
            expected_task_revision,
            config: SubAgentConfig {
                name,
                prompt,
                max_turns: request.role.budget.max_turns.unwrap_or(120),
                max_tokens,
                system_prompt: None,
            },
            overrides: ForkOverrides {
                model: request
                    .context
                    .node
                    .model_policy
                    .model
                    .clone()
                    .or(request.role.model_policy.model.clone()),
                effort: request.context.node.model_policy.reasoning_effort.clone().or(request
                    .role
                    .model_policy
                    .reasoning_effort
                    .clone()),
                allowed_tools,
                inherit_capabilities: false,
                collaboration: request.collaboration,
                workflow: Some(request.context.workflow.clone()),
                execution_boundary: None,
            },
            permission_ceiling: request
                .role
                .permission_ceiling
                .intersect(request.context.node.permission_ceiling),
            resource_budget: request.role.budget.clone(),
            context_policy: request.role.context_policy.clone(),
            recursion_limit,
        })
    }

    fn validate_configured_role_inputs(
        &self,
        context: &WorkflowExecutionContext,
        config: &CollaborationRuntimeConfig,
        role_input: &Value,
    ) -> Result<(), WorkflowNodeError> {
        let mut role_ids = Vec::new();
        if let Some(role_id) = context.node.role.as_deref() {
            role_ids.push(role_id);
        }
        if config.strategy != CollaborationStrategy::Supervisor {
            for role_id in config.worker_roles.iter().map(|policy| policy.role.as_str()) {
                if !role_ids.contains(&role_id) {
                    role_ids.push(role_id);
                }
            }
        }
        for role_id in role_ids {
            let role = self
                .roles
                .get(role_id)
                .ok_or_else(|| format!("unknown agent role: {role_id}"))?;
            if let Some(schema) = &role.input_schema {
                validate_value(role_input, schema)
                    .map_err(|error| format!("role {} input invalid: {error}", role.id))?;
            }
        }
        Ok(())
    }

    fn configured_worker_specs(
        &self,
        context: &WorkflowExecutionContext,
        config: &CollaborationRuntimeConfig,
        role_input: &Value,
        collaboration: &AgentCollaborationContext,
    ) -> Result<Vec<(AgentSpawnSpec, bool)>, WorkflowNodeError> {
        let mut specs = Vec::new();
        for policy in &config.worker_roles {
            let role = self
                .roles
                .get(&policy.role)
                .ok_or_else(|| format!("unknown agent role: {}", policy.role))?;
            for index in 0..policy.max_total {
                let suffix = format!("worker:{}:{index}", policy.role);
                let instruction = match config.strategy {
                    CollaborationStrategy::Team => {
                        "Work on the assigned part and send useful facts or artifacts to the coordinator."
                    }
                    CollaborationStrategy::Fanout => {
                        "Produce a complete independent candidate without relying on other workers."
                    }
                    _ => "Complete the assigned role.",
                };
                specs.push((
                    self.configured_role_spec(ConfiguredSpecRequest {
                        context,
                        role: &role,
                        role_input,
                        strategy: config.strategy,
                        collaboration: Some(collaboration.clone()),
                        suffix: &suffix,
                        instruction,
                        owns_workflow_task: false,
                    })?,
                    true,
                ));
            }
        }
        Ok(specs)
    }

    async fn join_configured_workers(
        &self,
        handles: Vec<(AgentHandle, bool)>,
        config: &CollaborationRuntimeConfig,
    ) -> Result<Vec<(String, SubAgentResult)>, WorkflowNodeError> {
        let role_limits: HashMap<_, _> = config
            .worker_roles
            .iter()
            .map(|policy| {
                (
                    policy.role.clone(),
                    Arc::new(Semaphore::new(policy.max_concurrent as usize)),
                )
            })
            .collect();
        let completed = stream::iter(handles.into_iter().enumerate().map(|(index, (handle, settle_task))| {
            let role_id = handle.role_key.clone();
            let role_limit = role_limits.get(&role_id).cloned();
            async move {
                let role_limit = role_limit.ok_or_else(|| {
                    WorkflowNodeError::non_retryable(format!(
                        "configured worker role {role_id} has no concurrency policy"
                    ))
                })?;
                let _role_permit = role_limit
                    .acquire_owned()
                    .await
                    .map_err(|_| WorkflowNodeError::retryable("configured worker role semaphore closed"))?;
                let result = self.join_handle(handle, settle_task).await?;
                Ok::<_, WorkflowNodeError>((index, role_id, result))
            }
        }))
        .buffer_unordered(config.max_concurrent_workers as usize)
        .collect::<Vec<_>>()
        .await;
        let mut completed: Vec<_> = completed.into_iter().collect::<Result<_, _>>()?;
        completed.sort_by_key(|(index, _, _)| *index);
        Ok(completed
            .into_iter()
            .map(|(_, role_id, result)| (role_id, result))
            .collect())
    }

    pub(super) fn configured_output(&self, role_id: &str, result: &SubAgentResult) -> Result<Value, WorkflowNodeError> {
        if result.is_error {
            return Err(workflow_failure_from_result(result));
        }
        let role = self
            .roles
            .get(role_id)
            .ok_or_else(|| format!("unknown agent role: {role_id}"))?;
        Ok(normalize_role_output(
            &role.id,
            role.output_schema.as_ref(),
            &result.text,
        )?)
    }

    async fn coordinator_cleanup_error(
        &self,
        coordinator_handle: AgentHandle,
        coordinator_task: bool,
        mut error: WorkflowNodeError,
    ) -> WorkflowNodeError {
        let mut coordinator = vec![(coordinator_handle, coordinator_task)];
        if let Err(cleanup) = self.cancel_handles(&mut coordinator).await {
            error.message = format!("{}; coordinator cleanup failed: {cleanup}", error.message);
        }
        error
    }

    async fn execute_configured_team(
        &self,
        context: &WorkflowExecutionContext,
        config: &CollaborationRuntimeConfig,
        role_input: &Value,
    ) -> Result<Value, WorkflowNodeError> {
        let coordinator_role_id = context
            .node
            .role
            .as_deref()
            .ok_or_else(|| "configured team collaboration requires a coordinator role".to_owned())?;
        let coordinator_role = self
            .roles
            .get(coordinator_role_id)
            .ok_or_else(|| format!("unknown agent role: {coordinator_role_id}"))?;
        let mut coordinator = self.configured_role_spec(ConfiguredSpecRequest {
            context,
            role: &coordinator_role,
            role_input,
            strategy: config.strategy,
            collaboration: None,
            suffix: "coordinator",
            instruction: "Coordinate the configured worker roles and return the final typed result.",
            owns_workflow_task: true,
        })?;
        let coordinator_id = self.resolved_agent_id(&coordinator);
        let collaboration = collaboration_context(
            &context.run_id,
            &context.node.id,
            context.attempt_id.as_str(),
            config.strategy,
            coordinator_id,
            Some(config),
        )
        .expect("team has collaboration context");
        coordinator.overrides.collaboration = Some(collaboration.clone());
        let mut specs = vec![(coordinator, false)];
        specs.extend(self.configured_worker_specs(context, config, role_input, &collaboration)?);
        let mut handles = self.spawn_handles(specs).await?;
        let (coordinator_handle, coordinator_task) = handles.remove(0);
        let worker_results = match self.join_configured_workers(handles, config).await {
            Ok(results) => results,
            Err(error) => {
                return Err(self
                    .coordinator_cleanup_error(coordinator_handle, coordinator_task, error)
                    .await);
            }
        };
        let delivery = (|| -> Result<(), WorkflowNodeError> {
            for (role_id, result) in worker_results {
                let output = self.configured_output(&role_id, &result)?;
                let worker_id = result
                    .agent_id
                    .ok_or_else(|| "configured team worker returned no stable Agent identity".to_owned())?;
                self.spawner
                    .lifecycle_runtime()
                    .send_message_with_id(
                        format!(
                            "workflow-worker-result:{}:{}:{}:{}",
                            context.run_id, context.node.id, context.attempt_id, worker_id
                        ),
                        coordinator_handle.run_id.clone(),
                        Some(collaboration.team_id.clone()),
                        worker_id,
                        coordinator_handle.agent_id.clone(),
                        "worker_result",
                        json!({"role": role_id, "output": output}),
                    )
                    .map_err(|error| {
                        WorkflowNodeError::reconciliation_required(format!(
                            "failed to deliver configured worker result to coordinator: {error}"
                        ))
                    })?;
            }
            Ok(())
        })();
        if let Err(error) = delivery {
            return Err(self
                .coordinator_cleanup_error(coordinator_handle, coordinator_task, error)
                .await);
        }
        let result = self.join_handle(coordinator_handle, coordinator_task).await?;
        self.configured_output(coordinator_role_id, &result)
    }

    async fn execute_configured_fanout(
        &self,
        context: &WorkflowExecutionContext,
        config: &CollaborationRuntimeConfig,
        role_input: &Value,
    ) -> Result<Value, WorkflowNodeError> {
        let synthesis_role_id = context
            .node
            .role
            .as_deref()
            .ok_or_else(|| "configured fanout collaboration requires a synthesis role".to_owned())?;
        let collaboration = collaboration_context(
            &context.run_id,
            &context.node.id,
            context.attempt_id.as_str(),
            config.strategy,
            self.spawner.parent_agent_id().clone(),
            Some(config),
        )
        .expect("fanout has collaboration context");
        let worker_specs = self.configured_worker_specs(context, config, role_input, &collaboration)?;
        let handles = self.spawn_handles(worker_specs).await?;
        let worker_results = self.join_configured_workers(handles, config).await?;
        let mut candidates = Vec::with_capacity(worker_results.len());
        for (role_id, result) in worker_results {
            candidates.push(json!({
                "role": role_id,
                "agent_id": result.agent_id,
                "output": self.configured_output(&role_id, &result)?,
            }));
        }
        let synthesis_role = self
            .roles
            .get(synthesis_role_id)
            .ok_or_else(|| format!("unknown agent role: {synthesis_role_id}"))?;
        let instruction = format!(
            "Synthesize these independent candidates into the final typed result:\n{}",
            serde_json::to_string_pretty(&candidates).unwrap_or_default()
        );
        let synthesis = self.configured_role_spec(ConfiguredSpecRequest {
            context,
            role: &synthesis_role,
            role_input,
            strategy: config.strategy,
            collaboration: Some(collaboration),
            suffix: "synthesis",
            instruction: &instruction,
            owns_workflow_task: true,
        })?;
        let result = self
            .execute_specs(vec![(synthesis, false)])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| "configured fanout synthesis returned no result".to_owned())?;
        self.configured_output(synthesis_role_id, &result)
    }

    async fn execute_configured_reviewer(
        &self,
        context: &WorkflowExecutionContext,
        config: &CollaborationRuntimeConfig,
        role_input: &Value,
    ) -> Result<Value, WorkflowNodeError> {
        let primary_role_id = config
            .primary_role
            .as_deref()
            .ok_or_else(|| "configured independent_reviewer requires primary_role".to_owned())?;
        let reviewer_role_id = config
            .reviewer_role
            .as_deref()
            .ok_or_else(|| "configured independent_reviewer requires reviewer_role".to_owned())?;
        let primary_role = self
            .roles
            .get(primary_role_id)
            .ok_or_else(|| format!("unknown agent role: {primary_role_id}"))?;
        let reviewer_role = self
            .roles
            .get(reviewer_role_id)
            .ok_or_else(|| format!("unknown agent role: {reviewer_role_id}"))?;
        let collaboration = collaboration_context(
            &context.run_id,
            &context.node.id,
            context.attempt_id.as_str(),
            config.strategy,
            self.spawner.parent_agent_id().clone(),
            Some(config),
        )
        .expect("independent_reviewer has collaboration context");
        let primary_policy = config
            .worker_roles
            .iter()
            .find(|policy| policy.role == primary_role_id)
            .expect("validated primary role has a worker policy");
        let reviewer_policy = config
            .worker_roles
            .iter()
            .find(|policy| policy.role == reviewer_role_id)
            .expect("validated reviewer role has a worker policy");
        let mut primary_specs = Vec::with_capacity(primary_policy.max_total as usize);
        for index in 0..primary_policy.max_total {
            let suffix = format!("primary:{index}");
            primary_specs.push((
                self.configured_role_spec(ConfiguredSpecRequest {
                    context,
                    role: &primary_role,
                    role_input,
                    strategy: config.strategy,
                    collaboration: Some(collaboration.clone()),
                    suffix: &suffix,
                    instruction: "Produce one independent primary candidate for later review.",
                    owns_workflow_task: false,
                })?,
                true,
            ));
        }
        let primary_handles = self.spawn_handles(primary_specs).await?;
        let primary_results = self.join_configured_workers(primary_handles, config).await?;
        let mut candidates = Vec::with_capacity(primary_results.len());
        for (_, result) in primary_results {
            candidates.push(json!({
                "agent_id": result.agent_id,
                "output": self.configured_output(primary_role_id, &result)?,
            }));
        }
        let candidates_json = serde_json::to_string_pretty(&candidates).unwrap_or_default();

        let preliminary_count = reviewer_policy.max_total.saturating_sub(1);
        let mut preliminary_specs = Vec::with_capacity(preliminary_count as usize);
        for index in 0..preliminary_count {
            let suffix = format!("reviewer:preliminary:{index}");
            let instruction = format!(
                "Perform preliminary review {index} of every primary candidate. Return one typed independent review for the final reviewer.\nPrimary candidates:\n{candidates_json}"
            );
            preliminary_specs.push((
                self.configured_role_spec(ConfiguredSpecRequest {
                    context,
                    role: &reviewer_role,
                    role_input,
                    strategy: config.strategy,
                    collaboration: Some(collaboration.clone()),
                    suffix: &suffix,
                    instruction: &instruction,
                    owns_workflow_task: false,
                })?,
                true,
            ));
        }
        let mut preliminary_reviews = Vec::with_capacity(preliminary_count as usize);
        if !preliminary_specs.is_empty() {
            let preliminary_handles = self.spawn_handles(preliminary_specs).await?;
            let preliminary_results = self.join_configured_workers(preliminary_handles, config).await?;
            for (_, result) in preliminary_results {
                preliminary_reviews.push(json!({
                    "agent_id": result.agent_id,
                    "output": self.configured_output(reviewer_role_id, &result)?,
                }));
            }
        }
        let final_index = reviewer_policy.max_total - 1;
        let suffix = format!("reviewer:final:{final_index}");
        let instruction = if preliminary_reviews.is_empty() {
            format!(
                "Review every primary candidate and return the unique final typed result.\nPrimary candidates:\n{candidates_json}"
            )
        } else {
            format!(
                "Review every primary candidate and the independent preliminary reviews, then return the unique final typed result.\nPrimary candidates:\n{candidates_json}\nPreliminary reviews:\n{}",
                serde_json::to_string_pretty(&preliminary_reviews).unwrap_or_default()
            )
        };
        let final_reviewer = self.configured_role_spec(ConfiguredSpecRequest {
            context,
            role: &reviewer_role,
            role_input,
            strategy: config.strategy,
            collaboration: Some(collaboration),
            suffix: &suffix,
            instruction: &instruction,
            owns_workflow_task: true,
        })?;
        let result = self
            .execute_specs(vec![(final_reviewer, false)])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| "configured independent_reviewer returned no review".to_owned())?;
        self.configured_output(reviewer_role_id, &result)
    }
}
