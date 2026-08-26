use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::future::join_all;
use serde_json::{Value, json};
use solaris_providers::LlmProvider;
use solaris_types::identity::{AgentId, ChildAgentKey, OperationId, RunId, TaskId, TeamId};
use solaris_types::llm::{LlmEvent, LlmRequest};
use solaris_types::message::{ContentBlock, Message, Role, TokenUsage};
use solaris_types::plugin::PluginContributionKind;
use solaris_types::runtime::{TaskFailureClass, TaskState};
use solaris_types::spawner::{
    AgentCollaborationContext, AgentHandle, AgentOutcomeStatus, AgentSpawnService, AgentSpawnSpec, ForkOverrides,
    SubAgentConfig, SubAgentResult,
};
use solaris_types::workflow::{CollaborationRuntimeConfig, CollaborationSelection, CollaborationStrategy};

use crate::builtin_workflows::DEFAULT_WORKFLOW_ROLE_TOKEN_BUDGET;
use crate::collaboration_runtime::TaskSettlement;
use crate::collaboration_strategy::plan_collaboration;
use crate::plugin_provider::PluginLlmProvider;
use crate::plugin_tool::PluginContributionDispatcher;
use crate::role_registry::AgentRoleRegistry;
use crate::schema_validation::validate_value;
use crate::spawner::AgentSpawner;
use crate::workflow_controller::{WorkflowExecutionContext, WorkflowNodeError, WorkflowNodeExecutor};

#[path = "workflow_executor_configured.rs"]
mod workflow_executor_configured;
#[path = "workflow_executor_supervisor.rs"]
mod workflow_executor_supervisor;

pub struct AgentWorkflowExecutor {
    spawner: Arc<AgentSpawner>,
    roles: Arc<AgentRoleRegistry>,
    plugins: Option<Arc<PluginContributionDispatcher>>,
    usage: Mutex<(usize, TokenUsage)>,
    applied_usage: Mutex<HashSet<String>>,
}

impl AgentWorkflowExecutor {
    fn resolved_agent_id(&self, spec: &AgentSpawnSpec) -> AgentId {
        let key = ChildAgentKey {
            run_id: spec.run_id.clone(),
            parent_agent_id: spec.parent_agent_id.clone(),
            role_key: spec.role_key.clone(),
            stable_task_key: spec.stable_task_key.clone(),
            spawn_operation_id: spec.operation_id.clone(),
        };
        self.spawner
            .lifecycle_runtime()
            .existing_child_identity(&key)
            .map_or_else(|| key.agent_id(), |(agent_id, _)| agent_id)
    }

    pub fn new(spawner: Arc<AgentSpawner>, roles: Arc<AgentRoleRegistry>) -> Self {
        Self {
            spawner,
            roles,
            plugins: None,
            usage: Mutex::new((0, TokenUsage::default())),
            applied_usage: Mutex::new(HashSet::new()),
        }
    }

    pub fn with_plugin_contributions(mut self, plugins: Arc<PluginContributionDispatcher>) -> Self {
        self.plugins = Some(plugins);
        self
    }

    pub(crate) fn usage(&self) -> (usize, TokenUsage) {
        self.usage.lock().unwrap_or_else(|error| error.into_inner()).clone()
    }

    fn record_usage(&self, turns: usize, usage: &TokenUsage) {
        let mut current = self.usage.lock().unwrap_or_else(|error| error.into_inner());
        current.0 = current.0.saturating_add(turns);
        current.1.input_tokens = current.1.input_tokens.saturating_add(usage.input_tokens);
        current.1.output_tokens = current.1.output_tokens.saturating_add(usage.output_tokens);
        current.1.cache_creation_tokens = current
            .1
            .cache_creation_tokens
            .saturating_add(usage.cache_creation_tokens);
        current.1.cache_read_tokens = current.1.cache_read_tokens.saturating_add(usage.cache_read_tokens);
    }

    fn record_usage_once(&self, identity: String, turns: usize, usage: &TokenUsage) {
        let inserted = self
            .applied_usage
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(identity);
        if inserted {
            self.record_usage(turns, usage);
        }
    }
}

fn parse_strategy(value: &str) -> Option<CollaborationStrategy> {
    match value.to_ascii_lowercase().as_str() {
        "single" => Some(CollaborationStrategy::Single),
        "supervisor" => Some(CollaborationStrategy::Supervisor),
        "team" => Some(CollaborationStrategy::Team),
        "fanout" => Some(CollaborationStrategy::Fanout),
        "independent_reviewer" | "independent-reviewer" | "reviewer" => {
            Some(CollaborationStrategy::IndependentReviewer)
        }
        _ => None,
    }
}

fn resolve_strategy(selection: &CollaborationSelection, parameters: &Value) -> CollaborationStrategy {
    match selection {
        CollaborationSelection::Fixed(strategy) => *strategy,
        CollaborationSelection::Configured(config) => config.strategy,
        CollaborationSelection::Auto | CollaborationSelection::Inherit => parameters
            .get("collaboration")
            .and_then(Value::as_str)
            .and_then(parse_strategy)
            .unwrap_or(CollaborationStrategy::Single),
    }
}

async fn execute_plugin_provider(
    provider: &dyn LlmProvider,
    model: String,
    system: String,
    prompt: String,
    max_tokens: u32,
    reasoning_effort: Option<String>,
) -> Result<(String, TokenUsage), String> {
    let request = LlmRequest {
        model,
        system,
        messages: vec![Message::new(Role::User, vec![ContentBlock::Text { text: prompt }])],
        tools: Vec::new(),
        max_tokens: Some(max_tokens),
        thinking: None,
        reasoning_effort,
    };
    let mut events = provider
        .stream(&request)
        .await
        .map_err(|error| format!("plugin provider request failed: {error}"))?;
    let mut text = String::new();
    let mut usage = None;
    while let Some(event) = events.recv().await {
        match event {
            LlmEvent::TextDelta(delta) => text.push_str(&delta),
            LlmEvent::Done { usage: final_usage, .. } => usage = Some(final_usage),
            LlmEvent::Error(message) => return Err(format!("plugin provider failed: {message}")),
            LlmEvent::ToolUse { .. } => {
                return Err("plugin provider requested a tool in a tool-free workflow provider call".to_owned());
            }
            LlmEvent::ThinkingDelta(_) | LlmEvent::ThinkingSignature(_) | LlmEvent::ProviderMetadata { .. } => {}
        }
    }
    let usage = usage.ok_or_else(|| "plugin provider response ended without usage".to_owned())?;
    Ok((text, usage))
}

fn collaboration_context(
    run_id: &RunId,
    node_id: &str,
    attempt_id: &str,
    strategy: CollaborationStrategy,
    coordinator_agent_id: AgentId,
    runtime_config: Option<&CollaborationRuntimeConfig>,
) -> Option<AgentCollaborationContext> {
    if strategy == CollaborationStrategy::Single {
        return None;
    }
    Some(AgentCollaborationContext {
        team_id: TeamId::new(format!(
            "workflow:{}:{}:{}:{}",
            run_id,
            node_id,
            attempt_id,
            strategy.as_str()
        )),
        strategy,
        coordinator_agent_id,
        max_pending_messages: runtime_config
            .map_or(CollaborationRuntimeConfig::DEFAULT_MAX_PENDING_MESSAGES, |config| {
                config.max_pending_messages
            }),
        max_message_bytes: runtime_config.map_or(CollaborationRuntimeConfig::DEFAULT_MAX_MESSAGE_BYTES, |config| {
            config.max_message_bytes
        }),
    })
}

fn collaboration_worker_count(parameters: &Value) -> usize {
    parameters
        .get("collaboration_workers")
        .and_then(Value::as_u64)
        .unwrap_or(2)
        .clamp(1, 8) as usize
}

fn variant_spec(
    base: &AgentSpawnSpec,
    suffix: &str,
    prompt_suffix: &str,
    collaboration: AgentCollaborationContext,
    owns_workflow_task: bool,
) -> AgentSpawnSpec {
    let mut spec = base.clone();
    if !owns_workflow_task {
        spec.task_id = TaskId::new(format!("{}:{suffix}", base.task_id));
        spec.stable_task_key = format!("{}:{suffix}", base.stable_task_key);
        spec.operation_id = OperationId::new(format!("{}:{suffix}", base.operation_id));
        spec.config.name = format!("{}:{suffix}", base.config.name);
    }
    spec.config.prompt = format!("{}\n\n{}", base.config.prompt, prompt_suffix);
    let may_manage_team = match collaboration.strategy {
        CollaborationStrategy::Supervisor => owns_workflow_task,
        CollaborationStrategy::Team => true,
        CollaborationStrategy::Single | CollaborationStrategy::Fanout | CollaborationStrategy::IndependentReviewer => {
            false
        }
    };
    if !may_manage_team {
        spec.overrides
            .allowed_tools
            .retain(|tool| !matches!(tool.as_str(), "BroadcastTeamMessage" | "CreateTeamTask" | "HandoffTask"));
    }
    spec.overrides.collaboration = Some(collaboration);
    spec
}

fn plugin_contribution_name(parameters: &Value, node_id: &str, key: &str) -> Option<String> {
    let value = parameters.get("plugin_contributions")?.get(key)?;
    if let Some(value) = value.as_str() {
        return Some(value.to_owned());
    }
    value
        .get(node_id)
        .or_else(|| value.get("*"))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn plugin_contribution_names(parameters: &Value, node_id: &str, key: &str) -> Vec<String> {
    let Some(value) = parameters.get("plugin_contributions").and_then(|value| value.get(key)) else {
        return Vec::new();
    };
    let value = value.get(node_id).or_else(|| value.get("*")).unwrap_or(value);
    value
        .as_array()
        .map(|values| values.iter().filter_map(Value::as_str).map(str::to_owned).collect())
        .or_else(|| value.as_str().map(|value| vec![value.to_owned()]))
        .unwrap_or_default()
}

fn contribution_input(base: &Value, phase: &str, index: usize) -> Value {
    let mut input = base.clone();
    if let Some(object) = input.as_object_mut() {
        object.insert("contribution_phase".into(), Value::String(phase.to_owned()));
        object.insert("contribution_index".into(), json!(index));
    }
    input
}

fn parse_structured_result(text: &str) -> Result<Value, String> {
    let trimmed = text.trim();
    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        return Ok(value);
    }
    let unfenced = trimmed
        .strip_prefix("```json")
        .or_else(|| trimmed.strip_prefix("```JSON"))
        .or_else(|| trimmed.strip_prefix("```"))
        .and_then(|value| value.strip_suffix("```"))
        .map(str::trim)
        .unwrap_or(trimmed);
    if let Ok(value) = serde_json::from_str(unfenced) {
        return Ok(value);
    }

    let mut fences = trimmed.match_indices("```");
    let Some((start, _)) = fences.next() else {
        return serde_json::from_str(trimmed).map_err(|error| error.to_string());
    };
    let Some((end, _)) = fences.next() else {
        return serde_json::from_str(trimmed).map_err(|error| error.to_string());
    };
    if fences.next().is_some() {
        return Err("structured output contains multiple fenced blocks".to_owned());
    }

    let fenced = &trimmed[start + 3..end];
    let fenced = fenced
        .strip_prefix("json\r\n")
        .or_else(|| fenced.strip_prefix("JSON\r\n"))
        .or_else(|| fenced.strip_prefix("json\n"))
        .or_else(|| fenced.strip_prefix("JSON\n"))
        .or_else(|| fenced.strip_prefix("\r\n"))
        .or_else(|| fenced.strip_prefix('\n'))
        .unwrap_or(fenced)
        .trim();
    serde_json::from_str(fenced).map_err(|error| error.to_string())
}

fn normalize_role_output(role_id: &str, schema: Option<&Value>, text: &str) -> Result<Value, String> {
    let parsed = parse_structured_result(text);
    match (schema, parsed) {
        (Some(schema), Ok(value)) => {
            validate_value(&value, schema).map_err(|error| format!("role {role_id} output invalid: {error}"))?;
            Ok(value)
        }
        (Some(_), Err(error)) => Err(format!("role {role_id} did not return valid JSON: {error}")),
        (None, Ok(value)) => Ok(value),
        (None, Err(_)) => Ok(json!({"text": text, "completed": true})),
    }
}

fn workflow_failure_from_result(result: &SubAgentResult) -> WorkflowNodeError {
    match result.status {
        AgentOutcomeStatus::OutcomeUnknown => WorkflowNodeError::outcome_unknown(result.text.clone()),
        AgentOutcomeStatus::ReconciliationRequired => WorkflowNodeError::reconciliation_required(result.text.clone()),
        AgentOutcomeStatus::Cancelled => WorkflowNodeError::non_retryable(result.text.clone()),
        AgentOutcomeStatus::Failed => WorkflowNodeError::retryable(result.text.clone()),
        AgentOutcomeStatus::Completed => {
            WorkflowNodeError::non_retryable("completed Agent result cannot be converted to a failure")
        }
    }
}

impl AgentWorkflowExecutor {
    fn settle_handle_task(
        &self,
        handle: &AgentHandle,
        state: TaskState,
        failure_class: Option<TaskFailureClass>,
    ) -> Result<u64, String> {
        let task = self
            .spawner
            .lifecycle_runtime()
            .tasks()
            .get(&handle.task_id)
            .ok_or_else(|| format!("collaboration task {} disappeared", handle.task_id))?;
        let task_operation_id = OperationId::new(format!("{}:task-settle:{state:?}", handle.operation_id));
        self.spawner
            .lifecycle_runtime()
            .settle_collaboration_task(
                &handle.run_id,
                &handle.task_id,
                &handle.agent_id,
                task.revision,
                &task_operation_id,
                TaskSettlement {
                    state,
                    outcome_ref: Some(format!("agent-outcome:{}", handle.operation_id)),
                    failure_class,
                },
            )
            .map_err(|error| format!("{error}; reconciliation is required"))
    }

    async fn cancel_handles(&self, handles: &mut Vec<(AgentHandle, bool)>) -> Result<(), String> {
        let mut failures = Vec::new();
        while let Some((handle, settle_task)) = handles.pop() {
            if settle_task && let Err(error) = self.settle_handle_task(&handle, TaskState::Cancelled, None) {
                failures.push(format!("failed to cancel task {}: {error}", handle.task_id));
            }
            if let Err(error) = self.spawner.cancel(&handle).await {
                failures.push(format!("failed to cancel Agent {}: {error}", handle.agent_id));
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }

    async fn spawn_handles(
        &self,
        specs: Vec<(AgentSpawnSpec, bool)>,
    ) -> Result<Vec<(AgentHandle, bool)>, WorkflowNodeError> {
        let mut handles = Vec::with_capacity(specs.len());
        for (spec, is_collaboration_task) in specs {
            let handle = match self.spawner.spawn(spec.clone()).await {
                Ok(handle) => handle,
                Err(error) => {
                    let cleanup = self.cancel_handles(&mut handles).await;
                    let mut failure = WorkflowNodeError::from(error);
                    if let Err(cleanup) = cleanup {
                        failure.message = format!("{}; spawn cleanup failed: {cleanup}", failure.message);
                    }
                    return Err(failure);
                }
            };
            handles.push((handle.clone(), is_collaboration_task));
        }
        if let Err(error) = self.spawner.prepare_collaboration_handles(&handles) {
            let mut cleanup: Vec<_> = handles.iter().map(|(handle, _)| (handle.clone(), false)).collect();
            let cleanup_error = self.cancel_handles(&mut cleanup).await.err();
            return Err(WorkflowNodeError::reconciliation_required(match cleanup_error {
                Some(cleanup) => format!("failed to prepare collaboration members: {error}; cleanup failed: {cleanup}"),
                None => format!("failed to prepare collaboration members: {error}"),
            }));
        }
        Ok(handles)
    }

    async fn join_handle(&self, handle: AgentHandle, settle_task: bool) -> Result<SubAgentResult, WorkflowNodeError> {
        let outcome = match self.spawner.join(&handle).await {
            Ok(outcome) => outcome,
            Err(error) => {
                let mut cleanup_failures = Vec::new();
                if settle_task
                    && let Err(cleanup) = self.settle_handle_task(
                        &handle,
                        TaskState::Failed,
                        Some(TaskFailureClass::ReconciliationRequired),
                    )
                {
                    cleanup_failures.push(format!("failed to settle task {}: {cleanup}", handle.task_id));
                }
                if let Err(cleanup) = self.spawner.cancel(&handle).await {
                    cleanup_failures.push(format!("failed to cancel Agent {}: {cleanup}", handle.agent_id));
                }
                return Err(WorkflowNodeError::reconciliation_required(
                    if cleanup_failures.is_empty() {
                        error
                    } else {
                        format!("{error}; join cleanup failed: {}", cleanup_failures.join("; "))
                    },
                ));
            }
        };
        self.record_usage(outcome.turns, &outcome.usage);
        if settle_task {
            let (state, failure_class) = match outcome.status {
                AgentOutcomeStatus::Completed => (TaskState::Completed, None),
                AgentOutcomeStatus::Cancelled => (TaskState::Cancelled, Some(TaskFailureClass::Cancelled)),
                AgentOutcomeStatus::Failed => (
                    TaskState::Failed,
                    Some(outcome.failure_class.unwrap_or(TaskFailureClass::NonRetryable)),
                ),
                AgentOutcomeStatus::OutcomeUnknown => (TaskState::Failed, Some(TaskFailureClass::OutcomeUnknown)),
                AgentOutcomeStatus::ReconciliationRequired => {
                    (TaskState::Failed, Some(TaskFailureClass::ReconciliationRequired))
                }
            };
            if let Err(error) = self.settle_handle_task(&handle, state, failure_class) {
                return Err(WorkflowNodeError::reconciliation_required(format!(
                    "failed to settle collaboration task: {error}"
                )));
            }
        }
        let text = outcome
            .output
            .get("text")
            .and_then(Value::as_str)
            .or(outcome.error.as_deref())
            .unwrap_or_default()
            .to_owned();
        Ok(SubAgentResult {
            name: handle.spec.config.name.clone(),
            agent_id: Some(handle.agent_id),
            task_id: Some(handle.task_id),
            status: outcome.status,
            output: Some(outcome.output),
            text,
            usage: outcome.usage,
            turns: outcome.turns,
            failure_class: outcome.failure_class,
            is_error: outcome.status != AgentOutcomeStatus::Completed,
        })
    }

    async fn execute_specs(
        &self,
        specs: Vec<(AgentSpawnSpec, bool)>,
    ) -> Result<Vec<SubAgentResult>, WorkflowNodeError> {
        let handles = self.spawn_handles(specs).await?;
        join_all(
            handles
                .into_iter()
                .map(|(handle, settle_task)| self.join_handle(handle, settle_task)),
        )
        .await
        .into_iter()
        .collect()
    }

    async fn execute_collaboration(
        &self,
        base: AgentSpawnSpec,
        context: &WorkflowExecutionContext,
        strategy: CollaborationStrategy,
    ) -> Result<SubAgentResult, WorkflowNodeError> {
        if strategy == CollaborationStrategy::Single {
            return self
                .execute_specs(vec![(base, false)])
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| WorkflowNodeError::retryable("Single collaboration returned no result"));
        }

        let worker_count = collaboration_worker_count(&context.parameters);
        match strategy {
            CollaborationStrategy::Single => unreachable!(),
            CollaborationStrategy::Supervisor | CollaborationStrategy::Team => {
                let coordinator_id = self.resolved_agent_id(&base);
                let collaboration = collaboration_context(
                    &context.run_id,
                    &context.node.id,
                    context.attempt_id.as_str(),
                    strategy,
                    coordinator_id.clone(),
                    None,
                )
                .expect("non-Single strategy has collaboration context");
                let coordinator_instruction = if strategy == CollaborationStrategy::Supervisor {
                    "You are the Supervisor Agent. Inspect Team state, assign or create durable tasks when needed, collect worker messages, and return the final typed result. Workers may communicate only through you."
                } else {
                    "You are the coordinating peer in a Team. Collaborate through Team messages and shared facts, then return the final typed result."
                };
                let coordinator = variant_spec(
                    &base,
                    "coordinator",
                    coordinator_instruction,
                    collaboration.clone(),
                    true,
                );
                let mut specs = vec![(coordinator, false)];
                let mut worker_ids = Vec::with_capacity(worker_count);
                for index in 0..worker_count {
                    let suffix = format!("worker:{index}");
                    let instruction = format!(
                        "You are collaboration worker {index}. Work on an independent part of the typed task, publish useful facts or artifacts, and send your result to coordinator {coordinator_id}. Do not return a coordinator-only final answer."
                    );
                    let worker = variant_spec(&base, &suffix, &instruction, collaboration.clone(), false);
                    worker_ids.push(self.resolved_agent_id(&worker));
                    specs.push((worker, true));
                }
                let plan = plan_collaboration(strategy, coordinator_id, worker_ids);
                debug_assert_eq!(plan.members.len(), worker_count + 1);
                let mut handles = self.spawn_handles(specs).await?;
                let (coordinator_handle, coordinator_task) = handles.remove(0);
                let worker_results: Result<Vec<_>, _> = join_all(
                    handles
                        .into_iter()
                        .map(|(handle, settle_task)| self.join_handle(handle, settle_task)),
                )
                .await
                .into_iter()
                .collect();
                let worker_results = match worker_results {
                    Ok(results) => results,
                    Err(error) => {
                        let mut coordinator = vec![(coordinator_handle, coordinator_task)];
                        let cleanup = self.cancel_handles(&mut coordinator).await;
                        let mut failure = error;
                        if let Err(cleanup) = cleanup {
                            failure.message = format!("{}; coordinator cleanup failed: {cleanup}", failure.message);
                        }
                        return Err(failure);
                    }
                };
                for result in &worker_results {
                    let Some(worker_id) = result.agent_id.clone() else {
                        let mut coordinator = vec![(coordinator_handle, coordinator_task)];
                        let _ = self.cancel_handles(&mut coordinator).await;
                        return Err(WorkflowNodeError::reconciliation_required(
                            "collaboration worker returned no stable Agent identity",
                        ));
                    };
                    let message_id = format!(
                        "workflow-worker-result:{}:{}:{}:{}",
                        context.run_id, context.node.id, context.attempt_id, worker_id
                    );
                    if let Err(error) = self.spawner.lifecycle_runtime().send_message_with_id(
                        message_id,
                        coordinator_handle.run_id.clone(),
                        coordinator_handle
                            .spec
                            .overrides
                            .collaboration
                            .as_ref()
                            .map(|value| value.team_id.clone()),
                        worker_id,
                        coordinator_handle.agent_id.clone(),
                        "worker_result",
                        json!({
                            "status": result.status,
                            "output": result.output,
                            "text": result.text,
                        }),
                    ) {
                        let mut coordinator = vec![(coordinator_handle, coordinator_task)];
                        let _ = self.cancel_handles(&mut coordinator).await;
                        return Err(WorkflowNodeError::reconciliation_required(format!(
                            "failed to deliver worker result to coordinator: {error}"
                        )));
                    }
                }
                let coordinator_result = self.join_handle(coordinator_handle, coordinator_task).await?;
                if coordinator_result.is_error {
                    return Err(workflow_failure_from_result(&coordinator_result));
                }
                Ok(coordinator_result)
            }
            CollaborationStrategy::Fanout => {
                let coordinator_id = self.spawner.parent_agent_id().clone();
                let collaboration = collaboration_context(
                    &context.run_id,
                    &context.node.id,
                    context.attempt_id.as_str(),
                    strategy,
                    coordinator_id.clone(),
                    None,
                )
                .expect("Fanout has collaboration context");
                let mut specs = Vec::with_capacity(worker_count);
                let mut worker_ids = Vec::with_capacity(worker_count);
                for index in 0..worker_count {
                    let suffix = format!("fanout:{index}");
                    let instruction = format!(
                        "You are independent Fanout worker {index}. Produce a complete candidate result without relying on other workers."
                    );
                    let worker = variant_spec(&base, &suffix, &instruction, collaboration.clone(), false);
                    worker_ids.push(self.resolved_agent_id(&worker));
                    specs.push((worker, true));
                }
                let plan = plan_collaboration(strategy, coordinator_id, worker_ids);
                debug_assert!(plan.requires_synthesis);
                let candidates = self.execute_specs(specs).await?;
                let candidate_values: Vec<_> = candidates
                    .iter()
                    .map(|result| json!({"agent_id": result.agent_id, "status": result.status, "text": result.text}))
                    .collect();
                let synth_prompt = format!(
                    "Synthesize the independent Fanout candidates below into one final answer matching the requested output schema. Resolve disagreements explicitly and return only the final typed JSON.\n\n{}",
                    serde_json::to_string_pretty(&candidate_values).unwrap_or_default()
                );
                let synth = variant_spec(&base, "synthesis", &synth_prompt, collaboration, true);
                let result = self
                    .execute_specs(vec![(synth, false)])
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(|| "Fanout synthesis returned no result".to_owned())?;
                if result.is_error {
                    return Err(workflow_failure_from_result(&result));
                }
                Ok(result)
            }
            CollaborationStrategy::IndependentReviewer => {
                let coordinator_id = self.spawner.parent_agent_id().clone();
                let collaboration = collaboration_context(
                    &context.run_id,
                    &context.node.id,
                    context.attempt_id.as_str(),
                    strategy,
                    coordinator_id.clone(),
                    None,
                )
                .expect("IndependentReviewer has collaboration context");
                let primary = variant_spec(
                    &base,
                    "candidate",
                    "Produce a complete candidate result. An isolated reviewer will verify it after you finish.",
                    collaboration.clone(),
                    false,
                );
                let primary_id = self.resolved_agent_id(&primary);
                let plan = plan_collaboration(strategy, coordinator_id, vec![primary_id]);
                debug_assert!(!plan.direct_peer_messaging);
                let candidate = self
                    .execute_specs(vec![(primary, true)])
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(|| "IndependentReviewer candidate returned no result".to_owned())?;
                if candidate.is_error {
                    return Err(workflow_failure_from_result(&candidate));
                }
                let review_prompt = format!(
                    "Act as an independent reviewer. Verify the candidate against the original typed input and output schema. Correct every defect you find, and return only the final typed JSON. Do not trust the candidate's claims.\n\nCandidate:\n{}",
                    candidate.text
                );
                let reviewer = variant_spec(&base, "reviewer", &review_prompt, collaboration, true);
                let result = self
                    .execute_specs(vec![(reviewer, false)])
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(|| "IndependentReviewer returned no review".to_owned())?;
                if result.is_error {
                    return Err(workflow_failure_from_result(&result));
                }
                Ok(result)
            }
        }
    }
}

#[async_trait]
impl WorkflowNodeExecutor for AgentWorkflowExecutor {
    async fn execute(&self, context: WorkflowExecutionContext) -> Result<Value, WorkflowNodeError> {
        let configured_final_role = match &context.node.collaboration {
            CollaborationSelection::Configured(config)
                if config.strategy == CollaborationStrategy::IndependentReviewer =>
            {
                config.reviewer_role.as_deref()
            }
            _ => None,
        };
        let role_id = context
            .node
            .role
            .as_deref()
            .or(configured_final_role)
            .ok_or_else(|| format!("workflow node {} has no executable role", context.node.id))?;
        let role = self
            .roles
            .get(role_id)
            .ok_or_else(|| format!("unknown agent role: {role_id}"))?;
        let max_turns = role.budget.max_turns.unwrap_or(120);
        let max_tokens = role
            .budget
            .max_tokens
            .unwrap_or(DEFAULT_WORKFLOW_ROLE_TOKEN_BUDGET)
            .min(u32::MAX as u64) as u32;
        let role_input = json!({
            "parameters": context.parameters,
            "dependency_outputs": context.dependency_outputs,
            "bound_inputs": context.bound_inputs,
        });
        let configured_multi = matches!(
            &context.node.collaboration,
            CollaborationSelection::Configured(config) if config.strategy != CollaborationStrategy::Single
        );
        if !configured_multi && let Some(schema) = &role.input_schema {
            validate_value(&role_input, schema).map_err(|error| format!("role {} input invalid: {error}", role.id))?;
        }
        let plugin_input = json!({
            "workflow_run_id": context.run_id,
            "node_id": context.node.id,
            "attempt_id": context.attempt_id,
            "role_id": role.id,
            "input": role_input,
        });
        if let Some(plugins) = &self.plugins {
            for (index, hook) in plugin_contribution_names(&context.parameters, &context.node.id, "before_hooks")
                .into_iter()
                .enumerate()
            {
                plugins
                    .invoke_scoped(
                        PluginContributionKind::Hook,
                        &hook,
                        contribution_input(&plugin_input, "before_hook", index),
                        Some(context.workflow.clone()),
                    )
                    .await?;
            }
        }
        let configured_collaboration = matches!(&context.node.collaboration, CollaborationSelection::Configured(_));
        let mut strategy = resolve_strategy(&context.node.collaboration, &context.parameters);
        if !configured_collaboration
            && let Some(plugins) = &self.plugins
            && let Some(name) =
                plugin_contribution_name(&context.parameters, &context.node.id, "collaboration_strategy")
        {
            let selected = plugins
                .invoke_scoped(
                    PluginContributionKind::CollaborationStrategy,
                    &name,
                    contribution_input(&plugin_input, "collaboration_strategy", 0),
                    Some(context.workflow.clone()),
                )
                .await?;
            let value = selected
                .as_str()
                .or_else(|| selected.get("strategy").and_then(Value::as_str))
                .ok_or_else(|| format!("plugin strategy {name} did not return a strategy"))?;
            strategy = parse_strategy(value).ok_or_else(|| format!("plugin strategy {name} returned {value}"))?;
        }
        if let CollaborationSelection::Configured(config) = &context.node.collaboration
            && strategy != CollaborationStrategy::Single
        {
            let output = self
                .execute_configured_collaboration(&context, config, &role_input)
                .await?;
            self.finish_plugin_contributions(&context, &plugin_input, &output)
                .await?;
            return Ok(output);
        }
        let prompt = format!(
            "Role: {}\n\n{}\n\nTyped workflow input:\n{}\n\nExpected output schema:\n{}\n\nReturn only valid JSON matching the output schema when one is provided. Do not wrap JSON in prose.",
            role.id,
            role.description,
            serde_json::to_string_pretty(&role_input).unwrap_or_default(),
            role.output_schema
                .as_ref()
                .and_then(|schema| serde_json::to_string_pretty(schema).ok())
                .unwrap_or_else(|| "null".to_owned()),
        );
        if let Some(plugins) = &self.plugins
            && let Some(name) = plugin_contribution_name(&context.parameters, &context.node.id, "provider")
        {
            if plugins.supports_provider_command_v1(&name) {
                let provider = PluginLlmProvider::new(name.clone(), Arc::clone(plugins))?;
                let model = context
                    .node
                    .model_policy
                    .model
                    .clone()
                    .or(role.model_policy.model.clone())
                    .unwrap_or_else(|| name.clone());
                let reasoning_effort = context
                    .node
                    .model_policy
                    .reasoning_effort
                    .clone()
                    .or(role.model_policy.reasoning_effort.clone());
                let (text, usage) = execute_plugin_provider(
                    &provider,
                    model,
                    role.description.clone(),
                    prompt,
                    max_tokens,
                    reasoning_effort,
                )
                .await?;
                self.record_usage(1, &usage);
                let output = normalize_role_output(&role.id, role.output_schema.as_ref(), &text)?;
                self.finish_plugin_contributions(&context, &plugin_input, &output)
                    .await?;
                return Ok(output);
            }
            let output = plugins
                .invoke_scoped(
                    PluginContributionKind::Provider,
                    &name,
                    contribution_input(&plugin_input, "provider", 0),
                    Some(context.workflow.clone()),
                )
                .await?;
            if let Some(schema) = &role.output_schema {
                validate_value(&output, schema).map_err(|error| format!("role {} output invalid: {error}", role.id))?;
            }
            self.finish_plugin_contributions(&context, &plugin_input, &output)
                .await?;
            return Ok(output);
        }
        let sub_config = SubAgentConfig {
            name: format!("{}:{}", context.node.id, role.id),
            prompt,
            max_turns,
            max_tokens,
            system_prompt: None,
        };
        let mut allowed_tools = if context.node.capability_scope.is_empty() {
            role.capability_scope.clone()
        } else {
            context.node.capability_scope.clone()
        };
        if strategy != CollaborationStrategy::Single {
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
        }
        let overrides = ForkOverrides {
            model: context
                .node
                .model_policy
                .model
                .clone()
                .or(role.model_policy.model.clone()),
            effort: context
                .node
                .model_policy
                .reasoning_effort
                .clone()
                .or(role.model_policy.reasoning_effort.clone()),
            allowed_tools,
            inherit_capabilities: false,
            collaboration: None,
            workflow: Some(context.workflow.clone()),
            execution_boundary: None,
        };
        let ceiling = role.permission_ceiling.intersect(context.node.permission_ceiling);
        let operation_id = OperationId::new(format!(
            "workflow:{}:{}:{}",
            context.run_id, context.node.id, context.attempt_id
        ));
        let spec = AgentSpawnSpec {
            run_id: self.spawner.run_id().clone(),
            parent_agent_id: self.spawner.parent_agent_id().clone(),
            task_id: TaskId::new(format!("workflow:{}:{}", context.run_id, context.node.id)),
            role_key: role.id.clone(),
            stable_task_key: format!("workflow:{}:{}:{}", context.run_id, context.node.id, context.attempt_id),
            operation_id: operation_id.clone(),
            expected_task_revision: self
                .spawner
                .lifecycle_runtime()
                .tasks()
                .get(&TaskId::new(format!("workflow:{}:{}", context.run_id, context.node.id)))
                .map(|task| task.revision),
            config: sub_config.clone(),
            overrides: overrides.clone(),
            permission_ceiling: ceiling,
            resource_budget: role.budget.clone(),
            context_policy: role.context_policy.clone(),
            recursion_limit: match role.recursion_policy.as_deref() {
                Some("none") => Some(0),
                Some("bounded") => Some(role.budget.max_spawn_depth.unwrap_or(1)),
                Some(other) => {
                    return Err(WorkflowNodeError::non_retryable(format!(
                        "unsupported recursion policy for role {}: {other}",
                        role.id
                    )));
                }
                None => None,
            },
        };
        let result = self.execute_collaboration(spec, &context, strategy).await?;
        if result.is_error {
            return Err(workflow_failure_from_result(&result));
        }
        let output = normalize_role_output(&role.id, role.output_schema.as_ref(), &result.text)?;
        self.finish_plugin_contributions(&context, &plugin_input, &output)
            .await?;
        Ok(output)
    }
}

impl AgentWorkflowExecutor {
    async fn finish_plugin_contributions(
        &self,
        context: &WorkflowExecutionContext,
        plugin_input: &Value,
        output: &Value,
    ) -> Result<(), String> {
        let Some(plugins) = &self.plugins else {
            return Ok(());
        };
        let completed = json!({"context": plugin_input, "output": output});
        if let Some(name) = plugin_contribution_name(&context.parameters, &context.node.id, "storage_backend") {
            plugins
                .invoke_scoped(
                    PluginContributionKind::StorageBackend,
                    &name,
                    contribution_input(&completed, "storage_backend", 0),
                    Some(context.workflow.clone()),
                )
                .await?;
        }
        for (index, hook) in plugin_contribution_names(&context.parameters, &context.node.id, "after_hooks")
            .into_iter()
            .enumerate()
        {
            plugins
                .invoke_scoped(
                    PluginContributionKind::Hook,
                    &hook,
                    contribution_input(&completed, "after_hook", index),
                    Some(context.workflow.clone()),
                )
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "workflow_executor_test.rs"]
mod tests;
