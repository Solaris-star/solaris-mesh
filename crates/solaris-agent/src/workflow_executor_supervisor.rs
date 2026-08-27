use std::collections::{BTreeMap, BTreeSet, HashMap};

use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use solaris_types::identity::{OperationId, TaskId, TeamId};
use solaris_types::permission::PermissionMode;
use solaris_types::runtime::{TaskFailureClass, TaskRecord, TaskState};
use solaris_types::spawner::{
    AgentConversationConfig, AgentConversationError, AgentConversationHandle, AgentConversationSpec, AgentOutcome,
    AgentOutcomeStatus, AgentSpawnService, AgentTurnOutcome, AgentTurnSpec, OutcomeBlobRef, SubAgentResult,
};
use solaris_types::workflow::{CollaborationRuntimeConfig, CollaborationStrategy, MultiAgentPolicy};
use tokio::sync::Semaphore;

use crate::execution_context::stable_digest_value;
use crate::spawner::AgentConversationService;
use crate::workflow_controller::{WorkflowExecutionContext, WorkflowNodeError};

use super::workflow_executor_configured::ConfiguredSpecRequest;
use super::*;

#[path = "workflow_executor_supervisor_records.rs"]
mod records;
#[path = "workflow_executor_supervisor_tasks.rs"]
mod tasks;

use records::{
    DurableSupervisorWorkerDelivery, DurableSupervisorWorkerOutcome, SUPERVISOR_SCHEMA_VERSION,
    SupervisorDecisionAccounting, SupervisorDecisionInputDelivery, SupervisorWorkerOutcomeBody,
    SupervisorWorkerResultFormat,
};
use tasks::{supervisor_task_state, supervisor_worker_boundary};

const HARD_MAX_SUPERVISOR_TASKS: usize = 32;
const HARD_MAX_COORDINATOR_ROUNDS: u32 = 8;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct SupervisorTaskProposal {
    task_key: String,
    role: String,
    instruction: String,
    #[serde(default)]
    depends_on: Vec<String>,
    #[serde(default, alias = "write_scope")]
    expected_write_scope: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
enum SupervisorDecision {
    Dispatch {
        tasks: Vec<SupervisorTaskProposal>,
    },
    Finalize {
        output: Value,
    },
    Abort {
        reason: String,
        #[serde(default)]
        failure_class: Option<TaskFailureClass>,
    },
}

struct SupervisorRuntime<'a> {
    context: &'a WorkflowExecutionContext,
    config: &'a CollaborationRuntimeConfig,
    role_input: &'a Value,
    conversation: AgentConversationService,
    handle: AgentConversationHandle,
    team_id: TeamId,
}

impl AgentWorkflowExecutor {
    pub(super) fn supervisor_coordinator_usage_delta(
        &self,
        context: &WorkflowExecutionContext,
        round: u32,
        cumulative: TokenUsage,
    ) -> Result<TokenUsage, WorkflowNodeError> {
        let mut previous = TokenUsage::default();
        for prior_round in 0..round {
            let Some(envelope) = self.load_supervisor_decision(context, prior_round)? else {
                return Err(WorkflowNodeError::reconciliation_required(format!(
                    "Supervisor round {round} is missing durable decision {prior_round}"
                )));
            };
            previous.input_tokens = previous.input_tokens.saturating_add(envelope.usage.input_tokens);
            previous.output_tokens = previous.output_tokens.saturating_add(envelope.usage.output_tokens);
            previous.cache_creation_tokens = previous
                .cache_creation_tokens
                .saturating_add(envelope.usage.cache_creation_tokens);
            previous.cache_read_tokens = previous
                .cache_read_tokens
                .saturating_add(envelope.usage.cache_read_tokens);
        }
        if cumulative.input_tokens < previous.input_tokens
            || cumulative.output_tokens < previous.output_tokens
            || cumulative.cache_creation_tokens < previous.cache_creation_tokens
            || cumulative.cache_read_tokens < previous.cache_read_tokens
        {
            return Err(WorkflowNodeError::reconciliation_required(format!(
                "Supervisor round {round} cumulative usage is lower than durable prior usage"
            )));
        }
        Ok(TokenUsage {
            input_tokens: cumulative.input_tokens - previous.input_tokens,
            output_tokens: cumulative.output_tokens - previous.output_tokens,
            cache_creation_tokens: cumulative.cache_creation_tokens - previous.cache_creation_tokens,
            cache_read_tokens: cumulative.cache_read_tokens - previous.cache_read_tokens,
        })
    }

    pub(super) fn find_supervisor_conversation_handle(
        &self,
        conversation_id: &str,
    ) -> Result<Option<AgentConversationHandle>, WorkflowNodeError> {
        let mut found = None;
        for record in self
            .spawner
            .lifecycle_runtime()
            .ledger()
            .records_for_run(self.spawner.run_id())
            .map_err(|error| WorkflowNodeError::reconciliation_required(error.to_string()))?
        {
            if record.record_type != "agent_conversation_opened"
                || record.payload.get("conversation_id").and_then(Value::as_str) != Some(conversation_id)
            {
                continue;
            }
            let handle: AgentConversationHandle = serde_json::from_value(record.payload).map_err(|error| {
                WorkflowNodeError::reconciliation_required(format!("invalid Supervisor handle: {error}"))
            })?;
            if found
                .as_ref()
                .is_some_and(|existing| serde_json::to_value(existing).ok() != serde_json::to_value(&handle).ok())
            {
                return Err(WorkflowNodeError::reconciliation_required(
                    "conflicting durable Supervisor conversation handles",
                ));
            }
            found = Some(handle);
        }
        Ok(found)
    }

    pub(super) fn validate_supervisor_handle(
        &self,
        spec: &AgentConversationSpec,
        handle: &AgentConversationHandle,
    ) -> Result<(), WorkflowNodeError> {
        let expected =
            serde_json::to_value(spec).map_err(|error| WorkflowNodeError::non_retryable(error.to_string()))?;
        let actual = serde_json::to_value(&handle.spec)
            .map_err(|error| WorkflowNodeError::reconciliation_required(error.to_string()))?;
        if expected != actual {
            return Err(WorkflowNodeError::reconciliation_required(
                "durable Supervisor conversation spec changed",
            ));
        }
        Ok(())
    }

    pub(super) async fn execute_configured_supervisor(
        &self,
        context: &WorkflowExecutionContext,
        config: &CollaborationRuntimeConfig,
        role_input: &Value,
    ) -> Result<Value, WorkflowNodeError> {
        self.ensure_supervisor_policy()?;
        let max_rounds = config.max_coordinator_rounds.min(HARD_MAX_COORDINATOR_ROUNDS);
        if max_rounds == 0 {
            return Err(WorkflowNodeError::non_retryable(
                "configured Supervisor requires at least one coordinator round",
            ));
        }
        let runtime = self.open_supervisor(context, config, role_input).await?;
        let mut known = BTreeMap::<String, SupervisorTaskProposal>::new();
        let result = self
            .run_configured_supervisor_rounds(&runtime, max_rounds, &mut known)
            .await;
        match result {
            Ok(output) => {
                runtime
                    .conversation
                    .close(&runtime.handle)
                    .await
                    .map_err(workflow_error_from_conversation)?;
                Ok(output)
            }
            Err(error) if supervisor_error_requires_recovery(&error) => Err(error),
            Err(error) => {
                let task_cleanup = self.terminate_supervisor_tasks(&runtime, &known).await;
                let conversation_cleanup = runtime.conversation.close(&runtime.handle).await;
                let mut cleanup_errors = Vec::new();
                if let Err(cleanup) = task_cleanup {
                    cleanup_errors.push(format!("Supervisor task cleanup failed: {cleanup}"));
                }
                if let Err(cleanup) = conversation_cleanup {
                    cleanup_errors.push(format!("Supervisor conversation cleanup failed: {cleanup}"));
                }
                supervisor_result_after_cleanup(error, cleanup_errors)
            }
        }
    }

    async fn run_configured_supervisor_rounds(
        &self,
        runtime: &SupervisorRuntime<'_>,
        max_rounds: u32,
        known: &mut BTreeMap<String, SupervisorTaskProposal>,
    ) -> Result<Value, WorkflowNodeError> {
        for round in 0..max_rounds {
            let pending = self.pending_supervisor_messages(runtime)?;
            let (envelope, durable) = match self.load_supervisor_decision(runtime.context, round)? {
                Some(envelope) => (envelope, true),
                None => {
                    let (decision, turns, usage, outcome_ref) =
                        self.run_supervisor_turn(runtime, round, &pending).await?;
                    self.validate_supervisor_decision(runtime.config, known, &decision, false)?;
                    let envelope = self.supervisor_decision_envelope(
                        runtime.context,
                        round,
                        &pending,
                        decision,
                        SupervisorDecisionAccounting {
                            turns,
                            usage,
                            turn_outcome_ref: outcome_ref,
                        },
                    )?;
                    self.persist_supervisor_decision(&envelope)?;
                    (envelope, false)
                }
            };
            self.validate_supervisor_envelope(runtime.context, round, &envelope)?;
            self.validate_supervisor_decision_delivery_chain(runtime, &envelope)?;
            self.record_usage_once(
                format!(
                    "workflow-supervisor-coordinator:{}:{}:{}:{round}",
                    runtime.context.run_id, runtime.context.node.id, runtime.context.attempt_id
                ),
                envelope.turns,
                &envelope.usage,
            );
            self.validate_supervisor_decision(runtime.config, known, &envelope.decision, durable)?;
            let observed_messages = pending
                .into_iter()
                .filter(|message| {
                    envelope.input_deliveries.iter().any(|input| {
                        input.message_id == message.message_id && input.delivery_digest == message.delivery_digest
                    })
                })
                .collect();
            self.acknowledge_supervisor_messages(runtime, &envelope, observed_messages)?;

            match envelope.decision {
                SupervisorDecision::Dispatch { tasks } => {
                    self.create_supervisor_tasks(runtime, round, known, &tasks)?;
                    for task in &tasks {
                        known.insert(task.task_key.clone(), task.clone());
                    }
                    self.run_ready_supervisor_tasks(runtime, known).await?;
                    self.persist_supervisor_worker_delivery(runtime, round, &tasks)?;
                }
                SupervisorDecision::Finalize { output } => {
                    self.validate_supervisor_final_output(runtime, &output)?;
                    self.require_supervisor_finalize_gate(runtime, known)?;
                    return Ok(output);
                }
                SupervisorDecision::Abort { reason, failure_class } => {
                    return Err(workflow_error(
                        failure_class.unwrap_or(TaskFailureClass::NonRetryable),
                        reason,
                    ));
                }
            }
        }
        Err(WorkflowNodeError::non_retryable(format!(
            "configured Supervisor exceeded its {max_rounds} coordinator rounds"
        )))
    }

    #[cfg(test)]
    pub(super) async fn validate_supervisor_pending_delivery_integrity_for_test(
        &self,
        context: &WorkflowExecutionContext,
        config: &CollaborationRuntimeConfig,
    ) -> Result<(), WorkflowNodeError> {
        records::validate_supervisor_pending_delivery_integrity_for_test(self, context, config).await
    }

    fn ensure_supervisor_policy(&self) -> Result<(), WorkflowNodeError> {
        let policy = *self
            .spawner
            .multi_agent_policy_state()
            .read()
            .unwrap_or_else(|error| error.into_inner());
        match policy {
            MultiAgentPolicy::Disabled => Err(WorkflowNodeError::non_retryable(
                "multi-agent policy is disabled for this Run",
            )),
            MultiAgentPolicy::OnDemand | MultiAgentPolicy::Proactive => Ok(()),
        }
    }

    async fn open_supervisor<'a>(
        &self,
        context: &'a WorkflowExecutionContext,
        config: &'a CollaborationRuntimeConfig,
        role_input: &'a Value,
    ) -> Result<SupervisorRuntime<'a>, WorkflowNodeError> {
        let coordinator_role_id = context
            .node
            .role
            .as_deref()
            .ok_or_else(|| WorkflowNodeError::non_retryable("configured Supervisor requires a coordinator role"))?;
        let coordinator_role = self
            .roles
            .get(coordinator_role_id)
            .ok_or_else(|| WorkflowNodeError::non_retryable(format!("unknown agent role: {coordinator_role_id}")))?;
        let mut base = self.configured_role_spec(ConfiguredSpecRequest {
            context,
            role: &coordinator_role,
            role_input,
            strategy: CollaborationStrategy::Supervisor,
            collaboration: None,
            suffix: "supervisor-coordinator",
            instruction: "Act as the durable coordinator. Return one typed Supervisor decision per turn.",
            owns_workflow_task: true,
        })?;
        base.overrides
            .allowed_tools
            .retain(|tool| matches!(tool.as_str(), "Read" | "Grep" | "Glob"));
        let conversation_id = supervisor_conversation_id(context);
        let spec = AgentConversationSpec {
            run_id: base.run_id,
            parent_agent_id: base.parent_agent_id,
            task_id: base.task_id,
            conversation_id: conversation_id.clone(),
            role_key: base.role_key,
            stable_task_key: format!("{}:supervisor-coordinator", base.stable_task_key),
            operation_id: OperationId::new(format!(
                "workflow-supervisor-open:{}:{}:{}",
                context.run_id, context.node.id, context.attempt_id
            )),
            config: AgentConversationConfig {
                name: base.config.name,
                max_turns: base.config.max_turns,
                max_tokens: base.config.max_tokens,
                system_prompt: base.config.system_prompt,
            },
            overrides: base.overrides,
            permission_ceiling: base.permission_ceiling,
            resource_budget: base.resource_budget,
            context_policy: base.context_policy,
            recursion_limit: base.recursion_limit,
        };
        let conversation = AgentConversationService::new(Arc::clone(&self.spawner));
        let handle = match self.find_supervisor_conversation_handle(&conversation_id)? {
            Some(handle) => {
                self.validate_supervisor_handle(&spec, &handle)?;
                handle
            }
            None => conversation
                .open(spec)
                .await
                .map_err(workflow_error_from_conversation)?,
        };
        let team_id = TeamId::new(format!(
            "workflow:{}:{}:{}:supervisor",
            context.run_id, context.node.id, context.attempt_id
        ));
        let collaboration = AgentCollaborationContext {
            team_id: team_id.clone(),
            strategy: CollaborationStrategy::Supervisor,
            coordinator_agent_id: handle.agent_id.clone(),
            max_pending_messages: config.max_pending_messages,
            max_message_bytes: config.max_message_bytes,
        };
        let lifecycle = self.spawner.lifecycle_runtime();
        lifecycle
            .ensure_collaboration_team(self.spawner.run_id().clone(), "workflow Supervisor", &collaboration)
            .map_err(|error| WorkflowNodeError::reconciliation_required(error.to_string()))?;
        lifecycle
            .join_team(self.spawner.run_id(), &team_id, handle.agent_id.clone())
            .map_err(|error| WorkflowNodeError::reconciliation_required(error.to_string()))?;
        Ok(SupervisorRuntime {
            context,
            config,
            role_input,
            conversation,
            handle,
            team_id,
        })
    }

    async fn run_supervisor_turn(
        &self,
        runtime: &SupervisorRuntime<'_>,
        round: u32,
        messages: &[DurableSupervisorWorkerDelivery],
    ) -> Result<(SupervisorDecision, usize, TokenUsage, Option<OutcomeBlobRef>), WorkflowNodeError> {
        let message_values: Vec<_> = messages
            .iter()
            .map(|message| self.supervisor_delivery_prompt_value(message))
            .collect::<Result<Vec<_>, _>>()?;
        let prompt = format!(
            "Supervisor round {round}. Typed workflow input:\n{}\n\nUnacknowledged durable worker results:\n{}\n\nReturn exactly one JSON decision. Dispatch shape: {{\"decision\":\"dispatch\",\"tasks\":[{{\"task_key\":\"stable-key\",\"role\":\"configured-role\",\"instruction\":\"work\",\"depends_on\":[],\"expected_write_scope\":[]}}]}}. Finalize shape: {{\"decision\":\"finalize\",\"output\":{{}}}}. Abort shape: {{\"decision\":\"abort\",\"reason\":\"reason\",\"failure_class\":\"non_retryable\"}}.",
            serde_json::to_string_pretty(runtime.role_input).unwrap_or_default(),
            serde_json::to_string_pretty(&message_values).unwrap_or_default(),
        );
        let parse = |text: &str| parse_structured_result(text).ok();
        let outcome = runtime
            .conversation
            .run_turn_with_json_text_projection(
                &runtime.handle,
                AgentTurnSpec {
                    turn_id: supervisor_turn_id(runtime.context, round),
                    prompt,
                },
                &runtime.context.run_id,
                "/output",
                &parse,
            )
            .await
            .map_err(workflow_error_from_conversation)?;
        require_completed_turn(&outcome)?;
        let text = outcome
            .output
            .get("text")
            .and_then(Value::as_str)
            .or_else(|| outcome.output.as_str())
            .ok_or_else(|| WorkflowNodeError::non_retryable("Supervisor turn returned no textual decision"))?;
        let value = parse_structured_result(text)
            .map_err(|error| WorkflowNodeError::non_retryable(format!("invalid Supervisor decision JSON: {error}")))?;
        let decision = serde_json::from_value(value)
            .map_err(|error| WorkflowNodeError::non_retryable(format!("invalid Supervisor decision: {error}")))?;
        let usage = self.supervisor_coordinator_usage_delta(runtime.context, round, outcome.usage)?;
        Ok((decision, outcome.turns, usage, outcome.outcome_ref))
    }

    fn create_supervisor_tasks(
        &self,
        runtime: &SupervisorRuntime<'_>,
        round: u32,
        known: &BTreeMap<String, SupervisorTaskProposal>,
        proposals: &[SupervisorTaskProposal],
    ) -> Result<(), WorkflowNodeError> {
        let ids: BTreeMap<_, _> = known
            .keys()
            .chain(proposals.iter().map(|proposal| &proposal.task_key))
            .map(|key| (key.clone(), supervisor_task_id(runtime.context, key)))
            .collect();
        for proposal in proposals {
            let task = TaskRecord {
                run_id: self.spawner.run_id().clone(),
                task_id: ids[&proposal.task_key].clone(),
                revision: 0,
                task_key: Some(proposal.task_key.clone()),
                team_id: Some(runtime.team_id.clone()),
                workflow_id: Some(runtime.context.workflow.implementation_id.clone()),
                node_id: None,
                role: Some(proposal.role.clone()),
                depends_on: proposal.depends_on.iter().map(|key| ids[key].clone()).collect(),
                content: Some(json!({
                    "workflow_node_id": runtime.context.node.id,
                    "instruction": proposal.instruction,
                    "decision_round": round,
                    "expected_write_scope_semantics": if self.spawner.permission_mode() == PermissionMode::Bypass {
                        "advisory"
                    } else {
                        "enforced"
                    },
                })),
                expected_write_scope: proposal.expected_write_scope.clone(),
                owner_agent_id: None,
                state: TaskState::Queued,
                outcome_ref: None,
                failure_class: None,
            };
            self.spawner
                .lifecycle_runtime()
                .create_collaboration_task(
                    self.spawner.run_id(),
                    &runtime.team_id,
                    &runtime.handle.agent_id,
                    self.spawner.max_tasks_per_run(),
                    task,
                )
                .map_err(|error| WorkflowNodeError::reconciliation_required(error.to_string()))?;
        }
        Ok(())
    }

    async fn run_ready_supervisor_tasks(
        &self,
        runtime: &SupervisorRuntime<'_>,
        known: &BTreeMap<String, SupervisorTaskProposal>,
    ) -> Result<(), WorkflowNodeError> {
        let global_limit = runtime.config.max_concurrent_workers.max(1) as usize;
        let role_limits: HashMap<_, _> = runtime
            .config
            .worker_roles
            .iter()
            .map(|policy| {
                (
                    policy.role.clone(),
                    Arc::new(Semaphore::new(policy.max_concurrent.max(1) as usize)),
                )
            })
            .collect();
        let mut processed = BTreeSet::new();
        loop {
            let ready: Vec<_> = known
                .values()
                .filter(|proposal| {
                    if processed.contains(&proposal.task_key) {
                        return false;
                    }
                    self.spawner
                        .lifecycle_runtime()
                        .tasks()
                        .get(&supervisor_task_id(runtime.context, &proposal.task_key))
                        .is_some_and(|task| {
                            task.depends_on.iter().all(|dependency| {
                                self.spawner
                                    .lifecycle_runtime()
                                    .tasks()
                                    .get(dependency)
                                    .is_some_and(|dependency| dependency.state == TaskState::Completed)
                            })
                        })
                })
                .cloned()
                .collect();
            if ready.is_empty() {
                return Ok(());
            }
            processed.extend(ready.iter().map(|proposal| proposal.task_key.clone()));
            let results = stream::iter(ready.into_iter().map(|proposal| {
                let role_limit = role_limits.get(&proposal.role).cloned();
                async move {
                    let role_limit = role_limit.ok_or_else(|| {
                        WorkflowNodeError::non_retryable(format!(
                            "configured Supervisor role {} has no concurrency policy",
                            proposal.role
                        ))
                    })?;
                    let _permit = role_limit
                        .acquire_owned()
                        .await
                        .map_err(|_| WorkflowNodeError::non_retryable("Supervisor role semaphore closed"))?;
                    self.execute_supervisor_task(runtime, &proposal).await
                }
            }))
            .buffer_unordered(global_limit)
            .collect::<Vec<_>>()
            .await;
            for result in results {
                result?;
            }
        }
    }

    async fn execute_supervisor_task(
        &self,
        runtime: &SupervisorRuntime<'_>,
        proposal: &SupervisorTaskProposal,
    ) -> Result<(), WorkflowNodeError> {
        let task_id = supervisor_task_id(runtime.context, &proposal.task_key);
        let operation_id = supervisor_worker_operation_id(runtime.context, &proposal.task_key);
        let (durable, body) =
            match self.load_supervisor_worker_outcome(runtime.context, proposal, &task_id, &operation_id)? {
                Some(outcome) => {
                    let body = self.read_supervisor_worker_body(&outcome.value)?;
                    (outcome.value, body)
                }
                None => {
                    let task = self
                        .spawner
                        .lifecycle_runtime()
                        .tasks()
                        .get(&task_id)
                        .ok_or_else(|| WorkflowNodeError::reconciliation_required("Supervisor Task disappeared"))?;
                    let handle = if task.state == TaskState::Queued {
                        self.spawner
                            .spawn(self.supervisor_worker_spec(runtime, proposal, task.revision)?)
                            .await
                            .map_err(WorkflowNodeError::from)?
                    } else {
                        self.find_supervisor_worker_handle(&task_id, &operation_id)?
                            .ok_or_else(|| {
                                WorkflowNodeError::reconciliation_required(format!(
                                    "Supervisor Task {task_id} has no durable Agent handle"
                                ))
                            })?
                    };
                    if self.find_agent_outcome(&handle)?.is_none() {
                        self.join_supervisor_worker(&handle).await?;
                    }
                    let (result, result_ref) = self.find_agent_outcome(&handle)?.ok_or_else(|| {
                        WorkflowNodeError::reconciliation_required(
                            "joined Supervisor worker has no durable Agent outcome",
                        )
                    })?;
                    let (durable, body) =
                        self.build_supervisor_worker_outcome(runtime.context, proposal, &handle, result, result_ref)?;
                    self.persist_supervisor_worker_outcome(&durable)?;
                    (durable, body)
                }
            };
        self.record_usage_once(
            format!("workflow-supervisor-worker:{}", durable.operation_id),
            body.result.turns,
            &body.result.usage,
        );
        self.settle_supervisor_worker(&durable)?;
        Ok(())
    }

    fn supervisor_worker_spec(
        &self,
        runtime: &SupervisorRuntime<'_>,
        proposal: &SupervisorTaskProposal,
        expected_revision: u64,
    ) -> Result<AgentSpawnSpec, WorkflowNodeError> {
        let role = self
            .roles
            .get(&proposal.role)
            .ok_or_else(|| WorkflowNodeError::non_retryable(format!("unknown agent role: {}", proposal.role)))?;
        let task_input = json!({
            "workflow_input": runtime.role_input,
            "supervisor_task": proposal,
        });
        if let Some(schema) = &role.input_schema {
            validate_value(&task_input, schema).map_err(|error| {
                WorkflowNodeError::non_retryable(format!("role {} input invalid: {error}", role.id))
            })?;
        }
        let collaboration = AgentCollaborationContext {
            team_id: runtime.team_id.clone(),
            strategy: CollaborationStrategy::Supervisor,
            coordinator_agent_id: runtime.handle.agent_id.clone(),
            max_pending_messages: runtime.config.max_pending_messages,
            max_message_bytes: runtime.config.max_message_bytes,
        };
        let mut spec = self.configured_role_spec(ConfiguredSpecRequest {
            context: runtime.context,
            role: &role,
            role_input: &task_input,
            strategy: CollaborationStrategy::Supervisor,
            collaboration: Some(collaboration),
            suffix: &format!("supervisor:{}", proposal.task_key),
            instruction: &proposal.instruction,
            owns_workflow_task: false,
        })?;
        spec.task_id = supervisor_task_id(runtime.context, &proposal.task_key);
        spec.stable_task_key = format!(
            "workflow-supervisor-task:{}:{}:{}:{}",
            runtime.context.run_id, runtime.context.node.id, runtime.context.attempt_id, proposal.task_key
        );
        spec.operation_id = supervisor_worker_operation_id(runtime.context, &proposal.task_key);
        spec.expected_task_revision = Some(expected_revision);
        spec.overrides.execution_boundary = supervisor_worker_boundary(self.spawner.permission_mode(), proposal);
        Ok(spec)
    }

    async fn join_supervisor_worker(&self, handle: &AgentHandle) -> Result<SubAgentResult, WorkflowNodeError> {
        let outcome = self
            .spawner
            .join(handle)
            .await
            .map_err(WorkflowNodeError::reconciliation_required)?;
        Ok(sub_agent_result_from_outcome(outcome))
    }

    fn build_supervisor_worker_outcome(
        &self,
        context: &WorkflowExecutionContext,
        proposal: &SupervisorTaskProposal,
        handle: &AgentHandle,
        result: SubAgentResult,
        agent_outcome_ref: Option<OutcomeBlobRef>,
    ) -> Result<(DurableSupervisorWorkerOutcome, SupervisorWorkerOutcomeBody), WorkflowNodeError> {
        let agent_outcome_status = result.status;
        let body = self.supervisor_worker_body(&proposal.role, result);
        let failure_class = body.failure_class;
        let status = body.result.status;
        let (result_ref, result_bytes, result_digest, result_ref_status, result_format) =
            if let Some(reference) = agent_outcome_ref {
                (
                    reference.reference,
                    reference.bytes,
                    reference.digest,
                    reference.status.or(Some(agent_outcome_status)),
                    SupervisorWorkerResultFormat::AgentOutcome,
                )
            } else {
                let (reference, bytes, digest) = self.store_supervisor_worker_body(&handle.operation_id, &body)?;
                (
                    reference,
                    bytes,
                    digest,
                    Some(status),
                    SupervisorWorkerResultFormat::LegacySupervisorBody,
                )
            };
        let proposal_digest = stable_digest_value(
            &serde_json::to_value(proposal).map_err(|error| WorkflowNodeError::non_retryable(error.to_string()))?,
        );
        let mut durable = DurableSupervisorWorkerOutcome {
            schema_version: SUPERVISOR_SCHEMA_VERSION,
            workflow_run_id: context.run_id.clone(),
            workflow_id: context.workflow.implementation_id.clone(),
            node_id: context.node.id.clone(),
            attempt_id: context.attempt_id.clone(),
            task_key: proposal.task_key.clone(),
            task_id: handle.task_id.clone(),
            role: proposal.role.clone(),
            proposal_digest,
            agent_id: handle.agent_id.clone(),
            operation_id: handle.operation_id.clone(),
            handle_spec_digest: handle.spec_digest.clone(),
            result_ref,
            result_bytes,
            result_digest,
            result_ref_status,
            result_format,
            status,
            failure_class,
            outcome_digest: String::new(),
        };
        durable.outcome_digest = records::supervisor_worker_outcome_digest(&durable);
        Ok((durable, body))
    }

    fn supervisor_worker_body(&self, role: &str, mut result: SubAgentResult) -> SupervisorWorkerOutcomeBody {
        let (normalized_output, failure_class) = match result.status {
            AgentOutcomeStatus::Completed => match self.configured_output(role, &result) {
                Ok(output) => (Some(output), None),
                Err(error) => {
                    result.status = AgentOutcomeStatus::Failed;
                    result.output = None;
                    result.text = error.message;
                    result.is_error = true;
                    (None, Some(error.failure_class))
                }
            },
            AgentOutcomeStatus::Cancelled => (None, Some(TaskFailureClass::Cancelled)),
            AgentOutcomeStatus::Failed => (
                None,
                Some(result.failure_class.unwrap_or(TaskFailureClass::NonRetryable)),
            ),
            AgentOutcomeStatus::OutcomeUnknown => (None, Some(TaskFailureClass::OutcomeUnknown)),
            AgentOutcomeStatus::ReconciliationRequired => (None, Some(TaskFailureClass::ReconciliationRequired)),
        };
        SupervisorWorkerOutcomeBody {
            result,
            normalized_output,
            failure_class,
        }
    }

    fn settle_supervisor_worker(&self, outcome: &DurableSupervisorWorkerOutcome) -> Result<(), WorkflowNodeError> {
        let task = self
            .spawner
            .lifecycle_runtime()
            .tasks()
            .get(&outcome.task_id)
            .ok_or_else(|| WorkflowNodeError::reconciliation_required("Supervisor Task disappeared"))?;
        if matches!(
            task.state,
            TaskState::Completed | TaskState::Failed | TaskState::Cancelled
        ) {
            let expected_state = supervisor_task_state(outcome);
            if task.state != expected_state || task.failure_class != outcome.failure_class {
                return Err(WorkflowNodeError::reconciliation_required(
                    "terminal Supervisor Task conflicts with its durable worker outcome",
                ));
            }
            return Ok(());
        }
        let handle = self
            .find_supervisor_worker_handle(&outcome.task_id, &outcome.operation_id)?
            .ok_or_else(|| WorkflowNodeError::reconciliation_required("worker handle disappeared"))?;
        let task_operation_id = OperationId::new(format!(
            "{}:task-settle:{:?}",
            handle.operation_id,
            supervisor_task_state(outcome)
        ));
        self.spawner
            .lifecycle_runtime()
            .settle_collaboration_task(
                &handle.run_id,
                &handle.task_id,
                &handle.agent_id,
                task.revision,
                &task_operation_id,
                TaskSettlement {
                    state: supervisor_task_state(outcome),
                    outcome_ref: Some(outcome.result_ref.clone()),
                    failure_class: outcome.failure_class,
                },
            )
            .map(|_| ())
            .map_err(|error| WorkflowNodeError::reconciliation_required(error.to_string()))
    }

    fn require_supervisor_finalize_gate(
        &self,
        runtime: &SupervisorRuntime<'_>,
        known: &BTreeMap<String, SupervisorTaskProposal>,
    ) -> Result<(), WorkflowNodeError> {
        if !self.pending_supervisor_messages(runtime)?.is_empty() {
            return Err(WorkflowNodeError::reconciliation_required(
                "Supervisor cannot finalize with unacknowledged worker results",
            ));
        }
        for proposal in known.values() {
            let task = self
                .spawner
                .lifecycle_runtime()
                .tasks()
                .get(&supervisor_task_id(runtime.context, &proposal.task_key))
                .ok_or_else(|| WorkflowNodeError::reconciliation_required("Supervisor Task disappeared"))?;
            match task.state {
                TaskState::Completed if task.failure_class.is_none() => {}
                TaskState::Failed => {
                    return Err(workflow_error(
                        task.failure_class.unwrap_or(TaskFailureClass::NonRetryable),
                        format!("Supervisor Task {} failed", proposal.task_key),
                    ));
                }
                TaskState::Queued | TaskState::Assigned | TaskState::Running | TaskState::Created => {
                    return Err(WorkflowNodeError::non_retryable(format!(
                        "Supervisor Task {} is not terminal",
                        proposal.task_key
                    )));
                }
                TaskState::Skipped | TaskState::Cancelled | TaskState::Completed => {
                    return Err(WorkflowNodeError::non_retryable(format!(
                        "Supervisor Task {} did not complete successfully",
                        proposal.task_key
                    )));
                }
            }
        }
        Ok(())
    }

    fn validate_supervisor_final_output(
        &self,
        runtime: &SupervisorRuntime<'_>,
        output: &Value,
    ) -> Result<(), WorkflowNodeError> {
        let role_id = runtime
            .context
            .node
            .role
            .as_deref()
            .ok_or_else(|| WorkflowNodeError::non_retryable("configured Supervisor has no coordinator role"))?;
        let role = self
            .roles
            .get(role_id)
            .ok_or_else(|| WorkflowNodeError::non_retryable(format!("unknown agent role: {role_id}")))?;
        if let Some(schema) = &role.output_schema {
            validate_value(output, schema).map_err(|error| {
                WorkflowNodeError::non_retryable(format!("role {} output invalid: {error}", role.id))
            })?;
        }
        Ok(())
    }
}

fn supervisor_conversation_id(context: &WorkflowExecutionContext) -> String {
    format!(
        "workflow-supervisor:{}:{}:{}",
        context.run_id, context.node.id, context.attempt_id
    )
}

fn supervisor_turn_id(context: &WorkflowExecutionContext, round: u32) -> String {
    format!("{}:round:{round}", supervisor_conversation_id(context))
}

fn supervisor_error_requires_recovery(error: &WorkflowNodeError) -> bool {
    matches!(
        error.failure_class,
        TaskFailureClass::OutcomeUnknown
            | TaskFailureClass::SideEffectUnknown
            | TaskFailureClass::ReconciliationRequired
    )
}

#[cfg(test)]
#[test]
fn unknown_supervisor_side_effect_preserves_recovery_state() {
    for class in [
        TaskFailureClass::OutcomeUnknown,
        TaskFailureClass::SideEffectUnknown,
        TaskFailureClass::ReconciliationRequired,
    ] {
        assert!(supervisor_error_requires_recovery(&workflow_error(class, "unknown")));
    }
    for class in [
        TaskFailureClass::Retryable,
        TaskFailureClass::NonRetryable,
        TaskFailureClass::PermissionDenied,
        TaskFailureClass::MaxTurns,
        TaskFailureClass::NonConvergent,
        TaskFailureClass::Cancelled,
    ] {
        assert!(!supervisor_error_requires_recovery(&workflow_error(class, "terminal")));
    }
}

fn supervisor_result_after_cleanup(
    original: WorkflowNodeError,
    cleanup_errors: Vec<String>,
) -> Result<Value, WorkflowNodeError> {
    if cleanup_errors.is_empty() {
        return Err(original);
    }
    Err(WorkflowNodeError::reconciliation_required(format!(
        "{}; {}",
        original.message,
        cleanup_errors.join("; ")
    )))
}

fn supervisor_task_id(context: &WorkflowExecutionContext, task_key: &str) -> TaskId {
    let digest = stable_digest_value(&json!({
        "workflow_run_id": context.run_id,
        "node_id": context.node.id,
        "attempt_id": context.attempt_id,
        "task_key": task_key,
    }));
    TaskId::new(format!("workflow-supervisor-task:{digest}"))
}

fn supervisor_worker_operation_id(context: &WorkflowExecutionContext, task_key: &str) -> OperationId {
    OperationId::new(format!(
        "workflow-supervisor-worker:{}",
        stable_digest_value(&json!({
            "workflow_run_id": context.run_id,
            "node_id": context.node.id,
            "attempt_id": context.attempt_id,
            "task_key": task_key,
        }))
    ))
}

fn supervisor_worker_message_id(context: &WorkflowExecutionContext, dispatch_round: u32) -> String {
    format!(
        "workflow-supervisor-worker-results:{}:{}:{}:round:{dispatch_round}",
        context.run_id, context.node.id, context.attempt_id
    )
}

fn supervisor_decision_inputs(deliveries: &[DurableSupervisorWorkerDelivery]) -> Vec<SupervisorDecisionInputDelivery> {
    let mut inputs: Vec<_> = deliveries
        .iter()
        .map(|delivery| SupervisorDecisionInputDelivery {
            message_id: delivery.message_id.clone(),
            delivery_digest: delivery.delivery_digest.clone(),
        })
        .collect();
    inputs.sort_by(|left, right| {
        (&left.message_id, &left.delivery_digest).cmp(&(&right.message_id, &right.delivery_digest))
    });
    inputs
}

fn sub_agent_result_from_outcome(outcome: AgentOutcome) -> SubAgentResult {
    let text = outcome
        .output
        .get("text")
        .and_then(Value::as_str)
        .or(outcome.error.as_deref())
        .unwrap_or_default()
        .to_owned();
    SubAgentResult {
        name: outcome.handle.spec.config.name,
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

fn require_completed_turn(outcome: &AgentTurnOutcome) -> Result<(), WorkflowNodeError> {
    if outcome.status == AgentOutcomeStatus::Completed {
        return Ok(());
    }
    Err(workflow_error(
        outcome
            .failure_class
            .unwrap_or_else(|| conservative_failure_class(outcome.status)),
        outcome
            .error
            .clone()
            .unwrap_or_else(|| "Supervisor coordinator turn failed".to_owned()),
    ))
}

fn conservative_failure_class(status: AgentOutcomeStatus) -> TaskFailureClass {
    match status {
        AgentOutcomeStatus::Completed | AgentOutcomeStatus::Failed => TaskFailureClass::NonRetryable,
        AgentOutcomeStatus::Cancelled => TaskFailureClass::Cancelled,
        AgentOutcomeStatus::OutcomeUnknown => TaskFailureClass::OutcomeUnknown,
        AgentOutcomeStatus::ReconciliationRequired => TaskFailureClass::ReconciliationRequired,
    }
}

fn workflow_error_from_conversation(error: AgentConversationError) -> WorkflowNodeError {
    workflow_error(error.failure_class, error.message)
}

fn workflow_error(failure_class: TaskFailureClass, message: impl Into<String>) -> WorkflowNodeError {
    WorkflowNodeError {
        failure_class,
        message: message.into(),
    }
}
