use std::sync::Arc;

mod provider_turn;
mod tool_result_output;

use self::tool_result_output::emit_tool_results_to_sink;
use super::task_phase::DurableTaskResume;
use super::{AgentEngine, AgentResult, ToolRoundOutput, build_assistant_message, merge_provider_metadata};
use crate::cache_diagnostics::{CacheDiagnostic, CacheStats};
use crate::compact::auto::{CompactError, autocompact_with_effect, should_autocompact};
use crate::compact::emergency::is_at_emergency_limit;
use crate::compact::estimate::estimate_tokens_from_messages;
use crate::compact::micro::{microcompact_with_observer, should_microcompact};
use crate::error::AgentError;
use crate::execution_context::DurableTaskPhase;
use crate::orchestration::{
    ExecutionControl, execute_tool_calls_with_approval_context, execute_tool_calls_with_policy_context,
};
use crate::plan::prompt::plan_mode_instructions;
use crate::run_preset::multi_agent_policy_instructions;
use crate::stream::StreamOutcome;
use crate::tool_call::{
    merge_tool_results, tool_call_failure_fingerprint, tool_call_malformed_fingerprint, tool_call_malformed_reason,
};
use crate::turn::{FinalizationReason, TurnGuardAction, TurnGuards, TurnKind, TurnOutcome};
use serde_json::to_string;
use solaris_types::llm::{LlmEvent, LlmRequest};
use solaris_types::message::{ContentBlock, Message, Role, StopReason, TokenUsage};
use solaris_types::permission::PermissionMode;
use solaris_types::provider_contract::ProviderNativeMetadata;
use solaris_types::runtime::TaskFailureClass;
use solaris_types::spawner::AgentOutcomeStatus;
use solaris_types::tool::ToolResultStatus;
use tokio::sync::mpsc::Receiver;
use tracing::{Instrument, debug, error, info, info_span, warn};

const MAX_MAX_TOKENS_CONTINUATIONS: usize = 3;
const ENGINE_ERROR_CODE: &str = "engine_error";
const AUTOCOMPACT_FAILURE_DIAGNOSTIC: &str = "Automatic context compaction failed; continuing without compaction.";

impl AgentEngine {
    /// Run the agent loop with user input
    pub async fn run(&mut self, user_input: &str, msg_id: &str) -> Result<AgentResult, AgentError> {
        let session_id = self.current_session.as_ref().map(|s| s.id.clone()).unwrap_or_default();
        let span = info_span!(
            target: "solaris_agent",
            "agent_run",
            session_id = %session_id,
            msg_id = %msg_id,
        );
        let result = self.run_inner(user_input, msg_id).instrument(span).await;
        // Local slash commands can finish without constructing an effect context.
        // Provider and tool paths require one before they can reach a successful
        // terminal result, so only those durable tasks have a phase to complete.
        if let Ok(result) = &result
            && self.execution_context.is_some()
        {
            self.record_completed_durable_task(result)?;
        }
        result
    }

    async fn run_inner(&mut self, user_input: &str, msg_id: &str) -> Result<AgentResult, AgentError> {
        self.activate_resumed_session()
            .map_err(|error| AgentError::ApiError(error.to_string()))?;
        self.ensure_session_lease()?;
        self.msg_id = msg_id.to_string();
        self.output.emit_stream_start(msg_id);

        // Slash command interception 鈥?before any LLM call
        if let Some(result) = self.handle_command(user_input).await? {
            return Ok(result);
        }

        let resume_phase = match self.begin_or_resume_durable_task(user_input)? {
            DurableTaskResume::Completed(result) => return Ok(result),
            DurableTaskResume::New(task) => {
                self.messages.push(Message::now(
                    Role::User,
                    vec![ContentBlock::Text {
                        text: user_input.to_string(),
                    }],
                ));
                self.checkpoint_new_durable_task_user(task.as_ref())?;
                None
            }
            DurableTaskResume::UserCheckpointed => None,
            DurableTaskResume::Resume(task) => Some(task),
        };

        let mut guards = TurnGuards::new(
            self.max_turns_per_run,
            self.max_tool_call_malformed_turns,
            self.max_tool_call_failure_turns,
        );
        let mut next_turn_kind = TurnKind::Normal;
        let mut successful_mutation_observed = false;
        let mut post_mutation_verification_sent = false;
        let mut pending_provider_resume = match self.resume_pending_tool_round(resume_phase.as_ref()).await? {
            Some(mutation) => {
                successful_mutation_observed |= mutation;
                None
            }
            None => resume_phase.filter(|task| {
                matches!(
                    task.phase,
                    DurableTaskPhase::AwaitingProvider
                        | DurableTaskPhase::ProviderInFlight
                        | DurableTaskPhase::ProviderCompleted
                )
            }),
        };
        'agent_loop: loop {
            if let Some(limit) = guards.turn_budget_reached() {
                self.save_session()?;
                let message = format!(
                    "Stopped after reaching the turn budget (max_turns={limit}); the task did not converge. Try adjusting the request or retrying."
                );
                warn!(target: "solaris_agent", limit, "stopping agent run at turn budget");
                self.output
                    .emit_protocol_error(&self.msg_id, ENGINE_ERROR_CODE, &message, false);
                return Ok(AgentResult {
                    status: AgentOutcomeStatus::Failed,
                    failure_class: Some(TaskFailureClass::MaxTurns),
                    text: String::new(),
                    stop_reason: StopReason::MaxTurns,
                    usage: self.total_usage.clone(),
                    turns: guards.counted_turns(),
                });
            }

            let turn_kind = next_turn_kind;
            next_turn_kind = TurnKind::Normal;
            let defer_completion_candidate = !post_mutation_verification_sent
                && successful_mutation_observed
                && self.uses_post_mutation_verification();
            let outcome = self
                .run_turn_with_durable_resume(turn_kind, !defer_completion_candidate, pending_provider_resume.as_ref())
                .await?;
            pending_provider_resume = None;
            guards.record_counted_turn();

            let (assistant_text, tool_calls) = match TurnOutcome::from_stream(outcome) {
                TurnOutcome::ToolRound(outcome) => {
                    if defer_completion_candidate && !outcome.assistant_text.is_empty() {
                        self.output.emit_text_delta(&outcome.assistant_text, &self.msg_id);
                    }
                    self.messages.push(build_assistant_message(&outcome));
                    (outcome.assistant_text, outcome.tool_calls)
                }
                TurnOutcome::Final(outcome) => {
                    self.messages.push(build_assistant_message(&outcome));
                    if !post_mutation_verification_sent
                        && successful_mutation_observed
                        && self.uses_post_mutation_verification()
                    {
                        post_mutation_verification_sent = true;
                        next_turn_kind = TurnKind::PostMutationVerification;
                        self.save_session()?;
                        debug!(
                            target: "solaris_agent",
                            effort = self.reasoning_effort.as_deref().unwrap_or("provider_default"),
                            "scheduling completion-bound semantic verification"
                        );
                        continue;
                    }
                    self.save_session()?;
                    return Ok(AgentResult {
                        status: AgentOutcomeStatus::Completed,
                        failure_class: None,
                        text: outcome.assistant_text,
                        stop_reason: outcome.stop_reason,
                        usage: self.total_usage.clone(),
                        turns: guards.counted_turns(),
                    });
                }
                TurnOutcome::Truncated(outcome) => {
                    let mut accumulated_text = outcome.assistant_text.clone();
                    self.messages.push(build_assistant_message(&outcome));
                    let mut continuation_count = 0;
                    loop {
                        continuation_count += 1;
                        let continuation = self
                            .run_turn_with_text_emission(TurnKind::MaxTokensContinuation, !defer_completion_candidate)
                            .await?;
                        match TurnOutcome::from_stream(continuation) {
                            TurnOutcome::ToolRound(continuation) => {
                                if defer_completion_candidate {
                                    self.output.emit_text_delta(&accumulated_text, &self.msg_id);
                                    self.output.emit_text_delta(&continuation.assistant_text, &self.msg_id);
                                }
                                self.messages.push(build_assistant_message(&continuation));
                                break (continuation.assistant_text, continuation.tool_calls);
                            }
                            TurnOutcome::Final(continuation) => {
                                accumulated_text.push_str(&continuation.assistant_text);
                                self.messages.push(build_assistant_message(&continuation));
                                if !post_mutation_verification_sent
                                    && successful_mutation_observed
                                    && self.uses_post_mutation_verification()
                                {
                                    post_mutation_verification_sent = true;
                                    next_turn_kind = TurnKind::PostMutationVerification;
                                    self.save_session()?;
                                    debug!(
                                        target: "solaris_agent",
                                        effort = self.reasoning_effort.as_deref().unwrap_or("provider_default"),
                                        "scheduling completion-bound semantic verification after continuation"
                                    );
                                    continue 'agent_loop;
                                }
                                self.save_session()?;
                                return Ok(AgentResult {
                                    status: AgentOutcomeStatus::Completed,
                                    failure_class: None,
                                    text: accumulated_text,
                                    stop_reason: StopReason::EndTurn,
                                    usage: self.total_usage.clone(),
                                    turns: guards.counted_turns(),
                                });
                            }
                            TurnOutcome::Truncated(continuation) => {
                                accumulated_text.push_str(&continuation.assistant_text);
                                self.messages.push(build_assistant_message(&continuation));
                                if continuation_count >= MAX_MAX_TOKENS_CONTINUATIONS {
                                    return self.complete_with_fallback(
                                        FinalizationReason::MaxTokens,
                                        accumulated_text,
                                        guards.counted_turns(),
                                        StopReason::MaxTokens,
                                    );
                                }
                            }
                            TurnOutcome::EmptyFinal(continuation) => {
                                accumulated_text.push_str(&continuation.assistant_text);
                                return self.complete_with_fallback(
                                    FinalizationReason::MaxTokens,
                                    accumulated_text,
                                    guards.counted_turns(),
                                    StopReason::MaxTokens,
                                );
                            }
                        }
                    }
                }
                TurnOutcome::EmptyFinal(outcome) => {
                    return self
                        .finalize_once(
                            FinalizationReason::EmptyFinal,
                            outcome.assistant_text,
                            guards.counted_turns(),
                            StopReason::EndTurn,
                        )
                        .await;
                }
            };

            // need to execute tool calls before the next turn
            // The assistant tool-use checkpoint must be durable before any tool
            // intent can start. A crash after this save resumes the saved round.
            self.save_session()?;
            let ToolRoundOutput {
                tool_results,
                tool_modifiers,
                tool_call_malformed_fingerprint,
                tool_call_failure_fingerprint,
                task_call_id,
            } = self.execute_tool_round(&tool_calls, &assistant_text).await?;

            // Persist a submitted plan before making its exit transition visible.
            self.persist_plan_artifacts(&tool_modifiers)?;

            // Apply any context modifiers from tool executions before the next turn.
            self.apply_context_modifiers(&tool_modifiers);

            successful_mutation_observed |= self.tool_round_has_successful_mutation(&tool_calls, &tool_results);

            self.messages.push(Message::now(Role::User, tool_results));

            // Save session after each tool round.
            self.save_session()?;
            self.record_durable_task_phase(DurableTaskPhase::ToolsCompleted, Some(&task_call_id))?;

            match guards.after_tool_round(tool_call_malformed_fingerprint, tool_call_failure_fingerprint) {
                TurnGuardAction::Continue => {}
                TurnGuardAction::Finalize => {
                    return self
                        .finalize_once(
                            FinalizationReason::TurnBudget,
                            String::new(),
                            guards.counted_turns(),
                            StopReason::MaxTurns,
                        )
                        .await;
                }
                TurnGuardAction::Stop(err) => return Err(err),
            }
        }
    }

    /// Build the next provider request, applying plan-mode tool/system filtering
    /// and recording the prompt state for cache diagnostics.
    pub(super) fn build_request(&mut self, kind: TurnKind) -> LlmRequest {
        // Tool visibility follows the effective permission posture. Top-level Plan
        // cannot be exited by the model; legacy temporary PlanState remains
        // available while running under Auto/Bypass.
        let top_level_plan = matches!(self.permission_context.mode(), PermissionMode::Plan);
        let effective_plan = top_level_plan || self.plan_state.is_active;
        let tools = if kind.disable_tools() {
            Vec::new()
        } else if effective_plan {
            self.tools.to_tool_defs_filtered(|tool| {
                let class = tool.describe_effect(&serde_json::json!({})).class;
                let local_read_only = class == solaris_types::effect::EffectClass::ReadOnly
                    && tool.category() == solaris_protocol::events::ToolCategory::Info;
                let legacy_exit = !top_level_plan && tool.name() == "ExitPlanMode";
                let transition_allowed = if top_level_plan {
                    tool.name() != "EnterPlanMode" && tool.name() != "ExitPlanMode"
                } else {
                    tool.name() != "EnterPlanMode"
                };
                (local_read_only || legacy_exit) && transition_allowed
            })
        } else {
            self.tools.to_tool_defs_filtered(|tool| tool.name() != "ExitPlanMode")
        };

        // Build system prompt: append plan instructions for either top-level or legacy Plan.
        let base_system = if effective_plan {
            format!("{}\n\n{}", self.system_prompt, plan_mode_instructions())
        } else {
            self.system_prompt.clone()
        };
        let multi_agent_policy = *self
            .multi_agent_policy
            .read()
            .unwrap_or_else(|error| error.into_inner());
        let policy_instructions = multi_agent_policy_instructions(multi_agent_policy);
        let system = if base_system.is_empty() {
            policy_instructions.to_owned()
        } else {
            format!("{base_system}\n\n{policy_instructions}")
        };

        // Record prompt state for cache diagnostics
        self.cache_detector.record_request(&system, &tools);

        let mut messages = self.messages.clone();
        if let Some(prompt) = kind.control_prompt() {
            messages.push(Message::now(
                Role::User,
                vec![ContentBlock::Text {
                    text: prompt.to_string(),
                }],
            ));
        }

        LlmRequest {
            model: self.model.clone(),
            system,
            messages,
            tools,
            max_tokens: self.max_tokens,
            thinking: self.thinking.clone(),
            reasoning_effort: self.reasoning_effort.clone(),
        }
    }

    /// Classify, execute and re-merge one model turn's tool calls.
    ///
    /// Malformed calls get synthetic error results; the rest are executed via
    /// the approval (JSON stream) or interactive (terminal) path. Results and
    /// skill modifiers are interleaved back into the original call order.
    /// `assistant_text` is the visible text from the same turn, used only to
    /// classify an all-error round for the consecutive-failure breaker.
    ///
    /// A `Quit` from tool execution is surfaced as `AgentError::UserAborted`
    /// after saving the session.
    pub(super) async fn execute_tool_round(
        &mut self,
        tool_calls: &[ContentBlock],
        assistant_text: &str,
    ) -> Result<ToolRoundOutput, AgentError> {
        self.execute_tool_round_with_call_id(tool_calls, assistant_text, None)
            .await
    }

    pub(super) async fn execute_tool_round_with_call_id(
        &mut self,
        tool_calls: &[ContentBlock],
        assistant_text: &str,
        persisted_call_id: Option<&str>,
    ) -> Result<ToolRoundOutput, AgentError> {
        let round_call_id = match persisted_call_id {
            Some(call_id) => call_id.to_owned(),
            None => self.current_tool_round_call_id(tool_calls)?,
        };
        let mut task_phase = self.begin_durable_task_phase(DurableTaskPhase::ToolsInFlight, &round_call_id)?;
        let tool_call_malformed_reasons: Vec<_> = tool_calls
            .iter()
            .map(|call| {
                let ContentBlock::ToolUse { id, name, .. } = call else {
                    return None;
                };
                tool_call_malformed_reason(id, name)
            })
            .collect();
        let tool_call_malformed_fingerprint = tool_call_malformed_fingerprint(tool_calls, &tool_call_malformed_reasons);
        let executable_tool_calls: Vec<_> = tool_calls
            .iter()
            .zip(&tool_call_malformed_reasons)
            .filter(|(_, reason)| reason.is_none())
            .map(|(call, _)| call.clone())
            .collect();

        let (executable_results, executable_modifiers, executable_statuses, executable_metadata) =
            if executable_tool_calls.is_empty() {
                (Vec::new(), Vec::new(), Vec::new(), std::collections::BTreeMap::new())
            } else if let Some(ref approval_mgr) = self.approval_manager {
                self.ensure_session_lease()?;
                // JSON stream mode: use protocol-based approval
                let writer = self
                    .protocol_writer
                    .as_ref()
                    .expect("protocol writer required for approval");
                let auto_approve = self.confirmer.lock().unwrap().is_auto_approve();
                let execution_context = self.execution_context.as_ref().ok_or_else(|| {
                    AgentError::ApiError("effect execution context is required before tool execution".to_owned())
                })?;
                let execution = execute_tool_calls_with_approval_context(
                    &self.tools,
                    &executable_tool_calls,
                    approval_mgr,
                    writer,
                    &self.msg_id,
                    auto_approve,
                    &self.allow_list,
                    self.permission_context.ceiling(),
                    execution_context,
                    self.hooks.as_mut(),
                    self.compact_level,
                    self.toon_enabled,
                )
                .await;
                match execution {
                    Ok(o) => (o.results, o.modifiers, o.statuses, o.metadata),
                    Err(ExecutionControl::Quit) => {
                        return self.close_cancelled_tool_round(
                            tool_calls,
                            &round_call_id,
                            &mut task_phase,
                            "Tool execution cancelled by the user",
                        );
                    }
                }
            } else {
                self.ensure_session_lease()?;
                // Terminal mode: use the same effect-aware permission policy.
                let permission_mode = if self.plan_state.is_active {
                    PermissionMode::Plan
                } else {
                    self.permission_context.mode()
                };
                let execution_context = self.execution_context.as_ref().ok_or_else(|| {
                    AgentError::ApiError("effect execution context is required before tool execution".to_owned())
                })?;
                let execution = execute_tool_calls_with_policy_context(
                    &self.tools,
                    &executable_tool_calls,
                    &self.confirmer,
                    permission_mode,
                    self.permission_context.ceiling(),
                    execution_context,
                    self.hooks.as_mut(),
                    self.compact_level,
                    self.toon_enabled,
                )
                .await;
                match execution {
                    Ok(o) => (o.results, o.modifiers, o.statuses, o.metadata),
                    Err(ExecutionControl::Quit) => {
                        return self.close_cancelled_tool_round(
                            tool_calls,
                            &round_call_id,
                            &mut task_phase,
                            "Tool execution cancelled by the user",
                        );
                    }
                }
            };

        let (tool_results, tool_modifiers) = merge_tool_results(
            tool_calls,
            &tool_call_malformed_reasons,
            executable_results,
            executable_modifiers,
        );
        let mut executable_statuses = executable_statuses.into_iter();
        let tool_statuses = tool_call_malformed_reasons
            .iter()
            .map(|reason| {
                if reason.is_some() {
                    ToolResultStatus::Failed
                } else {
                    executable_statuses
                        .next()
                        .expect("tool execution status missing for executable tool call")
                }
            })
            .collect::<Vec<_>>();
        debug_assert_eq!(tool_results.len(), tool_statuses.len());
        if let Some(context) = &self.execution_context {
            let stats = self.tool_call_stats(tool_calls, &tool_statuses);
            context
                .record_tool_calls_once(&round_call_id, &stats)
                .map_err(AgentError::ResourceBudgetExceeded)?;
        }
        let phase = if tool_statuses.contains(&ToolResultStatus::OutcomeUnknown) {
            DurableTaskPhase::OutcomeUnknown
        } else {
            DurableTaskPhase::ToolsCompleted
        };
        self.emit_tool_results(tool_calls, &tool_results, &tool_statuses, &executable_metadata);
        if phase == DurableTaskPhase::OutcomeUnknown {
            self.messages.push(Message::now(Role::User, tool_results.clone()));
            self.save_session()?;
            task_phase.complete(DurableTaskPhase::OutcomeUnknown)?;
            return Err(AgentError::ReconciliationRequired {
                task_key: "current durable task".to_owned(),
                call_id: Some(round_call_id),
            });
        }
        task_phase.disarm();

        let tool_call_failure_fingerprint = (tool_call_malformed_fingerprint.is_none()
            && assistant_text.trim().is_empty()
            && !tool_results.is_empty()
            && tool_results
                .iter()
                .all(|result| matches!(result, ContentBlock::ToolResult { is_error: true, .. })))
        .then(|| tool_call_failure_fingerprint(tool_calls))
        .flatten();

        Ok(ToolRoundOutput {
            tool_results,
            tool_modifiers,
            tool_call_malformed_fingerprint,
            tool_call_failure_fingerprint,
            task_call_id: round_call_id,
        })
    }

    /// Emit each tool result to the output sink, resolving the tool name from
    /// the originating `tool_calls` for display and logging.
    pub(super) fn emit_tool_results(
        &self,
        tool_calls: &[ContentBlock],
        tool_results: &[ContentBlock],
        tool_statuses: &[ToolResultStatus],
        tool_metadata: &std::collections::BTreeMap<String, solaris_types::tool::ToolResultMetadata>,
    ) {
        emit_tool_results_to_sink(
            self.output.as_ref(),
            tool_calls,
            tool_results,
            tool_statuses,
            tool_metadata,
        );
    }

    pub(super) async fn run_turn(&mut self, kind: TurnKind) -> Result<StreamOutcome, AgentError> {
        self.run_turn_with_text_emission(kind, true).await
    }

    async fn finalize_once(
        &mut self,
        reason: FinalizationReason,
        prefix_text: String,
        counted_turns: usize,
        fallback_stop_reason: StopReason,
    ) -> Result<AgentResult, AgentError> {
        let outcome = self.run_turn(TurnKind::Finalization(reason)).await?;
        let combined_text = format!("{}{}", prefix_text, outcome.assistant_text);
        let is_success = outcome.tool_calls.is_empty()
            && outcome.stop_reason == StopReason::EndTurn
            && !outcome.assistant_text.trim().is_empty();

        if is_success {
            self.messages.push(build_assistant_message(&outcome));
            self.save_session()?;
            return Ok(AgentResult {
                status: AgentOutcomeStatus::Completed,
                failure_class: None,
                text: combined_text,
                stop_reason: StopReason::EndTurn,
                usage: self.total_usage.clone(),
                turns: counted_turns,
            });
        }

        self.complete_with_fallback(reason, combined_text, counted_turns, fallback_stop_reason)
    }

    fn complete_with_fallback(
        &mut self,
        reason: FinalizationReason,
        text: String,
        counted_turns: usize,
        fallback_stop_reason: StopReason,
    ) -> Result<AgentResult, AgentError> {
        let fallback = reason.fallback_prompt();
        self.output
            .emit_protocol_error(&self.msg_id, ENGINE_ERROR_CODE, fallback, false);
        let fallback_text = if text.trim().is_empty() {
            fallback.to_string()
        } else {
            text
        };

        self.messages.push(Message::now(
            Role::Assistant,
            vec![ContentBlock::Text {
                text: fallback_text.clone(),
            }],
        ));
        self.save_session()?;
        Ok(AgentResult {
            status: AgentOutcomeStatus::Failed,
            failure_class: Some(reason.failure_class()),
            text: fallback_text,
            stop_reason: fallback_stop_reason,
            usage: self.total_usage.clone(),
            turns: counted_turns,
        })
    }

    /// Drain one provider stream into a [`StreamOutcome`].
    ///
    /// Emits text/thinking/tool-call events to the output sink as they arrive
    /// and accumulates the assistant text, thinking block, tool calls, stop
    /// reason and usage for the caller. Returns early on `LlmEvent::Error`.
    async fn consume_stream(
        &self,
        rx: &mut Receiver<LlmEvent>,
        emit_assistant_text: bool,
    ) -> Result<StreamOutcome, AgentError> {
        let mut assistant_text = String::new();
        let mut thinking_text = String::new();
        let mut thinking_signature: Option<String> = None;
        let mut provider_metadata = ProviderNativeMetadata::new();
        let mut tool_calls: Vec<ContentBlock> = Vec::new();
        let mut stop_reason = StopReason::EndTurn;
        let mut usage = TokenUsage::default();

        while let Some(event) = rx.recv().await {
            match event {
                LlmEvent::TextDelta(text) => {
                    if emit_assistant_text {
                        self.output.emit_text_delta(&text, &self.msg_id);
                    }
                    assistant_text.push_str(&text);
                }
                LlmEvent::ToolUse {
                    id,
                    name,
                    input,
                    extra: _,
                } => {
                    if id.trim().is_empty() {
                        error!(
                            target: "solaris_agent",
                            tool = %name,
                            "provider emitted tool call with empty tool_use_id"
                        );
                    } else {
                        debug!(
                            target: "solaris_agent",
                            tool_use_id = %id,
                            tool = %name,
                            "provider tool call received"
                        );
                    }
                    let input_str = to_string(&input).unwrap_or_default();
                    self.output.emit_tool_call(&id, &name, &input_str);
                    tool_calls.push(ContentBlock::ToolUse {
                        id,
                        name,
                        input,
                        extra: None,
                    });
                }
                LlmEvent::ThinkingDelta(text) => {
                    self.output.emit_thinking(&text, &self.msg_id);
                    thinking_text.push_str(&text);
                }
                LlmEvent::ThinkingSignature(signature) => {
                    thinking_signature = Some(signature);
                }
                LlmEvent::ProviderMetadata { namespace, value } => {
                    merge_provider_metadata(&mut provider_metadata, namespace, value);
                }
                LlmEvent::Done {
                    stop_reason: sr,
                    usage: u,
                } => {
                    stop_reason = sr;
                    usage = u;
                }
                LlmEvent::Error(e) => {
                    return Err(AgentError::ApiError(e));
                }
            }
        }

        Ok(StreamOutcome {
            assistant_text,
            thinking_text,
            thinking_signature,
            provider_metadata,
            tool_calls,
            stop_reason,
            usage,
        })
    }

    /// Fold one turn's token usage into the running totals and update the
    /// compaction watermark and cache-break diagnostics.
    pub(super) fn record_turn_usage(
        &mut self,
        turn_usage: &TokenUsage,
        effect_id: &solaris_types::identity::EffectId,
    ) -> Result<(), AgentError> {
        self.total_usage.input_tokens += turn_usage.input_tokens;
        self.total_usage.output_tokens += turn_usage.output_tokens;
        self.total_usage.cache_creation_tokens += turn_usage.cache_creation_tokens;
        self.total_usage.cache_read_tokens += turn_usage.cache_read_tokens;

        // Track per-turn input tokens for compaction watermark.
        // Use max(provider_reported, local_estimate) as a safety net:
        // some providers (e.g. DeepSeek with prefix caching) underreport
        // prompt_tokens, causing compaction to never trigger.
        let local_estimate = estimate_tokens_from_messages(&self.messages);
        let effective_watermark = turn_usage.input_tokens.max(local_estimate);

        if local_estimate > turn_usage.input_tokens && local_estimate.saturating_sub(turn_usage.input_tokens) > 10_000 {
            self.output.emit_info(&format!(
                "Token watermark override: provider={}, local_estimate={}, using={}",
                turn_usage.input_tokens, local_estimate, effective_watermark
            ));
        }

        self.compact_state.last_input_tokens = effective_watermark;

        // Cache break detection
        let cache_stats = CacheStats {
            input_tokens: turn_usage.input_tokens,
            cache_read_tokens: turn_usage.cache_read_tokens,
            cache_creation_tokens: turn_usage.cache_creation_tokens,
        };
        if let Some(diagnostic) = self.cache_detector.check_response(cache_stats) {
            match &diagnostic {
                CacheDiagnostic::FullMiss { cause } => {
                    self.output.emit_info(&format!("Cache full miss: {cause:?}"));
                }
                CacheDiagnostic::PartialMiss { hit_rate, cause } => {
                    if self.compact_config.cache_diagnostics {
                        self.output
                            .emit_info(&format!("Cache: {:.0}% hit rate (cause: {cause:?})", hit_rate * 100.0));
                    }
                }
                CacheDiagnostic::Healthy { hit_rate } => {
                    if self.compact_config.cache_diagnostics {
                        self.output
                            .emit_info(&format!("Cache: {:.0}% hit rate", hit_rate * 100.0));
                    }
                }
            }
        }
        if let Some(context) = &self.execution_context {
            context
                .record_model_usage_once(effect_id, turn_usage, true)
                .map_err(AgentError::ResourceBudgetExceeded)?;
        }
        Ok(())
    }

    /// Run the multi-level compaction pipeline before each API call.
    ///
    /// Execution order: microcompact 鈫?autocompact 鈫?emergency check.
    /// After a successful autocompact the emergency check is skipped
    /// because the context has been significantly reduced.
    pub(super) async fn run_compaction(&mut self) -> Result<(), AgentError> {
        // 1. Microcompact (lightweight, no LLM call)
        if should_microcompact(&self.messages, &self.compact_config) {
            let tools = &self.tools;
            let result = microcompact_with_observer(&mut self.messages, &self.compact_config, |name, input| {
                tools.notify_result_compacted(name, input);
            });
            if result.cleared_count > 0 {
                self.output.emit_info(&format!(
                    "Microcompact: cleared {} tool results (~{} tokens freed)",
                    result.cleared_count, result.estimated_tokens_freed
                ));
            }
        }

        // 2. Autocompact (LLM summarization)
        let mut compacted = false;
        let should_compact = should_autocompact(self.compact_state.last_input_tokens, &self.compact_config);
        if should_compact {
            info!(target: "solaris_agent", last_input_tokens = self.compact_state.last_input_tokens, "context compaction triggered");
            let threshold = if let Some(pct) = self.compact_config.autocompact_threshold_pct {
                let t = self.compact_config.context_window * pct as usize / 100;
                self.output.emit_info(&format!(
                    "Autocompact threshold: {} tokens ({}% of {})",
                    t, pct, self.compact_config.context_window
                ));
                t
            } else {
                self.compact_config
                    .context_window
                    .saturating_sub(self.compact_config.output_reserve)
                    .saturating_sub(self.compact_config.autocompact_buffer)
            };
            let _ = threshold;
        }
        if should_compact && !self.compact_state.is_circuit_broken(&self.compact_config) {
            let provider = Arc::clone(&self.provider);
            let standalone_context = self
                .execution_context
                .is_none()
                .then(|| crate::compact::auto::standalone_compaction_context(&self.provider_effect));
            let execution_context = self
                .execution_context
                .as_ref()
                .or(standalone_context.as_ref())
                .expect("a standalone compaction context was created");
            match autocompact_with_effect(
                provider.as_ref(),
                &self.messages,
                &self.model,
                &self.compact_config,
                &mut self.compact_state,
                execution_context,
                &self.provider_effect,
            )
            .await
            {
                Ok(result) => {
                    self.output.emit_info(&format!(
                        "Autocompact: summarized {} messages ({} tokens 鈫?compact)",
                        result.messages_summarized, result.pre_compact_tokens
                    ));
                    self.messages = result.messages;
                    self.tools.notify_history_compacted();
                    compacted = true;
                }
                Err(CompactError::CircuitBroken { .. }) => {
                    // Already tripped; logged at circuit-breaker level
                }
                Err(CompactError::ReconciliationRequired(reason)) => {
                    return Err(AgentError::ReconciliationRequired {
                        task_key: "autocompact".to_owned(),
                        call_id: Some(reason),
                    });
                }
                Err(error) => {
                    let error_kind = match error {
                        CompactError::Provider(_) => "provider",
                        CompactError::PromptTooLong { .. } => "prompt_too_long",
                        CompactError::EmptyResponse => "empty_response",
                        CompactError::StreamError(_) => "stream",
                        CompactError::CircuitBroken { .. } => "circuit_broken",
                        CompactError::Effect(_) => "effect",
                        CompactError::ReconciliationRequired(_) => unreachable!("handled above"),
                    };
                    warn!(
                        target: "solaris_agent",
                        error_kind,
                        consecutive_failures = self.compact_state.consecutive_failures,
                        "automatic context compaction failed; continuing without compaction"
                    );
                    self.output
                        .emit_protocol_diagnostic(&self.msg_id, AUTOCOMPACT_FAILURE_DIAGNOSTIC);
                }
            }
        } else if should_compact {
            self.output.emit_info(&format!(
                "Autocompact: skipped (circuit breaker tripped after {} consecutive failures, \
                 last_input_tokens={})",
                self.compact_state.consecutive_failures, self.compact_state.last_input_tokens
            ));
        } else if !self.compact_config.enabled {
            let threshold = if let Some(pct) = self.compact_config.autocompact_threshold_pct {
                self.compact_config.context_window * pct as usize / 100
            } else {
                self.compact_config
                    .context_window
                    .saturating_sub(self.compact_config.output_reserve)
                    .saturating_sub(self.compact_config.autocompact_buffer)
            };
            if self.compact_state.last_input_tokens as usize >= threshold {
                self.output.emit_info(&format!(
                    "Autocompact: disabled (compact.enabled=false, \
                     last_input_tokens={}, threshold={})",
                    self.compact_state.last_input_tokens, threshold
                ));
            }
        }

        // 3. Emergency check (skip if autocompact just succeeded)
        if !compacted && is_at_emergency_limit(self.compact_state.last_input_tokens, &self.compact_config) {
            return Err(AgentError::ContextTooLong {
                input_tokens: self.compact_state.last_input_tokens,
                limit: self
                    .compact_config
                    .context_window
                    .saturating_sub(self.compact_config.emergency_buffer),
            });
        }

        Ok(())
    }
}
