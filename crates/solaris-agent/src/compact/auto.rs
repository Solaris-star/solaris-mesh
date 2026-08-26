//! Autocompact: watermark-triggered LLM summarization.
//!
//! When the token watermark exceeds the configured threshold, this module
//! calls the LLM to produce a structured summary of the conversation,
//! then replaces the full history with a compact boundary marker and the
//! summary.  A circuit breaker prevents runaway retries.

use std::sync::Arc;

use solaris_config::compact::CompactConfig;
use solaris_providers::{LlmProvider, ProviderError};
use solaris_types::compact::{CompactMetadata, CompactTrigger};
use solaris_types::effect::EffectDescriptor;
use solaris_types::identity::{AgentId, RunId};
use solaris_types::llm::{LlmEvent, LlmRequest, ThinkingConfig};
use solaris_types::message::{ContentBlock, Message, Role, TokenUsage};
use solaris_types::permission::PermissionDecision;
use solaris_types::permission::{PermissionCeiling, PermissionMode, PermissionRule};
use solaris_types::runtime::OperationEnvironmentSnapshot;
use tokio::sync::mpsc;

use super::prompt::{
    COMPACT_MAX_OUTPUT_TOKENS, COMPACT_SYSTEM_PROMPT, build_compact_prompt, build_summary_content,
    format_compact_summary,
};
use super::state::CompactState;
use crate::compact::estimate::estimate_tokens_from_messages;
use crate::execution_context::{
    EffectExecutionContext, EffectOutcomeGuard, EffectRecoveryDecision, stable_digest_value,
};
use crate::permission_engine::PermissionContext;
use crate::runtime_ledger::InMemoryRuntimeLedger;

/// Maximum number of prompt-too-long retries.
const MAX_PTL_RETRIES: u32 = 2;

/// Content prefix for the compact boundary marker message.
pub const BOUNDARY_PREFIX: &str = "[Conversation compacted]";

// ── Public types ────────────────────────────────────────────────────────────

/// Result of a successful autocompact operation.
#[derive(Debug, Clone)]
pub struct CompactResult {
    /// Post-compact messages that replace the original conversation.
    /// Contains a boundary marker and a summary message.
    pub messages: Vec<Message>,
    /// How many original messages were summarized.
    pub messages_summarized: usize,
    /// Input token count before compaction (from the last API call).
    pub pre_compact_tokens: u64,
}

/// Errors specific to autocompact.
#[derive(Debug, thiserror::Error)]
pub enum CompactError {
    #[error("LLM provider error: {0}")]
    Provider(#[from] ProviderError),
    #[error("Prompt too long after {attempts} retries")]
    PromptTooLong { attempts: u32 },
    #[error("Empty response from LLM")]
    EmptyResponse,
    #[error("Stream error: {0}")]
    StreamError(String),
    #[error("Circuit breaker tripped after {failures} consecutive failures")]
    CircuitBroken { failures: u32 },
    #[error("Provider effect rejected: {0}")]
    Effect(String),
    #[error("Provider effect requires reconciliation: {0}")]
    ReconciliationRequired(String),
}

// ── Trigger check ───────────────────────────────────────────────────────────

/// Check if autocompact should trigger based on the token watermark.
///
/// When `autocompact_threshold_pct` is set, threshold = context_window * pct / 100.
/// Otherwise falls back to: `threshold = context_window - output_reserve - autocompact_buffer`
pub fn should_autocompact(last_input_tokens: u64, config: &CompactConfig) -> bool {
    if !config.enabled {
        return false;
    }
    let threshold = if let Some(pct) = config.autocompact_threshold_pct {
        config.context_window * pct as usize / 100
    } else {
        let effective_window = config.context_window.saturating_sub(config.output_reserve);
        effective_window.saturating_sub(config.autocompact_buffer)
    };
    last_input_tokens as usize >= threshold
}

// ── Core autocompact ────────────────────────────────────────────────────────

/// Execute autocompact: call LLM to summarize the conversation.
///
/// 1. Build a summary prompt and send conversation + prompt to the LLM.
/// 2. If the prompt is too long, truncate oldest 20% messages and retry
///    (up to [`MAX_PTL_RETRIES`] times).
/// 3. Parse the `<summary>` from the response.
/// 4. Return a [`CompactResult`] with boundary marker + summary messages.
///
/// On failure, increments `state.consecutive_failures`.
/// On success, resets the failure counter.
pub async fn autocompact(
    provider: &dyn LlmProvider,
    messages: &[Message],
    model: &str,
    config: &CompactConfig,
    state: &mut CompactState,
) -> Result<CompactResult, CompactError> {
    let provider_effect = crate::bootstrap::provider_effect_descriptor("");
    let context = standalone_compaction_context(&provider_effect);
    autocompact_with_effect(provider, messages, model, config, state, &context, &provider_effect).await
}

pub(crate) async fn autocompact_with_effect(
    provider: &dyn LlmProvider,
    messages: &[Message],
    model: &str,
    config: &CompactConfig,
    state: &mut CompactState,
    execution_context: &EffectExecutionContext,
    provider_effect: &EffectDescriptor,
) -> Result<CompactResult, CompactError> {
    // Circuit breaker check
    if state.is_circuit_broken(config) {
        return Err(CompactError::CircuitBroken {
            failures: state.consecutive_failures,
        });
    }

    let pre_compact_tokens = state.last_input_tokens;
    let messages_summarized = messages.len();

    // Build messages for the compact LLM call: conversation + summary prompt
    let prompt = build_compact_prompt();
    let mut conv_messages = messages.to_vec();
    conv_messages.push(Message::new(Role::User, vec![ContentBlock::Text { text: prompt }]));

    let mut ptl_attempts = 0u32;

    let summary_text = loop {
        let request = LlmRequest {
            model: model.to_string(),
            system: COMPACT_SYSTEM_PROMPT.to_string(),
            messages: conv_messages.clone(),
            tools: vec![],
            max_tokens: Some(COMPACT_MAX_OUTPUT_TOKENS),
            thinking: Some(ThinkingConfig::Disabled),
            reasoning_effort: None,
        };

        match compact_provider_call(provider, &request, execution_context, provider_effect).await {
            Ok((text, _usage)) => break text,
            Err(CompactError::Provider(ProviderError::PromptTooLong(_))) if ptl_attempts < MAX_PTL_RETRIES => {
                ptl_attempts += 1;
                // Remove the summary prompt (last msg), truncate, re-add prompt
                let conversation_part = &conv_messages[..conv_messages.len() - 1];
                match truncate_for_retry(conversation_part) {
                    Some(mut truncated) => {
                        truncated.push(Message::new(
                            Role::User,
                            vec![ContentBlock::Text {
                                text: build_compact_prompt(),
                            }],
                        ));
                        conv_messages = truncated;
                    }
                    None => {
                        state.record_failure();
                        return Err(CompactError::PromptTooLong { attempts: ptl_attempts });
                    }
                }
            }
            Err(CompactError::Provider(ProviderError::PromptTooLong(_))) => {
                state.record_failure();
                return Err(CompactError::PromptTooLong { attempts: ptl_attempts });
            }
            Err(e) => {
                state.record_failure();
                return Err(e);
            }
        }
    };

    if summary_text.trim().is_empty() {
        state.record_failure();
        return Err(CompactError::EmptyResponse);
    }

    // Format and build post-compact messages
    let formatted = format_compact_summary(&summary_text);
    let summary_content = build_summary_content(&formatted, true);

    let metadata = CompactMetadata {
        trigger: CompactTrigger::Auto,
        pre_compact_tokens,
        messages_summarized,
    };

    let boundary_text = format!(
        "{BOUNDARY_PREFIX}\n{}",
        serde_json::to_string(&metadata).expect("CompactMetadata serialization cannot fail")
    );

    let boundary_msg = Message::new(Role::User, vec![ContentBlock::Text { text: boundary_text }]);

    let summary_msg = Message::new(Role::User, vec![ContentBlock::Text { text: summary_content }]);

    state.record_success();

    Ok(CompactResult {
        messages: vec![boundary_msg, summary_msg],
        messages_summarized,
        pre_compact_tokens,
    })
}

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Collect all text from a streaming LLM response.
async fn collect_stream_text(mut rx: mpsc::Receiver<LlmEvent>) -> Result<(String, TokenUsage), CompactError> {
    let mut text = String::new();

    while let Some(event) = rx.recv().await {
        match event {
            LlmEvent::TextDelta(delta) => text.push_str(&delta),
            LlmEvent::Done { usage, .. } => return Ok((text, usage)),
            LlmEvent::Error(e) => return Err(CompactError::StreamError(e)),
            // Ignore thinking deltas and tool calls (shouldn't happen in compact)
            _ => {}
        }
    }

    // Channel closed without a Done event
    Err(CompactError::EmptyResponse)
}

#[derive(serde::Serialize, serde::Deserialize)]
struct CompactProviderOutcome {
    text: String,
    usage: TokenUsage,
}

async fn compact_provider_call(
    provider: &dyn LlmProvider,
    request: &LlmRequest,
    context: &EffectExecutionContext,
    provider_effect: &EffectDescriptor,
) -> Result<(String, TokenUsage), CompactError> {
    let input = serde_json::to_value(request).map_err(|error| CompactError::Effect(error.to_string()))?;
    let effect_request = context.effect_request(
        &format!("provider:autocompact:{}", stable_digest_value(&input)),
        "AutoCompact",
        &input,
        provider_effect.clone(),
    );
    let effect_id = effect_request.effect_id.clone();
    if context
        .unresolved_other_effect_for_capability("AutoCompact", effect_id.as_str())
        .map_err(CompactError::ReconciliationRequired)?
        .is_some()
    {
        return Err(CompactError::ReconciliationRequired(
            "a previous AutoCompact call has an unresolved outcome under a different durable identity".to_owned(),
        ));
    }
    match context.recover_effect(&effect_request).map_err(CompactError::Effect)? {
        EffectRecoveryDecision::Execute => {}
        EffectRecoveryDecision::Reuse { is_error: true, output } => return Err(CompactError::Effect(output)),
        EffectRecoveryDecision::Reuse {
            is_error: false,
            output,
        } => {
            let recovered: CompactProviderOutcome =
                serde_json::from_str(&output).map_err(|error| CompactError::Effect(error.to_string()))?;
            context
                .record_model_usage_once(&effect_id, &recovered.usage, false)
                .map_err(CompactError::Effect)?;
            return Ok((recovered.text, recovered.usage));
        }
        EffectRecoveryDecision::Reconcile { reason } => return Err(CompactError::ReconciliationRequired(reason)),
    }
    let evaluation = context.evaluate(&effect_request);
    let approved_environment = context.environment();
    context
        .record_permission_decision(&effect_request, &evaluation, "provider_autocompact")
        .map_err(|error| CompactError::Effect(error.to_string()))?;
    if evaluation.decision != PermissionDecision::Allow {
        return Err(CompactError::Effect(format!(
            "provider request permission denied: {}",
            evaluation.reason
        )));
    }
    let estimated_tokens =
        estimate_tokens_from_messages(&request.messages).saturating_add(u64::from(request.max_tokens.unwrap_or(0)));
    let mut provider_rate_permit = context
        .acquire_provider_request(estimated_tokens)
        .map_err(CompactError::Effect)?;
    let _permit = context.acquire_effect_permit().await.map_err(CompactError::Effect)?;
    context
        .revalidate_environment(&effect_request, &approved_environment)
        .map_err(CompactError::Effect)?;
    context
        .record_effect_intent(&effect_request)
        .map_err(|error| CompactError::Effect(error.to_string()))?;
    let mut guard = EffectOutcomeGuard::new(
        context.clone(),
        effect_request,
        "provider autocompact request cancelled before a terminal result",
    );
    let provider_signals = context.resource_manager().map(|resources| resources.provider_signals());
    let single_attempt = provider_signals
        .as_ref()
        .is_some_and(|signals| signals.requests_per_minute.is_some() || signals.tokens_per_minute.is_some());
    if let Some(permit) = provider_rate_permit.as_mut() {
        permit.mark_started().map_err(CompactError::Effect)?;
    }
    let provider_stream = if single_attempt {
        provider.stream_once(request).await
    } else {
        provider.stream(request).await
    };
    let rx = match provider_stream {
        Ok(rx) => rx,
        Err(error) => {
            if let Err(persistence_error) = guard.complete(true, &error.to_string()) {
                return Err(CompactError::Effect(format!(
                    "provider autocompact request failed: {error}; outcome persistence failed: {persistence_error}"
                )));
            }
            return Err(CompactError::Provider(error));
        }
    };
    match collect_stream_text(rx).await {
        Ok((text, usage)) => {
            let output = serde_json::to_string(&CompactProviderOutcome {
                text: text.clone(),
                usage: usage.clone(),
            })
            .map_err(|error| CompactError::Effect(error.to_string()))?;
            guard
                .complete(false, &output)
                .map_err(|error| CompactError::Effect(error.to_string()))?;
            if let Some(permit) = provider_rate_permit.take() {
                permit
                    .commit(usage.input_tokens.saturating_add(usage.output_tokens))
                    .map_err(CompactError::Effect)?;
            }
            context
                .record_model_usage_once(&effect_id, &usage, false)
                .map_err(CompactError::Effect)?;
            Ok((text, usage))
        }
        Err(error) => {
            if let Err(persistence_error) = guard.complete(true, &error.to_string()) {
                return Err(CompactError::Effect(format!(
                    "provider autocompact stream failed: {error}; outcome persistence failed: {persistence_error}"
                )));
            }
            Err(error)
        }
    }
}

pub(crate) fn standalone_compaction_context(provider_effect: &EffectDescriptor) -> EffectExecutionContext {
    let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    permissions.allow_configured_effect_for("config:provider", "AutoCompact", provider_effect);
    permissions.replace_rules(vec![PermissionRule {
        capability: Some("AutoCompact".into()),
        action: None,
        effect_class: Some(solaris_types::effect::EffectClass::Network),
        resource_prefixes: Vec::new(),
        decision: PermissionDecision::Allow,
    }]);
    EffectExecutionContext::new(
        RunId::new(format!("standalone-compaction-{}", uuid::Uuid::now_v7())),
        AgentId::from("standalone-compaction"),
        Arc::new(InMemoryRuntimeLedger::default()),
        permissions,
        OperationEnvironmentSnapshot::default(),
    )
}

/// Truncate the oldest ~20% of messages for PTL retry.
///
/// Returns `None` if there are too few messages to truncate meaningfully.
fn truncate_for_retry(messages: &[Message]) -> Option<Vec<Message>> {
    if messages.len() < 2 {
        return None;
    }

    let drop_count = (messages.len() / 5).max(1);
    if drop_count >= messages.len() {
        return None;
    }

    let remaining = &messages[drop_count..];
    let mut result = Vec::with_capacity(remaining.len() + 1);

    // Ensure the first message is User role for API compatibility
    if remaining.first().map(|m| m.role) != Some(Role::User) {
        result.push(Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "[earlier conversation truncated for compaction retry]".to_string(),
            }],
        ));
    }

    result.extend_from_slice(remaining);
    Some(result)
}

/// Check if a message is a compact boundary marker.
pub fn is_compact_boundary(message: &Message) -> bool {
    message.content.iter().any(|block| {
        if let ContentBlock::Text { text } = block {
            text.starts_with(BOUNDARY_PREFIX)
        } else {
            false
        }
    })
}

/// Extract [`CompactMetadata`] from a boundary marker message.
pub fn extract_compact_metadata(message: &Message) -> Option<CompactMetadata> {
    for block in &message.content {
        if let ContentBlock::Text { text } = block
            && let Some(json_str) = text.strip_prefix(BOUNDARY_PREFIX)
        {
            let json_str = json_str.trim_start_matches('\n');
            return serde_json::from_str(json_str).ok();
        }
    }
    None
}

#[cfg(test)]
#[path = "auto_test.rs"]
mod auto_test;
