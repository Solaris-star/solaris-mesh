use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

mod approval;
mod durable_tool_outcome;
mod execution;
mod hook_effect;
mod process_policy;

pub use approval::{
    execute_tool_calls_with_approval, execute_tool_calls_with_approval_and_ceiling,
    execute_tool_calls_with_approval_context,
};

use approval::permission_error_result;
use durable_tool_outcome::DurableToolOutcome;
use execution::execute_single;
pub(crate) use hook_effect::EffectHookExecutor;
use process_policy::with_process_workspace_root;

use crate::permission_engine::PermissionContext;
use crate::runtime_ledger::InMemoryRuntimeLedger;

use crate::confirm::{ConfirmResult, ToolConfirmer};
use crate::execution_context::{
    ApprovedRequestRegistration, EffectExecutionContext, EffectOutcomeCompletion, stable_digest_bytes,
};
#[cfg(test)]
use async_trait::async_trait;
use futures::future::join_all;
use solaris_config::hooks::{HookEngine, HookError};
#[cfg(test)]
use solaris_config::hooks::{HookExecutionResult, HookExecutor, HookInvocation};
#[cfg(test)]
use solaris_process::inspect_executable;
use solaris_protocol::events::{OutputType, ProtocolEvent, ToolStatus};
use solaris_protocol::writer::ProtocolEmitter;
use solaris_types::effect::{EffectClass, EffectDescriptor, EffectReplayPolicy, ResourceFootprint};
use solaris_types::identity::{AgentId, RunId};
use solaris_types::message::ContentBlock;
#[cfg(test)]
use solaris_types::permission::PermissionRule;
use solaris_types::permission::{ExecutionBoundary, PermissionCeiling, PermissionDecision, PermissionMode};
use solaris_types::runtime::OperationEnvironmentSnapshot;
use solaris_types::skill_types::ContextModifier;
#[cfg(test)]
use solaris_types::tool::ToolResult;
use solaris_types::tool::ToolResultMetadata;

#[cfg(test)]
use solaris_tools::PreparedToolExecution;
use solaris_tools::registry::ToolRegistry;
use solaris_tools::{PreparedToolEffect, ToolExecutionContext};
use uuid::Uuid;

fn ephemeral_effect_context(mode: PermissionMode, ceiling: PermissionCeiling) -> EffectExecutionContext {
    let permissions = PermissionContext::new(mode, ceiling);
    let workspace = std::env::current_dir()
        .ok()
        .and_then(|path| path.canonicalize().ok().or(Some(path)))
        .map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_else(|| ".".to_owned());
    permissions.set_boundary(ExecutionBoundary::workspace(workspace));
    let run_id = RunId::new(format!("run-ephemeral-{}", Uuid::now_v7()));
    EffectExecutionContext::new(
        run_id.clone(),
        AgentId::new(format!("agent:root:{}", run_id.as_str())),
        Arc::new(InMemoryRuntimeLedger::default()),
        permissions,
        OperationEnvironmentSnapshot::default(),
    )
}

fn unavailable_tool_recovery(
    execution_context: &EffectExecutionContext,
    effect_id: &str,
    reason: impl Into<String>,
) -> (String, ToolStatus) {
    let reason = reason.into();
    match execution_context.exact_effect_intent_exists(effect_id) {
        Ok(false) => (reason, ToolStatus::Failed),
        Ok(true) => (
            format!("{reason}; a durable intent already exists for this call and requires reconciliation"),
            ToolStatus::OutcomeUnknown,
        ),
        Err(error) => (
            format!("{reason}; durable effect history could not be verified: {error}"),
            ToolStatus::OutcomeUnknown,
        ),
    }
}

fn skipped_after_reconciliation(id: &str) -> ContentBlock {
    permission_error_result(
        id,
        "Tool call skipped because an earlier call in this round requires reconciliation",
    )
}

/// The combined output of a tool execution batch: protocol content blocks
/// paired with per-call context modifiers (None for non-skill tools).
pub struct ToolCallOutcome {
    pub results: Vec<ContentBlock>,
    pub modifiers: Vec<Option<ContextModifier>>,
    pub statuses: Vec<ToolStatus>,
    pub metadata: BTreeMap<String, ToolResultMetadata>,
}

impl std::ops::Deref for ToolCallOutcome {
    type Target = Vec<ContentBlock>;
    fn deref(&self) -> &Self::Target {
        &self.results
    }
}

impl std::ops::DerefMut for ToolCallOutcome {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.results
    }
}

fn describe_effect(registry: &ToolRegistry, name: &str, input: &serde_json::Value) -> EffectDescriptor {
    registry.get(name).map_or_else(
        || EffectDescriptor {
            class: EffectClass::ExternalSideEffect,
            action: format!("Unknown tool {name}"),
            resources: ResourceFootprint {
                external_resources: vec![format!("tool:{name}")],
                ..Default::default()
            },
            replay_policy: EffectReplayPolicy::ReconcileRequired,
        },
        |tool| tool.describe_effect(input),
    )
}

fn prepare_tool_effect(
    registry: &ToolRegistry,
    name: &str,
    input: &serde_json::Value,
    effect_id: &str,
) -> Result<PreparedToolEffect, String> {
    registry.get(name).map_or_else(
        || {
            Ok(PreparedToolEffect::new(
                describe_effect(registry, name, input),
                ToolExecutionContext::new(effect_id),
            ))
        },
        |tool| tool.prepare_effect(effect_id, input),
    )
}

/// Backward-compatible terminal execution entry point.
pub async fn execute_tool_calls(
    registry: &ToolRegistry,
    tool_calls: &[ContentBlock],
    confirmer: &Arc<Mutex<ToolConfirmer>>,
    hooks: Option<&mut HookEngine>,
    compaction_level: solaris_compact::CompactLevel,
    toon_enabled: bool,
) -> Result<ToolCallOutcome, ExecutionControl> {
    let mode = PermissionMode::Auto;
    execute_tool_calls_with_policy(
        registry,
        tool_calls,
        confirmer,
        mode,
        PermissionCeiling::unrestricted(),
        hooks,
        compaction_level,
        toon_enabled,
    )
    .await
}

/// Backward-compatible context-free policy entry point.
#[allow(clippy::too_many_arguments)]
pub async fn execute_tool_calls_with_policy(
    registry: &ToolRegistry,
    tool_calls: &[ContentBlock],
    confirmer: &Arc<Mutex<ToolConfirmer>>,
    permission_mode: PermissionMode,
    permission_ceiling: PermissionCeiling,
    mut hooks: Option<&mut HookEngine>,
    compaction_level: solaris_compact::CompactLevel,
    toon_enabled: bool,
) -> Result<ToolCallOutcome, ExecutionControl> {
    let execution_context = ephemeral_effect_context(permission_mode, permission_ceiling);
    if let Some(hook_engine) = hooks.as_deref_mut() {
        hook_engine.set_executor(Arc::new(EffectHookExecutor::new(execution_context.clone())));
    }
    execute_tool_calls_with_policy_and_context(
        registry,
        tool_calls,
        confirmer,
        permission_mode,
        permission_ceiling,
        &execution_context,
        hooks,
        compaction_level,
        toon_enabled,
    )
    .await
}

/// Execute terminal tool calls through the effect-aware permission policy.
#[allow(clippy::too_many_arguments)]
pub async fn execute_tool_calls_with_policy_and_context(
    registry: &ToolRegistry,
    tool_calls: &[ContentBlock],
    confirmer: &Arc<Mutex<ToolConfirmer>>,
    permission_mode: PermissionMode,
    permission_ceiling: PermissionCeiling,
    execution_context: &EffectExecutionContext,
    hooks: Option<&mut HookEngine>,
    compaction_level: solaris_compact::CompactLevel,
    toon_enabled: bool,
) -> Result<ToolCallOutcome, ExecutionControl> {
    execute_tool_calls_with_policy_context(
        registry,
        tool_calls,
        confirmer,
        permission_mode,
        permission_ceiling,
        execution_context,
        hooks,
        compaction_level,
        toon_enabled,
    )
    .await
}
/// Signal that the user wants to abort
#[derive(Debug)]
pub enum ExecutionControl {
    Quit,
}

async fn run_pre_tool_guard(call: &ContentBlock, hooks: Option<&HookEngine>) -> Option<(ContentBlock, ToolStatus)> {
    let ContentBlock::ToolUse { id, name, input, .. } = call else {
        return None;
    };
    let hook_engine = hooks?;
    if let Err(error) = hook_engine.run_pre_tool_use_for_call(id, name, input).await {
        let status = if error.is_outcome_unknown() {
            ToolStatus::OutcomeUnknown
        } else {
            ToolStatus::Denied
        };
        return Some((
            ContentBlock::ToolResult {
                tool_use_id: id.clone(),
                content: format!("Blocked by hook: {error}"),
                is_error: true,
            },
            status,
        ));
    }
    None
}

async fn reconcile_pre_hooks_for_completed_tool(
    call: &ContentBlock,
    hooks: Option<&HookEngine>,
) -> Result<(), HookError> {
    let Some(hook_engine) = hooks else {
        return Ok(());
    };
    let ContentBlock::ToolUse { id, name, input, .. } = call else {
        return Ok(());
    };
    hook_engine
        .reconcile_pre_tool_use_for_completed_call(id, name, input)
        .await
}

async fn run_effect_aware_post_hooks(
    call: &ContentBlock,
    result: &ContentBlock,
    hooks: Option<&HookEngine>,
) -> Result<(), HookError> {
    let Some(hook_engine) = hooks else {
        return Ok(());
    };
    let ContentBlock::ToolUse { id, name, input, .. } = call else {
        return Ok(());
    };
    let ContentBlock::ToolResult { content, .. } = result else {
        return Ok(());
    };
    for message in hook_engine.run_post_tool_use_for_call(id, name, input, content).await? {
        tracing::info!(
            target: "solaris_agent",
            hook_output_digest = %stable_digest_bytes(message.as_bytes()),
            hook_output_bytes = message.len(),
            "post-tool-use hook completed"
        );
    }
    Ok(())
}

fn post_hook_outcome_unknown(id: &str, error: HookError) -> (ContentBlock, Option<ContextModifier>, ToolStatus) {
    (
        permission_error_result(
            id,
            format!("tool outcome was saved, but a post-tool hook requires reconciliation: {error}"),
        ),
        None,
        ToolStatus::OutcomeUnknown,
    )
}

#[allow(clippy::too_many_arguments)]
pub async fn execute_tool_calls_with_policy_context(
    registry: &ToolRegistry,
    tool_calls: &[ContentBlock],
    confirmer: &Arc<Mutex<ToolConfirmer>>,
    permission_mode: PermissionMode,
    permission_ceiling: PermissionCeiling,
    execution_context: &EffectExecutionContext,
    mut hooks: Option<&mut HookEngine>,
    compaction_level: solaris_compact::CompactLevel,
    toon_enabled: bool,
) -> Result<ToolCallOutcome, ExecutionControl> {
    let mut results = Vec::new();
    let mut modifiers = Vec::new();
    let mut statuses = Vec::new();
    let mut metadata = BTreeMap::new();
    let mut pending_safe = Vec::new();
    let mut stop_after_unknown = false;
    macro_rules! flush_safe {
        () => {
            if flush_concurrency_safe_calls(
                registry,
                &mut pending_safe,
                execution_context,
                &mut hooks,
                compaction_level,
                toon_enabled,
                &mut results,
                &mut modifiers,
                &mut statuses,
                &mut metadata,
                None,
            )
            .await
            {
                stop_after_unknown = true;
            }
        };
    }
    for call in tool_calls {
        let ContentBlock::ToolUse { id, name, input, .. } = call else {
            continue;
        };
        if stop_after_unknown {
            results.push(skipped_after_reconciliation(id));
            modifiers.push(None);
            statuses.push(ToolStatus::Aborted);
            continue;
        }
        let effect_id = execution_context.effect_id_for_call(id);
        let Some(tool) = registry.get(name) else {
            flush_safe!();
            let (reason, status) =
                unavailable_tool_recovery(execution_context, effect_id.as_str(), format!("Unknown tool: {name}"));
            results.push(permission_error_result(id, reason));
            modifiers.push(None);
            statuses.push(status);
            stop_after_unknown |= status == ToolStatus::OutcomeUnknown;
            continue;
        };
        let concurrency_safe = tool.is_concurrency_safe(input);
        if !concurrency_safe {
            flush_safe!();
            if stop_after_unknown {
                results.push(skipped_after_reconciliation(id));
                modifiers.push(None);
                statuses.push(ToolStatus::Aborted);
                continue;
            }
        }
        let capability = tool.permission_capability().to_owned();
        let (descriptor, tool_execution) = match prepare_tool_effect(registry, name, input, effect_id.as_str()) {
            Ok(prepared) => prepared.into_parts(),
            Err(reason) => {
                flush_safe!();
                let (reason, status) = unavailable_tool_recovery(execution_context, effect_id.as_str(), reason);
                results.push(permission_error_result(id, reason));
                modifiers.push(None);
                statuses.push(status);
                stop_after_unknown |= status == ToolStatus::OutcomeUnknown;
                continue;
            }
        };
        let tool_execution = with_process_workspace_root(descriptor.class, tool_execution, execution_context);
        let request = execution_context.effect_request(id, &capability, input, descriptor);
        match execution_context
            .recover_effect_with_tool_implementation(&request, tool_execution.prepared_implementation())
        {
            Ok(crate::execution_context::EffectRecoveryDecision::Execute) => {}
            Ok(crate::execution_context::EffectRecoveryDecision::Reuse { is_error, output }) => {
                flush_safe!();
                if stop_after_unknown {
                    results.push(skipped_after_reconciliation(id));
                    modifiers.push(None);
                    statuses.push(ToolStatus::Aborted);
                    continue;
                }
                if let Err(error) = reconcile_pre_hooks_for_completed_tool(call, hooks.as_deref()).await {
                    results.push(permission_error_result(
                        id,
                        format!("tool outcome was saved, but pre-tool hook history requires reconciliation: {error}"),
                    ));
                    modifiers.push(None);
                    statuses.push(ToolStatus::OutcomeUnknown);
                    stop_after_unknown |= true;
                    continue;
                }
                let recovered = match DurableToolOutcome::decode(output, is_error) {
                    Ok(recovered) => recovered,
                    Err(reason) => {
                        results.push(permission_error_result(id, reason));
                        modifiers.push(None);
                        statuses.push(ToolStatus::OutcomeUnknown);
                        stop_after_unknown |= true;
                        continue;
                    }
                };
                if recovered.is_legacy()
                    && (tool.context_modifier_for(input).is_some() || tool.skill_hooks_for(input).is_some())
                {
                    results.push(permission_error_result(
                        id,
                        "legacy durable tool outcome cannot restore context state safely; reconciliation is required",
                    ));
                    modifiers.push(None);
                    statuses.push(ToolStatus::OutcomeUnknown);
                    stop_after_unknown |= true;
                    continue;
                }
                let mut status = recovered.replay_status();
                let mut modifier = recovered.modifier;
                let mut result_metadata = recovered.metadata;
                let mut block = ContentBlock::ToolResult {
                    tool_use_id: id.clone(),
                    content: recovered.content,
                    is_error: recovered.is_error,
                };
                if let Err(error) = run_effect_aware_post_hooks(call, &block, hooks.as_deref()).await {
                    (block, modifier, status) = post_hook_outcome_unknown(id, error);
                    result_metadata = None;
                    stop_after_unknown |= true;
                }
                let is_error = block_is_error(&block);
                results.push(block);
                modifiers.push(modifier);
                statuses.push(status);
                if let Some(result_metadata) = result_metadata {
                    metadata.insert(id.clone(), result_metadata);
                }
                if !is_error && status != ToolStatus::OutcomeUnknown {
                    maybe_merge_skill_hooks(registry, call, hooks.as_deref_mut());
                }
                continue;
            }
            Ok(crate::execution_context::EffectRecoveryDecision::Reconcile { reason }) => {
                flush_safe!();
                results.push(permission_error_result(id, reason));
                modifiers.push(None);
                statuses.push(ToolStatus::OutcomeUnknown);
                stop_after_unknown |= true;
                continue;
            }
            Err(reason) => {
                flush_safe!();
                results.push(permission_error_result(id, reason));
                modifiers.push(None);
                statuses.push(ToolStatus::OutcomeUnknown);
                stop_after_unknown |= true;
                continue;
            }
        }
        let mut evaluation = execution_context.evaluate_with(&request, permission_mode, permission_ceiling);
        if let Err(error) = execution_context.record_permission_decision(
            &request,
            &evaluation,
            if evaluation.matched_lease { "lease" } else { "policy" },
        ) {
            flush_safe!();
            results.push(permission_error_result(
                id,
                format!("failed to persist permission decision: {error}"),
            ));
            modifiers.push(None);
            statuses.push(ToolStatus::Failed);
            continue;
        }
        if evaluation.decision == PermissionDecision::Deny {
            flush_safe!();
            results.push(permission_error_result(id, evaluation.reason));
            modifiers.push(None);
            statuses.push(ToolStatus::Denied);
            continue;
        }
        if matches!(
            evaluation.decision,
            PermissionDecision::Ask | PermissionDecision::AutoReview
        ) {
            flush_safe!();
            let input_display = serde_json::to_string(input).unwrap_or_default();
            let confirmation = match confirmer.lock() {
                Ok(mut confirmer) => confirmer.check(name, &truncate_display(&input_display, 200)),
                Err(_) => {
                    results.push(permission_error_result(id, "tool confirmation state unavailable"));
                    modifiers.push(None);
                    statuses.push(ToolStatus::Failed);
                    continue;
                }
            };
            match confirmation {
                ConfirmResult::ApprovedOnce | ConfirmResult::ApprovedAlways => {
                    let always = matches!(confirmation, ConfirmResult::ApprovedAlways);
                    if let Err(error) = execution_context.issue_approval_lease(&request, always) {
                        flush_safe!();
                        results.push(permission_error_result(
                            id,
                            format!("failed to persist capability lease: {error}"),
                        ));
                        modifiers.push(None);
                        statuses.push(ToolStatus::Failed);
                        continue;
                    }
                    evaluation = execution_context.evaluate_with(&request, permission_mode, permission_ceiling);
                    if let Err(error) =
                        execution_context.record_permission_decision(&request, &evaluation, "interactive_approval")
                    {
                        flush_safe!();
                        results.push(permission_error_result(
                            id,
                            format!("failed to persist approved decision: {error}"),
                        ));
                        modifiers.push(None);
                        statuses.push(ToolStatus::Failed);
                        continue;
                    }
                    if evaluation.decision != PermissionDecision::Allow {
                        flush_safe!();
                        results.push(permission_error_result(
                            id,
                            "approved lease did not satisfy current permission policy",
                        ));
                        modifiers.push(None);
                        statuses.push(ToolStatus::Denied);
                        continue;
                    }
                }
                ConfirmResult::Denied => {
                    flush_safe!();
                    results.push(ContentBlock::ToolResult {
                        tool_use_id: id.clone(),
                        content: "Tool execution denied by user".to_owned(),
                        is_error: true,
                    });
                    modifiers.push(None);
                    statuses.push(ToolStatus::Denied);
                    continue;
                }
                ConfirmResult::Quit => {
                    flush_safe!();
                    let _ = stop_after_unknown;
                    return Err(ExecutionControl::Quit);
                }
            }
        }
        if let Some((blocked, status)) = run_pre_tool_guard(call, hooks.as_deref()).await {
            flush_safe!();
            results.push(blocked);
            modifiers.push(None);
            statuses.push(status);
            stop_after_unknown |= status == ToolStatus::OutcomeUnknown;
            continue;
        }
        let registration = execution_context.remember_approved_request_with_registered_tool_context(
            request,
            name,
            tool_execution,
            permission_mode,
            permission_ceiling,
        );
        if concurrency_safe {
            pending_safe.push((call, registration));
            continue;
        }
        let (mut block, mut modifier, mut status, mut result_metadata) = execute_single_with_effect_context(
            registry,
            call,
            None,
            execution_context,
            compaction_level,
            toon_enabled,
            Some(registration),
        )
        .await;
        if status != ToolStatus::OutcomeUnknown
            && let Err(error) = run_effect_aware_post_hooks(call, &block, hooks.as_deref()).await
        {
            (block, modifier, status) = post_hook_outcome_unknown(id, error);
            result_metadata = None;
            stop_after_unknown |= true;
        }
        if !block_is_error(&block) && status != ToolStatus::OutcomeUnknown {
            maybe_merge_skill_hooks(registry, call, hooks.as_deref_mut());
        }
        stop_after_unknown |= status == ToolStatus::OutcomeUnknown;
        results.push(block);
        modifiers.push(modifier);
        statuses.push(status);
        if let Some(result_metadata) = result_metadata {
            metadata.insert(id.clone(), result_metadata);
        }
    }
    let _ = flush_concurrency_safe_calls(
        registry,
        &mut pending_safe,
        execution_context,
        &mut hooks,
        compaction_level,
        toon_enabled,
        &mut results,
        &mut modifiers,
        &mut statuses,
        &mut metadata,
        None,
    )
    .await;
    Ok(ToolCallOutcome {
        results,
        modifiers,
        statuses,
        metadata,
    })
}

async fn execute_single_with_effect_context(
    registry: &ToolRegistry,
    call: &ContentBlock,
    hooks: Option<&HookEngine>,
    execution_context: &EffectExecutionContext,
    compaction_level: solaris_compact::CompactLevel,
    toon_enabled: bool,
    registration: Option<ApprovedRequestRegistration>,
) -> (
    ContentBlock,
    Option<ContextModifier>,
    ToolStatus,
    Option<ToolResultMetadata>,
) {
    let ContentBlock::ToolUse { id, .. } = call else {
        unreachable!("effect execution requires ToolUse")
    };
    let Some(approved_consumption) = execution_context.take_approved_request_for_call(id) else {
        return (
            permission_error_result(id, "missing approved effect request before execution"),
            None,
            ToolStatus::Failed,
            None,
        );
    };
    if let Some(mut registration) = registration {
        registration.disarm();
    }
    let approved = &*approved_consumption;
    let request = &approved.request;
    let _effect_permit = match execution_context.acquire_effect_permit().await {
        Ok(permit) => permit,
        Err(reason) => {
            execution_context.clear_request(&request.effect_id);
            return (
                permission_error_result(id, format!("effect resource budget denied: {reason}")),
                None,
                ToolStatus::Denied,
                None,
            );
        }
    };
    let tool_execution = match execution_context.revalidate(registry, approved) {
        Ok(tool_execution) => tool_execution,
        Err(reason) => {
            let _ = execution_context.record_revalidation_failure(request, &reason);
            execution_context.clear_request(&request.effect_id);
            return (permission_error_result(id, reason), None, ToolStatus::Denied, None);
        }
    };
    let Some(tool) = registry.get(&approved.registered_tool_name) else {
        execution_context.clear_request(&request.effect_id);
        return (
            permission_error_result(
                id,
                format!("tool {} disappeared before execution", approved.registered_tool_name),
            ),
            None,
            ToolStatus::Failed,
            None,
        );
    };
    let prepared_execution = match tool.prepare_execution(request.effective_input.clone(), tool_execution) {
        Ok(execution) => execution,
        Err(reason) => {
            let _ = execution_context.record_revalidation_failure(request, &reason);
            execution_context.clear_request(&request.effect_id);
            return (permission_error_result(id, reason), None, ToolStatus::Denied, None);
        }
    };
    if approved.tool_execution.prepared_implementation() != prepared_execution.implementation()
        && approved.tool_execution.prepared_implementation().is_some()
    {
        let reason = "prepared tool implementation changed after recovery validation";
        let _ = execution_context.record_revalidation_failure(request, reason);
        return (permission_error_result(id, reason), None, ToolStatus::Denied, None);
    }
    let intent_result = prepared_execution.implementation().map_or_else(
        || execution_context.record_effect_intent(request),
        |implementation| execution_context.record_effect_intent_with_tool_implementation(request, implementation),
    );
    if let Err(error) = intent_result {
        execution_context.clear_request(&request.effect_id);
        return (
            permission_error_result(id, format!("failed to persist effect intent: {error}")),
            None,
            ToolStatus::Failed,
            None,
        );
    }
    let mut outcome_guard = crate::execution_context::EffectOutcomeGuard::new(
        execution_context.clone(),
        request.clone(),
        "tool execution cancelled before a terminal result",
    );
    let (mut block, mut modifier, mut status, metadata) = execute_single(
        registry,
        call,
        prepared_execution,
        hooks,
        compaction_level,
        toon_enabled,
    )
    .await;
    if let ContentBlock::ToolResult { content, is_error, .. } = &block {
        let durable = DurableToolOutcome::new(content.clone(), *is_error, status, modifier.clone(), metadata.clone());
        let persistence = durable
            .encode()
            .map_err(std::io::Error::other)
            .and_then(|output| outcome_guard.complete_canonical(*is_error, &output));
        match persistence {
            Ok(EffectOutcomeCompletion::Committed) => {}
            Ok(EffectOutcomeCompletion::OutcomeUnknown) => {
                modifier = None;
                status = ToolStatus::OutcomeUnknown;
            }
            Err(error) => {
                block = permission_error_result(
                    id,
                    format!("tool executed but effect outcome persistence failed; reconciliation required: {error}"),
                );
                modifier = None;
                status = ToolStatus::OutcomeUnknown;
            }
        }
    }
    (block, modifier, status, metadata)
}

#[allow(clippy::too_many_arguments)]
async fn flush_concurrency_safe_calls(
    registry: &ToolRegistry,
    pending: &mut Vec<(&ContentBlock, ApprovedRequestRegistration)>,
    execution_context: &EffectExecutionContext,
    hooks: &mut Option<&mut HookEngine>,
    compaction_level: solaris_compact::CompactLevel,
    toon_enabled: bool,
    results: &mut Vec<ContentBlock>,
    modifiers: &mut Vec<Option<ContextModifier>>,
    statuses: &mut Vec<ToolStatus>,
    metadata: &mut BTreeMap<String, ToolResultMetadata>,
    protocol: Option<(&dyn ProtocolEmitter, &str)>,
) -> bool {
    if pending.is_empty() {
        return false;
    }
    let calls = std::mem::take(pending);
    if let Some((writer, msg_id)) = protocol {
        for (call, _) in &calls {
            if let ContentBlock::ToolUse { id, name, .. } = call {
                let _ = writer.emit(&ProtocolEvent::ToolRunning {
                    msg_id: msg_id.to_owned(),
                    call_id: id.clone(),
                    tool_name: name.clone(),
                });
            }
        }
    }
    let completed = join_all(calls.into_iter().map(|(call, registration)| async move {
        let result = execute_single_with_effect_context(
            registry,
            call,
            None,
            execution_context,
            compaction_level,
            toon_enabled,
            Some(registration),
        )
        .await;
        (call, result)
    }))
    .await;
    let mut encountered_unknown = false;
    for (call, (mut block, mut modifier, mut status, mut result_metadata)) in completed {
        let call_id = match call {
            ContentBlock::ToolUse { id, .. } => id.as_str(),
            _ => continue,
        };
        if status != ToolStatus::OutcomeUnknown
            && let Err(error) = run_effect_aware_post_hooks(call, &block, hooks.as_deref()).await
        {
            (block, modifier, status) = post_hook_outcome_unknown(call_id, error);
            result_metadata = None;
        }
        encountered_unknown |= status == ToolStatus::OutcomeUnknown;
        if let (
            Some((writer, msg_id)),
            ContentBlock::ToolUse { id, name, .. },
            ContentBlock::ToolResult { content, .. },
        ) = (protocol, call, &block)
        {
            let _ = writer.emit(&ProtocolEvent::ToolResult {
                msg_id: msg_id.to_owned(),
                call_id: id.clone(),
                tool_name: name.clone(),
                status,
                output: content.clone(),
                output_type: OutputType::Text,
                metadata: result_metadata.clone(),
            });
        }
        if !block_is_error(&block) && status != ToolStatus::OutcomeUnknown {
            maybe_merge_skill_hooks(registry, call, hooks.as_deref_mut());
        }
        results.push(block);
        modifiers.push(modifier);
        statuses.push(status);
        if let Some(result_metadata) = result_metadata {
            metadata.insert(call_id.to_owned(), result_metadata);
        }
    }
    encountered_unknown
}

/// If `call` is a Skill tool call that returned successfully, parse and merge
/// its declared hooks into the active HookEngine.
/// If `call` is a Skill tool call that returned successfully, merge skill hooks into the engine.
fn merge_skill_hooks_into(engine: &mut HookEngine, registry: &ToolRegistry, call: &ContentBlock) {
    let ContentBlock::ToolUse { name, input, .. } = call else {
        return;
    };
    if name != "Skill" {
        return;
    }
    let Some(tool) = registry.get(name) else {
        return;
    };
    if let Some(skill_hooks) = tool.skill_hooks_for(input) {
        engine.merge_hooks(skill_hooks);
    }
}

fn maybe_merge_skill_hooks(registry: &ToolRegistry, call: &ContentBlock, hooks: Option<&mut HookEngine>) {
    if let Some(engine) = hooks {
        merge_skill_hooks_into(engine, registry, call);
    }
}

/// Returns true when a ContentBlock::ToolResult has is_error=true.
fn block_is_error(block: &ContentBlock) -> bool {
    matches!(block, ContentBlock::ToolResult { is_error: true, .. })
}

/// When a deferred tool fails AND the input is missing required fields from
/// its full schema, append a hint telling the LLM to call ToolSearch first.
/// If required fields are all present (or the schema has none), the original
/// error is returned unchanged — the failure is a runtime issue, not a
/// missing-schema problem.
pub(super) fn maybe_append_deferred_hint(
    original_error: &str,
    schema: serde_json::Value,
    input: &serde_json::Value,
) -> String {
    let missing: Vec<&str> = schema["required"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str())
                .filter(|key| input.get(key).is_none())
                .collect()
        })
        .unwrap_or_default();

    if missing.is_empty() {
        return original_error.to_string();
    }

    format!(
        "{}\n\nThis is a deferred tool — its full parameter schema was not loaded. \
         Call ToolSearch to load the schema, then retry.",
        original_error
    )
}

pub(super) fn truncate_result(content: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    if max_chars == 1 {
        return content
            .chars()
            .next()
            .map(|character| character.to_string())
            .unwrap_or_default();
    }
    if content.len() <= max_chars {
        return content.to_string();
    }
    let half = max_chars / 2;
    // Find char boundaries to avoid panicking on multi-byte characters
    let head_end = content
        .char_indices()
        .nth(half)
        .map(|(i, _)| i)
        .unwrap_or(content.len());
    let tail_start = content.char_indices().rev().nth(half - 1).map(|(i, _)| i).unwrap_or(0);
    let head = &content[..head_end];
    let tail = &content[tail_start..];
    format!(
        "{}\n\n... [truncated {} chars] ...\n\n{}",
        head,
        content.len() - max_chars,
        tail
    )
}

fn truncate_display(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        // Find a char boundary to avoid panicking on multi-byte characters
        let end = s.char_indices().nth(max).map(|(i, _)| i).unwrap_or(s.len());
        format!("{}...", &s[..end])
    }
}

#[cfg(test)]
#[path = "orchestration_test.rs"]
mod orchestration_test;
