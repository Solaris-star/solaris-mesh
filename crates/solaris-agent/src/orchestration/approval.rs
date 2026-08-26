use std::collections::BTreeMap;
use std::sync::Arc;

use solaris_config::hooks::HookEngine;
use solaris_protocol::commands::ApprovalScope;
use solaris_protocol::events::{OutputType, ProtocolEvent, ToolCategory, ToolInfo, ToolStatus};
use solaris_protocol::writer::ProtocolEmitter;
use solaris_protocol::{ToolApprovalManager, ToolApprovalResult};
use solaris_tools::registry::ToolRegistry;
use solaris_types::effect::{EffectAuditProjection, EffectRequest};
use solaris_types::message::ContentBlock;
use solaris_types::permission::{PermissionCeiling, PermissionDecision};

use crate::execution_context::{EffectExecutionContext, EffectRecoveryDecision};

use super::{
    DurableToolOutcome, EffectHookExecutor, ExecutionControl, ToolCallOutcome, block_is_error,
    ephemeral_effect_context, execute_single_with_effect_context, flush_concurrency_safe_calls,
    maybe_merge_skill_hooks, prepare_tool_effect, reconcile_pre_hooks_for_completed_tool, run_effect_aware_post_hooks,
    run_pre_tool_guard, skipped_after_reconciliation, unavailable_tool_recovery,
};

pub(super) fn permission_error_result(id: &str, message: impl Into<String>) -> ContentBlock {
    ContentBlock::ToolResult {
        tool_use_id: id.to_owned(),
        content: message.into(),
        is_error: true,
    }
}

fn emit_terminal_tool_result(
    writer: &dyn ProtocolEmitter,
    msg_id: &str,
    call_id: &str,
    tool_name: &str,
    status: ToolStatus,
    result: &ContentBlock,
) {
    let ContentBlock::ToolResult { content, .. } = result else {
        return;
    };
    let _ = writer.emit(&ProtocolEvent::ToolResult {
        msg_id: msg_id.to_owned(),
        call_id: call_id.to_owned(),
        tool_name: tool_name.to_owned(),
        status,
        output: content.clone(),
        output_type: OutputType::Text,
        metadata: None,
    });
}

pub(super) fn host_safe_tool_info(name: &str, category: ToolCategory, request: &EffectRequest) -> ToolInfo {
    let effect = EffectAuditProjection::from_descriptor(&request.descriptor);
    ToolInfo {
        name: name.to_owned(),
        category,
        args: serde_json::json!({
            "redacted": true,
            "input_digest": request.input_digest,
        }),
        effect: Some(Box::new(effect.to_host_descriptor())),
        description: format!("{} ({})", effect.action.summary, effect.action.digest),
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn execute_tool_calls_with_approval_context(
    registry: &ToolRegistry,
    tool_calls: &[ContentBlock],
    approval_manager: &Arc<ToolApprovalManager>,
    writer: &Arc<dyn ProtocolEmitter>,
    msg_id: &str,
    auto_approve: bool,
    allow_list: &[String],
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
                Some((writer.as_ref(), msg_id)),
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
            let result = skipped_after_reconciliation(id);
            emit_terminal_tool_result(writer.as_ref(), msg_id, id, name, ToolStatus::Aborted, &result);
            results.push(result);
            modifiers.push(None);
            statuses.push(ToolStatus::Aborted);
            continue;
        }
        let tool = registry.get(name);
        let category = tool.map(|value| value.category()).unwrap_or(ToolCategory::Exec);
        let concurrency_safe = tool.is_some_and(|tool| tool.is_concurrency_safe(input));
        if !concurrency_safe {
            flush_safe!();
            if stop_after_unknown {
                let result = skipped_after_reconciliation(id);
                emit_terminal_tool_result(writer.as_ref(), msg_id, id, name, ToolStatus::Aborted, &result);
                results.push(result);
                modifiers.push(None);
                statuses.push(ToolStatus::Aborted);
                continue;
            }
        }
        let effect_id = execution_context.effect_id_for_call(id);
        let (descriptor, tool_execution) = match prepare_tool_effect(registry, name, input, effect_id.as_str()) {
            Ok(prepared) => prepared.into_parts(),
            Err(reason) => {
                flush_safe!();
                let (reason, status) = unavailable_tool_recovery(execution_context, effect_id.as_str(), reason);
                let result = permission_error_result(id, reason);
                emit_terminal_tool_result(writer.as_ref(), msg_id, id, name, status, &result);
                results.push(result);
                modifiers.push(None);
                statuses.push(status);
                stop_after_unknown |= status == ToolStatus::OutcomeUnknown;
                continue;
            }
        };
        let tool_execution = super::with_process_workspace_root(descriptor.class, tool_execution, execution_context);
        let capability = tool.map_or_else(|| name.clone(), |tool| tool.permission_capability().to_owned());
        let request = execution_context.effect_request(id, &capability, input, descriptor);
        match execution_context
            .recover_effect_with_tool_implementation(&request, tool_execution.prepared_implementation())
        {
            Ok(EffectRecoveryDecision::Execute) => {}
            Ok(EffectRecoveryDecision::Reuse { is_error, output }) => {
                flush_safe!();
                if stop_after_unknown {
                    let result = skipped_after_reconciliation(id);
                    emit_terminal_tool_result(writer.as_ref(), msg_id, id, name, ToolStatus::Aborted, &result);
                    results.push(result);
                    modifiers.push(None);
                    statuses.push(ToolStatus::Aborted);
                    continue;
                }
                if let Err(error) = reconcile_pre_hooks_for_completed_tool(call, hooks.as_deref()).await {
                    let result = permission_error_result(
                        id,
                        format!("tool outcome was saved, but pre-tool hook history requires reconciliation: {error}"),
                    );
                    emit_terminal_tool_result(writer.as_ref(), msg_id, id, name, ToolStatus::OutcomeUnknown, &result);
                    results.push(result);
                    modifiers.push(None);
                    statuses.push(ToolStatus::OutcomeUnknown);
                    stop_after_unknown |= true;
                    continue;
                }
                let recovered = match DurableToolOutcome::decode(output, is_error) {
                    Ok(recovered) => recovered,
                    Err(reason) => {
                        let result = permission_error_result(id, reason);
                        emit_terminal_tool_result(
                            writer.as_ref(),
                            msg_id,
                            id,
                            name,
                            ToolStatus::OutcomeUnknown,
                            &result,
                        );
                        results.push(result);
                        modifiers.push(None);
                        statuses.push(ToolStatus::OutcomeUnknown);
                        stop_after_unknown |= true;
                        continue;
                    }
                };
                if recovered.is_legacy()
                    && tool.is_some_and(|tool| {
                        tool.context_modifier_for(input).is_some() || tool.skill_hooks_for(input).is_some()
                    })
                {
                    let result = permission_error_result(
                        id,
                        "legacy durable tool outcome cannot restore context state safely; reconciliation is required",
                    );
                    emit_terminal_tool_result(writer.as_ref(), msg_id, id, name, ToolStatus::OutcomeUnknown, &result);
                    results.push(result);
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
                    (block, modifier, status) = super::post_hook_outcome_unknown(id, error);
                    result_metadata = None;
                    stop_after_unknown |= true;
                }
                let ContentBlock::ToolResult { content, is_error, .. } = &block else {
                    unreachable!("post-hook recovery always returns a tool result")
                };
                let _ = writer.emit(&ProtocolEvent::ToolResult {
                    msg_id: msg_id.to_owned(),
                    call_id: id.clone(),
                    tool_name: name.clone(),
                    status,
                    output: content.clone(),
                    output_type: OutputType::Text,
                    metadata: result_metadata.clone(),
                });
                let is_error = *is_error;
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
            Ok(EffectRecoveryDecision::Reconcile { reason }) => {
                flush_safe!();
                let result = permission_error_result(id, reason);
                emit_terminal_tool_result(writer.as_ref(), msg_id, id, name, ToolStatus::OutcomeUnknown, &result);
                results.push(result);
                modifiers.push(None);
                statuses.push(ToolStatus::OutcomeUnknown);
                stop_after_unknown |= true;
                continue;
            }
            Err(reason) => {
                flush_safe!();
                let result = permission_error_result(id, reason);
                emit_terminal_tool_result(writer.as_ref(), msg_id, id, name, ToolStatus::OutcomeUnknown, &result);
                results.push(result);
                modifiers.push(None);
                statuses.push(ToolStatus::OutcomeUnknown);
                stop_after_unknown |= true;
                continue;
            }
        }
        let permission_mode = approval_manager.permission_mode();
        let mut evaluation = execution_context.evaluate_with(&request, permission_mode, permission_ceiling);
        if let Err(error) = execution_context.record_permission_decision(
            &request,
            &evaluation,
            if evaluation.matched_lease { "lease" } else { "policy" },
        ) {
            flush_safe!();
            let result = permission_error_result(id, format!("failed to persist permission decision: {error}"));
            emit_terminal_tool_result(writer.as_ref(), msg_id, id, name, ToolStatus::Failed, &result);
            results.push(result);
            modifiers.push(None);
            statuses.push(ToolStatus::Failed);
            continue;
        }
        if evaluation.decision == PermissionDecision::Deny {
            flush_safe!();
            let _ = writer.emit(&ProtocolEvent::ToolCancelled {
                msg_id: msg_id.to_owned(),
                call_id: id.clone(),
                status: ToolStatus::Denied,
                reason: evaluation.reason.clone(),
            });
            let result = permission_error_result(id, evaluation.reason);
            emit_terminal_tool_result(writer.as_ref(), msg_id, id, name, ToolStatus::Denied, &result);
            results.push(result);
            modifiers.push(None);
            statuses.push(ToolStatus::Denied);
            continue;
        }
        if matches!(
            evaluation.decision,
            PermissionDecision::Ask | PermissionDecision::AutoReview
        ) {
            flush_safe!();
            let legacy_allowed =
                auto_approve || allow_list.iter().any(|value| value == name) || approval_manager.is_auto_approved(name);
            let approval_scope = if legacy_allowed {
                Some(ApprovalScope::Always)
            } else {
                let _ = writer.emit(&ProtocolEvent::ToolRequest {
                    msg_id: msg_id.to_owned(),
                    call_id: id.clone(),
                    run_id: Some(execution_context.run_id().to_string()),
                    agent_id: Some(execution_context.agent_id().to_string()),
                    operation_id: Some(request.operation_id.to_string()),
                    effect_id: Some(request.effect_id.to_string()),
                    tool: host_safe_tool_info(name, category, &request),
                });
                let rx = approval_manager.request_approval(id, name);
                match rx.await {
                    Ok(ToolApprovalResult::Approved { scope }) => Some(scope),
                    Ok(ToolApprovalResult::Denied { reason }) => {
                        flush_safe!();
                        let _ = writer.emit(&ProtocolEvent::ToolCancelled {
                            msg_id: msg_id.to_owned(),
                            call_id: id.clone(),
                            status: ToolStatus::Denied,
                            reason: reason.clone(),
                        });
                        let result = permission_error_result(id, format!("Tool denied: {reason}"));
                        emit_terminal_tool_result(writer.as_ref(), msg_id, id, name, ToolStatus::Denied, &result);
                        results.push(result);
                        modifiers.push(None);
                        statuses.push(ToolStatus::Denied);
                        continue;
                    }
                    Err(_) => {
                        flush_safe!();
                        let _ = stop_after_unknown;
                        let reason = "approval request cancelled";
                        let _ = writer.emit(&ProtocolEvent::ToolCancelled {
                            msg_id: msg_id.to_owned(),
                            call_id: id.clone(),
                            status: ToolStatus::Aborted,
                            reason: reason.to_owned(),
                        });
                        let result = permission_error_result(id, reason);
                        emit_terminal_tool_result(writer.as_ref(), msg_id, id, name, ToolStatus::Aborted, &result);
                        return Err(ExecutionControl::Quit);
                    }
                }
            };
            if let Some(scope) = approval_scope {
                if let Err(error) = execution_context.ensure_session_fence() {
                    flush_safe!();
                    let result = permission_error_result(
                        id,
                        format!("session lease expired while waiting for approval: {error}"),
                    );
                    emit_terminal_tool_result(writer.as_ref(), msg_id, id, name, ToolStatus::Aborted, &result);
                    results.push(result);
                    modifiers.push(None);
                    statuses.push(ToolStatus::Aborted);
                    continue;
                }
                if let Err(error) =
                    execution_context.issue_approval_lease(&request, matches!(scope, ApprovalScope::Always))
                {
                    flush_safe!();
                    let result = permission_error_result(id, format!("failed to persist capability lease: {error}"));
                    emit_terminal_tool_result(writer.as_ref(), msg_id, id, name, ToolStatus::Failed, &result);
                    results.push(result);
                    modifiers.push(None);
                    statuses.push(ToolStatus::Failed);
                    continue;
                }
                evaluation = execution_context.evaluate_with(&request, permission_mode, permission_ceiling);
                if let Err(error) = execution_context.record_permission_decision(&request, &evaluation, "host_approval")
                {
                    flush_safe!();
                    let result = permission_error_result(id, format!("failed to persist approved decision: {error}"));
                    emit_terminal_tool_result(writer.as_ref(), msg_id, id, name, ToolStatus::Failed, &result);
                    results.push(result);
                    modifiers.push(None);
                    statuses.push(ToolStatus::Failed);
                    continue;
                }
                if evaluation.decision != PermissionDecision::Allow {
                    flush_safe!();
                    let result =
                        permission_error_result(id, "approved lease did not satisfy current permission policy");
                    emit_terminal_tool_result(writer.as_ref(), msg_id, id, name, ToolStatus::Denied, &result);
                    results.push(result);
                    modifiers.push(None);
                    statuses.push(ToolStatus::Denied);
                    continue;
                }
            }
        }
        if let Some((blocked, status)) = run_pre_tool_guard(call, hooks.as_deref()).await {
            flush_safe!();
            emit_terminal_tool_result(writer.as_ref(), msg_id, id, name, status, &blocked);
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
        let _ = writer.emit(&ProtocolEvent::ToolRunning {
            msg_id: msg_id.to_owned(),
            call_id: id.clone(),
            tool_name: name.clone(),
        });
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
            (block, modifier, status) = super::post_hook_outcome_unknown(id, error);
            result_metadata = None;
            stop_after_unknown |= true;
        }
        if let ContentBlock::ToolResult { content, .. } = &block {
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
        Some((writer.as_ref(), msg_id)),
    )
    .await;
    Ok(ToolCallOutcome {
        results,
        modifiers,
        statuses,
        metadata,
    })
}

/// Backward-compatible JSON approval entry point with an unrestricted runtime ceiling.
#[allow(clippy::too_many_arguments)]
pub async fn execute_tool_calls_with_approval(
    registry: &ToolRegistry,
    tool_calls: &[ContentBlock],
    approval_manager: &Arc<ToolApprovalManager>,
    writer: &Arc<dyn ProtocolEmitter>,
    msg_id: &str,
    auto_approve: bool,
    allow_list: &[String],
    hooks: Option<&mut HookEngine>,
    compaction_level: solaris_compact::CompactLevel,
    toon_enabled: bool,
) -> Result<ToolCallOutcome, ExecutionControl> {
    execute_tool_calls_with_approval_and_ceiling(
        registry,
        tool_calls,
        approval_manager,
        writer,
        msg_id,
        auto_approve,
        allow_list,
        PermissionCeiling::unrestricted(),
        hooks,
        compaction_level,
        toon_enabled,
    )
    .await
}

/// Execute JSON-host tool calls through effect-aware permission and approval handling.
#[allow(clippy::too_many_arguments)]
pub async fn execute_tool_calls_with_approval_and_ceiling(
    registry: &ToolRegistry,
    tool_calls: &[ContentBlock],
    approval_manager: &Arc<ToolApprovalManager>,
    writer: &Arc<dyn ProtocolEmitter>,
    msg_id: &str,
    auto_approve: bool,
    allow_list: &[String],
    permission_ceiling: PermissionCeiling,
    mut hooks: Option<&mut HookEngine>,
    compaction_level: solaris_compact::CompactLevel,
    toon_enabled: bool,
) -> Result<ToolCallOutcome, ExecutionControl> {
    let permission_mode = approval_manager.permission_mode();
    let execution_context = ephemeral_effect_context(permission_mode, permission_ceiling);
    if let Some(hook_engine) = hooks.as_deref_mut() {
        hook_engine.set_executor(Arc::new(EffectHookExecutor::new(execution_context.clone())));
    }
    execute_tool_calls_with_approval_context(
        registry,
        tool_calls,
        approval_manager,
        writer,
        msg_id,
        auto_approve,
        allow_list,
        permission_ceiling,
        &execution_context,
        hooks,
        compaction_level,
        toon_enabled,
    )
    .await
}
