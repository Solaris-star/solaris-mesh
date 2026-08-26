//! Top-level `ProtocolCommand` dispatch for the JSON stream main loop.
//!
//! Handles every command except `Message` (which needs the inner
//! select-loop machinery in `message.rs`) and the `AddMcpServer` pre-message
//! phase (handled in `pre_message.rs` before this loop starts).

use std::sync::Arc;

use serde_json::json;

use solaris_agent::engine::AgentEngine;
use solaris_agent::execution_context::secret_safe_ledger_payload;
use solaris_agent::output::OutputSink;
use solaris_agent::plugin_tool::PluginContributionDispatcher;
use solaris_agent::run_preset::{RuntimeTaskRouter, stable_workflow_run_id};
use solaris_agent::runtime_ledger::RuntimeLedger;
use solaris_agent::workflow_controller::{
    WorkflowNodeExecutor, WorkflowRunSnapshot, WorkflowRunStatus, WorkflowRuntimeIdentity,
};
use solaris_agent::workflow_executor::AgentWorkflowExecutor;
use solaris_protocol::ToolApprovalResult;
use solaris_protocol::commands::ProtocolCommand;
use solaris_protocol::events::{ProtocolEvent, RuntimeJournalRecord};
use solaris_types::config::RuntimeConfigUpdate;
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::RunId;

use super::context::{DetachedWorkflowRegistry, StreamContext, WorkflowTurnCommit};

mod plan;
mod plugin;

/// Outcome of handling one top-level command.
pub(super) enum DispatchOutcome {
    /// Keep looping.
    Continue,
    /// A `Stop` command was received — shut down.
    Stop,
}

pub(super) struct ConfigCommand {
    pub(super) request_id: Option<String>,
    pub(super) update: RuntimeConfigUpdate,
}

fn run_workflow_failure_events(request_id: &str, message: &str) -> Vec<ProtocolEvent> {
    vec![
        ProtocolEvent::Error {
            msg_id: Some(request_id.to_owned()),
            error: solaris_protocol::events::ErrorInfo {
                code: "workflow_start_failed".to_owned(),
                message: message.to_owned(),
                retryable: false,
            },
        },
        ProtocolEvent::CommandResult {
            request_id: request_id.to_owned(),
            command: "run_workflow".to_owned(),
            applied: false,
            message: Some(message.to_owned()),
            config_results: None,
        },
    ]
}

fn emit_run_workflow_failure(ctx: &StreamContext, request_id: &str, message: &str) {
    for event in run_workflow_failure_events(request_id, message) {
        let _ = ctx.writer.emit(&event);
    }
}

pub(super) fn apply_config_command(engine: &mut AgentEngine, ctx: &StreamContext, command: ConfigCommand) -> bool {
    let outcome = engine.apply_runtime_config_update(command.update);
    let changed = outcome.changed;
    let info = if !outcome.applied {
        format!("set_config rejected: {}", outcome.message)
    } else if changed {
        format!("config updated: {}", outcome.message)
    } else {
        format!("set_config applied without state changes: {}", outcome.message)
    };
    let _ = ctx.writer.emit(&ProtocolEvent::Info {
        msg_id: String::new(),
        message: info,
    });
    ctx.emit_config_command_result(command.request_id, outcome);
    changed
}

/// Handle a single top-level command (i.e. one that arrived outside of an
/// in-flight `Message`). `Message` itself is handled by the caller via
/// `message::handle`, not here.
pub(super) fn handle(cmd: ProtocolCommand, engine: &mut AgentEngine, ctx: &StreamContext) -> DispatchOutcome {
    match cmd {
        ProtocolCommand::Stop => {
            ctx.detached_workflows.cancel_all();
            return DispatchOutcome::Stop;
        }
        ProtocolCommand::Cancel { request_id, msg_id } => {
            let cancelled = ctx.detached_workflows.cancel_required_turn(&msg_id);
            ctx.emit_command_result(
                request_id,
                "cancel",
                cancelled,
                (!cancelled).then(|| "target turn is no longer active".to_owned()),
            );
            let _ = ctx.writer.emit(&ProtocolEvent::Info {
                msg_id,
                message: if cancelled {
                    "cancel requested for restored required workflow turn".to_owned()
                } else {
                    "cancel ignored because the target turn is no longer active".to_owned()
                },
            });
        }
        ProtocolCommand::CancelWorkflow { request_id, run_id } => {
            let run_id = RunId::from(run_id);
            let cancelled = ctx.detached_workflows.cancel(&run_id);
            ctx.emit_command_result(
                request_id,
                "cancel_workflow",
                cancelled,
                (!cancelled).then(|| format!("workflow run {run_id} is not active")),
            );
            let _ = ctx.writer.emit(&ProtocolEvent::Info {
                msg_id: String::new(),
                message: if cancelled {
                    format!("cancel requested for workflow run {run_id}")
                } else {
                    format!("workflow cancel ignored because run {run_id} is not active")
                },
            });
        }
        ProtocolCommand::ToolApprove {
            request_id,
            call_id,
            scope,
        } => {
            let applied = ctx.approval_manager.approve(&call_id, scope).was_applied();
            ctx.emit_command_result(
                request_id,
                "tool_approve",
                applied,
                (!applied).then(|| format!("approval {call_id} is no longer pending")),
            );
        }
        ProtocolCommand::ToolDeny {
            request_id,
            call_id,
            reason,
        } => {
            let applied = ctx
                .approval_manager
                .resolve(&call_id, ToolApprovalResult::Denied { reason })
                .was_applied();
            ctx.emit_command_result(
                request_id,
                "tool_deny",
                applied,
                (!applied).then(|| format!("approval {call_id} is no longer pending")),
            );
        }
        ProtocolCommand::InitHistory { messages, text } => {
            let mut imported: Vec<_> = messages
                .into_iter()
                .map(|message| {
                    solaris_types::message::Message::now(
                        message.role,
                        vec![solaris_types::message::ContentBlock::Text { text: message.content }],
                    )
                })
                .collect();
            if imported.is_empty()
                && let Some(text) = text
                && !text.is_empty()
            {
                imported.push(solaris_types::message::Message::now(
                    solaris_types::message::Role::System,
                    vec![solaris_types::message::ContentBlock::Text { text }],
                ));
            }
            match engine.import_history(imported) {
                Ok(count) => tracing::debug!(target: "solaris_protocol", messages = count, "InitHistory imported"),
                Err(error) => {
                    let _ = ctx.writer.emit(&ProtocolEvent::Error {
                        msg_id: None,
                        error: solaris_protocol::events::ErrorInfo {
                            code: "history_import_failed".into(),
                            message: error,
                            retryable: false,
                        },
                    });
                }
            }
        }
        ProtocolCommand::SetMode { request_id, mode } => {
            let mode_str = format!("{mode:?}").to_lowercase();
            ctx.approval_manager.set_mode(mode);
            engine.set_permission_mode(mode);
            let _ = ctx.writer.emit(&ProtocolEvent::Info {
                msg_id: String::new(),
                message: format!("mode updated: {}", ctx.approval_manager.current_mode()),
            });
            ctx.emit_config_changed(engine.compat());
            ctx.emit_command_result(request_id, "set_mode", true, None);
            tracing::debug!(target: "solaris_protocol", mode = %mode_str, "SetMode applied");
        }
        ProtocolCommand::SetIntensity { request_id, intensity } => {
            engine.apply_intensity(intensity);
            let effective_effort = ctx.configuration().effective_effort;
            let _ = ctx.writer.emit(&ProtocolEvent::Info {
                msg_id: String::new(),
                message: format!(
                    "intensity updated: {} (effort={})",
                    intensity,
                    effective_effort.as_deref().unwrap_or("provider_default")
                ),
            });
            ctx.emit_config_changed(engine.compat());
            ctx.emit_command_result(request_id, "set_intensity", true, None);
        }
        ProtocolCommand::SetConfig { request_id, update } => {
            if apply_config_command(engine, ctx, ConfigCommand { request_id, update }) {
                ctx.emit_config_changed(engine.compat());
            }
        }
        ProtocolCommand::AddMcpServer { name, .. } => {
            ctx.output.emit_error(&format!(
                "AddMcpServer '{name}': rejected — only allowed before first Message"
            ));
        }
        ProtocolCommand::HostContextReady => {}
        ProtocolCommand::InstallPlugin {
            request_id,
            manifest_path,
        } => plugin::install_plugin(&request_id, &manifest_path, ctx),
        ProtocolCommand::ActivatePlugin { request_id, plugin_id } => {
            plugin::activate_plugin(&request_id, &plugin_id, engine, ctx)
        }
        ProtocolCommand::DeactivatePlugin { request_id, plugin_id } => {
            plugin::deactivate_plugin(&request_id, &plugin_id, engine, ctx)
        }
        ProtocolCommand::GetRuntimeJournal {
            request_id,
            run_id,
            after_sequence,
            limit,
        } => emit_runtime_journal(request_id, run_id, after_sequence, limit, ctx),
        ProtocolCommand::GetRuntimeSnapshot { request_id } => {
            emit_runtime_snapshot(request_id, ctx);
        }
        ProtocolCommand::GetPlanArtifacts { request_id, run_id } => {
            plan::emit_plan_artifacts(request_id, run_id, ctx);
        }
        ProtocolCommand::RunWorkflow {
            request_id,
            workflow,
            parameters,
        } => {
            let Some(definition) = ctx.workflow_controller.definition(&workflow) else {
                emit_run_workflow_failure(ctx, &request_id, &format!("unknown workflow: {workflow}"));
                return DispatchOutcome::Continue;
            };
            let workflow_run_id = stable_workflow_run_id(
                &ctx.run_id,
                &request_id,
                &workflow,
                &definition.version,
                engine.provider_label(),
                engine.model(),
                &parameters,
            );
            match ctx.workflow_controller.start_with_runtime(
                workflow_run_id.clone(),
                &workflow,
                parameters,
                WorkflowRuntimeIdentity::new(engine.provider_label(), engine.model()),
            ) {
                Ok(outcome) => {
                    ctx.emit_runtime_event(
                        if outcome.started {
                            "workflow_started"
                        } else {
                            "workflow_reused"
                        },
                        json!({
                            "request_id": request_id,
                            "workflow_run_id": workflow_run_id,
                            "workflow": workflow,
                            "snapshot": outcome.snapshot,
                        }),
                    );
                    if outcome.snapshot.status != WorkflowRunStatus::Running {
                        ctx.emit_command_result(
                            Some(request_id),
                            "run_workflow",
                            true,
                            Some(format!(
                                "workflow run {workflow_run_id} is already {:?}",
                                outcome.snapshot.status
                            )),
                        );
                        return DispatchOutcome::Continue;
                    }
                    match spawn_workflow_execution(ctx, request_id.clone(), workflow_run_id.clone()) {
                        Ok(started) => ctx.emit_command_result(
                            Some(request_id),
                            "run_workflow",
                            true,
                            Some(if started {
                                workflow_run_id.to_string()
                            } else {
                                format!("workflow run {workflow_run_id} is already active")
                            }),
                        ),
                        Err(error) => {
                            if outcome.started {
                                let _ = ctx.workflow_controller.cancel(&workflow_run_id, &error);
                            }
                            emit_run_workflow_failure(ctx, &request_id, &error);
                        }
                    }
                }
                Err(error) => {
                    emit_run_workflow_failure(ctx, &request_id, &error);
                }
            }
        }
        ProtocolCommand::Ping => {
            let _ = ctx.writer.emit(&ProtocolEvent::Pong);
        }
        ProtocolCommand::AcknowledgeDelivery(acknowledgement) => {
            super::outbox::handle_acknowledgement(ctx.writer.as_ref(), &acknowledgement);
        }
        ProtocolCommand::Message { .. } => {
            // `Message` is routed to `message::handle` by the caller before
            // reaching this dispatcher. Reaching here means the caller's
            // routing changed; log and ignore rather than panic.
            tracing::warn!(
                target: "solaris_protocol",
                "Message reached dispatch::handle; expected routing to message::handle"
            );
        }
    }

    DispatchOutcome::Continue
}

pub(super) fn handle_runtime_query(command: &ProtocolCommand, ctx: &StreamContext) -> bool {
    match command {
        ProtocolCommand::GetRuntimeJournal {
            request_id,
            run_id,
            after_sequence,
            limit,
        } => {
            emit_runtime_journal(request_id.clone(), run_id.clone(), *after_sequence, *limit, ctx);
            true
        }
        ProtocolCommand::GetRuntimeSnapshot { request_id } => {
            emit_runtime_snapshot(request_id.clone(), ctx);
            true
        }
        ProtocolCommand::GetPlanArtifacts { request_id, run_id } => {
            plan::emit_plan_artifacts(request_id.clone(), run_id.clone(), ctx);
            true
        }
        _ => false,
    }
}

fn emit_runtime_journal(
    request_id: String,
    run_id: Option<String>,
    after_sequence: u64,
    limit: Option<usize>,
    ctx: &StreamContext,
) {
    let journal_run_id = match run_id {
        Some(run_id) if run_id == ctx.run_id.as_str() || run_id.starts_with(&format!("{}:", ctx.run_id.as_str())) => {
            RunId::from(run_id)
        }
        Some(_) => {
            let _ = ctx.writer.emit(&ProtocolEvent::Error {
                msg_id: Some(request_id),
                error: solaris_protocol::events::ErrorInfo {
                    code: "runtime_journal_forbidden".into(),
                    message: "requested journal run is outside this Host session".into(),
                    retryable: false,
                },
            });
            return;
        }
        None => ctx.run_id.clone(),
    };
    let ledger = ctx.collaboration_runtime.ledger();
    match build_runtime_journal_event(
        request_id.clone(),
        journal_run_id,
        after_sequence,
        limit,
        ledger.as_ref(),
    ) {
        Ok(event) => {
            let _ = ctx.writer.emit(&event);
        }
        Err(error) => {
            let _ = ctx.writer.emit(&ProtocolEvent::Error {
                msg_id: Some(request_id),
                error: solaris_protocol::events::ErrorInfo {
                    code: "runtime_journal_failed".into(),
                    message: error.to_string(),
                    retryable: true,
                },
            });
        }
    }
}

fn build_runtime_journal_event(
    request_id: String,
    journal_run_id: RunId,
    after_sequence: u64,
    limit: Option<usize>,
    ledger: &dyn RuntimeLedger,
) -> std::io::Result<ProtocolEvent> {
    let page_limit = limit.unwrap_or(1000).clamp(1, 5000);
    let mut records = ledger.records_after_tree(&journal_run_id, after_sequence, page_limit.saturating_add(1))?;
    let truncated = records.len() > page_limit;
    records.truncate(page_limit);

    // The continuation cursor must describe this page, not a separate later
    // view of the ledger. Records appended after this read remain reachable.
    let last_sequence = records.last().map(|record| record.seq).unwrap_or(after_sequence);
    let records = records
        .into_iter()
        .map(|record| RuntimeJournalRecord {
            schema_version: record.schema_version,
            sequence: record.seq,
            run_id: record.run_id.to_string(),
            timestamp_unix_ms: record.timestamp_unix_ms,
            durability: record.durability,
            record_type: record.record_type,
            payload: secret_safe_ledger_payload(record.payload),
        })
        .collect();

    Ok(ProtocolEvent::RuntimeJournal {
        request_id,
        run_id: journal_run_id.to_string(),
        after_sequence,
        last_sequence,
        records,
        truncated,
    })
}

fn emit_runtime_snapshot(request_id: String, ctx: &StreamContext) {
    let captured = ctx
        .collaboration_runtime
        .capture_snapshot_with(&ctx.run_id, || ctx.capture_runtime_extras());
    let (snapshot, extras) = match captured {
        Ok(captured) => captured,
        Err(error) => {
            let _ = ctx.writer.emit(&ProtocolEvent::Error {
                msg_id: Some(request_id),
                error: solaris_protocol::events::ErrorInfo {
                    code: "runtime_snapshot_failed".into(),
                    message: error.to_string(),
                    retryable: true,
                },
            });
            return;
        }
    };
    let _ = ctx.writer.emit(&ProtocolEvent::RuntimeSnapshot {
        request_id,
        schema_version: snapshot.schema_version,
        timestamp_unix_ms: snapshot.timestamp_unix_ms,
        live_sequence: snapshot.live_sequence,
        journal_sequence: snapshot.journal_sequence,
        run_id: ctx.run_id.to_string(),
        snapshot: ctx.runtime_snapshot_from(
            snapshot.projection,
            snapshot.journal_sequence,
            snapshot.live_sequence,
            snapshot.timestamp_unix_ms,
            extras,
        ),
    });
}

pub(super) fn resume_restored_workflows(ctx: &StreamContext, engine: &mut solaris_agent::engine::AgentEngine) {
    let snapshots = ctx.workflow_controller.snapshots();
    for snapshot in snapshots.iter().filter(|snapshot| {
        snapshot.status == WorkflowRunStatus::Completed
            && snapshot.parent_run_id.is_none()
            && required_workflow_host_msg_id(snapshot).is_some()
    }) {
        if let Err(error) = validate_workflow_runtime_identity(snapshot, engine.provider_label(), engine.model()) {
            emit_required_workflow_failure(ctx, snapshot, &error);
            continue;
        }
        match required_workflow_turn_was_emitted(ctx, snapshot) {
            Ok(true) => continue,
            Ok(false) => {}
            Err(error) => {
                emit_required_workflow_failure(ctx, snapshot, &error);
                continue;
            }
        }
        if let Err(error) =
            commit_completed_required_workflow_turn(ctx, engine, &format!("resume:{}", snapshot.run_id), snapshot)
        {
            emit_required_workflow_failure(ctx, snapshot, &error);
        }
    }
    for child in snapshots.iter().filter(|snapshot| {
        snapshot.status == WorkflowRunStatus::Running
            && snapshot.parent_run_id.as_ref().is_some_and(|parent_run_id| {
                !snapshots
                    .iter()
                    .any(|parent| parent.run_id == *parent_run_id && parent.status == WorkflowRunStatus::Running)
            })
    }) {
        let _ = ctx
            .workflow_controller
            .cancel(&child.run_id, "parent workflow was not running during recovery");
    }
    for snapshot in snapshots
        .into_iter()
        .filter(|snapshot| snapshot.status == WorkflowRunStatus::Running && snapshot.parent_run_id.is_none())
    {
        let request_id = format!("resume:{}", snapshot.run_id);
        if let Err(error) = validate_workflow_runtime_identity(&snapshot, engine.provider_label(), engine.model()) {
            let _ = ctx.workflow_controller.cancel(&snapshot.run_id, &error);
            if required_workflow_host_msg_id(&snapshot).is_some() {
                emit_required_workflow_failure(ctx, &snapshot, &error);
            }
            ctx.emit_runtime_event(
                "workflow_resume_failed",
                json!({"workflow_run_id": snapshot.run_id, "error": error}),
            );
            continue;
        }
        ctx.emit_runtime_event(
            "workflow_resumed",
            json!({"request_id": request_id, "workflow_run_id": snapshot.run_id}),
        );
        if let Err(error) = spawn_workflow_execution(ctx, request_id, snapshot.run_id.clone()) {
            let _ = ctx.workflow_controller.cancel(&snapshot.run_id, &error);
            ctx.emit_runtime_event(
                "workflow_resume_failed",
                json!({"workflow_run_id": snapshot.run_id, "error": error}),
            );
        }
    }
}

fn validate_workflow_runtime_identity(
    snapshot: &WorkflowRunSnapshot,
    provider: &str,
    model: &str,
) -> Result<(), String> {
    match snapshot.runtime_identity.as_ref() {
        Some(identity) if identity.provider == provider && identity.model == model => Ok(()),
        Some(_) => Err(format!(
            "workflow run {} cannot resume with a different provider or model",
            snapshot.run_id
        )),
        None => Err(format!(
            "workflow run {} has no durable provider or model identity",
            snapshot.run_id
        )),
    }
}

fn spawn_workflow_execution(ctx: &StreamContext, request_id: String, workflow_run_id: RunId) -> Result<bool, String> {
    let restored_snapshot = ctx
        .workflow_controller
        .snapshots()
        .into_iter()
        .find(|snapshot| snapshot.run_id == workflow_run_id)
        .ok_or_else(|| format!("unknown restored workflow run: {workflow_run_id}"))?;
    let required_msg_id = required_workflow_host_msg_id(&restored_snapshot);
    let registration = match required_msg_id.as_ref() {
        Some(msg_id) => ctx
            .detached_workflows
            .register_required_if_absent(workflow_run_id.clone(), msg_id.clone())?,
        None => ctx.detached_workflows.register_if_absent(workflow_run_id.clone())?,
    };
    let Some(mut cancel_rx) = registration else {
        return Ok(false);
    };
    if let Some(msg_id) = required_msg_id.as_ref() {
        ensure_required_workflow_stream_started(ctx.output.as_ref(), msg_id, false);
        ctx.emit_runtime_event(
            "required_workflow_resumed",
            json!({"msg_id": msg_id, "workflow_run_id": workflow_run_id}),
        );
    }
    let controller = Arc::clone(&ctx.workflow_controller);
    let executor: Arc<dyn WorkflowNodeExecutor> = Arc::new(
        AgentWorkflowExecutor::new(Arc::clone(&ctx.spawner), Arc::clone(&ctx.role_registry)).with_plugin_contributions(
            Arc::new(PluginContributionDispatcher::new(
                Arc::clone(&ctx.plugin_runtime),
                ctx.execution_context.clone(),
                format!("run:{}", ctx.run_id),
            )),
        ),
    );
    let event_ctx = ctx.clone();
    let task_run_id = workflow_run_id.clone();
    let required_snapshot = restored_snapshot;
    let task_required_msg_id = required_msg_id;
    let (start_tx, start_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move {
        if start_rx.await.is_err() {
            event_ctx.detached_workflows.finish(&task_run_id);
            return;
        }
        tokio::select! {
            result = controller.execute_until_settled(&task_run_id, executor) => {
                match result {
                    Ok(snapshot) => {
                        if required_workflow_host_msg_id(&snapshot).is_some() {
                            if let Err(error) = commit_and_emit_required_workflow_turn(
                                &event_ctx,
                                &request_id,
                                &snapshot,
                            )
                            .await
                            {
                                emit_required_workflow_failure(&event_ctx, &snapshot, &error);
                            }
                        } else {
                            event_ctx.emit_runtime_event(
                                "workflow_settled",
                                json!({"request_id": request_id, "workflow_run_id": task_run_id, "snapshot": snapshot}),
                            );
                        }
                    }
                    Err(error) => {
                        if task_required_msg_id.is_some() {
                            emit_required_workflow_failure(&event_ctx, &required_snapshot, &error);
                        } else {
                            event_ctx.emit_runtime_event(
                                "workflow_failed",
                                json!({"request_id": request_id, "workflow_run_id": task_run_id, "error": error}),
                            );
                        }
                    }
                }
            }
            result = cancel_rx.changed() => {
                let reason = if result.is_ok() && *cancel_rx.borrow() { "host_cancel" } else { "host_shutdown" };
                match controller.cancel(&task_run_id, reason) {
                    Ok(snapshot) => {
                        if let Some(msg_id) = task_required_msg_id.as_ref() {
                            event_ctx.emit_runtime_event(
                                "required_workflow_cancelled",
                                json!({
                                    "request_id": request_id,
                                    "msg_id": msg_id,
                                    "workflow_run_id": task_run_id,
                                    "reason": reason,
                                    "snapshot": snapshot,
                                }),
                            );
                            event_ctx.output.emit_stream_end(msg_id, 0, 0, 0, 0, 0);
                        }
                        event_ctx.emit_runtime_event(
                            "workflow_cancelled",
                            json!({
                                "request_id": request_id,
                                "workflow_run_id": task_run_id,
                                "reason": reason,
                                "snapshot": snapshot,
                            }),
                        );
                    }
                    Err(error) => {
                        if task_required_msg_id.is_some() {
                            emit_required_workflow_failure(&event_ctx, &required_snapshot, &error);
                        } else {
                            event_ctx.emit_runtime_event(
                                "workflow_failed",
                                json!({"request_id": request_id, "workflow_run_id": task_run_id, "error": error}),
                            );
                        }
                    }
                }
            }
        }
        event_ctx.detached_workflows.finish(&task_run_id);
    });
    activate_workflow_execution(&ctx.detached_workflows, &workflow_run_id, &handle, start_tx)?;
    Ok(true)
}

fn activate_workflow_execution(
    registry: &DetachedWorkflowRegistry,
    run_id: &RunId,
    handle: &tokio::task::JoinHandle<()>,
    start_tx: tokio::sync::oneshot::Sender<()>,
) -> Result<(), String> {
    if let Err(error) = registry.attach_abort_handle(run_id, handle.abort_handle()) {
        handle.abort();
        registry.finish(run_id);
        return Err(error);
    }
    if start_tx.send(()).is_err() {
        handle.abort();
        registry.finish(run_id);
        return Err(format!("workflow run '{run_id}' ended before execution started"));
    }
    Ok(())
}

fn required_workflow_host_msg_id(snapshot: &WorkflowRunSnapshot) -> Option<String> {
    if snapshot
        .parameters
        .get("required_workflow")
        .and_then(serde_json::Value::as_bool)
        != Some(true)
    {
        return None;
    }
    snapshot
        .parameters
        .get("host_msg_id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
}

fn commit_completed_required_workflow_turn(
    ctx: &StreamContext,
    engine: &mut solaris_agent::engine::AgentEngine,
    request_id: &str,
    snapshot: &WorkflowRunSnapshot,
) -> Result<(), String> {
    let host_msg_id = required_workflow_host_msg_id(snapshot)
        .ok_or_else(|| "restored required workflow has no durable host_msg_id".to_owned())?;
    let user_content = required_workflow_prompt(snapshot)?;
    let output = required_workflow_output(ctx, snapshot);
    let _newly_committed = engine.commit_workflow_turn(
        user_content,
        &snapshot.run_id,
        &snapshot.workflow_id,
        &snapshot.workflow_version,
        &output,
    )?;
    emit_and_record_required_workflow_turn(ctx, snapshot, &host_msg_id, &output, false)?;
    emit_required_workflow_terminal(ctx, request_id, snapshot, &host_msg_id);
    Ok(())
}

async fn commit_and_emit_required_workflow_turn(
    ctx: &StreamContext,
    request_id: &str,
    snapshot: &WorkflowRunSnapshot,
) -> Result<(), String> {
    let host_msg_id = required_workflow_host_msg_id(snapshot)
        .ok_or_else(|| "restored required workflow has no durable host_msg_id".to_owned())?;
    let user_content = required_workflow_prompt(snapshot)?;
    let output = required_workflow_output(ctx, snapshot);
    let (completion, committed) = tokio::sync::oneshot::channel();
    ctx.workflow_turn_commits
        .send(WorkflowTurnCommit {
            host_msg_id: host_msg_id.clone(),
            user_content: user_content.to_owned(),
            workflow_run_id: snapshot.run_id.clone(),
            workflow_id: snapshot.workflow_id.clone(),
            workflow_version: snapshot.workflow_version.clone(),
            output: output.clone(),
            completion,
        })
        .map_err(|_| "required workflow turn commit channel closed before persistence".to_owned())?;
    let applied = committed
        .await
        .map_err(|_| "required workflow turn commit acknowledgement was dropped".to_owned())??;
    let _newly_committed = applied;
    emit_and_record_required_workflow_turn(ctx, snapshot, &host_msg_id, &output, true)?;
    emit_required_workflow_terminal(ctx, request_id, snapshot, &host_msg_id);
    Ok(())
}

fn required_workflow_prompt(snapshot: &WorkflowRunSnapshot) -> Result<&str, String> {
    snapshot
        .parameters
        .get("prompt")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "restored required workflow has no durable prompt".to_owned())
}

fn required_workflow_output(ctx: &StreamContext, snapshot: &WorkflowRunSnapshot) -> serde_json::Value {
    RuntimeTaskRouter::new(
        Arc::clone(&ctx.workflow_controller),
        Arc::clone(&ctx.role_registry),
        Arc::clone(&ctx.spawner),
    )
    .final_output(snapshot)
    .unwrap_or_else(|| {
        json!({
            "workflow_run_id": snapshot.run_id,
            "status": snapshot.status,
        })
    })
}

fn emit_required_workflow_terminal(
    ctx: &StreamContext,
    request_id: &str,
    snapshot: &WorkflowRunSnapshot,
    host_msg_id: &str,
) {
    ctx.emit_runtime_event(
        "required_workflow_settled",
        json!({
            "request_id": request_id,
            "msg_id": host_msg_id,
            "workflow_run_id": snapshot.run_id,
            "snapshot": snapshot,
        }),
    );
    ctx.output.emit_stream_end(host_msg_id, 0, 0, 0, 0, 0);
    ctx.emit_runtime_event(
        "workflow_settled",
        json!({"request_id": request_id, "workflow_run_id": snapshot.run_id, "snapshot": snapshot}),
    );
}

fn ensure_required_workflow_stream_started(output: &dyn OutputSink, host_msg_id: &str, stream_already_started: bool) {
    if !stream_already_started {
        output.emit_stream_start(host_msg_id);
    }
}

fn emit_required_workflow_text(
    output_sink: &dyn OutputSink,
    host_msg_id: &str,
    output: &serde_json::Value,
    stream_already_started: bool,
) {
    ensure_required_workflow_stream_started(output_sink, host_msg_id, stream_already_started);
    let text = output
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| serde_json::to_string_pretty(output).unwrap_or_else(|_| output.to_string()));
    output_sink.emit_text_delta(&text, host_msg_id);
}

pub(super) fn emit_and_record_required_workflow_turn(
    ctx: &StreamContext,
    snapshot: &WorkflowRunSnapshot,
    host_msg_id: &str,
    output: &serde_json::Value,
    stream_already_started: bool,
) -> Result<(), String> {
    let ledger = ctx.collaboration_runtime.ledger();
    emit_and_record_required_workflow_turn_in(
        ctx.output.as_ref(),
        ledger.as_ref(),
        &snapshot.run_id,
        host_msg_id,
        output,
        stream_already_started,
    )
}

fn required_workflow_turn_was_emitted(ctx: &StreamContext, snapshot: &WorkflowRunSnapshot) -> Result<bool, String> {
    let Some(host_msg_id) = required_workflow_host_msg_id(snapshot) else {
        return Ok(false);
    };
    let ledger = ctx.collaboration_runtime.ledger();
    required_workflow_turn_was_emitted_in(ledger.as_ref(), &snapshot.run_id, &host_msg_id)
}

fn record_required_workflow_turn_emitted_in(
    ledger: &dyn RuntimeLedger,
    run_id: &RunId,
    host_msg_id: &str,
) -> Result<(), String> {
    if required_workflow_turn_was_emitted_in(ledger, run_id, host_msg_id)? {
        return Ok(());
    }
    ledger
        .append(
            run_id,
            DurabilityClass::SyncCritical,
            "required_workflow_turn_emitted",
            json!({"msg_id": host_msg_id, "workflow_run_id": run_id}),
        )
        .map(|_| ())
        .map_err(|error| format!("failed to persist required workflow turn delivery marker: {error}"))
}

fn emit_and_record_required_workflow_turn_in(
    output_sink: &dyn OutputSink,
    ledger: &dyn RuntimeLedger,
    run_id: &RunId,
    host_msg_id: &str,
    output: &serde_json::Value,
    stream_already_started: bool,
) -> Result<(), String> {
    emit_required_workflow_text(output_sink, host_msg_id, output, stream_already_started);
    record_required_workflow_turn_emitted_in(ledger, run_id, host_msg_id)
}

fn required_workflow_turn_was_emitted_in(
    ledger: &dyn RuntimeLedger,
    run_id: &RunId,
    host_msg_id: &str,
) -> Result<bool, String> {
    ledger
        .records_for_run(run_id)
        .map(|records| {
            records.into_iter().any(|record| {
                record.record_type == "required_workflow_turn_emitted"
                    && record.payload.get("msg_id").and_then(serde_json::Value::as_str) == Some(host_msg_id)
            })
        })
        .map_err(|error| format!("failed to inspect required workflow delivery marker: {error}"))
}

fn emit_required_workflow_failure(ctx: &StreamContext, snapshot: &WorkflowRunSnapshot, error: &str) {
    let msg_id = required_workflow_host_msg_id(snapshot);
    let _ = ctx.writer.emit(&ProtocolEvent::Error {
        msg_id: msg_id.clone(),
        error: solaris_protocol::events::ErrorInfo {
            code: "required_workflow_commit_failed".to_owned(),
            message: error.to_owned(),
            retryable: true,
        },
    });
    ctx.emit_runtime_event(
        "required_workflow_failed",
        json!({"msg_id": msg_id, "workflow_run_id": snapshot.run_id, "error": error}),
    );
    if let Some(msg_id) = msg_id {
        ctx.output.emit_stream_end(&msg_id, 0, 0, 0, 0, 0);
    }
}

#[cfg(test)]
#[path = "dispatch_test.rs"]
mod dispatch_test;
