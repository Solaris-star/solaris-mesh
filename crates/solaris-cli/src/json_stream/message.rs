//! Handling of one Message command in the Solaris Mesh host protocol.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use solaris_agent::engine::{AgentEngine, AgentResult, RuntimeConfigurationView};
use solaris_agent::plugin_tool::PluginContributionDispatcher;
use solaris_agent::run_preset::{
    RuntimeTaskRouter, required_workflow_for_task, required_workflow_parameters, resolve_run_preset,
    stable_workflow_run_id,
};
use solaris_agent::workflow_controller::{
    WorkflowController, WorkflowRunSnapshot, WorkflowRunStatus, WorkflowRuntimeIdentity,
};
use solaris_protocol::ToolApprovalResult;
use solaris_protocol::commands::ProtocolCommand;
use solaris_protocol::events::ProtocolEvent;
use solaris_protocol::reader::ProtocolInput;
use solaris_types::config::{ConfigField, ConfigFieldResult, ConfigFieldStatus, ConfigUpdateOutcome};
use solaris_types::identity::RunId;
use solaris_types::message::TokenUsage;
use solaris_types::permission::PermissionMode;
use solaris_types::run_preset::{Intensity, RunPreset};
use solaris_types::spawner::AgentOutcomeStatus;
use tokio::sync::mpsc::UnboundedReceiver;

use super::context::StreamContext;
use super::dispatch::{ConfigCommand, apply_config_command};

#[derive(Default)]
struct PendingControls {
    configurations: VecDeque<PendingConfiguration>,
}

enum PendingConfiguration {
    Config(ConfigCommand),
    Mode {
        request_id: Option<String>,
        mode: PermissionMode,
    },
    Intensity {
        request_id: Option<String>,
        intensity: Intensity,
    },
}

struct IntensityApplication {
    request_id: Option<String>,
    applied: bool,
    message: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OperationTerminal {
    Succeeded,
    Cancelled,
    Stopped,
    Failed,
}

const ENGINE_FAILURE_CODE: &str = "engine_error";
const ENGINE_FAILURE_MESSAGE: &str = "agent turn failed";
const REQUIRED_WORKFLOW_FAILURE_CODE: &str = "required_workflow_failed";
const REQUIRED_WORKFLOW_FAILURE_MESSAGE: &str = "required workflow failed";
const REQUIRED_WORKFLOW_CANCEL_FAILURE_CODE: &str = "required_workflow_cancel_failed";
const REQUIRED_WORKFLOW_CANCEL_FAILURE_MESSAGE: &str = "required workflow cancellation could not be saved";
const REQUIRED_WORKFLOW_ALREADY_COMPLETED_MESSAGE: &str = "required workflow completed before cancellation was applied";
const REQUIRED_WORKFLOW_ALREADY_FAILED_MESSAGE: &str = "required workflow failed before cancellation was applied";

fn emit_turn_failure(
    output: &dyn solaris_agent::output::OutputSink,
    msg_id: &str,
    code: &str,
    message: &str,
    retryable: bool,
) {
    output.emit_protocol_error(msg_id, code, message, retryable);
}

fn cancel_required_workflow(
    controller: &WorkflowController,
    workflow_run_id: &RunId,
    reason: &str,
) -> Result<WorkflowRunSnapshot, String> {
    controller.cancel(workflow_run_id, reason)
}

enum WorkflowWait<T> {
    Settled(T),
    Control(Box<ProtocolInput>),
}

async fn wait_for_workflow_or_control<F>(
    workflow_fut: Pin<&mut F>,
    cmd_rx: &mut UnboundedReceiver<ProtocolInput>,
) -> WorkflowWait<F::Output>
where
    F: Future,
{
    tokio::select! {
        biased;
        result = workflow_fut => WorkflowWait::Settled(result),
        input = cmd_rx.recv() => WorkflowWait::Control(Box::new(
            input.unwrap_or_else(|| ProtocolInput::Command(Box::new(ProtocolCommand::Stop))),
        )),
    }
}

async fn wait_for_engine_or_control<F>(
    engine_fut: Pin<&mut F>,
    cmd_rx: &mut UnboundedReceiver<ProtocolInput>,
) -> WorkflowWait<F::Output>
where
    F: Future,
{
    wait_for_workflow_or_control(engine_fut, cmd_rx).await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequiredCancellationDisposition {
    Cancelled,
    Completed,
    Failed,
    PersistenceFailed,
}

fn classify_required_cancellation(result: Result<WorkflowRunStatus, String>) -> RequiredCancellationDisposition {
    match result {
        Ok(WorkflowRunStatus::Cancelled) => RequiredCancellationDisposition::Cancelled,
        Ok(WorkflowRunStatus::Completed) => RequiredCancellationDisposition::Completed,
        Ok(WorkflowRunStatus::Failed) => RequiredCancellationDisposition::Failed,
        Ok(WorkflowRunStatus::Running) | Err(_) => RequiredCancellationDisposition::PersistenceFailed,
    }
}

fn operation_terminal_for_agent_result(result: &AgentResult) -> OperationTerminal {
    if result.status == AgentOutcomeStatus::Completed {
        OperationTerminal::Succeeded
    } else {
        OperationTerminal::Failed
    }
}

fn apply_queued_intensity(
    engine: &mut AgentEngine,
    request_id: Option<String>,
    intensity: Intensity,
    rejection_message: Option<&str>,
) -> IntensityApplication {
    if let Some(message) = rejection_message {
        return IntensityApplication {
            request_id,
            applied: false,
            message: Some(message.to_owned()),
        };
    }
    engine.apply_intensity(intensity);
    IntensityApplication {
        request_id,
        applied: true,
        message: None,
    }
}

fn required_workflow_for_message<'a>(
    engine: &AgentEngine,
    configuration: &RuntimeConfigurationView,
    content: &str,
    preset: &'a RunPreset,
) -> Option<(PermissionMode, &'a str)> {
    if engine.recognizes_slash_command(content) {
        return None;
    }
    let permission_mode = configuration.snapshot().permission;
    required_workflow_for_task(permission_mode, content, preset).map(|workflow_id| (permission_mode, workflow_id))
}

pub(super) async fn handle(
    msg_id: &str,
    content: &str,
    engine: &mut AgentEngine,
    cmd_rx: &mut UnboundedReceiver<ProtocolInput>,
    ctx: &StreamContext,
) -> bool {
    let preset = resolve_run_preset(ctx.intensity(), engine.compat().effort_levels());
    if let Some((permission_mode, _)) =
        required_workflow_for_message(engine, &ctx.runtime_configuration, content, &preset)
    {
        handle_required_workflow(msg_id, content, engine, cmd_rx, ctx, preset, permission_mode).await
    } else {
        handle_engine_message(msg_id, content, engine, cmd_rx, ctx).await
    }
}

fn emit_stream_end_with_usage(ctx: &StreamContext, msg_id: &str, turns: usize, usage: &TokenUsage) {
    let signals = ctx.spawner.resource_manager().provider_signals();
    let categorized = signals.categorize_usage(usage);
    ctx.output.emit_stream_end_with_accounting(
        msg_id,
        turns,
        usage.input_tokens,
        usage.output_tokens,
        usage.cache_creation_tokens,
        usage.cache_read_tokens,
        categorized.uncached_input_tokens,
        signals.usage_cost(usage),
    );
}

async fn handle_engine_message(
    msg_id: &str,
    content: &str,
    engine: &mut AgentEngine,
    cmd_rx: &mut UnboundedReceiver<ProtocolInput>,
    ctx: &StreamContext,
) -> bool {
    let configuration_before = ctx.configuration();
    let mut terminal = OperationTerminal::Failed;
    let mut cancel_request_id = None;
    let mut pending = PendingControls::default();

    {
        let engine_fut = engine.run(content, msg_id);
        tokio::pin!(engine_fut);
        loop {
            match wait_for_engine_or_control(engine_fut.as_mut(), cmd_rx).await {
                WorkflowWait::Settled(result) => {
                    match result {
                        Ok(result) => {
                            terminal = operation_terminal_for_agent_result(&result);
                            emit_stream_end_with_usage(ctx, msg_id, result.turns, &result.usage);
                        }
                        Err(_) => {
                            emit_turn_failure(
                                ctx.output.as_ref(),
                                msg_id,
                                ENGINE_FAILURE_CODE,
                                ENGINE_FAILURE_MESSAGE,
                                false,
                            );
                            ctx.output.emit_stream_end(msg_id, 0, 0, 0, 0, 0);
                        }
                    }
                    break;
                }
                WorkflowWait::Control(input) => {
                    let command = match *input {
                        ProtocolInput::Command(command) => *command,
                        ProtocolInput::Invalid(error) => {
                            let _ = ctx.writer.emit(&error.into_event());
                            continue;
                        }
                    };
                    match handle_control(command, msg_id, ctx, &mut pending) {
                        ActiveControl::Continue => {}
                        ActiveControl::Cancel { request_id } => {
                            terminal = OperationTerminal::Cancelled;
                            cancel_request_id = request_id;
                            break;
                        }
                        ActiveControl::Stop => {
                            terminal = OperationTerminal::Stopped;
                            break;
                        }
                    }
                }
            }
        }
    }

    if matches!(terminal, OperationTerminal::Cancelled | OperationTerminal::Stopped) {
        let stopped = terminal == OperationTerminal::Stopped;
        engine.abort_current_turn(if stopped { "host_stop" } else { "host_cancel" });
        if terminal == OperationTerminal::Cancelled {
            ctx.emit_command_result(cancel_request_id, "cancel", true, None);
        }
    }
    let rejection = pending_control_rejection(terminal);
    let configuration_event_emitted = apply_pending_controls(engine, ctx, pending, rejection);
    if !configuration_event_emitted && configuration_before != ctx.configuration() {
        ctx.emit_config_changed(engine.compat());
    }
    if matches!(terminal, OperationTerminal::Cancelled | OperationTerminal::Stopped) {
        ctx.output.emit_stream_end(msg_id, 0, 0, 0, 0, 0);
    }
    terminal == OperationTerminal::Stopped
}

async fn handle_required_workflow(
    msg_id: &str,
    content: &str,
    engine: &mut AgentEngine,
    cmd_rx: &mut UnboundedReceiver<ProtocolInput>,
    ctx: &StreamContext,
    preset: RunPreset,
    permission_mode: PermissionMode,
) -> bool {
    let configuration_before = ctx.configuration();
    let mut terminal = OperationTerminal::Failed;
    let mut stop_requested = false;
    let mut pending = PendingControls::default();
    let Some(workflow_id) = required_workflow_for_task(permission_mode, content, &preset) else {
        ctx.output.emit_stream_start(msg_id);
        emit_turn_failure(
            ctx.output.as_ref(),
            msg_id,
            REQUIRED_WORKFLOW_FAILURE_CODE,
            REQUIRED_WORKFLOW_FAILURE_MESSAGE,
            false,
        );
        ctx.output.emit_stream_end(msg_id, 0, 0, 0, 0, 0);
        return false;
    };
    let Some(definition) = ctx.workflow_controller.definition(workflow_id) else {
        ctx.output.emit_stream_start(msg_id);
        emit_turn_failure(
            ctx.output.as_ref(),
            msg_id,
            REQUIRED_WORKFLOW_FAILURE_CODE,
            REQUIRED_WORKFLOW_FAILURE_MESSAGE,
            false,
        );
        ctx.output.emit_stream_end(msg_id, 0, 0, 0, 0, 0);
        return false;
    };
    let provider = engine.provider_label().to_owned();
    let model = engine.model().to_owned();
    let runtime_identity = WorkflowRuntimeIdentity::new(&provider, &model);
    let parameters = required_workflow_parameters(msg_id, content, &provider, &model, permission_mode, &preset);
    let workflow_run_id = stable_workflow_run_id(
        &ctx.run_id,
        msg_id,
        workflow_id,
        &definition.version,
        &provider,
        &model,
        &parameters,
    );
    let router = RuntimeTaskRouter::new(
        Arc::clone(&ctx.workflow_controller),
        Arc::clone(&ctx.role_registry),
        Arc::clone(&ctx.spawner),
    )
    .with_plugin_contributions(Arc::new(PluginContributionDispatcher::new(
        Arc::clone(&ctx.plugin_runtime),
        ctx.execution_context.clone(),
        format!("run:{}", ctx.run_id),
    )));

    let start_outcome = match ctx.workflow_controller.start_with_runtime(
        workflow_run_id.clone(),
        workflow_id,
        parameters,
        runtime_identity.clone(),
    ) {
        Ok(outcome) => outcome,
        Err(_) => {
            ctx.output.emit_stream_start(msg_id);
            emit_turn_failure(
                ctx.output.as_ref(),
                msg_id,
                REQUIRED_WORKFLOW_FAILURE_CODE,
                REQUIRED_WORKFLOW_FAILURE_MESSAGE,
                false,
            );
            ctx.output.emit_stream_end(msg_id, 0, 0, 0, 0, 0);
            return false;
        }
    };

    ctx.output.emit_stream_start(msg_id);
    ctx.emit_runtime_event(
        if start_outcome.started {
            "required_workflow_started"
        } else {
            "required_workflow_reused"
        },
        serde_json::json!({
            "msg_id": msg_id,
            "workflow_run_id": workflow_run_id,
            "intensity": preset.intensity,
        }),
    );

    {
        let workflow_fut = router.execute_required_workflow(
            &ctx.run_id,
            msg_id,
            content,
            &runtime_identity,
            permission_mode,
            &preset,
        );
        tokio::pin!(workflow_fut);
        loop {
            match wait_for_workflow_or_control(workflow_fut.as_mut(), cmd_rx).await {
                WorkflowWait::Settled(result) => {
                    match result {
                        Ok(Some(workflow)) => {
                            let committed_output = workflow.final_output.clone().unwrap_or_else(|| {
                                serde_json::json!({
                                    "workflow_run_id": workflow.workflow_run_id,
                                    "status": workflow.snapshot.status,
                                })
                            });
                            if engine
                                .commit_workflow_turn(
                                    content,
                                    &workflow.workflow_run_id,
                                    &workflow.snapshot.workflow_id,
                                    &workflow.snapshot.workflow_version,
                                    &committed_output,
                                )
                                .is_err()
                            {
                                emit_turn_failure(
                                    ctx.output.as_ref(),
                                    msg_id,
                                    REQUIRED_WORKFLOW_FAILURE_CODE,
                                    REQUIRED_WORKFLOW_FAILURE_MESSAGE,
                                    false,
                                );
                                ctx.emit_runtime_event(
                                    "required_workflow_failed",
                                    serde_json::json!({
                                        "msg_id": msg_id,
                                        "workflow_run_id": workflow.workflow_run_id,
                                        "error": REQUIRED_WORKFLOW_FAILURE_MESSAGE,
                                    }),
                                );
                                emit_stream_end_with_usage(ctx, msg_id, workflow.turns, &workflow.usage);
                                break;
                            }
                            if super::dispatch::emit_and_record_required_workflow_turn(
                                ctx,
                                &workflow.snapshot,
                                msg_id,
                                &committed_output,
                                true,
                            )
                            .is_err()
                            {
                                emit_turn_failure(
                                    ctx.output.as_ref(),
                                    msg_id,
                                    REQUIRED_WORKFLOW_FAILURE_CODE,
                                    REQUIRED_WORKFLOW_FAILURE_MESSAGE,
                                    false,
                                );
                                ctx.emit_runtime_event(
                                    "required_workflow_failed",
                                    serde_json::json!({
                                        "msg_id": msg_id,
                                        "workflow_run_id": &workflow.workflow_run_id,
                                        "error": REQUIRED_WORKFLOW_FAILURE_MESSAGE,
                                    }),
                                );
                                emit_stream_end_with_usage(ctx, msg_id, workflow.turns, &workflow.usage);
                                break;
                            }
                            ctx.emit_runtime_event(
                                "required_workflow_settled",
                                serde_json::json!({
                                    "msg_id": msg_id,
                                    "workflow_run_id": &workflow.workflow_run_id,
                                    "snapshot": &workflow.snapshot,
                                }),
                            );
                            emit_stream_end_with_usage(ctx, msg_id, workflow.turns, &workflow.usage);
                            terminal = OperationTerminal::Succeeded;
                        }
                        Ok(None) => {
                            emit_turn_failure(
                                ctx.output.as_ref(),
                                msg_id,
                                REQUIRED_WORKFLOW_FAILURE_CODE,
                                REQUIRED_WORKFLOW_FAILURE_MESSAGE,
                                false,
                            );
                            ctx.output.emit_stream_end(msg_id, 0, 0, 0, 0, 0);
                        }
                        Err(error) => {
                            emit_turn_failure(
                                ctx.output.as_ref(),
                                msg_id,
                                REQUIRED_WORKFLOW_FAILURE_CODE,
                                REQUIRED_WORKFLOW_FAILURE_MESSAGE,
                                false,
                            );
                            ctx.emit_runtime_event(
                                "required_workflow_failed",
                                serde_json::json!({
                                    "msg_id": msg_id,
                                    "workflow_run_id": workflow_run_id,
                                    "error": REQUIRED_WORKFLOW_FAILURE_MESSAGE,
                                }),
                            );
                            emit_stream_end_with_usage(ctx, msg_id, error.turns, &error.usage);
                        }
                    }
                    break;
                }
                WorkflowWait::Control(input) => {
                    let command = match *input {
                        ProtocolInput::Command(command) => *command,
                        ProtocolInput::Invalid(error) => {
                            let _ = ctx.writer.emit(&error.into_event());
                            continue;
                        }
                    };
                    match handle_control(command, msg_id, ctx, &mut pending) {
                        ActiveControl::Continue => {}
                        ActiveControl::Cancel { request_id } => {
                            let disposition = classify_required_cancellation(
                                cancel_required_workflow(&ctx.workflow_controller, &workflow_run_id, "host_cancel")
                                    .map(|snapshot| snapshot.status),
                            );
                            match disposition {
                                RequiredCancellationDisposition::Cancelled => {
                                    ctx.emit_command_result(request_id, "cancel", true, None);
                                    ctx.emit_runtime_event(
                                        "required_workflow_cancelled",
                                        serde_json::json!({
                                            "msg_id": msg_id,
                                            "workflow_run_id": workflow_run_id,
                                        }),
                                    );
                                    ctx.output.emit_stream_end(msg_id, 0, 0, 0, 0, 0);
                                    terminal = OperationTerminal::Cancelled;
                                    break;
                                }
                                RequiredCancellationDisposition::Completed => {
                                    ctx.emit_command_result(
                                        request_id,
                                        "cancel",
                                        false,
                                        Some(REQUIRED_WORKFLOW_ALREADY_COMPLETED_MESSAGE.to_owned()),
                                    );
                                }
                                RequiredCancellationDisposition::Failed => {
                                    ctx.emit_command_result(
                                        request_id,
                                        "cancel",
                                        false,
                                        Some(REQUIRED_WORKFLOW_ALREADY_FAILED_MESSAGE.to_owned()),
                                    );
                                    emit_turn_failure(
                                        ctx.output.as_ref(),
                                        msg_id,
                                        REQUIRED_WORKFLOW_FAILURE_CODE,
                                        REQUIRED_WORKFLOW_FAILURE_MESSAGE,
                                        false,
                                    );
                                    ctx.emit_runtime_event(
                                        "required_workflow_failed",
                                        serde_json::json!({
                                            "msg_id": msg_id,
                                            "workflow_run_id": workflow_run_id,
                                            "error": REQUIRED_WORKFLOW_FAILURE_MESSAGE,
                                        }),
                                    );
                                    ctx.output.emit_stream_end(msg_id, 0, 0, 0, 0, 0);
                                    terminal = OperationTerminal::Failed;
                                    break;
                                }
                                RequiredCancellationDisposition::PersistenceFailed => {
                                    ctx.emit_command_result(
                                        request_id,
                                        "cancel",
                                        false,
                                        Some(REQUIRED_WORKFLOW_CANCEL_FAILURE_MESSAGE.to_owned()),
                                    );
                                    emit_turn_failure(
                                        ctx.output.as_ref(),
                                        msg_id,
                                        REQUIRED_WORKFLOW_CANCEL_FAILURE_CODE,
                                        REQUIRED_WORKFLOW_CANCEL_FAILURE_MESSAGE,
                                        true,
                                    );
                                    ctx.output.emit_stream_end(msg_id, 0, 0, 0, 0, 0);
                                    terminal = OperationTerminal::Failed;
                                    break;
                                }
                            }
                        }
                        ActiveControl::Stop => {
                            stop_requested = true;
                            let disposition = classify_required_cancellation(
                                cancel_required_workflow(&ctx.workflow_controller, &workflow_run_id, "host_stop")
                                    .map(|snapshot| snapshot.status),
                            );
                            match disposition {
                                RequiredCancellationDisposition::Cancelled => {
                                    ctx.emit_runtime_event(
                                        "required_workflow_cancelled",
                                        serde_json::json!({
                                            "msg_id": msg_id,
                                            "workflow_run_id": workflow_run_id,
                                        }),
                                    );
                                    ctx.output.emit_stream_end(msg_id, 0, 0, 0, 0, 0);
                                    terminal = OperationTerminal::Stopped;
                                    break;
                                }
                                RequiredCancellationDisposition::Completed => {}
                                RequiredCancellationDisposition::Failed => {
                                    emit_turn_failure(
                                        ctx.output.as_ref(),
                                        msg_id,
                                        REQUIRED_WORKFLOW_FAILURE_CODE,
                                        REQUIRED_WORKFLOW_FAILURE_MESSAGE,
                                        false,
                                    );
                                    ctx.emit_runtime_event(
                                        "required_workflow_failed",
                                        serde_json::json!({
                                            "msg_id": msg_id,
                                            "workflow_run_id": workflow_run_id,
                                            "error": REQUIRED_WORKFLOW_FAILURE_MESSAGE,
                                        }),
                                    );
                                    ctx.output.emit_stream_end(msg_id, 0, 0, 0, 0, 0);
                                    terminal = OperationTerminal::Failed;
                                    break;
                                }
                                RequiredCancellationDisposition::PersistenceFailed => {
                                    emit_turn_failure(
                                        ctx.output.as_ref(),
                                        msg_id,
                                        REQUIRED_WORKFLOW_CANCEL_FAILURE_CODE,
                                        REQUIRED_WORKFLOW_CANCEL_FAILURE_MESSAGE,
                                        true,
                                    );
                                    ctx.output.emit_stream_end(msg_id, 0, 0, 0, 0, 0);
                                    terminal = OperationTerminal::Failed;
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let control_terminal = if stop_requested {
        OperationTerminal::Stopped
    } else {
        terminal
    };
    let rejection = pending_control_rejection(control_terminal);
    let configuration_event_emitted = apply_pending_controls(engine, ctx, pending, rejection);
    if !configuration_event_emitted && configuration_before != ctx.configuration() {
        ctx.emit_config_changed(engine.compat());
    }
    stop_requested
}

enum ActiveControl {
    Continue,
    Cancel { request_id: Option<String> },
    Stop,
}

fn handle_control(
    command: ProtocolCommand,
    active_msg_id: &str,
    ctx: &StreamContext,
    pending: &mut PendingControls,
) -> ActiveControl {
    if super::dispatch::handle_runtime_query(&command, ctx) {
        return ActiveControl::Continue;
    }
    match command {
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
        ProtocolCommand::Cancel { request_id, msg_id } if msg_id == active_msg_id => {
            return ActiveControl::Cancel { request_id };
        }
        ProtocolCommand::Cancel { request_id, msg_id } => {
            ctx.emit_command_result(
                request_id,
                "cancel",
                false,
                Some("target turn is not active".to_owned()),
            );
            let _ = ctx.writer.emit(&ProtocolEvent::Info {
                msg_id,
                message: "cancel ignored because the target turn is not active".to_owned(),
            });
        }
        ProtocolCommand::CancelWorkflow { request_id, run_id } => {
            let run_id = RunId::new(run_id);
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
                    format!("workflow cancellation requested: {run_id}")
                } else {
                    format!("workflow cancellation ignored because the target is not active: {run_id}")
                },
            });
        }
        ProtocolCommand::Stop => return ActiveControl::Stop,
        ProtocolCommand::SetConfig { request_id, update } => {
            pending
                .configurations
                .push_back(PendingConfiguration::Config(ConfigCommand { request_id, update }));
            let _ = ctx.writer.emit(&ProtocolEvent::Info {
                msg_id: String::new(),
                message: "set_config: queued, will apply after current operation".to_owned(),
            });
        }
        ProtocolCommand::SetMode { request_id, mode } => {
            pending
                .configurations
                .push_back(PendingConfiguration::Mode { request_id, mode });
            let _ = ctx.writer.emit(&ProtocolEvent::Info {
                msg_id: String::new(),
                message: format!("mode queued for next operation: {mode:?}").to_lowercase(),
            });
        }
        ProtocolCommand::SetIntensity { request_id, intensity } => {
            pending
                .configurations
                .push_back(PendingConfiguration::Intensity { request_id, intensity });
            let _ = ctx.writer.emit(&ProtocolEvent::Info {
                msg_id: String::new(),
                message: format!("intensity queued for next operation: {intensity}"),
            });
        }
        ProtocolCommand::Ping => {
            let _ = ctx.writer.emit(&ProtocolEvent::Pong);
        }
        ProtocolCommand::AcknowledgeDelivery(acknowledgement) => {
            super::outbox::handle_acknowledgement(ctx.writer.as_ref(), &acknowledgement);
        }
        ProtocolCommand::InstallPlugin { request_id, .. } => {
            ctx.emit_command_result(
                Some(request_id),
                "install_plugin",
                false,
                Some("command is unavailable while a turn is active".to_owned()),
            );
        }
        ProtocolCommand::ActivatePlugin { request_id, .. } => {
            ctx.emit_command_result(
                Some(request_id),
                "activate_plugin",
                false,
                Some("command is unavailable while a turn is active".to_owned()),
            );
        }
        ProtocolCommand::DeactivatePlugin { request_id, .. } => {
            ctx.emit_command_result(
                Some(request_id),
                "deactivate_plugin",
                false,
                Some("command is unavailable while a turn is active".to_owned()),
            );
        }
        ProtocolCommand::RunWorkflow { request_id, .. } => {
            ctx.emit_command_result(
                Some(request_id),
                "run_workflow",
                false,
                Some("command is unavailable while a turn is active".to_owned()),
            );
        }
        ProtocolCommand::Message { msg_id, .. } => {
            let _ = ctx.writer.emit(&ProtocolEvent::Error {
                msg_id: Some(msg_id),
                error: solaris_protocol::events::ErrorInfo {
                    code: "turn_busy".to_owned(),
                    message: "another turn is already active".to_owned(),
                    retryable: true,
                },
            });
        }
        _ => {
            tracing::debug!(target: "solaris_protocol", "ignoring command during active operation");
        }
    }
    ActiveControl::Continue
}

fn apply_pending_controls(
    engine: &mut AgentEngine,
    ctx: &StreamContext,
    mut pending: PendingControls,
    rejection: Option<&str>,
) -> bool {
    let mut configuration_event_emitted = false;
    while let Some(command) = pending.configurations.pop_front() {
        match command {
            PendingConfiguration::Config(command) => {
                if let Some(rejection) = rejection {
                    let outcome = rejected_config_outcome(&command, rejection);
                    ctx.emit_config_command_result(command.request_id, outcome);
                } else if apply_config_command(engine, ctx, command) {
                    ctx.emit_config_changed(engine.compat());
                    configuration_event_emitted = true;
                }
            }
            PendingConfiguration::Mode { request_id, mode } => {
                if let Some(rejection) = rejection {
                    ctx.emit_command_result(request_id, "set_mode", false, Some(rejection.to_owned()));
                } else {
                    ctx.approval_manager.set_mode(mode);
                    engine.set_permission_mode(mode);
                    ctx.emit_config_changed(engine.compat());
                    ctx.emit_command_result(request_id, "set_mode", true, None);
                    configuration_event_emitted = true;
                }
            }
            PendingConfiguration::Intensity { request_id, intensity } => {
                let application = apply_queued_intensity(engine, request_id, intensity, rejection);
                if application.applied {
                    ctx.emit_config_changed(engine.compat());
                    configuration_event_emitted = true;
                }
                ctx.emit_command_result(
                    application.request_id,
                    "set_intensity",
                    application.applied,
                    application.message,
                );
            }
        }
    }
    configuration_event_emitted
}

fn rejected_config_outcome(command: &ConfigCommand, message: &str) -> ConfigUpdateOutcome {
    let mut results = Vec::new();
    let mut push = |field, supplied| {
        if supplied {
            results.push(ConfigFieldResult::new(field, ConfigFieldStatus::Rejected, message));
        }
    };
    push(ConfigField::Model, command.update.model.is_some());
    push(ConfigField::Thinking, command.update.thinking.is_some());
    push(ConfigField::ThinkingBudget, command.update.thinking_budget.is_some());
    push(ConfigField::Effort, command.update.effort.is_some());
    push(ConfigField::Compaction, command.update.compaction.is_some());
    push(
        ConfigField::MultiAgentPolicy,
        command.update.multi_agent_policy.is_some(),
    );
    push(ConfigField::MaxActiveAgents, command.update.max_active_agents.is_some());
    ConfigUpdateOutcome {
        applied: false,
        changed: false,
        results,
        message: message.to_owned(),
    }
}

fn pending_control_rejection(terminal: OperationTerminal) -> Option<&'static str> {
    match terminal {
        OperationTerminal::Succeeded => None,
        OperationTerminal::Cancelled => Some("turn was cancelled before the queued command was applied"),
        OperationTerminal::Stopped => Some("session stopped before the queued command was applied"),
        OperationTerminal::Failed => Some("turn failed before the queued command was applied"),
    }
}

#[cfg(test)]
#[path = "message_test.rs"]
mod message_test;
