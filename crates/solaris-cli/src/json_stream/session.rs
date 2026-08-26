//! Setup and top-level orchestration for the JSON stream protocol.
//!
//! Flow: build protocol plumbing (writer/sink/approval manager) → bootstrap
//! the engine (silently resuming a session if requested — unlike the
//! terminal REPL, JSON stream mode never prints a resume banner) → atomically
//! activate the outbox, emit the transport-only `Ready` handshake, replay
//! pending deliveries, and emit the durable, visible `Ready` → drain the
//! `AddMcpServer` pre-message phase → run the main
//! command loop, dispatching `Message` to `message::handle` and everything
//! else to `dispatch::handle` → shut down all MCP managers on exit.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use solaris_agent::output::OutputSink;
use solaris_agent::output::protocol_sink::ProtocolSink;
use solaris_config::config::Config;
use solaris_mcp::manager::McpManager;
use solaris_protocol::ToolApprovalManager;
use solaris_protocol::commands::ProtocolCommand;
use solaris_protocol::events::ProtocolEvent;
use solaris_protocol::reader::{ProtocolInput, spawn_stdin_reader};
use solaris_protocol::writer::{ProtocolEmitter, ProtocolWriter};
use solaris_types::config::RuntimeConfigUpdate;
use solaris_types::effect::DurabilityClass;
use solaris_types::permission::PermissionMode;
use solaris_types::run_preset::Intensity;

use super::context::{DetachedWorkflowRegistry, StreamContext, WorkflowTurnCommit};
use super::dispatch::DispatchOutcome;
use super::outbox::DurableHostEmitter;
use super::pre_message::PreMessageOutcome;
use super::{dispatch, message, pre_message};
use crate::bootstrap::build_engine;
use solaris_agent::session::SessionManager;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::Instant;

const MCP_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(5_000);
const OUTPUT_HEALTH_INTERVAL: Duration = Duration::from_millis(100);

fn check_protocol_output_health(protocol_sink: &ProtocolSink, durable_writer: &DurableHostEmitter) -> io::Result<()> {
    protocol_sink.check_fatal_error()?;
    durable_writer.check_fatal_error()
}

async fn shutdown_mcp_managers_with_timeout(managers: &[Arc<McpManager>], timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    let mut unique = Vec::new();
    for manager in managers {
        if !unique.iter().any(|existing| Arc::ptr_eq(existing, manager)) {
            manager.begin_disable();
            unique.push(Arc::clone(manager));
        }
    }

    let mut shutdowns = JoinSet::new();
    for manager in unique {
        shutdowns.spawn(async move { manager.shutdown().await });
    }
    let joined = async {
        let mut completed = true;
        while let Some(result) = shutdowns.join_next().await {
            if let Err(error) = result {
                completed = false;
                tracing::warn!(target: "solaris_mcp", %error, "mcp manager shutdown task failed");
            }
        }
        completed
    };

    match tokio::time::timeout_at(deadline, joined).await {
        Ok(completed) => completed,
        Err(_) => {
            shutdowns.abort_all();
            tracing::warn!(target: "solaris_mcp", "timed out shutting down mcp managers before host deadline");
            false
        }
    }
}

pub(crate) async fn run(
    config: Config,
    cwd: &str,
    resume: Option<String>,
    session_id: Option<String>,
    permission_mode: PermissionMode,
    intensity: Intensity,
) -> anyhow::Result<()> {
    let stdout_writer = Arc::new(ProtocolWriter::new());
    let durable_writer = Arc::new(DurableHostEmitter::new(stdout_writer));
    let writer: Arc<dyn ProtocolEmitter> = durable_writer.clone();
    let protocol_sink = Arc::new(ProtocolSink::new(writer.clone()));
    let approval_manager = Arc::new(ToolApprovalManager::new());
    let output: Arc<dyn OutputSink> = protocol_sink.clone();

    let provider_name = config.provider_label.clone();

    // JSON stream mode never prints a resume banner — the host is expected
    // to render its own resume UX from the `Ready` event's session_id.
    let manager = SessionManager::new(config.session.directory.clone().into(), config.session.max_sessions);
    let effective_resume = resolve_effective_resume(&manager, resume.as_deref(), session_id.as_deref())?;
    let resumed = effective_resume.is_some();
    let result = build_engine(
        config,
        cwd,
        output.clone(),
        permission_mode,
        effective_resume.as_deref(),
        |_session| {},
    )
    .await?;
    let mut engine = result.engine;
    engine.set_permission_mode(permission_mode);
    // `build_engine` already resolved policy precedence from CLI, environment,
    // project and user configuration. Intensity supplies a default only; it
    // must not replace an explicit multi-agent policy during stream startup.
    let configured_multi_agent_policy = engine.runtime_configuration_view().snapshot().multi_agent_policy;
    engine.apply_intensity(intensity);
    let _ = engine.apply_runtime_config_update(RuntimeConfigUpdate {
        multi_agent_policy: Some(configured_multi_agent_policy),
        ..Default::default()
    });
    let initial_has_mcp = result.has_mcp;
    let run_id = result.run_id.clone();
    let root_agent_id = result.root_agent_id.clone();
    let collaboration_runtime = Arc::clone(&result.collaboration_runtime);
    let workflow_controller = Arc::clone(&result.workflow_controller);
    let role_registry = Arc::clone(&result.role_registry);
    let spawner = Arc::clone(&result.spawner);
    let plugin_runtime = Arc::clone(&result.plugin_runtime);
    let execution_context = result.execution_context.clone();
    let mcp_identity_key = result.mcp_identity_key.clone();

    if !resumed {
        engine.init_session(&provider_name, cwd, session_id.as_deref())?;
    }

    approval_manager.set_mode(permission_mode);
    let permission_context = engine.permission_context();
    let runtime_configuration = engine.runtime_configuration_view();
    let sid = engine
        .current_session_id()
        .ok_or_else(|| anyhow::anyhow!("JSON stream mode requires a durable session"))?;
    let host_outbox = manager.open_host_outbox(&sid, run_id.as_str())?;
    let ready = ProtocolEvent::Ready {
        version: env!("CARGO_PKG_VERSION").to_owned(),
        session_id: Some(sid),
        resumed,
        capabilities: ProtocolSink::capabilities(engine.compat(), initial_has_mcp, &approval_manager.current_mode()),
    };
    durable_writer.activate_and_start_session(host_outbox, &ready)?;

    engine.set_approval_manager(approval_manager.clone());
    engine.set_protocol_writer(writer.clone());
    spawner.set_host_approval(approval_manager.clone(), writer.clone());

    let mut cmd_rx = spawn_stdin_reader();

    let pre_message_outcome = pre_message::run(
        &mut cmd_rx,
        &mut engine,
        &output,
        &writer,
        &spawner,
        &execution_context,
        mcp_identity_key.as_deref(),
    )
    .await;
    let output_health = check_protocol_output_health(&protocol_sink, &durable_writer);
    let (dynamic_managers, mut pending_cmd) = match pre_message_outcome {
        PreMessageOutcome::Stop { dynamic_managers } => {
            let managers = result
                .mcp_managers
                .iter()
                .chain(dynamic_managers.iter())
                .cloned()
                .collect::<Vec<_>>();
            let _ = shutdown_mcp_managers_with_timeout(&managers, MCP_SHUTDOWN_TIMEOUT).await;
            output_health?;
            return Ok(());
        }
        PreMessageOutcome::Continue {
            dynamic_managers,
            next_command,
        } => (dynamic_managers, next_command.map(|c| *c)),
    };
    output_health?;

    let has_mcp = initial_has_mcp || !dynamic_managers.is_empty();
    let host_capabilities = serde_json::to_value(ProtocolSink::capabilities(
        engine.compat(),
        has_mcp,
        &approval_manager.current_mode(),
    ))?;

    let (workflow_turn_commits, mut workflow_turn_commit_rx) = mpsc::unbounded_channel();
    let ctx = StreamContext {
        output: output.clone(),
        writer: writer.clone(),
        approval_manager: approval_manager.clone(),
        permission_context,
        runtime_configuration,
        workspace: std::path::PathBuf::from(cwd),
        run_id,
        root_agent_id,
        collaboration_runtime,
        workflow_controller,
        role_registry,
        spawner,
        plugin_runtime,
        execution_context,
        detached_workflows: DetachedWorkflowRegistry::default(),
        workflow_turn_commits,
        protocol_sink: protocol_sink.clone(),
        has_mcp,
        host_capabilities,
    };

    dispatch::resume_restored_workflows(&ctx, &mut engine);
    while let Ok(commit) = workflow_turn_commit_rx.try_recv() {
        apply_workflow_turn_commit(&mut engine, commit);
    }

    let runtime_subscription = ctx
        .collaboration_runtime
        .subscribe_with_snapshot_and(&ctx.run_id, || ctx.capture_runtime_extras())?;
    let extras = runtime_subscription.extra;
    let initial_snapshot = runtime_subscription.snapshot;
    let _ = writer.emit(&ProtocolEvent::RuntimeSnapshot {
        request_id: "initial-runtime-snapshot".to_owned(),
        schema_version: initial_snapshot.schema_version,
        timestamp_unix_ms: initial_snapshot.timestamp_unix_ms,
        live_sequence: initial_snapshot.live_sequence,
        journal_sequence: initial_snapshot.journal_sequence,
        run_id: ctx.run_id.to_string(),
        snapshot: ctx.runtime_snapshot_from(
            initial_snapshot.projection,
            initial_snapshot.journal_sequence,
            initial_snapshot.live_sequence,
            initial_snapshot.timestamp_unix_ms,
            extras,
        ),
    });
    let mut runtime_events = runtime_subscription.receiver;
    let runtime_writer = Arc::clone(&writer);
    let runtime_forwarder = tokio::spawn(async move {
        loop {
            match runtime_events.recv().await {
                Ok(event) => {
                    let mut payload = event.payload;
                    if let Some(agent_id) = event.agent_id {
                        if let Some(object) = payload.as_object_mut() {
                            object
                                .entry("agent_id".to_owned())
                                .or_insert_with(|| serde_json::json!(agent_id));
                        } else {
                            payload = serde_json::json!({"agent_id": agent_id, "value": payload});
                        }
                    }
                    let _ = runtime_writer.emit(&ProtocolEvent::RuntimeEvent {
                        schema_version: event.schema_version,
                        kind: event.kind,
                        sequence: event.sequence,
                        timestamp_unix_ms: event.timestamp_unix_ms,
                        journal_sequence: event.journal_sequence,
                        run_id: event.run_id.to_string(),
                        payload,
                    });
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(missed)) => {
                    let _ = runtime_writer.emit(&ProtocolEvent::RuntimeEvent {
                        schema_version: 1,
                        kind: "runtime_event_gap".to_owned(),
                        sequence: 0,
                        timestamp_unix_ms: chrono::Utc::now().timestamp_millis(),
                        journal_sequence: None,
                        run_id: String::new(),
                        payload: serde_json::json!({
                            "missed": missed,
                            "action": "request_runtime_journal_then_snapshot"
                        }),
                    });
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    let mut fatal_output_error = None;
    let mut output_health_interval =
        tokio::time::interval_at(Instant::now() + OUTPUT_HEALTH_INTERVAL, OUTPUT_HEALTH_INTERVAL);
    output_health_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    'main_loop: loop {
        if let Err(error) = check_protocol_output_health(&protocol_sink, &durable_writer) {
            fatal_output_error = Some(error);
            break;
        }
        let cmd = if let Some(c) = pending_cmd.take() {
            c
        } else {
            loop {
                tokio::select! {
                    biased;
                    commit = workflow_turn_commit_rx.recv() => {
                        let Some(commit) = commit else {
                            continue;
                        };
                        apply_workflow_turn_commit(&mut engine, commit);
                    }
                    _ = output_health_interval.tick() => {
                        if let Err(error) = check_protocol_output_health(&protocol_sink, &durable_writer) {
                            fatal_output_error = Some(error);
                            break 'main_loop;
                        }
                    }
                    command = cmd_rx.recv() => match command {
                        Some(ProtocolInput::Command(command)) => break *command,
                        Some(ProtocolInput::Invalid(error)) => {
                            let _ = writer.emit(&error.into_event());
                        }
                        None => break 'main_loop,
                    }
                }
            }
        };

        if let ProtocolCommand::Message { msg_id, .. } = &cmd
            && let Some((active_msg_id, workflow_run_id)) = ctx.detached_workflows.active_required_turn()
        {
            let _ = ctx.writer.emit(&ProtocolEvent::Error {
                msg_id: Some(msg_id.clone()),
                error: solaris_protocol::events::ErrorInfo {
                    code: "turn_busy".to_owned(),
                    message: format!(
                        "required workflow turn {active_msg_id} ({workflow_run_id}) is still being restored"
                    ),
                    retryable: true,
                },
            });
            continue;
        }

        if let ProtocolCommand::Message {
            msg_id,
            content,
            files: _,
        } = cmd
        {
            let stopped = message::handle(&msg_id, &content, &mut engine, &mut cmd_rx, &ctx).await;
            if stopped {
                break;
            }
            continue;
        }

        match dispatch::handle(cmd, &mut engine, &ctx) {
            DispatchOutcome::Stop => break,
            DispatchOutcome::Continue => {}
        }
    }

    if !ctx
        .detached_workflows
        .cancel_and_wait(std::time::Duration::from_secs(5))
        .await
    {
        for workflow_run_id in ctx.detached_workflows.active_run_ids() {
            let _ = ctx
                .workflow_controller
                .cancel(&workflow_run_id, "host_shutdown_timeout");
            let _ = ctx.collaboration_runtime.ledger().append(
                &workflow_run_id,
                DurabilityClass::SyncCritical,
                "workflow_shutdown_reconcile_required",
                serde_json::json!({"workflow_run_id": workflow_run_id}),
            );
            ctx.emit_runtime_event(
                "workflow_shutdown_reconcile_required",
                serde_json::json!({"workflow_run_id": workflow_run_id}),
            );
        }
        ctx.detached_workflows.abort_all();
    }
    runtime_forwarder.abort();
    engine.run_stop_hooks().await;
    let managers = result
        .mcp_managers
        .iter()
        .chain(dynamic_managers.iter())
        .cloned()
        .collect::<Vec<_>>();
    let _ = shutdown_mcp_managers_with_timeout(&managers, MCP_SHUTDOWN_TIMEOUT).await;

    if fatal_output_error.is_none()
        && let Err(error) = check_protocol_output_health(&protocol_sink, &durable_writer)
    {
        fatal_output_error = Some(error);
    }
    match fatal_output_error {
        Some(error) => Err(error.into()),
        None => Ok(()),
    }
}

fn apply_workflow_turn_commit(engine: &mut solaris_agent::engine::AgentEngine, commit: WorkflowTurnCommit) {
    let host_msg_id = commit.host_msg_id.clone();
    let result = engine.commit_workflow_turn(
        &commit.user_content,
        &commit.workflow_run_id,
        &commit.workflow_id,
        &commit.workflow_version,
        &commit.output,
    );
    if let Err(error) = &result {
        tracing::error!(msg_id = %host_msg_id, error = %error, "failed to persist restored required workflow turn");
    }
    let _ = commit.completion.send(result);
}

fn resolve_effective_resume(
    manager: &SessionManager,
    explicit_resume: Option<&str>,
    exact_session_id: Option<&str>,
) -> anyhow::Result<Option<String>> {
    if let Some(resume) = explicit_resume {
        return Ok(Some(resume.to_owned()));
    }
    let Some(session_id) = exact_session_id else {
        return Ok(None);
    };
    Ok(manager.load_if_exists(session_id)?.map(|_| session_id.to_owned()))
}

#[cfg(test)]
#[path = "session_test.rs"]
mod session_test;
