use std::env;
use std::sync::Arc;
use std::time::Duration;

use solaris_agent::engine::AgentEngine;
use solaris_agent::error::AgentError;
use solaris_agent::output::OutputSink;
use solaris_agent::output::terminal::TerminalSink;
use solaris_agent::plugin_tool::PluginContributionDispatcher;
use solaris_agent::run_preset::{RuntimeTaskRouter, resolve_run_preset};
use solaris_agent::workflow_controller::WorkflowRuntimeIdentity;
use solaris_types::identity::RunId;
use solaris_types::permission::PermissionMode;
use solaris_types::run_preset::{Intensity, RunPreset};
use uuid::Uuid;

use crate::bootstrap::{build_engine, init_logging, resolve_config};
use crate::cli::Cli;
use crate::json_stream;

pub(crate) async fn run_main_flow(cli: Cli) -> anyhow::Result<()> {
    const PROCESS_RECOVERY_RETRY_INTERVAL: Duration = Duration::from_millis(100);
    const PROCESS_RECOVERY_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

    let json_stream = cli.json_stream;
    let force_recovery_exit = cli.force_exit_with_pending_process_recovery;
    let mut recovery_lifecycle = solaris_process::ProcessRecoveryLifecycle::start(PROCESS_RECOVERY_RETRY_INTERVAL);
    let runtime_result = run_main_flow_inner(cli).await;
    let recovery_result = finish_process_recovery(
        &mut recovery_lifecycle,
        PROCESS_RECOVERY_SHUTDOWN_TIMEOUT,
        force_recovery_exit,
        |error| report_process_recovery_required(json_stream, error),
    )
    .await;
    match (runtime_result, recovery_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(runtime_error), Ok(())) => Err(runtime_error),
        (Ok(()), Err(recovery_error)) => {
            report_process_recovery_required(json_stream, &recovery_error);
            Err(recovery_error.into())
        }
        (Err(runtime_error), Err(recovery_error)) => {
            report_process_recovery_required(json_stream, &recovery_error);
            Err(anyhow::anyhow!(
                "runtime failed: {runtime_error}; process cleanup requires reconciliation: {recovery_error}"
            ))
        }
    }
}

async fn finish_process_recovery(
    lifecycle: &mut solaris_process::ProcessRecoveryLifecycle,
    timeout: Duration,
    force_exit: bool,
    mut report: impl FnMut(&solaris_process::ProcessRecoveryLifecycleError),
) -> Result<(), solaris_process::ProcessRecoveryLifecycleError> {
    loop {
        match lifecycle.shutdown(timeout).await {
            Ok(()) => return Ok(()),
            Err(error) => {
                report(&error);
                if force_exit {
                    return Err(error);
                }
            }
        }
    }
}

async fn run_main_flow_inner(cli: Cli) -> anyhow::Result<()> {
    if cli.resume.is_some() && cli.session_id.is_some() {
        anyhow::bail!("Cannot use --resume and --session-id together");
    }

    let terminal = Arc::new(TerminalSink::new(cli.no_color));
    let output: Arc<dyn OutputSink> = terminal.clone();
    let intensity = resolve_intensity(&cli)?;
    let config = resolve_config(&cli)?;
    let permission_mode = resolve_permission_mode(&cli, config.tools.auto_approve)?;
    let _log_guard = init_logging(&config, cli.log_dir.as_deref(), cli.log_level.as_deref());
    let cwd = env::current_dir()?.to_string_lossy().to_string();

    if cli.json_stream {
        return json_stream::run(config, &cwd, cli.resume, cli.session_id, permission_mode, intensity).await;
    }

    let provider_name = config.provider_label.clone();
    let terminal_for_resume = terminal.clone();
    let result = build_engine(
        config,
        &cwd,
        output.clone(),
        permission_mode,
        cli.resume.as_deref(),
        |session| {
            terminal_for_resume.formatter().session_info(&format!(
                "Resumed session {} ({} messages, {} model)",
                session.id,
                session.messages.len(),
                session.model
            ));
        },
    )
    .await?;

    let run_id = result.run_id.clone();
    let router = RuntimeTaskRouter::new(
        Arc::clone(&result.workflow_controller),
        Arc::clone(&result.role_registry),
        Arc::clone(&result.spawner),
    )
    .with_plugin_contributions(Arc::new(PluginContributionDispatcher::new(
        Arc::clone(&result.plugin_runtime),
        result.execution_context.clone(),
        format!("run:{}", result.run_id),
    )));
    let mut engine = result.engine;
    engine.set_permission_mode(permission_mode);
    let preset = resolve_run_preset(intensity, engine.compat().effort_levels());
    engine.set_initial_reasoning_effort(preset.reasoning_effort.clone());

    if cli.resume.is_none() {
        engine.init_session(&provider_name, &cwd, cli.session_id.as_deref())?;
    }

    let prompt = cli.prompt.join(" ");
    if prompt.is_empty() {
        repl_loop(&mut engine, &terminal, &output, &router, &run_id, &preset).await?;
    } else {
        execute_prompt(&mut engine, &output, &router, &run_id, &preset, &prompt, "cli").await?;
    }

    engine.run_stop_hooks().await;
    for mgr in &result.mcp_managers {
        mgr.shutdown().await;
    }
    Ok(())
}

fn report_process_recovery_required(json_stream: bool, error: &solaris_process::ProcessRecoveryLifecycleError) {
    let event = process_recovery_event(error);
    let solaris_protocol::events::ProtocolEvent::Error { error: info, .. } = &event else {
        unreachable!("process recovery reporting always creates an error event");
    };
    tracing::error!(
        recovery_report = %info.message,
        "Host shutdown left process cleanup requiring reconciliation"
    );
    if json_stream {
        use solaris_protocol::writer::ProtocolEmitter;

        let writer = solaris_protocol::writer::ProtocolWriter::new();
        if let Err(emit_error) = writer.emit(&event) {
            tracing::error!(
                error_kind = ?emit_error.kind(),
                "failed to emit process reconciliation report"
            );
        }
    } else {
        eprintln!("{}", process_recovery_terminal_message(error));
    }
}

fn process_recovery_terminal_message(error: &solaris_process::ProcessRecoveryLifecycleError) -> String {
    let report = process_recovery_report(error);
    format!("Process cleanup requires reconciliation: {report}")
}

fn process_recovery_event(
    error: &solaris_process::ProcessRecoveryLifecycleError,
) -> solaris_protocol::events::ProtocolEvent {
    let report = process_recovery_report(error);
    solaris_protocol::events::ProtocolEvent::Error {
        msg_id: None,
        error: solaris_protocol::events::ErrorInfo {
            code: "process_reconciliation_required".to_owned(),
            message: report,
            retryable: false,
        },
    }
}

fn process_recovery_report(error: &solaris_process::ProcessRecoveryLifecycleError) -> String {
    let recoveries = error
        .pending()
        .iter()
        .map(|recovery| {
            serde_json::json!({
                "id": recovery.id().get(),
                "kind": recovery.kind().as_str(),
                "ref": recovery.reference(),
            })
        })
        .collect::<Vec<_>>();
    serde_json::json!({
        "schema": "solaris/process-recovery-report/v1",
        "recoveries": recoveries,
    })
    .to_string()
}

pub(crate) fn resolve_permission_mode(cli: &Cli, _resolved_auto_approve: bool) -> anyhow::Result<PermissionMode> {
    match cli.permission.as_deref() {
        Some("plan") => Ok(PermissionMode::Plan),
        Some("auto") => Ok(PermissionMode::Auto),
        Some("bypass") => Ok(PermissionMode::Bypass),
        Some(other) => anyhow::bail!("Invalid permission mode: {other}"),
        None => Ok(PermissionMode::Auto),
    }
}

pub(crate) fn resolve_intensity(cli: &Cli) -> anyhow::Result<Intensity> {
    cli.intensity
        .as_deref()
        .unwrap_or(Intensity::default().as_str())
        .parse::<Intensity>()
        .map_err(anyhow::Error::msg)
}

async fn execute_prompt(
    engine: &mut AgentEngine,
    output: &Arc<dyn OutputSink>,
    router: &RuntimeTaskRouter,
    run_id: &RunId,
    preset: &RunPreset,
    prompt: &str,
    request_id: &str,
) -> anyhow::Result<()> {
    let workflow = if engine.recognizes_slash_command(prompt) {
        None
    } else {
        let provider = engine.provider_label().to_owned();
        let model = engine.model().to_owned();
        let runtime_identity = WorkflowRuntimeIdentity::new(provider, model);
        router
            .execute_required_workflow(
                run_id,
                request_id,
                prompt,
                &runtime_identity,
                engine.permission_mode(),
                preset,
            )
            .await
            .map_err(anyhow::Error::msg)?
    };
    if let Some(workflow) = workflow {
        let committed_output = workflow.final_output.clone().unwrap_or_else(|| {
            serde_json::json!({
                "workflow_run_id": workflow.workflow_run_id,
                "status": workflow.snapshot.status,
            })
        });
        engine
            .commit_workflow_turn(
                prompt,
                &workflow.workflow_run_id,
                &workflow.snapshot.workflow_id,
                &workflow.snapshot.workflow_version,
                &committed_output,
            )
            .map_err(anyhow::Error::msg)?;
        output.emit_stream_start(request_id);
        let text = committed_output.as_str().map(str::to_owned).unwrap_or_else(|| {
            serde_json::to_string_pretty(&committed_output).unwrap_or_else(|_| committed_output.to_string())
        });
        output.emit_text_delta(&text, request_id);
        output.emit_stream_end(
            request_id,
            workflow.turns,
            workflow.usage.input_tokens,
            workflow.usage.output_tokens,
            workflow.usage.cache_creation_tokens,
            workflow.usage.cache_read_tokens,
        );
        return Ok(());
    }

    let result = engine.run(prompt, request_id).await?;
    output.emit_stream_end(
        request_id,
        result.turns,
        result.usage.input_tokens,
        result.usage.output_tokens,
        result.usage.cache_creation_tokens,
        result.usage.cache_read_tokens,
    );
    Ok(())
}

async fn repl_loop(
    engine: &mut AgentEngine,
    terminal: &Arc<TerminalSink>,
    output: &Arc<dyn OutputSink>,
    router: &RuntimeTaskRouter,
    run_id: &RunId,
    preset: &RunPreset,
) -> anyhow::Result<()> {
    use std::io::{self, BufRead};

    loop {
        terminal.formatter().repl_prompt();
        let mut input = String::new();
        io::stdin().lock().read_line(&mut input)?;
        let input = input.trim();
        if input.is_empty() {
            break;
        }

        let request_id = format!("repl-{}", Uuid::now_v7());
        match execute_prompt(engine, output, router, run_id, preset, input, &request_id).await {
            Ok(()) => {}
            Err(error)
                if error
                    .downcast_ref::<AgentError>()
                    .is_some_and(|value| matches!(value, AgentError::UserAborted)) =>
            {
                break;
            }
            Err(error) => output.emit_error(&error.to_string()),
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "run_test.rs"]
mod run_test;
