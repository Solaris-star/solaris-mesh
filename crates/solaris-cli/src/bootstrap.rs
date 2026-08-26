//! Shared initialization steps for Solaris Mesh CLI and host entry points.
//!
//! `run.rs` (terminal REPL) and `json_stream` (host-integration protocol)
//! both need to resolve config, initialize logging, and bootstrap an
//! `AgentEngine` (optionally resuming a saved session). This module holds
//! that shared logic so the two call sites don't duplicate it.
//!
//! Note: `solaris_agent` also exposes a module named `bootstrap`
//! (`solaris_agent::bootstrap::AgentBootstrap`). Call sites should keep using
//! that fully-qualified path for the engine builder itself; this module is
//! `crate::bootstrap` and only wraps it.

use std::ffi::OsString;
use std::sync::Arc;

use solaris_agent::bootstrap::{AgentBootstrap, BootstrapResult};
use solaris_agent::output::OutputSink;
use solaris_agent::session::{Session, SessionManager};
use solaris_compact::CompactLevel;
use solaris_config::config::{CliArgs, Config, MultiAgentOverrides};
use solaris_config::logging::{LoggingGuard, create_file_layer};
use solaris_process::filter_resource_environment;
use solaris_types::permission::PermissionMode;

use crate::cli::Cli;

fn resource_runtime_env_from(values: impl IntoIterator<Item = (OsString, OsString)>) -> Vec<(String, String)> {
    let selected: Vec<_> = values
        .into_iter()
        .filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)))
        .collect();
    filter_resource_environment(selected)
}

/// Resolve layered config (files + CLI args + env vars), then apply
/// CLI-only overrides that don't have a `CliArgs` slot (compaction level,
/// TOON encoding).
pub(crate) fn resolve_config(cli: &Cli) -> anyhow::Result<Config> {
    let cli_args = CliArgs {
        provider: cli.provider.clone(),
        api_key: cli.api_key.clone(),
        base_url: cli.base_url.clone(),
        model: cli.model.clone(),
        max_tokens: cli.max_tokens,
        thinking: cli.thinking.clone(),
        thinking_budget: cli.thinking_budget,
        max_turns: cli.max_turns,
        max_tool_call_malformed_turns: cli.max_tool_call_malformed_turns,
        max_tool_call_failure_turns: cli.max_tool_call_failure_turns,
        system_prompt: cli.system_prompt.clone(),
        profile: cli.profile.clone(),
        auto_approve: cli.auto_approve,
        project_dir: cli.project_dir.clone(),
    };

    let mut config = Config::resolve_with_multi_agent(
        &cli_args,
        MultiAgentOverrides {
            policy: cli.multi_agent_policy.clone(),
            strategy: cli.collaboration_strategy.clone(),
            max_active_agents: cli.max_active_agents,
            max_tasks_per_run: cli.max_agent_tasks,
        },
    )?;

    // Intensity supplies a default only when the user did not choose an
    // explicit policy or strategy through config, environment, or CLI.
    let intensity = cli
        .intensity
        .as_deref()
        .unwrap_or("medium")
        .parse::<solaris_types::run_preset::Intensity>()
        .map_err(anyhow::Error::msg)?;
    config.multi_agent.apply_intensity_default(intensity);

    if let Some(ref level_str) = cli.compaction {
        match level_str.parse::<CompactLevel>() {
            Ok(level) => config.compact.compaction = level,
            Err(e) => anyhow::bail!("Invalid --compaction value: {e}"),
        }
    }
    if cli.toon {
        config.compact.toon = true;
    }

    Ok(config)
}

/// Initialize file logging from the resolved config + CLI overrides.
///
/// Returns the worker guard that must be kept alive for the process
/// lifetime, or `None` if logging is disabled or failed to initialize.
pub(crate) fn init_logging(config: &Config, log_dir: Option<&str>, log_level: Option<&str>) -> Option<LoggingGuard> {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let resolved = config.logging.resolve(log_dir, log_level);
    if !resolved.enabled {
        return None;
    }

    match create_file_layer(&resolved) {
        Ok((layer, guard)) => {
            tracing_subscriber::registry().with(layer).init();
            Some(guard)
        }
        Err(e) => {
            eprintln!("Warning: failed to initialize logging: {e}");
            None
        }
    }
}

/// Bootstrap an `AgentEngine`, optionally resuming a saved session.
///
/// Both call sites (terminal REPL, JSON stream) need identical bootstrap +
/// resume-load logic, but differ in what happens *when* a session is
/// resumed: the terminal prints a "Resumed session ..." line, JSON stream
/// mode stays silent. `on_resume` captures that difference without forcing
/// the two call sites to converge on identical behavior.
pub(crate) async fn build_engine(
    config: Config,
    cwd: &str,
    output: Arc<dyn OutputSink>,
    permission_mode: PermissionMode,
    resume_id: Option<&str>,
    on_resume: impl FnOnce(&Session),
) -> anyhow::Result<BootstrapResult> {
    let mut agent_bootstrap = AgentBootstrap::new(config, cwd, output)
        .runtime_env(resource_runtime_env_from(std::env::vars_os()))
        .permission_mode(permission_mode);

    if let Some(resume_id) = resume_id {
        let cfg = agent_bootstrap.config();
        let session_mgr = SessionManager::new(cfg.session.directory.clone().into(), cfg.session.max_sessions);
        let session = session_mgr.load(resume_id)?;
        on_resume(&session);
        agent_bootstrap = agent_bootstrap.resume(session);
    }

    agent_bootstrap.build().await
}

#[cfg(test)]
#[path = "bootstrap_test.rs"]
mod bootstrap_test;
