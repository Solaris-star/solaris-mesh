//! ACP agent subcommand.

use std::env;

use crate::acp;
use crate::bootstrap::{init_logging, resolve_config};
use crate::cli::Cli;
use crate::run::{resolve_intensity, resolve_permission_mode};

/// Resolve the ordinary CLI configuration and serve an ACP connection on stdio.
pub(crate) async fn run(cli: Cli) -> anyhow::Result<()> {
    let intensity = resolve_intensity(&cli)?;
    let config = resolve_config(&cli)?;
    let permission_mode = resolve_permission_mode(&cli, config.tools.auto_approve)?;
    let _log_guard = init_logging(&config, cli.log_dir.as_deref(), cli.log_level.as_deref());
    let cwd = cli
        .project_dir
        .clone()
        .unwrap_or(env::current_dir()?)
        .canonicalize()
        .unwrap_or_else(|_| env::current_dir().unwrap_or_else(|_| ".".into()));
    acp::run(config, &cwd.to_string_lossy(), permission_mode, intensity).await
}
