//! Top-level subcommand dispatch for the `solaris` CLI binary.

use super::{cmd_acp, cmd_auth, cmd_config, cmd_sandbox, cmd_session, cmd_skills};
use crate::cli::Commands;

pub(crate) async fn dispatch(cmd: Commands, cli: crate::cli::Cli) -> anyhow::Result<()> {
    match cmd {
        Commands::Config { action } => cmd_config::run(action),
        Commands::Auth { action } => cmd_auth::run(action).await,
        Commands::Session { action } => cmd_session::run(action),
        Commands::Skills { action } => cmd_skills::run(action),
        Commands::Sandbox { action } => cmd_sandbox::run(action),
        Commands::Acp => cmd_acp::run(cli).await,
    }
}
