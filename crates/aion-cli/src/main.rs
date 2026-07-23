use clap::Parser;

mod bootstrap;
mod cli;
mod commands;
mod json_stream;
mod run;

use cli::Cli;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut cli = Cli::parse();
    let project_dir = cli.project_dir.clone().unwrap_or(std::env::current_dir()?);
    aion_config::migration::migrate_legacy_data(&project_dir)?;
    let command = cli.command.take();
    match command {
        Some(cmd) => commands::dispatch(cmd).await,
        None => run::run_main_flow(cli).await,
    }
}
