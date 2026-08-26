use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "solaris",
    about = "Solaris Mesh CLI — lightweight access to the multi-agent runtime",
    version
)]
pub(crate) struct Cli {
    // --- Subcommand ---
    #[command(subcommand)]
    pub(crate) command: Option<Commands>,

    // --- Provider / model connection ---
    /// Provider: "anthropic" or "openai"
    #[arg(short, long, env = "PROVIDER")]
    pub(crate) provider: Option<String>,

    /// API key
    #[arg(short = 'k', long, env = "API_KEY")]
    pub(crate) api_key: Option<String>,

    /// Base URL for the API
    #[arg(short, long, env = "BASE_URL")]
    pub(crate) base_url: Option<String>,

    /// Model name
    #[arg(short, long, env = "MODEL")]
    pub(crate) model: Option<String>,

    /// Max output tokens per response
    #[arg(long)]
    pub(crate) max_tokens: Option<u32>,

    /// Thinking mode for providers that support a thinking request object: enabled or disabled
    #[arg(long)]
    pub(crate) thinking: Option<String>,

    /// Token budget used with --thinking enabled; Anthropic-only, ignored by OpenAI-compatible requests
    #[arg(long)]
    pub(crate) thinking_budget: Option<u32>,

    // --- Runtime guards ---
    /// Max model turns per run. Defaults to 20; 0 disables.
    #[arg(long)]
    pub(crate) max_turns: Option<usize>,

    /// Max consecutive same tool-call-malformed rounds before stopping. 0 disables.
    #[arg(long)]
    pub(crate) max_tool_call_malformed_turns: Option<usize>,

    /// Max consecutive tool-call-failure rounds before stopping. 0 disables.
    #[arg(long)]
    pub(crate) max_tool_call_failure_turns: Option<usize>,

    // --- Prompt / profile ---
    /// Custom system prompt
    #[arg(long, env = "SYSTEM_PROMPT")]
    pub(crate) system_prompt: Option<String>,

    /// Named profile from config file
    #[arg(long)]
    pub(crate) profile: Option<String>,

    /// Permission posture: plan, auto (default), or bypass.
    #[arg(long, value_parser = ["plan", "auto", "bypass"])]
    pub(crate) permission: Option<String>,

    /// Execution intensity: low, medium, high, xhigh, extra, or ultracode.
    #[arg(long, value_parser = ["low", "medium", "high", "xhigh", "extra", "ultracode"])]
    pub(crate) intensity: Option<String>,

    /// Multi-agent policy: disabled, on_demand, or proactive.
    #[arg(
        long,
        value_parser = ["disabled", "on_demand", "on-demand", "ondemand", "explicit", "adaptive", "proactive"]
    )]
    pub(crate) multi_agent_policy: Option<String>,

    /// Collaboration strategy: auto, single, supervisor, team, fanout, or independent_reviewer.
    #[arg(
        long,
        value_parser = ["auto", "single", "supervisor", "team", "fanout", "independent_reviewer", "independent-reviewer", "reviewer"]
    )]
    pub(crate) collaboration_strategy: Option<String>,

    /// Maximum number of active Child Agents (1..=64).
    #[arg(long, value_parser = clap::value_parser!(usize))]
    pub(crate) max_active_agents: Option<usize>,

    /// Maximum number of tasks registered by one Run (1..=256).
    #[arg(long, value_parser = clap::value_parser!(u32))]
    pub(crate) max_agent_tasks: Option<u32>,

    /// Skip interactive tool approval while retaining auto permission isolation.
    #[arg(long)]
    pub(crate) auto_approve: bool,

    /// Project directory to load .solaris.toml from (defaults to CWD)
    #[arg(long)]
    pub(crate) project_dir: Option<PathBuf>,

    // --- Session ---
    /// Resume a previous session
    #[arg(long)]
    pub(crate) resume: Option<String>,

    /// Use a specific session ID (instead of auto-generating one)
    #[arg(long)]
    pub(crate) session_id: Option<String>,

    // --- Output ---
    /// Disable colored output
    #[arg(long)]
    pub(crate) no_color: bool,

    /// Enable JSON streaming mode for host client integration
    #[arg(long)]
    pub(crate) json_stream: bool,

    /// Exit after reporting process cleanup that still requires reconciliation.
    #[arg(long)]
    pub(crate) force_exit_with_pending_process_recovery: bool,

    /// Output compaction level: off, safe (default), full
    #[arg(long)]
    pub(crate) compaction: Option<String>,

    /// Enable TOON encoding for JSON arrays (session-level, cannot change mid-conversation)
    #[arg(long)]
    pub(crate) toon: bool,

    // --- Logging ---
    /// Log directory (enables file logging)
    #[arg(long)]
    pub(crate) log_dir: Option<String>,

    /// Log level filter (e.g. "info", "debug", "info,solaris_providers=debug")
    #[arg(long)]
    pub(crate) log_level: Option<String>,

    // --- Trailing prompt ---
    /// Initial prompt (if omitted, enters interactive REPL mode)
    #[arg(trailing_var_arg = true)]
    pub(crate) prompt: Vec<String>,
}

#[derive(Subcommand)]
pub(crate) enum Commands {
    /// Configuration file management
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
    /// Authentication (Anthropic OAuth)
    Auth {
        #[command(subcommand)]
        action: AuthAction,
    },
    /// Session management
    Session {
        #[command(subcommand)]
        action: SessionAction,
    },
    /// Skills directory introspection
    Skills {
        #[command(subcommand)]
        action: SkillsAction,
    },
    /// Process sandbox package verification
    Sandbox {
        #[command(subcommand)]
        action: SandboxAction,
    },
    /// Run Solaris Mesh as an Agent Client Protocol (ACP) agent over stdio.
    Acp,
}

#[derive(Subcommand)]
pub(crate) enum ConfigAction {
    /// Generate a default config file
    Init,
    /// Print config file path and exit
    Path,
}

#[derive(Subcommand)]
pub(crate) enum AuthAction {
    /// Login with Anthropic account (OAuth device flow)
    Login,
    /// Logout (remove saved OAuth credentials)
    Logout,
}

#[derive(Subcommand)]
pub(crate) enum SessionAction {
    /// List saved sessions
    List,
}

#[derive(Subcommand)]
pub(crate) enum SkillsAction {
    /// Print skill directory paths and exit
    Path,
}

#[derive(Subcommand)]
pub(crate) enum SandboxAction {
    /// Verify the packaged helper and run the strict platform sandbox probe
    VerifyPackage,
}

#[cfg(test)]
#[path = "cli_test.rs"]
mod cli_test;
