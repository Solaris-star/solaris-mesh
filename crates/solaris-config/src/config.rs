mod init;

pub use init::init_config;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::auth::{AuthConfig, OAuthManager};
use crate::compact::CompactConfig;
use crate::compat::ProviderCompat;
use crate::file_cache::FileCacheConfig;
use crate::hooks::HooksConfig;
use crate::logging::LoggingConfig;
use crate::memory_config::{MemoryConfig, MemoryConfigFile};
use crate::plan::PlanConfig;
use crate::shell::ShellConfig;
use solaris_types::llm::ThinkingConfig;
use solaris_types::provider_contract::{ProtocolId, ProviderContract, ProviderSignals, ReasoningCapabilities};
use solaris_types::workflow::{
    CollaborationRuntimeConfig, CollaborationSelection, CollaborationStrategy, MultiAgentPolicy,
};

// ---------------------------------------------------------------------------
// Provider-specific sub-configurations (defined here to avoid circular deps)
// ---------------------------------------------------------------------------

/// AWS Bedrock credentials configuration
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct BedrockConfig {
    pub region: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub profile: Option<String>,
    /// Resolved shared credentials file. Bootstrap pins relative paths to the
    /// workspace before provider construction.
    pub credentials_file: Option<String>,
}

/// Google Vertex AI authentication configuration
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct VertexConfig {
    pub project_id: Option<String>,
    pub region: Option<String>,
    pub credentials_file: Option<String>,
    pub service_account_json: Option<String>,
}

/// Transport type for MCP server connections
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum TransportType {
    #[default]
    Stdio,
    Sse,
    StreamableHttp,
}

/// A single MCP server configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct McpServerConfig {
    pub transport: TransportType,
    /// For stdio transport: the command to run
    pub command: Option<String>,
    /// For stdio transport: arguments to the command
    pub args: Option<Vec<String>>,
    /// Environment variables to set for this server (stdio)
    pub env: Option<HashMap<String, String>>,
    /// For SSE/HTTP transport: the URL
    pub url: Option<String>,
    /// HTTP headers for SSE/HTTP transports
    pub headers: Option<HashMap<String, String>>,
    /// Exact HTTP(S) destinations required by a stdio server in Auto mode.
    #[serde(default)]
    pub network: solaris_types::permission::ProcessNetworkConfig,
    /// Whether tools from this server should be deferred (name-only stub sent to LLM).
    /// Defaults to true when omitted — MCP tools are deferred by default to reduce
    /// input token usage. Set to `false` to send full schemas eagerly.
    pub deferred: Option<bool>,
    /// Startup timeout in milliseconds for connecting, initializing, and listing tools.
    /// Defaults to 30000ms when omitted.
    pub startup_timeout_ms: Option<u64>,
}

/// Collection of MCP server configurations
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct McpConfig {
    #[serde(default)]
    pub servers: HashMap<String, McpServerConfig>,
}

/// Top-level config file structure
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct ConfigFile {
    #[serde(default)]
    pub default: DefaultConfig,

    #[serde(default)]
    pub providers: HashMap<String, ProviderConfig>,

    #[serde(default)]
    pub profiles: HashMap<String, ProfileConfig>,

    #[serde(default)]
    pub tools: ToolsConfig,

    #[serde(default)]
    pub session: SessionConfig,

    #[serde(default)]
    pub memory: MemoryConfigFile,

    #[serde(default)]
    pub compact: CompactConfig,

    #[serde(default)]
    pub plan: PlanConfig,

    #[serde(default)]
    pub shell: ShellConfig,

    #[serde(default)]
    pub file_cache: FileCacheConfig,

    #[serde(default)]
    pub hooks: HooksConfig,

    pub bedrock: Option<BedrockConfig>,
    pub vertex: Option<VertexConfig>,
    pub auth: Option<AuthConfig>,

    #[serde(default)]
    pub mcp: McpConfig,

    #[serde(default)]
    pub logging: LoggingConfig,

    /// Multi-agent policy shared by the CLI and every child Agent.
    #[serde(default)]
    pub multi_agent: MultiAgentConfigFile,
}

/// Values accepted in a config file for the Mesh collaboration runtime.
///
/// Every field is optional so global and project files can be merged without
/// losing the distinction between an omitted value and an explicit default.
#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq, Eq)]
pub struct MultiAgentConfigFile {
    #[serde(default)]
    pub policy: Option<MultiAgentPolicy>,
    /// `auto` is represented by `CollaborationSelection::Auto` at runtime.
    #[serde(default)]
    pub strategy: Option<String>,
    #[serde(default)]
    pub max_active_agents: Option<usize>,
    #[serde(default)]
    pub max_tasks_per_run: Option<u32>,
}

/// Resolved Mesh collaboration settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultiAgentConfig {
    pub policy: MultiAgentPolicy,
    pub strategy: CollaborationSelection,
    pub max_active_agents: Option<usize>,
    pub max_tasks_per_run: u32,
    policy_explicit: bool,
    strategy_explicit: bool,
}

impl Default for MultiAgentConfig {
    fn default() -> Self {
        Self {
            policy: MultiAgentPolicy::OnDemand,
            strategy: CollaborationSelection::Auto,
            max_active_agents: None,
            max_tasks_per_run: 32,
            policy_explicit: false,
            strategy_explicit: false,
        }
    }
}

impl MultiAgentConfig {
    pub const MIN_MAX_ACTIVE_AGENTS: usize = 1;
    pub const MAX_MAX_ACTIVE_AGENTS: usize = 64;
    pub const DEFAULT_MAX_TASKS_PER_RUN: u32 = 32;
    pub const MIN_MAX_TASKS_PER_RUN: u32 = 1;
    pub const MAX_MAX_TASKS_PER_RUN: u32 = 256;

    pub fn policy_is_explicit(&self) -> bool {
        self.policy_explicit
    }

    pub fn strategy_is_explicit(&self) -> bool {
        self.strategy_explicit
    }

    /// Apply an intensity default only when no stronger source selected a value.
    pub fn apply_intensity_default(&mut self, intensity: solaris_types::run_preset::Intensity) {
        if !self.policy_explicit {
            self.policy = if intensity == solaris_types::run_preset::Intensity::Ultracode {
                MultiAgentPolicy::Proactive
            } else {
                MultiAgentPolicy::OnDemand
            };
        }
        if !self.strategy_explicit {
            self.strategy = CollaborationSelection::Auto;
        }
    }

    fn resolve(file: MultiAgentConfigFile) -> anyhow::Result<Self> {
        let policy = file.policy.unwrap_or(MultiAgentPolicy::OnDemand);
        let strategy = parse_collaboration_selection(file.strategy.as_deref())?;
        let max_active_agents = file.max_active_agents;
        if let Some(limit) = max_active_agents
            && !(Self::MIN_MAX_ACTIVE_AGENTS..=Self::MAX_MAX_ACTIVE_AGENTS).contains(&limit)
        {
            anyhow::bail!(
                "multi_agent.max_active_agents must be between {} and {}",
                Self::MIN_MAX_ACTIVE_AGENTS,
                Self::MAX_MAX_ACTIVE_AGENTS
            );
        }
        let max_tasks_per_run = file.max_tasks_per_run.unwrap_or(Self::DEFAULT_MAX_TASKS_PER_RUN);
        if !(Self::MIN_MAX_TASKS_PER_RUN..=Self::MAX_MAX_TASKS_PER_RUN).contains(&max_tasks_per_run) {
            anyhow::bail!(
                "multi_agent.max_tasks_per_run must be between {} and {}",
                Self::MIN_MAX_TASKS_PER_RUN,
                Self::MAX_MAX_TASKS_PER_RUN
            );
        }
        Ok(Self {
            policy,
            strategy,
            max_active_agents,
            max_tasks_per_run,
            policy_explicit: file.policy.is_some(),
            strategy_explicit: file.strategy.is_some(),
        })
    }

    fn merge_overrides(&mut self, overrides: MultiAgentOverrides) -> anyhow::Result<()> {
        if let Some(policy) = overrides.policy {
            self.policy = parse_multi_agent_policy(&policy)?;
            self.policy_explicit = true;
        }
        if let Some(strategy) = overrides.strategy {
            self.strategy = parse_collaboration_selection(Some(&strategy))?;
            self.strategy_explicit = true;
        }
        if let Some(limit) = overrides.max_active_agents {
            validate_max_active_agents(limit)?;
            self.max_active_agents = Some(limit);
        }
        if let Some(limit) = overrides.max_tasks_per_run {
            validate_max_tasks_per_run(limit)?;
            self.max_tasks_per_run = limit;
        }
        Ok(())
    }
}

/// CLI and environment overrides for `Config::resolve_with_multi_agent`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MultiAgentOverrides {
    pub policy: Option<String>,
    pub strategy: Option<String>,
    pub max_active_agents: Option<usize>,
    pub max_tasks_per_run: Option<u32>,
}

fn parse_multi_agent_policy(value: &str) -> anyhow::Result<MultiAgentPolicy> {
    match value.trim().to_ascii_lowercase().as_str() {
        "disabled" => Ok(MultiAgentPolicy::Disabled),
        "on_demand" | "on-demand" | "ondemand" | "explicit" | "adaptive" => Ok(MultiAgentPolicy::OnDemand),
        "proactive" => Ok(MultiAgentPolicy::Proactive),
        other => anyhow::bail!("invalid multi-agent policy '{other}'; expected disabled, on_demand, or proactive"),
    }
}

fn parse_collaboration_selection(value: Option<&str>) -> anyhow::Result<CollaborationSelection> {
    let Some(value) = value else {
        return Ok(CollaborationSelection::Auto);
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "auto" => Ok(CollaborationSelection::Auto),
        "single" => Ok(CollaborationSelection::Fixed(CollaborationStrategy::Single)),
        "supervisor" => Ok(CollaborationSelection::Configured(CollaborationRuntimeConfig {
            strategy: CollaborationStrategy::Supervisor,
            ..CollaborationRuntimeConfig::default()
        })),
        "team" => Ok(CollaborationSelection::Configured(CollaborationRuntimeConfig {
            strategy: CollaborationStrategy::Team,
            ..CollaborationRuntimeConfig::default()
        })),
        "fanout" => Ok(CollaborationSelection::Configured(CollaborationRuntimeConfig {
            strategy: CollaborationStrategy::Fanout,
            ..CollaborationRuntimeConfig::default()
        })),
        "independent_reviewer" | "independent-reviewer" | "reviewer" => {
            Ok(CollaborationSelection::Configured(CollaborationRuntimeConfig {
                strategy: CollaborationStrategy::IndependentReviewer,
                ..CollaborationRuntimeConfig::default()
            }))
        }
        other => anyhow::bail!(
            "invalid collaboration strategy '{other}'; expected auto, single, supervisor, team, fanout, or independent_reviewer"
        ),
    }
}

fn validate_max_active_agents(value: usize) -> anyhow::Result<()> {
    if !(MultiAgentConfig::MIN_MAX_ACTIVE_AGENTS..=MultiAgentConfig::MAX_MAX_ACTIVE_AGENTS).contains(&value) {
        anyhow::bail!(
            "max_active_agents must be between {} and {}",
            MultiAgentConfig::MIN_MAX_ACTIVE_AGENTS,
            MultiAgentConfig::MAX_MAX_ACTIVE_AGENTS
        );
    }
    Ok(())
}

fn validate_max_tasks_per_run(value: u32) -> anyhow::Result<()> {
    if !(MultiAgentConfig::MIN_MAX_TASKS_PER_RUN..=MultiAgentConfig::MAX_MAX_TASKS_PER_RUN).contains(&value) {
        anyhow::bail!(
            "max_agent_tasks must be between {} and {}",
            MultiAgentConfig::MIN_MAX_TASKS_PER_RUN,
            MultiAgentConfig::MAX_MAX_TASKS_PER_RUN
        );
    }
    Ok(())
}

fn apply_multi_agent_environment(config: &mut MultiAgentConfigFile) -> anyhow::Result<()> {
    if let Ok(value) = std::env::var("SOLARIS_MULTI_AGENT_POLICY") {
        config.policy = Some(parse_multi_agent_policy(&value)?);
    }
    if let Ok(value) = std::env::var("SOLARIS_COLLABORATION_STRATEGY") {
        // Validate now so an invalid environment value cannot silently fall
        // back to a different strategy later.
        let _ = parse_collaboration_selection(Some(&value))?;
        config.strategy = Some(value);
    }
    if let Ok(value) = std::env::var("SOLARIS_MAX_ACTIVE_AGENTS") {
        let parsed = value
            .parse::<usize>()
            .map_err(|_| anyhow::anyhow!("SOLARIS_MAX_ACTIVE_AGENTS must be an integer"))?;
        validate_max_active_agents(parsed)?;
        config.max_active_agents = Some(parsed);
    }
    if let Ok(value) = std::env::var("SOLARIS_MAX_AGENT_TASKS") {
        let parsed = value
            .parse::<u32>()
            .map_err(|_| anyhow::anyhow!("SOLARIS_MAX_AGENT_TASKS must be an integer"))?;
        validate_max_tasks_per_run(parsed)?;
        config.max_tasks_per_run = Some(parsed);
    }
    Ok(())
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DefaultConfig {
    #[serde(default = "default_provider")]
    pub provider: String,
    pub model: Option<String>,
    #[serde(default)]
    pub max_tokens: Option<u32>,
    #[serde(default)]
    pub max_turns: Option<usize>,
    #[serde(default)]
    pub max_tool_call_malformed_turns: Option<usize>,
    #[serde(default)]
    pub max_tool_call_failure_turns: Option<usize>,
    pub system_prompt: Option<String>,
}

impl Default for DefaultConfig {
    fn default() -> Self {
        Self {
            provider: default_provider(),
            model: None,
            max_tokens: None,
            max_turns: None,
            max_tool_call_malformed_turns: None,
            max_tool_call_failure_turns: None,
            system_prompt: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct ProviderConfig {
    /// Underlying built-in provider type for a custom provider alias.
    pub provider: Option<String>,
    /// Wire protocol, independent from the deployment/auth provider.
    pub protocol: Option<String>,
    /// Optional default model for this provider entry.
    pub model: Option<String>,
    pub api_key: Option<String>,
    pub base_url: Option<String>,
    /// Enable prompt caching (Anthropic only, default: true)
    pub prompt_caching: Option<bool>,
    /// Provider compatibility overrides
    pub compat: Option<ProviderCompat>,
}

/// A named profile bundles provider + model + overrides
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct ProfileConfig {
    pub provider: Option<String>,
    pub model: Option<String>,
    pub api_key: Option<String>,
    pub base_url: Option<String>,
    pub max_tokens: Option<u32>,
    pub max_turns: Option<usize>,
    pub max_tool_call_malformed_turns: Option<usize>,
    pub max_tool_call_failure_turns: Option<usize>,
    /// Inherit settings from another profile
    pub extends: Option<String>,
    /// MCP server names to enable for this profile (references [mcp.servers.*])
    pub mcp_servers: Option<Vec<String>>,
    /// Default shell override for this profile.
    pub shell: Option<String>,
    /// Provider compatibility overrides
    pub compat: Option<ProviderCompat>,
}

/// Per-skill deny/allow rule lists loaded from `[tools.skills]` in config.toml.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct SkillsPermissionConfig {
    #[serde(default)]
    pub deny: Vec<String>,
    #[serde(default)]
    pub allow: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ToolsConfig {
    #[serde(default)]
    pub auto_approve: bool,
    #[serde(default = "default_allow_list")]
    pub allow_list: Vec<String>,
    /// Skill-level deny/allow rules. Merged by concatenation across global + project configs.
    #[serde(default)]
    pub skills: SkillsPermissionConfig,
}

impl Default for ToolsConfig {
    fn default() -> Self {
        Self {
            auto_approve: false,
            allow_list: default_allow_list(),
            skills: SkillsPermissionConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SessionConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_session_dir")]
    pub directory: String,
    #[serde(default = "default_max_sessions")]
    pub max_sessions: usize,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            directory: default_session_dir(),
            max_sessions: default_max_sessions(),
        }
    }
}

// --- Default value functions ---

fn default_provider() -> String {
    "anthropic".to_string()
}
fn default_allow_list() -> Vec<String> {
    vec!["Read".into(), "Grep".into(), "Glob".into()]
}
fn default_true() -> bool {
    true
}
fn default_session_dir() -> String {
    ".solaris/sessions".to_string()
}
fn default_max_sessions() -> usize {
    20
}

fn resolve_max_turns(configured: Option<usize>) -> Option<usize> {
    match configured {
        Some(0) => None,
        Some(limit) => Some(limit),
        None => None,
    }
}

// --- Resolved runtime config ---

#[derive(Debug, Clone)]
pub struct Config {
    pub provider_label: String,
    pub provider: ProviderType,
    pub api_key: String,
    pub base_url: String,
    pub model: String,
    pub max_tokens: Option<u32>,
    pub max_turns: Option<usize>,
    pub max_tool_call_malformed_turns: Option<usize>,
    pub max_tool_call_failure_turns: Option<usize>,
    pub system_prompt: Option<String>,
    pub thinking: Option<ThinkingConfig>,
    pub prompt_caching: bool,
    pub compat: ProviderCompat,
    pub tools: ToolsConfig,
    pub session: SessionConfig,
    pub memory: MemoryConfig,
    pub compact: CompactConfig,
    pub plan: PlanConfig,
    pub shell: ShellConfig,
    pub file_cache: FileCacheConfig,
    pub hooks: HooksConfig,
    pub bedrock: Option<BedrockConfig>,
    pub vertex: Option<VertexConfig>,
    pub mcp: McpConfig,
    pub logging: LoggingConfig,
    pub multi_agent: MultiAgentConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderType {
    Anthropic,
    OpenAI,
    Bedrock,
    Vertex,
}

#[derive(Debug, Clone)]
struct ResolvedProviderConfig {
    requested_name: String,
    provider_type: ProviderType,
    effective_config: ProviderConfig,
}

/// CLI arguments needed for config resolution
pub struct CliArgs {
    pub provider: Option<String>,
    pub api_key: Option<String>,
    pub base_url: Option<String>,
    pub model: Option<String>,
    pub max_tokens: Option<u32>,
    pub thinking: Option<String>,
    pub thinking_budget: Option<u32>,
    pub max_turns: Option<usize>,
    pub max_tool_call_malformed_turns: Option<usize>,
    pub max_tool_call_failure_turns: Option<usize>,
    pub system_prompt: Option<String>,
    pub profile: Option<String>,
    pub auto_approve: bool,
    pub project_dir: Option<PathBuf>,
}

impl Config {
    /// Load and merge config from all sources
    pub fn resolve(cli: &CliArgs) -> anyhow::Result<Self> {
        Self::resolve_with_multi_agent(cli, MultiAgentOverrides::default())
    }

    /// Load config while applying CLI-owned multi-agent overrides.
    ///
    /// Keeping these overrides separate from `CliArgs` preserves source
    /// compatibility for embedders that construct `CliArgs` directly.
    pub fn resolve_with_multi_agent(cli: &CliArgs, multi_agent_overrides: MultiAgentOverrides) -> anyhow::Result<Self> {
        let project_dir = cli.project_dir.clone().unwrap_or(std::env::current_dir()?);
        crate::migration::migrate_legacy_data(&project_dir)?;

        // 1. Load global config
        let global = load_config_file(&global_config_path());

        // 2. Load project config (from project_dir if specified, else CWD)
        let project = load_config_file(&project_dir.join(".solaris.toml"));

        // 3. Merge: global <- project
        let mut merged = merge_config_files(global, project);

        // 4. If --profile specified, overlay profile settings
        if let Some(profile_name) = &cli.profile {
            merged = apply_profile(merged, profile_name)?;
        }

        let mut multi_agent_file = merged.multi_agent.clone();
        apply_multi_agent_environment(&mut multi_agent_file)?;
        let mut multi_agent = MultiAgentConfig::resolve(multi_agent_file)?;
        multi_agent.merge_overrides(multi_agent_overrides)?;

        // 5. Apply CLI overrides and resolve final config
        let provider_str = cli.provider.as_deref().unwrap_or(&merged.default.provider);

        let resolved_provider = resolve_provider_alias(&merged.providers, provider_str)?;
        let provider_label = resolved_provider.requested_name.clone();
        let provider = resolved_provider.provider_type;
        let provider_config = resolved_provider.effective_config;

        let base_url = cli
            .base_url
            .clone()
            .or_else(|| provider_config.base_url.clone())
            .unwrap_or_else(|| match provider {
                ProviderType::Anthropic => "https://api.anthropic.com".into(),
                ProviderType::OpenAI => "https://api.openai.com/v1".into(),
                // Bedrock/Vertex URLs are constructed from region/project, not base_url
                ProviderType::Bedrock | ProviderType::Vertex => String::new(),
            });
        let base_url = normalize_base_url(provider, base_url);

        let model = cli
            .model
            .clone()
            .or(provider_config.model.clone())
            .or(merged.default.model.clone())
            .unwrap_or_else(|| match provider {
                ProviderType::Anthropic => "claude-sonnet-4-20250514".into(),
                ProviderType::OpenAI => "gpt-4o".into(),
                ProviderType::Bedrock => "anthropic.claude-sonnet-4-20250514-v1:0".into(),
                ProviderType::Vertex => "claude-sonnet-4@20250514".into(),
            });

        let max_tokens = cli.max_tokens.or(merged.default.max_tokens);
        let max_turns = resolve_max_turns(cli.max_turns.or(merged.default.max_turns));
        let max_tool_call_malformed_turns = cli
            .max_tool_call_malformed_turns
            .or(merged.default.max_tool_call_malformed_turns);
        let max_tool_call_failure_turns = cli
            .max_tool_call_failure_turns
            .or(merged.default.max_tool_call_failure_turns);
        let system_prompt = cli.system_prompt.clone().or(merged.default.system_prompt.clone());

        // 6. Resolve API key: CLI > config file > env var
        let api_key = resolve_api_key(cli.api_key.as_deref(), provider_config.api_key.as_deref(), provider)?;

        // 7. Apply auto_approve from CLI
        let mut tools = merged.tools;
        if cli.auto_approve {
            tools.auto_approve = true;
        }

        // Resolve prompt_caching: default true for Anthropic
        let prompt_caching = provider_config
            .prompt_caching
            .unwrap_or(matches!(provider, ProviderType::Anthropic));

        // Resolve compat: provider-type defaults + user overrides
        let compat_defaults = match provider {
            ProviderType::Anthropic => ProviderCompat::anthropic_defaults(),
            ProviderType::OpenAI => ProviderCompat::openai_defaults(),
            ProviderType::Bedrock => ProviderCompat::bedrock_defaults(),
            ProviderType::Vertex => ProviderCompat::anthropic_defaults(),
        };

        let mut user_compat = provider_config.compat.clone().unwrap_or_default();
        if let Some(protocol) = provider_config.protocol.clone() {
            user_compat.transport.protocol_id = Some(protocol);
        }

        let compat = ProviderCompat::merge(compat_defaults, user_compat);
        let thinking = resolve_cli_thinking(cli.thinking.as_deref(), cli.thinking_budget)?;

        Ok(Config {
            provider_label,
            provider,
            api_key,
            base_url,
            model,
            max_tokens,
            max_turns,
            max_tool_call_malformed_turns,
            max_tool_call_failure_turns,
            system_prompt,
            thinking,
            prompt_caching,
            compat,
            tools,
            session: merged.session,
            memory: merged.memory.resolve(),
            compact: merged.compact,
            plan: merged.plan,
            shell: merged.shell,
            file_cache: merged.file_cache,
            hooks: merged.hooks,
            bedrock: merged.bedrock,
            vertex: merged.vertex,
            mcp: merged.mcp,
            logging: merged.logging,
            multi_agent,
        })
    }
    /// Return the resolved provider contract without coupling wire protocol to provider branding.
    pub fn provider_contract(&self) -> ProviderContract {
        let protocol = self
            .compat
            .protocol_id()
            .map(|value| ProtocolId(value.to_owned()))
            .unwrap_or_else(|| match self.provider {
                ProviderType::OpenAI => ProtocolId::openai_chat(),
                ProviderType::Anthropic | ProviderType::Bedrock | ProviderType::Vertex => {
                    ProtocolId::anthropic_messages()
                }
            });
        let metadata_namespaces = match protocol.0.as_str() {
            "anthropic-messages" => vec!["anthropic".to_owned()],
            "openai-chat" | "openai-responses" => vec!["openai".to_owned()],
            _ => Vec::new(),
        };
        let cache_token_accounting = self
            .compat
            .transport
            .cache_token_accounting
            .or_else(|| protocol.cache_token_accounting());
        ProviderContract {
            protocol,
            reasoning: ReasoningCapabilities {
                supported: self.compat.supports_thinking() || self.compat.supports_effort(),
                effort_levels: self.compat.effort_levels().to_vec(),
                requires_round_trip_metadata: self.compat.supports_thinking(),
            },
            tool_calling: self.compat.emit_tools(),
            streaming: true,
            context_window: None,
            cache_stable_append_tail: self.prompt_caching,
            metadata_namespaces,
            signals: ProviderSignals {
                cache_token_accounting,
                input_cost_per_million: self.compat.transport.input_cost_per_million,
                cache_read_cost_per_million: self.compat.transport.cache_read_cost_per_million,
                cache_write_cost_per_million: self.compat.transport.cache_write_cost_per_million,
                output_cost_per_million: self.compat.transport.output_cost_per_million,
                ..Default::default()
            },
        }
    }
}

const DEFAULT_THINKING_BUDGET: u32 = 10_000;

fn resolve_cli_thinking(
    thinking: Option<&str>,
    thinking_budget: Option<u32>,
) -> anyhow::Result<Option<ThinkingConfig>> {
    match thinking {
        Some("enabled") => Ok(Some(ThinkingConfig::Enabled {
            budget_tokens: thinking_budget.unwrap_or(DEFAULT_THINKING_BUDGET),
        })),
        Some("disabled") => Ok(Some(ThinkingConfig::Disabled)),
        Some(other) => anyhow::bail!("Invalid --thinking value: {other}. Expected 'enabled' or 'disabled'."),
        None => Ok(None),
    }
}

fn normalize_base_url(provider: ProviderType, base_url: String) -> String {
    if provider != ProviderType::OpenAI {
        return base_url;
    }

    let trimmed = base_url.trim_end_matches('/');
    if trimmed.eq_ignore_ascii_case("https://api.openai.com") || trimmed.eq_ignore_ascii_case("http://api.openai.com") {
        return format!("{trimmed}/v1");
    }

    base_url
}

fn parse_builtin_provider(s: &str) -> Option<ProviderType> {
    match s {
        "anthropic" => Some(ProviderType::Anthropic),
        "openai" => Some(ProviderType::OpenAI),
        "bedrock" => Some(ProviderType::Bedrock),
        "vertex" => Some(ProviderType::Vertex),
        _ => None,
    }
}

fn merge_provider_configs(base: ProviderConfig, overlay: ProviderConfig) -> ProviderConfig {
    ProviderConfig {
        provider: overlay.provider.or(base.provider),
        protocol: overlay.protocol.or(base.protocol),
        model: overlay.model.or(base.model),
        api_key: overlay.api_key.or(base.api_key),
        base_url: overlay.base_url.or(base.base_url),
        prompt_caching: overlay.prompt_caching.or(base.prompt_caching),
        compat: match (base.compat, overlay.compat) {
            (Some(base), Some(overlay)) => Some(ProviderCompat::merge(base, overlay)),
            (Some(base), None) => Some(base),
            (None, Some(overlay)) => Some(overlay),
            (None, None) => None,
        },
    }
}

fn resolve_provider_alias(
    providers: &HashMap<String, ProviderConfig>,
    requested: &str,
) -> anyhow::Result<ResolvedProviderConfig> {
    if let Some(provider_type) = parse_builtin_provider(requested) {
        return Ok(ResolvedProviderConfig {
            requested_name: requested.to_string(),
            provider_type,
            effective_config: providers.get(requested).cloned().unwrap_or_default(),
        });
    }

    let alias_config = providers.get(requested).cloned().ok_or_else(|| {
        anyhow::anyhow!(
            "Unknown provider: '{}'. Expected a built-in provider (anthropic, openai, bedrock, vertex) \
             or a custom alias defined in [providers.{}].",
            requested,
            requested
        )
    })?;

    let underlying = alias_config.provider.clone().ok_or_else(|| {
        anyhow::anyhow!(
            "Provider alias '{}' requires a 'provider' field in [providers.{}] \
             that maps to a built-in type (anthropic, openai, bedrock, vertex).",
            requested,
            requested
        )
    })?;

    let provider_type = parse_builtin_provider(&underlying).ok_or_else(|| {
        anyhow::anyhow!(
            "Provider alias '{}' maps to '{}', which is not a built-in provider. \
             Use one of: anthropic, openai, bedrock, vertex.",
            requested,
            underlying
        )
    })?;

    Ok(ResolvedProviderConfig {
        requested_name: requested.to_string(),
        provider_type,
        effective_config: merge_provider_configs(providers.get(&underlying).cloned().unwrap_or_default(), alias_config),
    })
}

fn resolve_api_key(cli_key: Option<&str>, config_key: Option<&str>, provider: ProviderType) -> anyhow::Result<String> {
    // CLI arg takes precedence
    if let Some(key) = cli_key {
        return Ok(key.to_string());
    }

    // Config file value
    if let Some(key) = config_key {
        return Ok(key.to_string());
    }

    // Env var fallback chain
    if let Ok(key) = std::env::var("API_KEY") {
        return Ok(key);
    }

    match provider {
        ProviderType::Anthropic => {
            if let Ok(key) = std::env::var("ANTHROPIC_API_KEY") {
                return Ok(key);
            }
        }
        ProviderType::OpenAI => {
            if let Ok(key) = std::env::var("OPENAI_API_KEY") {
                return Ok(key);
            }
        }
        // Bedrock uses AWS credentials, Vertex uses GCP credentials
        // They don't need a traditional API key
        ProviderType::Bedrock | ProviderType::Vertex => {
            return Ok(String::new());
        }
    }

    // Try OAuth credentials as last resort
    let oauth = OAuthManager::new(AuthConfig::default());
    if oauth.has_credentials() {
        return Ok(String::new()); // Will be resolved at runtime via OAuth
    }

    anyhow::bail!(
        "No API key found. Provide via --api-key, config file, environment variable \
         (API_KEY, ANTHROPIC_API_KEY, or OPENAI_API_KEY), or run 'solaris auth login'."
    )
}

// --- App directories ---

/// Platform-aware app config root.
///
/// - Linux:   `~/.config/solaris`
/// - macOS:   `~/Library/Application Support/solaris`
/// - Windows: `%APPDATA%\solaris`
pub fn app_config_dir() -> Option<PathBuf> {
    dirs::config_dir().map(|d| d.join("solaris"))
}

// --- Config file loading and merging ---

pub fn global_config_path() -> PathBuf {
    app_config_dir()
        .unwrap_or_else(|| PathBuf::from("solaris"))
        .join("config.toml")
}

fn load_config_file(path: &Path) -> ConfigFile {
    match std::fs::read_to_string(path) {
        Ok(content) => toml::from_str(&content).unwrap_or_else(|e| {
            tracing::warn!(target: "solaris_config", path = %path.display(), error = %e, "failed to parse config file");
            ConfigFile::default()
        }),
        Err(_) => ConfigFile::default(),
    }
}

/// Merge two config files. Project overrides global.
fn merge_config_files(global: ConfigFile, project: ConfigFile) -> ConfigFile {
    let default = DefaultConfig {
        provider: if project.default.provider != default_provider() {
            project.default.provider
        } else {
            global.default.provider
        },
        model: project.default.model.or(global.default.model),
        max_tokens: project.default.max_tokens.or(global.default.max_tokens),
        max_turns: project.default.max_turns.or(global.default.max_turns),
        max_tool_call_malformed_turns: project
            .default
            .max_tool_call_malformed_turns
            .or(global.default.max_tool_call_malformed_turns),
        max_tool_call_failure_turns: project
            .default
            .max_tool_call_failure_turns
            .or(global.default.max_tool_call_failure_turns),
        system_prompt: project.default.system_prompt.or(global.default.system_prompt),
    };

    // Merge providers: global as base, project overrides
    let mut providers = global.providers;
    for (k, v) in project.providers {
        let base = providers.remove(&k).unwrap_or_default();
        providers.insert(k, merge_provider_configs(base, v));
    }

    // Merge profiles: global as base, project overrides
    let mut profiles = global.profiles;
    profiles.extend(project.profiles);

    // Tools: project overrides global for scalar fields; skills deny/allow are concatenated
    // (global first, then project) — consistent with the hooks merge strategy.
    let tools = if project.tools.allow_list != default_allow_list() || project.tools.auto_approve {
        ToolsConfig {
            auto_approve: global.tools.auto_approve || project.tools.auto_approve,
            allow_list: project.tools.allow_list,
            skills: SkillsPermissionConfig {
                deny: [global.tools.skills.deny, project.tools.skills.deny].concat(),
                allow: [global.tools.skills.allow, project.tools.skills.allow].concat(),
            },
        }
    } else {
        ToolsConfig {
            auto_approve: global.tools.auto_approve || project.tools.auto_approve,
            allow_list: global.tools.allow_list,
            skills: SkillsPermissionConfig {
                deny: [global.tools.skills.deny, project.tools.skills.deny].concat(),
                allow: [global.tools.skills.allow, project.tools.skills.allow].concat(),
            },
        }
    };

    // Session: project overrides global
    let session = if project.session.directory != default_session_dir() {
        project.session
    } else {
        SessionConfig {
            enabled: global.session.enabled && project.session.enabled,
            directory: if project.session.directory != default_session_dir() {
                project.session.directory
            } else {
                global.session.directory
            },
            max_sessions: if project.session.max_sessions != default_max_sessions() {
                project.session.max_sessions
            } else {
                global.session.max_sessions
            },
        }
    };

    let memory = MemoryConfigFile::merge(global.memory, project.memory);

    // Hooks: combine hooks from both configs (project hooks appended after global)
    let hooks = HooksConfig {
        pre_tool_use: [global.hooks.pre_tool_use, project.hooks.pre_tool_use].concat(),
        post_tool_use: [global.hooks.post_tool_use, project.hooks.post_tool_use].concat(),
        stop: [global.hooks.stop, project.hooks.stop].concat(),
    };

    // MCP: merge servers from both configs, project overrides global
    let mut mcp_servers = global.mcp.servers;
    mcp_servers.extend(project.mcp.servers);
    let mcp = McpConfig { servers: mcp_servers };

    // Plan: project overrides global if any field differs from default
    let plan = if !project.plan.enabled || project.plan.plan_directory != PlanConfig::default().plan_directory {
        project.plan
    } else {
        global.plan
    };

    // File cache: project overrides global if any field differs from default.
    let file_cache = if !project.file_cache.enabled
        || project.file_cache.max_entries != FileCacheConfig::default().max_entries
        || project.file_cache.max_size_bytes != FileCacheConfig::default().max_size_bytes
    {
        project.file_cache
    } else {
        global.file_cache
    };

    // Bedrock/Vertex/Auth: project overrides global
    let bedrock = project.bedrock.or(global.bedrock);
    let vertex = project.vertex.or(global.vertex);
    let auth = project.auth.or(global.auth);

    // Compact: project overrides global for any non-default field.
    // Since CompactConfig uses serde defaults, a fully-default project config
    // is indistinguishable from "absent". We use project if its context_window
    // differs from the default, otherwise fall back to global.
    let compact =
        if project.compact.context_window != CompactConfig::default().context_window || !project.compact.enabled {
            project.compact
        } else {
            global.compact
        };

    let logging = LoggingConfig::merge(global.logging, project.logging);

    let shell = if project.shell.default != ShellConfig::default().default {
        project.shell
    } else {
        global.shell
    };

    let multi_agent = MultiAgentConfigFile {
        policy: project.multi_agent.policy.or(global.multi_agent.policy),
        strategy: project.multi_agent.strategy.or(global.multi_agent.strategy),
        max_active_agents: project
            .multi_agent
            .max_active_agents
            .or(global.multi_agent.max_active_agents),
        max_tasks_per_run: project
            .multi_agent
            .max_tasks_per_run
            .or(global.multi_agent.max_tasks_per_run),
    };

    ConfigFile {
        default,
        providers,
        profiles,
        tools,
        session,
        memory,
        compact,
        plan,
        shell,
        file_cache,
        hooks,
        bedrock,
        vertex,
        auth,
        mcp,
        logging,
        multi_agent,
    }
}

/// Resolve a profile with inheritance chain (with cycle detection)
fn resolve_profile(
    profiles: &HashMap<String, ProfileConfig>,
    name: &str,
    visited: &mut Vec<String>,
) -> anyhow::Result<ProfileConfig> {
    if visited.contains(&name.to_string()) {
        anyhow::bail!(
            "Circular profile inheritance detected: {} -> {}",
            visited.join(" -> "),
            name
        );
    }
    visited.push(name.to_string());

    let profile = profiles
        .get(name)
        .ok_or_else(|| anyhow::anyhow!("Profile '{}' not found in config", name))?
        .clone();

    if let Some(parent_name) = &profile.extends {
        let parent = resolve_profile(profiles, parent_name, visited)?;
        Ok(merge_profiles(parent, profile))
    } else {
        Ok(profile)
    }
}

/// Merge two profiles: overlay takes precedence over base
fn merge_profiles(base: ProfileConfig, overlay: ProfileConfig) -> ProfileConfig {
    ProfileConfig {
        provider: overlay.provider.or(base.provider),
        model: overlay.model.or(base.model),
        api_key: overlay.api_key.or(base.api_key),
        base_url: overlay.base_url.or(base.base_url),
        max_tokens: overlay.max_tokens.or(base.max_tokens),
        max_turns: overlay.max_turns.or(base.max_turns),
        max_tool_call_malformed_turns: overlay
            .max_tool_call_malformed_turns
            .or(base.max_tool_call_malformed_turns),
        max_tool_call_failure_turns: overlay.max_tool_call_failure_turns.or(base.max_tool_call_failure_turns),
        extends: None, // already resolved
        mcp_servers: overlay.mcp_servers.or(base.mcp_servers),
        shell: overlay.shell.or(base.shell),
        compat: overlay.compat.or(base.compat),
    }
}

fn apply_profile(mut config: ConfigFile, profile_name: &str) -> anyhow::Result<ConfigFile> {
    let mut visited = Vec::new();
    let profile = resolve_profile(&config.profiles, profile_name, &mut visited)?;

    if let Some(provider) = profile.provider {
        config.default.provider = provider;
    }
    if let Some(model) = profile.model {
        config.default.model = Some(model);
    }
    if let Some(max_tokens) = profile.max_tokens {
        config.default.max_tokens = Some(max_tokens);
    }
    if let Some(max_turns) = profile.max_turns {
        config.default.max_turns = Some(max_turns);
    }
    if let Some(max_tool_call_malformed_turns) = profile.max_tool_call_malformed_turns {
        config.default.max_tool_call_malformed_turns = Some(max_tool_call_malformed_turns);
    }
    if let Some(max_tool_call_failure_turns) = profile.max_tool_call_failure_turns {
        config.default.max_tool_call_failure_turns = Some(max_tool_call_failure_turns);
    }
    if let Some(shell) = profile.shell {
        config.shell.default = shell;
    }

    // Profile can override api_key, base_url, and compat for the active provider
    let provider_name = config.default.provider.clone();
    let entry = config.providers.entry(provider_name).or_default();
    if let Some(api_key) = profile.api_key {
        entry.api_key = Some(api_key);
    }
    if let Some(base_url) = profile.base_url {
        entry.base_url = Some(base_url);
    }
    if let Some(compat) = profile.compat {
        entry.compat = Some(match entry.compat.take() {
            Some(existing) => ProviderCompat::merge(existing, compat),
            None => compat,
        });
    }

    // Filter MCP servers by profile's mcp_servers list
    if let Some(server_names) = profile.mcp_servers {
        config.mcp.servers.retain(|name, _| server_names.contains(name));
    }

    Ok(config)
}

#[cfg(test)]
#[path = "config_test.rs"]
mod config_test;
