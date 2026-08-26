use super::global_config_path;

pub fn init_config() -> anyhow::Result<()> {
    let path = global_config_path();
    if path.exists() {
        tracing::info!(target: "solaris_config", path = %path.display(), "config file already exists");
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&path, DEFAULT_CONFIG_TEMPLATE)?;
    tracing::info!(target: "solaris_config", path = %path.display(), "config file created");
    Ok(())
}

const DEFAULT_CONFIG_TEMPLATE: &str = r#"# Solaris Mesh configuration

# Default provider settings
[default]
provider = "anthropic"            # built-in provider or custom alias from [providers.<name>]
# model = "claude-sonnet-4-20250514"
# max_tokens = 8192                  # optional per-response output cap; omit to use provider/model defaults
# max_turns = 20                  # optional max model turns per run; omit or set 0 to disable
# max_tool_call_malformed_turns = 3  # 0 disables the tool-call-malformed round breaker
# max_tool_call_failure_turns = 3    # 0 disables the tool-call-failure round breaker
# system_prompt = "..."          # optional custom system prompt

# Shell execution settings
[shell]
default = "auto"                 # auto, powershell, pwsh, cmd, bash, zsh, sh, or executable path

# Provider-specific API settings
[providers.anthropic]
# api_key = "sk-ant-xxx"         # can also use env: API_KEY or ANTHROPIC_API_KEY
# base_url = "https://api.anthropic.com"

[providers.openai]
# api_key = "sk-xxx"             # can also use env: OPENAI_API_KEY
# base_url = "https://api.openai.com/v1"

# Custom provider alias (maps to a built-in provider type)
# [providers.my-service]
# provider = "openai"
# model = "custom-model-v1"
# api_key = "sk-xxx"
# base_url = "https://my-service.example.com/api/openai"

# Provider compatibility overrides (usually not needed — defaults work)
# [providers.openai.compat]
# max_tokens_field = "max_completion_tokens"  # for OpenAI official models
# merge_assistant_messages = true
# clean_orphan_tool_calls = true
# dedup_tool_results = true
# strip_patterns = ["__OPENROUTER_REASONING_DETAILS__"]

# AWS Bedrock configuration (uses AWS SigV4 auth, no API key needed)
# [bedrock]
# region = "us-east-1"
# access_key_id = "AKIA..."
# secret_access_key = "..."
# session_token = "..."
# profile = "my-profile"        # or use AWS profile

# Google Vertex AI configuration (uses GCP OAuth2 auth, no API key needed)
# [vertex]
# project_id = "my-gcp-project"
# region = "us-central1"
# credentials_file = "/path/to/service-account.json"  # or use ADC

# OAuth settings (for `solaris auth login` with Claude.ai account)
# [auth]
# auth_url = "https://claude.ai/oauth"
# token_url = "https://claude.ai/oauth/token"
# client_id = "solaris-mesh"

# Named profiles for quick switching (--profile <name>)
# [profiles.deepseek]
# provider = "openai"
# model = "deepseek-chat"
# api_key = "sk-xxx"
# base_url = "https://api.deepseek.com/v1"

# [profiles.deepseek-v4-pro]
# provider = "openai"
# model = "deepseek-v4-pro"
# api_key = "sk-xxx"
# base_url = "https://api.deepseek.com/v1"
# max_tokens = 16384
#
# [profiles.deepseek-v4-pro.compat]
# supports_thinking = true

# [profiles.ollama]
# provider = "openai"
# model = "qwen2.5:32b"
# api_key = "ollama"
# base_url = "http://localhost:11434"

# [profiles.my-service]
# provider = "my-service"

# [profiles.bedrock-claude]
# provider = "bedrock"
# model = "anthropic.claude-sonnet-4-20250514-v1:0"

# [profiles.vertex-claude]
# provider = "vertex"
# model = "claude-sonnet-4@20250514"

# Tool confirmation settings
[tools]
auto_approve = false             # --auto-approve overrides
# Tools that skip confirmation even when auto_approve = false
allow_list = ["Read", "Grep", "Glob"]

# Context compaction settings
# [compact]
# context_window = 200000        # context window size in tokens
# output_reserve = 20000         # tokens reserved for output
# autocompact_buffer = 13000     # buffer below effective window for autocompact trigger
# emergency_buffer = 3000        # tokens from limit for emergency block
# max_failures = 3               # consecutive failures before circuit-breaker trips
# micro_keep_recent = 5          # keep N most recent tool results
# micro_gap_seconds = 3600       # gap threshold for time-based microcompact
# compactable_tools = ["Read", "ExecCommand", "Grep", "Glob", "Write", "Edit"]
# enabled = true

# File state cache (dedup repeated reads, staleness detection)
# [file_cache]
# max_entries = 100            # max cached file entries
# max_size_bytes = 26214400    # 25 MB total cache size
# enabled = true

# Session settings
[session]
enabled = true
directory = ".solaris/sessions"  # session.sqlite3 root, relative to project root
max_sessions = 20                # visible inactive sessions; active sessions are retained

# Hook system: run shell commands at tool lifecycle events
# [[hooks.post_tool_use]]
# name = "rustfmt"
# tool_match = ["Write", "Edit"]
# file_match = ["*.rs"]
# command = "rustfmt ${TOOL_INPUT_FILE_PATH}"

# [[hooks.post_tool_use]]
# name = "prettier"
# tool_match = ["Write", "Edit"]
# file_match = ["*.ts", "*.tsx"]
# command = "npx prettier --write ${TOOL_INPUT_FILE_PATH}"

# [[hooks.stop]]
# name = "final-lint"
# command = "cargo clippy --quiet 2>&1 | tail -5"

# Logging configuration
# [logging]
# enabled = true                   # enable file logging (default: false)
# level = "info"                   # log level filter (default: "info")
# dir = "~/Library/Logs/solaris"    # log directory (default: platform-specific)

# MCP (Model Context Protocol) servers
# [mcp.servers.filesystem]
# transport = "stdio"
# command = "npx"
# args = ["-y", "@modelcontextprotocol/server-filesystem", "/Users/me/project"]

# [mcp.servers.github]
# transport = "stdio"
# command = "npx"
# args = ["-y", "@modelcontextprotocol/server-github"]
# env = { GITHUB_TOKEN = "ghp_xxx" }
# startup_timeout_ms = 30000

# [mcp.servers.remote]
# transport = "sse"
# url = "http://localhost:3001/sse"

# [mcp.servers.api]
# transport = "streamable-http"
# url = "https://tools.example.com/mcp"
# headers = { Authorization = "Bearer xxx" }
"#;
