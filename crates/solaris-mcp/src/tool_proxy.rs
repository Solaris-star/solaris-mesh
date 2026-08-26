use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use hmac::{Hmac, Mac};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use url::Url;

use super::config::McpServerConfig;
use super::identity::McpIdentityKey;
use super::manager::McpManager;
use solaris_process::ExecutableIdentity;
use solaris_protocol::events::ToolCategory;
use solaris_tools::Tool;
use solaris_tools::tool_search::ToolSearchTool;
use solaris_types::effect::{EffectClass, EffectDescriptor, EffectReplayPolicy, ProcessInvocation, ResourceFootprint};
use solaris_types::plugin::ImplementationIdentity;
use solaris_types::tool::{JsonSchema, ToolResult};

#[derive(Debug, Serialize)]
struct NamedSecretIdentity {
    name: String,
    value_hmac_sha256: String,
}

fn sha256(value: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(value))
}

fn hmac_sha256(identity_key: &McpIdentityKey, value: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(identity_key.key()).expect("HMAC accepts keys of any size");
    mac.update(value);
    format!("hmac-sha256:{:x}", mac.finalize().into_bytes())
}

fn network_origin(raw_url: &str) -> Option<String> {
    let parsed = Url::parse(raw_url).ok()?;
    if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
        return None;
    }
    Some(parsed.origin().ascii_serialization())
}

fn named_secret_identities(
    values: Option<&HashMap<String, String>>,
    identity_key: &McpIdentityKey,
) -> Vec<NamedSecretIdentity> {
    let mut identities = values
        .into_iter()
        .flat_map(HashMap::iter)
        .map(|(name, value)| NamedSecretIdentity {
            name: name.clone(),
            value_hmac_sha256: hmac_sha256(identity_key, value.as_bytes()),
        })
        .collect::<Vec<_>>();
    identities.sort_by(|left, right| left.name.cmp(&right.name));
    identities
}

/// Return the stable, secret-safe identity used to pin one MCP server configuration.
/// Secret-bearing values are represented only by a Host-keyed HMAC-SHA-256 digest.
pub fn mcp_server_config_identity(config: &McpServerConfig, identity_key: &McpIdentityKey) -> Value {
    mcp_server_config_identity_with_executable(config, identity_key, None)
}

pub fn mcp_server_config_identity_with_executable(
    config: &McpServerConfig,
    identity_key: &McpIdentityKey,
    executable: Option<&ExecutableIdentity>,
) -> Value {
    let args = config
        .args
        .as_deref()
        .unwrap_or_default()
        .iter()
        .enumerate()
        .map(|(index, value)| {
            serde_json::json!({
                "index": index,
                "value_hmac_sha256": hmac_sha256(identity_key, value.as_bytes()),
            })
        })
        .collect::<Vec<_>>();
    serde_json::json!({
        "schema": "solaris.mcp.server-config.v3",
        "identity_key_version": identity_key.version(),
        "transport": config.transport,
        "command_hmac_sha256": config.command.as_deref().map(|value| hmac_sha256(identity_key, value.as_bytes())),
        "executable": executable.map(|identity| serde_json::json!({
            "path_identity": identity.path_digest(),
            "content_identity": identity.content_digest(),
        })),
        "args": args,
        "env": named_secret_identities(config.env.as_ref(), identity_key),
        "url_hmac_sha256": config.url.as_deref().map(|value| hmac_sha256(identity_key, value.as_bytes())),
        "headers": named_secret_identities(config.headers.as_ref(), identity_key),
        "deferred": config.deferred,
        "startup_timeout_ms": config.startup_timeout_ms,
    })
}

fn mcp_tool_implementation_identity(
    server_name: &str,
    tool_name: &str,
    config: &McpServerConfig,
    identity_key: &McpIdentityKey,
    executable: Option<&ExecutableIdentity>,
) -> ImplementationIdentity {
    let identity = mcp_server_config_identity_with_executable(config, identity_key, executable);
    let serialized = serde_json::to_vec(&identity).unwrap_or_default();
    ImplementationIdentity {
        implementation_id: mcp_permission_capability(server_name, tool_name),
        version: Some("server-config-v3".to_owned()),
        digest: Some(sha256(&serialized)),
    }
}

fn mcp_permission_capability(server_name: &str, tool_name: &str) -> String {
    format!(
        "mcp:{}:{server_name}:{}:{tool_name}",
        server_name.len(),
        tool_name.len()
    )
}

const MCP_DISPLAY_ALIAS_PREFIX: &str = "mcp_v3_";
const MCP_DISPLAY_DIGEST_BYTES: usize = 28;

fn mcp_display_name_from_digest(digest: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(MCP_DISPLAY_ALIAS_PREFIX.len() + MCP_DISPLAY_DIGEST_BYTES * 2);
    encoded.push_str(MCP_DISPLAY_ALIAS_PREFIX);
    for byte in digest.iter().take(MCP_DISPLAY_DIGEST_BYTES) {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn mcp_display_name(server_name: &str, tool_name: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"solaris.mcp.tool-display.v3\0");
    digest.update((server_name.len() as u64).to_be_bytes());
    digest.update(server_name.as_bytes());
    digest.update((tool_name.len() as u64).to_be_bytes());
    digest.update(tool_name.as_bytes());
    mcp_display_name_from_digest(&digest.finalize().into())
}

/// Wraps an MCP server tool as a local Tool trait implementation.
///
/// Model-visible names use a fixed-length SHA-256-derived v3 alias. This is an
/// intentional compatibility break from the unbounded v2 hexadecimal names.
/// Permission capabilities remain independent of display names, so an
/// existing exact permission grant does not widen.
pub struct McpToolProxy {
    /// Display name used for registration (may be prefixed)
    display_name: String,
    /// Original tool name on the MCP server
    tool_name: String,
    /// Server this tool belongs to
    server_name: String,
    description: String,
    input_schema: JsonSchema,
    manager: Arc<McpManager>,
    /// Whether this tool's schema should be deferred (sent as name-only stub).
    deferred: bool,
    effect: EffectDescriptor,
    implementation: ImplementationIdentity,
    permission_capability: String,
}

impl McpToolProxy {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        display_name: String,
        tool_name: String,
        server_name: String,
        description: String,
        input_schema: JsonSchema,
        manager: Arc<McpManager>,
        server_config: &McpServerConfig,
        identity_key: &McpIdentityKey,
        executable: Option<&ExecutableIdentity>,
        read_only_claim: bool,
    ) -> Self {
        let effect = mcp_effect_descriptor(&server_name, &tool_name, server_config, read_only_claim);
        let implementation =
            mcp_tool_implementation_identity(&server_name, &tool_name, server_config, identity_key, executable);
        let permission_capability = mcp_permission_capability(&server_name, &tool_name);
        Self {
            display_name,
            tool_name,
            server_name,
            description,
            input_schema,
            manager,
            deferred: server_config.deferred.unwrap_or(true),
            effect,
            implementation,
            permission_capability,
        }
    }
}

#[async_trait]
impl Tool for McpToolProxy {
    fn name(&self) -> &str {
        &self.display_name
    }

    fn permission_capability(&self) -> &str {
        &self.permission_capability
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn input_schema(&self) -> JsonSchema {
        self.input_schema.clone()
    }

    fn is_concurrency_safe(&self, _input: &Value) -> bool {
        // MCP tools are assumed not concurrency-safe
        false
    }

    fn is_deferred(&self) -> bool {
        self.deferred
    }

    async fn execute(&self, input: Value) -> ToolResult {
        match self.manager.call_tool(&self.server_name, &self.tool_name, input).await {
            Ok(content) => ToolResult {
                content,
                is_error: false,
            },
            Err(e) => ToolResult {
                content: format!("MCP tool error: {}", e),
                is_error: true,
            },
        }
    }

    fn describe_effect(&self, _input: &Value) -> EffectDescriptor {
        self.effect.clone()
    }

    fn implementation_identity(&self) -> Option<ImplementationIdentity> {
        Some(self.implementation.clone())
    }

    fn category(&self) -> ToolCategory {
        ToolCategory::Mcp
    }

    fn describe(&self, input: &Value) -> String {
        format!(
            "MCP {}/{}: {}",
            self.server_name,
            self.tool_name,
            serde_json::to_string(input).unwrap_or_default()
        )
    }
}

/// Register all MCP tools into the tool registry using deterministic names.
///
/// Each tool's deferred flag is read from the server's config:
/// `McpServerConfig::deferred` — defaults to `true` when absent.
pub fn register_mcp_tools(
    registry: &mut solaris_tools::registry::ToolRegistry,
    manager: &Arc<McpManager>,
    _builtin_names: &[String],
    server_configs: &HashMap<String, McpServerConfig>,
    identity_key: &McpIdentityKey,
) {
    let all_tools = manager.all_tools();

    // Determine which names need prefixing
    for (server_name, tool_def) in &all_tools {
        let original_name = &tool_def.name;

        let display_name = mcp_display_name(server_name, original_name);

        // MCP tools are deferred by default; server config can override.
        let Some(server_config) = server_configs.get(*server_name) else {
            tracing::warn!(target: "solaris_mcp", server = *server_name, "skipping MCP tool without its connection config");
            continue;
        };
        let proxy = McpToolProxy::new(
            display_name,
            original_name.clone(),
            server_name.to_string(),
            tool_def.description.clone().unwrap_or_default(),
            tool_def.input_schema.clone(),
            Arc::clone(manager),
            server_config,
            identity_key,
            manager.server_executable_identity(server_name),
            tool_def.annotations.is_trusted_read_only(),
        );

        if let Err(error) = registry.register_unique(Box::new(proxy)) {
            tracing::warn!(target: "solaris_mcp", server = *server_name, tool = %original_name, %error, "skipping duplicate MCP tool registration");
        }
    }
}

/// Register tools from a single newly-connected MCP server.
pub fn register_single_server_tools(
    registry: &mut solaris_tools::registry::ToolRegistry,
    manager: &Arc<McpManager>,
    server_name: &str,
    _builtin_names: &[String],
    server_config: &McpServerConfig,
    identity_key: &McpIdentityKey,
) {
    let all_tools = manager.all_tools();
    let server_tools: Vec<_> = all_tools.iter().filter(|(sn, _)| *sn == server_name).collect();

    for (_, tool_def) in &server_tools {
        let original_name = &tool_def.name;
        let display_name = mcp_display_name(server_name, original_name);

        let proxy = McpToolProxy::new(
            display_name,
            original_name.clone(),
            server_name.to_string(),
            tool_def.description.clone().unwrap_or_default(),
            tool_def.input_schema.clone(),
            Arc::clone(manager),
            server_config,
            identity_key,
            manager.server_executable_identity(server_name),
            tool_def.annotations.is_trusted_read_only(),
        );

        if let Err(error) = registry.register_unique(Box::new(proxy)) {
            tracing::warn!(target: "solaris_mcp", server = %server_name, tool = %original_name, %error, "skipping duplicate MCP tool registration");
        }
    }

    let tool_search = std::collections::HashSet::from(["ToolSearch".to_owned()]);
    registry.remove_names(&tool_search);
    let snapshot = registry.to_tool_defs();
    if let Err(error) = registry.register_unique(Box::new(ToolSearchTool::new(snapshot))) {
        tracing::warn!(target: "solaris_mcp", %error, "failed to refresh ToolSearch after dynamic MCP registration");
    }
}

pub fn mcp_effect_descriptor(
    server_name: &str,
    tool_name: &str,
    config: &McpServerConfig,
    read_only_claim: bool,
) -> EffectDescriptor {
    let mut resources = ResourceFootprint {
        external_resources: vec![format!("mcp:{server_name}:{tool_name}")],
        ..Default::default()
    };
    let class = match config.transport {
        solaris_config::config::TransportType::Stdio => {
            resources.declare_sandboxed_process_access();
            if read_only_claim {
                EffectClass::Process
            } else {
                EffectClass::ExternalSideEffect
            }
        }
        solaris_config::config::TransportType::Sse | solaris_config::config::TransportType::StreamableHttp => {
            if let Some(origin) = config.url.as_deref().and_then(network_origin) {
                resources.network_domains.push(origin);
            }
            if read_only_claim {
                EffectClass::Network
            } else {
                EffectClass::ExternalSideEffect
            }
        }
    };
    EffectDescriptor {
        class,
        action: format!("call MCP {server_name}/{tool_name}"),
        resources,
        replay_policy: EffectReplayPolicy::ReconcileRequired,
    }
}

/// Build the secret-safe resource footprint for starting one configured MCP
/// transport. Actual argv values remain only in the in-memory server config;
/// the durable permission/effect descriptor contains Host-keyed identities.
pub fn mcp_connection_resources(config: &McpServerConfig, identity_key: &McpIdentityKey) -> ResourceFootprint {
    mcp_connection_resources_with_executable(config, identity_key, None)
}

pub fn mcp_connection_resources_with_executable(
    config: &McpServerConfig,
    identity_key: &McpIdentityKey,
    executable_identity: Option<&ExecutableIdentity>,
) -> ResourceFootprint {
    let mut resources = ResourceFootprint::default();
    match config.transport {
        solaris_config::config::TransportType::Stdio => {
            resources
                .network_domains
                .extend(config.network.network_domains.iter().cloned());
            if let Some(command) = &config.command {
                let executable = executable_identity
                    .map(|identity| identity.path_digest().to_owned())
                    .unwrap_or_else(|| hmac_sha256(identity_key, command.as_bytes()));
                resources.process_commands.push(executable.clone());
                resources.process_invocations.push(ProcessInvocation {
                    executable,
                    argv: config
                        .args
                        .as_deref()
                        .unwrap_or_default()
                        .iter()
                        .map(|arg| hmac_sha256(identity_key, arg.as_bytes()))
                        .collect(),
                });
            }
        }
        solaris_config::config::TransportType::Sse | solaris_config::config::TransportType::StreamableHttp => {
            if let Some(origin) = config.url.as_deref().and_then(network_origin) {
                resources.network_domains.push(origin);
            }
        }
    }
    resources
}

#[cfg(test)]
#[path = "tool_proxy_test.rs"]
mod tool_proxy_test;
