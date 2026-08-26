use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::effect::EffectDescriptor;
use crate::llm::LlmRequest;
use crate::message::{StopReason, TokenUsage, ToolUseId};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ImplementationIdentity {
    pub implementation_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PluginSource {
    Local { path: String },
    Git { repository: String, reference: String },
    Package { package: String, version: String },
    HostBundled { id: String },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginCapabilities {
    #[serde(default)]
    pub tools: Vec<String>,
    #[serde(default)]
    pub skills: Vec<String>,
    #[serde(default)]
    pub providers: Vec<String>,
    #[serde(default)]
    pub workflows: Vec<String>,
    #[serde(default)]
    pub collaboration_strategies: Vec<String>,
    #[serde(default)]
    pub storage_backends: Vec<String>,
    #[serde(default)]
    pub hooks: Vec<String>,
    #[serde(default)]
    pub services: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginCompatibility {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_api_version: Option<u32>,
    #[serde(default)]
    pub required_protocols: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginContributionKind {
    Provider,
    CollaborationStrategy,
    StorageBackend,
    Hook,
}

impl PluginContributionKind {
    pub fn capability_prefix(self) -> &'static str {
        match self {
            Self::Provider => "provider",
            Self::CollaborationStrategy => "strategy",
            Self::StorageBackend => "storage",
            Self::Hook => "hook",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginCommandContributionDefinition {
    pub kind: PluginContributionKind,
    pub name: String,
    /// Executable path relative to the resolved plugin authority root.
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default = "default_object_schema")]
    pub input_schema: Value,
    #[serde(default = "default_plugin_result_size")]
    pub max_result_size: usize,
    #[serde(default = "default_plugin_timeout")]
    pub timeout_ms: u64,
}

/// Stable input envelope for command-backed LLM providers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginProviderCommandRequest {
    pub protocol: String,
    pub request: LlmRequest,
}

impl PluginProviderCommandRequest {
    pub const PROTOCOL: &'static str = "provider-command-v1";

    pub fn new(request: LlmRequest) -> Self {
        Self {
            protocol: Self::PROTOCOL.to_owned(),
            request,
        }
    }
}

/// Complete response emitted by a command-backed LLM provider.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginProviderCommandResponse {
    pub events: Vec<PluginProviderEvent>,
}

/// Provider-neutral event representation used on the plugin JSON boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PluginProviderEvent {
    TextDelta {
        text: String,
    },
    ToolUse {
        id: ToolUseId,
        name: String,
        input: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        extra: Option<Value>,
    },
    ThinkingDelta {
        text: String,
    },
    ThinkingSignature {
        signature: String,
    },
    ProviderMetadata {
        namespace: String,
        value: Value,
    },
    Done {
        stop_reason: StopReason,
        usage: TokenUsage,
    },
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PluginScope {
    Global,
    Workspace { workspace_id: String },
    Run { run_id: String },
    Team { team_id: String },
    Agent { agent_id: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginCommandToolDefinition {
    pub name: String,
    pub description: String,
    #[serde(default = "default_object_schema")]
    pub input_schema: Value,
    /// Executable path relative to the resolved plugin authority root.
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    pub effect: EffectDescriptor,
    #[serde(default)]
    pub concurrency_safe: bool,
    #[serde(default = "default_plugin_result_size")]
    pub max_result_size: usize,
    #[serde(default = "default_plugin_timeout")]
    pub timeout_ms: u64,
}

fn default_object_schema() -> Value {
    serde_json::json!({"type": "object", "properties": {}})
}

fn default_plugin_result_size() -> usize {
    100_000
}

fn default_plugin_timeout() -> u64 {
    60_000
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginResources {
    /// Workflow JSON files relative to the resolved plugin authority root.
    #[serde(default)]
    pub workflow_files: Vec<String>,
    /// Skill directories relative to the resolved plugin authority root.
    #[serde(default)]
    pub skill_dirs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginDefinition {
    pub id: String,
    pub version: String,
    pub source: PluginSource,
    /// Optional Host-materialized package root. Required for executable/resources
    /// when source is Git/Package; it does not replace the canonical source identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub materialized_path: Option<String>,
    #[serde(default)]
    pub capabilities: PluginCapabilities,
    #[serde(default)]
    pub compatibility: PluginCompatibility,
    #[serde(default)]
    pub requested_paths: Vec<String>,
    #[serde(default)]
    pub requires_services: Vec<String>,
    #[serde(default)]
    pub resources: PluginResources,
    #[serde(default)]
    pub command_tools: Vec<PluginCommandToolDefinition>,
    #[serde(default)]
    pub command_contributions: Vec<PluginCommandContributionDefinition>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolvedPluginDefinition {
    pub definition: PluginDefinition,
    pub identity: ResolvedPluginIdentity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_root: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedPluginIdentity {
    pub plugin_id: String,
    pub source: PluginSource,
    pub implementation: ImplementationIdentity,
}

#[cfg(test)]
#[path = "plugin_test.rs"]
mod plugin_test;
