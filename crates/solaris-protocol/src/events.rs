use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use solaris_compact::CompactLevel;
use solaris_types::config::ConfigFieldResult;
use solaris_types::effect::{DurabilityClass, EffectDescriptor};
use solaris_types::llm::ThinkingConfig;
use solaris_types::permission::PermissionMode;
use solaris_types::plan::PlanArtifact;
use solaris_types::run_preset::Intensity;
use solaris_types::tool::ToolResultMetadata;
use solaris_types::workflow::MultiAgentPolicy;

pub use solaris_types::tool::ToolResultStatus as ToolStatus;

/// Provider-neutral configuration that is currently effective for new model turns.
///
/// `selected_intensity` preserves the user's requested preset while
/// `effective_effort` reports the provider-supported effort actually sent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeConfiguration {
    pub provider: String,
    pub model: String,
    pub permission: PermissionMode,
    pub selected_intensity: Intensity,
    #[serde(default)]
    pub multi_agent_policy: MultiAgentPolicy,
    #[serde(default)]
    pub max_active_agents: Option<usize>,
    #[serde(default = "default_effective_max_active_agents")]
    pub effective_max_active_agents: usize,
    pub effective_effort: Option<String>,
    #[serde(default)]
    pub thinking: Option<ThinkingConfig>,
    #[serde(default)]
    pub thinking_budget: Option<u32>,
    #[serde(default)]
    pub compaction: CompactLevel,
}

const fn default_effective_max_active_agents() -> usize {
    1
}

/// Typed body of a runtime snapshot.
///
/// The configuration field is intentionally not part of the open extension
/// map, so every snapshot and `ConfigChanged` event serialize the same
/// `RuntimeConfiguration` contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeSnapshotPayload {
    pub configuration: RuntimeConfiguration,
    #[serde(flatten)]
    extensions: Map<String, Value>,
}

impl RuntimeSnapshotPayload {
    pub fn new(configuration: RuntimeConfiguration, extensions: Value) -> Self {
        let mut extensions = extensions.as_object().cloned().unwrap_or_default();
        extensions.remove("configuration");
        Self {
            configuration,
            extensions,
        }
    }

    pub fn extension(&self, key: &str) -> Option<&Value> {
        self.extensions.get(key)
    }
}

/// Events emitted by the agent to the client (Agent -> Client)
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum ProtocolEvent {
    Ready {
        version: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        session_id: Option<String>,
        #[serde(skip_serializing_if = "std::ops::Not::not")]
        resumed: bool,
        capabilities: Capabilities,
    },
    StreamStart {
        msg_id: String,
    },
    TextDelta {
        text: String,
        msg_id: String,
    },
    Thinking {
        text: String,
        msg_id: String,
    },
    ToolRequest {
        msg_id: String,
        call_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        run_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        agent_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        operation_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        effect_id: Option<String>,
        tool: ToolInfo,
    },
    ToolRunning {
        msg_id: String,
        call_id: String,
        tool_name: String,
    },
    ToolResult {
        msg_id: String,
        call_id: String,
        tool_name: String,
        status: ToolStatus,
        output: String,
        output_type: OutputType,
        #[serde(skip_serializing_if = "Option::is_none")]
        metadata: Option<ToolResultMetadata>,
    },
    ToolCancelled {
        msg_id: String,
        call_id: String,
        status: ToolStatus,
        reason: String,
    },
    StreamEnd {
        msg_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        usage: Option<Usage>,
    },
    Error {
        #[serde(skip_serializing_if = "Option::is_none")]
        msg_id: Option<String>,
        error: ErrorInfo,
    },
    Info {
        msg_id: String,
        message: String,
    },
    ConfigChanged {
        capabilities: Capabilities,
        configuration: RuntimeConfiguration,
    },
    CommandResult {
        request_id: String,
        command: String,
        applied: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        config_results: Option<Vec<ConfigFieldResult>>,
    },
    McpReady {
        name: String,
        tools: Vec<String>,
    },
    RuntimeSnapshot {
        request_id: String,
        schema_version: u32,
        timestamp_unix_ms: i64,
        live_sequence: u64,
        journal_sequence: u64,
        run_id: String,
        snapshot: RuntimeSnapshotPayload,
    },
    PlanArtifacts {
        request_id: String,
        run_id: String,
        artifacts: Vec<PlanArtifact>,
    },
    RuntimeJournal {
        request_id: String,
        run_id: String,
        after_sequence: u64,
        last_sequence: u64,
        records: Vec<RuntimeJournalRecord>,
        truncated: bool,
    },
    RuntimeEvent {
        schema_version: u32,
        kind: String,
        sequence: u64,
        timestamp_unix_ms: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        journal_sequence: Option<u64>,
        run_id: String,
        payload: Value,
    },
    Pong,
}

impl ProtocolEvent {
    pub fn delivery_msg_id(&self) -> &str {
        match self {
            Self::Ready { session_id, .. } => nonempty(session_id.as_deref(), "ready"),
            Self::StreamStart { msg_id }
            | Self::TextDelta { msg_id, .. }
            | Self::Thinking { msg_id, .. }
            | Self::ToolRequest { msg_id, .. }
            | Self::ToolRunning { msg_id, .. }
            | Self::ToolResult { msg_id, .. }
            | Self::ToolCancelled { msg_id, .. }
            | Self::StreamEnd { msg_id, .. }
            | Self::Info { msg_id, .. } => nonempty(Some(msg_id), "host-event"),
            Self::Error { msg_id, .. } => nonempty(msg_id.as_deref(), "error"),
            Self::ConfigChanged { .. } => "configuration",
            Self::CommandResult { request_id, .. }
            | Self::RuntimeSnapshot { request_id, .. }
            | Self::PlanArtifacts { request_id, .. }
            | Self::RuntimeJournal { request_id, .. } => nonempty(Some(request_id), "request"),
            Self::McpReady { name, .. } => nonempty(Some(name), "mcp"),
            Self::RuntimeEvent { run_id, kind, .. } => nonempty(Some(run_id), nonempty(Some(kind), "runtime")),
            Self::Pong => "pong",
        }
    }
}

fn nonempty<'value>(value: Option<&'value str>, fallback: &'value str) -> &'value str {
    value.filter(|value| !value.is_empty()).unwrap_or(fallback)
}

#[derive(Debug, Clone, Serialize)]
pub struct RuntimeJournalRecord {
    pub schema_version: u32,
    pub sequence: u64,
    pub run_id: String,
    pub timestamp_unix_ms: i64,
    pub durability: DurabilityClass,
    pub record_type: String,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct Capabilities {
    pub tool_approval: bool,
    pub thinking: bool,
    pub effort: bool,
    pub effort_levels: Vec<String>,
    pub modes: Vec<String>,
    pub current_mode: String,
    pub mcp: bool,
}

#[derive(Debug, Serialize)]
pub struct ToolInfo {
    pub name: String,
    pub category: ToolCategory,
    pub args: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effect: Option<Box<EffectDescriptor>>,
    pub description: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolCategory {
    Info,
    Edit,
    Exec,
    Mcp,
}

impl std::fmt::Display for ToolCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Info => write!(f, "info"),
            Self::Edit => write!(f, "edit"),
            Self::Exec => write!(f, "exec"),
            Self::Mcp => write!(f, "mcp"),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputType {
    Text,
    Diff,
    Image,
}

#[derive(Debug, Serialize)]
pub struct Usage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uncached_input_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_read_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_write_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_usd: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct ErrorInfo {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

#[cfg(test)]
#[path = "events_test.rs"]
mod events_test;
