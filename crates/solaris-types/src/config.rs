use std::fmt;

use serde::{Deserialize, Serialize};

use crate::workflow::MultiAgentPolicy;

/// Typed fields accepted by one atomic runtime configuration update.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeConfigUpdate {
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub thinking: Option<String>,
    #[serde(default)]
    pub thinking_budget: Option<u32>,
    #[serde(default)]
    pub effort: Option<String>,
    #[serde(default)]
    pub compaction: Option<String>,
    #[serde(default)]
    pub multi_agent_policy: Option<MultiAgentPolicy>,
    #[serde(default)]
    pub max_active_agents: Option<usize>,
}

/// A field accepted by the runtime `set_config` command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigField {
    Model,
    Thinking,
    ThinkingBudget,
    Effort,
    Compaction,
    MultiAgentPolicy,
    MaxActiveAgents,
}

impl fmt::Display for ConfigField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Model => "model",
            Self::Thinking => "thinking",
            Self::ThinkingBudget => "thinking_budget",
            Self::Effort => "effort",
            Self::Compaction => "compaction",
            Self::MultiAgentPolicy => "multi_agent_policy",
            Self::MaxActiveAgents => "max_active_agents",
        };
        f.write_str(value)
    }
}

/// Per-field status returned by a runtime configuration update.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigFieldStatus {
    Applied,
    Unsupported,
    Rejected,
}

impl fmt::Display for ConfigFieldStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Applied => "applied",
            Self::Unsupported => "unsupported",
            Self::Rejected => "rejected",
        };
        f.write_str(value)
    }
}

/// Typed result for one supplied `set_config` field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigFieldResult {
    pub field: ConfigField,
    pub status: ConfigFieldStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl ConfigFieldResult {
    pub fn new(field: ConfigField, status: ConfigFieldStatus, message: impl Into<String>) -> Self {
        Self {
            field,
            status,
            message: Some(message.into()),
        }
    }
}

/// Atomic outcome of one runtime configuration request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigUpdateOutcome {
    /// Whether every supplied field was accepted.
    pub applied: bool,
    /// Whether accepting the request changed any runtime state.
    pub changed: bool,
    /// Results in protocol field order.
    pub results: Vec<ConfigFieldResult>,
    /// Human-readable summary retained for older protocol clients.
    pub message: String,
}

#[cfg(test)]
#[path = "config_test.rs"]
mod config_test;
