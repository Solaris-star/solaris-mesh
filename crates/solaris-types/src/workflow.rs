use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::identity::{AgentId, OperationId, TaskId};
use crate::message::TokenUsage;
use crate::permission::PermissionCeiling;
use crate::resource::ResourceBudget;
use crate::runtime::{TaskFailureClass, TaskState};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MultiAgentPolicy {
    Disabled,
    #[default]
    #[serde(alias = "explicit", alias = "adaptive")]
    OnDemand,
    Proactive,
}

impl MultiAgentPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::OnDemand => "on_demand",
            Self::Proactive => "proactive",
        }
    }
}

impl std::fmt::Display for MultiAgentPolicy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CollaborationStrategy {
    #[default]
    Single,
    Supervisor,
    Team,
    Fanout,
    IndependentReviewer,
}

impl CollaborationStrategy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Single => "single",
            Self::Supervisor => "supervisor",
            Self::Team => "team",
            Self::Fanout => "fanout",
            Self::IndependentReviewer => "independent_reviewer",
        }
    }
}

impl std::fmt::Display for CollaborationStrategy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

const DEFAULT_MAX_CONCURRENT_WORKERS: u32 = 4;
const DEFAULT_MAX_TASKS: u32 = 32;
const DEFAULT_MAX_COORDINATOR_ROUNDS: u32 = 8;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerRolePolicy {
    pub role: String,
    pub max_concurrent: u32,
    pub max_total: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollaborationRuntimeConfig {
    pub strategy: CollaborationStrategy,
    #[serde(default)]
    pub worker_roles: Vec<WorkerRolePolicy>,
    #[serde(default = "default_max_concurrent_workers")]
    pub max_concurrent_workers: u32,
    #[serde(default = "default_max_tasks")]
    pub max_tasks: u32,
    #[serde(default = "default_max_coordinator_rounds")]
    pub max_coordinator_rounds: u32,
    #[serde(default = "CollaborationRuntimeConfig::default_max_pending_messages")]
    pub max_pending_messages: u32,
    #[serde(default = "CollaborationRuntimeConfig::default_max_message_bytes")]
    pub max_message_bytes: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_role: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reviewer_role: Option<String>,
}

impl Default for CollaborationRuntimeConfig {
    fn default() -> Self {
        Self {
            strategy: CollaborationStrategy::default(),
            worker_roles: Vec::new(),
            max_concurrent_workers: DEFAULT_MAX_CONCURRENT_WORKERS,
            max_tasks: DEFAULT_MAX_TASKS,
            max_coordinator_rounds: DEFAULT_MAX_COORDINATOR_ROUNDS,
            max_pending_messages: Self::DEFAULT_MAX_PENDING_MESSAGES,
            max_message_bytes: Self::DEFAULT_MAX_MESSAGE_BYTES,
            primary_role: None,
            reviewer_role: None,
        }
    }
}

impl CollaborationRuntimeConfig {
    pub const DEFAULT_MAX_PENDING_MESSAGES: u32 = 64;
    pub const DEFAULT_MAX_MESSAGE_BYTES: u32 = 65_536;

    pub(crate) const fn default_max_pending_messages() -> u32 {
        Self::DEFAULT_MAX_PENDING_MESSAGES
    }

    pub(crate) const fn default_max_message_bytes() -> u32 {
        Self::DEFAULT_MAX_MESSAGE_BYTES
    }

    pub(crate) const fn is_default_max_pending_messages(value: &u32) -> bool {
        *value == Self::DEFAULT_MAX_PENDING_MESSAGES
    }

    pub(crate) const fn is_default_max_message_bytes(value: &u32) -> bool {
        *value == Self::DEFAULT_MAX_MESSAGE_BYTES
    }
}

fn default_max_concurrent_workers() -> u32 {
    DEFAULT_MAX_CONCURRENT_WORKERS
}

fn default_max_tasks() -> u32 {
    DEFAULT_MAX_TASKS
}

fn default_max_coordinator_rounds() -> u32 {
    DEFAULT_MAX_COORDINATOR_ROUNDS
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", content = "strategy", rename_all = "snake_case")]
pub enum CollaborationSelection {
    Auto,
    Fixed(CollaborationStrategy),
    Configured(CollaborationRuntimeConfig),
    #[default]
    Inherit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkflowRequirement {
    Optional,
    Required { workflow_id: String },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelPolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    #[serde(default)]
    pub max_attempts: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputBinding {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowNode {
    pub id: String,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub when: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(default)]
    pub collaboration: CollaborationSelection,
    #[serde(default)]
    pub model_policy: ModelPolicy,
    #[serde(default)]
    pub capability_scope: Vec<String>,
    #[serde(default)]
    pub permission_ceiling: PermissionCeiling,
    #[serde(default)]
    pub retry: RetryPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub output_bindings: Vec<OutputBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowDefinition {
    pub id: String,
    #[serde(default = "workflow_schema_v1")]
    pub schema_version: u32,
    pub version: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub roles: Vec<AgentRoleDefinition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters_schema: Option<Value>,
    pub nodes: Vec<WorkflowNode>,
    #[serde(default)]
    pub outputs: BTreeMap<String, String>,
}

fn workflow_schema_v1() -> u32 {
    1
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowNodeStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Skipped,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowTaskHandle {
    pub task_id: TaskId,
    pub operation_id: OperationId,
    pub node_id: String,
    pub state: WorkflowNodeStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentRoleDefinition {
    pub id: String,
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_schema: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<Value>,
    #[serde(default)]
    pub model_policy: ModelPolicy,
    #[serde(default)]
    pub capability_scope: Vec<String>,
    #[serde(default)]
    pub permission_ceiling: PermissionCeiling,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_policy: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recursion_policy: Option<String>,
    #[serde(default)]
    pub budget: ResourceBudget,
}

/// One task accepted by the Spawn v2 tool.
///
/// The legacy `{name, prompt}` shape is normalized into this type with a
/// generated stable ID before execution. v2 callers should always provide
/// `id`, which makes retries and recovery deterministic.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollaborationTaskInput {
    #[serde(default)]
    pub id: Option<String>,
    pub name: String,
    pub prompt: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_output: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_budget: Option<ResourceBudget>,
}

/// Terminal state of a Spawn v2 collaboration run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CollaborationRunStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
    OutcomeUnknown,
}

/// Per-task result included in a collaboration summary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollaborationTaskSummary {
    pub id: String,
    pub name: String,
    pub status: TaskState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_kind: Option<TaskFailureClass>,
    #[serde(default)]
    pub retries: u32,
    #[serde(default)]
    pub duration_ms: u64,
    #[serde(default)]
    pub usage: TokenUsage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Typed, durable summary returned by Spawn v2 and host integrations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollaborationRunSummary {
    pub status: CollaborationRunStatus,
    #[serde(default)]
    pub summary: String,
    #[serde(default)]
    pub next_actions: Vec<String>,
    #[serde(default)]
    pub artifacts: Vec<String>,
    #[serde(default)]
    pub tasks: Vec<CollaborationTaskSummary>,
    #[serde(default)]
    pub created: u32,
    #[serde(default)]
    pub reattached: u32,
    #[serde(default)]
    pub queued: u32,
    #[serde(default)]
    pub peak_active: u32,
    #[serde(default)]
    pub uncached_input_tokens: u64,
    #[serde(default)]
    pub cache_read_tokens: u64,
    #[serde(default)]
    pub cache_write_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
    #[serde(default)]
    pub tool_calls: u64,
    #[serde(default)]
    pub useful_call_rate: Option<f64>,
    #[serde(default)]
    pub duplicate_call_rate: Option<f64>,
    #[serde(default)]
    pub outcome_unknown: bool,
    #[serde(default)]
    pub needs_manual_verification: Vec<String>,
}

impl CollaborationRunSummary {
    pub fn queued(tasks: impl IntoIterator<Item = CollaborationTaskSummary>) -> Self {
        let tasks: Vec<_> = tasks.into_iter().collect();
        Self {
            status: CollaborationRunStatus::Queued,
            queued: tasks.len() as u32,
            tasks,
            ..Self::default()
        }
    }
}

impl Default for CollaborationRunSummary {
    fn default() -> Self {
        Self {
            status: CollaborationRunStatus::Running,
            summary: String::new(),
            next_actions: Vec::new(),
            artifacts: Vec::new(),
            tasks: Vec::new(),
            created: 0,
            reattached: 0,
            queued: 0,
            peak_active: 0,
            uncached_input_tokens: 0,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
            output_tokens: 0,
            tool_calls: 0,
            useful_call_rate: None,
            duplicate_call_rate: None,
            outcome_unknown: false,
            needs_manual_verification: Vec::new(),
        }
    }
}

#[cfg(test)]
#[path = "workflow_test.rs"]
mod workflow_test;
