use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::identity::{AgentId, OperationId, RunId, TaskId, TeamId};
use crate::message::TokenUsage;
use crate::permission::{ExecutionBoundary, PermissionCeiling};
use crate::plugin::ImplementationIdentity;
use crate::resource::ResourceBudget;
use crate::runtime::TaskFailureClass;
use crate::workflow::{CollaborationRuntimeConfig, CollaborationStrategy};

/// Configuration for a sub-agent invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubAgentConfig {
    /// Descriptive name for logging
    pub name: String,
    /// The task prompt
    pub prompt: String,
    /// Max turns for this sub-agent (typically lower than main agent)
    pub max_turns: usize,
    /// Max output tokens per response
    pub max_tokens: u32,
    /// Optional system prompt override
    pub system_prompt: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentCollaborationContext {
    pub team_id: TeamId,
    pub strategy: CollaborationStrategy,
    pub coordinator_agent_id: AgentId,
    #[serde(
        default = "CollaborationRuntimeConfig::default_max_pending_messages",
        skip_serializing_if = "CollaborationRuntimeConfig::is_default_max_pending_messages"
    )]
    pub max_pending_messages: u32,
    #[serde(
        default = "CollaborationRuntimeConfig::default_max_message_bytes",
        skip_serializing_if = "CollaborationRuntimeConfig::is_default_max_message_bytes"
    )]
    pub max_message_bytes: u32,
}

/// Overrides applied when spawning a fork-mode skill sub-agent.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ForkOverrides {
    /// Replace the parent's configured model with this one.
    pub model: Option<String>,
    /// Reasoning effort ("low"/"medium"/"high"/"max").
    pub effort: Option<String>,
    /// Restrict inherited capabilities to this list. An empty list denies every capability.
    pub allowed_tools: Vec<String>,
    /// Explicitly inherit the parent Run capability blueprint instead of using `allowed_tools`.
    #[serde(default)]
    pub inherit_capabilities: bool,
    /// Optional Mesh collaboration membership assigned before the child starts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collaboration: Option<AgentCollaborationContext>,
    /// Workflow definition pinned for this child operation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow: Option<ImplementationIdentity>,
    /// Optional hard execution boundary that can only narrow the parent's boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_boundary: Option<ExecutionBoundary>,
}

/// Canonical input for one logical child Agent operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentSpawnSpec {
    pub run_id: RunId,
    pub parent_agent_id: AgentId,
    pub task_id: TaskId,
    pub role_key: String,
    pub stable_task_key: String,
    pub operation_id: OperationId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_task_revision: Option<u64>,
    pub config: SubAgentConfig,
    #[serde(default)]
    pub overrides: ForkOverrides,
    #[serde(default)]
    pub permission_ceiling: PermissionCeiling,
    #[serde(default)]
    pub resource_budget: ResourceBudget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_policy: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recursion_limit: Option<usize>,
}

/// Immutable configuration for a continuable child Agent.
///
/// Unlike `SubAgentConfig`, this configuration intentionally has no prompt.
/// Prompts belong to individually durable `AgentTurnSpec` values.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConversationConfig {
    pub name: String,
    pub max_turns: usize,
    pub max_tokens: u32,
    pub system_prompt: Option<String>,
}

/// Canonical input for opening one durable, multi-turn child Agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConversationSpec {
    pub run_id: RunId,
    pub parent_agent_id: AgentId,
    pub task_id: TaskId,
    pub conversation_id: String,
    pub role_key: String,
    pub stable_task_key: String,
    pub operation_id: OperationId,
    pub config: AgentConversationConfig,
    #[serde(default)]
    pub overrides: ForkOverrides,
    #[serde(default)]
    pub permission_ceiling: PermissionCeiling,
    #[serde(default)]
    pub resource_budget: ResourceBudget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_policy: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recursion_limit: Option<usize>,
}

/// Stable reference to a durable, cold-resumable child Agent conversation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConversationHandle {
    pub schema_version: u8,
    pub conversation_id: String,
    pub run_id: RunId,
    pub parent_agent_id: AgentId,
    pub agent_id: AgentId,
    pub identity_version: u8,
    pub session_id: String,
    pub task_id: TaskId,
    pub operation_id: OperationId,
    pub spec_digest: String,
    pub environment_digest: String,
    pub effective_permission_ceiling: PermissionCeiling,
    pub spec: AgentConversationSpec,
}

/// Canonical input for one FIFO turn in a durable Agent conversation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentTurnSpec {
    pub turn_id: String,
    pub prompt: String,
}

/// Stable identities derived from a conversation and caller-owned turn key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTurnIdentity {
    pub operation_id: OperationId,
    pub message_id: String,
    pub input_digest: String,
}

impl AgentConversationHandle {
    pub fn turn_identity(&self, turn: &AgentTurnSpec) -> AgentTurnIdentity {
        let identity_digest = length_prefixed_digest(&[
            b"solaris.agent-conversation.turn.v1",
            self.run_id.as_str().as_bytes(),
            self.parent_agent_id.as_str().as_bytes(),
            self.agent_id.as_str().as_bytes(),
            self.session_id.as_bytes(),
            self.operation_id.as_str().as_bytes(),
            self.spec_digest.as_bytes(),
            self.conversation_id.as_bytes(),
            turn.turn_id.as_bytes(),
        ]);
        AgentTurnIdentity {
            operation_id: OperationId::new(format!("conversation-turn:{identity_digest}")),
            message_id: format!("conversation-message:{identity_digest}"),
            input_digest: length_prefixed_digest(&[
                b"solaris.agent-conversation.turn-input.v1",
                identity_digest.as_bytes(),
                turn.prompt.as_bytes(),
            ]),
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test(conversation_id: &str, agent_id: &str, session_id: &str) -> Self {
        let spec = AgentConversationSpec {
            run_id: RunId::from("run"),
            parent_agent_id: AgentId::from("parent"),
            task_id: TaskId::from("task"),
            conversation_id: conversation_id.to_owned(),
            role_key: "worker".to_owned(),
            stable_task_key: conversation_id.to_owned(),
            operation_id: OperationId::from("open"),
            config: AgentConversationConfig {
                name: "worker".to_owned(),
                max_turns: 1,
                max_tokens: 32,
                system_prompt: None,
            },
            overrides: ForkOverrides::default(),
            permission_ceiling: PermissionCeiling::unrestricted(),
            resource_budget: ResourceBudget::default(),
            context_policy: None,
            recursion_limit: None,
        };
        Self {
            schema_version: 1,
            conversation_id: conversation_id.to_owned(),
            run_id: spec.run_id.clone(),
            parent_agent_id: spec.parent_agent_id.clone(),
            agent_id: AgentId::from(agent_id),
            identity_version: 2,
            session_id: session_id.to_owned(),
            task_id: spec.task_id.clone(),
            operation_id: spec.operation_id.clone(),
            spec_digest: "spec".to_owned(),
            environment_digest: "environment".to_owned(),
            effective_permission_ceiling: PermissionCeiling::unrestricted(),
            spec,
        }
    }
}

fn length_prefixed_digest(parts: &[&[u8]]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part);
    }
    format!("{:x}", hasher.finalize())
}

/// Stable reference returned before a child is joined.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentHandle {
    pub run_id: RunId,
    pub agent_id: AgentId,
    #[serde(default)]
    pub identity_version: u8,
    pub task_id: TaskId,
    pub operation_id: OperationId,
    pub role_key: String,
    pub stable_task_key: String,
    pub spec_digest: String,
    /// Keeping the canonical spec in the durable handle makes restart-time join independent of process memory.
    pub spec: AgentSpawnSpec,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentOutcomeStatus {
    Completed,
    #[default]
    Failed,
    Cancelled,
    OutcomeUnknown,
    ReconciliationRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSpawnError {
    pub failure_class: TaskFailureClass,
    pub message: String,
}

/// Typed failure returned by durable Agent conversation lifecycle operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentConversationError {
    pub failure_class: TaskFailureClass,
    pub message: String,
}

impl AgentConversationError {
    pub fn retryable(message: impl Into<String>) -> Self {
        Self {
            failure_class: TaskFailureClass::Retryable,
            message: message.into(),
        }
    }

    pub fn non_retryable(message: impl Into<String>) -> Self {
        Self {
            failure_class: TaskFailureClass::NonRetryable,
            message: message.into(),
        }
    }

    pub fn outcome_unknown(message: impl Into<String>) -> Self {
        Self {
            failure_class: TaskFailureClass::OutcomeUnknown,
            message: message.into(),
        }
    }

    pub fn reconciliation_required(message: impl Into<String>) -> Self {
        Self {
            failure_class: TaskFailureClass::ReconciliationRequired,
            message: message.into(),
        }
    }

    pub fn permission_denied(message: impl Into<String>) -> Self {
        Self {
            failure_class: TaskFailureClass::PermissionDenied,
            message: message.into(),
        }
    }

    pub fn max_turns(message: impl Into<String>) -> Self {
        Self {
            failure_class: TaskFailureClass::MaxTurns,
            message: message.into(),
        }
    }

    pub fn non_convergent(message: impl Into<String>) -> Self {
        Self {
            failure_class: TaskFailureClass::NonConvergent,
            message: message.into(),
        }
    }

    pub fn cancelled(message: impl Into<String>) -> Self {
        Self {
            failure_class: TaskFailureClass::Cancelled,
            message: message.into(),
        }
    }

    pub fn side_effect_unknown(message: impl Into<String>) -> Self {
        Self {
            failure_class: TaskFailureClass::SideEffectUnknown,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for AgentConversationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for AgentConversationError {}

impl AgentSpawnError {
    pub fn retryable(message: impl Into<String>) -> Self {
        Self {
            failure_class: TaskFailureClass::Retryable,
            message: message.into(),
        }
    }

    pub fn non_retryable(message: impl Into<String>) -> Self {
        Self {
            failure_class: TaskFailureClass::NonRetryable,
            message: message.into(),
        }
    }

    pub fn outcome_unknown(message: impl Into<String>) -> Self {
        Self {
            failure_class: TaskFailureClass::OutcomeUnknown,
            message: message.into(),
        }
    }

    pub fn reconciliation_required(message: impl Into<String>) -> Self {
        Self {
            failure_class: TaskFailureClass::ReconciliationRequired,
            message: message.into(),
        }
    }

    pub fn permission_denied(message: impl Into<String>) -> Self {
        Self {
            failure_class: TaskFailureClass::PermissionDenied,
            message: message.into(),
        }
    }

    pub fn max_turns(message: impl Into<String>) -> Self {
        Self {
            failure_class: TaskFailureClass::MaxTurns,
            message: message.into(),
        }
    }

    pub fn non_convergent(message: impl Into<String>) -> Self {
        Self {
            failure_class: TaskFailureClass::NonConvergent,
            message: message.into(),
        }
    }

    pub fn cancelled(message: impl Into<String>) -> Self {
        Self {
            failure_class: TaskFailureClass::Cancelled,
            message: message.into(),
        }
    }

    pub fn side_effect_unknown(message: impl Into<String>) -> Self {
        Self {
            failure_class: TaskFailureClass::SideEffectUnknown,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for AgentSpawnError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for AgentSpawnError {}

/// Typed terminal result for `AgentSpawnService::join`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentOutcome {
    pub handle: AgentHandle,
    pub status: AgentOutcomeStatus,
    /// Typed failure classification for non-completed outcomes.
    /// Retry decisions must use this field, never the bare `status`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_class: Option<TaskFailureClass>,
    pub output: Value,
    pub usage: TokenUsage,
    pub turns: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_class: Option<TaskFailureClass>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Content-addressed reference to one durable Agent outcome body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeBlobRef {
    pub reference: String,
    pub bytes: u64,
    pub digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<RunId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<AgentOutcomeStatus>,
}

/// Describes how a redacted durable turn output is reconstructed from its blob.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "format", rename_all = "snake_case")]
pub enum AgentTurnOutputProjection {
    JsonTextPointer { pointer: String },
}

/// Durable result of one child Agent conversation turn.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentTurnOutcome {
    pub schema_version: u8,
    pub run_id: RunId,
    pub parent_agent_id: AgentId,
    pub conversation_id: String,
    pub task_id: TaskId,
    pub open_operation_id: OperationId,
    pub spec_digest: String,
    pub turn_id: String,
    pub agent_id: AgentId,
    pub session_id: String,
    pub operation_id: OperationId,
    pub message_id: String,
    pub status: AgentOutcomeStatus,
    pub output: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome_ref: Option<OutcomeBlobRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_projection: Option<AgentTurnOutputProjection>,
    pub usage: TokenUsage,
    pub turns: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_class: Option<TaskFailureClass>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Result from a completed sub-agent execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubAgentResult {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<TaskId>,
    #[serde(default)]
    pub status: AgentOutcomeStatus,
    /// Typed failure classification for non-completed outcomes.
    /// Retry decisions must use this field, never the bare `status`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_class: Option<TaskFailureClass>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
    pub text: String,
    pub usage: TokenUsage,
    pub turns: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_class: Option<TaskFailureClass>,
    pub is_error: bool,
}

/// Abstraction over fork-mode agent spawning — enables mock implementations in tests.
#[async_trait]
pub trait Spawner: Send + Sync {
    /// Spawn a fork-mode sub-agent with optional overrides and wait for its result.
    async fn spawn_fork(&self, config: SubAgentConfig, overrides: ForkOverrides) -> SubAgentResult;
}

/// Durable child-Agent lifecycle API used by Workflow and Team runtimes.
#[async_trait]
pub trait AgentSpawnService: Send + Sync {
    async fn spawn(&self, spec: AgentSpawnSpec) -> Result<AgentHandle, AgentSpawnError>;
    async fn join(&self, handle: &AgentHandle) -> Result<AgentOutcome, String>;
    async fn cancel(&self, handle: &AgentHandle) -> Result<(), String>;
}

#[cfg(test)]
#[path = "spawner_test.rs"]
mod spawner_test;
