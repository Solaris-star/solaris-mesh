use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{AgentId, MailboxMessage, TaskId, TaskSpec, TeamId, TeamTopology, WorkspaceId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentStatus {
    Starting,
    Ready,
    Busy,
    Recovering,
    Stopped,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Assigned,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RuntimeCommand {
    StartTeam {
        topology: TeamTopology,
        workspace_id: Option<WorkspaceId>,
    },
    SubmitTask {
        team_id: TeamId,
        task: TaskSpec,
    },
    DelegateTask {
        team_id: TeamId,
        task_id: TaskId,
        from: AgentId,
        to: AgentId,
    },
    SendMessage {
        message: MailboxMessage,
    },
    RecoverAgent {
        team_id: TeamId,
        agent_id: AgentId,
    },
    StopTeam {
        team_id: TeamId,
        reason: Option<String>,
    },
}

impl RuntimeCommand {
    pub fn team_id(&self) -> &TeamId {
        match self {
            Self::StartTeam { topology, .. } => &topology.team_id,
            Self::SubmitTask { team_id, .. }
            | Self::DelegateTask { team_id, .. }
            | Self::RecoverAgent { team_id, .. }
            | Self::StopTeam { team_id, .. } => team_id,
            Self::SendMessage { message } => &message.team_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RuntimeEvent {
    TeamStarted {
        team_id: TeamId,
    },
    AgentStatusChanged {
        team_id: TeamId,
        agent_id: AgentId,
        status: AgentStatus,
    },
    TaskStatusChanged {
        team_id: TeamId,
        task_id: TaskId,
        status: TaskStatus,
    },
    TaskDelegated {
        team_id: TeamId,
        task_id: TaskId,
        from: AgentId,
        to: AgentId,
    },
    MessageQueued {
        team_id: TeamId,
        message_id: crate::MessageId,
    },
    RecoveryStarted {
        team_id: TeamId,
        agent_id: AgentId,
    },
    FailureObserved {
        team_id: TeamId,
        agent_id: Option<AgentId>,
        code: String,
        retryable: bool,
    },
    TeamStopped {
        team_id: TeamId,
    },
}

impl RuntimeEvent {
    pub fn team_id(&self) -> &TeamId {
        match self {
            Self::TeamStarted { team_id }
            | Self::AgentStatusChanged { team_id, .. }
            | Self::TaskStatusChanged { team_id, .. }
            | Self::TaskDelegated { team_id, .. }
            | Self::MessageQueued { team_id, .. }
            | Self::RecoveryStarted { team_id, .. }
            | Self::FailureObserved { team_id, .. }
            | Self::TeamStopped { team_id } => team_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RuntimeError {
    #[error("runtime command is invalid: {0}")]
    InvalidCommand(String),
    #[error("runtime resource was not found: {0}")]
    NotFound(String),
    #[error("runtime state conflict: {0}")]
    Conflict(String),
    #[error("runtime is unavailable: {0}")]
    Unavailable(String),
    #[error("runtime operation failed: {0}")]
    Internal(String),
}

#[async_trait]
pub trait Runtime: Send + Sync {
    async fn execute(&self, command: RuntimeCommand) -> Result<Vec<RuntimeEvent>, RuntimeError>;
}
