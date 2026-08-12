use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{AgentId, MessageId, TeamId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageKind {
    Task,
    Coordination,
    Status,
    Control,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "agent_id", rename_all = "snake_case")]
pub enum MessageTarget {
    Agent(AgentId),
    Broadcast,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MailboxMessageError {
    #[error("mailbox message content must not be empty")]
    EmptyContent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MailboxMessage {
    pub id: MessageId,
    pub team_id: TeamId,
    pub from: AgentId,
    pub to: MessageTarget,
    pub kind: MessageKind,
    pub content: String,
    pub created_at_ms: i64,
    pub reply_to: Option<MessageId>,
}

impl MailboxMessage {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: MessageId,
        team_id: TeamId,
        from: AgentId,
        to: MessageTarget,
        kind: MessageKind,
        content: impl Into<String>,
        created_at_ms: i64,
        reply_to: Option<MessageId>,
    ) -> Result<Self, MailboxMessageError> {
        let content = content.into();
        if content.trim().is_empty() {
            return Err(MailboxMessageError::EmptyContent);
        }
        Ok(Self {
            id,
            team_id,
            from,
            to,
            kind,
            content,
            created_at_ms,
            reply_to,
        })
    }
}
