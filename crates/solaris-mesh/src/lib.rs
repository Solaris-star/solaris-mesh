//! Host-neutral contracts for multi-agent collaboration runtimes.

mod identity;
mod mailbox;
mod runtime;
mod task_graph;
mod topology;

pub use identity::{
    AgentId, AgentIdentity, AgentRole, IdentifierError, MessageId, NodeId, TaskId, TeamId, WorkspaceId,
};
pub use mailbox::{MailboxMessage, MailboxMessageError, MessageKind, MessageTarget};
pub use runtime::{AgentStatus, Runtime, RuntimeCommand, RuntimeError, RuntimeEvent, TaskStatus};
pub use task_graph::{TaskGraph, TaskGraphError, TaskSpec};
pub use topology::{AgentLink, LinkKind, TeamTopology, TopologyError};
