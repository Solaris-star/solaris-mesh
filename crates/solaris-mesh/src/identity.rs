use std::collections::BTreeSet;
use std::fmt::{self, Display};
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{kind} must not be empty")]
pub struct IdentifierError {
    kind: &'static str,
}

macro_rules! identifier {
    ($name:ident, $kind:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, IdentifierError> {
                let value = value.into();
                if value.trim().is_empty() {
                    return Err(IdentifierError { kind: $kind });
                }
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = IdentifierError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }
    };
}

identifier!(AgentId, "agent id");
identifier!(MessageId, "message id");
identifier!(NodeId, "node id");
identifier!(TaskId, "task id");
identifier!(TeamId, "team id");
identifier!(WorkspaceId, "workspace id");

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentRole {
    Supervisor,
    Worker,
    Peer,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentIdentity {
    pub id: AgentId,
    pub role: AgentRole,
    pub display_name: Option<String>,
    pub capabilities: BTreeSet<String>,
    pub node_id: Option<NodeId>,
}

impl AgentIdentity {
    pub fn new(id: AgentId, role: AgentRole) -> Self {
        Self {
            id,
            role,
            display_name: None,
            capabilities: BTreeSet::new(),
            node_id: None,
        }
    }

    pub fn supports(&self, capability: &str) -> bool {
        self.capabilities.contains(capability)
    }
}
