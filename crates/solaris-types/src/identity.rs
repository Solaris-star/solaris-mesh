use std::fmt;

use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

macro_rules! string_id {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_owned())
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

string_id!(RunId);
string_id!(TeamId);
string_id!(AgentId);
string_id!(TaskId);
string_id!(OperationId);
string_id!(AttemptId);
string_id!(EffectId);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChildAgentKey {
    pub run_id: RunId,
    pub parent_agent_id: AgentId,
    pub role_key: String,
    pub stable_task_key: String,
    pub spawn_operation_id: OperationId,
}

impl PartialEq for ChildAgentKey {
    fn eq(&self, other: &Self) -> bool {
        self.run_id == other.run_id
            && self.parent_agent_id == other.parent_agent_id
            && self.role_key == other.role_key
            && self.stable_task_key == other.stable_task_key
    }
}

impl Eq for ChildAgentKey {}

impl Hash for ChildAgentKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.run_id.hash(state);
        self.parent_agent_id.hash(state);
        self.role_key.hash(state);
        self.stable_task_key.hash(state);
    }
}

impl ChildAgentKey {
    pub const CURRENT_IDENTITY_VERSION: u8 = 2;
    pub const LEGACY_IDENTITY_VERSION: u8 = 1;

    /// Return the versioned, unambiguous encoding of the logical child identity.
    pub fn stable_string(&self) -> String {
        let mut encoded = String::from("v2");
        for value in [
            self.run_id.as_str(),
            self.parent_agent_id.as_str(),
            self.role_key.as_str(),
            self.stable_task_key.as_str(),
        ] {
            encoded.push(':');
            encoded.push_str(&value.len().to_string());
            encoded.push(':');
            encoded.push_str(value);
        }
        encoded
    }

    /// Return the digest shared by the public Agent id and the session directory.
    pub fn stable_digest(&self) -> String {
        format!("{:x}", Sha256::digest(self.stable_string().as_bytes()))
    }

    pub fn agent_id(&self) -> AgentId {
        AgentId::new(format!("child:v2:{}", self.stable_digest()))
    }

    pub fn legacy_agent_id(&self) -> AgentId {
        AgentId::new(format!("child:{}", self.legacy_stable_string()))
    }

    pub fn identity_version_for_agent_id(&self, agent_id: &AgentId) -> Option<u8> {
        if agent_id == &self.agent_id() {
            Some(Self::CURRENT_IDENTITY_VERSION)
        } else if agent_id == &self.legacy_agent_id() {
            Some(Self::LEGACY_IDENTITY_VERSION)
        } else {
            None
        }
    }

    /// Return a deterministic identifier that is safe to use as a session directory name.
    pub fn session_id(&self) -> String {
        format!("mesh-child-v2-{}", self.stable_digest())
    }

    pub fn legacy_session_id(&self) -> String {
        const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
        const FNV_PRIME: u64 = 0x100000001b3;

        let hash = self
            .legacy_stable_string()
            .bytes()
            .fold(FNV_OFFSET_BASIS, |hash, byte| {
                (hash ^ u64::from(byte)).wrapping_mul(FNV_PRIME)
            });
        format!("mesh-child-{hash:016x}")
    }

    pub fn session_id_for_agent_id(&self, agent_id: &AgentId) -> Option<String> {
        match self.identity_version_for_agent_id(agent_id) {
            Some(Self::CURRENT_IDENTITY_VERSION) => Some(self.session_id()),
            Some(Self::LEGACY_IDENTITY_VERSION) => Some(self.legacy_session_id()),
            _ => None,
        }
    }

    fn legacy_stable_string(&self) -> String {
        format!(
            "{}:{}:{}:{}",
            self.run_id, self.parent_agent_id, self.role_key, self.stable_task_key
        )
    }
}

#[cfg(test)]
#[path = "identity_test.rs"]
mod identity_test;
