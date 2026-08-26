use std::collections::HashMap;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};
use solaris_types::identity::{AgentId, ChildAgentKey, OperationId, RunId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRelationship {
    pub run_id: RunId,
    pub parent_agent_id: AgentId,
    pub child_agent_id: AgentId,
    #[serde(default)]
    pub child_identity_version: u8,
    #[serde(default)]
    pub role_key: String,
    #[serde(default)]
    pub stable_task_key: String,
    pub spawn_operation_id: OperationId,
}

pub trait AgentRelationshipStore: Send + Sync {
    fn put(&self, relationship: AgentRelationship);
    fn relationship_for_key(&self, key: &ChildAgentKey) -> Option<AgentRelationship>;
    fn child_for_key(&self, key: &ChildAgentKey) -> Option<AgentId>;
    fn children_of(&self, parent_agent_id: &AgentId) -> Vec<AgentRelationship>;
}

#[derive(Default)]
pub struct InMemoryAgentRelationshipStore {
    by_key: RwLock<HashMap<ChildAgentKey, AgentRelationship>>,
}

impl InMemoryAgentRelationshipStore {
    pub fn snapshot(&self) -> Vec<AgentRelationship> {
        self.by_key
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .cloned()
            .collect()
    }
}

impl AgentRelationshipStore for InMemoryAgentRelationshipStore {
    fn put(&self, relationship: AgentRelationship) {
        let key = ChildAgentKey {
            run_id: relationship.run_id.clone(),
            parent_agent_id: relationship.parent_agent_id.clone(),
            role_key: relationship.role_key.clone(),
            stable_task_key: relationship.stable_task_key.clone(),
            spawn_operation_id: relationship.spawn_operation_id.clone(),
        };
        self.by_key
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .insert(key, relationship);
    }

    fn child_for_key(&self, key: &ChildAgentKey) -> Option<AgentId> {
        self.relationship_for_key(key)
            .map(|relationship| relationship.child_agent_id)
    }

    fn relationship_for_key(&self, key: &ChildAgentKey) -> Option<AgentRelationship> {
        self.by_key
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(key)
            .cloned()
    }

    fn children_of(&self, parent_agent_id: &AgentId) -> Vec<AgentRelationship> {
        self.by_key
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .filter(|relationship| &relationship.parent_agent_id == parent_agent_id)
            .cloned()
            .collect()
    }
}
