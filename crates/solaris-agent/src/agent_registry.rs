use std::collections::HashMap;
use std::sync::RwLock;

use solaris_types::identity::AgentId;
use solaris_types::runtime::{AgentLifecycleState, AgentRecord};

#[derive(Default)]
pub struct AgentRegistry {
    agents: RwLock<HashMap<AgentId, AgentRecord>>,
}

impl AgentRegistry {
    pub fn upsert(&self, record: AgentRecord) {
        self.agents
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .insert(record.agent_id.clone(), record);
    }

    pub fn get(&self, agent_id: &AgentId) -> Option<AgentRecord> {
        self.agents
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(agent_id)
            .cloned()
    }

    pub fn set_team(&self, agent_id: &AgentId, team_id: Option<solaris_types::identity::TeamId>) -> bool {
        let mut agents = self.agents.write().unwrap_or_else(|error| error.into_inner());
        let Some(record) = agents.get_mut(agent_id) else {
            return false;
        };
        record.team_id = team_id;
        true
    }

    pub fn set_state(&self, agent_id: &AgentId, state: AgentLifecycleState) -> bool {
        let mut agents = self.agents.write().unwrap_or_else(|error| error.into_inner());
        let Some(record) = agents.get_mut(agent_id) else {
            return false;
        };
        record.state = state;
        true
    }

    pub fn remove(&self, agent_id: &AgentId) -> Option<AgentRecord> {
        self.agents
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .remove(agent_id)
    }

    pub fn snapshot(&self) -> Vec<AgentRecord> {
        self.agents
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .cloned()
            .collect()
    }
}
