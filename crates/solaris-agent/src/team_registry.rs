use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use serde::{Deserialize, Serialize};
use solaris_types::identity::{AgentId, RunId, TeamId};
use solaris_types::workflow::{CollaborationRuntimeConfig, CollaborationStrategy};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamRecord {
    pub run_id: RunId,
    pub team_id: TeamId,
    pub name: String,
    pub strategy: CollaborationStrategy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coordinator: Option<AgentId>,
    #[serde(default)]
    pub direct_peer_messaging: bool,
    #[serde(default = "default_max_pending_messages")]
    pub max_pending_messages: u32,
    #[serde(default = "default_max_message_bytes")]
    pub max_message_bytes: u32,
    #[serde(default)]
    pub members: HashSet<AgentId>,
}

fn default_max_pending_messages() -> u32 {
    CollaborationRuntimeConfig::DEFAULT_MAX_PENDING_MESSAGES
}

fn default_max_message_bytes() -> u32 {
    CollaborationRuntimeConfig::DEFAULT_MAX_MESSAGE_BYTES
}

impl TeamRecord {
    pub(crate) fn validate_message_limits(&self) -> std::io::Result<()> {
        if self.max_pending_messages == 0
            || self.max_pending_messages > CollaborationRuntimeConfig::DEFAULT_MAX_PENDING_MESSAGES
        {
            return Err(std::io::Error::other(format!(
                "team {} max_pending_messages must be between 1 and {}",
                self.team_id,
                CollaborationRuntimeConfig::DEFAULT_MAX_PENDING_MESSAGES
            )));
        }
        if self.max_message_bytes == 0 || self.max_message_bytes > CollaborationRuntimeConfig::DEFAULT_MAX_MESSAGE_BYTES
        {
            return Err(std::io::Error::other(format!(
                "team {} max_message_bytes must be between 1 and {}",
                self.team_id,
                CollaborationRuntimeConfig::DEFAULT_MAX_MESSAGE_BYTES
            )));
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct TeamRegistry {
    teams: RwLock<HashMap<TeamId, TeamRecord>>,
}

impl TeamRegistry {
    pub fn create(&self, record: TeamRecord) -> bool {
        let mut teams = self.teams.write().unwrap_or_else(|error| error.into_inner());
        if teams.contains_key(&record.team_id) {
            return false;
        }
        teams.insert(record.team_id.clone(), record);
        true
    }

    pub fn join(&self, team_id: &TeamId, agent_id: AgentId) -> bool {
        self.teams
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .get_mut(team_id)
            .is_some_and(|team| team.members.insert(agent_id))
    }

    pub fn leave(&self, team_id: &TeamId, agent_id: &AgentId) -> bool {
        self.teams
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .get_mut(team_id)
            .is_some_and(|team| team.members.remove(agent_id))
    }

    pub fn get(&self, team_id: &TeamId) -> Option<TeamRecord> {
        self.teams
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(team_id)
            .cloned()
    }

    pub fn snapshot(&self) -> Vec<TeamRecord> {
        self.teams
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .cloned()
            .collect()
    }
}
