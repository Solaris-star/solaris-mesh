use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{AgentId, AgentIdentity, TeamId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LinkKind {
    Directed,
    Peer,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentLink {
    pub from: AgentId,
    pub to: AgentId,
    pub kind: LinkKind,
}

impl AgentLink {
    pub fn directed(from: AgentId, to: AgentId) -> Self {
        Self {
            from,
            to,
            kind: LinkKind::Directed,
        }
    }

    pub fn peer(left: AgentId, right: AgentId) -> Self {
        Self {
            from: left,
            to: right,
            kind: LinkKind::Peer,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TopologyError {
    #[error("team topology must contain at least one agent")]
    Empty,
    #[error("duplicate agent id: {0}")]
    DuplicateAgent(AgentId),
    #[error("link references unknown agent: {0}")]
    UnknownAgent(AgentId),
    #[error("agent link must not reference the same endpoint twice: {0}")]
    SelfLink(AgentId),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamTopology {
    pub team_id: TeamId,
    agents: Vec<AgentIdentity>,
    links: Vec<AgentLink>,
}

impl TeamTopology {
    pub fn new(team_id: TeamId, agents: Vec<AgentIdentity>, links: Vec<AgentLink>) -> Result<Self, TopologyError> {
        if agents.is_empty() {
            return Err(TopologyError::Empty);
        }

        let mut agent_ids = BTreeSet::new();
        for agent in &agents {
            if !agent_ids.insert(agent.id.clone()) {
                return Err(TopologyError::DuplicateAgent(agent.id.clone()));
            }
        }
        for link in &links {
            if link.from == link.to {
                return Err(TopologyError::SelfLink(link.from.clone()));
            }
            for endpoint in [&link.from, &link.to] {
                if !agent_ids.contains(endpoint) {
                    return Err(TopologyError::UnknownAgent(endpoint.clone()));
                }
            }
        }

        Ok(Self { team_id, agents, links })
    }

    pub fn agents(&self) -> &[AgentIdentity] {
        &self.agents
    }

    pub fn links(&self) -> &[AgentLink] {
        &self.links
    }

    pub fn agent(&self, id: &AgentId) -> Option<&AgentIdentity> {
        self.agents.iter().find(|agent| &agent.id == id)
    }
}
