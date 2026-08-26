use std::collections::HashMap;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use solaris_types::identity::{AgentId, RunId, TeamId};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TeamFact {
    pub run_id: RunId,
    pub team_id: TeamId,
    pub key: String,
    pub value: Value,
    pub updated_by: AgentId,
    pub updated_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactRef {
    pub artifact_id: String,
    pub run_id: RunId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_id: Option<TeamId>,
    pub agent_id: AgentId,
    pub uri: String,
    pub kind: String,
    #[serde(default)]
    pub metadata: Value,
    pub created_at_unix_ms: i64,
}

#[derive(Default)]
pub struct TeamStateStore {
    facts: RwLock<HashMap<(TeamId, String), TeamFact>>,
    artifacts: RwLock<HashMap<String, ArtifactRef>>,
}

impl TeamStateStore {
    pub fn set_fact(&self, fact: TeamFact) -> Option<TeamFact> {
        self.facts
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .insert((fact.team_id.clone(), fact.key.clone()), fact)
    }

    pub fn facts(&self, team_id: &TeamId) -> Vec<TeamFact> {
        let mut values: Vec<_> = self
            .facts
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .filter(|fact| &fact.team_id == team_id)
            .cloned()
            .collect();
        values.sort_by(|left, right| left.key.cmp(&right.key));
        values
    }

    pub fn put_artifact(&self, artifact: ArtifactRef) {
        self.artifacts
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .insert(artifact.artifact_id.clone(), artifact);
    }

    pub fn register_artifact(
        &self,
        run_id: RunId,
        team_id: Option<TeamId>,
        agent_id: AgentId,
        uri: String,
        kind: String,
        metadata: Value,
    ) -> ArtifactRef {
        let artifact = ArtifactRef {
            artifact_id: format!("artifact-{}", Uuid::now_v7()),
            run_id,
            team_id,
            agent_id,
            uri,
            kind,
            metadata,
            created_at_unix_ms: chrono::Utc::now().timestamp_millis(),
        };
        self.artifacts
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .insert(artifact.artifact_id.clone(), artifact.clone());
        artifact
    }

    pub fn artifacts(&self, team_id: Option<&TeamId>, agent_id: Option<&AgentId>) -> Vec<ArtifactRef> {
        let mut values: Vec<_> = self
            .artifacts
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .filter(|artifact| team_id.is_none_or(|team| artifact.team_id.as_ref() == Some(team)))
            .filter(|artifact| agent_id.is_none_or(|agent| &artifact.agent_id == agent))
            .cloned()
            .collect();
        values.sort_by_key(|artifact| (artifact.created_at_unix_ms, artifact.artifact_id.clone()));
        values
    }

    pub fn all_facts(&self) -> Vec<TeamFact> {
        let mut values: Vec<_> = self
            .facts
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .cloned()
            .collect();
        values.sort_by(|left, right| {
            left.team_id
                .as_str()
                .cmp(right.team_id.as_str())
                .then_with(|| left.key.cmp(&right.key))
        });
        values
    }

    pub fn all_artifacts(&self) -> Vec<ArtifactRef> {
        self.artifacts(None, None)
    }
}

#[cfg(test)]
#[path = "team_state_test.rs"]
mod team_state_test;
