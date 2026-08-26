use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};
use solaris_protocol::events::ToolCategory;
use solaris_tools::Tool;
use solaris_types::effect::{EffectClass, EffectDescriptor, EffectReplayPolicy, ResourceFootprint};
use solaris_types::identity::{AgentId, RunId, TeamId};
use solaris_types::tool::{JsonSchema, ToolResult};

use crate::collaboration_runtime::CollaborationRuntime;

pub struct RegisterArtifactTool {
    runtime: Arc<CollaborationRuntime<()>>,
    run_id: RunId,
    agent_id: AgentId,
}

impl RegisterArtifactTool {
    pub fn new(runtime: Arc<CollaborationRuntime<()>>, run_id: RunId, agent_id: AgentId) -> Self {
        Self {
            runtime,
            run_id,
            agent_id,
        }
    }
}

#[async_trait]
impl Tool for RegisterArtifactTool {
    fn name(&self) -> &str {
        "RegisterArtifact"
    }

    fn description(&self) -> &str {
        "Register a durable reference to an artifact produced by this Agent. This records Mesh state only; it does not write or upload the artifact itself."
    }

    fn input_schema(&self) -> JsonSchema {
        json!({
            "type": "object",
            "properties": {
                "team_id": {"type": "string"},
                "uri": {"type": "string"},
                "kind": {"type": "string"},
                "metadata": {}
            },
            "required": ["uri", "kind"]
        })
    }

    fn is_concurrency_safe(&self, _input: &Value) -> bool {
        false
    }

    async fn execute(&self, input: Value) -> ToolResult {
        let Some(uri) = input.get("uri").and_then(Value::as_str) else {
            return ToolResult {
                content: "Missing required parameter: uri".into(),
                is_error: true,
            };
        };
        let Some(kind) = input.get("kind").and_then(Value::as_str) else {
            return ToolResult {
                content: "Missing required parameter: kind".into(),
                is_error: true,
            };
        };
        let team_id = input
            .get("team_id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(TeamId::from);
        let metadata = input.get("metadata").cloned().unwrap_or_else(|| json!({}));
        match self
            .runtime
            .register_artifact(&self.run_id, team_id, &self.agent_id, uri, kind, metadata)
        {
            Ok(artifact) => ToolResult {
                content: serde_json::to_string(&artifact).unwrap_or_else(|_| "artifact registered".into()),
                is_error: false,
            },
            Err(error) => ToolResult {
                content: error.to_string(),
                is_error: true,
            },
        }
    }

    fn describe_effect(&self, input: &Value) -> EffectDescriptor {
        let uri = input.get("uri").and_then(Value::as_str).unwrap_or("unknown");
        EffectDescriptor {
            class: EffectClass::MeshStateMutation,
            action: format!("Register Mesh artifact reference {uri}"),
            resources: ResourceFootprint {
                mesh_resources: vec![format!("mesh:artifact:{uri}")],
                ..Default::default()
            },
            replay_policy: EffectReplayPolicy::Idempotent,
        }
    }

    fn category(&self) -> ToolCategory {
        ToolCategory::Info
    }
}

pub struct ListArtifactsTool {
    runtime: Arc<CollaborationRuntime<()>>,
    agent_id: AgentId,
}

impl ListArtifactsTool {
    pub fn new(runtime: Arc<CollaborationRuntime<()>>, agent_id: AgentId) -> Self {
        Self { runtime, agent_id }
    }
}

#[async_trait]
impl Tool for ListArtifactsTool {
    fn name(&self) -> &str {
        "ListArtifacts"
    }

    fn description(&self) -> &str {
        "List artifact references for this Agent, or for a Team that this Agent belongs to."
    }

    fn input_schema(&self) -> JsonSchema {
        json!({
            "type": "object",
            "properties": {"team_id": {"type": "string"}}
        })
    }

    fn is_concurrency_safe(&self, _input: &Value) -> bool {
        true
    }

    async fn execute(&self, input: Value) -> ToolResult {
        let values = if let Some(team_id) = input
            .get("team_id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
        {
            let team_id = TeamId::from(team_id);
            let Some(team) = self.runtime.teams().get(&team_id) else {
                return ToolResult {
                    content: format!("Unknown team: {team_id}"),
                    is_error: true,
                };
            };
            if !team.members.contains(&self.agent_id) {
                return ToolResult {
                    content: "Agent does not belong to the requested team".into(),
                    is_error: true,
                };
            }
            self.runtime.artifacts(Some(&team_id), None)
        } else {
            self.runtime.artifacts(None, Some(&self.agent_id))
        };
        ToolResult {
            content: serde_json::to_string(&values).unwrap_or_else(|_| "[]".into()),
            is_error: false,
        }
    }

    fn describe_effect(&self, _input: &Value) -> EffectDescriptor {
        EffectDescriptor::read_only(format!("List Mesh artifacts for {}", self.agent_id))
    }

    fn category(&self) -> ToolCategory {
        ToolCategory::Info
    }
}
