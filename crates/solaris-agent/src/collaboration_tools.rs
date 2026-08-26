use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};
use solaris_protocol::events::ToolCategory;
use solaris_tools::{PreparedToolExecution, Tool, ToolExecutionContext};
use solaris_types::effect::{EffectClass, EffectDescriptor, EffectReplayPolicy, ResourceFootprint};
use solaris_types::identity::{AgentId, OperationId, RunId, TaskId, TeamId};
use solaris_types::runtime::{TaskRecord, TaskState};
use solaris_types::tool::{JsonSchema, ToolResult};

use crate::collaboration_runtime::CollaborationRuntime;

#[path = "collaboration_tools/artifacts.rs"]
mod artifacts;

pub use artifacts::{ListArtifactsTool, RegisterArtifactTool};

pub struct SendAgentMessageTool {
    runtime: Arc<CollaborationRuntime<()>>,
    run_id: RunId,
    agent_id: AgentId,
}

impl SendAgentMessageTool {
    pub fn new(runtime: Arc<CollaborationRuntime<()>>, run_id: RunId, agent_id: AgentId) -> Self {
        Self {
            runtime,
            run_id,
            agent_id,
        }
    }

    async fn execute_with_message_id(&self, input: Value, message_id: Option<String>) -> ToolResult {
        let Some(to) = input.get("to_agent_id").and_then(Value::as_str) else {
            return ToolResult {
                content: "Missing required parameter: to_agent_id".into(),
                is_error: true,
            };
        };
        let team_id = input
            .get("team_id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(TeamId::from);
        let kind = input.get("kind").and_then(Value::as_str).unwrap_or("peer");
        let body = input.get("body").cloned().unwrap_or(Value::Null);
        let result = if let Some(message_id) = message_id {
            self.runtime.send_message_with_id(
                message_id,
                self.run_id.clone(),
                team_id,
                self.agent_id.clone(),
                AgentId::from(to),
                kind,
                body,
            )
        } else {
            self.runtime.send_message(
                self.run_id.clone(),
                team_id,
                self.agent_id.clone(),
                AgentId::from(to),
                kind,
                body,
            )
        };
        match result {
            Ok(message) => ToolResult {
                content: serde_json::to_string(&message).unwrap_or_else(|_| "message delivered".into()),
                is_error: false,
            },
            Err(error) => ToolResult {
                content: error.to_string(),
                is_error: true,
            },
        }
    }
}

#[async_trait]
impl Tool for SendAgentMessageTool {
    fn name(&self) -> &str {
        "SendAgentMessage"
    }

    fn description(&self) -> &str {
        "Send an internal Solaris Mesh message to another Agent. Supervisor collaboration rejects worker-to-worker messages; Team collaboration permits peer messaging."
    }

    fn input_schema(&self) -> JsonSchema {
        json!({
            "type": "object",
            "properties": {
                "to_agent_id": {"type": "string"},
                "team_id": {"type": "string"},
                "kind": {"type": "string"},
                "body": {}
            },
            "required": ["to_agent_id", "body"]
        })
    }

    fn is_concurrency_safe(&self, _input: &Value) -> bool {
        false
    }

    async fn execute(&self, input: Value) -> ToolResult {
        self.execute_with_message_id(input, None).await
    }

    fn prepare_execution<'a>(
        &'a self,
        input: Value,
        context: ToolExecutionContext,
    ) -> Result<PreparedToolExecution<'a>, String> {
        let message_id = format!("message-effect:{}", context.effect_id());
        Ok(PreparedToolExecution::new(
            None,
            Box::pin(async move { self.execute_with_message_id(input, Some(message_id)).await }),
        ))
    }

    fn describe_effect(&self, input: &Value) -> EffectDescriptor {
        let target = input.get("to_agent_id").and_then(Value::as_str).unwrap_or("unknown");
        EffectDescriptor {
            class: EffectClass::AgentLifecycle,
            action: format!("Send Mesh message to {target}"),
            resources: ResourceFootprint {
                mesh_resources: vec![format!("mesh:agent:{target}")],
                ..Default::default()
            },
            replay_policy: EffectReplayPolicy::Idempotent,
        }
    }

    fn category(&self) -> ToolCategory {
        ToolCategory::Info
    }
}

pub struct BroadcastTeamMessageTool {
    runtime: Arc<CollaborationRuntime<()>>,
    run_id: RunId,
    agent_id: AgentId,
}

impl BroadcastTeamMessageTool {
    pub fn new(runtime: Arc<CollaborationRuntime<()>>, run_id: RunId, agent_id: AgentId) -> Self {
        Self {
            runtime,
            run_id,
            agent_id,
        }
    }

    async fn execute_with_broadcast_id(&self, input: Value, broadcast_id: Option<String>) -> ToolResult {
        let Some(team_id) = input.get("team_id").and_then(Value::as_str) else {
            return ToolResult {
                content: "Missing required parameter: team_id".into(),
                is_error: true,
            };
        };
        let kind = input.get("kind").and_then(Value::as_str).unwrap_or("broadcast");
        let body = input.get("body").cloned().unwrap_or(Value::Null);
        let result = if let Some(broadcast_id) = broadcast_id {
            self.runtime.broadcast_message_with_id(
                broadcast_id,
                self.run_id.clone(),
                TeamId::from(team_id),
                self.agent_id.clone(),
                kind,
                body,
            )
        } else {
            self.runtime.broadcast_message(
                self.run_id.clone(),
                TeamId::from(team_id),
                self.agent_id.clone(),
                kind,
                body,
            )
        };
        match result {
            Ok(messages) => ToolResult {
                content: serde_json::to_string(&messages).unwrap_or_else(|_| "[]".into()),
                is_error: false,
            },
            Err(error) => ToolResult {
                content: error.to_string(),
                is_error: true,
            },
        }
    }
}

#[async_trait]
impl Tool for BroadcastTeamMessageTool {
    fn name(&self) -> &str {
        "BroadcastTeamMessage"
    }

    fn description(&self) -> &str {
        "Send one durable Solaris Mesh message to every other member of a Team."
    }

    fn input_schema(&self) -> JsonSchema {
        json!({
            "type": "object",
            "properties": {
                "team_id": {"type": "string"},
                "kind": {"type": "string"},
                "body": {}
            },
            "required": ["team_id", "body"]
        })
    }

    fn is_concurrency_safe(&self, _input: &Value) -> bool {
        false
    }

    async fn execute(&self, input: Value) -> ToolResult {
        self.execute_with_broadcast_id(input, None).await
    }

    fn prepare_execution<'a>(
        &'a self,
        input: Value,
        context: ToolExecutionContext,
    ) -> Result<PreparedToolExecution<'a>, String> {
        let broadcast_id = format!("broadcast-effect:{}", context.effect_id());
        Ok(PreparedToolExecution::new(
            None,
            Box::pin(async move { self.execute_with_broadcast_id(input, Some(broadcast_id)).await }),
        ))
    }

    fn describe_effect(&self, input: &Value) -> EffectDescriptor {
        let team = input.get("team_id").and_then(Value::as_str).unwrap_or("unknown");
        EffectDescriptor {
            class: EffectClass::MeshStateMutation,
            action: format!("Broadcast Mesh message to Team {team}"),
            resources: ResourceFootprint {
                mesh_resources: vec![format!("mesh:team:{team}:messages")],
                ..Default::default()
            },
            replay_policy: EffectReplayPolicy::Idempotent,
        }
    }

    fn category(&self) -> ToolCategory {
        ToolCategory::Info
    }
}

pub struct HandoffTaskTool {
    runtime: Arc<CollaborationRuntime<()>>,
    run_id: RunId,
    agent_id: AgentId,
}

impl HandoffTaskTool {
    pub fn new(runtime: Arc<CollaborationRuntime<()>>, run_id: RunId, agent_id: AgentId) -> Self {
        Self {
            runtime,
            run_id,
            agent_id,
        }
    }

    async fn execute_with_operation_id(&self, input: Value, operation_id: OperationId) -> ToolResult {
        let Some(task_id) = input.get("task_id").and_then(Value::as_str) else {
            return ToolResult {
                content: "Missing required parameter: task_id".into(),
                is_error: true,
            };
        };
        let Some(to_agent_id) = input.get("to_agent_id").and_then(Value::as_str) else {
            return ToolResult {
                content: "Missing required parameter: to_agent_id".into(),
                is_error: true,
            };
        };
        let Some(expected_revision) = input.get("expected_revision").and_then(Value::as_u64) else {
            return ToolResult {
                content: "Missing required parameter: expected_revision".into(),
                is_error: true,
            };
        };
        match self.runtime.handoff_task(
            &self.run_id,
            &TaskId::from(task_id),
            &self.agent_id,
            AgentId::from(to_agent_id),
            expected_revision,
            &operation_id,
        ) {
            Ok(revision) => ToolResult {
                content: json!({"task_id": task_id, "owner_agent_id": to_agent_id, "revision": revision}).to_string(),
                is_error: false,
            },
            Err(error) => ToolResult {
                content: error.to_string(),
                is_error: true,
            },
        }
    }
}

#[async_trait]
impl Tool for HandoffTaskTool {
    fn name(&self) -> &str {
        "HandoffTask"
    }

    fn description(&self) -> &str {
        "Transfer an owned Solaris Mesh task to another Agent in the same Run lineage."
    }

    fn input_schema(&self) -> JsonSchema {
        json!({
            "type": "object",
            "properties": {
                "task_id": {"type": "string"},
                "to_agent_id": {"type": "string"},
                "expected_revision": {"type": "integer", "minimum": 0}
            },
            "required": ["task_id", "to_agent_id", "expected_revision"]
        })
    }

    fn is_concurrency_safe(&self, _input: &Value) -> bool {
        false
    }

    async fn execute(&self, input: Value) -> ToolResult {
        let operation_id = OperationId::new(format!("handoff-{}", uuid::Uuid::now_v7()));
        self.execute_with_operation_id(input, operation_id).await
    }

    fn prepare_execution<'a>(
        &'a self,
        input: Value,
        context: ToolExecutionContext,
    ) -> Result<PreparedToolExecution<'a>, String> {
        let operation_id = OperationId::new(format!("handoff-effect:{}", context.effect_id()));
        Ok(PreparedToolExecution::new(
            None,
            Box::pin(async move { self.execute_with_operation_id(input, operation_id).await }),
        ))
    }

    fn describe_effect(&self, input: &Value) -> EffectDescriptor {
        let task = input.get("task_id").and_then(Value::as_str).unwrap_or("unknown");
        EffectDescriptor {
            class: EffectClass::MeshStateMutation,
            action: format!("Handoff Mesh task {task}"),
            resources: ResourceFootprint {
                mesh_resources: vec![format!("mesh:task:{task}")],
                ..Default::default()
            },
            replay_policy: EffectReplayPolicy::Idempotent,
        }
    }

    fn category(&self) -> ToolCategory {
        ToolCategory::Info
    }
}

pub struct CreateTeamTaskTool {
    runtime: Arc<CollaborationRuntime<()>>,
    run_id: RunId,
    agent_id: AgentId,
}

impl CreateTeamTaskTool {
    pub fn new(runtime: Arc<CollaborationRuntime<()>>, run_id: RunId, agent_id: AgentId) -> Self {
        Self {
            runtime,
            run_id,
            agent_id,
        }
    }
}

#[async_trait]
impl Tool for CreateTeamTaskTool {
    fn name(&self) -> &str {
        "CreateTeamTask"
    }

    fn description(&self) -> &str {
        "Create a durable task for a Solaris Mesh Team and optionally assign it to a current member."
    }

    fn input_schema(&self) -> JsonSchema {
        json!({
            "type": "object",
            "properties": {
                "team_id": {"type": "string"},
                "task_id": {"type": "string"},
                "task_key": {"type": "string", "minLength": 1},
                "role": {"type": "string"},
                "to_agent_id": {"type": "string"},
                "content": {},
                "expected_write_scope": {
                    "type": "array",
                    "items": {"type": "string", "minLength": 1}
                },
                "depends_on": {
                    "type": "array",
                    "items": {"type": "string", "minLength": 1}
                }
            },
            "required": ["team_id", "task_id"]
        })
    }

    fn is_concurrency_safe(&self, _input: &Value) -> bool {
        false
    }

    async fn execute(&self, input: Value) -> ToolResult {
        let Some(team_id) = input.get("team_id").and_then(Value::as_str) else {
            return ToolResult {
                content: "Missing required parameter: team_id".into(),
                is_error: true,
            };
        };
        let Some(task_id) = input.get("task_id").and_then(Value::as_str) else {
            return ToolResult {
                content: "Missing required parameter: task_id".into(),
                is_error: true,
            };
        };
        let owner = input
            .get("to_agent_id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(AgentId::from);
        let typed_task = input.get("content").is_some()
            || input.get("expected_write_scope").is_some()
            || input.get("depends_on").is_some();
        let task_key = input.get("task_key").and_then(Value::as_str);
        if (typed_task && task_key.is_none()) || task_key.is_some_and(|value| value.trim().is_empty()) {
            return ToolResult {
                content: "Typed Team tasks require a non-empty task_key".into(),
                is_error: true,
            };
        }
        let parse_strings = |name: &str| -> Result<Vec<String>, String> {
            let Some(value) = input.get(name) else {
                return Ok(Vec::new());
            };
            let values = value
                .as_array()
                .ok_or_else(|| format!("{name} must be an array of non-empty strings"))?;
            values
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .filter(|value| !value.trim().is_empty())
                        .map(str::to_owned)
                        .ok_or_else(|| format!("{name} must contain only non-empty strings"))
                })
                .collect()
        };
        let expected_write_scope = match parse_strings("expected_write_scope") {
            Ok(values) => values,
            Err(error) => {
                return ToolResult {
                    content: error,
                    is_error: true,
                };
            }
        };
        let dependencies = match parse_strings("depends_on") {
            Ok(values) => values,
            Err(error) => {
                return ToolResult {
                    content: error,
                    is_error: true,
                };
            }
        };
        let team_id = TeamId::from(team_id);
        let task = TaskRecord {
            run_id: self.run_id.clone(),
            task_id: CollaborationRuntime::<()>::scoped_team_task_id(&team_id, task_id),
            revision: 0,
            task_key: task_key.map(str::to_owned),
            team_id: Some(team_id.clone()),
            workflow_id: None,
            node_id: None,
            role: input.get("role").and_then(Value::as_str).map(str::to_owned),
            depends_on: dependencies
                .iter()
                .map(|dependency| CollaborationRuntime::<()>::scoped_team_task_id(&team_id, dependency))
                .collect(),
            content: input.get("content").cloned(),
            expected_write_scope,
            owner_agent_id: owner,
            state: TaskState::Queued,
            outcome_ref: None,
            failure_class: None,
        };
        let durable_task_id = task.task_id.clone();
        match self
            .runtime
            .create_collaboration_task(&self.run_id, &team_id, &self.agent_id, task)
        {
            Ok(created) => ToolResult {
                content: json!({"task_id": durable_task_id, "created": created, "revision": 0}).to_string(),
                is_error: false,
            },
            Err(error) => ToolResult {
                content: error.to_string(),
                is_error: true,
            },
        }
    }

    fn describe_effect(&self, input: &Value) -> EffectDescriptor {
        let team = input.get("team_id").and_then(Value::as_str).unwrap_or("unknown");
        let task = input.get("task_id").and_then(Value::as_str).unwrap_or("unknown");
        let team_id = TeamId::from(team);
        let scoped_task = CollaborationRuntime::<()>::scoped_team_task_id(&team_id, task);
        EffectDescriptor {
            class: EffectClass::MeshStateMutation,
            action: format!("Create Mesh task {scoped_task}"),
            resources: ResourceFootprint {
                mesh_resources: vec![format!("mesh:team:{team_id}"), format!("mesh:task:{scoped_task}")],
                ..Default::default()
            },
            replay_policy: EffectReplayPolicy::Idempotent,
        }
    }

    fn category(&self) -> ToolCategory {
        ToolCategory::Info
    }
}

pub struct GetTeamStateTool {
    runtime: Arc<CollaborationRuntime<()>>,
}

impl GetTeamStateTool {
    pub fn new(runtime: Arc<CollaborationRuntime<()>>) -> Self {
        Self { runtime }
    }
}

#[async_trait]
impl Tool for GetTeamStateTool {
    fn name(&self) -> &str {
        "GetTeamState"
    }

    fn description(&self) -> &str {
        "Read a Solaris Mesh collaboration team's strategy, coordinator and current members."
    }

    fn input_schema(&self) -> JsonSchema {
        json!({
            "type": "object",
            "properties": {"team_id": {"type": "string"}},
            "required": ["team_id"]
        })
    }

    fn is_concurrency_safe(&self, _input: &Value) -> bool {
        true
    }

    async fn execute(&self, input: Value) -> ToolResult {
        let Some(team_id) = input.get("team_id").and_then(Value::as_str) else {
            return ToolResult {
                content: "Missing required parameter: team_id".into(),
                is_error: true,
            };
        };
        match self.runtime.teams().get(&TeamId::from(team_id)) {
            Some(team) => ToolResult {
                content: serde_json::to_string(&team).unwrap_or_else(|_| "{}".into()),
                is_error: false,
            },
            None => ToolResult {
                content: format!("Unknown team: {team_id}"),
                is_error: true,
            },
        }
    }

    fn describe_effect(&self, _input: &Value) -> EffectDescriptor {
        EffectDescriptor::read_only("Read Mesh collaboration team state")
    }

    fn category(&self) -> ToolCategory {
        ToolCategory::Info
    }
}

pub struct ReadAgentMessagesTool {
    runtime: Arc<CollaborationRuntime<()>>,
    agent_id: AgentId,
}

impl ReadAgentMessagesTool {
    pub fn new(runtime: Arc<CollaborationRuntime<()>>, agent_id: AgentId) -> Self {
        Self { runtime, agent_id }
    }
}

#[async_trait]
impl Tool for ReadAgentMessagesTool {
    fn name(&self) -> &str {
        "ReadAgentMessages"
    }

    fn description(&self) -> &str {
        "Read the current Solaris Mesh mailbox for this Agent without consuming messages."
    }

    fn input_schema(&self) -> JsonSchema {
        json!({"type": "object", "properties": {}})
    }

    fn is_concurrency_safe(&self, _input: &Value) -> bool {
        true
    }

    async fn execute(&self, _input: Value) -> ToolResult {
        let claim_id = format!("mailbox-read:{}", uuid::Uuid::now_v7());
        let Some(agent) = self.runtime.agents().get(&self.agent_id) else {
            return ToolResult {
                content: format!("Unknown Agent: {}", self.agent_id),
                is_error: true,
            };
        };
        match self.runtime.messages().claim(&agent.run_id, &self.agent_id, &claim_id) {
            Ok(messages) => ToolResult {
                content: serde_json::to_string(&messages).unwrap_or_else(|_| "[]".into()),
                is_error: false,
            },
            Err(error) => ToolResult {
                content: format!("Failed to claim Agent messages: {error}"),
                is_error: true,
            },
        }
    }

    fn prepare_execution<'a>(
        &'a self,
        _input: Value,
        context: ToolExecutionContext,
    ) -> Result<PreparedToolExecution<'a>, String> {
        let claim_id = context.effect_id().to_owned();
        Ok(PreparedToolExecution::new(
            None,
            Box::pin(async move {
                let Some(agent) = self.runtime.agents().get(&self.agent_id) else {
                    return ToolResult {
                        content: format!("Unknown Agent: {}", self.agent_id),
                        is_error: true,
                    };
                };
                match self.runtime.messages().claim(&agent.run_id, &self.agent_id, &claim_id) {
                    Ok(messages) => ToolResult {
                        content: serde_json::to_string(&messages).unwrap_or_else(|_| "[]".into()),
                        is_error: false,
                    },
                    Err(error) => ToolResult {
                        content: format!("Failed to claim Agent messages: {error}"),
                        is_error: true,
                    },
                }
            }),
        ))
    }

    fn describe_effect(&self, _input: &Value) -> EffectDescriptor {
        EffectDescriptor::read_only(format!("Read Mesh mailbox for {}", self.agent_id))
    }

    fn category(&self) -> ToolCategory {
        ToolCategory::Info
    }
}

pub struct AcknowledgeAgentMessageTool {
    runtime: Arc<CollaborationRuntime<()>>,
    run_id: RunId,
    agent_id: AgentId,
}

impl AcknowledgeAgentMessageTool {
    pub fn new(runtime: Arc<CollaborationRuntime<()>>, run_id: RunId, agent_id: AgentId) -> Self {
        Self {
            runtime,
            run_id,
            agent_id,
        }
    }
}

#[async_trait]
impl Tool for AcknowledgeAgentMessageTool {
    fn name(&self) -> &str {
        "AcknowledgeAgentMessage"
    }

    fn description(&self) -> &str {
        "Acknowledge one message in this Agent's Solaris Mesh mailbox."
    }

    fn input_schema(&self) -> JsonSchema {
        json!({
            "type": "object",
            "properties": {"message_id": {"type": "string"}},
            "required": ["message_id"]
        })
    }

    fn is_concurrency_safe(&self, _input: &Value) -> bool {
        false
    }

    async fn execute(&self, input: Value) -> ToolResult {
        let Some(message_id) = input.get("message_id").and_then(Value::as_str) else {
            return ToolResult {
                content: "Missing required parameter: message_id".into(),
                is_error: true,
            };
        };
        match self
            .runtime
            .acknowledge_message(&self.run_id, &self.agent_id, message_id)
        {
            Ok(true) => ToolResult {
                content: format!("Acknowledged message {message_id}"),
                is_error: false,
            },
            Ok(false) => ToolResult {
                content: format!("Unknown mailbox message: {message_id}"),
                is_error: true,
            },
            Err(error) => ToolResult {
                content: error.to_string(),
                is_error: true,
            },
        }
    }

    fn describe_effect(&self, input: &Value) -> EffectDescriptor {
        let message_id = input.get("message_id").and_then(Value::as_str).unwrap_or("unknown");
        EffectDescriptor {
            class: EffectClass::MeshStateMutation,
            action: format!("Acknowledge Mesh message {message_id}"),
            resources: ResourceFootprint {
                mesh_resources: vec![format!("mesh:message:{message_id}")],
                ..Default::default()
            },
            replay_policy: EffectReplayPolicy::Idempotent,
        }
    }

    fn category(&self) -> ToolCategory {
        ToolCategory::Info
    }
}

pub struct SetTeamFactTool {
    runtime: Arc<CollaborationRuntime<()>>,
    run_id: RunId,
    agent_id: AgentId,
}

impl SetTeamFactTool {
    pub fn new(runtime: Arc<CollaborationRuntime<()>>, run_id: RunId, agent_id: AgentId) -> Self {
        Self {
            runtime,
            run_id,
            agent_id,
        }
    }
}

#[async_trait]
impl Tool for SetTeamFactTool {
    fn name(&self) -> &str {
        "SetTeamFact"
    }

    fn description(&self) -> &str {
        "Set or replace a durable shared fact for a Solaris Mesh Team. The caller must belong to the target Team."
    }

    fn input_schema(&self) -> JsonSchema {
        json!({
            "type": "object",
            "properties": {
                "team_id": {"type": "string"},
                "key": {"type": "string"},
                "value": {}
            },
            "required": ["team_id", "key", "value"]
        })
    }

    fn is_concurrency_safe(&self, _input: &Value) -> bool {
        false
    }

    async fn execute(&self, input: Value) -> ToolResult {
        let Some(team_id) = input.get("team_id").and_then(Value::as_str) else {
            return ToolResult {
                content: "Missing required parameter: team_id".into(),
                is_error: true,
            };
        };
        let Some(key) = input.get("key").and_then(Value::as_str) else {
            return ToolResult {
                content: "Missing required parameter: key".into(),
                is_error: true,
            };
        };
        let value = input.get("value").cloned().unwrap_or(Value::Null);
        match self
            .runtime
            .set_team_fact(&self.run_id, &TeamId::from(team_id), &self.agent_id, key, value)
        {
            Ok(fact) => ToolResult {
                content: serde_json::to_string(&fact).unwrap_or_else(|_| "fact stored".into()),
                is_error: false,
            },
            Err(error) => ToolResult {
                content: error.to_string(),
                is_error: true,
            },
        }
    }

    fn describe_effect(&self, input: &Value) -> EffectDescriptor {
        let team = input.get("team_id").and_then(Value::as_str).unwrap_or("unknown");
        let key = input.get("key").and_then(Value::as_str).unwrap_or("unknown");
        EffectDescriptor {
            class: EffectClass::MeshStateMutation,
            action: format!("Set Team fact {team}/{key}"),
            resources: ResourceFootprint {
                mesh_resources: vec![format!("mesh:team:{team}:fact:{key}")],
                ..Default::default()
            },
            replay_policy: EffectReplayPolicy::Idempotent,
        }
    }

    fn category(&self) -> ToolCategory {
        ToolCategory::Info
    }
}

pub struct ReadTeamFactsTool {
    runtime: Arc<CollaborationRuntime<()>>,
    agent_id: AgentId,
}

impl ReadTeamFactsTool {
    pub fn new(runtime: Arc<CollaborationRuntime<()>>, agent_id: AgentId) -> Self {
        Self { runtime, agent_id }
    }
}

#[async_trait]
impl Tool for ReadTeamFactsTool {
    fn name(&self) -> &str {
        "ReadTeamFacts"
    }

    fn description(&self) -> &str {
        "Read the durable shared facts for a Team that this Agent belongs to."
    }

    fn input_schema(&self) -> JsonSchema {
        json!({
            "type": "object",
            "properties": {"team_id": {"type": "string"}},
            "required": ["team_id"]
        })
    }

    fn is_concurrency_safe(&self, _input: &Value) -> bool {
        true
    }

    async fn execute(&self, input: Value) -> ToolResult {
        let Some(team_id) = input.get("team_id").and_then(Value::as_str) else {
            return ToolResult {
                content: "Missing required parameter: team_id".into(),
                is_error: true,
            };
        };
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
        let facts = self.runtime.team_facts(&team_id);
        ToolResult {
            content: serde_json::to_string(&facts).unwrap_or_else(|_| "[]".into()),
            is_error: false,
        }
    }

    fn describe_effect(&self, _input: &Value) -> EffectDescriptor {
        EffectDescriptor::read_only(format!("Read Team facts for {}", self.agent_id))
    }

    fn category(&self) -> ToolCategory {
        ToolCategory::Info
    }
}

#[cfg(test)]
#[path = "collaboration_tools_test.rs"]
mod collaboration_tools_test;
