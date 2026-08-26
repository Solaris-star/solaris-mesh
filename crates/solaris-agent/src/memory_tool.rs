use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use solaris_memory::service::{MemoryMutation, MemoryProposalDecision, MemoryScope};
use solaris_memory::types::MemoryType;
use solaris_protocol::events::ToolCategory;
use solaris_tools::Tool;
use solaris_types::effect::{EffectClass, EffectDescriptor, EffectReplayPolicy, ResourceFootprint};
use solaris_types::tool::{JsonSchema, ToolResult};

use crate::memory_runtime::MemoryRuntime;

#[derive(Debug, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
enum MemoryToolInput {
    Search {
        query: String,
    },
    List,
    Create {
        scope: MemoryScope,
        #[serde(rename = "type")]
        memory_type: MemoryType,
        name: String,
        #[serde(default)]
        description: String,
        content: String,
    },
    Edit {
        id: String,
        expected_version: u64,
        name: String,
        #[serde(default)]
        description: String,
        content: String,
    },
    Delete {
        id: String,
        expected_version: u64,
    },
    Pending,
    Review {
        proposal_id: String,
        decision: ReviewDecision,
    },
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ReviewDecision {
    Approve,
    Reject,
}

impl ReviewDecision {
    fn into_service(self) -> MemoryProposalDecision {
        match self {
            Self::Approve => MemoryProposalDecision::Approve,
            Self::Reject => MemoryProposalDecision::Reject,
        }
    }
}

pub(crate) struct MemoryTool {
    runtime: Arc<MemoryRuntime>,
    child_agent: bool,
}

impl MemoryTool {
    pub(crate) fn root(runtime: Arc<MemoryRuntime>) -> Self {
        Self {
            runtime,
            child_agent: false,
        }
    }

    pub(crate) fn child(runtime: Arc<MemoryRuntime>) -> Self {
        Self {
            runtime,
            child_agent: true,
        }
    }

    fn mutation_requires_proposal(&self) -> bool {
        self.child_agent || self.runtime.review_enabled()
    }

    fn execute_mutation(&self, mutation: MemoryMutation) -> ToolResult {
        if self.mutation_requires_proposal() {
            match self.runtime.service().submit_proposal(mutation) {
                Ok(proposal) => success_json(json!({ "status": "proposed", "proposal": proposal })),
                Err(error) => failure(error.to_string()),
            }
        } else {
            match self.runtime.service().apply(mutation) {
                Ok(record) => success_json(json!({ "status": "applied", "record": record })),
                Err(error) => failure(error.to_string()),
            }
        }
    }
}

#[async_trait]
impl Tool for MemoryTool {
    fn name(&self) -> &str {
        "Memory"
    }

    fn description(&self) -> &str {
        "Search the frozen session memory snapshot, or propose versioned memory changes."
    }

    fn input_schema(&self) -> JsonSchema {
        json!({
            "type": "object",
            "oneOf": [
                {
                    "properties": {
                        "operation": { "const": "search" },
                        "query": { "type": "string", "maxLength": 1024 }
                    },
                    "required": ["operation", "query"]
                },
                {
                    "properties": { "operation": { "const": "list" } },
                    "required": ["operation"]
                },
                {
                    "properties": {
                        "operation": { "const": "create" },
                        "scope": { "enum": ["USER", "MEMORY"] },
                        "type": { "enum": ["user", "feedback", "project", "reference"] },
                        "name": { "type": "string" },
                        "description": { "type": "string" },
                        "content": { "type": "string" }
                    },
                    "required": ["operation", "scope", "type", "name", "content"]
                },
                {
                    "properties": {
                        "operation": { "const": "edit" },
                        "id": { "type": "string" },
                        "expected_version": { "type": "integer", "minimum": 1 },
                        "name": { "type": "string" },
                        "description": { "type": "string" },
                        "content": { "type": "string" }
                    },
                    "required": ["operation", "id", "expected_version", "name", "content"]
                },
                {
                    "properties": {
                        "operation": { "const": "delete" },
                        "id": { "type": "string" },
                        "expected_version": { "type": "integer", "minimum": 1 }
                    },
                    "required": ["operation", "id", "expected_version"]
                },
                {
                    "properties": { "operation": { "const": "pending" } },
                    "required": ["operation"]
                },
                {
                    "properties": {
                        "operation": { "const": "review" },
                        "proposal_id": { "type": "string" },
                        "decision": { "enum": ["approve", "reject"] }
                    },
                    "required": ["operation", "proposal_id", "decision"]
                }
            ]
        })
    }

    fn is_concurrency_safe(&self, input: &Value) -> bool {
        matches!(
            serde_json::from_value::<MemoryToolInput>(input.clone()),
            Ok(MemoryToolInput::Search { .. } | MemoryToolInput::List | MemoryToolInput::Pending)
        )
    }

    async fn execute(&self, input: Value) -> ToolResult {
        let input = match serde_json::from_value::<MemoryToolInput>(input) {
            Ok(input) => input,
            Err(_) => return failure("memory tool input is invalid"),
        };
        match input {
            MemoryToolInput::Search { query } => match self.runtime.snapshot().search(&query) {
                Ok(records) => success_json(json!({ "records": records })),
                Err(error) => failure(error.to_string()),
            },
            MemoryToolInput::List => {
                let mut bytes = 0usize;
                let mut truncated = false;
                let mut records = Vec::new();
                for record in self.runtime.snapshot().records().iter().take(256) {
                    let value = json!({
                            "id": record.id,
                            "scope": record.scope,
                            "type": record.memory_type,
                            "name": record.name,
                            "description": record.description,
                            "version": record.version,
                    });
                    let encoded_bytes = value.to_string().len();
                    if bytes.saturating_add(encoded_bytes) > 64 * 1024 {
                        truncated = true;
                        break;
                    }
                    bytes += encoded_bytes;
                    records.push(value);
                }
                success_json(json!({ "records": records, "truncated": truncated }))
            }
            MemoryToolInput::Create {
                scope,
                memory_type,
                name,
                description,
                content,
            } => self.execute_mutation(MemoryMutation::Create {
                scope,
                memory_type,
                name,
                description,
                content,
            }),
            MemoryToolInput::Edit {
                id,
                expected_version,
                name,
                description,
                content,
            } => self.execute_mutation(MemoryMutation::Edit {
                id,
                expected_version,
                name,
                description,
                content,
            }),
            MemoryToolInput::Delete { id, expected_version } => {
                self.execute_mutation(MemoryMutation::Delete { id, expected_version })
            }
            MemoryToolInput::Pending => {
                if self.child_agent {
                    return failure("child agents cannot review memory proposals");
                }
                match self.runtime.service().pending_proposals() {
                    Ok(proposals) => success_json(json!({ "proposals": proposals })),
                    Err(error) => failure(error.to_string()),
                }
            }
            MemoryToolInput::Review { proposal_id, decision } => {
                if self.child_agent {
                    return failure("child agents cannot review memory proposals");
                }
                match self
                    .runtime
                    .service()
                    .review_proposal(&proposal_id, decision.into_service())
                {
                    Ok(record) => success_json(json!({ "status": "reviewed", "record": record })),
                    Err(error) => failure(error.to_string()),
                }
            }
        }
    }

    fn describe_effect(&self, input: &Value) -> EffectDescriptor {
        let effect_kind = match serde_json::from_value::<MemoryToolInput>(input.clone()) {
            Ok(MemoryToolInput::Search { .. } | MemoryToolInput::List | MemoryToolInput::Pending) => {
                MemoryEffectKind::Read
            }
            Ok(MemoryToolInput::Create { .. } | MemoryToolInput::Edit { .. } | MemoryToolInput::Delete { .. })
                if self.mutation_requires_proposal() =>
            {
                MemoryEffectKind::Proposal
            }
            _ => MemoryEffectKind::Write,
        };
        EffectDescriptor {
            class: match effect_kind {
                MemoryEffectKind::Read => EffectClass::ReadOnly,
                MemoryEffectKind::Proposal => EffectClass::MeshStateMutation,
                MemoryEffectKind::Write => EffectClass::ExternalSideEffect,
            },
            action: match effect_kind {
                MemoryEffectKind::Read => "read session memory state",
                MemoryEffectKind::Proposal => "submit a long-term memory proposal",
                MemoryEffectKind::Write => "change durable long-term memory",
            }
            .to_owned(),
            resources: if effect_kind == MemoryEffectKind::Read {
                ResourceFootprint::default()
            } else {
                ResourceFootprint {
                    external_resources: vec![match effect_kind {
                        MemoryEffectKind::Proposal => "memory:proposals".to_owned(),
                        MemoryEffectKind::Write => "memory:store".to_owned(),
                        MemoryEffectKind::Read => unreachable!(),
                    }],
                    ..Default::default()
                }
            },
            replay_policy: if effect_kind == MemoryEffectKind::Read {
                EffectReplayPolicy::Idempotent
            } else {
                EffectReplayPolicy::ReconcileRequired
            },
        }
    }

    fn category(&self) -> ToolCategory {
        ToolCategory::Info
    }

    fn is_deferred(&self) -> bool {
        true
    }

    fn max_result_size(&self) -> usize {
        128 * 1024
    }

    fn describe(&self, input: &Value) -> String {
        let operation = input.get("operation").and_then(Value::as_str).unwrap_or("invalid");
        format!("Memory {operation}")
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum MemoryEffectKind {
    Read,
    Proposal,
    Write,
}

fn success_json(value: Value) -> ToolResult {
    ToolResult {
        content: value.to_string(),
        is_error: false,
    }
}

fn failure(message: impl Into<String>) -> ToolResult {
    ToolResult {
        content: message.into(),
        is_error: true,
    }
}

#[cfg(test)]
#[path = "memory_tool_test.rs"]
mod memory_tool_test;
