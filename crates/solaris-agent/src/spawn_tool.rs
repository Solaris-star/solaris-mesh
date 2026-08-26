use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::spawner::AgentSpawner;
#[cfg(test)]
use crate::spawner::SubAgentConfig;
use solaris_protocol::events::ToolCategory;
use solaris_types::effect::{EffectClass, EffectDescriptor, EffectReplayPolicy, ResourceFootprint};
use solaris_types::tool::{JsonSchema, ToolResult};
use solaris_types::workflow::{CollaborationSelection, CollaborationTaskInput};

use solaris_tools::Tool;

pub(crate) const DEFAULT_SUB_AGENT_MAX_TURNS: usize = 200;
pub(crate) const DEFAULT_SUB_AGENT_MAX_TOKENS: u32 = 4096;
const LEGACY_MAX_SPAWN_TASKS: usize = 32;
pub(crate) const MAX_SPAWN_TASKS: usize = 256;

#[derive(Debug, Clone)]
pub struct ParsedSpawnRequest {
    pub strategy: CollaborationSelection,
    pub tasks: Vec<CollaborationTaskInput>,
}

pub struct SpawnTool {
    spawner: Arc<AgentSpawner>,
}

impl SpawnTool {
    pub fn new(spawner: Arc<AgentSpawner>) -> Self {
        Self { spawner }
    }
}

#[async_trait]
impl Tool for SpawnTool {
    fn name(&self) -> &str {
        "Spawn"
    }

    fn description(&self) -> &str {
        "Create durable Child Agent tasks. Each task has its own conversation context and inherits the current permission state. Independent tasks may run in parallel; tasks are queued when available resources are busy. Each Child Agent defaults to at most 200 turns and 4096 tokens. Use strategy=fanout for independent work, strategy=supervisor or team for coordinated work, and depends_on for ordered stages."
    }

    fn input_schema(&self) -> JsonSchema {
        spawn_input_schema()
    }

    fn is_concurrency_safe(&self, _input: &Value) -> bool {
        false // manages its own concurrency
    }

    fn is_deferred(&self) -> bool {
        true
    }

    async fn execute(&self, input: Value) -> ToolResult {
        let request = match parse_spawn_request(&input) {
            Ok(request) => request,
            Err(e) => {
                return ToolResult {
                    content: e,
                    is_error: true,
                };
            }
        };

        let summary = self.spawner.spawn_collaboration(request).await;

        let output: Vec<String> = summary
            .tasks
            .iter()
            .map(|r| {
                let status = if r.error_kind.is_some() { "ERROR" } else { "OK" };
                format!(
                    "## {} [{}]\nChild Agent: {}\n[duration: {} ms | tokens: {} in / {} out]",
                    r.name,
                    status,
                    r.agent_id.as_ref().map_or("none", |id| id.as_str()),
                    r.duration_ms,
                    r.usage.input_tokens,
                    r.usage.output_tokens
                )
            })
            .collect();

        let all_error = summary.tasks.iter().all(|task| task.error_kind.is_some());

        ToolResult {
            content: if output.is_empty() {
                summary.summary.clone()
            } else {
                output.join("\n\n---\n\n")
            },
            is_error: all_error,
        }
    }

    async fn execute_classified(&self, input: Value) -> solaris_types::tool::ClassifiedToolResult {
        let request = match parse_spawn_request(&input) {
            Ok(request) => request,
            Err(error) => {
                return solaris_types::tool::ClassifiedToolResult::new(
                    error,
                    solaris_types::tool::ToolResultStatus::Failed,
                );
            }
        };
        let summary = self.spawner.spawn_collaboration(request).await;
        let status = match summary.status {
            solaris_types::workflow::CollaborationRunStatus::Completed => {
                solaris_types::tool::ToolResultStatus::Executed
            }
            solaris_types::workflow::CollaborationRunStatus::OutcomeUnknown => {
                solaris_types::tool::ToolResultStatus::OutcomeUnknown
            }
            solaris_types::workflow::CollaborationRunStatus::Cancelled => {
                solaris_types::tool::ToolResultStatus::Aborted
            }
            _ => solaris_types::tool::ToolResultStatus::Failed,
        };
        let content = summary.summary.clone();
        solaris_types::tool::ClassifiedToolResult::new(content, status)
            .with_metadata(solaris_types::tool::ToolResultMetadata::collaboration_summary(summary))
    }

    fn describe_effect(&self, input: &Value) -> EffectDescriptor {
        let count = input.get("tasks").and_then(Value::as_array).map_or(0, Vec::len);
        EffectDescriptor {
            class: EffectClass::AgentLifecycle,
            action: format!("Spawn {count} child agent task(s)"),
            resources: ResourceFootprint {
                mesh_resources: vec![format!("mesh:agent-spawn:{count}")],
                ..Default::default()
            },
            replay_policy: EffectReplayPolicy::ReconcileRequired,
        }
    }

    fn category(&self) -> ToolCategory {
        ToolCategory::Exec
    }

    fn describe(&self, input: &Value) -> String {
        let task = input.get("task").and_then(|v| v.as_str()).unwrap_or("sub-agent");
        format!("Spawn: {}", solaris_tools::truncate_utf8(task, 80))
    }
}

fn spawn_input_schema() -> JsonSchema {
    json!({
        "type": "object",
        "properties": {
            "strategy": {
                "type": "string",
                "enum": ["auto", "single", "supervisor", "team", "fanout", "independent_reviewer"],
                "default": "auto",
                "description": "Collaboration strategy. Auto selects Single, Fanout, or Supervisor from the task graph."
            },
            "tasks": {
                "type": "array",
                "description": "List of durable collaboration tasks. Legacy tasks without ids are limited to 32; v2 ids support up to 256 subject to the Run policy.",
                "minItems": 1,
                "maxItems": MAX_SPAWN_TASKS,
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {
                            "type": "string",
                            "minLength": 1,
                            "description": "Stable task identity used for dependency and recovery."
                        },
                        "name": {
                            "type": "string",
                            "description": "Short descriptive name for the task"
                        },
                        "prompt": {
                            "type": "string",
                            "description": "The task description / prompt for the sub-agent"
                        },
                        "role": {
                            "type": "string",
                            "description": "Free-form expert role name."
                        },
                        "depends_on": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Stable IDs that must complete before this task starts."
                        },
                        "expected_output": {
                            "description": "Optional typed output contract or expected result metadata."
                        },
                        "budget": {
                            "type": "object",
                            "description": "Optional ResourceBudget override for this task."
                        },
                        "resource_budget": {
                            "type": "object",
                            "description": "Compatibility alias for budget."
                        }
                    },
                    "required": ["name", "prompt"]
                }
            }
        },
        "required": ["tasks"]
    })
}

#[cfg(test)]
pub(crate) fn parse_tasks(input: &Value) -> Result<Vec<SubAgentConfig>, String> {
    let request = parse_spawn_request(input)?;
    request
        .tasks
        .into_iter()
        .map(|task| {
            Ok(SubAgentConfig {
                name: task.name,
                prompt: task.prompt,
                max_turns: task
                    .resource_budget
                    .as_ref()
                    .and_then(|budget| budget.max_turns)
                    .unwrap_or(DEFAULT_SUB_AGENT_MAX_TURNS),
                max_tokens: task
                    .resource_budget
                    .as_ref()
                    .and_then(|budget| budget.max_tokens)
                    .unwrap_or(u64::from(DEFAULT_SUB_AGENT_MAX_TOKENS))
                    .min(u64::from(u32::MAX)) as u32,
                system_prompt: None,
            })
        })
        .collect()
}

pub(crate) fn parse_spawn_request(input: &Value) -> Result<ParsedSpawnRequest, String> {
    let tasks_arr = input["tasks"].as_array().ok_or("Missing or invalid 'tasks' array")?;
    if tasks_arr.is_empty() {
        return Err("No tasks provided".to_owned());
    }
    if tasks_arr.len() > MAX_SPAWN_TASKS {
        return Err(format!(
            "Spawn accepts at most {MAX_SPAWN_TASKS} tasks per call; received {}",
            tasks_arr.len()
        ));
    }

    let has_v2_ids = tasks_arr.iter().any(|task| task.get("id").is_some());
    if !has_v2_ids && tasks_arr.len() > LEGACY_MAX_SPAWN_TASKS {
        return Err(format!(
            "legacy Spawn accepts at most {LEGACY_MAX_SPAWN_TASKS} tasks per call; use task.id for up to {MAX_SPAWN_TASKS} durable tasks"
        ));
    }

    let strategy = parse_request_strategy(input.get("strategy"))?;
    let mut tasks = Vec::with_capacity(tasks_arr.len());
    for (index, task) in tasks_arr.iter().enumerate() {
        let name = task["name"]
            .as_str()
            .ok_or("Each task must have a 'name' string")?
            .to_string();
        let prompt = task["prompt"]
            .as_str()
            .ok_or("Each task must have a 'prompt' string")?
            .to_string();

        let id = task
            .get("id")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| legacy_task_id(index, &name, &prompt));
        let depends_on = match task.get("depends_on") {
            None => Vec::new(),
            Some(value) => value
                .as_array()
                .ok_or("task.depends_on must be an array of strings")?
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| "task.depends_on must be an array of strings".to_owned())
                })
                .collect::<Result<Vec<_>, _>>()?,
        };
        let role = task
            .get("role")
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| "task.role must be a string".to_owned())
            })
            .transpose()?;
        let resource_budget = task
            .get("budget")
            .or_else(|| task.get("resource_budget"))
            .map(|value| {
                serde_json::from_value(value.clone()).map_err(|error| format!("task budget is invalid: {error}"))
            })
            .transpose()?;
        tasks.push(CollaborationTaskInput {
            id: Some(id),
            name,
            prompt,
            role,
            depends_on,
            expected_output: task.get("expected_output").cloned(),
            resource_budget,
        });
    }
    validate_task_graph(&tasks)?;
    Ok(ParsedSpawnRequest { strategy, tasks })
}

fn parse_request_strategy(value: Option<&Value>) -> Result<CollaborationSelection, String> {
    let Some(value) = value else {
        return Ok(CollaborationSelection::Auto);
    };
    let strategy = value
        .as_str()
        .ok_or_else(|| "top-level strategy must be a string".to_owned())?
        .to_ascii_lowercase();
    let selection = match strategy.as_str() {
        "auto" => CollaborationSelection::Auto,
        "single" => CollaborationSelection::Fixed(solaris_types::workflow::CollaborationStrategy::Single),
        "supervisor" => configured_strategy(solaris_types::workflow::CollaborationStrategy::Supervisor),
        "team" => configured_strategy(solaris_types::workflow::CollaborationStrategy::Team),
        "fanout" => configured_strategy(solaris_types::workflow::CollaborationStrategy::Fanout),
        "independent_reviewer" | "independent-reviewer" | "reviewer" => {
            configured_strategy(solaris_types::workflow::CollaborationStrategy::IndependentReviewer)
        }
        _ => return Err(format!("unknown collaboration strategy: {strategy}")),
    };
    Ok(selection)
}

fn configured_strategy(strategy: solaris_types::workflow::CollaborationStrategy) -> CollaborationSelection {
    CollaborationSelection::Configured(solaris_types::workflow::CollaborationRuntimeConfig {
        strategy,
        ..solaris_types::workflow::CollaborationRuntimeConfig::default()
    })
}

fn legacy_task_id(index: usize, name: &str, prompt: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"solaris.spawn.legacy-task.v1");
    hasher.update(index.to_be_bytes());
    hasher.update(name.as_bytes());
    hasher.update(prompt.as_bytes());
    format!("legacy:{index}:{:x}", hasher.finalize())
}

fn validate_task_graph(tasks: &[CollaborationTaskInput]) -> Result<(), String> {
    use std::collections::{HashMap, HashSet};

    let mut ids = HashSet::with_capacity(tasks.len());
    for task in tasks {
        let id = task.id.as_deref().unwrap_or_default();
        if !ids.insert(id.to_owned()) {
            return Err(format!("duplicate task id: {id}"));
        }
    }
    let by_id: HashMap<_, _> = tasks
        .iter()
        .filter_map(|task| task.id.as_deref().map(|id| (id, task)))
        .collect();
    for task in tasks {
        let id = task.id.as_deref().unwrap_or_default();
        for dependency in &task.depends_on {
            if dependency == id {
                return Err(format!("task {id} cannot depend on itself"));
            }
            if !by_id.contains_key(dependency.as_str()) {
                return Err(format!("task {id} depends on missing task {dependency}"));
            }
        }
    }
    fn visit(
        id: &str,
        by_id: &HashMap<&str, &CollaborationTaskInput>,
        visiting: &mut HashSet<String>,
        visited: &mut HashSet<String>,
    ) -> Result<(), String> {
        if visited.contains(id) {
            return Ok(());
        }
        if !visiting.insert(id.to_owned()) {
            return Err(format!("task dependency cycle includes {id}"));
        }
        if let Some(task) = by_id.get(id) {
            for dependency in &task.depends_on {
                visit(dependency, by_id, visiting, visited)?;
            }
        }
        visiting.remove(id);
        visited.insert(id.to_owned());
        Ok(())
    }
    let mut visiting = HashSet::new();
    let mut visited = HashSet::new();
    for id in ids {
        visit(&id, &by_id, &mut visiting, &mut visited)?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "spawn_tool_test.rs"]
mod spawn_tool_test;
