use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use serde_json::{Value, json};

use solaris_protocol::events::ToolCategory;
use solaris_tools::Tool;
use solaris_types::effect::{EffectClass, EffectDescriptor, EffectReplayPolicy, ResourceFootprint};
use solaris_types::skill_types::{ContextModifier, PlanModeTransition};
use solaris_types::tool::{JsonSchema, ToolResult};

const MAX_PLAN_MARKDOWN_BYTES: usize = 1024 * 1024;

// ---------------------------------------------------------------------------
// EnterPlanModeTool
// ---------------------------------------------------------------------------

/// Transitions the agent into Plan Mode.
///
/// While in plan mode the engine restricts the available tool set to
/// read-only (`Info`-category) tools so the LLM can focus on understanding
/// the codebase and composing an implementation plan.
pub struct EnterPlanModeTool {
    /// Shared flag indicating whether plan mode is currently active.
    /// Read by `execute()` to prevent double-entry.
    plan_active: Arc<AtomicBool>,
}

impl EnterPlanModeTool {
    pub fn new(plan_active: Arc<AtomicBool>) -> Self {
        Self { plan_active }
    }
}

#[async_trait]
impl Tool for EnterPlanModeTool {
    fn name(&self) -> &str {
        "EnterPlanMode"
    }

    fn description(&self) -> &str {
        "Enter plan mode to focus on reading code and creating an implementation plan. \
         While in plan mode, only read-only tools are available. \
         Use ExitPlanMode when your plan is ready."
    }

    fn input_schema(&self) -> JsonSchema {
        json!({
            "type": "object",
            "properties": {},
            "required": []
        })
    }

    fn is_concurrency_safe(&self, _input: &Value) -> bool {
        true
    }

    fn is_deferred(&self) -> bool {
        true
    }

    async fn execute(&self, _input: Value) -> ToolResult {
        if self.plan_active.load(Ordering::Acquire) {
            return ToolResult {
                content: "Already in plan mode. Use ExitPlanMode to exit first.".to_string(),
                is_error: true,
            };
        }

        ToolResult {
            content: "Entered plan mode. You can now only use read-only tools to explore \
                      the codebase and create your implementation plan. When your plan is \
                      ready, use ExitPlanMode to exit plan mode and begin implementation."
                .to_string(),
            is_error: false,
        }
    }

    fn context_modifier_for(&self, _input: &Value) -> Option<ContextModifier> {
        Some(ContextModifier {
            plan_mode_transition: Some(PlanModeTransition::Enter),
            ..Default::default()
        })
    }

    fn describe_effect(&self, _input: &Value) -> EffectDescriptor {
        EffectDescriptor {
            class: EffectClass::MeshStateMutation,
            action: "enter legacy plan mode".into(),
            resources: ResourceFootprint {
                external_resources: vec!["mesh:plan-mode".into()],
                ..Default::default()
            },
            replay_policy: EffectReplayPolicy::Idempotent,
        }
    }

    fn category(&self) -> ToolCategory {
        ToolCategory::Info
    }

    fn describe(&self, _input: &Value) -> String {
        "Enter plan mode".to_string()
    }
}

// ---------------------------------------------------------------------------
// ExitPlanModeTool
// ---------------------------------------------------------------------------

/// Transitions the agent out of Plan Mode.
///
/// On exit the engine restores the full tool set and the allow-list
/// that was in effect before plan mode was entered.
pub struct ExitPlanModeTool {
    /// Shared flag indicating whether plan mode is currently active.
    /// Read by `execute()` to reject exit when not in plan mode.
    plan_active: Arc<AtomicBool>,
}

impl ExitPlanModeTool {
    pub fn new(plan_active: Arc<AtomicBool>) -> Self {
        Self { plan_active }
    }
}

#[async_trait]
impl Tool for ExitPlanModeTool {
    fn name(&self) -> &str {
        "ExitPlanMode"
    }

    fn description(&self) -> &str {
        "Submit the complete markdown plan and exit plan mode. \
         The plan must start with a # heading and is saved as a durable PlanArtifact."
    }

    fn input_schema(&self) -> JsonSchema {
        json!({
            "type": "object",
            "properties": {
                "plan": {
                    "type": "string",
                    "description": "The complete markdown plan, starting with a # heading.",
                    "minLength": 3,
                    "maxLength": MAX_PLAN_MARKDOWN_BYTES
                }
            },
            "required": ["plan"],
            "additionalProperties": false
        })
    }

    fn is_concurrency_safe(&self, _input: &Value) -> bool {
        true
    }

    fn is_deferred(&self) -> bool {
        true
    }

    async fn execute(&self, input: Value) -> ToolResult {
        if !self.plan_active.load(Ordering::Acquire) {
            return ToolResult {
                content: "Not in plan mode. Use EnterPlanMode to enter plan mode first.".to_string(),
                is_error: true,
            };
        }

        let Some(plan) = validated_plan_markdown(&input) else {
            return ToolResult {
                content: "ExitPlanMode requires a non-empty markdown plan starting with a # heading and no larger than 1 MiB."
                    .to_string(),
                is_error: true,
            };
        };

        ToolResult {
            content: format!(
                "Saved plan artifact ({} bytes) and exited plan mode. Full tool access has been restored.",
                plan.len()
            ),
            is_error: false,
        }
    }

    fn context_modifier_for(&self, input: &Value) -> Option<ContextModifier> {
        let plan_content = validated_plan_markdown(input)?.to_owned();
        Some(ContextModifier {
            plan_mode_transition: Some(PlanModeTransition::Exit {
                plan_content: Some(plan_content),
            }),
            ..Default::default()
        })
    }

    fn describe_effect(&self, _input: &Value) -> EffectDescriptor {
        EffectDescriptor {
            class: EffectClass::MeshStateMutation,
            action: "exit legacy plan mode".into(),
            resources: ResourceFootprint {
                external_resources: vec!["mesh:plan-mode".into()],
                ..Default::default()
            },
            replay_policy: EffectReplayPolicy::Idempotent,
        }
    }

    fn category(&self) -> ToolCategory {
        ToolCategory::Info
    }

    fn describe(&self, _input: &Value) -> String {
        "Exit plan mode".to_string()
    }
}

fn validated_plan_markdown(input: &Value) -> Option<&str> {
    let plan = input.get("plan")?.as_str()?.trim();
    (plan.len() <= MAX_PLAN_MARKDOWN_BYTES && plan.starts_with("# ")).then_some(plan)
}

#[cfg(test)]
#[path = "tools_test.rs"]
mod tools_test;
