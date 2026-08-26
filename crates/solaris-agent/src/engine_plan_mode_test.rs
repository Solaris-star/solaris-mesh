use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use solaris_protocol::events::ToolCategory;
use solaris_providers::error::ProviderError;
use solaris_providers::provider::LlmProvider;
use solaris_tools::Tool;
use solaris_tools::registry::ToolRegistry;
use solaris_tools::tool_search::ToolSearchTool;
use solaris_types::effect::{EffectClass, EffectDescriptor, EffectReplayPolicy, ResourceFootprint};
use solaris_types::identity::{AgentId, RunId};
use solaris_types::llm::{LlmEvent, LlmRequest};
use solaris_types::runtime::OperationEnvironmentSnapshot;
use solaris_types::skill_types::{ContextModifier, PlanModeTransition};
use solaris_types::tool::{JsonSchema, ToolResult};

use super::{CompactLevel, ProviderCompat};
use crate::compact::state::CompactState;
use crate::confirm::ToolConfirmer;
use crate::execution_context::EffectExecutionContext;
use crate::output::OutputSink;
use crate::plan::state::PlanState;
use crate::plan::tools::{EnterPlanModeTool, ExitPlanModeTool};
use crate::runtime_ledger::InMemoryRuntimeLedger;
use crate::turn::TurnKind;

struct NullOutput;
impl OutputSink for NullOutput {
    fn emit_text_delta(&self, _: &str, _: &str) {}
    fn emit_thinking(&self, _: &str, _: &str) {}
    fn emit_tool_call(&self, _: &str, _: &str, _: &str) {}
    fn emit_tool_result(&self, _: &str, _: &str, _: bool, _: &str) {}
    fn emit_stream_start(&self, _: &str) {}
    fn emit_stream_end(&self, _: &str, _: usize, _: u64, _: u64, _: u64, _: u64) {}
    fn emit_error(&self, _: &str) {}
    fn emit_info(&self, _: &str) {}
}

struct NullProvider;
#[async_trait::async_trait]
impl LlmProvider for NullProvider {
    async fn stream(&self, _: &LlmRequest) -> Result<tokio::sync::mpsc::Receiver<LlmEvent>, ProviderError> {
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        Ok(rx)
    }
}

struct PlanVisibilityTool {
    name: &'static str,
    class: EffectClass,
    category: ToolCategory,
}

#[async_trait::async_trait]
impl Tool for PlanVisibilityTool {
    fn name(&self) -> &str {
        self.name
    }

    fn description(&self) -> &str {
        "plan visibility test tool"
    }

    fn input_schema(&self) -> JsonSchema {
        serde_json::json!({"type": "object"})
    }

    fn is_concurrency_safe(&self, _input: &serde_json::Value) -> bool {
        true
    }

    fn is_deferred(&self) -> bool {
        self.category == ToolCategory::Mcp
    }

    async fn execute(&self, _input: serde_json::Value) -> ToolResult {
        ToolResult {
            content: "ok".into(),
            is_error: false,
        }
    }

    fn describe_effect(&self, _input: &serde_json::Value) -> EffectDescriptor {
        EffectDescriptor {
            class: self.class,
            action: self.name.into(),
            resources: ResourceFootprint::default(),
            replay_policy: EffectReplayPolicy::ReplaySafe,
        }
    }

    fn category(&self) -> ToolCategory {
        self.category
    }
}

fn make_plan_engine(allow_list: Vec<String>) -> super::AgentEngine {
    let flag = Arc::new(AtomicBool::new(false));
    super::AgentEngine {
        provider: Arc::new(NullProvider),
        provider_label: "test-provider".to_owned(),
        provider_effect: crate::bootstrap::provider_effect_descriptor("https://provider.test"),
        model: "test-model".to_string(),
        max_tokens: Some(4096),
        thinking: None,
        compat: ProviderCompat::anthropic_defaults(),
        system_prompt: String::new(),
        reasoning_effort: None,
        runtime_configuration: super::runtime_configuration_state(
            "test-provider",
            "test-model",
            solaris_types::permission::PermissionMode::Bypass,
            &None,
            CompactLevel::default(),
        ),
        multi_agent_policy: std::sync::Arc::new(std::sync::RwLock::new(
            solaris_types::workflow::MultiAgentPolicy::default(),
        )),
        resources: crate::resource_manager::ResourceManager::new(Default::default()),
        messages: vec![],
        total_usage: Default::default(),
        msg_id: String::new(),
        max_turns_per_run: Some(10),
        max_tool_call_malformed_turns: 3,
        max_tool_call_failure_turns: 3,
        tools: ToolRegistry::new(),
        confirmer: Arc::new(Mutex::new(ToolConfirmer::new(true, allow_list.clone()))),
        permission_context: crate::permission_engine::PermissionContext::new(
            solaris_types::permission::PermissionMode::Bypass,
            solaris_types::permission::PermissionCeiling::unrestricted(),
        ),
        execution_context: None,
        allow_list,
        hooks: None,
        session_manager: None,
        current_session: None,
        output: Arc::new(NullOutput),
        approval_manager: None,
        protocol_writer: None,
        compact_config: solaris_config::compact::CompactConfig::default(),
        compact_state: CompactState::new(),
        compact_level: CompactLevel::default(),
        toon_enabled: false,
        plan_state: PlanState::default(),
        plan_active_flag: Some(flag),
        plan_mode_disable_handler: None,
        cache_detector: super::CacheBreakDetector::new(),
        commands: crate::commands::default_registry(),
    }
}

#[test]
fn submitted_plan_is_persisted_before_exit_transition() {
    let mut engine = make_plan_engine(Vec::new());
    let run_id = RunId::new("run-plan-artifact");
    let context = EffectExecutionContext::new(
        run_id.clone(),
        AgentId::new("agent-plan-artifact"),
        Arc::new(InMemoryRuntimeLedger::default()),
        engine.permission_context.clone(),
        OperationEnvironmentSnapshot::default(),
    );
    engine.execution_context = Some(context.clone());
    engine.msg_id = "msg-plan".to_owned();
    let modifiers = [Some(ContextModifier {
        plan_mode_transition: Some(PlanModeTransition::Exit {
            plan_content: Some("# Durable plan\n\n- Verify it.".to_owned()),
        }),
        ..Default::default()
    })];

    engine.persist_plan_artifacts(&modifiers).unwrap();
    let stored = context.plan_artifacts_for_run(&run_id).unwrap();

    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].revision, 1);
    assert_eq!(stored[0].msg_id, "msg-plan");
    assert_eq!(stored[0].markdown, "# Durable plan\n\n- Verify it.");
}

// --- TC-3.5-03: Enter transition activates plan mode ---

#[test]
fn enter_transition_activates_plan_mode() {
    let mut engine = make_plan_engine(vec!["Read".into(), "ExecCommand".into()]);
    let modifiers = vec![Some(ContextModifier {
        plan_mode_transition: Some(PlanModeTransition::Enter),
        ..Default::default()
    })];

    engine.apply_context_modifiers(&modifiers);

    assert!(engine.plan_state.is_active, "plan mode should be active");
    assert_eq!(
        engine.plan_state.pre_plan_allow_list,
        vec!["Read".to_string(), "ExecCommand".to_string()],
        "pre_plan_allow_list should capture original allow_list"
    );
    assert_eq!(
        engine.runtime_configuration_view().snapshot().permission,
        solaris_types::permission::PermissionMode::Plan
    );
}

#[tokio::test]
async fn both_plan_entries_disable_mcp_and_refresh_tool_search() {
    for enter_with_host_mode in [false, true] {
        let mut engine = make_plan_engine(vec![]);
        engine.tools.register(Box::new(PlanVisibilityTool {
            name: "mcp__server__remote",
            class: EffectClass::Network,
            category: ToolCategory::Mcp,
        }));
        let snapshot = engine.tools.to_tool_defs();
        engine.tools.register(Box::new(ToolSearchTool::new(snapshot)));
        let disable_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let recorded = Arc::clone(&disable_calls);
        engine.set_plan_mode_disable_handler(Arc::new(move || {
            recorded.fetch_add(1, Ordering::SeqCst);
        }));

        if enter_with_host_mode {
            engine.set_permission_mode(solaris_types::permission::PermissionMode::Plan);
        } else {
            engine.apply_context_modifiers(&[Some(ContextModifier {
                plan_mode_transition: Some(PlanModeTransition::Enter),
                ..Default::default()
            })]);
        }

        assert_eq!(disable_calls.load(Ordering::SeqCst), 1);
        assert!(engine.tools.get("mcp__server__remote").is_none());
        let search = engine.tools.get("ToolSearch").unwrap();
        let result = search.execute(serde_json::json!({"query": "remote"})).await;
        assert!(!result.content.contains("mcp__server__remote"));
    }
}

#[test]
fn exit_transition_restores_configured_permission_in_runtime_configuration() {
    let mut engine = make_plan_engine(vec![]);
    engine.apply_context_modifiers(&[Some(ContextModifier {
        plan_mode_transition: Some(PlanModeTransition::Enter),
        ..Default::default()
    })]);

    engine.apply_context_modifiers(&[Some(ContextModifier {
        plan_mode_transition: Some(PlanModeTransition::Exit { plan_content: None }),
        ..Default::default()
    })]);

    assert_eq!(
        engine.runtime_configuration_view().snapshot().permission,
        solaris_types::permission::PermissionMode::Bypass
    );
}

// --- TC-3.5-03 supplement: shared flag updated on enter ---

#[test]
fn enter_transition_updates_shared_flag() {
    let mut engine = make_plan_engine(vec![]);
    let flag = engine.plan_active_flag.clone().unwrap();
    assert!(!flag.load(Ordering::Acquire));

    engine.apply_context_modifiers(&[Some(ContextModifier {
        plan_mode_transition: Some(PlanModeTransition::Enter),
        ..Default::default()
    })]);

    assert!(flag.load(Ordering::Acquire), "shared flag should be true");
}

// --- TC-3.5-04: Exit transition deactivates plan mode and restores allow_list ---

#[test]
fn exit_transition_deactivates_and_restores() {
    let mut engine = make_plan_engine(vec!["Read".into(), "ExecCommand".into()]);

    // Enter plan mode first
    engine.apply_context_modifiers(&[Some(ContextModifier {
        plan_mode_transition: Some(PlanModeTransition::Enter),
        ..Default::default()
    })]);
    assert!(engine.plan_state.is_active);

    // Modify allow_list while in plan mode (simulating a skill adding tools)
    engine.allow_list.push("NewTool".into());

    // Exit plan mode
    engine.apply_context_modifiers(&[Some(ContextModifier {
        plan_mode_transition: Some(PlanModeTransition::Exit { plan_content: None }),
        ..Default::default()
    })]);

    assert!(!engine.plan_state.is_active, "plan mode should be inactive");
    assert_eq!(
        engine.allow_list,
        vec!["Read".to_string(), "ExecCommand".to_string()],
        "allow_list should be restored to pre-plan state"
    );
}

// --- TC-3.5-04 supplement: shared flag updated on exit ---

#[test]
fn exit_transition_updates_shared_flag() {
    let mut engine = make_plan_engine(vec![]);
    let flag = engine.plan_active_flag.clone().unwrap();

    // Enter
    engine.apply_context_modifiers(&[Some(ContextModifier {
        plan_mode_transition: Some(PlanModeTransition::Enter),
        ..Default::default()
    })]);
    assert!(flag.load(Ordering::Acquire));

    // Exit
    engine.apply_context_modifiers(&[Some(ContextModifier {
        plan_mode_transition: Some(PlanModeTransition::Exit { plan_content: None }),
        ..Default::default()
    })]);
    assert!(!flag.load(Ordering::Acquire), "shared flag should be false after exit");
}

// --- TC-3.5-05: No transition does not affect plan state ---

#[test]
fn no_transition_does_not_affect_plan_state() {
    let mut engine = make_plan_engine(vec![]);

    engine.apply_context_modifiers(&[Some(ContextModifier {
        model: Some("new-model".into()),
        plan_mode_transition: None,
        ..Default::default()
    })]);

    assert_eq!(engine.model, "new-model");
    assert!(!engine.plan_state.is_active, "plan state should remain inactive");
}

#[test]
fn legacy_plan_request_exposes_exit_and_tool_search_by_effect_class() {
    let mut engine = make_plan_engine(vec![]);
    let flag = engine.plan_active_flag.clone().unwrap();
    engine
        .tools
        .register(Box::new(EnterPlanModeTool::new(Arc::clone(&flag))));
    engine.tools.register(Box::new(ExitPlanModeTool::new(flag)));
    engine.tools.register(Box::new(ToolSearchTool::new(Vec::new())));
    engine.apply_context_modifiers(&[Some(ContextModifier {
        plan_mode_transition: Some(PlanModeTransition::Enter),
        ..Default::default()
    })]);

    let request = engine.build_request(TurnKind::Normal);
    let names: Vec<_> = request.tools.iter().map(|tool| tool.name.as_str()).collect();
    assert!(names.contains(&"ExitPlanMode"));
    assert!(names.contains(&"ToolSearch"));
    assert!(!names.contains(&"EnterPlanMode"));
}

#[test]
fn plan_request_exposes_only_local_read_only_tools() {
    let mut engine = make_plan_engine(vec![]);
    engine.tools.register(Box::new(ToolSearchTool::new(Vec::new())));
    engine.tools.register(Box::new(PlanVisibilityTool {
        name: "RemoteReadOnlyClaim",
        class: EffectClass::ReadOnly,
        category: ToolCategory::Mcp,
    }));
    engine.tools.register(Box::new(PlanVisibilityTool {
        name: "RemoteNetwork",
        class: EffectClass::Network,
        category: ToolCategory::Mcp,
    }));
    engine.tools.register(Box::new(PlanVisibilityTool {
        name: "LocalStateMutation",
        class: EffectClass::MeshStateMutation,
        category: ToolCategory::Info,
    }));
    engine.apply_context_modifiers(&[Some(ContextModifier {
        plan_mode_transition: Some(PlanModeTransition::Enter),
        ..Default::default()
    })]);

    let request = engine.build_request(TurnKind::Normal);
    let names: Vec<_> = request.tools.iter().map(|tool| tool.name.as_str()).collect();

    assert!(names.contains(&"ToolSearch"));
    assert!(!names.contains(&"RemoteReadOnlyClaim"));
    assert!(!names.contains(&"RemoteNetwork"));
    assert!(!names.contains(&"LocalStateMutation"));
}

// --- Enter + other modifiers applied together ---

#[test]
fn enter_with_model_override_both_applied() {
    let mut engine = make_plan_engine(vec![]);

    engine.apply_context_modifiers(&[Some(ContextModifier {
        model: Some("planning-model".into()),
        plan_mode_transition: Some(PlanModeTransition::Enter),
        ..Default::default()
    })]);

    assert!(engine.plan_state.is_active);
    assert_eq!(engine.model, "planning-model");
}

// --- No plan_active_flag set does not panic ---

#[test]
fn enter_without_flag_does_not_panic() {
    let mut engine = make_plan_engine(vec![]);
    engine.plan_active_flag = None;

    engine.apply_context_modifiers(&[Some(ContextModifier {
        plan_mode_transition: Some(PlanModeTransition::Enter),
        ..Default::default()
    })]);

    assert!(engine.plan_state.is_active);
}
