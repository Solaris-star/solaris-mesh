use super::*;

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use solaris_config::config::{CliArgs, Config};
use solaris_providers::{LlmProvider, ProviderError};
use solaris_types::identity::AgentId;
use solaris_types::llm::{LlmEvent, LlmRequest};
use solaris_types::message::StopReason;
use solaris_types::permission::PermissionCeiling;
use solaris_types::resource::ResourceBudget;
use solaris_types::runtime::{AgentLifecycleState, AgentRecord};
use solaris_types::workflow::{
    AgentRoleDefinition, CollaborationStrategy, ModelPolicy, RetryPolicy, WorkflowDefinition, WorkflowNode,
};
use tokio::sync::{Barrier, mpsc};

use crate::collaboration_runtime::CollaborationRuntime;
use crate::resource_policy::ResourcePolicy;
use crate::scheduler::Scheduler;

struct ConcurrentUsageProvider {
    gate: Arc<Barrier>,
}

#[async_trait]
impl LlmProvider for ConcurrentUsageProvider {
    async fn stream(&self, request: &LlmRequest) -> Result<mpsc::Receiver<LlmEvent>, ProviderError> {
        let messages = serde_json::to_string(&request.messages).unwrap();
        let usage = if messages.contains("usage-first") {
            TokenUsage {
                input_tokens: 11,
                output_tokens: 3,
                cache_creation_tokens: 2,
                cache_read_tokens: 5,
            }
        } else if messages.contains("usage-second") {
            TokenUsage {
                input_tokens: 101,
                output_tokens: 17,
                cache_creation_tokens: 7,
                cache_read_tokens: 29,
            }
        } else {
            panic!("unexpected workflow prompt: {messages}");
        };
        self.gate.wait().await;
        let (tx, rx) = mpsc::channel(2);
        tx.send(LlmEvent::TextDelta(r#"{"ok":true}"#.into())).await.unwrap();
        tx.send(LlmEvent::Done {
            stop_reason: StopReason::EndTurn,
            usage,
        })
        .await
        .unwrap();
        Ok(rx)
    }
}

fn concurrent_usage_config() -> Config {
    let mut config = Config::resolve(&CliArgs {
        provider: Some("anthropic".into()),
        api_key: Some("test".into()),
        base_url: None,
        model: Some("test-model".into()),
        max_tokens: None,
        thinking: None,
        thinking_budget: None,
        max_turns: None,
        max_tool_call_malformed_turns: None,
        max_tool_call_failure_turns: None,
        system_prompt: None,
        profile: None,
        auto_approve: true,
        project_dir: None,
    })
    .unwrap();
    // Usage isolation is the subject of this test. Disable unrelated durable
    // child sessions so two fixed workflow identities can run concurrently.
    config.session.enabled = false;
    config
}

fn concurrent_usage_role() -> AgentRoleDefinition {
    AgentRoleDefinition {
        id: "usage-worker".into(),
        description: "Return typed JSON".into(),
        input_schema: None,
        output_schema: Some(json!({"type":"object", "required":["ok"]})),
        model_policy: ModelPolicy::default(),
        capability_scope: Vec::new(),
        permission_ceiling: PermissionCeiling::unrestricted(),
        context_policy: Some("isolated".into()),
        recursion_policy: Some("none".into()),
        budget: ResourceBudget::default(),
    }
}

fn concurrent_usage_workflow() -> WorkflowDefinition {
    WorkflowDefinition {
        id: STANDARD_PLAN_WORKFLOW.into(),
        schema_version: 1,
        version: "usage-test-v1".into(),
        description: "Measure one workflow independently".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![WorkflowNode {
            id: "work".into(),
            depends_on: Vec::new(),
            when: None,
            role: Some("usage-worker".into()),
            collaboration: CollaborationSelection::Fixed(CollaborationStrategy::Single),
            model_policy: ModelPolicy::default(),
            capability_scope: Vec::new(),
            permission_ceiling: PermissionCeiling::unrestricted(),
            retry: RetryPolicy { max_attempts: 1 },
            timeout_ms: None,
            output_bindings: Vec::new(),
            workflow_ref: None,
        }],
        outputs: BTreeMap::from([("result".into(), "work".into())]),
    }
}

#[test]
fn ultracode_is_a_required_workflow_preset() {
    let preset = resolve_run_preset(
        Intensity::Ultracode,
        &["low".into(), "medium".into(), "high".into(), "xhigh".into()],
    );
    assert_eq!(preset.reasoning_effort.as_deref(), Some("xhigh"));
    assert!(matches!(
        preset.workflow_requirement,
        WorkflowRequirement::Required { ref workflow_id } if workflow_id == ULTRACODE_WORKFLOW
    ));
    assert_eq!(preset.multi_agent_policy, MultiAgentPolicy::Proactive);
}

#[test]
fn extra_falls_back_to_high_when_provider_has_no_higher_tier() {
    let preset = resolve_run_preset(Intensity::Extra, &["low".into(), "medium".into(), "high".into()]);
    assert_eq!(preset.reasoning_effort.as_deref(), Some("high"));
    assert!(matches!(preset.workflow_requirement, WorkflowRequirement::Optional));
}

#[test]
fn plan_routes_to_standard_plan_by_default() {
    let preset = resolve_run_preset(Intensity::High, &["high".into()]);
    assert_eq!(
        required_workflow_for_task(PermissionMode::Plan, "inspect the repository", &preset),
        Some(STANDARD_PLAN_WORKFLOW)
    );
}

#[test]
fn research_command_routes_to_deep_research() {
    let preset = resolve_run_preset(Intensity::High, &["high".into()]);
    assert_eq!(
        required_workflow_for_task(PermissionMode::Plan, "/research compare the providers", &preset),
        Some(DEEP_RESEARCH_WORKFLOW)
    );
    assert_eq!(
        required_workflow_for_task(PermissionMode::Plan, "/researching is not a command", &preset),
        None
    );
}

#[test]
fn research_command_routes_to_deep_research_in_every_permission_mode() {
    let preset = resolve_run_preset(Intensity::High, &["high".into()]);

    for permission_mode in [PermissionMode::Plan, PermissionMode::Auto, PermissionMode::Bypass] {
        assert_eq!(
            required_workflow_for_task(permission_mode, "/research compare providers", &preset),
            Some(DEEP_RESEARCH_WORKFLOW)
        );
    }
}

#[test]
fn research_command_overrides_ultracode_required_workflow() {
    let preset = resolve_run_preset(Intensity::Ultracode, &["ultra".into()]);

    assert_eq!(
        required_workflow_for_task(PermissionMode::Bypass, "/research compare providers", &preset),
        Some(DEEP_RESEARCH_WORKFLOW)
    );
}

#[test]
fn ultracode_remains_required_for_regular_prompt_in_plan_mode() {
    let preset = resolve_run_preset(Intensity::Ultracode, &["high".into()]);
    assert_eq!(
        required_workflow_for_task(PermissionMode::Plan, "compare the providers", &preset),
        Some(ULTRACODE_WORKFLOW)
    );
}

#[test]
fn failed_workflow_keeps_turn_and_cache_telemetry() {
    let error = workflow_task_error(
        "required workflow failed".into(),
        3,
        TokenUsage {
            input_tokens: 400,
            output_tokens: 70,
            cache_creation_tokens: 7,
            cache_read_tokens: 200,
        },
    );
    assert_eq!(error.to_string(), "required workflow failed");
    assert_eq!(error.turns, 3);
    assert_eq!(error.usage.input_tokens, 400);
    assert_eq!(error.usage.output_tokens, 70);
    assert_eq!(error.usage.cache_creation_tokens, 7);
    assert_eq!(error.usage.cache_read_tokens, 200);
}

#[tokio::test]
async fn concurrent_workflows_report_only_their_own_agent_usage() {
    let runtime = Arc::new(CollaborationRuntime::new(Scheduler::new(ResourcePolicy::new(2))));
    let root_run = RunId::from("concurrent-usage-root");
    let root_agent = AgentId::from("concurrent-usage-agent");
    runtime.agents().upsert(AgentRecord {
        run_id: root_run.clone(),
        agent_id: root_agent.clone(),
        team_id: None,
        parent_agent_id: None,
        state: AgentLifecycleState::Active,
    });
    let spawner = Arc::new(
        AgentSpawner::new(
            Arc::new(ConcurrentUsageProvider {
                gate: Arc::new(Barrier::new(2)),
            }),
            concurrent_usage_config(),
            std::env::temp_dir(),
        )
        .with_runtime_context(Arc::clone(&runtime), root_run.clone(), root_agent),
    );
    let roles = Arc::new(AgentRoleRegistry::default());
    roles.register(concurrent_usage_role());
    let controller = Arc::new(WorkflowController::with_runtime_and_roles(
        runtime,
        Some(Arc::clone(&roles)),
    ));
    controller.register(concurrent_usage_workflow()).unwrap();
    let router = RuntimeTaskRouter::new(controller, roles, spawner);
    let preset = resolve_run_preset(Intensity::High, &["high".into()]);
    let identity = WorkflowRuntimeIdentity::new("test-provider", "test-model");

    let (first, second) = tokio::join!(
        router.execute_required_workflow(
            &root_run,
            "usage-first-message",
            "usage-first",
            &identity,
            PermissionMode::Plan,
            &preset,
        ),
        router.execute_required_workflow(
            &root_run,
            "usage-second-message",
            "usage-second",
            &identity,
            PermissionMode::Plan,
            &preset,
        ),
    );
    let first = first.unwrap().unwrap();
    let second = second.unwrap().unwrap();

    assert_eq!(first.turns, 1);
    assert_eq!(first.usage.input_tokens, 11);
    assert_eq!(first.usage.output_tokens, 3);
    assert_eq!(first.usage.cache_creation_tokens, 2);
    assert_eq!(first.usage.cache_read_tokens, 5);
    assert_eq!(second.turns, 1);
    assert_eq!(second.usage.input_tokens, 101);
    assert_eq!(second.usage.output_tokens, 17);
    assert_eq!(second.usage.cache_creation_tokens, 7);
    assert_eq!(second.usage.cache_read_tokens, 29);
}

#[test]
fn required_workflow_identity_tracks_normalized_input_and_version() {
    let parent = RunId::from("root");
    let first = stable_workflow_run_id(
        &parent,
        "message-1",
        "workflow-a",
        "1",
        "provider-a",
        "model-a",
        &json!({"nested":{"z":2,"a":1},"prompt":"same"}),
    );
    let reordered = stable_workflow_run_id(
        &parent,
        "message-1",
        "workflow-a",
        "1",
        "provider-a",
        "model-a",
        &json!({"prompt":"same","nested":{"a":1,"z":2}}),
    );
    let different_input = stable_workflow_run_id(
        &parent,
        "message-1",
        "workflow-a",
        "1",
        "provider-a",
        "model-a",
        &json!({"prompt":"different"}),
    );
    let different_version = stable_workflow_run_id(
        &parent,
        "message-1",
        "workflow-a",
        "2",
        "provider-a",
        "model-a",
        &json!({"nested":{"z":2,"a":1},"prompt":"same"}),
    );
    let different_provider = stable_workflow_run_id(
        &parent,
        "message-1",
        "workflow-a",
        "1",
        "provider-b",
        "model-a",
        &json!({"nested":{"z":2,"a":1},"prompt":"same"}),
    );
    let different_model = stable_workflow_run_id(
        &parent,
        "message-1",
        "workflow-a",
        "1",
        "provider-a",
        "model-b",
        &json!({"nested":{"z":2,"a":1},"prompt":"same"}),
    );

    assert_eq!(first, reordered);
    assert_ne!(first, different_input);
    assert_ne!(first, different_version);
    assert_ne!(first, different_provider);
    assert_ne!(first, different_model);
    assert!(first.as_str().starts_with("root:workflow:message-1:v3:"));
}

#[test]
fn required_workflow_parameters_are_shared_identity_input() {
    let preset = resolve_run_preset(Intensity::Ultracode, &["ultra".into()]);

    let parameters = required_workflow_parameters(
        "message-7",
        "fix the bug",
        "provider-a",
        "model-a",
        PermissionMode::Bypass,
        &preset,
    );

    assert_eq!(parameters["host_msg_id"], "message-7");
    assert_eq!(parameters["prompt"], "fix the bug");
    assert_eq!(parameters["provider"], "provider-a");
    assert_eq!(parameters["model"], "model-a");
    assert_eq!(parameters["intensity"], "ultracode");
    assert_eq!(parameters["reasoning_effort"], "ultra");
    assert_eq!(parameters["collaboration"], "team");
    assert_eq!(parameters["permission_mode"], "bypass");
}
