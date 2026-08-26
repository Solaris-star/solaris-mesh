use super::*;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use solaris_config::config::{CliArgs, Config};
use solaris_providers::{LlmProvider, ProviderError};
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::{AgentId, RunId};
use solaris_types::llm::{LlmEvent, LlmRequest};
use solaris_types::message::{StopReason, TokenUsage};
use solaris_types::permission::PermissionCeiling;
use solaris_types::resource::ResourceBudget;
use solaris_types::runtime::{AgentLifecycleState, AgentRecord};
use solaris_types::workflow::{
    AgentRoleDefinition, CollaborationRuntimeConfig, ModelPolicy, RetryPolicy, WorkflowDefinition, WorkflowNode,
};
use tokio::sync::mpsc;

use crate::collaboration_runtime::CollaborationRuntime;
use crate::resource_policy::ResourcePolicy;
use crate::runtime_ledger::{InMemoryRuntimeLedger, LedgerRecord, RuntimeLedger};
use crate::scheduler::Scheduler;
use crate::workflow_controller::{WorkflowController, WorkflowRunStatus};

fn stable_agent_id(spec: &AgentSpawnSpec) -> AgentId {
    let key = ChildAgentKey {
        run_id: spec.run_id.clone(),
        parent_agent_id: spec.parent_agent_id.clone(),
        role_key: spec.role_key.clone(),
        stable_task_key: spec.stable_task_key.clone(),
        spawn_operation_id: spec.operation_id.clone(),
    };
    key.agent_id()
}

#[test]
fn configured_strategy_is_not_overridden_by_legacy_parameters() {
    let selection = CollaborationSelection::Configured(CollaborationRuntimeConfig {
        strategy: CollaborationStrategy::Supervisor,
        ..CollaborationRuntimeConfig::default()
    });

    assert_eq!(
        resolve_strategy(
            &selection,
            &serde_json::json!({
                "collaboration": "fanout",
                "collaboration_strategy": "fanout"
            }),
        ),
        CollaborationStrategy::Supervisor
    );
}

#[test]
fn legacy_collaboration_strategy_parser_accepts_all_supported_names_and_aliases() {
    let cases = [
        ("single", CollaborationStrategy::Single),
        ("supervisor", CollaborationStrategy::Supervisor),
        ("team", CollaborationStrategy::Team),
        ("fanout", CollaborationStrategy::Fanout),
        ("independent_reviewer", CollaborationStrategy::IndependentReviewer),
        ("independent-reviewer", CollaborationStrategy::IndependentReviewer),
        ("reviewer", CollaborationStrategy::IndependentReviewer),
    ];

    for (name, expected) in cases {
        assert_eq!(parse_strategy(name), Some(expected), "legacy strategy name {name}");
        assert_eq!(
            resolve_strategy(
                &CollaborationSelection::Auto,
                &serde_json::json!({"collaboration": name}),
            ),
            expected,
            "legacy strategy name {name}"
        );
    }

    assert_eq!(parse_strategy("TEAM"), Some(CollaborationStrategy::Team));
}

#[test]
fn unknown_legacy_collaboration_strategy_falls_back_to_single() {
    assert_eq!(parse_strategy("unknown"), None);
    assert_eq!(
        resolve_strategy(
            &CollaborationSelection::Inherit,
            &serde_json::json!({"collaboration": "unknown"}),
        ),
        CollaborationStrategy::Single
    );
}

struct JsonProvider;

struct FailOnceTaskSettlementLedger {
    inner: InMemoryRuntimeLedger,
    fail_next_settlement: AtomicBool,
}

impl Default for FailOnceTaskSettlementLedger {
    fn default() -> Self {
        Self {
            inner: InMemoryRuntimeLedger::default(),
            fail_next_settlement: AtomicBool::new(true),
        }
    }
}

impl RuntimeLedger for FailOnceTaskSettlementLedger {
    crate::runtime_ledger::forward_compare_and_append!();

    fn append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        payload: Value,
    ) -> std::io::Result<LedgerRecord> {
        if record_type == "task_cas"
            && payload.get("transition").and_then(Value::as_str) == Some("settle")
            && self.fail_next_settlement.swap(false, Ordering::SeqCst)
        {
            return Err(std::io::Error::other("injected task settlement failure"));
        }
        self.inner.append(run_id, durability, record_type, payload)
    }

    fn run_ids(&self) -> std::io::Result<Vec<RunId>> {
        self.inner.run_ids()
    }

    fn records_for_run(&self, run_id: &RunId) -> std::io::Result<Vec<LedgerRecord>> {
        self.inner.records_for_run(run_id)
    }

    fn effect_output_root(&self) -> Option<std::path::PathBuf> {
        self.inner.effect_output_root()
    }
}

#[async_trait]
impl LlmProvider for JsonProvider {
    async fn stream(&self, _request: &LlmRequest) -> Result<mpsc::Receiver<LlmEvent>, ProviderError> {
        let (tx, rx) = mpsc::channel(4);
        tx.send(LlmEvent::TextDelta(r#"{"ok":true}"#.into())).await.unwrap();
        tx.send(LlmEvent::Done {
            stop_reason: StopReason::EndTurn,
            usage: TokenUsage::default(),
        })
        .await
        .unwrap();
        Ok(rx)
    }
}

struct CountingJsonProvider {
    calls: Arc<AtomicUsize>,
    systems: Arc<Mutex<Vec<String>>>,
    runtime: Arc<CollaborationRuntime<()>>,
    expected_ready_members: Option<usize>,
}

struct InvalidJsonThenValidProvider {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl LlmProvider for InvalidJsonThenValidProvider {
    async fn stream(&self, _request: &LlmRequest) -> Result<mpsc::Receiver<LlmEvent>, ProviderError> {
        let attempt = self.calls.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = mpsc::channel(4);
        let text = if attempt == 0 { "not-json" } else { r#"{"ok":true}"# };
        tx.send(LlmEvent::TextDelta(text.into())).await.unwrap();
        tx.send(LlmEvent::Done {
            stop_reason: StopReason::EndTurn,
            usage: TokenUsage::default(),
        })
        .await
        .unwrap();
        Ok(rx)
    }
}

#[async_trait]
impl LlmProvider for CountingJsonProvider {
    async fn stream(&self, _request: &LlmRequest) -> Result<mpsc::Receiver<LlmEvent>, ProviderError> {
        if let Some(expected) = self.expected_ready_members {
            assert_eq!(
                self.runtime.teams().snapshot().first().map(|team| team.members.len()),
                Some(expected),
                "every collaboration member must be joined before the first model request"
            );
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.systems
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(_request.system.clone());
        JsonProvider.stream(_request).await
    }
}

fn test_config() -> Config {
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
    // These tests exercise Workflow lifecycle semantics, not child session
    // persistence. A shared default session database would make fixed test Run
    // IDs interfere with parallel tests and with state left by earlier runs.
    config.session.enabled = false;
    config
}

fn test_role() -> AgentRoleDefinition {
    AgentRoleDefinition {
        id: "worker".into(),
        description: "Return JSON".into(),
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

fn direct_spawn_spec(run_id: &RunId, parent: &AgentId, suffix: &str) -> AgentSpawnSpec {
    AgentSpawnSpec {
        run_id: run_id.clone(),
        parent_agent_id: parent.clone(),
        task_id: TaskId::new(format!("task-{suffix}")),
        role_key: format!("role-{suffix}"),
        stable_task_key: format!("stable-{suffix}"),
        operation_id: OperationId::new(format!("operation-{suffix}")),
        expected_task_revision: None,
        config: SubAgentConfig {
            name: format!("agent-{suffix}"),
            prompt: "return JSON".into(),
            max_turns: 1,
            max_tokens: 64,
            system_prompt: None,
        },
        overrides: ForkOverrides::default(),
        permission_ceiling: PermissionCeiling::unrestricted(),
        resource_budget: ResourceBudget::default(),
        context_policy: None,
        recursion_limit: None,
    }
}

fn collaboration_spawn_spec(
    run_id: &RunId,
    parent: &AgentId,
    suffix: &str,
    max_pending_messages: u32,
    max_message_bytes: u32,
) -> AgentSpawnSpec {
    let mut spec = direct_spawn_spec(run_id, parent, suffix);
    spec.overrides.collaboration = Some(AgentCollaborationContext {
        team_id: TeamId::from(format!("legacy-wire-team-{suffix}")),
        strategy: CollaborationStrategy::Team,
        coordinator_agent_id: parent.clone(),
        max_pending_messages,
        max_message_bytes,
    });
    spec
}

fn legacy_wire_handle(spec: &AgentSpawnSpec) -> (Value, AgentHandle) {
    let mut spec_value = serde_json::to_value(spec).unwrap();
    let collaboration = spec_value
        .pointer_mut("/overrides/collaboration")
        .and_then(Value::as_object_mut)
        .unwrap();
    collaboration.remove("max_pending_messages");
    collaboration.remove("max_message_bytes");
    let spec_digest = crate::execution_context::stable_digest_value(&spec_value);
    let key = ChildAgentKey {
        run_id: spec.run_id.clone(),
        parent_agent_id: spec.parent_agent_id.clone(),
        role_key: spec.role_key.clone(),
        stable_task_key: spec.stable_task_key.clone(),
        spawn_operation_id: spec.operation_id.clone(),
    };
    let payload = json!({
        "run_id": spec.run_id,
        "agent_id": key.agent_id(),
        "identity_version": ChildAgentKey::CURRENT_IDENTITY_VERSION,
        "task_id": spec.task_id,
        "operation_id": spec.operation_id,
        "role_key": spec.role_key,
        "stable_task_key": spec.stable_task_key,
        "spec_digest": spec_digest,
        "spec": spec_value,
    });
    let handle = serde_json::from_value(payload.clone()).unwrap();
    (payload, handle)
}

fn workflow_executor_for_runtime(
    runtime: Arc<CollaborationRuntime<()>>,
    root_run: &RunId,
    root_agent: &AgentId,
) -> (Arc<AgentSpawner>, AgentWorkflowExecutor) {
    runtime.agents().upsert(AgentRecord {
        run_id: root_run.clone(),
        agent_id: root_agent.clone(),
        team_id: None,
        parent_agent_id: None,
        state: AgentLifecycleState::Active,
    });
    let spawner = Arc::new(
        AgentSpawner::new(Arc::new(JsonProvider), test_config(), std::env::temp_dir()).with_runtime_context(
            runtime,
            root_run.clone(),
            root_agent.clone(),
        ),
    );
    let executor = AgentWorkflowExecutor::new(Arc::clone(&spawner), Arc::new(AgentRoleRegistry::default()));
    (spawner, executor)
}

fn seed_committed_spawn(runtime: &CollaborationRuntime<()>, spec: &AgentSpawnSpec) {
    let permissions = crate::permission_engine::PermissionContext::new(
        solaris_types::permission::PermissionMode::Bypass,
        PermissionCeiling::unrestricted(),
    );
    let reservation = runtime
        .reserve_spawn_typed(
            spec.run_id.clone(),
            spec.parent_agent_id.clone(),
            spec.role_key.clone(),
            spec.stable_task_key.clone(),
            spec.operation_id.clone(),
            &permissions,
            spec.permission_ceiling,
        )
        .unwrap();
    runtime.commit_spawn(&reservation).unwrap();
}

#[tokio::test]
async fn legacy_wire_collaboration_handle_reattaches_and_joins_to_terminal_state() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let runtime = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(2)),
        ledger.clone(),
    ));
    let root_run = RunId::from("legacy-wire-join-root");
    let root_agent = AgentId::from("legacy-wire-join-agent");
    let (_, executor) = workflow_executor_for_runtime(Arc::clone(&runtime), &root_run, &root_agent);
    let spec = collaboration_spawn_spec(
        &root_run,
        &root_agent,
        "join",
        CollaborationRuntimeConfig::DEFAULT_MAX_PENDING_MESSAGES,
        CollaborationRuntimeConfig::DEFAULT_MAX_MESSAGE_BYTES,
    );
    let (payload, legacy_handle) = legacy_wire_handle(&spec);
    ledger
        .append(&root_run, DurabilityClass::SyncCritical, "agent_handle_issued", payload)
        .unwrap();
    seed_committed_spawn(&runtime, &spec);

    let mut issued = executor.spawn_handles(vec![(spec, true)]).await.unwrap();
    assert_eq!(issued.pop().unwrap().0.spec_digest, legacy_handle.spec_digest);
    let result = executor.join_handle(legacy_handle.clone(), true).await.unwrap();

    assert_eq!(result.status, AgentOutcomeStatus::Completed);
    assert_eq!(
        runtime.agents().get(&legacy_handle.agent_id).unwrap().state,
        AgentLifecycleState::Completed
    );
    assert_eq!(
        runtime.tasks().get(&legacy_handle.task_id).unwrap().state,
        TaskState::Completed
    );
}

#[tokio::test]
async fn legacy_wire_collaboration_handle_reattaches_and_cancels_to_terminal_state() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let runtime = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(2)),
        ledger.clone(),
    ));
    let root_run = RunId::from("legacy-wire-cancel-root");
    let root_agent = AgentId::from("legacy-wire-cancel-agent");
    let (_, executor) = workflow_executor_for_runtime(Arc::clone(&runtime), &root_run, &root_agent);
    let spec = collaboration_spawn_spec(
        &root_run,
        &root_agent,
        "cancel",
        CollaborationRuntimeConfig::DEFAULT_MAX_PENDING_MESSAGES,
        CollaborationRuntimeConfig::DEFAULT_MAX_MESSAGE_BYTES,
    );
    let (payload, legacy_handle) = legacy_wire_handle(&spec);
    ledger
        .append(&root_run, DurabilityClass::SyncCritical, "agent_handle_issued", payload)
        .unwrap();
    seed_committed_spawn(&runtime, &spec);
    let relationship_before = runtime.relationships().snapshot().into_iter().next().unwrap();
    assert_eq!(relationship_before.child_agent_id, legacy_handle.agent_id);
    assert_eq!(
        runtime.agents().get(&legacy_handle.agent_id).unwrap().state,
        AgentLifecycleState::Active
    );
    let spawn_intents_before = ledger
        .records_for_run(&root_run)
        .unwrap()
        .iter()
        .filter(|record| record.record_type == "agent_spawn_intent")
        .count();

    let issued = executor.spawn_handles(vec![(spec, true)]).await.unwrap();
    assert_eq!(issued[0].0.spec_digest, legacy_handle.spec_digest);
    assert_eq!(issued[0].0.agent_id, legacy_handle.agent_id);
    assert_eq!(runtime.relationships().snapshot(), vec![relationship_before]);
    assert_eq!(
        runtime.agents().get(&legacy_handle.agent_id).unwrap().state,
        AgentLifecycleState::Active
    );
    assert_eq!(
        ledger
            .records_for_run(&root_run)
            .unwrap()
            .iter()
            .filter(|record| record.record_type == "agent_spawn_intent")
            .count(),
        spawn_intents_before
    );
    let mut legacy = vec![(legacy_handle.clone(), true)];
    executor.cancel_handles(&mut legacy).await.unwrap();

    assert_eq!(
        runtime.agents().get(&legacy_handle.agent_id).unwrap().state,
        AgentLifecycleState::Cancelled
    );
    assert_eq!(
        runtime.tasks().get(&legacy_handle.task_id).unwrap().state,
        TaskState::Cancelled
    );
}

#[tokio::test]
async fn non_default_message_limits_change_digest_and_tampering_is_rejected() {
    let runtime = Arc::new(CollaborationRuntime::new(Scheduler::new(ResourcePolicy::new(2))));
    let root_run = RunId::from("message-limit-digest-root");
    let root_agent = AgentId::from("message-limit-digest-agent");
    let (spawner, _) = workflow_executor_for_runtime(Arc::clone(&runtime), &root_run, &root_agent);
    let default_spec = collaboration_spawn_spec(
        &root_run,
        &root_agent,
        "limit-digest",
        CollaborationRuntimeConfig::DEFAULT_MAX_PENDING_MESSAGES,
        CollaborationRuntimeConfig::DEFAULT_MAX_MESSAGE_BYTES,
    );
    let mut limited_spec = default_spec.clone();
    let collaboration = limited_spec.overrides.collaboration.as_mut().unwrap();
    collaboration.max_pending_messages = 1;
    collaboration.max_message_bytes = 1_024;
    let default_value = serde_json::to_value(&default_spec).unwrap();
    let limited_value = serde_json::to_value(&limited_spec).unwrap();
    assert_ne!(
        crate::execution_context::stable_digest_value(&default_value),
        crate::execution_context::stable_digest_value(&limited_value)
    );
    assert_eq!(
        limited_value.pointer("/overrides/collaboration/max_pending_messages"),
        Some(&json!(1))
    );
    assert_eq!(
        limited_value.pointer("/overrides/collaboration/max_message_bytes"),
        Some(&json!(1_024))
    );

    let handle = spawner.spawn(limited_spec).await.unwrap();
    let mut join_tampered = handle.clone();
    join_tampered
        .spec
        .overrides
        .collaboration
        .as_mut()
        .unwrap()
        .max_pending_messages = 2;
    assert!(
        spawner
            .join(&join_tampered)
            .await
            .unwrap_err()
            .contains("digest changed")
    );
    let mut cancel_tampered = handle.clone();
    cancel_tampered
        .spec
        .overrides
        .collaboration
        .as_mut()
        .unwrap()
        .max_message_bytes = 2_048;
    assert!(
        spawner
            .cancel(&cancel_tampered)
            .await
            .unwrap_err()
            .contains("identity mismatch")
    );

    spawner.cancel(&handle).await.unwrap();
    assert_eq!(
        runtime.agents().get(&handle.agent_id).unwrap().state,
        AgentLifecycleState::Cancelled
    );
}

#[tokio::test]
async fn batch_spawn_failure_cancels_prior_handle_effect_and_task() {
    let runtime = Arc::new(CollaborationRuntime::new(Scheduler::new(ResourcePolicy::new(2))));
    let root_run = RunId::from("batch-cleanup-root");
    let root_agent = AgentId::from("batch-cleanup-agent");
    runtime.agents().upsert(AgentRecord {
        run_id: root_run.clone(),
        agent_id: root_agent.clone(),
        team_id: None,
        parent_agent_id: None,
        state: AgentLifecycleState::Active,
    });
    let spawner = Arc::new(
        AgentSpawner::new(Arc::new(JsonProvider), test_config(), std::env::temp_dir()).with_runtime_context(
            Arc::clone(&runtime),
            root_run.clone(),
            root_agent.clone(),
        ),
    );
    let executor = AgentWorkflowExecutor::new(Arc::clone(&spawner), Arc::new(AgentRoleRegistry::default()));
    let first = direct_spawn_spec(&root_run, &root_agent, "first");
    let first_agent = stable_agent_id(&first);
    let mut invalid = direct_spawn_spec(&RunId::from("other-run"), &root_agent, "invalid");
    invalid.parent_agent_id = root_agent;

    let error = executor
        .spawn_handles(vec![(first.clone(), true), (invalid, true)])
        .await
        .expect_err("second reservation must fail");

    assert!(error.message.contains("Run"));
    assert_eq!(
        runtime.agents().get(&first_agent).map(|agent| agent.state),
        Some(AgentLifecycleState::Cancelled)
    );
    assert!(runtime.tasks().get(&first.task_id).is_none());
    assert!(
        runtime
            .ledger()
            .records_for_run(&root_run)
            .unwrap()
            .iter()
            .any(|record| record.record_type == "effect_outcome")
    );
}

#[tokio::test]
async fn join_settlement_failure_requires_reconciliation_without_overwriting_task() {
    let ledger = Arc::new(FailOnceTaskSettlementLedger::default());
    let runtime = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(2)),
        ledger,
    ));
    let root_run = RunId::from("join-settlement-cleanup-root");
    let root_agent = AgentId::from("join-settlement-cleanup-agent");
    runtime.agents().upsert(AgentRecord {
        run_id: root_run.clone(),
        agent_id: root_agent.clone(),
        team_id: None,
        parent_agent_id: None,
        state: AgentLifecycleState::Active,
    });
    let spawner = Arc::new(
        AgentSpawner::new(Arc::new(JsonProvider), test_config(), std::env::temp_dir()).with_runtime_context(
            Arc::clone(&runtime),
            root_run.clone(),
            root_agent.clone(),
        ),
    );
    let executor = AgentWorkflowExecutor::new(spawner, Arc::new(AgentRoleRegistry::default()));
    let spec = direct_spawn_spec(&root_run, &root_agent, "settlement-failure");
    let agent_id = stable_agent_id(&spec);
    let task_id = spec.task_id.clone();
    let mut handles = executor.spawn_handles(vec![(spec, true)]).await.unwrap();
    let (handle, settle_task) = handles.pop().unwrap();

    let error = executor
        .join_handle(handle, settle_task)
        .await
        .expect_err("the first task settlement write is injected to fail");

    assert_eq!(error.failure_class, TaskFailureClass::ReconciliationRequired);
    assert!(error.message.contains("injected task settlement failure"));
    assert_eq!(
        runtime.agents().get(&agent_id).map(|agent| agent.state),
        Some(AgentLifecycleState::Completed)
    );
    assert!(error.message.contains("reconciliation is required"));
    assert_eq!(runtime.tasks().get(&task_id).unwrap().state, TaskState::Running);
}

#[tokio::test]
async fn cancelled_spawn_handle_is_terminal_after_runtime_and_spawner_restart() {
    let ledger = Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default());
    let root_run = RunId::from("cancel-restart-root");
    let root_agent = AgentId::from("cancel-restart-agent");
    let runtime = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(2)),
        ledger.clone(),
    ));
    runtime.agents().upsert(AgentRecord {
        run_id: root_run.clone(),
        agent_id: root_agent.clone(),
        team_id: None,
        parent_agent_id: None,
        state: AgentLifecycleState::Active,
    });
    let spawner = AgentSpawner::new(Arc::new(JsonProvider), test_config(), std::env::temp_dir()).with_runtime_context(
        Arc::clone(&runtime),
        root_run.clone(),
        root_agent.clone(),
    );
    let spec = direct_spawn_spec(&root_run, &root_agent, "cancelled");
    let handle = spawner.spawn(spec.clone()).await.unwrap();
    assert_eq!(
        ledger
            .records_for_run(&root_run)
            .unwrap()
            .iter()
            .filter(|record| record.record_type == "effect_intent")
            .count(),
        0,
        "reserving a handle must not claim that child execution started"
    );

    spawner.cancel(&handle).await.unwrap();
    assert_eq!(
        spawner.join(&handle).await.unwrap().status,
        AgentOutcomeStatus::Cancelled
    );

    let restored = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(2)),
        ledger.clone(),
    ));
    restored.restore_projection(&root_run).unwrap();
    restored.agents().upsert(AgentRecord {
        run_id: root_run.clone(),
        agent_id: root_agent.clone(),
        team_id: None,
        parent_agent_id: None,
        state: AgentLifecycleState::Active,
    });
    let restarted = AgentSpawner::new(Arc::new(JsonProvider), test_config(), std::env::temp_dir())
        .with_runtime_context(Arc::clone(&restored), root_run.clone(), root_agent);
    let restored_handle = restarted.spawn(spec).await.unwrap();
    let restored_outcome = restarted.join(&restored_handle).await.unwrap();

    assert_eq!(restored_outcome.status, AgentOutcomeStatus::Cancelled);
    assert_eq!(
        restored
            .agents()
            .get(&restored_handle.agent_id)
            .map(|agent| agent.state),
        Some(AgentLifecycleState::Cancelled)
    );
    assert_eq!(
        ledger
            .records_for_run(&root_run)
            .unwrap()
            .iter()
            .filter(|record| record.record_type == "effect_outcome")
            .count(),
        1
    );
    assert_eq!(
        ledger
            .records_for_run(&root_run)
            .unwrap()
            .iter()
            .filter(|record| record.record_type == "effect_intent")
            .count(),
        1
    );
}

#[tokio::test]
async fn collaboration_prepare_failure_leaves_no_assigned_tasks_or_agents() {
    let runtime = Arc::new(CollaborationRuntime::new(Scheduler::new(ResourcePolicy::new(4))));
    let root_run = RunId::from("prepare-cleanup-root");
    let root_agent = AgentId::from("prepare-cleanup-agent");
    runtime.agents().upsert(AgentRecord {
        run_id: root_run.clone(),
        agent_id: root_agent.clone(),
        team_id: None,
        parent_agent_id: None,
        state: AgentLifecycleState::Active,
    });
    let spawner = Arc::new(
        AgentSpawner::new(Arc::new(JsonProvider), test_config(), std::env::temp_dir()).with_runtime_context(
            Arc::clone(&runtime),
            root_run.clone(),
            root_agent.clone(),
        ),
    );
    let executor = AgentWorkflowExecutor::new(spawner, Arc::new(AgentRoleRegistry::default()));
    let mut first = direct_spawn_spec(&root_run, &root_agent, "first-team");
    first.overrides.collaboration = Some(AgentCollaborationContext {
        team_id: TeamId::from("prepared-team"),
        strategy: CollaborationStrategy::Team,
        coordinator_agent_id: root_agent.clone(),
        max_pending_messages: CollaborationRuntimeConfig::DEFAULT_MAX_PENDING_MESSAGES,
        max_message_bytes: CollaborationRuntimeConfig::DEFAULT_MAX_MESSAGE_BYTES,
    });
    let mut second = direct_spawn_spec(&root_run, &root_agent, "broken-team");
    second.overrides.collaboration = Some(AgentCollaborationContext {
        team_id: TeamId::from("broken-team"),
        strategy: CollaborationStrategy::Team,
        coordinator_agent_id: AgentId::from("missing-coordinator"),
        max_pending_messages: CollaborationRuntimeConfig::DEFAULT_MAX_PENDING_MESSAGES,
        max_message_bytes: CollaborationRuntimeConfig::DEFAULT_MAX_MESSAGE_BYTES,
    });
    let first_agent = stable_agent_id(&first);
    let second_agent = stable_agent_id(&second);

    let error = executor
        .spawn_handles(vec![(first.clone(), true), (second.clone(), true)])
        .await
        .expect_err("second Team preparation must fail");

    assert!(error.message.contains("prepare collaboration"));
    assert!(runtime.tasks().get(&first.task_id).is_none());
    assert!(runtime.tasks().get(&second.task_id).is_none());
    assert_eq!(
        runtime.agents().get(&first_agent).map(|agent| agent.state),
        Some(AgentLifecycleState::Cancelled)
    );
    assert_eq!(
        runtime.agents().get(&second_agent).map(|agent| agent.state),
        Some(AgentLifecycleState::Cancelled)
    );
    assert!(runtime.teams().get(&TeamId::from("prepared-team")).is_none());
    assert!(runtime.teams().get(&TeamId::from("broken-team")).is_none());
    assert_eq!(runtime.agents().get(&root_agent).and_then(|agent| agent.team_id), None);
}

#[tokio::test]
async fn collaboration_prepare_rejects_mismatched_message_limits_without_leaks() {
    let runtime = Arc::new(CollaborationRuntime::new(Scheduler::new(ResourcePolicy::new(4))));
    let root_run = RunId::from("prepare-limit-mismatch-root");
    let root_agent = AgentId::from("prepare-limit-mismatch-agent");
    runtime.agents().upsert(AgentRecord {
        run_id: root_run.clone(),
        agent_id: root_agent.clone(),
        team_id: None,
        parent_agent_id: None,
        state: AgentLifecycleState::Active,
    });
    let spawner = Arc::new(
        AgentSpawner::new(Arc::new(JsonProvider), test_config(), std::env::temp_dir()).with_runtime_context(
            Arc::clone(&runtime),
            root_run.clone(),
            root_agent.clone(),
        ),
    );
    let executor = AgentWorkflowExecutor::new(spawner, Arc::new(AgentRoleRegistry::default()));
    let team_id = TeamId::from("message-limit-team");
    let mut first = direct_spawn_spec(&root_run, &root_agent, "first-limit");
    first.overrides.collaboration = Some(AgentCollaborationContext {
        team_id: team_id.clone(),
        strategy: CollaborationStrategy::Team,
        coordinator_agent_id: root_agent.clone(),
        max_pending_messages: 1,
        max_message_bytes: CollaborationRuntimeConfig::DEFAULT_MAX_MESSAGE_BYTES,
    });
    let mut second = direct_spawn_spec(&root_run, &root_agent, "second-limit");
    second.overrides.collaboration = Some(AgentCollaborationContext {
        team_id: team_id.clone(),
        strategy: CollaborationStrategy::Team,
        coordinator_agent_id: root_agent.clone(),
        max_pending_messages: 2,
        max_message_bytes: CollaborationRuntimeConfig::DEFAULT_MAX_MESSAGE_BYTES,
    });
    let first_agent = stable_agent_id(&first);
    let second_agent = stable_agent_id(&second);

    let error = executor
        .spawn_handles(vec![(first.clone(), true), (second.clone(), true)])
        .await
        .expect_err("one Team cannot use conflicting message limits");

    assert!(error.message.contains("incompatible collaboration metadata"));
    assert!(runtime.tasks().get(&first.task_id).is_none());
    assert!(runtime.tasks().get(&second.task_id).is_none());
    assert_eq!(
        runtime.agents().get(&first_agent).map(|agent| agent.state),
        Some(AgentLifecycleState::Cancelled)
    );
    assert_eq!(
        runtime.agents().get(&second_agent).map(|agent| agent.state),
        Some(AgentLifecycleState::Cancelled)
    );
    assert!(runtime.teams().get(&team_id).is_none());
    assert_eq!(runtime.agents().get(&root_agent).and_then(|agent| agent.team_id), None);
}

#[test]
fn strict_role_output_schema_rejects_prose_and_invalid_enum() {
    let schema = json!({
        "type": "object",
        "required": ["verdict"],
        "properties": {"verdict": {"enum": ["PASS", "FAIL"]}}
    });
    assert!(normalize_role_output("verifier", Some(&schema), "looks good").is_err());
    assert!(normalize_role_output("verifier", Some(&schema), r#"{"verdict":"MAYBE"}"#).is_err());
    assert_eq!(
        normalize_role_output("verifier", Some(&schema), r#"{"verdict":"PASS"}"#).unwrap()["verdict"],
        "PASS"
    );
}

#[test]
fn fenced_json_is_accepted_for_structured_roles() {
    let schema = json!({"type":"object", "required":["ok"], "properties":{"ok":{"type":"boolean"}}});
    let value = normalize_role_output("role", Some(&schema), "```json\n{\"ok\":true}\n```").unwrap();
    assert_eq!(value["ok"], true);
}

#[test]
fn one_fenced_json_object_surrounded_by_prose_is_accepted() {
    let schema = json!({"type":"object", "required":["ok"], "properties":{"ok":{"type":"boolean"}}});
    let value = normalize_role_output(
        "role",
        Some(&schema),
        "Here is the result:\n```json\n{\"ok\":true}\n```\nDone.",
    )
    .unwrap();
    assert_eq!(value["ok"], true);

    assert!(
        normalize_role_output(
            "role",
            Some(&schema),
            "```json\n{\"ok\":true}\n```\n```json\n{\"ok\":false}\n```",
        )
        .is_err()
    );
}

#[test]
fn plugin_contribution_selection_supports_global_and_per_node_values() {
    let parameters = json!({
        "plugin_contributions": {
            "provider": {"draft": "acme", "*": "fallback"},
            "before_hooks": {"draft": ["audit", "trace"]},
            "storage_backend": "sqlite"
        }
    });

    assert_eq!(
        plugin_contribution_name(&parameters, "draft", "provider").as_deref(),
        Some("acme")
    );
    assert_eq!(
        plugin_contribution_name(&parameters, "review", "provider").as_deref(),
        Some("fallback")
    );
    assert_eq!(
        plugin_contribution_name(&parameters, "draft", "storage_backend").as_deref(),
        Some("sqlite")
    );
    assert_eq!(
        plugin_contribution_names(&parameters, "draft", "before_hooks"),
        vec!["audit", "trace"]
    );
}

#[tokio::test]
async fn real_workflow_spawn_assigns_the_workflow_task_owner() {
    let runtime = Arc::new(CollaborationRuntime::new(Scheduler::new(ResourcePolicy::new(1))));
    let root_run = RunId::from("workflow-owner-root");
    let root_agent = AgentId::from("workflow-owner-agent");
    runtime.agents().upsert(AgentRecord {
        run_id: root_run.clone(),
        agent_id: root_agent.clone(),
        team_id: None,
        parent_agent_id: None,
        state: AgentLifecycleState::Active,
    });
    let config = test_config();
    let spawner = Arc::new(
        AgentSpawner::new(Arc::new(JsonProvider), config, std::env::temp_dir()).with_runtime_context(
            Arc::clone(&runtime),
            root_run.clone(),
            root_agent,
        ),
    );
    let roles = Arc::new(AgentRoleRegistry::default());
    roles.register(test_role());
    let controller = WorkflowController::with_runtime_and_roles(Arc::clone(&runtime), Some(Arc::clone(&roles)));
    controller
        .register(WorkflowDefinition {
            id: "owner-workflow".into(),
            schema_version: 1,
            version: "1".into(),
            description: "Owner integration".into(),
            roles: Vec::new(),
            parameters_schema: None,
            nodes: vec![WorkflowNode {
                id: "work".into(),
                depends_on: Vec::new(),
                when: None,
                role: Some("worker".into()),
                collaboration: CollaborationSelection::Fixed(CollaborationStrategy::Single),
                model_policy: ModelPolicy::default(),
                capability_scope: Vec::new(),
                permission_ceiling: PermissionCeiling::unrestricted(),
                retry: RetryPolicy { max_attempts: 1 },
                timeout_ms: None,
                output_bindings: Vec::new(),
                workflow_ref: None,
            }],
            outputs: Default::default(),
        })
        .unwrap();
    let workflow_run = RunId::from("workflow-owner-root:workflow:one");
    controller
        .start(workflow_run.clone(), "owner-workflow", json!({}))
        .unwrap();
    let settled = controller
        .execute_until_settled(&workflow_run, Arc::new(AgentWorkflowExecutor::new(spawner, roles)))
        .await
        .unwrap();
    assert_eq!(settled.status, WorkflowRunStatus::Completed, "{settled:#?}");
    let task_id = TaskId::from("workflow:workflow-owner-root:workflow:one:work");
    let task = runtime.tasks().get(&task_id).unwrap();
    assert!(task.owner_agent_id.is_some());
    assert!(runtime.agents().get(task.owner_agent_id.as_ref().unwrap()).is_some());
    assert!(
        runtime
            .ledger()
            .records_for_run(&root_run)
            .unwrap()
            .iter()
            .any(|record| {
                record.record_type == "task_cas"
                    && record.payload.get("transition").and_then(Value::as_str) == Some("assign")
            })
    );
}

#[path = "workflow_executor_integration_test.rs"]
mod workflow_executor_integration_test;

#[path = "workflow_executor_recovery_test.rs"]
mod workflow_executor_recovery_test;

include!("workflow_executor_v2_test.rs");
include!("workflow_executor_supervisor_completion_recovery_test.rs");
