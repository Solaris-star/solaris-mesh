use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use solaris_config::config::{CliArgs, Config};
use solaris_providers::{LlmProvider, ProviderError};
use solaris_types::identity::{ChildAgentKey, OperationId, TaskId};
use solaris_types::llm::{LlmEvent, LlmRequest};
use solaris_types::message::{StopReason, TokenUsage};
use solaris_types::permission::PermissionCeiling;
use solaris_types::resource::ResourceBudget;
use solaris_types::runtime::AgentLifecycleState;
use solaris_types::spawner::{AgentHandle, AgentOutcomeStatus, AgentSpawnSpec, ForkOverrides, SubAgentConfig};
use solaris_types::workflow::MultiAgentPolicy;
use tokio::sync::mpsc;

use crate::execution_context::stable_digest_value;

use super::*;

struct CountingProvider {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl LlmProvider for CountingProvider {
    async fn stream(&self, _request: &LlmRequest) -> Result<mpsc::Receiver<LlmEvent>, ProviderError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let (sender, receiver) = mpsc::channel(2);
        sender.send(LlmEvent::TextDelta("done".to_owned())).await.unwrap();
        sender
            .send(LlmEvent::Done {
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::default(),
            })
            .await
            .unwrap();
        Ok(receiver)
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
    config.session.enabled = false;
    config
}

fn test_spawner(calls: Arc<AtomicUsize>) -> Arc<AgentSpawner> {
    Arc::new(AgentSpawner::new(
        Arc::new(CountingProvider { calls }),
        test_config(),
        std::env::temp_dir(),
    ))
}

fn spawn_spec(spawner: &AgentSpawner, operation_id: &str) -> AgentSpawnSpec {
    AgentSpawnSpec {
        run_id: spawner.run_id().clone(),
        parent_agent_id: spawner.parent_agent_id().clone(),
        task_id: TaskId::new(format!("{operation_id}-task")),
        role_key: "worker".to_owned(),
        stable_task_key: format!("{operation_id}-task"),
        operation_id: OperationId::from(operation_id),
        expected_task_revision: None,
        config: SubAgentConfig {
            name: "supervised-worker".to_owned(),
            prompt: "execute once".to_owned(),
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

fn first_attempt_spec(spec: &AgentSpawnSpec) -> AgentSpawnSpec {
    let mut attempt = spec.clone();
    attempt.operation_id = OperationId::new(format!("{}:supervisor:1", spec.operation_id));
    attempt
}

fn handle_for_spec(spec: AgentSpawnSpec) -> AgentHandle {
    let key = ChildAgentKey {
        run_id: spec.run_id.clone(),
        parent_agent_id: spec.parent_agent_id.clone(),
        role_key: spec.role_key.clone(),
        stable_task_key: spec.stable_task_key.clone(),
        spawn_operation_id: spec.operation_id.clone(),
    };
    AgentHandle {
        run_id: spec.run_id.clone(),
        agent_id: key.agent_id(),
        identity_version: ChildAgentKey::CURRENT_IDENTITY_VERSION,
        task_id: spec.task_id.clone(),
        operation_id: spec.operation_id.clone(),
        role_key: spec.role_key.clone(),
        stable_task_key: spec.stable_task_key.clone(),
        spec_digest: stable_digest_value(&serde_json::to_value(&spec).unwrap()),
        spec,
    }
}

fn record_count(spawner: &AgentSpawner, record_type: &str) -> usize {
    spawner
        .lifecycle_runtime()
        .ledger()
        .records_for_run(spawner.run_id())
        .unwrap()
        .iter()
        .filter(|record| record.record_type == record_type)
        .count()
}

fn recorded_failure_class(spawner: &AgentSpawner) -> Option<String> {
    spawner
        .lifecycle_runtime()
        .ledger()
        .records_for_run(spawner.run_id())
        .unwrap()
        .iter()
        .find(|record| record.record_type == "supervisor_attempt_failed")
        .and_then(|record| record.payload.get("result"))
        .and_then(|result| result.get("output"))
        .and_then(|output| output.get("failure_class"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
}

fn child_count(spawner: &AgentSpawner) -> usize {
    spawner
        .lifecycle_runtime()
        .agents()
        .snapshot()
        .iter()
        .filter(|agent| agent.parent_agent_id.as_ref() == Some(spawner.parent_agent_id()))
        .count()
}

struct NonRetryableErrorProvider {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl LlmProvider for NonRetryableErrorProvider {
    async fn stream(&self, _request: &LlmRequest) -> Result<mpsc::Receiver<LlmEvent>, ProviderError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(ProviderError::Api {
            status: 400,
            message: "invalid request".to_owned(),
        })
    }
}

fn non_retryable_spawner(calls: Arc<AtomicUsize>) -> Arc<AgentSpawner> {
    Arc::new(AgentSpawner::new(
        Arc::new(NonRetryableErrorProvider { calls }),
        test_config(),
        std::env::temp_dir(),
    ))
}

#[tokio::test]
async fn non_retryable_child_failure_stops_supervisor_without_retry() {
    let provider_calls = Arc::new(AtomicUsize::new(0));
    let spawner = non_retryable_spawner(Arc::clone(&provider_calls));
    let spec = spawn_spec(&spawner, "supervisor-non-retryable-child");

    let result = SupervisorCoordinator::new(Arc::clone(&spawner)).execute(spec, 3).await;

    assert_eq!(result.status, AgentOutcomeStatus::Failed);
    assert_eq!(result.failure_class, Some(TaskFailureClass::NonRetryable));
    assert_eq!(
        result.output.as_ref().and_then(|output| output.get("failure_class")),
        Some(&serde_json::json!("non_retryable"))
    );
    // A typed NonRetryable child failure must not authorize a second attempt.
    assert_eq!(record_count(&spawner, "supervisor_assignment"), 1);
    assert_eq!(record_count(&spawner, "supervisor_attempt_failed"), 1);
    assert_eq!(recorded_failure_class(&spawner).as_deref(), Some("non_retryable"));
    assert_eq!(provider_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn outcome_unknown_spawn_stops_supervisor_without_a_second_operation() {
    let provider_calls = Arc::new(AtomicUsize::new(0));
    let spawner = test_spawner(Arc::clone(&provider_calls));
    let spec = spawn_spec(&spawner, "supervisor-outcome-unknown");
    spawner
        .record_lifecycle_effect_intent_for_test(&first_attempt_spec(&spec))
        .unwrap();

    let result = SupervisorCoordinator::new(Arc::clone(&spawner)).execute(spec, 3).await;

    assert_eq!(result.status, AgentOutcomeStatus::OutcomeUnknown);
    assert_eq!(
        result.output.as_ref().and_then(|output| output.get("failure_class")),
        Some(&serde_json::json!("outcome_unknown"))
    );
    assert_eq!(record_count(&spawner, "supervisor_assignment"), 1);
    assert_eq!(record_count(&spawner, "supervisor_attempt_failed"), 1);
    assert_eq!(recorded_failure_class(&spawner).as_deref(), Some("outcome_unknown"));
    assert_eq!(record_count(&spawner, "agent_handle_issued"), 1);
    assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
    assert_eq!(child_count(&spawner), 0);
}

#[tokio::test]
async fn non_retryable_spawn_stops_supervisor_after_one_assignment() {
    let provider_calls = Arc::new(AtomicUsize::new(0));
    let spawner = test_spawner(Arc::clone(&provider_calls));
    *spawner
        .multi_agent_policy_state()
        .write()
        .unwrap_or_else(|error| error.into_inner()) = MultiAgentPolicy::Disabled;

    let result = SupervisorCoordinator::new(Arc::clone(&spawner))
        .execute(spawn_spec(&spawner, "supervisor-disabled"), 3)
        .await;

    assert_eq!(result.status, AgentOutcomeStatus::Failed);
    assert_eq!(
        result.output.as_ref().and_then(|output| output.get("failure_class")),
        Some(&serde_json::json!("non_retryable"))
    );
    assert_eq!(record_count(&spawner, "supervisor_assignment"), 1);
    assert_eq!(record_count(&spawner, "supervisor_attempt_failed"), 1);
    assert_eq!(recorded_failure_class(&spawner).as_deref(), Some("non_retryable"));
    assert_eq!(record_count(&spawner, "agent_handle_issued"), 0);
    assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
    assert_eq!(child_count(&spawner), 0);
}

#[tokio::test]
async fn reconciliation_spawn_stops_supervisor_without_changing_operation_id() {
    let provider_calls = Arc::new(AtomicUsize::new(0));
    let spawner = test_spawner(Arc::clone(&provider_calls));
    let mut spec = spawn_spec(&spawner, "supervisor-reconciliation");
    let mut original_attempt = first_attempt_spec(&spec);
    original_attempt.config.prompt = "original immutable prompt".to_owned();
    spawner
        .lifecycle_runtime()
        .record_agent_handle(&handle_for_spec(original_attempt))
        .unwrap();
    spec.config.prompt = "conflicting immutable prompt".to_owned();

    let result = SupervisorCoordinator::new(Arc::clone(&spawner)).execute(spec, 3).await;

    assert_eq!(result.status, AgentOutcomeStatus::ReconciliationRequired);
    assert_eq!(
        result.output.as_ref().and_then(|output| output.get("failure_class")),
        Some(&serde_json::json!("reconciliation_required"))
    );
    assert_eq!(record_count(&spawner, "supervisor_assignment"), 1);
    assert_eq!(record_count(&spawner, "supervisor_attempt_failed"), 1);
    assert_eq!(
        recorded_failure_class(&spawner).as_deref(),
        Some("reconciliation_required")
    );
    assert_eq!(record_count(&spawner, "agent_handle_issued"), 1);
    assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
    assert_eq!(child_count(&spawner), 0);
    assert!(
        spawner
            .lifecycle_runtime()
            .agents()
            .snapshot()
            .iter()
            .all(|agent| agent.state != AgentLifecycleState::Reserved)
    );
}
