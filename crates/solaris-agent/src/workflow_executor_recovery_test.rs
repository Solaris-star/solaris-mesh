use super::*;

use solaris_types::runtime::{TaskFailureClass, TaskState};
use solaris_types::spawner::{AgentConversationHandle, AgentSpawnService};

struct IntentCountingProvider {
    calls: Arc<AtomicUsize>,
}

struct FailSpawnAbortLedger {
    inner: InMemoryRuntimeLedger,
}

impl RuntimeLedger for FailSpawnAbortLedger {
    crate::runtime_ledger::forward_compare_and_append!();

    fn append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        payload: Value,
    ) -> std::io::Result<LedgerRecord> {
        if record_type == "agent_spawn_aborted" {
            return Err(std::io::Error::other("injected spawn abort failure"));
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
impl LlmProvider for IntentCountingProvider {
    async fn stream(&self, _request: &LlmRequest) -> Result<mpsc::Receiver<LlmEvent>, ProviderError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        JsonProvider.stream(_request).await
    }
}

struct IntentOnlyWorkflowExecutor {
    spawner: Arc<AgentSpawner>,
    calls: AtomicUsize,
}

struct SpawnFailureWorkflowExecutor {
    spawner: Arc<AgentSpawner>,
    calls: AtomicUsize,
    operation_id: OperationId,
    stable_task_key: String,
    prompt: String,
    expected_task_revision: Option<u64>,
}

impl SpawnFailureWorkflowExecutor {
    fn spec(&self, context: &WorkflowExecutionContext) -> AgentSpawnSpec {
        AgentSpawnSpec {
            run_id: self.spawner.run_id().clone(),
            parent_agent_id: self.spawner.parent_agent_id().clone(),
            task_id: TaskId::new(format!("workflow:{}:{}", context.run_id, context.node.id)),
            role_key: "worker".to_owned(),
            stable_task_key: self.stable_task_key.clone(),
            operation_id: self.operation_id.clone(),
            expected_task_revision: self.expected_task_revision,
            config: SubAgentConfig {
                name: "failure-worker".to_owned(),
                prompt: self.prompt.clone(),
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
}

#[async_trait]
impl WorkflowNodeExecutor for SpawnFailureWorkflowExecutor {
    async fn execute(&self, context: WorkflowExecutionContext) -> Result<Value, WorkflowNodeError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.spawner
            .spawn(self.spec(&context))
            .await
            .map(|_| json!({"unexpected": "spawn succeeded"}))
            .map_err(WorkflowNodeError::from)
    }
}

fn recovery_workflow(id: &str) -> WorkflowDefinition {
    WorkflowDefinition {
        id: id.into(),
        schema_version: 1,
        version: "1".into(),
        description: "Spawn failure recovery classification".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![WorkflowNode {
            id: "work".into(),
            depends_on: Vec::new(),
            when: None,
            role: None,
            collaboration: CollaborationSelection::Inherit,
            model_policy: ModelPolicy::default(),
            capability_scope: Vec::new(),
            permission_ceiling: PermissionCeiling::unrestricted(),
            retry: RetryPolicy { max_attempts: 3 },
            timeout_ms: None,
            output_bindings: Vec::new(),
            workflow_ref: None,
        }],
        outputs: Default::default(),
    }
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
        spec_digest: crate::execution_context::stable_digest_value(&serde_json::to_value(&spec).unwrap()),
        spec,
    }
}

#[async_trait]
impl WorkflowNodeExecutor for IntentOnlyWorkflowExecutor {
    async fn execute(&self, context: WorkflowExecutionContext) -> Result<Value, WorkflowNodeError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let task_id = TaskId::new(format!("workflow:{}:{}", context.run_id, context.node.id));
        let operation_id = OperationId::new(format!("intent-only:{}", context.attempt_id));
        let spec = AgentSpawnSpec {
            run_id: self.spawner.run_id().clone(),
            parent_agent_id: self.spawner.parent_agent_id().clone(),
            task_id: task_id.clone(),
            role_key: "worker".to_owned(),
            stable_task_key: format!("intent-only:{}", context.attempt_id),
            operation_id,
            expected_task_revision: self
                .spawner
                .lifecycle_runtime()
                .tasks()
                .get(&task_id)
                .map(|task| task.revision),
            config: SubAgentConfig {
                name: "intent-only-worker".to_owned(),
                prompt: "must not execute twice".to_owned(),
                max_turns: 1,
                max_tokens: 64,
                system_prompt: None,
            },
            overrides: ForkOverrides::default(),
            permission_ceiling: PermissionCeiling::unrestricted(),
            resource_budget: ResourceBudget::default(),
            context_policy: None,
            recursion_limit: None,
        };
        if call == 0 {
            self.spawner.record_lifecycle_effect_intent_for_test(&spec)?;
        }
        let handle = self.spawner.spawn(spec).await.map_err(WorkflowNodeError::from)?;
        let outcome = self
            .spawner
            .join(&handle)
            .await
            .map_err(WorkflowNodeError::reconciliation_required)?;
        if outcome.status == AgentOutcomeStatus::Completed {
            Ok(outcome.output)
        } else {
            Err(WorkflowNodeError::retryable(
                outcome.error.unwrap_or_else(|| "child failed".to_owned()),
            ))
        }
    }
}

#[tokio::test]
async fn intent_only_spawn_recovery_is_outcome_unknown_and_stops_workflow_retry() {
    let ledger: Arc<dyn RuntimeLedger> = Arc::new(InMemoryRuntimeLedger::default());
    let root_run = RunId::from("intent-only-root");
    let workflow_run = RunId::from("intent-only-root:workflow:one");
    let root_agent = AgentId::from("intent-only-root-agent");
    let runtime = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(4)),
        Arc::clone(&ledger),
    ));
    runtime.agents().upsert(AgentRecord {
        run_id: root_run.clone(),
        agent_id: root_agent.clone(),
        team_id: None,
        parent_agent_id: None,
        state: AgentLifecycleState::Active,
    });
    let provider_calls = Arc::new(AtomicUsize::new(0));
    let spawner = Arc::new(
        AgentSpawner::new(
            Arc::new(IntentCountingProvider {
                calls: Arc::clone(&provider_calls),
            }),
            test_config(),
            std::env::temp_dir(),
        )
        .with_runtime_context(Arc::clone(&runtime), root_run.clone(), root_agent),
    );
    let executor = Arc::new(IntentOnlyWorkflowExecutor {
        spawner,
        calls: AtomicUsize::new(0),
    });
    let controller = WorkflowController::with_runtime_and_roles(Arc::clone(&runtime), None);
    controller
        .register(WorkflowDefinition {
            id: "intent-only-workflow".into(),
            schema_version: 1,
            version: "1".into(),
            description: "Intent-only recovery".into(),
            roles: Vec::new(),
            parameters_schema: None,
            nodes: vec![WorkflowNode {
                id: "work".into(),
                depends_on: Vec::new(),
                when: None,
                role: None,
                collaboration: CollaborationSelection::Inherit,
                model_policy: ModelPolicy::default(),
                capability_scope: Vec::new(),
                permission_ceiling: PermissionCeiling::unrestricted(),
                retry: RetryPolicy { max_attempts: 3 },
                timeout_ms: None,
                output_bindings: Vec::new(),
                workflow_ref: None,
            }],
            outputs: Default::default(),
        })
        .unwrap();
    controller
        .start(workflow_run.clone(), "intent-only-workflow", json!({}))
        .unwrap();

    let settled = controller
        .execute_until_settled(&workflow_run, executor.clone())
        .await
        .unwrap();

    assert_eq!(settled.status, WorkflowRunStatus::Failed);
    assert_eq!(settled.nodes["work"].attempt_number, 1);
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
    assert_eq!(runtime.agents().snapshot().len(), 1);
    let task = runtime
        .tasks()
        .get(&TaskId::from("workflow:intent-only-root:workflow:one:work"))
        .unwrap();
    assert_eq!(task.state, TaskState::Failed);
    assert_eq!(task.failure_class, Some(TaskFailureClass::OutcomeUnknown));
    let records = ledger.records_for_run(&root_run).unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|record| record.record_type == "effect_intent")
            .count(),
        1
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| record.record_type == "agent_spawn_intent")
            .count(),
        0
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| record.record_type == "agent_handle_issued")
            .count(),
        1
    );
}

#[tokio::test]
async fn disabled_spawn_is_non_retryable_for_a_multi_attempt_workflow() {
    let ledger: Arc<dyn RuntimeLedger> = Arc::new(InMemoryRuntimeLedger::default());
    let root_run = RunId::from("disabled-spawn-root");
    let workflow_run = RunId::from("disabled-spawn-root:workflow:one");
    let root_agent = AgentId::from("disabled-spawn-root-agent");
    let runtime = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(2)),
        ledger,
    ));
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
            root_run,
            root_agent,
        ),
    );
    *spawner
        .multi_agent_policy_state()
        .write()
        .unwrap_or_else(|error| error.into_inner()) = solaris_types::workflow::MultiAgentPolicy::Disabled;
    let executor = Arc::new(SpawnFailureWorkflowExecutor {
        spawner,
        calls: AtomicUsize::new(0),
        operation_id: OperationId::from("disabled-spawn-operation"),
        stable_task_key: "disabled-spawn-task".into(),
        prompt: "must be rejected".into(),
        expected_task_revision: None,
    });
    let controller = WorkflowController::with_runtime_and_roles(Arc::clone(&runtime), None);
    controller
        .register(recovery_workflow("disabled-spawn-workflow"))
        .unwrap();
    controller
        .start(workflow_run.clone(), "disabled-spawn-workflow", json!({}))
        .unwrap();

    let settled = controller
        .execute_until_settled(&workflow_run, executor.clone())
        .await
        .unwrap();

    assert_eq!(settled.status, WorkflowRunStatus::Failed);
    assert_eq!(settled.nodes["work"].attempt_number, 1);
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.agents().snapshot().len(), 1);
    assert_eq!(
        runtime
            .tasks()
            .get(&TaskId::from("workflow:disabled-spawn-root:workflow:one:work"))
            .unwrap()
            .failure_class,
        Some(TaskFailureClass::NonRetryable)
    );
}

#[tokio::test]
async fn immutable_spawn_spec_conflict_requires_reconciliation_without_workflow_retry() {
    let ledger: Arc<dyn RuntimeLedger> = Arc::new(InMemoryRuntimeLedger::default());
    let root_run = RunId::from("spawn-spec-conflict-root");
    let workflow_run = RunId::from("spawn-spec-conflict-root:workflow:one");
    let root_agent = AgentId::from("spawn-spec-conflict-root-agent");
    let runtime = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(2)),
        Arc::clone(&ledger),
    ));
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
    let controller = WorkflowController::with_runtime_and_roles(Arc::clone(&runtime), None);
    controller
        .register(recovery_workflow("spawn-spec-conflict-workflow"))
        .unwrap();
    controller
        .start(workflow_run.clone(), "spawn-spec-conflict-workflow", json!({}))
        .unwrap();
    let executor = Arc::new(SpawnFailureWorkflowExecutor {
        spawner: Arc::clone(&spawner),
        calls: AtomicUsize::new(0),
        operation_id: OperationId::from("spawn-spec-conflict-operation"),
        stable_task_key: "spawn-spec-conflict-task".into(),
        prompt: "changed prompt".into(),
        expected_task_revision: None,
    });
    let seed_context = WorkflowExecutionContext {
        run_id: workflow_run.clone(),
        workflow: solaris_types::plugin::ImplementationIdentity {
            implementation_id: "workflow:spawn-spec-conflict-workflow".into(),
            version: Some("1".into()),
            digest: None,
        },
        node: recovery_workflow("spawn-spec-conflict-workflow").nodes.remove(0),
        attempt_id: solaris_types::identity::AttemptId::from("seed-attempt"),
        parameters: json!({}),
        dependency_outputs: Default::default(),
        bound_inputs: json!({}),
    };
    let mut seed_spec = executor.spec(&seed_context);
    seed_spec.config.prompt = "original prompt".into();
    runtime.record_agent_handle(&handle_for_spec(seed_spec)).unwrap();

    let settled = controller
        .execute_until_settled(&workflow_run, executor.clone())
        .await
        .unwrap();

    assert_eq!(settled.status, WorkflowRunStatus::Failed);
    assert_eq!(settled.nodes["work"].attempt_number, 1);
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.agents().snapshot().len(), 1);
    assert_eq!(
        runtime
            .tasks()
            .get(&TaskId::from("workflow:spawn-spec-conflict-root:workflow:one:work"))
            .unwrap()
            .failure_class,
        Some(TaskFailureClass::ReconciliationRequired)
    );
    assert_eq!(
        ledger
            .records_for_run(&root_run)
            .unwrap()
            .iter()
            .filter(|record| record.record_type == "agent_spawn_intent")
            .count(),
        0
    );
}

#[tokio::test]
async fn stale_task_assignment_aborts_reservation_and_stops_workflow_retry() {
    let ledger: Arc<dyn RuntimeLedger> = Arc::new(InMemoryRuntimeLedger::default());
    let root_run = RunId::from("stale-task-assign-root");
    let workflow_run = RunId::from("stale-task-assign-root:workflow:one");
    let root_agent = AgentId::from("stale-task-assign-root-agent");
    let runtime = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(2)),
        Arc::clone(&ledger),
    ));
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
            root_agent,
        ),
    );
    let executor = Arc::new(SpawnFailureWorkflowExecutor {
        spawner,
        calls: AtomicUsize::new(0),
        operation_id: OperationId::from("stale-task-assign-operation"),
        stable_task_key: "stale-task-assign-task".into(),
        prompt: "must not reserve twice".into(),
        expected_task_revision: Some(99),
    });
    let controller = WorkflowController::with_runtime_and_roles(Arc::clone(&runtime), None);
    controller
        .register(recovery_workflow("stale-task-assign-workflow"))
        .unwrap();
    controller
        .start(workflow_run.clone(), "stale-task-assign-workflow", json!({}))
        .unwrap();

    let settled = controller
        .execute_until_settled(&workflow_run, executor.clone())
        .await
        .unwrap();

    assert_eq!(settled.status, WorkflowRunStatus::Failed);
    assert_eq!(settled.nodes["work"].attempt_number, 1);
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.agents().snapshot().len(), 1);
    assert_eq!(
        runtime
            .tasks()
            .get(&TaskId::from("workflow:stale-task-assign-root:workflow:one:work"))
            .unwrap()
            .failure_class,
        Some(TaskFailureClass::ReconciliationRequired)
    );
    let records = ledger.records_for_run(&root_run).unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|record| record.record_type == "agent_spawn_intent")
            .count(),
        1
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| record.record_type == "agent_spawn_aborted")
            .count(),
        1
    );
}

#[tokio::test]
async fn reservation_cleanup_failure_preserves_assignment_error_and_stops_retry() {
    let ledger: Arc<dyn RuntimeLedger> = Arc::new(FailSpawnAbortLedger {
        inner: InMemoryRuntimeLedger::default(),
    });
    let root_run = RunId::from("spawn-abort-failure-root");
    let workflow_run = RunId::from("spawn-abort-failure-root:workflow:one");
    let root_agent = AgentId::from("spawn-abort-failure-root-agent");
    let runtime = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(2)),
        ledger,
    ));
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
            root_run,
            root_agent,
        ),
    );
    let executor = Arc::new(SpawnFailureWorkflowExecutor {
        spawner,
        calls: AtomicUsize::new(0),
        operation_id: OperationId::from("spawn-abort-failure-operation"),
        stable_task_key: "spawn-abort-failure-task".into(),
        prompt: "surface both failures".into(),
        expected_task_revision: Some(99),
    });
    let controller = WorkflowController::with_runtime_and_roles(Arc::clone(&runtime), None);
    controller
        .register(recovery_workflow("spawn-abort-failure-workflow"))
        .unwrap();
    controller
        .start(workflow_run.clone(), "spawn-abort-failure-workflow", json!({}))
        .unwrap();

    let settled = controller
        .execute_until_settled(&workflow_run, executor.clone())
        .await
        .unwrap();

    assert_eq!(settled.nodes["work"].attempt_number, 1);
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    let error = settled.nodes["work"].error.as_deref().unwrap();
    assert!(error.contains("stale task revision"));
    assert!(error.contains("reservation cleanup failed: injected spawn abort failure"));
    assert_eq!(
        runtime
            .agents()
            .snapshot()
            .iter()
            .filter(|agent| agent.state == AgentLifecycleState::Reserved)
            .count(),
        1,
        "the injected durable cleanup failure must remain visible for reconciliation"
    );
}

#[path = "workflow_executor_supervisor_recovery_test.rs"]
mod workflow_executor_supervisor_recovery_test;

#[path = "spawner_task_assignment_recovery_test.rs"]
mod spawner_task_assignment_recovery_test;
