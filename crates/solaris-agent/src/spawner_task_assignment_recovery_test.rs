use super::*;

use solaris_types::runtime::TaskRecord;

struct TaskAssignmentCrashLedger {
    inner: InMemoryRuntimeLedger,
    task_append_failed: AtomicBool,
    remaining_read_failures: AtomicUsize,
    reads_blocked: AtomicBool,
    terminal_append_failure_enabled: AtomicBool,
    terminal_append_failed: AtomicBool,
    marker_append_failure_enabled: AtomicBool,
    marker_append_failed: AtomicBool,
    post_append_read_failures: AtomicUsize,
}

impl TaskAssignmentCrashLedger {
    fn with_read_failures(read_failures: usize) -> Self {
        Self {
            inner: InMemoryRuntimeLedger::default(),
            task_append_failed: AtomicBool::new(false),
            remaining_read_failures: AtomicUsize::new(read_failures),
            reads_blocked: AtomicBool::new(false),
            terminal_append_failure_enabled: AtomicBool::new(false),
            terminal_append_failed: AtomicBool::new(false),
            marker_append_failure_enabled: AtomicBool::new(false),
            marker_append_failed: AtomicBool::new(false),
            post_append_read_failures: AtomicUsize::new(0),
        }
    }

    fn block_reads(&self) {
        self.reads_blocked.store(true, Ordering::SeqCst);
    }

    fn allow_reads(&self) {
        self.reads_blocked.store(false, Ordering::SeqCst);
    }

    fn fail_terminal_append_once(&self) {
        self.terminal_append_failure_enabled.store(true, Ordering::SeqCst);
    }

    fn fail_marker_append_once(&self) {
        self.marker_append_failure_enabled.store(true, Ordering::SeqCst);
    }

    fn after_append(&self, record_type: &str, payload: &Value) -> std::io::Result<()> {
        if record_type == "task_cas"
            && payload.get("transition").and_then(Value::as_str) == Some("assign")
            && !self.task_append_failed.swap(true, Ordering::SeqCst)
        {
            return Err(std::io::Error::other(
                "injected failure after durable task assignment append",
            ));
        }
        if record_type == "task_cas"
            && payload.get("transition").and_then(Value::as_str) == Some("workflow_state")
            && self.terminal_append_failure_enabled.load(Ordering::SeqCst)
            && !self.terminal_append_failed.swap(true, Ordering::SeqCst)
        {
            self.post_append_read_failures.store(1, Ordering::SeqCst);
            return Err(std::io::Error::other(
                "injected failure after durable terminal task CAS append",
            ));
        }
        if record_type == "workflow_task_terminal_reconciled"
            && self.marker_append_failure_enabled.load(Ordering::SeqCst)
            && !self.marker_append_failed.swap(true, Ordering::SeqCst)
        {
            self.post_append_read_failures.store(1, Ordering::SeqCst);
            return Err(std::io::Error::other(
                "injected failure after durable terminal reconciliation marker append",
            ));
        }
        Ok(())
    }
}

impl RuntimeLedger for TaskAssignmentCrashLedger {
    fn logical_append_capability(&self) -> crate::runtime_ledger::LogicalAppendCapability {
        self.inner.logical_append_capability()
    }

    fn acquire_workflow_mutation_lease(
        &self,
        run_id: &RunId,
        owner_id: &str,
        now_unix_ms: i64,
    ) -> std::io::Result<crate::runtime_ledger::WorkflowMutationLease> {
        self.inner
            .acquire_workflow_mutation_lease(run_id, owner_id, now_unix_ms)
    }

    fn renew_workflow_mutation_lease(
        &self,
        lease: &crate::runtime_ledger::WorkflowMutationLease,
        now_unix_ms: i64,
    ) -> std::io::Result<crate::runtime_ledger::WorkflowMutationLease> {
        self.inner.renew_workflow_mutation_lease(lease, now_unix_ms)
    }

    fn commit_workflow_restore(
        &self,
        lease: &crate::runtime_ledger::WorkflowMutationLease,
        expected_sequence: u64,
        now_unix_ms: i64,
    ) -> std::io::Result<crate::runtime_ledger::WorkflowRestoreCommit> {
        self.inner
            .commit_workflow_restore(lease, expected_sequence, now_unix_ms)
    }

    fn release_workflow_mutation_lease(
        &self,
        lease: &crate::runtime_ledger::WorkflowMutationLease,
    ) -> std::io::Result<()> {
        self.inner.release_workflow_mutation_lease(lease)
    }

    fn append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        payload: Value,
    ) -> std::io::Result<LedgerRecord> {
        let record = self.inner.append(run_id, durability, record_type, payload.clone())?;
        self.after_append(record_type, &payload)?;
        Ok(record)
    }

    fn append_under_workflow_lease(
        &self,
        lease: &crate::runtime_ledger::WorkflowMutationLease,
        now_unix_ms: i64,
        durability: DurabilityClass,
        record_type: &str,
        payload: Value,
    ) -> std::io::Result<LedgerRecord> {
        let record =
            self.inner
                .append_under_workflow_lease(lease, now_unix_ms, durability, record_type, payload.clone())?;
        self.after_append(record_type, &payload)?;
        Ok(record)
    }

    fn compare_and_append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        identity_fields: &[&str],
        payload: Value,
    ) -> std::io::Result<LedgerRecord> {
        self.inner
            .compare_and_append(run_id, durability, record_type, identity_fields, payload)
    }

    fn compare_and_append_under_workflow_lease(
        &self,
        lease: &crate::runtime_ledger::WorkflowMutationLease,
        now_unix_ms: i64,
        durability: DurabilityClass,
        record_type: &str,
        identity_fields: &[&str],
        payload: Value,
    ) -> std::io::Result<LedgerRecord> {
        self.inner.compare_and_append_under_workflow_lease(
            lease,
            now_unix_ms,
            durability,
            record_type,
            identity_fields,
            payload,
        )
    }

    fn admit_tasks_and_append_under_workflow_lease(
        &self,
        lease: &crate::runtime_ledger::WorkflowMutationLease,
        now_unix_ms: i64,
        root_run_id: &RunId,
        max_tasks: usize,
        tasks: &[TaskRecord],
        records: &[(DurabilityClass, String, Value)],
    ) -> std::io::Result<Vec<LedgerRecord>> {
        self.inner.admit_tasks_and_append_under_workflow_lease(
            lease,
            now_unix_ms,
            root_run_id,
            max_tasks,
            tasks,
            records,
        )
    }

    fn run_ids(&self) -> std::io::Result<Vec<RunId>> {
        self.inner.run_ids()
    }

    fn records_for_run(&self, run_id: &RunId) -> std::io::Result<Vec<LedgerRecord>> {
        if self
            .post_append_read_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| remaining.checked_sub(1))
            .is_ok()
        {
            return Err(std::io::Error::other("injected post-append durable lookup failure"));
        }
        if self.task_append_failed.load(Ordering::SeqCst)
            && (self.reads_blocked.load(Ordering::SeqCst)
                || self
                    .remaining_read_failures
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| remaining.checked_sub(1))
                    .is_ok())
        {
            return Err(std::io::Error::other("injected durable assignment lookup failure"));
        }
        self.inner.records_for_run(run_id)
    }

    fn effect_output_root(&self) -> Option<std::path::PathBuf> {
        self.inner.effect_output_root()
    }
}

struct AssignmentFixture {
    runtime: Arc<CollaborationRuntime<()>>,
    spawner: AgentSpawner,
    run_id: RunId,
    task_id: TaskId,
    expected_agent_id: AgentId,
    spec: AgentSpawnSpec,
}

fn queued_task(run_id: &RunId, task_id: &TaskId) -> TaskRecord {
    TaskRecord {
        run_id: run_id.clone(),
        task_id: task_id.clone(),
        revision: 0,
        task_key: Some("spawn-crash-task".to_owned()),
        team_id: None,
        workflow_id: None,
        node_id: None,
        role: Some("worker".to_owned()),
        depends_on: Vec::new(),
        content: None,
        expected_write_scope: Vec::new(),
        owner_agent_id: None,
        state: TaskState::Queued,
        outcome_ref: None,
        failure_class: None,
    }
}

fn assignment_fixture(runtime_ledger: Arc<dyn RuntimeLedger>, prefix: &str) -> AssignmentFixture {
    let run_id = RunId::new(format!("{prefix}-root"));
    let root_agent = AgentId::new(format!("{prefix}-root-agent"));
    let task_id = TaskId::new(format!("{prefix}-task"));
    let runtime = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(2)),
        runtime_ledger,
    ));
    runtime.agents().upsert(AgentRecord {
        run_id: run_id.clone(),
        agent_id: root_agent.clone(),
        team_id: None,
        parent_agent_id: None,
        state: AgentLifecycleState::Active,
    });
    runtime
        .register_runtime_task(&run_id, queued_task(&run_id, &task_id))
        .unwrap();
    let spawner = AgentSpawner::new(Arc::new(JsonProvider), test_config(), std::env::temp_dir()).with_runtime_context(
        Arc::clone(&runtime),
        run_id.clone(),
        root_agent,
    );
    let spec = AgentSpawnSpec {
        run_id: run_id.clone(),
        parent_agent_id: spawner.parent_agent_id().clone(),
        task_id: task_id.clone(),
        role_key: "worker".to_owned(),
        stable_task_key: format!("{prefix}-stable-task"),
        operation_id: OperationId::new(format!("{prefix}-operation")),
        expected_task_revision: Some(0),
        config: SubAgentConfig {
            name: "crash-boundary-worker".to_owned(),
            prompt: "reserve only".to_owned(),
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
    let expected_agent_id = ChildAgentKey {
        run_id: spec.run_id.clone(),
        parent_agent_id: spec.parent_agent_id.clone(),
        role_key: spec.role_key.clone(),
        stable_task_key: spec.stable_task_key.clone(),
        spawn_operation_id: spec.operation_id.clone(),
    }
    .agent_id();
    AssignmentFixture {
        runtime,
        spawner,
        run_id,
        task_id,
        expected_agent_id,
        spec,
    }
}

fn assert_restored_assignment(
    runtime_ledger: Arc<dyn RuntimeLedger>,
    run_id: &RunId,
    task_id: &TaskId,
    expected_agent_id: &AgentId,
) {
    let restored: CollaborationRuntime<()> =
        CollaborationRuntime::with_ledger(Scheduler::new(ResourcePolicy::new(2)), runtime_ledger);
    restored.restore_projection(run_id).unwrap();
    let restored_task = restored.tasks().get(task_id).unwrap();
    assert_eq!(restored_task.revision, 1);
    assert_eq!(restored_task.owner_agent_id.as_ref(), Some(expected_agent_id));
    assert_eq!(
        restored.agents().get(expected_agent_id).map(|agent| agent.state),
        Some(AgentLifecycleState::Reserved)
    );
}

#[tokio::test]
async fn spawn_replays_task_assignment_after_append_error_without_aborting_reservation() {
    let ledger = Arc::new(TaskAssignmentCrashLedger::with_read_failures(0));
    let runtime_ledger: Arc<dyn RuntimeLedger> = ledger.clone();
    let fixture = assignment_fixture(Arc::clone(&runtime_ledger), "spawn-task-append-crash");

    let handle = fixture
        .spawner
        .spawn(fixture.spec)
        .await
        .expect("exact CAS replay must recover the assignment");

    let assigned = fixture.runtime.tasks().get(&fixture.task_id).unwrap();
    assert_eq!(assigned.revision, 1);
    assert_eq!(assigned.state, TaskState::Assigned);
    assert_eq!(assigned.owner_agent_id.as_ref(), Some(&handle.agent_id));
    let records = ledger.records_for_run(&fixture.run_id).unwrap();
    assert_eq!(
        records.iter().filter(|record| record.record_type == "task_cas").count(),
        1
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| record.record_type == "agent_spawn_aborted")
            .count(),
        0
    );
    assert_restored_assignment(runtime_ledger, &fixture.run_id, &fixture.task_id, &handle.agent_id);
}

async fn assert_uncertain_assignment_is_preserved(read_failures: usize, expected_error: &str) {
    let ledger = Arc::new(TaskAssignmentCrashLedger::with_read_failures(read_failures));
    let runtime_ledger: Arc<dyn RuntimeLedger> = ledger.clone();
    let fixture = assignment_fixture(
        Arc::clone(&runtime_ledger),
        &format!("uncertain-assignment-{read_failures}"),
    );

    let error = fixture
        .spawner
        .spawn(fixture.spec)
        .await
        .expect_err("assignment must remain uncertain");

    assert_eq!(error.failure_class, TaskFailureClass::ReconciliationRequired);
    assert!(error.message.contains(expected_error), "{}", error.message);
    assert_eq!(
        fixture
            .runtime
            .agents()
            .get(&fixture.expected_agent_id)
            .map(|agent| agent.state),
        Some(AgentLifecycleState::Reserved)
    );
    let records = ledger.records_for_run(&fixture.run_id).unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|record| record.record_type == "agent_spawn_aborted")
            .count(),
        0
    );
    assert_restored_assignment(
        runtime_ledger,
        &fixture.run_id,
        &fixture.task_id,
        &fixture.expected_agent_id,
    );
}

#[tokio::test]
async fn durable_assignment_found_after_failed_replay_preserves_reservation_for_reconciliation() {
    assert_uncertain_assignment_is_preserved(1, "exact CAS replay failed").await;
}

#[tokio::test]
async fn failed_durable_lookup_preserves_reservation_until_restart_reconciliation() {
    assert_uncertain_assignment_is_preserved(2, "durable assignment lookup failed").await;
}

#[tokio::test]
async fn workflow_recovers_durable_task_assignment_after_append_error_without_a_second_attempt() {
    let ledger = Arc::new(TaskAssignmentCrashLedger::with_read_failures(0));
    let runtime_ledger: Arc<dyn RuntimeLedger> = ledger.clone();
    let root_run = RunId::from("workflow-task-append-crash-root");
    let workflow_run = RunId::from("workflow-task-append-crash-root:workflow:one");
    let root_agent = AgentId::from("workflow-task-append-crash-root-agent");
    let runtime = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(2)),
        Arc::clone(&runtime_ledger),
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
        operation_id: OperationId::from("workflow-task-append-crash-operation"),
        stable_task_key: "workflow-task-append-crash-task".into(),
        prompt: "reserve once".into(),
        expected_task_revision: Some(0),
    });
    let controller = WorkflowController::with_runtime_and_roles(Arc::clone(&runtime), None);
    controller
        .register(recovery_workflow("workflow-task-append-crash"))
        .unwrap();
    controller
        .start(workflow_run.clone(), "workflow-task-append-crash", json!({}))
        .unwrap();

    let settled = controller
        .execute_until_settled(&workflow_run, executor.clone())
        .await
        .unwrap();

    assert_eq!(settled.status, WorkflowRunStatus::Completed);
    assert_eq!(settled.nodes["work"].attempt_number, 1);
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    let task_id = TaskId::from("workflow:workflow-task-append-crash-root:workflow:one:work");
    let task = runtime.tasks().get(&task_id).unwrap();
    let owner = task.owner_agent_id.clone().expect("durable child owner");
    assert_eq!(
        runtime.agents().get(&owner).map(|agent| agent.state),
        Some(AgentLifecycleState::Reserved)
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
        0
    );

    let restored: CollaborationRuntime<()> =
        CollaborationRuntime::with_ledger(Scheduler::new(ResourcePolicy::new(2)), runtime_ledger);
    restored.restore_projection(&root_run).unwrap();
    let restored_task = restored.tasks().get(&task_id).unwrap();
    assert_eq!(restored_task.state, TaskState::Assigned);
    assert_eq!(restored_task.owner_agent_id.as_ref(), Some(&owner));
    assert_eq!(
        restored.agents().get(&owner).map(|agent| agent.state),
        Some(AgentLifecycleState::Reserved)
    );
}

fn assert_workflow_restore_order(
    runtime_ledger: Arc<dyn RuntimeLedger>,
    first: &RunId,
    second: &RunId,
    task_id: &TaskId,
    owner: &AgentId,
) {
    let restored: CollaborationRuntime<()> =
        CollaborationRuntime::with_ledger(Scheduler::new(ResourcePolicy::new(2)), runtime_ledger);
    restored.restore_projection(first).unwrap();
    restored.restore_projection(second).unwrap();
    let task = restored.tasks().get(task_id).unwrap();
    assert_eq!(task.revision, 2);
    assert_eq!(task.state, TaskState::Failed);
    assert_eq!(task.owner_agent_id.as_ref(), Some(owner));
    assert_eq!(task.failure_class, Some(TaskFailureClass::ReconciliationRequired));
    assert_eq!(
        restored.agents().get(owner).map(|agent| agent.state),
        Some(AgentLifecycleState::Reserved)
    );
}

async fn assert_workflow_reconciles_uncertain_assignment(read_failures: usize) {
    let ledger = Arc::new(TaskAssignmentCrashLedger::with_read_failures(read_failures));
    let runtime_ledger: Arc<dyn RuntimeLedger> = ledger.clone();
    let root_run = RunId::new(format!("workflow-uncertain-{read_failures}-root"));
    let workflow_run = RunId::new(format!("{root_run}:workflow:one"));
    let root_agent = AgentId::new(format!("workflow-uncertain-{read_failures}-root-agent"));
    let runtime = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(2)),
        Arc::clone(&runtime_ledger),
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
        operation_id: OperationId::new(format!("workflow-uncertain-{read_failures}-operation")),
        stable_task_key: format!("workflow-uncertain-{read_failures}-task"),
        prompt: "do not retry uncertain assignment".into(),
        expected_task_revision: Some(0),
    });
    let workflow_id = format!("workflow-uncertain-{read_failures}");
    let controller = WorkflowController::with_runtime_and_roles(Arc::clone(&runtime), None);
    controller.register(recovery_workflow(&workflow_id)).unwrap();
    controller.start(workflow_run.clone(), &workflow_id, json!({})).unwrap();

    let settled = controller
        .execute_until_settled(&workflow_run, executor.clone())
        .await
        .unwrap();

    assert_eq!(settled.status, WorkflowRunStatus::Failed);
    assert_eq!(settled.nodes["work"].attempt_number, 1);
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    let task_id = TaskId::new(format!("workflow:{workflow_run}:work"));
    let task = runtime.tasks().get(&task_id).unwrap();
    let owner = task
        .owner_agent_id
        .clone()
        .expect("durable child owner must survive failure");
    assert_eq!(task.revision, 2);
    assert_eq!(task.state, TaskState::Failed);
    assert_eq!(task.failure_class, Some(TaskFailureClass::ReconciliationRequired));
    assert_eq!(
        runtime.agents().get(&owner).map(|agent| agent.state),
        Some(AgentLifecycleState::Reserved)
    );
    assert_eq!(
        runtime
            .agents()
            .snapshot()
            .iter()
            .filter(|agent| agent.parent_agent_id.is_some())
            .count(),
        1
    );
    let root_records = ledger.records_for_run(&root_run).unwrap();
    assert_eq!(
        root_records
            .iter()
            .filter(|record| record.record_type == "agent_spawn_intent")
            .count(),
        1
    );
    assert_eq!(
        root_records
            .iter()
            .filter(|record| record.record_type == "agent_spawn_aborted")
            .count(),
        0
    );
    assert_eq!(
        root_records
            .iter()
            .filter(|record| record.record_type == "task_cas")
            .count(),
        1
    );
    let workflow_records = ledger.records_for_run(&workflow_run).unwrap();
    assert_eq!(
        workflow_records
            .iter()
            .filter(|record| {
                record.record_type == "task_cas"
                    && record.payload.get("expected_revision").and_then(Value::as_u64) == Some(0)
            })
            .count(),
        0,
        "Workflow must not write a terminal CAS from the stale revision"
    );
    assert!(workflow_records.iter().any(|record| {
        record.record_type == "task_cas"
            && record.payload.get("expected_revision").and_then(Value::as_u64) == Some(1)
            && record.payload.get("new_revision").and_then(Value::as_u64) == Some(2)
    }));
    assert_workflow_restore_order(Arc::clone(&runtime_ledger), &root_run, &workflow_run, &task_id, &owner);
    assert_workflow_restore_order(runtime_ledger, &workflow_run, &root_run, &task_id, &owner);
}

#[tokio::test]
async fn workflow_restores_durable_assignment_after_one_failed_lookup_before_failing_node() {
    assert_workflow_reconciles_uncertain_assignment(1).await;
}

#[tokio::test]
async fn workflow_restores_durable_assignment_after_two_failed_lookups_before_failing_node() {
    assert_workflow_reconciles_uncertain_assignment(2).await;
}

struct NoDispatchWorkflowExecutor {
    calls: AtomicUsize,
}

#[async_trait]
impl WorkflowNodeExecutor for NoDispatchWorkflowExecutor {
    async fn execute(&self, _context: WorkflowExecutionContext) -> Result<Value, WorkflowNodeError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(WorkflowNodeError::non_retryable(
            "deferred recovery must not dispatch a second Agent",
        ))
    }
}

#[derive(Clone, Copy)]
enum DeferredRecoveryCrash {
    TerminalTaskCas,
    ReconciledMarker,
}

async fn assert_deferred_terminal_recovery_order(root_first: bool, crash: Option<DeferredRecoveryCrash>) {
    let ledger = Arc::new(TaskAssignmentCrashLedger::with_read_failures(0));
    ledger.block_reads();
    let runtime_ledger: Arc<dyn RuntimeLedger> = ledger.clone();
    let root_run = RunId::from("workflow-deferred-root");
    let workflow_run = RunId::from("workflow-deferred-root:workflow:one");
    let root_agent = AgentId::from("workflow-deferred-root-agent");
    let runtime = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(2)),
        Arc::clone(&runtime_ledger),
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
        operation_id: OperationId::from("workflow-deferred-operation"),
        stable_task_key: "workflow-deferred-task".to_owned(),
        prompt: "preserve deferred task terminal write".into(),
        expected_task_revision: Some(0),
    });
    let controller = WorkflowController::with_runtime_and_roles(Arc::clone(&runtime), None);
    controller.register(recovery_workflow("workflow-deferred")).unwrap();
    controller
        .start(workflow_run.clone(), "workflow-deferred", json!({}))
        .unwrap();

    let error = controller
        .execute_until_settled(&workflow_run, executor.clone())
        .await
        .expect_err("deferred terminal write must prevent Workflow settlement");
    assert!(error.contains("deferred"), "{error}");
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    let serialized = serde_json::to_value(controller.snapshot(&workflow_run).unwrap()).unwrap();
    assert!(serialized["nodes"]["work"]["deferred_task_terminal_write"].is_object());
    let workflow_records = ledger.inner.records_for_run(&workflow_run).unwrap();
    assert_eq!(
        workflow_records
            .iter()
            .filter(|record| record.record_type == "workflow_settled")
            .count(),
        0
    );

    ledger.allow_reads();
    match crash {
        Some(DeferredRecoveryCrash::TerminalTaskCas) => ledger.fail_terminal_append_once(),
        Some(DeferredRecoveryCrash::ReconciledMarker) => ledger.fail_marker_append_once(),
        None => {}
    }
    let restored_runtime = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(2)),
        Arc::clone(&runtime_ledger),
    ));
    let restored = WorkflowController::with_runtime_and_roles(Arc::clone(&restored_runtime), None);
    restored.register(recovery_workflow("workflow-deferred")).unwrap();
    if root_first {
        restored_runtime.restore_projection(&root_run).unwrap();
        restored.restore_from_ledger(&root_run).unwrap();
    } else {
        restored.restore_from_ledger(&root_run).unwrap();
        restored_runtime.restore_projection(&root_run).unwrap();
    }
    let restored_serialized = serde_json::to_value(restored.snapshot(&workflow_run).unwrap()).unwrap();
    assert!(restored_serialized["nodes"]["work"]["deferred_task_terminal_write"].is_object());
    let no_dispatch = Arc::new(NoDispatchWorkflowExecutor {
        calls: AtomicUsize::new(0),
    });
    let (settled, final_runtime) = if crash.is_some() {
        restored
            .execute_until_settled(&workflow_run, no_dispatch.clone())
            .await
            .expect_err("injected post-append lookup failure must preserve deferred recovery");
        assert_eq!(
            ledger
                .inner
                .records_for_run(&workflow_run)
                .unwrap()
                .iter()
                .filter(|record| record.record_type == "workflow_settled")
                .count(),
            0
        );
        let final_runtime = Arc::new(CollaborationRuntime::with_ledger(
            Scheduler::new(ResourcePolicy::new(2)),
            Arc::clone(&runtime_ledger),
        ));
        let final_controller = WorkflowController::with_runtime_and_roles(Arc::clone(&final_runtime), None);
        final_controller
            .register(recovery_workflow("workflow-deferred"))
            .unwrap();
        if root_first {
            final_runtime.restore_projection(&root_run).unwrap();
            final_controller.restore_from_ledger(&root_run).unwrap();
        } else {
            final_controller.restore_from_ledger(&root_run).unwrap();
            final_runtime.restore_projection(&root_run).unwrap();
        }
        let settled = final_controller
            .execute_until_settled(&workflow_run, no_dispatch.clone())
            .await
            .unwrap();
        (settled, final_runtime)
    } else {
        let settled = restored
            .execute_until_settled(&workflow_run, no_dispatch.clone())
            .await
            .unwrap();
        (settled, restored_runtime)
    };
    assert_eq!(settled.status, WorkflowRunStatus::Failed);
    assert!(serde_json::to_value(&settled).unwrap()["nodes"]["work"]["deferred_task_terminal_write"].is_null());
    assert_eq!(no_dispatch.calls.load(Ordering::SeqCst), 0);
    let task_id = TaskId::from("workflow:workflow-deferred-root:workflow:one:work");
    let task = final_runtime.tasks().get(&task_id).unwrap();
    assert_eq!(task.revision, 2);
    assert_eq!(task.state, TaskState::Failed);
    assert_eq!(task.failure_class, Some(TaskFailureClass::ReconciliationRequired));
    let records = ledger.inner.records_for_run(&workflow_run).unwrap();
    assert!(
        records
            .iter()
            .any(|record| record.record_type == "workflow_task_terminal_reconciled")
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| {
                record.record_type == "task_cas"
                    && record.payload.get("transition").and_then(Value::as_str) == Some("workflow_state")
                    && record
                        .payload
                        .get("task")
                        .and_then(|task| task.get("state"))
                        .and_then(Value::as_str)
                        == Some("failed")
            })
            .count(),
        1
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| record.record_type == "workflow_task_terminal_reconciled")
            .count(),
        1
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| record.record_type == "workflow_settled")
            .count(),
        1
    );
}

#[tokio::test]
async fn deferred_task_terminal_recovers_after_root_then_workflow_restore() {
    assert_deferred_terminal_recovery_order(true, None).await;
}

#[tokio::test]
async fn deferred_task_terminal_recovers_after_workflow_then_root_restore() {
    assert_deferred_terminal_recovery_order(false, None).await;
}

#[tokio::test]
async fn deferred_terminal_task_cas_append_error_recovers_in_both_restore_orders() {
    for root_first in [true, false] {
        assert_deferred_terminal_recovery_order(root_first, Some(DeferredRecoveryCrash::TerminalTaskCas)).await;
    }
}

#[tokio::test]
async fn deferred_terminal_marker_append_error_recovers_in_both_restore_orders() {
    for root_first in [true, false] {
        assert_deferred_terminal_recovery_order(root_first, Some(DeferredRecoveryCrash::ReconciledMarker)).await;
    }
}
