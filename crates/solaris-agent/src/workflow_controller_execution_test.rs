struct EchoExecutor;

#[async_trait]
impl WorkflowNodeExecutor for EchoExecutor {
    async fn execute(&self, context: WorkflowExecutionContext) -> Result<Value, WorkflowNodeError> {
        Ok(json!({"node": context.node.id}))
    }
}

struct BindingExecutor;

#[async_trait]
impl WorkflowNodeExecutor for BindingExecutor {
    async fn execute(&self, context: WorkflowExecutionContext) -> Result<Value, WorkflowNodeError> {
        if context.node.id == "source" {
            Ok(json!({"plan": {"steps": ["a", "b"]}}))
        } else {
            Ok(json!({"bound": context.bound_inputs}))
        }
    }
}

struct SlowExecutor;

#[async_trait]
impl WorkflowNodeExecutor for SlowExecutor {
    async fn execute(&self, _context: WorkflowExecutionContext) -> Result<Value, WorkflowNodeError> {
        std::future::pending().await
    }
}

struct UltracodePlanExecutor;

#[async_trait]
impl WorkflowNodeExecutor for UltracodePlanExecutor {
    async fn execute(&self, context: WorkflowExecutionContext) -> Result<Value, WorkflowNodeError> {
        match context.node.id.as_str() {
            "verify" => Ok(json!({"verdict":"PASS", "evidence": []})),
            "finalize" => Ok(json!({"completed": true, "mode": "plan"})),
            node => Ok(json!({"node": node, "completed": true})),
        }
    }
}

struct WorkflowAppendBlockingLedger {
    inner: crate::runtime_ledger::InMemoryRuntimeLedger,
    appended: std::sync::mpsc::Sender<()>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}

impl crate::runtime_ledger::RuntimeLedger for WorkflowAppendBlockingLedger {
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
        durability: solaris_types::effect::DurabilityClass,
        record_type: &str,
        payload: Value,
    ) -> std::io::Result<crate::runtime_ledger::LedgerRecord> {
        self.inner.append(run_id, durability, record_type, payload)
    }

    fn append_under_workflow_lease(
        &self,
        lease: &crate::runtime_ledger::WorkflowMutationLease,
        now_unix_ms: i64,
        durability: solaris_types::effect::DurabilityClass,
        record_type: &str,
        payload: Value,
    ) -> std::io::Result<crate::runtime_ledger::LedgerRecord> {
        let record = self
            .inner
            .append_under_workflow_lease(lease, now_unix_ms, durability, record_type, payload)?;
        if record_type == "workflow_started" {
            self.appended.send(()).unwrap();
            self.release.lock().unwrap().recv().unwrap();
        }
        Ok(record)
    }

    fn compare_and_append(
        &self,
        run_id: &RunId,
        durability: solaris_types::effect::DurabilityClass,
        record_type: &str,
        identity_fields: &[&str],
        payload: Value,
    ) -> std::io::Result<crate::runtime_ledger::LedgerRecord> {
        self.inner
            .compare_and_append(run_id, durability, record_type, identity_fields, payload)
    }

    fn compare_and_append_under_workflow_lease(
        &self,
        lease: &crate::runtime_ledger::WorkflowMutationLease,
        now_unix_ms: i64,
        durability: solaris_types::effect::DurabilityClass,
        record_type: &str,
        identity_fields: &[&str],
        payload: Value,
    ) -> std::io::Result<crate::runtime_ledger::LedgerRecord> {
        self.inner.compare_and_append_under_workflow_lease(
            lease,
            now_unix_ms,
            durability,
            record_type,
            identity_fields,
            payload,
        )
    }

    fn run_ids(&self) -> std::io::Result<Vec<RunId>> {
        self.inner.run_ids()
    }

    fn records_for_run(
        &self,
        run_id: &RunId,
    ) -> std::io::Result<Vec<crate::runtime_ledger::LedgerRecord>> {
        self.inner.records_for_run(run_id)
    }
}

#[test]
fn runtime_snapshot_waits_for_durable_workflow_projection_mutation() {
    use std::sync::mpsc;
    use std::time::Duration;

    use crate::collaboration_runtime::CollaborationRuntime;
    use crate::resource_policy::ResourcePolicy;
    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};
    use crate::scheduler::Scheduler;

    let (appended_tx, appended_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let ledger: Arc<dyn RuntimeLedger> = Arc::new(WorkflowAppendBlockingLedger {
        inner: InMemoryRuntimeLedger::default(),
        appended: appended_tx,
        release: Mutex::new(release_rx),
    });
    let runtime = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(1)),
        ledger,
    ));
    let controller = Arc::new(WorkflowController::with_runtime(Arc::clone(&runtime)));
    controller
        .register(WorkflowDefinition {
            id: "atomic-workflow".into(),
            schema_version: 1,
            version: "1".into(),
            description: "Test workflow".into(),
            roles: Vec::new(),
            parameters_schema: None,
            nodes: vec![node("work", &[])],
            outputs: BTreeMap::new(),
        })
        .unwrap();
    let workflow_run_id = RunId::from("root-atomic:workflow:one");
    let start_controller = Arc::clone(&controller);
    let start_run_id = workflow_run_id.clone();
    let start = std::thread::spawn(move || {
        start_controller
            .start(start_run_id, "atomic-workflow", json!({}))
            .unwrap()
    });
    appended_rx.recv().unwrap();

    let (snapshot_started_tx, snapshot_started_rx) = mpsc::channel();
    let (snapshot_tx, snapshot_rx) = mpsc::channel();
    let snapshot_runtime = Arc::clone(&runtime);
    let snapshot_controller = Arc::clone(&controller);
    let snapshot = std::thread::spawn(move || {
        snapshot_started_tx.send(()).unwrap();
        let captured = snapshot_runtime
            .capture_snapshot_with(&RunId::from("root-atomic"), || snapshot_controller.snapshots())
            .unwrap();
        snapshot_tx.send(captured).unwrap();
    });
    snapshot_started_rx.recv().unwrap();
    assert!(snapshot_rx.recv_timeout(Duration::from_millis(25)).is_err());

    release_tx.send(()).unwrap();
    let (runtime_snapshot, workflow_runs) = snapshot_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let started = start.join().unwrap();
    snapshot.join().unwrap();

    assert_eq!(workflow_runs, vec![started]);
    assert!(
        runtime_snapshot
            .projection
            .tasks
            .iter()
            .any(|task| task.run_id == workflow_run_id && task.state == TaskState::Queued)
    );
}

#[test]
fn concurrent_workflow_start_returns_the_persisted_snapshot_to_both_callers() {
    use std::sync::mpsc;

    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};

    let (appended_tx, appended_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let ledger: Arc<dyn RuntimeLedger> = Arc::new(WorkflowAppendBlockingLedger {
        inner: InMemoryRuntimeLedger::default(),
        appended: appended_tx,
        release: Mutex::new(release_rx),
    });
    let controller = Arc::new(WorkflowController::new(ledger));
    controller
        .register(WorkflowDefinition {
            id: "concurrent-start".into(),
            schema_version: 1,
            version: "1".into(),
            description: "Test workflow".into(),
            roles: Vec::new(),
            parameters_schema: None,
            nodes: vec![node("work", &[])],
            outputs: BTreeMap::new(),
        })
        .unwrap();
    let run_id = RunId::from("concurrent-start-run");
    let first = {
        let controller = Arc::clone(&controller);
        let run_id = run_id.clone();
        std::thread::spawn(move || {
            controller
                .start(run_id, "concurrent-start", json!({"query":"same"}))
                .unwrap()
        })
    };
    appended_rx.recv().unwrap();
    let (second_started_tx, second_started_rx) = mpsc::channel();
    let second = {
        let controller = Arc::clone(&controller);
        let run_id = run_id.clone();
        std::thread::spawn(move || {
            second_started_tx.send(()).unwrap();
            controller
                .start(run_id, "concurrent-start", json!({"query":"same"}))
                .unwrap()
        })
    };

    second_started_rx.recv().unwrap();
    std::thread::sleep(Duration::from_millis(25));
    release_tx.send(()).unwrap();
    let first = first.join().unwrap();
    let second = second.join().unwrap();
    let persisted = controller.snapshot(&run_id).unwrap();

    assert_eq!(first, persisted);
    assert_eq!(second, persisted);
}

#[tokio::test]
async fn independent_nodes_run_before_join_node_and_checkpoint() {
    let controller = WorkflowController::default();
    controller
        .register(WorkflowDefinition {
            id: "test".into(),
            schema_version: 1,
            version: "1".into(),
            description: "Test workflow".into(),
            roles: Vec::new(),
            parameters_schema: None,
            nodes: vec![node("a", &[]), node("b", &[]), node("join", &["a", "b"])],
            outputs: BTreeMap::new(),
        })
        .unwrap();
    let run = RunId::from("run");
    controller.start(run.clone(), "test", json!({})).unwrap();
    let settled = controller
        .execute_until_settled(&run, Arc::new(EchoExecutor))
        .await
        .unwrap();
    assert_eq!(settled.status, WorkflowRunStatus::Completed);
    assert!(settled.nodes.values().all(|node| node.attempt_number == 1));
    assert!(settled.nodes.values().all(|node| node.input_digest.is_some()));
    assert!(settled.nodes.values().all(|node| node.output_ref.is_some()));
    assert!(settled.nodes.values().all(|node| node.committed_at_unix_ms.is_some()));
    assert!(controller.task_handle(&run, "join").unwrap().output_ref.is_some());
    let task = controller
        .task_registry()
        .get(&solaris_types::identity::TaskId::from("workflow:run:join"))
        .unwrap();
    assert_eq!(task.workflow_id.as_deref(), Some("test"));
    assert_eq!(task.node_id.as_deref(), Some("join"));
    assert_eq!(task.state, solaris_types::runtime::TaskState::Completed);
}

struct FlakyExecutor {
    failures: std::sync::Mutex<u32>,
}

#[async_trait]
impl WorkflowNodeExecutor for FlakyExecutor {
    async fn execute(&self, context: WorkflowExecutionContext) -> Result<Value, WorkflowNodeError> {
        if context.node.id == "flaky" {
            let mut failures = self.failures.lock().unwrap();
            if *failures == 0 {
                *failures += 1;
                return Err("first failure".into());
            }
        }
        Ok(json!({"ok": true}))
    }
}

#[tokio::test]
async fn retry_does_not_replay_completed_sibling() {
    let controller = WorkflowController::default();
    let stable = node("stable", &[]);
    let mut flaky = node("flaky", &[]);
    flaky.retry.max_attempts = 2;
    controller
        .register(WorkflowDefinition {
            id: "retry".into(),
            schema_version: 1,
            version: "1".into(),
            description: "Test workflow".into(),
            roles: Vec::new(),
            parameters_schema: None,
            nodes: vec![stable, flaky],
            outputs: BTreeMap::new(),
        })
        .unwrap();
    let run = RunId::from("retry-run");
    controller.start(run.clone(), "retry", json!({})).unwrap();
    let settled = controller
        .execute_until_settled(
            &run,
            Arc::new(FlakyExecutor {
                failures: std::sync::Mutex::new(0),
            }),
        )
        .await
        .unwrap();
    assert_eq!(settled.nodes["stable"].attempt_number, 1);
    assert_eq!(settled.nodes["flaky"].attempt_number, 2);
    assert_eq!(settled.status, WorkflowRunStatus::Completed);
}

#[test]
fn invalid_parameters_and_cycles_are_rejected() {
    let controller = WorkflowController::default();
    let mut a = node("a", &["b"]);
    let b = node("b", &["a"]);
    let cycle = WorkflowDefinition {
        id: "cycle".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Test workflow".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![a.clone(), b],
        outputs: BTreeMap::new(),
    };
    assert!(controller.register(cycle).unwrap_err().contains("cycle"));

    a.depends_on.clear();
    controller
        .register(WorkflowDefinition {
            id: "schema".into(),
            schema_version: 1,
            version: "1".into(),
            description: "Test workflow".into(),
            roles: Vec::new(),
            parameters_schema: Some(json!({"type":"object", "required":["query"]})),
            nodes: vec![a],
            outputs: BTreeMap::new(),
        })
        .unwrap();
    assert!(controller.start(RunId::from("bad"), "schema", json!({})).is_err());
    assert!(
        controller
            .start(RunId::from("good"), "schema", json!({"query":"x"}))
            .is_ok()
    );
}

#[tokio::test]
async fn output_binding_is_typed_and_explicit() {
    let controller = WorkflowController::default();
    let source = node("source", &[]);
    let mut consumer = node("consumer", &["source"]);
    consumer.output_bindings = vec![OutputBinding {
        from: "source.plan.steps".into(),
        to: "implementation.steps".into(),
    }];
    controller
        .register(WorkflowDefinition {
            id: "bindings".into(),
            schema_version: 1,
            version: "1".into(),
            description: "Test workflow".into(),
            roles: Vec::new(),
            parameters_schema: None,
            nodes: vec![source, consumer],
            outputs: BTreeMap::from([("result".into(), "consumer".into())]),
        })
        .unwrap();
    let run = RunId::from("binding-run");
    controller.start(run.clone(), "bindings", json!({})).unwrap();
    let settled = controller
        .execute_until_settled(&run, Arc::new(BindingExecutor))
        .await
        .unwrap();
    assert_eq!(
        settled.nodes["consumer"].output.as_ref().unwrap()["bound"]["implementation"]["steps"],
        json!(["a", "b"])
    );
}

#[tokio::test]
async fn binding_error_is_persisted_as_a_terminal_node_failure() {
    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};

    let ledger: Arc<dyn RuntimeLedger> = Arc::new(InMemoryRuntimeLedger::default());
    let controller = WorkflowController::new(Arc::clone(&ledger));
    let source = node("source", &[]);
    let mut consumer = node("consumer", &["source"]);
    consumer.output_bindings = vec![OutputBinding {
        from: "source.plan.missing".into(),
        to: "input.value".into(),
    }];
    let definition = WorkflowDefinition {
        id: "bad-binding".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Binding failure".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![source, consumer],
        outputs: BTreeMap::new(),
    };
    controller.register(definition.clone()).unwrap();
    let run = RunId::from("binding-root:workflow:bad");
    controller.start(run.clone(), &definition.id, json!({})).unwrap();
    let settled = controller
        .execute_until_settled(&run, Arc::new(BindingExecutor))
        .await
        .unwrap();
    assert_eq!(settled.status, WorkflowRunStatus::Failed);
    assert_eq!(settled.nodes["consumer"].status, WorkflowNodeStatus::Failed);
    assert!(
        ledger
            .records_for_run(&run)
            .unwrap()
            .iter()
            .any(|record| { record.record_type == "workflow_node_failed" && record.payload["pre_dispatch"] == true })
    );

    let restored = WorkflowController::new(ledger);
    restored.register(definition).unwrap();
    assert_eq!(restored.restore_from_ledger(&RunId::from("binding-root")).unwrap(), 1);
    assert_eq!(restored.snapshot(&run).unwrap().status, WorkflowRunStatus::Failed);
}

#[test]
fn begin_attempt_is_compare_and_set_from_pending() {
    let controller = WorkflowController::default();
    let definition = WorkflowDefinition {
        id: "attempt-cas".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Attempt CAS".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("work", &[])],
        outputs: BTreeMap::new(),
    };
    controller.register(definition.clone()).unwrap();
    let run = RunId::from("attempt-cas-run");
    controller.start(run.clone(), &definition.id, json!({})).unwrap();
    controller.begin_attempt(&run, &definition.nodes[0]).unwrap();
    assert!(
        controller
            .begin_attempt(&run, &definition.nodes[0])
            .unwrap_err()
            .contains("not pending")
    );
}

#[test]
fn cancelled_parent_rejects_a_late_child_start() {
    let controller = WorkflowController::default();
    let child_definition = WorkflowDefinition {
        id: "late-child".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Late child".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("work", &[])],
        outputs: BTreeMap::new(),
    };
    controller.register(child_definition).unwrap();
    let parent_definition = WorkflowDefinition {
        id: "late-parent".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Late parent".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("work", &[])],
        outputs: BTreeMap::new(),
    };
    controller.register(parent_definition).unwrap();
    let parent = RunId::from("late-root:workflow:parent");
    controller.start(parent.clone(), "late-parent", json!({})).unwrap();
    controller.cancel(&parent, "cancel first").unwrap();
    let child = RunId::from("late-root:workflow:parent:subworkflow:work:attempt");
    assert!(
        controller
            .start_child(child.clone(), "late-child", json!({}), parent)
            .unwrap_err()
            .contains("is not running")
    );
    assert!(controller.snapshot(&child).is_none());
}

#[tokio::test]
async fn timeout_fails_node_without_hanging_run() {
    let controller = WorkflowController::default();
    let mut slow = node("slow", &[]);
    slow.timeout_ms = Some(1);
    controller
        .register(WorkflowDefinition {
            id: "timeout".into(),
            schema_version: 1,
            version: "1".into(),
            description: "Test workflow".into(),
            roles: Vec::new(),
            parameters_schema: None,
            nodes: vec![slow],
            outputs: BTreeMap::new(),
        })
        .unwrap();
    let run = RunId::from("timeout-run");
    controller.start(run.clone(), "timeout", json!({})).unwrap();
    let settled = controller
        .execute_until_settled(&run, Arc::new(SlowExecutor))
        .await
        .unwrap();
    assert_eq!(settled.status, WorkflowRunStatus::Failed);
    assert!(settled.nodes["slow"].error.as_deref().unwrap().contains("timed out"));
}

#[tokio::test]
async fn subworkflow_executes_with_durable_child_run() {
    let controller = WorkflowController::default();
    controller
        .register(WorkflowDefinition {
            id: "child".into(),
            schema_version: 1,
            version: "1".into(),
            description: "Test workflow".into(),
            roles: Vec::new(),
            parameters_schema: None,
            nodes: vec![node("work", &[])],
            outputs: BTreeMap::from([("value".into(), "work".into())]),
        })
        .unwrap();
    let mut delegated = node("delegate", &[]);
    delegated.workflow_ref = Some("child".into());
    controller
        .register(WorkflowDefinition {
            id: "parent".into(),
            schema_version: 1,
            version: "1".into(),
            description: "Test workflow".into(),
            roles: Vec::new(),
            parameters_schema: None,
            nodes: vec![delegated],
            outputs: BTreeMap::from([("result".into(), "delegate".into())]),
        })
        .unwrap();
    let run = RunId::from("parent-run");
    controller.start(run.clone(), "parent", json!({})).unwrap();
    let settled = controller
        .execute_until_settled(&run, Arc::new(EchoExecutor))
        .await
        .unwrap();
    assert_eq!(settled.status, WorkflowRunStatus::Completed);
    assert_eq!(
        settled.nodes["delegate"].output.as_ref().unwrap()["value"]["node"],
        "work"
    );
    assert!(
        controller
            .snapshots()
            .iter()
            .any(|snapshot| snapshot.workflow_id == "child")
    );
}

#[tokio::test]
async fn plan_ultracode_keeps_workflow_but_skips_mutation_nodes() {
    let controller = WorkflowController::default();
    controller.register(ultracode()).unwrap();
    let run = RunId::from("ultracode-plan");
    controller
        .start(
            run.clone(),
            "ultracode-v1",
            json!({"prompt":"inspect only", "permission_mode":"plan", "collaboration":"team"}),
        )
        .unwrap();
    let settled = controller
        .execute_until_settled(&run, Arc::new(UltracodePlanExecutor))
        .await
        .unwrap();
    assert_eq!(settled.status, WorkflowRunStatus::Completed);
    assert_eq!(settled.nodes["implement"].status, WorkflowNodeStatus::Skipped);
    assert_eq!(settled.nodes["repair"].status, WorkflowNodeStatus::Skipped);
    assert_eq!(settled.nodes["reverify"].status, WorkflowNodeStatus::Skipped);
    assert_eq!(settled.nodes["verify"].status, WorkflowNodeStatus::Completed);
}
