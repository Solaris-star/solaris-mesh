use crate::resource_policy::ResourcePolicy;
use crate::scheduler::Scheduler;

struct TypedFailureExecutor {
    class: TaskFailureClass,
}

#[async_trait]
impl WorkflowNodeExecutor for TypedFailureExecutor {
    async fn execute(&self, _context: WorkflowExecutionContext) -> Result<Value, WorkflowNodeError> {
        Err(WorkflowNodeError {
            failure_class: self.class,
            message: "typed child failure".to_owned(),
        })
    }
}

fn nested_failure_definitions() -> (WorkflowDefinition, WorkflowDefinition) {
    let child = WorkflowDefinition {
        id: "typed-child".into(),
        schema_version: 1,
        version: "1".into(),
        description: "child".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("child-node", &[])],
        outputs: BTreeMap::new(),
    };
    let mut delegate = node("delegate", &[]);
    delegate.workflow_ref = Some(child.id.clone());
    delegate.retry.max_attempts = 3;
    let parent = WorkflowDefinition {
        id: "typed-parent".into(),
        schema_version: 1,
        version: "1".into(),
        description: "parent".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![delegate],
        outputs: BTreeMap::new(),
    };
    (child, parent)
}

#[tokio::test]
async fn nested_workflow_preserves_non_retryable_failure_classes() {
    for class in [
        TaskFailureClass::PermissionDenied,
        TaskFailureClass::NonRetryable,
        TaskFailureClass::MaxTurns,
        TaskFailureClass::NonConvergent,
        TaskFailureClass::Cancelled,
        TaskFailureClass::SideEffectUnknown,
        TaskFailureClass::OutcomeUnknown,
        TaskFailureClass::ReconciliationRequired,
    ] {
        let runtime = Arc::new(CollaborationRuntime::new(Scheduler::new(ResourcePolicy::new(2))));
        let controller = WorkflowController::with_runtime(runtime);
        let (child, parent) = nested_failure_definitions();
        controller.register(child).unwrap();
        controller.register(parent).unwrap();
        let run = RunId::new(format!("nested-{class:?}"));
        controller.start(run.clone(), "typed-parent", json!({})).unwrap();
        let settled = controller
            .execute_until_settled(&run, Arc::new(TypedFailureExecutor { class }))
            .await
            .unwrap();
        assert_eq!(settled.status, WorkflowRunStatus::Failed);
        let attempt = &settled.nodes["delegate"];
        assert_eq!(attempt.status, WorkflowNodeStatus::Failed);
        assert_eq!(attempt.failure_class, Some(class));
        assert_eq!(attempt.attempt_number, 1, "{class:?} must not retry");
    }
}

#[tokio::test]
async fn nested_failure_class_survives_restore() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let runtime = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(2)),
        ledger.clone(),
    ));
    let controller = WorkflowController::with_runtime(runtime);
    let (child, parent) = nested_failure_definitions();
    controller.register(child.clone()).unwrap();
    controller.register(parent.clone()).unwrap();
    let root = RunId::from("nested-restore-root");
    let run = RunId::from("nested-restore-root:workflow:parent");
    controller.start(run.clone(), "typed-parent", json!({})).unwrap();
    let settled = controller
        .execute_until_settled(
            &run,
            Arc::new(TypedFailureExecutor {
                class: TaskFailureClass::PermissionDenied,
            }),
        )
        .await
        .unwrap();
    let child_run = settled
        .nodes
        .values()
        .find_map(|attempt| {
            controller
                .snapshots()
                .into_iter()
                .find(|snapshot| snapshot.parent_run_id.as_ref() == Some(&run))
                .map(|snapshot| snapshot.run_id)
                .or_else(|| attempt.failure_class.map(|_| RunId::from("missing")))
        })
        .unwrap();
    assert_ne!(child_run.as_str(), "missing");

    let restored_runtime = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(2)),
        ledger,
    ));
    let restored = WorkflowController::with_runtime(restored_runtime);
    restored.register(child).unwrap();
    restored.register(parent).unwrap();
    restored.restore_from_ledger(&root).unwrap();
    let restored_parent = restored.snapshot(&run).unwrap();
    assert_eq!(
        restored_parent.nodes["delegate"].failure_class,
        Some(TaskFailureClass::PermissionDenied)
    );
    let restored_child = restored.snapshot(&child_run).unwrap();
    assert_eq!(
        restored_child.nodes["child-node"].failure_class,
        Some(TaskFailureClass::PermissionDenied)
    );
}

#[test]
fn untyped_string_workflow_errors_are_non_retryable() {
    assert_eq!(
        WorkflowNodeError::from("unknown failure").failure_class,
        TaskFailureClass::NonRetryable
    );
    assert_eq!(
        WorkflowNodeError::from("unknown failure".to_owned()).failure_class,
        TaskFailureClass::NonRetryable
    );
}

struct MappedFailureExecutor {
    classes: BTreeMap<String, TaskFailureClass>,
    delays_ms: BTreeMap<String, u64>,
    calls: Arc<std::sync::Mutex<BTreeMap<String, usize>>>,
}

#[async_trait]
impl WorkflowNodeExecutor for MappedFailureExecutor {
    async fn execute(&self, context: WorkflowExecutionContext) -> Result<Value, WorkflowNodeError> {
        let node_id = context.node.id.clone();
        if let Some(delay_ms) = self.delays_ms.get(&node_id).copied() {
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        }
        *self
            .calls
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entry(node_id.clone())
            .or_default() += 1;
        let class = self.classes.get(&node_id).copied().unwrap_or(TaskFailureClass::NonRetryable);
        Err(WorkflowNodeError {
            failure_class: class,
            message: format!("{node_id} failed as {class:?}"),
        })
    }
}

fn multi_failure_definitions(child_id: &str, node_ids: &[&str]) -> (WorkflowDefinition, WorkflowDefinition) {
    let child = WorkflowDefinition {
        id: child_id.into(),
        schema_version: 1,
        version: "1".into(),
        description: "multi failure child".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: node_ids.iter().map(|id| node(id, &[])).collect(),
        outputs: BTreeMap::new(),
    };
    let mut delegate = node("delegate", &[]);
    delegate.workflow_ref = Some(child.id.clone());
    delegate.retry.max_attempts = 3;
    let parent = WorkflowDefinition {
        id: format!("{child_id}-parent"),
        schema_version: 1,
        version: "1".into(),
        description: "multi failure parent".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![delegate],
        outputs: BTreeMap::new(),
    };
    (child, parent)
}

async fn run_multi_failure_case(
    run_id: &str,
    node_classes: &[(&str, TaskFailureClass, u64)],
) -> (WorkflowRunSnapshot, WorkflowRunSnapshot, Arc<std::sync::Mutex<BTreeMap<String, usize>>>) {
    let runtime = Arc::new(CollaborationRuntime::new(Scheduler::new(ResourcePolicy::new(4))));
    let controller = WorkflowController::with_runtime(runtime);
    let node_ids = node_classes.iter().map(|(node_id, _, _)| *node_id).collect::<Vec<_>>();
    let (child, parent) = multi_failure_definitions(&format!("{run_id}-child"), &node_ids);
    let parent_id = parent.id.clone();
    controller.register(child).unwrap();
    controller.register(parent).unwrap();
    let classes = node_classes
        .iter()
        .map(|(node_id, class, _)| ((*node_id).to_owned(), *class))
        .collect();
    let delays_ms = node_classes
        .iter()
        .map(|(node_id, _, delay)| ((*node_id).to_owned(), *delay))
        .collect();
    let calls = Arc::new(std::sync::Mutex::new(BTreeMap::new()));
    let run = RunId::from(run_id);
    controller.start(run.clone(), &parent_id, json!({})).unwrap();
    let parent = controller
        .execute_until_settled(
            &run,
            Arc::new(MappedFailureExecutor {
                classes,
                delays_ms,
                calls: Arc::clone(&calls),
            }),
        )
        .await
        .unwrap();
    let child = controller
        .snapshots()
        .into_iter()
        .filter(|snapshot| snapshot.parent_run_id.as_ref() == Some(&run))
        .max_by(|left, right| left.run_id.as_str().cmp(right.run_id.as_str()))
        .unwrap();
    (parent, child, calls)
}

#[tokio::test]
async fn nested_workflow_aggregates_all_failures_independent_of_names_and_completion_order() {
    let cases = [
        (
            "aggregate-unknown-a",
            vec![
                ("a-retry", TaskFailureClass::Retryable, 30),
                ("z-unknown", TaskFailureClass::OutcomeUnknown, 0),
            ],
            TaskFailureClass::OutcomeUnknown,
        ),
        (
            "aggregate-unknown-b",
            vec![
                ("a-unknown", TaskFailureClass::OutcomeUnknown, 30),
                ("z-retry", TaskFailureClass::Retryable, 0),
            ],
            TaskFailureClass::OutcomeUnknown,
        ),
        (
            "aggregate-reconcile",
            vec![
                ("a-retry", TaskFailureClass::Retryable, 0),
                ("m-denied", TaskFailureClass::PermissionDenied, 20),
                ("z-reconcile", TaskFailureClass::ReconciliationRequired, 10),
            ],
            TaskFailureClass::ReconciliationRequired,
        ),
    ];
    for (run_id, failures, expected_primary) in cases {
        let expected_count = failures.len();
        let (parent, child, _) = run_multi_failure_case(run_id, &failures).await;
        let summary = child.failure_summary.as_ref().expect("failed child has a failure summary");
        assert_eq!(summary.primary_failure_class, expected_primary, "case={run_id}");
        assert_eq!(summary.failures.len(), expected_count, "case={run_id}");
        assert_eq!(parent.nodes["delegate"].failure_class, Some(expected_primary), "case={run_id}");
        assert_eq!(parent.nodes["delegate"].attempt_number, 1, "case={run_id} must not retry");
    }
}

#[tokio::test]
async fn nested_workflow_any_blocking_sibling_prevents_parent_retry() {
    for blocking in [
        TaskFailureClass::PermissionDenied,
        TaskFailureClass::NonRetryable,
        TaskFailureClass::MaxTurns,
        TaskFailureClass::NonConvergent,
        TaskFailureClass::Cancelled,
        TaskFailureClass::OutcomeUnknown,
        TaskFailureClass::SideEffectUnknown,
        TaskFailureClass::ReconciliationRequired,
    ] {
        let run_id = format!("aggregate-blocking-{blocking:?}");
        let (parent, child, calls) = run_multi_failure_case(
            &run_id,
            &[
                ("a-retry", TaskFailureClass::Retryable, 10),
                ("z-blocking", blocking, 0),
            ],
        )
        .await;
        let summary = child.failure_summary.expect("failed child has a failure summary");
        assert_eq!(summary.primary_failure_class, blocking, "case={blocking:?}");
        assert_eq!(summary.failures.get("a-retry"), Some(&TaskFailureClass::Retryable));
        assert_eq!(summary.failures.get("z-blocking"), Some(&blocking));
        assert_eq!(parent.nodes["delegate"].attempt_number, 1, "case={blocking:?}");
        assert_eq!(parent.nodes["delegate"].failure_class, Some(blocking), "case={blocking:?}");
        let calls = calls.lock().unwrap_or_else(|error| error.into_inner());
        assert_eq!(calls.get("a-retry"), Some(&1), "case={blocking:?}");
        assert_eq!(calls.get("z-blocking"), Some(&1), "case={blocking:?}");
    }
}

#[tokio::test]
async fn nested_workflow_retries_only_when_every_failed_branch_is_retryable() {
    let (parent, child, calls) = run_multi_failure_case(
        "aggregate-all-retryable",
        &[
            ("left", TaskFailureClass::Retryable, 20),
            ("right", TaskFailureClass::Retryable, 0),
        ],
    )
    .await;
    assert_eq!(parent.nodes["delegate"].failure_class, Some(TaskFailureClass::Retryable));
    assert_eq!(parent.nodes["delegate"].attempt_number, 3);
    assert_eq!(child.failure_summary.unwrap().primary_failure_class, TaskFailureClass::Retryable);
    let calls = calls.lock().unwrap_or_else(|error| error.into_inner());
    assert_eq!(calls.get("left"), Some(&3));
    assert_eq!(calls.get("right"), Some(&3));
}

#[tokio::test]
async fn nested_unknown_side_effect_is_never_reexecuted_by_retryable_sibling() {
    let (parent, child, calls) = run_multi_failure_case(
        "aggregate-side-effect",
        &[
            ("a-retry", TaskFailureClass::Retryable, 0),
            ("z-side-effect", TaskFailureClass::SideEffectUnknown, 15),
        ],
    )
    .await;
    assert_eq!(
        child.failure_summary.unwrap().primary_failure_class,
        TaskFailureClass::SideEffectUnknown
    );
    assert_eq!(parent.nodes["delegate"].attempt_number, 1);
    assert_eq!(parent.nodes["delegate"].failure_class, Some(TaskFailureClass::SideEffectUnknown));
    let calls = calls.lock().unwrap_or_else(|error| error.into_inner());
    assert_eq!(calls.get("a-retry"), Some(&1));
    assert_eq!(calls.get("z-side-effect"), Some(&1));
}

#[tokio::test]
async fn nested_failure_summary_is_durable_and_restores_identically() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let runtime = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(4)),
        ledger.clone(),
    ));
    let controller = WorkflowController::with_runtime(runtime);
    let (child_definition, parent_definition) =
        multi_failure_definitions("aggregate-durable-child", &["retry", "unknown"]);
    let parent_id = parent_definition.id.clone();
    controller.register(child_definition.clone()).unwrap();
    controller.register(parent_definition.clone()).unwrap();
    let run = RunId::from("aggregate-durable-root:workflow:parent");
    controller.start(run.clone(), &parent_id, json!({})).unwrap();
    let calls = Arc::new(std::sync::Mutex::new(BTreeMap::new()));
    let settled = controller
        .execute_until_settled(
            &run,
            Arc::new(MappedFailureExecutor {
                classes: BTreeMap::from([
                    ("retry".to_owned(), TaskFailureClass::Retryable),
                    ("unknown".to_owned(), TaskFailureClass::OutcomeUnknown),
                ]),
                delays_ms: BTreeMap::new(),
                calls,
            }),
        )
        .await
        .unwrap();
    let child = controller
        .snapshots()
        .into_iter()
        .find(|snapshot| snapshot.parent_run_id.as_ref() == Some(&run))
        .unwrap();
    let expected = child.failure_summary.clone().unwrap();
    assert_eq!(expected.primary_failure_class, TaskFailureClass::OutcomeUnknown);
    let settled_record = ledger
        .records_for_run(&child.run_id)
        .unwrap()
        .into_iter()
        .rev()
        .find(|record| record.record_type == "workflow_settled")
        .unwrap();
    assert_eq!(
        serde_json::from_value::<WorkflowFailureSummary>(settled_record.payload["failure_summary"].clone()).unwrap(),
        expected
    );
    assert_eq!(
        serde_json::to_value(&settled).unwrap()["nodes"]["delegate"]["failure_class"],
        json!(TaskFailureClass::OutcomeUnknown)
    );

    let restored_runtime = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(4)),
        ledger,
    ));
    let restored = WorkflowController::with_runtime(restored_runtime);
    restored.register(child_definition).unwrap();
    restored.register(parent_definition).unwrap();
    restored.restore_from_ledger(&RunId::from("aggregate-durable-root")).unwrap();
    assert_eq!(restored.snapshot(&child.run_id).unwrap().failure_summary, Some(expected));
}
