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
