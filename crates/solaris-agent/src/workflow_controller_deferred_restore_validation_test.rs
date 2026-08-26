use solaris_types::effect::DurabilityClass;
use solaris_types::identity::OperationId;
use solaris_types::runtime::TaskFailureClass;

use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};

fn invalid_typed_deferred_payload(_run_id: &RunId, _attempt_id: &str, typed: Value) -> Value {
    json!({
        "node_id": "work",
        "error": "uncertain outcome",
        "failure_class": TaskFailureClass::ReconciliationRequired,
        "state": WorkflowNodeStatus::Failed,
        "task_terminal_write_deferred": true,
        "deferred_task_terminal_write": typed,
    })
}

fn restore_with_typed_deferred_payload(payload: Value) -> (Result<usize, String>, bool) {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let runtime_ledger: Arc<dyn RuntimeLedger> = ledger.clone();
    let root = RunId::from("typed-deferred-validation-root");
    let run = RunId::from("typed-deferred-validation-root:workflow:one");
    let definition = WorkflowDefinition {
        id: "typed-deferred-validation".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Validate typed deferred recovery".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("work", &[])],
        outputs: BTreeMap::new(),
    };
    let initial = WorkflowController::new(Arc::clone(&runtime_ledger));
    initial.register(definition.clone()).unwrap();
    initial.start(run.clone(), &definition.id, json!({})).unwrap();
    ledger
        .append(
            &run,
            DurabilityClass::SyncCritical,
            "workflow_node_started",
            json!({
                "node_id": "work",
                "attempt_id": "typed-attempt",
                "input_digest": "typed-input",
            }),
        )
        .unwrap();
    ledger
        .append(&run, DurabilityClass::SyncCritical, "workflow_node_failed", payload)
        .unwrap();
    ledger
        .append(
            &run,
            DurabilityClass::SyncCritical,
            "workflow_settled",
            json!({"status": WorkflowRunStatus::Failed}),
        )
        .unwrap();

    let restored = WorkflowController::new(runtime_ledger);
    restored.register(definition).unwrap();
    let result = restored.restore_from_ledger(&root);
    (result, restored.snapshot(&run).is_some())
}

#[test]
fn typed_deferred_restore_rejects_malformed_and_inconsistent_records() {
    let run = RunId::from("typed-deferred-validation-root:workflow:one");
    let canonical = OperationId::new(format!("workflow:{run}:work:typed-attempt:failed"));
    let valid = DeferredTaskTerminalWrite {
        state: TaskState::Failed,
        clear_owner: false,
        outcome_ref: None,
        failure_class: TaskFailureClass::ReconciliationRequired,
        operation_id: canonical,
    };
    let mut wrong_operation = valid.clone();
    wrong_operation.operation_id = OperationId::from("workflow:wrong-run:work:typed-attempt:failed");
    let mut wrong_attempt = valid.clone();
    wrong_attempt.operation_id = OperationId::new(format!("workflow:{run}:work:other-attempt:failed"));
    let mut wrong_failure = valid.clone();
    wrong_failure.failure_class = TaskFailureClass::NonRetryable;
    let mut wrong_clear_owner = valid.clone();
    wrong_clear_owner.clear_owner = true;
    let mut wrong_outcome = valid.clone();
    wrong_outcome.outcome_ref = Some("unexpected-outcome".to_owned());

    let cases = [
        (
            "malformed typed object",
            invalid_typed_deferred_payload(&run, "typed-attempt", json!({"operation_id": 7})),
        ),
        (
            "wrong operation",
            invalid_typed_deferred_payload(&run, "typed-attempt", serde_json::to_value(wrong_operation).unwrap()),
        ),
        (
            "wrong attempt",
            invalid_typed_deferred_payload(&run, "typed-attempt", serde_json::to_value(wrong_attempt).unwrap()),
        ),
        (
            "wrong typed failure class",
            invalid_typed_deferred_payload(&run, "typed-attempt", serde_json::to_value(wrong_failure).unwrap()),
        ),
        ("bool conflict", {
            let mut payload =
                invalid_typed_deferred_payload(&run, "typed-attempt", serde_json::to_value(&valid).unwrap());
            payload["task_terminal_write_deferred"] = Value::Bool(false);
            payload
        }),
        ("missing legacy bool", {
            let mut payload =
                invalid_typed_deferred_payload(&run, "typed-attempt", serde_json::to_value(&valid).unwrap());
            payload.as_object_mut().unwrap().remove("task_terminal_write_deferred");
            payload
        }),
        ("outer failure class conflict", {
            let mut payload =
                invalid_typed_deferred_payload(&run, "typed-attempt", serde_json::to_value(&valid).unwrap());
            payload["failure_class"] = serde_json::to_value(TaskFailureClass::NonRetryable).unwrap();
            payload
        }),
        ("missing outer failure class", {
            let mut payload =
                invalid_typed_deferred_payload(&run, "typed-attempt", serde_json::to_value(&valid).unwrap());
            payload.as_object_mut().unwrap().remove("failure_class");
            payload
        }),
        ("node state conflict", {
            let mut payload =
                invalid_typed_deferred_payload(&run, "typed-attempt", serde_json::to_value(&valid).unwrap());
            payload["state"] = serde_json::to_value(WorkflowNodeStatus::Pending).unwrap();
            payload
        }),
        ("malformed node state", {
            let mut payload =
                invalid_typed_deferred_payload(&run, "typed-attempt", serde_json::to_value(&valid).unwrap());
            payload["state"] = json!(7);
            payload
        }),
        (
            "clear owner is not allowed",
            invalid_typed_deferred_payload(&run, "typed-attempt", serde_json::to_value(wrong_clear_owner).unwrap()),
        ),
        (
            "outcome reference is not allowed",
            invalid_typed_deferred_payload(&run, "typed-attempt", serde_json::to_value(wrong_outcome).unwrap()),
        ),
    ];

    for (name, payload) in cases {
        let (result, restored) = restore_with_typed_deferred_payload(payload);
        assert!(result.is_err(), "{name} must fail closed");
        assert!(!restored, "{name} must not settle");
    }
}
