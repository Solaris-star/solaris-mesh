use serde_json::json;

use super::{TaskFailureClass, TaskRecord, TaskState};

#[test]
fn old_task_record_wire_defaults_cas_fields() {
    let record: TaskRecord = serde_json::from_value(json!({
        "run_id": "run",
        "task_id": "task",
        "state": "queued"
    }))
    .expect("old TaskRecord must remain readable");

    assert_eq!(record.revision, 0);
    assert_eq!(record.task_key, None);
    assert_eq!(record.content, None);
    assert!(record.expected_write_scope.is_empty());
    assert_eq!(record.outcome_ref, None);
    assert_eq!(record.failure_class, None);
    assert_eq!(record.state, TaskState::Queued);
}

#[test]
fn task_failure_class_has_stable_wire_names() {
    for (class, wire) in [
        (TaskFailureClass::Retryable, "retryable"),
        (TaskFailureClass::NonRetryable, "non_retryable"),
        (TaskFailureClass::PermissionDenied, "permission_denied"),
        (TaskFailureClass::MaxTurns, "max_turns"),
        (TaskFailureClass::NonConvergent, "non_convergent"),
        (TaskFailureClass::Cancelled, "cancelled"),
        (TaskFailureClass::OutcomeUnknown, "outcome_unknown"),
        (TaskFailureClass::ReconciliationRequired, "reconciliation_required"),
        (TaskFailureClass::SideEffectUnknown, "side_effect_unknown"),
    ] {
        assert_eq!(serde_json::to_value(class).unwrap(), json!(wire));
    }
}

#[test]
fn only_retryable_failure_class_allows_automatic_retry() {
    for class in [
        TaskFailureClass::Retryable,
        TaskFailureClass::NonRetryable,
        TaskFailureClass::PermissionDenied,
        TaskFailureClass::MaxTurns,
        TaskFailureClass::NonConvergent,
        TaskFailureClass::Cancelled,
        TaskFailureClass::OutcomeUnknown,
        TaskFailureClass::ReconciliationRequired,
        TaskFailureClass::SideEffectUnknown,
    ] {
        assert_eq!(class.is_retryable(), class == TaskFailureClass::Retryable);
    }
}
