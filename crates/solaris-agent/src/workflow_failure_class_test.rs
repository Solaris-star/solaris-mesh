use super::*;

use solaris_types::message::TokenUsage;

fn failed_result(failure_class: Option<TaskFailureClass>) -> SubAgentResult {
    SubAgentResult {
        name: "worker".to_owned(),
        agent_id: None,
        task_id: None,
        status: AgentOutcomeStatus::Failed,
        failure_class,
        output: None,
        text: "failed".to_owned(),
        usage: TokenUsage::default(),
        turns: 1,
        is_error: true,
    }
}

#[test]
fn workflow_failure_preserves_every_typed_class() {
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
        let error = workflow_failure_from_result(&failed_result(Some(class)));
        assert_eq!(error.failure_class, class);
        assert_eq!(error.message, "failed");
    }
}

#[test]
fn bare_failed_result_is_conservatively_non_retryable() {
    let error = workflow_failure_from_result(&failed_result(None));
    assert_eq!(error.failure_class, TaskFailureClass::NonRetryable);
}

#[test]
fn status_specific_recovery_semantics_override_missing_class() {
    let mut result = failed_result(None);
    result.status = AgentOutcomeStatus::OutcomeUnknown;
    assert_eq!(
        workflow_failure_from_result(&result).failure_class,
        TaskFailureClass::OutcomeUnknown
    );

    result.status = AgentOutcomeStatus::ReconciliationRequired;
    assert_eq!(
        workflow_failure_from_result(&result).failure_class,
        TaskFailureClass::ReconciliationRequired
    );

    result.status = AgentOutcomeStatus::Cancelled;
    assert_eq!(
        workflow_failure_from_result(&result).failure_class,
        TaskFailureClass::Cancelled
    );
}
