use super::*;

use solaris_providers::error::ProviderError;

#[test]
fn centralized_agent_error_classification_is_conservative() {
    let cases = [
        (
            AgentError::ApiError("internal".to_owned()),
            TaskFailureClass::NonRetryable,
        ),
        (
            AgentError::PermissionDenied("provider".to_owned()),
            TaskFailureClass::PermissionDenied,
        ),
        (
            AgentError::OutcomeUnknown("provider".to_owned()),
            TaskFailureClass::OutcomeUnknown,
        ),
        (
            AgentError::SideEffectUnknown("tool".to_owned()),
            TaskFailureClass::SideEffectUnknown,
        ),
        (
            AgentError::DurableState("corrupt".to_owned()),
            TaskFailureClass::ReconciliationRequired,
        ),
        (AgentError::UserAborted, TaskFailureClass::Cancelled),
        (
            AgentError::ToolCallMalformed { count: 2, limit: 2 },
            TaskFailureClass::NonConvergent,
        ),
        (
            AgentError::ToolCallFailures { count: 2, limit: 2 },
            TaskFailureClass::NonConvergent,
        ),
        (
            AgentError::ContextTooLong {
                input_tokens: 10,
                limit: 8,
            },
            TaskFailureClass::NonRetryable,
        ),
        (
            AgentError::ResourceBudgetExceeded("tokens".to_owned()),
            TaskFailureClass::NonRetryable,
        ),
    ];

    for (error, expected) in cases {
        assert_eq!(error.failure_class(), expected, "{error}");
    }
}

#[test]
fn only_explicit_transient_provider_failures_are_retryable() {
    assert_eq!(
        AgentError::Provider(ProviderError::Connection("reset".to_owned())).failure_class(),
        TaskFailureClass::Retryable
    );
    assert_eq!(
        AgentError::Provider(ProviderError::RateLimited {
            retry_after_ms: 25,
            body: None,
        })
        .failure_class(),
        TaskFailureClass::Retryable
    );
    assert_eq!(
        AgentError::Provider(ProviderError::Api {
            status: 403,
            message: "forbidden".to_owned(),
        })
        .failure_class(),
        TaskFailureClass::NonRetryable
    );
    assert_eq!(
        AgentError::Provider(ProviderError::PromptTooLong("large".to_owned())).failure_class(),
        TaskFailureClass::NonRetryable
    );
}
