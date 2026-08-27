use solaris_types::runtime::TaskFailureClass;
use solaris_types::spawner::AgentSpawnError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowNodeError {
    pub failure_class: TaskFailureClass,
    pub message: String,
}

impl WorkflowNodeError {
    pub fn retryable(message: impl Into<String>) -> Self {
        Self {
            failure_class: TaskFailureClass::Retryable,
            message: message.into(),
        }
    }

    pub fn non_retryable(message: impl Into<String>) -> Self {
        Self {
            failure_class: TaskFailureClass::NonRetryable,
            message: message.into(),
        }
    }

    pub fn outcome_unknown(message: impl Into<String>) -> Self {
        Self {
            failure_class: TaskFailureClass::OutcomeUnknown,
            message: message.into(),
        }
    }

    pub fn reconciliation_required(message: impl Into<String>) -> Self {
        Self {
            failure_class: TaskFailureClass::ReconciliationRequired,
            message: message.into(),
        }
    }

    pub fn permission_denied(message: impl Into<String>) -> Self {
        Self {
            failure_class: TaskFailureClass::PermissionDenied,
            message: message.into(),
        }
    }

    pub fn max_turns(message: impl Into<String>) -> Self {
        Self {
            failure_class: TaskFailureClass::MaxTurns,
            message: message.into(),
        }
    }

    pub fn non_convergent(message: impl Into<String>) -> Self {
        Self {
            failure_class: TaskFailureClass::NonConvergent,
            message: message.into(),
        }
    }

    pub fn cancelled(message: impl Into<String>) -> Self {
        Self {
            failure_class: TaskFailureClass::Cancelled,
            message: message.into(),
        }
    }

    pub fn side_effect_unknown(message: impl Into<String>) -> Self {
        Self {
            failure_class: TaskFailureClass::SideEffectUnknown,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for WorkflowNodeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for WorkflowNodeError {}

impl From<String> for WorkflowNodeError {
    fn from(message: String) -> Self {
        Self::non_retryable(message)
    }
}

impl From<&str> for WorkflowNodeError {
    fn from(message: &str) -> Self {
        Self::non_retryable(message)
    }
}

impl From<AgentSpawnError> for WorkflowNodeError {
    fn from(error: AgentSpawnError) -> Self {
        Self {
            failure_class: error.failure_class,
            message: error.message,
        }
    }
}
