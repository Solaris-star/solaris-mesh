use solaris_providers::error::ProviderError;
use solaris_types::runtime::TaskFailureClass;

#[derive(Debug, thiserror::Error)]
pub enum AgentError {
    #[error("API error: {0}")]
    ApiError(String),
    #[error(
        "provider repeatedly returned tool-call malformed outputs ({count}/{limit}); stopped to avoid wasting tokens"
    )]
    ToolCallMalformed { count: usize, limit: usize },
    #[error(
        "stopped after {count}/{limit} consecutive tool-call failures; the task did not converge. Try adjusting the request or retrying."
    )]
    ToolCallFailures { count: usize, limit: usize },
    #[error("Provider error: {0}")]
    Provider(#[from] ProviderError),
    #[error("User aborted the session")]
    UserAborted,
    #[error("durable task '{task_key}' requires reconciliation before it can continue (call_id={call_id:?})")]
    ReconciliationRequired { task_key: String, call_id: Option<String> },
    #[error("Solaris Mesh resource budget exceeded: {0}")]
    ResourceBudgetExceeded(String),
    #[error("Context window nearly full ({input_tokens} tokens used, limit {limit})")]
    ContextTooLong { input_tokens: u64, limit: usize },
}

impl AgentError {
    /// Typed failure classification for retry decisions.
    ///
    /// Transient provider failures are Retryable; convergence, budget, and
    /// reconciliation failures are not.
    pub fn failure_class(&self) -> TaskFailureClass {
        match self {
            AgentError::ApiError(_) => TaskFailureClass::Retryable,
            AgentError::Provider(error) => {
                if error.is_retryable() {
                    TaskFailureClass::Retryable
                } else {
                    TaskFailureClass::NonRetryable
                }
            }
            AgentError::ToolCallMalformed { .. } | AgentError::ToolCallFailures { .. } => {
                TaskFailureClass::NonConvergent
            }
            AgentError::UserAborted => TaskFailureClass::Cancelled,
            AgentError::ReconciliationRequired { .. } => TaskFailureClass::ReconciliationRequired,
            AgentError::ResourceBudgetExceeded(_) | AgentError::ContextTooLong { .. } => TaskFailureClass::NonRetryable,
        }
    }
}
