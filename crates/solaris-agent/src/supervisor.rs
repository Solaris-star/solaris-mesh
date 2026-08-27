use std::sync::Arc;

use serde_json::json;
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::OperationId;
use solaris_types::runtime::TaskFailureClass;
use solaris_types::spawner::{
    AgentOutcome, AgentOutcomeStatus, AgentSpawnError, AgentSpawnService, AgentSpawnSpec, SubAgentResult,
};

use crate::spawner::AgentSpawner;

/// Durable coordinator for Supervisor strategy nodes.
///
/// It owns retry decisions and records every assignment/result before a live
/// event is published. Dropping an in-flight attempt is handled by the
/// spawner's cancellation guard.
pub struct SupervisorCoordinator {
    spawner: Arc<AgentSpawner>,
}

impl SupervisorCoordinator {
    pub fn new(spawner: Arc<AgentSpawner>) -> Self {
        Self { spawner }
    }

    pub async fn execute(&self, spec: AgentSpawnSpec, max_attempts: u32) -> SubAgentResult {
        let config = spec.config.clone();
        let operation_id = spec.operation_id.clone();
        let runtime = self.spawner.lifecycle_runtime();
        let run_id = self.spawner.run_id().clone();
        let attempts = max_attempts.max(1);
        if runtime
            .ledger()
            .append(
                &run_id,
                DurabilityClass::SyncCritical,
                "supervisor_started",
                json!({"operation_id": operation_id, "worker": config.name, "max_attempts": attempts}),
            )
            .is_err()
        {
            return supervisor_error(&config.name, "failed to persist Supervisor start");
        }
        runtime.emit_live_event(
            run_id.clone(),
            Some(self.spawner.parent_agent_id().clone()),
            "supervisor_started",
            json!({"operation_id": operation_id, "worker": config.name, "max_attempts": attempts}),
        );

        let mut last = None;
        for attempt in 1..=attempts {
            let attempt_operation = OperationId::new(format!("{}:supervisor:{attempt}", operation_id.as_str()));
            if runtime
                .ledger()
                .append(
                    &run_id,
                    DurabilityClass::SyncCritical,
                    "supervisor_assignment",
                    json!({
                        "operation_id": operation_id,
                        "attempt_operation_id": attempt_operation,
                        "attempt": attempt,
                        "worker": config.name,
                    }),
                )
                .is_err()
            {
                return supervisor_error(&config.name, "failed to persist Supervisor assignment");
            }
            runtime.emit_live_event(
                run_id.clone(),
                Some(self.spawner.parent_agent_id().clone()),
                "supervisor_assignment",
                json!({"operation_id": operation_id, "attempt_operation_id": attempt_operation, "attempt": attempt}),
            );
            let mut attempt_spec = spec.clone();
            attempt_spec.operation_id = attempt_operation;
            let (result, retryable) = match self.spawner.spawn(attempt_spec).await {
                Ok(handle) => match self.spawner.join(&handle).await {
                    Ok(outcome) => {
                        let retryable = outcome.failure_class == Some(TaskFailureClass::Retryable);
                        (supervisor_outcome(outcome), retryable)
                    }
                    Err(error) => (supervisor_reconciliation(&config.name, error), false),
                },
                Err(error) => {
                    let retryable = error.failure_class == TaskFailureClass::Retryable;
                    (supervisor_spawn_failure(&config.name, error), retryable)
                }
            };
            let record_type = if result.is_error {
                "supervisor_attempt_failed"
            } else {
                "supervisor_settled"
            };
            if runtime
                .ledger()
                .append(
                    &run_id,
                    DurabilityClass::SyncCritical,
                    record_type,
                    json!({"operation_id": operation_id, "attempt": attempt, "result": result}),
                )
                .is_err()
            {
                return supervisor_error(&config.name, "failed to persist Supervisor result");
            }
            runtime.emit_live_event(
                run_id.clone(),
                Some(self.spawner.parent_agent_id().clone()),
                record_type,
                json!({"operation_id": operation_id, "attempt": attempt, "is_error": result.is_error}),
            );
            if !result.is_error || !retryable {
                return result;
            }
            last = Some(result);
        }
        last.unwrap_or_else(|| supervisor_error(&config.name, "Supervisor did not execute an attempt"))
    }
}

fn supervisor_error(name: &str, message: &str) -> SubAgentResult {
    SubAgentResult {
        name: name.to_owned(),
        agent_id: None,
        task_id: None,
        status: solaris_types::spawner::AgentOutcomeStatus::Failed,
        output: Some(serde_json::json!({"error": message})),
        text: message.to_owned(),
        usage: Default::default(),
        turns: 0,
        failure_class: Some(TaskFailureClass::NonRetryable),
        is_error: true,
    }
}

fn supervisor_spawn_failure(name: &str, error: AgentSpawnError) -> SubAgentResult {
    let status = match error.failure_class {
        TaskFailureClass::OutcomeUnknown => AgentOutcomeStatus::OutcomeUnknown,
        TaskFailureClass::ReconciliationRequired => AgentOutcomeStatus::ReconciliationRequired,
        TaskFailureClass::Cancelled => AgentOutcomeStatus::Cancelled,
        TaskFailureClass::Retryable
        | TaskFailureClass::NonRetryable
        | TaskFailureClass::PermissionDenied
        | TaskFailureClass::MaxTurns
        | TaskFailureClass::NonConvergent
        | TaskFailureClass::SideEffectUnknown => AgentOutcomeStatus::Failed,
    };
    SubAgentResult {
        name: name.to_owned(),
        agent_id: None,
        task_id: None,
        status,
        output: Some(json!({"error": &error.message, "failure_class": error.failure_class})),
        text: error.message,
        usage: Default::default(),
        turns: 0,
        failure_class: Some(error.failure_class),
        is_error: true,
    }
}

fn supervisor_reconciliation(name: &str, message: String) -> SubAgentResult {
    SubAgentResult {
        name: name.to_owned(),
        agent_id: None,
        task_id: None,
        status: AgentOutcomeStatus::ReconciliationRequired,
        output: Some(json!({"error": &message, "reconciliation_required": true})),
        text: message,
        usage: Default::default(),
        turns: 0,
        failure_class: Some(TaskFailureClass::ReconciliationRequired),
        is_error: true,
    }
}

fn supervisor_outcome(outcome: AgentOutcome) -> SubAgentResult {
    let text = outcome
        .output
        .get("text")
        .and_then(serde_json::Value::as_str)
        .or(outcome.error.as_deref())
        .unwrap_or_default()
        .to_owned();
    let output = match outcome.failure_class {
        Some(failure_class) => {
            let mut output = outcome.output;
            if let Some(object) = output.as_object_mut() {
                object.insert("failure_class".to_owned(), json!(failure_class));
            }
            output
        }
        None => outcome.output,
    };
    SubAgentResult {
        name: outcome.handle.spec.config.name.clone(),
        agent_id: Some(outcome.handle.agent_id),
        task_id: Some(outcome.handle.task_id),
        status: outcome.status,
        output: Some(output),
        text,
        usage: outcome.usage,
        turns: outcome.turns,
        failure_class: outcome.failure_class,
        is_error: outcome.status != AgentOutcomeStatus::Completed,
    }
}

#[cfg(test)]
#[path = "supervisor_test.rs"]
mod supervisor_test;
