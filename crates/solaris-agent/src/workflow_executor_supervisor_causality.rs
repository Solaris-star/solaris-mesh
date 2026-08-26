use solaris_types::runtime::{TaskRecord, TaskState};
use solaris_types::spawner::AgentHandle;

use crate::runtime_ledger::LedgerRecord;

use super::*;

#[derive(Debug, Clone)]
pub(in super::super) struct SequencedRecord<T> {
    pub(in super::super) seq: u64,
    pub(in super::super) value: T,
}

impl AgentWorkflowExecutor {
    pub(super) fn validate_supervisor_delivery_causality(
        &self,
        runtime: &SupervisorRuntime<'_>,
        delivery: &DurableSupervisorWorkerDelivery,
        delivery_record: &LedgerRecord,
        outcomes: &[SequencedRecord<DurableSupervisorWorkerOutcome>],
        records: &[LedgerRecord],
    ) -> Result<(), WorkflowNodeError> {
        let dispatch_record = unique_record(records, "Dispatch decision", |record| {
            record.record_type == SUPERVISOR_DECISION_RECORD
                && record.payload.get("workflow_run_id").and_then(Value::as_str)
                    == Some(runtime.context.run_id.as_str())
                && record.payload.get("workflow_id").and_then(Value::as_str)
                    == Some(runtime.context.workflow.implementation_id.as_str())
                && record.payload.get("node_id").and_then(Value::as_str) == Some(runtime.context.node.id.as_str())
                && record.payload.get("attempt_id").and_then(Value::as_str) == Some(runtime.context.attempt_id.as_str())
                && record.payload.get("round").and_then(Value::as_u64) == Some(u64::from(delivery.dispatch_round))
                && record.payload.get("decision_digest").and_then(Value::as_str)
                    == Some(delivery.dispatch_decision_digest.as_str())
        })?;
        if dispatch_record.seq >= delivery_record.seq {
            return Err(WorkflowNodeError::reconciliation_required(
                "Supervisor worker delivery is not ordered after its durable Dispatch decision",
            ));
        }
        for reference in &delivery.outcomes {
            let task_created = unique_record(records, "task creation", |record| {
                record.record_type == "task_created"
                    && record.payload.get("task_id").and_then(Value::as_str) == Some(reference.task_id.as_str())
            })?;
            let task: TaskRecord = serde_json::from_value(task_created.payload.clone()).map_err(|error| {
                WorkflowNodeError::reconciliation_required(format!("invalid Supervisor task creation: {error}"))
            })?;
            if task.run_id != *self.spawner.run_id()
                || task.task_id != reference.task_id
                || task.task_key.as_deref() != Some(reference.task_key.as_str())
                || task.team_id.as_ref() != Some(&runtime.team_id)
                || task.workflow_id.as_deref() != Some(runtime.context.workflow.implementation_id.as_str())
                || task.role.as_deref() != Some(reference.role.as_str())
                || task.revision != 0
                || task.owner_agent_id.is_some()
                || task.state != TaskState::Queued
                || task
                    .content
                    .as_ref()
                    .and_then(|content| content.get("decision_round"))
                    .and_then(Value::as_u64)
                    != Some(u64::from(delivery.dispatch_round))
            {
                return Err(WorkflowNodeError::reconciliation_required(
                    "durable Supervisor task creation identity changed",
                ));
            }

            let handle_record = unique_record(records, "Agent handle", |record| {
                record.record_type == "agent_handle_issued"
                    && record.payload.get("task_id").and_then(Value::as_str) == Some(reference.task_id.as_str())
                    && record.payload.get("operation_id").and_then(Value::as_str)
                        == Some(reference.operation_id.as_str())
            })?;
            let handle: AgentHandle = serde_json::from_value(handle_record.payload.clone()).map_err(|error| {
                WorkflowNodeError::reconciliation_required(format!("invalid Supervisor Agent handle: {error}"))
            })?;
            if handle.run_id != *self.spawner.run_id()
                || handle.task_id != reference.task_id
                || handle.operation_id != reference.operation_id
                || handle.agent_id != reference.sender_agent_id
                || handle.spec_digest != reference.handle_spec_digest
            {
                return Err(WorkflowNodeError::reconciliation_required(
                    "durable Supervisor Agent handle identity changed",
                ));
            }

            let assignment_operation = format!("{}:task-assign", reference.operation_id);
            let assignment = unique_record(records, "task assignment", |record| {
                record.record_type == "task_cas"
                    && record.payload.get("task_id").and_then(Value::as_str) == Some(reference.task_id.as_str())
                    && record.payload.get("operation_id").and_then(Value::as_str) == Some(assignment_operation.as_str())
                    && record.payload.get("transition").and_then(Value::as_str) == Some("assign")
                    && record
                        .payload
                        .get("task")
                        .and_then(|task| task.get("owner_agent_id"))
                        .and_then(Value::as_str)
                        == Some(reference.sender_agent_id.as_str())
            })?;

            let mut supervisor_outcomes = outcomes.iter().filter(|outcome| {
                outcome.value.task_id == reference.task_id
                    && outcome.value.operation_id == reference.operation_id
                    && outcome.value.outcome_digest == reference.outcome_digest
            });
            let Some(supervisor_outcome) = supervisor_outcomes.next() else {
                return Err(WorkflowNodeError::reconciliation_required(
                    "Supervisor causal history has no worker outcome",
                ));
            };
            if supervisor_outcomes.next().is_some() {
                return Err(WorkflowNodeError::reconciliation_required(
                    "Supervisor causal history has duplicate worker outcomes",
                ));
            }
            let outcome = &supervisor_outcome.value;

            let agent_outcomes: Vec<_> = records
                .iter()
                .filter(|record| {
                    record.record_type == "agent_outcome"
                        && record.payload.get("spawn_operation_id").and_then(Value::as_str)
                            == Some(reference.operation_id.as_str())
                        && record.payload.get("child_agent_id").and_then(Value::as_str)
                            == Some(reference.sender_agent_id.as_str())
                })
                .collect();
            let agent_outcome = match (outcome.result_format, agent_outcomes.as_slice()) {
                (SupervisorWorkerResultFormat::AgentOutcome, [record]) => Some(*record),
                (SupervisorWorkerResultFormat::AgentOutcome, _) => {
                    return Err(WorkflowNodeError::reconciliation_required(
                        "Supervisor outcome does not have exactly one durable Agent outcome",
                    ));
                }
                (SupervisorWorkerResultFormat::LegacySupervisorBody, []) => None,
                (SupervisorWorkerResultFormat::LegacySupervisorBody, [record]) => Some(*record),
                (SupervisorWorkerResultFormat::LegacySupervisorBody, _) => {
                    return Err(WorkflowNodeError::reconciliation_required(
                        "legacy Supervisor outcome has duplicate durable Agent outcomes",
                    ));
                }
            };

            let settlement = unique_record(records, "task settlement", |record| {
                record.record_type == "task_cas"
                    && record.payload.get("task_id").and_then(Value::as_str) == Some(reference.task_id.as_str())
                    && record.payload.get("transition").and_then(Value::as_str) == Some("settle")
                    && record
                        .payload
                        .get("task")
                        .and_then(|task| task.get("owner_agent_id"))
                        .and_then(Value::as_str)
                        == Some(reference.sender_agent_id.as_str())
                    && record
                        .payload
                        .get("task")
                        .and_then(|task| task.get("outcome_ref"))
                        .and_then(Value::as_str)
                        == Some(reference.result_ref.as_str())
            })?;

            // These anchors are written synchronously by the Supervisor path. Generic
            // permission/effect records are validated by their own subsystem and are
            // deliberately not assumed to have a fixed position in this chain.
            require_strict_order("Dispatch decision", dispatch_record, "task creation", task_created)?;
            require_strict_order("task creation", task_created, "Agent handle", handle_record)?;
            require_strict_order("Agent handle", handle_record, "task assignment", assignment)?;
            if let Some(agent_outcome) = agent_outcome {
                require_strict_order("task assignment", assignment, "Agent outcome", agent_outcome)?;
                require_strict_sequence(
                    "Agent outcome",
                    agent_outcome.seq,
                    "Supervisor worker outcome",
                    supervisor_outcome.seq,
                )?;
            } else {
                require_strict_sequence(
                    "task assignment",
                    assignment.seq,
                    "legacy Supervisor worker outcome",
                    supervisor_outcome.seq,
                )?;
            }
            require_strict_sequence(
                "Supervisor worker outcome",
                supervisor_outcome.seq,
                "task settlement",
                settlement.seq,
            )?;
            require_strict_order("task settlement", settlement, "worker delivery", delivery_record)?;
        }
        Ok(())
    }
}

fn require_strict_order(
    earlier_name: &str,
    earlier: &LedgerRecord,
    later_name: &str,
    later: &LedgerRecord,
) -> Result<(), WorkflowNodeError> {
    require_strict_sequence(earlier_name, earlier.seq, later_name, later.seq)
}

fn require_strict_sequence(
    earlier_name: &str,
    earlier_seq: u64,
    later_name: &str,
    later_seq: u64,
) -> Result<(), WorkflowNodeError> {
    if earlier_seq < later_seq {
        Ok(())
    } else {
        Err(WorkflowNodeError::reconciliation_required(format!(
            "Supervisor causal history requires {earlier_name} before {later_name}"
        )))
    }
}

fn unique_record<'a>(
    records: &'a [LedgerRecord],
    name: &str,
    predicate: impl Fn(&LedgerRecord) -> bool,
) -> Result<&'a LedgerRecord, WorkflowNodeError> {
    let mut matches = records.iter().filter(|record| predicate(record));
    let Some(found) = matches.next() else {
        return Err(WorkflowNodeError::reconciliation_required(format!(
            "Supervisor causal history has no {name}"
        )));
    };
    if matches.next().is_some() {
        return Err(WorkflowNodeError::reconciliation_required(format!(
            "Supervisor causal history has duplicate {name} records"
        )));
    }
    Ok(found)
}
