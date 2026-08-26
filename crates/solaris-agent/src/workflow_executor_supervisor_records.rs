use crate::execution_context::{EffectOutputStore, stable_digest_bytes, stable_digest_value};
use crate::runtime_ledger::LogicalAppendCapability;
use crate::workflow_controller::{WorkflowExecutionContext, WorkflowNodeError};
use crate::workflow_supervisor_turn_chain::SupervisorCoordinatorTurnBinding;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::{AgentId, AttemptId, OperationId, RunId, TaskId};
use solaris_types::message::TokenUsage;
use solaris_types::runtime::TaskFailureClass;
use solaris_types::spawner::{AgentOutcomeStatus, OutcomeBlobRef, SubAgentResult};

use super::{
    AgentWorkflowExecutor, HARD_MAX_SUPERVISOR_TASKS, SupervisorDecision, SupervisorRuntime, SupervisorTaskProposal,
    supervisor_task_id, supervisor_worker_message_id, supervisor_worker_operation_id,
};

pub(super) const SUPERVISOR_SCHEMA_VERSION: u8 = 1;
pub(super) const SUPERVISOR_DECISION_SCHEMA_VERSION: u8 = 2;
const SUPERVISOR_DECISION_RECORD: &str = "workflow_supervisor_decision";
const SUPERVISOR_WORKER_OUTCOME_RECORD: &str = "workflow_supervisor_worker_outcome";
const SUPERVISOR_WORKER_DELIVERY_RECORD: &str = "workflow_supervisor_worker_delivery";
const SUPERVISOR_WORKER_DELIVERY_ACK_RECORD: &str = "workflow_supervisor_worker_delivery_ack";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct SupervisorDecisionEnvelope {
    pub(super) schema_version: u8,
    pub(super) workflow_run_id: RunId,
    pub(super) workflow_id: String,
    pub(super) node_id: String,
    pub(super) attempt_id: AttemptId,
    pub(super) round: u32,
    #[serde(default)]
    pub(super) input_message_ids: Vec<String>,
    #[serde(default)]
    pub(super) input_deliveries: Vec<SupervisorDecisionInputDelivery>,
    pub(super) decision_digest: String,
    #[serde(default)]
    pub(super) turns: usize,
    #[serde(default)]
    pub(super) usage: TokenUsage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) final_output_ref: Option<OutcomeBlobRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) coordinator_turn: Option<SupervisorCoordinatorTurnBinding>,
    pub(super) decision: SupervisorDecision,
}

pub(super) struct SupervisorDecisionAccounting {
    pub(super) turns: usize,
    pub(super) usage: TokenUsage,
    pub(super) turn_outcome_ref: Option<OutcomeBlobRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct SupervisorDecisionInputDelivery {
    pub(super) message_id: String,
    pub(super) delivery_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct DurableSupervisorWorkerOutcome {
    pub(super) schema_version: u8,
    pub(super) workflow_run_id: RunId,
    pub(super) workflow_id: String,
    pub(super) node_id: String,
    pub(super) attempt_id: AttemptId,
    pub(super) task_key: String,
    pub(super) task_id: TaskId,
    pub(super) role: String,
    pub(super) proposal_digest: String,
    pub(super) agent_id: AgentId,
    pub(super) operation_id: OperationId,
    pub(super) handle_spec_digest: String,
    pub(super) result_ref: String,
    pub(super) result_bytes: u64,
    pub(super) result_digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) result_ref_status: Option<AgentOutcomeStatus>,
    #[serde(default)]
    pub(super) result_format: SupervisorWorkerResultFormat,
    pub(super) status: AgentOutcomeStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) failure_class: Option<TaskFailureClass>,
    pub(super) outcome_digest: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum SupervisorWorkerResultFormat {
    AgentOutcome,
    #[default]
    LegacySupervisorBody,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct SupervisorWorkerOutcomeBody {
    pub(super) result: SubAgentResult,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) normalized_output: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) failure_class: Option<TaskFailureClass>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct SupervisorWorkerDeliveryRef {
    pub(super) task_key: String,
    pub(super) task_id: TaskId,
    pub(super) role: String,
    pub(super) sender_agent_id: AgentId,
    pub(super) operation_id: OperationId,
    pub(super) handle_spec_digest: String,
    pub(super) outcome_digest: String,
    pub(super) result_ref: String,
    pub(super) result_bytes: u64,
    pub(super) result_digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) status: Option<AgentOutcomeStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct DurableSupervisorWorkerDelivery {
    pub(super) schema_version: u8,
    pub(super) workflow_run_id: RunId,
    pub(super) workflow_id: String,
    pub(super) node_id: String,
    pub(super) attempt_id: AttemptId,
    pub(super) dispatch_round: u32,
    pub(super) dispatch_decision_digest: String,
    pub(super) message_id: String,
    pub(super) sender: String,
    pub(super) coordinator_agent_id: AgentId,
    pub(super) outcomes: Vec<SupervisorWorkerDeliveryRef>,
    pub(super) delivery_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(in super::super) struct SupervisorWorkerDeliveryAck {
    schema_version: u8,
    workflow_run_id: RunId,
    workflow_id: String,
    node_id: String,
    attempt_id: AttemptId,
    message_id: String,
    delivery_digest: String,
    dispatch_round: u32,
    coordinator_agent_id: AgentId,
    decision_round: u32,
    decision_digest: String,
    ack_digest: String,
}

#[path = "workflow_executor_supervisor_ack_records.rs"]
mod ack;
#[path = "workflow_executor_supervisor_causality.rs"]
mod causality;
#[path = "workflow_executor_supervisor_decision_records.rs"]
mod decision;
#[path = "workflow_executor_supervisor_delivery_records.rs"]
mod delivery;
#[path = "workflow_executor_supervisor_history.rs"]
mod history;
#[path = "workflow_executor_supervisor_outcome_records.rs"]
mod outcome;
#[path = "workflow_executor_supervisor_turn_records.rs"]
mod turn;

#[cfg(test)]
pub(super) use ack::validate_supervisor_pending_delivery_integrity_for_test;
pub(super) use outcome::supervisor_worker_outcome_digest;

fn supervisor_ack_digest(acknowledgement: &SupervisorWorkerDeliveryAck) -> String {
    ack::supervisor_ack_digest(acknowledgement)
}

impl AgentWorkflowExecutor {
    fn persist_unique_supervisor_record(
        &self,
        record_type: &str,
        identity_fields: &[&str],
        payload: Value,
    ) -> Result<(), WorkflowNodeError> {
        let lifecycle = self.spawner.lifecycle_runtime();
        let ledger = lifecycle.ledger();
        if ledger.logical_append_capability() == LogicalAppendCapability::Unsupported {
            return Err(WorkflowNodeError::reconciliation_required(
                "runtime ledger does not support atomic Supervisor record persistence",
            ));
        }
        lifecycle
            .with_run_mutation(self.spawner.run_id(), || {
                ledger
                    .compare_and_append(
                        self.spawner.run_id(),
                        DurabilityClass::SyncCritical,
                        record_type,
                        identity_fields,
                        payload,
                    )
                    .map(|_| ())
            })
            .map_err(|error| WorkflowNodeError::reconciliation_required(error.to_string()))
    }
}

#[cfg(test)]
#[path = "workflow_executor_supervisor_records_sqlite_test.rs"]
mod records_sqlite_test;
