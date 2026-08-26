#[cfg(test)]
use solaris_types::workflow::CollaborationRuntimeConfig;

use super::delivery::supervisor_worker_message_prefix;
use super::*;

#[cfg(test)]
pub(in super::super) async fn validate_supervisor_pending_delivery_integrity_for_test(
    executor: &AgentWorkflowExecutor,
    context: &WorkflowExecutionContext,
    config: &CollaborationRuntimeConfig,
) -> Result<(), WorkflowNodeError> {
    let role_input = json!({
        "parameters": context.parameters,
        "dependency_outputs": context.dependency_outputs,
        "bound_inputs": context.bound_inputs,
    });
    let runtime = executor.open_supervisor(context, config, &role_input).await?;
    executor.pending_supervisor_messages(&runtime)?;
    Ok(())
}

impl AgentWorkflowExecutor {
    pub(in super::super) fn acknowledge_supervisor_messages(
        &self,
        runtime: &SupervisorRuntime<'_>,
        decision: &SupervisorDecisionEnvelope,
        deliveries: Vec<DurableSupervisorWorkerDelivery>,
    ) -> Result<(), WorkflowNodeError> {
        for delivery in deliveries {
            let mut ack = SupervisorWorkerDeliveryAck {
                schema_version: SUPERVISOR_SCHEMA_VERSION,
                workflow_run_id: runtime.context.run_id.clone(),
                workflow_id: runtime.context.workflow.implementation_id.clone(),
                node_id: runtime.context.node.id.clone(),
                attempt_id: runtime.context.attempt_id.clone(),
                message_id: delivery.message_id,
                delivery_digest: delivery.delivery_digest,
                dispatch_round: delivery.dispatch_round,
                coordinator_agent_id: runtime.handle.agent_id.clone(),
                decision_round: decision.round,
                decision_digest: decision.decision_digest.clone(),
                ack_digest: String::new(),
            };
            ack.ack_digest = supervisor_ack_digest(&ack);
            let payload =
                serde_json::to_value(ack).map_err(|error| WorkflowNodeError::non_retryable(error.to_string()))?;
            self.persist_unique_supervisor_record(SUPERVISOR_WORKER_DELIVERY_ACK_RECORD, &["message_id"], payload)?;
        }
        Ok(())
    }

    pub(in super::super) fn load_supervisor_delivery_acks(
        &self,
        runtime: &SupervisorRuntime<'_>,
    ) -> Result<std::collections::BTreeMap<String, SupervisorWorkerDeliveryAck>, WorkflowNodeError> {
        let prefix = supervisor_worker_message_prefix(runtime.context);
        let mut acknowledgements = std::collections::BTreeMap::new();
        let records = self
            .spawner
            .lifecycle_runtime()
            .ledger()
            .records_for_run(self.spawner.run_id())
            .map_err(|error| WorkflowNodeError::reconciliation_required(error.to_string()))?;
        for record in &records {
            if record.record_type != SUPERVISOR_WORKER_DELIVERY_ACK_RECORD
                || !record
                    .payload
                    .get("message_id")
                    .and_then(Value::as_str)
                    .is_some_and(|message_id| message_id.starts_with(&prefix))
            {
                continue;
            }
            let ack: SupervisorWorkerDeliveryAck = serde_json::from_value(record.payload.clone()).map_err(|error| {
                WorkflowNodeError::reconciliation_required(format!("invalid worker delivery ACK: {error}"))
            })?;
            if ack.schema_version != SUPERVISOR_SCHEMA_VERSION
                || ack.workflow_run_id != runtime.context.run_id
                || ack.workflow_id != runtime.context.workflow.implementation_id
                || ack.node_id != runtime.context.node.id
                || ack.attempt_id != runtime.context.attempt_id
                || ack.coordinator_agent_id != runtime.handle.agent_id
                || ack.ack_digest != supervisor_ack_digest(&ack)
            {
                return Err(WorkflowNodeError::reconciliation_required(
                    "durable Supervisor worker delivery ACK identity changed",
                ));
            }
            let deliveries: Vec<_> = records
                .iter()
                .filter(|candidate| {
                    candidate.record_type == SUPERVISOR_WORKER_DELIVERY_RECORD
                        && candidate.payload.get("message_id").and_then(Value::as_str) == Some(&ack.message_id)
                })
                .collect();
            let decision_records: Vec<_> = records
                .iter()
                .filter(|candidate| {
                    candidate.record_type == SUPERVISOR_DECISION_RECORD
                        && candidate.payload.get("workflow_run_id").and_then(Value::as_str)
                            == Some(runtime.context.run_id.as_str())
                        && candidate.payload.get("node_id").and_then(Value::as_str)
                            == Some(runtime.context.node.id.as_str())
                        && candidate.payload.get("attempt_id").and_then(Value::as_str)
                            == Some(runtime.context.attempt_id.as_str())
                        && candidate.payload.get("round").and_then(Value::as_u64) == Some(u64::from(ack.decision_round))
                })
                .collect();
            if deliveries.len() != 1
                || decision_records.len() != 1
                || record.seq <= deliveries[0].seq
                || record.seq <= decision_records[0].seq
            {
                return Err(WorkflowNodeError::reconciliation_required(
                    "Supervisor worker delivery ACK is not ordered after one delivery and decision",
                ));
            }
            let delivery: DurableSupervisorWorkerDelivery = serde_json::from_value(deliveries[0].payload.clone())
                .map_err(|error| {
                    WorkflowNodeError::reconciliation_required(format!("invalid worker delivery: {error}"))
                })?;
            self.validate_supervisor_delivery_record(runtime, &delivery, deliveries[0], &records)?;
            let decision = self
                .load_supervisor_decision(runtime.context, ack.decision_round)?
                .ok_or_else(|| WorkflowNodeError::reconciliation_required("delivery ACK has no consuming decision"))?;
            if ack.delivery_digest != delivery.delivery_digest
                || ack.dispatch_round != delivery.dispatch_round
                || ack.decision_digest != decision.decision_digest
                || ack.dispatch_round >= ack.decision_round
                || !decision
                    .input_deliveries
                    .iter()
                    .any(|input| input.message_id == ack.message_id && input.delivery_digest == ack.delivery_digest)
            {
                return Err(WorkflowNodeError::reconciliation_required(
                    "Supervisor worker delivery ACK does not match its delivery and consuming decision",
                ));
            }
            if acknowledgements.insert(ack.message_id.clone(), ack).is_some() {
                return Err(WorkflowNodeError::reconciliation_required(
                    "duplicate Supervisor worker delivery ACK identity",
                ));
            }
        }
        Ok(acknowledgements)
    }
}

pub(super) fn supervisor_ack_digest(ack: &SupervisorWorkerDeliveryAck) -> String {
    stable_digest_value(&json!({
        "schema_version": ack.schema_version,
        "workflow_run_id": ack.workflow_run_id,
        "workflow_id": ack.workflow_id,
        "node_id": ack.node_id,
        "attempt_id": ack.attempt_id,
        "message_id": ack.message_id,
        "delivery_digest": ack.delivery_digest,
        "dispatch_round": ack.dispatch_round,
        "coordinator_agent_id": ack.coordinator_agent_id,
        "decision_round": ack.decision_round,
        "decision_digest": ack.decision_digest,
    }))
}
