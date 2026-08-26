use std::collections::{BTreeMap, BTreeSet};

use crate::runtime_ledger::LedgerRecord;

use super::delivery::supervisor_worker_message_prefix;
use super::*;

impl AgentWorkflowExecutor {
    pub(super) fn validate_supervisor_decision_pending_history(
        &self,
        runtime: &SupervisorRuntime<'_>,
        envelope: &SupervisorDecisionEnvelope,
        records: &[LedgerRecord],
        decision_record: &LedgerRecord,
    ) -> Result<(), WorkflowNodeError> {
        let prefix = supervisor_worker_message_prefix(runtime.context);
        let mut supervisor_sequences = BTreeSet::new();
        for record in records.iter().filter(|record| record.seq <= decision_record.seq) {
            if matches!(
                record.record_type.as_str(),
                SUPERVISOR_DECISION_RECORD
                    | SUPERVISOR_WORKER_OUTCOME_RECORD
                    | SUPERVISOR_WORKER_DELIVERY_RECORD
                    | SUPERVISOR_WORKER_DELIVERY_ACK_RECORD
            ) && supervisor_history_record_is_relevant(record, runtime, &prefix)
                && !supervisor_sequences.insert(record.seq)
            {
                return Err(WorkflowNodeError::reconciliation_required(
                    "Supervisor history contains duplicate causal sequence identities",
                ));
            }
        }
        let mut ordered: Vec<_> = records
            .iter()
            .enumerate()
            .filter(|(_, record)| record.seq < decision_record.seq)
            .collect();
        ordered.sort_by_key(|(index, record)| (record.seq, *index));

        let mut deliveries = BTreeMap::new();
        for (_, record) in &ordered {
            if record.record_type != SUPERVISOR_WORKER_DELIVERY_RECORD
                || !supervisor_history_record_is_relevant(record, runtime, &prefix)
            {
                continue;
            }
            let delivery: DurableSupervisorWorkerDelivery =
                serde_json::from_value(record.payload.clone()).map_err(|error| {
                    WorkflowNodeError::reconciliation_required(format!("invalid worker delivery: {error}"))
                })?;
            self.validate_supervisor_delivery_record(runtime, &delivery, record, records)?;
            if delivery.dispatch_round >= envelope.round {
                return Err(WorkflowNodeError::reconciliation_required(
                    "Supervisor worker delivery is not ordered after an earlier dispatch decision",
                ));
            }
            let input = SupervisorDecisionInputDelivery {
                message_id: delivery.message_id.clone(),
                delivery_digest: delivery.delivery_digest.clone(),
            };
            if deliveries
                .insert(delivery.message_id.clone(), (record.seq, delivery, input))
                .is_some()
            {
                return Err(WorkflowNodeError::reconciliation_required(
                    "duplicate Supervisor worker delivery identity",
                ));
            }
        }

        let mut acknowledged = BTreeSet::new();
        for (_, record) in &ordered {
            if record.record_type != SUPERVISOR_WORKER_DELIVERY_ACK_RECORD
                || !supervisor_history_record_is_relevant(record, runtime, &prefix)
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
            let Some((delivery_seq, delivery, input)) = deliveries.get(&ack.message_id) else {
                return Err(WorkflowNodeError::reconciliation_required(
                    "Supervisor worker delivery ACK has no matching earlier delivery",
                ));
            };
            let consuming_records: Vec<_> = records
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
            if consuming_records.len() != 1
                || *delivery_seq >= consuming_records[0].seq
                || consuming_records[0].seq >= record.seq
                || record.seq >= decision_record.seq
            {
                return Err(WorkflowNodeError::reconciliation_required(
                    "Supervisor worker delivery ACK is not ordered after one delivery and decision",
                ));
            }
            let consuming = self
                .load_supervisor_decision(runtime.context, ack.decision_round)?
                .ok_or_else(|| WorkflowNodeError::reconciliation_required("delivery ACK has no consuming decision"))?;
            if ack.delivery_digest != delivery.delivery_digest
                || ack.dispatch_round != delivery.dispatch_round
                || ack.decision_round >= envelope.round
                || ack.decision_digest != consuming.decision_digest
                || !consuming.input_deliveries.contains(input)
            {
                return Err(WorkflowNodeError::reconciliation_required(
                    "Supervisor worker delivery ACK does not match its delivery and consuming decision",
                ));
            }
            if !acknowledged.insert((ack.message_id, ack.delivery_digest)) {
                return Err(WorkflowNodeError::reconciliation_required(
                    "duplicate Supervisor worker delivery ACK identity",
                ));
            }
        }

        let expected: Vec<_> = deliveries
            .into_values()
            .filter_map(|(_, _, input)| {
                (!acknowledged.contains(&(input.message_id.clone(), input.delivery_digest.clone()))).then_some(input)
            })
            .collect();
        if envelope.input_deliveries != expected {
            return Err(WorkflowNodeError::reconciliation_required(
                "Supervisor decision inputs do not exactly match unacknowledged durable deliveries",
            ));
        }
        Ok(())
    }
}

fn supervisor_history_record_is_relevant(
    record: &LedgerRecord,
    runtime: &SupervisorRuntime<'_>,
    message_prefix: &str,
) -> bool {
    record
        .payload
        .get("message_id")
        .and_then(Value::as_str)
        .is_some_and(|message_id| message_id.starts_with(message_prefix))
        || (record.payload.get("workflow_run_id").and_then(Value::as_str) == Some(runtime.context.run_id.as_str())
            && record.payload.get("node_id").and_then(Value::as_str) == Some(runtime.context.node.id.as_str())
            && record.payload.get("attempt_id").and_then(Value::as_str) == Some(runtime.context.attempt_id.as_str()))
}
