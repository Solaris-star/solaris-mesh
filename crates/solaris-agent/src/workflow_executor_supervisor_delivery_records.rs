use crate::runtime_ledger::LedgerRecord;

use super::causality::SequencedRecord;
use super::outcome::supervisor_worker_outcome_digest;
use super::*;

impl AgentWorkflowExecutor {
    pub(in super::super) fn persist_supervisor_worker_delivery(
        &self,
        runtime: &SupervisorRuntime<'_>,
        dispatch_round: u32,
        proposals: &[SupervisorTaskProposal],
    ) -> Result<(), WorkflowNodeError> {
        let message_id = supervisor_worker_message_id(runtime.context, dispatch_round);
        if self
            .load_supervisor_deliveries(runtime)?
            .into_iter()
            .any(|delivery| delivery.message_id == message_id)
        {
            return Ok(());
        }
        let mut outcomes = Vec::with_capacity(proposals.len());
        for proposal in proposals {
            let task_id = supervisor_task_id(runtime.context, &proposal.task_key);
            let operation_id = supervisor_worker_operation_id(runtime.context, &proposal.task_key);
            let Some(outcome) =
                self.load_supervisor_worker_outcome(runtime.context, proposal, &task_id, &operation_id)?
            else {
                continue;
            };
            outcomes.push(supervisor_delivery_ref(&outcome.value));
        }
        if outcomes.is_empty() {
            return Ok(());
        }
        outcomes.sort_by(|left, right| left.task_key.cmp(&right.task_key));
        let dispatch_decision_digest = self
            .load_supervisor_decision(runtime.context, dispatch_round)?
            .ok_or_else(|| WorkflowNodeError::reconciliation_required("worker delivery has no durable Dispatch"))?
            .decision_digest;
        let mut delivery = DurableSupervisorWorkerDelivery {
            schema_version: SUPERVISOR_SCHEMA_VERSION,
            workflow_run_id: runtime.context.run_id.clone(),
            workflow_id: runtime.context.workflow.implementation_id.clone(),
            node_id: runtime.context.node.id.clone(),
            attempt_id: runtime.context.attempt_id.clone(),
            dispatch_round,
            dispatch_decision_digest,
            message_id,
            sender: "solaris-runtime/supervisor-worker-results/v1".to_owned(),
            coordinator_agent_id: runtime.handle.agent_id.clone(),
            outcomes,
            delivery_digest: String::new(),
        };
        delivery.delivery_digest = supervisor_delivery_digest(&delivery);
        let encoded = serde_json::to_vec(&delivery)
            .map_err(|error| WorkflowNodeError::non_retryable(format!("failed to encode worker delivery: {error}")))?;
        let max_bytes = usize::try_from(runtime.config.max_message_bytes).unwrap_or(usize::MAX);
        if encoded.len() > max_bytes {
            return Err(WorkflowNodeError::non_retryable(format!(
                "Supervisor worker result delivery exceeds max_message_bytes ({max_bytes})"
            )));
        }
        let pending = self.pending_supervisor_messages(runtime)?;
        let max_pending = usize::try_from(runtime.config.max_pending_messages).unwrap_or(usize::MAX);
        if pending.len() >= max_pending {
            return Err(WorkflowNodeError::reconciliation_required(
                "Supervisor worker result delivery exceeds max_pending_messages",
            ));
        }
        let payload =
            serde_json::to_value(delivery).map_err(|error| WorkflowNodeError::non_retryable(error.to_string()))?;
        self.persist_unique_supervisor_record(SUPERVISOR_WORKER_DELIVERY_RECORD, &["message_id"], payload)
    }

    pub(in super::super) fn pending_supervisor_messages(
        &self,
        runtime: &SupervisorRuntime<'_>,
    ) -> Result<Vec<DurableSupervisorWorkerDelivery>, WorkflowNodeError> {
        let acknowledged = self.load_supervisor_delivery_acks(runtime)?;
        Ok(self
            .load_supervisor_deliveries(runtime)?
            .into_iter()
            .filter(|delivery| !acknowledged.contains_key(&delivery.message_id))
            .collect())
    }

    pub(in super::super) fn supervisor_delivery_prompt_value(
        &self,
        delivery: &DurableSupervisorWorkerDelivery,
    ) -> Result<Value, WorkflowNodeError> {
        let mut outcomes = Vec::with_capacity(delivery.outcomes.len());
        for reference in &delivery.outcomes {
            let outcome = self.load_delivery_outcome(delivery, reference)?;
            let body = self.read_supervisor_worker_body(&outcome.value)?;
            outcomes.push(json!({
                "task_key": outcome.value.task_key,
                "task_id": outcome.value.task_id,
                "role": outcome.value.role,
                "sender_agent_id": outcome.value.agent_id,
                "operation_id": outcome.value.operation_id,
                "outcome_digest": outcome.value.outcome_digest,
                "status": body.result.status,
                "failure_class": body.failure_class,
                "output": body.normalized_output,
                "error": body.result.is_error.then_some(body.result.text),
            }));
        }
        Ok(json!({
            "message_id": delivery.message_id,
            "sender": delivery.sender,
            "delivery_digest": delivery.delivery_digest,
            "outcomes": outcomes,
        }))
    }

    fn load_supervisor_deliveries(
        &self,
        runtime: &SupervisorRuntime<'_>,
    ) -> Result<Vec<DurableSupervisorWorkerDelivery>, WorkflowNodeError> {
        let prefix = supervisor_worker_message_prefix(runtime.context);
        let mut deliveries = Vec::new();
        let records = self
            .spawner
            .lifecycle_runtime()
            .ledger()
            .records_for_run(self.spawner.run_id())
            .map_err(|error| WorkflowNodeError::reconciliation_required(error.to_string()))?;
        for record in &records {
            if record.record_type != SUPERVISOR_WORKER_DELIVERY_RECORD
                || !record
                    .payload
                    .get("message_id")
                    .and_then(Value::as_str)
                    .is_some_and(|message_id| message_id.starts_with(&prefix))
            {
                continue;
            }
            let delivery: DurableSupervisorWorkerDelivery =
                serde_json::from_value(record.payload.clone()).map_err(|error| {
                    WorkflowNodeError::reconciliation_required(format!("invalid worker delivery: {error}"))
                })?;
            self.validate_supervisor_delivery_record(runtime, &delivery, record, &records)?;
            deliveries.push(delivery);
        }
        deliveries.sort_by(|left, right| left.message_id.cmp(&right.message_id));
        for pair in deliveries.windows(2) {
            if pair[0].message_id == pair[1].message_id {
                let left = serde_json::to_value(&pair[0]).ok();
                let right = serde_json::to_value(&pair[1]).ok();
                if left != right {
                    return Err(WorkflowNodeError::reconciliation_required(
                        "conflicting Supervisor worker deliveries share one message identity",
                    ));
                }
            }
        }
        deliveries.dedup_by(|left, right| left.message_id == right.message_id);
        Ok(deliveries)
    }

    fn validate_supervisor_delivery(
        &self,
        runtime: &SupervisorRuntime<'_>,
        delivery: &DurableSupervisorWorkerDelivery,
    ) -> Result<Vec<SequencedRecord<DurableSupervisorWorkerOutcome>>, WorkflowNodeError> {
        let mut sorted = delivery.outcomes.clone();
        sorted.sort_by(|left, right| left.task_key.cmp(&right.task_key));
        let unique = sorted.windows(2).all(|pair| pair[0].task_key != pair[1].task_key);
        if delivery.schema_version != SUPERVISOR_SCHEMA_VERSION
            || delivery.workflow_run_id != runtime.context.run_id
            || delivery.workflow_id != runtime.context.workflow.implementation_id
            || delivery.node_id != runtime.context.node.id
            || delivery.attempt_id != runtime.context.attempt_id
            || delivery.message_id != supervisor_worker_message_id(runtime.context, delivery.dispatch_round)
            || delivery.sender != "solaris-runtime/supervisor-worker-results/v1"
            || delivery.coordinator_agent_id != runtime.handle.agent_id
            || delivery.outcomes.is_empty()
            || delivery.outcomes.len() > HARD_MAX_SUPERVISOR_TASKS
            || delivery
                .outcomes
                .iter()
                .map(|item| &item.task_key)
                .ne(sorted.iter().map(|item| &item.task_key))
            || !unique
            || delivery.delivery_digest != supervisor_delivery_digest(delivery)
        {
            return Err(WorkflowNodeError::reconciliation_required(
                "durable Supervisor worker delivery identity changed",
            ));
        }
        let dispatch = self
            .load_supervisor_decision(runtime.context, delivery.dispatch_round)?
            .ok_or_else(|| WorkflowNodeError::reconciliation_required("worker delivery has no durable Dispatch"))?;
        let SupervisorDecision::Dispatch { tasks } = dispatch.decision else {
            return Err(WorkflowNodeError::reconciliation_required(
                "worker delivery is bound to a non-Dispatch decision",
            ));
        };
        if delivery.dispatch_decision_digest != dispatch.decision_digest {
            return Err(WorkflowNodeError::reconciliation_required(
                "worker delivery changed its Dispatch decision identity",
            ));
        }
        let mut sequenced_outcomes = Vec::with_capacity(delivery.outcomes.len());
        for reference in &delivery.outcomes {
            let proposal = tasks
                .iter()
                .find(|proposal| proposal.task_key == reference.task_key)
                .ok_or_else(|| {
                    WorkflowNodeError::reconciliation_required(
                        "worker delivery contains a task outside its Dispatch proposal set",
                    )
                })?;
            let task_id = supervisor_task_id(runtime.context, &proposal.task_key);
            let operation_id = supervisor_worker_operation_id(runtime.context, &proposal.task_key);
            if reference.task_key != proposal.task_key
                || reference.role != proposal.role
                || reference.task_id != task_id
                || reference.operation_id != operation_id
            {
                return Err(WorkflowNodeError::reconciliation_required(
                    "worker delivery changed its Dispatch proposal identity",
                ));
            }
            let outcome = self.load_delivery_outcome(delivery, reference)?;
            self.validate_supervisor_worker_outcome(
                runtime.context,
                proposal,
                &task_id,
                &operation_id,
                &outcome.value,
            )?;
            self.read_supervisor_worker_body(&outcome.value)?;
            sequenced_outcomes.push(outcome);
        }
        Ok(sequenced_outcomes)
    }

    pub(super) fn validate_supervisor_delivery_record(
        &self,
        runtime: &SupervisorRuntime<'_>,
        delivery: &DurableSupervisorWorkerDelivery,
        delivery_record: &LedgerRecord,
        records: &[LedgerRecord],
    ) -> Result<(), WorkflowNodeError> {
        let outcomes = self.validate_supervisor_delivery(runtime, delivery)?;
        self.validate_supervisor_delivery_causality(runtime, delivery, delivery_record, &outcomes, records)
    }

    fn load_delivery_outcome(
        &self,
        delivery: &DurableSupervisorWorkerDelivery,
        reference: &SupervisorWorkerDeliveryRef,
    ) -> Result<SequencedRecord<DurableSupervisorWorkerOutcome>, WorkflowNodeError> {
        let mut found = None;
        for record in self
            .spawner
            .lifecycle_runtime()
            .ledger()
            .records_for_run(self.spawner.run_id())
            .map_err(|error| WorkflowNodeError::reconciliation_required(error.to_string()))?
        {
            if record.record_type != SUPERVISOR_WORKER_OUTCOME_RECORD
                || record.payload.get("task_id").and_then(Value::as_str) != Some(reference.task_id.as_str())
            {
                continue;
            }
            let outcome: DurableSupervisorWorkerOutcome = serde_json::from_value(record.payload).map_err(|error| {
                WorkflowNodeError::reconciliation_required(format!("invalid worker outcome: {error}"))
            })?;
            self.validate_supervisor_worker_outcome_identity(delivery, reference, &outcome)?;
            if found.is_some() {
                return Err(WorkflowNodeError::reconciliation_required(
                    "duplicate durable Supervisor worker outcome identity",
                ));
            }
            found = Some(SequencedRecord {
                seq: record.seq,
                value: outcome,
            });
        }
        found.ok_or_else(|| WorkflowNodeError::reconciliation_required("worker delivery outcome disappeared"))
    }

    fn validate_supervisor_worker_outcome_identity(
        &self,
        delivery: &DurableSupervisorWorkerDelivery,
        reference: &SupervisorWorkerDeliveryRef,
        outcome: &DurableSupervisorWorkerOutcome,
    ) -> Result<(), WorkflowNodeError> {
        let digest = supervisor_worker_outcome_digest(outcome);
        let handle = self
            .find_supervisor_worker_handle(&outcome.task_id, &outcome.operation_id)?
            .ok_or_else(|| WorkflowNodeError::reconciliation_required("worker delivery has no Agent handle"))?;
        let mut expected_ref = supervisor_delivery_ref(outcome);
        let expected_status = outcome.result_ref_status.unwrap_or(outcome.status);
        let status_matches = reference.status.is_none_or(|status| status == expected_status);
        expected_ref.status = reference.status;
        if outcome.schema_version != SUPERVISOR_SCHEMA_VERSION
            || outcome.workflow_run_id != delivery.workflow_run_id
            || outcome.workflow_id != delivery.workflow_id
            || outcome.node_id != delivery.node_id
            || outcome.attempt_id != delivery.attempt_id
            || outcome.handle_spec_digest != handle.spec_digest
            || outcome.agent_id != handle.agent_id
            || outcome.outcome_digest != digest
            || !status_matches
            || serde_json::to_value(reference).ok() != serde_json::to_value(expected_ref).ok()
        {
            return Err(WorkflowNodeError::reconciliation_required(
                "worker delivery does not match its durable outcome",
            ));
        }
        Ok(())
    }
}

fn supervisor_delivery_ref(outcome: &DurableSupervisorWorkerOutcome) -> SupervisorWorkerDeliveryRef {
    SupervisorWorkerDeliveryRef {
        task_key: outcome.task_key.clone(),
        task_id: outcome.task_id.clone(),
        role: outcome.role.clone(),
        sender_agent_id: outcome.agent_id.clone(),
        operation_id: outcome.operation_id.clone(),
        handle_spec_digest: outcome.handle_spec_digest.clone(),
        outcome_digest: outcome.outcome_digest.clone(),
        result_ref: outcome.result_ref.clone(),
        result_bytes: outcome.result_bytes,
        result_digest: outcome.result_digest.clone(),
        status: Some(outcome.result_ref_status.unwrap_or(outcome.status)),
    }
}

fn supervisor_delivery_digest(delivery: &DurableSupervisorWorkerDelivery) -> String {
    stable_digest_value(&json!({
        "schema_version": delivery.schema_version,
        "workflow_run_id": delivery.workflow_run_id,
        "workflow_id": delivery.workflow_id,
        "node_id": delivery.node_id,
        "attempt_id": delivery.attempt_id,
        "dispatch_round": delivery.dispatch_round,
        "dispatch_decision_digest": delivery.dispatch_decision_digest,
        "message_id": delivery.message_id,
        "sender": delivery.sender,
        "coordinator_agent_id": delivery.coordinator_agent_id,
        "outcomes": delivery.outcomes,
    }))
}

pub(in super::super) fn supervisor_worker_message_prefix(context: &WorkflowExecutionContext) -> String {
    format!(
        "workflow-supervisor-worker-results:{}:{}:{}:round:",
        context.run_id, context.node.id, context.attempt_id
    )
}
