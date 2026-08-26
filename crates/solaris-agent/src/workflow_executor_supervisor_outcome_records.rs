use super::causality::SequencedRecord;
use super::*;

impl AgentWorkflowExecutor {
    pub(in super::super) fn load_supervisor_worker_outcome(
        &self,
        context: &WorkflowExecutionContext,
        proposal: &SupervisorTaskProposal,
        task_id: &TaskId,
        operation_id: &OperationId,
    ) -> Result<Option<SequencedRecord<DurableSupervisorWorkerOutcome>>, WorkflowNodeError> {
        let mut found: Option<SequencedRecord<DurableSupervisorWorkerOutcome>> = None;
        for record in self
            .spawner
            .lifecycle_runtime()
            .ledger()
            .records_for_run(self.spawner.run_id())
            .map_err(|error| WorkflowNodeError::reconciliation_required(error.to_string()))?
        {
            if record.record_type != SUPERVISOR_WORKER_OUTCOME_RECORD
                || record.payload.get("task_id").and_then(Value::as_str) != Some(task_id.as_str())
            {
                continue;
            }
            let outcome: DurableSupervisorWorkerOutcome = serde_json::from_value(record.payload).map_err(|error| {
                WorkflowNodeError::reconciliation_required(format!("invalid worker outcome: {error}"))
            })?;
            self.validate_supervisor_worker_outcome(context, proposal, task_id, operation_id, &outcome)?;
            if let Some(existing) = found.as_ref() {
                let qualifier = if serde_json::to_value(&existing.value).ok() == serde_json::to_value(&outcome).ok() {
                    "duplicate"
                } else {
                    "conflicting"
                };
                return Err(WorkflowNodeError::reconciliation_required(format!(
                    "{qualifier} durable Supervisor worker outcomes at sequences {} and {}",
                    existing.seq, record.seq
                )));
            }
            found = Some(SequencedRecord {
                seq: record.seq,
                value: outcome,
            });
        }
        Ok(found)
    }

    pub(in super::super) fn validate_supervisor_worker_outcome(
        &self,
        context: &WorkflowExecutionContext,
        proposal: &SupervisorTaskProposal,
        task_id: &TaskId,
        operation_id: &OperationId,
        outcome: &DurableSupervisorWorkerOutcome,
    ) -> Result<(), WorkflowNodeError> {
        let proposal_digest = serde_json::to_value(proposal)
            .map(|value| stable_digest_value(&value))
            .map_err(|error| WorkflowNodeError::reconciliation_required(error.to_string()))?;
        let handle = self
            .find_supervisor_worker_handle(task_id, operation_id)?
            .ok_or_else(|| WorkflowNodeError::reconciliation_required("durable worker outcome has no Agent handle"))?;
        let actual_spec_digest = serde_json::to_value(&handle.spec)
            .map(|value| stable_digest_value(&value))
            .map_err(|error| WorkflowNodeError::reconciliation_required(error.to_string()))?;
        let digest = supervisor_worker_outcome_digest(outcome);
        if outcome.schema_version != SUPERVISOR_SCHEMA_VERSION
            || outcome.workflow_run_id != context.run_id
            || outcome.workflow_id != context.workflow.implementation_id
            || outcome.node_id != context.node.id
            || outcome.attempt_id != context.attempt_id
            || outcome.task_key != proposal.task_key
            || outcome.task_id != *task_id
            || outcome.role != proposal.role
            || outcome.proposal_digest != proposal_digest
            || outcome.operation_id != *operation_id
            || outcome.agent_id != handle.agent_id
            || outcome.handle_spec_digest != handle.spec_digest
            || outcome.handle_spec_digest != actual_spec_digest
            || outcome.outcome_digest != digest
        {
            return Err(WorkflowNodeError::reconciliation_required(
                "durable Supervisor worker outcome identity changed",
            ));
        }
        Ok(())
    }

    pub(in super::super) fn persist_supervisor_worker_outcome(
        &self,
        outcome: &DurableSupervisorWorkerOutcome,
    ) -> Result<(), WorkflowNodeError> {
        let payload =
            serde_json::to_value(outcome).map_err(|error| WorkflowNodeError::non_retryable(error.to_string()))?;
        self.persist_unique_supervisor_record(SUPERVISOR_WORKER_OUTCOME_RECORD, &["task_id"], payload)
    }

    pub(in super::super) fn store_supervisor_worker_body(
        &self,
        operation_id: &OperationId,
        body: &SupervisorWorkerOutcomeBody,
    ) -> Result<(String, u64, String), WorkflowNodeError> {
        let serialized = serde_json::to_string(body)
            .map_err(|error| WorkflowNodeError::non_retryable(format!("failed to encode worker outcome: {error}")))?;
        let bytes = u64::try_from(serialized.len())
            .map_err(|_| WorkflowNodeError::non_retryable("worker outcome is too large"))?;
        let digest = stable_digest_bytes(serialized.as_bytes());
        let lifecycle = self.spawner.lifecycle_runtime();
        let store = EffectOutputStore::for_run_with_ledger(self.spawner.run_id(), lifecycle.ledger().as_ref());
        let reference = store.write_named(operation_id.as_str(), &serialized).map_err(|error| {
            WorkflowNodeError::reconciliation_required(format!("failed to store worker outcome: {error}"))
        })?;
        Ok((reference, bytes, digest))
    }

    pub(in super::super) fn read_supervisor_worker_body(
        &self,
        outcome: &DurableSupervisorWorkerOutcome,
    ) -> Result<SupervisorWorkerOutcomeBody, WorkflowNodeError> {
        let lifecycle = self.spawner.lifecycle_runtime();
        let store = EffectOutputStore::for_run_with_ledger(self.spawner.run_id(), lifecycle.ledger().as_ref());
        let serialized = store.read(&outcome.result_ref).map_err(|error| {
            WorkflowNodeError::reconciliation_required(format!("failed to read worker outcome: {error}"))
        })?;
        if u64::try_from(serialized.len()).ok() != Some(outcome.result_bytes)
            || stable_digest_bytes(serialized.as_bytes()) != outcome.result_digest
        {
            return Err(WorkflowNodeError::reconciliation_required(
                "durable Supervisor worker outcome blob identity changed",
            ));
        }
        let body = match outcome.result_format {
            SupervisorWorkerResultFormat::AgentOutcome => {
                let (result, agent_ref) = lifecycle
                    .agent_outcome_by_identity(self.spawner.run_id(), &outcome.operation_id, &outcome.agent_id)
                    .map_err(|error| {
                        WorkflowNodeError::reconciliation_required(format!("invalid Agent outcome blob: {error}"))
                    })?
                    .ok_or_else(|| {
                        WorkflowNodeError::reconciliation_required("Supervisor outcome has no Agent outcome record")
                    })?;
                let Some(agent_ref) = agent_ref else {
                    return Err(WorkflowNodeError::reconciliation_required(
                        "Supervisor outcome does not reuse its durable Agent outcome reference",
                    ));
                };
                if agent_ref.reference != outcome.result_ref
                    || agent_ref.bytes != outcome.result_bytes
                    || agent_ref.digest != outcome.result_digest
                    || outcome
                        .result_ref_status
                        .is_some_and(|status| agent_ref.status != Some(status))
                {
                    return Err(WorkflowNodeError::reconciliation_required(
                        "Supervisor outcome does not reuse its durable Agent outcome reference",
                    ));
                }
                self.supervisor_worker_body(&outcome.role, result)
            }
            SupervisorWorkerResultFormat::LegacySupervisorBody => {
                serde_json::from_str(&serialized).map_err(|error| {
                    WorkflowNodeError::reconciliation_required(format!("invalid worker outcome blob: {error}"))
                })?
            }
        };
        if body.result.status != outcome.status || body.failure_class != outcome.failure_class {
            return Err(WorkflowNodeError::reconciliation_required(
                "durable Supervisor worker outcome blob conflicts with its record",
            ));
        }
        Ok(body)
    }
}

pub(in super::super) fn supervisor_worker_outcome_digest(outcome: &DurableSupervisorWorkerOutcome) -> String {
    let mut digest = json!({
        "task_key": outcome.task_key,
        "task_id": outcome.task_id,
        "role": outcome.role,
        "proposal_digest": outcome.proposal_digest,
        "agent_id": outcome.agent_id,
        "operation_id": outcome.operation_id,
        "handle_spec_digest": outcome.handle_spec_digest,
        "result_ref": outcome.result_ref,
        "result_bytes": outcome.result_bytes,
        "result_digest": outcome.result_digest,
        "result_format": outcome.result_format,
        "status": outcome.status,
        "failure_class": outcome.failure_class,
    });
    if let Some(status) = outcome.result_ref_status {
        digest["result_ref_status"] = json!(status);
    }
    stable_digest_value(&digest)
}
