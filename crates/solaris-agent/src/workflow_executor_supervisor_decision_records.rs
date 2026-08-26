use super::super::supervisor_decision_inputs;
use super::*;

impl AgentWorkflowExecutor {
    pub(in super::super) fn supervisor_decision_envelope(
        &self,
        context: &WorkflowExecutionContext,
        round: u32,
        deliveries: &[DurableSupervisorWorkerDelivery],
        decision: SupervisorDecision,
        accounting: SupervisorDecisionAccounting,
    ) -> Result<SupervisorDecisionEnvelope, WorkflowNodeError> {
        let input_deliveries = supervisor_decision_inputs(deliveries);
        let input_message_ids = input_deliveries.iter().map(|input| input.message_id.clone()).collect();
        let final_output_ref = match (&decision, accounting.turn_outcome_ref) {
            (SupervisorDecision::Finalize { .. }, Some(reference)) => Some(reference),
            (SupervisorDecision::Finalize { .. }, None) => {
                return Err(WorkflowNodeError::reconciliation_required(
                    "new Supervisor final decision has no projected turn output reference",
                ));
            }
            (_, None) => None,
            (_, Some(_)) => {
                return Err(WorkflowNodeError::reconciliation_required(
                    "non-final Supervisor decision unexpectedly projected an output body",
                ));
            }
        };
        let coordinator_turn = Some(self.build_supervisor_coordinator_turn_binding(
            context,
            round,
            &decision,
            &SupervisorDecisionAccounting {
                turns: accounting.turns,
                usage: accounting.usage.clone(),
                turn_outcome_ref: final_output_ref.clone(),
            },
        )?);
        let mut envelope = SupervisorDecisionEnvelope {
            schema_version: SUPERVISOR_DECISION_SCHEMA_VERSION,
            workflow_run_id: context.run_id.clone(),
            workflow_id: context.workflow.implementation_id.clone(),
            node_id: context.node.id.clone(),
            attempt_id: context.attempt_id.clone(),
            round,
            input_message_ids,
            input_deliveries,
            decision_digest: String::new(),
            turns: accounting.turns,
            usage: accounting.usage,
            final_output_ref,
            coordinator_turn,
            decision,
        };
        envelope.decision_digest = supervisor_decision_digest(&envelope)?;
        Ok(envelope)
    }

    pub(in super::super) fn load_supervisor_decision(
        &self,
        context: &WorkflowExecutionContext,
        round: u32,
    ) -> Result<Option<SupervisorDecisionEnvelope>, WorkflowNodeError> {
        let mut found = None;
        for record in self
            .spawner
            .lifecycle_runtime()
            .ledger()
            .records_for_run(self.spawner.run_id())
            .map_err(|error| WorkflowNodeError::reconciliation_required(error.to_string()))?
        {
            if record.record_type != SUPERVISOR_DECISION_RECORD {
                continue;
            }
            let mut envelope: SupervisorDecisionEnvelope = serde_json::from_value(record.payload).map_err(|error| {
                WorkflowNodeError::reconciliation_required(format!("malformed Supervisor decision: {error}"))
            })?;
            if envelope.workflow_run_id != context.run_id
                || envelope.node_id != context.node.id
                || envelope.attempt_id != context.attempt_id
                || envelope.round != round
            {
                continue;
            }
            self.hydrate_supervisor_final_output(&mut envelope)?;
            self.validate_supervisor_envelope(context, round, &envelope)?;
            if found
                .as_ref()
                .is_some_and(|existing| serde_json::to_value(existing).ok() != serde_json::to_value(&envelope).ok())
            {
                return Err(WorkflowNodeError::reconciliation_required(
                    "conflicting Supervisor decisions share one round identity",
                ));
            }
            found = Some(envelope);
        }
        Ok(found)
    }

    pub(in super::super) fn persist_supervisor_decision(
        &self,
        envelope: &SupervisorDecisionEnvelope,
    ) -> Result<(), WorkflowNodeError> {
        if envelope.schema_version != SUPERVISOR_DECISION_SCHEMA_VERSION || envelope.coordinator_turn.is_none() {
            return Err(WorkflowNodeError::reconciliation_required(
                "new Supervisor decision is missing its v2 coordinator turn binding",
            ));
        }
        self.validate_supervisor_final_output_ref(envelope)?;
        let mut payload =
            serde_json::to_value(envelope).map_err(|error| WorkflowNodeError::non_retryable(error.to_string()))?;
        if envelope.final_output_ref.is_some() {
            payload["decision"]["output"] = Value::Null;
        }
        self.persist_unique_supervisor_record(
            SUPERVISOR_DECISION_RECORD,
            &["workflow_run_id", "node_id", "attempt_id", "round"],
            payload,
        )
    }

    fn hydrate_supervisor_final_output(
        &self,
        envelope: &mut SupervisorDecisionEnvelope,
    ) -> Result<(), WorkflowNodeError> {
        let Some(reference) = envelope.final_output_ref.as_ref() else {
            return Ok(());
        };
        let SupervisorDecision::Finalize { output } = &mut envelope.decision else {
            return Err(WorkflowNodeError::reconciliation_required(
                "Supervisor final output reference belongs to a non-final decision",
            ));
        };
        if !output.is_null() {
            return Err(WorkflowNodeError::reconciliation_required(
                "Supervisor final output is duplicated inline and by reference",
            ));
        }
        let serialized = self.read_supervisor_final_output(&envelope.workflow_run_id, reference)?;
        *output = serde_json::from_str(&serialized).map_err(|error| {
            WorkflowNodeError::reconciliation_required(format!("invalid Supervisor final output blob: {error}"))
        })?;
        Ok(())
    }

    fn validate_supervisor_final_output_ref(
        &self,
        envelope: &SupervisorDecisionEnvelope,
    ) -> Result<(), WorkflowNodeError> {
        match (&envelope.decision, envelope.final_output_ref.as_ref()) {
            (SupervisorDecision::Finalize { output }, Some(reference)) => {
                let serialized = self.read_supervisor_final_output(&envelope.workflow_run_id, reference)?;
                let expected = serde_json::to_string(output).map_err(|error| {
                    WorkflowNodeError::non_retryable(format!("failed to encode final output: {error}"))
                })?;
                if serialized != expected {
                    return Err(WorkflowNodeError::reconciliation_required(
                        "Supervisor final output blob conflicts with its decision",
                    ));
                }
                Ok(())
            }
            (SupervisorDecision::Finalize { .. }, None) => Ok(()),
            (_, None) => Ok(()),
            (_, Some(_)) => Err(WorkflowNodeError::reconciliation_required(
                "Supervisor final output reference belongs to a non-final decision",
            )),
        }
    }

    pub(super) fn read_supervisor_final_output(
        &self,
        run_id: &RunId,
        reference: &OutcomeBlobRef,
    ) -> Result<String, WorkflowNodeError> {
        if reference
            .run_id
            .as_ref()
            .is_some_and(|reference_run| reference_run != run_id)
        {
            return Err(WorkflowNodeError::reconciliation_required(
                "Supervisor final output reference belongs to a different Workflow Run",
            ));
        }
        if reference
            .status
            .is_some_and(|status| status != AgentOutcomeStatus::Completed)
        {
            return Err(WorkflowNodeError::reconciliation_required(
                "Supervisor final output status conflicts with its blob reference",
            ));
        }
        let lifecycle = self.spawner.lifecycle_runtime();
        let store = EffectOutputStore::for_run_with_ledger(
            reference.run_id.as_ref().unwrap_or(run_id),
            lifecycle.ledger().as_ref(),
        );
        let serialized = store.read(&reference.reference).map_err(|error| {
            WorkflowNodeError::reconciliation_required(format!("failed to read Supervisor final output: {error}"))
        })?;
        if u64::try_from(serialized.len()).ok() != Some(reference.bytes)
            || stable_digest_bytes(serialized.as_bytes()) != reference.digest
        {
            return Err(WorkflowNodeError::reconciliation_required(
                "Supervisor final output blob identity changed",
            ));
        }
        Ok(serialized)
    }

    pub(in super::super) fn validate_supervisor_envelope(
        &self,
        context: &WorkflowExecutionContext,
        round: u32,
        envelope: &SupervisorDecisionEnvelope,
    ) -> Result<(), WorkflowNodeError> {
        let digest = supervisor_decision_digest(envelope)
            .map_err(|error| WorkflowNodeError::reconciliation_required(error.message))?;
        let mut normalized_message_ids = envelope.input_message_ids.clone();
        normalized_message_ids.sort();
        normalized_message_ids.dedup();
        let mut normalized_inputs = envelope.input_deliveries.clone();
        normalized_inputs.sort_by(|left, right| {
            (&left.message_id, &left.delivery_digest).cmp(&(&right.message_id, &right.delivery_digest))
        });
        normalized_inputs.dedup();
        let delivery_message_ids: Vec<_> = envelope
            .input_deliveries
            .iter()
            .map(|input| input.message_id.clone())
            .collect();
        let schema_is_valid = match envelope.schema_version {
            SUPERVISOR_SCHEMA_VERSION => envelope.coordinator_turn.is_none(),
            SUPERVISOR_DECISION_SCHEMA_VERSION => envelope.coordinator_turn.is_some(),
            _ => false,
        };
        if !schema_is_valid
            || envelope.workflow_run_id != context.run_id
            || envelope.workflow_id != context.workflow.implementation_id
            || envelope.node_id != context.node.id
            || envelope.attempt_id != context.attempt_id
            || envelope.round != round
            || envelope.input_message_ids != normalized_message_ids
            || envelope.input_deliveries != normalized_inputs
            || envelope.input_message_ids != delivery_message_ids
            || envelope.decision_digest != digest
        {
            return Err(WorkflowNodeError::reconciliation_required(
                "durable Supervisor decision envelope does not match its workflow attempt",
            ));
        }
        Ok(())
    }

    pub(in super::super) fn validate_supervisor_decision_delivery_chain(
        &self,
        runtime: &SupervisorRuntime<'_>,
        envelope: &SupervisorDecisionEnvelope,
    ) -> Result<(), WorkflowNodeError> {
        let records = self
            .spawner
            .lifecycle_runtime()
            .ledger()
            .records_for_run(self.spawner.run_id())
            .map_err(|error| WorkflowNodeError::reconciliation_required(error.to_string()))?;
        let decisions: Vec<_> = records
            .iter()
            .filter(|record| {
                record.record_type == SUPERVISOR_DECISION_RECORD
                    && record.payload.get("workflow_run_id").and_then(Value::as_str)
                        == Some(runtime.context.run_id.as_str())
                    && record.payload.get("node_id").and_then(Value::as_str) == Some(runtime.context.node.id.as_str())
                    && record.payload.get("attempt_id").and_then(Value::as_str)
                        == Some(runtime.context.attempt_id.as_str())
                    && record.payload.get("round").and_then(Value::as_u64) == Some(u64::from(envelope.round))
            })
            .collect();
        if decisions.len() != 1
            || decisions[0].payload.get("decision_digest").and_then(Value::as_str) != Some(&envelope.decision_digest)
        {
            return Err(WorkflowNodeError::reconciliation_required(
                "Supervisor decision has no unique durable identity",
            ));
        }
        match envelope.schema_version {
            SUPERVISOR_SCHEMA_VERSION => {}
            SUPERVISOR_DECISION_SCHEMA_VERSION => {
                self.validate_supervisor_coordinator_turn_binding(runtime.context, envelope, decisions[0], &records)?
            }
            _ => {
                return Err(WorkflowNodeError::reconciliation_required(
                    "Supervisor decision has an unsupported schema version",
                ));
            }
        }
        self.validate_supervisor_decision_pending_history(runtime, envelope, &records, decisions[0])?;
        for input in &envelope.input_deliveries {
            let deliveries: Vec<_> = records
                .iter()
                .filter(|record| {
                    record.record_type == SUPERVISOR_WORKER_DELIVERY_RECORD
                        && record.payload.get("message_id").and_then(Value::as_str) == Some(&input.message_id)
                })
                .collect();
            if deliveries.len() != 1 || deliveries[0].seq >= decisions[0].seq {
                return Err(WorkflowNodeError::reconciliation_required(
                    "Supervisor decision input was not durably delivered first",
                ));
            }
            let delivery: DurableSupervisorWorkerDelivery = serde_json::from_value(deliveries[0].payload.clone())
                .map_err(|error| {
                    WorkflowNodeError::reconciliation_required(format!("invalid worker delivery: {error}"))
                })?;
            self.validate_supervisor_delivery_record(runtime, &delivery, deliveries[0], &records)?;
            if delivery.delivery_digest != input.delivery_digest || delivery.dispatch_round >= envelope.round {
                return Err(WorkflowNodeError::reconciliation_required(
                    "Supervisor decision input does not match its durable delivery",
                ));
            }
        }
        Ok(())
    }
}

fn supervisor_decision_digest(envelope: &SupervisorDecisionEnvelope) -> Result<String, WorkflowNodeError> {
    let value = match envelope.schema_version {
        SUPERVISOR_SCHEMA_VERSION => json!({
            "schema_version": envelope.schema_version,
            "workflow_run_id": envelope.workflow_run_id,
            "workflow_id": envelope.workflow_id,
            "node_id": envelope.node_id,
            "attempt_id": envelope.attempt_id,
            "round": envelope.round,
            "input_message_ids": envelope.input_message_ids,
            "input_deliveries": envelope.input_deliveries,
            "turns": envelope.turns,
            "usage": envelope.usage,
            "final_output_ref": envelope.final_output_ref,
            "decision": envelope.decision,
        }),
        SUPERVISOR_DECISION_SCHEMA_VERSION => json!({
            "schema_version": envelope.schema_version,
            "workflow_run_id": envelope.workflow_run_id,
            "workflow_id": envelope.workflow_id,
            "node_id": envelope.node_id,
            "attempt_id": envelope.attempt_id,
            "round": envelope.round,
            "input_message_ids": envelope.input_message_ids,
            "input_deliveries": envelope.input_deliveries,
            "turns": envelope.turns,
            "usage": envelope.usage,
            "final_output_ref": envelope.final_output_ref,
            "coordinator_turn": envelope.coordinator_turn,
            "decision": envelope.decision,
        }),
        _ => {
            return Err(WorkflowNodeError::reconciliation_required(
                "Supervisor decision has an unsupported schema version",
            ));
        }
    };
    Ok(stable_digest_value(&value))
}

#[cfg(test)]
#[path = "workflow_executor_supervisor_legacy_wire_test.rs"]
mod legacy_wire_test;
