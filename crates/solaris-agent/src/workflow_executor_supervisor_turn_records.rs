use solaris_types::spawner::{AgentOutcomeStatus, AgentTurnOutcome, AgentTurnOutputProjection};

use crate::runtime_ledger::LedgerRecord;
use crate::workflow_supervisor_turn_chain::{
    SupervisorCoordinatorTurnBinding, validate_supervisor_coordinator_turn_chain,
};

use super::super::super::parse_structured_result;
use super::super::{supervisor_conversation_id, supervisor_turn_id};
use super::*;

impl AgentWorkflowExecutor {
    pub(super) fn build_supervisor_coordinator_turn_binding(
        &self,
        context: &WorkflowExecutionContext,
        round: u32,
        decision: &SupervisorDecision,
        accounting: &SupervisorDecisionAccounting,
    ) -> Result<SupervisorCoordinatorTurnBinding, WorkflowNodeError> {
        let records = self.supervisor_root_records()?;
        let conversation_id = supervisor_conversation_id(context);
        let turn_id = supervisor_turn_id(context, round);
        let validated = validate_supervisor_coordinator_turn_chain(
            &records,
            self.spawner.run_id(),
            &conversation_id,
            &turn_id,
            u64::MAX,
        )
        .map_err(WorkflowNodeError::reconciliation_required)?;
        self.validate_supervisor_turn_result(context, round, decision, accounting, &validated.outcome)?;
        Ok(validated.binding)
    }

    pub(super) fn validate_supervisor_coordinator_turn_binding(
        &self,
        context: &WorkflowExecutionContext,
        envelope: &SupervisorDecisionEnvelope,
        decision_record: &LedgerRecord,
        records: &[LedgerRecord],
    ) -> Result<(), WorkflowNodeError> {
        let binding = envelope.coordinator_turn.as_ref().ok_or_else(|| {
            WorkflowNodeError::reconciliation_required("Supervisor decision v2 is missing its coordinator turn binding")
        })?;
        let conversation_id = supervisor_conversation_id(context);
        let turn_id = supervisor_turn_id(context, envelope.round);
        let validated = validate_supervisor_coordinator_turn_chain(
            records,
            self.spawner.run_id(),
            &conversation_id,
            &turn_id,
            decision_record.seq,
        )
        .map_err(WorkflowNodeError::reconciliation_required)?;
        let accounting = SupervisorDecisionAccounting {
            turns: envelope.turns,
            usage: envelope.usage.clone(),
            turn_outcome_ref: envelope.final_output_ref.clone(),
        };
        self.validate_supervisor_turn_result(
            context,
            envelope.round,
            &envelope.decision,
            &accounting,
            &validated.outcome,
        )?;
        let binding_value = serde_json::to_value(binding).map_err(|error| {
            WorkflowNodeError::reconciliation_required(format!("invalid Supervisor coordinator turn binding: {error}"))
        })?;
        let expected_value = serde_json::to_value(&validated.binding).map_err(|error| {
            WorkflowNodeError::reconciliation_required(format!(
                "invalid durable Supervisor coordinator turn binding: {error}"
            ))
        })?;
        if binding_value != expected_value {
            return Err(WorkflowNodeError::reconciliation_required(
                "Supervisor decision coordinator turn binding is not unique and earlier",
            ));
        }
        Ok(())
    }

    fn supervisor_root_records(&self) -> Result<Vec<LedgerRecord>, WorkflowNodeError> {
        self.spawner
            .lifecycle_runtime()
            .ledger()
            .records_for_run(self.spawner.run_id())
            .map_err(|error| WorkflowNodeError::reconciliation_required(error.to_string()))
    }

    fn validate_supervisor_turn_result(
        &self,
        context: &WorkflowExecutionContext,
        round: u32,
        decision: &SupervisorDecision,
        accounting: &SupervisorDecisionAccounting,
        outcome: &AgentTurnOutcome,
    ) -> Result<(), WorkflowNodeError> {
        let parsed = self.parse_supervisor_turn_decision(context, outcome)?;
        let usage = self.supervisor_coordinator_usage_delta(context, round, outcome.usage.clone())?;
        if &parsed != decision
            || outcome.turns != accounting.turns
            || stable_digest_value(&json!(usage)) != stable_digest_value(&json!(accounting.usage))
            || outcome.outcome_ref != accounting.turn_outcome_ref
        {
            return Err(WorkflowNodeError::reconciliation_required(
                "Supervisor decision does not match its coordinator turn outcome",
            ));
        }
        match (decision, outcome.outcome_ref.as_ref()) {
            (SupervisorDecision::Finalize { .. }, Some(reference))
                if reference.status == Some(AgentOutcomeStatus::Completed) => {}
            (SupervisorDecision::Finalize { .. }, _) => {
                return Err(WorkflowNodeError::reconciliation_required(
                    "Supervisor final decision has no completed coordinator outcome reference",
                ));
            }
            (_, None) => {}
            (_, Some(_)) => {
                return Err(WorkflowNodeError::reconciliation_required(
                    "non-final Supervisor decision has a coordinator output reference",
                ));
            }
        }
        Ok(())
    }

    fn parse_supervisor_turn_decision(
        &self,
        context: &WorkflowExecutionContext,
        outcome: &AgentTurnOutcome,
    ) -> Result<SupervisorDecision, WorkflowNodeError> {
        let text = outcome.output.get("text").and_then(Value::as_str).ok_or_else(|| {
            WorkflowNodeError::reconciliation_required("Supervisor coordinator turn has no output.text")
        })?;
        let mut value = parse_structured_result(text).map_err(WorkflowNodeError::reconciliation_required)?;
        if let (Some(reference), Some(AgentTurnOutputProjection::JsonTextPointer { pointer })) =
            (&outcome.outcome_ref, &outcome.output_projection)
        {
            let serialized = self.read_supervisor_final_output(&context.run_id, reference)?;
            let body: Value = serde_json::from_str(&serialized).map_err(|error| {
                WorkflowNodeError::reconciliation_required(format!(
                    "invalid Supervisor coordinator output blob: {error}"
                ))
            })?;
            let slot = value.pointer_mut(pointer).ok_or_else(|| {
                WorkflowNodeError::reconciliation_required("Supervisor coordinator output projection slot is missing")
            })?;
            if !slot.is_null() {
                return Err(WorkflowNodeError::reconciliation_required(
                    "Supervisor coordinator output is duplicated inline and by reference",
                ));
            }
            *slot = body;
        }
        serde_json::from_value(value).map_err(|error| {
            WorkflowNodeError::reconciliation_required(format!(
                "invalid durable Supervisor coordinator decision: {error}"
            ))
        })
    }
}
