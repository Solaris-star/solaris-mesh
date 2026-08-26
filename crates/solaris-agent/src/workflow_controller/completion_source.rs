use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use solaris_types::identity::{AttemptId, RunId};
use solaris_types::spawner::{AgentOutcomeStatus, OutcomeBlobRef};
use solaris_types::workflow::{CollaborationSelection, CollaborationStrategy, WorkflowDefinition, WorkflowNode};

use crate::execution_context::{EffectOutputStore, stable_digest_bytes, stable_digest_value};
use crate::runtime_ledger::LedgerRecord;
use crate::workflow_supervisor_turn_chain::{
    SupervisorCoordinatorTurnBinding, supervisor_conversation_identity, supervisor_turn_identity,
    validate_supervisor_coordinator_turn_chain,
};

use super::WorkflowController;

pub(super) const WORKFLOW_COMPLETION_SCHEMA_VERSION: u8 = 2;
const LEGACY_WORKFLOW_COMPLETION_SCHEMA_VERSION: u8 = 1;
const SUPERVISOR_DECISION_SCHEMA_VERSION: u8 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct SupervisorCompletionSource {
    decision_ledger_run_id: RunId,
    workflow_run_id: RunId,
    node_id: String,
    attempt_id: AttemptId,
    round: u32,
    decision_record_seq: u64,
    decision_digest: String,
    final_output_ref: OutcomeBlobRef,
}

struct ValidatedFinalizeDecision {
    schema_version: u8,
    source: SupervisorCompletionSource,
}

impl WorkflowController {
    pub(super) fn build_supervisor_completion_source(
        &self,
        node: &WorkflowNode,
        workflow_run_id: &RunId,
        attempt_id: &AttemptId,
        output_ref: &str,
        output_bytes: u64,
        output_digest: &str,
    ) -> Result<Option<SupervisorCompletionSource>, String> {
        if !is_configured_supervisor(node) {
            return Ok(None);
        }
        let decisions = self.validated_finalize_decisions(workflow_run_id, &node.id, attempt_id)?;
        if decisions.len() != 1 || decisions[0].schema_version != SUPERVISOR_DECISION_SCHEMA_VERSION {
            return Err(format!(
                "Supervisor completion has no unique v2 Finalize decision for node {}",
                node.id
            ));
        }
        let source = &decisions[0].source;
        if source.final_output_ref.reference != output_ref
            || source.final_output_ref.bytes != output_bytes
            || source.final_output_ref.digest != output_digest
            || source.final_output_ref.run_id.as_ref() != Some(workflow_run_id)
            || source.final_output_ref.status != Some(AgentOutcomeStatus::Completed)
        {
            return Err(format!(
                "Supervisor completion output does not match its Finalize decision for node {}",
                node.id
            ));
        }
        Ok(Some(source.clone()))
    }

    pub(super) fn validate_completion_source(
        &self,
        definition: &WorkflowDefinition,
        workflow_run_id: &RunId,
        completion_record: &LedgerRecord,
    ) -> Result<(), String> {
        let node_id = completion_record
            .payload
            .get("node_id")
            .and_then(Value::as_str)
            .ok_or_else(|| "Workflow completion is missing its node identity".to_owned())?;
        let node = definition
            .nodes
            .iter()
            .find(|node| node.id == node_id)
            .ok_or_else(|| format!("Workflow completion refers to unknown node {node_id}"))?;
        let schema_version = completion_record
            .payload
            .get("completion_schema_version")
            .and_then(Value::as_u64)
            .map(|value| u8::try_from(value).map_err(|_| "invalid Workflow completion schema version".to_owned()))
            .transpose()?
            .unwrap_or(LEGACY_WORKFLOW_COMPLETION_SCHEMA_VERSION);
        if !is_configured_supervisor(node) {
            if schema_version == WORKFLOW_COMPLETION_SCHEMA_VERSION
                && completion_record
                    .payload
                    .get("source_supervisor_decision")
                    .is_some_and(|value| !value.is_null())
            {
                return Err(format!(
                    "non-Supervisor Workflow completion for node {node_id} has a Supervisor source"
                ));
            }
            return match schema_version {
                LEGACY_WORKFLOW_COMPLETION_SCHEMA_VERSION | WORKFLOW_COMPLETION_SCHEMA_VERSION => Ok(()),
                _ => Err(format!(
                    "unsupported Workflow completion schema version {schema_version}"
                )),
            };
        }
        let attempt_id = completion_record
            .payload
            .get("attempt_id")
            .and_then(Value::as_str)
            .map(AttemptId::from)
            .ok_or_else(|| format!("Supervisor completion for node {node_id} is missing its attempt identity"))?;
        let decisions = self.validated_finalize_decisions(workflow_run_id, node_id, &attempt_id)?;
        if decisions.len() != 1 {
            return Err(format!(
                "Supervisor completion for node {node_id} has no unique Finalize decision"
            ));
        }
        let decision = &decisions[0];
        match schema_version {
            LEGACY_WORKFLOW_COMPLETION_SCHEMA_VERSION => {
                if decision.schema_version != LEGACY_WORKFLOW_COMPLETION_SCHEMA_VERSION
                    || completion_record.payload.get("source_supervisor_decision").is_some()
                {
                    return Err(format!(
                        "legacy Supervisor completion for node {node_id} cannot omit a v2 decision source"
                    ));
                }
            }
            WORKFLOW_COMPLETION_SCHEMA_VERSION => {
                if decision.schema_version != SUPERVISOR_DECISION_SCHEMA_VERSION {
                    return Err(format!(
                        "Supervisor completion for node {node_id} does not refer to a v2 Finalize decision"
                    ));
                }
                let source: SupervisorCompletionSource = completion_record
                    .payload
                    .get("source_supervisor_decision")
                    .cloned()
                    .ok_or_else(|| format!("Supervisor completion for node {node_id} is missing its decision source"))
                    .and_then(|value| {
                        serde_json::from_value(value).map_err(|error| {
                            format!("invalid Supervisor completion decision source for node {node_id}: {error}")
                        })
                    })?;
                if source != decision.source || source.decision_record_seq >= completion_record.seq {
                    return Err(format!(
                        "Supervisor completion for node {node_id} does not match its earlier Finalize decision"
                    ));
                }
                self.validate_completion_output_identity(completion_record, &source)?;
            }
            _ => {
                return Err(format!(
                    "unsupported Workflow completion schema version {schema_version}"
                ));
            }
        }
        Ok(())
    }

    fn validated_finalize_decisions(
        &self,
        workflow_run_id: &RunId,
        node_id: &str,
        attempt_id: &AttemptId,
    ) -> Result<Vec<ValidatedFinalizeDecision>, String> {
        let mut run_ids = self.ledger.run_ids().map_err(|error| error.to_string())?;
        run_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        let mut decisions = Vec::new();
        for ledger_run_id in run_ids {
            let records = self
                .ledger
                .records_for_run(&ledger_run_id)
                .map_err(|error| error.to_string())?;
            for record in records {
                if record.record_type != "workflow_supervisor_decision"
                    || record.payload.get("workflow_run_id").and_then(Value::as_str) != Some(workflow_run_id.as_str())
                    || record.payload.get("node_id").and_then(Value::as_str) != Some(node_id)
                    || record.payload.get("attempt_id").and_then(Value::as_str) != Some(attempt_id.as_str())
                    || record.payload.pointer("/decision/decision").and_then(Value::as_str) != Some("finalize")
                {
                    continue;
                }
                decisions.push(self.validate_finalize_decision(&ledger_run_id, workflow_run_id, record)?);
            }
        }
        Ok(decisions)
    }

    fn validate_finalize_decision(
        &self,
        ledger_run_id: &RunId,
        workflow_run_id: &RunId,
        record: LedgerRecord,
    ) -> Result<ValidatedFinalizeDecision, String> {
        if record.run_id != *ledger_run_id {
            return Err("Supervisor Finalize decision has a forged ledger Run identity".to_owned());
        }
        let schema_version = record
            .payload
            .get("schema_version")
            .and_then(Value::as_u64)
            .and_then(|value| u8::try_from(value).ok())
            .ok_or_else(|| "Supervisor Finalize decision is missing its schema version".to_owned())?;
        if !matches!(
            schema_version,
            LEGACY_WORKFLOW_COMPLETION_SCHEMA_VERSION | SUPERVISOR_DECISION_SCHEMA_VERSION
        ) {
            return Err(format!(
                "unsupported Supervisor decision schema version {schema_version}"
            ));
        }
        if (schema_version == SUPERVISOR_DECISION_SCHEMA_VERSION)
            != record.payload.get("coordinator_turn").is_some_and(Value::is_object)
        {
            return Err("Supervisor Finalize decision schema does not match its coordinator binding".to_owned());
        }
        let final_output_ref: OutcomeBlobRef = serde_json::from_value(record.payload["final_output_ref"].clone())
            .map_err(|error| format!("invalid Supervisor Finalize output reference: {error}"))?;
        if final_output_ref.run_id.as_ref() != Some(workflow_run_id)
            || final_output_ref.status != Some(AgentOutcomeStatus::Completed)
        {
            return Err("Supervisor Finalize output reference has an invalid Run or status".to_owned());
        }
        let serialized = EffectOutputStore::for_run_with_ledger(workflow_run_id, self.ledger.as_ref())
            .read(&final_output_ref.reference)
            .map_err(|error| format!("failed to read Supervisor Finalize output: {error}"))?;
        if u64::try_from(serialized.len()).ok() != Some(final_output_ref.bytes)
            || stable_digest_bytes(serialized.as_bytes()) != final_output_ref.digest
        {
            return Err("Supervisor Finalize output reference failed integrity validation".to_owned());
        }
        let output: Value = serde_json::from_str(&serialized)
            .map_err(|error| format!("invalid Supervisor Finalize output JSON: {error}"))?;
        let mut decision = record.payload["decision"].clone();
        if decision.get("output").is_none_or(|value| !value.is_null()) {
            return Err("Supervisor Finalize output is not uniquely stored by reference".to_owned());
        }
        decision["output"] = output;
        let mut digest_payload = json!({
            "schema_version": record.payload["schema_version"],
            "workflow_run_id": record.payload["workflow_run_id"],
            "workflow_id": record.payload["workflow_id"],
            "node_id": record.payload["node_id"],
            "attempt_id": record.payload["attempt_id"],
            "round": record.payload["round"],
            "input_message_ids": record.payload["input_message_ids"],
            "input_deliveries": record.payload["input_deliveries"],
            "turns": record.payload["turns"],
            "usage": record.payload["usage"],
            "final_output_ref": record.payload["final_output_ref"],
            "decision": decision,
        });
        if schema_version == SUPERVISOR_DECISION_SCHEMA_VERSION {
            digest_payload["coordinator_turn"] = record.payload["coordinator_turn"].clone();
        }
        let decision_digest = record
            .payload
            .get("decision_digest")
            .and_then(Value::as_str)
            .ok_or_else(|| "Supervisor Finalize decision has no digest".to_owned())?;
        if stable_digest_value(&digest_payload) != decision_digest {
            return Err("Supervisor Finalize decision digest failed validation".to_owned());
        }
        let round = record
            .payload
            .get("round")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or_else(|| "Supervisor Finalize decision has an invalid round".to_owned())?;
        let node_id = record.payload["node_id"]
            .as_str()
            .ok_or_else(|| "Supervisor Finalize decision has no node identity".to_owned())?;
        let attempt_id = record.payload["attempt_id"]
            .as_str()
            .map(AttemptId::from)
            .ok_or_else(|| "Supervisor Finalize decision has no attempt identity".to_owned())?;
        if schema_version == SUPERVISOR_DECISION_SCHEMA_VERSION {
            let binding: SupervisorCoordinatorTurnBinding = serde_json::from_value(
                record
                    .payload
                    .get("coordinator_turn")
                    .cloned()
                    .ok_or_else(|| "Supervisor Finalize decision has no coordinator turn binding".to_owned())?,
            )
            .map_err(|error| format!("invalid Supervisor Finalize coordinator turn binding: {error}"))?;
            let records = self
                .ledger
                .records_for_run(ledger_run_id)
                .map_err(|error| error.to_string())?;
            let conversation_id = supervisor_conversation_identity(workflow_run_id, node_id, &attempt_id);
            let turn_id = supervisor_turn_identity(&conversation_id, round);
            let validated = validate_supervisor_coordinator_turn_chain(
                &records,
                ledger_run_id,
                &conversation_id,
                &turn_id,
                record.seq,
            )?;
            let binding_value = serde_json::to_value(binding)
                .map_err(|error| format!("invalid Supervisor Finalize coordinator binding: {error}"))?;
            let durable_value = serde_json::to_value(validated.binding)
                .map_err(|error| format!("invalid durable Supervisor coordinator binding: {error}"))?;
            if binding_value != durable_value {
                return Err("Supervisor Finalize coordinator turn binding does not match its durable chain".to_owned());
            }
        }
        Ok(ValidatedFinalizeDecision {
            schema_version,
            source: SupervisorCompletionSource {
                decision_ledger_run_id: ledger_run_id.clone(),
                workflow_run_id: workflow_run_id.clone(),
                node_id: node_id.to_owned(),
                attempt_id,
                round,
                decision_record_seq: record.seq,
                decision_digest: decision_digest.to_owned(),
                final_output_ref,
            },
        })
    }

    fn validate_completion_output_identity(
        &self,
        completion_record: &LedgerRecord,
        source: &SupervisorCompletionSource,
    ) -> Result<(), String> {
        let output_bytes = completion_record.payload.get("output_bytes").and_then(Value::as_u64);
        let output_digest = completion_record.payload.get("output_digest").and_then(Value::as_str);
        let output_ref = completion_record.payload.get("output_ref").and_then(Value::as_str);
        let output_status = completion_record.payload.get("output_status").and_then(Value::as_str);
        if output_bytes != Some(source.final_output_ref.bytes)
            || output_digest != Some(source.final_output_ref.digest.as_str())
            || output_ref != Some(source.final_output_ref.reference.as_str())
            || output_status != Some("completed")
        {
            return Err(format!(
                "Supervisor completion for node {} has an output identity different from its Finalize decision",
                source.node_id
            ));
        }
        Ok(())
    }
}

fn is_configured_supervisor(node: &WorkflowNode) -> bool {
    matches!(
        &node.collaboration,
        CollaborationSelection::Configured(config) if config.strategy == CollaborationStrategy::Supervisor
    )
}

#[cfg(test)]
#[path = "completion_source_test.rs"]
mod completion_source_test;
