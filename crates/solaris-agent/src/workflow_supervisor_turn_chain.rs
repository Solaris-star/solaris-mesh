use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use solaris_types::identity::{AttemptId, OperationId, RunId};
use solaris_types::message::TokenUsage;
use solaris_types::spawner::{AgentOutcomeStatus, AgentTurnOutcome, OutcomeBlobRef};

use crate::execution_context::stable_digest_value;
use crate::runtime_ledger::LedgerRecord;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SupervisorCoordinatorTurnBinding {
    pub(crate) ledger_run_id: RunId,
    pub(crate) conversation_id: String,
    pub(crate) turn_id: String,
    pub(crate) operation_id: OperationId,
    pub(crate) message_id: String,
    pub(crate) outcome_record_seq: u64,
    pub(crate) outcome_payload_digest: String,
    pub(crate) status: AgentOutcomeStatus,
    pub(crate) turns: usize,
    pub(crate) usage: TokenUsage,
    pub(crate) outcome_ref: Option<OutcomeBlobRef>,
}

pub(crate) struct ValidatedSupervisorCoordinatorTurn {
    pub(crate) binding: SupervisorCoordinatorTurnBinding,
    pub(crate) outcome: AgentTurnOutcome,
}

pub(crate) fn supervisor_conversation_identity(
    workflow_run_id: &RunId,
    node_id: &str,
    attempt_id: &AttemptId,
) -> String {
    format!("workflow-supervisor:{workflow_run_id}:{node_id}:{attempt_id}")
}

pub(crate) fn supervisor_turn_identity(conversation_id: &str, round: u32) -> String {
    format!("{conversation_id}:round:{round}")
}

pub(crate) fn validate_supervisor_coordinator_turn_chain(
    records: &[LedgerRecord],
    ledger_run_id: &RunId,
    conversation_id: &str,
    turn_id: &str,
    decision_record_seq: u64,
) -> Result<ValidatedSupervisorCoordinatorTurn, String> {
    let outcomes: Vec<_> = records
        .iter()
        .filter(|record| {
            record.record_type == "agent_conversation_turn_outcome"
                && record.payload.get("conversation_id").and_then(Value::as_str) == Some(conversation_id)
                && record.payload.get("turn_id").and_then(Value::as_str) == Some(turn_id)
        })
        .collect();
    let intents: Vec<_> = records
        .iter()
        .filter(|record| {
            record.record_type == "agent_conversation_turn_intent"
                && record.payload.get("conversation_id").and_then(Value::as_str) == Some(conversation_id)
                && record.payload.get("turn_id").and_then(Value::as_str) == Some(turn_id)
        })
        .collect();
    let opens: Vec<_> = records
        .iter()
        .filter(|record| {
            record.record_type == "agent_conversation_opened"
                && record.payload.get("conversation_id").and_then(Value::as_str) == Some(conversation_id)
        })
        .collect();
    if outcomes.len() != 1 || intents.len() != 1 || opens.len() != 1 {
        return Err("Supervisor decision has no unique durable coordinator turn chain".to_owned());
    }
    let outcome_record = outcomes[0];
    let outcome: AgentTurnOutcome = serde_json::from_value(outcome_record.payload.clone())
        .map_err(|error| format!("malformed Supervisor coordinator turn outcome: {error}"))?;
    let intent = intents[0];
    let open = opens[0];
    let invalid = open.seq >= intent.seq
        || intent.seq >= outcome_record.seq
        || outcome_record.seq >= decision_record_seq
        || open.run_id != *ledger_run_id
        || intent.run_id != *ledger_run_id
        || outcome_record.run_id != *ledger_run_id
        || outcome.run_id != *ledger_run_id
        || outcome.conversation_id != conversation_id
        || outcome.turn_id != turn_id
        || outcome.status != AgentOutcomeStatus::Completed
        || open.payload.get("run_id") != Some(&json!(outcome.run_id))
        || open.payload.get("agent_id") != Some(&json!(outcome.agent_id))
        || open.payload.get("session_id") != Some(&json!(outcome.session_id))
        || open.payload.get("task_id") != Some(&json!(outcome.task_id))
        || open.payload.get("operation_id") != Some(&json!(outcome.open_operation_id))
        || open.payload.get("spec_digest") != Some(&json!(outcome.spec_digest))
        || intent.payload.get("run_id") != Some(&json!(outcome.run_id))
        || intent.payload.get("agent_id") != Some(&json!(outcome.agent_id))
        || intent.payload.get("session_id") != Some(&json!(outcome.session_id))
        || intent.payload.get("task_id") != Some(&json!(outcome.task_id))
        || intent.payload.get("open_operation_id") != Some(&json!(outcome.open_operation_id))
        || intent.payload.get("spec_digest") != Some(&json!(outcome.spec_digest))
        || intent.payload.pointer("/identity/operation_id") != Some(&json!(outcome.operation_id))
        || intent.payload.pointer("/identity/message_id") != Some(&json!(outcome.message_id));
    if invalid {
        return Err(
            "Supervisor coordinator turn outcome does not match its durable intent and conversation".to_owned(),
        );
    }
    Ok(ValidatedSupervisorCoordinatorTurn {
        binding: SupervisorCoordinatorTurnBinding {
            ledger_run_id: outcome_record.run_id.clone(),
            conversation_id: outcome.conversation_id.clone(),
            turn_id: outcome.turn_id.clone(),
            operation_id: outcome.operation_id.clone(),
            message_id: outcome.message_id.clone(),
            outcome_record_seq: outcome_record.seq,
            outcome_payload_digest: stable_digest_value(&outcome_record.payload),
            status: outcome.status,
            turns: outcome.turns,
            usage: outcome.usage.clone(),
            outcome_ref: outcome.outcome_ref.clone(),
        },
        outcome,
    })
}
