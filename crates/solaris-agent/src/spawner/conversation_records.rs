use std::collections::HashSet;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::Value;

use solaris_types::effect::DurabilityClass;
use solaris_types::identity::RunId;
use solaris_types::runtime::{AgentLifecycleState, TaskFailureClass};
use solaris_types::spawner::{
    AgentConversationError, AgentConversationHandle, AgentOutcomeStatus, AgentTurnIdentity, AgentTurnOutcome,
    AgentTurnSpec,
};

use crate::runtime_ledger::{LedgerRecord, RuntimeLedger};

use super::AgentConversationService;
use super::{
    AppendDisposition, CLOSE_RECORD, DurableClose, DurableTurnIntent, OPEN_RECORD, TURN_INTENT_RECORD,
    TURN_OUTCOME_RECORD,
};

impl AgentConversationService {
    fn ledger(&self) -> Arc<dyn RuntimeLedger> {
        self.spawner.lifecycle_runtime.ledger()
    }

    pub(super) fn find_open(
        &self,
        run_id: &RunId,
        conversation_id: &str,
    ) -> Result<Option<AgentConversationHandle>, AgentConversationError> {
        find_typed_record(self.ledger().as_ref(), run_id, OPEN_RECORD, |payload| {
            payload.get("conversation_id").and_then(Value::as_str) == Some(conversation_id)
        })
    }

    pub(super) fn record_open(&self, handle: &AgentConversationHandle) -> Result<(), AgentConversationError> {
        let payload =
            serde_json::to_value(handle).map_err(|error| AgentConversationError::non_retryable(error.to_string()))?;
        self.append_exact(&handle.run_id, OPEN_RECORD, payload, |record| {
            record.payload.get("conversation_id").and_then(Value::as_str) == Some(&handle.conversation_id)
        })
        .map(|_| ())
    }

    pub(super) fn find_turn_intent(
        &self,
        handle: &AgentConversationHandle,
        turn_id: &str,
    ) -> Result<Option<DurableTurnIntent>, AgentConversationError> {
        let intent: Option<DurableTurnIntent> =
            find_typed_record(self.ledger().as_ref(), &handle.run_id, TURN_INTENT_RECORD, |payload| {
                payload.get("conversation_id").and_then(Value::as_str) == Some(&handle.conversation_id)
                    && payload.get("turn_id").and_then(Value::as_str) == Some(turn_id)
            })?;
        if let Some(intent) = intent.as_ref()
            && (intent.schema_version != super::CONVERSATION_SCHEMA_VERSION
                || intent.run_id != handle.run_id
                || intent.parent_agent_id != handle.parent_agent_id
                || intent.conversation_id != handle.conversation_id
                || intent.task_id != handle.task_id
                || intent.open_operation_id != handle.operation_id
                || intent.spec_digest != handle.spec_digest
                || intent.turn_id != turn_id
                || intent.agent_id != handle.agent_id
                || intent.session_id != handle.session_id)
        {
            return Err(AgentConversationError::reconciliation_required(
                "durable turn intent does not match its Agent conversation handle",
            ));
        }
        Ok(intent)
    }

    pub(super) fn record_turn_intent(
        &self,
        handle: &AgentConversationHandle,
        intent: &DurableTurnIntent,
    ) -> Result<AppendDisposition, AgentConversationError> {
        let payload =
            serde_json::to_value(intent).map_err(|error| AgentConversationError::non_retryable(error.to_string()))?;
        self.append_exact(&handle.run_id, TURN_INTENT_RECORD, payload, |record| {
            record.payload.get("conversation_id").and_then(Value::as_str) == Some(&handle.conversation_id)
                && record.payload.get("turn_id").and_then(Value::as_str) == Some(&intent.turn_id)
        })
    }

    pub(super) fn find_turn_outcome(
        &self,
        handle: &AgentConversationHandle,
        turn_id: &str,
    ) -> Result<Option<AgentTurnOutcome>, AgentConversationError> {
        let outcome: Option<AgentTurnOutcome> =
            find_typed_record(self.ledger().as_ref(), &handle.run_id, TURN_OUTCOME_RECORD, |payload| {
                payload.get("conversation_id").and_then(Value::as_str) == Some(&handle.conversation_id)
                    && payload.get("turn_id").and_then(Value::as_str) == Some(turn_id)
            })?;
        if let Some(outcome) = outcome.as_ref() {
            let intent = self
                .find_turn_intent(handle, turn_id)?
                .ok_or_else(|| AgentConversationError::reconciliation_required("turn outcome has no durable intent"))?;
            validate_outcome(handle, turn_id, &intent.identity, outcome)?;
        }
        Ok(outcome)
    }

    pub(super) fn record_turn_outcome(
        &self,
        handle: &AgentConversationHandle,
        outcome: &AgentTurnOutcome,
    ) -> Result<(), AgentConversationError> {
        let payload =
            serde_json::to_value(outcome).map_err(|error| AgentConversationError::non_retryable(error.to_string()))?;
        self.append_exact(&handle.run_id, TURN_OUTCOME_RECORD, payload, |record| {
            record.payload.get("conversation_id").and_then(Value::as_str) == Some(&handle.conversation_id)
                && record.payload.get("turn_id").and_then(Value::as_str) == Some(&outcome.turn_id)
        })
        .map(|_| ())
    }

    pub(super) fn require_turn_input(
        &self,
        handle: &AgentConversationHandle,
        turn: &AgentTurnSpec,
        identity: &AgentTurnIdentity,
    ) -> Result<(), AgentConversationError> {
        let intent = self
            .find_turn_intent(handle, &turn.turn_id)?
            .ok_or_else(|| AgentConversationError::reconciliation_required("turn outcome has no durable intent"))?;
        if intent.identity != *identity {
            return Err(AgentConversationError::non_retryable(
                "turn ID is already bound to different input",
            ));
        }
        Ok(())
    }

    pub(super) fn has_unresolved_turn(&self, handle: &AgentConversationHandle) -> Result<bool, AgentConversationError> {
        let records = self
            .ledger()
            .records_for_run(&handle.run_id)
            .map_err(|error| AgentConversationError::reconciliation_required(error.to_string()))?;
        let mut turn_ids = HashSet::new();
        for record in records {
            if record.payload.get("conversation_id").and_then(Value::as_str) != Some(&handle.conversation_id)
                || !matches!(record.record_type.as_str(), TURN_INTENT_RECORD | TURN_OUTCOME_RECORD)
            {
                continue;
            }
            let turn_id = record.payload.get("turn_id").and_then(Value::as_str).ok_or_else(|| {
                AgentConversationError::reconciliation_required("durable turn record is missing turn_id")
            })?;
            turn_ids.insert(turn_id.to_owned());
        }
        for turn_id in turn_ids {
            let intent = self.find_turn_intent(handle, &turn_id)?;
            let outcome = self.find_turn_outcome(handle, &turn_id)?;
            if outcome.is_some() && intent.is_none() {
                return Err(AgentConversationError::reconciliation_required(
                    "durable turn outcome has no matching intent",
                ));
            }
            if intent.is_some() && outcome.is_none() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(super) fn find_close(
        &self,
        handle: &AgentConversationHandle,
    ) -> Result<Option<DurableClose>, AgentConversationError> {
        let close: Option<DurableClose> =
            find_typed_record(self.ledger().as_ref(), &handle.run_id, CLOSE_RECORD, |payload| {
                payload.get("conversation_id").and_then(Value::as_str) == Some(&handle.conversation_id)
            })?;
        if let Some(close) = close.as_ref()
            && (close.schema_version != super::CONVERSATION_SCHEMA_VERSION
                || close.run_id != handle.run_id
                || close.parent_agent_id != handle.parent_agent_id
                || close.conversation_id != handle.conversation_id
                || close.task_id != handle.task_id
                || close.open_operation_id != handle.operation_id
                || close.spec_digest != handle.spec_digest
                || close.agent_id != handle.agent_id
                || close.session_id != handle.session_id
                || !matches!(
                    close.terminal_state,
                    AgentLifecycleState::Completed | AgentLifecycleState::Failed
                ))
        {
            return Err(AgentConversationError::reconciliation_required(
                "durable conversation close record is invalid",
            ));
        }
        Ok(close)
    }

    pub(super) fn record_close(
        &self,
        handle: &AgentConversationHandle,
        close: &DurableClose,
    ) -> Result<(), AgentConversationError> {
        let payload =
            serde_json::to_value(close).map_err(|error| AgentConversationError::non_retryable(error.to_string()))?;
        self.append_exact(&handle.run_id, CLOSE_RECORD, payload, |record| {
            record.payload.get("conversation_id").and_then(Value::as_str) == Some(&handle.conversation_id)
        })
        .map(|_| ())
    }

    fn append_exact(
        &self,
        run_id: &RunId,
        record_type: &str,
        payload: Value,
        matches_identity: impl Fn(&LedgerRecord) -> bool,
    ) -> Result<AppendDisposition, AgentConversationError> {
        let ledger = self.ledger();
        self.spawner.lifecycle_runtime.with_run_mutation(run_id, || {
            let existing = matching_records(ledger.as_ref(), run_id, record_type, &matches_identity)?;
            if !existing.is_empty() {
                return exact_payload_disposition(&existing, &payload, record_type, AppendDisposition::Existing);
            }
            let append = ledger.append(run_id, DurabilityClass::SyncCritical, record_type, payload.clone());
            if append.is_ok() {
                return Ok(AppendDisposition::Appended);
            }
            let recovered = matching_records(ledger.as_ref(), run_id, record_type, &matches_identity)?;
            if !recovered.is_empty() {
                return exact_payload_disposition(&recovered, &payload, record_type, AppendDisposition::Appended);
            }
            Err(AgentConversationError::reconciliation_required(format!(
                "failed to append durable {record_type}: {}",
                append.expect_err("append result was checked")
            )))
        })
    }
}

pub(super) fn validate_outcome(
    handle: &AgentConversationHandle,
    turn_id: &str,
    identity: &AgentTurnIdentity,
    outcome: &AgentTurnOutcome,
) -> Result<(), AgentConversationError> {
    let invalid = outcome.schema_version != super::CONVERSATION_SCHEMA_VERSION
        || outcome.run_id != handle.run_id
        || outcome.parent_agent_id != handle.parent_agent_id
        || outcome.conversation_id != handle.conversation_id
        || outcome.task_id != handle.task_id
        || outcome.open_operation_id != handle.operation_id
        || outcome.spec_digest != handle.spec_digest
        || outcome.turn_id != turn_id
        || outcome.agent_id != handle.agent_id
        || outcome.session_id != handle.session_id
        || outcome.operation_id != identity.operation_id
        || outcome.message_id != identity.message_id
        || !valid_outcome_result(outcome);
    if invalid {
        Err(AgentConversationError::reconciliation_required(
            "durable turn outcome does not match its intent and Agent conversation handle",
        ))
    } else {
        Ok(())
    }
}

fn valid_outcome_result(outcome: &AgentTurnOutcome) -> bool {
    match (outcome.status, outcome.failure_class, outcome.error.as_deref()) {
        (AgentOutcomeStatus::Completed, None, None) => true,
        (
            AgentOutcomeStatus::Failed,
            Some(TaskFailureClass::Retryable | TaskFailureClass::NonRetryable),
            Some(error),
        )
        | (AgentOutcomeStatus::Cancelled, Some(TaskFailureClass::NonRetryable), Some(error))
        | (AgentOutcomeStatus::OutcomeUnknown, Some(TaskFailureClass::OutcomeUnknown), Some(error))
        | (AgentOutcomeStatus::ReconciliationRequired, Some(TaskFailureClass::ReconciliationRequired), Some(error)) => {
            !error.trim().is_empty()
        }
        _ => false,
    }
}

fn matching_records(
    ledger: &dyn RuntimeLedger,
    run_id: &RunId,
    record_type: &str,
    matches_identity: &impl Fn(&LedgerRecord) -> bool,
) -> Result<Vec<LedgerRecord>, AgentConversationError> {
    Ok(ledger
        .records_for_run(run_id)
        .map_err(|error| AgentConversationError::reconciliation_required(error.to_string()))?
        .into_iter()
        .filter(|record| record.record_type == record_type && matches_identity(record))
        .collect())
}

fn exact_payload_disposition(
    records: &[LedgerRecord],
    payload: &Value,
    record_type: &str,
    disposition: AppendDisposition,
) -> Result<AppendDisposition, AgentConversationError> {
    if records.iter().all(|record| &record.payload == payload) {
        Ok(disposition)
    } else {
        Err(AgentConversationError::reconciliation_required(format!(
            "durable {record_type} identity is bound to different content"
        )))
    }
}

fn find_typed_record<T: for<'de> Deserialize<'de>>(
    ledger: &dyn RuntimeLedger,
    run_id: &RunId,
    record_type: &str,
    matches: impl Fn(&Value) -> bool,
) -> Result<Option<T>, AgentConversationError> {
    let typed_records = ledger
        .records_for_run(run_id)
        .map_err(|error| AgentConversationError::reconciliation_required(error.to_string()))?
        .into_iter()
        .filter(|record| record.record_type == record_type)
        .map(|record| {
            serde_json::from_value::<T>(record.payload.clone())
                .map(|typed| (record, typed))
                .map_err(|error| {
                    AgentConversationError::reconciliation_required(format!(
                        "malformed durable {record_type} record: {error}"
                    ))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let records = typed_records
        .into_iter()
        .filter(|(record, _)| matches(&record.payload))
        .collect::<Vec<_>>();
    let Some(first) = records.first() else {
        return Ok(None);
    };
    if records.iter().any(|(record, _)| record.payload != first.0.payload) {
        return Err(AgentConversationError::reconciliation_required(format!(
            "conflicting durable {record_type} records share one identity"
        )));
    }
    Ok(records.into_iter().next().map(|(_, typed)| typed))
}
