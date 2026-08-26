use serde_json::{Value, json};
use solaris_types::identity::RunId;
use solaris_types::spawner::{
    AgentConversationError, AgentConversationHandle, AgentTurnOutcome, AgentTurnOutputProjection, AgentTurnSpec,
    OutcomeBlobRef,
};

use crate::execution_context::{EffectOutputStore, stable_digest_bytes};

use super::AgentConversationService;

pub(super) struct JsonTextProjection<'a> {
    pub(super) blob_run_id: &'a RunId,
    pub(super) pointer: &'a str,
    pub(super) parse: &'a (dyn Fn(&str) -> Option<Value> + Send + Sync),
}

impl AgentConversationService {
    pub(crate) async fn run_turn_with_json_text_projection(
        &self,
        handle: &AgentConversationHandle,
        turn: AgentTurnSpec,
        blob_run_id: &RunId,
        pointer: &str,
        parse: &(dyn Fn(&str) -> Option<Value> + Send + Sync),
    ) -> Result<AgentTurnOutcome, AgentConversationError> {
        let projection = JsonTextProjection {
            blob_run_id,
            pointer,
            parse,
        };
        self.run_turn_inner(handle, turn, Some(&projection)).await
    }

    pub(super) fn project_turn_outcome(
        &self,
        outcome: &AgentTurnOutcome,
        projection: Option<&JsonTextProjection<'_>>,
    ) -> Result<AgentTurnOutcome, AgentConversationError> {
        let Some(projection) = projection else {
            return Ok(outcome.clone());
        };
        let Some(text) = outcome.output.get("text").and_then(Value::as_str) else {
            return Ok(outcome.clone());
        };
        let Some(mut structured) = (projection.parse)(text) else {
            return Ok(outcome.clone());
        };
        let Some(body) = structured.pointer(projection.pointer).cloned() else {
            return Ok(outcome.clone());
        };
        let Some(slot) = structured.pointer_mut(projection.pointer) else {
            return Ok(outcome.clone());
        };
        *slot = Value::Null;
        let serialized = serde_json::to_string(&body).map_err(|error| {
            AgentConversationError::non_retryable(format!("failed to encode projected turn output: {error}"))
        })?;
        let bytes = u64::try_from(serialized.len())
            .map_err(|_| AgentConversationError::non_retryable("projected turn output is too large"))?;
        let digest = stable_digest_bytes(serialized.as_bytes());
        let reference = EffectOutputStore::for_run_with_ledger(
            projection.blob_run_id,
            self.spawner.lifecycle_runtime.ledger().as_ref(),
        )
        .write_named(
            &format!("agent-conversation-turn:{}", outcome.operation_id),
            &serialized,
        )
        .map_err(|error| {
            AgentConversationError::reconciliation_required(format!("failed to store projected turn output: {error}"))
        })?;
        let mut durable = outcome.clone();
        durable.output = json!({"text": serde_json::to_string(&structured).map_err(|error| {
            AgentConversationError::non_retryable(format!("failed to redact projected turn output: {error}"))
        })?});
        durable.outcome_ref = Some(OutcomeBlobRef {
            reference,
            bytes,
            digest,
            run_id: Some(projection.blob_run_id.clone()),
            status: Some(outcome.status),
        });
        durable.output_projection = Some(AgentTurnOutputProjection::JsonTextPointer {
            pointer: projection.pointer.to_owned(),
        });
        Ok(durable)
    }

    pub(super) fn hydrate_turn_outcome(
        &self,
        outcome: &AgentTurnOutcome,
    ) -> Result<AgentTurnOutcome, AgentConversationError> {
        let (Some(reference), Some(AgentTurnOutputProjection::JsonTextPointer { pointer })) =
            (&outcome.outcome_ref, &outcome.output_projection)
        else {
            return Ok(outcome.clone());
        };
        if reference.status.is_some_and(|status| status != outcome.status) {
            return Err(AgentConversationError::reconciliation_required(
                "durable turn output status conflicts with its blob reference",
            ));
        }
        let blob_run_id = reference.run_id.as_ref().unwrap_or(&outcome.run_id);
        let serialized =
            EffectOutputStore::for_run_with_ledger(blob_run_id, self.spawner.lifecycle_runtime.ledger().as_ref())
                .read(&reference.reference)
                .map_err(|error| {
                    AgentConversationError::reconciliation_required(format!(
                        "failed to read projected turn output: {error}"
                    ))
                })?;
        if u64::try_from(serialized.len()).ok() != Some(reference.bytes)
            || stable_digest_bytes(serialized.as_bytes()) != reference.digest
        {
            return Err(AgentConversationError::reconciliation_required(
                "durable turn output blob identity changed",
            ));
        }
        let body: Value = serde_json::from_str(&serialized).map_err(|error| {
            AgentConversationError::reconciliation_required(format!("invalid projected turn output blob: {error}"))
        })?;
        let text = outcome.output.get("text").and_then(Value::as_str).ok_or_else(|| {
            AgentConversationError::reconciliation_required("projected turn outcome is missing output.text")
        })?;
        let mut structured: Value = serde_json::from_str(text).map_err(|error| {
            AgentConversationError::reconciliation_required(format!("invalid redacted turn output: {error}"))
        })?;
        let slot = structured.pointer_mut(pointer).ok_or_else(|| {
            AgentConversationError::reconciliation_required("redacted turn output is missing its projection slot")
        })?;
        if !slot.is_null() {
            return Err(AgentConversationError::reconciliation_required(
                "turn output body is duplicated inline and by reference",
            ));
        }
        *slot = body;
        let mut hydrated = outcome.clone();
        hydrated.output = json!({"text": serde_json::to_string(&structured).map_err(|error| {
            AgentConversationError::reconciliation_required(format!("failed to hydrate turn output: {error}"))
        })?});
        Ok(hydrated)
    }
}
