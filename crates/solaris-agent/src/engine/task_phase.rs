use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use solaris_types::message::{ContentBlock, StopReason, TokenUsage};
use solaris_types::runtime::TaskFailureClass;
use solaris_types::spawner::AgentOutcomeStatus;
use solaris_types::tool::{ToolCallStat, ToolResultStatus};

use crate::error::AgentError;
use crate::execution_context::EffectExecutionContext;
use crate::session::{ActiveSessionFence, DurableTaskPhase, StoredDurableTask};
use crate::turn::{FinalizationReason, TurnKind};

use super::{AgentEngine, AgentResult};

const TASK_KEY_DOMAIN_V1: &[u8] = b"solaris/agent-task-key/v1\0";
const TASK_INPUT_DOMAIN_V1: &[u8] = b"solaris/agent-task-input/v1\0";
const PROVIDER_CALL_DOMAIN_V1: &[u8] = b"solaris/provider-call/v1\0";
const TOOL_ROUND_CALL_DOMAIN_V1: &[u8] = b"solaris/tool-round-call/v1\0";
const TOOL_ROUND_CALL_DOMAIN_V2: &[u8] = b"solaris/tool-round-call/v2\0";
const TOOL_ROUND_CALL_DOMAIN_V3: &[u8] = b"solaris/tool-round-call/v3\0";

pub(super) enum DurableTaskResume {
    New(Option<StoredDurableTask>),
    UserCheckpointed,
    Resume(StoredDurableTask),
    Completed(AgentResult),
}

pub(super) struct DurableTaskPhaseGuard {
    context: EffectExecutionContext,
    task_fence: Option<ActiveSessionFence>,
    message_id: String,
    task_key: String,
    call_id: String,
    terminal: bool,
}

impl DurableTaskPhaseGuard {
    pub(super) fn complete(&mut self, phase: DurableTaskPhase) -> Result<(), AgentError> {
        transition_session_task(
            self.task_fence.as_ref(),
            &self.task_key,
            phase,
            Some(&self.call_id),
            None,
        )?;
        // session.sqlite3 is the recovery source of truth. If the following
        // Ledger audit append fails after that commit, Drop must not overwrite
        // the committed phase with OutcomeUnknown.
        self.terminal = true;
        self.context
            .record_task_phase(&self.message_id, phase, Some(&self.call_id))
            .map_err(task_phase_error)?;
        Ok(())
    }

    pub(super) fn disarm(&mut self) {
        self.terminal = true;
    }
}

impl Drop for DurableTaskPhaseGuard {
    fn drop(&mut self) {
        if self.terminal {
            return;
        }
        if let Err(error) = transition_session_task(
            self.task_fence.as_ref(),
            &self.task_key,
            DurableTaskPhase::OutcomeUnknown,
            Some(&self.call_id),
            None,
        ) {
            tracing::error!(target: "solaris_agent", error = %error, "durable task outcome-unknown session persistence failed");
        }
        if let Err(error) =
            self.context
                .record_task_phase(&self.message_id, DurableTaskPhase::OutcomeUnknown, Some(&self.call_id))
        {
            tracing::error!(target: "solaris_agent", error = %error, "durable task outcome-unknown ledger persistence failed");
        }
    }
}

impl AgentEngine {
    pub(super) fn current_tool_round_call_id(&self, tool_calls: &[ContentBlock]) -> Result<String, AgentError> {
        let task_key = durable_task_key(&self.msg_id);
        let round_digest = tool_round_digest(tool_calls)?;
        let Some(fence) = self.durable_task_fence()? else {
            return Ok(ephemeral_tool_round_call_id(&task_key, &round_digest));
        };
        let task = fence
            .load_task(&task_key)
            .map_err(|error| AgentError::ApiError(format!("durable task load failed: {error}")))?;
        let transition_revision = task
            .task_revision
            .checked_add(1)
            .ok_or_else(|| AgentError::ApiError("durable task revision exhausted".to_owned()))?;
        Ok(tool_round_call_id(&task_key, transition_revision, &round_digest))
    }

    pub(super) fn pending_tool_round_call_id(&self, tool_calls: &[ContentBlock]) -> Result<String, AgentError> {
        let task_key = durable_task_key(&self.msg_id);
        if let Some(fence) = self.durable_task_fence()? {
            let task = fence
                .load_task(&task_key)
                .map_err(|error| AgentError::ApiError(format!("durable task load failed: {error}")))?;
            if task.phase == DurableTaskPhase::ToolsInFlight {
                validate_persisted_tool_round_call_id(&task, tool_calls, self.legacy_tool_round_ordinal())?;
                return task.call_id.ok_or_else(|| AgentError::ReconciliationRequired {
                    task_key,
                    call_id: None,
                });
            }
        }
        self.current_tool_round_call_id(tool_calls)
    }

    pub(super) fn legacy_tool_round_ordinal(&self) -> usize {
        self.messages
            .iter()
            .filter(|message| {
                message
                    .content
                    .iter()
                    .any(|block| matches!(block, ContentBlock::ToolUse { .. }))
            })
            .count()
    }

    pub(super) fn begin_or_resume_durable_task(&self, user_input: &str) -> Result<DurableTaskResume, AgentError> {
        let Some(fence) = self.durable_task_fence()? else {
            return Ok(DurableTaskResume::New(None));
        };
        let task_key = durable_task_key(&self.msg_id);
        let input_digest = domain_digest(TASK_INPUT_DOMAIN_V1, user_input.as_bytes());
        let task = fence
            .begin_or_resume_task(&task_key, &input_digest)
            .map_err(|error| AgentError::ApiError(format!("durable task resume failed: {error}")))?;
        match task.phase {
            DurableTaskPhase::Completed => decode_terminal_result(&task).map(DurableTaskResume::Completed),
            DurableTaskPhase::OutcomeUnknown => Err(AgentError::ReconciliationRequired {
                task_key,
                call_id: task.call_id,
            }),
            DurableTaskPhase::Aborted => Err(AgentError::UserAborted),
            DurableTaskPhase::Created => Ok(DurableTaskResume::New(Some(task))),
            DurableTaskPhase::UserCheckpointed => Ok(DurableTaskResume::UserCheckpointed),
            _ => Ok(DurableTaskResume::Resume(task)),
        }
    }

    pub(super) fn checkpoint_new_durable_task_user(
        &mut self,
        task: Option<&StoredDurableTask>,
    ) -> Result<(), AgentError> {
        let Some(task) = task else {
            return self.save_session();
        };
        let manager = self.session_manager.as_ref().ok_or_else(|| {
            AgentError::ApiError("session manager is required for durable user checkpoint".to_owned())
        })?;
        let session = self
            .current_session
            .as_mut()
            .ok_or_else(|| AgentError::ApiError("active session is required for durable user checkpoint".to_owned()))?;
        session.messages.clone_from(&self.messages);
        session.provider.clone_from(&self.provider_label);
        session.model.clone_from(&self.model);
        session.total_usage.clone_from(&self.total_usage);
        session.updated_at = Utc::now();
        manager
            .checkpoint_active_task_user(session, &task.task_key)
            .map_err(|error| AgentError::ApiError(format!("durable user checkpoint failed: {error}")))?;
        let context = self.execution_context.as_ref().ok_or_else(|| {
            AgentError::ApiError("effect execution context is required for durable user checkpoint audit".to_owned())
        })?;
        context
            .record_task_phase(&self.msg_id, DurableTaskPhase::UserCheckpointed, None)
            .map_err(task_phase_error)
    }

    pub(super) fn record_durable_task_phase(
        &self,
        phase: DurableTaskPhase,
        call_id: Option<&str>,
    ) -> Result<(), AgentError> {
        let context = self.execution_context.as_ref().ok_or_else(|| {
            AgentError::ApiError("effect execution context is required for durable task phase persistence".to_owned())
        })?;
        let task_key = durable_task_key(&self.msg_id);
        let fence = self.durable_task_fence()?;
        transition_session_task(fence.as_ref(), &task_key, phase, call_id, None)?;
        context
            .record_task_phase(&self.msg_id, phase, call_id)
            .map_err(task_phase_error)
    }

    pub(super) fn record_completed_durable_task(&self, result: &AgentResult) -> Result<(), AgentError> {
        let context = self.execution_context.as_ref().ok_or_else(|| {
            AgentError::ApiError("effect execution context is required for durable task completion".to_owned())
        })?;
        let task_key = durable_task_key(&self.msg_id);
        let terminal = serde_json::to_vec(&StoredTerminalResult::from(result))
            .map_err(|error| AgentError::ApiError(format!("durable task terminal serialization failed: {error}")))?;
        let fence = self.durable_task_fence()?;
        transition_session_task(
            fence.as_ref(),
            &task_key,
            DurableTaskPhase::Completed,
            None,
            Some(&terminal),
        )?;
        context
            .record_task_phase(&self.msg_id, DurableTaskPhase::Completed, None)
            .map_err(task_phase_error)
    }

    pub(super) fn begin_durable_task_phase(
        &self,
        phase: DurableTaskPhase,
        call_id: &str,
    ) -> Result<DurableTaskPhaseGuard, AgentError> {
        let context = self.execution_context.clone().ok_or_else(|| {
            AgentError::ApiError("effect execution context is required for durable task phase persistence".to_owned())
        })?;
        let task_key = durable_task_key(&self.msg_id);
        let task_fence = self.durable_task_fence()?;
        transition_session_task(task_fence.as_ref(), &task_key, phase, Some(call_id), None)?;
        context
            .record_task_phase(&self.msg_id, phase, Some(call_id))
            .map_err(task_phase_error)?;
        Ok(DurableTaskPhaseGuard {
            context,
            task_fence,
            message_id: self.msg_id.clone(),
            task_key,
            call_id: call_id.to_owned(),
            terminal: false,
        })
    }

    fn durable_task_fence(&self) -> Result<Option<ActiveSessionFence>, AgentError> {
        let (Some(manager), Some(session)) = (&self.session_manager, &self.current_session) else {
            return Ok(None);
        };
        manager
            .active_task_fence(&session.id)
            .map(Some)
            .map_err(|error| AgentError::ApiError(format!("durable task lease unavailable: {error}")))
    }

    /// Build duplicate-detection stats for one tool round.
    ///
    /// Each `ToolUse` block is paired positionally with its terminal status and
    /// fingerprinted within the current task and environment scope.
    /// Non-`ToolUse` blocks are skipped together with their status so the
    /// pairing stays aligned.
    pub(super) fn tool_call_stats(
        &self,
        tool_calls: &[ContentBlock],
        statuses: &[ToolResultStatus],
    ) -> Vec<ToolCallStat> {
        let Some(context) = self.execution_context.as_ref() else {
            return Vec::new();
        };
        let task_scope = durable_task_key(&self.msg_id);
        let scope = context.tool_call_scope(&task_scope);
        tool_calls
            .iter()
            .zip(statuses)
            .filter_map(|(block, status)| match block {
                ContentBlock::ToolUse { name, input, .. } => Some((name.as_str(), input, *status)),
                _ => None,
            })
            .map(|(name, input, status)| ToolCallStat::new(&scope, name, input, status))
            .collect()
    }
}

pub(super) fn provider_call_id(kind_key: &str, request_digest: &str) -> String {
    let mut input = Vec::with_capacity(kind_key.len() + request_digest.len() + 1);
    input.extend_from_slice(kind_key.as_bytes());
    input.push(0);
    input.extend_from_slice(request_digest.as_bytes());
    format!(
        "provider-call-v1:sha256:{}",
        hex_digest(PROVIDER_CALL_DOMAIN_V1, &input)
    )
}

pub(super) fn tool_round_call_id(task_key: &str, task_revision: i64, round_digest: &str) -> String {
    let mut input = Vec::with_capacity(task_key.len() + round_digest.len() + std::mem::size_of::<i64>() + 2);
    input.extend_from_slice(task_key.as_bytes());
    input.push(0);
    input.extend_from_slice(&task_revision.to_be_bytes());
    input.push(0);
    input.extend_from_slice(round_digest.as_bytes());
    format!(
        "tool-round-call-v3:sha256:{}",
        hex_digest(TOOL_ROUND_CALL_DOMAIN_V3, &input)
    )
}

fn legacy_v2_tool_round_call_id(task_key: &str, round_ordinal: usize, round_digest: &str) -> String {
    let mut input = Vec::with_capacity(task_key.len() + round_digest.len() + std::mem::size_of::<u64>() + 2);
    input.extend_from_slice(task_key.as_bytes());
    input.push(0);
    input.extend_from_slice(&(round_ordinal as u64).to_be_bytes());
    input.push(0);
    input.extend_from_slice(round_digest.as_bytes());
    format!(
        "tool-round-call-v2:sha256:{}",
        hex_digest(TOOL_ROUND_CALL_DOMAIN_V2, &input)
    )
}

fn ephemeral_tool_round_call_id(task_key: &str, round_digest: &str) -> String {
    let nonce = Uuid::now_v7();
    let mut input = Vec::with_capacity(task_key.len() + round_digest.len() + nonce.as_bytes().len() + 2);
    input.extend_from_slice(task_key.as_bytes());
    input.push(0);
    input.extend_from_slice(nonce.as_bytes());
    input.push(0);
    input.extend_from_slice(round_digest.as_bytes());
    format!(
        "tool-round-call-v3:sha256:{}",
        hex_digest(TOOL_ROUND_CALL_DOMAIN_V3, &input)
    )
}

pub(super) fn legacy_tool_round_call_id(round_digest: &str) -> String {
    format!(
        "tool-round-call-v1:sha256:{}",
        hex_digest(TOOL_ROUND_CALL_DOMAIN_V1, round_digest.as_bytes())
    )
}

pub(super) fn validate_persisted_tool_round_call_id(
    task: &StoredDurableTask,
    tool_calls: &[ContentBlock],
    legacy_round_ordinal: usize,
) -> Result<(), AgentError> {
    let round_digest = tool_round_digest(tool_calls)?;
    let expected = tool_round_call_id(&task.task_key, task.task_revision, &round_digest);
    if task.call_id.as_deref() == Some(expected.as_str()) {
        return Ok(());
    }
    let legacy_v2 = legacy_v2_tool_round_call_id(&task.task_key, legacy_round_ordinal, &round_digest);
    if task.call_id.as_deref() == Some(legacy_v2.as_str()) {
        return Ok(());
    }
    let legacy_v1 = legacy_tool_round_call_id(&round_digest);
    if task.call_id.as_deref() == Some(legacy_v1.as_str()) {
        return Ok(());
    }
    Err(AgentError::ReconciliationRequired {
        task_key: task.task_key.clone(),
        call_id: task.call_id.clone(),
    })
}

fn tool_round_digest(tool_calls: &[ContentBlock]) -> Result<String, AgentError> {
    let round_input = serde_json::to_value(tool_calls)
        .map_err(|error| AgentError::ApiError(format!("tool round serialization failed: {error}")))?;
    Ok(crate::execution_context::stable_digest_value(&round_input))
}

pub(super) fn require_resume_call_id(task: &StoredDurableTask, expected_call_id: &str) -> Result<(), AgentError> {
    if task.call_id.as_deref() == Some(expected_call_id) {
        return Ok(());
    }
    Err(AgentError::ReconciliationRequired {
        task_key: task.task_key.clone(),
        call_id: task.call_id.clone(),
    })
}

pub(super) fn turn_kind_key(kind: TurnKind) -> &'static str {
    match kind {
        TurnKind::Normal => "normal",
        TurnKind::MaxTokensContinuation => "max_tokens_continuation",
        TurnKind::PostMutationVerification => "post_mutation_verification",
        TurnKind::Finalization(FinalizationReason::TurnBudget) => "finalization_turn_budget",
        TurnKind::Finalization(FinalizationReason::MaxTokens) => "finalization_max_tokens",
        TurnKind::Finalization(FinalizationReason::EmptyFinal) => "finalization_empty_final",
    }
}

pub(super) fn durable_task_key(message_id: &str) -> String {
    format!(
        "task-key-v1:sha256:{}",
        hex_digest(TASK_KEY_DOMAIN_V1, message_id.as_bytes())
    )
}

fn domain_digest(domain: &[u8], input: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(input);
    hasher.finalize().to_vec()
}

fn hex_digest(domain: &[u8], input: &[u8]) -> String {
    domain_digest(domain, input)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn transition_session_task(
    fence: Option<&ActiveSessionFence>,
    task_key: &str,
    phase: DurableTaskPhase,
    call_id: Option<&str>,
    terminal_result_json: Option<&[u8]>,
) -> Result<(), AgentError> {
    let Some(fence) = fence else {
        return Ok(());
    };
    fence
        .transition_task(task_key, phase, call_id, terminal_result_json)
        .map(|_| ())
        .map_err(|error| AgentError::ApiError(format!("durable task transition failed: {error}")))
}

fn task_phase_error(error: std::io::Error) -> AgentError {
    AgentError::ApiError(format!("durable task phase persistence failed: {error}"))
}

#[derive(Serialize, Deserialize)]
struct StoredTerminalResult {
    status: AgentOutcomeStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    failure_class: Option<TaskFailureClass>,
    text: String,
    stop_reason: StopReason,
    usage: TokenUsage,
    turns: usize,
}

impl From<&AgentResult> for StoredTerminalResult {
    fn from(result: &AgentResult) -> Self {
        Self {
            status: result.status,
            failure_class: result.failure_class,
            text: result.text.clone(),
            stop_reason: result.stop_reason,
            usage: result.usage.clone(),
            turns: result.turns,
        }
    }
}

fn decode_terminal_result(task: &StoredDurableTask) -> Result<AgentResult, AgentError> {
    let bytes = task
        .terminal_result_json
        .as_deref()
        .ok_or_else(|| AgentError::ApiError("completed durable task is missing its terminal result".to_owned()))?;
    let stored: StoredTerminalResult = serde_json::from_slice(bytes)
        .map_err(|error| AgentError::ApiError(format!("completed durable task result is invalid: {error}")))?;
    Ok(AgentResult {
        status: stored.status,
        failure_class: stored.failure_class,
        text: stored.text,
        stop_reason: stored.stop_reason,
        usage: stored.usage,
        turns: stored.turns,
    })
}
