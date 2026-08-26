use solaris_protocol::events::ToolCategory;
use solaris_types::message::{ContentBlock, Message, Role};

use crate::error::AgentError;
use crate::execution_context::DurableTaskPhase;
use crate::session::StoredDurableTask;
use crate::stream::StreamOutcome;

use super::AgentEngine;
use super::task_phase::validate_persisted_tool_round_call_id;

impl AgentEngine {
    pub(super) async fn resume_pending_tool_round(
        &mut self,
        task: Option<&StoredDurableTask>,
    ) -> Result<Option<bool>, AgentError> {
        let Some(task) = task else {
            return Ok(None);
        };
        let saved_pending_tools = matches!(
            task.phase,
            DurableTaskPhase::ToolsInFlight | DurableTaskPhase::ProviderCompleted
        ) && self.messages.last().is_some_and(|message| {
            message.role == Role::Assistant
                && message
                    .content
                    .iter()
                    .any(|block| matches!(block, ContentBlock::ToolUse { .. }))
        });
        if !saved_pending_tools {
            if task.phase == DurableTaskPhase::ToolsInFlight {
                return Err(AgentError::ReconciliationRequired {
                    task_key: task.task_key.clone(),
                    call_id: task.call_id.clone(),
                });
            }
            return Ok(None);
        }
        let pending = self
            .messages
            .last()
            .ok_or_else(|| AgentError::ApiError("tools_in_flight task has no saved assistant tool round".to_owned()))?;
        let tool_calls = pending
            .content
            .iter()
            .filter(|block| matches!(block, ContentBlock::ToolUse { .. }))
            .cloned()
            .collect::<Vec<_>>();
        let resumed_call_id = if task.phase == DurableTaskPhase::ToolsInFlight {
            validate_persisted_tool_round_call_id(task, &tool_calls, self.legacy_tool_round_ordinal())?;
            Some(
                task.call_id
                    .as_deref()
                    .ok_or_else(|| AgentError::ReconciliationRequired {
                        task_key: task.task_key.clone(),
                        call_id: None,
                    })?,
            )
        } else {
            None
        };
        let assistant_text = pending
            .content
            .iter()
            .filter_map(|block| match block {
                ContentBlock::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<String>();
        let resumed_round = self
            .execute_tool_round_with_call_id(&tool_calls, &assistant_text, resumed_call_id)
            .await?;
        self.persist_plan_artifacts(&resumed_round.tool_modifiers)?;
        self.apply_context_modifiers(&resumed_round.tool_modifiers);
        let mutation = self.tool_round_has_successful_mutation(&tool_calls, &resumed_round.tool_results);
        self.messages.push(Message::now(Role::User, resumed_round.tool_results));
        self.save_session()?;
        self.record_durable_task_phase(DurableTaskPhase::ToolsCompleted, Some(&resumed_round.task_call_id))?;
        Ok(Some(mutation))
    }

    pub(super) fn uses_post_mutation_verification(&self) -> bool {
        matches!(
            self.reasoning_effort.as_deref().map(str::to_ascii_lowercase).as_deref(),
            Some("high" | "xhigh" | "x_high" | "extra" | "max" | "ultra" | "ultracode")
        )
    }

    pub(super) fn tool_round_has_successful_mutation(
        &self,
        tool_calls: &[ContentBlock],
        tool_results: &[ContentBlock],
    ) -> bool {
        tool_calls.iter().zip(tool_results).any(|(call, result)| {
            let ContentBlock::ToolUse { name, .. } = call else {
                return false;
            };
            matches!(result, ContentBlock::ToolResult { is_error: false, .. })
                && self
                    .tools
                    .get(name)
                    .is_some_and(|tool| tool.category() == ToolCategory::Edit)
        })
    }

    pub(super) fn emit_recovered_stream(&self, outcome: &StreamOutcome, emit_assistant_text: bool) {
        if !outcome.thinking_text.is_empty() {
            self.output.emit_thinking(&outcome.thinking_text, &self.msg_id);
        }
        if emit_assistant_text && !outcome.assistant_text.is_empty() {
            self.output.emit_text_delta(&outcome.assistant_text, &self.msg_id);
        }
        for call in &outcome.tool_calls {
            if let ContentBlock::ToolUse { id, name, input, .. } = call {
                self.output
                    .emit_tool_call(id, name, &serde_json::to_string(input).unwrap_or_default());
            }
        }
    }
}
