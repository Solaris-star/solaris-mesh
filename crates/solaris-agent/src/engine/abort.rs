use solaris_types::message::{ContentBlock, Message, Role};
use solaris_types::tool::ToolResultStatus;
use tracing::{error, info};

use crate::error::AgentError;
use crate::execution_context::DurableTaskPhase;

use super::AgentEngine;
use super::task_phase::DurableTaskPhaseGuard;

impl AgentEngine {
    pub(super) fn close_cancelled_tool_round<T>(
        &mut self,
        tool_calls: &[ContentBlock],
        call_id: &str,
        task_phase: &mut DurableTaskPhaseGuard,
        reason: &str,
    ) -> Result<T, AgentError> {
        let outcome_unknown = self
            .execution_context
            .as_ref()
            .is_some_and(|context| context.has_unresolved_effect_intent().unwrap_or(true));
        let status = if outcome_unknown {
            ToolResultStatus::OutcomeUnknown
        } else {
            ToolResultStatus::Aborted
        };
        let statuses = tool_calls
            .iter()
            .filter(|block| matches!(block, ContentBlock::ToolUse { .. }))
            .map(|_| status)
            .collect::<Vec<_>>();
        if let Some(context) = &self.execution_context {
            context
                .record_tool_calls_with_inputs_once(call_id, tool_calls, &statuses)
                .map_err(AgentError::ResourceBudgetExceeded)?;
        }
        let result_blocks = tool_calls
            .iter()
            .filter_map(|block| match block {
                ContentBlock::ToolUse { id, name, .. } => {
                    self.output.emit_tool_result_with_status(id, name, status, reason);
                    Some(ContentBlock::ToolResult {
                        tool_use_id: id.clone(),
                        content: reason.to_owned(),
                        is_error: true,
                    })
                }
                _ => None,
            })
            .collect();
        self.messages.push(Message::now(Role::User, result_blocks));
        self.save_session()?;

        let phase = if outcome_unknown {
            DurableTaskPhase::OutcomeUnknown
        } else {
            DurableTaskPhase::Aborted
        };
        task_phase.complete(phase)?;
        if outcome_unknown {
            return Err(AgentError::ReconciliationRequired {
                task_key: "current durable task".to_owned(),
                call_id: Some(call_id.to_owned()),
            });
        }
        Err(AgentError::UserAborted)
    }

    /// Close a partially recorded turn after the host cancels execution.
    pub fn abort_current_turn(&mut self, reason: &str) {
        if let Err(error) = self.ensure_session_lease() {
            error!(target: "solaris_agent", error = %error, "cannot close aborted turn after session lease loss");
            return;
        }
        let Some(last_message) = self.messages.last() else {
            return;
        };
        if last_message.role != Role::Assistant {
            return;
        }
        let pending_tool_calls = last_message
            .content
            .iter()
            .filter(|block| matches!(block, ContentBlock::ToolUse { .. }))
            .cloned()
            .collect::<Vec<_>>();
        if pending_tool_calls.is_empty() {
            return;
        }

        let outcome_unknown = self
            .execution_context
            .as_ref()
            .is_some_and(|context| context.has_unresolved_effect_intent().unwrap_or(true));
        let status = if outcome_unknown {
            ToolResultStatus::OutcomeUnknown
        } else {
            ToolResultStatus::Aborted
        };
        match self.pending_tool_round_call_id(&pending_tool_calls) {
            Ok(call_id) => {
                let statuses = vec![status; pending_tool_calls.len()];
                if let Some(context) = &self.execution_context
                    && let Err(error) =
                        context.record_tool_calls_with_inputs_once(&call_id, &pending_tool_calls, &statuses)
                {
                    error!(target: "solaris_agent", error = %error, "failed to persist aborted tool-call statistics");
                }
            }
            Err(error) => {
                error!(target: "solaris_agent", error = %error, "failed to identify aborted tool round for statistics");
            }
        }
        let result_blocks = pending_tool_calls
            .into_iter()
            .filter_map(|block| match block {
                ContentBlock::ToolUse {
                    id: tool_use_id, name, ..
                } => {
                    info!(
                        target: "solaris_agent",
                        tool_use_id = %tool_use_id,
                        tool = %name,
                        "closing pending tool_use after abort"
                    );
                    self.output
                        .emit_tool_result_with_status(&tool_use_id, &name, status, reason);
                    Some(ContentBlock::ToolResult {
                        tool_use_id,
                        content: reason.to_owned(),
                        is_error: true,
                    })
                }
                _ => None,
            })
            .collect();
        self.messages.push(Message::now(Role::User, result_blocks));
        if let Err(error) = self.save_session() {
            error!(target: "solaris_agent", error = %error, "failed to persist aborted session turn");
            return;
        }
        let phase = if outcome_unknown {
            DurableTaskPhase::OutcomeUnknown
        } else {
            DurableTaskPhase::Aborted
        };
        if let Err(error) = self.record_durable_task_phase(phase, None) {
            error!(target: "solaris_agent", error = %error, "failed to persist aborted durable task phase");
        }
    }
}
