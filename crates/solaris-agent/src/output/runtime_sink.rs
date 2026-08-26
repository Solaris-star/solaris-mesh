use std::sync::Arc;

use serde_json::json;
use solaris_types::identity::{AgentId, RunId};
use solaris_types::tool::{ToolResultMetadata, ToolResultStatus};

use crate::collaboration_runtime::CollaborationRuntime;

use super::OutputSink;

fn tool_result_payload(tool_use_id: &str, name: &str, status: ToolResultStatus, content: &str) -> serde_json::Value {
    json!({
        "call_id": tool_use_id,
        "tool": name,
        "status": status,
        "is_error": status.is_error(),
        "content": content,
    })
}

fn tool_result_payload_with_metadata(
    tool_use_id: &str,
    name: &str,
    status: ToolResultStatus,
    content: &str,
    metadata: Option<&ToolResultMetadata>,
) -> serde_json::Value {
    let mut payload = tool_result_payload(tool_use_id, name, status, content);
    if let Some(metadata) = metadata
        && let Some(object) = payload.as_object_mut()
    {
        object.insert("metadata".to_owned(), json!(metadata));
    }
    payload
}

/// Child-agent output sink that keeps sub-agent streams out of the parent's
/// ordinary text channel while publishing them to the Mesh runtime event bus.
pub struct RuntimeOutputSink {
    runtime: Arc<CollaborationRuntime<()>>,
    run_id: RunId,
    agent_id: AgentId,
}

impl RuntimeOutputSink {
    pub fn new(runtime: Arc<CollaborationRuntime<()>>, run_id: RunId, agent_id: AgentId) -> Self {
        Self {
            runtime,
            run_id,
            agent_id,
        }
    }

    fn emit(&self, kind: &str, payload: serde_json::Value) {
        self.runtime
            .emit_live_event(self.run_id.clone(), Some(self.agent_id.clone()), kind, payload);
    }
}

impl OutputSink for RuntimeOutputSink {
    fn emit_text_delta(&self, text: &str, msg_id: &str) {
        self.emit("agent_text_delta", json!({"msg_id": msg_id, "text": text}));
    }

    fn emit_thinking(&self, text: &str, msg_id: &str) {
        self.emit("agent_thinking_delta", json!({"msg_id": msg_id, "text": text}));
    }

    fn emit_tool_call(&self, tool_use_id: &str, name: &str, input: &str) {
        self.emit(
            "agent_tool_call",
            json!({"call_id": tool_use_id, "tool": name, "input": input}),
        );
    }

    fn emit_tool_result(&self, tool_use_id: &str, name: &str, is_error: bool, content: &str) {
        self.emit(
            "agent_tool_result",
            tool_result_payload(
                tool_use_id,
                name,
                ToolResultStatus::from_legacy_is_error(is_error),
                content,
            ),
        );
    }

    fn emit_tool_result_with_status(&self, tool_use_id: &str, name: &str, status: ToolResultStatus, content: &str) {
        self.emit(
            "agent_tool_result",
            tool_result_payload(tool_use_id, name, status, content),
        );
    }

    fn emit_tool_result_with_metadata(
        &self,
        tool_use_id: &str,
        name: &str,
        status: ToolResultStatus,
        content: &str,
        metadata: Option<&ToolResultMetadata>,
    ) {
        self.emit(
            "agent_tool_result",
            tool_result_payload_with_metadata(tool_use_id, name, status, content, metadata),
        );
    }

    fn emit_stream_start(&self, msg_id: &str) {
        self.emit("agent_stream_start", json!({"msg_id": msg_id}));
    }

    fn emit_stream_end(
        &self,
        msg_id: &str,
        turns: usize,
        input_tokens: u64,
        output_tokens: u64,
        cache_creation_tokens: u64,
        cache_read_tokens: u64,
    ) {
        self.emit(
            "agent_stream_end",
            json!({
                "msg_id": msg_id,
                "turns": turns,
                "usage": {
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "cache_creation_tokens": cache_creation_tokens,
                    "cache_read_tokens": cache_read_tokens,
                }
            }),
        );
    }

    fn emit_stream_end_with_accounting(
        &self,
        msg_id: &str,
        turns: usize,
        input_tokens: u64,
        output_tokens: u64,
        cache_creation_tokens: u64,
        cache_read_tokens: u64,
        uncached_input_tokens: u64,
        cost_usd: Option<f64>,
    ) {
        self.emit(
            "agent_stream_end",
            json!({
                "msg_id": msg_id,
                "turns": turns,
                "usage": {
                    "input_tokens": input_tokens,
                    "uncached_input_tokens": uncached_input_tokens,
                    "output_tokens": output_tokens,
                    "cache_creation_tokens": cache_creation_tokens,
                    "cache_read_tokens": cache_read_tokens,
                    "cost_usd": cost_usd,
                }
            }),
        );
    }

    fn emit_error(&self, msg: &str) {
        self.emit("agent_error", json!({"message": msg}));
    }

    fn emit_info(&self, msg: &str) {
        self.emit("agent_info", json!({"message": msg}));
    }
}

#[cfg(test)]
#[path = "runtime_sink_test.rs"]
mod runtime_sink_test;
