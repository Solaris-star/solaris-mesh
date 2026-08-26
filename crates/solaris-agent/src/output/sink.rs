use solaris_types::tool::{ToolResultMetadata, ToolResultStatus};

/// Abstraction over output channels (terminal vs JSON stream protocol)
pub trait OutputSink: Send + Sync {
    /// Stream text delta from LLM
    fn emit_text_delta(&self, text: &str, msg_id: &str);

    /// Stream thinking content from LLM
    fn emit_thinking(&self, text: &str, msg_id: &str);

    /// Announce a tool call.
    fn emit_tool_call(&self, tool_use_id: &str, name: &str, input: &str);

    /// Display tool result.
    fn emit_tool_result(&self, tool_use_id: &str, name: &str, is_error: bool, content: &str);

    /// Display a tool result with its full terminal status.
    fn emit_tool_result_with_status(&self, tool_use_id: &str, name: &str, status: ToolResultStatus, content: &str) {
        self.emit_tool_result(tool_use_id, name, status.is_error(), content);
    }

    /// Display a terminal tool result with typed Host diagnostics.
    fn emit_tool_result_with_metadata(
        &self,
        tool_use_id: &str,
        name: &str,
        status: ToolResultStatus,
        content: &str,
        _metadata: Option<&ToolResultMetadata>,
    ) {
        self.emit_tool_result_with_status(tool_use_id, name, status, content);
    }

    /// Signal start of a new message stream
    fn emit_stream_start(&self, msg_id: &str);

    /// Signal end of a message stream with usage stats
    fn emit_stream_end(
        &self,
        msg_id: &str,
        turns: usize,
        input_tokens: u64,
        output_tokens: u64,
        cache_creation_tokens: u64,
        cache_read_tokens: u64,
    );

    /// Signal stream completion with provider-normalized token accounting.
    ///
    /// Sinks that do not expose categorized usage retain the legacy payload.
    #[allow(clippy::too_many_arguments)]
    fn emit_stream_end_with_accounting(
        &self,
        msg_id: &str,
        turns: usize,
        input_tokens: u64,
        output_tokens: u64,
        cache_creation_tokens: u64,
        cache_read_tokens: u64,
        _uncached_input_tokens: u64,
        _cost_usd: Option<f64>,
    ) {
        self.emit_stream_end(
            msg_id,
            turns,
            input_tokens,
            output_tokens,
            cache_creation_tokens,
            cache_read_tokens,
        );
    }

    /// Display error
    fn emit_error(&self, msg: &str);

    /// Emit a terminal structured error associated with one host message.
    ///
    /// Non-protocol sinks retain their existing human-readable behavior.
    fn emit_protocol_error(&self, _msg_id: &str, _code: &str, message: &str, _retryable: bool) {
        self.emit_error(message);
    }

    /// Emit a non-terminal diagnostic associated with one host message.
    ///
    /// Non-protocol sinks retain their existing informational behavior. This
    /// default keeps existing sink implementations source-compatible.
    fn emit_protocol_diagnostic(&self, _msg_id: &str, message: &str) {
        self.emit_info(message);
    }

    /// Display informational message
    fn emit_info(&self, msg: &str);
}

#[cfg(test)]
#[path = "sink_test.rs"]
mod sink_test;
