use std::io;
use std::sync::{Arc, Mutex};

use solaris_config::compat::ProviderCompat;
use solaris_protocol::events::{Capabilities, ErrorInfo, ProtocolEvent, RuntimeConfiguration, Usage};
use solaris_protocol::writer::ProtocolEmitter;

use super::OutputSink;

/// JSON stream protocol output sink
pub struct ProtocolSink {
    writer: Arc<dyn ProtocolEmitter>,
    fatal_error: Mutex<Option<StoredIoError>>,
}

#[derive(Clone)]
struct StoredIoError {
    kind: io::ErrorKind,
    message: String,
}

impl ProtocolSink {
    pub fn new(writer: Arc<dyn ProtocolEmitter>) -> Self {
        Self {
            writer,
            fatal_error: Mutex::new(None),
        }
    }

    /// Emit the ready event at session start
    pub fn emit_ready(
        &self,
        compat: &ProviderCompat,
        has_mcp: bool,
        session_id: Option<String>,
        resumed: bool,
        current_mode: &str,
    ) {
        self.emit_event(&ProtocolEvent::Ready {
            version: env!("CARGO_PKG_VERSION").to_string(),
            session_id,
            resumed,
            capabilities: Self::capabilities(compat, has_mcp, current_mode),
        });
    }

    /// Emit a config_changed event after set_config or set_mode updates
    pub fn emit_config_changed(
        &self,
        compat: &ProviderCompat,
        has_mcp: bool,
        current_mode: &str,
        configuration: RuntimeConfiguration,
    ) {
        self.emit_event(&ProtocolEvent::ConfigChanged {
            capabilities: Self::capabilities(compat, has_mcp, current_mode),
            configuration,
        });
    }

    /// Access the underlying writer for custom events
    pub fn writer(&self) -> &Arc<dyn ProtocolEmitter> {
        &self.writer
    }

    /// Return the first protocol output failure without clearing it.
    ///
    /// `OutputSink` predates fallible output, so its methods cannot propagate
    /// `io::Error` without changing every engine integration. Protocol Hosts
    /// poll this sticky state at safe points and terminate the run if durable
    /// persistence or transport has failed.
    pub fn check_fatal_error(&self) -> io::Result<()> {
        let fatal_error = self
            .fatal_error
            .lock()
            .map_err(|_| io::Error::other("protocol sink fatal error lock poisoned"))?;
        match fatal_error.as_ref() {
            Some(error) => Err(io::Error::new(error.kind, error.message.clone())),
            None => Ok(()),
        }
    }

    /// Build the Host capability document used by ready, config changes, and runtime snapshots.
    pub fn capabilities(compat: &ProviderCompat, has_mcp: bool, current_mode: &str) -> Capabilities {
        Capabilities {
            tool_approval: true,
            thinking: compat.supports_thinking(),
            effort: compat.supports_effort(),
            effort_levels: compat.effort_levels().to_vec(),
            modes: vec!["plan".into(), "auto".into(), "bypass".into()],
            current_mode: current_mode.to_string(),
            mcp: has_mcp,
        }
    }

    fn diagnostic_event(msg_id: &str, message: &str) -> ProtocolEvent {
        ProtocolEvent::Info {
            msg_id: msg_id.to_owned(),
            message: message.to_owned(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn emit_stream_usage(
        &self,
        msg_id: &str,
        input_tokens: u64,
        output_tokens: u64,
        cache_creation_tokens: u64,
        cache_read_tokens: u64,
        uncached_input_tokens: Option<u64>,
        cost_usd: Option<f64>,
    ) {
        self.emit_event(&ProtocolEvent::StreamEnd {
            msg_id: msg_id.to_string(),
            usage: Some(Usage {
                input_tokens,
                output_tokens,
                uncached_input_tokens,
                cache_read_tokens: (cache_read_tokens > 0).then_some(cache_read_tokens),
                cache_write_tokens: (cache_creation_tokens > 0).then_some(cache_creation_tokens),
                cost_usd,
            }),
        });
    }

    fn emit_event(&self, event: &ProtocolEvent) {
        let Err(error) = self.writer.emit(event) else {
            return;
        };
        if let Ok(mut fatal_error) = self.fatal_error.lock()
            && fatal_error.is_none()
        {
            *fatal_error = Some(StoredIoError {
                kind: error.kind(),
                message: error.to_string(),
            });
        }
    }
}

impl OutputSink for ProtocolSink {
    fn emit_text_delta(&self, text: &str, msg_id: &str) {
        self.emit_event(&ProtocolEvent::TextDelta {
            text: text.to_string(),
            msg_id: msg_id.to_string(),
        });
    }

    fn emit_thinking(&self, text: &str, msg_id: &str) {
        self.emit_event(&ProtocolEvent::Thinking {
            text: text.to_string(),
            msg_id: msg_id.to_string(),
        });
    }

    fn emit_tool_call(&self, _tool_use_id: &str, name: &str, _input: &str) {
        // In protocol mode, tool_call is handled by tool_request/tool_running events.
        // This is a fallback for compatibility.
        self.emit_event(&ProtocolEvent::Info {
            msg_id: String::new(),
            message: format!("Tool call: {name}"),
        });
    }

    fn emit_tool_result(&self, _tool_use_id: &str, name: &str, is_error: bool, content: &str) {
        // In protocol mode, tool results are emitted via explicit ToolResult events
        // with call_id. This fallback emits an info event.
        let status = if is_error { "error" } else { "success" };
        self.emit_event(&ProtocolEvent::Info {
            msg_id: String::new(),
            message: format!("[{name} {status}] {content}"),
        });
    }

    fn emit_stream_start(&self, msg_id: &str) {
        self.emit_event(&ProtocolEvent::StreamStart {
            msg_id: msg_id.to_string(),
        });
    }

    fn emit_stream_end(
        &self,
        msg_id: &str,
        _turns: usize,
        input_tokens: u64,
        output_tokens: u64,
        cache_creation_tokens: u64,
        cache_read_tokens: u64,
    ) {
        self.emit_stream_usage(
            msg_id,
            input_tokens,
            output_tokens,
            cache_creation_tokens,
            cache_read_tokens,
            None,
            None,
        );
    }

    fn emit_stream_end_with_accounting(
        &self,
        msg_id: &str,
        _turns: usize,
        input_tokens: u64,
        output_tokens: u64,
        cache_creation_tokens: u64,
        cache_read_tokens: u64,
        uncached_input_tokens: u64,
        cost_usd: Option<f64>,
    ) {
        self.emit_stream_usage(
            msg_id,
            input_tokens,
            output_tokens,
            cache_creation_tokens,
            cache_read_tokens,
            Some(uncached_input_tokens),
            cost_usd,
        );
    }

    fn emit_error(&self, msg: &str) {
        self.emit_event(&ProtocolEvent::Error {
            msg_id: None,
            error: ErrorInfo {
                code: "engine_error".to_string(),
                message: msg.to_string(),
                retryable: false,
            },
        });
    }

    fn emit_protocol_error(&self, msg_id: &str, code: &str, message: &str, retryable: bool) {
        self.emit_event(&ProtocolEvent::Error {
            msg_id: Some(msg_id.to_owned()),
            error: ErrorInfo {
                code: code.to_owned(),
                message: message.to_owned(),
                retryable,
            },
        });
    }

    fn emit_protocol_diagnostic(&self, msg_id: &str, message: &str) {
        self.emit_event(&Self::diagnostic_event(msg_id, message));
    }

    fn emit_info(&self, msg: &str) {
        self.emit_event(&ProtocolEvent::Info {
            msg_id: String::new(),
            message: msg.to_string(),
        });
    }
}

#[cfg(test)]
#[path = "protocol_sink_test.rs"]
mod protocol_sink_test;
