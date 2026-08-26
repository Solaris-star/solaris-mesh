use std::io;
use std::sync::{Arc, Mutex};

use serde_json::{Value, to_value};
use solaris_config::compat::ProviderCompat;
use solaris_protocol::events::ProtocolEvent;
use solaris_protocol::writer::ProtocolEmitter;

use super::{OutputSink, ProtocolSink};

#[derive(Default)]
struct CapturingEmitter {
    values: Mutex<Vec<Value>>,
}

struct FailingEmitter;

impl ProtocolEmitter for CapturingEmitter {
    fn emit(&self, event: &ProtocolEvent) -> io::Result<()> {
        let value = to_value(event).map_err(io::Error::other)?;
        self.values.lock().unwrap().push(value);
        Ok(())
    }
}

impl ProtocolEmitter for FailingEmitter {
    fn emit(&self, _event: &ProtocolEvent) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::WriteZero,
            "injected protocol output failure",
        ))
    }
}

#[test]
fn scoped_diagnostic_is_non_terminal_info_with_turn_identity() {
    let event = ProtocolSink::diagnostic_event("turn-42", "Safe diagnostic");

    let value = to_value(event).expect("diagnostic event should serialize");
    assert_eq!(value["type"], "info");
    assert_eq!(value["msg_id"], "turn-42");
    assert_eq!(value["message"], "Safe diagnostic");
    assert!(value.get("error").is_none());
}

#[test]
fn host_capabilities_are_reusable_by_runtime_snapshots() {
    let capabilities = ProtocolSink::capabilities(&ProviderCompat::default(), true, "auto");
    let value = to_value(capabilities).expect("capabilities should serialize");

    assert_eq!(value["current_mode"], "auto");
    assert_eq!(value["mcp"], true);
    assert_eq!(value["tool_approval"], true);
}

#[test]
fn unknown_provider_cost_is_omitted_from_stream_usage() {
    let emitter = Arc::new(CapturingEmitter::default());
    let sink = ProtocolSink::new(emitter.clone());

    sink.emit_stream_end_with_accounting("message-1", 1, 20, 5, 7, 8, 20, None);

    let values = emitter.values.lock().unwrap();
    assert_eq!(values.len(), 1);
    assert_eq!(values[0]["usage"]["uncached_input_tokens"], 20);
    assert!(values[0]["usage"].get("cost_usd").is_none());
}

#[test]
fn output_failure_remains_visible_after_void_output_sink_call() {
    let sink = ProtocolSink::new(Arc::new(FailingEmitter));

    sink.emit_info("this event cannot be delivered");

    let first = sink.check_fatal_error().unwrap_err();
    let second = sink.check_fatal_error().unwrap_err();
    assert_eq!(first.kind(), io::ErrorKind::WriteZero);
    assert_eq!(first.to_string(), "injected protocol output failure");
    assert_eq!(second.kind(), first.kind(), "the fatal error must remain sticky");
}
