use std::sync::Mutex;

use super::OutputSink;

#[derive(Default)]
struct DefaultDiagnosticSink {
    infos: Mutex<Vec<String>>,
}

impl OutputSink for DefaultDiagnosticSink {
    fn emit_text_delta(&self, _: &str, _: &str) {}
    fn emit_thinking(&self, _: &str, _: &str) {}
    fn emit_tool_call(&self, _: &str, _: &str, _: &str) {}
    fn emit_tool_result(&self, _: &str, _: &str, _: bool, _: &str) {}
    fn emit_stream_start(&self, _: &str) {}
    fn emit_stream_end(&self, _: &str, _: usize, _: u64, _: u64, _: u64, _: u64) {}
    fn emit_error(&self, _: &str) {}

    fn emit_info(&self, message: &str) {
        self.infos.lock().unwrap().push(message.to_owned());
    }
}

#[test]
fn protocol_diagnostic_defaults_to_non_protocol_info() {
    let sink = DefaultDiagnosticSink::default();

    sink.emit_protocol_diagnostic("turn-ignored", "Safe diagnostic");

    assert_eq!(sink.infos.lock().unwrap().as_slice(), ["Safe diagnostic"]);
}
