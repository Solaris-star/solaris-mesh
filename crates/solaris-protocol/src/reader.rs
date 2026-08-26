use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;

use crate::commands::ProtocolCommand;
use crate::events::{ErrorInfo, ProtocolEvent};

#[derive(Debug)]
pub enum ProtocolInput {
    Command(Box<ProtocolCommand>),
    Invalid(ProtocolInputError),
}

#[derive(Debug)]
pub struct ProtocolInputError {
    correlation_id: Option<String>,
}

impl ProtocolInputError {
    pub fn into_event(self) -> ProtocolEvent {
        ProtocolEvent::Error {
            msg_id: self.correlation_id,
            error: ErrorInfo {
                code: "protocol_error".to_owned(),
                message: "Invalid protocol command".to_owned(),
                retryable: false,
            },
        }
    }
}

fn parse_protocol_input(line: &str) -> ProtocolInput {
    match serde_json::from_str::<ProtocolCommand>(line) {
        Ok(command) => ProtocolInput::Command(Box::new(command)),
        Err(_) => ProtocolInput::Invalid(ProtocolInputError {
            correlation_id: safe_correlation_id(line),
        }),
    }
}

fn safe_correlation_id(line: &str) -> Option<String> {
    let value = serde_json::from_str::<serde_json::Value>(line).ok()?;
    ["request_id", "msg_id"]
        .into_iter()
        .filter_map(|field| value.get(field).and_then(serde_json::Value::as_str))
        .find(|candidate| {
            !candidate.is_empty()
                && candidate.len() <= 128
                && candidate
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
        })
        .map(str::to_owned)
}

#[cfg(test)]
#[path = "reader_test.rs"]
mod reader_test;

/// Reads JSON Lines from stdin in a background task.
/// Returns a channel receiver for parsed commands.
pub fn spawn_stdin_reader() -> mpsc::UnboundedReceiver<ProtocolInput> {
    let (tx, rx) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        let stdin = tokio::io::stdin();
        let mut reader = BufReader::new(stdin);
        let mut line = String::new();

        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => break, // EOF - client closed stdin
                Ok(_) => {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    if tx.send(parse_protocol_input(trimmed)).is_err() {
                        break;
                    }
                }
                Err(e) => {
                    tracing::debug!(target: "solaris_protocol", error = %e, "stdin read error");
                    break;
                }
            }
        }
    });

    rx
}
