use std::io::{self, BufWriter, Stdout, Write};
use std::sync::Mutex;

use serde::Serialize;

use crate::commands::DeliveryAcknowledgement;
use crate::delivery::ProtocolEnvelope;
use crate::events::ProtocolEvent;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryAckOutcome {
    Acknowledged,
    AlreadyAcknowledged,
}

/// Trait for emitting protocol events to a host.
///
/// The default implementation (`ProtocolWriter`) writes JSON Lines to stdout.
/// Backend integrations provide alternative implementations that bridge events
/// to their own event systems.
pub trait ProtocolEmitter: Send + Sync {
    fn emit(&self, event: &ProtocolEvent) -> io::Result<()>;

    fn acknowledge_delivery(&self, _acknowledgement: &DeliveryAcknowledgement) -> io::Result<DeliveryAckOutcome> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "delivery acknowledgements are unavailable",
        ))
    }
}

/// Thread-safe JSON Lines writer to stdout
pub struct ProtocolWriter {
    writer: Mutex<BufWriter<Stdout>>,
}

impl Default for ProtocolWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl ProtocolWriter {
    pub fn new() -> Self {
        Self {
            writer: Mutex::new(BufWriter::new(io::stdout())),
        }
    }

    pub fn emit_envelope(&self, envelope: &ProtocolEnvelope) -> io::Result<()> {
        self.emit_serializable(envelope)
    }

    fn emit_serializable(&self, value: &impl Serialize) -> io::Result<()> {
        let mut writer = self
            .writer
            .lock()
            .map_err(|_| io::Error::other("protocol writer lock poisoned"))?;
        serde_json::to_writer(&mut *writer, value)
            .map_err(|_| io::Error::other("failed to serialize protocol output"))?;
        writeln!(&mut *writer)?;
        writer.flush()
    }
}

impl ProtocolEmitter for ProtocolWriter {
    fn emit(&self, event: &ProtocolEvent) -> io::Result<()> {
        self.emit_serializable(event)
    }
}

#[cfg(test)]
#[path = "writer_test.rs"]
mod writer_test;
