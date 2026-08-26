use std::io;
use std::sync::{Arc, Mutex, RwLock};

use solaris_agent::session::{HostOutbox, HostOutboxAckOutcome, HostOutboxDelivery, HostOutboxError};
use solaris_protocol::commands::DeliveryAcknowledgement;
use solaris_protocol::delivery::ProtocolEnvelope;
use solaris_protocol::events::{ErrorInfo, ProtocolEvent};
use solaris_protocol::writer::{DeliveryAckOutcome, ProtocolEmitter, ProtocolWriter};

trait OutboxBackend: Send + Sync {
    fn enqueue(&self, msg_id: &str, payload: &[u8]) -> Result<HostOutboxDelivery, HostOutboxError>;
    fn pending(&self) -> Result<Vec<HostOutboxDelivery>, HostOutboxError>;
    fn acknowledge(&self, acknowledgement: &DeliveryAcknowledgement) -> Result<HostOutboxAckOutcome, HostOutboxError>;
}

impl OutboxBackend for HostOutbox {
    fn enqueue(&self, msg_id: &str, payload: &[u8]) -> Result<HostOutboxDelivery, HostOutboxError> {
        self.enqueue(msg_id, payload)
    }

    fn pending(&self) -> Result<Vec<HostOutboxDelivery>, HostOutboxError> {
        self.pending()
    }

    fn acknowledge(&self, acknowledgement: &DeliveryAcknowledgement) -> Result<HostOutboxAckOutcome, HostOutboxError> {
        self.acknowledge(acknowledgement)
    }
}

trait HostDeliveryTransport: Send + Sync {
    fn emit_event(&self, event: &ProtocolEvent) -> io::Result<()>;
    fn emit_envelope(&self, envelope: &ProtocolEnvelope) -> io::Result<()>;
}

struct StdoutHostDeliveryTransport {
    writer: Arc<ProtocolWriter>,
}

impl HostDeliveryTransport for StdoutHostDeliveryTransport {
    fn emit_event(&self, event: &ProtocolEvent) -> io::Result<()> {
        self.writer.emit(event)
    }

    fn emit_envelope(&self, envelope: &ProtocolEnvelope) -> io::Result<()> {
        self.writer.emit_envelope(envelope)
    }
}

pub(super) struct DurableHostEmitter {
    transport: Arc<dyn HostDeliveryTransport>,
    active: RwLock<Option<Arc<dyn OutboxBackend>>>,
    delivery: Mutex<()>,
    fatal_error: Mutex<Option<StoredIoError>>,
}

#[derive(Clone)]
struct StoredIoError {
    kind: io::ErrorKind,
    message: String,
}

impl DurableHostEmitter {
    pub(super) fn new(writer: Arc<ProtocolWriter>) -> Self {
        Self {
            transport: Arc::new(StdoutHostDeliveryTransport { writer }),
            active: RwLock::new(None),
            delivery: Mutex::new(()),
            fatal_error: Mutex::new(None),
        }
    }

    /// Atomically activate durable delivery and publish the startup sequence.
    ///
    /// The legacy transport-only `Ready` must remain first for existing Hosts.
    /// Pending durable deliveries follow in committed order, then the new
    /// durable `Ready`. Holding `delivery` across activation prevents a
    /// concurrent visible event from observing an active outbox and overtaking
    /// any part of this sequence.
    pub(super) fn activate_and_start_session(&self, outbox: HostOutbox, event: &ProtocolEvent) -> io::Result<()> {
        self.activate_and_start_session_backend(Arc::new(outbox), event)
    }

    fn activate_and_start_session_backend(
        &self,
        outbox: Arc<dyn OutboxBackend>,
        event: &ProtocolEvent,
    ) -> io::Result<()> {
        if !matches!(event, ProtocolEvent::Ready { .. }) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "bootstrap host event must be Ready",
            ));
        }
        let result = self.activate_and_start_session_inner(outbox, event);
        self.remember_failure(&result);
        result
    }

    fn activate_and_start_session_inner(
        &self,
        outbox: Arc<dyn OutboxBackend>,
        event: &ProtocolEvent,
    ) -> io::Result<()> {
        let _delivery = self
            .delivery
            .lock()
            .map_err(|_| io::Error::other("durable host delivery lock poisoned"))?;
        let mut active = self
            .active
            .write()
            .map_err(|_| io::Error::other("durable host emitter lock poisoned"))?;
        if active.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "durable host emitter is already active",
            ));
        }
        *active = Some(Arc::clone(&outbox));
        drop(active);

        self.transport.emit_event(event)?;
        for pending in outbox.pending().map_err(map_outbox_error)? {
            self.emit_delivery(&pending)?;
        }
        let delivery = enqueue_event(outbox.as_ref(), event)?;
        self.emit_delivery(&delivery)
    }

    #[cfg(test)]
    pub(super) fn replay_pending(&self) -> io::Result<()> {
        let _delivery = self
            .delivery
            .lock()
            .map_err(|_| io::Error::other("durable host delivery lock poisoned"))?;
        let outbox = self.active_outbox()?;
        let pending = outbox.pending().map_err(map_outbox_error)?;
        for delivery in pending {
            self.emit_delivery(&delivery)?;
        }
        Ok(())
    }

    fn active_outbox(&self) -> io::Result<Arc<dyn OutboxBackend>> {
        self.active
            .read()
            .map_err(|_| io::Error::other("durable host emitter lock poisoned"))?
            .clone()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "durable host emitter is not active"))
    }

    fn emit_delivery(&self, delivery: &HostOutboxDelivery) -> io::Result<()> {
        let envelope = ProtocolEnvelope::from_event_payload(&delivery.payload, delivery.metadata())
            .map_err(|_| io::Error::other("stored host delivery is invalid"))?;
        self.transport.emit_envelope(&envelope)
    }

    /// Return the first output failure without clearing it.
    ///
    /// Output methods are used through interfaces that intentionally return
    /// `()`. Keeping the first failure here lets the JSON-stream main loop stop
    /// at a safe point instead of silently continuing after durable storage or
    /// Host transport has failed.
    pub(super) fn check_fatal_error(&self) -> io::Result<()> {
        let fatal_error = self
            .fatal_error
            .lock()
            .map_err(|_| io::Error::other("durable host fatal error lock poisoned"))?;
        match fatal_error.as_ref() {
            Some(error) => Err(io::Error::new(error.kind, error.message.clone())),
            None => Ok(()),
        }
    }

    fn remember_failure(&self, result: &io::Result<()>) {
        let Err(error) = result else {
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

    #[cfg(test)]
    fn new_for_test(transport: Arc<dyn HostDeliveryTransport>) -> Self {
        Self {
            transport,
            active: RwLock::new(None),
            delivery: Mutex::new(()),
            fatal_error: Mutex::new(None),
        }
    }

    #[cfg(test)]
    fn with_parts(transport: Arc<dyn HostDeliveryTransport>, outbox: Arc<dyn OutboxBackend>) -> Self {
        Self {
            transport,
            active: RwLock::new(Some(outbox)),
            delivery: Mutex::new(()),
            fatal_error: Mutex::new(None),
        }
    }
}

impl ProtocolEmitter for DurableHostEmitter {
    fn emit(&self, event: &ProtocolEvent) -> io::Result<()> {
        let result = self.emit_inner(event);
        self.remember_failure(&result);
        result
    }

    fn acknowledge_delivery(&self, acknowledgement: &DeliveryAcknowledgement) -> io::Result<DeliveryAckOutcome> {
        let outcome = self
            .active_outbox()?
            .acknowledge(acknowledgement)
            .map_err(map_outbox_error)?;
        Ok(match outcome {
            HostOutboxAckOutcome::Acknowledged => DeliveryAckOutcome::Acknowledged,
            HostOutboxAckOutcome::AlreadyAcknowledged => DeliveryAckOutcome::AlreadyAcknowledged,
        })
    }
}

impl DurableHostEmitter {
    fn emit_inner(&self, event: &ProtocolEvent) -> io::Result<()> {
        // Take the delivery gate before observing activation. This pairs with
        // `activate_and_start_session_inner`, so an event either completes as a
        // pre-session transport event or waits for the full startup sequence.
        let _delivery = self
            .delivery
            .lock()
            .map_err(|_| io::Error::other("durable host delivery lock poisoned"))?;
        let outbox = self
            .active
            .read()
            .map_err(|_| io::Error::other("durable host emitter lock poisoned"))?
            .clone();
        let Some(outbox) = outbox else {
            return self.transport.emit_event(event);
        };
        let delivery = enqueue_event(outbox.as_ref(), event)?;
        self.emit_delivery(&delivery)
    }
}

fn enqueue_event(outbox: &dyn OutboxBackend, event: &ProtocolEvent) -> io::Result<HostOutboxDelivery> {
    let payload = serde_json::to_vec(event).map_err(|_| io::Error::other("failed to serialize host event"))?;
    outbox
        .enqueue(event.delivery_msg_id(), &payload)
        .map_err(map_outbox_error)
}

fn map_outbox_error(error: HostOutboxError) -> io::Error {
    let kind = match error {
        HostOutboxError::StaleEpoch => io::ErrorKind::InvalidData,
        HostOutboxError::InvalidAcknowledgement
        | HostOutboxError::UnknownDelivery
        | HostOutboxError::DigestMismatch => io::ErrorKind::PermissionDenied,
        HostOutboxError::StorageUnavailable
        | HostOutboxError::SessionRunMismatch
        | HostOutboxError::CorruptDelivery => io::ErrorKind::Other,
    };
    io::Error::new(kind, "durable host delivery operation failed")
}

pub(super) fn handle_acknowledgement(writer: &dyn ProtocolEmitter, acknowledgement: &DeliveryAcknowledgement) {
    if writer.acknowledge_delivery(acknowledgement).is_err() {
        let _ = writer.emit(&ProtocolEvent::Error {
            msg_id: None,
            error: ErrorInfo {
                code: "delivery_ack_rejected".to_owned(),
                message: "Delivery acknowledgement was rejected".to_owned(),
                retryable: false,
            },
        });
    }
}

#[cfg(test)]
#[path = "outbox_test.rs"]
mod outbox_test;
