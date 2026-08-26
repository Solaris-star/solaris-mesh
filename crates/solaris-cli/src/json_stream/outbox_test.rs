use std::io;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

use solaris_agent::session::{HostOutboxAckOutcome, HostOutboxDelivery, HostOutboxError};
use solaris_protocol::commands::DeliveryAcknowledgement;
use solaris_protocol::delivery::{ProtocolEnvelope, canonical_event_digest};
use solaris_protocol::events::ProtocolEvent;
use solaris_protocol::writer::{DeliveryAckOutcome, ProtocolEmitter};

use super::*;

#[derive(Default)]
struct MemoryOutbox {
    pending: Mutex<Vec<HostOutboxDelivery>>,
    next_sequence: Mutex<u64>,
}

impl OutboxBackend for MemoryOutbox {
    fn enqueue(&self, msg_id: &str, payload: &[u8]) -> Result<HostOutboxDelivery, HostOutboxError> {
        let mut sequence = self.next_sequence.lock().unwrap();
        let event = serde_json::from_slice::<serde_json::Value>(payload)
            .ok()
            .and_then(|value| value.as_object().cloned())
            .ok_or(HostOutboxError::CorruptDelivery)?;
        let delivery = HostOutboxDelivery {
            delivery_id: format!("delivery-{sequence}"),
            msg_id: msg_id.to_owned(),
            run_epoch: 3,
            sequence: *sequence,
            digest: canonical_event_digest(&event),
            payload: payload.to_vec(),
        };
        *sequence += 1;
        self.pending.lock().unwrap().push(delivery.clone());
        Ok(delivery)
    }

    fn pending(&self) -> Result<Vec<HostOutboxDelivery>, HostOutboxError> {
        Ok(self.pending.lock().unwrap().clone())
    }

    fn acknowledge(&self, acknowledgement: &DeliveryAcknowledgement) -> Result<HostOutboxAckOutcome, HostOutboxError> {
        let mut pending = self.pending.lock().unwrap();
        let Some(index) = pending
            .iter()
            .position(|delivery| delivery.delivery_id == acknowledgement.delivery_id)
        else {
            return Err(HostOutboxError::UnknownDelivery);
        };
        if pending[index].run_epoch != acknowledgement.run_epoch {
            return Err(HostOutboxError::StaleEpoch);
        }
        if pending[index].digest != acknowledgement.digest {
            return Err(HostOutboxError::DigestMismatch);
        }
        pending.remove(index);
        Ok(HostOutboxAckOutcome::Acknowledged)
    }
}

#[derive(Default)]
struct RecordingTransport {
    fail_delivery: Mutex<bool>,
    envelopes: Mutex<Vec<ProtocolEnvelope>>,
    plain_events: Mutex<usize>,
    order: Mutex<Vec<&'static str>>,
}

struct RejectingOutbox;

struct FailingOutbox(HostOutboxError);

impl OutboxBackend for RejectingOutbox {
    fn enqueue(&self, _msg_id: &str, _payload: &[u8]) -> Result<HostOutboxDelivery, HostOutboxError> {
        Err(HostOutboxError::StorageUnavailable)
    }

    fn pending(&self) -> Result<Vec<HostOutboxDelivery>, HostOutboxError> {
        Err(HostOutboxError::StorageUnavailable)
    }

    fn acknowledge(&self, _acknowledgement: &DeliveryAcknowledgement) -> Result<HostOutboxAckOutcome, HostOutboxError> {
        Err(HostOutboxError::DigestMismatch)
    }
}

impl OutboxBackend for FailingOutbox {
    fn enqueue(&self, _msg_id: &str, _payload: &[u8]) -> Result<HostOutboxDelivery, HostOutboxError> {
        Err(self.0)
    }

    fn pending(&self) -> Result<Vec<HostOutboxDelivery>, HostOutboxError> {
        Err(self.0)
    }

    fn acknowledge(&self, _acknowledgement: &DeliveryAcknowledgement) -> Result<HostOutboxAckOutcome, HostOutboxError> {
        Err(self.0)
    }
}

struct BlockingFirstTransport {
    envelopes: Mutex<Vec<ProtocolEnvelope>>,
    first_started: mpsc::SyncSender<()>,
    release_first: Mutex<mpsc::Receiver<()>>,
}

struct BlockingBootstrapTransport {
    envelopes: Mutex<Vec<ProtocolEnvelope>>,
    bootstrap_started: mpsc::SyncSender<()>,
    release_bootstrap: Mutex<mpsc::Receiver<()>>,
}

impl HostDeliveryTransport for BlockingFirstTransport {
    fn emit_event(&self, _event: &ProtocolEvent) -> io::Result<()> {
        Ok(())
    }

    fn emit_envelope(&self, envelope: &ProtocolEnvelope) -> io::Result<()> {
        if envelope.delivery.sequence == 0 {
            self.first_started
                .send(())
                .map_err(|_| io::Error::other("first delivery observer closed"))?;
            self.release_first
                .lock()
                .unwrap()
                .recv()
                .map_err(|_| io::Error::other("first delivery release closed"))?;
        }
        self.envelopes.lock().unwrap().push(envelope.clone());
        Ok(())
    }
}

impl HostDeliveryTransport for BlockingBootstrapTransport {
    fn emit_event(&self, _event: &ProtocolEvent) -> io::Result<()> {
        self.bootstrap_started
            .send(())
            .map_err(|_| io::Error::other("bootstrap observer closed"))?;
        self.release_bootstrap
            .lock()
            .unwrap()
            .recv()
            .map_err(|_| io::Error::other("bootstrap release closed"))
    }

    fn emit_envelope(&self, envelope: &ProtocolEnvelope) -> io::Result<()> {
        self.envelopes.lock().unwrap().push(envelope.clone());
        Ok(())
    }
}

impl HostDeliveryTransport for RecordingTransport {
    fn emit_event(&self, _event: &ProtocolEvent) -> io::Result<()> {
        *self.plain_events.lock().unwrap() += 1;
        self.order.lock().unwrap().push("plain");
        Ok(())
    }

    fn emit_envelope(&self, envelope: &ProtocolEnvelope) -> io::Result<()> {
        if *self.fail_delivery.lock().unwrap() {
            return Err(io::Error::other("injected transport failure"));
        }
        self.order.lock().unwrap().push("envelope");
        self.envelopes.lock().unwrap().push(envelope.clone());
        Ok(())
    }
}

fn ready_event() -> ProtocolEvent {
    ProtocolEvent::Ready {
        version: "test".to_owned(),
        session_id: Some("session-1".to_owned()),
        resumed: true,
        capabilities: solaris_protocol::events::Capabilities {
            tool_approval: true,
            thinking: false,
            effort: false,
            effort_levels: Vec::new(),
            modes: vec!["plan".to_owned(), "auto".to_owned(), "bypass".to_owned()],
            current_mode: "auto".to_owned(),
            mcp: false,
        },
    }
}

#[test]
fn persist_happens_before_send_and_restart_replays_the_same_delivery_id() {
    let backend = Arc::new(MemoryOutbox::default());
    let first_transport = Arc::new(RecordingTransport::default());
    *first_transport.fail_delivery.lock().unwrap() = true;
    let first = DurableHostEmitter::with_parts(first_transport, Arc::clone(&backend) as Arc<dyn OutboxBackend>);
    let event = ProtocolEvent::TextDelta {
        text: "hello".to_owned(),
        msg_id: "message-1".to_owned(),
    };

    assert!(first.emit(&event).is_err());
    let persisted = backend.pending().unwrap();
    assert_eq!(persisted.len(), 1);

    let second_transport = Arc::new(RecordingTransport::default());
    let second = DurableHostEmitter::with_parts(
        Arc::clone(&second_transport) as Arc<dyn HostDeliveryTransport>,
        Arc::clone(&backend) as Arc<dyn OutboxBackend>,
    );
    second.replay_pending().unwrap();

    let envelopes = second_transport.envelopes.lock().unwrap();
    assert_eq!(envelopes.len(), 1);
    assert_eq!(envelopes[0].delivery.delivery_id, persisted[0].delivery_id);
    assert_eq!(envelopes[0].delivery.msg_id, "message-1");
    assert_eq!(envelopes[0].delivery.run_epoch, 3);
}

#[test]
fn a_successfully_sent_but_unacknowledged_event_is_replayed_with_the_same_identity() {
    let backend = Arc::new(MemoryOutbox::default());
    let first_transport = Arc::new(RecordingTransport::default());
    let first = DurableHostEmitter::with_parts(
        Arc::clone(&first_transport) as Arc<dyn HostDeliveryTransport>,
        Arc::clone(&backend) as Arc<dyn OutboxBackend>,
    );
    first
        .emit(&ProtocolEvent::Info {
            msg_id: "message-2".to_owned(),
            message: "sent before crash".to_owned(),
        })
        .unwrap();
    let first_delivery_id = first_transport.envelopes.lock().unwrap()[0]
        .delivery
        .delivery_id
        .clone();

    let restarted_transport = Arc::new(RecordingTransport::default());
    let restarted = DurableHostEmitter::with_parts(
        Arc::clone(&restarted_transport) as Arc<dyn HostDeliveryTransport>,
        Arc::clone(&backend) as Arc<dyn OutboxBackend>,
    );
    restarted.replay_pending().unwrap();

    assert_eq!(
        restarted_transport.envelopes.lock().unwrap()[0].delivery.delivery_id,
        first_delivery_id
    );
}

#[test]
fn pre_session_diagnostics_are_plain_but_visible_events_are_enveloped_after_activation() {
    let transport = Arc::new(RecordingTransport::default());
    let emitter = DurableHostEmitter::new_for_test(Arc::clone(&transport) as Arc<dyn HostDeliveryTransport>);
    emitter.emit(&ProtocolEvent::Pong).unwrap();
    assert_eq!(*transport.plain_events.lock().unwrap(), 1);

    emitter
        .activate_and_start_session_backend(Arc::new(MemoryOutbox::default()), &ready_event())
        .unwrap();
    for event in [
        ProtocolEvent::Thinking {
            text: "thought".to_owned(),
            msg_id: "turn-1".to_owned(),
        },
        ProtocolEvent::Info {
            msg_id: "turn-1".to_owned(),
            message: "info".to_owned(),
        },
        ProtocolEvent::StreamEnd {
            msg_id: "turn-1".to_owned(),
            usage: None,
        },
    ] {
        emitter.emit(&event).unwrap();
    }
    let envelopes = transport.envelopes.lock().unwrap();
    assert_eq!(*transport.plain_events.lock().unwrap(), 2);
    assert_eq!(envelopes.len(), 4);
    assert_eq!(envelopes[0].delivery.msg_id, "session-1");
    assert!(
        envelopes[1..]
            .iter()
            .all(|envelope| envelope.delivery.msg_id == "turn-1")
    );
}

#[test]
fn visible_ready_is_enveloped_after_pending_deliveries_in_committed_order() {
    let backend = Arc::new(MemoryOutbox::default());
    let previous = backend
        .enqueue(
            "previous-turn",
            &serde_json::to_vec(&ProtocolEvent::Info {
                msg_id: "previous-turn".to_owned(),
                message: "pending before restart".to_owned(),
            })
            .unwrap(),
        )
        .unwrap();
    let transport = Arc::new(RecordingTransport::default());
    let emitter = DurableHostEmitter::new_for_test(Arc::clone(&transport) as Arc<dyn HostDeliveryTransport>);
    let ready = ready_event();
    emitter
        .activate_and_start_session_backend(Arc::clone(&backend) as Arc<dyn OutboxBackend>, &ready)
        .unwrap();

    let envelopes = transport.envelopes.lock().unwrap();
    assert_eq!(*transport.plain_events.lock().unwrap(), 1);
    assert_eq!(*transport.order.lock().unwrap(), ["plain", "envelope", "envelope"]);
    assert_eq!(envelopes.len(), 2);
    assert_eq!(envelopes[0].delivery.delivery_id, previous.delivery_id);
    assert_eq!(envelopes[0].delivery.sequence, 0);
    assert_eq!(envelopes[1].delivery.msg_id, "session-1");
    assert_eq!(envelopes[1].delivery.sequence, 1);
}

#[test]
fn session_start_rejects_non_ready_events() {
    let emitter = DurableHostEmitter::new_for_test(Arc::new(RecordingTransport::default()));

    let error = emitter
        .activate_and_start_session_backend(Arc::new(MemoryOutbox::default()), &ProtocolEvent::Pong)
        .unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(emitter.check_fatal_error().is_ok());
}

#[test]
fn concurrent_visible_event_cannot_overtake_atomic_activation_and_startup_replay() {
    let (bootstrap_started_tx, bootstrap_started_rx) = mpsc::sync_channel(1);
    let (release_bootstrap_tx, release_bootstrap_rx) = mpsc::sync_channel(1);
    let transport = Arc::new(BlockingBootstrapTransport {
        envelopes: Mutex::new(Vec::new()),
        bootstrap_started: bootstrap_started_tx,
        release_bootstrap: Mutex::new(release_bootstrap_rx),
    });
    let emitter = Arc::new(DurableHostEmitter::new_for_test(
        Arc::clone(&transport) as Arc<dyn HostDeliveryTransport>
    ));
    let backend = Arc::new(MemoryOutbox::default());

    let startup = {
        let emitter = Arc::clone(&emitter);
        let backend = Arc::clone(&backend);
        thread::spawn(move || {
            emitter.activate_and_start_session_backend(backend as Arc<dyn OutboxBackend>, &ready_event())
        })
    };
    bootstrap_started_rx.recv_timeout(Duration::from_secs(2)).unwrap();

    let (event_done_tx, event_done_rx) = mpsc::sync_channel(1);
    let concurrent = {
        let emitter = Arc::clone(&emitter);
        thread::spawn(move || {
            let result = emitter.emit(&ProtocolEvent::Info {
                msg_id: "concurrent".to_owned(),
                message: "must follow Ready".to_owned(),
            });
            event_done_tx.send(()).unwrap();
            result
        })
    };
    let event_overtook_startup = event_done_rx.recv_timeout(Duration::from_millis(300)).is_ok();

    release_bootstrap_tx.send(()).unwrap();
    startup.join().unwrap().unwrap();
    concurrent.join().unwrap().unwrap();

    assert!(!event_overtook_startup);
    let sequences = transport
        .envelopes
        .lock()
        .unwrap()
        .iter()
        .map(|envelope| envelope.delivery.sequence)
        .collect::<Vec<_>>();
    assert_eq!(sequences, [0, 1]);
}

#[test]
fn acknowledgement_is_idempotently_delegated_through_the_emitter() {
    let backend = Arc::new(MemoryOutbox::default());
    let emitter = DurableHostEmitter::with_parts(
        Arc::new(RecordingTransport::default()),
        Arc::clone(&backend) as Arc<dyn OutboxBackend>,
    );
    emitter
        .emit(&ProtocolEvent::Info {
            msg_id: "message".to_owned(),
            message: "safe".to_owned(),
        })
        .unwrap();
    let delivery = backend.pending().unwrap().remove(0);
    let outcome = emitter
        .acknowledge_delivery(&DeliveryAcknowledgement {
            session_id: "session-1".to_owned(),
            run_epoch: delivery.run_epoch,
            delivery_id: delivery.delivery_id,
            digest: delivery.digest,
        })
        .unwrap();

    assert_eq!(outcome, DeliveryAckOutcome::Acknowledged);
    assert!(backend.pending().unwrap().is_empty());
}

#[test]
fn host_recomputes_digest_from_received_top_level_map_before_acknowledging() {
    let backend = Arc::new(MemoryOutbox::default());
    let transport = Arc::new(RecordingTransport::default());
    let emitter = DurableHostEmitter::with_parts(
        Arc::clone(&transport) as Arc<dyn HostDeliveryTransport>,
        Arc::clone(&backend) as Arc<dyn OutboxBackend>,
    );
    emitter
        .emit(&ProtocolEvent::Info {
            msg_id: "message-studio".to_owned(),
            message: "visible output".to_owned(),
        })
        .unwrap();
    let envelope = transport.envelopes.lock().unwrap()[0].clone();
    let received = serde_json::to_value(&envelope).unwrap();
    let received = received.as_object().unwrap();
    let recomputed = canonical_event_digest(received);

    assert_eq!(recomputed, envelope.delivery.digest);
    assert_eq!(
        emitter
            .acknowledge_delivery(&DeliveryAcknowledgement {
                session_id: "session-1".to_owned(),
                run_epoch: envelope.delivery.run_epoch,
                delivery_id: envelope.delivery.delivery_id,
                digest: recomputed,
            })
            .unwrap(),
        DeliveryAckOutcome::Acknowledged
    );
    assert!(backend.pending().unwrap().is_empty());
}

#[test]
fn delivery_failures_do_not_copy_event_or_acknowledgement_content_into_errors() {
    let emitter = DurableHostEmitter::with_parts(Arc::new(RecordingTransport::default()), Arc::new(RejectingOutbox));
    let event_error = emitter
        .emit(&ProtocolEvent::Info {
            msg_id: "message".to_owned(),
            message: "event-content-must-stay-private".to_owned(),
        })
        .unwrap_err();
    assert_eq!(event_error.to_string(), "durable host delivery operation failed");

    let acknowledgement_error = emitter
        .acknowledge_delivery(&DeliveryAcknowledgement {
            session_id: "session".to_owned(),
            run_epoch: 3,
            delivery_id: "ack-content-must-stay-private".to_owned(),
            digest: "digest-content-must-stay-private".to_owned(),
        })
        .unwrap_err();
    assert_eq!(
        acknowledgement_error.to_string(),
        "durable host delivery operation failed"
    );
}

#[test]
fn transport_storage_lease_and_corruption_failures_remain_fatal() {
    let transport = Arc::new(RecordingTransport::default());
    *transport.fail_delivery.lock().unwrap() = true;
    let transport_failure = DurableHostEmitter::with_parts(
        Arc::clone(&transport) as Arc<dyn HostDeliveryTransport>,
        Arc::new(MemoryOutbox::default()),
    );
    transport_failure.emit(&ProtocolEvent::Pong).unwrap_err();
    assert_eq!(
        transport_failure.check_fatal_error().unwrap_err().to_string(),
        "injected transport failure"
    );

    for (failure, expected_kind) in [
        (HostOutboxError::StorageUnavailable, io::ErrorKind::Other),
        (HostOutboxError::StaleEpoch, io::ErrorKind::InvalidData),
        (HostOutboxError::CorruptDelivery, io::ErrorKind::Other),
    ] {
        let emitter = DurableHostEmitter::with_parts(
            Arc::new(RecordingTransport::default()),
            Arc::new(FailingOutbox(failure)),
        );
        emitter.emit(&ProtocolEvent::Pong).unwrap_err();
        let fatal = emitter.check_fatal_error().unwrap_err();
        assert_eq!(fatal.kind(), expected_kind);
        assert_eq!(fatal.to_string(), "durable host delivery operation failed");
    }
}

#[test]
fn startup_outbox_failure_is_sticky_after_activation() {
    let emitter = DurableHostEmitter::new_for_test(Arc::new(RecordingTransport::default()));

    emitter
        .activate_and_start_session_backend(Arc::new(RejectingOutbox), &ready_event())
        .unwrap_err();

    let fatal = emitter.check_fatal_error().unwrap_err();
    assert_eq!(fatal.kind(), io::ErrorKind::Other);
    assert_eq!(fatal.to_string(), "durable host delivery operation failed");
}

#[test]
fn concurrent_emission_preserves_the_committed_delivery_sequence() {
    let (first_started_tx, first_started_rx) = mpsc::sync_channel(1);
    let (release_first_tx, release_first_rx) = mpsc::sync_channel(1);
    let transport = Arc::new(BlockingFirstTransport {
        envelopes: Mutex::new(Vec::new()),
        first_started: first_started_tx,
        release_first: Mutex::new(release_first_rx),
    });
    let emitter = Arc::new(DurableHostEmitter::with_parts(
        Arc::clone(&transport) as Arc<dyn HostDeliveryTransport>,
        Arc::new(MemoryOutbox::default()),
    ));

    let first = {
        let emitter = Arc::clone(&emitter);
        thread::spawn(move || {
            emitter.emit(&ProtocolEvent::Info {
                msg_id: "first".to_owned(),
                message: "first".to_owned(),
            })
        })
    };
    first_started_rx.recv_timeout(Duration::from_secs(2)).unwrap();

    let (second_started_tx, second_started_rx) = mpsc::sync_channel(1);
    let (second_done_tx, second_done_rx) = mpsc::sync_channel(1);
    let second = {
        let emitter = Arc::clone(&emitter);
        thread::spawn(move || {
            second_started_tx.send(()).unwrap();
            let result = emitter.emit(&ProtocolEvent::Info {
                msg_id: "second".to_owned(),
                message: "second".to_owned(),
            });
            second_done_tx.send(()).unwrap();
            result
        })
    };
    second_started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let second_finished_before_release = second_done_rx.recv_timeout(Duration::from_millis(300)).is_ok();

    release_first_tx.send(()).unwrap();
    first.join().unwrap().unwrap();
    second.join().unwrap().unwrap();

    assert!(!second_finished_before_release);
    let sequences: Vec<_> = transport
        .envelopes
        .lock()
        .unwrap()
        .iter()
        .map(|envelope| envelope.delivery.sequence)
        .collect();
    assert_eq!(sequences, [0, 1]);
}
