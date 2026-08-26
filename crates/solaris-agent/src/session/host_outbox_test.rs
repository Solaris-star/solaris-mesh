use solaris_protocol::commands::DeliveryAcknowledgement;

use super::*;
use crate::session::SessionManager;

#[test]
fn public_outbox_maps_forged_acknowledgements_to_safe_errors() {
    let directory = tempfile::tempdir().unwrap();
    let manager = SessionManager::new(directory.path().to_path_buf(), 20);
    let session = manager
        .create_active_session("provider", "model", "/workspace", Some("public-outbox"), "public-run")
        .unwrap();
    let outbox = manager.open_host_outbox(&session.id, "public-run").unwrap();
    let delivery = outbox
        .enqueue("message", br#"{"type":"info","msg_id":"message"}"#)
        .unwrap();

    let error = outbox
        .acknowledge(&DeliveryAcknowledgement {
            session_id: "another-session".to_owned(),
            run_epoch: delivery.run_epoch,
            delivery_id: delivery.delivery_id,
            digest: delivery.digest,
        })
        .unwrap_err();

    assert_eq!(error, HostOutboxError::InvalidAcknowledgement);
    assert_eq!(error.to_string(), "host outbox acknowledgement is invalid");
}
