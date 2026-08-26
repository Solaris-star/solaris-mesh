use solaris_protocol::commands::DeliveryAcknowledgement;
use solaris_protocol::delivery::DeliveryMetadata;
use thiserror::Error;

use super::store::{SessionStore, SessionStoreError, StoredHostAckOutcome, StoredHostDelivery, StoredHostOutbox};

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum HostOutboxError {
    #[error("host outbox storage is unavailable")]
    StorageUnavailable,
    #[error("host outbox session run does not match")]
    SessionRunMismatch,
    #[error("host outbox epoch is stale")]
    StaleEpoch,
    #[error("host outbox acknowledgement is invalid")]
    InvalidAcknowledgement,
    #[error("host outbox delivery was not found")]
    UnknownDelivery,
    #[error("host outbox delivery digest does not match")]
    DigestMismatch,
    #[error("host outbox delivery is corrupt")]
    CorruptDelivery,
}

impl HostOutboxError {
    pub(crate) fn from_store(error: SessionStoreError) -> Self {
        match error {
            SessionStoreError::RunIdConflict { .. } => Self::SessionRunMismatch,
            SessionStoreError::StaleLease { .. } | SessionStoreError::HostOutboxStaleEpoch => Self::StaleEpoch,
            SessionStoreError::InvalidHostOutboxInput | SessionStoreError::HostOutboxPayloadTooLarge { .. } => {
                Self::InvalidAcknowledgement
            }
            SessionStoreError::HostOutboxDeliveryNotFound => Self::UnknownDelivery,
            SessionStoreError::HostOutboxDigestMismatch => Self::DigestMismatch,
            SessionStoreError::HostOutboxCorrupt => Self::CorruptDelivery,
            _ => Self::StorageUnavailable,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostOutboxAckOutcome {
    Acknowledged,
    AlreadyAcknowledged,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostOutboxDelivery {
    pub delivery_id: String,
    pub msg_id: String,
    pub run_epoch: u64,
    pub sequence: u64,
    pub digest: String,
    pub payload: Vec<u8>,
}

impl HostOutboxDelivery {
    pub fn metadata(&self) -> DeliveryMetadata {
        DeliveryMetadata {
            delivery_id: self.delivery_id.clone(),
            msg_id: self.msg_id.clone(),
            run_epoch: self.run_epoch,
            sequence: self.sequence,
            digest: self.digest.clone(),
        }
    }
}

#[derive(Clone)]
pub struct HostOutbox {
    store: SessionStore,
    stored: StoredHostOutbox,
}

impl HostOutbox {
    pub(crate) fn open(store: SessionStore, session_id: &str, run_id: &str) -> Result<Self, HostOutboxError> {
        let stored = store
            .prepare_host_outbox(session_id, run_id)
            .map_err(HostOutboxError::from_store)?;
        Ok(Self { store, stored })
    }

    pub fn enqueue(&self, msg_id: &str, event_payload: &[u8]) -> Result<HostOutboxDelivery, HostOutboxError> {
        self.store
            .enqueue_host_delivery(&self.stored, msg_id, event_payload)
            .map_err(HostOutboxError::from_store)
            .and_then(convert_delivery)
    }

    pub fn pending(&self) -> Result<Vec<HostOutboxDelivery>, HostOutboxError> {
        self.store
            .pending_host_deliveries(&self.stored)
            .map_err(HostOutboxError::from_store)?
            .into_iter()
            .map(convert_delivery)
            .collect()
    }

    pub fn acknowledge(
        &self,
        acknowledgement: &DeliveryAcknowledgement,
    ) -> Result<HostOutboxAckOutcome, HostOutboxError> {
        if acknowledgement.session_id != self.stored.session_id {
            return Err(HostOutboxError::InvalidAcknowledgement);
        }
        let run_epoch = i64::try_from(acknowledgement.run_epoch).map_err(|_| HostOutboxError::StaleEpoch)?;
        self.store
            .acknowledge_host_delivery(
                &self.stored,
                run_epoch,
                &acknowledgement.delivery_id,
                &acknowledgement.digest,
            )
            .map_err(HostOutboxError::from_store)
            .map(|outcome| match outcome {
                StoredHostAckOutcome::Acknowledged => HostOutboxAckOutcome::Acknowledged,
                StoredHostAckOutcome::AlreadyAcknowledged => HostOutboxAckOutcome::AlreadyAcknowledged,
            })
    }
}

fn convert_delivery(stored: StoredHostDelivery) -> Result<HostOutboxDelivery, HostOutboxError> {
    Ok(HostOutboxDelivery {
        delivery_id: stored.delivery_id,
        msg_id: stored.msg_id,
        run_epoch: u64::try_from(stored.run_epoch).map_err(|_| HostOutboxError::CorruptDelivery)?,
        sequence: u64::try_from(stored.sequence).map_err(|_| HostOutboxError::CorruptDelivery)?,
        digest: stored.digest,
        payload: stored.payload,
    })
}

#[cfg(test)]
#[path = "host_outbox_test.rs"]
mod host_outbox_test;
