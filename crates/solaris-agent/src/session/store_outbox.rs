#[cfg(test)]
use std::process;

use chrono::{DateTime, Utc};
use rusqlite::{OptionalExtension, Transaction, params};
use solaris_protocol::delivery::canonical_event_payload_digest;
use uuid::Uuid;

use super::{
    SessionStore, SessionStoreError, begin_immediate, counter_from_sql, db_error, query_lease, query_run_id,
    require_not_tombstoned,
};

const MAX_HOST_OUTBOX_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
const MAX_HOST_OUTBOX_ID_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredHostOutbox {
    pub(crate) session_id: String,
    pub(crate) run_id: String,
    pub(crate) run_epoch: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredHostDelivery {
    pub(crate) delivery_id: String,
    pub(crate) session_id: String,
    pub(crate) msg_id: String,
    pub(crate) run_epoch: i64,
    pub(crate) sequence: i64,
    pub(crate) digest: String,
    pub(crate) payload: Vec<u8>,
    created_at_ms: i64,
    digest_bytes: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StoredHostAckOutcome {
    Acknowledged,
    AlreadyAcknowledged,
}

impl SessionStore {
    pub(crate) fn prepare_host_outbox(
        &self,
        session_id: &str,
        run_id: &str,
    ) -> Result<StoredHostOutbox, SessionStoreError> {
        self.prepare_host_outbox_with_clock(session_id, run_id, Utc::now)
    }

    #[cfg(test)]
    pub(crate) fn prepare_host_outbox_at(
        &self,
        session_id: &str,
        run_id: &str,
        now: DateTime<Utc>,
    ) -> Result<StoredHostOutbox, SessionStoreError> {
        self.prepare_host_outbox_with_clock(session_id, run_id, move || now)
    }

    fn prepare_host_outbox_with_clock(
        &self,
        session_id: &str,
        run_id: &str,
        clock: impl FnOnce() -> DateTime<Utc>,
    ) -> Result<StoredHostOutbox, SessionStoreError> {
        validate_id(session_id)?;
        validate_id(run_id)?;
        let mut connection = self.open_connection()?;
        let storage = connection.storage_guard();
        let transaction = begin_immediate(&mut connection, "begin host outbox activation")?;
        let now_ms = clock().timestamp_millis();
        let run_epoch = require_host_outbox_epoch(&transaction, session_id, run_id, None, now_ms)?;
        adopt_pending_deliveries(&transaction, session_id, run_epoch)?;
        storage.verify()?;
        transaction
            .commit()
            .map_err(|source| db_error("commit host outbox activation", source))?;
        connection.verify_storage_slots()?;
        Ok(StoredHostOutbox {
            session_id: session_id.to_owned(),
            run_id: run_id.to_owned(),
            run_epoch,
        })
    }

    pub(crate) fn enqueue_host_delivery(
        &self,
        outbox: &StoredHostOutbox,
        msg_id: &str,
        payload: &[u8],
    ) -> Result<StoredHostDelivery, SessionStoreError> {
        self.enqueue_host_delivery_with_clock(outbox, msg_id, payload, Utc::now)
    }

    #[cfg(test)]
    pub(crate) fn enqueue_host_delivery_at(
        &self,
        outbox: &StoredHostOutbox,
        msg_id: &str,
        payload: &[u8],
        now: DateTime<Utc>,
    ) -> Result<StoredHostDelivery, SessionStoreError> {
        self.enqueue_host_delivery_with_clock(outbox, msg_id, payload, move || now)
    }

    fn enqueue_host_delivery_with_clock(
        &self,
        outbox: &StoredHostOutbox,
        msg_id: &str,
        payload: &[u8],
        clock: impl FnOnce() -> DateTime<Utc>,
    ) -> Result<StoredHostDelivery, SessionStoreError> {
        validate_outbox(outbox)?;
        validate_id(msg_id)?;
        let digest_bytes = new_payload_digest(payload)?;
        let delivery_id = Uuid::now_v7().to_string();
        let mut connection = self.open_connection()?;
        let storage = connection.storage_guard();
        let transaction = begin_immediate(&mut connection, "begin host delivery enqueue")?;
        let now_ms = clock().timestamp_millis();
        require_host_outbox_epoch(
            &transaction,
            &outbox.session_id,
            &outbox.run_id,
            Some(outbox.run_epoch),
            now_ms,
        )?;
        let sequence = next_sequence(&transaction, &outbox.session_id, outbox.run_epoch)?;
        transaction
            .execute(
                "INSERT INTO host_outbox
                    (delivery_id, session_id, msg_id, run_epoch, sequence, digest, payload, created_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    &delivery_id,
                    &outbox.session_id,
                    msg_id,
                    outbox.run_epoch,
                    sequence,
                    &digest_bytes,
                    payload,
                    now_ms,
                ],
            )
            .map_err(|source| db_error("insert host outbox delivery", source))?;
        storage.verify()?;
        transaction
            .commit()
            .map_err(|source| db_error("commit host delivery enqueue", source))?;
        connection.verify_storage_slots()?;
        Ok(StoredHostDelivery {
            delivery_id,
            session_id: outbox.session_id.clone(),
            msg_id: msg_id.to_owned(),
            run_epoch: outbox.run_epoch,
            sequence,
            digest: digest_marker(&digest_bytes),
            payload: payload.to_vec(),
            created_at_ms: now_ms,
            digest_bytes,
        })
    }

    pub(crate) fn pending_host_deliveries(
        &self,
        outbox: &StoredHostOutbox,
    ) -> Result<Vec<StoredHostDelivery>, SessionStoreError> {
        self.pending_host_deliveries_with_clock(outbox, Utc::now)
    }

    #[cfg(test)]
    pub(crate) fn pending_host_deliveries_at(
        &self,
        outbox: &StoredHostOutbox,
        now: DateTime<Utc>,
    ) -> Result<Vec<StoredHostDelivery>, SessionStoreError> {
        self.pending_host_deliveries_with_clock(outbox, move || now)
    }

    fn pending_host_deliveries_with_clock(
        &self,
        outbox: &StoredHostOutbox,
        clock: impl FnOnce() -> DateTime<Utc>,
    ) -> Result<Vec<StoredHostDelivery>, SessionStoreError> {
        validate_outbox(outbox)?;
        let mut connection = self.open_connection()?;
        let storage = connection.storage_guard();
        let transaction = begin_immediate(&mut connection, "begin host outbox pending query")?;
        require_host_outbox_epoch(
            &transaction,
            &outbox.session_id,
            &outbox.run_id,
            Some(outbox.run_epoch),
            clock().timestamp_millis(),
        )?;
        let deliveries = query_pending_deliveries(&transaction, &outbox.session_id, Some(outbox.run_epoch))?;
        storage.verify()?;
        transaction
            .commit()
            .map_err(|source| db_error("commit host outbox pending query", source))?;
        connection.verify_storage_slots()?;
        Ok(deliveries)
    }

    pub(crate) fn acknowledge_host_delivery(
        &self,
        outbox: &StoredHostOutbox,
        run_epoch: i64,
        delivery_id: &str,
        digest: &str,
    ) -> Result<StoredHostAckOutcome, SessionStoreError> {
        self.acknowledge_host_delivery_with_clock(outbox, run_epoch, delivery_id, digest, Utc::now)
    }

    #[cfg(test)]
    pub(crate) fn acknowledge_host_delivery_at(
        &self,
        outbox: &StoredHostOutbox,
        run_epoch: i64,
        delivery_id: &str,
        digest: &str,
        now: DateTime<Utc>,
    ) -> Result<StoredHostAckOutcome, SessionStoreError> {
        self.acknowledge_host_delivery_with_clock(outbox, run_epoch, delivery_id, digest, move || now)
    }

    fn acknowledge_host_delivery_with_clock(
        &self,
        outbox: &StoredHostOutbox,
        run_epoch: i64,
        delivery_id: &str,
        digest: &str,
        clock: impl FnOnce() -> DateTime<Utc>,
    ) -> Result<StoredHostAckOutcome, SessionStoreError> {
        validate_outbox(outbox)?;
        if run_epoch != outbox.run_epoch {
            return Err(SessionStoreError::HostOutboxStaleEpoch);
        }
        validate_id(delivery_id)?;
        validate_id(digest)?;
        let mut connection = self.open_connection()?;
        let storage = connection.storage_guard();
        let transaction = begin_immediate(&mut connection, "begin host delivery acknowledgement")?;
        let now_ms = clock().timestamp_millis();
        require_host_outbox_epoch(
            &transaction,
            &outbox.session_id,
            &outbox.run_id,
            Some(run_epoch),
            now_ms,
        )?;
        let row = transaction
            .query_row(
                "SELECT digest, payload, acknowledged_at_ms FROM host_outbox
                 WHERE delivery_id = ?1 AND session_id = ?2 AND run_epoch = ?3",
                params![delivery_id, &outbox.session_id, run_epoch],
                |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(|source| db_error("query host delivery acknowledgement", source))?
            .ok_or(SessionStoreError::HostOutboxDeliveryNotFound)?;
        let (stored_digest, payload, acknowledged_at_ms) = row;
        verify_payload_digest(&payload, &stored_digest)?;
        if digest_marker(&stored_digest) != digest {
            return Err(SessionStoreError::HostOutboxDigestMismatch);
        }
        if acknowledged_at_ms.is_some() {
            return Ok(StoredHostAckOutcome::AlreadyAcknowledged);
        }
        let changed = transaction
            .execute(
                "UPDATE host_outbox SET acknowledged_at_ms = ?2
                 WHERE delivery_id = ?1 AND acknowledged_at_ms IS NULL",
                params![delivery_id, now_ms],
            )
            .map_err(|source| db_error("acknowledge host outbox delivery", source))?;
        if changed != 1 {
            return Err(SessionStoreError::HostOutboxDeliveryNotFound);
        }
        storage.verify()?;
        transaction
            .commit()
            .map_err(|source| db_error("commit host delivery acknowledgement", source))?;
        connection.verify_storage_slots()?;
        Ok(StoredHostAckOutcome::Acknowledged)
    }

    #[cfg(test)]
    pub(crate) fn test_exit_before_host_enqueue_commit(&self, outbox: &StoredHostOutbox, exit_code: i32) -> ! {
        let payload = br#"{"type":"info","message":"not-committed"}"#;
        let digest = new_payload_digest(payload).unwrap();
        let mut connection = self.open_connection().unwrap();
        let transaction = begin_immediate(&mut connection, "begin host enqueue crash injection").unwrap();
        require_host_outbox_epoch(
            &transaction,
            &outbox.session_id,
            &outbox.run_id,
            Some(outbox.run_epoch),
            Utc::now().timestamp_millis(),
        )
        .unwrap();
        let sequence = next_sequence(&transaction, &outbox.session_id, outbox.run_epoch).unwrap();
        transaction
            .execute(
                "INSERT INTO host_outbox
                    (delivery_id, session_id, msg_id, run_epoch, sequence, digest, payload, created_at_ms)
                 VALUES (?1, ?2, 'crash-message', ?3, ?4, ?5, ?6, ?7)",
                params![
                    Uuid::now_v7().to_string(),
                    &outbox.session_id,
                    outbox.run_epoch,
                    sequence,
                    digest,
                    payload,
                    Utc::now().timestamp_millis(),
                ],
            )
            .unwrap();
        process::exit(exit_code)
    }

    #[cfg(test)]
    pub(crate) fn test_exit_before_host_ack_commit(
        &self,
        outbox: &StoredHostOutbox,
        delivery_id: &str,
        exit_code: i32,
    ) -> ! {
        let mut connection = self.open_connection().unwrap();
        let transaction = begin_immediate(&mut connection, "begin host ack crash injection").unwrap();
        require_host_outbox_epoch(
            &transaction,
            &outbox.session_id,
            &outbox.run_id,
            Some(outbox.run_epoch),
            Utc::now().timestamp_millis(),
        )
        .unwrap();
        transaction
            .execute(
                "UPDATE host_outbox SET acknowledged_at_ms = ?2 WHERE delivery_id = ?1",
                params![delivery_id, Utc::now().timestamp_millis()],
            )
            .unwrap();
        process::exit(exit_code)
    }
}

fn require_host_outbox_epoch(
    transaction: &Transaction<'_>,
    session_id: &str,
    run_id: &str,
    expected_epoch: Option<i64>,
    now_ms: i64,
) -> Result<i64, SessionStoreError> {
    require_not_tombstoned(transaction, session_id)?;
    let stored_run_id = query_run_id(transaction, session_id)?;
    if stored_run_id.as_deref() != Some(run_id) {
        return Err(SessionStoreError::RunIdConflict {
            session_id: session_id.to_owned(),
        });
    }
    let lease = query_lease(transaction, session_id)?.ok_or_else(|| SessionStoreError::StaleLease {
        session_id: session_id.to_owned(),
    })?;
    if lease.owner_id.is_none() || lease.expires_at_ms.is_none_or(|expires_at| expires_at <= now_ms) {
        return Err(SessionStoreError::StaleLease {
            session_id: session_id.to_owned(),
        });
    }
    if expected_epoch.is_some_and(|expected| expected != lease.epoch) {
        return Err(SessionStoreError::HostOutboxStaleEpoch);
    }
    Ok(lease.epoch)
}

fn adopt_pending_deliveries(
    transaction: &Transaction<'_>,
    session_id: &str,
    run_epoch: i64,
) -> Result<(), SessionStoreError> {
    let pending = query_pending_deliveries(transaction, session_id, None)?;
    if pending.iter().all(|delivery| delivery.run_epoch == run_epoch) {
        return Ok(());
    }
    transaction
        .execute(
            "DELETE FROM host_outbox WHERE session_id = ?1 AND acknowledged_at_ms IS NULL",
            params![session_id],
        )
        .map_err(|source| db_error("stage host outbox epoch adoption", source))?;
    let mut sequence = transaction
        .query_row(
            "SELECT MAX(sequence) FROM host_outbox
             WHERE session_id = ?1 AND run_epoch = ?2",
            params![session_id, run_epoch],
            |row| row.get::<_, Option<i64>>(0),
        )
        .map_err(|source| db_error("query host outbox adoption sequence", source))?
        .map(counter_from_sql)
        .transpose()?
        .map(|value| {
            value.checked_add(1).ok_or(SessionStoreError::RevisionExhausted {
                session_id: session_id.to_owned(),
            })
        })
        .transpose()?
        .unwrap_or(0);
    for delivery in pending {
        transaction
            .execute(
                "INSERT INTO host_outbox
                    (delivery_id, session_id, msg_id, run_epoch, sequence, digest, payload, created_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    delivery.delivery_id,
                    delivery.session_id,
                    delivery.msg_id,
                    run_epoch,
                    sequence,
                    delivery.digest_bytes,
                    delivery.payload,
                    delivery.created_at_ms,
                ],
            )
            .map_err(|source| db_error("adopt pending host outbox delivery", source))?;
        sequence = sequence
            .checked_add(1)
            .ok_or_else(|| SessionStoreError::RevisionExhausted {
                session_id: session_id.to_owned(),
            })?;
    }
    Ok(())
}

fn query_pending_deliveries(
    transaction: &Transaction<'_>,
    session_id: &str,
    run_epoch: Option<i64>,
) -> Result<Vec<StoredHostDelivery>, SessionStoreError> {
    let mut statement = transaction
        .prepare(
            "SELECT delivery_id, session_id, msg_id, run_epoch, sequence, digest, payload, created_at_ms
             FROM host_outbox
             WHERE session_id = ?1 AND acknowledged_at_ms IS NULL
               AND (?2 IS NULL OR run_epoch = ?2)
             ORDER BY created_at_ms, run_epoch, sequence, delivery_id",
        )
        .map_err(|source| db_error("prepare pending host outbox query", source))?;
    let rows = statement
        .query_map(params![session_id, run_epoch], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, Vec<u8>>(5)?,
                row.get::<_, Vec<u8>>(6)?,
                row.get::<_, i64>(7)?,
            ))
        })
        .map_err(|source| db_error("query pending host outbox deliveries", source))?;
    let mut deliveries = Vec::new();
    for row in rows {
        let (delivery_id, session_id, msg_id, run_epoch, sequence, digest_bytes, payload, created_at_ms) =
            row.map_err(|source| db_error("read pending host outbox delivery", source))?;
        verify_payload_digest(&payload, &digest_bytes)?;
        deliveries.push(StoredHostDelivery {
            delivery_id,
            session_id,
            msg_id,
            run_epoch: counter_from_sql(run_epoch)?,
            sequence: counter_from_sql(sequence)?,
            digest: digest_marker(&digest_bytes),
            payload,
            created_at_ms,
            digest_bytes,
        });
    }
    Ok(deliveries)
}

fn next_sequence(transaction: &Transaction<'_>, session_id: &str, run_epoch: i64) -> Result<i64, SessionStoreError> {
    let current = transaction
        .query_row(
            "SELECT MAX(sequence) FROM host_outbox WHERE session_id = ?1 AND run_epoch = ?2",
            params![session_id, run_epoch],
            |row| row.get::<_, Option<i64>>(0),
        )
        .map_err(|source| db_error("query next host outbox sequence", source))?;
    current
        .map(counter_from_sql)
        .transpose()?
        .map(|value| {
            value
                .checked_add(1)
                .ok_or_else(|| SessionStoreError::RevisionExhausted {
                    session_id: session_id.to_owned(),
                })
        })
        .transpose()
        .map(Option::unwrap_or_default)
}

fn validate_outbox(outbox: &StoredHostOutbox) -> Result<(), SessionStoreError> {
    validate_id(&outbox.session_id)?;
    validate_id(&outbox.run_id)?;
    if outbox.run_epoch < 1 {
        return Err(SessionStoreError::InvalidHostOutboxInput);
    }
    Ok(())
}

fn validate_id(value: &str) -> Result<(), SessionStoreError> {
    if value.is_empty() || value.len() > MAX_HOST_OUTBOX_ID_BYTES || value.chars().any(char::is_control) {
        return Err(SessionStoreError::InvalidHostOutboxInput);
    }
    Ok(())
}

fn new_payload_digest(payload: &[u8]) -> Result<Vec<u8>, SessionStoreError> {
    if payload.len() > MAX_HOST_OUTBOX_PAYLOAD_BYTES {
        return Err(SessionStoreError::HostOutboxPayloadTooLarge {
            max_bytes: MAX_HOST_OUTBOX_PAYLOAD_BYTES,
        });
    }
    canonical_event_payload_digest(payload)
        .map(|digest| digest.to_vec())
        .map_err(|_| SessionStoreError::InvalidHostOutboxInput)
}

fn verify_payload_digest(payload: &[u8], digest: &[u8]) -> Result<(), SessionStoreError> {
    if payload.len() > MAX_HOST_OUTBOX_PAYLOAD_BYTES {
        return Err(SessionStoreError::HostOutboxCorrupt);
    }
    let canonical_digest = canonical_event_payload_digest(payload).map_err(|_| SessionStoreError::HostOutboxCorrupt)?;
    if canonical_digest.as_slice() != digest {
        return Err(SessionStoreError::HostOutboxCorrupt);
    }
    Ok(())
}

fn digest_marker(digest: &[u8]) -> String {
    let mut marker = String::with_capacity(7 + digest.len() * 2);
    marker.push_str("sha256:");
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(marker, "{byte:02x}");
    }
    marker
}

#[cfg(test)]
#[path = "store_outbox_test.rs"]
mod store_outbox_test;
