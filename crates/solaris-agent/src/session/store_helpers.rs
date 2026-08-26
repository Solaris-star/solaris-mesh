use std::io;

use rusqlite::Error as SqliteError;

use super::{DEFAULT_LEASE_SECONDS, Session, SessionStoreError};

pub(super) fn counter_from_sql(value: i64) -> Result<i64, SessionStoreError> {
    if value < 0 {
        return Err(SessionStoreError::Database {
            operation: "decode session store counter",
            source: SqliteError::IntegralValueOutOfRange(0, value),
        });
    }
    Ok(value)
}

pub(super) fn validate_owner(owner_id: &str) -> Result<(), SessionStoreError> {
    if owner_id.is_empty() {
        return Err(SessionStoreError::InvalidOwner);
    }
    Ok(())
}

pub(super) fn lease_expiry(now_ms: i64) -> i64 {
    now_ms.saturating_add(DEFAULT_LEASE_SECONDS.saturating_mul(1_000))
}

pub(super) fn encode_session(session: &Session, operation: &'static str) -> Result<Vec<u8>, SessionStoreError> {
    serde_json::to_vec(session).map_err(|source| SessionStoreError::Json { operation, source })
}

pub(super) fn decode_session(bytes: &[u8], operation: &'static str) -> Result<Session, SessionStoreError> {
    serde_json::from_slice(bytes).map_err(|source| SessionStoreError::Json { operation, source })
}

pub(super) fn db_error(operation: &'static str, source: rusqlite::Error) -> SessionStoreError {
    SessionStoreError::Database { operation, source }
}

pub(super) fn io_error(operation: &'static str, source: io::Error) -> SessionStoreError {
    SessionStoreError::Io { operation, source }
}

pub(super) fn sidecar_slot_may_be_changing(error: &SessionStoreError) -> bool {
    match error {
        SessionStoreError::UnsafePath { .. } => true,
        SessionStoreError::Io { source, .. } => matches!(
            source.kind(),
            io::ErrorKind::NotFound | io::ErrorKind::AlreadyExists | io::ErrorKind::PermissionDenied
        ),
        _ => false,
    }
}
