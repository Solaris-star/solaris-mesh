use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::process;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use thiserror::Error;

use super::Session;

#[path = "store_connection.rs"]
mod store_connection;
#[path = "store_conversation.rs"]
mod store_conversation;
#[path = "store_gc.rs"]
mod store_gc;
#[path = "store_helpers.rs"]
mod store_helpers;
#[path = "store_import.rs"]
mod store_import;
#[path = "store_outbox.rs"]
mod store_outbox;
#[path = "store_path.rs"]
mod store_path;
#[path = "store_retention.rs"]
mod store_retention;
#[path = "store_schema.rs"]
mod store_schema;
#[path = "store_task.rs"]
mod store_task;

use store_connection::{begin_immediate, enable_wal};
pub(crate) use store_conversation::{
    ConversationCloseClaim, ConversationIdentity, ConversationOpenClaim, ConversationState, ConversationTurnClaim,
    ConversationTurnCompletion, ConversationTurnEnqueue, ConversationTurnIdentity, ConversationTurnState,
    StoredConversationTurn,
};
pub(crate) use store_gc::SessionGcReport;
use store_helpers::{
    counter_from_sql, db_error, decode_session, encode_session, io_error, lease_expiry, sidecar_slot_may_be_changing,
    validate_owner,
};
use store_import::import_legacy_json;
#[cfg(test)]
use store_import::metadata_if_exists;
pub(crate) use store_outbox::{StoredHostAckOutcome, StoredHostDelivery, StoredHostOutbox};
use store_path::{StoreDirectory, StoreFileSlot};
use store_schema::initialize_schema;
#[cfg(test)]
pub(crate) use store_task::TaskTransitionFault;
pub(crate) use store_task::{DurableTaskPhase, StoredDurableTask};

pub(crate) const SESSION_STORE_SCHEMA_VERSION: i64 = 9;
pub(crate) const DEFAULT_HEARTBEAT_SECONDS: i64 = 15;
pub(crate) const DEFAULT_LEASE_SECONDS: i64 = 90;
pub(crate) const MAX_LEGACY_SESSION_BYTES: u64 = 64 * 1024 * 1024;
const BUSY_TIMEOUT_SECONDS: u64 = 30;
const DATABASE_FILE_NAME: &str = "session.sqlite3";
const SQLITE_SIDECAR_SUFFIXES: [&str; 3] = ["-wal", "-shm", "-journal"];
const SIDECAR_SLOT_RETRIES: usize = 32;

#[derive(Debug, Error)]
pub(crate) enum SessionStoreError {
    #[error("session '{session_id}' already exists")]
    AlreadyExists { session_id: String },
    #[error("session '{session_id}' was not found")]
    NotFound { session_id: String },
    #[error("session '{session_id}' has been tombstoned")]
    Tombstoned { session_id: String },
    #[error("session '{session_id}' is active under owner '{owner_id}' until {expires_at_ms}")]
    LeaseHeld {
        session_id: String,
        owner_id: String,
        expires_at_ms: i64,
    },
    #[error("session '{session_id}' lease is stale")]
    StaleLease { session_id: String },
    #[error("session '{session_id}' revision changed from {expected} to {actual}")]
    RevisionConflict {
        session_id: String,
        expected: i64,
        actual: i64,
    },
    #[error("session '{session_id}' requires a non-empty run ID when it is created")]
    MissingRunId { session_id: String },
    #[error("session '{session_id}' cannot change its persisted run ID")]
    RunIdConflict { session_id: String },
    #[error("session '{session_id}' cannot replace or remove its durable Memory snapshot")]
    MemorySnapshotConflict { session_id: String },
    #[error("run '{requested_run_id}' overlaps active GC claim '{claimed_run_id}'")]
    RunGcClaimed {
        requested_run_id: String,
        claimed_run_id: String,
    },
    #[error("session store owner ID must not be empty")]
    InvalidOwner,
    #[error("session state does not match lease for '{lease_session_id}'")]
    SessionIdMismatch {
        lease_session_id: String,
        state_session_id: String,
    },
    #[error("session store schema version {found} is not supported by version {supported}")]
    UnsupportedSchema { found: i64, supported: i64 },
    #[error("session store epoch is exhausted for '{session_id}'")]
    EpochExhausted { session_id: String },
    #[error("session store revision is exhausted for '{session_id}'")]
    RevisionExhausted { session_id: String },
    #[error("session store could not enter WAL mode (reported '{actual}')")]
    WalUnavailable { actual: String },
    #[error("unsafe session store path during {operation}")]
    UnsafePath { operation: &'static str },
    #[error("legacy session source exceeds {max_bytes} bytes")]
    LegacySourceTooLarge { max_bytes: u64 },
    #[error("host outbox input is invalid")]
    InvalidHostOutboxInput,
    #[error("host outbox payload exceeds {max_bytes} bytes")]
    HostOutboxPayloadTooLarge { max_bytes: usize },
    #[error("host outbox epoch is stale")]
    HostOutboxStaleEpoch,
    #[error("host outbox delivery was not found")]
    HostOutboxDeliveryNotFound,
    #[error("host outbox delivery digest does not match")]
    HostOutboxDigestMismatch,
    #[error("host outbox delivery is corrupt")]
    HostOutboxCorrupt,
    #[error(
        "durable task input conflicts with the existing message identity for '{task_key}' in session '{session_id}'"
    )]
    TaskInputConflict { session_id: String, task_key: String },
    #[error("durable task '{task_key}' was not found in session '{session_id}'")]
    TaskNotFound { session_id: String, task_key: String },
    #[error("durable task revision conflict for '{task_key}': expected {expected}, actual {actual}")]
    TaskRevisionConflict {
        task_key: String,
        expected: i64,
        actual: i64,
    },
    #[error("durable task revision is exhausted")]
    TaskRevisionExhausted,
    #[error("invalid durable task transition from {from} to {to}")]
    InvalidTaskTransition { from: &'static str, to: &'static str },
    #[error("durable task input is invalid")]
    InvalidTaskInput,
    #[error("durable task state is corrupt")]
    TaskStateCorrupt,
    #[error("durable Agent conversation input is invalid")]
    InvalidConversationInput,
    #[error("durable Agent conversation was not found")]
    ConversationNotFound,
    #[error("durable Agent conversation identity conflicts with persisted state")]
    ConversationIdentityConflict,
    #[error("durable Agent conversation handle does not match persisted state")]
    ConversationHandleMismatch,
    #[error("durable Agent conversation is in state '{state}' and cannot perform this operation")]
    ConversationStateConflict { state: String },
    #[error("durable Agent conversation revision changed from {expected} to {actual}")]
    ConversationRevisionConflict { expected: i64, actual: i64 },
    #[error("durable Agent conversation revision is exhausted")]
    ConversationRevisionExhausted,
    #[error("durable Agent conversation opening epoch is exhausted")]
    ConversationEpochExhausted,
    #[error("durable Agent conversation turn input conflicts with its persisted identity")]
    ConversationTurnInputConflict,
    #[error("durable Agent conversation turn was not found")]
    ConversationTurnNotFound,
    #[error("durable Agent conversation state is corrupt")]
    ConversationStateCorrupt,
    #[error("{operation}: {source}")]
    Database {
        operation: &'static str,
        #[source]
        source: rusqlite::Error,
    },
    #[error("{operation}: {source}")]
    Io {
        operation: &'static str,
        #[source]
        source: io::Error,
    },
    #[error("{operation}: invalid session JSON: {source}")]
    Json {
        operation: &'static str,
        #[source]
        source: serde_json::Error,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct SessionLease {
    pub(crate) session_id: String,
    pub(crate) owner_id: String,
    pub(crate) epoch: i64,
    pub(crate) revision: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct StoredSession {
    pub(crate) session: Session,
    pub(crate) revision: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct ActiveSession {
    pub(crate) session: Session,
    pub(crate) lease: SessionLease,
}

#[derive(Clone)]
pub(crate) struct SessionStore {
    directory: Arc<StoreDirectory>,
    database_slot: Arc<StoreFileSlot>,
    database_path: PathBuf,
}

impl fmt::Debug for SessionStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionStore")
            .field("database_path", &self.database_path)
            .finish_non_exhaustive()
    }
}

impl SessionStore {
    pub(crate) fn open(directory: impl AsRef<Path>) -> Result<Self, SessionStoreError> {
        let directory = Arc::new(StoreDirectory::open(directory.as_ref())?);
        let database_path = directory.path().join(DATABASE_FILE_NAME);
        let database_slot = Arc::new(directory.create_file_slot(Path::new(DATABASE_FILE_NAME))?);
        let store = Self {
            directory,
            database_slot,
            database_path,
        };
        let mut connection = store.open_connection()?;
        enable_wal(&connection)?;
        initialize_schema(&mut connection)?;
        connection.verify_storage_slots()?;
        drop(connection);
        store.verify_database_files()?;
        import_legacy_json(&store)?;
        Ok(store)
    }

    #[cfg(test)]
    pub(crate) fn database_path(&self) -> &Path {
        &self.database_path
    }

    pub(crate) fn create_active(&self, session: &Session, owner_id: &str) -> Result<SessionLease, SessionStoreError> {
        self.create_active_with_clock(session, owner_id, Utc::now)
    }

    pub(crate) fn load_active(&self, session_id: &str, owner_id: &str) -> Result<ActiveSession, SessionStoreError> {
        self.load_active_with_clock(session_id, owner_id, Utc::now)
    }

    pub(crate) fn save(&self, lease: &mut SessionLease, session: &Session) -> Result<(), SessionStoreError> {
        self.save_with_clock(lease, session, Utc::now)
    }

    pub(crate) fn heartbeat(&self, lease: &SessionLease) -> Result<(), SessionStoreError> {
        self.heartbeat_with_clock(lease, Utc::now)
    }

    pub(crate) fn release(&self, lease: &SessionLease) -> Result<(), SessionStoreError> {
        self.release_with_clock(lease, Utc::now)
    }

    pub(crate) fn load(&self, session_id: &str) -> Result<Option<StoredSession>, SessionStoreError> {
        let connection = self.open_connection()?;
        let stored = query_stored_session(&connection, session_id)?;
        connection.verify_storage_slots()?;
        Ok(stored)
    }

    pub(crate) fn list(&self) -> Result<Vec<StoredSession>, SessionStoreError> {
        let connection = self.open_connection()?;
        let mut statement = connection
            .prepare(
                "SELECT state_json, revision FROM sessions
                 WHERE NOT EXISTS (
                    SELECT 1 FROM session_tombstones
                    WHERE session_tombstones.session_id = sessions.session_id
                 )
                 ORDER BY created_at_ms, session_id",
            )
            .map_err(|source| db_error("prepare session list", source))?;
        let rows = statement
            .query_map([], |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?)))
            .map_err(|source| db_error("query session list", source))?;
        let mut sessions = Vec::new();
        for row in rows {
            let (state_json, revision) = row.map_err(|source| db_error("read session list", source))?;
            sessions.push(StoredSession {
                session: decode_session(&state_json, "decode stored session")?,
                revision: counter_from_sql(revision)?,
            });
        }
        drop(statement);
        connection.verify_storage_slots()?;
        Ok(sessions)
    }

    #[cfg(test)]
    fn create_active_at(
        &self,
        session: &Session,
        owner_id: &str,
        now: DateTime<Utc>,
    ) -> Result<SessionLease, SessionStoreError> {
        self.create_active_with_clock(session, owner_id, move || now)
    }

    fn create_active_with_clock(
        &self,
        session: &Session,
        owner_id: &str,
        clock: impl FnOnce() -> DateTime<Utc>,
    ) -> Result<SessionLease, SessionStoreError> {
        validate_owner(owner_id)?;
        let run_id = require_new_session_run_id(session)?;
        let encoded = encode_session(session, "encode new session")?;
        let mut connection = self.open_connection()?;
        let storage = connection.storage_guard();
        let transaction = begin_immediate(&mut connection, "begin create active session")?;
        let now_ms = clock().timestamp_millis();
        let expires_at_ms = lease_expiry(now_ms);
        require_not_tombstoned(&transaction, &session.id)?;
        require_run_not_gc_claimed(&transaction, run_id)?;
        if row_exists(
            &transaction,
            "SELECT EXISTS(SELECT 1 FROM sessions WHERE session_id = ?1)",
            &session.id,
            "check existing session",
        )? {
            return Err(SessionStoreError::AlreadyExists {
                session_id: session.id.clone(),
            });
        }
        transaction
            .execute(
                "INSERT INTO sessions
                    (session_id, state_json, revision, run_id, created_at_ms, updated_at_ms)
                 VALUES (?1, ?2, 0, ?3, ?4, ?5)",
                params![
                    &session.id,
                    encoded,
                    run_id,
                    session.created_at.timestamp_millis(),
                    session.updated_at.timestamp_millis(),
                ],
            )
            .map_err(|source| db_error("insert active session", source))?;
        sync_session_run_reference(&transaction, &session.id, Some(run_id), now_ms)?;
        transaction
            .execute(
                "INSERT INTO session_leases
                    (session_id, owner_id, epoch, heartbeat_at_ms, expires_at_ms)
                 VALUES (?1, ?2, 1, ?3, ?4)",
                params![&session.id, owner_id, now_ms, expires_at_ms],
            )
            .map_err(|source| db_error("insert session lease", source))?;
        storage.verify()?;
        transaction
            .commit()
            .map_err(|source| db_error("commit active session creation", source))?;
        connection.verify_storage_slots()?;
        Ok(SessionLease {
            session_id: session.id.clone(),
            owner_id: owner_id.to_owned(),
            epoch: 1,
            revision: 0,
        })
    }

    #[cfg(test)]
    fn load_active_at(
        &self,
        session_id: &str,
        owner_id: &str,
        now: DateTime<Utc>,
    ) -> Result<ActiveSession, SessionStoreError> {
        self.load_active_with_clock(session_id, owner_id, move || now)
    }

    fn load_active_with_clock(
        &self,
        session_id: &str,
        owner_id: &str,
        clock: impl FnOnce() -> DateTime<Utc>,
    ) -> Result<ActiveSession, SessionStoreError> {
        validate_owner(owner_id)?;
        let mut connection = self.open_connection()?;
        let storage = connection.storage_guard();
        let transaction = begin_immediate(&mut connection, "begin active session load")?;
        let now_ms = clock().timestamp_millis();
        let expires_at_ms = lease_expiry(now_ms);
        require_not_tombstoned(&transaction, session_id)?;
        let stored = query_stored_session(&transaction, session_id)?.ok_or_else(|| SessionStoreError::NotFound {
            session_id: session_id.to_owned(),
        })?;
        sync_session_run_reference(
            &transaction,
            session_id,
            optional_nonempty_run_id(&stored.session)?,
            now_ms,
        )?;
        let lease_row = query_lease(&transaction, session_id)?;
        let epoch = match lease_row {
            None => {
                transaction
                    .execute(
                        "INSERT INTO session_leases
                            (session_id, owner_id, epoch, heartbeat_at_ms, expires_at_ms)
                         VALUES (?1, ?2, 1, ?3, ?4)",
                        params![session_id, owner_id, now_ms, expires_at_ms],
                    )
                    .map_err(|source| db_error("insert acquired session lease", source))?;
                1
            }
            Some(current)
                if current.owner_id.as_deref() != Some(owner_id)
                    && current.owner_id.is_some()
                    && current.expires_at_ms.is_some_and(|expiry| expiry > now_ms) =>
            {
                return Err(SessionStoreError::LeaseHeld {
                    session_id: session_id.to_owned(),
                    owner_id: current.owner_id.unwrap_or_default(),
                    expires_at_ms: current.expires_at_ms.unwrap_or(now_ms),
                });
            }
            Some(current) => {
                let next_epoch = current
                    .epoch
                    .checked_add(1)
                    .ok_or_else(|| SessionStoreError::EpochExhausted {
                        session_id: session_id.to_owned(),
                    })?;
                transaction
                    .execute(
                        "UPDATE session_leases
                         SET owner_id = ?2, epoch = ?3, heartbeat_at_ms = ?4, expires_at_ms = ?5
                         WHERE session_id = ?1 AND epoch = ?6",
                        params![session_id, owner_id, next_epoch, now_ms, expires_at_ms, current.epoch],
                    )
                    .map_err(|source| db_error("take over session lease", source))?;
                next_epoch
            }
        };
        storage.verify()?;
        transaction
            .commit()
            .map_err(|source| db_error("commit active session load", source))?;
        connection.verify_storage_slots()?;
        Ok(ActiveSession {
            session: stored.session,
            lease: SessionLease {
                session_id: session_id.to_owned(),
                owner_id: owner_id.to_owned(),
                epoch,
                revision: stored.revision,
            },
        })
    }

    #[cfg(test)]
    fn save_at(
        &self,
        lease: &mut SessionLease,
        session: &Session,
        now: DateTime<Utc>,
    ) -> Result<(), SessionStoreError> {
        self.save_with_clock(lease, session, move || now)
    }

    fn save_with_clock(
        &self,
        lease: &mut SessionLease,
        session: &Session,
        clock: impl FnOnce() -> DateTime<Utc>,
    ) -> Result<(), SessionStoreError> {
        if lease.session_id != session.id {
            return Err(SessionStoreError::SessionIdMismatch {
                lease_session_id: lease.session_id.clone(),
                state_session_id: session.id.clone(),
            });
        }
        validate_owner(&lease.owner_id)?;
        let encoded = encode_session(session, "encode session save")?;
        let mut connection = self.open_connection()?;
        let storage = connection.storage_guard();
        let transaction = begin_immediate(&mut connection, "begin session save")?;
        let now_ms = clock().timestamp_millis();
        let expires_at_ms = lease_expiry(now_ms);
        require_not_tombstoned(&transaction, &lease.session_id)?;
        let stored =
            query_stored_session(&transaction, &lease.session_id)?.ok_or_else(|| SessionStoreError::NotFound {
                session_id: lease.session_id.clone(),
            })?;
        let actual_revision = stored.revision;
        require_current_lease(&transaction, lease, now_ms)?;
        let stored_run_id = query_run_id(&transaction, &lease.session_id)?;
        let requested_run_id = optional_nonempty_run_id(session)?;
        if stored_run_id
            .as_deref()
            .is_some_and(|stored| Some(stored) != requested_run_id)
        {
            return Err(SessionStoreError::RunIdConflict {
                session_id: lease.session_id.clone(),
            });
        }
        if actual_revision != lease.revision {
            return Err(SessionStoreError::RevisionConflict {
                session_id: lease.session_id.clone(),
                expected: lease.revision,
                actual: actual_revision,
            });
        }
        require_memory_snapshot_set_once(&stored.session, session)?;
        let next_revision = lease
            .revision
            .checked_add(1)
            .ok_or_else(|| SessionStoreError::RevisionExhausted {
                session_id: lease.session_id.clone(),
            })?;
        let changed = transaction
            .execute(
                "UPDATE sessions
                 SET state_json = ?2, revision = ?3, run_id = ?4, updated_at_ms = ?5
                 WHERE session_id = ?1 AND revision = ?6",
                params![
                    &lease.session_id,
                    encoded,
                    next_revision,
                    session.run_id.as_deref(),
                    session.updated_at.timestamp_millis(),
                    lease.revision,
                ],
            )
            .map_err(|source| db_error("compare and save session", source))?;
        if changed != 1 {
            let actual = query_revision(&transaction, &lease.session_id)?;
            return Err(SessionStoreError::RevisionConflict {
                session_id: lease.session_id.clone(),
                expected: lease.revision,
                actual,
            });
        }
        transaction
            .execute(
                "UPDATE session_leases
                 SET heartbeat_at_ms = ?4, expires_at_ms = ?5
                 WHERE session_id = ?1 AND owner_id = ?2 AND epoch = ?3",
                params![&lease.session_id, &lease.owner_id, lease.epoch, now_ms, expires_at_ms],
            )
            .map_err(|source| db_error("refresh lease after session save", source))?;
        sync_session_run_reference(&transaction, &lease.session_id, requested_run_id, now_ms)?;
        storage.verify()?;
        transaction
            .commit()
            .map_err(|source| db_error("commit session save", source))?;
        connection.verify_storage_slots()?;
        lease.revision = next_revision;
        Ok(())
    }

    #[cfg(test)]
    fn heartbeat_at(&self, lease: &SessionLease, now: DateTime<Utc>) -> Result<(), SessionStoreError> {
        self.heartbeat_with_clock(lease, move || now)
    }

    fn heartbeat_with_clock(
        &self,
        lease: &SessionLease,
        clock: impl FnOnce() -> DateTime<Utc>,
    ) -> Result<(), SessionStoreError> {
        validate_owner(&lease.owner_id)?;
        let mut connection = self.open_connection()?;
        let storage = connection.storage_guard();
        let transaction = begin_immediate(&mut connection, "begin session heartbeat")?;
        let now_ms = clock().timestamp_millis();
        require_current_lease(&transaction, lease, now_ms)?;
        transaction
            .execute(
                "UPDATE session_leases
                 SET heartbeat_at_ms = ?4, expires_at_ms = ?5
                 WHERE session_id = ?1 AND owner_id = ?2 AND epoch = ?3",
                params![
                    &lease.session_id,
                    &lease.owner_id,
                    lease.epoch,
                    now_ms,
                    lease_expiry(now_ms)
                ],
            )
            .map_err(|source| db_error("update session heartbeat", source))?;
        storage.verify()?;
        transaction
            .commit()
            .map_err(|source| db_error("commit session heartbeat", source))?;
        connection.verify_storage_slots()
    }

    #[cfg(test)]
    fn release_at(&self, lease: &SessionLease, now: DateTime<Utc>) -> Result<(), SessionStoreError> {
        self.release_with_clock(lease, move || now)
    }

    fn release_with_clock(
        &self,
        lease: &SessionLease,
        clock: impl FnOnce() -> DateTime<Utc>,
    ) -> Result<(), SessionStoreError> {
        validate_owner(&lease.owner_id)?;
        let mut connection = self.open_connection()?;
        let storage = connection.storage_guard();
        let transaction = begin_immediate(&mut connection, "begin session lease release")?;
        let now_ms = clock().timestamp_millis();
        require_current_lease(&transaction, lease, now_ms)?;
        transaction
            .execute(
                "UPDATE session_leases
                 SET owner_id = NULL, heartbeat_at_ms = NULL, expires_at_ms = NULL
                 WHERE session_id = ?1 AND owner_id = ?2 AND epoch = ?3",
                params![&lease.session_id, &lease.owner_id, lease.epoch],
            )
            .map_err(|source| db_error("release session lease", source))?;
        storage.verify()?;
        transaction
            .commit()
            .map_err(|source| db_error("commit session lease release", source))?;
        connection.verify_storage_slots()
    }

    #[cfg(test)]
    fn test_exit_before_save_commit(&self, lease: &SessionLease, exit_code: i32) -> ! {
        let mut connection = self.open_connection().unwrap();
        let transaction = begin_immediate(&mut connection, "begin crash injection").unwrap();
        transaction
            .execute(
                "UPDATE sessions SET state_json = X'7B7D', revision = revision + 1
                 WHERE session_id = ?1 AND revision = ?2",
                params![&lease.session_id, lease.revision],
            )
            .unwrap();
        process::exit(exit_code)
    }
}

fn require_memory_snapshot_set_once(current: &Session, requested: &Session) -> Result<(), SessionStoreError> {
    let current_reference = current
        .runtime_state
        .as_ref()
        .and_then(|state| state.memory_snapshot.as_ref());
    let requested_reference = requested
        .runtime_state
        .as_ref()
        .and_then(|state| state.memory_snapshot.as_ref());
    if current_reference.is_some_and(|current| Some(current) != requested_reference) {
        return Err(SessionStoreError::MemorySnapshotConflict {
            session_id: current.id.clone(),
        });
    }
    Ok(())
}

#[derive(Debug)]
struct LeaseRow {
    owner_id: Option<String>,
    epoch: i64,
    expires_at_ms: Option<i64>,
}

fn query_stored_session(connection: &Connection, session_id: &str) -> Result<Option<StoredSession>, SessionStoreError> {
    let row = connection
        .query_row(
            "SELECT state_json, revision FROM sessions
             WHERE session_id = ?1
               AND NOT EXISTS (
                    SELECT 1 FROM session_tombstones
                    WHERE session_tombstones.session_id = sessions.session_id
               )",
            params![session_id],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .map_err(|source| db_error("load stored session", source))?;
    row.map(|(state_json, revision)| {
        Ok(StoredSession {
            session: decode_session(&state_json, "decode stored session")?,
            revision: counter_from_sql(revision)?,
        })
    })
    .transpose()
}

fn query_revision(transaction: &Transaction<'_>, session_id: &str) -> Result<i64, SessionStoreError> {
    let revision = transaction
        .query_row(
            "SELECT revision FROM sessions WHERE session_id = ?1",
            params![session_id],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(|source| db_error("load session revision", source))?
        .ok_or_else(|| SessionStoreError::NotFound {
            session_id: session_id.to_owned(),
        })?;
    counter_from_sql(revision)
}

fn query_run_id(transaction: &Transaction<'_>, session_id: &str) -> Result<Option<String>, SessionStoreError> {
    transaction
        .query_row(
            "SELECT run_id FROM sessions WHERE session_id = ?1",
            params![session_id],
            |row| row.get(0),
        )
        .map_err(|source| db_error("query session run ID", source))
}

fn require_new_session_run_id(session: &Session) -> Result<&str, SessionStoreError> {
    optional_nonempty_run_id(session)?.ok_or_else(|| SessionStoreError::MissingRunId {
        session_id: session.id.clone(),
    })
}

fn optional_nonempty_run_id(session: &Session) -> Result<Option<&str>, SessionStoreError> {
    match session.run_id.as_deref() {
        Some(run_id) if !run_id.trim().is_empty() => Ok(Some(run_id)),
        Some(_) => Err(SessionStoreError::MissingRunId {
            session_id: session.id.clone(),
        }),
        None => Ok(None),
    }
}

fn sync_session_run_reference(
    transaction: &Transaction<'_>,
    session_id: &str,
    run_id: Option<&str>,
    created_at_ms: i64,
) -> Result<(), SessionStoreError> {
    transaction
        .execute(
            "DELETE FROM session_run_references
             WHERE session_id = ?1 AND reference_kind = 'session'",
            params![session_id],
        )
        .map_err(|source| db_error("replace session run reference", source))?;
    if let Some(run_id) = run_id {
        transaction
            .execute(
                "INSERT INTO session_run_references
                    (session_id, run_id, reference_kind, created_at_ms)
                 VALUES (?1, ?2, 'session', ?3)",
                params![session_id, run_id, created_at_ms],
            )
            .map_err(|source| db_error("record session run reference", source))?;
    }
    Ok(())
}

fn query_lease(transaction: &Transaction<'_>, session_id: &str) -> Result<Option<LeaseRow>, SessionStoreError> {
    transaction
        .query_row(
            "SELECT owner_id, epoch, expires_at_ms FROM session_leases WHERE session_id = ?1",
            params![session_id],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                ))
            },
        )
        .optional()
        .map_err(|source| db_error("load session lease", source))?
        .map(|(owner_id, epoch, expires_at_ms)| {
            Ok(LeaseRow {
                owner_id,
                epoch: counter_from_sql(epoch)?,
                expires_at_ms,
            })
        })
        .transpose()
}

fn require_current_lease(
    transaction: &Transaction<'_>,
    lease: &SessionLease,
    now_ms: i64,
) -> Result<(), SessionStoreError> {
    require_not_tombstoned(transaction, &lease.session_id)?;
    let Some(current) = query_lease(transaction, &lease.session_id)? else {
        return Err(SessionStoreError::StaleLease {
            session_id: lease.session_id.clone(),
        });
    };
    if current.owner_id.as_deref() != Some(&lease.owner_id)
        || current.epoch != lease.epoch
        || current.expires_at_ms.is_none_or(|expiry| expiry <= now_ms)
    {
        return Err(SessionStoreError::StaleLease {
            session_id: lease.session_id.clone(),
        });
    }
    Ok(())
}

fn require_not_tombstoned(transaction: &Transaction<'_>, session_id: &str) -> Result<(), SessionStoreError> {
    if row_exists(
        transaction,
        "SELECT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id = ?1)",
        session_id,
        "check session tombstone",
    )? {
        return Err(SessionStoreError::Tombstoned {
            session_id: session_id.to_owned(),
        });
    }
    Ok(())
}

fn require_run_not_gc_claimed(transaction: &Transaction<'_>, requested_run_id: &str) -> Result<(), SessionStoreError> {
    let mut statement = transaction
        .prepare(
            "SELECT session_gc_run_claims.root_run_id
             FROM session_gc_run_claims
             JOIN session_gc_jobs ON session_gc_jobs.job_id = session_gc_run_claims.job_id
             WHERE session_gc_jobs.phase != 'complete'
             ORDER BY session_gc_run_claims.root_run_id",
        )
        .map_err(|source| db_error("prepare active session GC Run claims", source))?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|source| db_error("query active session GC Run claims", source))?;
    for claimed_run_id in rows {
        let claimed_run_id = claimed_run_id.map_err(|source| db_error("read active session GC Run claim", source))?;
        if store_gc::run_trees_overlap(requested_run_id, &claimed_run_id) {
            return Err(SessionStoreError::RunGcClaimed {
                requested_run_id: requested_run_id.to_owned(),
                claimed_run_id,
            });
        }
    }
    Ok(())
}

fn row_exists(
    transaction: &Transaction<'_>,
    sql: &str,
    value: &str,
    operation: &'static str,
) -> Result<bool, SessionStoreError> {
    let exists: i64 = transaction
        .query_row(sql, params![value], |row| row.get(0))
        .map_err(|source| db_error(operation, source))?;
    Ok(exists != 0)
}

#[cfg(test)]
#[path = "store_test.rs"]
mod store_test;

#[cfg(test)]
#[path = "store_task_test.rs"]
mod store_task_test;

#[cfg(test)]
#[path = "store_security_review_test.rs"]
mod store_security_review_test;

#[cfg(test)]
#[path = "store_test_support.rs"]
mod store_test_support;
