use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use rusqlite::{Connection, Transaction, TransactionBehavior};

use super::{
    BUSY_TIMEOUT_SECONDS, DATABASE_FILE_NAME, SIDECAR_SLOT_RETRIES, SQLITE_SIDECAR_SUFFIXES, SessionStore,
    SessionStoreError, StoreDirectory, StoreFileSlot, db_error, sidecar_slot_may_be_changing,
};

#[derive(Clone)]
pub(super) struct SqliteStorageGuard {
    directory: Arc<StoreDirectory>,
    database_slot: Arc<StoreFileSlot>,
    sidecar_slots: Arc<Vec<StoreFileSlot>>,
}

impl SqliteStorageGuard {
    pub(super) fn verify(&self) -> Result<(), SessionStoreError> {
        self.directory.verify_file_slot(&self.database_slot)?;
        for (index, slot) in self.sidecar_slots.iter().enumerate() {
            if index + 1 == SQLITE_SIDECAR_SUFFIXES.len() {
                self.directory.verify_file_slot_if_present(slot)?;
            } else {
                self.directory.verify_file_slot(slot)?;
            }
        }
        Ok(())
    }
}

pub(super) struct GuardedConnection {
    connection: Connection,
    storage: SqliteStorageGuard,
}

impl GuardedConnection {
    pub(super) fn storage_guard(&self) -> SqliteStorageGuard {
        self.storage.clone()
    }

    pub(super) fn verify_storage_slots(&self) -> Result<(), SessionStoreError> {
        self.storage.verify()
    }
}

impl Deref for GuardedConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

impl DerefMut for GuardedConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.connection
    }
}

impl SessionStore {
    pub(super) fn open_connection(&self) -> Result<GuardedConnection, SessionStoreError> {
        for attempt in 0..SIDECAR_SLOT_RETRIES {
            match self.open_connection_once() {
                Ok(connection) => return Ok(connection),
                Err(error) if attempt + 1 < SIDECAR_SLOT_RETRIES && sidecar_slot_may_be_changing(&error) => {
                    thread::yield_now();
                }
                Err(error) => return Err(error),
            }
        }
        Err(SessionStoreError::UnsafePath {
            operation: "stabilize live session store SQLite slots",
        })
    }

    fn open_connection_once(&self) -> Result<GuardedConnection, SessionStoreError> {
        let sidecar_slots = Arc::new(self.prepare_sqlite_sidecar_slots()?);
        let connection = open_sqlite_connection(&self.database_path)?;
        let guarded = GuardedConnection {
            connection,
            storage: SqliteStorageGuard {
                directory: Arc::clone(&self.directory),
                database_slot: Arc::clone(&self.database_slot),
                sidecar_slots,
            },
        };
        guarded.verify_storage_slots()?;
        Ok(guarded)
    }

    fn prepare_sqlite_sidecar_slots(&self) -> Result<Vec<StoreFileSlot>, SessionStoreError> {
        self.verify_database_files()?;
        let mut slots = Vec::with_capacity(SQLITE_SIDECAR_SUFFIXES.len());
        for suffix in SQLITE_SIDECAR_SUFFIXES {
            let file_name = format!("{DATABASE_FILE_NAME}{suffix}");
            let slot = self.create_sqlite_sidecar_slot(Path::new(&file_name))?;
            slots.push(slot);
        }
        self.verify_database_files()?;
        Ok(slots)
    }

    fn create_sqlite_sidecar_slot(&self, relative_path: &Path) -> Result<StoreFileSlot, SessionStoreError> {
        for attempt in 0..SIDECAR_SLOT_RETRIES {
            match self.directory.create_file_slot(relative_path) {
                Ok(slot) => return Ok(slot),
                Err(error) if attempt + 1 < SIDECAR_SLOT_RETRIES && sidecar_slot_may_be_changing(&error) => {
                    thread::yield_now();
                }
                Err(error) => return Err(error),
            }
        }
        Err(SessionStoreError::UnsafePath {
            operation: "stabilize session store SQLite sidecar slot",
        })
    }

    pub(super) fn verify_database_files(&self) -> Result<(), SessionStoreError> {
        self.directory.verify_file_slot(&self.database_slot)
    }
}

fn open_sqlite_connection(path: &Path) -> Result<Connection, SessionStoreError> {
    let connection = Connection::open(path).map_err(|source| db_error("open session store", source))?;
    connection
        .busy_timeout(Duration::from_secs(BUSY_TIMEOUT_SECONDS))
        .map_err(|source| db_error("configure session store busy timeout", source))?;
    connection
        .pragma_update(None, "foreign_keys", true)
        .map_err(|source| db_error("enable session store foreign keys", source))?;
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(|source| db_error("configure session store durability", source))?;
    Ok(connection)
}

pub(super) fn enable_wal(connection: &Connection) -> Result<(), SessionStoreError> {
    let journal_mode: String = connection
        .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
        .map_err(|source| db_error("enable session store WAL mode", source))?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(SessionStoreError::WalUnavailable { actual: journal_mode });
    }
    Ok(())
}

pub(super) fn begin_immediate<'connection>(
    connection: &'connection mut Connection,
    operation: &'static str,
) -> Result<Transaction<'connection>, SessionStoreError> {
    connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|source| db_error(operation, source))
}
