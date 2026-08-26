#[cfg(test)]
use std::fs;
#[cfg(test)]
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use chrono::Utc;
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use sha2::{Digest, Sha256};

#[cfg(test)]
use super::io_error;
use super::{
    MAX_LEGACY_SESSION_BYTES, SessionStore, SessionStoreError, db_error, encode_session, row_exists,
    sync_session_run_reference,
};
use crate::session::Session;

struct ImportSource {
    path: PathBuf,
    path_digest: Vec<u8>,
    content: Vec<u8>,
    content_digest: Vec<u8>,
}

pub(super) fn import_legacy_json(store: &SessionStore) -> Result<(), SessionStoreError> {
    for relative_path in discover_sources(store)? {
        let path_digest = store.directory.source_path_digest(&relative_path)?;
        if source_completed(store, &path_digest)? {
            continue;
        }
        let Some(opened) = store.directory.read_source(&relative_path, MAX_LEGACY_SESSION_BYTES)? else {
            continue;
        };
        let path = store.directory.display_path(&relative_path)?;
        let session: Session = match serde_json::from_slice(&opened.content) {
            Ok(session) => session,
            Err(error) => {
                tracing::warn!(
                    target: "solaris_agent",
                    path = %path.display(),
                    error = %error,
                    "skipping invalid legacy session JSON"
                );
                continue;
            }
        };
        let content_digest = Sha256::digest(&opened.content).to_vec();
        import_source(
            store,
            ImportSource {
                path,
                path_digest,
                content: opened.content,
                content_digest,
            },
            &session,
        )?;
    }
    Ok(())
}

fn discover_sources(store: &SessionStore) -> Result<Vec<PathBuf>, SessionStoreError> {
    let mut sources = Vec::new();
    for name in store.directory.list_root_names()? {
        let path = PathBuf::from(name);
        if path == Path::new("index.json") || path.extension().and_then(|extension| extension.to_str()) != Some("json")
        {
            continue;
        }
        sources.push(path);
    }

    let sessions_directory = Path::new("sessions");
    if let Some(children) = store.directory.list_directory_names(sessions_directory)? {
        for child in children {
            let child_directory = sessions_directory.join(child);
            if store.directory.relative_is_directory(&child_directory)? {
                sources.push(child_directory.join("state.json"));
            }
        }
    }
    sources.sort();
    sources.dedup();
    Ok(sources)
}

fn source_completed(store: &SessionStore, path_digest: &[u8]) -> Result<bool, SessionStoreError> {
    let connection = store.open_connection()?;
    let completed: i64 = connection
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM session_migration_sources WHERE source_digest = ?1
             )",
            params![path_digest],
            |row| row.get(0),
        )
        .map_err(|error| db_error("check legacy session import source before read", error))?;
    connection.verify_storage_slots()?;
    Ok(completed != 0)
}

#[cfg(test)]
pub(super) fn metadata_if_exists(path: &Path) -> Result<Option<fs::Metadata>, SessionStoreError> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(source) if source.kind() == ErrorKind::NotFound => Ok(None),
        Err(source) => Err(io_error("inspect legacy session source", source)),
    }
}

fn import_source(store: &SessionStore, source: ImportSource, session: &Session) -> Result<(), SessionStoreError> {
    let mut connection = store.open_connection()?;
    let storage = connection.storage_guard();
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| db_error("begin legacy session import", error))?;
    let completed: Option<i64> = transaction
        .query_row(
            "SELECT 1 FROM session_migration_sources WHERE source_digest = ?1",
            params![&source.path_digest],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| db_error("recheck legacy session import source", error))?;
    if completed.is_some() {
        storage.verify()?;
        return Ok(());
    }

    let tombstoned = row_exists(
        &transaction,
        "SELECT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id = ?1)",
        &session.id,
        "check imported session tombstone",
    )?;
    let duplicate = row_exists(
        &transaction,
        "SELECT EXISTS(SELECT 1 FROM sessions WHERE session_id = ?1)",
        &session.id,
        "check imported session duplicate",
    )?;
    let outcome = if tombstoned {
        "tombstoned"
    } else if duplicate {
        "duplicate"
    } else {
        let encoded = encode_session(session, "encode imported session")?;
        transaction
            .execute(
                "INSERT INTO sessions
                    (session_id, state_json, revision, run_id, created_at_ms, updated_at_ms)
                 VALUES (?1, ?2, 0, ?3, ?4, ?5)",
                params![
                    &session.id,
                    encoded,
                    session.run_id.as_deref(),
                    session.created_at.timestamp_millis(),
                    session.updated_at.timestamp_millis(),
                ],
            )
            .map_err(|error| db_error("insert imported session", error))?;
        sync_session_run_reference(
            &transaction,
            &session.id,
            session.run_id.as_deref().filter(|run_id| !run_id.trim().is_empty()),
            session.created_at.timestamp_millis(),
        )?;
        "inserted"
    };
    let content_length = i64::try_from(source.content.len()).map_err(|_| SessionStoreError::LegacySourceTooLarge {
        max_bytes: MAX_LEGACY_SESSION_BYTES,
    })?;
    transaction
        .execute(
            "INSERT INTO session_migration_sources
                (source_digest, source_path, content_digest, content_length, completed_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                &source.path_digest,
                source.path.to_string_lossy().as_ref(),
                &source.content_digest,
                content_length,
                Utc::now().timestamp_millis(),
            ],
        )
        .map_err(|error| db_error("record legacy session import source", error))?;
    transaction
        .execute(
            "INSERT INTO session_migration_imports (source_digest, session_id, outcome)
             VALUES (?1, ?2, ?3)",
            params![&source.path_digest, &session.id, outcome],
        )
        .map_err(|error| db_error("record legacy session import outcome", error))?;
    storage.verify()?;
    transaction
        .commit()
        .map_err(|error| db_error("commit legacy session import", error))?;
    connection.verify_storage_slots()
}
