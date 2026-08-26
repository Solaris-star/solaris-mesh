use std::collections::BTreeSet;
use std::io::{self, ErrorKind};

use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::RunId;

use super::{
    InMemoryLedgerState, InMemoryRuntimeLedger, LEDGER_SCHEMA_VERSION, LedgerRecord, SqliteRuntimeLedger,
    allocate_sqlite_sequence, decode_sqlite_record, durability_code, query_sqlite_records, set_sqlite_synchronous,
    sqlite_error, sqlite_sequence_to_u64,
};

pub(super) fn compare_and_append_in_memory(
    ledger: &InMemoryRuntimeLedger,
    run_id: &RunId,
    durability: DurabilityClass,
    record_type: &str,
    identity_fields: &[&str],
    payload: Value,
) -> io::Result<LedgerRecord> {
    let mut state = ledger.state.lock().unwrap_or_else(|error| error.into_inner());
    compare_and_append_in_memory_locked(&mut state, run_id, durability, record_type, identity_fields, payload)
}

pub(super) fn compare_and_append_in_memory_locked(
    state: &mut InMemoryLedgerState,
    run_id: &RunId,
    durability: DurabilityClass,
    record_type: &str,
    identity_fields: &[&str],
    payload: Value,
) -> io::Result<LedgerRecord> {
    require_durable(durability)?;
    let identity = canonical_identity(&payload, identity_fields)?;
    let mut matching = Vec::new();
    for record in state.records.get(run_id).into_iter().flatten() {
        if record.record_type != record_type {
            continue;
        }
        if canonical_identity(&record.payload, identity_fields)? == identity {
            matching.push(record);
        }
    }
    if matching.len() > 1 {
        return Err(duplicate_identity(record_type));
    }
    if let Some(record) = matching.first() {
        return require_same_payload(record_type, record, &payload);
    }

    let seq = state
        .next_sequence
        .checked_add(1)
        .ok_or_else(|| io::Error::other("runtime ledger sequence exhausted"))?;
    let record = LedgerRecord {
        schema_version: LEDGER_SCHEMA_VERSION,
        seq,
        run_id: run_id.clone(),
        timestamp_unix_ms: chrono::Utc::now().timestamp_millis(),
        durability,
        record_type: record_type.to_owned(),
        payload,
    };
    state.next_sequence = seq;
    state.records.entry(run_id.clone()).or_default().push(record.clone());
    Ok(record)
}

pub(super) fn compare_and_append_sqlite<B, A>(
    ledger: &SqliteRuntimeLedger,
    run_id: &RunId,
    durability: DurabilityClass,
    record_type: &str,
    identity_fields: &[&str],
    payload: Value,
    hooks: (B, A),
) -> io::Result<LedgerRecord>
where
    B: FnOnce() -> io::Result<()>,
    A: FnOnce() -> io::Result<()>,
{
    let (before_commit, after_commit) = hooks;
    require_durable(durability)?;
    let mut state = ledger.connection.lock().unwrap_or_else(|error| error.into_inner());
    set_sqlite_synchronous(&mut state, durability)?;
    let transaction = state
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| sqlite_error("begin logical runtime ledger append", error))?;
    let (record, identity) = compare_and_append_sqlite_transaction(
        &transaction,
        run_id,
        durability,
        record_type,
        identity_fields,
        &payload,
    )?;

    if let Err(error) = before_commit() {
        transaction
            .rollback()
            .map_err(|rollback_error| sqlite_error("roll back logical runtime ledger append", rollback_error))?;
        return Err(error);
    }

    if let Err(error) = transaction.commit() {
        let commit_error = sqlite_error("commit logical runtime ledger append", error);
        return recover_committed(
            &state.connection,
            run_id,
            record_type,
            &identity,
            &payload,
            commit_error,
        );
    }
    if let Err(error) = after_commit() {
        return recover_committed(&state.connection, run_id, record_type, &identity, &payload, error);
    }
    Ok(record)
}

pub(super) fn compare_and_append_sqlite_transaction(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    durability: DurabilityClass,
    record_type: &str,
    identity_fields: &[&str],
    payload: &Value,
) -> io::Result<(LedgerRecord, Vec<u8>)> {
    require_durable(durability)?;
    let identity = canonical_identity(payload, identity_fields)?;
    let payload_digest = Sha256::digest(canonical_payload(payload)?).to_vec();
    let append = SqliteLogicalAppend {
        run_id,
        durability,
        record_type,
        identity_fields,
        identity: &identity,
        payload_digest: &payload_digest,
        payload,
    };
    let record = match mapped_record(transaction, run_id, record_type, &identity)? {
        Some(record) => require_same_payload(record_type, &record, payload)?,
        None => backfill_or_insert(transaction, &append)?,
    };
    Ok((record, identity))
}

struct SqliteLogicalAppend<'a> {
    run_id: &'a RunId,
    durability: DurabilityClass,
    record_type: &'a str,
    identity_fields: &'a [&'a str],
    identity: &'a [u8],
    payload_digest: &'a [u8],
    payload: &'a Value,
}

fn backfill_or_insert(transaction: &Transaction<'_>, append: &SqliteLogicalAppend<'_>) -> io::Result<LedgerRecord> {
    let legacy = query_sqlite_records(
        transaction,
        "SELECT sequence, schema_version, run_id, timestamp_unix_ms, durability, record_type, payload
         FROM runtime_ledger_records WHERE run_id = ?1 AND record_type = ?2 ORDER BY sequence",
        params![append.run_id.as_str(), append.record_type],
    )?;
    let mut matching = Vec::new();
    for record in legacy {
        if canonical_identity(&record.payload, append.identity_fields)? == append.identity {
            matching.push(record);
        }
    }
    if matching.len() > 1 {
        return Err(duplicate_identity(append.record_type));
    }
    if let Some(record) = matching.into_iter().next() {
        let record = require_same_payload(append.record_type, &record, append.payload)?;
        insert_mapping(
            transaction,
            append.run_id,
            append.record_type,
            append.identity,
            append.payload_digest,
            record.seq,
        )?;
        return Ok(record);
    }

    let durability_code = durability_code(append.durability)
        .ok_or_else(|| io::Error::other("ephemeral runtime ledger record reached persistence"))?;
    let sequence = allocate_sqlite_sequence(transaction)?;
    let timestamp_unix_ms = chrono::Utc::now().timestamp_millis();
    let encoded_payload = canonical_payload(append.payload)?;
    transaction
        .execute(
            "INSERT INTO runtime_ledger_records
                (sequence, schema_version, run_id, timestamp_unix_ms, durability, record_type, payload)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                sequence,
                i64::from(LEDGER_SCHEMA_VERSION),
                append.run_id.as_str(),
                timestamp_unix_ms,
                durability_code,
                append.record_type,
                encoded_payload,
            ],
        )
        .map_err(|error| sqlite_error("insert logical runtime ledger record", error))?;
    let sequence_u64 = sqlite_sequence_to_u64(sequence)?;
    insert_mapping(
        transaction,
        append.run_id,
        append.record_type,
        append.identity,
        append.payload_digest,
        sequence_u64,
    )?;
    Ok(LedgerRecord {
        schema_version: LEDGER_SCHEMA_VERSION,
        seq: sequence_u64,
        run_id: append.run_id.clone(),
        timestamp_unix_ms,
        durability: append.durability,
        record_type: append.record_type.to_owned(),
        payload: append.payload.clone(),
    })
}

fn insert_mapping(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    record_type: &str,
    identity: &[u8],
    payload_digest: &[u8],
    sequence: u64,
) -> io::Result<()> {
    let sequence = i64::try_from(sequence).map_err(|_| io::Error::other("runtime ledger sequence exceeds SQLite"))?;
    transaction
        .execute(
            "INSERT INTO runtime_ledger_logical_records
                (run_id, record_type, logical_identity, sequence, payload_digest)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![run_id.as_str(), record_type, identity, sequence, payload_digest],
        )
        .map_err(|error| sqlite_error("bind logical runtime ledger identity", error))?;
    Ok(())
}

fn mapped_record(
    transaction: &Transaction<'_>,
    run_id: &RunId,
    record_type: &str,
    identity: &[u8],
) -> io::Result<Option<LedgerRecord>> {
    let mapped = transaction
        .query_row(
            "SELECT r.sequence, r.schema_version, r.run_id, r.timestamp_unix_ms,
                    r.durability, r.record_type, r.payload, l.payload_digest
             FROM runtime_ledger_logical_records l
             JOIN runtime_ledger_records r ON r.sequence = l.sequence
             WHERE l.run_id = ?1 AND l.record_type = ?2 AND l.logical_identity = ?3",
            params![run_id.as_str(), record_type, identity],
            |row| {
                let record = decode_sqlite_record(row)?;
                let digest = row.get::<_, Vec<u8>>(7)?;
                Ok((record, digest))
            },
        )
        .optional()
        .map_err(|error| sqlite_error("query logical runtime ledger identity", error))?;
    let Some((record, stored_digest)) = mapped else {
        return Ok(None);
    };
    let actual_digest = Sha256::digest(canonical_payload(&record.payload)?).to_vec();
    if actual_digest != stored_digest {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "logical runtime ledger payload digest does not match its record",
        ));
    }
    Ok(Some(record))
}

fn recover_committed(
    connection: &rusqlite::Connection,
    run_id: &RunId,
    record_type: &str,
    identity: &[u8],
    payload: &Value,
    original_error: io::Error,
) -> io::Result<LedgerRecord> {
    let transaction = connection
        .unchecked_transaction()
        .map_err(|error| sqlite_error("begin logical runtime ledger recovery", error))?;
    let recovered = mapped_record(&transaction, run_id, record_type, identity)?;
    transaction
        .commit()
        .map_err(|error| sqlite_error("finish logical runtime ledger recovery", error))?;
    match recovered {
        Some(record) => require_same_payload(record_type, &record, payload),
        None => Err(original_error),
    }
}

fn canonical_identity(payload: &Value, identity_fields: &[&str]) -> io::Result<Vec<u8>> {
    if identity_fields.is_empty() {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "logical runtime ledger identity must contain at least one field",
        ));
    }
    let unique = identity_fields.iter().copied().collect::<BTreeSet<_>>();
    if unique.len() != identity_fields.len() {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "logical runtime ledger identity fields contain a duplicate",
        ));
    }
    let object = payload.as_object().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidInput,
            "logical runtime ledger payload must be an object",
        )
    })?;
    let mut identity = Map::new();
    for field in unique {
        let value = object.get(field).ok_or_else(|| {
            io::Error::new(
                ErrorKind::InvalidData,
                format!("logical runtime ledger payload is missing identity field {field}"),
            )
        })?;
        identity.insert(field.to_owned(), value.clone());
    }
    serde_json::to_vec(&Value::Object(identity)).map_err(io::Error::other)
}

fn canonical_payload(payload: &Value) -> io::Result<Vec<u8>> {
    serde_json::to_vec(payload).map_err(io::Error::other)
}

fn require_durable(durability: DurabilityClass) -> io::Result<()> {
    if durability == DurabilityClass::Ephemeral {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "logical runtime ledger append requires durable storage",
        ));
    }
    Ok(())
}

fn require_same_payload(record_type: &str, record: &LedgerRecord, payload: &Value) -> io::Result<LedgerRecord> {
    if record.payload == *payload {
        return Ok(record.clone());
    }
    Err(io::Error::new(
        ErrorKind::AlreadyExists,
        format!("{record_type} identity is bound to different content"),
    ))
}

fn duplicate_identity(record_type: &str) -> io::Error {
    io::Error::new(
        ErrorKind::InvalidData,
        format!("{record_type} identity is bound to multiple durable records"),
    )
}
