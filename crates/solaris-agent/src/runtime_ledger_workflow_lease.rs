use std::io::{self, ErrorKind};

use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::Value;
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::RunId;

use crate::session::store::{DEFAULT_HEARTBEAT_SECONDS, DEFAULT_LEASE_SECONDS};

use super::runtime_ledger_unique::{compare_and_append_in_memory_locked, compare_and_append_sqlite_transaction};
use super::{
    InMemoryLedgerState, InMemoryRuntimeLedger, LEDGER_SCHEMA_VERSION, LedgerRecord, SqliteRuntimeLedger,
    WorkflowMutationLease, WorkflowRestoreCommit, allocate_sqlite_sequence, durability_code, set_sqlite_synchronous,
    sqlite_error, sqlite_sequence_to_u64,
};

pub(crate) const WORKFLOW_MUTATION_HEARTBEAT_MILLIS: i64 = DEFAULT_HEARTBEAT_SECONDS * 1_000;
const WORKFLOW_MUTATION_LEASE_MILLIS: i64 = DEFAULT_LEASE_SECONDS * 1_000;

#[derive(Clone)]
pub(super) struct InMemoryWorkflowMutationLease {
    owner_id: Option<String>,
    epoch: u64,
    heartbeat_at_unix_ms: i64,
    expires_at_unix_ms: i64,
    observed_sequence: u64,
    committed_sequence: u64,
}

pub(super) fn acquire_workflow_mutation_lease_in_memory(
    ledger: &InMemoryRuntimeLedger,
    run_id: &RunId,
    owner_id: &str,
    now_unix_ms: i64,
) -> io::Result<WorkflowMutationLease> {
    require_owner(owner_id)?;
    let mut state = ledger.state.lock().unwrap_or_else(|error| error.into_inner());
    let observed_sequence = state
        .records
        .get(run_id)
        .and_then(|records| records.last())
        .map_or(0, |record| record.seq);
    let existing = state.workflow_mutation_leases.get(run_id).cloned();
    if let Some(existing) = existing.as_ref() {
        if existing.owner_id.as_deref().is_some_and(|owner| owner != owner_id)
            && existing.expires_at_unix_ms > now_unix_ms
        {
            return Err(lease_contended(run_id));
        }
        reject_clock_rollback(existing.heartbeat_at_unix_ms, now_unix_ms)?;
    }
    let epoch = existing
        .as_ref()
        .map_or(0, |lease| lease.epoch)
        .checked_add(1)
        .ok_or_else(|| io::Error::other("Workflow mutation lease epoch exhausted"))?;
    let expires_at_unix_ms = lease_expiry(now_unix_ms);
    state.workflow_mutation_leases.insert(
        run_id.clone(),
        InMemoryWorkflowMutationLease {
            owner_id: Some(owner_id.to_owned()),
            epoch,
            heartbeat_at_unix_ms: now_unix_ms,
            expires_at_unix_ms,
            observed_sequence,
            committed_sequence: existing.as_ref().map_or(0, |lease| lease.committed_sequence),
        },
    );
    Ok(WorkflowMutationLease {
        run_id: run_id.clone(),
        owner_id: owner_id.to_owned(),
        epoch,
        expires_at_unix_ms,
        observed_sequence,
    })
}

pub(super) fn renew_workflow_mutation_lease_in_memory(
    ledger: &InMemoryRuntimeLedger,
    lease: &WorkflowMutationLease,
    now_unix_ms: i64,
) -> io::Result<WorkflowMutationLease> {
    let mut state = ledger.state.lock().unwrap_or_else(|error| error.into_inner());
    let current = state
        .workflow_mutation_leases
        .get_mut(&lease.run_id)
        .ok_or_else(|| lease_lost(&lease.run_id))?;
    require_current(current, lease, now_unix_ms)?;
    current.heartbeat_at_unix_ms = now_unix_ms;
    current.expires_at_unix_ms = lease_expiry(now_unix_ms);
    Ok(WorkflowMutationLease {
        expires_at_unix_ms: current.expires_at_unix_ms,
        observed_sequence: current.observed_sequence,
        ..lease.clone()
    })
}

pub(super) fn commit_workflow_restore_in_memory(
    ledger: &InMemoryRuntimeLedger,
    lease: &WorkflowMutationLease,
    expected_sequence: u64,
    now_unix_ms: i64,
) -> io::Result<WorkflowRestoreCommit> {
    let mut state = ledger.state.lock().unwrap_or_else(|error| error.into_inner());
    let current_sequence = state
        .records
        .get(&lease.run_id)
        .and_then(|records| records.last())
        .map_or(0, |record| record.seq);
    let current = state
        .workflow_mutation_leases
        .get_mut(&lease.run_id)
        .ok_or_else(|| lease_lost(&lease.run_id))?;
    require_current(current, lease, now_unix_ms)?;
    current.heartbeat_at_unix_ms = now_unix_ms;
    current.expires_at_unix_ms = lease_expiry(now_unix_ms);
    current.observed_sequence = current_sequence;
    if current_sequence != expected_sequence {
        return Ok(WorkflowRestoreCommit::Stale { current_sequence });
    }
    current.committed_sequence = expected_sequence;
    Ok(WorkflowRestoreCommit::Current)
}

pub(super) fn release_workflow_mutation_lease_in_memory(
    ledger: &InMemoryRuntimeLedger,
    lease: &WorkflowMutationLease,
) -> io::Result<()> {
    let mut state = ledger.state.lock().unwrap_or_else(|error| error.into_inner());
    let current = state
        .workflow_mutation_leases
        .get_mut(&lease.run_id)
        .ok_or_else(|| lease_lost(&lease.run_id))?;
    if current.owner_id.as_deref() != Some(lease.owner_id.as_str()) || current.epoch != lease.epoch {
        return Err(lease_lost(&lease.run_id));
    }
    current.owner_id = None;
    current.heartbeat_at_unix_ms = 0;
    current.expires_at_unix_ms = 0;
    Ok(())
}

pub(super) fn append_under_workflow_lease_in_memory(
    ledger: &InMemoryRuntimeLedger,
    lease: &WorkflowMutationLease,
    now_unix_ms: i64,
    durability: DurabilityClass,
    record_type: &str,
    payload: Value,
) -> io::Result<LedgerRecord> {
    require_durable_workflow_record(durability)?;
    let mut state = ledger.state.lock().unwrap_or_else(|error| error.into_inner());
    require_current(
        state
            .workflow_mutation_leases
            .get(&lease.run_id)
            .ok_or_else(|| lease_lost(&lease.run_id))?,
        lease,
        now_unix_ms,
    )?;
    let seq = state
        .next_sequence
        .checked_add(1)
        .ok_or_else(|| io::Error::other("runtime ledger sequence exhausted"))?;
    let record = LedgerRecord {
        schema_version: LEDGER_SCHEMA_VERSION,
        seq,
        run_id: lease.run_id.clone(),
        timestamp_unix_ms: now_unix_ms,
        durability,
        record_type: record_type.to_owned(),
        payload,
    };
    state.next_sequence = seq;
    state
        .records
        .entry(lease.run_id.clone())
        .or_default()
        .push(record.clone());
    state
        .workflow_mutation_leases
        .get_mut(&lease.run_id)
        .expect("Workflow mutation lease was validated under the same lock")
        .observed_sequence = seq;
    Ok(record)
}

pub(super) fn compare_and_append_under_workflow_lease_in_memory(
    ledger: &InMemoryRuntimeLedger,
    lease: &WorkflowMutationLease,
    now_unix_ms: i64,
    durability: DurabilityClass,
    record_type: &str,
    identity_fields: &[&str],
    payload: Value,
) -> io::Result<LedgerRecord> {
    let mut state = ledger.state.lock().unwrap_or_else(|error| error.into_inner());
    require_current(
        state
            .workflow_mutation_leases
            .get(&lease.run_id)
            .ok_or_else(|| lease_lost(&lease.run_id))?,
        lease,
        now_unix_ms,
    )?;
    let record = compare_and_append_in_memory_locked(
        &mut state,
        &lease.run_id,
        durability,
        record_type,
        identity_fields,
        payload,
    )?;
    let observed_sequence = state
        .records
        .get(&lease.run_id)
        .and_then(|records| records.last())
        .map_or(0, |record| record.seq);
    state
        .workflow_mutation_leases
        .get_mut(&lease.run_id)
        .expect("Workflow mutation lease was validated under the same lock")
        .observed_sequence = observed_sequence;
    Ok(record)
}

pub(super) fn acquire_workflow_mutation_lease_sqlite(
    ledger: &SqliteRuntimeLedger,
    run_id: &RunId,
    owner_id: &str,
    now_unix_ms: i64,
) -> io::Result<WorkflowMutationLease> {
    require_owner(owner_id)?;
    let mut state = ledger.connection.lock().unwrap_or_else(|error| error.into_inner());
    let transaction = state
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| sqlite_error("begin Workflow mutation lease acquisition", error))?;
    let observed_sequence = run_sequence(&transaction, run_id)?;
    let existing = load_sqlite_lease(&transaction, run_id)?;
    if let Some(existing) = existing.as_ref() {
        if existing.owner_id.as_deref().is_some_and(|owner| owner != owner_id)
            && existing.expires_at_unix_ms > now_unix_ms
        {
            return Err(lease_contended(run_id));
        }
        reject_clock_rollback(existing.heartbeat_at_unix_ms, now_unix_ms)?;
    }
    let epoch = existing
        .as_ref()
        .map_or(0, |lease| lease.epoch)
        .checked_add(1)
        .ok_or_else(|| io::Error::other("Workflow mutation lease epoch exhausted"))?;
    let committed_sequence = existing.as_ref().map_or(0, |lease| lease.committed_sequence);
    let expires_at_unix_ms = lease_expiry(now_unix_ms);
    transaction
        .execute(
            "INSERT INTO runtime_ledger_workflow_mutation_leases
                (run_id, owner_id, epoch, heartbeat_at_ms, expires_at_ms, observed_sequence, committed_sequence)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(run_id) DO UPDATE SET
                owner_id = excluded.owner_id,
                epoch = excluded.epoch,
                heartbeat_at_ms = excluded.heartbeat_at_ms,
                expires_at_ms = excluded.expires_at_ms,
                observed_sequence = excluded.observed_sequence,
                committed_sequence = excluded.committed_sequence",
            params![
                run_id.as_str(),
                owner_id,
                u64_to_sqlite(epoch, "Workflow mutation lease epoch")?,
                now_unix_ms,
                expires_at_unix_ms,
                u64_to_sqlite(observed_sequence, "Workflow mutation observed sequence")?,
                u64_to_sqlite(committed_sequence, "Workflow mutation committed sequence")?,
            ],
        )
        .map_err(|error| sqlite_error("acquire Workflow mutation lease", error))?;
    transaction
        .commit()
        .map_err(|error| sqlite_error("commit Workflow mutation lease acquisition", error))?;
    Ok(WorkflowMutationLease {
        run_id: run_id.clone(),
        owner_id: owner_id.to_owned(),
        epoch,
        expires_at_unix_ms,
        observed_sequence,
    })
}

pub(super) fn renew_workflow_mutation_lease_sqlite(
    ledger: &SqliteRuntimeLedger,
    lease: &WorkflowMutationLease,
    now_unix_ms: i64,
) -> io::Result<WorkflowMutationLease> {
    let mut state = ledger.connection.lock().unwrap_or_else(|error| error.into_inner());
    let transaction = state
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| sqlite_error("begin Workflow mutation lease renewal", error))?;
    let current = require_sqlite_current(&transaction, lease, now_unix_ms)?;
    let expires_at_unix_ms = lease_expiry(now_unix_ms);
    update_heartbeat(&transaction, lease, now_unix_ms, expires_at_unix_ms)?;
    transaction
        .commit()
        .map_err(|error| sqlite_error("commit Workflow mutation lease renewal", error))?;
    Ok(WorkflowMutationLease {
        expires_at_unix_ms,
        observed_sequence: current.observed_sequence,
        ..lease.clone()
    })
}

pub(super) fn commit_workflow_restore_sqlite(
    ledger: &SqliteRuntimeLedger,
    lease: &WorkflowMutationLease,
    expected_sequence: u64,
    now_unix_ms: i64,
) -> io::Result<WorkflowRestoreCommit> {
    let mut state = ledger.connection.lock().unwrap_or_else(|error| error.into_inner());
    let transaction = state
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| sqlite_error("begin Workflow restore high-water commit", error))?;
    require_sqlite_current(&transaction, lease, now_unix_ms)?;
    let current_sequence = run_sequence(&transaction, &lease.run_id)?;
    let expires_at_unix_ms = lease_expiry(now_unix_ms);
    let committed = if current_sequence == expected_sequence {
        expected_sequence
    } else {
        load_sqlite_lease(&transaction, &lease.run_id)?.map_or(0, |current| current.committed_sequence)
    };
    let updated = transaction
        .execute(
            "UPDATE runtime_ledger_workflow_mutation_leases
             SET heartbeat_at_ms = ?1, expires_at_ms = ?2,
                 observed_sequence = ?3, committed_sequence = ?4
             WHERE run_id = ?5 AND owner_id = ?6 AND epoch = ?7",
            params![
                now_unix_ms,
                expires_at_unix_ms,
                u64_to_sqlite(current_sequence, "Workflow mutation observed sequence")?,
                u64_to_sqlite(committed, "Workflow mutation committed sequence")?,
                lease.run_id.as_str(),
                lease.owner_id,
                u64_to_sqlite(lease.epoch, "Workflow mutation lease epoch")?,
            ],
        )
        .map_err(|error| sqlite_error("commit Workflow restore high-water", error))?;
    if updated != 1 {
        return Err(lease_lost(&lease.run_id));
    }
    transaction
        .commit()
        .map_err(|error| sqlite_error("finish Workflow restore high-water commit", error))?;
    Ok(if current_sequence == expected_sequence {
        WorkflowRestoreCommit::Current
    } else {
        WorkflowRestoreCommit::Stale { current_sequence }
    })
}

pub(super) fn release_workflow_mutation_lease_sqlite(
    ledger: &SqliteRuntimeLedger,
    lease: &WorkflowMutationLease,
) -> io::Result<()> {
    let mut state = ledger.connection.lock().unwrap_or_else(|error| error.into_inner());
    let transaction = state
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| sqlite_error("begin Workflow mutation lease release", error))?;
    let updated = transaction
        .execute(
            "UPDATE runtime_ledger_workflow_mutation_leases
             SET owner_id = NULL, heartbeat_at_ms = 0, expires_at_ms = 0
             WHERE run_id = ?1 AND owner_id = ?2 AND epoch = ?3",
            params![
                lease.run_id.as_str(),
                lease.owner_id,
                u64_to_sqlite(lease.epoch, "Workflow mutation lease epoch")?,
            ],
        )
        .map_err(|error| sqlite_error("release Workflow mutation lease", error))?;
    if updated != 1 {
        return Err(lease_lost(&lease.run_id));
    }
    transaction
        .commit()
        .map_err(|error| sqlite_error("commit Workflow mutation lease release", error))
}

pub(super) fn append_under_workflow_lease_sqlite(
    ledger: &SqliteRuntimeLedger,
    lease: &WorkflowMutationLease,
    now_unix_ms: i64,
    durability: DurabilityClass,
    record_type: &str,
    payload: Value,
) -> io::Result<LedgerRecord> {
    require_durable_workflow_record(durability)?;
    let encoded_payload = serde_json::to_vec(&payload).map_err(io::Error::other)?;
    let mut state = ledger.connection.lock().unwrap_or_else(|error| error.into_inner());
    set_sqlite_synchronous(&mut state, durability)?;
    let transaction = state
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| sqlite_error("begin fenced Workflow ledger append", error))?;
    require_sqlite_current(&transaction, lease, now_unix_ms)?;
    let sequence = allocate_sqlite_sequence(&transaction)?;
    let durability_code =
        durability_code(durability).ok_or_else(|| io::Error::other("ephemeral Workflow record reached persistence"))?;
    transaction
        .execute(
            "INSERT INTO runtime_ledger_records
                (sequence, schema_version, run_id, timestamp_unix_ms, durability, record_type, payload)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                sequence,
                i64::from(LEDGER_SCHEMA_VERSION),
                lease.run_id.as_str(),
                now_unix_ms,
                durability_code,
                record_type,
                encoded_payload,
            ],
        )
        .map_err(|error| sqlite_error("insert fenced Workflow ledger record", error))?;
    advance_sqlite_observed_sequence(&transaction, lease, sqlite_sequence_to_u64(sequence)?)?;
    transaction
        .commit()
        .map_err(|error| sqlite_error("commit fenced Workflow ledger append", error))?;
    Ok(LedgerRecord {
        schema_version: LEDGER_SCHEMA_VERSION,
        seq: sqlite_sequence_to_u64(sequence)?,
        run_id: lease.run_id.clone(),
        timestamp_unix_ms: now_unix_ms,
        durability,
        record_type: record_type.to_owned(),
        payload,
    })
}

pub(super) fn compare_and_append_under_workflow_lease_sqlite(
    ledger: &SqliteRuntimeLedger,
    lease: &WorkflowMutationLease,
    now_unix_ms: i64,
    durability: DurabilityClass,
    record_type: &str,
    identity_fields: &[&str],
    payload: Value,
) -> io::Result<LedgerRecord> {
    require_durable_workflow_record(durability)?;
    let mut state = ledger.connection.lock().unwrap_or_else(|error| error.into_inner());
    set_sqlite_synchronous(&mut state, durability)?;
    let transaction = state
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| sqlite_error("begin fenced logical Workflow append", error))?;
    require_sqlite_current(&transaction, lease, now_unix_ms)?;
    let (record, _) = compare_and_append_sqlite_transaction(
        &transaction,
        &lease.run_id,
        durability,
        record_type,
        identity_fields,
        &payload,
    )?;
    let observed_sequence = run_sequence(&transaction, &lease.run_id)?;
    advance_sqlite_observed_sequence(&transaction, lease, observed_sequence)?;
    transaction
        .commit()
        .map_err(|error| sqlite_error("commit fenced logical Workflow append", error))?;
    Ok(record)
}

#[derive(Clone)]
struct LoadedWorkflowMutationLease {
    owner_id: Option<String>,
    epoch: u64,
    heartbeat_at_unix_ms: i64,
    expires_at_unix_ms: i64,
    observed_sequence: u64,
    committed_sequence: u64,
}

fn load_sqlite_lease(transaction: &Transaction<'_>, run_id: &RunId) -> io::Result<Option<LoadedWorkflowMutationLease>> {
    transaction
        .query_row(
            "SELECT owner_id, epoch, heartbeat_at_ms, expires_at_ms, observed_sequence, committed_sequence
             FROM runtime_ledger_workflow_mutation_leases WHERE run_id = ?1",
            [run_id.as_str()],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            },
        )
        .optional()
        .map_err(|error| sqlite_error("load Workflow mutation lease", error))?
        .map(|(owner_id, epoch, heartbeat, expires, observed, committed)| {
            Ok(LoadedWorkflowMutationLease {
                owner_id,
                epoch: sqlite_sequence_to_u64(epoch)?,
                heartbeat_at_unix_ms: heartbeat,
                expires_at_unix_ms: expires,
                observed_sequence: sqlite_sequence_to_u64(observed)?,
                committed_sequence: sqlite_sequence_to_u64(committed)?,
            })
        })
        .transpose()
}

fn require_sqlite_current(
    transaction: &Transaction<'_>,
    lease: &WorkflowMutationLease,
    now_unix_ms: i64,
) -> io::Result<LoadedWorkflowMutationLease> {
    let current = load_sqlite_lease(transaction, &lease.run_id)?.ok_or_else(|| lease_lost(&lease.run_id))?;
    reject_clock_rollback(current.heartbeat_at_unix_ms, now_unix_ms)?;
    if current.owner_id.as_deref() != Some(lease.owner_id.as_str())
        || current.epoch != lease.epoch
        || current.expires_at_unix_ms <= now_unix_ms
    {
        return Err(lease_lost(&lease.run_id));
    }
    Ok(current)
}

fn update_heartbeat(
    transaction: &Transaction<'_>,
    lease: &WorkflowMutationLease,
    now_unix_ms: i64,
    expires_at_unix_ms: i64,
) -> io::Result<()> {
    let updated = transaction
        .execute(
            "UPDATE runtime_ledger_workflow_mutation_leases
             SET heartbeat_at_ms = ?1, expires_at_ms = ?2
             WHERE run_id = ?3 AND owner_id = ?4 AND epoch = ?5",
            params![
                now_unix_ms,
                expires_at_unix_ms,
                lease.run_id.as_str(),
                lease.owner_id,
                u64_to_sqlite(lease.epoch, "Workflow mutation lease epoch")?,
            ],
        )
        .map_err(|error| sqlite_error("renew Workflow mutation lease", error))?;
    if updated != 1 {
        return Err(lease_lost(&lease.run_id));
    }
    Ok(())
}

fn advance_sqlite_observed_sequence(
    transaction: &Transaction<'_>,
    lease: &WorkflowMutationLease,
    observed_sequence: u64,
) -> io::Result<()> {
    let updated = transaction
        .execute(
            "UPDATE runtime_ledger_workflow_mutation_leases
             SET observed_sequence = ?1
             WHERE run_id = ?2 AND owner_id = ?3 AND epoch = ?4",
            params![
                u64_to_sqlite(observed_sequence, "Workflow mutation observed sequence")?,
                lease.run_id.as_str(),
                lease.owner_id,
                u64_to_sqlite(lease.epoch, "Workflow mutation lease epoch")?,
            ],
        )
        .map_err(|error| sqlite_error("advance Workflow mutation observed sequence", error))?;
    if updated != 1 {
        return Err(lease_lost(&lease.run_id));
    }
    Ok(())
}

fn run_sequence(transaction: &Transaction<'_>, run_id: &RunId) -> io::Result<u64> {
    let sequence: Option<i64> = transaction
        .query_row(
            "SELECT MAX(sequence) FROM runtime_ledger_records WHERE run_id = ?1",
            [run_id.as_str()],
            |row| row.get(0),
        )
        .map_err(|error| sqlite_error("read Workflow mutation high-water", error))?;
    sequence.map_or(Ok(0), sqlite_sequence_to_u64)
}

pub(super) fn validate_workflow_mutation_lease_in_memory(
    state: &InMemoryLedgerState,
    lease: &WorkflowMutationLease,
    now_unix_ms: i64,
) -> io::Result<()> {
    require_current(
        state
            .workflow_mutation_leases
            .get(&lease.run_id)
            .ok_or_else(|| lease_lost(&lease.run_id))?,
        lease,
        now_unix_ms,
    )
}

pub(super) fn advance_workflow_mutation_lease_in_memory(
    state: &mut InMemoryLedgerState,
    lease: &WorkflowMutationLease,
    now_unix_ms: i64,
    observed_sequence: u64,
) {
    let current = state
        .workflow_mutation_leases
        .get_mut(&lease.run_id)
        .expect("Workflow mutation lease was validated under the same ledger lock");
    debug_assert_eq!(current.owner_id.as_deref(), Some(lease.owner_id.as_str()));
    debug_assert_eq!(current.epoch, lease.epoch);
    current.heartbeat_at_unix_ms = now_unix_ms;
    current.expires_at_unix_ms = lease_expiry(now_unix_ms);
    current.observed_sequence = observed_sequence;
}

pub(super) fn validate_workflow_mutation_lease_sqlite_transaction(
    transaction: &Transaction<'_>,
    lease: &WorkflowMutationLease,
    now_unix_ms: i64,
) -> io::Result<()> {
    require_sqlite_current(transaction, lease, now_unix_ms).map(|_| ())
}

pub(super) fn advance_workflow_mutation_lease_sqlite_transaction(
    transaction: &Transaction<'_>,
    lease: &WorkflowMutationLease,
    now_unix_ms: i64,
    observed_sequence: u64,
) -> io::Result<()> {
    update_heartbeat(transaction, lease, now_unix_ms, lease_expiry(now_unix_ms))?;
    advance_sqlite_observed_sequence(transaction, lease, observed_sequence)
}

fn require_current(
    current: &InMemoryWorkflowMutationLease,
    lease: &WorkflowMutationLease,
    now_unix_ms: i64,
) -> io::Result<()> {
    reject_clock_rollback(current.heartbeat_at_unix_ms, now_unix_ms)?;
    if current.owner_id.as_deref() != Some(lease.owner_id.as_str())
        || current.epoch != lease.epoch
        || current.expires_at_unix_ms <= now_unix_ms
    {
        return Err(lease_lost(&lease.run_id));
    }
    Ok(())
}

fn require_owner(owner_id: &str) -> io::Result<()> {
    if owner_id.is_empty() {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "Workflow mutation lease owner must not be empty",
        ));
    }
    Ok(())
}

fn require_durable_workflow_record(durability: DurabilityClass) -> io::Result<()> {
    if durability == DurabilityClass::Ephemeral {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "Workflow mutation lease cannot protect an ephemeral record",
        ));
    }
    Ok(())
}

fn reject_clock_rollback(previous_ms: i64, now_ms: i64) -> io::Result<()> {
    if now_ms < previous_ms {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "system clock moved backwards while a Workflow mutation lease was active",
        ));
    }
    Ok(())
}

fn lease_expiry(now_unix_ms: i64) -> i64 {
    now_unix_ms.saturating_add(WORKFLOW_MUTATION_LEASE_MILLIS)
}

fn lease_contended(run_id: &RunId) -> io::Error {
    io::Error::new(
        ErrorKind::WouldBlock,
        format!("Workflow Run {run_id} has an active mutation lease"),
    )
}

fn lease_lost(run_id: &RunId) -> io::Error {
    io::Error::new(
        ErrorKind::PermissionDenied,
        format!("Workflow Run {run_id} mutation lease was lost"),
    )
}

fn u64_to_sqlite(value: u64, label: &str) -> io::Result<i64> {
    i64::try_from(value).map_err(|_| io::Error::other(format!("{label} exceeds SQLite")))
}
