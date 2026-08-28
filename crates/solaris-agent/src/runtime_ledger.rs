use std::collections::HashMap;
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use rusqlite::types::Type;
use rusqlite::{Connection, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::RunId;
use solaris_types::plan::PlanArtifact;
use solaris_types::runtime::TaskRecord;

#[path = "runtime_ledger_export.rs"]
mod runtime_ledger_export;
#[path = "runtime_ledger_jsonl.rs"]
mod runtime_ledger_jsonl;
#[path = "runtime_ledger_migration.rs"]
mod runtime_ledger_migration;
#[path = "runtime_ledger_mutation.rs"]
mod runtime_ledger_mutation;
#[path = "runtime_ledger_plan.rs"]
mod runtime_ledger_plan;
#[path = "runtime_ledger_task_admission.rs"]
mod runtime_ledger_task_admission;
#[cfg(test)]
#[path = "runtime_ledger_test_support.rs"]
mod runtime_ledger_test_support;
#[path = "runtime_ledger_unique.rs"]
mod runtime_ledger_unique;
#[path = "runtime_ledger_workflow_lease.rs"]
mod runtime_ledger_workflow_lease;

use runtime_ledger_export::{
    ProtectedExportPath, export_sqlite_records_atomically, resolve_ledger_path, retain_protected_export_path,
    retain_protected_export_paths,
};
#[cfg(test)]
use runtime_ledger_export::{
    export_records_atomically_with, export_records_atomically_with_revalidation_hook,
    export_records_atomically_with_steps,
};
pub use runtime_ledger_jsonl::JsonlRuntimeLedger;
#[cfg(test)]
use runtime_ledger_jsonl::{JsonlLedgerState, JsonlWriter};
use runtime_ledger_migration::{import_jsonl_explicit, import_jsonl_once, initialize_sqlite_schema};
pub use runtime_ledger_mutation::RunMutationCoordinator;
use runtime_ledger_plan::{record_plan_artifact_default, record_plan_artifact_sqlite};
use runtime_ledger_task_admission::{
    admit_collaboration_tasks_in_memory, admit_collaboration_tasks_sqlite, admit_tasks_and_append_in_memory,
    admit_tasks_and_append_sqlite, admit_tasks_and_append_under_workflow_lease_in_memory,
    admit_tasks_and_append_under_workflow_lease_sqlite,
};
#[cfg(test)]
pub(crate) use runtime_ledger_test_support::forward_workflow_mutation_lease;
#[cfg(test)]
pub(crate) use runtime_ledger_test_support::{forward_compare_and_append, unsupported_compare_and_append};
use runtime_ledger_unique::{compare_and_append_in_memory, compare_and_append_sqlite};
pub(crate) use runtime_ledger_workflow_lease::WORKFLOW_MUTATION_HEARTBEAT_MILLIS;
use runtime_ledger_workflow_lease::{
    InMemoryWorkflowMutationLease, acquire_workflow_mutation_lease_in_memory, acquire_workflow_mutation_lease_sqlite,
    commit_workflow_restore_in_memory, commit_workflow_restore_sqlite, release_workflow_mutation_lease_in_memory,
    release_workflow_mutation_lease_sqlite, renew_workflow_mutation_lease_in_memory,
    renew_workflow_mutation_lease_sqlite,
};

pub const LEDGER_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LedgerRecord {
    pub schema_version: u32,
    pub seq: u64,
    pub run_id: RunId,
    pub timestamp_unix_ms: i64,
    pub durability: DurabilityClass,
    pub record_type: String,
    pub payload: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalAppendCapability {
    Unsupported,
    ProcessLocal,
    CrossProcess,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowMutationLease {
    pub run_id: RunId,
    pub owner_id: String,
    pub epoch: u64,
    pub expires_at_unix_ms: i64,
    pub observed_sequence: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowRestoreCommit {
    Current,
    Stale { current_sequence: u64 },
}

pub trait RuntimeLedger: Send + Sync {
    fn logical_append_capability(&self) -> LogicalAppendCapability;

    fn acquire_workflow_mutation_lease(
        &self,
        run_id: &RunId,
        owner_id: &str,
        now_unix_ms: i64,
    ) -> io::Result<WorkflowMutationLease>;

    fn renew_workflow_mutation_lease(
        &self,
        lease: &WorkflowMutationLease,
        now_unix_ms: i64,
    ) -> io::Result<WorkflowMutationLease>;

    fn commit_workflow_restore(
        &self,
        lease: &WorkflowMutationLease,
        expected_sequence: u64,
        now_unix_ms: i64,
    ) -> io::Result<WorkflowRestoreCommit>;

    fn release_workflow_mutation_lease(&self, lease: &WorkflowMutationLease) -> io::Result<()>;

    fn append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        payload: Value,
    ) -> std::io::Result<LedgerRecord>;

    /// Appends while validating the Workflow lease and advances that lease's
    /// observed Run sequence in the same atomic operation.
    fn append_under_workflow_lease(
        &self,
        lease: &WorkflowMutationLease,
        now_unix_ms: i64,
        durability: DurabilityClass,
        record_type: &str,
        payload: Value,
    ) -> std::io::Result<LedgerRecord>;

    /// Atomically binds a logical record identity to one complete payload.
    ///
    /// Implementations must compare and append as one indivisible operation.
    /// Returning [`ErrorKind::Unsupported`] is required when that guarantee is
    /// unavailable; callers must fail closed instead of using read-then-append.
    fn compare_and_append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        identity_fields: &[&str],
        payload: Value,
    ) -> std::io::Result<LedgerRecord>;

    /// Performs a logical append while validating the Workflow lease and
    /// advances that lease's observed Run sequence in the same atomic
    /// operation, including exact replay.
    fn compare_and_append_under_workflow_lease(
        &self,
        lease: &WorkflowMutationLease,
        now_unix_ms: i64,
        durability: DurabilityClass,
        record_type: &str,
        identity_fields: &[&str],
        payload: Value,
    ) -> std::io::Result<LedgerRecord>;

    /// Atomically admits new collaboration tasks against one Run-wide quota.
    /// Existing identical tasks are replays and consume no additional quota.
    fn admit_collaboration_tasks(
        &self,
        run_id: &RunId,
        max_tasks: usize,
        tasks: &[TaskRecord],
    ) -> io::Result<Vec<LedgerRecord>> {
        self.admit_collaboration_tasks_for_root(run_id, run_id, max_tasks, tasks)
    }

    /// Atomically admits tasks recorded under `run_id` while charging the
    /// historical quota of `root_run_id` and all its descendant Runs.
    fn admit_collaboration_tasks_for_root(
        &self,
        _root_run_id: &RunId,
        _run_id: &RunId,
        _max_tasks: usize,
        _tasks: &[TaskRecord],
    ) -> io::Result<Vec<LedgerRecord>> {
        Err(io::Error::new(
            ErrorKind::Unsupported,
            "runtime ledger does not support atomic Root Run task admission",
        ))
    }

    /// Returns whether this backend can guarantee one all-or-nothing durable
    /// commit for task admission plus related collaboration metadata.
    fn supports_atomic_task_metadata_admission(&self) -> bool {
        false
    }

    /// Atomically admits tasks and appends additional records under the task
    /// Run. Implementations must commit the complete batch or nothing.
    fn admit_tasks_and_append(
        &self,
        root_run_id: &RunId,
        run_id: &RunId,
        max_tasks: usize,
        tasks: &[TaskRecord],
        records: &[(DurabilityClass, String, Value)],
    ) -> io::Result<Vec<LedgerRecord>> {
        if records.is_empty() {
            return self.admit_collaboration_tasks_for_root(root_run_id, run_id, max_tasks, tasks);
        }
        Err(io::Error::new(
            ErrorKind::Unsupported,
            "runtime ledger does not support atomic task and metadata admission",
        ))
    }

    /// Atomically admits tasks and metadata while validating a Workflow
    /// mutation lease and advancing that lease's observed sequence in the same
    /// durable operation.
    fn admit_tasks_and_append_under_workflow_lease(
        &self,
        _lease: &WorkflowMutationLease,
        _now_unix_ms: i64,
        _root_run_id: &RunId,
        _max_tasks: usize,
        _tasks: &[TaskRecord],
        _records: &[(DurabilityClass, String, Value)],
    ) -> io::Result<Vec<LedgerRecord>> {
        Err(io::Error::new(
            ErrorKind::Unsupported,
            "runtime ledger does not support fenced atomic task and metadata admission",
        ))
    }

    fn run_ids(&self) -> std::io::Result<Vec<RunId>>;

    fn records_for_run(&self, run_id: &RunId) -> std::io::Result<Vec<LedgerRecord>>;

    /// Stores one PlanArtifact revision, reusing an identical submission.
    ///
    /// The default preserves the serialized in-process behavior used by the
    /// in-memory and JSONL ledgers. SQLite overrides this method so lookup,
    /// revision allocation, and insertion share one `BEGIN IMMEDIATE`
    /// transaction across independent connections and processes.
    fn record_plan_artifact(&self, run_id: &RunId, msg_id: &str, markdown: &str) -> std::io::Result<PlanArtifact> {
        record_plan_artifact_default(self, run_id, msg_id, markdown)
    }

    /// Deletes only records whose Run identity exactly matches `run_id`.
    /// This never removes output blobs or any other filesystem state.
    fn delete_run_exact(&self, _run_id: &RunId) -> std::io::Result<usize> {
        Err(std::io::Error::new(
            ErrorKind::Unsupported,
            "runtime ledger does not support Run deletion",
        ))
    }

    /// Returns the ledger-local directory used for newly written effect outputs.
    ///
    /// A ledger supplied to `EffectExecutionContext` must return `Some` here.
    /// New durable outputs never fall back to the legacy
    /// global output directory; `None` makes the first outcome write fail closed.
    fn effect_output_root(&self) -> Option<PathBuf> {
        None
    }

    fn protected_state_paths(&self) -> Vec<PathBuf> {
        Vec::new()
    }

    fn records_after(&self, run_id: &RunId, after_sequence: u64, limit: usize) -> std::io::Result<Vec<LedgerRecord>> {
        Ok(self
            .records_for_run(run_id)?
            .into_iter()
            .filter(|record| record.seq > after_sequence)
            .take(limit)
            .collect())
    }

    fn last_sequence(&self, run_id: &RunId) -> std::io::Result<u64> {
        Ok(self
            .records_for_run(run_id)?
            .last()
            .map(|record| record.seq)
            .unwrap_or(0))
    }

    fn records_after_tree(
        &self,
        root_run_id: &RunId,
        after_sequence: u64,
        limit: usize,
    ) -> std::io::Result<Vec<LedgerRecord>> {
        let prefix = format!("{}:", root_run_id.as_str());
        let mut records = Vec::new();
        for run_id in self.run_ids()? {
            if run_id == *root_run_id || run_id.as_str().starts_with(&prefix) {
                records.extend(
                    self.records_for_run(&run_id)?
                        .into_iter()
                        .filter(|record| record.seq > after_sequence),
                );
            }
        }
        records.sort_by_key(|record| record.seq);
        records.truncate(limit);
        Ok(records)
    }

    fn last_sequence_tree(&self, root_run_id: &RunId) -> std::io::Result<u64> {
        Ok(self
            .records_after_tree(root_run_id, 0, usize::MAX)?
            .last()
            .map(|record| record.seq)
            .unwrap_or(0))
    }
}

pub struct InMemoryRuntimeLedger {
    state: Mutex<InMemoryLedgerState>,
    effect_output_root: PathBuf,
}

impl Default for InMemoryRuntimeLedger {
    fn default() -> Self {
        Self {
            state: Mutex::new(InMemoryLedgerState::default()),
            effect_output_root: std::env::temp_dir()
                .join("solaris-mesh-ephemeral-effect-outcomes")
                .join(uuid::Uuid::now_v7().to_string()),
        }
    }
}

#[derive(Default, Clone)]
struct InMemoryLedgerState {
    records: HashMap<RunId, Vec<LedgerRecord>>,
    next_sequence: u64,
    workflow_mutation_leases: HashMap<RunId, InMemoryWorkflowMutationLease>,
}

impl RuntimeLedger for InMemoryRuntimeLedger {
    fn logical_append_capability(&self) -> LogicalAppendCapability {
        LogicalAppendCapability::ProcessLocal
    }

    fn supports_atomic_task_metadata_admission(&self) -> bool {
        true
    }

    fn acquire_workflow_mutation_lease(
        &self,
        run_id: &RunId,
        owner_id: &str,
        now_unix_ms: i64,
    ) -> io::Result<WorkflowMutationLease> {
        acquire_workflow_mutation_lease_in_memory(self, run_id, owner_id, now_unix_ms)
    }

    fn renew_workflow_mutation_lease(
        &self,
        lease: &WorkflowMutationLease,
        now_unix_ms: i64,
    ) -> io::Result<WorkflowMutationLease> {
        renew_workflow_mutation_lease_in_memory(self, lease, now_unix_ms)
    }

    fn commit_workflow_restore(
        &self,
        lease: &WorkflowMutationLease,
        expected_sequence: u64,
        now_unix_ms: i64,
    ) -> io::Result<WorkflowRestoreCommit> {
        commit_workflow_restore_in_memory(self, lease, expected_sequence, now_unix_ms)
    }

    fn release_workflow_mutation_lease(&self, lease: &WorkflowMutationLease) -> io::Result<()> {
        release_workflow_mutation_lease_in_memory(self, lease)
    }

    fn append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        payload: Value,
    ) -> std::io::Result<LedgerRecord> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let seq = state.next_sequence.saturating_add(1);
        let record = LedgerRecord {
            schema_version: LEDGER_SCHEMA_VERSION,
            seq,
            run_id: run_id.clone(),
            timestamp_unix_ms: chrono::Utc::now().timestamp_millis(),
            durability,
            record_type: record_type.to_owned(),
            payload,
        };
        if durability != DurabilityClass::Ephemeral {
            state.next_sequence = seq;
            state.records.entry(run_id.clone()).or_default().push(record.clone());
        }
        Ok(record)
    }

    fn append_under_workflow_lease(
        &self,
        lease: &WorkflowMutationLease,
        now_unix_ms: i64,
        durability: DurabilityClass,
        record_type: &str,
        payload: Value,
    ) -> io::Result<LedgerRecord> {
        runtime_ledger_workflow_lease::append_under_workflow_lease_in_memory(
            self,
            lease,
            now_unix_ms,
            durability,
            record_type,
            payload,
        )
    }

    fn compare_and_append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        identity_fields: &[&str],
        payload: Value,
    ) -> io::Result<LedgerRecord> {
        compare_and_append_in_memory(self, run_id, durability, record_type, identity_fields, payload)
    }

    fn compare_and_append_under_workflow_lease(
        &self,
        lease: &WorkflowMutationLease,
        now_unix_ms: i64,
        durability: DurabilityClass,
        record_type: &str,
        identity_fields: &[&str],
        payload: Value,
    ) -> io::Result<LedgerRecord> {
        runtime_ledger_workflow_lease::compare_and_append_under_workflow_lease_in_memory(
            self,
            lease,
            now_unix_ms,
            durability,
            record_type,
            identity_fields,
            payload,
        )
    }

    fn admit_collaboration_tasks_for_root(
        &self,
        root_run_id: &RunId,
        run_id: &RunId,
        max_tasks: usize,
        tasks: &[TaskRecord],
    ) -> io::Result<Vec<LedgerRecord>> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        admit_collaboration_tasks_in_memory(&mut state, root_run_id, run_id, max_tasks, tasks)
    }

    fn admit_tasks_and_append(
        &self,
        root_run_id: &RunId,
        run_id: &RunId,
        max_tasks: usize,
        tasks: &[TaskRecord],
        records: &[(DurabilityClass, String, Value)],
    ) -> io::Result<Vec<LedgerRecord>> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        admit_tasks_and_append_in_memory(&mut state, root_run_id, run_id, max_tasks, tasks, records)
    }

    fn admit_tasks_and_append_under_workflow_lease(
        &self,
        lease: &WorkflowMutationLease,
        now_unix_ms: i64,
        root_run_id: &RunId,
        max_tasks: usize,
        tasks: &[TaskRecord],
        records: &[(DurabilityClass, String, Value)],
    ) -> io::Result<Vec<LedgerRecord>> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        admit_tasks_and_append_under_workflow_lease_in_memory(
            &mut state,
            lease,
            now_unix_ms,
            root_run_id,
            max_tasks,
            tasks,
            records,
        )
    }

    fn run_ids(&self) -> std::io::Result<Vec<RunId>> {
        let mut ids: Vec<_> = self
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .records
            .keys()
            .cloned()
            .collect();
        ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        Ok(ids)
    }

    fn records_for_run(&self, run_id: &RunId) -> std::io::Result<Vec<LedgerRecord>> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .records
            .get(run_id)
            .cloned()
            .unwrap_or_default())
    }

    fn delete_run_exact(&self, run_id: &RunId) -> std::io::Result<usize> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .records
            .remove(run_id)
            .map_or(0, |records| records.len()))
    }

    fn effect_output_root(&self) -> Option<PathBuf> {
        Some(self.effect_output_root.clone())
    }

    fn last_sequence(&self, run_id: &RunId) -> std::io::Result<u64> {
        Ok(self
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .records
            .get(run_id)
            .and_then(|records| records.last())
            .map(|record| record.seq)
            .unwrap_or(0))
    }
}

pub struct SqliteRuntimeLedger {
    path: PathBuf,
    connection: Mutex<SqliteLedgerConnection>,
    export_protection: Vec<ProtectedExportPath>,
}

struct SqliteLedgerConnection {
    connection: Connection,
    synchronous: SqliteSynchronous,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SqliteSynchronous {
    Full,
    Normal,
}

impl SqliteRuntimeLedger {
    pub fn open(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        let path = resolve_ledger_path(&path)?;
        let database_protection = retain_protected_export_path(&path, true)?;
        let mut connection = Connection::open(&path).map_err(|error| sqlite_error("open runtime ledger", error))?;
        database_protection.verify_opened_slot()?;
        connection
            .busy_timeout(Duration::from_secs(30))
            .map_err(|error| sqlite_error("configure runtime ledger busy timeout", error))?;
        let journal_mode: String = connection
            .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
            .map_err(|error| sqlite_error("enable runtime ledger WAL mode", error))?;
        if !journal_mode.eq_ignore_ascii_case("wal") {
            return Err(io::Error::other("runtime ledger did not enter WAL mode"));
        }
        connection
            .pragma_update(None, "foreign_keys", true)
            .map_err(|error| sqlite_error("enable runtime ledger foreign keys", error))?;
        connection
            .pragma_update(None, "synchronous", "NORMAL")
            .map_err(|error| sqlite_error("configure runtime ledger durability", error))?;
        initialize_sqlite_schema(&mut connection)?;
        let mut export_protection = vec![database_protection];
        export_protection.extend(retain_protected_export_paths(&[
            sqlite_sidecar_path(&path, "-wal"),
            sqlite_sidecar_path(&path, "-shm"),
        ])?);
        Ok(Self {
            path,
            connection: Mutex::new(SqliteLedgerConnection {
                connection,
                synchronous: SqliteSynchronous::Normal,
            }),
            export_protection,
        })
    }

    /// Opens the SQLite ledger and imports a legacy JSONL source once per canonical source path.
    /// Later source changes require an explicit [`Self::import_jsonl`] call.
    pub fn open_with_jsonl_migration(
        path: impl Into<PathBuf>,
        legacy_jsonl_path: impl AsRef<Path>,
    ) -> io::Result<Self> {
        let ledger = Self::open(path)?;
        import_jsonl_once(&ledger, legacy_jsonl_path.as_ref())?;
        Ok(ledger)
    }

    pub fn protected_state_paths(&self) -> Vec<PathBuf> {
        vec![
            self.path.clone(),
            sqlite_sidecar_path(&self.path, "-wal"),
            sqlite_sidecar_path(&self.path, "-shm"),
        ]
    }

    /// Imports complete JSONL lines that have not previously been imported.
    /// This is the explicit path for source content appended after automatic migration.
    pub fn import_jsonl(&self, path: impl AsRef<Path>) -> io::Result<usize> {
        import_jsonl_explicit(self, path.as_ref())
    }

    pub fn export_jsonl(&self, path: impl AsRef<Path>) -> io::Result<usize> {
        let path = path.as_ref();
        let mut state = self.connection.lock().unwrap_or_else(|error| error.into_inner());
        export_sqlite_records_atomically(&mut state.connection, path, &self.export_protection)
    }

    fn delete_run_exact_with(
        &self,
        run_id: &RunId,
        before_commit: impl FnOnce() -> io::Result<()>,
    ) -> io::Result<usize> {
        let mut state = self.connection.lock().unwrap_or_else(|error| error.into_inner());
        set_sqlite_synchronous(&mut state, DurabilityClass::SyncCritical)?;
        let transaction = state
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| sqlite_error("begin exact Run deletion", error))?;
        let deleted = transaction
            .execute(
                "DELETE FROM runtime_ledger_records WHERE run_id = ?1",
                params![run_id.as_str()],
            )
            .map_err(|error| sqlite_error("delete exact Run records", error))?;
        if let Err(error) = before_commit() {
            transaction
                .rollback()
                .map_err(|rollback_error| sqlite_error("roll back exact Run deletion", rollback_error))?;
            return Err(error);
        }
        transaction
            .commit()
            .map_err(|error| sqlite_error("commit exact Run deletion", error))?;
        Ok(deleted)
    }

    #[cfg(test)]
    fn delete_run_exact_with_before_commit(
        &self,
        run_id: &RunId,
        before_commit: impl FnOnce() -> io::Result<()>,
    ) -> io::Result<usize> {
        self.delete_run_exact_with(run_id, before_commit)
    }
}

impl RuntimeLedger for SqliteRuntimeLedger {
    fn logical_append_capability(&self) -> LogicalAppendCapability {
        LogicalAppendCapability::CrossProcess
    }

    fn supports_atomic_task_metadata_admission(&self) -> bool {
        true
    }

    fn acquire_workflow_mutation_lease(
        &self,
        run_id: &RunId,
        owner_id: &str,
        now_unix_ms: i64,
    ) -> io::Result<WorkflowMutationLease> {
        acquire_workflow_mutation_lease_sqlite(self, run_id, owner_id, now_unix_ms)
    }

    fn renew_workflow_mutation_lease(
        &self,
        lease: &WorkflowMutationLease,
        now_unix_ms: i64,
    ) -> io::Result<WorkflowMutationLease> {
        renew_workflow_mutation_lease_sqlite(self, lease, now_unix_ms)
    }

    fn commit_workflow_restore(
        &self,
        lease: &WorkflowMutationLease,
        expected_sequence: u64,
        now_unix_ms: i64,
    ) -> io::Result<WorkflowRestoreCommit> {
        commit_workflow_restore_sqlite(self, lease, expected_sequence, now_unix_ms)
    }

    fn release_workflow_mutation_lease(&self, lease: &WorkflowMutationLease) -> io::Result<()> {
        release_workflow_mutation_lease_sqlite(self, lease)
    }

    fn append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        payload: Value,
    ) -> io::Result<LedgerRecord> {
        let encoded_payload =
            serde_json::to_vec(&payload).map_err(|_| io::Error::other("encode runtime ledger payload"))?;
        let timestamp_unix_ms = chrono::Utc::now().timestamp_millis();
        let mut state = self.connection.lock().unwrap_or_else(|error| error.into_inner());
        if durability == DurabilityClass::Ephemeral {
            let sequence = current_sqlite_sequence(&state.connection)?
                .checked_add(1)
                .ok_or_else(|| io::Error::other("runtime ledger sequence exhausted"))?;
            return Ok(LedgerRecord {
                schema_version: LEDGER_SCHEMA_VERSION,
                seq: sequence,
                run_id: run_id.clone(),
                timestamp_unix_ms,
                durability,
                record_type: record_type.to_owned(),
                payload,
            });
        }

        set_sqlite_synchronous(&mut state, durability)?;
        let durability_code = durability_code(durability)
            .ok_or_else(|| io::Error::other("ephemeral runtime ledger record reached persistence"))?;
        let transaction = state
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| sqlite_error("begin runtime ledger append", error))?;
        let sequence = allocate_sqlite_sequence(&transaction)?;
        transaction
            .execute(
                "INSERT INTO runtime_ledger_records
                    (sequence, schema_version, run_id, timestamp_unix_ms, durability, record_type, payload)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    sequence,
                    i64::from(LEDGER_SCHEMA_VERSION),
                    run_id.as_str(),
                    timestamp_unix_ms,
                    durability_code,
                    record_type,
                    encoded_payload,
                ],
            )
            .map_err(|error| sqlite_error("insert runtime ledger record", error))?;
        transaction
            .commit()
            .map_err(|error| sqlite_error("commit runtime ledger append", error))?;
        let sequence = sqlite_sequence_to_u64(sequence)?;
        Ok(LedgerRecord {
            schema_version: LEDGER_SCHEMA_VERSION,
            seq: sequence,
            run_id: run_id.clone(),
            timestamp_unix_ms,
            durability,
            record_type: record_type.to_owned(),
            payload,
        })
    }

    fn append_under_workflow_lease(
        &self,
        lease: &WorkflowMutationLease,
        now_unix_ms: i64,
        durability: DurabilityClass,
        record_type: &str,
        payload: Value,
    ) -> io::Result<LedgerRecord> {
        runtime_ledger_workflow_lease::append_under_workflow_lease_sqlite(
            self,
            lease,
            now_unix_ms,
            durability,
            record_type,
            payload,
        )
    }

    fn compare_and_append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        identity_fields: &[&str],
        payload: Value,
    ) -> io::Result<LedgerRecord> {
        compare_and_append_sqlite(
            self,
            run_id,
            durability,
            record_type,
            identity_fields,
            payload,
            (|| Ok(()), || Ok(())),
        )
    }

    fn compare_and_append_under_workflow_lease(
        &self,
        lease: &WorkflowMutationLease,
        now_unix_ms: i64,
        durability: DurabilityClass,
        record_type: &str,
        identity_fields: &[&str],
        payload: Value,
    ) -> io::Result<LedgerRecord> {
        runtime_ledger_workflow_lease::compare_and_append_under_workflow_lease_sqlite(
            self,
            lease,
            now_unix_ms,
            durability,
            record_type,
            identity_fields,
            payload,
        )
    }

    fn admit_collaboration_tasks_for_root(
        &self,
        root_run_id: &RunId,
        run_id: &RunId,
        max_tasks: usize,
        tasks: &[TaskRecord],
    ) -> io::Result<Vec<LedgerRecord>> {
        admit_collaboration_tasks_sqlite(self, root_run_id, run_id, max_tasks, tasks)
    }

    fn admit_tasks_and_append(
        &self,
        root_run_id: &RunId,
        run_id: &RunId,
        max_tasks: usize,
        tasks: &[TaskRecord],
        records: &[(DurabilityClass, String, Value)],
    ) -> io::Result<Vec<LedgerRecord>> {
        admit_tasks_and_append_sqlite(self, root_run_id, run_id, max_tasks, tasks, records)
    }

    fn admit_tasks_and_append_under_workflow_lease(
        &self,
        lease: &WorkflowMutationLease,
        now_unix_ms: i64,
        root_run_id: &RunId,
        max_tasks: usize,
        tasks: &[TaskRecord],
        records: &[(DurabilityClass, String, Value)],
    ) -> io::Result<Vec<LedgerRecord>> {
        admit_tasks_and_append_under_workflow_lease_sqlite(
            self,
            lease,
            now_unix_ms,
            root_run_id,
            max_tasks,
            tasks,
            records,
        )
    }

    fn run_ids(&self) -> io::Result<Vec<RunId>> {
        let state = self.connection.lock().unwrap_or_else(|error| error.into_inner());
        let mut statement = state
            .connection
            .prepare("SELECT DISTINCT run_id FROM runtime_ledger_records ORDER BY run_id")
            .map_err(|error| sqlite_error("prepare runtime ledger run query", error))?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|error| sqlite_error("query runtime ledger runs", error))?;
        rows.map(|row| {
            row.map(RunId::from)
                .map_err(|error| sqlite_error("decode runtime ledger run", error))
        })
        .collect()
    }

    fn records_for_run(&self, run_id: &RunId) -> io::Result<Vec<LedgerRecord>> {
        let state = self.connection.lock().unwrap_or_else(|error| error.into_inner());
        query_sqlite_records(
            &state.connection,
            "SELECT sequence, schema_version, run_id, timestamp_unix_ms, durability, record_type, payload
             FROM runtime_ledger_records WHERE run_id = ?1 ORDER BY sequence",
            params![run_id.as_str()],
        )
    }

    fn record_plan_artifact(&self, run_id: &RunId, msg_id: &str, markdown: &str) -> io::Result<PlanArtifact> {
        record_plan_artifact_sqlite(self, run_id, msg_id, markdown)
    }

    fn delete_run_exact(&self, run_id: &RunId) -> io::Result<usize> {
        self.delete_run_exact_with(run_id, || Ok(()))
    }

    fn records_after(&self, run_id: &RunId, after_sequence: u64, limit: usize) -> io::Result<Vec<LedgerRecord>> {
        let Ok(after_sequence) = i64::try_from(after_sequence) else {
            return Ok(Vec::new());
        };
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let state = self.connection.lock().unwrap_or_else(|error| error.into_inner());
        query_sqlite_records(
            &state.connection,
            "SELECT sequence, schema_version, run_id, timestamp_unix_ms, durability, record_type, payload
             FROM runtime_ledger_records
             WHERE run_id = ?1 AND sequence > ?2 ORDER BY sequence LIMIT ?3",
            params![run_id.as_str(), after_sequence, limit],
        )
    }

    fn last_sequence(&self, run_id: &RunId) -> io::Result<u64> {
        let state = self.connection.lock().unwrap_or_else(|error| error.into_inner());
        let sequence: Option<i64> = state
            .connection
            .query_row(
                "SELECT MAX(sequence) FROM runtime_ledger_records WHERE run_id = ?1",
                params![run_id.as_str()],
                |row| row.get(0),
            )
            .map_err(|error| sqlite_error("query runtime ledger last sequence", error))?;
        sequence.map_or(Ok(0), sqlite_sequence_to_u64)
    }

    fn records_after_tree(
        &self,
        root_run_id: &RunId,
        after_sequence: u64,
        limit: usize,
    ) -> io::Result<Vec<LedgerRecord>> {
        let Ok(after_sequence) = i64::try_from(after_sequence) else {
            return Ok(Vec::new());
        };
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let (descendant_start, descendant_end) = tree_run_bounds(root_run_id);
        let state = self.connection.lock().unwrap_or_else(|error| error.into_inner());
        query_sqlite_records(
            &state.connection,
            "SELECT sequence, schema_version, run_id, timestamp_unix_ms, durability, record_type, payload
             FROM runtime_ledger_records
             WHERE sequence > ?1
               AND (run_id = ?2 OR (run_id >= ?3 AND run_id < ?4))
             ORDER BY sequence
             LIMIT ?5",
            params![
                after_sequence,
                root_run_id.as_str(),
                descendant_start,
                descendant_end,
                limit,
            ],
        )
    }

    fn last_sequence_tree(&self, root_run_id: &RunId) -> io::Result<u64> {
        let (descendant_start, descendant_end) = tree_run_bounds(root_run_id);
        let state = self.connection.lock().unwrap_or_else(|error| error.into_inner());
        let sequence: Option<i64> = state
            .connection
            .query_row(
                "SELECT MAX(sequence) FROM runtime_ledger_records
                 WHERE run_id = ?1 OR (run_id >= ?2 AND run_id < ?3)",
                params![root_run_id.as_str(), descendant_start, descendant_end],
                |row| row.get(0),
            )
            .map_err(|error| sqlite_error("query runtime ledger tree sequence", error))?;
        sequence.map_or(Ok(0), sqlite_sequence_to_u64)
    }

    fn protected_state_paths(&self) -> Vec<PathBuf> {
        SqliteRuntimeLedger::protected_state_paths(self)
    }

    fn effect_output_root(&self) -> Option<PathBuf> {
        self.path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .map(|parent| parent.join("effect-outcomes"))
    }
}

fn tree_run_bounds(root_run_id: &RunId) -> (String, String) {
    (
        format!("{}:", root_run_id.as_str()),
        format!("{};", root_run_id.as_str()),
    )
}

fn set_sqlite_synchronous(state: &mut SqliteLedgerConnection, durability: DurabilityClass) -> io::Result<()> {
    let desired = match durability {
        DurabilityClass::SyncCritical => SqliteSynchronous::Full,
        DurabilityClass::AsyncDurable | DurabilityClass::Ephemeral => SqliteSynchronous::Normal,
    };
    if state.synchronous == desired {
        return Ok(());
    }
    let mode = match desired {
        SqliteSynchronous::Full => "FULL",
        SqliteSynchronous::Normal => "NORMAL",
    };
    state
        .connection
        .pragma_update(None, "synchronous", mode)
        .map_err(|error| sqlite_error("configure runtime ledger durability", error))?;
    state.synchronous = desired;
    Ok(())
}

fn current_sqlite_sequence(connection: &Connection) -> io::Result<u64> {
    let sequence: i64 = connection
        .query_row(
            "SELECT next_sequence FROM runtime_ledger_meta WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .map_err(|error| sqlite_error("read runtime ledger sequence", error))?;
    sqlite_sequence_to_u64(sequence)
}

fn allocate_sqlite_sequence(transaction: &Transaction<'_>) -> io::Result<i64> {
    let current: i64 = transaction
        .query_row(
            "SELECT next_sequence FROM runtime_ledger_meta WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .map_err(|error| sqlite_error("read runtime ledger sequence", error))?;
    let next = current
        .checked_add(1)
        .ok_or_else(|| io::Error::other("runtime ledger sequence exhausted"))?;
    let updated = transaction
        .execute(
            "UPDATE runtime_ledger_meta SET next_sequence = ?1 WHERE singleton = 1 AND next_sequence = ?2",
            params![next, current],
        )
        .map_err(|error| sqlite_error("advance runtime ledger sequence", error))?;
    if updated != 1 {
        return Err(io::Error::other("runtime ledger sequence state is inconsistent"));
    }
    Ok(next)
}

fn query_sqlite_records(
    connection: &Connection,
    sql: &str,
    parameters: impl rusqlite::Params,
) -> io::Result<Vec<LedgerRecord>> {
    let mut statement = connection
        .prepare(sql)
        .map_err(|error| sqlite_error("prepare runtime ledger record query", error))?;
    let rows = statement
        .query_map(parameters, decode_sqlite_record)
        .map_err(|error| sqlite_error("query runtime ledger records", error))?;
    rows.map(|row| row.map_err(|error| sqlite_error("decode runtime ledger record", error)))
        .collect()
}

fn decode_sqlite_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<LedgerRecord> {
    let sequence: i64 = row.get(0)?;
    let schema_version: i64 = row.get(1)?;
    let durability: i64 = row.get(4)?;
    let encoded_payload: Vec<u8> = row.get(6)?;
    Ok(LedgerRecord {
        schema_version: u32::try_from(schema_version)
            .map_err(|_| invalid_sqlite_value(1, Type::Integer, "invalid runtime ledger schema version"))?,
        seq: u64::try_from(sequence)
            .map_err(|_| invalid_sqlite_value(0, Type::Integer, "invalid runtime ledger sequence"))?,
        run_id: RunId::from(row.get::<_, String>(2)?),
        timestamp_unix_ms: row.get(3)?,
        durability: durability_from_code(durability)
            .ok_or_else(|| invalid_sqlite_value(4, Type::Integer, "invalid runtime ledger durability"))?,
        record_type: row.get(5)?,
        payload: serde_json::from_slice(&encoded_payload)
            .map_err(|_| invalid_sqlite_value(6, Type::Blob, "invalid runtime ledger payload encoding"))?,
    })
}

fn durability_code(durability: DurabilityClass) -> Option<i64> {
    match durability {
        DurabilityClass::SyncCritical => Some(1),
        DurabilityClass::AsyncDurable => Some(2),
        DurabilityClass::Ephemeral => None,
    }
}

fn durability_from_code(code: i64) -> Option<DurabilityClass> {
    match code {
        1 => Some(DurabilityClass::SyncCritical),
        2 => Some(DurabilityClass::AsyncDurable),
        _ => None,
    }
}

fn sqlite_sequence_to_u64(sequence: i64) -> io::Result<u64> {
    u64::try_from(sequence)
        .map_err(|_| io::Error::new(ErrorKind::InvalidData, "runtime ledger contains an invalid sequence"))
}

fn invalid_sqlite_value(column: usize, value_type: Type, message: &'static str) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        column,
        value_type,
        Box::new(io::Error::new(ErrorKind::InvalidData, message)),
    )
}

fn sqlite_error(context: &'static str, error: rusqlite::Error) -> io::Error {
    io::Error::other(format!("{context}: {error}"))
}

fn sqlite_sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let mut sidecar = path.as_os_str().to_os_string();
    sidecar.push(suffix);
    PathBuf::from(sidecar)
}

#[cfg(test)]
#[path = "runtime_ledger_test.rs"]
mod runtime_ledger_test;

#[cfg(test)]
#[path = "runtime_ledger_sqlite_review_test.rs"]
mod runtime_ledger_sqlite_review_test;

#[cfg(test)]
#[path = "runtime_ledger_gc_test.rs"]
mod runtime_ledger_gc_test;

#[cfg(test)]
#[path = "runtime_ledger_tree_test.rs"]
mod runtime_ledger_tree_test;

#[cfg(test)]
#[path = "runtime_ledger_unique_test.rs"]
mod runtime_ledger_unique_test;

#[cfg(test)]
#[path = "runtime_ledger_workflow_lease_test.rs"]
mod runtime_ledger_workflow_lease_test;
