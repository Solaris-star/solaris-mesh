use std::collections::BTreeSet;
use std::io;

use chrono::{DateTime, Utc};
use rusqlite::{OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use solaris_types::identity::RunId;

use crate::runtime_ledger::RuntimeLedger;

use super::{SessionStore, SessionStoreError, begin_immediate, db_error, io_error};

const RUN_TREE_SCHEMA: &str = "solaris/session-run-tree/v2";
const GC_STAGE_CLAIM_MILLISECONDS: i64 = 90_000;

#[path = "store_gc_stage.rs"]
mod store_gc_stage;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SessionGcReport {
    pub(crate) examined: usize,
    pub(crate) completed: usize,
    pub(crate) deferred: usize,
    pub(crate) failed: usize,
    pub(crate) ledger_records_deleted: usize,
    pub(crate) blob_runs_processed: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GcJobPhase {
    Planned,
    LedgerDeleted,
    BlobsDeleted,
    Complete,
}

impl GcJobPhase {
    fn parse(value: &str) -> Result<Self, SessionStoreError> {
        match value {
            "planned" => Ok(Self::Planned),
            "ledger_deleted" => Ok(Self::LedgerDeleted),
            "blobs_deleted" => Ok(Self::BlobsDeleted),
            "complete" => Ok(Self::Complete),
            _ => Err(invalid_gc_data("session GC job has an invalid phase")),
        }
    }
}

#[derive(Debug, Clone)]
struct StoredGcJob {
    job_id: String,
    session_id: String,
    phase: GcJobPhase,
    run_tree: StoredRunTree,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct StoredGcStageClaim {
    stage: String,
    token: String,
    expires_at_ms: i64,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
struct StoredRunTree {
    #[serde(default)]
    schema: String,
    session_id: String,
    #[serde(default)]
    run_ids: Vec<String>,
    #[serde(default)]
    reference_roots: Vec<String>,
    #[serde(default)]
    exact_run_ids: Vec<String>,
    #[serde(default)]
    released_shared_roots: Vec<String>,
    #[serde(default)]
    references: Vec<RunReference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stage_claim: Option<StoredGcStageClaim>,
}

impl StoredRunTree {
    fn roots(&self) -> Vec<String> {
        let roots = if self.reference_roots.is_empty() {
            &self.run_ids
        } else {
            &self.reference_roots
        };
        sorted_unique(roots.iter().filter(|run_id| !run_id.is_empty()).cloned())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct RunReference {
    run_id: String,
    reference_kind: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GcJobStatus {
    Completed,
    Deferred,
    Noop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GcJobOutcome {
    status: GcJobStatus,
    ledger_records_deleted: usize,
    blob_runs_processed: usize,
}

impl GcJobOutcome {
    fn new(status: GcJobStatus, ledger_records_deleted: usize, blob_runs_processed: usize) -> Self {
        Self {
            status,
            ledger_records_deleted,
            blob_runs_processed,
        }
    }

    fn empty(status: GcJobStatus) -> Self {
        Self::new(status, 0, 0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DestructiveGcStage {
    Ledger,
    Blobs,
}

impl DestructiveGcStage {
    fn name(self) -> &'static str {
        match self {
            Self::Ledger => "ledger",
            Self::Blobs => "blobs",
        }
    }

    fn expected_phase(self) -> GcJobPhase {
        match self {
            Self::Ledger => GcJobPhase::Planned,
            Self::Blobs => GcJobPhase::LedgerDeleted,
        }
    }

    fn completed_phase_name(self) -> &'static str {
        match self {
            Self::Ledger => "ledger_deleted",
            Self::Blobs => "blobs_deleted",
        }
    }

    fn observer_step(self) -> GcStep {
        match self {
            Self::Ledger => GcStep::LedgerDeleted,
            Self::Blobs => GcStep::BlobsDeleted,
        }
    }
}

#[derive(Debug)]
struct AcquiredGcStage {
    job: StoredGcJob,
    token: String,
}

#[derive(Debug)]
enum GcStageAcquisition {
    Acquired(Box<AcquiredGcStage>),
    PhaseChanged,
    Deferred,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GcStageCommit {
    Committed,
    PhaseChanged,
    Deferred,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SnapshotAllocation {
    eligible_roots: Vec<String>,
    released_shared_roots: Vec<String>,
}

#[derive(Debug)]
enum GcSnapshotPreparation {
    Ready {
        job: Box<StoredGcJob>,
        allocation: SnapshotAllocation,
    },
    AlreadyCommitted,
    Deferred,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GcStep {
    LedgerDeleted,
    BlobsDeleted,
}

impl SessionStore {
    pub(crate) fn run_due_gc(
        &self,
        ledger: &dyn RuntimeLedger,
        max_jobs: usize,
    ) -> Result<SessionGcReport, SessionStoreError> {
        self.run_due_gc_with(ledger, max_jobs, Utc::now(), &mut |_, _| Ok(()))
    }

    #[cfg(test)]
    pub(super) fn run_due_gc_at(
        &self,
        ledger: &dyn RuntimeLedger,
        max_jobs: usize,
        now: DateTime<Utc>,
    ) -> Result<SessionGcReport, SessionStoreError> {
        self.run_due_gc_with(ledger, max_jobs, now, &mut |_, _| Ok(()))
    }

    #[cfg(test)]
    pub(super) fn run_due_gc_at_with_observer(
        &self,
        ledger: &dyn RuntimeLedger,
        max_jobs: usize,
        now: DateTime<Utc>,
        observer: &mut dyn FnMut(GcStep, &str) -> io::Result<()>,
    ) -> Result<SessionGcReport, SessionStoreError> {
        self.run_due_gc_with(ledger, max_jobs, now, observer)
    }

    fn run_due_gc_with(
        &self,
        ledger: &dyn RuntimeLedger,
        max_jobs: usize,
        now: DateTime<Utc>,
        observer: &mut dyn FnMut(GcStep, &str) -> io::Result<()>,
    ) -> Result<SessionGcReport, SessionStoreError> {
        if max_jobs == 0 {
            return Ok(SessionGcReport::default());
        }
        let now_ms = now.timestamp_millis();
        let job_ids = self.due_gc_job_ids(now_ms, max_jobs)?;
        let mut report = SessionGcReport {
            examined: job_ids.len(),
            ..SessionGcReport::default()
        };
        for job_id in job_ids {
            match self.run_gc_job(&job_id, ledger, now_ms, observer) {
                Ok(outcome) => {
                    report.ledger_records_deleted = report
                        .ledger_records_deleted
                        .saturating_add(outcome.ledger_records_deleted);
                    report.blob_runs_processed = report.blob_runs_processed.saturating_add(outcome.blob_runs_processed);
                    match outcome.status {
                        GcJobStatus::Completed => report.completed = report.completed.saturating_add(1),
                        GcJobStatus::Deferred => report.deferred = report.deferred.saturating_add(1),
                        GcJobStatus::Noop => {}
                    }
                }
                Err(error) => {
                    self.record_gc_error(&job_id, now_ms, &error)?;
                    report.failed = report.failed.saturating_add(1);
                }
            }
        }
        Ok(report)
    }

    fn due_gc_job_ids(&self, now_ms: i64, max_jobs: usize) -> Result<Vec<String>, SessionStoreError> {
        let connection = self.open_connection()?;
        let limit = i64::try_from(max_jobs).unwrap_or(i64::MAX);
        let mut statement = connection
            .prepare(
                "SELECT job_id FROM session_gc_jobs
                 WHERE phase != 'complete' AND eligible_at_ms <= ?1
                 ORDER BY eligible_at_ms, created_at_ms, job_id
                 LIMIT ?2",
            )
            .map_err(|source| db_error("prepare due session GC jobs", source))?;
        let rows = statement
            .query_map(params![now_ms, limit], |row| row.get::<_, String>(0))
            .map_err(|source| db_error("query due session GC jobs", source))?;
        let job_ids = rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(|source| db_error("read due session GC jobs", source))?;
        drop(statement);
        connection.verify_storage_slots()?;
        Ok(job_ids)
    }

    fn run_gc_job(
        &self,
        job_id: &str,
        ledger: &dyn RuntimeLedger,
        now_ms: i64,
        observer: &mut dyn FnMut(GcStep, &str) -> io::Result<()>,
    ) -> Result<GcJobOutcome, SessionStoreError> {
        let Some(job) = self.load_gc_job(job_id)? else {
            return Ok(GcJobOutcome::empty(GcJobStatus::Noop));
        };
        if job.phase == GcJobPhase::Complete {
            return Ok(GcJobOutcome::empty(GcJobStatus::Noop));
        }
        if job.phase == GcJobPhase::Planned
            && job.run_tree.schema != RUN_TREE_SCHEMA
            && !self.snapshot_exact_run_tree(job_id, ledger, now_ms)?
        {
            return Ok(GcJobOutcome::empty(GcJobStatus::Deferred));
        }
        self.execute_gc_phases(job_id, ledger, now_ms, observer)
    }

    fn load_gc_job(&self, job_id: &str) -> Result<Option<StoredGcJob>, SessionStoreError> {
        let connection = self.open_connection()?;
        let job = query_gc_job(&connection, job_id)?;
        connection.verify_storage_slots()?;
        Ok(job)
    }

    fn snapshot_exact_run_tree(
        &self,
        job_id: &str,
        ledger: &dyn RuntimeLedger,
        now_ms: i64,
    ) -> Result<bool, SessionStoreError> {
        let (job, allocation) = match self.prepare_gc_snapshot(job_id, now_ms)? {
            GcSnapshotPreparation::Ready { job, allocation } => (*job, allocation),
            GcSnapshotPreparation::AlreadyCommitted => return Ok(true),
            GcSnapshotPreparation::Deferred => return Ok(false),
        };
        let ledger_run_ids = ledger
            .run_ids()
            .map_err(|source| io_error("list Runtime Ledger runs for session GC", source))?;
        self.commit_gc_snapshot(job_id, &job, &allocation, &ledger_run_ids, now_ms)
    }

    fn prepare_gc_snapshot(&self, job_id: &str, now_ms: i64) -> Result<GcSnapshotPreparation, SessionStoreError> {
        let mut connection = self.open_connection()?;
        let storage = connection.storage_guard();
        let transaction = begin_immediate(&mut connection, "begin session GC run-tree snapshot preparation")?;
        let Some(job) = query_gc_job(&transaction, job_id)? else {
            storage.verify()?;
            transaction
                .commit()
                .map_err(|source| db_error("commit missing session GC run-tree snapshot preparation", source))?;
            connection.verify_storage_slots()?;
            return Ok(GcSnapshotPreparation::AlreadyCommitted);
        };
        if job.phase != GcJobPhase::Planned || job.run_tree.schema == RUN_TREE_SCHEMA {
            storage.verify()?;
            transaction
                .commit()
                .map_err(|source| db_error("commit skipped session GC run-tree snapshot preparation", source))?;
            connection.verify_storage_slots()?;
            return Ok(GcSnapshotPreparation::AlreadyCommitted);
        }
        if !candidate_is_inactive(&transaction, &job.session_id, now_ms)?
            || has_pending_host_deliveries(&transaction, &job.session_id)?
        {
            storage.verify()?;
            transaction
                .commit()
                .map_err(|source| db_error("commit deferred session GC run-tree snapshot preparation", source))?;
            connection.verify_storage_slots()?;
            return Ok(GcSnapshotPreparation::Deferred);
        }
        let Some(allocation) = allocate_run_tree(&transaction, &job)? else {
            storage.verify()?;
            transaction
                .commit()
                .map_err(|source| db_error("commit shared session GC run-tree deferral", source))?;
            connection.verify_storage_slots()?;
            return Ok(GcSnapshotPreparation::Deferred);
        };
        storage.verify()?;
        transaction
            .commit()
            .map_err(|source| db_error("commit session GC run-tree snapshot preparation", source))?;
        connection.verify_storage_slots()?;
        Ok(GcSnapshotPreparation::Ready {
            job: Box::new(job),
            allocation,
        })
    }

    fn commit_gc_snapshot(
        &self,
        job_id: &str,
        prepared_job: &StoredGcJob,
        prepared_allocation: &SnapshotAllocation,
        ledger_run_ids: &[RunId],
        now_ms: i64,
    ) -> Result<bool, SessionStoreError> {
        let mut connection = self.open_connection()?;
        let storage = connection.storage_guard();
        let transaction = begin_immediate(&mut connection, "begin session GC run-tree snapshot commit")?;
        let Some(job) = query_gc_job(&transaction, job_id)? else {
            storage.verify()?;
            transaction
                .commit()
                .map_err(|source| db_error("commit missing session GC run-tree snapshot", source))?;
            connection.verify_storage_slots()?;
            return Ok(true);
        };
        if job.phase != GcJobPhase::Planned || job.run_tree.schema == RUN_TREE_SCHEMA {
            storage.verify()?;
            transaction
                .commit()
                .map_err(|source| db_error("commit skipped session GC run-tree snapshot", source))?;
            connection.verify_storage_slots()?;
            return Ok(true);
        }
        if job.session_id != prepared_job.session_id
            || !candidate_is_inactive(&transaction, &job.session_id, now_ms)?
            || has_pending_host_deliveries(&transaction, &job.session_id)?
        {
            storage.verify()?;
            transaction
                .commit()
                .map_err(|source| db_error("commit deferred session GC run-tree snapshot", source))?;
            connection.verify_storage_slots()?;
            return Ok(false);
        }
        let Some(current_allocation) = allocate_run_tree(&transaction, &job)? else {
            storage.verify()?;
            transaction
                .commit()
                .map_err(|source| db_error("commit shared session GC run-tree snapshot deferral", source))?;
            connection.verify_storage_slots()?;
            return Ok(false);
        };
        if current_allocation != *prepared_allocation {
            storage.verify()?;
            transaction
                .commit()
                .map_err(|source| db_error("commit changed session GC run-tree snapshot", source))?;
            connection.verify_storage_slots()?;
            return Ok(false);
        }
        let mut exact_run_ids = BTreeSet::new();
        for root in &current_allocation.eligible_roots {
            exact_run_ids.insert(root.clone());
            for run_id in ledger_run_ids {
                if run_is_in_tree(run_id.as_str(), root) {
                    exact_run_ids.insert(run_id.to_string());
                }
            }
        }
        let run_tree = StoredRunTree {
            schema: RUN_TREE_SCHEMA.to_owned(),
            session_id: job.session_id.clone(),
            run_ids: Vec::new(),
            reference_roots: sorted_unique(
                current_allocation
                    .eligible_roots
                    .iter()
                    .chain(current_allocation.released_shared_roots.iter())
                    .cloned(),
            ),
            exact_run_ids: exact_run_ids.into_iter().collect(),
            released_shared_roots: current_allocation.released_shared_roots,
            references: job.run_tree.references,
            stage_claim: None,
        };
        let encoded = encode_run_tree(&run_tree, "encode exact session GC run tree")?;
        let changed = transaction
            .execute(
                "UPDATE session_gc_jobs
                 SET exact_run_tree_json = ?2, updated_at_ms = ?3, last_error = NULL
                 WHERE job_id = ?1 AND phase = 'planned'",
                params![job_id, encoded, now_ms],
            )
            .map_err(|source| db_error("save exact session GC run tree", source))?;
        if changed != 1 {
            return Err(invalid_gc_data("session GC run-tree snapshot lost its job row"));
        }
        storage.verify()?;
        transaction
            .commit()
            .map_err(|source| db_error("commit exact session GC run tree", source))?;
        connection.verify_storage_slots()?;
        Ok(true)
    }

    fn execute_gc_phases(
        &self,
        job_id: &str,
        ledger: &dyn RuntimeLedger,
        now_ms: i64,
        observer: &mut dyn FnMut(GcStep, &str) -> io::Result<()>,
    ) -> Result<GcJobOutcome, SessionStoreError> {
        let mut ledger_records_deleted = 0usize;
        let mut blob_runs_processed = 0usize;
        loop {
            let Some(job) = self.load_gc_job(job_id)? else {
                return Ok(GcJobOutcome::new(
                    GcJobStatus::Noop,
                    ledger_records_deleted,
                    blob_runs_processed,
                ));
            };
            match job.phase {
                GcJobPhase::Planned => {
                    let (commit, processed) = self.execute_destructive_gc_stage(
                        job_id,
                        DestructiveGcStage::Ledger,
                        ledger,
                        now_ms,
                        observer,
                    )?;
                    ledger_records_deleted = ledger_records_deleted.saturating_add(processed);
                    if commit == GcStageCommit::Deferred {
                        return Ok(GcJobOutcome::new(
                            GcJobStatus::Deferred,
                            ledger_records_deleted,
                            blob_runs_processed,
                        ));
                    }
                }
                GcJobPhase::LedgerDeleted => {
                    let (commit, processed) =
                        self.execute_destructive_gc_stage(job_id, DestructiveGcStage::Blobs, ledger, now_ms, observer)?;
                    blob_runs_processed = blob_runs_processed.saturating_add(processed);
                    if commit == GcStageCommit::Deferred {
                        return Ok(GcJobOutcome::new(
                            GcJobStatus::Deferred,
                            ledger_records_deleted,
                            blob_runs_processed,
                        ));
                    }
                }
                GcJobPhase::BlobsDeleted => {
                    return self.finalize_gc_job(job_id, now_ms, ledger_records_deleted, blob_runs_processed);
                }
                GcJobPhase::Complete => {
                    return Ok(GcJobOutcome::new(
                        GcJobStatus::Noop,
                        ledger_records_deleted,
                        blob_runs_processed,
                    ));
                }
            }
        }
    }

    fn finalize_gc_job(
        &self,
        job_id: &str,
        now_ms: i64,
        ledger_records_deleted: usize,
        blob_runs_processed: usize,
    ) -> Result<GcJobOutcome, SessionStoreError> {
        let mut connection = self.open_connection()?;
        let storage = connection.storage_guard();
        let transaction = begin_immediate(&mut connection, "begin completed session GC metadata")?;
        let Some(job) = query_gc_job(&transaction, job_id)? else {
            storage.verify()?;
            transaction
                .commit()
                .map_err(|source| db_error("commit missing completed session GC metadata", source))?;
            connection.verify_storage_slots()?;
            return Ok(GcJobOutcome::new(
                GcJobStatus::Noop,
                ledger_records_deleted,
                blob_runs_processed,
            ));
        };
        if job.phase == GcJobPhase::Complete {
            storage.verify()?;
            transaction
                .commit()
                .map_err(|source| db_error("commit skipped completed session GC metadata", source))?;
            connection.verify_storage_slots()?;
            return Ok(GcJobOutcome::new(
                GcJobStatus::Noop,
                ledger_records_deleted,
                blob_runs_processed,
            ));
        }
        if job.run_tree.schema != RUN_TREE_SCHEMA || job.run_tree.stage_claim.is_some() {
            return Err(invalid_gc_data("session GC completion has no committed exact Run tree"));
        }
        if job.phase != GcJobPhase::BlobsDeleted || !destructive_stage_is_safe(&transaction, &job, now_ms)? {
            storage.verify()?;
            transaction
                .commit()
                .map_err(|source| db_error("commit deferred completed session GC metadata", source))?;
            connection.verify_storage_slots()?;
            return Ok(GcJobOutcome::new(
                GcJobStatus::Deferred,
                ledger_records_deleted,
                blob_runs_processed,
            ));
        }
        finalize_gc_metadata(&transaction, &job, now_ms)?;
        storage.verify()?;
        transaction
            .commit()
            .map_err(|source| db_error("commit completed session GC metadata", source))?;
        connection.verify_storage_slots()?;
        Ok(GcJobOutcome::new(
            GcJobStatus::Completed,
            ledger_records_deleted,
            blob_runs_processed,
        ))
    }

    fn record_gc_error(&self, job_id: &str, now_ms: i64, error: &SessionStoreError) -> Result<(), SessionStoreError> {
        let mut connection = self.open_connection()?;
        let storage = connection.storage_guard();
        let transaction = begin_immediate(&mut connection, "begin session GC error record")?;
        transaction
            .execute(
                "UPDATE session_gc_jobs SET updated_at_ms = ?2, last_error = ?3
                 WHERE job_id = ?1 AND phase != 'complete'",
                params![job_id, now_ms, error.to_string()],
            )
            .map_err(|source| db_error("record session GC failure", source))?;
        storage.verify()?;
        transaction
            .commit()
            .map_err(|source| db_error("commit session GC failure", source))?;
        connection.verify_storage_slots()
    }
}

fn query_gc_job(connection: &rusqlite::Connection, job_id: &str) -> Result<Option<StoredGcJob>, SessionStoreError> {
    let row = connection
        .query_row(
            "SELECT session_id, phase, exact_run_tree_json FROM session_gc_jobs WHERE job_id = ?1",
            params![job_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            },
        )
        .optional()
        .map_err(|source| db_error("load session GC job", source))?;
    row.map(|(session_id, phase, encoded)| {
        let run_tree = serde_json::from_slice::<StoredRunTree>(&encoded).map_err(|source| SessionStoreError::Json {
            operation: "decode session GC run tree",
            source,
        })?;
        if run_tree.session_id != session_id {
            return Err(invalid_gc_data("session GC run tree belongs to another session"));
        }
        Ok(StoredGcJob {
            job_id: job_id.to_owned(),
            session_id,
            phase: GcJobPhase::parse(&phase)?,
            run_tree,
        })
    })
    .transpose()
}

fn claimed_roots(transaction: &Transaction<'_>, job: &StoredGcJob) -> Result<Vec<String>, SessionStoreError> {
    let mut statement = transaction
        .prepare("SELECT root_run_id FROM session_gc_run_claims WHERE job_id = ?1 ORDER BY root_run_id")
        .map_err(|source| db_error("prepare session GC Run claims", source))?;
    let rows = statement
        .query_map(params![&job.job_id], |row| row.get::<_, String>(0))
        .map_err(|source| db_error("query session GC Run claims", source))?;
    let claims = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| db_error("read session GC Run claims", source))?;
    if claims.is_empty() {
        let legacy_roots = job.run_tree.roots();
        if legacy_roots.is_empty() {
            return Err(invalid_gc_data("session GC job has no Run claim"));
        }
        return Ok(legacy_roots);
    }
    Ok(claims)
}

#[derive(Debug)]
struct OtherRunReference {
    session_id: String,
    run_id: String,
}

fn other_run_references(
    transaction: &Transaction<'_>,
    session_id: &str,
) -> Result<Vec<OtherRunReference>, SessionStoreError> {
    let mut statement = transaction
        .prepare(
            "SELECT session_id, run_id FROM session_run_references
             WHERE session_id != ?1 ORDER BY session_id, run_id",
        )
        .map_err(|source| db_error("prepare other session Run references", source))?;
    let rows = statement
        .query_map(params![session_id], |row| {
            Ok(OtherRunReference {
                session_id: row.get(0)?,
                run_id: row.get(1)?,
            })
        })
        .map_err(|source| db_error("query other session Run references", source))?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|source| db_error("read other session Run references", source))
}

fn allocate_run_tree(
    transaction: &Transaction<'_>,
    job: &StoredGcJob,
) -> Result<Option<SnapshotAllocation>, SessionStoreError> {
    let roots = claimed_roots(transaction, job)?;
    let other_references = other_run_references(transaction, &job.session_id)?;
    let mut eligible_roots = Vec::new();
    let mut released_shared_roots = Vec::new();
    for root in roots {
        let mut owners = BTreeSet::from([job.session_id.clone()]);
        for reference in &other_references {
            if run_trees_overlap(&root, &reference.run_id) {
                owners.insert(reference.session_id.clone());
            }
        }
        let owner = owners
            .iter()
            .next_back()
            .cloned()
            .unwrap_or_else(|| job.session_id.clone());
        if owner == job.session_id && owners.len() > 1 {
            return Ok(None);
        }
        if owner == job.session_id {
            eligible_roots.push(root);
        } else {
            released_shared_roots.push(root);
        }
    }
    Ok(Some(SnapshotAllocation {
        eligible_roots: sorted_unique(eligible_roots),
        released_shared_roots: sorted_unique(released_shared_roots),
    }))
}

fn candidate_is_inactive(
    transaction: &Transaction<'_>,
    session_id: &str,
    now_ms: i64,
) -> Result<bool, SessionStoreError> {
    transaction
        .query_row(
            "SELECT NOT EXISTS(
                SELECT 1 FROM session_leases
                WHERE session_id = ?1 AND owner_id IS NOT NULL AND expires_at_ms > ?2
             )",
            params![session_id, now_ms],
            |row| row.get(0),
        )
        .map_err(|source| db_error("recheck inactive session before GC", source))
}

fn has_pending_host_deliveries(transaction: &Transaction<'_>, session_id: &str) -> Result<bool, SessionStoreError> {
    transaction
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM host_outbox
                WHERE session_id = ?1 AND acknowledged_at_ms IS NULL
             )",
            params![session_id],
            |row| row.get(0),
        )
        .map_err(|source| db_error("recheck pending Host deliveries before session GC", source))
}

fn exact_tree_has_other_references(
    transaction: &Transaction<'_>,
    job: &StoredGcJob,
) -> Result<bool, SessionStoreError> {
    let other_references = other_run_references(transaction, &job.session_id)?;
    Ok(job.run_tree.exact_run_ids.iter().any(|run_id| {
        other_references
            .iter()
            .any(|reference| run_trees_overlap(run_id, &reference.run_id))
    }))
}

fn destructive_stage_is_safe(
    transaction: &Transaction<'_>,
    job: &StoredGcJob,
    now_ms: i64,
) -> Result<bool, SessionStoreError> {
    Ok(candidate_is_inactive(transaction, &job.session_id, now_ms)?
        && !has_pending_host_deliveries(transaction, &job.session_id)?
        && !exact_tree_has_other_references(transaction, job)?)
}

fn stage_claim_matches(job: &StoredGcJob, stage: DestructiveGcStage, token: &str) -> bool {
    job.run_tree
        .stage_claim
        .as_ref()
        .is_some_and(|claim| claim.stage == stage.name() && claim.token == token)
}

fn phase_name(phase: GcJobPhase) -> &'static str {
    match phase {
        GcJobPhase::Planned => "planned",
        GcJobPhase::LedgerDeleted => "ledger_deleted",
        GcJobPhase::BlobsDeleted => "blobs_deleted",
        GcJobPhase::Complete => "complete",
    }
}

fn encode_run_tree(run_tree: &StoredRunTree, operation: &'static str) -> Result<Vec<u8>, SessionStoreError> {
    serde_json::to_vec(run_tree).map_err(|source| SessionStoreError::Json { operation, source })
}

fn finalize_gc_metadata(
    transaction: &Transaction<'_>,
    job: &StoredGcJob,
    now_ms: i64,
) -> Result<(), SessionStoreError> {
    transaction
        .execute(
            "DELETE FROM host_outbox WHERE session_id = ?1 AND acknowledged_at_ms IS NOT NULL",
            params![&job.session_id],
        )
        .map_err(|source| db_error("delete acknowledged Host deliveries during session GC", source))?;
    transaction
        .execute(
            "DELETE FROM session_run_references WHERE session_id = ?1",
            params![&job.session_id],
        )
        .map_err(|source| db_error("delete session Run references during GC", source))?;
    transaction
        .execute("DELETE FROM sessions WHERE session_id = ?1", params![&job.session_id])
        .map_err(|source| db_error("delete retained session state", source))?;
    transaction
        .execute(
            "DELETE FROM session_gc_run_claims WHERE job_id = ?1",
            params![&job.job_id],
        )
        .map_err(|source| db_error("release session GC Run claims", source))?;
    let changed = transaction
        .execute(
            "UPDATE session_gc_jobs
             SET phase = 'complete', updated_at_ms = ?2, last_error = NULL
             WHERE job_id = ?1 AND phase != 'complete'",
            params![&job.job_id, now_ms],
        )
        .map_err(|source| db_error("complete session GC job", source))?;
    if changed != 1 {
        return Err(invalid_gc_data("session GC completion lost its job row"));
    }
    Ok(())
}

fn run_is_in_tree(run_id: &str, root: &str) -> bool {
    run_id == root || run_id.strip_prefix(root).is_some_and(|suffix| suffix.starts_with(':'))
}

pub(super) fn run_trees_overlap(left: &str, right: &str) -> bool {
    run_is_in_tree(left, right) || run_is_in_tree(right, left)
}

fn sorted_unique(values: impl IntoIterator<Item = String>) -> Vec<String> {
    values.into_iter().collect::<BTreeSet<_>>().into_iter().collect()
}

fn invalid_gc_data(message: &'static str) -> SessionStoreError {
    io_error(
        "validate session GC state",
        io::Error::new(io::ErrorKind::InvalidData, message),
    )
}

#[cfg(test)]
#[path = "store_gc_test.rs"]
mod store_gc_test;
