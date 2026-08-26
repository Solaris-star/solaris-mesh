use std::io;

use chrono::Utc;
use rusqlite::params;
use solaris_types::identity::RunId;
use uuid::Uuid;

use crate::execution_context::delete_local_run_outputs;
use crate::runtime_ledger::RuntimeLedger;

use super::super::{SessionStore, SessionStoreError, begin_immediate, db_error, io_error};
use super::{
    AcquiredGcStage, DestructiveGcStage, GC_STAGE_CLAIM_MILLISECONDS, GcStageAcquisition, GcStageCommit, GcStep,
    RUN_TREE_SCHEMA, StoredGcStageClaim, destructive_stage_is_safe, encode_run_tree, invalid_gc_data, phase_name,
    query_gc_job, stage_claim_matches,
};

impl SessionStore {
    pub(super) fn execute_destructive_gc_stage(
        &self,
        job_id: &str,
        stage: DestructiveGcStage,
        ledger: &dyn RuntimeLedger,
        now_ms: i64,
        observer: &mut dyn FnMut(GcStep, &str) -> io::Result<()>,
    ) -> Result<(GcStageCommit, usize), SessionStoreError> {
        let acquired = match self.acquire_gc_stage(job_id, stage, now_ms)? {
            GcStageAcquisition::Acquired(acquired) => acquired,
            GcStageAcquisition::PhaseChanged => return Ok((GcStageCommit::PhaseChanged, 0)),
            GcStageAcquisition::Deferred => return Ok((GcStageCommit::Deferred, 0)),
        };
        let processed = match stage {
            DestructiveGcStage::Ledger => {
                let mut deleted = 0usize;
                for run_id in &acquired.job.run_tree.exact_run_ids {
                    match ledger.delete_run_exact(&RunId::from(run_id.as_str())) {
                        Ok(count) => deleted = deleted.saturating_add(count),
                        Err(source) => {
                            self.release_gc_stage_claim(job_id, stage, &acquired.token, now_ms)?;
                            return Err(io_error("delete exact Runtime Ledger run for session GC", source));
                        }
                    }
                }
                deleted
            }
            DestructiveGcStage::Blobs => {
                let mut processed = 0usize;
                for run_id in &acquired.job.run_tree.exact_run_ids {
                    if let Err(source) = delete_local_run_outputs(&RunId::from(run_id.as_str()), ledger) {
                        self.release_gc_stage_claim(job_id, stage, &acquired.token, now_ms)?;
                        return Err(io_error("delete local Runtime blob run for session GC", source));
                    }
                    processed = processed.saturating_add(1);
                }
                processed
            }
        };
        if let Err(source) = observer(stage.observer_step(), job_id) {
            self.release_gc_stage_claim(job_id, stage, &acquired.token, now_ms)?;
            return Err(io_error("observe destructive session GC stage", source));
        }
        let commit = match self.commit_gc_stage(job_id, stage, &acquired.token, now_ms) {
            Ok(commit) => commit,
            Err(error) => {
                self.release_gc_stage_claim(job_id, stage, &acquired.token, now_ms)?;
                return Err(error);
            }
        };
        Ok((commit, processed))
    }

    fn acquire_gc_stage(
        &self,
        job_id: &str,
        stage: DestructiveGcStage,
        now_ms: i64,
    ) -> Result<GcStageAcquisition, SessionStoreError> {
        let mut connection = self.open_connection()?;
        let storage = connection.storage_guard();
        let transaction = begin_immediate(&mut connection, "begin session GC stage acquisition")?;
        let Some(mut job) = query_gc_job(&transaction, job_id)? else {
            storage.verify()?;
            transaction
                .commit()
                .map_err(|source| db_error("commit missing session GC stage acquisition", source))?;
            connection.verify_storage_slots()?;
            return Ok(GcStageAcquisition::PhaseChanged);
        };
        if job.phase != stage.expected_phase() {
            storage.verify()?;
            transaction
                .commit()
                .map_err(|source| db_error("commit changed session GC stage acquisition", source))?;
            connection.verify_storage_slots()?;
            return Ok(GcStageAcquisition::PhaseChanged);
        }
        if job.run_tree.schema != RUN_TREE_SCHEMA {
            return Err(invalid_gc_data("session GC job has no committed exact Run tree"));
        }
        if !destructive_stage_is_safe(&transaction, &job, now_ms)? {
            storage.verify()?;
            transaction
                .commit()
                .map_err(|source| db_error("commit deferred session GC stage acquisition", source))?;
            connection.verify_storage_slots()?;
            return Ok(GcStageAcquisition::Deferred);
        }
        let claim_now_ms = now_ms.max(Utc::now().timestamp_millis());
        if job
            .run_tree
            .stage_claim
            .as_ref()
            .is_some_and(|claim| claim.expires_at_ms > claim_now_ms)
        {
            storage.verify()?;
            transaction
                .commit()
                .map_err(|source| db_error("commit occupied session GC stage acquisition", source))?;
            connection.verify_storage_slots()?;
            return Ok(GcStageAcquisition::Deferred);
        }
        let token = Uuid::now_v7().to_string();
        job.run_tree.stage_claim = Some(StoredGcStageClaim {
            stage: stage.name().to_owned(),
            token: token.clone(),
            expires_at_ms: claim_now_ms.saturating_add(GC_STAGE_CLAIM_MILLISECONDS),
        });
        let encoded = encode_run_tree(&job.run_tree, "encode acquired session GC stage")?;
        let changed = transaction
            .execute(
                "UPDATE session_gc_jobs
                 SET exact_run_tree_json = ?2, updated_at_ms = ?3, last_error = NULL
                 WHERE job_id = ?1 AND phase = ?4",
                params![job_id, encoded, claim_now_ms, phase_name(stage.expected_phase())],
            )
            .map_err(|source| db_error("acquire destructive session GC stage", source))?;
        if changed != 1 {
            return Err(invalid_gc_data("session GC stage acquisition lost its job row"));
        }
        storage.verify()?;
        transaction
            .commit()
            .map_err(|source| db_error("commit session GC stage acquisition", source))?;
        connection.verify_storage_slots()?;
        Ok(GcStageAcquisition::Acquired(Box::new(AcquiredGcStage { job, token })))
    }

    fn commit_gc_stage(
        &self,
        job_id: &str,
        stage: DestructiveGcStage,
        token: &str,
        now_ms: i64,
    ) -> Result<GcStageCommit, SessionStoreError> {
        let mut connection = self.open_connection()?;
        let storage = connection.storage_guard();
        let transaction = begin_immediate(&mut connection, "begin session GC stage checkpoint")?;
        let Some(mut job) = query_gc_job(&transaction, job_id)? else {
            storage.verify()?;
            transaction
                .commit()
                .map_err(|source| db_error("commit missing session GC stage checkpoint", source))?;
            connection.verify_storage_slots()?;
            return Ok(GcStageCommit::PhaseChanged);
        };
        if job.phase != stage.expected_phase() {
            storage.verify()?;
            transaction
                .commit()
                .map_err(|source| db_error("commit changed session GC stage checkpoint", source))?;
            connection.verify_storage_slots()?;
            return Ok(GcStageCommit::PhaseChanged);
        }
        if !stage_claim_matches(&job, stage, token) {
            storage.verify()?;
            transaction
                .commit()
                .map_err(|source| db_error("commit replaced session GC stage checkpoint", source))?;
            connection.verify_storage_slots()?;
            return Ok(GcStageCommit::Deferred);
        }
        let safe = destructive_stage_is_safe(&transaction, &job, now_ms)?;
        job.run_tree.stage_claim = None;
        let encoded = encode_run_tree(&job.run_tree, "encode checkpointed session GC stage")?;
        let changed = transaction
            .execute(
                "UPDATE session_gc_jobs
                 SET exact_run_tree_json = ?2, phase = ?3, updated_at_ms = ?4, last_error = NULL
                 WHERE job_id = ?1 AND phase = ?5",
                params![
                    job_id,
                    encoded,
                    if safe {
                        stage.completed_phase_name()
                    } else {
                        phase_name(stage.expected_phase())
                    },
                    now_ms,
                    phase_name(stage.expected_phase()),
                ],
            )
            .map_err(|source| db_error("checkpoint destructive session GC stage", source))?;
        if changed != 1 {
            return Err(invalid_gc_data("session GC stage checkpoint lost its job row"));
        }
        storage.verify()?;
        transaction
            .commit()
            .map_err(|source| db_error("commit destructive session GC stage checkpoint", source))?;
        connection.verify_storage_slots()?;
        Ok(if safe {
            GcStageCommit::Committed
        } else {
            GcStageCommit::Deferred
        })
    }

    fn release_gc_stage_claim(
        &self,
        job_id: &str,
        stage: DestructiveGcStage,
        token: &str,
        now_ms: i64,
    ) -> Result<(), SessionStoreError> {
        let mut connection = self.open_connection()?;
        let storage = connection.storage_guard();
        let transaction = begin_immediate(&mut connection, "begin failed session GC stage release")?;
        if let Some(mut job) = query_gc_job(&transaction, job_id)?
            && job.phase == stage.expected_phase()
            && stage_claim_matches(&job, stage, token)
        {
            job.run_tree.stage_claim = None;
            let encoded = encode_run_tree(&job.run_tree, "encode released session GC stage")?;
            let changed = transaction
                .execute(
                    "UPDATE session_gc_jobs SET exact_run_tree_json = ?2, updated_at_ms = ?3
                     WHERE job_id = ?1 AND phase = ?4",
                    params![job_id, encoded, now_ms, phase_name(stage.expected_phase())],
                )
                .map_err(|source| db_error("release failed destructive session GC stage", source))?;
            if changed != 1 {
                return Err(invalid_gc_data("session GC stage release lost its job row"));
            }
        }
        storage.verify()?;
        transaction
            .commit()
            .map_err(|source| db_error("commit failed session GC stage release", source))?;
        connection.verify_storage_slots()
    }
}
