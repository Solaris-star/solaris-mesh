use std::io::ErrorKind;
use std::time::Duration;

use serde_json::Value;
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::RunId;

use crate::runtime_ledger::{
    LedgerRecord, LogicalAppendCapability, WORKFLOW_MUTATION_HEARTBEAT_MILLIS, WorkflowMutationLease,
    WorkflowRestoreCommit,
};

use super::{WorkflowController, WorkflowProjection, WorkflowRunSnapshot};

pub(super) struct WorkflowMutationGuard {
    pub(super) run_id: RunId,
    pub(super) owner_id: String,
    pub(super) lease: Option<WorkflowMutationLease>,
    last_renewed_unix_ms: i64,
}

#[cfg(test)]
impl WorkflowMutationGuard {
    pub(super) fn force_renewal_due(&mut self) {
        self.last_renewed_unix_ms = i64::MIN;
    }
}

impl WorkflowController {
    pub(super) fn mutate_projection<R>(
        &self,
        run_id: &RunId,
        action: impl FnOnce(&mut WorkflowMutationGuard, &mut Option<WorkflowRunSnapshot>) -> Result<R, String>,
    ) -> Result<R, String> {
        let line = self.mutation.line_for(run_id);
        let _mutation_guard = line.lock().unwrap_or_else(|error| error.into_inner());
        let mut guard = self.acquire_workflow_mutation_guard(run_id)?;
        let original = self
            .runs
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(run_id)
            .cloned();
        if let Err(error) = self.require_current_projection(&guard, original.as_ref()) {
            let release = self.release_workflow_mutation_guard(guard);
            return match release {
                Ok(()) => Err(error),
                Err(release_error) => Err(format!(
                    "{error}; failed to release Workflow mutation lease: {release_error}"
                )),
            };
        }
        let mut prepared = original.as_ref().map(|projection| projection.snapshot.clone());
        let result = action(&mut guard, &mut prepared).and_then(|value| {
            self.validate_workflow_mutation_guard(&mut guard)?;
            let durable_sequence = guard
                .lease
                .as_ref()
                .ok_or_else(|| "runtime ledger cannot fence Workflow projection publication".to_owned())?
                .observed_sequence;
            let published = prepared.map(|snapshot| WorkflowProjection {
                snapshot,
                durable_sequence,
            });
            let mut runs = self.runs.write().unwrap_or_else(|error| error.into_inner());
            if runs.get(run_id) != original.as_ref() {
                return Err(format!("Workflow Run {run_id} projection changed during mutation"));
            }
            match published.as_ref() {
                Some(projection) => {
                    runs.insert(run_id.clone(), projection.clone());
                }
                None => {
                    runs.remove(run_id);
                }
            }
            if let Err(error) = self.commit_projection_high_water(&guard, durable_sequence) {
                match original.as_ref() {
                    Some(projection) => {
                        runs.insert(run_id.clone(), projection.clone());
                    }
                    None => {
                        runs.remove(run_id);
                    }
                }
                return Err(error);
            }
            drop(runs);
            Ok(value)
        });
        let release = self.release_workflow_mutation_guard(guard);
        match (result, release) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(release_error)) => Err(release_error),
            (Err(error), Err(release_error)) => Err(format!(
                "{error}; failed to release Workflow mutation lease: {release_error}"
            )),
        }
    }

    fn require_current_projection(
        &self,
        guard: &WorkflowMutationGuard,
        projection: Option<&WorkflowProjection>,
    ) -> Result<(), String> {
        let lease = guard
            .lease
            .as_ref()
            .ok_or_else(|| "runtime ledger cannot fence Workflow projection mutation".to_owned())?;
        let projection_sequence = projection.map_or(0, |projection| projection.durable_sequence);
        if projection_sequence != lease.observed_sequence {
            return Err(format!(
                "Workflow Run {} projection high-water {} is stale; durable high-water is {}; restore is required",
                guard.run_id, projection_sequence, lease.observed_sequence
            ));
        }
        Ok(())
    }

    pub(super) fn commit_projection_high_water(
        &self,
        guard: &WorkflowMutationGuard,
        durable_sequence: u64,
    ) -> Result<(), String> {
        let lease = guard
            .lease
            .as_ref()
            .ok_or_else(|| "runtime ledger cannot commit Workflow projection high-water".to_owned())?;
        match self
            .ledger
            .commit_workflow_restore(lease, durable_sequence, chrono::Utc::now().timestamp_millis())
            .map_err(|error| format!("failed to commit Workflow projection high-water: {error}"))?
        {
            WorkflowRestoreCommit::Current => Ok(()),
            WorkflowRestoreCommit::Stale { current_sequence } => Err(format!(
                "Workflow Run {} projection high-water {} became stale; durable high-water is {}",
                guard.run_id, durable_sequence, current_sequence
            )),
        }
    }

    pub(super) fn acquire_workflow_mutation_guard(&self, run_id: &RunId) -> Result<WorkflowMutationGuard, String> {
        let lease = if self.ledger.logical_append_capability() != LogicalAppendCapability::Unsupported {
            loop {
                let now = chrono::Utc::now().timestamp_millis();
                match self
                    .ledger
                    .acquire_workflow_mutation_lease(run_id, &self.mutation_owner_id, now)
                {
                    Ok(lease) => break Some(lease),
                    Err(error) if error.kind() == ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => return Err(format!("failed to acquire Workflow mutation lease: {error}")),
                }
            }
        } else {
            None
        };
        Ok(WorkflowMutationGuard {
            run_id: run_id.clone(),
            owner_id: self.mutation_owner_id.clone(),
            lease,
            last_renewed_unix_ms: chrono::Utc::now().timestamp_millis(),
        })
    }

    pub(super) fn renew_workflow_mutation_guard(&self, guard: &mut WorkflowMutationGuard) -> Result<(), String> {
        if guard.run_id.as_str().is_empty() || guard.owner_id != self.mutation_owner_id {
            return Err("Workflow mutation guard does not belong to this Controller".to_owned());
        }
        let now = chrono::Utc::now().timestamp_millis();
        if now.saturating_sub(guard.last_renewed_unix_ms) < WORKFLOW_MUTATION_HEARTBEAT_MILLIS {
            return Ok(());
        }
        if let Some(lease) = guard.lease.as_ref() {
            guard.lease = Some(
                self.ledger
                    .renew_workflow_mutation_lease(lease, now)
                    .map_err(|error| format!("failed to renew Workflow mutation lease: {error}"))?,
            );
        }
        guard.last_renewed_unix_ms = now;
        Ok(())
    }

    pub(super) fn validate_workflow_mutation_guard(&self, guard: &mut WorkflowMutationGuard) -> Result<(), String> {
        if guard.run_id.as_str().is_empty() || guard.owner_id != self.mutation_owner_id {
            return Err("Workflow mutation guard does not belong to this Controller".to_owned());
        }
        let now = chrono::Utc::now().timestamp_millis();
        let lease = guard
            .lease
            .as_ref()
            .ok_or_else(|| "runtime ledger cannot fence Workflow mutations".to_owned())?;
        guard.lease = Some(
            self.ledger
                .renew_workflow_mutation_lease(lease, now)
                .map_err(|error| format!("failed to validate Workflow mutation lease: {error}"))?,
        );
        guard.last_renewed_unix_ms = now;
        Ok(())
    }

    pub(super) fn release_workflow_mutation_guard(&self, guard: WorkflowMutationGuard) -> Result<(), String> {
        if guard.run_id.as_str().is_empty() || guard.owner_id != self.mutation_owner_id {
            return Err("Workflow mutation guard does not belong to this Controller".to_owned());
        }
        let Some(lease) = guard.lease.as_ref() else {
            return Ok(());
        };
        self.ledger
            .release_workflow_mutation_lease(lease)
            .map_err(|error| error.to_string())
    }

    pub(super) fn append_record(
        &self,
        guard: &WorkflowMutationGuard,
        run_id: &RunId,
        record_type: &str,
        payload: Value,
    ) -> Result<LedgerRecord, String> {
        if guard.run_id != *run_id || guard.owner_id != self.mutation_owner_id {
            return Err("Workflow mutation guard does not cover durable append".to_owned());
        }
        let lease = guard
            .lease
            .as_ref()
            .ok_or_else(|| "runtime ledger cannot fence Workflow durable append".to_owned())?;
        self.ledger
            .append_under_workflow_lease(
                lease,
                chrono::Utc::now().timestamp_millis(),
                DurabilityClass::SyncCritical,
                record_type,
                payload,
            )
            .map_err(|error| error.to_string())
    }
}
