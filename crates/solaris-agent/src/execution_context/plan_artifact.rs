use solaris_types::identity::RunId;
use solaris_types::plan::PlanArtifact;

use super::EffectExecutionContext;
use crate::plan::artifact::plan_artifacts_from_records;

impl EffectExecutionContext {
    pub(crate) fn record_plan_artifact(&self, msg_id: &str, markdown: &str) -> std::io::Result<PlanArtifact> {
        self.ensure_session_fence()?;
        let line = self.mutation.line_for(&self.run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        self.ledger.record_plan_artifact(&self.run_id, msg_id, markdown)
    }

    /// Read and integrity-check every PlanArtifact stored for one Run.
    pub fn plan_artifacts_for_run(&self, run_id: &RunId) -> std::io::Result<Vec<PlanArtifact>> {
        let records = self.ledger.records_for_run(run_id)?;
        plan_artifacts_from_records(&records).map_err(std::io::Error::other)
    }

    /// Read and integrity-check PlanArtifacts for a root Run and its descendants.
    pub fn plan_artifacts_for_run_tree(&self, run_id: &RunId) -> std::io::Result<Vec<PlanArtifact>> {
        let records = self.ledger.records_after_tree(run_id, 0, usize::MAX)?;
        plan_artifacts_from_records(&records).map_err(std::io::Error::other)
    }
}

#[cfg(test)]
#[path = "plan_artifact_test.rs"]
mod plan_artifact_test;
