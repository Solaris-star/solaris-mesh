use sha2::{Digest, Sha256};
use solaris_types::identity::RunId;
use solaris_types::plan::PlanArtifact;

use crate::runtime_ledger::LedgerRecord;

pub(crate) const PLAN_ARTIFACT_RECORD_TYPE: &str = "plan_artifact";

pub(crate) fn next_plan_artifact(
    records: &[LedgerRecord],
    run_id: &RunId,
    msg_id: &str,
    markdown: &str,
    timestamp_unix_ms: i64,
) -> Result<PlanArtifact, String> {
    let id = artifact_id(run_id);
    let existing = plan_artifacts_from_records(records)?;
    let previous = existing
        .iter()
        .filter(|artifact| artifact.id == id)
        .max_by_key(|artifact| artifact.revision);
    let revision = previous.map_or(Ok(1), |artifact| {
        artifact
            .revision
            .checked_add(1)
            .ok_or("plan artifact revision overflow")
    })?;
    let created_at_unix_ms = previous.map_or(timestamp_unix_ms, |artifact| artifact.created_at_unix_ms);

    Ok(PlanArtifact {
        id,
        revision,
        markdown: markdown.to_owned(),
        digest: PlanArtifact::markdown_digest(markdown),
        run_id: run_id.clone(),
        msg_id: msg_id.to_owned(),
        created_at_unix_ms,
        updated_at_unix_ms: timestamp_unix_ms,
    })
}

pub(crate) fn plan_artifacts_from_records(records: &[LedgerRecord]) -> Result<Vec<PlanArtifact>, String> {
    let mut artifacts = Vec::new();
    for record in records
        .iter()
        .filter(|record| record.record_type == PLAN_ARTIFACT_RECORD_TYPE)
    {
        let artifact: PlanArtifact = serde_json::from_value(record.payload.clone())
            .map_err(|_| "stored plan artifact is malformed".to_owned())?;
        if artifact.revision == 0
            || artifact.run_id != record.run_id
            || artifact.id != artifact_id(&artifact.run_id)
            || artifact.digest != PlanArtifact::markdown_digest(&artifact.markdown)
        {
            return Err("stored plan artifact failed integrity validation".to_owned());
        }
        artifacts.push(artifact);
    }
    artifacts.sort_by(|left, right| {
        left.id
            .cmp(&right.id)
            .then_with(|| left.revision.cmp(&right.revision))
            .then_with(|| left.updated_at_unix_ms.cmp(&right.updated_at_unix_ms))
    });
    Ok(artifacts)
}

pub(crate) fn matching_plan_artifact(
    records: &[LedgerRecord],
    msg_id: &str,
    digest: &str,
) -> Result<Option<PlanArtifact>, String> {
    Ok(plan_artifacts_from_records(records)?
        .into_iter()
        .rev()
        .find(|artifact| artifact.msg_id == msg_id && artifact.digest == digest))
}

fn artifact_id(run_id: &RunId) -> String {
    let stable = format!("plan-artifact/v1:{}:{}", run_id.as_str().len(), run_id.as_str());
    format!("plan:v1:{:x}", Sha256::digest(stable.as_bytes()))
}

#[cfg(test)]
#[path = "artifact_test.rs"]
mod artifact_test;
