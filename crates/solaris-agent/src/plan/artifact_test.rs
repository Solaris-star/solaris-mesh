use serde_json::json;
use solaris_types::effect::DurabilityClass;

use super::*;

fn record(run_id: &RunId, artifact: &PlanArtifact) -> LedgerRecord {
    LedgerRecord {
        schema_version: 1,
        seq: artifact.revision,
        run_id: run_id.clone(),
        timestamp_unix_ms: artifact.updated_at_unix_ms,
        durability: DurabilityClass::SyncCritical,
        record_type: PLAN_ARTIFACT_RECORD_TYPE.to_owned(),
        payload: serde_json::to_value(artifact).unwrap(),
    }
}

#[test]
fn next_artifact_keeps_id_and_created_time_while_incrementing_revision() {
    let run_id = RunId::new("run-1");
    let first = next_plan_artifact(&[], &run_id, "msg-1", "# First", 10).unwrap();
    let second = next_plan_artifact(&[record(&run_id, &first)], &run_id, "msg-2", "# Second", 20).unwrap();

    assert_eq!(first.id, second.id);
    assert_eq!(second.revision, 2);
    assert_eq!(second.created_at_unix_ms, 10);
    assert_eq!(second.updated_at_unix_ms, 20);
    assert_eq!(second.msg_id, "msg-2");
}

#[test]
fn stored_artifact_requires_matching_digest_and_run() {
    let run_id = RunId::new("run-1");
    let artifact = next_plan_artifact(&[], &run_id, "msg-1", "# Plan", 10).unwrap();
    let mut corrupted = record(&run_id, &artifact);
    corrupted.payload["markdown"] = json!("# Changed");

    let error = plan_artifacts_from_records(&[corrupted]).unwrap_err();
    assert_eq!(error, "stored plan artifact failed integrity validation");
}
