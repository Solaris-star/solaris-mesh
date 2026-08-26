use super::*;

#[test]
fn plan_artifact_round_trips_with_stable_markdown_digest() {
    let markdown = "# Release plan\n\n- Verify the build.";
    let artifact = PlanArtifact {
        id: "plan:v1:example".to_owned(),
        revision: 2,
        markdown: markdown.to_owned(),
        digest: PlanArtifact::markdown_digest(markdown),
        run_id: RunId::new("run-1"),
        msg_id: "msg-1".to_owned(),
        created_at_unix_ms: 10,
        updated_at_unix_ms: 20,
    };

    let encoded = serde_json::to_string(&artifact).unwrap();
    let decoded: PlanArtifact = serde_json::from_str(&encoded).unwrap();

    assert_eq!(decoded, artifact);
    assert_eq!(decoded.digest.len(), "sha256:".len() + 64);
    assert_eq!(decoded.reference().revision, 2);
    assert_eq!(decoded.reference().run_id, RunId::new("run-1"));
}
