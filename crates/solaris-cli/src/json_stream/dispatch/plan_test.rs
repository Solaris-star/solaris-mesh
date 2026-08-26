use super::*;

#[test]
fn plan_artifact_query_is_limited_to_the_host_run_tree() {
    let root = RunId::from("root-run");

    assert_eq!(resolve_artifact_run_id(&root, None).unwrap(), root);
    assert_eq!(
        resolve_artifact_run_id(&root, Some("root-run:workflow:child")).unwrap(),
        RunId::from("root-run:workflow:child")
    );
    assert!(resolve_artifact_run_id(&root, Some("root-run-escape")).is_err());
    assert!(resolve_artifact_run_id(&root, Some("other-run")).is_err());
}
