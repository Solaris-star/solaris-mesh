use super::*;

#[test]
fn runtime_snapshot_capabilities_follow_set_mode_state() {
    let template = json!({
        "tool_approval": true,
        "current_mode": "auto",
        "modes": ["plan", "auto", "bypass"]
    });
    let permissions = PermissionContext::from_auto_approve(false);
    let before_set_mode = capabilities_for_permission(&template, permissions.mode());

    permissions.set_mode(PermissionMode::Bypass);
    let after_set_mode = capabilities_for_permission(&template, permissions.mode());

    assert_eq!(before_set_mode["current_mode"], "auto");
    assert_eq!(after_set_mode["current_mode"], "bypass");
    assert_eq!(after_set_mode["tool_approval"], true);
    assert_eq!(after_set_mode["modes"], template["modes"]);
    assert_eq!(template["current_mode"], "auto");
}

#[test]
fn detached_workflow_cancellation_targets_only_the_requested_run() {
    let registry = DetachedWorkflowRegistry::default();
    let run_a = RunId::from("workflow-a");
    let run_b = RunId::from("workflow-b");
    let receiver_a = registry.register(run_a.clone()).unwrap();
    let receiver_b = registry.register(run_b.clone()).unwrap();

    assert!(registry.cancel(&run_a));
    assert!(*receiver_a.borrow());
    assert!(!*receiver_b.borrow());
    assert!(!registry.cancel(&RunId::from("workflow-missing")));
}

#[tokio::test]
async fn detached_workflow_registry_cancels_and_releases_runs() {
    let registry = DetachedWorkflowRegistry::default();
    let run_id = RunId::from("run:workflow:request");
    let mut receiver = registry.register(run_id.clone()).expect("register workflow");

    assert!(registry.register(run_id.clone()).is_err());
    assert_eq!(registry.cancel_all(), 1);
    receiver.changed().await.expect("receive cancellation");
    assert!(*receiver.borrow());

    registry.finish(&run_id);
    assert!(registry.register(run_id).is_ok());
}

#[test]
fn duplicate_workflow_claim_reports_active_without_replacing_or_cancelling() {
    let registry = DetachedWorkflowRegistry::default();
    let run_id = RunId::from("run:workflow:duplicate");
    let receiver = registry
        .register_if_absent(run_id.clone())
        .unwrap()
        .expect("first claim starts the workflow");

    let duplicate = registry.register_if_absent(run_id.clone()).unwrap();

    assert!(duplicate.is_none());
    assert!(registry.is_active(&run_id));
    assert!(!*receiver.borrow());
}

#[tokio::test]
async fn detached_workflow_registry_waits_for_terminal_cleanup() {
    let registry = DetachedWorkflowRegistry::default();
    let run_id = RunId::from("run:workflow:wait");
    let mut receiver = registry.register(run_id.clone()).unwrap();
    let worker_registry = registry.clone();
    let worker = tokio::spawn(async move {
        receiver.changed().await.unwrap();
        worker_registry.finish(&run_id);
    });

    assert!(registry.cancel_and_wait(std::time::Duration::from_secs(1)).await);
    worker.await.unwrap();
}

#[test]
fn required_workflow_retains_host_turn_identity_until_finished() {
    let registry = DetachedWorkflowRegistry::default();
    let run_id = RunId::from("root:workflow:required");
    let receiver = registry
        .register_required(run_id.clone(), "host-message-a".to_owned())
        .unwrap();

    assert_eq!(
        registry.active_required_turn(),
        Some(("host-message-a".to_owned(), run_id.clone()))
    );
    assert!(!registry.cancel_required_turn("host-message-b"));
    assert!(registry.cancel_required_turn("host-message-a"));
    assert!(*receiver.borrow());

    registry.finish(&run_id);
    assert!(registry.active_required_turn().is_none());
}
