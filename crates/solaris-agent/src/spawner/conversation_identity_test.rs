#[tokio::test]
async fn permission_and_handle_identity_changes_fail_closed() {
    let harness = Harness::new();
    let handle = harness.service.open(harness.spec()).await.unwrap();
    harness
        .service
        .spawner
        .permission_context
        .set_ceiling(PermissionCeiling::plan());
    let permission_error = harness
        .service
        .run_turn(&handle, turn("permission", "must fail"))
        .await
        .unwrap_err();
    assert_eq!(permission_error.failure_class, TaskFailureClass::ReconciliationRequired);

    let mut tampered = handle;
    tampered.agent_id = AgentId::from("tampered");
    let identity_error = harness
        .service
        .run_turn(&tampered, turn("identity", "must fail"))
        .await
        .unwrap_err();
    assert_eq!(identity_error.failure_class, TaskFailureClass::ReconciliationRequired);
    assert_eq!(harness.provider.calls(), 0);
}

#[tokio::test]
async fn top_level_handle_identity_must_equal_its_spec_and_exact_open_record() {
    let harness = Harness::new();
    let handle = harness.service.open(harness.spec()).await.unwrap();

    let mut changed_conversation = handle.clone();
    changed_conversation.conversation_id = "another-conversation".to_owned();
    let conversation_error = harness
        .service
        .run_turn(&changed_conversation, turn("identity-conversation", "must fail"))
        .await
        .unwrap_err();
    assert!(matches!(
        conversation_error.failure_class,
        TaskFailureClass::NonRetryable | TaskFailureClass::ReconciliationRequired
    ));

    let mut changed_task = handle;
    changed_task.task_id = TaskId::from("another-task");
    let task_error = harness.service.close(&changed_task).await.unwrap_err();
    assert!(matches!(
        task_error.failure_class,
        TaskFailureClass::NonRetryable | TaskFailureClass::ReconciliationRequired
    ));
    assert_eq!(harness.provider.calls(), 0);
}

#[tokio::test]
async fn different_conversation_ids_cannot_share_a_child_or_session() {
    let harness = Harness::new();
    let first = harness.service.open(harness.spec()).await.unwrap();
    let mut second_spec = harness.spec();
    second_spec.conversation_id = "conversation-secondary".to_owned();
    assert_ne!(
        child_key(&harness.spec()).agent_id(),
        child_key(&second_spec).agent_id()
    );

    let second = harness.service.open(second_spec).await.unwrap();
    assert_ne!(first.agent_id, second.agent_id);
    assert_ne!(first.session_id, second.session_id);
}

#[tokio::test]
async fn execution_environment_change_fails_before_provider_execution() {
    let harness = Harness::new();
    let mut handle = harness.service.open(harness.spec()).await.unwrap();
    handle.environment_digest = "changed-environment".to_owned();

    let error = harness
        .service
        .run_turn(&handle, turn("environment", "must fail"))
        .await
        .unwrap_err();
    assert_eq!(error.failure_class, TaskFailureClass::ReconciliationRequired);
    assert!(error.message.contains("execution environment changed"));
    assert_eq!(harness.provider.calls(), 0);
}

#[tokio::test]
async fn conflicting_duplicate_open_and_turn_records_fail_closed() {
    let harness = Harness::new();
    let spec = harness.spec();
    let handle = harness.service.open(spec.clone()).await.unwrap();
    let mut conflicting_open = serde_json::to_value(&handle).unwrap();
    conflicting_open["environment_digest"] = json!("tampered");
    harness
        .ledger
        .append(
            &harness.run_id,
            DurabilityClass::SyncCritical,
            OPEN_RECORD,
            conflicting_open,
        )
        .unwrap();
    harness
        .ledger
        .append(
            &harness.run_id,
            DurabilityClass::SyncCritical,
            OPEN_RECORD,
            serde_json::to_value(&handle).unwrap(),
        )
        .unwrap();
    let open_error = harness.service.open(spec).await.unwrap_err();
    assert_eq!(open_error.failure_class, TaskFailureClass::ReconciliationRequired);

    let turn = turn("conflict", "must not run");
    let intent = DurableTurnIntent {
        schema_version: CONVERSATION_SCHEMA_VERSION,
        run_id: handle.run_id.clone(),
        parent_agent_id: handle.parent_agent_id.clone(),
        conversation_id: handle.conversation_id.clone(),
        task_id: handle.task_id.clone(),
        open_operation_id: handle.operation_id.clone(),
        spec_digest: handle.spec_digest.clone(),
        turn_id: turn.turn_id.clone(),
        agent_id: handle.agent_id.clone(),
        session_id: handle.session_id.clone(),
        identity: handle.turn_identity(&turn),
    };
    harness
        .ledger
        .append(
            &harness.run_id,
            DurabilityClass::SyncCritical,
            TURN_INTENT_RECORD,
            serde_json::to_value(&intent).unwrap(),
        )
        .unwrap();
    let mut conflicting_intent = serde_json::to_value(&intent).unwrap();
    conflicting_intent["session_id"] = json!("another-session");
    harness
        .ledger
        .append(
            &harness.run_id,
            DurabilityClass::SyncCritical,
            TURN_INTENT_RECORD,
            conflicting_intent,
        )
        .unwrap();
    harness
        .ledger
        .append(
            &harness.run_id,
            DurabilityClass::SyncCritical,
            TURN_INTENT_RECORD,
            serde_json::to_value(&intent).unwrap(),
        )
        .unwrap();
    let turn_error = harness.service.run_turn(&handle, turn).await.unwrap_err();
    assert_eq!(turn_error.failure_class, TaskFailureClass::ReconciliationRequired);
    assert_eq!(harness.provider.calls(), 0);
}

#[tokio::test]
async fn malformed_target_conversation_record_is_not_filtered_out() {
    let harness = Harness::new();
    let handle = harness.service.open(harness.spec()).await.unwrap();
    harness
        .ledger
        .append(
            &harness.run_id,
            DurabilityClass::SyncCritical,
            TURN_INTENT_RECORD,
            json!({
                "schema_version": CONVERSATION_SCHEMA_VERSION,
                "turn_id": "malformed",
                "agent_id": handle.agent_id,
                "session_id": handle.session_id,
            }),
        )
        .unwrap();

    let error = harness
        .service
        .run_turn(&handle, turn("valid-after-malformed", "must not execute"))
        .await
        .unwrap_err();
    assert_eq!(error.failure_class, TaskFailureClass::ReconciliationRequired);
    assert_eq!(harness.provider.calls(), 0);
}

#[tokio::test]
async fn conflicting_outcome_or_close_record_cannot_be_hidden_by_a_later_record() {
    let harness = Harness::new();
    let handle = harness.service.open(harness.spec()).await.unwrap();
    let outcome = harness.service.run_turn(&handle, turn("one", "first")).await.unwrap();
    let mut conflicting_outcome = serde_json::to_value(&outcome).unwrap();
    conflicting_outcome["session_id"] = json!("another-session");
    harness
        .ledger
        .append(
            &harness.run_id,
            DurabilityClass::SyncCritical,
            TURN_OUTCOME_RECORD,
            conflicting_outcome,
        )
        .unwrap();
    harness
        .ledger
        .append(
            &harness.run_id,
            DurabilityClass::SyncCritical,
            TURN_OUTCOME_RECORD,
            serde_json::to_value(&outcome).unwrap(),
        )
        .unwrap();
    let replay_error = harness
        .service
        .run_turn(&handle, turn("one", "first"))
        .await
        .unwrap_err();
    assert_eq!(replay_error.failure_class, TaskFailureClass::ReconciliationRequired);

    let close = DurableClose {
        schema_version: CONVERSATION_SCHEMA_VERSION,
        run_id: handle.run_id.clone(),
        parent_agent_id: handle.parent_agent_id.clone(),
        conversation_id: handle.conversation_id.clone(),
        task_id: handle.task_id.clone(),
        open_operation_id: handle.operation_id.clone(),
        spec_digest: handle.spec_digest.clone(),
        agent_id: handle.agent_id.clone(),
        session_id: handle.session_id.clone(),
        terminal_state: AgentLifecycleState::Completed,
    };
    harness
        .ledger
        .append(
            &harness.run_id,
            DurabilityClass::SyncCritical,
            CLOSE_RECORD,
            serde_json::to_value(&close).unwrap(),
        )
        .unwrap();
    let mut conflicting_close = serde_json::to_value(&close).unwrap();
    conflicting_close["terminal_state"] = json!("cancelled");
    harness
        .ledger
        .append(
            &harness.run_id,
            DurabilityClass::SyncCritical,
            CLOSE_RECORD,
            conflicting_close,
        )
        .unwrap();
    harness
        .ledger
        .append(
            &harness.run_id,
            DurabilityClass::SyncCritical,
            CLOSE_RECORD,
            serde_json::to_value(&close).unwrap(),
        )
        .unwrap();
    let close_error = harness.service.close(&handle).await.unwrap_err();
    assert_eq!(close_error.failure_class, TaskFailureClass::ReconciliationRequired);
}

#[tokio::test]
async fn disabled_session_persistence_rejects_open_before_agent_publication() {
    let workspace = tempfile::tempdir().unwrap();
    let sessions = workspace.path().join("sessions");
    let provider = Arc::new(RecordingProvider::default());
    let ledger: Arc<dyn RuntimeLedger> = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("run-disabled-session");
    let parent_id = AgentId::from("coordinator");
    let service = service_with(
        provider.clone(),
        config_for(workspace.path(), &sessions, false),
        workspace.path(),
        Arc::clone(&ledger),
        run_id.clone(),
        parent_id.clone(),
        ResourceBudget::default(),
    );

    let error = service
        .open(conversation_spec(run_id.clone(), parent_id))
        .await
        .unwrap_err();
    assert_eq!(error.failure_class, TaskFailureClass::NonRetryable);
    assert_eq!(provider.calls(), 0);
    assert!(
        ledger
            .records_for_run(&run_id)
            .unwrap()
            .into_iter()
            .all(|record| record.record_type != "agent_spawn_intent")
    );
}
