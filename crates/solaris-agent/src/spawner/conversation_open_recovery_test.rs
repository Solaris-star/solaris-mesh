#[tokio::test]
async fn open_persists_idle_agent_and_session_without_provider_call() {
    let harness = Harness::new();
    let handle = harness.service.open(harness.spec()).await.unwrap();

    assert_eq!(harness.provider.calls(), 0);
    assert_eq!(handle.schema_version, CONVERSATION_SCHEMA_VERSION);
    assert_eq!(
        harness
            .service
            .spawner
            .lifecycle_runtime
            .agents()
            .get(&handle.agent_id)
            .unwrap()
            .state,
        AgentLifecycleState::Idle
    );
    let session = SessionManager::new(harness.sessions.clone(), 20)
        .load(&handle.session_id)
        .unwrap();
    assert_eq!(session.run_id.as_deref(), Some(harness.run_id.as_str()));
}

#[tokio::test]
async fn two_turns_cold_resume_the_same_agent_and_session() {
    let harness = Harness::new();
    let handle = harness.service.open(harness.spec()).await.unwrap();

    let first = harness.service.run_turn(&handle, turn("one", "first")).await.unwrap();
    let second = harness.service.run_turn(&handle, turn("two", "second")).await.unwrap();

    assert_eq!(harness.provider.calls(), 2);
    assert_eq!(first.agent_id, handle.agent_id);
    assert_eq!(second.agent_id, handle.agent_id);
    assert_eq!(first.session_id, handle.session_id);
    assert_eq!(second.session_id, handle.session_id);
    assert_eq!(output_text(&first), "reply-1");
    assert_eq!(output_text(&second), "reply-2");
}

#[tokio::test]
async fn restart_continues_the_same_conversation_session() {
    let harness = Harness::new();
    let spec = harness.spec();
    let handle = harness.service.open(spec.clone()).await.unwrap();
    harness.service.run_turn(&handle, turn("one", "first")).await.unwrap();

    let restarted = service_with(
        harness.provider.clone(),
        config_for(harness._workspace.path(), &harness.sessions, true),
        harness._workspace.path(),
        Arc::clone(&harness.ledger),
        harness.run_id.clone(),
        harness.parent_id.clone(),
        ResourceBudget::default(),
    );
    let reopened = restarted.open(spec).await.unwrap();
    let second = restarted.run_turn(&reopened, turn("two", "second")).await.unwrap();

    assert_eq!(reopened.agent_id, handle.agent_id);
    assert_eq!(reopened.session_id, handle.session_id);
    assert_eq!(second.session_id, handle.session_id);
    assert_eq!(harness.provider.calls(), 2);
}

#[tokio::test]
async fn restart_takes_over_an_expired_opening_claim_before_ledger_publication() {
    let harness = Harness::new();
    let spec = harness.spec();
    let key = child_key(&spec);
    let agent_id = key.agent_id();
    let session_id = key.session_id();
    let runtime = harness
        .service
        .build_runtime(&spec, agent_id.clone(), key, true)
        .unwrap();
    harness
        .service
        .ensure_session_persisted(&spec, &runtime, &session_id)
        .unwrap();
    let spec_digest = digest_serialized(&spec).unwrap();
    let identity = conversation_identity(&spec, &agent_id, &session_id, &spec_digest);
    let store = harness.service.conversation_store().unwrap();
    let ConversationOpenClaim::Owned { .. } = store.claim_conversation_open(&identity, "dead-service").unwrap() else {
        panic!("the crashed service must own the initial Opening claim");
    };
    Connection::open(store.database_path())
        .unwrap()
        .execute(
            "UPDATE agent_conversations SET opening_expires_at_ms = 0
             WHERE run_id = ?1 AND parent_agent_id = ?2 AND conversation_id = ?3",
            rusqlite::params![&identity.run_id, &identity.parent_agent_id, &identity.conversation_id],
        )
        .unwrap();

    let restarted = service_with(
        harness.provider.clone(),
        config_for(harness._workspace.path(), &harness.sessions, true),
        harness._workspace.path(),
        Arc::clone(&harness.ledger),
        harness.run_id.clone(),
        harness.parent_id.clone(),
        ResourceBudget::default(),
    );
    let handle = restarted.open(spec).await.unwrap();

    assert_eq!(handle.agent_id, agent_id);
    assert_eq!(handle.session_id, session_id);
    assert_eq!(harness.provider.calls(), 0);
    assert_eq!(
        harness
            .ledger
            .records_for_run(&harness.run_id)
            .unwrap()
            .into_iter()
            .filter(|record| record.record_type == OPEN_RECORD)
            .count(),
        1
    );
}

#[tokio::test]
async fn slow_live_opener_heartbeats_and_cannot_be_taken_over() {
    let workspace = tempfile::tempdir().unwrap();
    let sessions = workspace.path().join("sessions");
    let provider = Arc::new(RecordingProvider::default());
    let ledger: Arc<dyn RuntimeLedger> = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("run-slow-opening-heartbeat");
    let parent_id = AgentId::from("coordinator");
    let service = Arc::new(
        service_with(
            provider.clone(),
            config_for(workspace.path(), &sessions, true),
            workspace.path(),
            Arc::clone(&ledger),
            run_id.clone(),
            parent_id.clone(),
            ResourceBudget::default(),
        )
        .with_open_lease_timing(Duration::from_millis(500), Duration::from_millis(20)),
    );
    let spec = conversation_spec(run_id, parent_id);
    let other_store = service.conversation_store().unwrap();
    let (reached, resume) = service.install_open_claim_hook();
    let opening = {
        let service = Arc::clone(&service);
        let spec = spec.clone();
        tokio::spawn(async move { service.open(spec).await })
    };
    reached.wait().await;
    tokio::time::sleep(Duration::from_millis(750)).await;

    let key = child_key(&spec);
    let agent_id = key.agent_id();
    let session_id = key.session_id();
    let spec_digest = digest_serialized(&spec).unwrap();
    let identity = conversation_identity(&spec, &agent_id, &session_id, &spec_digest);
    let before_claim = other_store.load_conversation(&identity).unwrap().unwrap();
    assert!(
        before_claim
            .opening_expires_at_ms
            .is_some_and(|expires| expires > Utc::now().timestamp_millis()),
        "the live opener did not renew its lease: {before_claim:?}"
    );
    assert!(matches!(
        other_store
            .claim_conversation_open(&identity, "competing-service")
            .unwrap(),
        ConversationOpenClaim::Busy(_)
    ));

    resume.wait().await;
    let handle = opening.await.unwrap().unwrap();
    assert_eq!(handle.agent_id, agent_id);
    assert_eq!(provider.calls(), 0);
}

#[tokio::test]
async fn exact_turn_replay_reuses_outcome_and_changed_input_is_rejected() {
    let harness = Harness::new();
    let handle = harness.service.open(harness.spec()).await.unwrap();
    let first = harness.service.run_turn(&handle, turn("one", "first")).await.unwrap();

    let replay = harness.service.run_turn(&handle, turn("one", "first")).await.unwrap();
    let changed = harness
        .service
        .run_turn(&handle, turn("one", "changed"))
        .await
        .unwrap_err();

    assert_eq!(harness.provider.calls(), 1);
    assert_eq!(replay.operation_id, first.operation_id);
    assert_eq!(changed.failure_class, TaskFailureClass::NonRetryable);
}

#[tokio::test]
async fn conversation_id_rejects_changed_open_spec() {
    let harness = Harness::new();
    let spec = harness.spec();
    harness.service.open(spec.clone()).await.unwrap();
    let mut changed = spec;
    changed.config.max_tokens += 1;

    let error = harness.service.open(changed).await.unwrap_err();
    assert_eq!(error.failure_class, TaskFailureClass::NonRetryable);
    assert_eq!(harness.provider.calls(), 0);
}

#[tokio::test]
async fn durable_intent_without_outcome_never_calls_provider() {
    let harness = Harness::new();
    let handle = harness.service.open(harness.spec()).await.unwrap();
    let turn = turn("unknown", "do not replay");
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
    harness.service.record_turn_intent(&handle, &intent).unwrap();

    let error = harness.service.run_turn(&handle, turn).await.unwrap_err();
    assert_eq!(error.failure_class, TaskFailureClass::OutcomeUnknown);
    assert_eq!(harness.provider.calls(), 0);
}

#[tokio::test]
async fn outcome_append_error_is_recovered_by_exact_durable_lookup() {
    let ledger = Arc::new(AfterAppendFailureLedger::default());
    let harness = Harness::with_ledger(ledger.clone());
    let handle = harness.service.open(harness.spec()).await.unwrap();
    ledger.fail_once(TURN_OUTCOME_RECORD);

    let outcome = harness.service.run_turn(&handle, turn("one", "first")).await.unwrap();

    assert_eq!(outcome.status, AgentOutcomeStatus::Completed);
    assert_eq!(harness.provider.calls(), 1);
    assert_eq!(
        harness
            .ledger
            .records_for_run(&harness.run_id)
            .unwrap()
            .into_iter()
            .filter(|record| record.record_type == TURN_OUTCOME_RECORD)
            .count(),
        1
    );
}

#[tokio::test]
async fn open_append_error_is_recovered_without_aborting_published_reservation() {
    let ledger = Arc::new(AfterAppendFailureLedger::default());
    let harness = Harness::with_ledger(ledger.clone());
    ledger.fail_once(OPEN_RECORD);

    let handle = harness.service.open(harness.spec()).await.unwrap();

    assert_eq!(
        harness
            .service
            .spawner
            .lifecycle_runtime
            .agents()
            .get(&handle.agent_id)
            .unwrap()
            .state,
        AgentLifecycleState::Idle
    );
    assert!(
        harness
            .ledger
            .records_for_run(&harness.run_id)
            .unwrap()
            .into_iter()
            .all(|record| record.record_type != "agent_spawn_aborted")
    );
}

#[tokio::test]
async fn open_append_and_recovery_read_error_never_aborts_a_published_agent() {
    let ledger = Arc::new(AfterAppendFailureLedger::default());
    let harness = Harness::with_ledger(ledger.clone());
    let spec = harness.spec();
    let expected_agent = child_key(&spec).agent_id();
    ledger.fail_with_recovery_read_once(OPEN_RECORD);

    let error = harness.service.open(spec.clone()).await.unwrap_err();
    assert_eq!(error.failure_class, TaskFailureClass::ReconciliationRequired);
    assert!(
        harness
            .service
            .spawner
            .lifecycle_runtime
            .agents()
            .get(&expected_agent)
            .is_some(),
        "an uncertain open append must retain the published Agent"
    );
    let reopened = harness.service.open(spec).await.unwrap();
    assert_eq!(reopened.agent_id, expected_agent);
    assert!(
        harness
            .ledger
            .records_for_run(&harness.run_id)
            .unwrap()
            .iter()
            .all(|record| record.record_type != "agent_spawn_aborted")
    );
}

#[tokio::test]
async fn two_service_instances_use_session_lease_as_durable_turn_claim() {
    let harness = Harness::new();
    let spec = harness.spec();
    let handle = harness.service.open(spec.clone()).await.unwrap();
    let second = service_with(
        harness.provider.clone(),
        config_for(harness._workspace.path(), &harness.sessions, true),
        harness._workspace.path(),
        Arc::clone(&harness.ledger),
        harness.run_id.clone(),
        harness.parent_id.clone(),
        ResourceBudget::default(),
    );
    second.open(spec).await.unwrap();
    let (winner_reached_provider, resume_winner) = harness.provider.pause_next_call();
    let first = Arc::new(harness.service);
    let second = Arc::new(second);
    let first_handle = handle.clone();
    let first_service = Arc::clone(&first);
    let first_turn =
        tokio::spawn(async move { first_service.run_turn(&first_handle, turn("shared", "only once")).await });
    winner_reached_provider.wait().await;
    let second_handle = handle.clone();
    let second_service = Arc::clone(&second);
    let second_turn = tokio::spawn(async move {
        second_service
            .run_turn(&second_handle, turn("shared", "only once"))
            .await
    });

    let second_result = second_turn.await.unwrap();
    assert!(second_result.is_err());
    assert!(matches!(
        second_result.unwrap_err().failure_class,
        TaskFailureClass::OutcomeUnknown | TaskFailureClass::ReconciliationRequired
    ));
    assert_eq!(
        first
            .spawner
            .lifecycle_runtime
            .agents()
            .get(&handle.agent_id)
            .unwrap()
            .state,
        AgentLifecycleState::Active,
        "a losing service must not repair a live winner to Idle"
    );
    assert_eq!(harness.provider.calls(), 1);
    resume_winner.wait().await;
    let first_result = first_turn.await.unwrap();
    assert!(first_result.is_ok());
    let durable_outcomes = harness
        .ledger
        .records_for_run(&harness.run_id)
        .unwrap()
        .into_iter()
        .filter(|record| {
            record.record_type == TURN_OUTCOME_RECORD
                && record.payload.get("turn_id").and_then(Value::as_str) == Some("shared")
        })
        .count();
    assert_eq!(durable_outcomes, 1, "the losing service must not append an outcome");
    let replay = second.run_turn(&handle, turn("shared", "only once")).await.unwrap();
    assert_eq!(output_text(&replay), "reply-1");
    assert_eq!(harness.provider.calls(), 1);
}
