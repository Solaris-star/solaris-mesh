#[tokio::test]
async fn exhausted_role_budget_is_non_retryable_before_provider_execution() {
    let workspace = tempfile::tempdir().unwrap();
    let sessions = workspace.path().join("sessions");
    let provider = Arc::new(RecordingProvider::default());
    let run_id = RunId::from("run-exhausted-role-budget");
    let parent_id = AgentId::from("coordinator");
    let service = service_with(
        provider.clone(),
        config_for(workspace.path(), &sessions, true),
        workspace.path(),
        Arc::new(InMemoryRuntimeLedger::default()),
        run_id.clone(),
        parent_id.clone(),
        ResourceBudget::default(),
    );
    let mut spec = conversation_spec(run_id, parent_id);
    spec.resource_budget.max_wall_time_ms = Some(0);
    let handle = service.open(spec).await.unwrap();

    let outcome = service
        .run_turn(&handle, turn("budget", "must not execute"))
        .await
        .unwrap();
    assert_eq!(outcome.failure_class, Some(TaskFailureClass::NonRetryable));
    assert_eq!(provider.calls(), 0);
}

#[tokio::test]
async fn provider_failure_after_intent_is_outcome_unknown_and_blocks_later_turns() {
    let harness = Harness::new();
    let handle = harness.service.open(harness.spec()).await.unwrap();
    harness.provider.set_fail(true);

    let first = harness
        .service
        .run_turn(&handle, turn("provider-unknown", "may have executed"))
        .await
        .unwrap();
    assert_eq!(first.status, AgentOutcomeStatus::OutcomeUnknown);
    assert_eq!(first.failure_class, Some(TaskFailureClass::OutcomeUnknown));
    harness.provider.set_fail(false);
    let later = harness
        .service
        .run_turn(&handle, turn("after-unknown", "must stay blocked"))
        .await
        .unwrap_err();
    assert!(matches!(
        later.failure_class,
        TaskFailureClass::OutcomeUnknown | TaskFailureClass::ReconciliationRequired | TaskFailureClass::NonRetryable
    ));
    assert_eq!(harness.provider.calls(), 1);
}

#[tokio::test]
async fn provider_unknown_closes_as_failed_in_ledger_and_sqlite() {
    let harness = Harness::new();
    let handle = harness.service.open(harness.spec()).await.unwrap();
    harness.provider.set_fail(true);
    let outcome = harness
        .service
        .run_turn(&handle, turn("provider-unknown-close", "may have executed"))
        .await
        .unwrap();
    assert_eq!(outcome.status, AgentOutcomeStatus::OutcomeUnknown);

    harness.service.close(&handle).await.unwrap();

    let close = harness.service.find_close(&handle).unwrap().unwrap();
    assert_eq!(close.terminal_state, AgentLifecycleState::Failed);
    let stored = harness
        .service
        .conversation_store()
        .unwrap()
        .load_conversation(&handle_identity(&handle))
        .unwrap()
        .unwrap();
    assert_eq!(stored.terminal_state.as_deref(), Some("failed"));
    let close_records = harness
        .ledger
        .records_for_run(&harness.run_id)
        .unwrap()
        .into_iter()
        .filter(|record| record.record_type == CLOSE_RECORD)
        .collect::<Vec<_>>();
    assert_eq!(close_records.len(), 1);
    assert_eq!(close_records[0].payload["terminal_state"], json!("failed"));
}

#[tokio::test]
async fn max_turns_failure_replays_exactly_and_closes_failed() {
    let harness = Harness::new();
    let mut spec = harness.spec();
    spec.config.max_turns = 0;
    let handle = harness.service.open(spec).await.unwrap();
    let original_turn = turn("max-turns-failure", "must stop at the configured turn limit");

    let first = harness.service.run_turn(&handle, original_turn.clone()).await.unwrap();
    assert_eq!(first.status, AgentOutcomeStatus::Failed);
    assert_eq!(first.failure_class, Some(TaskFailureClass::NonRetryable));
    assert!(first.error.as_deref().is_some_and(|error| error.contains("turn limit")));
    assert_eq!(harness.provider.calls(), 0);

    let restarted = service_with(
        harness.provider.clone(),
        config_for(harness._workspace.path(), &harness.sessions, true),
        harness._workspace.path(),
        Arc::clone(&harness.ledger),
        harness.run_id.clone(),
        harness.parent_id.clone(),
        ResourceBudget::default(),
    );
    let replay = restarted.run_turn(&handle, original_turn).await.unwrap();
    assert_eq!(
        serde_json::to_value(&replay).unwrap(),
        serde_json::to_value(&first).unwrap()
    );
    assert_eq!(harness.provider.calls(), 0);

    restarted.close(&handle).await.unwrap();
    assert_eq!(
        restarted.find_close(&handle).unwrap().unwrap().terminal_state,
        AgentLifecycleState::Failed
    );
}

#[tokio::test]
async fn fallback_failure_replays_exactly_and_closes_failed() {
    let harness = Harness::new();
    harness.provider.set_empty(true);
    let handle = harness.service.open(harness.spec()).await.unwrap();
    let original_turn = turn("fallback-failure", "empty model responses must fail durably");

    let first = harness.service.run_turn(&handle, original_turn.clone()).await.unwrap();
    assert_eq!(first.status, AgentOutcomeStatus::Failed);
    assert_eq!(first.failure_class, Some(TaskFailureClass::NonRetryable));
    assert!(
        first
            .error
            .as_deref()
            .is_some_and(|error| error.contains("valid completed result"))
    );
    assert_eq!(harness.provider.calls(), 2);

    let restarted = service_with(
        harness.provider.clone(),
        config_for(harness._workspace.path(), &harness.sessions, true),
        harness._workspace.path(),
        Arc::clone(&harness.ledger),
        harness.run_id.clone(),
        harness.parent_id.clone(),
        ResourceBudget::default(),
    );
    let replay = restarted.run_turn(&handle, original_turn).await.unwrap();
    assert_eq!(
        serde_json::to_value(&replay).unwrap(),
        serde_json::to_value(&first).unwrap()
    );
    assert_eq!(harness.provider.calls(), 2);

    restarted.close(&handle).await.unwrap();
    assert_eq!(
        restarted.find_close(&handle).unwrap().unwrap().terminal_state,
        AgentLifecycleState::Failed
    );
}

#[tokio::test]
async fn role_wall_time_is_cumulative_across_cold_resumed_turns() {
    let workspace = tempfile::tempdir().unwrap();
    let sessions = workspace.path().join("sessions");
    let provider = Arc::new(RecordingProvider::default());
    let now_ms = Arc::new(AtomicI64::new(10_000));
    let role_clock: Arc<dyn Fn() -> i64 + Send + Sync> = {
        let now_ms = Arc::clone(&now_ms);
        Arc::new(move || now_ms.load(Ordering::SeqCst))
    };
    let run_id = RunId::from("run-cumulative-wall-time");
    let parent_id = AgentId::from("coordinator");
    let service = service_with(
        provider.clone(),
        config_for(workspace.path(), &sessions, true),
        workspace.path(),
        Arc::new(InMemoryRuntimeLedger::default()),
        run_id.clone(),
        parent_id.clone(),
        ResourceBudget::default(),
    )
    .with_role_clock(role_clock);
    let mut spec = conversation_spec(run_id, parent_id);
    spec.resource_budget.max_wall_time_ms = Some(1_000);
    let handle = service.open(spec).await.unwrap();

    let first = service.run_turn(&handle, turn("wall-one", "first")).await.unwrap();
    now_ms.store(11_001, Ordering::SeqCst);
    let second = service.run_turn(&handle, turn("wall-two", "second")).await.unwrap();
    assert_eq!(first.status, AgentOutcomeStatus::Completed, "{first:?}");
    assert_eq!(second.failure_class, Some(TaskFailureClass::NonRetryable));
    assert_eq!(provider.calls(), 1);
}

#[tokio::test]
async fn role_turn_and_token_budgets_accumulate_across_resumed_turns() {
    let workspace = tempfile::tempdir().unwrap();
    let sessions = workspace.path().join("sessions");
    let provider = Arc::new(RecordingProvider::default());
    provider.set_input_tokens(1);
    let run_id = RunId::from("run-cumulative-turn-token");
    let parent_id = AgentId::from("coordinator");
    let service = service_with(
        provider.clone(),
        config_for(workspace.path(), &sessions, true),
        workspace.path(),
        Arc::new(InMemoryRuntimeLedger::default()),
        run_id.clone(),
        parent_id.clone(),
        ResourceBudget::default(),
    );
    let mut spec = conversation_spec(run_id, parent_id);
    spec.resource_budget.max_turns = Some(1);
    spec.resource_budget.max_tokens = Some(1);
    let handle = service.open(spec).await.unwrap();

    let first = service.run_turn(&handle, turn("usage-one", "first")).await.unwrap();
    let second = service.run_turn(&handle, turn("usage-two", "second")).await.unwrap();
    assert_eq!(first.status, AgentOutcomeStatus::Completed);
    assert_eq!(second.failure_class, Some(TaskFailureClass::NonRetryable));
    assert_eq!(provider.calls(), 1);
}

#[tokio::test]
async fn close_is_idempotent_retains_session_and_rejects_new_turns() {
    let harness = Harness::new();
    let handle = harness.service.open(harness.spec()).await.unwrap();
    harness.service.run_turn(&handle, turn("one", "first")).await.unwrap();

    harness.service.close(&handle).await.unwrap();
    harness.service.close(&handle).await.unwrap();

    assert!(
        SessionManager::new(harness.sessions, 20)
            .load_if_exists(&handle.session_id)
            .unwrap()
            .is_some()
    );
    let error = harness
        .service
        .run_turn(&handle, turn("after-close", "must fail"))
        .await
        .unwrap_err();
    assert_eq!(error.failure_class, TaskFailureClass::NonRetryable);
}

#[tokio::test]
async fn restart_close_repairs_outcome_persisted_before_idle_state() {
    let ledger = Arc::new(BeforeIdleFailureLedger::default());
    let harness = Harness::with_ledger(ledger.clone());
    let spec = harness.spec();
    let handle = harness.service.open(spec.clone()).await.unwrap();
    ledger.fail_idle_once();

    let error = harness
        .service
        .run_turn(&handle, turn("idle-crash", "finish before idle"))
        .await
        .unwrap_err();
    assert_eq!(error.failure_class, TaskFailureClass::ReconciliationRequired);
    assert_eq!(harness.provider.calls(), 1);

    let restarted = service_with(
        harness.provider.clone(),
        config_for(harness._workspace.path(), &harness.sessions, true),
        harness._workspace.path(),
        Arc::clone(&harness.ledger),
        harness.run_id.clone(),
        harness.parent_id.clone(),
        ResourceBudget::default(),
    );
    restarted.close(&handle).await.unwrap();
    assert_eq!(harness.provider.calls(), 1);
}

#[tokio::test]
async fn restart_exact_replay_repairs_idle_after_outcome_persisted_before_idle_state() {
    let ledger = Arc::new(BeforeIdleFailureLedger::default());
    let harness = Harness::with_ledger(ledger.clone());
    let spec = harness.spec();
    let handle = harness.service.open(spec.clone()).await.unwrap();
    let original_turn = turn("idle-replay", "finish before idle");
    ledger.fail_idle_once();
    let error = harness
        .service
        .run_turn(&handle, original_turn.clone())
        .await
        .unwrap_err();
    assert_eq!(error.failure_class, TaskFailureClass::ReconciliationRequired);

    let restarted = service_with(
        harness.provider.clone(),
        config_for(harness._workspace.path(), &harness.sessions, true),
        harness._workspace.path(),
        Arc::clone(&harness.ledger),
        harness.run_id.clone(),
        harness.parent_id.clone(),
        ResourceBudget::default(),
    );
    let replay = restarted.run_turn(&handle, original_turn).await.unwrap();

    assert_eq!(replay.status, AgentOutcomeStatus::Completed);
    assert_eq!(harness.provider.calls(), 1);
    assert_eq!(
        restarted
            .spawner
            .lifecycle_runtime
            .agents()
            .get(&handle.agent_id)
            .unwrap()
            .state,
        AgentLifecycleState::Idle
    );
}

#[tokio::test]
async fn durable_outcome_status_failure_and_error_combinations_are_exact() {
    let harness = Harness::new();
    let handle = harness.service.open(harness.spec()).await.unwrap();
    let turn = turn("outcome-combinations", "complete once");
    let identity = handle.turn_identity(&turn);
    let base = harness.service.run_turn(&handle, turn.clone()).await.unwrap();
    let statuses = [
        AgentOutcomeStatus::Completed,
        AgentOutcomeStatus::Failed,
        AgentOutcomeStatus::Cancelled,
        AgentOutcomeStatus::OutcomeUnknown,
        AgentOutcomeStatus::ReconciliationRequired,
    ];
    let failure_classes = [
        None,
        Some(TaskFailureClass::Retryable),
        Some(TaskFailureClass::NonRetryable),
        Some(TaskFailureClass::OutcomeUnknown),
        Some(TaskFailureClass::ReconciliationRequired),
    ];
    let errors = [None, Some("failure"), Some("")];

    for status in statuses {
        for failure_class in failure_classes {
            for error in errors {
                let mut candidate = base.clone();
                candidate.status = status;
                candidate.failure_class = failure_class;
                candidate.error = error.map(str::to_owned);
                let valid = match (status, failure_class) {
                    (AgentOutcomeStatus::Completed, None) => error.is_none(),
                    (
                        AgentOutcomeStatus::Failed,
                        Some(TaskFailureClass::Retryable | TaskFailureClass::NonRetryable),
                    )
                    | (AgentOutcomeStatus::Cancelled, Some(TaskFailureClass::NonRetryable))
                    | (AgentOutcomeStatus::OutcomeUnknown, Some(TaskFailureClass::OutcomeUnknown))
                    | (AgentOutcomeStatus::ReconciliationRequired, Some(TaskFailureClass::ReconciliationRequired)) => {
                        error.is_some_and(|message| !message.is_empty())
                    }
                    _ => false,
                };
                assert_eq!(
                    conversation_records::validate_outcome(&handle, &turn.turn_id, &identity, &candidate).is_ok(),
                    valid,
                    "status={status:?}, failure_class={failure_class:?}, error={error:?}"
                );
                if valid {
                    assert_eq!(
                        blocks_following_turns(&candidate),
                        matches!(
                            failure_class,
                            Some(
                                TaskFailureClass::NonRetryable
                                    | TaskFailureClass::OutcomeUnknown
                                    | TaskFailureClass::ReconciliationRequired
                            )
                        )
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn close_rejects_a_terminal_sqlite_outcome_with_an_invalid_status_combination() {
    let harness = Harness::new();
    let handle = harness.service.open(harness.spec()).await.unwrap();
    let turn = turn("forged-terminal-close", "complete once");
    let mut forged = harness.service.run_turn(&handle, turn.clone()).await.unwrap();
    forged.status = AgentOutcomeStatus::OutcomeUnknown;
    forged.failure_class = Some(TaskFailureClass::Retryable);
    forged.error = Some("invalid durable combination".to_owned());
    let store = harness.service.conversation_store().unwrap();
    Connection::open(store.database_path())
        .unwrap()
        .execute(
            "UPDATE agent_conversation_turns SET outcome_json = ?5
             WHERE run_id = ?1 AND parent_agent_id = ?2 AND conversation_id = ?3 AND turn_id = ?4",
            rusqlite::params![
                handle.run_id.as_str(),
                handle.parent_agent_id.as_str(),
                &handle.conversation_id,
                &turn.turn_id,
                serde_json::to_vec(&forged).unwrap(),
            ],
        )
        .unwrap();

    let error = harness.service.close(&handle).await.unwrap_err();

    assert_eq!(error.failure_class, TaskFailureClass::ReconciliationRequired);
    assert!(harness.service.find_close(&handle).unwrap().is_none());
}
