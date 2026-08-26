#[tokio::test]
async fn two_services_execute_different_turns_in_durable_fifo_order() {
    let harness = Harness::new();
    let spec = harness.spec();
    let handle = harness.service.open(spec.clone()).await.unwrap();
    let second = Arc::new(service_with(
        harness.provider.clone(),
        config_for(harness._workspace.path(), &harness.sessions, true),
        harness._workspace.path(),
        Arc::clone(&harness.ledger),
        harness.run_id.clone(),
        harness.parent_id.clone(),
        ResourceBudget::default(),
    ));
    second.open(spec).await.unwrap();
    let (first_reached_provider, resume_first) = harness.provider.pause_next_call();
    let first = Arc::new(harness.service);
    let first_handle = handle.clone();
    let first_service = Arc::clone(&first);
    let first_turn = tokio::spawn(async move {
        first_service
            .run_turn(&first_handle, turn("fifo-one", "first prompt"))
            .await
    });
    first_reached_provider.wait().await;
    assert_eq!(harness.provider.calls(), 1, "first FIFO turn did not start");
    let second_handle = handle.clone();
    let second_service = Arc::clone(&second);
    let second_turn = tokio::spawn(async move {
        second_service
            .run_turn(&second_handle, turn("fifo-two", "second prompt"))
            .await
    });
    resume_first.wait().await;

    let first_outcome = first_turn.await.unwrap().unwrap();
    let second_outcome = second_turn.await.unwrap().unwrap();
    assert_eq!(first_outcome.status, AgentOutcomeStatus::Completed);
    assert_eq!(second_outcome.status, AgentOutcomeStatus::Completed);
    assert_eq!(harness.provider.prompts(), vec!["first prompt", "second prompt"]);
}

#[tokio::test]
async fn queued_second_turn_is_blocked_after_first_outcome_unknown_and_close_settles_the_queue() {
    let harness = Harness::new();
    let spec = harness.spec();
    let handle = harness.service.open(spec.clone()).await.unwrap();
    let second = Arc::new(service_with(
        harness.provider.clone(),
        config_for(harness._workspace.path(), &harness.sessions, true),
        harness._workspace.path(),
        Arc::clone(&harness.ledger),
        harness.run_id.clone(),
        harness.parent_id.clone(),
        ResourceBudget::default(),
    ));
    second.open(spec).await.unwrap();
    harness.provider.set_fail(true);
    let (head_reached_provider, resume_head) = harness.provider.pause_next_call();
    let first = Arc::new(harness.service);
    let first_service = Arc::clone(&first);
    let first_handle = handle.clone();
    let first_turn = tokio::spawn(async move {
        first_service
            .run_turn(&first_handle, turn("unknown-head", "may have executed"))
            .await
    });
    head_reached_provider.wait().await;
    assert_eq!(harness.provider.calls(), 1, "queue head did not reach the Provider");
    let (tail_enqueued, resume_tail) = second.install_turn_enqueued_hook();
    let second_service = Arc::clone(&second);
    let second_handle = handle.clone();
    let second_turn = tokio::spawn(async move {
        second_service
            .run_turn(&second_handle, turn("blocked-tail", "must never execute"))
            .await
    });
    tail_enqueued.wait().await;
    let store = first.conversation_store().unwrap();
    let identity = handle_identity(&handle);
    assert_eq!(store.list_conversation_turns(&identity).unwrap().len(), 2);
    resume_head.wait().await;
    resume_tail.wait().await;

    let first_outcome = first_turn.await.unwrap().unwrap();
    assert_eq!(first_outcome.status, AgentOutcomeStatus::OutcomeUnknown);
    let second_error = second_turn.await.unwrap().unwrap_err();
    assert_eq!(second_error.failure_class, TaskFailureClass::OutcomeUnknown);
    assert_eq!(harness.provider.calls(), 1, "blocked FIFO tail called the Provider");

    first.close(&handle).await.unwrap();
    let turns = store.list_conversation_turns(&identity).unwrap();
    assert!(turns.iter().all(|turn| turn.state.is_terminal()));
    let conversation = store.load_conversation(&identity).unwrap().unwrap();
    assert_eq!(conversation.next_to_run, conversation.next_sequence);
    assert!(conversation.active_sequence.is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn sqlite_lock_wait_does_not_block_the_current_thread_runtime() {
    let harness = Harness::new();
    let handle = harness.service.open(harness.spec()).await.unwrap();
    let database_path = harness
        .service
        .conversation_store()
        .unwrap()
        .database_path()
        .to_path_buf();
    let timer_ran = Arc::new(AtomicBool::new(false));
    let (ready_tx, ready_rx) = mpsc::channel();
    let (observed_tx, observed_rx) = mpsc::channel();
    let observed_timer = Arc::clone(&timer_ran);
    let blocker = std::thread::spawn(move || {
        let blocker = Connection::open(database_path).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
        ready_tx.send(()).unwrap();
        std::thread::sleep(Duration::from_millis(250));
        observed_tx.send(observed_timer.load(Ordering::SeqCst)).unwrap();
        blocker.execute_batch("ROLLBACK").unwrap();
    });
    ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let timer_flag = Arc::clone(&timer_ran);
    let timer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        timer_flag.store(true, Ordering::SeqCst);
    });

    let outcome = harness
        .service
        .run_turn(&handle, turn("sqlite-contention", "runtime must remain responsive"))
        .await
        .unwrap();
    timer.await.unwrap();
    blocker.join().unwrap();

    assert_eq!(outcome.status, AgentOutcomeStatus::Completed);
    assert!(
        observed_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "a synchronous SQLite wait blocked the current-thread Tokio runtime"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn opening_a_new_conversation_does_not_block_tokio_while_sqlite_is_locked() {
    let harness = Harness::new();
    let store = SessionStore::open(&harness.sessions).unwrap();
    let database_path = store.database_path().to_path_buf();
    let timer_ran = Arc::new(AtomicBool::new(false));
    let (ready_tx, ready_rx) = mpsc::channel();
    let (observed_tx, observed_rx) = mpsc::channel();
    let observed_timer = Arc::clone(&timer_ran);
    let blocker = std::thread::spawn(move || {
        let blocker = Connection::open(database_path).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
        ready_tx.send(()).unwrap();
        std::thread::sleep(Duration::from_millis(250));
        observed_tx.send(observed_timer.load(Ordering::SeqCst)).unwrap();
        blocker.execute_batch("ROLLBACK").unwrap();
    });
    ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let timer_flag = Arc::clone(&timer_ran);
    let timer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        timer_flag.store(true, Ordering::SeqCst);
    });

    let handle = harness.service.open(harness.spec()).await.unwrap();
    timer.await.unwrap();
    blocker.join().unwrap();

    assert_eq!(handle.conversation_id, "conversation-main");
    assert!(
        observed_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "session creation blocked the current-thread Tokio runtime"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn provider_completion_and_session_release_do_not_block_tokio_on_sqlite() {
    let harness = Harness::new();
    let handle = harness.service.open(harness.spec()).await.unwrap();
    let database_path = harness
        .service
        .conversation_store()
        .unwrap()
        .database_path()
        .to_path_buf();
    let (provider_reached, resume_provider) = harness.provider.pause_next_call();
    let service = Arc::new(harness.service);
    let running_service = Arc::clone(&service);
    let running_handle = handle.clone();
    let running = tokio::spawn(async move {
        running_service
            .run_turn(&running_handle, turn("release-contention", "complete while locked"))
            .await
    });
    provider_reached.wait().await;

    let timer_ran = Arc::new(AtomicBool::new(false));
    let (ready_tx, ready_rx) = mpsc::channel();
    let (observed_tx, observed_rx) = mpsc::channel();
    let observed_timer = Arc::clone(&timer_ran);
    let blocker = std::thread::spawn(move || {
        let blocker = Connection::open(database_path).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
        ready_tx.send(()).unwrap();
        std::thread::sleep(Duration::from_millis(250));
        observed_tx.send(observed_timer.load(Ordering::SeqCst)).unwrap();
        blocker.execute_batch("ROLLBACK").unwrap();
    });
    ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let timer_flag = Arc::clone(&timer_ran);
    let timer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        timer_flag.store(true, Ordering::SeqCst);
    });
    resume_provider.wait().await;

    let outcome = running.await.unwrap().unwrap();
    timer.await.unwrap();
    blocker.join().unwrap();
    assert_eq!(outcome.status, AgentOutcomeStatus::Completed);
    assert!(
        observed_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        "Provider completion or session release blocked the current-thread Tokio runtime"
    );
}

#[test]
fn long_provider_turn_does_not_starve_short_sqlite_work_on_a_small_blocking_pool() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let harness = Harness::new();
        let first_handle = harness.service.open(harness.spec()).await.unwrap();
        let (provider_reached, resume_provider) = harness.provider.pause_next_call();
        let mut second_spec = harness.spec();
        second_spec.conversation_id = "conversation-while-provider-waits".to_owned();
        let service = Arc::new(harness.service);
        let running_service = Arc::clone(&service);
        let running = tokio::spawn(async move {
            running_service
                .run_turn(&first_handle, turn("long-provider", "wait for release"))
                .await
        });
        provider_reached.wait().await;

        let opening_service = Arc::clone(&service);
        let mut opening = tokio::spawn(async move { opening_service.open(second_spec).await });
        let opened_while_busy = tokio::time::timeout(Duration::from_secs(5), &mut opening).await;
        resume_provider.wait().await;
        running.await.unwrap().unwrap();
        if opened_while_busy.is_err() {
            let _ = opening.await;
            panic!("a long Provider turn exhausted the blocking pool needed by SQLite");
        }
        opened_while_busy.unwrap().unwrap().unwrap();
    });
}

#[tokio::test]
async fn resource_restore_does_not_append_noop_snapshots_and_invalid_handle_writes_nothing() {
    let harness = Harness::new();
    let handle = harness.service.open(harness.spec()).await.unwrap();
    let resource_run = RunId::new(format!("{}:agent-resource:{}", handle.run_id, handle.agent_id));
    let checkpoint_count = || {
        harness
            .ledger
            .records_for_run(&resource_run)
            .unwrap()
            .iter()
            .filter(|record| record.record_type == "resource_usage_checkpoint")
            .count()
    };
    assert_eq!(checkpoint_count(), 1);

    harness
        .service
        .run_turn(&handle, turn("resource-one", "first"))
        .await
        .unwrap();
    harness
        .service
        .run_turn(&handle, turn("resource-two", "second"))
        .await
        .unwrap();
    assert_eq!(checkpoint_count(), 1, "reattach appended a no-op resource snapshot");

    let before_invalid = harness.ledger.records_for_run(&resource_run).unwrap().len();
    let mut invalid = handle;
    invalid.environment_digest = "tampered-environment".to_owned();
    let error = harness
        .service
        .run_turn(&invalid, turn("invalid-resource", "must not write"))
        .await
        .unwrap_err();
    assert_eq!(error.failure_class, TaskFailureClass::ReconciliationRequired);
    assert_eq!(
        harness.ledger.records_for_run(&resource_run).unwrap().len(),
        before_invalid
    );
}

#[tokio::test]
async fn aborting_run_turn_cleans_runtime_maps_releases_session_and_close_recovers() {
    let harness = Harness::new();
    let handle = harness.service.open(harness.spec()).await.unwrap();
    let (provider_reached, _resume_provider) = harness.provider.pause_next_call();
    let service = Arc::new(harness.service);
    let running_service = Arc::clone(&service);
    let running_handle = handle.clone();
    let running = tokio::spawn(async move {
        running_service
            .run_turn(&running_handle, turn("abort-active", "will be aborted"))
            .await
    });
    let active_key = conversation_lock_key(&handle.run_id, &handle.parent_agent_id, &handle.conversation_id);
    provider_reached.wait().await;
    assert!(
        service
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .contains_key(&active_key)
    );
    assert_eq!(harness.provider.calls(), 1);
    running.abort();
    let _ = running.await;
    service.wait_for_session_cleanup(&handle).await;

    assert!(
        service
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_empty()
    );
    assert!(
        service
            .locks
            .locks
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_empty()
    );
    let manager = SessionManager::new(harness.sessions, 20);
    manager.load_active_session(&handle.session_id).unwrap();
    manager.release_active_session().unwrap();
    service.close(&handle).await.unwrap();
    assert_eq!(
        service.find_close(&handle).unwrap().unwrap().terminal_state,
        AgentLifecycleState::Failed
    );
}

#[tokio::test]
async fn close_cas_wins_before_turn_admission_without_provider_or_intent() {
    let harness = Harness::new();
    let spec = harness.spec();
    let handle = harness.service.open(spec.clone()).await.unwrap();
    let turn_service = Arc::new(service_with(
        harness.provider.clone(),
        config_for(harness._workspace.path(), &harness.sessions, true),
        harness._workspace.path(),
        Arc::clone(&harness.ledger),
        harness.run_id.clone(),
        harness.parent_id.clone(),
        ResourceBudget::default(),
    ));
    turn_service.open(spec).await.unwrap();
    let (reached, resume) = turn_service.install_turn_admission_hook();
    let running_service = Arc::clone(&turn_service);
    let running_handle = handle.clone();
    let running = tokio::spawn(async move {
        running_service
            .run_turn(&running_handle, turn("close-race", "must never execute"))
            .await
    });
    reached.wait().await;

    harness.service.close(&handle).await.unwrap();
    resume.wait().await;
    let error = running.await.unwrap().unwrap_err();
    assert_eq!(error.failure_class, TaskFailureClass::NonRetryable);
    assert_eq!(harness.provider.calls(), 0);
    assert!(
        harness
            .ledger
            .records_for_run(&harness.run_id)
            .unwrap()
            .iter()
            .all(|record| {
                record.record_type != TURN_INTENT_RECORD
                    || record.payload.get("turn_id").and_then(Value::as_str) != Some("close-race")
            })
    );
}

#[tokio::test]
async fn active_key_is_run_scoped_and_invalid_close_cannot_cancel_live_turn() {
    let harness = Harness::new();
    let handle = harness.service.open(harness.spec()).await.unwrap();
    harness.provider.set_delay_ms(100);
    let service = Arc::new(harness.service);
    let running_service = Arc::clone(&service);
    let running_handle = handle.clone();
    let running = tokio::spawn(async move {
        running_service
            .run_turn(&running_handle, turn("live", "finish normally"))
            .await
            .unwrap()
    });
    let active_key = conversation_lock_key(&handle.run_id, &handle.parent_agent_id, &handle.conversation_id);
    for _ in 0..1_000 {
        if service
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .contains_key(&active_key)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    {
        let active = service.active.lock().unwrap();
        assert!(active.contains_key(&active_key));
        assert!(!active.contains_key(&handle.conversation_id));
    }
    let mut tampered = handle;
    tampered.agent_id = AgentId::from("another-agent");
    let error = service.close(&tampered).await.unwrap_err();
    assert_eq!(error.failure_class, TaskFailureClass::ReconciliationRequired);

    let outcome = running.await.unwrap();
    assert_eq!(outcome.status, AgentOutcomeStatus::Completed);
    assert_eq!(harness.provider.calls(), 1);
}

#[tokio::test]
async fn live_close_only_notifies_the_full_run_parent_conversation_key() {
    let harness = Harness::new();
    let handle = harness.service.open(harness.spec()).await.unwrap();
    let (provider_reached, _resume_provider) = harness.provider.pause_next_call();
    let service = Arc::new(harness.service);
    let running_service = Arc::clone(&service);
    let running_handle = handle.clone();
    let running = tokio::spawn(async move {
        running_service
            .run_turn(&running_handle, turn("live-close", "cancel safely"))
            .await
            .unwrap()
    });

    let active_key = conversation_lock_key(&handle.run_id, &handle.parent_agent_id, &handle.conversation_id);
    provider_reached.wait().await;
    assert!(
        service
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .contains_key(&active_key),
        "turn did not become active"
    );

    let foreign_notify = Arc::new(Notify::new());
    let foreign_key = conversation_lock_key(
        &RunId::from("another-run"),
        &handle.parent_agent_id,
        &handle.conversation_id,
    );
    let foreign_parent_key = conversation_lock_key(
        &handle.run_id,
        &AgentId::from("another-parent"),
        &handle.conversation_id,
    );
    assert_ne!(active_key, foreign_key);
    assert_ne!(active_key, foreign_parent_key);
    service
        .active
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .insert(foreign_key.clone(), Arc::clone(&foreign_notify));
    let mut foreign_waiter = Box::pin(foreign_notify.notified());
    assert!(matches!(futures::poll!(&mut foreign_waiter), Poll::Pending));

    service.close(&handle).await.unwrap();
    let outcome = running.await.unwrap();
    assert_eq!(outcome.status, AgentOutcomeStatus::Cancelled);
    assert!(
        matches!(futures::poll!(&mut foreign_waiter), Poll::Pending),
        "closing one Run notified the same conversation ID in another Run"
    );
    service
        .active
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .remove(&foreign_key);

    assert_eq!(harness.provider.calls(), 1);
    let closed = service.find_close(&handle).unwrap().unwrap();
    assert_eq!(closed.terminal_state, AgentLifecycleState::Completed);
}

#[tokio::test]
async fn concurrent_turns_run_in_fifo_order() {
    let harness = Harness::new();
    let handle = harness.service.open(harness.spec()).await.unwrap();
    let service = Arc::new(harness.service);
    let first_service = Arc::clone(&service);
    let first_handle = handle.clone();
    let first = tokio::spawn(async move {
        first_service
            .run_turn(&first_handle, turn("one", "first"))
            .await
            .unwrap()
    });
    tokio::task::yield_now().await;
    let second_service = Arc::clone(&service);
    let second_handle = handle.clone();
    let second = tokio::spawn(async move {
        second_service
            .run_turn(&second_handle, turn("two", "second"))
            .await
            .unwrap()
    });

    assert_eq!(output_text(&first.await.unwrap()), "reply-1");
    assert_eq!(output_text(&second.await.unwrap()), "reply-2");
}

#[tokio::test]
async fn active_agent_permit_is_released_between_turns_at_limit_one() {
    let workspace = tempfile::tempdir().unwrap();
    let sessions = workspace.path().join("sessions");
    let provider = Arc::new(RecordingProvider::default());
    let run_id = RunId::from("run-limit-one");
    let parent_id = AgentId::from("coordinator");
    let service = service_with(
        provider,
        config_for(workspace.path(), &sessions, true),
        workspace.path(),
        Arc::new(InMemoryRuntimeLedger::default()),
        run_id.clone(),
        parent_id.clone(),
        ResourceBudget {
            max_active_agents: Some(1),
            ..ResourceBudget::default()
        },
    );
    let resources = service.spawner.resource_manager();
    let handle = service.open(conversation_spec(run_id, parent_id)).await.unwrap();

    tokio::time::timeout(Duration::from_secs(10), service.run_turn(&handle, turn("one", "first")))
        .await
        .unwrap()
        .unwrap();
    let permit = resources
        .try_acquire_reattached_agent(1)
        .expect("turn must release permit");
    drop(permit);
    tokio::time::timeout(
        Duration::from_secs(10),
        service.run_turn(&handle, turn("two", "second")),
    )
    .await
    .unwrap()
    .unwrap();
}
