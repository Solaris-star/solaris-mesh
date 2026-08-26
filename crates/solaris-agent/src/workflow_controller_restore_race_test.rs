#[test]
fn concurrent_completion_is_not_overwritten_by_same_process_restore() {
    use std::sync::mpsc;
    use std::time::Duration;

    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};

    let ledger: Arc<dyn RuntimeLedger> = Arc::new(InMemoryRuntimeLedger::default());
    let controller = Arc::new(WorkflowController::new(ledger));
    let definition = WorkflowDefinition {
        id: "restore-complete-race".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Restore and completion race".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("work", &[])],
        outputs: BTreeMap::new(),
    };
    let run = RunId::from("restore-complete-race-root:workflow:one");
    controller.register(definition.clone()).unwrap();
    controller.start(run.clone(), &definition.id, json!({})).unwrap();
    let attempt = controller.begin_attempt(&run, &definition.nodes[0]).unwrap().attempt_id;
    let (prepared_tx, prepared_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let restoring = {
        let controller = Arc::clone(&controller);
        let run = run.clone();
        std::thread::spawn(move || {
            controller.restore_workflow_run_with_before_projection(&run, || {
                prepared_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            })
        })
    };
    prepared_rx.recv().unwrap();
    let (completed_tx, completed_rx) = mpsc::channel();
    let completing = {
        let controller = Arc::clone(&controller);
        let run = run.clone();
        std::thread::spawn(move || {
            let result = controller.complete_node(&run, "work", &attempt, json!({"done": true}));
            completed_tx.send(result).unwrap();
        })
    };
    let completed_early = completed_rx.recv_timeout(Duration::from_millis(100)).ok();
    release_tx.send(()).unwrap();
    let completion = completed_early.unwrap_or_else(|| completed_rx.recv().unwrap());
    restoring.join().unwrap().unwrap();
    completing.join().unwrap();

    assert!(completion.unwrap());
    assert_eq!(
        controller.snapshot(&run).unwrap().nodes["work"].status,
        WorkflowNodeStatus::Completed
    );
}

#[test]
fn sqlite_restore_claim_fences_third_host_completion_until_release() {
    use std::sync::{Barrier, Condvar, Mutex, mpsc};
    use std::time::Duration;

    use crate::runtime_ledger::{RuntimeLedger, SqliteRuntimeLedger};

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ledger.sqlite3");
    let definition = WorkflowDefinition {
        id: "sqlite-restore-race".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Cross-host restore race".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("work", &[])],
        outputs: BTreeMap::new(),
    };
    let root = RunId::from("sqlite-restore-race-root");
    let run = RunId::from("sqlite-restore-race-root:workflow:one");
    let seed_ledger: Arc<dyn RuntimeLedger> = Arc::new(SqliteRuntimeLedger::open(&path).unwrap());
    let seed = Arc::new(WorkflowController::new(seed_ledger));
    seed.register(definition.clone()).unwrap();
    seed.start(run.clone(), &definition.id, json!({})).unwrap();
    let attempt = seed.begin_attempt(&run, &definition.nodes[0]).unwrap().attempt_id;

    let first = Arc::new(WorkflowController::new(Arc::new(
        SqliteRuntimeLedger::open(&path).unwrap(),
    )));
    let second = Arc::new(WorkflowController::new(Arc::new(
        SqliteRuntimeLedger::open(&path).unwrap(),
    )));
    first.register(definition.clone()).unwrap();
    second.register(definition).unwrap();
    let start = Arc::new(Barrier::new(3));
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let (ready_tx, ready_rx) = mpsc::channel();
    let spawn_restore = |controller: Arc<WorkflowController>| {
        let start = Arc::clone(&start);
        let gate = Arc::clone(&gate);
        let ready_tx = ready_tx.clone();
        let run = run.clone();
        std::thread::spawn(move || {
            start.wait();
            controller.restore_workflow_run_with_before_projection(&run, || {
                ready_tx.send(()).unwrap();
                let (released, wake) = &*gate;
                let mut released = released.lock().unwrap();
                while !*released {
                    released = wake.wait(released).unwrap();
                }
            })
        })
    };
    let first_restore = spawn_restore(Arc::clone(&first));
    let second_restore = spawn_restore(Arc::clone(&second));
    start.wait();
    ready_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    let (completed_tx, completed_rx) = mpsc::channel();
    let completing = {
        let seed = Arc::clone(&seed);
        let run = run.clone();
        std::thread::spawn(move || {
            completed_tx
                .send(seed.complete_node(&run, "work", &attempt, json!({"done": true})))
                .unwrap();
        })
    };
    let completed_early = completed_rx.recv_timeout(Duration::from_millis(100)).ok();
    let completed_before_release = completed_early.is_some();
    {
        let (released, wake) = &*gate;
        *released.lock().unwrap() = true;
        wake.notify_all();
    }
    let completion = completed_early.unwrap_or_else(|| completed_rx.recv_timeout(Duration::from_secs(2)).unwrap());
    first_restore.join().unwrap().unwrap();
    second_restore.join().unwrap().unwrap();
    completing.join().unwrap();
    first.restore_from_ledger(&root).unwrap();
    second.restore_from_ledger(&root).unwrap();

    assert!(
        !completed_before_release,
        "third Host completion bypassed an active restore mutation lease"
    );
    assert!(completion.unwrap());
    assert_eq!(
        first.snapshot(&run).unwrap().nodes["work"].status,
        WorkflowNodeStatus::Completed
    );
    assert_eq!(
        second.snapshot(&run).unwrap().nodes["work"].status,
        WorkflowNodeStatus::Completed
    );
}

#[test]
fn prepared_workflow_restore_has_no_projection_or_ledger_side_effects() {
    use std::sync::mpsc;

    use crate::collaboration_runtime::CollaborationRuntime;
    use crate::resource_policy::ResourcePolicy;
    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};
    use crate::scheduler::Scheduler;

    let ledger: Arc<dyn RuntimeLedger> = Arc::new(InMemoryRuntimeLedger::default());
    let definition = WorkflowDefinition {
        id: "restore-prepared-pure".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Prepared restore must be read-only".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("work", &[])],
        outputs: BTreeMap::new(),
    };
    let run = RunId::from("restore-prepared-pure-root:workflow:one");
    let seed = WorkflowController::new(Arc::clone(&ledger));
    seed.register(definition.clone()).unwrap();
    seed.start(run.clone(), &definition.id, json!({})).unwrap();
    let initial_records = ledger.records_for_run(&run).unwrap().len();
    let runtime = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(1)),
        Arc::clone(&ledger),
    ));
    let restored = Arc::new(WorkflowController::with_runtime(runtime));
    restored.register(definition).unwrap();
    let task_id = TaskId::from(format!("workflow:{run}:work"));
    let (prepared_tx, prepared_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let restoring = {
        let restored = Arc::clone(&restored);
        let run = run.clone();
        std::thread::spawn(move || {
            restored.restore_workflow_run_with_observers(
                &run,
                || {
                    prepared_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                },
                || {},
            )
        })
    };
    prepared_rx.recv().unwrap();

    assert!(restored.snapshot(&run).is_none());
    assert!(restored.task_registry().get(&task_id).is_none());
    assert_eq!(ledger.records_for_run(&run).unwrap().len(), initial_records);

    release_tx.send(()).unwrap();
    assert!(restoring.join().unwrap().unwrap());
}

#[test]
fn sqlite_restore_reloads_after_high_water_changes_before_commit() {
    use solaris_types::effect::DurabilityClass;

    use crate::runtime_ledger::{RuntimeLedger, SqliteRuntimeLedger};

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ledger.sqlite3");
    let definition = WorkflowDefinition {
        id: "sqlite-restore-stale".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Restore must reload a stale high-water mark".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("work", &[])],
        outputs: BTreeMap::new(),
    };
    let run = RunId::from("sqlite-restore-stale-root:workflow:one");
    let seed_ledger: Arc<dyn RuntimeLedger> = Arc::new(SqliteRuntimeLedger::open(&path).unwrap());
    let seed = WorkflowController::new(Arc::clone(&seed_ledger));
    seed.register(definition.clone()).unwrap();
    seed.start(run.clone(), &definition.id, json!({})).unwrap();
    let initial_high_water = seed_ledger.records_for_run(&run).unwrap().last().unwrap().seq;

    let restored_ledger: Arc<dyn RuntimeLedger> = Arc::new(SqliteRuntimeLedger::open(&path).unwrap());
    let restored = WorkflowController::new(Arc::clone(&restored_ledger));
    restored.register(definition).unwrap();
    let concurrent_ledger = Arc::clone(&seed_ledger);
    let concurrent_run = run.clone();
    assert!(
        restored
            .restore_workflow_run_with_observers(
                &run,
                move || {
                    concurrent_ledger
                        .append(
                            &concurrent_run,
                            DurabilityClass::SyncCritical,
                            "test_concurrent_marker",
                            json!({"after": initial_high_water}),
                        )
                        .unwrap();
                },
                || {},
            )
            .unwrap()
    );

    let final_high_water = restored_ledger.records_for_run(&run).unwrap().last().unwrap().seq;
    assert!(final_high_water > initial_high_water);
    assert_eq!(
        restored
            .runs
            .read()
            .unwrap()
            .get(&run)
            .map(|projection| projection.durable_sequence),
        Some(final_high_water)
    );
}

#[test]
fn sqlite_restore_stops_when_heartbeat_renewal_loses_its_epoch() {
    use std::sync::Mutex;

    use crate::runtime_ledger::{RuntimeLedger, SqliteRuntimeLedger, WorkflowMutationLease};

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ledger.sqlite3");
    let definition = WorkflowDefinition {
        id: "sqlite-restore-lost-renewal".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Restore must stop after losing its lease epoch".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("work", &[])],
        outputs: BTreeMap::new(),
    };
    let run = RunId::from("sqlite-restore-lost-renewal-root:workflow:one");
    let seed_ledger: Arc<dyn RuntimeLedger> = Arc::new(SqliteRuntimeLedger::open(&path).unwrap());
    let seed = WorkflowController::new(Arc::clone(&seed_ledger));
    seed.register(definition.clone()).unwrap();
    seed.start(run.clone(), &definition.id, json!({})).unwrap();
    let initial_records = seed_ledger.records_for_run(&run).unwrap().len();

    let restored_ledger: Arc<dyn RuntimeLedger> = Arc::new(SqliteRuntimeLedger::open(&path).unwrap());
    let restored = WorkflowController::new(Arc::clone(&restored_ledger));
    restored.register(definition).unwrap();
    let takeover = Arc::new(SqliteRuntimeLedger::open(&path).unwrap());
    let replacement: Arc<Mutex<Option<WorkflowMutationLease>>> = Arc::new(Mutex::new(None));
    let replacement_for_hook = Arc::clone(&replacement);
    let takeover_for_hook = Arc::clone(&takeover);
    let path_for_hook = path.clone();
    let run_for_hook = run.clone();
    let error = restored
        .restore_workflow_run_with_renew_observer(&run, move |_| {
            let now = chrono::Utc::now().timestamp_millis();
            let connection = rusqlite::Connection::open(path_for_hook).unwrap();
            connection
                .execute(
                    "UPDATE runtime_ledger_workflow_mutation_leases
                     SET heartbeat_at_ms = ?1, expires_at_ms = ?2
                     WHERE run_id = ?3",
                    rusqlite::params![now - 100_000, now - 1, run_for_hook.as_str()],
                )
                .unwrap();
            let lease = takeover_for_hook
                .acquire_workflow_mutation_lease(&run_for_hook, "replacement", now)
                .unwrap();
            *replacement_for_hook.lock().unwrap() = Some(lease);
        })
        .unwrap_err();

    assert!(error.contains("failed to renew Workflow mutation lease"), "{error}");
    assert!(restored.snapshot(&run).is_none());
    assert_eq!(restored_ledger.records_for_run(&run).unwrap().len(), initial_records);
    let replacement = replacement.lock().unwrap().take().unwrap();
    takeover.release_workflow_mutation_lease(&replacement).unwrap();
}

#[test]
fn expired_restore_cannot_publish_tasks_or_projection_after_takeover_completion() {
    use crate::runtime_ledger::{RuntimeLedger, SqliteRuntimeLedger};

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ledger.sqlite3");
    let definition = WorkflowDefinition {
        id: "sqlite-restore-expired-after-commit".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Expired restore cannot publish prepared state".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("work", &[])],
        outputs: BTreeMap::new(),
    };
    let run = RunId::from("sqlite-restore-expired-after-commit-root:workflow:one");
    let seed_ledger: Arc<dyn RuntimeLedger> = Arc::new(SqliteRuntimeLedger::open(&path).unwrap());
    let seed = WorkflowController::new(Arc::clone(&seed_ledger));
    seed.register(definition.clone()).unwrap();
    seed.start(run.clone(), &definition.id, json!({})).unwrap();
    let attempt = seed.begin_attempt(&run, &definition.nodes[0]).unwrap().attempt_id;

    let restored_ledger: Arc<dyn RuntimeLedger> = Arc::new(SqliteRuntimeLedger::open(&path).unwrap());
    let restored = WorkflowController::new(Arc::clone(&restored_ledger));
    restored.register(definition).unwrap();
    let path_for_takeover = path.clone();
    let run_for_takeover = run.clone();
    let error = restored
        .restore_workflow_run_with_before_projection(&run, || {
            let now = chrono::Utc::now().timestamp_millis();
            rusqlite::Connection::open(path_for_takeover)
                .unwrap()
                .execute(
                    "UPDATE runtime_ledger_workflow_mutation_leases
                     SET heartbeat_at_ms = ?1, expires_at_ms = ?2
                     WHERE run_id = ?3",
                    rusqlite::params![now - 100_000, now - 1, run_for_takeover.as_str()],
                )
                .unwrap();
            assert!(
                seed.complete_node(&run_for_takeover, "work", &attempt, json!({"done": true}))
                    .unwrap()
            );
        })
        .unwrap_err();

    assert!(error.contains("mutation lease was lost"), "{error}");
    assert!(
        restored.snapshot(&run).is_none(),
        "expired restore published a stale projection"
    );
    assert!(
        restored
            .task_registry()
            .get(&TaskId::from(format!("workflow:{run}:work")))
            .is_none(),
        "expired restore published a stale Task projection"
    );
    assert_eq!(
        seed.snapshot(&run).unwrap().nodes["work"].status,
        WorkflowNodeStatus::Completed
    );
}

#[test]
fn expired_ordinary_mutation_is_rejected_before_its_guarded_append() {
    use std::sync::Mutex;

    use crate::runtime_ledger::{RuntimeLedger, SqliteRuntimeLedger, WorkflowMutationLease};

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ledger.sqlite3");
    let definition = WorkflowDefinition {
        id: "ordinary-expired-before-append".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Expired ordinary mutation cannot append".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("work", &[])],
        outputs: BTreeMap::new(),
    };
    let run = RunId::from("ordinary-expired-before-append-root:workflow:one");
    let ledger: Arc<dyn RuntimeLedger> = Arc::new(SqliteRuntimeLedger::open(&path).unwrap());
    let controller = WorkflowController::new(Arc::clone(&ledger));
    controller.register(definition).unwrap();
    controller
        .start(run.clone(), "ordinary-expired-before-append", json!({}))
        .unwrap();
    let initial_records = ledger.records_for_run(&run).unwrap();
    let takeover = Arc::new(SqliteRuntimeLedger::open(&path).unwrap());
    let replacement: Arc<Mutex<Option<WorkflowMutationLease>>> = Arc::new(Mutex::new(None));
    let replacement_for_action = Arc::clone(&replacement);
    let takeover_for_action = Arc::clone(&takeover);
    let path_for_action = path.clone();
    let run_for_action = run.clone();

    let error = controller
        .mutate_projection(&run, |guard, prepared| {
            prepared.as_mut().unwrap().status = WorkflowRunStatus::Cancelled;
            let now = chrono::Utc::now().timestamp_millis();
            rusqlite::Connection::open(path_for_action)
                .unwrap()
                .execute(
                    "UPDATE runtime_ledger_workflow_mutation_leases
                     SET heartbeat_at_ms = ?1, expires_at_ms = ?2 WHERE run_id = ?3",
                    rusqlite::params![now - 100_000, now - 1, run_for_action.as_str()],
                )
                .unwrap();
            let lease = takeover_for_action
                .acquire_workflow_mutation_lease(&run_for_action, "replacement", now)
                .unwrap();
            *replacement_for_action.lock().unwrap() = Some(lease);
            controller.append_record(
                guard,
                &run_for_action,
                "workflow_cancelled",
                json!({"reason": "expired", "snapshot": prepared}),
            )?;
            Ok(())
        })
        .unwrap_err();

    assert!(error.contains("mutation lease was lost"), "{error}");
    assert_eq!(ledger.records_for_run(&run).unwrap(), initial_records);
    assert_eq!(controller.snapshot(&run).unwrap().status, WorkflowRunStatus::Running);
    takeover
        .release_workflow_mutation_lease(&replacement.lock().unwrap().take().unwrap())
        .unwrap();
}

#[test]
fn expired_ordinary_mutation_cannot_publish_after_a_valid_guarded_append() {
    use crate::runtime_ledger::{RuntimeLedger, SqliteRuntimeLedger};

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ledger.sqlite3");
    let definition = WorkflowDefinition {
        id: "ordinary-expired-after-append".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Expired ordinary mutation cannot publish".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("work", &[])],
        outputs: BTreeMap::new(),
    };
    let root = RunId::from("ordinary-expired-after-append-root");
    let run = RunId::from("ordinary-expired-after-append-root:workflow:one");
    let first_ledger: Arc<dyn RuntimeLedger> = Arc::new(SqliteRuntimeLedger::open(&path).unwrap());
    let first = WorkflowController::new(Arc::clone(&first_ledger));
    first.register(definition.clone()).unwrap();
    first.start(run.clone(), &definition.id, json!({})).unwrap();
    let second = WorkflowController::new(Arc::new(SqliteRuntimeLedger::open(&path).unwrap()));
    second.register(definition.clone()).unwrap();
    second.restore_from_ledger(&root).unwrap();
    let path_for_action = path.clone();
    let run_for_action = run.clone();

    let error = first
        .mutate_projection(&run, |guard, prepared| {
            let cancelled = prepared.as_mut().unwrap();
            cancelled.status = WorkflowRunStatus::Cancelled;
            first.append_record(
                guard,
                &run_for_action,
                "workflow_cancelled",
                json!({"reason": "first", "snapshot": cancelled}),
            )?;
            let now = chrono::Utc::now().timestamp_millis();
            rusqlite::Connection::open(path_for_action)
                .unwrap()
                .execute(
                    "UPDATE runtime_ledger_workflow_mutation_leases
                     SET heartbeat_at_ms = ?1, expires_at_ms = ?2 WHERE run_id = ?3",
                    rusqlite::params![now - 100_000, now - 1, run_for_action.as_str()],
                )
                .unwrap();
            second.restore_from_ledger(&root)?;
            second.cancel(&run_for_action, "replacement")?;
            Ok(())
        })
        .unwrap_err();

    assert!(error.contains("failed to validate Workflow mutation lease"), "{error}");
    assert_eq!(first.snapshot(&run).unwrap().status, WorkflowRunStatus::Running);
    assert_eq!(second.snapshot(&run).unwrap().status, WorkflowRunStatus::Cancelled);
    let verifier = WorkflowController::new(Arc::new(SqliteRuntimeLedger::open(&path).unwrap()));
    verifier.register(definition).unwrap();
    verifier.restore_from_ledger(&root).unwrap();
    assert_eq!(verifier.snapshot(&run).unwrap().status, WorkflowRunStatus::Cancelled);
}

#[test]
fn cancelled_ordinary_mutation_before_append_does_not_publish_prepared_projection() {
    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};

    let ledger: Arc<dyn RuntimeLedger> = Arc::new(InMemoryRuntimeLedger::default());
    let definition = WorkflowDefinition {
        id: "ordinary-cancelled-before-append".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Cancelled action cannot publish".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("work", &[])],
        outputs: BTreeMap::new(),
    };
    let run = RunId::from("ordinary-cancelled-before-append-root:workflow:one");
    let controller = WorkflowController::new(Arc::clone(&ledger));
    controller.register(definition).unwrap();
    controller
        .start(run.clone(), "ordinary-cancelled-before-append", json!({}))
        .unwrap();
    let records_before = ledger.records_for_run(&run).unwrap();

    let error = controller
        .mutate_projection(&run, |_guard, prepared| -> Result<(), String> {
            prepared.as_mut().unwrap().status = WorkflowRunStatus::Cancelled;
            Err("mutation cancelled before append".to_owned())
        })
        .unwrap_err();

    assert_eq!(error, "mutation cancelled before append");
    assert_eq!(controller.snapshot(&run).unwrap().status, WorkflowRunStatus::Running);
    assert_eq!(ledger.records_for_run(&run).unwrap(), records_before);
    assert_eq!(
        controller.cancel(&run, "later valid mutation").unwrap().status,
        WorkflowRunStatus::Cancelled
    );
}

#[test]
fn panicking_ordinary_mutation_after_append_does_not_publish_and_allows_expiry_takeover() {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use crate::runtime_ledger::{RuntimeLedger, SqliteRuntimeLedger};

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ledger.sqlite3");
    let definition = WorkflowDefinition {
        id: "ordinary-panic-after-append".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Panicking action cannot publish".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("work", &[])],
        outputs: BTreeMap::new(),
    };
    let root = RunId::from("ordinary-panic-after-append-root");
    let run = RunId::from("ordinary-panic-after-append-root:workflow:one");
    let ledger: Arc<dyn RuntimeLedger> = Arc::new(SqliteRuntimeLedger::open(&path).unwrap());
    let first = WorkflowController::new(Arc::clone(&ledger));
    first.register(definition.clone()).unwrap();
    first.start(run.clone(), &definition.id, json!({})).unwrap();

    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let _ = first.mutate_projection(&run, |guard, prepared| -> Result<(), String> {
            let cancelled = prepared.as_mut().unwrap();
            cancelled.status = WorkflowRunStatus::Cancelled;
            first.append_record(
                guard,
                &run,
                "workflow_cancelled",
                json!({"reason": "durable before panic", "snapshot": cancelled}),
            )?;
            panic!("injected panic after fenced append");
        });
    }));
    assert!(panicked.is_err());
    assert_eq!(first.snapshot(&run).unwrap().status, WorkflowRunStatus::Running);

    let now = chrono::Utc::now().timestamp_millis();
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute(
            "UPDATE runtime_ledger_workflow_mutation_leases
             SET heartbeat_at_ms = ?1, expires_at_ms = ?2 WHERE run_id = ?3",
            rusqlite::params![now - 100_000, now - 1, run.as_str()],
        )
        .unwrap();
    let takeover = WorkflowController::new(Arc::new(SqliteRuntimeLedger::open(&path).unwrap()));
    takeover.register(definition).unwrap();
    takeover.restore_from_ledger(&root).unwrap();
    assert_eq!(takeover.snapshot(&run).unwrap().status, WorkflowRunStatus::Cancelled);
}

#[test]
fn sqlite_stale_restored_controller_cannot_begin_a_second_attempt() {
    use crate::runtime_ledger::{RuntimeLedger, SqliteRuntimeLedger};

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ledger.sqlite3");
    let definition = WorkflowDefinition {
        id: "stale-controller-begin".into(),
        schema_version: 1,
        version: "1".into(),
        description: "A stale restored Controller cannot repeat begin_attempt".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("work", &[])],
        outputs: BTreeMap::new(),
    };
    let root = RunId::from("stale-controller-begin-root");
    let run = RunId::from("stale-controller-begin-root:workflow:one");
    let seed_ledger: Arc<dyn RuntimeLedger> = Arc::new(SqliteRuntimeLedger::open(&path).unwrap());
    let seed = WorkflowController::new(Arc::clone(&seed_ledger));
    seed.register(definition.clone()).unwrap();
    seed.start(run.clone(), &definition.id, json!({})).unwrap();

    let first = WorkflowController::new(Arc::new(SqliteRuntimeLedger::open(&path).unwrap()));
    let second = WorkflowController::new(Arc::new(SqliteRuntimeLedger::open(&path).unwrap()));
    first.register(definition.clone()).unwrap();
    second.register(definition.clone()).unwrap();
    assert_eq!(first.restore_from_ledger(&root).unwrap(), 1);
    assert_eq!(second.restore_from_ledger(&root).unwrap(), 1);

    first.begin_attempt(&run, &definition.nodes[0]).unwrap();
    let error = second.begin_attempt(&run, &definition.nodes[0]).unwrap_err();

    assert!(error.contains("projection high-water"), "{error}");
    assert_eq!(
        seed_ledger
            .records_for_run(&run)
            .unwrap()
            .iter()
            .filter(|record| record.record_type == "workflow_node_started")
            .count(),
        1
    );
}

#[test]
fn sqlite_stale_host_cannot_cancel_or_fail_after_another_host_completes() {
    use crate::runtime_ledger::{RuntimeLedger, SqliteRuntimeLedger};

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ledger.sqlite3");
    let definition = WorkflowDefinition {
        id: "stale-controller-terminal".into(),
        schema_version: 1,
        version: "1".into(),
        description: "A stale Host cannot replace another Host terminal state".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("work", &[])],
        outputs: BTreeMap::new(),
    };
    let root = RunId::from("stale-controller-terminal-root");
    let run = RunId::from("stale-controller-terminal-root:workflow:one");
    let seed_ledger: Arc<dyn RuntimeLedger> = Arc::new(SqliteRuntimeLedger::open(&path).unwrap());
    let seed = WorkflowController::new(Arc::clone(&seed_ledger));
    seed.register(definition.clone()).unwrap();
    seed.start(run.clone(), &definition.id, json!({})).unwrap();
    let attempt = seed.begin_attempt(&run, &definition.nodes[0]).unwrap().attempt_id;

    let old = WorkflowController::new(Arc::new(SqliteRuntimeLedger::open(&path).unwrap()));
    let current = WorkflowController::new(Arc::new(SqliteRuntimeLedger::open(&path).unwrap()));
    old.register(definition.clone()).unwrap();
    current.register(definition.clone()).unwrap();
    assert_eq!(old.restore_from_ledger(&root).unwrap(), 1);
    assert_eq!(current.restore_from_ledger(&root).unwrap(), 1);
    let current_attempt = current.begin_attempt(&run, &definition.nodes[0]).unwrap().attempt_id;
    assert_eq!(current_attempt, attempt);
    assert!(
        current
            .complete_node(&run, "work", &current_attempt, json!({"done": true}))
            .unwrap()
    );
    let records_after_completion = seed_ledger.records_for_run(&run).unwrap();

    let cancel_error = old.cancel(&run, "stale cancellation").unwrap_err();
    let fail_error = old
        .fail_node(
            &run,
            "work",
            &attempt,
            WorkflowNodeError::non_retryable("stale failure"),
        )
        .unwrap_err();

    assert!(cancel_error.contains("projection high-water"), "{cancel_error}");
    assert!(fail_error.contains("projection high-water"), "{fail_error}");
    assert_eq!(seed_ledger.records_for_run(&run).unwrap(), records_after_completion);
}

#[test]
fn projection_publication_rolls_back_when_its_durable_high_water_commit_is_stale() {
    use solaris_types::effect::DurabilityClass;

    use crate::runtime_ledger::{RuntimeLedger, SqliteRuntimeLedger};

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ledger.sqlite3");
    let definition = WorkflowDefinition {
        id: "projection-commit-rollback".into(),
        schema_version: 1,
        version: "1".into(),
        description: "A failed projection high-water commit restores the prior projection".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("work", &[])],
        outputs: BTreeMap::new(),
    };
    let root = RunId::from("projection-commit-rollback-root");
    let run = RunId::from("projection-commit-rollback-root:workflow:one");
    let ledger: Arc<dyn RuntimeLedger> = Arc::new(SqliteRuntimeLedger::open(&path).unwrap());
    let controller = WorkflowController::new(Arc::clone(&ledger));
    controller.register(definition.clone()).unwrap();
    controller.start(run.clone(), &definition.id, json!({})).unwrap();
    let concurrent = SqliteRuntimeLedger::open(&path).unwrap();

    let error = controller
        .mutate_projection(&run, |guard, prepared| {
            let cancelled = prepared.as_mut().unwrap();
            cancelled.status = WorkflowRunStatus::Cancelled;
            controller.append_record(
                guard,
                &run,
                "workflow_cancelled",
                json!({"reason": "durable before stale commit", "snapshot": cancelled}),
            )?;
            concurrent
                .append(
                    &run,
                    DurabilityClass::SyncCritical,
                    "test_projection_commit_race",
                    json!({}),
                )
                .map_err(|append_error| append_error.to_string())?;
            Ok(())
        })
        .unwrap_err();

    assert!(
        error.contains("projection high-water") && error.contains("became stale"),
        "{error}"
    );
    assert_eq!(controller.snapshot(&run).unwrap().status, WorkflowRunStatus::Running);

    let recovered = WorkflowController::new(Arc::new(SqliteRuntimeLedger::open(&path).unwrap()));
    recovered.register(definition).unwrap();
    assert_eq!(recovered.restore_from_ledger(&root).unwrap(), 1);
    assert_eq!(recovered.snapshot(&run).unwrap().status, WorkflowRunStatus::Cancelled);
}
