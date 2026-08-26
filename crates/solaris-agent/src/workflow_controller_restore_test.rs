#[tokio::test]
async fn sqlite_restart_restores_completed_workflow_and_task() {
    use crate::runtime_ledger::{RuntimeLedger, SqliteRuntimeLedger};

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("workflow-restart.sqlite3");
    let root = RunId::from("root-durable");
    let run = RunId::from("root-durable:workflow:done");
    let definition = WorkflowDefinition {
        id: "durable-workflow".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Test workflow".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("work", &[])],
        outputs: BTreeMap::from([("result".into(), "work".into())]),
    };

    {
        let ledger: Arc<dyn RuntimeLedger> = Arc::new(SqliteRuntimeLedger::open(&path).unwrap());
        let controller = WorkflowController::new(ledger);
        controller.register(definition.clone()).unwrap();
        controller
            .start(run.clone(), &definition.id, json!({"query":"persist"}))
            .unwrap();
        let settled = controller
            .execute_until_settled(&run, Arc::new(EchoExecutor))
            .await
            .unwrap();
        assert_eq!(settled.status, WorkflowRunStatus::Completed);
    }

    {
        let ledger: Arc<dyn RuntimeLedger> = Arc::new(SqliteRuntimeLedger::open(&path).unwrap());
        let controller = WorkflowController::new(ledger);
        controller.register(definition.clone()).unwrap();
        assert_eq!(controller.restore_from_ledger(&root).unwrap(), 1);
        let restored = controller.snapshot(&run).unwrap();
        assert_eq!(restored.status, WorkflowRunStatus::Completed);
        assert_eq!(restored.parameters["query"], "persist");
        assert!(restored.input_digest.is_some());
        assert_eq!(restored.nodes["work"].output.as_ref().unwrap()["node"], "work");
        assert_eq!(
            controller
                .task_registry()
                .get(&TaskId::from("workflow:root-durable:workflow:done:work"))
                .unwrap()
                .state,
            TaskState::Completed
        );
        assert_eq!(
            controller
                .start(run.clone(), &definition.id, json!({"query":"persist"}))
                .unwrap(),
            restored
        );
    }
}

#[test]
fn jsonl_workflow_mutation_fails_closed_without_atomic_fencing() {
    use crate::runtime_ledger::{JsonlRuntimeLedger, RuntimeLedger};

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("workflow.jsonl");
    let ledger = Arc::new(JsonlRuntimeLedger::open(&path).unwrap());
    let runtime_ledger: Arc<dyn RuntimeLedger> = ledger.clone();
    let controller = WorkflowController::new(runtime_ledger);
    controller
        .register(WorkflowDefinition {
            id: "jsonl-fail-closed".into(),
            schema_version: 1,
            version: "1".into(),
            description: "JSONL has no cross-process Workflow fence".into(),
            roles: Vec::new(),
            parameters_schema: None,
            nodes: vec![node("work", &[])],
            outputs: BTreeMap::new(),
        })
        .unwrap();

    let run = RunId::from("jsonl-fail-closed-root:workflow:one");
    let error = controller
        .start(run.clone(), "jsonl-fail-closed", json!({}))
        .unwrap_err();
    assert!(error.contains("cannot fence Workflow projection mutation"), "{error}");
    assert!(controller.snapshot(&run).is_none());
    assert!(ledger.records_for_run(&run).unwrap().is_empty());
}

#[test]
fn workflow_restore_preserves_task_cas_projection_restored_first() {
    use solaris_types::identity::{AgentId, OperationId};
    use solaris_types::runtime::{AgentLifecycleState, AgentRecord};

    use crate::collaboration_runtime::{CollaborationRuntime, TaskSettlement};
    use crate::resource_policy::ResourcePolicy;
    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};
    use crate::scheduler::Scheduler;

    let ledger: Arc<dyn RuntimeLedger> = Arc::new(InMemoryRuntimeLedger::default());
    let root = RunId::from("root-task-cas-order");
    let run = RunId::from("root-task-cas-order:workflow:one");
    let definition = WorkflowDefinition {
        id: "task-cas-order".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Preserve task CAS during restore".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("work", &[])],
        outputs: BTreeMap::new(),
    };
    let owner = AgentId::from("task-cas-owner");
    let next_owner = AgentId::from("task-cas-next-owner");
    let runtime = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(2)),
        Arc::clone(&ledger),
    ));
    for agent_id in [&owner, &next_owner] {
        runtime.agents().upsert(AgentRecord {
            run_id: run.clone(),
            agent_id: agent_id.clone(),
            team_id: None,
            parent_agent_id: None,
            state: AgentLifecycleState::Active,
        });
    }
    let controller = WorkflowController::with_runtime_and_roles(Arc::clone(&runtime), None);
    controller.register(definition.clone()).unwrap();
    controller.start(run.clone(), &definition.id, json!({})).unwrap();
    let task_id = TaskId::from("workflow:root-task-cas-order:workflow:one:work");
    runtime
        .assign_task_owner(&run, &task_id, &owner, 0, &OperationId::from("restore-assign"))
        .unwrap();
    runtime
        .handoff_task(
            &run,
            &task_id,
            &owner,
            next_owner.clone(),
            1,
            &OperationId::from("restore-handoff"),
        )
        .unwrap();
    runtime
        .settle_collaboration_task(
            &run,
            &task_id,
            &next_owner,
            2,
            &OperationId::from("restore-settle"),
            TaskSettlement {
                state: TaskState::Failed,
                outcome_ref: Some("mesh://restore/outcome".to_owned()),
                failure_class: Some(solaris_types::runtime::TaskFailureClass::OutcomeUnknown),
            },
        )
        .unwrap();
    let expected = runtime.tasks().get(&task_id).unwrap();

    let restored_runtime = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(2)),
        Arc::clone(&ledger),
    ));
    restored_runtime.restore_projection(&run).unwrap();
    assert_eq!(restored_runtime.tasks().get(&task_id), Some(expected.clone()));
    let restored = WorkflowController::with_runtime_and_roles(Arc::clone(&restored_runtime), None);
    restored.register(definition).unwrap();
    assert_eq!(restored.restore_from_ledger(&root).unwrap(), 1);

    assert_eq!(restored_runtime.tasks().get(&task_id), Some(expected));
}

#[test]
fn workflow_restore_rejects_incompatible_existing_task_metadata_without_overwrite() {
    use crate::collaboration_runtime::CollaborationRuntime;
    use crate::resource_policy::ResourcePolicy;
    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};
    use crate::scheduler::Scheduler;

    let ledger: Arc<dyn RuntimeLedger> = Arc::new(InMemoryRuntimeLedger::default());
    let root = RunId::from("root-task-metadata-conflict");
    let run = RunId::from("root-task-metadata-conflict:workflow:one");
    let definition = WorkflowDefinition {
        id: "task-metadata-conflict".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Reject incompatible restored task metadata".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("work", &[])],
        outputs: BTreeMap::new(),
    };
    let seed = WorkflowController::new(Arc::clone(&ledger));
    seed.register(definition.clone()).unwrap();
    seed.start(run.clone(), &definition.id, json!({})).unwrap();

    let runtime = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(1)),
        ledger,
    ));
    let task_id = TaskId::from("workflow:root-task-metadata-conflict:workflow:one:work");
    let incompatible = solaris_types::runtime::TaskRecord {
        run_id: run,
        task_id: task_id.clone(),
        revision: 7,
        task_key: Some("tampered-task-key".into()),
        team_id: None,
        workflow_id: Some(definition.id.clone()),
        node_id: Some("work".into()),
        role: None,
        depends_on: Vec::new(),
        content: None,
        expected_write_scope: Vec::new(),
        owner_agent_id: None,
        state: TaskState::Running,
        outcome_ref: Some("mesh://existing/outcome".into()),
        failure_class: None,
    };
    runtime.tasks().upsert(incompatible.clone());
    let restored = WorkflowController::with_runtime_and_roles(Arc::clone(&runtime), None);
    restored.register(definition).unwrap();

    let error = restored.restore_from_ledger(&root).unwrap_err();

    assert!(error.contains("incompatible immutable metadata"));
    assert_eq!(runtime.tasks().get(&task_id), Some(incompatible));
}

#[test]
fn ledger_restore_backfills_legacy_input_digest_and_enforces_reuse() {
    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};
    use solaris_types::effect::DurabilityClass;

    let root = RunId::from("root-legacy-input");
    let run = RunId::from("root-legacy-input:workflow:request");
    let parameters = json!({"query":"legacy","options":{"z":2,"a":1}});
    let definition = WorkflowDefinition {
        id: "legacy-input-digest".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Test workflow".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("work", &[])],
        outputs: BTreeMap::new(),
    };
    let seed = WorkflowController::default();
    seed.register(definition.clone()).unwrap();
    let snapshot = seed.start(run.clone(), &definition.id, parameters.clone()).unwrap();
    let mut serialized = serde_json::to_value(snapshot).unwrap();
    serialized.as_object_mut().unwrap().remove("input_digest");

    let ledger: Arc<dyn RuntimeLedger> = Arc::new(InMemoryRuntimeLedger::default());
    ledger
        .append(
            &run,
            DurabilityClass::SyncCritical,
            "workflow_started",
            json!({
                "workflow_id": definition.id,
                "workflow_version": definition.version,
                "snapshot": serialized,
            }),
        )
        .unwrap();

    let controller = WorkflowController::new(ledger);
    controller.register(definition.clone()).unwrap();
    assert_eq!(controller.restore_from_ledger(&root).unwrap(), 1);
    let restored = controller.snapshot(&run).unwrap();

    assert_eq!(restored.input_digest, Some(workflow_input_digest(&parameters)));
    assert_eq!(restored.parameters, parameters);
    assert_eq!(
        controller
            .start(
                run.clone(),
                &definition.id,
                json!({"options":{"a":1,"z":2},"query":"legacy"}),
            )
            .unwrap(),
        restored
    );

    let error = controller
        .start(run, &definition.id, json!({"query":"changed"}))
        .unwrap_err();
    assert!(error.contains("different input"), "unexpected error: {error}");
}

#[test]
fn ledger_restore_rejects_legacy_snapshot_without_definition_digest() {
    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};
    use solaris_types::effect::DurabilityClass;

    let root = RunId::from("root-legacy-definition");
    let run = RunId::from("root-legacy-definition:workflow:request");
    let definition = WorkflowDefinition {
        id: "legacy-definition-digest".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Test workflow".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("work", &[])],
        outputs: BTreeMap::new(),
    };
    let seed = WorkflowController::default();
    seed.register(definition.clone()).unwrap();
    let snapshot = seed.start(run.clone(), &definition.id, json!({})).unwrap();
    let mut serialized = serde_json::to_value(snapshot).unwrap();
    serialized.as_object_mut().unwrap().remove("workflow_definition_digest");

    let ledger: Arc<dyn RuntimeLedger> = Arc::new(InMemoryRuntimeLedger::default());
    ledger
        .append(
            &run,
            DurabilityClass::SyncCritical,
            "workflow_started",
            json!({
                "workflow_id": definition.id,
                "workflow_version": definition.version,
                "snapshot": serialized,
            }),
        )
        .unwrap();

    let controller = WorkflowController::new(ledger);
    controller.register(definition).unwrap();
    assert_eq!(controller.restore_from_ledger(&root).unwrap(), 1);
    let restored = controller.snapshot(&run).unwrap();
    assert_eq!(restored.status, WorkflowRunStatus::Failed);
    assert!(
        restored
            .reconciliation_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("definition digest"))
    );
}

#[test]
fn ledger_restore_safely_rejects_runtime_binding_for_legacy_snapshot() {
    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};
    use solaris_types::effect::DurabilityClass;

    let root = RunId::from("root-legacy-runtime");
    let run = RunId::from("root-legacy-runtime:workflow:request");
    let definition = WorkflowDefinition {
        id: "legacy-runtime-identity".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Test workflow".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("work", &[])],
        outputs: BTreeMap::new(),
    };
    let seed = WorkflowController::default();
    seed.register(definition.clone()).unwrap();
    let snapshot = seed
        .start(run.clone(), &definition.id, json!({"prompt":"same"}))
        .unwrap();

    let ledger: Arc<dyn RuntimeLedger> = Arc::new(InMemoryRuntimeLedger::default());
    ledger
        .append(
            &run,
            DurabilityClass::SyncCritical,
            "workflow_started",
            json!({
                "workflow_id": definition.id,
                "workflow_version": definition.version,
                "snapshot": snapshot,
            }),
        )
        .unwrap();

    let controller = WorkflowController::new(ledger);
    controller.register(definition.clone()).unwrap();
    assert_eq!(controller.restore_from_ledger(&root).unwrap(), 1);
    let error = controller
        .start_with_runtime(
            run,
            &definition.id,
            json!({"prompt":"same"}),
            WorkflowRuntimeIdentity::new("provider-a", "model-a"),
        )
        .unwrap_err();
    assert!(error.contains("provider or model"), "unexpected error: {error}");
}

#[tokio::test]
async fn restore_keeps_completed_node_after_skipped_dependency() {
    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};

    let ledger: Arc<dyn RuntimeLedger> = Arc::new(InMemoryRuntimeLedger::default());
    let mut skipped = node("skipped", &[]);
    skipped.when = Some(json!({"always": false}));
    let definition = WorkflowDefinition {
        id: "skipped-dependency".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Test workflow".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![skipped, node("downstream", &["skipped"])],
        outputs: BTreeMap::new(),
    };
    let root = RunId::from("root-skipped-dependency");
    let run = RunId::from("root-skipped-dependency:workflow:request");
    let first = WorkflowController::new(Arc::clone(&ledger));
    first.register(definition.clone()).unwrap();
    first.start(run.clone(), &definition.id, json!({})).unwrap();
    let settled = first.execute_until_settled(&run, Arc::new(EchoExecutor)).await.unwrap();
    assert_eq!(settled.nodes["skipped"].status, WorkflowNodeStatus::Skipped);
    assert_eq!(settled.nodes["downstream"].status, WorkflowNodeStatus::Completed);

    let restored = WorkflowController::new(ledger);
    restored.register(definition).unwrap();
    assert_eq!(restored.restore_from_ledger(&root).unwrap(), 1);
    let snapshot = restored.snapshot(&run).unwrap();
    assert_eq!(snapshot.status, WorkflowRunStatus::Completed);
    assert_eq!(snapshot.nodes["skipped"].status, WorkflowNodeStatus::Skipped);
    assert_eq!(snapshot.nodes["downstream"].status, WorkflowNodeStatus::Completed);
}

#[tokio::test]
async fn restore_propagates_invalid_upstream_checkpoint_in_reverse_definition_order() {
    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};
    use solaris_types::effect::DurabilityClass;

    let ledger: Arc<dyn RuntimeLedger> = Arc::new(InMemoryRuntimeLedger::default());
    let mut skipped_descendant = node("skipped-descendant", &["upstream"]);
    skipped_descendant.when = Some(json!({"always": false}));
    let definition = WorkflowDefinition {
        id: "reverse-checkpoint-order".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Test workflow".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![
            skipped_descendant,
            node("downstream", &["upstream"]),
            node("upstream", &[]),
        ],
        outputs: BTreeMap::new(),
    };
    let root = RunId::from("root-reverse-checkpoint");
    let run = RunId::from("root-reverse-checkpoint:workflow:request");
    let first = WorkflowController::new(Arc::clone(&ledger));
    first.register(definition.clone()).unwrap();
    first.start(run.clone(), &definition.id, json!({})).unwrap();
    let settled = first.execute_until_settled(&run, Arc::new(EchoExecutor)).await.unwrap();
    assert_eq!(settled.nodes["skipped-descendant"].status, WorkflowNodeStatus::Skipped);
    let upstream = &settled.nodes["upstream"];
    let output = serde_json::to_string(upstream.output.as_ref().unwrap()).unwrap();
    ledger
        .append(
            &run,
            DurabilityClass::SyncCritical,
            "workflow_node_started",
            json!({
                "node_id": "upstream",
                "attempt_id": upstream.attempt_id,
                "input_digest": "invalid-upstream-input",
            }),
        )
        .unwrap();
    ledger
        .append(
            &run,
            DurabilityClass::SyncCritical,
            "workflow_node_completed",
            json!({
                "node_id": "upstream",
                "attempt_id": upstream.attempt_id,
                "output_ref": upstream.output_ref,
                "output_digest": stable_digest_bytes(output.as_bytes()),
                "output_bytes": output.len(),
                "committed_at_unix_ms": upstream.committed_at_unix_ms,
            }),
        )
        .unwrap();
    ledger
        .append(
            &run,
            DurabilityClass::SyncCritical,
            "workflow_settled",
            json!({"status": "completed"}),
        )
        .unwrap();

    let restored = WorkflowController::new(ledger);
    restored.register(definition).unwrap();
    assert_eq!(restored.restore_from_ledger(&root).unwrap(), 1);
    let snapshot = restored.snapshot(&run).unwrap();
    assert_eq!(snapshot.status, WorkflowRunStatus::Running);
    assert_eq!(snapshot.nodes["upstream"].status, WorkflowNodeStatus::Pending);
    assert_eq!(snapshot.nodes["downstream"].status, WorkflowNodeStatus::Pending);
    assert_eq!(snapshot.nodes["skipped-descendant"].status, WorkflowNodeStatus::Pending);
}

#[tokio::test]
async fn sqlite_restart_reuses_running_attempt_identity_and_settles() {
    use crate::runtime_ledger::{RuntimeLedger, SqliteRuntimeLedger};

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("workflow-attempt.sqlite3");
    let root = RunId::from("root-attempt");
    let run = RunId::from("root-attempt:workflow:running");
    let definition = WorkflowDefinition {
        id: "attempt-workflow".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Test workflow".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("work", &[])],
        outputs: BTreeMap::new(),
    };
    let original_attempt;
    let original_number;

    {
        let ledger: Arc<dyn RuntimeLedger> = Arc::new(SqliteRuntimeLedger::open(&path).unwrap());
        let controller = WorkflowController::new(ledger);
        controller.register(definition.clone()).unwrap();
        controller.start(run.clone(), &definition.id, json!({})).unwrap();
        let context = controller.begin_attempt(&run, &definition.nodes[0]).unwrap();
        original_attempt = context.attempt_id;
        original_number = controller.snapshot(&run).unwrap().nodes["work"].attempt_number;
    }

    {
        let ledger: Arc<dyn RuntimeLedger> = Arc::new(SqliteRuntimeLedger::open(&path).unwrap());
        let controller = WorkflowController::new(ledger);
        controller.register(definition.clone()).unwrap();
        assert_eq!(controller.restore_from_ledger(&root).unwrap(), 1);
        let recovered = controller.snapshot(&run).unwrap();
        assert_eq!(recovered.nodes["work"].status, WorkflowNodeStatus::Pending);
        assert!(recovered.nodes["work"].resume_existing_attempt);
        assert_eq!(recovered.nodes["work"].attempt_id, original_attempt);
        assert_eq!(recovered.nodes["work"].attempt_number, original_number);

        let settled = controller
            .execute_until_settled(&run, Arc::new(EchoExecutor))
            .await
            .unwrap();
        assert_eq!(settled.status, WorkflowRunStatus::Completed);
        assert_eq!(settled.nodes["work"].attempt_id, original_attempt);
        assert_eq!(settled.nodes["work"].attempt_number, original_number);
    }
}

#[tokio::test]
async fn restart_rejects_checkpoint_with_tampered_input_digest() {
    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};
    use solaris_types::effect::DurabilityClass;

    let ledger: Arc<dyn RuntimeLedger> = Arc::new(InMemoryRuntimeLedger::default());
    let definition = WorkflowDefinition {
        id: "digest-workflow".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Test workflow".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("work", &[])],
        outputs: BTreeMap::new(),
    };
    let root = RunId::from("root-digest");
    let run = RunId::from("root-digest:workflow:tampered");
    let controller = WorkflowController::new(Arc::clone(&ledger));
    controller.register(definition.clone()).unwrap();
    controller
        .start(run.clone(), &definition.id, json!({"query":"safe"}))
        .unwrap();
    controller
        .execute_until_settled(&run, Arc::new(EchoExecutor))
        .await
        .unwrap();
    ledger
        .append(
            &run,
            DurabilityClass::SyncCritical,
            "workflow_node_started",
            json!({"node_id":"work", "attempt_id":"tampered", "input_digest":"wrong"}),
        )
        .unwrap();
    let serialized_output = serde_json::to_string(&json!({"node":"work"})).unwrap();
    let output_ref = EffectOutputStore::for_legacy_run(&run)
        .write_legacy_fixture(&serialized_output)
        .unwrap();
    ledger
        .append(
            &run,
            DurabilityClass::SyncCritical,
            "workflow_node_completed",
            json!({
                "node_id":"work",
                "attempt_id":"tampered",
                "output_ref":output_ref,
                "output_digest":stable_digest_bytes(serialized_output.as_bytes()),
                "output_bytes":serialized_output.len(),
                "committed_at_unix_ms":1
            }),
        )
        .unwrap();
    ledger
        .append(
            &run,
            DurabilityClass::SyncCritical,
            "workflow_settled",
            json!({"status":"completed"}),
        )
        .unwrap();

    let restored = WorkflowController::new(Arc::clone(&ledger));
    restored.register(definition).unwrap();
    assert_eq!(restored.restore_from_ledger(&root).unwrap(), 1);
    let snapshot = restored.snapshot(&run).unwrap();
    assert_eq!(snapshot.status, WorkflowRunStatus::Running);
    assert_eq!(snapshot.nodes["work"].status, WorkflowNodeStatus::Pending);
    assert!(snapshot.nodes["work"].output.is_none());
}

struct WorkflowIdentityExecutor {
    identity: std::sync::Mutex<Option<solaris_types::plugin::ImplementationIdentity>>,
}

#[async_trait]
impl WorkflowNodeExecutor for WorkflowIdentityExecutor {
    async fn execute(&self, context: WorkflowExecutionContext) -> Result<Value, WorkflowNodeError> {
        *self.identity.lock().unwrap() = Some(context.workflow);
        Ok(json!({"ok": true}))
    }
}

#[tokio::test]
async fn workflow_execution_context_pins_definition_digest() {
    let controller = WorkflowController::default();
    let definition = WorkflowDefinition {
        id: "identity-workflow".into(),
        schema_version: 1,
        version: "7".into(),
        description: "Test workflow".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("work", &[])],
        outputs: BTreeMap::new(),
    };
    controller.register(definition).unwrap();
    let run = RunId::from("identity-run");
    let started = controller.start(run.clone(), "identity-workflow", json!({})).unwrap();
    let executor = Arc::new(WorkflowIdentityExecutor {
        identity: std::sync::Mutex::new(None),
    });
    controller.execute_until_settled(&run, executor.clone()).await.unwrap();
    let identity = executor.identity.lock().unwrap().clone().unwrap();
    assert_eq!(identity.implementation_id, "workflow:identity-workflow");
    assert_eq!(identity.version.as_deref(), Some("7"));
    assert_eq!(identity.digest, started.workflow_definition_digest);
}

#[test]
fn restore_rejects_changed_definition_with_same_id_and_version() {
    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};

    let ledger: Arc<dyn RuntimeLedger> = Arc::new(InMemoryRuntimeLedger::default());
    let original = WorkflowDefinition {
        id: "pinned".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Test workflow".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("original", &[])],
        outputs: BTreeMap::new(),
    };
    let first = WorkflowController::new(Arc::clone(&ledger));
    first.register(original).unwrap();
    first
        .start(RunId::from("root-pinned:workflow:one"), "pinned", json!({}))
        .unwrap();

    let changed = WorkflowDefinition {
        id: "pinned".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Test workflow".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("changed", &[])],
        outputs: BTreeMap::new(),
    };
    let restored = WorkflowController::new(Arc::clone(&ledger));
    restored.register(changed).unwrap();
    assert_eq!(restored.restore_from_ledger(&RunId::from("root-pinned")).unwrap(), 1);
    let snapshot = restored
        .snapshot(&RunId::from("root-pinned:workflow:one"))
        .expect("incompatible workflow remains visible for reconciliation");
    assert_eq!(snapshot.status, WorkflowRunStatus::Failed);
    assert!(
        snapshot
            .reconciliation_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("definition digest"))
    );
    assert!(
        ledger
            .records_for_run(&RunId::from("root-pinned:workflow:one"))
            .unwrap()
            .iter()
            .any(|record| record.record_type == "workflow_reconciliation_required")
    );
}

struct GateExecutor {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl WorkflowNodeExecutor for GateExecutor {
    async fn execute(&self, _context: WorkflowExecutionContext) -> Result<Value, WorkflowNodeError> {
        self.started.notify_one();
        self.release.notified().await;
        Ok(json!({"late": true}))
    }
}

#[tokio::test]
async fn late_node_completion_cannot_change_cancelled_terminal_state() {
    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};

    let ledger: Arc<dyn RuntimeLedger> = Arc::new(InMemoryRuntimeLedger::default());
    let controller = Arc::new(WorkflowController::new(Arc::clone(&ledger)));
    controller
        .register(WorkflowDefinition {
            id: "cancel-late".into(),
            schema_version: 1,
            version: "1".into(),
            description: "Test workflow".into(),
            roles: Vec::new(),
            parameters_schema: None,
            nodes: vec![node("work", &[])],
            outputs: BTreeMap::new(),
        })
        .unwrap();
    let run = RunId::from("cancel-late-run");
    controller.start(run.clone(), "cancel-late", json!({})).unwrap();
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let task = {
        let controller = Arc::clone(&controller);
        let run = run.clone();
        let executor = Arc::new(GateExecutor {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
        });
        tokio::spawn(async move { controller.execute_until_settled(&run, executor).await })
    };
    started.notified().await;
    controller.cancel(&run, "test cancellation").unwrap();
    release.notify_waiters();
    let settled = task.await.unwrap().unwrap();
    assert_eq!(settled.status, WorkflowRunStatus::Cancelled);
    assert_eq!(settled.nodes["work"].status, WorkflowNodeStatus::Cancelled);
    let records = ledger.records_for_run(&run).unwrap();
    let cancelled_sequence = records
        .iter()
        .find(|record| record.record_type == "workflow_cancelled")
        .unwrap()
        .seq;
    assert!(
        !records
            .iter()
            .any(|record| { record.record_type == "workflow_node_completed" && record.seq > cancelled_sequence })
    );
}

#[tokio::test]
async fn parent_timeout_cancels_durable_child_workflow() {
    let controller = WorkflowController::default();
    controller
        .register(WorkflowDefinition {
            id: "slow-child".into(),
            schema_version: 1,
            version: "1".into(),
            description: "Test workflow".into(),
            roles: Vec::new(),
            parameters_schema: None,
            nodes: vec![node("wait", &[])],
            outputs: BTreeMap::new(),
        })
        .unwrap();
    let mut child = node("child", &[]);
    child.workflow_ref = Some("slow-child".into());
    child.timeout_ms = Some(5);
    controller
        .register(WorkflowDefinition {
            id: "timed-parent".into(),
            schema_version: 1,
            version: "1".into(),
            description: "Test workflow".into(),
            roles: Vec::new(),
            parameters_schema: None,
            nodes: vec![child],
            outputs: BTreeMap::new(),
        })
        .unwrap();
    let run = RunId::from("timed-parent-run");
    controller.start(run.clone(), "timed-parent", json!({})).unwrap();
    let settled = controller
        .execute_until_settled(&run, Arc::new(SlowExecutor))
        .await
        .unwrap();
    assert_eq!(settled.status, WorkflowRunStatus::Failed);
    let child = controller
        .snapshots()
        .into_iter()
        .find(|snapshot| snapshot.workflow_id == "slow-child")
        .unwrap();
    assert_eq!(child.parent_run_id.as_ref(), Some(&run));
    assert_eq!(child.status, WorkflowRunStatus::Cancelled);
}

#[tokio::test]
async fn workflow_output_is_protected_from_journal_and_snapshot_streams() {
    use crate::runtime_ledger::{RuntimeLedger, SqliteRuntimeLedger};

    struct SecretExecutor;
    #[async_trait]
    impl WorkflowNodeExecutor for SecretExecutor {
        async fn execute(&self, _context: WorkflowExecutionContext) -> Result<Value, WorkflowNodeError> {
            Ok(json!({"secret":"workflow-secret-9812"}))
        }
    }

    let workspace = tempfile::tempdir().unwrap();
    let runtime_root = workspace.path().join(".solaris").join("runtime");
    let ledger: Arc<dyn RuntimeLedger> =
        Arc::new(SqliteRuntimeLedger::open(runtime_root.join("ledger.sqlite3")).unwrap());
    let controller = WorkflowController::new(Arc::clone(&ledger));
    controller
        .register(WorkflowDefinition {
            id: "protected-workflow".into(),
            schema_version: 1,
            version: "1".into(),
            description: "Protected output".into(),
            roles: Vec::new(),
            parameters_schema: None,
            nodes: vec![node("work", &[])],
            outputs: BTreeMap::new(),
        })
        .unwrap();
    let run = RunId::from("protected-root:workflow:one");
    controller.start(run.clone(), "protected-workflow", json!({})).unwrap();
    let snapshot = controller
        .execute_until_settled(&run, Arc::new(SecretExecutor))
        .await
        .unwrap();
    assert_eq!(
        snapshot.nodes["work"].output.as_ref().unwrap()["secret"],
        "workflow-secret-9812"
    );
    let journal = serde_json::to_string(&ledger.records_for_run(&run).unwrap()).unwrap();
    assert!(!journal.contains("workflow-secret-9812"));
    assert!(
        !serde_json::to_string(&snapshot)
            .unwrap()
            .contains("workflow-secret-9812")
    );
    let output_ref = snapshot.nodes["work"].output_ref.as_deref().unwrap();
    let run_directory = runtime_root
        .join("effect-outcomes")
        .join(stable_digest_bytes(run.as_str().as_bytes()));
    assert!(
        std::fs::read_to_string(run_directory.join(output_ref))
            .unwrap()
            .contains("workflow-secret-9812")
    );
    assert_eq!(std::fs::read_dir(run_directory).unwrap().count(), 1);

    let restored = WorkflowController::new(Arc::clone(&ledger));
    restored
        .register(controller.definition("protected-workflow").unwrap())
        .unwrap();
    assert_eq!(restored.restore_from_ledger(&RunId::from("protected-root")).unwrap(), 1);
    assert_eq!(
        restored.snapshot(&run).unwrap().nodes["work"].output.as_ref().unwrap()["secret"],
        "workflow-secret-9812"
    );
}

#[test]
fn legacy_deferred_boolean_restores_typed_attempt_state_and_blocks_old_settlement() {
    use solaris_types::effect::DurabilityClass;
    use solaris_types::runtime::TaskFailureClass;

    use crate::collaboration_runtime::CollaborationRuntime;
    use crate::resource_policy::ResourcePolicy;
    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};
    use crate::scheduler::Scheduler;

    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let runtime_ledger: Arc<dyn RuntimeLedger> = ledger.clone();
    let root = RunId::from("legacy-deferred-root");
    let run = RunId::from("legacy-deferred-root:workflow:one");
    let definition = WorkflowDefinition {
        id: "legacy-deferred".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Legacy deferred terminal migration".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("work", &[])],
        outputs: BTreeMap::new(),
    };
    let initial = WorkflowController::new(Arc::clone(&runtime_ledger));
    initial.register(definition.clone()).unwrap();
    initial.start(run.clone(), &definition.id, json!({})).unwrap();
    let attempt_id = "legacy-deferred-attempt";
    ledger
        .append(
            &run,
            DurabilityClass::SyncCritical,
            "workflow_node_started",
            json!({"node_id": "work", "attempt_id": attempt_id, "input_digest": "legacy-input"}),
        )
        .unwrap();
    ledger
        .append(
            &run,
            DurabilityClass::SyncCritical,
            "workflow_node_failed",
            json!({
                "node_id": "work",
                "error": "legacy uncertain outcome",
                "failure_class": TaskFailureClass::ReconciliationRequired,
                "state": WorkflowNodeStatus::Failed,
                "task_terminal_write_deferred": true,
            }),
        )
        .unwrap();
    ledger
        .append(
            &run,
            DurabilityClass::SyncCritical,
            "workflow_settled",
            json!({"status": WorkflowRunStatus::Failed}),
        )
        .unwrap();

    let runtime = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(1)),
        runtime_ledger,
    ));
    let restored = WorkflowController::with_runtime(Arc::clone(&runtime));
    restored.register(definition).unwrap();
    assert_eq!(restored.restore_from_ledger(&root).unwrap(), 1);
    let snapshot = restored.snapshot(&run).unwrap();
    assert_eq!(snapshot.status, WorkflowRunStatus::Running);
    let deferred = snapshot.nodes["work"].deferred_task_terminal_write.as_ref().unwrap();
    assert_eq!(deferred.failure_class, TaskFailureClass::ReconciliationRequired);
    assert_eq!(deferred.state, TaskState::Failed);
    assert_eq!(
        runtime
            .tasks()
            .get(&TaskId::from("workflow:legacy-deferred-root:workflow:one:work"))
            .unwrap()
            .state,
        TaskState::Queued
    );
}
