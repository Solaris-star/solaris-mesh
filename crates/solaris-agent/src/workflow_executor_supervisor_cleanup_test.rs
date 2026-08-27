use super::*;

struct FailSupervisorCleanupLedger {
    inner: InMemoryRuntimeLedger,
    task_armed: AtomicBool,
    close_armed: AtomicBool,
}

impl Default for FailSupervisorCleanupLedger {
    fn default() -> Self {
        Self {
            inner: InMemoryRuntimeLedger::default(),
            task_armed: AtomicBool::new(true),
            close_armed: AtomicBool::new(true),
        }
    }
}

impl RuntimeLedger for FailSupervisorCleanupLedger {
    crate::runtime_ledger::forward_compare_and_append!();

    fn append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        payload: Value,
    ) -> std::io::Result<LedgerRecord> {
        if record_type == "task_cas"
            && payload.get("transition").and_then(Value::as_str) == Some("workflow_state")
            && self.task_armed.swap(false, Ordering::SeqCst)
        {
            return Err(std::io::Error::other("injected Supervisor cleanup failure"));
        }
        if record_type == "agent_conversation_closed" && self.close_armed.swap(false, Ordering::SeqCst) {
            return Err(std::io::Error::other(
                "injected Supervisor close failure after task cleanup failure",
            ));
        }
        self.inner.append(run_id, durability, record_type, payload)
    }

    fn run_ids(&self) -> std::io::Result<Vec<RunId>> {
        self.inner.run_ids()
    }

    fn records_for_run(&self, run_id: &RunId) -> std::io::Result<Vec<LedgerRecord>> {
        self.inner.records_for_run(run_id)
    }

    fn effect_output_root(&self) -> Option<std::path::PathBuf> {
        self.inner.effect_output_root()
    }
}

struct FailSupervisorCloseLedger {
    inner: InMemoryRuntimeLedger,
    armed: AtomicBool,
}

impl Default for FailSupervisorCloseLedger {
    fn default() -> Self {
        Self {
            inner: InMemoryRuntimeLedger::default(),
            armed: AtomicBool::new(true),
        }
    }
}

impl RuntimeLedger for FailSupervisorCloseLedger {
    crate::runtime_ledger::forward_compare_and_append!();

    fn append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        payload: Value,
    ) -> std::io::Result<LedgerRecord> {
        if record_type == "agent_conversation_closed" && self.armed.swap(false, Ordering::SeqCst) {
            return Err(std::io::Error::other("injected Supervisor close failure"));
        }
        self.inner.append(run_id, durability, record_type, payload)
    }

    fn run_ids(&self) -> std::io::Result<Vec<RunId>> {
        self.inner.run_ids()
    }

    fn records_for_run(&self, run_id: &RunId) -> std::io::Result<Vec<LedgerRecord>> {
        self.inner.records_for_run(run_id)
    }

    fn effect_output_root(&self) -> Option<std::path::PathBuf> {
        self.inner.effect_output_root()
    }
}

fn assert_supervisor_team_agents_are_terminal(runtime: &CollaborationRuntime<()>) {
    for agent in runtime
        .agents()
        .snapshot()
        .into_iter()
        .filter(|agent| agent.team_id.is_some())
    {
        let durable_kinds: Vec<_> = runtime
            .ledger()
            .run_ids()
            .unwrap()
            .into_iter()
            .flat_map(|run_id| runtime.ledger().records_for_run(&run_id).unwrap())
            .filter(|record| {
                serde_json::to_string(&record.payload)
                    .unwrap()
                    .contains(agent.agent_id.as_str())
            })
            .map(|record| record.record_type)
            .collect();
        assert!(
            matches!(
                agent.state,
                AgentLifecycleState::Completed | AgentLifecycleState::Failed | AgentLifecycleState::Cancelled
            ),
            "Supervisor agent {} remained non-terminal: {:?}; durable kinds: {:?}",
            agent.agent_id,
            agent.state,
            durable_kinds
        );
    }
}

#[tokio::test]
async fn wrapper_schema_error_cancels_created_task_and_closes_coordinator() {
    let collaboration = CollaborationRuntimeConfig {
        strategy: CollaborationStrategy::Supervisor,
        worker_roles: vec![WorkerRolePolicy {
            role: "worker-a".into(),
            max_concurrent: 1,
            max_total: 1,
        }],
        max_tasks: 1,
        max_coordinator_rounds: 2,
        ..CollaborationRuntimeConfig::default()
    };
    let scripted = HashMap::from([(
        "coordinator".to_owned(),
        VecDeque::from([json!({
            "decision":"dispatch",
            "tasks":[{"task_key":"invalid-wrapper", "role":"worker-a", "instruction":"inspect"}]
        })
        .to_string()]),
    )]);
    let mut worker = v2_role("worker-a", &["Read"], PermissionCeiling::plan());
    worker.input_schema = Some(json!({
        "type":"object",
        "required":["field-that-wrapper-never-provides"]
    }));
    let (snapshot, runtime, _, state) = run_supervisor_workflow(
        "supervisor-wrapper-cleanup",
        collaboration,
        vec![v2_role("coordinator", &["Read"], PermissionCeiling::plan()), worker],
        scripted,
        2,
        solaris_types::workflow::MultiAgentPolicy::OnDemand,
    )
    .await;

    assert_eq!(snapshot.status, WorkflowRunStatus::Failed);
    assert_eq!(
        state.lock().unwrap_or_else(|error| error.into_inner()).calls,
        ["coordinator"]
    );
    let tasks: Vec<_> = runtime
        .tasks()
        .snapshot()
        .into_iter()
        .filter(|task| task.team_id.is_some())
        .collect();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].state, TaskState::Cancelled);
    assert_supervisor_team_agents_are_terminal(&runtime);
}

#[tokio::test]
async fn cleanup_failure_upgrades_to_reconciliation_and_preserves_both_causes() {
    let collaboration = CollaborationRuntimeConfig {
        strategy: CollaborationStrategy::Supervisor,
        worker_roles: vec![WorkerRolePolicy {
            role: "worker-a".into(),
            max_concurrent: 2,
            max_total: 2,
        }],
        max_tasks: 2,
        max_coordinator_rounds: 2,
        ..CollaborationRuntimeConfig::default()
    };
    let scripted = HashMap::from([(
        "coordinator".to_owned(),
        VecDeque::from([json!({
            "decision":"dispatch",
            "tasks":[
                {"task_key":"a-cleanup-fails", "role":"worker-a", "instruction":"inspect"},
                {"task_key":"b-cleanup-continues", "role":"worker-a", "instruction":"inspect"}
            ]
        })
        .to_string()]),
    )]);
    let mut worker = v2_role("worker-a", &["Read"], PermissionCeiling::plan());
    worker.input_schema = Some(json!({
        "type":"object",
        "required":["field-that-wrapper-never-provides"]
    }));
    let (snapshot, runtime, _, _) = run_supervisor_workflow_with_permission_mode(
        "supervisor-cleanup-failure",
        collaboration,
        vec![v2_role("coordinator", &["Read"], PermissionCeiling::plan()), worker],
        scripted,
        2,
        solaris_types::workflow::MultiAgentPolicy::OnDemand,
        SupervisorWorkflowRuntimeOptions {
            permission_mode: solaris_types::permission::PermissionMode::Auto,
            ledger: Some(Arc::new(FailSupervisorCleanupLedger::default())),
            concurrency_gate: None,
            max_tasks_per_run: None,
        },
    )
    .await;

    assert_eq!(snapshot.status, WorkflowRunStatus::Failed);
    let failed = runtime
        .ledger()
        .records_for_run(&snapshot.run_id)
        .unwrap()
        .into_iter()
        .find(|record| record.record_type == "workflow_node_failed")
        .unwrap();
    assert_eq!(failed.payload["failure_class"], "reconciliation_required");
    let error = failed.payload["error"].as_str().unwrap();
    assert!(error.contains("field-that-wrapper-never-provides"), "{error}");
    assert!(error.contains("injected Supervisor cleanup failure"), "{error}");
    assert!(
        error.contains("injected Supervisor close failure after task cleanup failure"),
        "{error}"
    );
    let tasks: HashMap<_, _> = runtime
        .tasks()
        .snapshot()
        .into_iter()
        .filter_map(|task| task.task_key.clone().map(|key| (key, task)))
        .collect();
    assert_eq!(
        tasks["a-cleanup-fails"].state,
        TaskState::Queued,
        "failed cleanup must preserve the task for reconciliation"
    );
    assert_eq!(
        tasks["b-cleanup-continues"].state,
        TaskState::Cancelled,
        "one failed CAS must not stop cleanup of the remaining known Tasks"
    );
}

#[tokio::test]
async fn close_failure_upgrades_to_reconciliation_and_preserves_both_causes() {
    let collaboration = CollaborationRuntimeConfig {
        strategy: CollaborationStrategy::Supervisor,
        worker_roles: vec![WorkerRolePolicy {
            role: "worker-a".into(),
            max_concurrent: 1,
            max_total: 1,
        }],
        max_tasks: 1,
        max_coordinator_rounds: 2,
        ..CollaborationRuntimeConfig::default()
    };
    let scripted = HashMap::from([(
        "coordinator".to_owned(),
        VecDeque::from([json!({
            "decision":"dispatch",
            "tasks":[{"task_key":"close-fails", "role":"worker-a", "instruction":"inspect"}]
        })
        .to_string()]),
    )]);
    let mut worker = v2_role("worker-a", &["Read"], PermissionCeiling::plan());
    worker.input_schema = Some(json!({
        "type":"object",
        "required":["field-that-wrapper-never-provides"]
    }));
    let (snapshot, runtime, _, _) = run_supervisor_workflow_with_permission_mode(
        "supervisor-close-failure",
        collaboration,
        vec![v2_role("coordinator", &["Read"], PermissionCeiling::plan()), worker],
        scripted,
        2,
        solaris_types::workflow::MultiAgentPolicy::OnDemand,
        SupervisorWorkflowRuntimeOptions {
            permission_mode: solaris_types::permission::PermissionMode::Auto,
            ledger: Some(Arc::new(FailSupervisorCloseLedger::default())),
            concurrency_gate: None,
            max_tasks_per_run: None,
        },
    )
    .await;

    assert_eq!(snapshot.status, WorkflowRunStatus::Failed);
    let failed = runtime
        .ledger()
        .records_for_run(&snapshot.run_id)
        .unwrap()
        .into_iter()
        .find(|record| record.record_type == "workflow_node_failed")
        .unwrap();
    assert_eq!(failed.payload["failure_class"], "reconciliation_required");
    let error = failed.payload["error"].as_str().unwrap();
    assert!(error.contains("field-that-wrapper-never-provides"), "{error}");
    assert!(error.contains("injected Supervisor close failure"), "{error}");
    let task = runtime
        .tasks()
        .snapshot()
        .into_iter()
        .find(|task| task.task_key.as_deref() == Some("close-fails"))
        .unwrap();
    assert_eq!(task.state, TaskState::Cancelled);
}

#[tokio::test]
async fn delivery_capacity_error_closes_coordinator_after_completed_worker() {
    let collaboration = CollaborationRuntimeConfig {
        strategy: CollaborationStrategy::Supervisor,
        worker_roles: vec![WorkerRolePolicy {
            role: "worker-a".into(),
            max_concurrent: 1,
            max_total: 1,
        }],
        max_tasks: 1,
        max_coordinator_rounds: 2,
        max_message_bytes: 1_024,
        ..CollaborationRuntimeConfig::default()
    };
    let scripted = HashMap::from([
        (
            "coordinator".to_owned(),
            VecDeque::from([json!({
                "decision":"dispatch",
                "tasks":[{"task_key":"large", "role":"worker-a", "instruction":"produce evidence"}]
            })
            .to_string()]),
        ),
        (
            "worker-a".to_owned(),
            VecDeque::from([json!({"role":"worker-a", "evidence":"x".repeat(8 * 1_024)}).to_string()]),
        ),
    ]);
    let (snapshot, runtime, _, state) = run_supervisor_workflow(
        "supervisor-delivery-capacity-cleanup",
        collaboration,
        vec![
            v2_role("coordinator", &["Read"], PermissionCeiling::plan()),
            v2_role("worker-a", &["Read"], PermissionCeiling::plan()),
        ],
        scripted,
        2,
        solaris_types::workflow::MultiAgentPolicy::OnDemand,
    )
    .await;

    assert_eq!(snapshot.status, WorkflowRunStatus::Failed);
    assert!(
        v2_node_attempt(&snapshot)
            .error
            .as_deref()
            .is_some_and(|error| error.contains("max_message_bytes")),
        "{snapshot:#?}"
    );
    assert_eq!(
        state.lock().unwrap_or_else(|error| error.into_inner()).calls,
        ["coordinator", "worker-a"]
    );
    assert!(
        runtime
            .tasks()
            .snapshot()
            .iter()
            .all(|task| !matches!(task.state, TaskState::Queued | TaskState::Assigned | TaskState::Running))
    );
    assert_supervisor_team_agents_are_terminal(&runtime);
}

#[tokio::test]
async fn concurrent_worker_failure_leaves_no_active_task_or_agent() {
    let collaboration = CollaborationRuntimeConfig {
        strategy: CollaborationStrategy::Supervisor,
        worker_roles: vec![
            WorkerRolePolicy {
                role: "worker-a".into(),
                max_concurrent: 1,
                max_total: 1,
            },
            WorkerRolePolicy {
                role: "invalid-worker".into(),
                max_concurrent: 1,
                max_total: 1,
            },
        ],
        max_concurrent_workers: 2,
        max_tasks: 2,
        max_coordinator_rounds: 2,
        ..CollaborationRuntimeConfig::default()
    };
    let scripted = HashMap::from([
        (
            "coordinator".to_owned(),
            VecDeque::from([
                json!({
                    "decision":"dispatch",
                    "tasks":[
                        {"task_key":"valid", "role":"worker-a", "instruction":"inspect"},
                        {"task_key":"invalid", "role":"invalid-worker", "instruction":"inspect"}
                    ]
                })
                .to_string(),
                json!({"decision":"finalize", "output":{"role":"must-not-finalize"}}).to_string(),
            ]),
        ),
        (
            "worker-a".to_owned(),
            VecDeque::from([json!({"role":"worker-a"}).to_string()]),
        ),
    ]);
    let coordinator = v2_role("coordinator", &["Read"], PermissionCeiling::plan());
    let valid = v2_role("worker-a", &["Read"], PermissionCeiling::plan());
    let invalid = v2_role("invalid-worker", &["Read"], PermissionCeiling::plan());
    let (snapshot, runtime, _, state) = run_supervisor_workflow_with_permission_mode(
        "supervisor-concurrent-cleanup",
        collaboration,
        vec![coordinator, valid, invalid],
        scripted,
        4,
        solaris_types::workflow::MultiAgentPolicy::Proactive,
        SupervisorWorkflowRuntimeOptions {
            concurrency_gate: Some(V2ProviderConcurrencyGate::new(
                &["worker-a", "invalid-worker"],
                2,
                2,
            )),
            ..SupervisorWorkflowRuntimeOptions::default()
        },
    )
    .await;

    assert_eq!(snapshot.status, WorkflowRunStatus::Failed);
    let state = state.lock().unwrap_or_else(|error| error.into_inner());
    assert_eq!(state.max_active_by_model.get("worker-a"), Some(&1));
    assert_eq!(state.max_active_by_model.get("invalid-worker"), Some(&1));
    assert_eq!(state.max_active, 2, "workers did not overlap: {:?}", state.timeline);
    drop(state);
    let tasks: Vec<_> = runtime
        .tasks()
        .snapshot()
        .into_iter()
        .filter(|task| task.team_id.is_some())
        .collect();
    assert_eq!(tasks.len(), 2);
    assert!(tasks.iter().any(|task| task.state == TaskState::Completed));
    assert!(tasks.iter().any(|task| task.state == TaskState::Failed));
    assert!(
        tasks
            .iter()
            .all(|task| !matches!(task.state, TaskState::Queued | TaskState::Assigned | TaskState::Running))
    );
    assert_supervisor_team_agents_are_terminal(&runtime);
}

#[tokio::test]
async fn auto_rejects_process_worker_scope_the_runner_cannot_enforce() {
    for (case, scope) in [
        ("empty", json!([])),
        ("multiple", json!([".", "subdir"])),
        ("file", json!(["file.rs"])),
        ("subdir", json!(["subdir"])),
    ] {
        let collaboration = CollaborationRuntimeConfig {
            strategy: CollaborationStrategy::Supervisor,
            worker_roles: vec![WorkerRolePolicy {
                role: "worker-a".into(),
                max_concurrent: 1,
                max_total: 1,
            }],
            max_tasks: 1,
            max_coordinator_rounds: 2,
            ..CollaborationRuntimeConfig::default()
        };
        let scripted = HashMap::from([(
            "coordinator".to_owned(),
            VecDeque::from([json!({
                "decision":"dispatch",
                "tasks":[{
                    "task_key":"unsupported-process-scope",
                    "role":"worker-a",
                    "instruction":"run a command",
                    "expected_write_scope":scope
                }]
            })
            .to_string()]),
        )]);
        let (snapshot, runtime, _, state) = run_supervisor_workflow(
            &format!("supervisor-auto-process-scope-{case}"),
            collaboration,
            vec![
                v2_role("coordinator", &["Read"], PermissionCeiling::plan()),
                v2_role("worker-a", &["ExecCommand"], PermissionCeiling::unrestricted()),
            ],
            scripted,
            2,
            solaris_types::workflow::MultiAgentPolicy::OnDemand,
        )
        .await;

        assert_eq!(snapshot.status, WorkflowRunStatus::Failed, "case={case}");
        assert!(
            v2_node_attempt(&snapshot)
                .error
                .as_deref()
                .is_some_and(|error| error.contains("cannot enforce process write scope")),
            "case={case}: {snapshot:#?}"
        );
        assert_eq!(
            state.lock().unwrap_or_else(|error| error.into_inner()).calls,
            ["coordinator"],
            "case={case}"
        );
        assert!(runtime.tasks().snapshot().iter().all(|task| task.team_id.is_none()));
        assert_supervisor_team_agents_are_terminal(&runtime);
    }
}

#[tokio::test]
async fn auto_accepts_process_worker_scoped_to_the_workspace_root() {
    let collaboration = CollaborationRuntimeConfig {
        strategy: CollaborationStrategy::Supervisor,
        worker_roles: vec![WorkerRolePolicy {
            role: "worker-a".into(),
            max_concurrent: 1,
            max_total: 1,
        }],
        max_tasks: 1,
        max_coordinator_rounds: 2,
        ..CollaborationRuntimeConfig::default()
    };
    let scripted = HashMap::from([
        (
            "coordinator".to_owned(),
            VecDeque::from([
                json!({
                    "decision":"dispatch",
                    "tasks":[{
                        "task_key":"workspace-process-scope",
                        "role":"worker-a",
                        "instruction":"run a command",
                        "expected_write_scope":["."]
                    }]
                })
                .to_string(),
                json!({"decision":"finalize", "output":{"role":"coordinator"}}).to_string(),
            ]),
        ),
        (
            "worker-a".to_owned(),
            VecDeque::from([json!({"role":"worker-a"}).to_string()]),
        ),
    ]);
    let (snapshot, runtime, _, state) = run_supervisor_workflow(
        "supervisor-auto-workspace-process-scope",
        collaboration,
        vec![
            v2_role("coordinator", &["Read"], PermissionCeiling::plan()),
            v2_role("worker-a", &["ExecCommand"], PermissionCeiling::unrestricted()),
        ],
        scripted,
        2,
        solaris_types::workflow::MultiAgentPolicy::OnDemand,
    )
    .await;

    assert_eq!(snapshot.status, WorkflowRunStatus::Completed, "{snapshot:#?}");
    assert_eq!(
        state.lock().unwrap_or_else(|error| error.into_inner()).calls,
        ["coordinator", "worker-a", "coordinator"]
    );
    let task = runtime
        .tasks()
        .snapshot()
        .into_iter()
        .find(|task| task.task_key.as_deref() == Some("workspace-process-scope"))
        .unwrap();
    assert_eq!(task.state, TaskState::Completed);
}

#[tokio::test]
async fn bypass_treats_supervisor_write_scope_as_advisory() {
    let collaboration = CollaborationRuntimeConfig {
        strategy: CollaborationStrategy::Supervisor,
        worker_roles: vec![WorkerRolePolicy {
            role: "worker-a".into(),
            max_concurrent: 1,
            max_total: 1,
        }],
        max_tasks: 1,
        max_coordinator_rounds: 2,
        ..CollaborationRuntimeConfig::default()
    };
    let scripted = HashMap::from([
        (
            "coordinator".to_owned(),
            VecDeque::from([
                json!({
                    "decision":"dispatch",
                    "tasks":[{
                        "task_key":"bypass-advisory-scope",
                        "role":"worker-a",
                        "instruction":"run a command",
                        "expected_write_scope":["subdir"]
                    }]
                })
                .to_string(),
                json!({"decision":"finalize", "output":{"role":"coordinator"}}).to_string(),
            ]),
        ),
        (
            "worker-a".to_owned(),
            VecDeque::from([json!({"role":"worker-a"}).to_string()]),
        ),
    ]);
    let (snapshot, runtime, root_run, _) = run_supervisor_workflow_with_permission_mode(
        "supervisor-bypass-advisory-scope",
        collaboration,
        vec![
            v2_role("coordinator", &["Read"], PermissionCeiling::plan()),
            v2_role("worker-a", &["ExecCommand"], PermissionCeiling::unrestricted()),
        ],
        scripted,
        2,
        solaris_types::workflow::MultiAgentPolicy::OnDemand,
        SupervisorWorkflowRuntimeOptions {
            permission_mode: solaris_types::permission::PermissionMode::Bypass,
            ledger: None,
            concurrency_gate: None,
            max_tasks_per_run: None,
        },
    )
    .await;

    assert_eq!(snapshot.status, WorkflowRunStatus::Completed, "{snapshot:#?}");
    let task = runtime
        .tasks()
        .snapshot()
        .into_iter()
        .find(|task| task.task_key.as_deref() == Some("bypass-advisory-scope"))
        .unwrap();
    assert_eq!(task.expected_write_scope, ["subdir"]);
    assert_eq!(
        task.content.as_ref().unwrap()["expected_write_scope_semantics"],
        "advisory"
    );
    let handle = v2_handles(&runtime, &root_run)
        .into_iter()
        .find(|handle| handle.task_id == task.task_id)
        .unwrap();
    assert!(handle.spec.overrides.execution_boundary.is_none());
}

#[tokio::test]
async fn finalize_and_workflow_completion_reuse_one_output_blob_reference() {
    let collaboration = CollaborationRuntimeConfig {
        strategy: CollaborationStrategy::Supervisor,
        worker_roles: vec![WorkerRolePolicy {
            role: "worker-a".into(),
            max_concurrent: 1,
            max_total: 1,
        }],
        max_tasks: 1,
        max_coordinator_rounds: 1,
        ..CollaborationRuntimeConfig::default()
    };
    let marker = "SOLARIS_UNIQUE_SUPERVISOR_FINAL_OUTPUT";
    let evidence = format!("{marker}{}", "z".repeat(128 * 1_024));
    let scripted = HashMap::from([(
        "coordinator".to_owned(),
        VecDeque::from([
            json!({"decision":"finalize", "output":{"role":"coordinator", "evidence":evidence}}).to_string(),
        ]),
    )]);
    let (snapshot, runtime, root_run, _) = run_supervisor_workflow(
        "supervisor-final-output-ref",
        collaboration,
        vec![
            v2_role("coordinator", &["Read"], PermissionCeiling::plan()),
            v2_role("worker-a", &["Read"], PermissionCeiling::plan()),
        ],
        scripted,
        1,
        solaris_types::workflow::MultiAgentPolicy::OnDemand,
    )
    .await;

    assert_eq!(snapshot.status, WorkflowRunStatus::Completed, "{snapshot:#?}");
    let records = runtime.ledger().records_for_run(&root_run).unwrap();
    assert!(
        records
            .iter()
            .all(|record| !serde_json::to_string(&record.payload).unwrap().contains(marker)),
        "large final output body must not be duplicated in Run Ledger records"
    );
    let coordinator_turn = records
        .iter()
        .find(|record| record.record_type == "agent_conversation_turn_outcome")
        .unwrap();
    assert_eq!(
        coordinator_turn.payload["output"]["text"]
            .as_str()
            .unwrap()
            .matches(marker)
            .count(),
        0
    );
    let coordinator_ref = coordinator_turn.payload["outcome_ref"]["reference"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(coordinator_turn.payload["outcome_ref"]["status"], "completed");
    let decision = records
        .into_iter()
        .find(|record| record.record_type == "workflow_supervisor_decision")
        .unwrap();
    assert_eq!(decision.payload["decision"]["output"], Value::Null);
    assert!(!serde_json::to_string(&decision.payload).unwrap().contains(marker));
    let final_ref = decision.payload["final_output_ref"]["reference"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(coordinator_ref, final_ref);
    assert_eq!(decision.payload["final_output_ref"]["status"], "completed");
    let completion = runtime
        .ledger()
        .records_for_run(&snapshot.run_id)
        .unwrap()
        .into_iter()
        .find(|record| record.record_type == "workflow_node_completed")
        .unwrap();
    assert_eq!(completion.payload["output_ref"].as_str(), Some(final_ref.as_str()));
    assert_eq!(
        completion.payload["output_bytes"],
        decision.payload["final_output_ref"]["bytes"]
    );
    assert_eq!(
        completion.payload["output_digest"],
        decision.payload["final_output_ref"]["digest"]
    );
    assert_eq!(completion.payload["output_status"], "completed");
    let body =
        crate::execution_context::EffectOutputStore::for_run_with_ledger(&snapshot.run_id, runtime.ledger().as_ref())
            .read(&final_ref)
            .unwrap();
    assert_eq!(body.matches(marker).count(), 1);
}
