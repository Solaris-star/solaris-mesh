use super::*;

#[tokio::test]
async fn configured_supervisor_runs_coordinator_first_and_respects_dependencies() {
    let collaboration = CollaborationRuntimeConfig {
        strategy: CollaborationStrategy::Supervisor,
        worker_roles: vec![
            WorkerRolePolicy {
                role: "worker-a".into(),
                max_concurrent: 1,
                max_total: 1,
            },
            WorkerRolePolicy {
                role: "worker-b".into(),
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
                    "decision": "dispatch",
                    "tasks": [
                        {
                            "task_key": "inspect",
                            "role": "worker-a",
                            "instruction": "Inspect the requested files",
                            "depends_on": [],
                            "expected_write_scope": ["src/a.rs"]
                        },
                        {
                            "task_key": "verify",
                            "role": "worker-b",
                            "instruction": "Verify the inspection result",
                            "depends_on": ["inspect"],
                            "expected_write_scope": []
                        }
                    ]
                })
                .to_string(),
                json!({"decision":"finalize", "output":{"role":"coordinator-final"}}).to_string(),
            ]),
        ),
        (
            "worker-a".to_owned(),
            VecDeque::from([json!({"role":"worker-a"}).to_string()]),
        ),
        (
            "worker-b".to_owned(),
            VecDeque::from([json!({"role":"worker-b"}).to_string()]),
        ),
    ]);
    let (snapshot, runtime, root_run, state) = run_supervisor_workflow(
        "supervisor",
        collaboration,
        vec![
            v2_role("coordinator", &["Read"], PermissionCeiling::plan()),
            v2_role("worker-a", &["Grep"], PermissionCeiling::plan()),
            v2_role("worker-b", &["Read"], PermissionCeiling::plan()),
        ],
        scripted,
        1,
        solaris_types::workflow::MultiAgentPolicy::OnDemand,
    )
    .await;

    assert_eq!(snapshot.status, WorkflowRunStatus::Completed, "{snapshot:#?}");
    assert_eq!(
        v2_node_attempt(&snapshot).output,
        Some(json!({"role":"coordinator-final"}))
    );
    let state = state.lock().unwrap_or_else(|error| error.into_inner());
    assert_eq!(state.calls, ["coordinator", "worker-a", "worker-b", "coordinator"]);
    assert_eq!(
        state.max_active, 1,
        "coordinator must release the Run permit between turns"
    );
    drop(state);

    let tasks: Vec<_> = runtime
        .tasks()
        .snapshot()
        .into_iter()
        .filter(|task| task.team_id.is_some())
        .collect();
    assert_eq!(tasks.len(), 2);
    let inspect = tasks
        .iter()
        .find(|task| task.task_key.as_deref() == Some("inspect"))
        .unwrap();
    let verify = tasks
        .iter()
        .find(|task| task.task_key.as_deref() == Some("verify"))
        .unwrap();
    assert_eq!(inspect.expected_write_scope, ["src/a.rs"]);
    assert_eq!(verify.depends_on.as_slice(), std::slice::from_ref(&inspect.task_id));
    assert_eq!(inspect.state, TaskState::Completed);
    assert_eq!(verify.state, TaskState::Completed);
    let inspect_handle = v2_handles(&runtime, &root_run)
        .into_iter()
        .find(|handle| handle.task_id == inspect.task_id)
        .unwrap();
    let inspect_boundary = inspect_handle.spec.overrides.execution_boundary.unwrap();
    assert!(!inspect_boundary.unrestricted_file_writes);
    assert!(
        inspect_boundary
            .writable_roots
            .iter()
            .any(|root| Path::new(root).ends_with(Path::new("src/a.rs")))
    );

    let records = runtime.ledger().records_for_run(&root_run).unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|record| record.record_type == "workflow_supervisor_decision")
            .count(),
        2
    );
    let first_decision = records
        .iter()
        .find(|record| record.record_type == "workflow_supervisor_decision")
        .unwrap();
    let first_task = records
        .iter()
        .find(|record| {
            record.record_type == "task_created" && record.payload.get("team_id").is_some_and(|value| !value.is_null())
        })
        .unwrap();
    assert!(
        first_decision.seq < first_task.seq,
        "decision must be durable before Task creation"
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| record.record_type == "agent_conversation_opened")
            .count(),
        1
    );
    let coordinator: solaris_types::spawner::AgentConversationHandle = serde_json::from_value(
        records
            .iter()
            .find(|record| record.record_type == "agent_conversation_opened")
            .unwrap()
            .payload
            .clone(),
    )
    .unwrap();
    assert_eq!(
        coordinator.spec.overrides.allowed_tools,
        ["Read"],
        "the coordinator turn must not receive Team mutation or inbox tools"
    );
}

#[tokio::test]
async fn configured_supervisor_rejects_invalid_proposals_before_creating_tasks() {
    let cases = vec![
        (
            "duplicate-key",
            vec![
                json!({"task_key":"same", "role":"worker-a", "instruction":"first"}),
                json!({"task_key":"same", "role":"worker-a", "instruction":"second"}),
            ],
            "duplicate Supervisor task key",
        ),
        (
            "unknown-dependency",
            vec![json!({
                "task_key":"first",
                "role":"worker-a",
                "instruction":"first",
                "depends_on":["missing"]
            })],
            "unknown dependency",
        ),
        (
            "self-dependency",
            vec![json!({
                "task_key":"first",
                "role":"worker-a",
                "instruction":"first",
                "depends_on":["first"]
            })],
            "depends on itself",
        ),
        (
            "cycle",
            vec![
                json!({
                    "task_key":"first",
                    "role":"worker-a",
                    "instruction":"first",
                    "depends_on":["second"]
                }),
                json!({
                    "task_key":"second",
                    "role":"worker-a",
                    "instruction":"second",
                    "depends_on":["first"]
                }),
            ],
            "dependency graph contains a cycle",
        ),
        (
            "unknown-role",
            vec![json!({
                "task_key":"first",
                "role":"not-configured",
                "instruction":"first"
            })],
            "unknown configured Supervisor worker role",
        ),
        (
            "task-count",
            vec![
                json!({"task_key":"first", "role":"worker-a", "instruction":"first"}),
                json!({"task_key":"second", "role":"worker-a", "instruction":"second"}),
                json!({"task_key":"third", "role":"worker-a", "instruction":"third"}),
            ],
            "Task count exceeds 2",
        ),
        (
            "absolute-write-scope",
            vec![json!({
                "task_key":"first",
                "role":"worker-a",
                "instruction":"first",
                "expected_write_scope":[std::env::temp_dir().join("outside").to_string_lossy()]
            })],
            "absolute write scope",
        ),
        (
            "parent-write-scope",
            vec![json!({
                "task_key":"first",
                "role":"worker-a",
                "instruction":"first",
                "expected_write_scope":["../outside"]
            })],
            "write scope contains '..'",
        ),
        (
            "glob-write-scope",
            vec![json!({
                "task_key":"first",
                "role":"worker-a",
                "instruction":"first",
                "expected_write_scope":["src/**/*.rs"]
            })],
            "unsupported glob",
        ),
    ];

    for (case, tasks, expected_error) in cases {
        let collaboration = CollaborationRuntimeConfig {
            strategy: CollaborationStrategy::Supervisor,
            worker_roles: vec![WorkerRolePolicy {
                role: "worker-a".into(),
                max_concurrent: 1,
                max_total: 2,
            }],
            max_tasks: 2,
            ..CollaborationRuntimeConfig::default()
        };
        let scripted = HashMap::from([(
            "coordinator".to_owned(),
            VecDeque::from([json!({"decision":"dispatch", "tasks":tasks}).to_string()]),
        )]);
        let (snapshot, runtime, root_run, state) = run_supervisor_workflow(
            &format!("supervisor-invalid-proposal-{case}"),
            collaboration,
            vec![
                v2_role("coordinator", &["Read"], PermissionCeiling::plan()),
                v2_role("worker-a", &["Grep"], PermissionCeiling::plan()),
            ],
            scripted,
            2,
            solaris_types::workflow::MultiAgentPolicy::OnDemand,
        )
        .await;

        assert_eq!(snapshot.status, WorkflowRunStatus::Failed, "case {case}");
        assert!(
            v2_node_attempt(&snapshot)
                .error
                .as_deref()
                .is_some_and(|error| error.contains(expected_error)),
            "case {case}: {snapshot:#?}"
        );
        assert!(
            runtime.tasks().snapshot().iter().all(|task| task.team_id.is_none()),
            "case {case} created a partial Task batch"
        );
        assert_eq!(
            runtime
                .ledger()
                .records_for_run(&root_run)
                .unwrap()
                .iter()
                .filter(|record| record.record_type == "workflow_supervisor_decision")
                .count(),
            0,
            "case {case} persisted an invalid decision"
        );
        assert_eq!(
            state.lock().unwrap_or_else(|error| error.into_inner()).calls,
            ["coordinator"],
            "case {case} executed a worker"
        );
    }
}

#[tokio::test]
async fn configured_supervisor_cannot_bypass_run_task_quota() {
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
    let scripted = HashMap::from([(
        "coordinator".to_owned(),
        VecDeque::from([json!({
            "decision":"dispatch",
            "tasks":[{"task_key":"extra", "role":"worker-a", "instruction":"must be rejected by Run quota"}]
        })
        .to_string()]),
    )]);
    let (snapshot, runtime, _, state) = run_supervisor_workflow_with_permission_mode(
        "supervisor-run-quota",
        collaboration,
        vec![
            v2_role("coordinator", &["Read"], PermissionCeiling::plan()),
            v2_role("worker-a", &["Grep"], PermissionCeiling::plan()),
        ],
        scripted,
        2,
        solaris_types::workflow::MultiAgentPolicy::OnDemand,
        SupervisorWorkflowRuntimeOptions {
            max_tasks_per_run: Some(1),
            ..SupervisorWorkflowRuntimeOptions::default()
        },
    )
    .await;

    assert_eq!(snapshot.status, WorkflowRunStatus::Failed, "{snapshot:#?}");
    assert_eq!(v2_node_attempt(&snapshot).attempt_number, 1);
    assert!(
        v2_node_attempt(&snapshot)
            .error
            .as_deref()
            .is_some_and(|error| error.contains("accepts at most 1 collaboration tasks")),
        "{snapshot:#?}"
    );
    assert_eq!(
        runtime.tasks().snapshot().iter().filter(|task| task.team_id.is_some()).count(),
        0,
        "Supervisor must not create a partial dynamic task after the Run quota is exhausted"
    );
    assert_eq!(
        state.lock().unwrap_or_else(|error| error.into_inner()).calls,
        ["coordinator"],
        "quota rejection must happen before a worker provider call"
    );
}

#[tokio::test]
async fn configured_supervisor_abort_is_typed_and_spawns_no_worker() {
    let collaboration = CollaborationRuntimeConfig {
        strategy: CollaborationStrategy::Supervisor,
        worker_roles: vec![WorkerRolePolicy {
            role: "worker-a".into(),
            max_concurrent: 1,
            max_total: 1,
        }],
        ..CollaborationRuntimeConfig::default()
    };
    let scripted = HashMap::from([(
        "coordinator".to_owned(),
        VecDeque::from([json!({
            "decision": "abort",
            "reason": "requirements are contradictory",
            "failure_class": "non_retryable"
        })
        .to_string()]),
    )]);
    let (snapshot, runtime, _, state) = run_supervisor_workflow(
        "supervisor-abort",
        collaboration,
        vec![
            v2_role("coordinator", &["Read"], PermissionCeiling::plan()),
            v2_role("worker-a", &["Grep"], PermissionCeiling::plan()),
        ],
        scripted,
        2,
        solaris_types::workflow::MultiAgentPolicy::OnDemand,
    )
    .await;

    assert_eq!(snapshot.status, WorkflowRunStatus::Failed);
    assert_eq!(v2_node_attempt(&snapshot).attempt_number, 1);
    assert!(
        v2_node_attempt(&snapshot)
            .error
            .as_deref()
            .is_some_and(|error| error.contains("requirements are contradictory"))
    );
    assert!(runtime.tasks().snapshot().iter().all(|task| task.team_id.is_none()));
    assert_eq!(
        state.lock().unwrap_or_else(|error| error.into_inner()).calls,
        ["coordinator"]
    );
}

#[tokio::test]
async fn configured_supervisor_abort_cancels_dependency_blocked_tasks() {
    let collaboration = CollaborationRuntimeConfig {
        strategy: CollaborationStrategy::Supervisor,
        worker_roles: vec![
            WorkerRolePolicy {
                role: "invalid-worker".into(),
                max_concurrent: 1,
                max_total: 1,
            },
            WorkerRolePolicy {
                role: "worker-b".into(),
                max_concurrent: 1,
                max_total: 1,
            },
        ],
        max_tasks: 2,
        max_coordinator_rounds: 2,
        ..CollaborationRuntimeConfig::default()
    };
    let scripted = HashMap::from([(
        "coordinator".to_owned(),
        VecDeque::from([
            json!({
                "decision":"dispatch",
                "tasks":[
                    {"task_key":"fails", "role":"invalid-worker", "instruction":"fail"},
                    {
                        "task_key":"blocked",
                        "role":"worker-b",
                        "instruction":"must not run",
                        "depends_on":["fails"]
                    }
                ]
            })
            .to_string(),
            json!({"decision":"abort", "reason":"worker failed", "failure_class":"non_retryable"}).to_string(),
        ]),
    )]);
    let (snapshot, runtime, _, state) = run_supervisor_workflow(
        "supervisor-abort-blocked",
        collaboration,
        vec![
            v2_role("coordinator", &["Read"], PermissionCeiling::plan()),
            v2_role("invalid-worker", &["Read"], PermissionCeiling::plan()),
            v2_role("worker-b", &["Read"], PermissionCeiling::plan()),
        ],
        scripted,
        2,
        solaris_types::workflow::MultiAgentPolicy::OnDemand,
    )
    .await;

    assert_eq!(snapshot.status, WorkflowRunStatus::Failed);
    let tasks: HashMap<_, _> = runtime
        .tasks()
        .snapshot()
        .into_iter()
        .filter_map(|task| task.task_key.clone().map(|key| (key, task)))
        .collect();
    assert_eq!(tasks["fails"].state, TaskState::Failed);
    assert_eq!(tasks["blocked"].state, TaskState::Cancelled, "{snapshot:#?}");
    assert_eq!(
        state.lock().unwrap_or_else(|error| error.into_inner()).calls,
        ["coordinator", "invalid-worker", "coordinator"]
    );
}

#[tokio::test]
async fn configured_supervisor_round_exhaustion_cancels_dependency_blocked_tasks() {
    let collaboration = CollaborationRuntimeConfig {
        strategy: CollaborationStrategy::Supervisor,
        worker_roles: vec![
            WorkerRolePolicy {
                role: "invalid-worker".into(),
                max_concurrent: 1,
                max_total: 1,
            },
            WorkerRolePolicy {
                role: "worker-b".into(),
                max_concurrent: 1,
                max_total: 1,
            },
        ],
        max_tasks: 2,
        max_coordinator_rounds: 1,
        ..CollaborationRuntimeConfig::default()
    };
    let scripted = HashMap::from([(
        "coordinator".to_owned(),
        VecDeque::from([json!({
            "decision":"dispatch",
            "tasks":[
                {"task_key":"fails", "role":"invalid-worker", "instruction":"fail"},
                {
                    "task_key":"blocked",
                    "role":"worker-b",
                    "instruction":"must not run",
                    "depends_on":["fails"]
                }
            ]
        })
        .to_string()]),
    )]);
    let (snapshot, runtime, _, _) = run_supervisor_workflow(
        "supervisor-round-exhaustion",
        collaboration,
        vec![
            v2_role("coordinator", &["Read"], PermissionCeiling::plan()),
            v2_role("invalid-worker", &["Read"], PermissionCeiling::plan()),
            v2_role("worker-b", &["Read"], PermissionCeiling::plan()),
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
            .is_some_and(|error| error.contains("exceeded its 1 coordinator rounds"))
    );
    let blocked = runtime
        .tasks()
        .snapshot()
        .into_iter()
        .find(|task| task.task_key.as_deref() == Some("blocked"))
        .unwrap();
    assert_eq!(blocked.state, TaskState::Cancelled);
}

#[tokio::test]
async fn configured_supervisor_finalize_gate_rejects_failed_worker_task() {
    let collaboration = CollaborationRuntimeConfig {
        strategy: CollaborationStrategy::Supervisor,
        worker_roles: vec![WorkerRolePolicy {
            role: "invalid-worker".into(),
            max_concurrent: 1,
            max_total: 1,
        }],
        max_tasks: 1,
        max_coordinator_rounds: 2,
        ..CollaborationRuntimeConfig::default()
    };
    let scripted = HashMap::from([(
        "coordinator".to_owned(),
        VecDeque::from([
            json!({
                "decision":"dispatch",
                "tasks":[{
                    "task_key":"invalid",
                    "role":"invalid-worker",
                    "instruction":"return invalid output"
                }]
            })
            .to_string(),
            json!({"decision":"finalize", "output":{"role":"must-not-pass"}}).to_string(),
        ]),
    )]);
    let (snapshot, runtime, _, state) = run_supervisor_workflow(
        "supervisor-finalize-gate",
        collaboration,
        vec![
            v2_role("coordinator", &["Read"], PermissionCeiling::plan()),
            v2_role("invalid-worker", &["Grep"], PermissionCeiling::plan()),
        ],
        scripted,
        2,
        solaris_types::workflow::MultiAgentPolicy::OnDemand,
    )
    .await;

    assert_eq!(snapshot.status, WorkflowRunStatus::Failed);
    assert_eq!(v2_node_attempt(&snapshot).attempt_number, 1);
    assert!(
        v2_node_attempt(&snapshot)
            .error
            .as_deref()
            .is_some_and(|error| error.contains("Supervisor Task invalid failed")),
        "{snapshot:#?}"
    );
    let task = runtime
        .tasks()
        .snapshot()
        .into_iter()
        .find(|task| task.task_key.as_deref() == Some("invalid"))
        .unwrap();
    assert_eq!(task.state, TaskState::Failed);
    assert_eq!(task.failure_class, Some(TaskFailureClass::NonConvergent));
    assert_eq!(
        state.lock().unwrap_or_else(|error| error.into_inner()).calls,
        ["coordinator", "invalid-worker", "coordinator"]
    );
}

#[tokio::test]
async fn configured_supervisor_rejects_finalize_output_that_violates_coordinator_schema() {
    let collaboration = CollaborationRuntimeConfig {
        strategy: CollaborationStrategy::Supervisor,
        worker_roles: vec![WorkerRolePolicy {
            role: "worker-a".into(),
            max_concurrent: 1,
            max_total: 1,
        }],
        max_coordinator_rounds: 1,
        ..CollaborationRuntimeConfig::default()
    };
    let scripted = HashMap::from([(
        "coordinator".to_owned(),
        VecDeque::from([json!({"decision":"finalize", "output":{"wrong":true}}).to_string()]),
    )]);
    let (snapshot, _, _, state) = run_supervisor_workflow(
        "supervisor-invalid-final-schema",
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

    assert_eq!(snapshot.status, WorkflowRunStatus::Failed);
    assert!(
        v2_node_attempt(&snapshot)
            .error
            .as_deref()
            .is_some_and(|error| error.contains("coordinator output invalid")),
        "{snapshot:#?}"
    );
    assert_eq!(
        state.lock().unwrap_or_else(|error| error.into_inner()).calls,
        ["coordinator"]
    );
}

#[tokio::test]
async fn configured_supervisor_validates_the_real_worker_wrapper_input() {
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
                    "tasks":[{"task_key":"wrapped", "role":"worker-a", "instruction":"inspect"}]
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
    let mut worker = v2_role("worker-a", &["Read"], PermissionCeiling::plan());
    worker.input_schema = Some(json!({
        "type":"object",
        "required":["workflow_input", "supervisor_task"],
        "properties": {
            "workflow_input": {"type":"object"},
            "supervisor_task": {
                "type":"object",
                "required":["task_key", "role", "instruction"]
            }
        }
    }));
    let (snapshot, _, _, state) = run_supervisor_workflow(
        "supervisor-wrapper-schema",
        collaboration,
        vec![v2_role("coordinator", &["Read"], PermissionCeiling::plan()), worker],
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
}

#[tokio::test]
async fn configured_supervisor_bounds_independent_worker_concurrency() {
    let collaboration = CollaborationRuntimeConfig {
        strategy: CollaborationStrategy::Supervisor,
        worker_roles: vec![WorkerRolePolicy {
            role: "worker-a".into(),
            max_concurrent: 2,
            max_total: 4,
        }],
        max_concurrent_workers: 2,
        max_tasks: 4,
        max_coordinator_rounds: 2,
        max_pending_messages: 1,
        ..CollaborationRuntimeConfig::default()
    };
    let tasks: Vec<_> = (0..4)
        .map(|index| {
            json!({
                "task_key": format!("task-{index}"),
                "role": "worker-a",
                "instruction": format!("work {index}")
            })
        })
        .collect();
    let scripted = HashMap::from([
        (
            "coordinator".to_owned(),
            VecDeque::from([
                json!({"decision":"dispatch", "tasks":tasks}).to_string(),
                json!({"decision":"finalize", "output":{"role":"bounded"}}).to_string(),
            ]),
        ),
        (
            "worker-a".to_owned(),
            VecDeque::from(
                (0..4)
                    .map(|_| json!({"role":"worker-a"}).to_string())
                    .collect::<Vec<_>>(),
            ),
        ),
    ]);
    let coordinator = v2_role("coordinator", &["Read"], PermissionCeiling::plan());
    let worker = v2_role("worker-a", &["Grep"], PermissionCeiling::plan());
    let (snapshot, runtime, root_run, state) = run_supervisor_workflow_with_permission_mode(
        "supervisor-concurrency",
        collaboration,
        vec![coordinator, worker],
        scripted,
        8,
        solaris_types::workflow::MultiAgentPolicy::Proactive,
        SupervisorWorkflowRuntimeOptions {
            concurrency_gate: Some(V2ProviderConcurrencyGate::new(&["worker-a"], 2, 4)),
            ..SupervisorWorkflowRuntimeOptions::default()
        },
    )
    .await;

    assert_eq!(snapshot.status, WorkflowRunStatus::Completed, "{snapshot:#?}");
    let state = state.lock().unwrap_or_else(|error| error.into_inner());
    assert_eq!(
        state.calls,
        [
            "coordinator",
            "worker-a",
            "worker-a",
            "worker-a",
            "worker-a",
            "coordinator"
        ]
    );
    assert_eq!(state.max_active, 2);
    assert_eq!(
        state.max_active_by_model.get("worker-a"),
        Some(&2),
        "a serial implementation reports 1 here and must fail this test; timeline: {:?}",
        state.timeline
    );
    drop(state);
    let records = runtime.ledger().records_for_run(&root_run).unwrap();
    let deliveries: Vec<_> = records
        .iter()
        .filter(|record| record.record_type == "workflow_supervisor_worker_delivery")
        .collect();
    assert_eq!(deliveries.len(), 1, "one dispatch must create one aggregate delivery");
    assert_eq!(
        deliveries[0]
            .payload
            .get("outcomes")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(4)
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| record.record_type == "message_delivered")
            .count(),
        0,
        "system results must not share the model-visible peer inbox"
    );
}

#[tokio::test]
async fn configured_supervisor_stores_large_worker_results_once_as_protected_blobs() {
    let collaboration = CollaborationRuntimeConfig {
        strategy: CollaborationStrategy::Supervisor,
        worker_roles: vec![WorkerRolePolicy {
            role: "worker-a".into(),
            max_concurrent: 1,
            max_total: 1,
        }],
        max_tasks: 1,
        max_coordinator_rounds: 2,
        max_pending_messages: 1,
        max_message_bytes: 2_048,
        ..CollaborationRuntimeConfig::default()
    };
    let marker = "SOLARIS_UNIQUE_SUPERVISOR_LARGE_OUTCOME";
    let large = format!("{marker}{}", "x".repeat(128 * 1_024));
    let scripted = HashMap::from([
        (
            "coordinator".to_owned(),
            VecDeque::from([
                json!({
                    "decision":"dispatch",
                    "tasks":[{"task_key":"large", "role":"worker-a", "instruction":"produce evidence"}]
                })
                .to_string(),
                json!({"decision":"finalize", "output":{"role":"coordinator"}}).to_string(),
            ]),
        ),
        (
            "worker-a".to_owned(),
            VecDeque::from([json!({"role":"worker-a", "evidence":large}).to_string()]),
        ),
    ]);
    let (snapshot, runtime, root_run, state) = run_supervisor_workflow(
        "supervisor-large-result",
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

    assert_eq!(snapshot.status, WorkflowRunStatus::Completed, "{snapshot:#?}");
    assert_eq!(
        state.lock().unwrap_or_else(|error| error.into_inner()).calls,
        ["coordinator", "worker-a", "coordinator"]
    );
    let records = runtime.ledger().records_for_run(&root_run).unwrap();
    let outcome = records
        .iter()
        .find(|record| record.record_type == "workflow_supervisor_worker_outcome")
        .unwrap();
    assert!(outcome.payload.get("result").is_none());
    assert!(outcome.payload.get("normalized_output").is_none());
    let output_ref = outcome.payload.get("result_ref").and_then(Value::as_str).unwrap();
    let agent_outcome = records
        .iter()
        .find(|record| record.record_type == "agent_outcome")
        .unwrap();
    assert_eq!(agent_outcome.payload["result"]["output"], Value::Null);
    assert_eq!(agent_outcome.payload["result"]["text"], "");
    assert_eq!(
        agent_outcome
            .payload
            .get("outcome_ref")
            .and_then(|reference| reference.get("reference"))
            .and_then(Value::as_str),
        Some(output_ref)
    );
    assert!(
        records
            .iter()
            .all(|record| !serde_json::to_string(&record.payload).unwrap().contains(marker)),
        "large worker body must not be copied into RuntimeLedger records"
    );
    let body = crate::execution_context::EffectOutputStore::for_run_with_ledger(&root_run, runtime.ledger().as_ref())
        .read(output_ref)
        .unwrap();
    assert!(body.len() > 128 * 1_024);
    assert_eq!(body.matches(marker).count(), 1);
    let delivery = records
        .iter()
        .find(|record| record.record_type == "workflow_supervisor_worker_delivery")
        .unwrap();
    assert_eq!(delivery.payload["outcomes"][0]["result_ref"].as_str(), Some(output_ref));
    assert_eq!(delivery.payload["outcomes"][0]["status"], "completed");
    assert_eq!(agent_outcome.payload["outcome_ref"]["status"], "completed");
    assert_eq!(
        runtime
            .tasks()
            .snapshot()
            .into_iter()
            .find(|task| task.task_key.as_deref() == Some("large"))
            .and_then(|task| task.outcome_ref),
        Some(output_ref.to_owned())
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| record.record_type == "workflow_supervisor_worker_outcome")
            .count(),
        1
    );
}

#[tokio::test]
async fn disabled_policy_rejects_configured_supervisor_before_provider_execution() {
    let collaboration = CollaborationRuntimeConfig {
        strategy: CollaborationStrategy::Supervisor,
        worker_roles: vec![WorkerRolePolicy {
            role: "worker-a".into(),
            max_concurrent: 1,
            max_total: 1,
        }],
        ..CollaborationRuntimeConfig::default()
    };
    let (snapshot, runtime, _, state) = run_supervisor_workflow(
        "supervisor-disabled",
        collaboration,
        vec![
            v2_role("coordinator", &["Read"], PermissionCeiling::plan()),
            v2_role("worker-a", &["Grep"], PermissionCeiling::plan()),
        ],
        HashMap::new(),
        2,
        solaris_types::workflow::MultiAgentPolicy::Disabled,
    )
    .await;

    assert_eq!(snapshot.status, WorkflowRunStatus::Failed);
    assert!(
        v2_node_attempt(&snapshot)
            .error
            .as_deref()
            .is_some_and(|error| error.contains("multi-agent policy is disabled"))
    );
    assert!(state.lock().unwrap_or_else(|error| error.into_inner()).calls.is_empty());
    assert!(v2_handles(&runtime, &RunId::from("v2-supervisor-disabled-root")).is_empty());
}

#[path = "workflow_executor_supervisor_cleanup_test.rs"]
mod workflow_executor_supervisor_cleanup_test;
