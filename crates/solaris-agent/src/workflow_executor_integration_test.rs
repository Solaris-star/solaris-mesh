use super::*;

#[tokio::test]
async fn workflow_invalid_role_output_is_non_convergent_and_not_retried() {
    let runtime = Arc::new(CollaborationRuntime::new(Scheduler::new(ResourcePolicy::new(1))));
    let root_run = RunId::from("workflow-retry-root");
    let root_agent = AgentId::from("workflow-retry-agent");
    runtime.agents().upsert(AgentRecord {
        run_id: root_run.clone(),
        agent_id: root_agent.clone(),
        team_id: None,
        parent_agent_id: None,
        state: AgentLifecycleState::Active,
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let spawner = Arc::new(
        AgentSpawner::new(
            Arc::new(InvalidJsonThenValidProvider {
                calls: Arc::clone(&calls),
            }),
            test_config(),
            std::env::temp_dir(),
        )
        .with_runtime_context(Arc::clone(&runtime), root_run.clone(), root_agent),
    );
    let roles = Arc::new(AgentRoleRegistry::default());
    roles.register(test_role());
    let controller = WorkflowController::with_runtime_and_roles(Arc::clone(&runtime), Some(Arc::clone(&roles)));
    controller
        .register(WorkflowDefinition {
            id: "retry-invalid-output".into(),
            schema_version: 1,
            version: "1".into(),
            description: "Retry invalid role output".into(),
            roles: Vec::new(),
            parameters_schema: None,
            nodes: vec![WorkflowNode {
                id: "work".into(),
                depends_on: Vec::new(),
                when: None,
                role: Some("worker".into()),
                collaboration: CollaborationSelection::Fixed(CollaborationStrategy::Single),
                model_policy: ModelPolicy::default(),
                capability_scope: Vec::new(),
                permission_ceiling: PermissionCeiling::plan(),
                retry: RetryPolicy { max_attempts: 2 },
                timeout_ms: None,
                output_bindings: Vec::new(),
                workflow_ref: None,
            }],
            outputs: Default::default(),
        })
        .unwrap();
    let workflow_run = RunId::from("workflow-retry-root:workflow:one");
    controller
        .start(workflow_run.clone(), "retry-invalid-output", json!({}))
        .unwrap();

    let settled = controller
        .execute_until_settled(&workflow_run, Arc::new(AgentWorkflowExecutor::new(spawner, roles)))
        .await
        .unwrap();

    assert_eq!(settled.status, WorkflowRunStatus::Failed);
    assert_eq!(settled.nodes["work"].attempt_number, 1);
    assert_eq!(
        settled.nodes["work"].failure_class,
        Some(TaskFailureClass::NonConvergent)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        runtime
            .ledger()
            .records_for_run(&root_run)
            .unwrap()
            .iter()
            .filter(|record| record.record_type == "agent_handle_issued")
            .count(),
        1
    );
}

#[tokio::test]
async fn every_collaboration_strategy_executes_its_declared_topology_once() {
    let cases = [
        (CollaborationStrategy::Single, 1usize, 0usize),
        (CollaborationStrategy::Supervisor, 3, 3),
        (CollaborationStrategy::Team, 3, 3),
        (CollaborationStrategy::Fanout, 3, 4),
        (CollaborationStrategy::IndependentReviewer, 2, 3),
    ];
    for (strategy, expected_calls, expected_members) in cases {
        let runtime = Arc::new(CollaborationRuntime::new(Scheduler::new(ResourcePolicy::new(8))));
        let strategy_name = format!("{strategy:?}").to_ascii_lowercase();
        let root_run = RunId::new(format!("strategy-{strategy_name}"));
        let root_agent = AgentId::new(format!("root-{strategy_name}"));
        runtime.agents().upsert(AgentRecord {
            run_id: root_run.clone(),
            agent_id: root_agent.clone(),
            team_id: None,
            parent_agent_id: None,
            state: AgentLifecycleState::Active,
        });
        let calls = Arc::new(AtomicUsize::new(0));
        let systems = Arc::new(Mutex::new(Vec::new()));
        let resources = crate::resource_manager::ResourceManager::new(ResourceBudget {
            max_active_agents: Some(1),
            ..ResourceBudget::default()
        });
        let spawner = Arc::new(
            AgentSpawner::new(
                Arc::new(CountingJsonProvider {
                    calls: Arc::clone(&calls),
                    systems: Arc::clone(&systems),
                    runtime: Arc::clone(&runtime),
                    expected_ready_members: matches!(
                        strategy,
                        CollaborationStrategy::Supervisor | CollaborationStrategy::Team
                    )
                    .then_some(expected_members),
                }),
                test_config(),
                std::env::temp_dir(),
            )
            .with_resource_manager(resources)
            .with_runtime_context(Arc::clone(&runtime), root_run.clone(), root_agent),
        );
        let roles = Arc::new(AgentRoleRegistry::default());
        roles.register(test_role());
        let controller = WorkflowController::with_runtime_and_roles(Arc::clone(&runtime), Some(Arc::clone(&roles)));
        let workflow_id = format!("workflow-{strategy_name}");
        controller
            .register(WorkflowDefinition {
                id: workflow_id.clone(),
                schema_version: 1,
                version: "1".into(),
                description: "Collaboration topology integration".into(),
                roles: Vec::new(),
                parameters_schema: None,
                nodes: vec![WorkflowNode {
                    id: "work".into(),
                    depends_on: Vec::new(),
                    when: None,
                    role: Some("worker".into()),
                    collaboration: CollaborationSelection::Fixed(strategy),
                    model_policy: ModelPolicy::default(),
                    capability_scope: Vec::new(),
                    permission_ceiling: PermissionCeiling::unrestricted(),
                    retry: RetryPolicy { max_attempts: 1 },
                    timeout_ms: None,
                    output_bindings: Vec::new(),
                    workflow_ref: None,
                }],
                outputs: Default::default(),
            })
            .unwrap();
        let workflow_run = RunId::new(format!("{root_run}:workflow:one"));
        controller.start(workflow_run.clone(), &workflow_id, json!({})).unwrap();
        let settled = controller
            .execute_until_settled(&workflow_run, Arc::new(AgentWorkflowExecutor::new(spawner, roles)))
            .await
            .unwrap();
        assert_eq!(settled.status, WorkflowRunStatus::Completed, "strategy {strategy:?}");
        assert_eq!(calls.load(Ordering::SeqCst), expected_calls, "strategy {strategy:?}");
        if matches!(
            strategy,
            CollaborationStrategy::Supervisor | CollaborationStrategy::Team
        ) {
            let systems = systems.lock().unwrap_or_else(|error| error.into_inner());
            assert!(
                systems.last().is_some_and(|system| system.contains("worker_result")),
                "final coordinator must receive every durable worker result"
            );
            assert_eq!(
                runtime
                    .projection()
                    .messages
                    .iter()
                    .filter(|message| message.kind == "worker_result")
                    .count(),
                2
            );
        }
        if strategy == CollaborationStrategy::Single {
            assert!(runtime.teams().snapshot().is_empty());
        } else {
            let teams = runtime.teams().snapshot();
            assert_eq!(teams.len(), 1, "strategy {strategy:?}");
            assert_eq!(teams[0].members.len(), expected_members, "strategy {strategy:?}");
            assert!(
                teams[0].team_id.as_str().contains("work") && teams[0].team_id.as_str().contains("attempt-"),
                "team identity must include node and attempt"
            );
        }
    }
}
