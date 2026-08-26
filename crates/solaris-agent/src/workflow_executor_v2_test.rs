use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
use std::time::{Duration, Instant};

use solaris_types::spawner::AgentHandle;
use solaris_types::workflow::WorkerRolePolicy;

#[derive(Default)]
struct V2ProviderState {
    active: usize,
    max_active: usize,
    active_by_model: HashMap<String, usize>,
    max_active_by_model: HashMap<String, usize>,
    calls: Vec<String>,
    requests: Vec<(String, String)>,
    scripted_responses: HashMap<String, VecDeque<String>>,
    timeline_origin: Option<Instant>,
    timeline: Vec<(String, u128)>,
}

impl V2ProviderState {
    fn record_timeline(&mut self, event: impl Into<String>) {
        let origin = *self.timeline_origin.get_or_insert_with(Instant::now);
        self.timeline.push((event.into(), origin.elapsed().as_millis()));
    }
}

struct V2TrackingProvider {
    state: Arc<Mutex<V2ProviderState>>,
    concurrency_gate: Option<V2ProviderConcurrencyGate>,
}

#[derive(Clone)]
struct V2ProviderConcurrencyGate {
    models: Arc<HashSet<String>>,
    barrier: Arc<tokio::sync::Barrier>,
    remaining_calls: Arc<AtomicUsize>,
}

impl V2ProviderConcurrencyGate {
    fn new(models: &[&str], concurrent_calls: usize, total_gated_calls: usize) -> Self {
        assert!(concurrent_calls > 1);
        assert!(total_gated_calls >= concurrent_calls);
        assert_eq!(total_gated_calls % concurrent_calls, 0);
        Self {
            models: Arc::new(models.iter().map(|model| (*model).to_owned()).collect()),
            barrier: Arc::new(tokio::sync::Barrier::new(concurrent_calls)),
            remaining_calls: Arc::new(AtomicUsize::new(total_gated_calls)),
        }
    }

    async fn wait_if_selected(&self, model: &str) {
        if !self.models.contains(model) {
            return;
        }
        let selected = self
            .remaining_calls
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| remaining.checked_sub(1))
            .is_ok();
        if selected {
            self.barrier.wait().await;
        }
    }
}

#[async_trait]
impl LlmProvider for V2TrackingProvider {
    async fn stream(&self, request: &LlmRequest) -> Result<mpsc::Receiver<LlmEvent>, ProviderError> {
        let (call_index, scripted_response) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.active += 1;
            state.record_timeline(format!("provider-start:{}", request.model));
            state.max_active = state.max_active.max(state.active);
            let active_for_model = state.active_by_model.entry(request.model.clone()).or_default();
            *active_for_model += 1;
            let active_for_model = *active_for_model;
            let max_for_model = state.max_active_by_model.entry(request.model.clone()).or_default();
            *max_for_model = (*max_for_model).max(active_for_model);
            let call_index = state.calls.iter().filter(|model| *model == &request.model).count() + 1;
            state.calls.push(request.model.clone());
            state
                .requests
                .push((request.model.clone(), serde_json::to_string(&request.messages).unwrap()));
            let scripted_response = state
                .scripted_responses
                .get_mut(&request.model)
                .and_then(VecDeque::pop_front);
            (call_index, scripted_response)
        };
        if let Some(gate) = &self.concurrency_gate {
            gate.wait_if_selected(&request.model).await;
        } else {
            tokio::task::yield_now().await;
        }
        {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.active -= 1;
            *state.active_by_model.get_mut(&request.model).unwrap() -= 1;
            state.record_timeline(format!("provider-finish:{}", request.model));
        }
        let (tx, rx) = mpsc::channel(4);
        let output = if let Some(output) = scripted_response {
            output
        } else if request.model == "invalid-worker" {
            "not-json".to_owned()
        } else if request.model == "reviewer" {
            format!(r#"{{"role":"reviewer-{call_index}"}}"#)
        } else {
            format!(r#"{{"role":"{}"}}"#, request.model)
        };
        tx.send(LlmEvent::TextDelta(output)).await.unwrap();
        tx.send(LlmEvent::Done {
            stop_reason: StopReason::EndTurn,
            usage: TokenUsage {
                input_tokens: 7,
                output_tokens: 3,
                cache_creation_tokens: 2,
                cache_read_tokens: 1,
            },
        })
        .await
        .unwrap();
        Ok(rx)
    }
}

fn v2_role(id: &str, tools: &[&str], permission_ceiling: PermissionCeiling) -> AgentRoleDefinition {
    AgentRoleDefinition {
        id: id.into(),
        description: format!("Configured role {id}"),
        input_schema: Some(json!({"type":"object"})),
        output_schema: Some(json!({"type":"object", "required":["role"]})),
        model_policy: ModelPolicy {
            model: Some(id.into()),
            reasoning_effort: Some("low".into()),
        },
        capability_scope: tools.iter().map(|tool| (*tool).to_owned()).collect(),
        permission_ceiling,
        context_policy: Some("isolated".into()),
        recursion_policy: Some("none".into()),
        budget: ResourceBudget {
            max_turns: Some(2),
            max_tokens: Some(128),
            max_wall_time_ms: None,
            ..ResourceBudget::default()
        },
    }
}

fn v2_handles(runtime: &CollaborationRuntime<()>, run_id: &RunId) -> Vec<AgentHandle> {
    runtime
        .ledger()
        .records_for_run(run_id)
        .unwrap()
        .into_iter()
        .filter(|record| record.record_type == "agent_handle_issued")
        .map(|record| serde_json::from_value(record.payload).unwrap())
        .collect()
}

fn v2_node_attempt(
    snapshot: &crate::workflow_controller::WorkflowRunSnapshot,
) -> &crate::workflow_controller::WorkflowNodeAttempt {
    snapshot.nodes.get("work").unwrap()
}

async fn run_v2_workflow(
    name: &str,
    node_role: Option<&str>,
    collaboration: CollaborationRuntimeConfig,
    roles_to_register: Vec<AgentRoleDefinition>,
) -> (
    crate::workflow_controller::WorkflowRunSnapshot,
    Arc<CollaborationRuntime<()>>,
    RunId,
    Arc<Mutex<V2ProviderState>>,
) {
    run_v2_workflow_with_parameters(name, node_role, collaboration, roles_to_register, json!({})).await
}

async fn run_v2_workflow_with_parameters(
    name: &str,
    node_role: Option<&str>,
    collaboration: CollaborationRuntimeConfig,
    roles_to_register: Vec<AgentRoleDefinition>,
    parameters: Value,
) -> (
    crate::workflow_controller::WorkflowRunSnapshot,
    Arc<CollaborationRuntime<()>>,
    RunId,
    Arc<Mutex<V2ProviderState>>,
) {
    let runtime = Arc::new(CollaborationRuntime::new(Scheduler::new(ResourcePolicy::new(16))));
    let root_run = RunId::new(format!("v2-{name}-root"));
    let root_agent = AgentId::new(format!("v2-{name}-agent"));
    runtime.agents().upsert(AgentRecord {
        run_id: root_run.clone(),
        agent_id: root_agent.clone(),
        team_id: None,
        parent_agent_id: None,
        state: AgentLifecycleState::Active,
    });
    let state = Arc::new(Mutex::new(V2ProviderState::default()));
    let spawner = Arc::new(
        AgentSpawner::new(
            Arc::new(V2TrackingProvider {
                state: Arc::clone(&state),
                concurrency_gate: None,
            }),
            test_config(),
            std::env::temp_dir(),
        )
        .with_runtime_context(Arc::clone(&runtime), root_run.clone(), root_agent),
    );
    let roles = Arc::new(AgentRoleRegistry::default());
    for role in roles_to_register {
        roles.register(role);
    }
    let controller = WorkflowController::with_runtime_and_roles(Arc::clone(&runtime), Some(Arc::clone(&roles)));
    let workflow_id = format!("v2-{name}");
    controller
        .register(WorkflowDefinition {
            id: workflow_id.clone(),
            schema_version: 2,
            version: "1".into(),
            description: "Configured collaboration execution".into(),
            roles: Vec::new(),
            parameters_schema: None,
            nodes: vec![WorkflowNode {
                id: "work".into(),
                depends_on: Vec::new(),
                when: None,
                role: node_role.map(str::to_owned),
                collaboration: CollaborationSelection::Configured(collaboration),
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
    controller
        .start(workflow_run.clone(), &workflow_id, parameters)
        .unwrap();
    let snapshot = controller
        .execute_until_settled(&workflow_run, Arc::new(AgentWorkflowExecutor::new(spawner, roles)))
        .await
        .unwrap();
    (snapshot, runtime, root_run, state)
}

async fn run_supervisor_workflow(
    name: &str,
    collaboration: CollaborationRuntimeConfig,
    roles_to_register: Vec<AgentRoleDefinition>,
    scripted_responses: HashMap<String, VecDeque<String>>,
    max_active: usize,
    policy: solaris_types::workflow::MultiAgentPolicy,
) -> (
    crate::workflow_controller::WorkflowRunSnapshot,
    Arc<CollaborationRuntime<()>>,
    RunId,
    Arc<Mutex<V2ProviderState>>,
) {
    run_supervisor_workflow_with_permission_mode(
        name,
        collaboration,
        roles_to_register,
        scripted_responses,
        max_active,
        policy,
        SupervisorWorkflowRuntimeOptions::default(),
    )
    .await
}

struct SupervisorWorkflowRuntimeOptions {
    permission_mode: solaris_types::permission::PermissionMode,
    ledger: Option<Arc<dyn RuntimeLedger>>,
    concurrency_gate: Option<V2ProviderConcurrencyGate>,
}

impl Default for SupervisorWorkflowRuntimeOptions {
    fn default() -> Self {
        Self {
            permission_mode: solaris_types::permission::PermissionMode::Auto,
            ledger: None,
            concurrency_gate: None,
        }
    }
}

async fn run_supervisor_workflow_with_permission_mode(
    name: &str,
    collaboration: CollaborationRuntimeConfig,
    roles_to_register: Vec<AgentRoleDefinition>,
    scripted_responses: HashMap<String, VecDeque<String>>,
    max_active: usize,
    policy: solaris_types::workflow::MultiAgentPolicy,
    runtime_options: SupervisorWorkflowRuntimeOptions,
) -> (
    crate::workflow_controller::WorkflowRunSnapshot,
    Arc<CollaborationRuntime<()>>,
    RunId,
    Arc<Mutex<V2ProviderState>>,
) {
    let SupervisorWorkflowRuntimeOptions {
        permission_mode,
        ledger,
        concurrency_gate,
    } = runtime_options;
    let session_dir = tempfile::tempdir().unwrap();
    let scheduler = Scheduler::new(ResourcePolicy::new(max_active));
    let runtime = Arc::new(match ledger {
        Some(ledger) => CollaborationRuntime::with_ledger(scheduler, ledger),
        None => CollaborationRuntime::new(scheduler),
    });
    let root_run = RunId::new(format!("v2-{name}-root"));
    let root_agent = AgentId::new(format!("v2-{name}-agent"));
    runtime.agents().upsert(AgentRecord {
        run_id: root_run.clone(),
        agent_id: root_agent.clone(),
        team_id: None,
        parent_agent_id: None,
        state: AgentLifecycleState::Active,
    });
    let state = Arc::new(Mutex::new(V2ProviderState {
        scripted_responses,
        ..V2ProviderState::default()
    }));
    let mut config = test_config();
    config.session.enabled = true;
    config.session.directory = session_dir.path().to_string_lossy().into_owned();
    let spawner = Arc::new(
        AgentSpawner::new(
            Arc::new(V2TrackingProvider {
                state: Arc::clone(&state),
                concurrency_gate,
            }),
            config,
            std::env::temp_dir(),
        )
        .with_permission_context(crate::permission_engine::PermissionContext::new(
            permission_mode,
            PermissionCeiling::unrestricted(),
        ))
        .with_runtime_context(Arc::clone(&runtime), root_run.clone(), root_agent),
    );
    *spawner
        .multi_agent_policy_state()
        .write()
        .unwrap_or_else(|error| error.into_inner()) = policy;
    let roles = Arc::new(AgentRoleRegistry::default());
    for role in roles_to_register {
        roles.register(role);
    }
    let controller = WorkflowController::with_runtime_and_roles(Arc::clone(&runtime), Some(Arc::clone(&roles)));
    let workflow_id = format!("v2-{name}");
    controller
        .register(WorkflowDefinition {
            id: workflow_id.clone(),
            schema_version: 2,
            version: "1".into(),
            description: "Configured Supervisor execution".into(),
            roles: Vec::new(),
            parameters_schema: None,
            nodes: vec![WorkflowNode {
                id: "work".into(),
                depends_on: Vec::new(),
                when: None,
                role: Some("coordinator".into()),
                collaboration: CollaborationSelection::Configured(collaboration),
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
    let executor: Arc<dyn WorkflowNodeExecutor> = Arc::new(AgentWorkflowExecutor::new(spawner, roles));
    state
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .record_timeline("workflow-start");
    let settled = tokio::time::timeout(
        Duration::from_secs(30),
        controller.execute_until_settled(&workflow_run, Arc::clone(&executor)),
    )
    .await;
    let snapshot = match settled {
        Ok(result) => result.unwrap(),
        Err(_) => {
            let timeline = state.lock().unwrap_or_else(|error| error.into_inner()).timeline.clone();
            panic!("configured Supervisor made no completion within 30 seconds; timeline: {timeline:?}");
        }
    };
    state
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .record_timeline("workflow-finish");
    drop(executor);
    state
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .record_timeline("executor-dropped");
    drop(session_dir);
    (snapshot, runtime, root_run, state)
}

#[tokio::test]
async fn configured_team_ignores_legacy_collaboration_worker_count() {
    let collaboration = CollaborationRuntimeConfig {
        strategy: CollaborationStrategy::Team,
        worker_roles: vec![
            WorkerRolePolicy {
                role: "worker-a".into(),
                max_concurrent: 1,
                max_total: 2,
            },
            WorkerRolePolicy {
                role: "worker-b".into(),
                max_concurrent: 1,
                max_total: 1,
            },
        ],
        max_tasks: 3,
        ..CollaborationRuntimeConfig::default()
    };
    let (snapshot, runtime, root_run, _) = run_v2_workflow_with_parameters(
        "configured-worker-count",
        Some("coordinator"),
        collaboration,
        vec![
            v2_role("coordinator", &["Read"], PermissionCeiling::plan()),
            v2_role("worker-a", &["Grep"], PermissionCeiling::plan()),
            v2_role("worker-b", &["Read"], PermissionCeiling::plan()),
        ],
        json!({"collaboration_workers": 8}),
    )
    .await;

    assert_eq!(snapshot.status, WorkflowRunStatus::Completed, "{snapshot:#?}");
    let handles = v2_handles(&runtime, &root_run);
    assert_eq!(handles.len(), 4);
    assert_eq!(handles.iter().filter(|handle| handle.role_key == "worker-a").count(), 2);
    assert_eq!(handles.iter().filter(|handle| handle.role_key == "worker-b").count(), 1);
}

#[tokio::test]
async fn configured_team_and_fanout_use_role_policies_and_bounded_concurrency() {
    for strategy in [CollaborationStrategy::Team, CollaborationStrategy::Fanout] {
        let name = strategy.as_str();
        let collaboration = CollaborationRuntimeConfig {
            strategy,
            worker_roles: vec![
                WorkerRolePolicy {
                    role: "worker-a".into(),
                    max_concurrent: 1,
                    max_total: 2,
                },
                WorkerRolePolicy {
                    role: "worker-b".into(),
                    max_concurrent: 1,
                    max_total: 1,
                },
            ],
            max_concurrent_workers: 2,
            max_tasks: 3,
            ..CollaborationRuntimeConfig::default()
        };
        let (snapshot, runtime, root_run, state) = run_v2_workflow(
            name,
            Some("coordinator"),
            collaboration,
            vec![
                v2_role("coordinator", &["Read"], PermissionCeiling::plan()),
                v2_role("worker-a", &["Grep"], PermissionCeiling::plan()),
                v2_role("worker-b", &["Read"], PermissionCeiling::unrestricted()),
            ],
        )
        .await;

        assert_eq!(snapshot.status, WorkflowRunStatus::Completed, "{snapshot:#?}");
        let handles = v2_handles(&runtime, &root_run);
        assert_eq!(handles.len(), 4);
        assert_eq!(handles.iter().filter(|handle| handle.role_key == "worker-a").count(), 2);
        assert_eq!(handles.iter().filter(|handle| handle.role_key == "worker-b").count(), 1);
        assert_eq!(
            handles.iter().filter(|handle| handle.role_key == "coordinator").count(),
            1
        );
        let worker_a = handles.iter().find(|handle| handle.role_key == "worker-a").unwrap();
        assert_eq!(worker_a.spec.overrides.model.as_deref(), Some("worker-a"));
        assert!(worker_a.spec.overrides.allowed_tools.iter().any(|tool| tool == "Grep"));
        assert_eq!(worker_a.spec.permission_ceiling, PermissionCeiling::plan());
        assert_eq!(worker_a.spec.resource_budget.max_turns, Some(2));
        assert_eq!(worker_a.spec.resource_budget.max_tokens, Some(128));
        let state = state.lock().unwrap_or_else(|error| error.into_inner());
        assert_eq!(state.max_active, 2);
        assert_eq!(state.max_active_by_model.get("worker-a"), Some(&1));
    }
}

#[tokio::test]
async fn configured_independent_reviewer_uses_distinct_primary_and_final_reviewer() {
    let collaboration = CollaborationRuntimeConfig {
        strategy: CollaborationStrategy::IndependentReviewer,
        worker_roles: vec![
            WorkerRolePolicy {
                role: "author".into(),
                max_concurrent: 1,
                max_total: 1,
            },
            WorkerRolePolicy {
                role: "reviewer".into(),
                max_concurrent: 1,
                max_total: 1,
            },
        ],
        primary_role: Some("author".into()),
        reviewer_role: Some("reviewer".into()),
        ..CollaborationRuntimeConfig::default()
    };
    let (snapshot, runtime, root_run, state) = run_v2_workflow(
        "reviewer",
        Some("reviewer"),
        collaboration,
        vec![
            v2_role("author", &["Read"], PermissionCeiling::plan()),
            v2_role("reviewer", &["Grep"], PermissionCeiling::unrestricted()),
        ],
    )
    .await;

    assert_eq!(snapshot.status, WorkflowRunStatus::Completed, "{snapshot:#?}");
    assert_eq!(v2_node_attempt(&snapshot).output, Some(json!({"role":"reviewer-1"})));
    let handles = v2_handles(&runtime, &root_run);
    assert_eq!(
        handles
            .iter()
            .map(|handle| handle.role_key.as_str())
            .collect::<Vec<_>>(),
        ["author", "reviewer"]
    );
    assert_eq!(
        state.lock().unwrap_or_else(|error| error.into_inner()).calls,
        ["author", "reviewer"]
    );
}

#[tokio::test]
async fn configured_independent_reviewer_runs_all_declared_instances_in_order() {
    let collaboration = CollaborationRuntimeConfig {
        strategy: CollaborationStrategy::IndependentReviewer,
        worker_roles: vec![
            WorkerRolePolicy {
                role: "author".into(),
                max_concurrent: 1,
                max_total: 2,
            },
            WorkerRolePolicy {
                role: "reviewer".into(),
                max_concurrent: 2,
                max_total: 3,
            },
        ],
        max_concurrent_workers: 2,
        max_tasks: 5,
        primary_role: Some("author".into()),
        reviewer_role: Some("reviewer".into()),
        ..CollaborationRuntimeConfig::default()
    };
    let (snapshot, runtime, root_run, state) = run_v2_workflow(
        "reviewer-multiple",
        Some("reviewer"),
        collaboration,
        vec![
            v2_role("author", &["Read"], PermissionCeiling::plan()),
            v2_role("reviewer", &["Grep"], PermissionCeiling::unrestricted()),
        ],
    )
    .await;

    assert_eq!(snapshot.status, WorkflowRunStatus::Completed, "{snapshot:#?}");
    assert_eq!(v2_node_attempt(&snapshot).output, Some(json!({"role":"reviewer-3"})));
    let handles = v2_handles(&runtime, &root_run);
    assert_eq!(handles.iter().filter(|handle| handle.role_key == "author").count(), 2);
    assert_eq!(handles.iter().filter(|handle| handle.role_key == "reviewer").count(), 3);
    let state = state.lock().unwrap_or_else(|error| error.into_inner());
    assert_eq!(state.calls, ["author", "author", "reviewer", "reviewer", "reviewer"]);
    assert_eq!(state.max_active, 2);
    assert_eq!(state.max_active_by_model.get("author"), Some(&1));
    assert_eq!(state.max_active_by_model.get("reviewer"), Some(&2));
    let reviewer_requests: Vec<_> = state
        .requests
        .iter()
        .filter(|(model, _)| model == "reviewer")
        .map(|(_, request)| request.as_str())
        .collect();
    assert!(
        reviewer_requests[..2]
            .iter()
            .all(|request| request.contains("preliminary review"))
    );
    assert!(reviewer_requests[2].contains("Preliminary reviews"));
}

#[tokio::test]
async fn configured_team_cancels_coordinator_when_worker_output_is_invalid() {
    let collaboration = CollaborationRuntimeConfig {
        strategy: CollaborationStrategy::Team,
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
        max_concurrent_workers: 2,
        max_tasks: 2,
        ..CollaborationRuntimeConfig::default()
    };
    let (snapshot, runtime, root_run, _) = run_v2_workflow(
        "team-invalid-worker",
        Some("coordinator"),
        collaboration,
        vec![
            v2_role("coordinator", &["Read"], PermissionCeiling::plan()),
            v2_role("invalid-worker", &["Grep"], PermissionCeiling::plan()),
            v2_role("worker-b", &["Read"], PermissionCeiling::plan()),
        ],
    )
    .await;

    assert_eq!(snapshot.status, WorkflowRunStatus::Failed);
    assert!(
        v2_node_attempt(&snapshot)
            .error
            .as_deref()
            .is_some_and(|error| error.contains("invalid-worker did not return valid JSON"))
    );
    let coordinator = v2_handles(&runtime, &root_run)
        .into_iter()
        .find(|handle| handle.role_key == "coordinator")
        .unwrap();
    assert_eq!(
        runtime.agents().get(&coordinator.agent_id).unwrap().state,
        AgentLifecycleState::Cancelled
    );
    assert_eq!(
        runtime.tasks().get(&coordinator.task_id).unwrap().state,
        TaskState::Failed
    );
}

#[tokio::test]
async fn configured_team_message_limit_failure_leaves_only_terminal_children_and_tasks() {
    let collaboration = CollaborationRuntimeConfig {
        strategy: CollaborationStrategy::Team,
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
        max_pending_messages: 1,
        ..CollaborationRuntimeConfig::default()
    };
    let (snapshot, runtime, root_run, _) = run_v2_workflow(
        "team-message-limit",
        Some("coordinator"),
        collaboration,
        vec![
            v2_role("coordinator", &["Read"], PermissionCeiling::plan()),
            v2_role("worker-a", &["Grep"], PermissionCeiling::plan()),
            v2_role("worker-b", &["Read"], PermissionCeiling::plan()),
        ],
    )
    .await;

    assert_eq!(snapshot.status, WorkflowRunStatus::Failed);
    assert!(
        v2_node_attempt(&snapshot)
            .error
            .as_deref()
            .is_some_and(|error| error.contains("max_pending_messages"))
    );
    let handles = v2_handles(&runtime, &root_run);
    let coordinator = handles.iter().find(|handle| handle.role_key == "coordinator").unwrap();
    assert_eq!(
        runtime.agents().get(&coordinator.agent_id).unwrap().state,
        AgentLifecycleState::Cancelled
    );
    for handle in &handles {
        assert!(matches!(
            runtime.agents().get(&handle.agent_id).unwrap().state,
            AgentLifecycleState::Completed | AgentLifecycleState::Failed | AgentLifecycleState::Cancelled
        ));
    }
    assert!(
        runtime
            .tasks()
            .snapshot()
            .iter()
            .all(|task| !matches!(task.state, TaskState::Assigned | TaskState::Running))
    );
    let team = runtime.teams().snapshot().into_iter().next().unwrap();
    assert_eq!(team.max_pending_messages, 1);
    assert_eq!(
        team.max_message_bytes,
        CollaborationRuntimeConfig::DEFAULT_MAX_MESSAGE_BYTES
    );
    assert_eq!(
        runtime
            .ledger()
            .records_for_run(&root_run)
            .unwrap()
            .iter()
            .filter(|record| record.record_type == "message_delivered")
            .count(),
        1
    );

    let restored: CollaborationRuntime<()> =
        CollaborationRuntime::with_ledger(Scheduler::new(ResourcePolicy::new(16)), runtime.ledger());
    restored.restore_projection(&root_run).unwrap();
    let restored_team = restored.teams().snapshot().into_iter().next().unwrap();
    assert_eq!(restored_team.max_pending_messages, 1);
    assert_eq!(
        restored_team.max_message_bytes,
        CollaborationRuntimeConfig::DEFAULT_MAX_MESSAGE_BYTES
    );
}

#[path = "workflow_executor_v2_supervisor_test.rs"]
mod workflow_executor_v2_supervisor_test;
