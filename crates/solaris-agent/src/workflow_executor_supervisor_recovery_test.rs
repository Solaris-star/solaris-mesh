use super::*;

enum SupervisorRecoveryFault {
    TaskCreate { task_key: String },
    DeliveryAck,
    DeliveryAckAtDecision { decision_round: u32 },
}

#[derive(Clone, Copy)]
enum SupervisorTamper {
    DecisionInputIds,
    DecisionInputDeliveryDigest,
    WorkerRoleWithRecomputedDigest,
    AckBeforeDelivery,
    AckDeliveryDigestWithRecomputedAck,
    AckCrossRoundWithRecomputedAck,
    DeliveryOutcomesAcrossRoundsWithRecomputedDigests,
    FinalOutputRunId,
    ReuseAckedDelivery,
    OmitPendingDelivery,
    DeliveryAtDispatchSequence,
    DeleteCoordinatorTurnOutcome,
    TamperCoordinatorTurnOutcome,
    CrossRunCoordinatorTurnOutcome,
    CoordinatorTurnOutcomeAfterDecision,
    CausalSequence {
        edge: causality_recovery::SupervisorCausalEdge,
        mutation: causality_recovery::SupervisorSequenceMutation,
    },
}

#[path = "workflow_executor_supervisor_causality_recovery_test.rs"]
mod causality_recovery;
#[path = "workflow_executor_supervisor_exact_once_recovery_test.rs"]
mod exact_once_recovery;

struct SupervisorRecoveryLedger {
    inner: InMemoryRuntimeLedger,
    fault: SupervisorRecoveryFault,
    armed: AtomicBool,
    tamper: Mutex<Option<SupervisorTamper>>,
}

impl SupervisorRecoveryLedger {
    fn new(fault: SupervisorRecoveryFault) -> Self {
        Self {
            inner: InMemoryRuntimeLedger::default(),
            fault,
            armed: AtomicBool::new(true),
            tamper: Mutex::new(None),
        }
    }

    fn arm_tamper(&self, tamper: SupervisorTamper) {
        *self.tamper.lock().unwrap_or_else(|error| error.into_inner()) = Some(tamper);
    }

    fn matches_fault(&self, record_type: &str, payload: &Value) -> bool {
        match &self.fault {
            SupervisorRecoveryFault::TaskCreate { task_key } => {
                record_type == "task_created" && payload.get("task_key").and_then(Value::as_str) == Some(task_key)
            }
            SupervisorRecoveryFault::DeliveryAck => record_type == "workflow_supervisor_worker_delivery_ack",
            SupervisorRecoveryFault::DeliveryAckAtDecision { decision_round } => {
                record_type == "workflow_supervisor_worker_delivery_ack"
                    && payload.get("decision_round").and_then(Value::as_u64) == Some(u64::from(*decision_round))
            }
        }
    }
}

impl RuntimeLedger for SupervisorRecoveryLedger {
    crate::runtime_ledger::forward_workflow_mutation_lease!();

    fn logical_append_capability(&self) -> crate::runtime_ledger::LogicalAppendCapability {
        self.inner.logical_append_capability()
    }

    fn admit_collaboration_tasks(
        &self,
        run_id: &RunId,
        max_tasks: usize,
        tasks: &[solaris_types::runtime::TaskRecord],
    ) -> std::io::Result<Vec<LedgerRecord>> {
        if let SupervisorRecoveryFault::TaskCreate { task_key } = &self.fault
            && tasks
                .iter()
                .any(|task| task.task_key.as_deref() == Some(task_key.as_str()))
            && self.armed.swap(false, Ordering::SeqCst)
        {
            return Err(std::io::Error::other(format!(
                "injected Supervisor failure before task_created for {task_key}"
            )));
        }
        self.inner.admit_collaboration_tasks(run_id, max_tasks, tasks)
    }

    fn compare_and_append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        identity_fields: &[&str],
        payload: Value,
    ) -> std::io::Result<LedgerRecord> {
        if self.matches_fault(record_type, &payload) && self.armed.swap(false, Ordering::SeqCst) {
            return Err(std::io::Error::other(format!(
                "injected Supervisor failure before {record_type}"
            )));
        }
        self.inner
            .compare_and_append(run_id, durability, record_type, identity_fields, payload)
    }

    fn append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        payload: Value,
    ) -> std::io::Result<LedgerRecord> {
        if self.matches_fault(record_type, &payload) && self.armed.swap(false, Ordering::SeqCst) {
            return Err(std::io::Error::other(format!(
                "injected Supervisor failure before {record_type}"
            )));
        }
        self.inner.append(run_id, durability, record_type, payload)
    }

    fn run_ids(&self) -> std::io::Result<Vec<RunId>> {
        self.inner.run_ids()
    }

    fn records_for_run(&self, run_id: &RunId) -> std::io::Result<Vec<LedgerRecord>> {
        let mut records = self.inner.records_for_run(run_id)?;
        match *self.tamper.lock().unwrap_or_else(|error| error.into_inner()) {
            Some(SupervisorTamper::DecisionInputIds) => {
                if let Some(record) = records
                    .iter_mut()
                    .find(|record| record.record_type == "workflow_supervisor_decision")
                {
                    record.payload["input_message_ids"] = json!(["forged-message"]);
                }
            }
            Some(SupervisorTamper::DecisionInputDeliveryDigest) => {
                let decision = records
                    .iter_mut()
                    .find(|record| {
                        record.record_type == "workflow_supervisor_decision"
                            && record.payload["input_deliveries"]
                                .as_array()
                                .is_some_and(|inputs| !inputs.is_empty())
                    })
                    .unwrap();
                decision.payload["input_deliveries"][0]["delivery_digest"] = json!("forged-delivery-digest");
            }
            Some(SupervisorTamper::WorkerRoleWithRecomputedDigest) => {
                if let Some(record) = records
                    .iter_mut()
                    .find(|record| record.record_type == "workflow_supervisor_worker_outcome")
                {
                    record.payload["role"] = json!("forged-role");
                    record.payload["outcome_digest"] = json!(crate::execution_context::stable_digest_value(&json!({
                        "task_key": record.payload["task_key"],
                        "task_id": record.payload["task_id"],
                        "role": record.payload["role"],
                        "proposal_digest": record.payload["proposal_digest"],
                        "agent_id": record.payload["agent_id"],
                        "operation_id": record.payload["operation_id"],
                        "handle_spec_digest": record.payload["handle_spec_digest"],
                        "result_ref": record.payload["result_ref"],
                        "result_bytes": record.payload["result_bytes"],
                        "result_digest": record.payload["result_digest"],
                        "result_format": record.payload["result_format"],
                        "status": record.payload["status"],
                        "failure_class": record.payload["failure_class"],
                    })));
                }
            }
            Some(SupervisorTamper::AckBeforeDelivery) => {
                let delivery_seq = records
                    .iter()
                    .find(|record| record.record_type == "workflow_supervisor_worker_delivery")
                    .map(|record| record.seq)
                    .unwrap();
                let ack = records
                    .iter_mut()
                    .find(|record| record.record_type == "workflow_supervisor_worker_delivery_ack")
                    .unwrap();
                ack.seq = delivery_seq.saturating_sub(1);
            }
            Some(SupervisorTamper::AckDeliveryDigestWithRecomputedAck) => {
                let ack = records
                    .iter_mut()
                    .find(|record| record.record_type == "workflow_supervisor_worker_delivery_ack")
                    .unwrap();
                ack.payload["delivery_digest"] = json!("forged-delivery-digest");
                ack.payload["ack_digest"] = json!(supervisor_test_ack_digest(&ack.payload));
            }
            Some(SupervisorTamper::AckCrossRoundWithRecomputedAck) => {
                let ack = records
                    .iter_mut()
                    .find(|record| record.record_type == "workflow_supervisor_worker_delivery_ack")
                    .unwrap();
                ack.payload["decision_round"] = json!(0);
                ack.payload["ack_digest"] = json!(supervisor_test_ack_digest(&ack.payload));
            }
            Some(SupervisorTamper::DeliveryOutcomesAcrossRoundsWithRecomputedDigests) => {
                let delivery_indexes: Vec<_> = records
                    .iter()
                    .enumerate()
                    .filter(|(_, record)| record.record_type == "workflow_supervisor_worker_delivery")
                    .map(|(index, _)| index)
                    .collect();
                assert_eq!(delivery_indexes.len(), 2);
                let first_outcomes = records[delivery_indexes[0]].payload["outcomes"].clone();
                let second_outcomes = records[delivery_indexes[1]].payload["outcomes"].clone();
                records[delivery_indexes[0]].payload["outcomes"] = second_outcomes;
                records[delivery_indexes[1]].payload["outcomes"] = first_outcomes;
                for index in &delivery_indexes {
                    let digest = supervisor_test_delivery_digest(&records[*index].payload);
                    records[*index].payload["delivery_digest"] = json!(digest);
                }
                for ack_index in 0..records.len() {
                    if records[ack_index].record_type != "workflow_supervisor_worker_delivery_ack" {
                        continue;
                    }
                    let message_id = records[ack_index].payload["message_id"].as_str().unwrap();
                    let delivery_digest = delivery_indexes
                        .iter()
                        .find_map(|index| {
                            (records[*index].payload["message_id"].as_str() == Some(message_id))
                                .then(|| records[*index].payload["delivery_digest"].clone())
                        })
                        .unwrap();
                    records[ack_index].payload["delivery_digest"] = delivery_digest;
                    records[ack_index].payload["ack_digest"] =
                        json!(supervisor_test_ack_digest(&records[ack_index].payload));
                }
            }
            Some(SupervisorTamper::FinalOutputRunId) => {
                let decision = records
                    .iter_mut()
                    .find(|record| {
                        record.record_type == "workflow_supervisor_decision"
                            && record
                                .payload
                                .get("final_output_ref")
                                .is_some_and(|value| !value.is_null())
                    })
                    .unwrap();
                decision.payload["final_output_ref"]["run_id"] = json!("forged-workflow-run");
            }
            Some(SupervisorTamper::DeleteCoordinatorTurnOutcome) => {
                records.retain(|record| !is_final_coordinator_turn_outcome(record));
            }
            Some(SupervisorTamper::TamperCoordinatorTurnOutcome) => {
                let outcome = records
                    .iter_mut()
                    .find(|record| is_final_coordinator_turn_outcome(record))
                    .unwrap();
                outcome.payload["operation_id"] = json!("forged-turn-operation");
            }
            Some(SupervisorTamper::CrossRunCoordinatorTurnOutcome) => {
                let outcome = records
                    .iter_mut()
                    .find(|record| is_final_coordinator_turn_outcome(record))
                    .unwrap();
                outcome.payload["run_id"] = json!("forged-root-run");
            }
            Some(SupervisorTamper::CoordinatorTurnOutcomeAfterDecision) => {
                let next_sequence = records.iter().map(|record| record.seq).max().unwrap().saturating_add(1);
                records
                    .iter_mut()
                    .find(|record| is_final_coordinator_turn_outcome(record))
                    .unwrap()
                    .seq = next_sequence;
            }
            Some(
                tamper @ (SupervisorTamper::ReuseAckedDelivery
                | SupervisorTamper::OmitPendingDelivery
                | SupervisorTamper::DeliveryAtDispatchSequence),
            ) => {
                exact_once_recovery::tamper_exact_once_decision(&mut records, tamper);
            }
            Some(SupervisorTamper::CausalSequence { edge, mutation }) => {
                causality_recovery::tamper_supervisor_causal_sequence(&mut records, edge, mutation);
            }
            None => {}
        }
        Ok(records)
    }

    fn effect_output_root(&self) -> Option<std::path::PathBuf> {
        self.inner.effect_output_root()
    }
}

fn is_final_coordinator_turn_outcome(record: &LedgerRecord) -> bool {
    record.record_type == "agent_conversation_turn_outcome"
        && record
            .payload
            .get("turn_id")
            .and_then(Value::as_str)
            .is_some_and(|turn_id| turn_id.ends_with(":round:2"))
}

fn supervisor_test_ack_digest(payload: &Value) -> String {
    crate::execution_context::stable_digest_value(&json!({
        "schema_version": payload["schema_version"],
        "workflow_run_id": payload["workflow_run_id"],
        "workflow_id": payload["workflow_id"],
        "node_id": payload["node_id"],
        "attempt_id": payload["attempt_id"],
        "message_id": payload["message_id"],
        "delivery_digest": payload["delivery_digest"],
        "dispatch_round": payload["dispatch_round"],
        "coordinator_agent_id": payload["coordinator_agent_id"],
        "decision_round": payload["decision_round"],
        "decision_digest": payload["decision_digest"],
    }))
}

fn supervisor_test_delivery_digest(payload: &Value) -> String {
    crate::execution_context::stable_digest_value(&json!({
        "schema_version": payload["schema_version"],
        "workflow_run_id": payload["workflow_run_id"],
        "workflow_id": payload["workflow_id"],
        "node_id": payload["node_id"],
        "attempt_id": payload["attempt_id"],
        "dispatch_round": payload["dispatch_round"],
        "dispatch_decision_digest": payload["dispatch_decision_digest"],
        "message_id": payload["message_id"],
        "sender": payload["sender"],
        "coordinator_agent_id": payload["coordinator_agent_id"],
        "outcomes": payload["outcomes"],
    }))
}

struct SupervisorRecoveryFixture {
    _session_dir: tempfile::TempDir,
    executor: Arc<AgentWorkflowExecutor>,
    context: WorkflowExecutionContext,
    runtime: Arc<CollaborationRuntime<()>>,
    root_run: RunId,
    provider_state: Arc<Mutex<V2ProviderState>>,
}

fn supervisor_recovery_fixture<L: RuntimeLedger + 'static>(
    name: &str,
    ledger: Arc<L>,
    tasks: Vec<Value>,
) -> SupervisorRecoveryFixture {
    let session_dir = tempfile::tempdir().unwrap();
    let runtime_ledger: Arc<dyn RuntimeLedger> = ledger;
    let runtime = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(2)),
        runtime_ledger,
    ));
    let root_run = RunId::new(format!("supervisor-recovery-{name}-root"));
    let workflow_run = RunId::new(format!("{root_run}:workflow:one"));
    let root_agent = AgentId::new(format!("supervisor-recovery-{name}-agent"));
    runtime.agents().upsert(AgentRecord {
        run_id: root_run.clone(),
        agent_id: root_agent.clone(),
        team_id: None,
        parent_agent_id: None,
        state: AgentLifecycleState::Active,
    });
    let provider_state = Arc::new(Mutex::new(V2ProviderState {
        scripted_responses: HashMap::from([
            (
                "coordinator".to_owned(),
                VecDeque::from([
                    json!({"decision":"dispatch", "tasks":tasks}).to_string(),
                    json!({"decision":"finalize", "output":{"role":"recovered"}}).to_string(),
                ]),
            ),
            (
                "worker-a".to_owned(),
                VecDeque::from([
                    json!({"role":"worker-a"}).to_string(),
                    json!({"role":"worker-a"}).to_string(),
                ]),
            ),
        ]),
        ..V2ProviderState::default()
    }));
    let mut config = test_config();
    config.session.enabled = true;
    config.session.directory = session_dir.path().to_string_lossy().into_owned();
    let spawner = Arc::new(
        AgentSpawner::new(
            Arc::new(V2TrackingProvider {
                state: Arc::clone(&provider_state),
                concurrency_gate: None,
            }),
            config,
            std::env::temp_dir(),
        )
        .with_runtime_context(Arc::clone(&runtime), root_run.clone(), root_agent),
    );
    *spawner
        .multi_agent_policy_state()
        .write()
        .unwrap_or_else(|error| error.into_inner()) = solaris_types::workflow::MultiAgentPolicy::OnDemand;
    let roles = Arc::new(AgentRoleRegistry::default());
    for role_id in ["coordinator", "worker-a"] {
        let mut role = v2_role(role_id, &["Read"], PermissionCeiling::plan());
        role.budget.max_wall_time_ms = None;
        if role_id == "coordinator" {
            role.budget.max_turns = Some(4);
        }
        roles.register(role);
    }
    let collaboration = CollaborationRuntimeConfig {
        strategy: CollaborationStrategy::Supervisor,
        worker_roles: vec![WorkerRolePolicy {
            role: "worker-a".into(),
            max_concurrent: 2,
            max_total: 2,
        }],
        max_concurrent_workers: 2,
        max_tasks: 2,
        max_coordinator_rounds: 3,
        ..CollaborationRuntimeConfig::default()
    };
    let workflow_id = format!("supervisor-recovery-{name}");
    let node = WorkflowNode {
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
    };
    let controller = WorkflowController::with_runtime_and_roles(Arc::clone(&runtime), Some(Arc::clone(&roles)));
    controller
        .register(WorkflowDefinition {
            id: workflow_id.clone(),
            schema_version: 2,
            version: "1".into(),
            description: "Configured Supervisor recovery".into(),
            roles: Vec::new(),
            parameters_schema: None,
            nodes: vec![node.clone()],
            outputs: Default::default(),
        })
        .unwrap();
    controller.start(workflow_run.clone(), &workflow_id, json!({})).unwrap();
    SupervisorRecoveryFixture {
        _session_dir: session_dir,
        executor: Arc::new(AgentWorkflowExecutor::new(spawner, roles)),
        context: WorkflowExecutionContext {
            run_id: workflow_run,
            workflow: solaris_types::plugin::ImplementationIdentity {
                implementation_id: format!("workflow:{workflow_id}"),
                version: Some("1".into()),
                digest: None,
            },
            node,
            attempt_id: solaris_types::identity::AttemptId::new(format!("supervisor-recovery-{name}-attempt")),
            parameters: json!({}),
            dependency_outputs: Default::default(),
            bound_inputs: json!({}),
        },
        runtime,
        root_run,
        provider_state,
    }
}

fn supervisor_provider_calls(fixture: &SupervisorRecoveryFixture) -> Vec<String> {
    fixture
        .provider_state
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .calls
        .clone()
}

#[tokio::test]
async fn supervisor_recovery_reuses_decision_written_before_first_task_creation() {
    let ledger = Arc::new(SupervisorRecoveryLedger::new(SupervisorRecoveryFault::TaskCreate {
        task_key: "first".into(),
    }));
    let fixture = supervisor_recovery_fixture(
        "decision-before-task",
        Arc::clone(&ledger),
        vec![json!({
            "task_key":"first",
            "role":"worker-a",
            "instruction":"inspect"
        })],
    );

    let first = fixture.executor.execute(fixture.context.clone()).await.unwrap_err();
    assert_eq!(first.failure_class, TaskFailureClass::ReconciliationRequired);
    assert_eq!(supervisor_provider_calls(&fixture), ["coordinator"]);
    assert_eq!(
        ledger
            .records_for_run(&fixture.root_run)
            .unwrap()
            .iter()
            .filter(|record| record.record_type == "workflow_supervisor_decision")
            .count(),
        1
    );
    assert!(
        fixture
            .runtime
            .tasks()
            .snapshot()
            .iter()
            .all(|task| task.team_id.is_none())
    );

    let recovered = AgentWorkflowExecutor::new(
        Arc::clone(&fixture.executor.spawner),
        Arc::clone(&fixture.executor.roles),
    );
    let output = recovered.execute(fixture.context.clone()).await.unwrap();
    assert_eq!(output, json!({"role":"recovered"}));
    assert_eq!(
        supervisor_provider_calls(&fixture),
        ["coordinator", "worker-a", "coordinator"]
    );
    assert_eq!(
        recovered.usage().0,
        3,
        "cold recovery must count the durable coordinator turn once"
    );
    assert_eq!(recovered.usage().1.input_tokens, 21);
    assert_eq!(recovered.usage().1.output_tokens, 9);
    assert_eq!(recovered.usage().1.cache_creation_tokens, 6);
    assert_eq!(recovered.usage().1.cache_read_tokens, 3);
}

#[tokio::test]
async fn supervisor_recovery_completes_a_partially_created_task_batch_without_duplicates() {
    let ledger = Arc::new(SupervisorRecoveryLedger::new(SupervisorRecoveryFault::TaskCreate {
        task_key: "second".into(),
    }));
    let fixture = supervisor_recovery_fixture(
        "partial-task-batch",
        Arc::clone(&ledger),
        vec![
            json!({"task_key":"first", "role":"worker-a", "instruction":"first"}),
            json!({"task_key":"second", "role":"worker-a", "instruction":"second"}),
        ],
    );

    let first = fixture.executor.execute(fixture.context.clone()).await.unwrap_err();
    assert_eq!(first.failure_class, TaskFailureClass::ReconciliationRequired);
    assert_eq!(supervisor_provider_calls(&fixture), ["coordinator"]);
    let created: Vec<_> = fixture
        .runtime
        .tasks()
        .snapshot()
        .into_iter()
        .filter(|task| task.team_id.is_some())
        .collect();
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].task_key.as_deref(), Some("first"));

    fixture.executor.execute(fixture.context.clone()).await.unwrap();
    assert_eq!(
        supervisor_provider_calls(&fixture),
        ["coordinator", "worker-a", "worker-a", "coordinator"]
    );
    let records = ledger.records_for_run(&fixture.root_run).unwrap();
    for task_key in ["first", "second"] {
        assert_eq!(
            records
                .iter()
                .filter(|record| {
                    record.record_type == "task_created"
                        && record.payload.get("task_key").and_then(Value::as_str) == Some(task_key)
                })
                .count(),
            1,
            "Task {task_key} must be created exactly once"
        );
    }
}

#[tokio::test]
async fn supervisor_recovery_reuses_worker_outcome_and_coordinator_decision_when_ack_failed() {
    let ledger = Arc::new(SupervisorRecoveryLedger::new(SupervisorRecoveryFault::DeliveryAck));
    let fixture = supervisor_recovery_fixture(
        "worker-outcome-before-ack",
        Arc::clone(&ledger),
        vec![json!({
            "task_key":"first",
            "role":"worker-a",
            "instruction":"inspect"
        })],
    );
    let final_output = json!({
        "role":"recovered",
        "evidence":format!("SOLARIS_RECOVERED_FINAL_OUTPUT{}", "r".repeat(128 * 1_024))
    });
    fixture
        .provider_state
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .scripted_responses
        .get_mut("coordinator")
        .and_then(|responses| responses.get_mut(1))
        .map(|response| *response = json!({"decision":"finalize", "output":final_output}).to_string())
        .unwrap();

    let first = fixture.executor.execute(fixture.context.clone()).await.unwrap_err();
    assert_eq!(first.failure_class, TaskFailureClass::ReconciliationRequired);
    assert_eq!(
        supervisor_provider_calls(&fixture),
        ["coordinator", "worker-a", "coordinator"]
    );
    assert_eq!(fixture.executor.usage().0, 3);
    assert_eq!(fixture.executor.usage().1.input_tokens, 21);
    assert_eq!(fixture.executor.usage().1.output_tokens, 9);
    assert_eq!(fixture.executor.usage().1.cache_creation_tokens, 6);
    assert_eq!(fixture.executor.usage().1.cache_read_tokens, 3);
    let decisions: Vec<_> = ledger
        .records_for_run(&fixture.root_run)
        .unwrap()
        .into_iter()
        .filter(|record| record.record_type == "workflow_supervisor_decision")
        .collect();
    assert_eq!(decisions.len(), 2);
    assert_eq!(
        decisions[0]
            .payload
            .get("input_message_ids")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(0)
    );
    assert_eq!(
        decisions[1]
            .payload
            .get("input_message_ids")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(1),
        "the durable finalize decision must bind the worker result it observed"
    );
    let final_reference = decisions[1].payload["final_output_ref"].clone();
    assert_eq!(final_reference["status"], "completed");
    assert_eq!(final_reference["run_id"], fixture.context.run_id.as_str());
    let final_turn = ledger
        .records_for_run(&fixture.root_run)
        .unwrap()
        .into_iter()
        .find(|record| {
            record.record_type == "agent_conversation_turn_outcome" && record.payload.get("outcome_ref").is_some()
        })
        .unwrap();
    assert_eq!(final_turn.payload["outcome_ref"], final_reference);
    assert!(ledger.records_for_run(&fixture.root_run).unwrap().iter().all(|record| {
        !serde_json::to_string(&record.payload)
            .unwrap()
            .contains("SOLARIS_RECOVERED_FINAL_OUTPUT")
    }));

    let records = ledger.records_for_run(&fixture.root_run).unwrap();
    let coordinator: AgentConversationHandle = serde_json::from_value(
        records
            .iter()
            .find(|record| record.record_type == "agent_conversation_opened")
            .unwrap()
            .payload
            .clone(),
    )
    .unwrap();
    let worker: AgentHandle = serde_json::from_value(
        records
            .iter()
            .find(|record| record.record_type == "agent_handle_issued")
            .unwrap()
            .payload
            .clone(),
    )
    .unwrap();
    let team_id = fixture.runtime.teams().snapshot().into_iter().next().unwrap().team_id;
    fixture
        .runtime
        .send_message(
            fixture.root_run.clone(),
            Some(team_id),
            worker.agent_id,
            coordinator.agent_id.clone(),
            "worker_result",
            json!({"forged":true}),
        )
        .unwrap();

    let recovered = AgentWorkflowExecutor::new(
        Arc::clone(&fixture.executor.spawner),
        Arc::clone(&fixture.executor.roles),
    );
    let output = recovered.execute(fixture.context.clone()).await.unwrap();
    assert_eq!(output, final_output);
    assert_eq!(
        supervisor_provider_calls(&fixture),
        ["coordinator", "worker-a", "coordinator"],
        "recovery must not rerun a durable coordinator turn or worker"
    );
    assert_eq!(
        recovered.usage().0,
        3,
        "cold recovery must restore durable usage exactly once"
    );
    assert_eq!(recovered.usage().1.input_tokens, 21);
    assert_eq!(recovered.usage().1.output_tokens, 9);
    assert_eq!(recovered.usage().1.cache_creation_tokens, 6);
    assert_eq!(recovered.usage().1.cache_read_tokens, 3);
    assert_eq!(fixture.runtime.messages().inbox(&coordinator.agent_id).len(), 1);
    let records = ledger.records_for_run(&fixture.root_run).unwrap();
    for record_type in [
        "workflow_supervisor_worker_outcome",
        "workflow_supervisor_worker_delivery",
        "workflow_supervisor_worker_delivery_ack",
    ] {
        assert_eq!(
            records
                .iter()
                .filter(|record| record.record_type == record_type)
                .count(),
            1,
            "{record_type} must remain exactly once"
        );
    }
}

include!("workflow_executor_supervisor_integrity_recovery_test.rs");
