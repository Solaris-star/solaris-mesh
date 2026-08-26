#[derive(Clone, Copy)]
enum SupervisorCompletionTamper {
    DeleteCoordinatorTurnOutcome,
    DuplicateCoordinatorTurnOutcome,
    TamperCoordinatorTurnOutcome,
    CrossRunCoordinatorTurnOutcome,
    CoordinatorTurnOutcomeAfterDecision,
    MissingSource,
    DigestAndReference,
    CrossRun,
    SourceAfterCompletion,
}

struct SupervisorCompletionTamperLedger {
    inner: Arc<InMemoryRuntimeLedger>,
    tamper: Mutex<Option<SupervisorCompletionTamper>>,
}

impl SupervisorCompletionTamperLedger {
    fn new() -> Self {
        Self {
            inner: Arc::new(InMemoryRuntimeLedger::default()),
            tamper: Mutex::new(None),
        }
    }

    fn arm(&self, tamper: SupervisorCompletionTamper) {
        *self.tamper.lock().unwrap_or_else(|error| error.into_inner()) = Some(tamper);
    }

    fn completion_source(&self, workflow_run_id: &RunId) -> Value {
        let root_run_id = RunId::from(workflow_run_id.as_str().split_once(":workflow:").unwrap().0);
        let decision = self
            .inner
            .records_for_run(&root_run_id)
            .unwrap()
            .into_iter()
            .find(|record| {
                record.record_type == "workflow_supervisor_decision"
                    && record.payload["workflow_run_id"].as_str() == Some(workflow_run_id.as_str())
                    && !record.payload["final_output_ref"].is_null()
            })
            .unwrap();
        json!({
            "decision_ledger_run_id": root_run_id,
            "workflow_run_id": workflow_run_id,
            "node_id": decision.payload["node_id"],
            "attempt_id": decision.payload["attempt_id"],
            "round": decision.payload["round"],
            "decision_record_seq": decision.seq,
            "decision_digest": decision.payload["decision_digest"],
            "final_output_ref": decision.payload["final_output_ref"],
        })
    }
}

impl RuntimeLedger for SupervisorCompletionTamperLedger {
    crate::runtime_ledger::forward_compare_and_append!();

    fn append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        payload: Value,
    ) -> std::io::Result<LedgerRecord> {
        self.inner.append(run_id, durability, record_type, payload)
    }

    fn run_ids(&self) -> std::io::Result<Vec<RunId>> {
        self.inner.run_ids()
    }

    fn records_for_run(&self, run_id: &RunId) -> std::io::Result<Vec<LedgerRecord>> {
        let mut records = self.inner.records_for_run(run_id)?;
        let Some(tamper) = *self.tamper.lock().unwrap_or_else(|error| error.into_inner()) else {
            return Ok(records);
        };
        if matches!(tamper, SupervisorCompletionTamper::DeleteCoordinatorTurnOutcome) {
            records.retain(|record| record.record_type != "agent_conversation_turn_outcome");
            return Ok(records);
        }
        if matches!(
            tamper,
            SupervisorCompletionTamper::DuplicateCoordinatorTurnOutcome
                | SupervisorCompletionTamper::TamperCoordinatorTurnOutcome
                | SupervisorCompletionTamper::CrossRunCoordinatorTurnOutcome
                | SupervisorCompletionTamper::CoordinatorTurnOutcomeAfterDecision
        ) {
            let decision_seq = records
                .iter()
                .find(|record| record.record_type == "workflow_supervisor_decision")
                .map(|record| record.seq);
            let outcome_index = records
                .iter()
                .position(|record| record.record_type == "agent_conversation_turn_outcome");
            if let Some(index) = outcome_index {
                match tamper {
                    SupervisorCompletionTamper::DuplicateCoordinatorTurnOutcome => {
                        let mut duplicate = records[index].clone();
                        duplicate.seq = records.iter().map(|record| record.seq).max().unwrap_or(0).saturating_add(1);
                        records.push(duplicate);
                    }
                    SupervisorCompletionTamper::TamperCoordinatorTurnOutcome => {
                        records[index].payload["usage"]["input_tokens"] = json!(u64::MAX);
                    }
                    SupervisorCompletionTamper::CrossRunCoordinatorTurnOutcome => {
                        records[index].run_id = RunId::from("forged-root-run");
                    }
                    SupervisorCompletionTamper::CoordinatorTurnOutcomeAfterDecision => {
                        records[index].seq = decision_seq.unwrap().saturating_add(1);
                    }
                    _ => unreachable!(),
                }
            }
            return Ok(records);
        }
        let Some(completion) = records
            .iter_mut()
            .find(|record| record.record_type == "workflow_node_completed")
        else {
            return Ok(records);
        };
        let source = self.completion_source(run_id);
        completion.payload["completion_schema_version"] = json!(2);
        completion.payload["source_supervisor_decision"] = source;
        match tamper {
            SupervisorCompletionTamper::DeleteCoordinatorTurnOutcome
            | SupervisorCompletionTamper::DuplicateCoordinatorTurnOutcome
            | SupervisorCompletionTamper::TamperCoordinatorTurnOutcome
            | SupervisorCompletionTamper::CrossRunCoordinatorTurnOutcome
            | SupervisorCompletionTamper::CoordinatorTurnOutcomeAfterDecision => unreachable!(),
            SupervisorCompletionTamper::MissingSource => {
                completion
                    .payload
                    .as_object_mut()
                    .unwrap()
                    .remove("source_supervisor_decision");
            }
            SupervisorCompletionTamper::DigestAndReference => {
                completion.payload["source_supervisor_decision"]["decision_digest"] = json!("forged-decision");
                completion.payload["source_supervisor_decision"]["final_output_ref"]["digest"] =
                    json!("forged-output");
            }
            SupervisorCompletionTamper::CrossRun => {
                completion.payload["source_supervisor_decision"]["workflow_run_id"] = json!("forged-workflow-run");
            }
            SupervisorCompletionTamper::SourceAfterCompletion => {
                completion.payload["source_supervisor_decision"]["decision_record_seq"] =
                    json!(completion.seq.saturating_add(1));
            }
        }
        Ok(records)
    }

    fn effect_output_root(&self) -> Option<std::path::PathBuf> {
        self.inner.effect_output_root()
    }
}

async fn assert_supervisor_completion_tamper_is_rejected(name: &str, tamper: SupervisorCompletionTamper) {
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
    let ledger = Arc::new(SupervisorCompletionTamperLedger::new());
    let runtime_ledger: Arc<dyn RuntimeLedger> = ledger.clone();
    let (snapshot, _, root_run, provider) = run_supervisor_workflow_with_permission_mode(
        &format!("completion-source-{name}"),
        collaboration.clone(),
        vec![
            v2_role("coordinator", &["Read"], PermissionCeiling::plan()),
            v2_role("worker-a", &["Read"], PermissionCeiling::plan()),
        ],
        HashMap::from([(
            "coordinator".to_owned(),
            VecDeque::from([json!({"decision":"finalize", "output":{"role":"coordinator"}}).to_string()]),
        )]),
        1,
        solaris_types::workflow::MultiAgentPolicy::OnDemand,
        SupervisorWorkflowRuntimeOptions {
            permission_mode: solaris_types::permission::PermissionMode::Auto,
            ledger: Some(runtime_ledger.clone()),
            concurrency_gate: None,
        },
    )
    .await;
    let root_records = ledger.inner.records_for_run(&root_run).unwrap();
    let workflow_records = ledger.inner.records_for_run(&snapshot.run_id).unwrap();
    let calls = provider.lock().unwrap_or_else(|error| error.into_inner()).calls.clone();
    let final_ref = root_records
        .iter()
        .find(|record| record.record_type == "workflow_supervisor_decision")
        .unwrap()
        .payload["final_output_ref"]["reference"]
        .as_str()
        .unwrap()
        .to_owned();
    let body_before = crate::execution_context::EffectOutputStore::for_run_with_ledger(
        &snapshot.run_id,
        runtime_ledger.as_ref(),
    )
    .read(&final_ref)
    .unwrap();
    ledger.arm(tamper);

    let recovery_runtime = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(1)),
        runtime_ledger.clone(),
    ));
    let roles = Arc::new(AgentRoleRegistry::default());
    roles.register(v2_role("coordinator", &["Read"], PermissionCeiling::plan()));
    roles.register(v2_role("worker-a", &["Read"], PermissionCeiling::plan()));
    let controller = WorkflowController::with_runtime_and_roles(recovery_runtime, Some(roles));
    controller
        .register(WorkflowDefinition {
            id: format!("v2-completion-source-{name}"),
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
    let error = controller.restore_from_ledger(&root_run).unwrap_err();
    assert!(error.contains("Supervisor"), "{error}");
    assert!(controller.snapshot(&snapshot.run_id).is_none());
    assert_eq!(ledger.inner.records_for_run(&root_run).unwrap(), root_records);
    assert_eq!(ledger.inner.records_for_run(&snapshot.run_id).unwrap(), workflow_records);
    assert_eq!(
        provider.lock().unwrap_or_else(|error| error.into_inner()).calls,
        calls
    );
    assert_eq!(
        crate::execution_context::EffectOutputStore::for_run_with_ledger(
            &snapshot.run_id,
            runtime_ledger.as_ref(),
        )
        .read(&final_ref)
        .unwrap(),
        body_before
    );
}

#[tokio::test]
async fn supervisor_completion_recovery_rejects_missing_coordinator_turn_outcome() {
    assert_supervisor_completion_tamper_is_rejected(
        "missing-turn-outcome",
        SupervisorCompletionTamper::DeleteCoordinatorTurnOutcome,
    )
    .await;
}

#[tokio::test]
async fn supervisor_completion_recovery_rejects_duplicate_coordinator_turn_outcome() {
    assert_supervisor_completion_tamper_is_rejected(
        "duplicate-turn-outcome",
        SupervisorCompletionTamper::DuplicateCoordinatorTurnOutcome,
    )
    .await;
}

#[tokio::test]
async fn supervisor_completion_recovery_rejects_tampered_coordinator_turn_outcome() {
    assert_supervisor_completion_tamper_is_rejected(
        "tampered-turn-outcome",
        SupervisorCompletionTamper::TamperCoordinatorTurnOutcome,
    )
    .await;
}

#[tokio::test]
async fn supervisor_completion_recovery_rejects_cross_run_coordinator_turn_outcome() {
    assert_supervisor_completion_tamper_is_rejected(
        "cross-run-turn-outcome",
        SupervisorCompletionTamper::CrossRunCoordinatorTurnOutcome,
    )
    .await;
}

#[tokio::test]
async fn supervisor_completion_recovery_rejects_coordinator_turn_outcome_after_decision() {
    assert_supervisor_completion_tamper_is_rejected(
        "turn-outcome-after-decision",
        SupervisorCompletionTamper::CoordinatorTurnOutcomeAfterDecision,
    )
    .await;
}

#[tokio::test]
async fn supervisor_completion_recovery_rejects_missing_source() {
    assert_supervisor_completion_tamper_is_rejected("missing", SupervisorCompletionTamper::MissingSource).await;
}

#[tokio::test]
async fn supervisor_completion_recovery_rejects_tampered_digest_and_reference() {
    assert_supervisor_completion_tamper_is_rejected(
        "digest-ref",
        SupervisorCompletionTamper::DigestAndReference,
    )
    .await;
}

#[tokio::test]
async fn supervisor_completion_recovery_rejects_cross_run_source() {
    assert_supervisor_completion_tamper_is_rejected("cross-run", SupervisorCompletionTamper::CrossRun).await;
}

#[tokio::test]
async fn supervisor_completion_recovery_rejects_source_after_completion() {
    assert_supervisor_completion_tamper_is_rejected(
        "after-completion",
        SupervisorCompletionTamper::SourceAfterCompletion,
    )
    .await;
}
