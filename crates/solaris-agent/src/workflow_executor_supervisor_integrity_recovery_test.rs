#[tokio::test]
async fn supervisor_recovery_rejects_tampered_coordinator_output_blob_without_provider_replay() {
    let ledger = Arc::new(SupervisorRecoveryLedger::new(SupervisorRecoveryFault::DeliveryAck));
    let fixture = supervisor_recovery_fixture(
        "tampered-coordinator-output-blob",
        Arc::clone(&ledger),
        vec![json!({
            "task_key":"first",
            "role":"worker-a",
            "instruction":"inspect"
        })],
    );

    let first = fixture.executor.execute(fixture.context.clone()).await.unwrap_err();
    assert_eq!(first.failure_class, TaskFailureClass::ReconciliationRequired);
    let calls = supervisor_provider_calls(&fixture);
    let reference = ledger
        .records_for_run(&fixture.root_run)
        .unwrap()
        .into_iter()
        .find(|record| {
            record.record_type == "workflow_supervisor_decision"
                && record
                    .payload
                    .get("final_output_ref")
                    .is_some_and(|value| !value.is_null())
        })
        .map(|record| record.payload["final_output_ref"].clone())
        .unwrap();
    assert_eq!(reference["run_id"], fixture.context.run_id.as_str());
    assert_eq!(reference["status"], "completed");
    let output_root = ledger
        .effect_output_root()
        .unwrap()
        .join(crate::execution_context::stable_digest_bytes(
            fixture.context.run_id.as_str().as_bytes(),
        ));
    std::fs::write(
        output_root.join(reference["reference"].as_str().unwrap()),
        b"tampered final output",
    )
    .unwrap();

    let recovered = AgentWorkflowExecutor::new(
        Arc::clone(&fixture.executor.spawner),
        Arc::clone(&fixture.executor.roles),
    );
    let error = recovered.execute(fixture.context.clone()).await.unwrap_err();
    assert_eq!(error.failure_class, TaskFailureClass::ReconciliationRequired);
    assert!(error.message.contains("final output"), "{error:?}");
    assert_eq!(
        supervisor_provider_calls(&fixture),
        calls,
        "restoring a durable final decision must not call a Provider after blob tampering"
    );
}

#[tokio::test]
async fn supervisor_recovery_rejects_final_output_reference_from_another_workflow_run() {
    let ledger = Arc::new(SupervisorRecoveryLedger::new(SupervisorRecoveryFault::DeliveryAck));
    let fixture = supervisor_recovery_fixture(
        "foreign-final-output-run",
        Arc::clone(&ledger),
        vec![json!({
            "task_key":"first",
            "role":"worker-a",
            "instruction":"inspect"
        })],
    );

    let first = fixture.executor.execute(fixture.context.clone()).await.unwrap_err();
    assert_eq!(first.failure_class, TaskFailureClass::ReconciliationRequired);
    let calls = supervisor_provider_calls(&fixture);
    ledger.arm_tamper(SupervisorTamper::FinalOutputRunId);
    let recovered = AgentWorkflowExecutor::new(
        Arc::clone(&fixture.executor.spawner),
        Arc::clone(&fixture.executor.roles),
    );
    let error = recovered.execute(fixture.context.clone()).await.unwrap_err();
    assert_eq!(error.failure_class, TaskFailureClass::ReconciliationRequired);
    assert!(error.message.contains("different Workflow Run"), "{error:?}");
    assert_eq!(supervisor_provider_calls(&fixture), calls);
}

#[tokio::test]
async fn supervisor_recovery_rejects_tampered_decision_input_message_ids_without_provider_replay() {
    let ledger = Arc::new(SupervisorRecoveryLedger::new(SupervisorRecoveryFault::TaskCreate {
        task_key: "first".into(),
    }));
    let fixture = supervisor_recovery_fixture(
        "tampered-decision-inputs",
        Arc::clone(&ledger),
        vec![json!({"task_key":"first", "role":"worker-a", "instruction":"inspect"})],
    );

    fixture.executor.execute(fixture.context.clone()).await.unwrap_err();
    assert_eq!(supervisor_provider_calls(&fixture), ["coordinator"]);
    ledger.arm_tamper(SupervisorTamper::DecisionInputIds);

    let error = fixture.executor.execute(fixture.context.clone()).await.unwrap_err();
    assert_eq!(error.failure_class, TaskFailureClass::ReconciliationRequired);
    assert!(error.message.contains("decision envelope"));
    assert_eq!(supervisor_provider_calls(&fixture), ["coordinator"]);
}

#[tokio::test]
async fn supervisor_recovery_rejects_worker_role_tampering_even_with_a_recomputed_outcome_digest() {
    let ledger = Arc::new(SupervisorRecoveryLedger::new(SupervisorRecoveryFault::DeliveryAck));
    let fixture = supervisor_recovery_fixture(
        "tampered-worker-role",
        Arc::clone(&ledger),
        vec![json!({"task_key":"first", "role":"worker-a", "instruction":"inspect"})],
    );

    fixture.executor.execute(fixture.context.clone()).await.unwrap_err();
    assert_eq!(
        supervisor_provider_calls(&fixture),
        ["coordinator", "worker-a", "coordinator"]
    );
    ledger.arm_tamper(SupervisorTamper::WorkerRoleWithRecomputedDigest);

    let error = fixture.executor.execute(fixture.context.clone()).await.unwrap_err();
    assert_eq!(error.failure_class, TaskFailureClass::ReconciliationRequired);
    assert!(
        error.message.contains("worker delivery") || error.message.contains("worker outcome identity"),
        "{error:?}"
    );
    assert_eq!(
        supervisor_provider_calls(&fixture),
        ["coordinator", "worker-a", "coordinator"]
    );
}

async fn assert_supervisor_chain_tamper_is_rejected(name: &str, tamper: SupervisorTamper) {
    let ledger = Arc::new(SupervisorRecoveryLedger::new(SupervisorRecoveryFault::TaskCreate {
        task_key: "never-created".into(),
    }));
    let fixture = supervisor_recovery_fixture(
        name,
        Arc::clone(&ledger),
        vec![json!({
            "task_key":"first",
            "role":"worker-a",
            "instruction":"inspect"
        })],
    );
    assert_eq!(
        fixture.executor.execute(fixture.context.clone()).await.unwrap(),
        json!({"role":"recovered"})
    );
    let calls = supervisor_provider_calls(&fixture);
    ledger.arm_tamper(tamper);

    let recovered = AgentWorkflowExecutor::new(
        Arc::clone(&fixture.executor.spawner),
        Arc::clone(&fixture.executor.roles),
    );
    let error = recovered.execute(fixture.context.clone()).await.unwrap_err();
    assert_eq!(error.failure_class, TaskFailureClass::ReconciliationRequired);
    assert_eq!(supervisor_provider_calls(&fixture), calls);
}

#[tokio::test]
async fn supervisor_recovery_rejects_ack_that_precedes_its_delivery() {
    assert_supervisor_chain_tamper_is_rejected("ack-before-delivery", SupervisorTamper::AckBeforeDelivery).await;
}

#[tokio::test]
async fn supervisor_recovery_rejects_tampered_acked_worker_blob_without_provider_replay() {
    let ledger = Arc::new(SupervisorRecoveryLedger::new(SupervisorRecoveryFault::TaskCreate {
        task_key: "never-created".into(),
    }));
    let fixture = supervisor_recovery_fixture(
        "tampered-acked-worker-blob",
        Arc::clone(&ledger),
        vec![json!({
            "task_key":"first",
            "role":"worker-a",
            "instruction":"inspect"
        })],
    );
    assert_eq!(
        fixture.executor.execute(fixture.context.clone()).await.unwrap(),
        json!({"role":"recovered"})
    );
    let calls = supervisor_provider_calls(&fixture);
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
            "the fixture must corrupt a completed and acknowledged worker result"
        );
    }
    assert_eq!(
        records
            .iter()
            .filter(|record| record.record_type == "workflow_supervisor_decision")
            .count(),
        2,
        "the worker delivery must already have been consumed by the next coordinator decision"
    );
    let result_ref = records
        .into_iter()
        .find(|record| record.record_type == "workflow_supervisor_worker_outcome")
        .and_then(|record| record.payload["result_ref"].as_str().map(str::to_owned))
        .unwrap();
    let run_output_root = ledger
        .effect_output_root()
        .unwrap()
        .join(crate::execution_context::stable_digest_bytes(
            fixture.root_run.as_str().as_bytes(),
        ));
    std::fs::write(run_output_root.join(result_ref), b"tampered worker outcome").unwrap();

    let recovered = AgentWorkflowExecutor::new(
        Arc::clone(&fixture.executor.spawner),
        Arc::clone(&fixture.executor.roles),
    );
    let CollaborationSelection::Configured(config) = fixture.context.node.collaboration.clone() else {
        panic!("Supervisor fixture must use configured collaboration");
    };
    let loader_error = recovered
        .validate_supervisor_pending_delivery_integrity_for_test(&fixture.context, &config)
        .await
        .unwrap_err();
    assert_eq!(
        loader_error.failure_class,
        TaskFailureClass::ReconciliationRequired,
        "the ACK loader must verify the actual worker outcome bytes before hiding the delivery"
    );
    let error = recovered.execute(fixture.context.clone()).await.unwrap_err();
    assert_eq!(error.failure_class, TaskFailureClass::ReconciliationRequired);
    assert!(error.message.contains("worker outcome"), "{error:?}");
    assert_eq!(
        supervisor_provider_calls(&fixture),
        calls,
        "restoring a durable decision must not call the coordinator or worker Provider again"
    );
}

#[tokio::test]
async fn supervisor_recovery_rejects_decision_with_a_tampered_input_delivery_digest() {
    assert_supervisor_chain_tamper_is_rejected(
        "decision-input-delivery-digest",
        SupervisorTamper::DecisionInputDeliveryDigest,
    )
    .await;
}

#[tokio::test]
async fn supervisor_recovery_rejects_ack_with_a_different_delivery_digest() {
    assert_supervisor_chain_tamper_is_rejected(
        "ack-delivery-digest",
        SupervisorTamper::AckDeliveryDigestWithRecomputedAck,
    )
    .await;
}

#[tokio::test]
async fn supervisor_recovery_rejects_ack_bound_to_the_wrong_decision_round() {
    assert_supervisor_chain_tamper_is_rejected("ack-cross-round", SupervisorTamper::AckCrossRoundWithRecomputedAck)
        .await;
}

#[tokio::test]
async fn supervisor_recovery_rejects_delivery_reassembled_under_another_round() {
    let ledger = Arc::new(SupervisorRecoveryLedger::new(SupervisorRecoveryFault::TaskCreate {
        task_key: "never-created".into(),
    }));
    let fixture = supervisor_recovery_fixture("delivery-cross-round", Arc::clone(&ledger), Vec::new());
    fixture
        .provider_state
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .scripted_responses
        .insert(
            "coordinator".into(),
            VecDeque::from([
                json!({
                    "decision":"dispatch",
                    "tasks":[{"task_key":"first", "role":"worker-a", "instruction":"first"}]
                })
                .to_string(),
                json!({
                    "decision":"dispatch",
                    "tasks":[{"task_key":"second", "role":"worker-a", "instruction":"second"}]
                })
                .to_string(),
                json!({"decision":"finalize", "output":{"role":"recovered"}}).to_string(),
            ]),
        );
    assert_eq!(
        fixture.executor.execute(fixture.context.clone()).await.unwrap(),
        json!({"role":"recovered"})
    );
    let calls = supervisor_provider_calls(&fixture);
    ledger.arm_tamper(SupervisorTamper::DeliveryOutcomesAcrossRoundsWithRecomputedDigests);

    let recovered = AgentWorkflowExecutor::new(
        Arc::clone(&fixture.executor.spawner),
        Arc::clone(&fixture.executor.roles),
    );
    let error = recovered.execute(fixture.context.clone()).await.unwrap_err();
    assert_eq!(error.failure_class, TaskFailureClass::ReconciliationRequired);
    assert_eq!(supervisor_provider_calls(&fixture), calls);
}
