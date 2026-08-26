use super::*;

pub(super) fn tamper_exact_once_decision(records: &mut [LedgerRecord], tamper: SupervisorTamper) {
    if matches!(tamper, SupervisorTamper::DeliveryAtDispatchSequence) {
        let dispatch_seq = records
            .iter()
            .find(|record| {
                record.record_type == "workflow_supervisor_decision" && record.payload["round"].as_u64() == Some(1)
            })
            .map(|record| record.seq)
            .unwrap();
        records
            .iter_mut()
            .find(|record| {
                record.record_type == "workflow_supervisor_worker_delivery"
                    && record.payload["dispatch_round"].as_u64() == Some(1)
            })
            .unwrap()
            .seq = dispatch_seq;
        return;
    }
    let mut deliveries: Vec<_> = records
        .iter()
        .filter(|record| record.record_type == "workflow_supervisor_worker_delivery")
        .map(|record| {
            (
                record.payload["dispatch_round"].as_u64().unwrap(),
                json!({
                    "message_id": record.payload["message_id"],
                    "delivery_digest": record.payload["delivery_digest"],
                }),
            )
        })
        .collect();
    deliveries.sort_by_key(|(round, _)| *round);
    assert_eq!(deliveries.len(), 2);

    let decision = records
        .iter_mut()
        .find(|record| {
            record.record_type == "workflow_supervisor_decision" && record.payload["round"].as_u64() == Some(2)
        })
        .unwrap();
    let inputs = match tamper {
        SupervisorTamper::ReuseAckedDelivery => vec![deliveries[0].1.clone(), deliveries[1].1.clone()],
        SupervisorTamper::OmitPendingDelivery => Vec::new(),
        _ => unreachable!(),
    };
    let message_ids: Vec<_> = inputs.iter().map(|input| input["message_id"].clone()).collect();
    decision.payload["input_message_ids"] = Value::Array(message_ids);
    decision.payload["input_deliveries"] = Value::Array(inputs);
    decision.payload["decision_digest"] = json!(supervisor_test_decision_digest(&decision.payload));
}

fn supervisor_test_decision_digest(payload: &Value) -> String {
    let mut digest_payload = json!({
        "schema_version": payload["schema_version"],
        "workflow_run_id": payload["workflow_run_id"],
        "workflow_id": payload["workflow_id"],
        "node_id": payload["node_id"],
        "attempt_id": payload["attempt_id"],
        "round": payload["round"],
        "input_message_ids": payload["input_message_ids"],
        "input_deliveries": payload["input_deliveries"],
        "turns": payload["turns"],
        "usage": payload["usage"],
        "final_output_ref": payload["final_output_ref"],
        "decision": payload["decision"],
    });
    if payload["schema_version"].as_u64() == Some(2) {
        digest_payload["coordinator_turn"] = payload["coordinator_turn"].clone();
    }
    crate::execution_context::stable_digest_value(&digest_payload)
}

pub(super) fn exact_once_recovery_fixture_for_causality(
    name: &str,
    terminal_decision: Value,
) -> (Arc<SupervisorRecoveryLedger>, SupervisorRecoveryFixture) {
    let ledger = Arc::new(SupervisorRecoveryLedger::new(
        SupervisorRecoveryFault::DeliveryAckAtDecision { decision_round: 2 },
    ));
    let fixture = supervisor_recovery_fixture(
        name,
        Arc::clone(&ledger),
        vec![json!({
            "task_key":"first",
            "role":"worker-a",
            "instruction":"inspect first"
        })],
    );
    fixture
        .provider_state
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .scripted_responses
        .insert(
            "coordinator".to_owned(),
            VecDeque::from([
                json!({
                    "decision":"dispatch",
                    "tasks":[{
                        "task_key":"first",
                        "role":"worker-a",
                        "instruction":"inspect first"
                    }]
                })
                .to_string(),
                json!({
                    "decision":"dispatch",
                    "tasks":[{
                        "task_key":"second",
                        "role":"worker-a",
                        "instruction":"inspect second"
                    }]
                })
                .to_string(),
                terminal_decision.to_string(),
            ]),
        );
    (ledger, fixture)
}

fn exact_once_recovery_fixture(
    name: &str,
    terminal_decision: Value,
) -> (Arc<SupervisorRecoveryLedger>, SupervisorRecoveryFixture) {
    exact_once_recovery_fixture_for_causality(name, terminal_decision)
}

fn round_one_message_id(ledger: &SupervisorRecoveryLedger, root_run: &RunId) -> String {
    ledger
        .inner
        .records_for_run(root_run)
        .unwrap()
        .into_iter()
        .find(|record| {
            record.record_type == "workflow_supervisor_worker_delivery"
                && record.payload["dispatch_round"].as_u64() == Some(1)
        })
        .and_then(|record| record.payload["message_id"].as_str().map(str::to_owned))
        .unwrap()
}

fn ack_count(ledger: &SupervisorRecoveryLedger, root_run: &RunId, message_id: &str) -> usize {
    ledger
        .inner
        .records_for_run(root_run)
        .unwrap()
        .iter()
        .filter(|record| {
            record.record_type == "workflow_supervisor_worker_delivery_ack"
                && record.payload["message_id"].as_str() == Some(message_id)
        })
        .count()
}

async fn assert_exact_once_tamper_is_rejected(tamper: SupervisorTamper, name: &str) {
    let (ledger, fixture) = exact_once_recovery_fixture(
        name,
        json!({
            "decision":"abort",
            "reason":"stop after two deliveries",
            "failure_class":"non_retryable"
        }),
    );
    let first = fixture.executor.execute(fixture.context.clone()).await.unwrap_err();
    assert_eq!(
        first.failure_class,
        TaskFailureClass::ReconciliationRequired,
        "unexpected fixture failure: {first:?}"
    );
    assert!(
        first
            .message
            .contains("injected Supervisor failure before workflow_supervisor_worker_delivery_ack"),
        "unexpected fixture failure: {first:?}"
    );
    assert_eq!(supervisor_provider_calls(&fixture).len(), 5);

    let message_id = round_one_message_id(&ledger, &fixture.root_run);
    assert_eq!(ack_count(&ledger, &fixture.root_run, &message_id), 0);
    ledger.arm_tamper(tamper);
    let calls_before = supervisor_provider_calls(&fixture);
    let recovered = AgentWorkflowExecutor::new(
        Arc::clone(&fixture.executor.spawner),
        Arc::clone(&fixture.executor.roles),
    );
    let error = recovered.execute(fixture.context.clone()).await.unwrap_err();

    assert_eq!(error.failure_class, TaskFailureClass::ReconciliationRequired);
    assert!(
        error
            .message
            .contains("decision inputs do not exactly match unacknowledged durable deliveries"),
        "unexpected recovery error: {}",
        error.message
    );
    assert_eq!(supervisor_provider_calls(&fixture), calls_before);
    assert_eq!(ack_count(&ledger, &fixture.root_run, &message_id), 0);
    assert!(
        ledger
            .inner
            .records_for_run(&fixture.root_run)
            .unwrap()
            .iter()
            .filter(|record| record.record_type == "workflow_supervisor_decision")
            .all(|record| record.payload["final_output_ref"].is_null()),
        "recovery must not expose a final output"
    );
}

#[tokio::test]
async fn supervisor_cold_recovery_rejects_reusing_an_acknowledged_delivery() {
    assert_exact_once_tamper_is_rejected(SupervisorTamper::ReuseAckedDelivery, "reuse-acked-delivery").await;
}

#[tokio::test]
async fn supervisor_cold_recovery_rejects_omitting_a_pending_delivery() {
    assert_exact_once_tamper_is_rejected(SupervisorTamper::OmitPendingDelivery, "omit-pending-delivery").await;
}

#[tokio::test]
async fn supervisor_cold_recovery_rejects_delivery_at_its_dispatch_sequence() {
    let (ledger, fixture) = exact_once_recovery_fixture(
        "delivery-at-dispatch-sequence",
        json!({"decision":"finalize", "output":{"final":"kept"}}),
    );
    let first = fixture.executor.execute(fixture.context.clone()).await.unwrap_err();
    assert_eq!(
        first.failure_class,
        TaskFailureClass::ReconciliationRequired,
        "unexpected fixture failure: {first:?}"
    );
    assert!(
        first
            .message
            .contains("injected Supervisor failure before workflow_supervisor_worker_delivery_ack"),
        "unexpected fixture failure: {first:?}"
    );
    assert_eq!(supervisor_provider_calls(&fixture).len(), 5);

    let calls_before = supervisor_provider_calls(&fixture);
    let records_before = ledger.inner.records_for_run(&fixture.root_run).unwrap();
    let message_id = round_one_message_id(&ledger, &fixture.root_run);
    assert_eq!(ack_count(&ledger, &fixture.root_run, &message_id), 0);
    ledger.arm_tamper(SupervisorTamper::DeliveryAtDispatchSequence);
    let recovered = AgentWorkflowExecutor::new(
        Arc::clone(&fixture.executor.spawner),
        Arc::clone(&fixture.executor.roles),
    );
    let error = recovered.execute(fixture.context.clone()).await.unwrap_err();

    assert_eq!(error.failure_class, TaskFailureClass::ReconciliationRequired);
    assert!(
        error
            .message
            .contains("delivery is not ordered after its durable Dispatch decision"),
        "unexpected recovery error: {}",
        error.message
    );
    assert_eq!(supervisor_provider_calls(&fixture), calls_before);
    assert_eq!(ledger.inner.records_for_run(&fixture.root_run).unwrap(), records_before);
}
