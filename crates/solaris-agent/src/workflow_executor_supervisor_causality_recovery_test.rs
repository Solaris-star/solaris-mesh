use super::*;

#[derive(Clone, Copy, Debug)]
pub(super) enum SupervisorCausalEdge {
    DispatchToTaskCreated,
    TaskCreatedToHandle,
    HandleToAssignment,
    AssignmentToAgentOutcome,
    AgentOutcomeToSupervisorOutcome,
    SupervisorOutcomeToSettlement,
    SupervisorOutcomeToDelivery,
    SettlementToDelivery,
    DeliveryToConsumingDecision,
    ConsumingDecisionToAck,
    AckToLaterDecision,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum SupervisorSequenceMutation {
    Equal,
    Reversed,
}

pub(super) fn tamper_supervisor_causal_sequence(
    records: &mut [LedgerRecord],
    edge: SupervisorCausalEdge,
    mutation: SupervisorSequenceMutation,
) {
    let (parent, child) = causal_record_indexes(records, edge);
    match mutation {
        SupervisorSequenceMutation::Equal => records[child].seq = records[parent].seq,
        SupervisorSequenceMutation::Reversed => {
            let parent_seq = records[parent].seq;
            records[parent].seq = records[child].seq;
            records[child].seq = parent_seq;
        }
    }
}

fn causal_record_indexes(records: &[LedgerRecord], edge: SupervisorCausalEdge) -> (usize, usize) {
    let task_id = records
        .iter()
        .find(|record| {
            record.record_type == "task_created"
                && record.payload.get("task_key").and_then(Value::as_str) == Some("first")
        })
        .and_then(|record| record.payload.get("task_id"))
        .and_then(Value::as_str)
        .unwrap();
    let operation_id = records
        .iter()
        .find(|record| {
            record.record_type == "agent_handle_issued"
                && record.payload.get("task_id").and_then(Value::as_str) == Some(task_id)
        })
        .and_then(|record| record.payload.get("operation_id"))
        .and_then(Value::as_str)
        .unwrap();
    let dispatch = find_record(records, |record| {
        record.record_type == "workflow_supervisor_decision" && record.payload["round"].as_u64() == Some(0)
    });
    let task_created = find_record(records, |record| {
        record.record_type == "task_created" && record.payload["task_id"].as_str() == Some(task_id)
    });
    let handle = find_record(records, |record| {
        record.record_type == "agent_handle_issued" && record.payload["operation_id"].as_str() == Some(operation_id)
    });
    let assignment = find_task_cas(records, task_id, "assign");
    let agent_outcome = find_record(records, |record| {
        record.record_type == "agent_outcome" && record.payload["spawn_operation_id"].as_str() == Some(operation_id)
    });
    let supervisor_outcome = find_record(records, |record| {
        record.record_type == "workflow_supervisor_worker_outcome"
            && record.payload["operation_id"].as_str() == Some(operation_id)
    });
    let settlement = find_task_cas(records, task_id, "settle");
    let delivery = find_record(records, |record| {
        record.record_type == "workflow_supervisor_worker_delivery"
            && record.payload["dispatch_round"].as_u64() == Some(0)
    });
    let consuming_decision = find_record(records, |record| {
        record.record_type == "workflow_supervisor_decision" && record.payload["round"].as_u64() == Some(1)
    });
    let ack = find_record(records, |record| {
        record.record_type == "workflow_supervisor_worker_delivery_ack"
            && record.payload["dispatch_round"].as_u64() == Some(0)
            && record.payload["decision_round"].as_u64() == Some(1)
    });
    let later_decision = find_record(records, |record| {
        record.record_type == "workflow_supervisor_decision" && record.payload["round"].as_u64() == Some(2)
    });
    match edge {
        SupervisorCausalEdge::DispatchToTaskCreated => (dispatch, task_created),
        SupervisorCausalEdge::TaskCreatedToHandle => (task_created, handle),
        SupervisorCausalEdge::HandleToAssignment => (handle, assignment),
        SupervisorCausalEdge::AssignmentToAgentOutcome => (assignment, agent_outcome),
        SupervisorCausalEdge::AgentOutcomeToSupervisorOutcome => (agent_outcome, supervisor_outcome),
        SupervisorCausalEdge::SupervisorOutcomeToSettlement => (supervisor_outcome, settlement),
        SupervisorCausalEdge::SupervisorOutcomeToDelivery => (supervisor_outcome, delivery),
        SupervisorCausalEdge::SettlementToDelivery => (settlement, delivery),
        SupervisorCausalEdge::DeliveryToConsumingDecision => (delivery, consuming_decision),
        SupervisorCausalEdge::ConsumingDecisionToAck => (consuming_decision, ack),
        SupervisorCausalEdge::AckToLaterDecision => (ack, later_decision),
    }
}

fn find_record(records: &[LedgerRecord], predicate: impl Fn(&LedgerRecord) -> bool) -> usize {
    let matches: Vec<_> = records
        .iter()
        .enumerate()
        .filter(|(_, record)| predicate(record))
        .map(|(index, _)| index)
        .collect();
    assert_eq!(matches.len(), 1);
    matches[0]
}

fn find_task_cas(records: &[LedgerRecord], task_id: &str, transition: &str) -> usize {
    find_record(records, |record| {
        record.record_type == "task_cas"
            && record.payload["task_id"].as_str() == Some(task_id)
            && record.payload["transition"].as_str() == Some(transition)
    })
}

fn assert_cold_recovery_did_not_mutate(
    ledger: &SupervisorRecoveryLedger,
    fixture: &SupervisorRecoveryFixture,
    records_before: &[LedgerRecord],
    calls_before: &[String],
) {
    assert_eq!(supervisor_provider_calls(fixture), calls_before);
    assert_eq!(ledger.inner.records_for_run(&fixture.root_run).unwrap(), records_before);
    let before_ack_count = records_before
        .iter()
        .filter(|record| record.record_type == "workflow_supervisor_worker_delivery_ack")
        .count();
    let before_final_count = records_before
        .iter()
        .filter(|record| {
            record.record_type == "workflow_supervisor_decision" && !record.payload["final_output_ref"].is_null()
        })
        .count();
    let after = ledger.inner.records_for_run(&fixture.root_run).unwrap();
    assert_eq!(
        after
            .iter()
            .filter(|record| record.record_type == "workflow_supervisor_worker_delivery_ack")
            .count(),
        before_ack_count
    );
    assert_eq!(
        after
            .iter()
            .filter(|record| {
                record.record_type == "workflow_supervisor_decision" && !record.payload["final_output_ref"].is_null()
            })
            .count(),
        before_final_count
    );
}

async fn completed_causal_fixture(
    name: &str,
) -> (
    Arc<SupervisorRecoveryLedger>,
    SupervisorRecoveryFixture,
    Vec<LedgerRecord>,
    Vec<String>,
) {
    let (ledger, fixture) = exact_once_recovery::exact_once_recovery_fixture_for_causality(
        name,
        json!({"decision":"finalize", "output":{"role":"recovered"}}),
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
    let records = ledger.inner.records_for_run(&fixture.root_run).unwrap();
    assert!(records.iter().any(|record| {
        record.record_type == "workflow_supervisor_decision" && !record.payload["final_output_ref"].is_null()
    }));
    let calls = supervisor_provider_calls(&fixture);
    (ledger, fixture, records, calls)
}

#[tokio::test]
async fn supervisor_cold_recovery_rejects_worker_outcome_after_its_delivery() {
    let (ledger, fixture, records_before, calls_before) = completed_causal_fixture("outcome-after-delivery").await;
    ledger.arm_tamper(SupervisorTamper::CausalSequence {
        edge: SupervisorCausalEdge::SupervisorOutcomeToDelivery,
        mutation: SupervisorSequenceMutation::Reversed,
    });
    let recovered = AgentWorkflowExecutor::new(
        Arc::clone(&fixture.executor.spawner),
        Arc::clone(&fixture.executor.roles),
    );
    let error = recovered.execute(fixture.context.clone()).await.unwrap_err();
    assert_eq!(error.failure_class, TaskFailureClass::ReconciliationRequired);
    assert_cold_recovery_did_not_mutate(&ledger, &fixture, &records_before, &calls_before);
}

#[tokio::test]
async fn supervisor_cold_recovery_rejects_equal_and_reversed_adjacent_causal_sequences() {
    use SupervisorCausalEdge::*;
    use SupervisorSequenceMutation::*;

    let cases = [
        DispatchToTaskCreated,
        TaskCreatedToHandle,
        HandleToAssignment,
        AssignmentToAgentOutcome,
        AgentOutcomeToSupervisorOutcome,
        SupervisorOutcomeToSettlement,
        SettlementToDelivery,
        DeliveryToConsumingDecision,
        ConsumingDecisionToAck,
        AckToLaterDecision,
    ];
    let (ledger, fixture, records_before, calls_before) = completed_causal_fixture("causal-sequence-table").await;
    for edge in cases {
        for mutation in [Equal, Reversed] {
            ledger.arm_tamper(SupervisorTamper::CausalSequence { edge, mutation });
            let recovered = AgentWorkflowExecutor::new(
                Arc::clone(&fixture.executor.spawner),
                Arc::clone(&fixture.executor.roles),
            );
            let error = recovered.execute(fixture.context.clone()).await.unwrap_err();
            assert_eq!(
                error.failure_class,
                TaskFailureClass::ReconciliationRequired,
                "{edge:?} {mutation:?}: {error:?}"
            );
            assert_cold_recovery_did_not_mutate(&ledger, &fixture, &records_before, &calls_before);
        }
    }
}

async fn assert_coordinator_turn_tamper_is_rejected(name: &str, tamper: SupervisorTamper) {
    let (ledger, fixture, records_before, calls_before) =
        completed_causal_fixture(&format!("coordinator-turn-{name}")).await;
    ledger.arm_tamper(tamper);
    let recovered = AgentWorkflowExecutor::new(
        Arc::clone(&fixture.executor.spawner),
        Arc::clone(&fixture.executor.roles),
    );
    let error = recovered.execute(fixture.context.clone()).await.unwrap_err();
    assert_eq!(
        error.failure_class,
        TaskFailureClass::ReconciliationRequired,
        "{name}: {error:?}"
    );
    assert_cold_recovery_did_not_mutate(&ledger, &fixture, &records_before, &calls_before);
}

#[tokio::test]
async fn supervisor_cold_recovery_rejects_missing_coordinator_turn_outcome() {
    assert_coordinator_turn_tamper_is_rejected("missing", SupervisorTamper::DeleteCoordinatorTurnOutcome).await;
}

#[tokio::test]
async fn supervisor_cold_recovery_rejects_tampered_coordinator_turn_outcome() {
    assert_coordinator_turn_tamper_is_rejected("tampered", SupervisorTamper::TamperCoordinatorTurnOutcome).await;
}

#[tokio::test]
async fn supervisor_cold_recovery_rejects_cross_run_coordinator_turn_outcome() {
    assert_coordinator_turn_tamper_is_rejected("cross-run", SupervisorTamper::CrossRunCoordinatorTurnOutcome).await;
}

#[tokio::test]
async fn supervisor_cold_recovery_rejects_coordinator_turn_outcome_after_decision() {
    assert_coordinator_turn_tamper_is_rejected("after-decision", SupervisorTamper::CoordinatorTurnOutcomeAfterDecision)
        .await;
}
