use super::*;

#[test]
fn legacy_v1_supervisor_decision_wire_reads_without_a_coordinator_binding() {
    let mut envelope = SupervisorDecisionEnvelope {
        schema_version: SUPERVISOR_SCHEMA_VERSION,
        workflow_run_id: RunId::from("legacy-workflow-run"),
        workflow_id: "workflow:legacy".to_owned(),
        node_id: "work".to_owned(),
        attempt_id: AttemptId::from("legacy-attempt"),
        round: 0,
        input_message_ids: Vec::new(),
        input_deliveries: Vec::new(),
        decision_digest: String::new(),
        turns: 1,
        usage: TokenUsage::default(),
        final_output_ref: None,
        coordinator_turn: None,
        decision: SupervisorDecision::Finalize {
            output: json!({"legacy": true}),
        },
    };
    envelope.decision_digest = supervisor_decision_digest(&envelope).unwrap();
    let mut wire = serde_json::to_value(&envelope).unwrap();
    wire.as_object_mut().unwrap().remove("coordinator_turn");

    let restored: SupervisorDecisionEnvelope = serde_json::from_value(wire).unwrap();

    assert_eq!(restored.schema_version, SUPERVISOR_SCHEMA_VERSION);
    assert!(restored.coordinator_turn.is_none());
    assert_eq!(supervisor_decision_digest(&restored).unwrap(), restored.decision_digest);
}
