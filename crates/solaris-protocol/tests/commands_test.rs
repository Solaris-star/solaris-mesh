use rstest::rstest;
use solaris_protocol::commands::{ApprovalScope, ProtocolCommand, SessionMode};

#[rstest]
#[case(
    r#"{"type":"message","msg_id":"m1","content":"Hello"}"#,
    ProtocolCommand::Message {
        msg_id: "m1".to_string(),
        content: "Hello".to_string(),
        files: vec![],
    }
)]
#[case(
    r#"{"type":"message","msg_id":"m2","content":"Read this","files":["/tmp/a.rs"]}"#,
    ProtocolCommand::Message {
        msg_id: "m2".to_string(),
        content: "Read this".to_string(),
        files: vec!["/tmp/a.rs".to_string()],
    }
)]
#[case(r#"{"type":"stop"}"#, ProtocolCommand::Stop)]
#[case(
    r#"{"type":"cancel","msg_id":"m1"}"#,
    ProtocolCommand::Cancel { request_id: None, msg_id: "m1".to_string() }
)]
#[case(
    r#"{"type":"cancel_workflow","run_id":"run-1:workflow:w1"}"#,
    ProtocolCommand::CancelWorkflow { request_id: None, run_id: "run-1:workflow:w1".to_string() }
)]
#[case(
    r#"{"type":"init_history","text":"history"}"#,
    ProtocolCommand::InitHistory {
        messages: Vec::new(),
        text: Some("history".to_string()),
    }
)]
#[case(
    r#"{"type":"set_mode","mode":"default"}"#,
    ProtocolCommand::SetMode {
        request_id: None,
        mode: SessionMode::Auto,
    }
)]
#[case(
    r#"{"type":"set_mode","mode":"auto_edit"}"#,
    ProtocolCommand::SetMode {
        request_id: None,
        mode: SessionMode::Auto,
    }
)]
#[case(
    r#"{"type":"set_mode","mode":"yolo"}"#,
    ProtocolCommand::SetMode {
        request_id: None,
        mode: SessionMode::Bypass,
    }
)]
fn deserializes_protocol_commands(#[case] json: &str, #[case] expected: ProtocolCommand) {
    let cmd: ProtocolCommand = serde_json::from_str(json).expect("command should deserialize");
    assert_eq!(cmd, expected);
}

#[rstest]
#[case(r#"{"type":"tool_approve","call_id":"c1"}"#, ApprovalScope::Once)]
#[case(r#"{"type":"tool_approve","call_id":"c1","scope":"always"}"#, ApprovalScope::Always)]
fn deserializes_tool_approve_scope(#[case] json: &str, #[case] expected_scope: ApprovalScope) {
    let cmd: ProtocolCommand = serde_json::from_str(json).expect("tool approve should deserialize");

    match cmd {
        ProtocolCommand::ToolApprove { call_id, scope, .. } => {
            assert_eq!(call_id, "c1");
            assert_eq!(scope, expected_scope);
        }
        other => panic!("expected ToolApprove, got {other:?}"),
    }
}

#[rstest]
#[case(r#"{"type":"tool_deny","call_id":"c1"}"#, "")]
#[case(r#"{"type":"tool_deny","call_id":"c1","reason":"not allowed"}"#, "not allowed")]
fn deserializes_tool_deny_reason(#[case] json: &str, #[case] expected_reason: &str) {
    let cmd: ProtocolCommand = serde_json::from_str(json).expect("tool deny should deserialize");

    match cmd {
        ProtocolCommand::ToolDeny { call_id, reason, .. } => {
            assert_eq!(call_id, "c1");
            assert_eq!(reason, expected_reason);
        }
        other => panic!("expected ToolDeny, got {other:?}"),
    }
}

#[test]
fn control_commands_preserve_optional_request_id() {
    let intensity: ProtocolCommand =
        serde_json::from_str(r#"{"type":"set_intensity","request_id":"studio-1","intensity":"high"}"#).unwrap();
    match intensity {
        ProtocolCommand::SetIntensity { request_id, intensity } => {
            assert_eq!(request_id.as_deref(), Some("studio-1"));
            assert_eq!(intensity.to_string(), "high");
        }
        other => panic!("expected SetIntensity, got {other:?}"),
    }

    let cancel: ProtocolCommand =
        serde_json::from_str(r#"{"type":"cancel","request_id":"studio-2","msg_id":"m1"}"#).unwrap();
    assert_eq!(
        cancel,
        ProtocolCommand::Cancel {
            request_id: Some("studio-2".to_owned()),
            msg_id: "m1".to_owned(),
        }
    );

    let cancel_workflow: ProtocolCommand =
        serde_json::from_str(r#"{"type":"cancel_workflow","request_id":"studio-3","run_id":"run-1:workflow:w1"}"#)
            .unwrap();
    assert_eq!(
        cancel_workflow,
        ProtocolCommand::CancelWorkflow {
            request_id: Some("studio-3".to_owned()),
            run_id: "run-1:workflow:w1".to_owned(),
        }
    );
}
