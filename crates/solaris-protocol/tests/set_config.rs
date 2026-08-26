use solaris_protocol::commands::ProtocolCommand;

#[test]
fn parse_set_config_with_model() {
    let json = r#"{"type":"set_config","model":"claude-sonnet-4-5-20250514"}"#;
    let cmd: ProtocolCommand = serde_json::from_str(json).unwrap();
    match cmd {
        ProtocolCommand::SetConfig { update, .. } => {
            assert_eq!(update.model.as_deref(), Some("claude-sonnet-4-5-20250514"));
        }
        other => panic!("expected SetConfig, got: {other:?}"),
    }
}

#[test]
fn parse_set_config_empty() {
    let json = r#"{"type":"set_config"}"#;
    let cmd: ProtocolCommand = serde_json::from_str(json).unwrap();
    match cmd {
        ProtocolCommand::SetConfig { update, .. } => {
            assert!(update.model.is_none());
        }
        other => panic!("expected SetConfig, got: {other:?}"),
    }
}

#[test]
fn parse_set_config_null_model() {
    let json = r#"{"type":"set_config","model":null}"#;
    let cmd: ProtocolCommand = serde_json::from_str(json).unwrap();
    match cmd {
        ProtocolCommand::SetConfig { update, .. } => {
            assert!(update.model.is_none());
        }
        other => panic!("expected SetConfig, got: {other:?}"),
    }
}

#[test]
fn parse_set_config_unknown_fields_ignored() {
    let json = r#"{"type":"set_config","model":"x","future_field":true,"nested":{"a":1}}"#;
    let cmd: ProtocolCommand = serde_json::from_str(json).unwrap();
    match cmd {
        ProtocolCommand::SetConfig { update, .. } => {
            assert_eq!(update.model.as_deref(), Some("x"));
        }
        other => panic!("expected SetConfig, got: {other:?}"),
    }
}

#[test]
fn existing_commands_still_parse() {
    // AC-7: Verify SetConfig addition doesn't break existing variants
    let message = r#"{"type":"message","msg_id":"m1","content":"hello"}"#;
    assert!(serde_json::from_str::<ProtocolCommand>(message).is_ok());

    let stop = r#"{"type":"stop"}"#;
    assert!(serde_json::from_str::<ProtocolCommand>(stop).is_ok());

    let approve = r#"{"type":"tool_approve","call_id":"c1"}"#;
    assert!(serde_json::from_str::<ProtocolCommand>(approve).is_ok());

    let deny = r#"{"type":"tool_deny","call_id":"c1"}"#;
    assert!(serde_json::from_str::<ProtocolCommand>(deny).is_ok());

    let init = r#"{"type":"init_history","text":"ctx"}"#;
    assert!(serde_json::from_str::<ProtocolCommand>(init).is_ok());

    let mode = r#"{"type":"set_mode","mode":"yolo"}"#;
    assert!(serde_json::from_str::<ProtocolCommand>(mode).is_ok());
}

// --- Cycle 2: Effort parsing tests ---

#[test]
fn parse_set_config_with_effort() {
    let json = r#"{"type":"set_config","effort":"high"}"#;
    let cmd: ProtocolCommand = serde_json::from_str(json).unwrap();
    match cmd {
        ProtocolCommand::SetConfig { update, .. } => {
            assert_eq!(update.effort.as_deref(), Some("high"));
            assert!(update.model.is_none());
        }
        other => panic!("expected SetConfig, got: {other:?}"),
    }
}

#[test]
fn parse_set_config_with_null_effort() {
    let json = r#"{"type":"set_config","effort":null}"#;
    let cmd: ProtocolCommand = serde_json::from_str(json).unwrap();
    match cmd {
        ProtocolCommand::SetConfig { update, .. } => {
            assert!(update.effort.is_none());
        }
        other => panic!("expected SetConfig, got: {other:?}"),
    }
}

// --- Cycle 2: Thinking parsing tests ---

#[test]
fn parse_set_config_with_thinking_enabled_and_budget() {
    let json = r#"{"type":"set_config","thinking":"enabled","thinking_budget":16000}"#;
    let cmd: ProtocolCommand = serde_json::from_str(json).unwrap();
    match cmd {
        ProtocolCommand::SetConfig { update, .. } => {
            assert_eq!(update.thinking.as_deref(), Some("enabled"));
            assert_eq!(update.thinking_budget, Some(16000));
        }
        other => panic!("expected SetConfig, got: {other:?}"),
    }
}

#[test]
fn parse_set_config_with_thinking_disabled() {
    let json = r#"{"type":"set_config","thinking":"disabled"}"#;
    let cmd: ProtocolCommand = serde_json::from_str(json).unwrap();
    match cmd {
        ProtocolCommand::SetConfig { update, .. } => {
            assert_eq!(update.thinking.as_deref(), Some("disabled"));
            assert!(update.thinking_budget.is_none());
        }
        other => panic!("expected SetConfig, got: {other:?}"),
    }
}

#[test]
fn parse_set_config_with_null_thinking() {
    let json = r#"{"type":"set_config","thinking":null}"#;
    let cmd: ProtocolCommand = serde_json::from_str(json).unwrap();
    match cmd {
        ProtocolCommand::SetConfig { update, .. } => {
            assert!(update.thinking.is_none());
        }
        other => panic!("expected SetConfig, got: {other:?}"),
    }
}

#[test]
fn parse_set_config_thinking_enabled_no_budget() {
    let json = r#"{"type":"set_config","thinking":"enabled"}"#;
    let cmd: ProtocolCommand = serde_json::from_str(json).unwrap();
    match cmd {
        ProtocolCommand::SetConfig { update, .. } => {
            assert_eq!(update.thinking.as_deref(), Some("enabled"));
            assert!(update.thinking_budget.is_none());
        }
        other => panic!("expected SetConfig, got: {other:?}"),
    }
}

// --- Cycle 2: Combined fields test ---

#[test]
fn parse_set_config_all_fields() {
    let json = r#"{"type":"set_config","model":"m","effort":"low","thinking":"disabled"}"#;
    let cmd: ProtocolCommand = serde_json::from_str(json).unwrap();
    match cmd {
        ProtocolCommand::SetConfig { update, .. } => {
            assert_eq!(update.model.as_deref(), Some("m"));
            assert_eq!(update.effort.as_deref(), Some("low"));
            assert_eq!(update.thinking.as_deref(), Some("disabled"));
            assert!(update.thinking_budget.is_none());
        }
        other => panic!("expected SetConfig, got: {other:?}"),
    }
}
