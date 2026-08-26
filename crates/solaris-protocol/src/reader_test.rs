use super::*;

#[test]
fn valid_protocol_command_is_preserved() {
    let input = parse_protocol_input(r#"{"type":"ping"}"#);

    assert!(matches!(input, ProtocolInput::Command(command) if *command == ProtocolCommand::Ping));
}

#[test]
fn invalid_command_preserves_only_a_safe_request_id() {
    let input = parse_protocol_input(
        r#"{"type":"set_intensity","request_id":"request-42","intensity":"not-valid","secret":"SECRET_SENTINEL"}"#,
    );
    let ProtocolInput::Invalid(error) = input else {
        panic!("invalid intensity must be reported");
    };

    let serialized = serde_json::to_string(&error.into_event()).unwrap();
    assert!(serialized.contains("request-42"));
    assert!(serialized.contains("protocol_error"));
    assert!(!serialized.contains("SECRET_SENTINEL"));
    assert!(!serialized.contains("not-valid"));
}

#[test]
fn unknown_command_uses_msg_id_as_a_safe_fallback_correlation() {
    let input = parse_protocol_input(r#"{"type":"future_command","msg_id":"message-7"}"#);
    let ProtocolInput::Invalid(error) = input else {
        panic!("unknown command must be reported");
    };

    let event = serde_json::to_value(error.into_event()).unwrap();
    assert_eq!(event["msg_id"], "message-7");
    assert_eq!(event["error"]["code"], "protocol_error");
}

#[test]
fn malformed_json_has_no_correlation_and_never_echoes_input() {
    let input = parse_protocol_input(r#"{"type":"ping","token":"SECRET_SENTINEL""#);
    let ProtocolInput::Invalid(error) = input else {
        panic!("malformed JSON must be reported");
    };

    let serialized = serde_json::to_string(&error.into_event()).unwrap();
    assert!(serialized.contains("protocol_error"));
    assert!(!serialized.contains("SECRET_SENTINEL"));
}
