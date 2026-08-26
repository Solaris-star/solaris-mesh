use solaris_protocol::commands::{DeliveryAcknowledgement, ProtocolCommand};
use solaris_protocol::delivery::{DeliveryMetadata, ProtocolEnvelope, canonical_event_bytes, canonical_event_digest};
use solaris_protocol::events::{ErrorInfo, OutputType, ProtocolEvent, ToolCategory, ToolInfo, ToolStatus};

fn metadata(digest: String) -> DeliveryMetadata {
    DeliveryMetadata {
        delivery_id: "delivery-1".to_owned(),
        msg_id: "message-1".to_owned(),
        run_epoch: 7,
        sequence: 11,
        digest,
    }
}

#[test]
fn delivery_envelope_adds_metadata_without_breaking_the_legacy_event_shape() {
    let event = ProtocolEvent::TextDelta {
        text: "hello".to_owned(),
        msg_id: "message-1".to_owned(),
    };
    let plain = serde_json::to_value(&event).unwrap();
    let payload = serde_json::to_vec(&event).unwrap();
    let digest = canonical_event_digest(plain.as_object().unwrap());
    let envelope = ProtocolEnvelope::from_event_payload(&payload, metadata(digest)).unwrap();
    let mut encoded = serde_json::to_value(envelope).unwrap();

    let delivery = encoded.get("delivery").unwrap();
    assert_eq!(delivery["delivery_id"], "delivery-1");
    assert_eq!(delivery["msg_id"], "message-1");
    assert_eq!(delivery["run_epoch"], 7);
    assert_eq!(delivery["sequence"], 11);
    encoded.as_object_mut().unwrap().remove("delivery");
    assert_eq!(encoded, plain);
}

#[test]
fn canonical_event_bytes_are_stable_across_recursive_field_order() {
    let left = serde_json::from_str::<serde_json::Value>(
        r#"{"type":"info","payload":{"label":"1","count":1},"items":[{"z":2,"a":true}]}"#,
    )
    .unwrap();
    let right = serde_json::from_str::<serde_json::Value>(
        r#"{"items":[{"a":true,"z":2}],"payload":{"count":1,"label":"1"},"type":"info"}"#,
    )
    .unwrap();

    let left_bytes = canonical_event_bytes(left.as_object().unwrap());
    let right_bytes = canonical_event_bytes(right.as_object().unwrap());

    assert_eq!(left_bytes, right_bytes);
    assert_eq!(
        left_bytes,
        br#"{"items":[{"a":true,"z":2}],"payload":{"count":1,"label":"1"},"type":"info"}"#
    );
    assert_eq!(
        canonical_event_digest(left.as_object().unwrap()),
        canonical_event_digest(right.as_object().unwrap())
    );
    assert_eq!(
        canonical_event_digest(left.as_object().unwrap()),
        "sha256:2d21fdbf242f6b48866e618320a9ba4b915202b3dab28d3e2fde45bb6c439984"
    );
}

#[test]
fn canonical_event_digest_changes_when_payload_semantics_change() {
    let first = serde_json::json!({
        "type": "info",
        "msg_id": "message-1",
        "message": "first",
    });
    let second = serde_json::json!({
        "type": "info",
        "msg_id": "message-1",
        "message": "second",
    });

    assert_ne!(
        canonical_event_digest(first.as_object().unwrap()),
        canonical_event_digest(second.as_object().unwrap())
    );
}

#[test]
fn canonical_event_bytes_preserve_json_types_and_omit_only_top_level_delivery() {
    let mut event = serde_json::json!({
        "integer": -7,
        "decimal": 1.25,
        "text": "quote: \"; newline:\n; unicode: 雪",
        "nested": {"delivery": "event-data"},
    });
    event
        .as_object_mut()
        .unwrap()
        .insert("delivery".to_owned(), serde_json::json!({"digest": "transport-only"}));
    let canonical = canonical_event_bytes(event.as_object().unwrap());
    let decoded: serde_json::Value = serde_json::from_slice(&canonical).unwrap();

    assert_eq!(
        decoded,
        serde_json::json!({
            "integer": -7,
            "decimal": 1.25,
            "text": "quote: \"; newline:\n; unicode: 雪",
            "nested": {"delivery": "event-data"},
        })
    );
}

#[test]
fn visible_events_expose_a_stable_delivery_message_id() {
    let text = ProtocolEvent::TextDelta {
        text: "a".to_owned(),
        msg_id: "text-message".to_owned(),
    };
    let thinking = ProtocolEvent::Thinking {
        text: "b".to_owned(),
        msg_id: "thinking-message".to_owned(),
    };
    let stream_end = ProtocolEvent::StreamEnd {
        msg_id: "end-message".to_owned(),
        usage: None,
    };
    let error = ProtocolEvent::Error {
        msg_id: Some("error-message".to_owned()),
        error: ErrorInfo {
            code: "test".to_owned(),
            message: "safe".to_owned(),
            retryable: false,
        },
    };
    let info = ProtocolEvent::Info {
        msg_id: "info-message".to_owned(),
        message: "safe".to_owned(),
    };
    let tool_request = ProtocolEvent::ToolRequest {
        msg_id: "tool-message".to_owned(),
        call_id: "call-1".to_owned(),
        run_id: None,
        agent_id: None,
        operation_id: None,
        effect_id: None,
        tool: ToolInfo {
            name: "Read".to_owned(),
            category: ToolCategory::Info,
            args: serde_json::json!({}),
            effect: None,
            description: "read".to_owned(),
        },
    };
    let tool_result = ProtocolEvent::ToolResult {
        msg_id: "tool-message".to_owned(),
        call_id: "call-1".to_owned(),
        tool_name: "Read".to_owned(),
        status: ToolStatus::Executed,
        output: "ok".to_owned(),
        output_type: OutputType::Text,
        metadata: None,
    };

    assert_eq!(text.delivery_msg_id(), "text-message");
    assert_eq!(thinking.delivery_msg_id(), "thinking-message");
    assert_eq!(stream_end.delivery_msg_id(), "end-message");
    assert_eq!(error.delivery_msg_id(), "error-message");
    assert_eq!(info.delivery_msg_id(), "info-message");
    assert_eq!(tool_request.delivery_msg_id(), "tool-message");
    assert_eq!(tool_result.delivery_msg_id(), "tool-message");
}

#[test]
fn acknowledgement_command_carries_the_full_delivery_fence() {
    let command: ProtocolCommand = serde_json::from_value(serde_json::json!({
        "type": "acknowledge_delivery",
        "session_id": "session-1",
        "run_epoch": 7,
        "delivery_id": "delivery-1",
        "digest": format!("sha256:{}", "a".repeat(64)),
    }))
    .unwrap();

    assert_eq!(
        command,
        ProtocolCommand::AcknowledgeDelivery(DeliveryAcknowledgement {
            session_id: "session-1".to_owned(),
            run_epoch: 7,
            delivery_id: "delivery-1".to_owned(),
            digest: format!("sha256:{}", "a".repeat(64)),
        })
    );
}
