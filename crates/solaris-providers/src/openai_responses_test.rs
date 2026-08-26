use super::*;
use solaris_types::message::{Message, Role};
use solaris_types::tool::ToolDef;

fn request() -> LlmRequest {
    LlmRequest {
        model: "gpt-test".into(),
        system: "system".into(),
        messages: vec![Message::new(
            Role::User,
            vec![ContentBlock::Text { text: "hello".into() }],
        )],
        tools: vec![ToolDef {
            name: "Read".into(),
            description: "read".into(),
            input_schema: json!({"type":"object"}),
            deferred: false,
        }],
        max_tokens: Some(100),
        thinking: None,
        reasoning_effort: Some("high".into()),
    }
}

#[test]
fn projects_responses_request_shape() {
    let compat = ProviderCompat::openai_defaults();
    let body = project_responses_request(&request(), &compat).unwrap();
    assert_eq!(body["model"], "gpt-test");
    assert_eq!(body["instructions"], "system");
    assert_eq!(body["input"][0]["role"], "user");
    assert_eq!(body["tools"][0]["type"], "function");
    assert_eq!(body["max_output_tokens"], 100);
}

#[test]
fn output_item_function_call_maps_to_tool_use() {
    let event = json!({
        "item": {
            "id":"fc_1",
            "type":"function_call",
            "call_id":"call_1",
            "name":"Read",
            "arguments":"{\"file_path\":\"a\"}"
        }
    });
    let mut saw = false;
    let mapped = map_output_item_done(&event, &mut saw);
    assert!(saw);
    assert!(matches!(
        &mapped[0],
        LlmEvent::ProviderMetadata { namespace, value }
            if namespace == "openai" && value["tool_calls"]["call_1"]["id"] == "fc_1"
    ));
    match &mapped[1] {
        LlmEvent::ToolUse { id, name, input, extra } => {
            assert_eq!(id, "call_1");
            assert_eq!(name, "Read");
            assert_eq!(input["file_path"], "a");
            assert!(extra.is_none());
        }
        _ => panic!("expected tool use"),
    }
}

#[test]
fn reasoning_item_emits_namespaced_metadata() {
    let event = json!({"item":{"id":"rs_1","type":"reasoning","encrypted_content":"opaque","summary":[]}});
    let mut saw = false;
    let mapped = map_output_item_done(&event, &mut saw);
    assert!(matches!(&mapped[0], LlmEvent::ThinkingSignature(_)));
    assert!(matches!(
        &mapped[1],
        LlmEvent::ProviderMetadata { namespace, value }
            if namespace == "openai" && value["reasoning_items"][0]["id"] == "rs_1"
    ));
}

#[test]
fn namespaced_metadata_replays_reasoning_and_tool_identity() {
    let mut request = request();
    let mut metadata = solaris_types::provider_contract::ProviderNativeMetadata::new();
    metadata.insert(
        "openai".into(),
        json!({
            "reasoning_items": [{"id":"rs_1","type":"reasoning","encrypted_content":"opaque"}],
            "tool_calls": {"call_1": {"id":"fc_1","type":"function_call"}}
        }),
    );
    request.messages = vec![
        Message::new(
            Role::Assistant,
            vec![
                ContentBlock::Thinking {
                    thinking: String::new(),
                    signature: None,
                },
                ContentBlock::ToolUse {
                    id: "call_1".into(),
                    name: "Read".into(),
                    input: json!({"file_path":"a"}),
                    extra: None,
                },
            ],
        )
        .with_provider_metadata(metadata),
    ];

    let body = project_responses_request(&request, &ProviderCompat::openai_defaults()).unwrap();
    let items = body["input"].as_array().unwrap();
    assert!(items.iter().any(|item| item["id"] == "rs_1"));
    let tool = items.iter().find(|item| item["type"] == "function_call").unwrap();
    assert_eq!(tool["id"], "fc_1");
    assert_eq!(tool["call_id"], "call_1");
}
