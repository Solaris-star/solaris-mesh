use super::*;

#[test]
fn command_tool_deserialization_applies_default_timeout() {
    let definition = PluginCommandToolDefinition {
        name: "test".into(),
        description: "test".into(),
        input_schema: default_object_schema(),
        command: "plugin".into(),
        args: Vec::new(),
        effect: EffectDescriptor::read_only("test"),
        concurrency_safe: false,
        max_result_size: 1024,
        timeout_ms: 7,
    };
    let mut value = serde_json::to_value(definition).unwrap();
    value.as_object_mut().unwrap().remove("timeout_ms");

    let restored: PluginCommandToolDefinition = serde_json::from_value(value).unwrap();

    assert_eq!(restored.timeout_ms, default_plugin_timeout());
}

#[test]
fn provider_command_protocol_round_trips_provider_neutral_events() {
    let response = PluginProviderCommandResponse {
        events: vec![
            PluginProviderEvent::ToolUse {
                id: "call-1".to_owned(),
                name: "Read".to_owned(),
                input: serde_json::json!({"path": "README.md"}),
                extra: Some(serde_json::json!({"opaque": true})),
            },
            PluginProviderEvent::Done {
                stop_reason: StopReason::ToolUse,
                usage: TokenUsage {
                    input_tokens: 3,
                    output_tokens: 2,
                    cache_creation_tokens: 1,
                    cache_read_tokens: 4,
                },
            },
        ],
    };

    let value = serde_json::to_value(&response).unwrap();
    assert_eq!(value["events"][0]["type"], "tool_use");
    assert_eq!(value["events"][1]["usage"]["cache_read_tokens"], 4);
    let restored: PluginProviderCommandResponse = serde_json::from_value(value).unwrap();
    assert!(matches!(
        restored.events.as_slice(),
        [PluginProviderEvent::ToolUse { name, .. }, PluginProviderEvent::Done { .. }] if name == "Read"
    ));
}
