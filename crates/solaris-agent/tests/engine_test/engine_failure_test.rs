#[tokio::test]
async fn repeated_tool_call_failure_turns_stop_before_another_provider_request() {
    let provider = Arc::new(RecordingRequestProvider::new(vec![
        tool_call_failure_turn("tool-1"),
        tool_call_failure_turn("tool-2"),
        tool_call_failure_turn("tool-3"),
        vec![
            LlmEvent::TextDelta("should not be requested".to_string()),
            LlmEvent::Done {
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::default(),
            },
        ],
    ]));
    let requests = provider.requests();

    let mut config = test_config();
    config.max_turns = Some(10);

    let mut registry = ToolRegistry::new();
    registry.register(Box::new(MockTool::new("mock_tool", "permission denied", true)));

    let mut engine = AgentEngine::new_with_provider(provider, config, registry, silent_output(), std::env::temp_dir());
    let err = engine
        .run("keep retrying a failing tool", "")
        .await
        .expect_err("engine should stop repeated tool-call-failure loops");

    assert!(
        err.to_string().contains("consecutive tool-call failures"),
        "unexpected error: {err}"
    );
    assert_eq!(
        requests.lock().unwrap().len(),
        3,
        "fourth provider request must not be sent"
    );
}

#[tokio::test]
async fn repeated_tool_call_failure_threshold_one_stops_immediately() {
    let provider = Arc::new(RecordingRequestProvider::new(vec![
        tool_call_failure_turn("tool-1"),
        tool_call_failure_turn("tool-2"),
    ]));
    let requests = provider.requests();

    let mut config = test_config();
    config.max_turns = Some(10);
    config.max_tool_call_failure_turns = Some(1);

    let mut registry = ToolRegistry::new();
    registry.register(Box::new(MockTool::new("mock_tool", "permission denied", true)));

    let mut engine = AgentEngine::new_with_provider(provider, config, registry, silent_output(), std::env::temp_dir());
    let err = engine
        .run("keep retrying a failing tool", "")
        .await
        .expect_err("engine should stop repeated tool-call-failure loops");

    assert!(matches!(err, AgentError::ToolCallFailures { count: 1, limit: 1 }));
    assert_eq!(requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn repeated_tool_call_failure_disabled_runs_grace_finalization() {
    let provider = Arc::new(FullRecordingRequestProvider::new(vec![
        tool_call_failure_turn("tool-1"),
        tool_call_failure_turn("tool-2"),
        vec![
            LlmEvent::TextDelta("Final after tool-call failures".to_string()),
            LlmEvent::Done {
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::default(),
            },
        ],
    ]));
    let requests = provider.requests();

    let mut config = test_config();
    config.max_turns = Some(2);
    config.max_tool_call_failure_turns = Some(0);

    let mut registry = ToolRegistry::new();
    registry.register(Box::new(MockTool::new("mock_tool", "permission denied", true)));

    let mut engine = AgentEngine::new_with_provider(provider, config, registry, silent_output(), std::env::temp_dir());
    let result = engine
        .run("keep retrying a failing tool", "")
        .await
        .expect("engine should stop cleanly");

    assert_eq!(result.stop_reason, StopReason::EndTurn);
    assert_eq!(result.text, "Final after tool-call failures");
    assert_eq!(result.turns, 2);

    let recorded = requests.lock().unwrap();
    assert_eq!(recorded.len(), 3);
    assert!(
        recorded[0].tool_count > 0,
        "normal requests should include registered tools"
    );
    assert_eq!(recorded[2].tool_count, 0);
    let last_message = recorded[2]
        .messages
        .last()
        .expect("grace finalization request should include control prompt");
    assert_eq!(last_message.role, Role::User);
    assert!(
        matches!(
            &last_message.content[..],
            [ContentBlock::Text { text }] if text.contains("Do not call any more tools")
        ),
        "grace finalization prompt should forbid more tool calls"
    );
}

#[tokio::test]
async fn repeated_tool_call_malformed_stops_on_default_third_turn() {
    let dir = tempdir().expect("tempdir should be created");
    let provider = Arc::new(RecordingRequestProvider::new(vec![
        tool_call_malformed_turn("bad", "", json!({})),
        tool_call_malformed_turn("bad", "", json!({})),
        tool_call_malformed_turn("bad", "", json!({})),
        vec![
            LlmEvent::TextDelta("should not be requested".to_string()),
            LlmEvent::Done {
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::default(),
            },
        ],
    ]));
    let requests = provider.requests();

    let mut config = test_config();
    config.max_tool_call_malformed_turns = None;
    config.session.enabled = true;
    config.session.directory = dir.path().to_string_lossy().into_owned();

    let mut engine = AgentEngine::new_with_provider(
        provider,
        config,
        ToolRegistry::new(),
        silent_output(),
        std::env::temp_dir(),
    );
    engine
        .init_session("test-provider", "/tmp", None)
        .expect("init_session should succeed");

    let err = engine
        .run("repeat malformed", "")
        .await
        .expect_err("engine should surface repeated tool-call-malformed loop");

    assert!(matches!(err, AgentError::ToolCallMalformed { count: 3, limit: 3 }));
    assert_eq!(
        requests.lock().unwrap().len(),
        3,
        "fourth provider request must not be sent"
    );

    let session = SessionManager::new(dir.path().to_path_buf(), 10)
        .load("latest")
        .expect("session should be loadable");
    let tool_uses = session
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .filter(|block| matches!(block, ContentBlock::ToolUse { .. }))
        .count();
    let tool_results: Vec<_> = session
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|block| {
            let ContentBlock::ToolResult {
                tool_use_id,
                content,
                is_error,
            } = block
            else {
                return None;
            };
            Some((tool_use_id, content, is_error))
        })
        .collect();

    assert_eq!(tool_uses, 3);
    assert_eq!(tool_results.len(), 3);
    assert!(
        tool_results.iter().all(|(id, content, is_error)| {
            id.as_str() == "bad" && **is_error && content.contains("Malformed tool call: empty function name")
        }),
        "tool-call malformed uses should have paired synthetic error results"
    );
}

#[tokio::test]
async fn repeated_tool_call_malformed_threshold_one_stops_immediately() {
    let provider = Arc::new(RecordingRequestProvider::new(vec![
        tool_call_malformed_turn("bad", "", json!({})),
        tool_call_malformed_turn("bad", "", json!({})),
    ]));
    let requests = provider.requests();

    let mut config = test_config();
    config.max_tool_call_malformed_turns = Some(1);

    let mut engine = AgentEngine::new_with_provider(
        provider,
        config,
        ToolRegistry::new(),
        silent_output(),
        std::env::temp_dir(),
    );
    let err = engine
        .run("repeat malformed", "")
        .await
        .expect_err("engine should surface repeated tool-call-malformed loop");

    assert!(matches!(err, AgentError::ToolCallMalformed { count: 1, limit: 1 }));
    assert_eq!(requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn repeated_tool_call_malformed_disabled_runs_grace_finalization() {
    let provider = Arc::new(FullRecordingRequestProvider::new(vec![
        tool_call_malformed_turn("bad", "", json!({})),
        tool_call_malformed_turn("bad", "", json!({})),
        vec![
            LlmEvent::TextDelta("Final after malformed attempts".to_string()),
            LlmEvent::Done {
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::default(),
            },
        ],
    ]));
    let requests = provider.requests();

    let mut config = test_config();
    config.max_tool_call_malformed_turns = Some(0);
    config.max_turns = Some(2);

    let mut registry = ToolRegistry::new();
    registry.register(Box::new(MockTool::new("mock_tool", "unused", false)));

    let mut engine = AgentEngine::new_with_provider(provider, config, registry, silent_output(), std::env::temp_dir());
    let result = engine
        .run("repeat malformed", "")
        .await
        .expect("engine should stop cleanly");

    assert_eq!(result.stop_reason, StopReason::EndTurn);
    assert_eq!(result.text, "Final after malformed attempts");
    assert_eq!(result.turns, 2);

    let recorded = requests.lock().unwrap();
    assert_eq!(recorded.len(), 3);
    assert!(
        recorded[0].tool_count > 0,
        "normal requests should include registered tools"
    );
    assert_eq!(recorded[2].tool_count, 0);
    let last_message = recorded[2]
        .messages
        .last()
        .expect("grace finalization request should include control prompt");
    assert_eq!(last_message.role, Role::User);
    assert!(
        matches!(
            &last_message.content[..],
            [ContentBlock::Text { text }] if text.contains("Do not call any more tools")
        ),
        "grace finalization prompt should forbid more tool calls"
    );
}

#[tokio::test]
async fn mixed_valid_and_tool_call_malformed_calls_do_not_trip_breaker() {
    let mixed_turn = || {
        vec![
            LlmEvent::ToolUse {
                id: "bad".to_string(),
                name: "".to_string(),
                input: json!({}),
                extra: None,
            },
            LlmEvent::ToolUse {
                id: "ok".to_string(),
                name: "mock_tool".to_string(),
                input: json!({}),
                extra: None,
            },
            LlmEvent::Done {
                stop_reason: StopReason::ToolUse,
                usage: TokenUsage::default(),
            },
        ]
    };
    let provider = Arc::new(RecordingRequestProvider::new(vec![
        mixed_turn(),
        mixed_turn(),
        vec![
            LlmEvent::TextDelta("done".to_string()),
            LlmEvent::Done {
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::default(),
            },
        ],
    ]));
    let requests = provider.requests();

    let mut config = test_config();
    config.max_tool_call_malformed_turns = Some(1);
    let output = Arc::new(RecordingOutputSink::default());
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(MockTool::new("mock_tool", "tool output", false)));

    let mut engine = AgentEngine::new_with_provider(provider, config, registry, output.clone(), std::env::temp_dir());
    let result = engine
        .run("mixed tool calls", "")
        .await
        .expect("engine should reach final text");

    assert_eq!(result.text, "done");
    assert_eq!(result.turns, 3);
    assert_eq!(requests.lock().unwrap().len(), 3);
    assert_eq!(
        *output.tool_results.lock().unwrap(),
        vec![
            ("bad".to_string(), "".to_string(), true),
            ("ok".to_string(), "mock_tool".to_string(), false),
            ("bad".to_string(), "".to_string(), true),
            ("ok".to_string(), "mock_tool".to_string(), false),
        ]
    );
}

// ---------------------------------------------------------------------------
// test_engine_api_error_handling
//
// Verifies that an LlmEvent::Error propagates as AgentError::ApiError with
// the original error message intact.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_engine_api_error_handling() {
    let events = vec![LlmEvent::Error("test error".to_string())];

    let provider = Arc::new(MockLlmProvider::with_events(events));
    let config = test_config();
    let registry = ToolRegistry::new();
    let output = silent_output();

    let mut engine = AgentEngine::new_with_provider(provider, config, registry, output, std::env::temp_dir());
    let err = engine
        .run("Hello", "")
        .await
        .map(|_| panic!("expected error, got Ok"))
        .unwrap_err();

    match err {
        AgentError::OutcomeUnknown(msg) => {
            assert!(msg.contains("provider stream terminated after request execution"));
            assert!(msg.contains("test error"));
        }
        other => panic!("expected OutcomeUnknown, got: {other:?}"),
    }
}
