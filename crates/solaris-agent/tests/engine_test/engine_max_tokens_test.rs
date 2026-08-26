#[tokio::test]
async fn repeated_max_tokens_responses_continue_until_a_visible_final() {
    let dir = tempdir().unwrap();
    let truncated = |text: &str| {
        vec![
            LlmEvent::TextDelta(text.to_string()),
            LlmEvent::Done {
                stop_reason: StopReason::MaxTokens,
                usage: TokenUsage::default(),
            },
        ]
    };
    let provider = Arc::new(FullRecordingRequestProvider::new(vec![
        truncated("part 1 "),
        truncated("part 2 "),
        truncated("part 3 "),
        vec![
            LlmEvent::TextDelta("done".to_string()),
            LlmEvent::Done {
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::default(),
            },
        ],
    ]));
    let requests = provider.requests();
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(MockTool::new("mock_tool", "tool output", false)));
    let mut engine = AgentEngine::new_with_provider(
        provider,
        test_config(),
        registry,
        silent_output(),
        dir.path().to_path_buf(),
    );

    let result = engine.run("Complete a long task", "max-tokens-complete").await.unwrap();

    assert_eq!(result.stop_reason, StopReason::EndTurn);
    assert_eq!(result.text, "part 1 part 2 part 3 done");
    assert_eq!(result.turns, 1, "continuations remain part of one logical turn");
    assert_eq!(result.status, AgentOutcomeStatus::Completed);
    let recorded = requests.lock().unwrap();
    assert_eq!(recorded.len(), 4);
    assert!(recorded.iter().skip(1).all(|request| request.tool_count > 0));
}

#[tokio::test]
async fn repeated_max_tokens_responses_stop_after_the_bounded_continuations() {
    let dir = tempdir().unwrap();
    let truncated = |text: &str| {
        vec![
            LlmEvent::TextDelta(text.to_string()),
            LlmEvent::Done {
                stop_reason: StopReason::MaxTokens,
                usage: TokenUsage::default(),
            },
        ]
    };
    let provider = Arc::new(FullRecordingRequestProvider::new(vec![
        truncated("part 1 "),
        truncated("part 2 "),
        truncated("part 3 "),
        truncated("part 4"),
    ]));
    let requests = provider.requests();
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(MockTool::new("mock_tool", "tool output", false)));
    let mut engine = AgentEngine::new_with_provider(
        provider,
        test_config(),
        registry,
        silent_output(),
        dir.path().to_path_buf(),
    );

    let result = engine.run("Complete a long task", "max-tokens-bounded").await.unwrap();

    assert_eq!(result.stop_reason, StopReason::MaxTokens);
    assert_eq!(result.text, "part 1 part 2 part 3 part 4");
    assert_eq!(result.status, AgentOutcomeStatus::Failed);
    assert_eq!(requests.lock().unwrap().len(), 4);
}
