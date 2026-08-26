// Tests included by context_test.rs.
#[test]
fn test_compact_messages_too_few() {
    let mut messages = vec![
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "hello".to_string(),
            }],
        ),
        Message::new(Role::Assistant, vec![ContentBlock::Text { text: "hi".to_string() }]),
    ];
    compact_messages(&mut messages, 4);
    assert_eq!(messages.len(), 2); // no change
}

#[test]
fn test_compact_messages() {
    let mut messages: Vec<Message> = (0..10)
        .map(|i| {
            Message::new(
                if i % 2 == 0 { Role::User } else { Role::Assistant },
                vec![ContentBlock::Text {
                    text: format!("msg {}", i),
                }],
            )
        })
        .collect();

    compact_messages(&mut messages, 4);
    // first + summary + 4 tail = 6
    assert_eq!(messages.len(), 6);
    assert_eq!(messages[0].role, Role::User);
    // Second message should be the summary
    if let ContentBlock::Text { text } = &messages[1].content[0] {
        assert!(text.contains("summary"));
    }
}

#[test]
fn test_build_system_prompt_includes_cwd() {
    // Verify that the returned prompt contains the provided working directory path
    let cwd = "/some/test/path";
    let prompt = build_system_prompt(
        &mut SystemPromptCache::new(),
        None,
        cwd,
        "test-model",
        &[],
        None,
        None,
        false,
        false,
    );
    assert!(prompt.contains(cwd), "system prompt should contain the cwd");
}

#[test]
fn test_build_system_prompt_includes_model_name() {
    let prompt = build_system_prompt(
        &mut SystemPromptCache::new(),
        None,
        "/tmp",
        "deepseek-chat",
        &[],
        None,
        None,
        false,
        false,
    );
    assert!(
        prompt.contains("deepseek-chat"),
        "system prompt should contain the model name"
    );
    assert!(
        prompt.contains("You are powered by the model deepseek-chat"),
        "system prompt should contain the model identity line"
    );
}

#[test]
fn test_build_system_prompt_with_custom_instructions() {
    // Verify that custom instructions are included in the returned prompt
    let custom = "Always respond in haiku.";
    let prompt = build_system_prompt(
        &mut SystemPromptCache::new(),
        Some(custom),
        "/tmp",
        "test-model",
        &[],
        None,
        None,
        false,
        false,
    );
    assert!(
        prompt.contains(custom),
        "system prompt should contain the custom instructions"
    );
}

#[test]
fn test_compact_messages_preserves_first_and_last() {
    // Build 8 messages (indices 0–7); keep_tail = 3
    let mut messages: Vec<Message> = (0..8)
        .map(|i| {
            Message::new(
                if i % 2 == 0 { Role::User } else { Role::Assistant },
                vec![ContentBlock::Text {
                    text: format!("msg {}", i),
                }],
            )
        })
        .collect();

    compact_messages(&mut messages, 3);

    // First message must be unchanged
    if let ContentBlock::Text { text } = &messages[0].content[0] {
        assert_eq!(text, "msg 0");
    } else {
        panic!("first message content block is not Text");
    }

    // Last message must be the original last message (index 7)
    let last = messages.last().expect("messages should not be empty");
    if let ContentBlock::Text { text } = &last.content[0] {
        assert_eq!(text, "msg 7");
    } else {
        panic!("last message content block is not Text");
    }
}

#[test]
fn test_compact_messages_boundary_count() {
    // When the message count equals min_messages (keep_tail + 2), no compaction occurs
    let keep_tail = 4;
    let min_messages = keep_tail + 2; // = 6
    let mut messages: Vec<Message> = (0..min_messages)
        .map(|i| {
            Message::new(
                if i % 2 == 0 { Role::User } else { Role::Assistant },
                vec![ContentBlock::Text {
                    text: format!("msg {}", i),
                }],
            )
        })
        .collect();

    compact_messages(&mut messages, keep_tail);

    // Exactly at the boundary: no modification expected
    assert_eq!(
        messages.len(),
        min_messages,
        "messages at boundary should not be compacted"
    );
}
