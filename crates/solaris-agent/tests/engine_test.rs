mod common;

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{Value, json};
use solaris_agent::engine::AgentEngine;
use solaris_agent::error::AgentError;
use solaris_agent::output::OutputSink;
use solaris_agent::output::terminal::TerminalSink;
use solaris_agent::session::SessionManager;
use solaris_providers::{LlmProvider, ProviderError};
use solaris_tools::registry::ToolRegistry;
use solaris_types::llm::{LlmEvent, LlmRequest};
use solaris_types::message::{ContentBlock, Message, Role, StopReason, TokenUsage};
use solaris_types::spawner::AgentOutcomeStatus;
use tempfile::tempdir;
use tokio::sync::mpsc;

use common::{MockLlmProvider, MockTool, test_config};

// ---------------------------------------------------------------------------
// Helper: build a no-color OutputFormatter for silent test output
// ---------------------------------------------------------------------------
fn silent_output() -> Arc<dyn OutputSink> {
    Arc::new(TerminalSink::new(true))
}

fn tool_call_malformed_turn(id: &str, name: &str, input: Value) -> Vec<LlmEvent> {
    vec![
        LlmEvent::ToolUse {
            id: id.to_string(),
            name: name.to_string(),
            input,
            extra: None,
        },
        LlmEvent::Done {
            stop_reason: StopReason::ToolUse,
            usage: TokenUsage {
                input_tokens: 50,
                output_tokens: 20,
                cache_creation_tokens: 0,
                cache_read_tokens: 0,
            },
        },
    ]
}

fn tool_call_failure_turn(id: &str) -> Vec<LlmEvent> {
    vec![
        LlmEvent::ToolUse {
            id: id.to_string(),
            name: "mock_tool".to_string(),
            input: json!({}),
            extra: None,
        },
        LlmEvent::Done {
            stop_reason: StopReason::ToolUse,
            usage: TokenUsage::default(),
        },
    ]
}

#[test]
fn structured_host_history_is_persisted_once_without_flattening_roles() {
    let directory = tempdir().unwrap();
    let mut config = test_config();
    config.session.enabled = true;
    config.session.directory = directory.path().to_string_lossy().into_owned();
    let mut engine = AgentEngine::new_with_provider(
        Arc::new(MockLlmProvider::with_text_response("unused")),
        config,
        ToolRegistry::new(),
        silent_output(),
        std::env::temp_dir(),
    );
    engine
        .init_session("test-provider", "workspace", Some("history-session"))
        .unwrap();
    let history = vec![
        Message::new(Role::User, vec![ContentBlock::Text { text: "first".into() }]),
        Message::new(Role::Assistant, vec![ContentBlock::Text { text: "second".into() }]),
    ];

    assert_eq!(engine.import_history(history.clone()).unwrap(), 2);
    assert!(engine.import_history(history).unwrap_err().contains("duplicate"));
    let restored = SessionManager::new(directory.path().to_path_buf(), 10)
        .load("history-session")
        .unwrap();
    assert_eq!(restored.messages.len(), 2);
    assert_eq!(restored.messages[0].role, Role::User);
    assert_eq!(restored.messages[1].role, Role::Assistant);
}

#[derive(Default)]
struct RecordingOutputSink {
    tool_calls: Mutex<Vec<(String, String)>>,
    tool_results: Mutex<Vec<(String, String, bool)>>,
}

impl OutputSink for RecordingOutputSink {
    fn emit_text_delta(&self, _text: &str, _msg_id: &str) {}
    fn emit_thinking(&self, _text: &str, _msg_id: &str) {}

    fn emit_tool_call(&self, tool_use_id: &str, name: &str, _input: &str) {
        self.tool_calls
            .lock()
            .unwrap()
            .push((tool_use_id.to_owned(), name.to_owned()));
    }

    fn emit_tool_result(&self, tool_use_id: &str, name: &str, is_error: bool, _content: &str) {
        self.tool_results
            .lock()
            .unwrap()
            .push((tool_use_id.to_owned(), name.to_owned(), is_error));
    }

    fn emit_stream_start(&self, _msg_id: &str) {}
    fn emit_stream_end(
        &self,
        _msg_id: &str,
        _turns: usize,
        _input_tokens: u64,
        _output_tokens: u64,
        _cache_creation_tokens: u64,
        _cache_read_tokens: u64,
    ) {
    }
    fn emit_error(&self, _msg: &str) {}
    fn emit_info(&self, _msg: &str) {}
}

struct RecordingRequestProvider {
    requests: Arc<Mutex<Vec<Vec<Message>>>>,
    responses: Mutex<Vec<Vec<LlmEvent>>>,
}

impl RecordingRequestProvider {
    fn new(responses: Vec<Vec<LlmEvent>>) -> Self {
        Self {
            requests: Arc::new(Mutex::new(Vec::new())),
            responses: Mutex::new(responses),
        }
    }

    fn requests(&self) -> Arc<Mutex<Vec<Vec<Message>>>> {
        Arc::clone(&self.requests)
    }
}

#[async_trait]
impl LlmProvider for RecordingRequestProvider {
    async fn stream(&self, request: &LlmRequest) -> Result<mpsc::Receiver<LlmEvent>, ProviderError> {
        self.requests.lock().unwrap().push(request.messages.clone());
        let events = self.responses.lock().unwrap().remove(0);
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            for event in events {
                let _ = tx.send(event).await;
            }
        });
        Ok(rx)
    }
}

#[derive(Debug, Clone)]
struct RecordedRequest {
    messages: Vec<Message>,
    tool_count: usize,
}

struct FullRecordingRequestProvider {
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    responses: Mutex<Vec<Vec<LlmEvent>>>,
}

impl FullRecordingRequestProvider {
    fn new(responses: Vec<Vec<LlmEvent>>) -> Self {
        Self {
            requests: Arc::new(Mutex::new(Vec::new())),
            responses: Mutex::new(responses),
        }
    }

    fn requests(&self) -> Arc<Mutex<Vec<RecordedRequest>>> {
        Arc::clone(&self.requests)
    }
}

#[async_trait]
impl LlmProvider for FullRecordingRequestProvider {
    async fn stream(&self, request: &LlmRequest) -> Result<mpsc::Receiver<LlmEvent>, ProviderError> {
        self.requests.lock().unwrap().push(RecordedRequest {
            messages: request.messages.clone(),
            tool_count: request.tools.len(),
        });
        let events = self.responses.lock().unwrap().remove(0);
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            for event in events {
                let _ = tx.send(event).await;
            }
        });
        Ok(rx)
    }
}

fn contains_empty_assistant_message(messages: &[Message]) -> bool {
    messages
        .iter()
        .any(|message| message.role == Role::Assistant && message.content.is_empty())
}

// ---------------------------------------------------------------------------
// test_engine_text_response_ends_turn
//
// Verifies that when the LLM returns a pure text response the engine:
//   - captures the full text
//   - reports StopReason::EndTurn
//   - completes in a single turn
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_engine_text_response_ends_turn() {
    let provider = Arc::new(MockLlmProvider::with_text_response("Hello, world!"));
    let config = test_config();
    let registry = ToolRegistry::new();
    let output = silent_output();

    let mut engine = AgentEngine::new_with_provider(provider, config, registry, output, std::env::temp_dir());
    let result = engine.run("Hi", "").await.expect("engine should succeed");

    assert_eq!(result.text, "Hello, world!");
    assert_eq!(result.stop_reason, StopReason::EndTurn);
    assert_eq!(result.turns, 1);
}

// ---------------------------------------------------------------------------
// test_engine_tool_use_executes_and_continues
//
// Verifies the agentic loop when the LLM first requests a tool then, after
// receiving the tool result, produces a final text answer.
//   - Turn 1: LLM emits ToolUse for "mock_tool"
//   - Turn 2: LLM emits TextDelta("Done") + EndTurn
//   - result.turns == 2 and result.text == "Done"
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_engine_tool_use_executes_and_continues() {
    let turn1 = vec![
        LlmEvent::ToolUse {
            id: "tool-1".to_string(),
            name: "mock_tool".to_string(),
            input: json!({}),
            extra: None,
        },
        LlmEvent::Done {
            stop_reason: StopReason::ToolUse,
            usage: TokenUsage {
                input_tokens: 80,
                output_tokens: 30,
                cache_creation_tokens: 0,
                cache_read_tokens: 0,
            },
        },
    ];
    let turn2 = vec![
        LlmEvent::TextDelta("Done".to_string()),
        LlmEvent::Done {
            stop_reason: StopReason::EndTurn,
            usage: TokenUsage {
                input_tokens: 100,
                output_tokens: 50,
                cache_creation_tokens: 0,
                cache_read_tokens: 0,
            },
        },
    ];

    let provider = Arc::new(MockLlmProvider::with_turns(vec![turn1, turn2]));
    let config = test_config();
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(MockTool::new("mock_tool", "tool output", false)));
    let output = silent_output();

    let mut engine = AgentEngine::new_with_provider(provider, config, registry, output, std::env::temp_dir());
    let result = engine.run("Use the tool", "").await.expect("engine should succeed");

    assert_eq!(result.turns, 2);
    assert_eq!(result.text, "Done");
}

#[tokio::test]
async fn test_engine_round_trips_thinking_signature_into_tool_followup_request() {
    let provider = Arc::new(RecordingRequestProvider::new(vec![
        vec![
            LlmEvent::ThinkingDelta("need a tool".to_string()),
            LlmEvent::ThinkingSignature("sig-123".to_string()),
            LlmEvent::ProviderMetadata {
                namespace: "anthropic".to_owned(),
                value: json!({"thinking_signature": "sig-123"}),
            },
            LlmEvent::ProviderMetadata {
                namespace: "custom-provider".to_owned(),
                value: json!({"opaque": {"version": 7}}),
            },
            LlmEvent::ToolUse {
                id: "call_1".to_string(),
                name: "mock_tool".to_string(),
                input: json!({}),
                extra: None,
            },
            LlmEvent::Done {
                stop_reason: StopReason::ToolUse,
                usage: TokenUsage::default(),
            },
        ],
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
    registry.register(Box::new(MockTool::new("mock_tool", "tool result", false)));

    let mut engine =
        AgentEngine::new_with_provider(provider, test_config(), registry, silent_output(), std::env::temp_dir());

    let result = engine.run("use tool", "").await.expect("engine should succeed");

    assert_eq!(result.text, "done");
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);

    let followup_messages = &requests[1];
    let assistant_message = followup_messages
        .iter()
        .find(|message| message.role == Role::Assistant)
        .expect("assistant message should be present");

    match &assistant_message.content[0] {
        ContentBlock::Thinking { thinking, signature } => {
            assert_eq!(thinking, "need a tool");
            assert_eq!(signature, &None);
        }
        other => panic!("expected thinking block, got {other:?}"),
    }
    assert_eq!(
        assistant_message.provider_metadata["anthropic"]["thinking_signature"],
        json!("sig-123")
    );
    assert_eq!(
        assistant_message.provider_metadata["custom-provider"],
        json!({"opaque": {"version": 7}})
    );
}

#[tokio::test]
async fn duplicate_tool_names_emit_distinct_tool_use_ids() {
    let turn1 = vec![
        LlmEvent::ToolUse {
            id: "call_a".to_string(),
            name: "Glob".to_string(),
            input: json!({"pattern": "*.rs"}),
            extra: None,
        },
        LlmEvent::ToolUse {
            id: "call_b".to_string(),
            name: "Glob".to_string(),
            input: json!({"pattern": "*.toml"}),
            extra: None,
        },
        LlmEvent::Done {
            stop_reason: StopReason::ToolUse,
            usage: TokenUsage {
                input_tokens: 80,
                output_tokens: 30,
                cache_creation_tokens: 0,
                cache_read_tokens: 0,
            },
        },
    ];
    let turn2 = vec![
        LlmEvent::TextDelta("Done".to_string()),
        LlmEvent::Done {
            stop_reason: StopReason::EndTurn,
            usage: TokenUsage {
                input_tokens: 100,
                output_tokens: 50,
                cache_creation_tokens: 0,
                cache_read_tokens: 0,
            },
        },
    ];

    let provider = Arc::new(MockLlmProvider::with_turns(vec![turn1, turn2]));
    let config = test_config();
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(MockTool::new("Glob", "tool output", false)));
    let output = Arc::new(RecordingOutputSink::default());

    let mut engine = AgentEngine::new_with_provider(provider, config, registry, output.clone(), std::env::temp_dir());
    let result = engine.run("Use Glob twice", "").await.expect("engine should succeed");

    assert_eq!(result.text, "Done");
    assert_eq!(
        *output.tool_calls.lock().unwrap(),
        vec![
            ("call_a".to_string(), "Glob".to_string()),
            ("call_b".to_string(), "Glob".to_string()),
        ]
    );
    assert_eq!(
        *output.tool_results.lock().unwrap(),
        vec![
            ("call_a".to_string(), "Glob".to_string(), false),
            ("call_b".to_string(), "Glob".to_string(), false),
        ]
    );
}

// ---------------------------------------------------------------------------
// test_engine_max_tokens_handling
//
// Verifies that a MaxTokens stop reason triggers a tool-capable
// continuation request and accumulates usage from both model calls.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_engine_max_tokens_handling() {
    let provider = Arc::new(FullRecordingRequestProvider::new(vec![
        vec![
            LlmEvent::TextDelta("partial ".to_string()),
            LlmEvent::Done {
                stop_reason: StopReason::MaxTokens,
                usage: TokenUsage {
                    input_tokens: 200,
                    output_tokens: 100,
                    cache_creation_tokens: 0,
                    cache_read_tokens: 0,
                },
            },
        ],
        vec![
            LlmEvent::TextDelta("finished".to_string()),
            LlmEvent::Done {
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage {
                    input_tokens: 20,
                    output_tokens: 10,
                    cache_creation_tokens: 0,
                    cache_read_tokens: 0,
                },
            },
        ],
    ]));
    let requests = provider.requests();

    let config = test_config();
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(MockTool::new("mock_tool", "tool output", false)));
    let output = silent_output();

    let mut engine = AgentEngine::new_with_provider(provider, config, registry, output, std::env::temp_dir());
    let result = engine
        .run("Give me a long answer", "")
        .await
        .expect("engine should succeed");

    assert_eq!(result.stop_reason, StopReason::EndTurn);
    assert_eq!(result.text, "partial finished");
    assert_eq!(result.turns, 1);
    assert_eq!(result.status, AgentOutcomeStatus::Completed);
    assert_eq!(result.usage.input_tokens, 220);
    assert_eq!(result.usage.output_tokens, 110);

    let recorded = requests.lock().unwrap();
    assert_eq!(recorded.len(), 2);
    assert!(recorded[1].tool_count > 0);
    let last_message = recorded[1]
        .messages
        .last()
        .expect("finalization request should include control prompt");
    assert_eq!(last_message.role, Role::User);
    assert!(
        matches!(
            &last_message.content[..],
            [ContentBlock::Text { text }] if text.contains("previous response was cut off")
        ),
        "finalization prompt should explain the max tokens continuation"
    );
}

#[tokio::test]
async fn empty_final_gets_one_visible_answer_nudge() {
    let provider = Arc::new(FullRecordingRequestProvider::new(vec![
        vec![LlmEvent::Done {
            stop_reason: StopReason::EndTurn,
            usage: TokenUsage::default(),
        }],
        vec![
            LlmEvent::TextDelta("Visible answer".to_string()),
            LlmEvent::Done {
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::default(),
            },
        ],
    ]));
    let requests = provider.requests();

    let config = test_config();
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(MockTool::new("mock_tool", "tool output", false)));
    let output = silent_output();

    let mut engine = AgentEngine::new_with_provider(provider, config, registry, output, std::env::temp_dir());
    let result = engine.run("Answer visibly", "").await.expect("engine should succeed");

    assert_eq!(result.text, "Visible answer");
    assert_eq!(result.stop_reason, StopReason::EndTurn);
    assert_eq!(result.turns, 1);

    let recorded = requests.lock().unwrap();
    assert_eq!(recorded.len(), 2);
    assert_eq!(recorded[1].tool_count, 0);
    assert!(
        !contains_empty_assistant_message(&recorded[1].messages),
        "empty finalization request must not include empty assistant content"
    );
    let last_message = recorded[1]
        .messages
        .last()
        .expect("finalization request should include control prompt");
    assert_eq!(last_message.role, Role::User);
    assert!(
        matches!(
            &last_message.content[..],
            [ContentBlock::Text { text }] if text.contains("visible answer text")
        ),
        "empty finalization prompt should ask for visible answer text"
    );
}

#[tokio::test]
async fn empty_final_falls_back_after_one_empty_retry() {
    let dir = tempdir().expect("tempdir should be created");
    let provider = Arc::new(FullRecordingRequestProvider::new(vec![
        vec![LlmEvent::Done {
            stop_reason: StopReason::EndTurn,
            usage: TokenUsage::default(),
        }],
        vec![LlmEvent::Done {
            stop_reason: StopReason::EndTurn,
            usage: TokenUsage::default(),
        }],
    ]));

    let mut config = test_config();
    config.session.enabled = true;
    config.session.directory = dir.path().to_string_lossy().into_owned();
    let registry = ToolRegistry::new();
    let output = silent_output();

    let mut engine = AgentEngine::new_with_provider(provider, config, registry, output, std::env::temp_dir());
    engine
        .init_session("test-provider", "/tmp", None)
        .expect("init_session should succeed");
    let result = engine
        .run("Answer visibly", "")
        .await
        .expect("engine should fall back successfully");

    assert_eq!(result.stop_reason, StopReason::EndTurn);
    assert_eq!(result.turns, 1);
    assert!(result.text.contains("finished without visible answer text"));
    assert_eq!(result.status, AgentOutcomeStatus::Failed);

    let session = SessionManager::new(dir.path().to_path_buf(), 10)
        .load("latest")
        .expect("session should be loadable");
    assert!(
        !contains_empty_assistant_message(&session.messages),
        "empty finalization fallback must not persist empty assistant content"
    );

    let last_message = session
        .messages
        .last()
        .expect("fallback assistant message should be persisted");
    assert_eq!(last_message.role, Role::Assistant);
    assert!(
        matches!(
            &last_message.content[..],
            [ContentBlock::Text { text }] if text == &result.text
        ),
        "fallback assistant message should be text-only"
    );
}

#[tokio::test]
async fn max_tokens_continuation_does_not_increment_reported_turns() {
    let provider = Arc::new(MockLlmProvider::with_turns(vec![
        vec![
            LlmEvent::TextDelta("partial ".to_string()),
            LlmEvent::Done {
                stop_reason: StopReason::MaxTokens,
                usage: TokenUsage::default(),
            },
        ],
        vec![
            LlmEvent::TextDelta("finished".to_string()),
            LlmEvent::Done {
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::default(),
            },
        ],
    ]));
    let mut config = test_config();
    config.max_turns = Some(1);
    let registry = ToolRegistry::new();
    let output = silent_output();

    let mut engine = AgentEngine::new_with_provider(provider, config, registry, output, std::env::temp_dir());
    let result = engine
        .run("Give me a long answer", "")
        .await
        .expect("engine should succeed");

    assert_eq!(result.text, "partial finished");
    assert_eq!(result.stop_reason, StopReason::EndTurn);
    assert_eq!(result.turns, 1);
}

#[tokio::test]
async fn max_tokens_continuation_can_execute_pending_tool_call() {
    let dir = tempdir().expect("tempdir should be created");
    let provider = Arc::new(FullRecordingRequestProvider::new(vec![
        vec![
            LlmEvent::TextDelta("partial ".to_string()),
            LlmEvent::Done {
                stop_reason: StopReason::MaxTokens,
                usage: TokenUsage::default(),
            },
        ],
        vec![
            LlmEvent::ToolUse {
                id: "pending-tool".to_string(),
                name: "mock_tool".to_string(),
                input: json!({}),
                extra: None,
            },
            LlmEvent::Done {
                stop_reason: StopReason::ToolUse,
                usage: TokenUsage::default(),
            },
        ],
        vec![
            LlmEvent::TextDelta("finished".to_string()),
            LlmEvent::Done {
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::default(),
            },
        ],
    ]));
    let requests = provider.requests();

    let mut config = test_config();
    config.session.enabled = true;
    config.session.directory = dir.path().to_string_lossy().into_owned();

    let mut registry = ToolRegistry::new();
    registry.register(Box::new(MockTool::new("mock_tool", "tool output", false)));

    let mut engine = AgentEngine::new_with_provider(provider, config, registry, silent_output(), std::env::temp_dir());
    engine
        .init_session("test-provider", "/tmp", None)
        .expect("init_session should succeed");

    let result = engine
        .run("Give me a long answer", "")
        .await
        .expect("engine should continue through the pending tool call");

    assert_eq!(result.stop_reason, StopReason::EndTurn);
    assert_eq!(result.turns, 2);
    assert_eq!(result.text, "finished");

    let recorded = requests.lock().unwrap();
    assert_eq!(recorded.len(), 3);
    assert!(
        recorded[0].tool_count > 0,
        "normal request should include registered tools"
    );
    assert!(
        recorded[1].tool_count > 0,
        "max-token continuation must retain tools when work is still pending"
    );
    drop(recorded);

    let session = SessionManager::new(dir.path().to_path_buf(), 10)
        .load("latest")
        .expect("session should be loadable");
    assert!(
        session.messages.iter().any(|message| {
            message.role == Role::Assistant
                && message
                    .content
                    .iter()
                    .any(|block| matches!(block, ContentBlock::ToolUse { id, .. } if id == "pending-tool"))
        }),
        "continued tool call must be persisted"
    );
    assert!(
        session.messages.iter().flat_map(|message| &message.content).any(
            |block| matches!(block, ContentBlock::ToolResult { tool_use_id, .. } if tool_use_id == "pending-tool")
        ),
        "continued tool result must be persisted"
    );

    let last_message = session
        .messages
        .last()
        .expect("fallback assistant message should be persisted");
    assert_eq!(last_message.role, Role::Assistant);
    assert!(
        matches!(
            &last_message.content[..],
        [ContentBlock::Text { text }] if text == "finished"
        ),
        "final assistant message should follow the continued tool round"
    );
}

// ---------------------------------------------------------------------------
// test_engine_message_accumulation
//
// Verifies that consecutive calls to `run` accumulate messages across turns.
// Session persistence is used to observe the messages externally since
// engine.messages is private.
//
// After two independent `run` calls the persisted session must contain
// exactly 4 messages: [user, assistant, user, assistant].
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_engine_message_accumulation() {
    let dir = tempdir().expect("tempdir should be created");

    // Provider needs two responses (one per run() call)
    let provider = Arc::new(MockLlmProvider::with_turns(vec![
        vec![
            LlmEvent::TextDelta("Response 1".to_string()),
            LlmEvent::Done {
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage {
                    input_tokens: 10,
                    output_tokens: 5,
                    cache_creation_tokens: 0,
                    cache_read_tokens: 0,
                },
            },
        ],
        vec![
            LlmEvent::TextDelta("Response 2".to_string()),
            LlmEvent::Done {
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage {
                    input_tokens: 10,
                    output_tokens: 5,
                    cache_creation_tokens: 0,
                    cache_read_tokens: 0,
                },
            },
        ],
    ]));

    let mut config = test_config();
    config.session.enabled = true;
    config.session.directory = dir.path().to_string_lossy().into_owned();

    let registry = ToolRegistry::new();
    let output = silent_output();

    let mut engine = AgentEngine::new_with_provider(provider, config.clone(), registry, output, std::env::temp_dir());

    // Initialize session so save_session() has a session to persist
    engine
        .init_session("test-provider", "/tmp", None)
        .expect("init_session should succeed");

    engine
        .run("First message", "message-1")
        .await
        .expect("first run should succeed");
    engine
        .run("Second message", "message-2")
        .await
        .expect("second run should succeed");

    // Load the persisted session and count accumulated messages
    let session_manager = SessionManager::new(dir.path().to_path_buf(), 10);
    let session = session_manager.load("latest").expect("session should be loadable");

    // Expected layout: user, assistant, user, assistant
    assert_eq!(
        session.messages.len(),
        4,
        "expected 4 messages (user+assistant for each run), got {}",
        session.messages.len()
    );
}

// ---------------------------------------------------------------------------
// test_engine_token_usage_tracking
//
// Verifies that token usage is accumulated correctly across multiple turns.
//   - Turn 1: ToolUse with usage(80 in, 30 out)
//   - Turn 2: EndTurn  with usage(100 in, 50 out)
//   - Expected total: input=180, output=80
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_engine_token_usage_tracking() {
    let turn1 = vec![
        LlmEvent::ToolUse {
            id: "tool-1".to_string(),
            name: "mock_tool".to_string(),
            input: json!({}),
            extra: None,
        },
        LlmEvent::Done {
            stop_reason: StopReason::ToolUse,
            usage: TokenUsage {
                input_tokens: 80,
                output_tokens: 30,
                cache_creation_tokens: 0,
                cache_read_tokens: 0,
            },
        },
    ];
    let turn2 = vec![
        LlmEvent::TextDelta("Final answer".to_string()),
        LlmEvent::Done {
            stop_reason: StopReason::EndTurn,
            usage: TokenUsage {
                input_tokens: 100,
                output_tokens: 50,
                cache_creation_tokens: 0,
                cache_read_tokens: 0,
            },
        },
    ];

    let provider = Arc::new(MockLlmProvider::with_turns(vec![turn1, turn2]));
    let config = test_config();
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(MockTool::new("mock_tool", "result", false)));
    let output = silent_output();

    let mut engine = AgentEngine::new_with_provider(provider, config, registry, output, std::env::temp_dir());
    let result = engine.run("Do work", "").await.expect("engine should succeed");

    assert_eq!(
        result.usage.input_tokens, 180,
        "input tokens should accumulate across turns"
    );
    assert_eq!(
        result.usage.output_tokens, 80,
        "output tokens should accumulate across turns"
    );
}

// ---------------------------------------------------------------------------
// test_engine_max_turns_runs_one_grace_finalization
//
// Verifies that exhausting max_turns after a tool round gets one tool-disabled
// finalization request before falling back to StopReason::MaxTurns.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_engine_max_turns_runs_one_grace_finalization() {
    let provider = Arc::new(FullRecordingRequestProvider::new(vec![
        vec![
            LlmEvent::ToolUse {
                id: "tool-1".to_string(),
                name: "mock_tool".to_string(),
                input: json!({}),
                extra: None,
            },
            LlmEvent::Done {
                stop_reason: StopReason::ToolUse,
                usage: TokenUsage {
                    input_tokens: 50,
                    output_tokens: 20,
                    cache_creation_tokens: 0,
                    cache_read_tokens: 0,
                },
            },
        ],
        vec![
            LlmEvent::TextDelta("Final from existing tool result".to_string()),
            LlmEvent::Done {
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage {
                    input_tokens: 10,
                    output_tokens: 5,
                    cache_creation_tokens: 0,
                    cache_read_tokens: 0,
                },
            },
        ],
    ]));
    let requests = provider.requests();

    let mut config = test_config();
    config.max_turns = Some(1);

    let mut registry = ToolRegistry::new();
    registry.register(Box::new(MockTool::new("mock_tool", "result", false)));
    let output = silent_output();

    let mut engine = AgentEngine::new_with_provider(provider, config, registry, output, std::env::temp_dir());
    let result = engine
        .run("Keep calling tools", "")
        .await
        .expect("engine should succeed with grace finalization");

    assert_eq!(result.stop_reason, StopReason::EndTurn);
    assert_eq!(result.text, "Final from existing tool result");
    assert_eq!(result.turns, 1);
    assert_eq!(result.status, AgentOutcomeStatus::Completed);

    let recorded = requests.lock().unwrap();
    assert_eq!(recorded.len(), 2);
    assert!(
        recorded[0].tool_count > 0,
        "normal request should include registered tools"
    );
    assert_eq!(recorded[1].tool_count, 0);
    let last_message = recorded[1]
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
async fn finalization_requests_can_be_asserted_without_tools() {
    let provider = Arc::new(FullRecordingRequestProvider::new(vec![vec![
        LlmEvent::TextDelta("done".to_string()),
        LlmEvent::Done {
            stop_reason: StopReason::EndTurn,
            usage: TokenUsage::default(),
        },
    ]]));
    let requests = provider.requests();

    let mut engine = AgentEngine::new_with_provider(
        provider,
        test_config(),
        ToolRegistry::new(),
        silent_output(),
        std::env::temp_dir(),
    );
    let result = engine
        .run("Say done", "")
        .await
        .expect("engine should return the final text");

    assert_eq!(result.text, "done");
    let recorded = requests.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].tool_count, 0);
    assert_eq!(recorded[0].messages.len(), 1);
}

include!("engine_test/engine_failure_test.rs");
include!("engine_test/engine_max_tokens_test.rs");
