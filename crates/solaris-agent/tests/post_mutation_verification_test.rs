mod common;

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use solaris_agent::engine::AgentEngine;
use solaris_agent::output::OutputSink;
use solaris_agent::output::terminal::TerminalSink;
use solaris_protocol::events::ToolCategory;
use solaris_providers::{LlmProvider, ProviderError};
use solaris_tools::Tool;
use solaris_tools::registry::ToolRegistry;
use solaris_types::effect::EffectDescriptor;
use solaris_types::llm::{LlmEvent, LlmRequest};
use solaris_types::message::{ContentBlock, Message, Role, StopReason, TokenUsage};
use solaris_types::tool::ToolResult;
use tokio::sync::mpsc;

use common::test_config;

struct MaterialMockTool {
    is_error: bool,
}

#[async_trait]
impl Tool for MaterialMockTool {
    fn name(&self) -> &str {
        "edit_tool"
    }

    fn description(&self) -> &str {
        "Mock workspace edit"
    }

    fn input_schema(&self) -> serde_json::Value {
        serde_json::json!({"type": "object"})
    }

    fn is_concurrency_safe(&self, _input: &serde_json::Value) -> bool {
        false
    }

    fn describe_effect(&self, _input: &serde_json::Value) -> EffectDescriptor {
        EffectDescriptor::read_only("in-memory test edit")
    }

    fn category(&self) -> ToolCategory {
        ToolCategory::Edit
    }

    async fn execute(&self, _input: serde_json::Value) -> ToolResult {
        ToolResult {
            content: "changed".into(),
            is_error: self.is_error,
        }
    }
}

#[derive(Debug)]
struct RecordedRequest {
    system: String,
    messages: Vec<Message>,
    tool_names: Vec<String>,
}

struct RecordingProvider {
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    responses: Mutex<Vec<Vec<LlmEvent>>>,
}

impl RecordingProvider {
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
impl LlmProvider for RecordingProvider {
    async fn stream(&self, request: &LlmRequest) -> Result<mpsc::Receiver<LlmEvent>, ProviderError> {
        self.requests.lock().unwrap().push(RecordedRequest {
            system: request.system.clone(),
            messages: request.messages.clone(),
            tool_names: request.tools.iter().map(|tool| tool.name.clone()).collect(),
        });
        let events = self.responses.lock().unwrap().remove(0);
        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(async move {
            for event in events {
                let _ = tx.send(event).await;
            }
        });
        Ok(rx)
    }
}

fn tool_turn() -> Vec<LlmEvent> {
    vec![
        LlmEvent::ToolUse {
            id: "write-workspace".into(),
            name: "edit_tool".into(),
            input: serde_json::json!({}),
            extra: None,
        },
        LlmEvent::Done {
            stop_reason: StopReason::ToolUse,
            usage: TokenUsage::default(),
        },
    ]
}

fn final_turn(text: &str) -> Vec<LlmEvent> {
    vec![
        LlmEvent::TextDelta(text.into()),
        LlmEvent::Done {
            stop_reason: StopReason::EndTurn,
            usage: TokenUsage::default(),
        },
    ]
}

fn silent_output() -> Arc<dyn OutputSink> {
    Arc::new(TerminalSink::new(true))
}

struct RecordingOutput {
    text: Arc<Mutex<String>>,
}

impl OutputSink for RecordingOutput {
    fn emit_text_delta(&self, text: &str, _msg_id: &str) {
        self.text.lock().unwrap().push_str(text);
    }

    fn emit_thinking(&self, _text: &str, _msg_id: &str) {}

    fn emit_tool_call(&self, _tool_use_id: &str, _name: &str, _input: &str) {}

    fn emit_tool_result(&self, _tool_use_id: &str, _name: &str, _is_error: bool, _content: &str) {}

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

fn recording_output() -> (Arc<dyn OutputSink>, Arc<Mutex<String>>) {
    let text = Arc::new(Mutex::new(String::new()));
    (
        Arc::new(RecordingOutput {
            text: Arc::clone(&text),
        }),
        text,
    )
}

#[tokio::test]
async fn strong_effort_material_tool_round_adds_one_bounded_verification_prompt() {
    for effort in ["high", "xhigh", "x_high", "extra", "max", "ultra", "ultracode"] {
        let provider = Arc::new(RecordingProvider::new(vec![
            tool_turn(),
            final_turn("candidate final"),
            final_turn("verified final"),
        ]));
        let requests = provider.requests();
        let mut tools = ToolRegistry::new();
        tools.register(Box::new(MaterialMockTool { is_error: false }));
        let mut engine =
            AgentEngine::new_with_provider(provider, test_config(), tools, silent_output(), std::env::temp_dir());
        engine.set_initial_reasoning_effort(Some(effort.into()));

        let result = engine
            .run("Implement every rule in SPEC.md", &format!("audit-{effort}"))
            .await
            .unwrap();

        assert_eq!(result.text, "verified final");
        let recorded = requests.lock().unwrap();
        assert_eq!(
            recorded.len(),
            3,
            "verification must run only at the completion boundary"
        );
        assert!(!recorded[2].tool_names.is_empty(), "verification must retain tools");
        assert_eq!(recorded[0].system, recorded[2].system);
        assert_eq!(recorded[0].tool_names, recorded[2].tool_names);
        assert!(!recorded[1].messages.iter().any(|message| {
            message.content.iter().any(
                |block| matches!(block, ContentBlock::Text { text } if text.contains("every explicit requirement")),
            )
        }));
        assert!(matches!(
            recorded[2].messages.last(),
            Some(Message { role: Role::User, content, .. })
                if matches!(&content[..], [ContentBlock::Text { text }] if text.contains("every explicit requirement"))
        ));
    }
}

#[tokio::test]
async fn low_and_medium_effort_do_not_add_verification_prompt() {
    for effort in ["low", "medium"] {
        let provider = Arc::new(RecordingProvider::new(vec![tool_turn(), final_turn("fast final")]));
        let requests = provider.requests();
        let mut tools = ToolRegistry::new();
        tools.register(Box::new(MaterialMockTool { is_error: false }));
        let mut engine =
            AgentEngine::new_with_provider(provider, test_config(), tools, silent_output(), std::env::temp_dir());
        engine.set_initial_reasoning_effort(Some(effort.into()));

        let result = engine
            .run("Make a quick change", &format!("audit-{effort}"))
            .await
            .unwrap();

        assert_eq!(result.text, "fast final");
        let recorded = requests.lock().unwrap();
        assert_eq!(recorded.len(), 2);
        assert!(!recorded[1].messages.iter().any(|message| {
            message.content.iter().any(
                |block| matches!(block, ContentBlock::Text { text } if text.contains("every explicit requirement")),
            )
        }));
    }
}

#[tokio::test]
async fn high_effort_verification_prompt_is_bounded_and_can_repair_once() {
    let provider = Arc::new(RecordingProvider::new(vec![
        tool_turn(),
        final_turn("candidate final"),
        tool_turn(),
        final_turn("repaired final"),
    ]));
    let requests = provider.requests();
    let mut tools = ToolRegistry::new();
    tools.register(Box::new(MaterialMockTool { is_error: false }));
    let mut engine =
        AgentEngine::new_with_provider(provider, test_config(), tools, silent_output(), std::env::temp_dir());
    engine.set_initial_reasoning_effort(Some("high".into()));

    let result = engine
        .run("Implement every rule in SPEC.md", "audit-repair")
        .await
        .unwrap();

    assert_eq!(result.text, "repaired final");
    let recorded = requests.lock().unwrap();
    assert_eq!(recorded.len(), 4);
    assert!(!recorded[1].messages.iter().any(|message| {
        message
            .content
            .iter()
            .any(|block| matches!(block, ContentBlock::Text { text } if text.contains("every explicit requirement")))
    }));
    assert!(recorded[2].messages.iter().any(|message| {
        message
            .content
            .iter()
            .any(|block| matches!(block, ContentBlock::Text { text } if text.contains("every explicit requirement")))
    }));
    assert!(!recorded[3].messages.iter().any(|message| {
        message
            .content
            .iter()
            .any(|block| matches!(block, ContentBlock::Text { text } if text.contains("every explicit requirement")))
    }));
}

#[tokio::test]
async fn failed_edit_does_not_consume_the_verification_prompt() {
    let provider = Arc::new(RecordingProvider::new(vec![tool_turn(), final_turn("failure handled")]));
    let requests = provider.requests();
    let mut tools = ToolRegistry::new();
    tools.register(Box::new(MaterialMockTool { is_error: true }));
    let mut engine =
        AgentEngine::new_with_provider(provider, test_config(), tools, silent_output(), std::env::temp_dir());
    engine.set_initial_reasoning_effort(Some("high".into()));

    let result = engine.run("Try a workspace edit", "audit-failed-edit").await.unwrap();

    assert_eq!(result.text, "failure handled");
    let recorded = requests.lock().unwrap();
    assert_eq!(recorded.len(), 2);
    assert!(!recorded[1].messages.iter().any(|message| {
        message
            .content
            .iter()
            .any(|block| matches!(block, ContentBlock::Text { text } if text.contains("every explicit requirement")))
    }));
}

#[tokio::test]
async fn completion_candidate_is_not_emitted_before_verification() {
    let provider = Arc::new(RecordingProvider::new(vec![
        tool_turn(),
        final_turn("candidate final"),
        final_turn("verified final"),
    ]));
    let mut tools = ToolRegistry::new();
    tools.register(Box::new(MaterialMockTool { is_error: false }));
    let (output, emitted_text) = recording_output();
    let mut engine = AgentEngine::new_with_provider(provider, test_config(), tools, output, std::env::temp_dir());
    engine.set_initial_reasoning_effort(Some("high".into()));

    let result = engine
        .run("Implement every rule in SPEC.md", "audit-visible-final")
        .await
        .unwrap();

    assert_eq!(result.text, "verified final");
    assert_eq!(&*emitted_text.lock().unwrap(), "verified final");
}
