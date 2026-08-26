use std::sync::{Arc, Mutex};

use solaris_providers::error::ProviderError;
use solaris_providers::provider::LlmProvider;
use solaris_tools::registry::ToolRegistry;
use solaris_types::llm::{LlmEvent, LlmRequest};
use solaris_types::message::{ContentBlock, Message, Role};

use super::{CompactLevel, ProviderCompat};
use crate::compact::state::CompactState;
use crate::confirm::ToolConfirmer;
use crate::output::OutputSink;

struct NullOutput;
impl OutputSink for NullOutput {
    fn emit_text_delta(&self, _: &str, _: &str) {}
    fn emit_thinking(&self, _: &str, _: &str) {}
    fn emit_tool_call(&self, _: &str, _: &str, _: &str) {}
    fn emit_tool_result(&self, _: &str, _: &str, _: bool, _: &str) {}
    fn emit_stream_start(&self, _: &str) {}
    fn emit_stream_end(&self, _: &str, _: usize, _: u64, _: u64, _: u64, _: u64) {}
    fn emit_error(&self, _: &str) {}
    fn emit_info(&self, _: &str) {}
}

#[derive(Default)]
struct SequenceOutput {
    events: Mutex<Vec<String>>,
}

impl SequenceOutput {
    fn events(&self) -> Vec<String> {
        self.events.lock().unwrap_or_else(|error| error.into_inner()).clone()
    }
}

impl OutputSink for SequenceOutput {
    fn emit_text_delta(&self, _: &str, _: &str) {}
    fn emit_thinking(&self, _: &str, _: &str) {}
    fn emit_tool_call(&self, _: &str, _: &str, _: &str) {}
    fn emit_tool_result(&self, _: &str, _: &str, _: bool, _: &str) {}

    fn emit_stream_start(&self, msg_id: &str) {
        self.events
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(format!("start:{msg_id}"));
    }

    fn emit_stream_end(&self, msg_id: &str, _: usize, _: u64, _: u64, _: u64, _: u64) {
        self.events
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(format!("end:{msg_id}"));
    }

    fn emit_error(&self, _: &str) {
        self.events
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push("error".to_owned());
    }

    fn emit_info(&self, _: &str) {
        self.events
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push("info".to_owned());
    }
}

struct NullProvider;
#[async_trait::async_trait]
impl LlmProvider for NullProvider {
    async fn stream(&self, _: &LlmRequest) -> Result<tokio::sync::mpsc::Receiver<LlmEvent>, ProviderError> {
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        Ok(rx)
    }
}

fn make_engine() -> super::AgentEngine {
    make_engine_with_output(Arc::new(NullOutput))
}

fn make_engine_with_output(output: Arc<dyn OutputSink>) -> super::AgentEngine {
    super::AgentEngine {
        provider: Arc::new(NullProvider),
        provider_label: "test-provider".to_owned(),
        provider_effect: crate::bootstrap::provider_effect_descriptor("https://provider.test"),
        model: "test-model".to_string(),
        max_tokens: Some(4096),
        thinking: None,
        compat: ProviderCompat::anthropic_defaults(),
        system_prompt: String::new(),
        reasoning_effort: None,
        runtime_configuration: super::runtime_configuration_state(
            "test-provider",
            "test-model",
            solaris_types::permission::PermissionMode::Bypass,
            &None,
            CompactLevel::default(),
        ),
        multi_agent_policy: std::sync::Arc::new(std::sync::RwLock::new(
            solaris_types::workflow::MultiAgentPolicy::default(),
        )),
        resources: crate::resource_manager::ResourceManager::new(Default::default()),
        messages: vec![],
        total_usage: Default::default(),
        msg_id: String::new(),
        max_turns_per_run: Some(10),
        max_tool_call_malformed_turns: 3,
        max_tool_call_failure_turns: 3,
        tools: ToolRegistry::new(),
        confirmer: Arc::new(Mutex::new(ToolConfirmer::new(true, vec![]))),
        permission_context: crate::permission_engine::PermissionContext::from_auto_approve(true),
        execution_context: None,
        allow_list: vec![],
        hooks: None,
        session_manager: None,
        current_session: None,
        output,
        approval_manager: None,
        protocol_writer: None,
        compact_config: solaris_config::compact::CompactConfig::default(),
        compact_state: CompactState::new(),
        compact_level: CompactLevel::default(),
        toon_enabled: false,
        plan_state: Default::default(),
        plan_active_flag: None,
        plan_mode_disable_handler: None,
        cache_detector: super::CacheBreakDetector::new(),
        commands: crate::commands::default_registry(),
    }
}

#[tokio::test]
async fn handle_command_quit() {
    let mut engine = make_engine();
    let err = engine.handle_command("/quit").await.unwrap_err();
    assert!(matches!(err, super::AgentError::UserAborted));
}

#[tokio::test]
async fn handle_command_exit_alias() {
    let mut engine = make_engine();
    let err = engine.handle_command("/exit").await.unwrap_err();
    assert!(matches!(err, super::AgentError::UserAborted));
}

#[tokio::test]
async fn handle_command_unknown() {
    let mut engine = make_engine();
    let error = engine.handle_command("/nonexistent").await.unwrap_err();
    assert!(error.to_string().contains("unknown slash command"));
}

#[tokio::test]
async fn handle_command_model_changes_current_model_without_provider_call() {
    let mut engine = make_engine();

    let result = engine
        .handle_command("/model replacement-model")
        .await
        .unwrap()
        .expect("model command should be handled");

    assert_eq!(result.turns, 0);
    assert_eq!(engine.model, "replacement-model");
    assert_eq!(
        engine.runtime_configuration_view().snapshot().model,
        "replacement-model"
    );
}

#[tokio::test]
async fn handle_command_clear() {
    let mut engine = make_engine();
    engine.messages.push(Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: "hello".to_string(),
        }],
    ));
    assert_eq!(engine.messages.len(), 1);

    let result = engine.handle_command("/clear").await;
    let result = result.unwrap().expect("clear command should be handled");
    assert_eq!(result.turns, 0);
    assert!(engine.messages.is_empty());
    assert_eq!(engine.compact_state.last_input_tokens, 0);
}

#[tokio::test]
async fn handle_command_with_args() {
    let mut engine = make_engine();
    let result = engine
        .handle_command("/help compact")
        .await
        .unwrap()
        .expect("help command should be handled");
    assert_eq!(result.turns, 0);
}

#[tokio::test]
async fn handle_command_not_a_command() {
    let mut engine = make_engine();
    let result = engine.handle_command("hello world").await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn run_intercepts_help_returns_zero_turns() {
    let mut engine = make_engine();
    let result = engine.run("/help", "msg-1").await.unwrap();
    assert_eq!(result.turns, 0);
    assert_eq!(result.usage.input_tokens, 0);
}

#[tokio::test]
async fn run_intercepts_quit_returns_user_aborted() {
    let mut engine = make_engine();
    let err = engine.run("/quit", "msg-1").await.unwrap_err();
    assert!(matches!(err, super::AgentError::UserAborted));
}

#[tokio::test]
async fn run_rejects_unknown_slash_before_provider_execution() {
    let mut engine = make_engine();

    let error = engine.run("/unknown", "msg-1").await.unwrap_err();

    assert!(error.to_string().contains("unknown slash command"));
}

#[tokio::test]
async fn slash_commands_start_before_info_or_error_and_end_with_the_same_message_id() {
    let help_output = Arc::new(SequenceOutput::default());
    let mut help_engine = make_engine_with_output(help_output.clone());
    let help = help_engine.run("/help", "help-message").await.unwrap();
    help_output.emit_stream_end("help-message", help.turns, 0, 0, 0, 0);
    assert_eq!(
        help_output.events(),
        vec!["start:help-message", "info", "end:help-message"]
    );

    let exit_output = Arc::new(SequenceOutput::default());
    let mut exit_engine = make_engine_with_output(exit_output.clone());
    let exit_error = exit_engine.run("/quit", "exit-message").await.unwrap_err();
    exit_output.emit_error(&exit_error.to_string());
    exit_output.emit_stream_end("exit-message", 0, 0, 0, 0, 0);
    assert_eq!(
        exit_output.events(),
        vec!["start:exit-message", "error", "end:exit-message"]
    );

    let unknown_output = Arc::new(SequenceOutput::default());
    let mut unknown_engine = make_engine_with_output(unknown_output.clone());
    let unknown_error = unknown_engine.run("/unknown", "unknown-message").await.unwrap_err();
    unknown_output.emit_error(&unknown_error.to_string());
    unknown_output.emit_stream_end("unknown-message", 0, 0, 0, 0, 0);
    assert_eq!(
        unknown_output.events(),
        vec!["start:unknown-message", "error", "end:unknown-message"]
    );
}

#[test]
fn slash_command_list_returns_all() {
    let engine = make_engine();
    let list = engine.slash_command_list();
    assert!(list.len() >= 4);
    let names: Vec<&str> = list.iter().map(|(n, _)| n.as_str()).collect();
    assert!(names.contains(&"help"));
    assert!(names.contains(&"compact"));
    assert!(names.contains(&"clear"));
    assert!(names.contains(&"quit"));
    assert!(names.contains(&"model"));
}

#[test]
fn recognized_slash_commands_are_distinct_from_workflow_requests() {
    let engine = make_engine();

    assert!(engine.recognizes_slash_command("/help"));
    assert!(engine.recognizes_slash_command("/help compact"));
    assert!(engine.recognizes_slash_command("/clear"));
    assert!(engine.recognizes_slash_command("/compact"));
    assert!(engine.recognizes_slash_command("/quit"));
    assert!(engine.recognizes_slash_command("/exit"));
    assert!(engine.recognizes_slash_command("/model"));
    assert!(engine.recognizes_slash_command("/model replacement-model"));
    assert!(!engine.recognizes_slash_command("/research compare providers"));
    assert!(!engine.recognizes_slash_command("/unknown"));
    assert!(!engine.recognizes_slash_command("plain text"));
}
