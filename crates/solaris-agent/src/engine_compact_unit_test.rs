use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use chrono::Utc;
use serde_json::json;
use solaris_config::compact::CompactConfig;
use solaris_protocol::events::ToolCategory;
use solaris_providers::error::ProviderError;
use solaris_providers::provider::LlmProvider;
use solaris_tools::Tool;
use solaris_tools::registry::ToolRegistry;
use solaris_types::llm::{LlmEvent, LlmRequest};
use solaris_types::message::{ContentBlock, Message, Role, StopReason, TokenUsage};
use solaris_types::tool::{ToolResult, ToolResultStatus};

use super::{CompactLevel, ProviderCompat};
use crate::compact::state::CompactState;
use crate::confirm::ToolConfirmer;
use crate::output::OutputSink;
use crate::session::{Session, SessionManager};

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
struct RecordingOutput {
    tool_results: Mutex<Vec<(String, String, bool, String)>>,
    tool_statuses: Mutex<Vec<ToolResultStatus>>,
    errors: Mutex<Vec<String>>,
    infos: Mutex<Vec<String>>,
    diagnostics: Mutex<Vec<(String, String)>>,
}

impl OutputSink for RecordingOutput {
    fn emit_text_delta(&self, _: &str, _: &str) {}
    fn emit_thinking(&self, _: &str, _: &str) {}
    fn emit_tool_call(&self, _: &str, _: &str, _: &str) {}
    fn emit_tool_result(&self, tool_use_id: &str, name: &str, is_error: bool, content: &str) {
        self.tool_results.lock().unwrap().push((
            tool_use_id.to_string(),
            name.to_string(),
            is_error,
            content.to_string(),
        ));
    }
    fn emit_tool_result_with_status(&self, tool_use_id: &str, name: &str, status: ToolResultStatus, content: &str) {
        self.tool_statuses.lock().unwrap().push(status);
        self.emit_tool_result(tool_use_id, name, status.is_error(), content);
    }
    fn emit_stream_start(&self, _: &str) {}
    fn emit_stream_end(&self, _: &str, _: usize, _: u64, _: u64, _: u64, _: u64) {}

    fn emit_error(&self, msg: &str) {
        self.errors.lock().unwrap().push(msg.to_string());
    }

    fn emit_protocol_diagnostic(&self, msg_id: &str, message: &str) {
        self.diagnostics
            .lock()
            .unwrap()
            .push((msg_id.to_owned(), message.to_owned()));
    }

    fn emit_info(&self, msg: &str) {
        self.infos.lock().unwrap().push(msg.to_string());
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

struct SummaryProvider;

#[async_trait::async_trait]
impl LlmProvider for SummaryProvider {
    async fn stream(&self, _: &LlmRequest) -> Result<tokio::sync::mpsc::Receiver<LlmEvent>, ProviderError> {
        let (tx, rx) = tokio::sync::mpsc::channel(2);
        tx.send(LlmEvent::TextDelta("<summary>preserved context</summary>".to_string()))
            .await
            .unwrap();
        tx.send(LlmEvent::Done {
            stop_reason: StopReason::EndTurn,
            usage: TokenUsage::default(),
        })
        .await
        .unwrap();
        Ok(rx)
    }
}

struct CompactionAwareTool {
    result_compactions: Arc<AtomicUsize>,
    history_compactions: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Tool for CompactionAwareTool {
    fn name(&self) -> &str {
        "Read"
    }

    fn description(&self) -> &str {
        "records compaction notifications"
    }

    fn input_schema(&self) -> serde_json::Value {
        json!({"type": "object"})
    }

    fn is_concurrency_safe(&self, _: &serde_json::Value) -> bool {
        true
    }

    async fn execute(&self, _: serde_json::Value) -> ToolResult {
        ToolResult {
            content: "ok".to_string(),
            is_error: false,
        }
    }

    fn on_result_compacted(&self, _: &serde_json::Value) {
        self.result_compactions.fetch_add(1, Ordering::SeqCst);
    }

    fn on_history_compacted(&self) {
        self.history_compactions.fetch_add(1, Ordering::SeqCst);
    }

    fn category(&self) -> ToolCategory {
        ToolCategory::Info
    }
}

fn make_compact_engine(
    compact_config: CompactConfig,
    compact_state: CompactState,
    messages: Vec<Message>,
) -> super::AgentEngine {
    make_compact_engine_with_output(compact_config, compact_state, messages, Arc::new(NullOutput))
}

fn make_compact_engine_with_output(
    compact_config: CompactConfig,
    compact_state: CompactState,
    messages: Vec<Message>,
    output: Arc<dyn OutputSink>,
) -> super::AgentEngine {
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
        messages,
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
        compact_config,
        compact_state,
        compact_level: CompactLevel::default(),
        toon_enabled: false,
        plan_state: Default::default(),
        plan_active_flag: None,
        plan_mode_disable_handler: None,
        cache_detector: super::CacheBreakDetector::new(),
        commands: crate::commands::default_registry(),
    }
}

fn tool_use_msg(id: &str, name: &str) -> Message {
    Message::new(
        Role::Assistant,
        vec![ContentBlock::ToolUse {
            id: id.to_string(),
            name: name.to_string(),
            input: json!({}),
            extra: None,
        }],
    )
}

fn tool_use_msg_with_input(id: &str, name: &str, input: serde_json::Value) -> Message {
    Message::new(
        Role::Assistant,
        vec![ContentBlock::ToolUse {
            id: id.to_string(),
            name: name.to_string(),
            input,
            extra: None,
        }],
    )
}

fn tool_use_msg_with_two_calls(first_id: &str, second_id: &str) -> Message {
    Message::new(
        Role::Assistant,
        vec![
            ContentBlock::ToolUse {
                id: first_id.to_string(),
                name: "Read".to_string(),
                input: json!({}),
                extra: None,
            },
            ContentBlock::ToolUse {
                id: second_id.to_string(),
                name: "ExecCommand".to_string(),
                input: json!({}),
                extra: None,
            },
        ],
    )
}

fn tool_result_msg(id: &str, content: &str) -> Message {
    Message::new(
        Role::User,
        vec![ContentBlock::ToolResult {
            tool_use_id: id.to_string(),
            content: content.to_string(),
            is_error: false,
        }],
    )
}

#[test]
fn abort_current_turn_closes_pending_tool_uses() {
    let output = Arc::new(RecordingOutput::default());
    let mut engine = make_compact_engine_with_output(
        CompactConfig::default(),
        CompactState::new(),
        vec![
            Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "run tools".to_string(),
                }],
            ),
            tool_use_msg_with_two_calls("call_read", "call_bash"),
        ],
        output.clone(),
    );
    let ledger = Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default());
    let run_id = solaris_types::identity::RunId::from("abort-unstarted-run");
    engine.set_execution_context(crate::execution_context::EffectExecutionContext::new(
        run_id.clone(),
        solaris_types::identity::AgentId::from("root"),
        ledger.clone(),
        crate::permission_engine::PermissionContext::new(
            solaris_types::permission::PermissionMode::Bypass,
            solaris_types::permission::PermissionCeiling::unrestricted(),
        ),
        solaris_types::runtime::OperationEnvironmentSnapshot::default(),
    ));

    engine.abort_current_turn("Tool execution canceled by user");

    let last = engine.messages.last().expect("synthetic result message");
    assert_eq!(last.role, Role::User);
    assert_eq!(last.content.len(), 2);
    assert!(
        matches!(&last.content[0], ContentBlock::ToolResult { tool_use_id, content, is_error }
            if tool_use_id == "call_read" && content == "Tool execution canceled by user" && *is_error)
    );
    assert!(
        matches!(&last.content[1], ContentBlock::ToolResult { tool_use_id, content, is_error }
            if tool_use_id == "call_bash" && content == "Tool execution canceled by user" && *is_error)
    );

    let emitted = output.tool_results.lock().unwrap();
    assert_eq!(emitted.len(), 2);
    assert_eq!(
        emitted[0],
        (
            "call_read".into(),
            "Read".into(),
            true,
            "Tool execution canceled by user".into()
        )
    );
    assert_eq!(
        emitted[1],
        (
            "call_bash".into(),
            "ExecCommand".into(),
            true,
            "Tool execution canceled by user".into()
        )
    );
    assert_eq!(
        *output.tool_statuses.lock().unwrap(),
        [ToolResultStatus::Aborted, ToolResultStatus::Aborted]
    );
    let records = crate::runtime_ledger::RuntimeLedger::records_for_run(ledger.as_ref(), &run_id).unwrap();
    assert!(
        records
            .iter()
            .any(|record| record.record_type == "agent_task_phase" && record.payload["phase"] == "aborted")
    );
}

#[test]
fn abort_after_effect_intent_is_visible_and_persisted_as_outcome_unknown() {
    let output = Arc::new(RecordingOutput::default());
    let mut engine = make_compact_engine_with_output(
        CompactConfig::default(),
        CompactState::new(),
        vec![tool_use_msg("call_started", "ExecCommand")],
        output.clone(),
    );
    let ledger = Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default());
    let run_id = solaris_types::identity::RunId::from("abort-started-run");
    let context = crate::execution_context::EffectExecutionContext::new(
        run_id.clone(),
        solaris_types::identity::AgentId::from("root"),
        ledger.clone(),
        crate::permission_engine::PermissionContext::new(
            solaris_types::permission::PermissionMode::Bypass,
            solaris_types::permission::PermissionCeiling::unrestricted(),
        ),
        solaris_types::runtime::OperationEnvironmentSnapshot::default(),
    );
    let request = context.effect_request(
        "call_started",
        "ExecCommand",
        &json!({}),
        solaris_types::effect::EffectDescriptor {
            class: solaris_types::effect::EffectClass::Process,
            action: "execute".to_owned(),
            resources: solaris_types::effect::ResourceFootprint::default(),
            replay_policy: solaris_types::effect::EffectReplayPolicy::ReconcileRequired,
        },
    );
    context.record_effect_intent(&request).unwrap();
    engine.set_execution_context(context);

    engine.abort_current_turn("cancelled");

    assert_eq!(
        *output.tool_statuses.lock().unwrap(),
        [ToolResultStatus::OutcomeUnknown]
    );
    let records = crate::runtime_ledger::RuntimeLedger::records_for_run(ledger.as_ref(), &run_id).unwrap();
    assert!(
        records
            .iter()
            .any(|record| { record.record_type == "agent_task_phase" && record.payload["phase"] == "outcome_unknown" })
    );
}

#[test]
fn cache_full_miss_is_reported_as_info_not_error() {
    let output = Arc::new(RecordingOutput::default());
    let mut engine =
        make_compact_engine_with_output(CompactConfig::default(), CompactState::new(), vec![], output.clone());

    engine.cache_detector.record_request("prompt", &[]);
    engine
        .record_turn_usage(
            &TokenUsage {
                input_tokens: 10_000,
                output_tokens: 100,
                cache_creation_tokens: 2_000,
                cache_read_tokens: 8_000,
            },
            &solaris_types::identity::EffectId::from("cache-hit"),
        )
        .unwrap();

    engine.cache_detector.record_request("prompt", &[]);
    engine
        .record_turn_usage(
            &TokenUsage {
                input_tokens: 10_000,
                output_tokens: 100,
                cache_creation_tokens: 10_000,
                cache_read_tokens: 0,
            },
            &solaris_types::identity::EffectId::from("cache-miss"),
        )
        .unwrap();

    assert!(
        output.errors.lock().unwrap().is_empty(),
        "cache diagnostics should not emit terminal errors"
    );
    assert!(
        output
            .infos
            .lock()
            .unwrap()
            .iter()
            .any(|msg| msg == "Cache full miss: Unknown"),
        "full cache misses should remain visible as diagnostics"
    );
}

// -- Emergency check fires when at limit --

#[tokio::test]
async fn emergency_fires_when_at_limit() {
    let config = CompactConfig {
        context_window: 200_000,
        emergency_buffer: 3_000,
        ..Default::default()
    };
    let mut state = CompactState::new();
    state.last_input_tokens = 198_000; // >= 197k limit

    let mut engine = make_compact_engine(config, state, vec![]);
    let result = engine.run_compaction().await;

    match result {
        Err(super::AgentError::ContextTooLong { input_tokens, limit }) => {
            assert_eq!(input_tokens, 198_000);
            assert_eq!(limit, 197_000);
        }
        other => panic!("expected ContextTooLong, got: {:?}", other),
    }
}

// -- Emergency does not fire when below limit --

#[tokio::test]
async fn emergency_silent_below_limit() {
    let config = CompactConfig::default();
    let mut state = CompactState::new();
    state.last_input_tokens = 190_000; // below 197k

    let mut engine = make_compact_engine(config, state, vec![]);
    assert!(engine.run_compaction().await.is_ok());
}

// -- Microcompact runs when count trigger fires --

#[tokio::test]
async fn microcompact_clears_old_results() {
    // 12 tool results with keep_recent=3 (threshold=6) 鈫?should clear 9
    let mut messages = Vec::new();
    for i in 0..12 {
        let id = format!("t{i}");
        messages.push(tool_use_msg(&id, "Read"));
        messages.push(tool_result_msg(&id, &format!("data-{i}")));
    }

    let config = CompactConfig {
        micro_keep_recent: 3,
        ..Default::default()
    };
    let state = CompactState::new();

    let mut engine = make_compact_engine(config, state, messages);
    engine.run_compaction().await.unwrap();

    // Last 3 tool results should be preserved
    let cleared_count = engine
        .messages
        .iter()
        .flat_map(|m| &m.content)
        .filter(|b| matches!(b, ContentBlock::ToolResult { content, .. } if content == "[Tool result cleared]"))
        .count();

    assert_eq!(cleared_count, 9);
}

#[tokio::test]
async fn microcompact_notifies_tools_for_each_cleared_result() {
    let result_compactions = Arc::new(AtomicUsize::new(0));
    let history_compactions = Arc::new(AtomicUsize::new(0));
    let mut messages = Vec::new();
    for i in 0..8 {
        let id = format!("t{i}");
        messages.push(tool_use_msg_with_input(
            &id,
            "Read",
            json!({"file_path": format!("file-{i}.txt")}),
        ));
        messages.push(tool_result_msg(&id, &format!("data-{i}")));
    }

    let config = CompactConfig {
        micro_keep_recent: 2,
        ..Default::default()
    };
    let mut engine = make_compact_engine(config, CompactState::new(), messages);
    engine.tools.register(Box::new(CompactionAwareTool {
        result_compactions: Arc::clone(&result_compactions),
        history_compactions: Arc::clone(&history_compactions),
    }));

    engine.run_compaction().await.unwrap();

    assert_eq!(result_compactions.load(Ordering::SeqCst), 6);
    assert_eq!(history_compactions.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn successful_autocompact_notifies_all_tool_history_caches() {
    let result_compactions = Arc::new(AtomicUsize::new(0));
    let history_compactions = Arc::new(AtomicUsize::new(0));
    let config = CompactConfig {
        context_window: 1_000,
        autocompact_threshold_pct: Some(10),
        emergency_buffer: 0,
        ..Default::default()
    };
    let mut state = CompactState::new();
    state.last_input_tokens = 100;
    let output = Arc::new(RecordingOutput::default());
    let mut engine = make_compact_engine_with_output(
        config,
        state,
        vec![Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "context to summarize".to_string(),
            }],
        )],
        output.clone(),
    );
    engine.provider = Arc::new(SummaryProvider);
    engine.tools.register(Box::new(CompactionAwareTool {
        result_compactions: Arc::clone(&result_compactions),
        history_compactions: Arc::clone(&history_compactions),
    }));

    engine.run_compaction().await.unwrap();

    assert_eq!(result_compactions.load(Ordering::SeqCst), 0);
    assert_eq!(
        history_compactions.load(Ordering::SeqCst),
        1,
        "autocompact errors: {:?}",
        output.errors.lock().unwrap()
    );
}

#[tokio::test]
async fn autocompact_failure_is_scoped_non_terminal_and_continues() {
    let config = CompactConfig {
        context_window: 1_000,
        autocompact_threshold_pct: Some(10),
        emergency_buffer: 0,
        ..Default::default()
    };
    let mut state = CompactState::new();
    state.last_input_tokens = 100;
    let output = Arc::new(RecordingOutput::default());
    let mut engine = make_compact_engine_with_output(
        config,
        state,
        vec![Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "context that cannot be summarized".to_owned(),
            }],
        )],
        output.clone(),
    );
    engine.msg_id = "active-turn-42".to_owned();

    let result = engine.run_compaction().await;

    assert!(
        result.is_ok(),
        "autocompact failure must not terminate the turn: {result:?}"
    );
    assert_eq!(engine.compact_state.consecutive_failures, 1);
    assert!(output.errors.lock().unwrap().is_empty());
    assert_eq!(
        output.diagnostics.lock().unwrap().as_slice(),
        [(
            "active-turn-42".to_owned(),
            "Automatic context compaction failed; continuing without compaction.".to_owned(),
        )]
    );
}

#[test]
fn active_turn_session_save_without_a_lease_is_terminal() {
    let blocker = tempfile::NamedTempFile::new().unwrap();
    let output = Arc::new(RecordingOutput::default());
    let mut engine =
        make_compact_engine_with_output(CompactConfig::default(), CompactState::new(), vec![], output.clone());
    engine.msg_id = "active-turn-save".to_owned();
    engine.session_manager = Some(SessionManager::new(blocker.path().to_path_buf(), 10));
    engine.current_session = Some(Session {
        id: "session-save".to_owned(),
        run_id: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        provider: "test-provider".to_owned(),
        model: "test-model".to_owned(),
        cwd: "workspace".to_owned(),
        total_usage: TokenUsage::default(),
        messages: vec![],
        runtime_state: None,
    });

    let error = engine.save_session().unwrap_err();

    assert!(error.to_string().contains("lease"));
    assert!(output.errors.lock().unwrap().is_empty());
    assert!(output.diagnostics.lock().unwrap().is_empty());
}

#[test]
fn background_session_save_without_a_lease_is_terminal() {
    let blocker = tempfile::NamedTempFile::new().unwrap();
    let output = Arc::new(RecordingOutput::default());
    let mut engine =
        make_compact_engine_with_output(CompactConfig::default(), CompactState::new(), vec![], output.clone());
    engine.session_manager = Some(SessionManager::new(blocker.path().to_path_buf(), 10));
    engine.current_session = Some(Session {
        id: "session-background-save".to_owned(),
        run_id: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        provider: "test-provider".to_owned(),
        model: "test-model".to_owned(),
        cwd: "workspace".to_owned(),
        total_usage: TokenUsage::default(),
        messages: vec![],
        runtime_state: None,
    });

    let error = engine.save_session().unwrap_err();

    assert!(error.to_string().contains("lease"));
    assert!(output.errors.lock().unwrap().is_empty());
    assert!(output.diagnostics.lock().unwrap().is_empty());
    assert!(output.infos.lock().unwrap().is_empty());
}

// -- Disabled config skips micro and auto but not emergency --

#[tokio::test]
async fn disabled_config_skips_micro_auto() {
    let mut messages = Vec::new();
    for i in 0..12 {
        let id = format!("t{i}");
        messages.push(tool_use_msg(&id, "Read"));
        messages.push(tool_result_msg(&id, &format!("data-{i}")));
    }

    let config = CompactConfig {
        enabled: false,
        micro_keep_recent: 3,
        ..Default::default()
    };
    let state = CompactState::new();

    let mut engine = make_compact_engine(config, state, messages);
    engine.run_compaction().await.unwrap();

    // Nothing should be cleared (microcompact skipped)
    let cleared_count = engine
        .messages
        .iter()
        .flat_map(|m| &m.content)
        .filter(|b| matches!(b, ContentBlock::ToolResult { content, .. } if content == "[Tool result cleared]"))
        .count();

    assert_eq!(cleared_count, 0, "microcompact should be skipped when disabled");
}

#[tokio::test]
async fn disabled_config_still_fires_emergency() {
    let config = CompactConfig {
        enabled: false,
        context_window: 200_000,
        emergency_buffer: 3_000,
        ..Default::default()
    };
    let mut state = CompactState::new();
    state.last_input_tokens = 198_000;

    let mut engine = make_compact_engine(config, state, vec![]);
    let result = engine.run_compaction().await;

    assert!(
        matches!(result, Err(super::AgentError::ContextTooLong { .. })),
        "emergency should fire even when disabled"
    );
}

// -- Zero tokens on first turn does not trigger anything --

#[tokio::test]
async fn first_turn_zero_tokens_no_compaction() {
    let config = CompactConfig::default();
    let state = CompactState::new(); // last_input_tokens = 0

    let mut engine = make_compact_engine(config, state, vec![]);
    assert!(engine.run_compaction().await.is_ok());
    assert_eq!(engine.compact_state.last_input_tokens, 0);
}

// -- Circuit broken prevents autocompact, emergency still fires --

#[tokio::test]
async fn circuit_broken_skips_auto_but_emergency_fires() {
    let config = CompactConfig {
        context_window: 200_000,
        emergency_buffer: 3_000,
        max_failures: 3,
        ..Default::default()
    };
    let mut state = CompactState::new();
    state.last_input_tokens = 198_000; // triggers both auto and emergency
    state.consecutive_failures = 3; // circuit broken

    let mut engine = make_compact_engine(config, state, vec![]);
    let result = engine.run_compaction().await;

    // Auto is skipped due to circuit breaker; emergency fires
    assert!(matches!(result, Err(super::AgentError::ContextTooLong { .. })));
}
