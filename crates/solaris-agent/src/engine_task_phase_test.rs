use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use solaris_config::config::{CliArgs, Config};
use solaris_providers::error::ProviderError;
use solaris_providers::provider::LlmProvider;
use solaris_tools::Tool;
use solaris_tools::registry::ToolRegistry;
use solaris_types::identity::{AgentId, RunId};
use solaris_types::llm::{LlmEvent, LlmRequest};
use solaris_types::message::{ContentBlock, Message, Role, StopReason, TokenUsage};
use solaris_types::permission::{PermissionCeiling, PermissionMode};
use solaris_types::resource::ResourceBudget;
use solaris_types::runtime::OperationEnvironmentSnapshot;
use solaris_types::tool::{ToolResult, ToolResultStatus};
use tempfile::tempdir;

use super::AgentEngine;
use super::task_phase::{legacy_tool_round_call_id, provider_call_id, tool_round_call_id, turn_kind_key};
use crate::error::AgentError;
use crate::execution_context::EffectExecutionContext;
use crate::execution_context::{DurableTaskPhase, stable_digest_value};
use crate::output::null_sink::NullSink;
use crate::permission_engine::PermissionContext;
use crate::resource_manager::ResourceManager;
use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};
use crate::turn::TurnKind;

struct CompletedProvider;

struct BlockingProvider;

struct CountingProvider {
    calls: Arc<AtomicUsize>,
}

struct ToolThenBlockingProvider {
    calls: AtomicUsize,
}

struct CheckpointTool;

struct CountingCheckpointTool {
    calls: Arc<AtomicUsize>,
}

struct TestRuntimeLedger {
    inner: InMemoryRuntimeLedger,
    effect_output_root: PathBuf,
}

struct FailProviderCompletedAuditOnce {
    inner: Arc<TestRuntimeLedger>,
    armed: std::sync::atomic::AtomicBool,
}

impl RuntimeLedger for TestRuntimeLedger {
    crate::runtime_ledger::forward_compare_and_append!();

    fn append(
        &self,
        run_id: &RunId,
        durability: solaris_types::effect::DurabilityClass,
        record_type: &str,
        payload: serde_json::Value,
    ) -> std::io::Result<crate::runtime_ledger::LedgerRecord> {
        self.inner.append(run_id, durability, record_type, payload)
    }

    fn run_ids(&self) -> std::io::Result<Vec<RunId>> {
        self.inner.run_ids()
    }

    fn records_for_run(&self, run_id: &RunId) -> std::io::Result<Vec<crate::runtime_ledger::LedgerRecord>> {
        self.inner.records_for_run(run_id)
    }

    fn effect_output_root(&self) -> Option<PathBuf> {
        Some(self.effect_output_root.clone())
    }
}

fn test_ledger(workspace: &Path) -> Arc<TestRuntimeLedger> {
    Arc::new(TestRuntimeLedger {
        inner: InMemoryRuntimeLedger::default(),
        effect_output_root: workspace.join("runtime-effect-outputs"),
    })
}

impl RuntimeLedger for FailProviderCompletedAuditOnce {
    crate::runtime_ledger::forward_compare_and_append!();

    fn append(
        &self,
        run_id: &RunId,
        durability: solaris_types::effect::DurabilityClass,
        record_type: &str,
        payload: serde_json::Value,
    ) -> std::io::Result<crate::runtime_ledger::LedgerRecord> {
        if record_type == "agent_task_phase"
            && payload["phase"] == "provider_completed"
            && self.armed.swap(false, Ordering::SeqCst)
        {
            return Err(std::io::Error::other(
                "injected audit append failure after session phase commit",
            ));
        }
        self.inner.append(run_id, durability, record_type, payload)
    }

    fn run_ids(&self) -> std::io::Result<Vec<RunId>> {
        self.inner.run_ids()
    }

    fn records_for_run(&self, run_id: &RunId) -> std::io::Result<Vec<crate::runtime_ledger::LedgerRecord>> {
        self.inner.records_for_run(run_id)
    }

    fn effect_output_root(&self) -> Option<std::path::PathBuf> {
        self.inner.effect_output_root()
    }
}

#[async_trait]
impl Tool for CheckpointTool {
    fn name(&self) -> &str {
        "CheckpointTool"
    }

    fn description(&self) -> &str {
        "test tool"
    }

    fn input_schema(&self) -> serde_json::Value {
        serde_json::json!({"type": "object"})
    }

    fn is_concurrency_safe(&self, _: &serde_json::Value) -> bool {
        true
    }

    async fn execute(&self, _: serde_json::Value) -> ToolResult {
        ToolResult {
            content: "checkpointed".to_owned(),
            is_error: false,
        }
    }

    fn category(&self) -> solaris_protocol::events::ToolCategory {
        solaris_protocol::events::ToolCategory::Info
    }
}

#[async_trait]
impl Tool for CountingCheckpointTool {
    fn name(&self) -> &str {
        "CheckpointTool"
    }

    fn description(&self) -> &str {
        "test tool"
    }

    fn input_schema(&self) -> serde_json::Value {
        serde_json::json!({"type": "object"})
    }

    fn is_concurrency_safe(&self, _: &serde_json::Value) -> bool {
        true
    }

    async fn execute(&self, _: serde_json::Value) -> ToolResult {
        self.calls.fetch_add(1, Ordering::SeqCst);
        ToolResult {
            content: "checkpointed".to_owned(),
            is_error: false,
        }
    }

    fn category(&self) -> solaris_protocol::events::ToolCategory {
        solaris_protocol::events::ToolCategory::Info
    }
}

#[async_trait]
impl LlmProvider for CompletedProvider {
    async fn stream(&self, _: &LlmRequest) -> Result<tokio::sync::mpsc::Receiver<LlmEvent>, ProviderError> {
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        sender.send(LlmEvent::TextDelta("done".to_owned())).await.unwrap();
        sender
            .send(LlmEvent::Done {
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::default(),
            })
            .await
            .unwrap();
        Ok(receiver)
    }
}

#[async_trait]
impl LlmProvider for BlockingProvider {
    async fn stream(&self, _: &LlmRequest) -> Result<tokio::sync::mpsc::Receiver<LlmEvent>, ProviderError> {
        std::future::pending().await
    }
}

#[async_trait]
impl LlmProvider for CountingProvider {
    async fn stream(&self, _: &LlmRequest) -> Result<tokio::sync::mpsc::Receiver<LlmEvent>, ProviderError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        sender.send(LlmEvent::TextDelta("done".to_owned())).await.unwrap();
        sender
            .send(LlmEvent::Done {
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::default(),
            })
            .await
            .unwrap();
        Ok(receiver)
    }
}

#[async_trait]
impl LlmProvider for ToolThenBlockingProvider {
    async fn stream(&self, _: &LlmRequest) -> Result<tokio::sync::mpsc::Receiver<LlmEvent>, ProviderError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) > 0 {
            return std::future::pending().await;
        }
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        sender
            .send(LlmEvent::ToolUse {
                id: "checkpoint-call".to_owned(),
                name: "CheckpointTool".to_owned(),
                input: serde_json::json!({}),
                extra: None,
            })
            .await
            .unwrap();
        sender
            .send(LlmEvent::Done {
                stop_reason: StopReason::ToolUse,
                usage: TokenUsage::default(),
            })
            .await
            .unwrap();
        Ok(receiver)
    }
}

#[test]
fn stable_call_ids_use_versioned_domains_and_round_identity() {
    assert_eq!(
        provider_call_id("post_mutation_verification", "sha256:request"),
        "provider-call-v1:sha256:cb11575f18c07a31c9c89c416bccfee0e904e8100bf9ec00d4d1b653a841c925"
    );
    assert_eq!(
        tool_round_call_id("task-key", 1, "sha256:round"),
        "tool-round-call-v3:sha256:fdb3a7213eb4731ee37b75c2abae1c189b11bdbab1f878d1c7173b273b8db35e"
    );
    assert_ne!(
        tool_round_call_id("task-key", 1, "sha256:round"),
        tool_round_call_id("another-task", 1, "sha256:round")
    );
    assert_ne!(
        tool_round_call_id("task-key", 1, "sha256:round"),
        tool_round_call_id("task-key", 2, "sha256:round")
    );
}

#[test]
fn durable_tool_round_identity_advances_when_compaction_replaces_message_history() {
    let workspace = tempdir().unwrap();
    let sessions = tempdir().unwrap();
    let mut config = test_config(workspace.path());
    config.session.directory = sessions.path().to_string_lossy().into_owned();
    let context = EffectExecutionContext::new(
        RunId::from("tool-round-after-compaction-run"),
        AgentId::from("root"),
        test_ledger(workspace.path()),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let mut engine = AgentEngine::new_with_provider(
        Arc::new(CompletedProvider),
        config,
        ToolRegistry::new(),
        Arc::new(NullSink),
        workspace.path().to_path_buf(),
    );
    engine.set_execution_context(context);
    engine
        .init_session(
            "openai",
            &workspace.path().to_string_lossy(),
            Some("tool-round-after-compaction"),
        )
        .unwrap();
    engine.msg_id = "message-tool-round-after-compaction".to_owned();
    let _ = engine.begin_or_resume_durable_task("same input").unwrap();
    let calls = vec![checkpoint_tool_call(false)];

    let before = engine.current_tool_round_call_id(&calls).unwrap();
    engine
        .record_durable_task_phase(DurableTaskPhase::ToolsInFlight, Some(&before))
        .unwrap();
    engine
        .record_durable_task_phase(DurableTaskPhase::ToolsCompleted, Some(&before))
        .unwrap();
    engine.messages = vec![Message::now(
        Role::User,
        vec![ContentBlock::Text {
            text: "compacted history".to_owned(),
        }],
    )];

    let after = engine.current_tool_round_call_id(&calls).unwrap();

    assert_ne!(before, after);
}

#[tokio::test]
async fn completed_task_replays_without_calling_provider_again() {
    let workspace = tempdir().unwrap();
    let sessions = tempdir().unwrap();
    let mut config = test_config(workspace.path());
    config.session.directory = sessions.path().to_string_lossy().into_owned();
    let calls = Arc::new(AtomicUsize::new(0));
    let provider = Arc::new(CountingProvider { calls: calls.clone() });
    let ledger = test_ledger(workspace.path());
    let context = EffectExecutionContext::new(
        RunId::from("replay-task-run"),
        AgentId::from("root"),
        ledger,
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let mut first = AgentEngine::new_with_provider(
        provider.clone(),
        config.clone(),
        ToolRegistry::new(),
        Arc::new(NullSink),
        workspace.path().to_path_buf(),
    );
    first.set_execution_context(context.clone());
    first
        .init_session("openai", &workspace.path().to_string_lossy(), Some("completed-replay"))
        .unwrap();
    let first_result = first.run("finish", "message-replay").await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    first.run_stop_hooks().await;
    drop(first);

    let session = crate::session::SessionManager::new(sessions.path().to_path_buf(), 20)
        .load("completed-replay")
        .unwrap();
    let mut resumed = AgentEngine::resume_with_provider(
        provider,
        config,
        ToolRegistry::new(),
        Arc::new(NullSink),
        session,
        workspace.path().to_path_buf(),
    );
    resumed.set_execution_context(context);
    let replayed = resumed.run("finish", "message-replay").await.unwrap();

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(replayed.text, first_result.text);
    assert_eq!(replayed.turns, first_result.turns);
    let changed_input = resumed.run("different input", "message-replay").await.unwrap_err();
    assert!(changed_input.to_string().contains("input conflicts"));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn outcome_unknown_blocks_resume_before_provider_and_never_completes() {
    let workspace = tempdir().unwrap();
    let sessions = tempdir().unwrap();
    let mut config = test_config(workspace.path());
    config.session.directory = sessions.path().to_string_lossy().into_owned();
    let ledger = test_ledger(workspace.path());
    let context = EffectExecutionContext::new(
        RunId::from("unknown-resume-run"),
        AgentId::from("root"),
        ledger.clone(),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let mut first = AgentEngine::new_with_provider(
        Arc::new(BlockingProvider),
        config.clone(),
        ToolRegistry::new(),
        Arc::new(NullSink),
        workspace.path().to_path_buf(),
    );
    first.set_execution_context(context.clone());
    first
        .init_session("openai", &workspace.path().to_string_lossy(), Some("unknown-resume"))
        .unwrap();
    let running = tokio::spawn(async move { first.run("wait", "message-unknown").await });
    wait_for_phase(&ledger, "unknown-resume-run", "provider_in_flight").await;
    running.abort();
    let _ = running.await;
    wait_for_phase(&ledger, "unknown-resume-run", "outcome_unknown").await;

    let session = crate::session::SessionManager::new(sessions.path().to_path_buf(), 20)
        .load("unknown-resume")
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut resumed = AgentEngine::resume_with_provider(
        Arc::new(CountingProvider { calls: calls.clone() }),
        config,
        ToolRegistry::new(),
        Arc::new(NullSink),
        session,
        workspace.path().to_path_buf(),
    );
    resumed.set_execution_context(context);
    let error = resumed.run("wait", "message-unknown").await.unwrap_err();

    assert!(matches!(error, AgentError::ReconciliationRequired { .. }));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let phases = ledger.records_for_run(&RunId::from("unknown-resume-run")).unwrap();
    assert!(
        !phases
            .iter()
            .any(|record| { record.record_type == "agent_task_phase" && record.payload["phase"] == "completed" })
    );
}

#[tokio::test]
async fn tool_results_are_saved_before_tools_completed_phase() {
    let workspace = tempdir().unwrap();
    let sessions = tempdir().unwrap();
    let mut config = test_config(workspace.path());
    config.session.directory = sessions.path().to_string_lossy().into_owned();
    let ledger = test_ledger(workspace.path());
    let context = EffectExecutionContext::new(
        RunId::from("tool-checkpoint-run"),
        AgentId::from("root"),
        ledger.clone(),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let mut tools = ToolRegistry::new();
    tools.register(Box::new(CheckpointTool));
    let mut engine = AgentEngine::new_with_provider(
        Arc::new(ToolThenBlockingProvider {
            calls: AtomicUsize::new(0),
        }),
        config,
        tools,
        Arc::new(NullSink),
        workspace.path().to_path_buf(),
    );
    engine.set_execution_context(context);
    engine
        .init_session("openai", &workspace.path().to_string_lossy(), Some("tool-checkpoint"))
        .unwrap();
    let running = tokio::spawn(async move { engine.run("use tool", "message-tool").await });
    wait_for_phase(&ledger, "tool-checkpoint-run", "tools_completed").await;

    let connection = rusqlite::Connection::open(sessions.path().join("session.sqlite3")).unwrap();
    let state: Vec<u8> = connection
        .query_row(
            "SELECT state_json FROM sessions WHERE session_id = 'tool-checkpoint'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let session: crate::session::Session = serde_json::from_slice(&state).unwrap();
    assert!(session.messages.last().is_some_and(|message| {
        message.role == solaris_types::message::Role::User
            && message
                .content
                .iter()
                .any(|block| matches!(block, solaris_types::message::ContentBlock::ToolResult { .. }))
    }));
    running.abort();
    let _ = running.await;
}

#[tokio::test]
async fn provider_phase_commit_then_audit_error_recovers_without_provider_retry() {
    let workspace = tempdir().unwrap();
    let sessions = tempdir().unwrap();
    let mut config = test_config(workspace.path());
    config.session.directory = sessions.path().to_string_lossy().into_owned();
    let calls = Arc::new(AtomicUsize::new(0));
    let provider = Arc::new(CountingProvider { calls: calls.clone() });
    let inner = test_ledger(workspace.path());
    let ledger = Arc::new(FailProviderCompletedAuditOnce {
        inner,
        armed: std::sync::atomic::AtomicBool::new(true),
    });
    let context = EffectExecutionContext::new(
        RunId::from("provider-commit-error-run"),
        AgentId::from("root"),
        ledger,
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let mut first = AgentEngine::new_with_provider(
        provider.clone(),
        config.clone(),
        ToolRegistry::new(),
        Arc::new(NullSink),
        workspace.path().to_path_buf(),
    );
    first.set_execution_context(context.clone());
    first
        .init_session(
            "openai",
            &workspace.path().to_string_lossy(),
            Some("provider-commit-error"),
        )
        .unwrap();
    let first_error = first.run("finish", "message-commit-error").await.unwrap_err();
    assert!(
        first_error
            .to_string()
            .contains("durable task phase persistence failed")
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    first.run_stop_hooks().await;
    drop(first);

    let session = crate::session::SessionManager::new(sessions.path().to_path_buf(), 20)
        .load("provider-commit-error")
        .unwrap();
    let mut resumed = AgentEngine::resume_with_provider(
        provider,
        config,
        ToolRegistry::new(),
        Arc::new(NullSink),
        session,
        workspace.path().to_path_buf(),
    );
    resumed.set_execution_context(context);
    let result = resumed.run("finish", "message-commit-error").await.unwrap();

    assert_eq!(result.text, "done");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn provider_resume_rejects_changed_model_before_provider_execution() {
    let workspace = tempdir().unwrap();
    let sessions = tempdir().unwrap();
    let (config, calls, context) =
        stage_provider_completed_task(workspace.path(), sessions.path(), "changed-model", "changed-model-run").await;
    let session = crate::session::SessionManager::new(sessions.path().to_path_buf(), 20)
        .load("changed-model")
        .unwrap();
    let mut resumed = AgentEngine::resume_with_provider(
        Arc::new(CountingProvider { calls: calls.clone() }),
        config,
        ToolRegistry::new(),
        Arc::new(NullSink),
        session,
        workspace.path().to_path_buf(),
    );
    resumed.set_execution_context(context);
    let update = resumed.apply_config_update(Some("different-model".to_owned()), None, None, None, None);
    assert!(update.applied);

    let error = resumed.run("finish", "message-changed-model").await.unwrap_err();

    assert!(matches!(error, AgentError::ReconciliationRequired { .. }));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn provider_resume_rejects_changed_request_before_provider_execution() {
    let workspace = tempdir().unwrap();
    let sessions = tempdir().unwrap();
    let (mut config, calls, context) = stage_provider_completed_task(
        workspace.path(),
        sessions.path(),
        "changed-request",
        "changed-request-run",
    )
    .await;
    config.system_prompt = Some("changed system instructions".to_owned());
    config.max_tokens = Some(17);
    let session = crate::session::SessionManager::new(sessions.path().to_path_buf(), 20)
        .load("changed-request")
        .unwrap();
    let mut resumed = AgentEngine::resume_with_provider(
        Arc::new(CountingProvider { calls: calls.clone() }),
        config,
        ToolRegistry::new(),
        Arc::new(NullSink),
        session,
        workspace.path().to_path_buf(),
    );
    resumed.set_execution_context(context);

    let error = resumed.run("finish", "message-changed-request").await.unwrap_err();

    assert!(matches!(error, AgentError::ReconciliationRequired { .. }));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn provider_in_flight_resume_checks_original_unknown_effect_without_retry() {
    let workspace = tempdir().unwrap();
    let sessions = tempdir().unwrap();
    let mut config = test_config(workspace.path());
    config.session.directory = sessions.path().to_string_lossy().into_owned();
    let ledger = test_ledger(workspace.path());
    let context = EffectExecutionContext::new(
        RunId::from("provider-in-flight-run"),
        AgentId::from("root"),
        ledger,
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let mut first = AgentEngine::new_with_provider(
        Arc::new(CompletedProvider),
        config.clone(),
        ToolRegistry::new(),
        Arc::new(NullSink),
        workspace.path().to_path_buf(),
    );
    first.set_execution_context(context.clone());
    first
        .init_session(
            "openai",
            &workspace.path().to_string_lossy(),
            Some("provider-in-flight"),
        )
        .unwrap();
    first.msg_id = "message-provider-in-flight".to_owned();
    first.messages.push(Message::now(
        Role::User,
        vec![ContentBlock::Text {
            text: "finish".to_owned(),
        }],
    ));
    first.save_session().unwrap();
    let _ = first.begin_or_resume_durable_task("finish").unwrap();
    let request = first.build_request(TurnKind::Normal);
    let input = serde_json::to_value(&request).unwrap();
    let call_id = provider_call_id(turn_kind_key(TurnKind::Normal), &stable_digest_value(&input));
    first
        .record_durable_task_phase(DurableTaskPhase::AwaitingProvider, Some(&call_id))
        .unwrap();
    let effect_request = context.effect_request(&call_id, "ProviderRequest", &input, first.provider_effect.clone());
    context.record_effect_intent(&effect_request).unwrap();
    first
        .record_durable_task_phase(DurableTaskPhase::ProviderInFlight, Some(&call_id))
        .unwrap();
    drop(first);

    let calls = Arc::new(AtomicUsize::new(0));
    let session = crate::session::SessionManager::new(sessions.path().to_path_buf(), 20)
        .load("provider-in-flight")
        .unwrap();
    let mut resumed = AgentEngine::resume_with_provider(
        Arc::new(CountingProvider { calls: calls.clone() }),
        config,
        ToolRegistry::new(),
        Arc::new(NullSink),
        session,
        workspace.path().to_path_buf(),
    );
    resumed.set_execution_context(context);

    let error = resumed.run("finish", "message-provider-in-flight").await.unwrap_err();

    assert!(matches!(error, AgentError::ReconciliationRequired { .. }));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn tools_in_flight_resume_rejects_changed_round_before_any_execution() {
    let workspace = tempdir().unwrap();
    let sessions = tempdir().unwrap();
    let (config, context, _) = stage_tools_in_flight(
        workspace.path(),
        sessions.path(),
        "changed-tool-round",
        "changed-tool-round-run",
        true,
        false,
    );
    let provider_calls = Arc::new(AtomicUsize::new(0));
    let tool_calls = Arc::new(AtomicUsize::new(0));
    let mut tools = ToolRegistry::new();
    tools.register(Box::new(CountingCheckpointTool {
        calls: tool_calls.clone(),
    }));
    let session = crate::session::SessionManager::new(sessions.path().to_path_buf(), 20)
        .load("changed-tool-round")
        .unwrap();
    let mut resumed = AgentEngine::resume_with_provider(
        Arc::new(CountingProvider {
            calls: provider_calls.clone(),
        }),
        config,
        tools,
        Arc::new(NullSink),
        session,
        workspace.path().to_path_buf(),
    );
    resumed.set_execution_context(context);

    let error = resumed.run("use tool", "message-changed-tool-round").await.unwrap_err();

    assert!(matches!(error, AgentError::ReconciliationRequired { .. }));
    assert_eq!(tool_calls.load(Ordering::SeqCst), 0);
    assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn tools_in_flight_resume_with_same_round_executes_once() {
    let workspace = tempdir().unwrap();
    let sessions = tempdir().unwrap();
    let (config, context, _) = stage_tools_in_flight(
        workspace.path(),
        sessions.path(),
        "same-tool-round",
        "same-tool-round-run",
        false,
        false,
    );
    let provider_calls = Arc::new(AtomicUsize::new(0));
    let tool_calls = Arc::new(AtomicUsize::new(0));
    let mut tools = ToolRegistry::new();
    tools.register(Box::new(CountingCheckpointTool {
        calls: tool_calls.clone(),
    }));
    let session = crate::session::SessionManager::new(sessions.path().to_path_buf(), 20)
        .load("same-tool-round")
        .unwrap();
    let mut resumed = AgentEngine::resume_with_provider(
        Arc::new(CountingProvider {
            calls: provider_calls.clone(),
        }),
        config,
        tools,
        Arc::new(NullSink),
        session,
        workspace.path().to_path_buf(),
    );
    resumed.set_execution_context(context);

    let result = resumed.run("use tool", "message-same-tool-round").await.unwrap();

    assert_eq!(result.text, "done");
    assert_eq!(tool_calls.load(Ordering::SeqCst), 1);
    assert_eq!(provider_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn legacy_v1_tools_in_flight_resume_reuses_precounted_call_identity() {
    let workspace = tempdir().unwrap();
    let sessions = tempdir().unwrap();
    let (config, context, resources) = stage_tools_in_flight(
        workspace.path(),
        sessions.path(),
        "legacy-v1-tool-round",
        "legacy-v1-tool-round-run",
        false,
        true,
    );
    let provider_calls = Arc::new(AtomicUsize::new(0));
    let tool_calls = Arc::new(AtomicUsize::new(0));
    let mut tools = ToolRegistry::new();
    tools.register(Box::new(CountingCheckpointTool {
        calls: tool_calls.clone(),
    }));
    let session = crate::session::SessionManager::new(sessions.path().to_path_buf(), 20)
        .load("legacy-v1-tool-round")
        .unwrap();
    let mut resumed = AgentEngine::resume_with_provider(
        Arc::new(CountingProvider {
            calls: provider_calls.clone(),
        }),
        config,
        tools,
        Arc::new(NullSink),
        session,
        workspace.path().to_path_buf(),
    );
    resumed.set_execution_context(context);

    let result = resumed.run("use tool", "message-legacy-v1-tool-round").await.unwrap();

    assert_eq!(result.text, "done");
    assert_eq!(tool_calls.load(Ordering::SeqCst), 1);
    assert_eq!(provider_calls.load(Ordering::SeqCst), 1);
    assert_eq!(resources.usage().tool_calls, 1);
    assert_eq!(resources.usage().useful_tool_calls, 1);
}

async fn stage_provider_completed_task(
    workspace: &Path,
    sessions: &Path,
    session_id: &str,
    run_id: &str,
) -> (Config, Arc<AtomicUsize>, EffectExecutionContext) {
    let mut config = test_config(workspace);
    config.session.directory = sessions.to_string_lossy().into_owned();
    let calls = Arc::new(AtomicUsize::new(0));
    let provider = Arc::new(CountingProvider { calls: calls.clone() });
    let ledger = Arc::new(FailProviderCompletedAuditOnce {
        inner: test_ledger(workspace),
        armed: std::sync::atomic::AtomicBool::new(true),
    });
    let context = EffectExecutionContext::new(
        RunId::from(run_id),
        AgentId::from("root"),
        ledger,
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let mut first = AgentEngine::new_with_provider(
        provider,
        config.clone(),
        ToolRegistry::new(),
        Arc::new(NullSink),
        workspace.to_path_buf(),
    );
    first.set_execution_context(context.clone());
    first
        .init_session("openai", &workspace.to_string_lossy(), Some(session_id))
        .unwrap();
    let error = first.run("finish", &format!("message-{session_id}")).await.unwrap_err();
    assert!(error.to_string().contains("durable task phase persistence failed"));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    first.run_stop_hooks().await;
    drop(first);
    (config, calls, context)
}

fn stage_tools_in_flight(
    workspace: &Path,
    sessions: &Path,
    session_id: &str,
    run_id: &str,
    change_saved_round: bool,
    legacy_v1_precounted: bool,
) -> (Config, EffectExecutionContext, Arc<ResourceManager>) {
    let mut config = test_config(workspace);
    config.session.directory = sessions.to_string_lossy().into_owned();
    let ledger = test_ledger(workspace);
    let resources = ResourceManager::new(ResourceBudget::default());
    let context = EffectExecutionContext::new(
        RunId::from(run_id),
        AgentId::from("root"),
        ledger,
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    )
    .with_resource_manager(resources.clone());
    let mut first = AgentEngine::new_with_provider(
        Arc::new(CompletedProvider),
        config.clone(),
        ToolRegistry::new(),
        Arc::new(NullSink),
        workspace.to_path_buf(),
    );
    first.set_execution_context(context.clone());
    first
        .init_session("openai", &workspace.to_string_lossy(), Some(session_id))
        .unwrap();
    let message_id = format!("message-{session_id}");
    first.msg_id.clone_from(&message_id);
    first.messages.push(Message::now(
        Role::User,
        vec![ContentBlock::Text {
            text: "use tool".to_owned(),
        }],
    ));
    let original_calls = vec![checkpoint_tool_call(false)];
    first
        .messages
        .push(Message::now(Role::Assistant, original_calls.clone()));
    first.save_session().unwrap();
    let _ = first.begin_or_resume_durable_task("use tool").unwrap();
    let call_id = if legacy_v1_precounted {
        let round_input = serde_json::to_value(&original_calls).unwrap();
        let call_id = legacy_tool_round_call_id(&stable_digest_value(&round_input));
        context
            .record_tool_calls_once(
                &call_id,
                &[solaris_types::tool::ToolCallStat::new(
                    "task:legacy|env:legacy",
                    "CheckpointTool",
                    &serde_json::json!({"changed": false}),
                    ToolResultStatus::Executed,
                )],
            )
            .unwrap();
        call_id
    } else {
        first.current_tool_round_call_id(&original_calls).unwrap()
    };
    first
        .record_durable_task_phase(DurableTaskPhase::ToolsInFlight, Some(&call_id))
        .unwrap();
    if change_saved_round {
        first.messages.last_mut().unwrap().content = vec![checkpoint_tool_call(true)];
        first.save_session().unwrap();
    }
    drop(first);
    (config, context, resources)
}

fn checkpoint_tool_call(changed: bool) -> ContentBlock {
    ContentBlock::ToolUse {
        id: "checkpoint-call".to_owned(),
        name: "CheckpointTool".to_owned(),
        input: serde_json::json!({"changed": changed}),
        extra: None,
    }
}

async fn wait_for_phase(ledger: &Arc<TestRuntimeLedger>, run_id: &str, phase: &str) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if ledger
                .records_for_run(&RunId::from(run_id))
                .unwrap()
                .iter()
                .any(|record| record.record_type == "agent_task_phase" && record.payload["phase"] == phase)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

fn test_config(workspace: &std::path::Path) -> Config {
    Config::resolve(&CliArgs {
        provider: Some("openai".to_owned()),
        api_key: Some("test-key".to_owned()),
        base_url: Some("https://provider.example.test/v1".to_owned()),
        model: Some("model".to_owned()),
        max_tokens: None,
        thinking: None,
        thinking_budget: None,
        max_turns: None,
        max_tool_call_malformed_turns: None,
        max_tool_call_failure_turns: None,
        system_prompt: None,
        profile: None,
        auto_approve: true,
        project_dir: Some(workspace.to_path_buf()),
    })
    .unwrap()
}

#[tokio::test]
async fn ordinary_agent_records_provider_and_terminal_task_phases() {
    let workspace = tempdir().unwrap();
    let config = Config::resolve(&CliArgs {
        provider: Some("openai".to_owned()),
        api_key: Some("test-key".to_owned()),
        base_url: Some("https://provider.example.test/v1".to_owned()),
        model: Some("model".to_owned()),
        max_tokens: None,
        thinking: None,
        thinking_budget: None,
        max_turns: None,
        max_tool_call_malformed_turns: None,
        max_tool_call_failure_turns: None,
        system_prompt: None,
        profile: None,
        auto_approve: true,
        project_dir: Some(workspace.path().to_path_buf()),
    })
    .unwrap();
    let ledger = test_ledger(workspace.path());
    let mut engine = AgentEngine::new_with_provider(
        Arc::new(CompletedProvider),
        config,
        ToolRegistry::new(),
        Arc::new(NullSink),
        workspace.path().to_path_buf(),
    );
    engine.set_execution_context(EffectExecutionContext::new(
        RunId::from("ordinary-task-run"),
        AgentId::from("root"),
        ledger.clone(),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    ));

    let result = engine.run("finish", "message-1").await.unwrap();

    assert_eq!(result.text, "done");
    let phases: Vec<_> = ledger
        .records_for_run(&RunId::from("ordinary-task-run"))
        .unwrap()
        .into_iter()
        .filter(|record| record.record_type == "agent_task_phase")
        .map(|record| record.payload["phase"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(
        phases,
        [
            "awaiting_provider",
            "provider_in_flight",
            "provider_completed",
            "completed"
        ]
    );
}

#[tokio::test]
async fn cancelled_provider_attempt_is_persisted_as_outcome_unknown() {
    let workspace = tempdir().unwrap();
    let config = Config::resolve(&CliArgs {
        provider: Some("openai".to_owned()),
        api_key: Some("test-key".to_owned()),
        base_url: Some("https://provider.example.test/v1".to_owned()),
        model: Some("model".to_owned()),
        max_tokens: None,
        thinking: None,
        thinking_budget: None,
        max_turns: None,
        max_tool_call_malformed_turns: None,
        max_tool_call_failure_turns: None,
        system_prompt: None,
        profile: None,
        auto_approve: true,
        project_dir: Some(workspace.path().to_path_buf()),
    })
    .unwrap();
    let ledger = test_ledger(workspace.path());
    let mut engine = AgentEngine::new_with_provider(
        Arc::new(BlockingProvider),
        config,
        ToolRegistry::new(),
        Arc::new(NullSink),
        workspace.path().to_path_buf(),
    );
    engine.set_execution_context(EffectExecutionContext::new(
        RunId::from("cancelled-task-run"),
        AgentId::from("root"),
        ledger.clone(),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    ));
    let run = tokio::spawn(async move { engine.run("wait", "message-2").await });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let in_flight = ledger
                .records_for_run(&RunId::from("cancelled-task-run"))
                .unwrap()
                .iter()
                .any(|record| record.payload["phase"] == "provider_in_flight");
            if in_flight {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    run.abort();
    let _ = run.await;

    let records = ledger.records_for_run(&RunId::from("cancelled-task-run")).unwrap();
    assert!(
        records
            .iter()
            .any(|record| { record.record_type == "agent_task_phase" && record.payload["phase"] == "outcome_unknown" })
    );
    assert!(records.iter().any(|record| record.record_type == "effect_outcome"));
}
