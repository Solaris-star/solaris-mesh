use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rusqlite::Connection;
use solaris_compact::CompactLevel;
use solaris_config::config::{CliArgs, Config};
use solaris_providers::error::ProviderError;
use solaris_providers::provider::LlmProvider;
use solaris_tools::registry::ToolRegistry;
use solaris_types::llm::{LlmEvent, LlmRequest, ThinkingConfig};
use solaris_types::message::{StopReason, TokenUsage};
use tempfile::tempdir;

use super::AgentEngine;
use crate::output::null_sink::NullSink;
use crate::session::{Session, SessionManager};

#[derive(Default)]
struct RecordingProvider {
    requests: Mutex<Vec<LlmRequest>>,
}

#[async_trait]
impl LlmProvider for RecordingProvider {
    async fn stream(&self, request: &LlmRequest) -> Result<tokio::sync::mpsc::Receiver<LlmEvent>, ProviderError> {
        self.requests.lock().unwrap().push(request.clone());
        let (tx, rx) = tokio::sync::mpsc::channel(2);
        tx.send(LlmEvent::TextDelta("saved".to_owned())).await.unwrap();
        tx.send(LlmEvent::Done {
            stop_reason: StopReason::EndTurn,
            usage: TokenUsage::default(),
        })
        .await
        .unwrap();
        Ok(rx)
    }
}

fn config_for(workspace: &Path, sessions: &Path, model: &str) -> Config {
    config_for_provider(workspace, sessions, "openai", model)
}

fn config_for_provider(workspace: &Path, sessions: &Path, provider: &str, model: &str) -> Config {
    let mut config = Config::resolve(&CliArgs {
        provider: Some(provider.to_owned()),
        api_key: Some("test-key".to_owned()),
        base_url: Some("https://provider.example.test/v1".to_owned()),
        model: Some(model.to_owned()),
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
    .unwrap();
    config.session.enabled = true;
    config.session.directory = sessions.to_string_lossy().into_owned();
    config
}

#[tokio::test]
async fn resume_uses_persisted_session_model_for_runtime_configuration_and_next_request() {
    let workspace = tempdir().unwrap();
    let sessions = workspace.path().join("sessions");
    let config_a = config_for(workspace.path(), &sessions, "model-a");
    let first_provider = Arc::new(RecordingProvider::default());
    let mut first_engine = AgentEngine::new_with_provider(
        first_provider,
        config_a.clone(),
        ToolRegistry::new(),
        Arc::new(NullSink),
        workspace.path().to_path_buf(),
    );
    first_engine
        .init_session("openai", &workspace.path().to_string_lossy(), Some("resume-model"))
        .unwrap();
    assert!(
        first_engine
            .apply_config_update(Some("model-b".to_owned()), None, None, None, None)
            .applied
    );
    first_engine.run("save the session", "first-run").await.unwrap();
    first_engine.run_stop_hooks().await;

    let session = SessionManager::new(sessions.clone(), 20).load("resume-model").unwrap();
    assert_eq!(session.model, "model-b");

    let resumed_provider = Arc::new(RecordingProvider::default());
    let mut resumed = AgentEngine::resume_with_provider(
        resumed_provider.clone(),
        config_a,
        ToolRegistry::new(),
        Arc::new(NullSink),
        session,
        workspace.path().to_path_buf(),
    );

    assert_eq!(resumed.model(), "model-b");
    assert_eq!(resumed.runtime_configuration_view().snapshot().model, "model-b");
    resumed.run("continue", "resumed-run").await.unwrap();
    resumed.run_stop_hooks().await;
    assert_eq!(resumed_provider.requests.lock().unwrap()[0].model, "model-b");
    let saved = SessionManager::new(sessions, 20).load("resume-model").unwrap();
    assert_eq!(saved.provider, "openai");
    assert_eq!(saved.model, "model-b");
}

#[tokio::test]
async fn legacy_session_without_run_id_is_bound_on_active_resume() {
    let workspace = tempdir().unwrap();
    let sessions = workspace.path().join("sessions");
    fs::create_dir_all(&sessions).unwrap();
    let legacy: Session = serde_json::from_value(serde_json::json!({
        "id": "legacy-run-binding",
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
        "provider": "openai",
        "model": "model-a",
        "cwd": workspace.path().to_string_lossy(),
        "total_usage": TokenUsage::default(),
        "messages": []
    }))
    .unwrap();
    fs::write(
        sessions.join("legacy-run-binding.json"),
        serde_json::to_vec_pretty(&legacy).unwrap(),
    )
    .unwrap();
    let loaded = SessionManager::new(sessions.clone(), 20)
        .load("legacy-run-binding")
        .unwrap();
    let provider = Arc::new(RecordingProvider::default());
    let mut engine = AgentEngine::resume_with_provider(
        provider,
        config_for(workspace.path(), &sessions, "model-a"),
        ToolRegistry::new(),
        Arc::new(NullSink),
        loaded,
        workspace.path().to_path_buf(),
    );
    let expected_run_id = engine.execution_context().unwrap().run_id().to_string();

    engine.run("continue", "legacy-run").await.unwrap();
    engine.run_stop_hooks().await;

    let connection = Connection::open(sessions.join("session.sqlite3")).unwrap();
    let (stored_run_id, reference_count): (Option<String>, i64) = connection
        .query_row(
            "SELECT sessions.run_id, COUNT(session_run_references.session_id)
             FROM sessions
             LEFT JOIN session_run_references
               ON session_run_references.session_id = sessions.session_id
              AND session_run_references.run_id = sessions.run_id
              AND session_run_references.reference_kind = 'session'
             WHERE sessions.session_id = 'legacy-run-binding'
             GROUP BY sessions.session_id",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(stored_run_id.as_deref(), Some(expected_run_id.as_str()));
    assert_eq!(reference_count, 1);
}

#[tokio::test]
async fn resume_uses_current_provider_model_pair_when_session_provider_differs() {
    let workspace = tempdir().unwrap();
    let sessions = workspace.path().join("sessions");
    let session = SessionManager::new(sessions.clone(), 20)
        .create(
            "openai",
            "openai-only-model",
            &workspace.path().to_string_lossy(),
            Some("cross-provider"),
        )
        .unwrap();
    let current_config = config_for_provider(workspace.path(), &sessions, "anthropic", "current-model");
    let resumed_provider = Arc::new(RecordingProvider::default());
    let mut resumed = AgentEngine::resume_with_provider(
        resumed_provider.clone(),
        current_config,
        ToolRegistry::new(),
        Arc::new(NullSink),
        session,
        workspace.path().to_path_buf(),
    );

    let snapshot = resumed.runtime_configuration_view().snapshot();
    assert_eq!(snapshot.provider, "anthropic");
    assert_eq!(snapshot.model, "current-model");
    assert_eq!(resumed.model(), "current-model");

    resumed.run("continue", "cross-provider-run").await.unwrap();
    assert_eq!(resumed_provider.requests.lock().unwrap()[0].model, "current-model");
    let saved = SessionManager::new(sessions, 20).load("cross-provider").unwrap();
    assert_eq!(saved.provider, "anthropic");
    assert_eq!(saved.model, "current-model");
}

#[test]
fn resume_uses_current_config_model_when_a_legacy_session_has_no_model() {
    let workspace = tempdir().unwrap();
    let sessions = workspace.path().join("sessions");
    let config = config_for(workspace.path(), &sessions, "current-model");
    let mut session = SessionManager::new(sessions, 20)
        .create(
            "openai",
            "old-model",
            &workspace.path().to_string_lossy(),
            Some("legacy-model"),
        )
        .unwrap();
    session.model.clear();

    let engine = AgentEngine::resume_with_provider(
        Arc::new(RecordingProvider::default()),
        config,
        ToolRegistry::new(),
        Arc::new(NullSink),
        session,
        workspace.path().to_path_buf(),
    );

    assert_eq!(engine.model(), "current-model");
    assert_eq!(engine.runtime_configuration_view().snapshot().model, "current-model");
}

#[test]
fn resume_uses_current_pair_when_legacy_session_has_no_provider_or_model() {
    let workspace = tempdir().unwrap();
    let sessions = workspace.path().join("sessions");
    let config = config_for_provider(workspace.path(), &sessions, "anthropic", "current-model");
    let session: Session = serde_json::from_value(serde_json::json!({
        "id": "legacy-pair",
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
        "cwd": workspace.path().to_string_lossy(),
        "total_usage": TokenUsage::default(),
        "messages": []
    }))
    .unwrap();
    assert!(session.provider.is_empty());
    assert!(session.model.is_empty());

    let resumed = AgentEngine::resume_with_provider(
        Arc::new(RecordingProvider::default()),
        config,
        ToolRegistry::new(),
        Arc::new(NullSink),
        session,
        workspace.path().to_path_buf(),
    );

    let snapshot = resumed.runtime_configuration_view().snapshot();
    assert_eq!(snapshot.provider, "anthropic");
    assert_eq!(snapshot.model, "current-model");
}

#[tokio::test]
async fn resume_uses_current_pair_when_session_provider_and_model_are_empty() {
    let workspace = tempdir().unwrap();
    let sessions = workspace.path().join("sessions");
    let session = SessionManager::new(sessions.clone(), 20)
        .create("", "", &workspace.path().to_string_lossy(), Some("empty-pair"))
        .unwrap();
    let config = config_for_provider(workspace.path(), &sessions, "anthropic", "current-model");
    let mut resumed = AgentEngine::resume_with_provider(
        Arc::new(RecordingProvider::default()),
        config,
        ToolRegistry::new(),
        Arc::new(NullSink),
        session,
        workspace.path().to_path_buf(),
    );

    let snapshot = resumed.runtime_configuration_view().snapshot();
    assert_eq!(snapshot.provider, "anthropic");
    assert_eq!(snapshot.model, "current-model");
    resumed.run("continue", "empty-pair-run").await.unwrap();
    let saved = SessionManager::new(sessions, 20).load("empty-pair").unwrap();
    assert_eq!(saved.provider, "anthropic");
    assert_eq!(saved.model, "current-model");
}

#[test]
fn initial_and_resumed_snapshots_reflect_current_turn_configuration() {
    let workspace = tempdir().unwrap();
    let sessions = workspace.path().join("sessions");
    let mut config = config_for(workspace.path(), &sessions, "configured-model");
    config.thinking = Some(ThinkingConfig::Enabled { budget_tokens: 24_000 });
    config.compact.compaction = CompactLevel::Full;

    let initial = AgentEngine::new_with_provider(
        Arc::new(RecordingProvider::default()),
        config.clone(),
        ToolRegistry::new(),
        Arc::new(NullSink),
        workspace.path().to_path_buf(),
    );
    let initial_snapshot = initial.runtime_configuration_view().snapshot();
    assert_eq!(
        initial_snapshot.thinking,
        Some(ThinkingConfig::Enabled { budget_tokens: 24_000 })
    );
    assert_eq!(initial_snapshot.thinking_budget, Some(24_000));
    assert_eq!(initial_snapshot.compaction, CompactLevel::Full);

    let session = SessionManager::new(sessions, 20)
        .create(
            "openai",
            "configured-model",
            &workspace.path().to_string_lossy(),
            Some("runtime-configuration"),
        )
        .unwrap();
    let resumed = AgentEngine::resume_with_provider(
        Arc::new(RecordingProvider::default()),
        config,
        ToolRegistry::new(),
        Arc::new(NullSink),
        session,
        workspace.path().to_path_buf(),
    );
    let resumed_snapshot = resumed.runtime_configuration_view().snapshot();
    assert_eq!(resumed_snapshot.thinking_budget, Some(24_000));
    assert_eq!(resumed_snapshot.compaction, CompactLevel::Full);
}
