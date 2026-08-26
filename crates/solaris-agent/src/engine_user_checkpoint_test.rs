use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use solaris_config::config::{CliArgs, Config};
use solaris_providers::error::ProviderError;
use solaris_providers::provider::LlmProvider;
use solaris_tools::registry::ToolRegistry;
use solaris_types::identity::{AgentId, RunId};
use solaris_types::llm::{LlmEvent, LlmRequest};
use solaris_types::message::{ContentBlock, Role, StopReason, TokenUsage};
use solaris_types::permission::{PermissionCeiling, PermissionMode};
use solaris_types::runtime::OperationEnvironmentSnapshot;
use tempfile::tempdir;

use super::AgentEngine;
use crate::execution_context::EffectExecutionContext;
use crate::output::null_sink::NullSink;
use crate::permission_engine::PermissionContext;
use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};

struct CapturingProvider {
    calls: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<LlmRequest>>>,
}

struct FailUserCheckpointAuditOnce {
    inner: InMemoryRuntimeLedger,
    effect_output_root: PathBuf,
    armed: AtomicBool,
}

impl RuntimeLedger for FailUserCheckpointAuditOnce {
    crate::runtime_ledger::forward_compare_and_append!();

    fn append(
        &self,
        run_id: &RunId,
        durability: solaris_types::effect::DurabilityClass,
        record_type: &str,
        payload: serde_json::Value,
    ) -> std::io::Result<crate::runtime_ledger::LedgerRecord> {
        if record_type == "agent_task_phase"
            && payload["phase"] == "user_checkpointed"
            && self.armed.swap(false, Ordering::SeqCst)
        {
            return Err(std::io::Error::other(
                "injected audit failure after atomic user checkpoint",
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

    fn effect_output_root(&self) -> Option<PathBuf> {
        Some(self.effect_output_root.clone())
    }
}

#[async_trait]
impl LlmProvider for CapturingProvider {
    async fn stream(&self, request: &LlmRequest) -> Result<tokio::sync::mpsc::Receiver<LlmEvent>, ProviderError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request.clone());
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

#[tokio::test]
async fn committed_user_checkpoint_resumes_without_duplicating_message() {
    let workspace = tempdir().unwrap();
    let sessions = tempdir().unwrap();
    let mut config = test_config(workspace.path());
    config.session.directory = sessions.path().to_string_lossy().into_owned();
    let calls = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let provider = Arc::new(CapturingProvider {
        calls: calls.clone(),
        requests: requests.clone(),
    });
    let ledger = Arc::new(FailUserCheckpointAuditOnce {
        inner: InMemoryRuntimeLedger::default(),
        effect_output_root: workspace.path().join("runtime-effect-outputs"),
        armed: AtomicBool::new(true),
    });
    let context = EffectExecutionContext::new(
        RunId::from("user-checkpoint-run"),
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
        .init_session("openai", &workspace.path().to_string_lossy(), Some("user-checkpoint"))
        .unwrap();

    let first_error = first.run("finish once", "message-user-checkpoint").await.unwrap_err();
    assert!(
        first_error
            .to_string()
            .contains("durable task phase persistence failed")
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    first.run_stop_hooks().await;
    drop(first);

    let connection = rusqlite::Connection::open(sessions.path().join("session.sqlite3")).unwrap();
    let (phase, call_id): (String, Option<String>) = connection
        .query_row(
            "SELECT phase, call_id FROM durable_agent_tasks
             WHERE session_id = 'user-checkpoint'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(phase, "user_checkpointed");
    assert!(call_id.is_none());

    let session = crate::session::SessionManager::new(sessions.path().to_path_buf(), 20)
        .load("user-checkpoint")
        .unwrap();
    assert_eq!(session.messages.len(), 1);
    let mut resumed = AgentEngine::resume_with_provider(
        provider,
        config,
        ToolRegistry::new(),
        Arc::new(NullSink),
        session,
        workspace.path().to_path_buf(),
    );
    resumed.set_execution_context(context);

    let result = resumed.run("finish once", "message-user-checkpoint").await.unwrap();

    assert_eq!(result.text, "done");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let requests = requests.lock().unwrap();
    let user_occurrences = requests[0]
        .messages
        .iter()
        .filter(|message| {
            message.role == Role::User
                && message
                    .content
                    .iter()
                    .any(|block| matches!(block, ContentBlock::Text { text } if text == "finish once"))
        })
        .count();
    assert_eq!(user_occurrences, 1);
}

fn test_config(workspace: &Path) -> Config {
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
