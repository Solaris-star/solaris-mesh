use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::task::Poll;

use async_trait::async_trait;
use chrono::Utc;
use rusqlite::Connection;
use serde_json::Value;
use tempfile::TempDir;

use solaris_config::config::{CliArgs, Config};
use solaris_providers::error::ProviderError;
use solaris_providers::provider::LlmProvider;
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::{AgentId, OperationId, RunId, TaskId};
use solaris_types::llm::{LlmEvent, LlmRequest};
use solaris_types::message::{ContentBlock, Role, StopReason, TokenUsage};
use solaris_types::permission::PermissionCeiling;
use solaris_types::resource::ResourceBudget;
use solaris_types::runtime::{AgentLifecycleState, TaskFailureClass};
use solaris_types::spawner::{
    AgentConversationConfig, AgentConversationSpec, AgentOutcomeStatus, AgentTurnSpec, ForkOverrides,
};

use crate::collaboration_runtime::CollaborationRuntime;
use crate::resource_manager::ResourceManager;
use crate::resource_policy::ResourcePolicy;
use crate::runtime_ledger::{InMemoryRuntimeLedger, LedgerRecord, RuntimeLedger};
use crate::scheduler::Scheduler;
use crate::session::SessionManager;

use super::*;

#[derive(Default)]
struct RecordingProvider {
    calls: AtomicUsize,
    delay_ms: AtomicUsize,
    empty: AtomicBool,
    fail: AtomicBool,
    input_tokens: AtomicUsize,
    prompts: Mutex<Vec<String>>,
    pause_next: Mutex<Option<(Arc<tokio::sync::Barrier>, Arc<tokio::sync::Barrier>)>>,
}

impl RecordingProvider {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn set_delay_ms(&self, delay_ms: usize) {
        self.delay_ms.store(delay_ms, Ordering::SeqCst);
    }

    fn set_fail(&self, fail: bool) {
        self.fail.store(fail, Ordering::SeqCst);
    }

    fn set_empty(&self, empty: bool) {
        self.empty.store(empty, Ordering::SeqCst);
    }

    fn set_input_tokens(&self, input_tokens: usize) {
        self.input_tokens.store(input_tokens, Ordering::SeqCst);
    }

    fn prompts(&self) -> Vec<String> {
        self.prompts.lock().unwrap().clone()
    }

    fn pause_next_call(&self) -> (Arc<tokio::sync::Barrier>, Arc<tokio::sync::Barrier>) {
        let reached = Arc::new(tokio::sync::Barrier::new(2));
        let resume = Arc::new(tokio::sync::Barrier::new(2));
        *self.pause_next.lock().unwrap() = Some((Arc::clone(&reached), Arc::clone(&resume)));
        (reached, resume)
    }
}

#[async_trait]
impl LlmProvider for RecordingProvider {
    async fn stream(&self, request: &LlmRequest) -> Result<tokio::sync::mpsc::Receiver<LlmEvent>, ProviderError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        let prompt = request
            .messages
            .iter()
            .rev()
            .find(|message| message.role == Role::User)
            .and_then(|message| {
                message.content.iter().find_map(|block| match block {
                    ContentBlock::Text { text } => Some(text.clone()),
                    _ => None,
                })
            })
            .unwrap_or_default();
        self.prompts.lock().unwrap().push(prompt);
        let pause = self.pause_next.lock().unwrap().take();
        if let Some((reached, resume)) = pause {
            reached.wait().await;
            resume.wait().await;
        }
        let delay_ms = self.delay_ms.load(Ordering::SeqCst);
        if delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(delay_ms as u64)).await;
        }
        if self.fail.load(Ordering::SeqCst) {
            return Err(ProviderError::Connection("injected Provider failure".to_owned()));
        }
        let (tx, rx) = tokio::sync::mpsc::channel(2);
        if !self.empty.load(Ordering::SeqCst) {
            tx.send(LlmEvent::TextDelta(format!("reply-{call}"))).await.unwrap();
        }
        tx.send(LlmEvent::Done {
            stop_reason: StopReason::EndTurn,
            usage: TokenUsage {
                input_tokens: self.input_tokens.load(Ordering::SeqCst) as u64,
                ..TokenUsage::default()
            },
        })
        .await
        .unwrap();
        Ok(rx)
    }
}

struct Harness {
    _workspace: TempDir,
    sessions: std::path::PathBuf,
    provider: Arc<RecordingProvider>,
    ledger: Arc<dyn RuntimeLedger>,
    run_id: RunId,
    parent_id: AgentId,
    service: AgentConversationService,
}

impl Harness {
    fn new() -> Self {
        Self::with_ledger(Arc::new(InMemoryRuntimeLedger::default()))
    }

    fn with_ledger(ledger: Arc<dyn RuntimeLedger>) -> Self {
        let workspace = tempfile::tempdir().unwrap();
        let sessions = workspace.path().join("sessions");
        let provider = Arc::new(RecordingProvider::default());
        let run_id = RunId::from("run-conversation-test");
        let parent_id = AgentId::from("coordinator");
        let service = service_with(
            provider.clone(),
            config_for(workspace.path(), &sessions, true),
            workspace.path(),
            Arc::clone(&ledger),
            run_id.clone(),
            parent_id.clone(),
            ResourceBudget::default(),
        );
        Self {
            _workspace: workspace,
            sessions,
            provider,
            ledger,
            run_id,
            parent_id,
            service,
        }
    }

    fn spec(&self) -> AgentConversationSpec {
        conversation_spec(self.run_id.clone(), self.parent_id.clone())
    }
}

fn config_for(workspace: &Path, sessions: &Path, sessions_enabled: bool) -> Config {
    let mut config = Config::resolve(&CliArgs {
        provider: Some("openai".to_owned()),
        api_key: Some("test-key".to_owned()),
        base_url: Some("https://provider.example.test/v1".to_owned()),
        model: Some("test-model".to_owned()),
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
    config.session.enabled = sessions_enabled;
    config.session.directory = sessions.to_string_lossy().into_owned();
    config
}

fn service_with(
    provider: Arc<RecordingProvider>,
    config: Config,
    workspace: &Path,
    ledger: Arc<dyn RuntimeLedger>,
    run_id: RunId,
    parent_id: AgentId,
    budget: ResourceBudget,
) -> AgentConversationService {
    let runtime = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(1)),
        ledger,
    ));
    let resources = ResourceManager::new(budget);
    let spawner = AgentSpawner::new(provider, config, workspace.to_path_buf())
        .with_resource_manager(resources)
        .with_runtime_context(runtime, run_id, parent_id);
    AgentConversationService::new(Arc::new(spawner))
}

fn conversation_spec(run_id: RunId, parent_id: AgentId) -> AgentConversationSpec {
    AgentConversationSpec {
        run_id,
        parent_agent_id: parent_id,
        task_id: TaskId::from("task-conversation"),
        conversation_id: "conversation-main".to_owned(),
        role_key: "worker".to_owned(),
        stable_task_key: "worker-main".to_owned(),
        operation_id: OperationId::from("open-conversation-main"),
        config: AgentConversationConfig {
            name: "worker".to_owned(),
            max_turns: 4,
            max_tokens: 256,
            system_prompt: Some("Work as a durable test worker.".to_owned()),
        },
        overrides: ForkOverrides {
            inherit_capabilities: true,
            ..ForkOverrides::default()
        },
        permission_ceiling: PermissionCeiling::unrestricted(),
        resource_budget: ResourceBudget::default(),
        context_policy: Some("isolated".to_owned()),
        recursion_limit: Some(2),
    }
}

fn turn(id: &str, prompt: &str) -> AgentTurnSpec {
    AgentTurnSpec {
        turn_id: id.to_owned(),
        prompt: prompt.to_owned(),
    }
}

fn output_text(outcome: &AgentTurnOutcome) -> &str {
    outcome.output.get("text").and_then(Value::as_str).unwrap()
}

include!("conversation_open_recovery_test.rs");
include!("conversation_turn_queue_test.rs");
include!("conversation_failure_test.rs");
include!("conversation_identity_test.rs");

#[derive(Default)]
struct AfterAppendFailureLedger {
    inner: InMemoryRuntimeLedger,
    fail_record: Mutex<Option<String>>,
    fail_recovery_read: AtomicBool,
    remaining_read_failures: AtomicUsize,
}

impl AfterAppendFailureLedger {
    fn fail_once(&self, record_type: &str) {
        *self.fail_record.lock().unwrap() = Some(record_type.to_owned());
    }

    fn fail_with_recovery_read_once(&self, record_type: &str) {
        self.fail_recovery_read.store(true, Ordering::SeqCst);
        self.fail_once(record_type);
    }
}

impl RuntimeLedger for AfterAppendFailureLedger {
    crate::runtime_ledger::forward_compare_and_append!();

    fn append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        payload: Value,
    ) -> io::Result<LedgerRecord> {
        let record = self.inner.append(run_id, durability, record_type, payload)?;
        let should_fail = self
            .fail_record
            .lock()
            .unwrap()
            .as_deref()
            .is_some_and(|selected| selected == record_type);
        if should_fail {
            *self.fail_record.lock().unwrap() = None;
            if self.fail_recovery_read.swap(false, Ordering::SeqCst) {
                self.remaining_read_failures.store(1, Ordering::SeqCst);
            }
            return Err(io::Error::other("injected error after durable append"));
        }
        Ok(record)
    }

    fn run_ids(&self) -> io::Result<Vec<RunId>> {
        self.inner.run_ids()
    }

    fn records_for_run(&self, run_id: &RunId) -> io::Result<Vec<LedgerRecord>> {
        if self
            .remaining_read_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| remaining.checked_sub(1))
            .is_ok()
        {
            return Err(io::Error::other("injected recovery read error"));
        }
        self.inner.records_for_run(run_id)
    }

    fn effect_output_root(&self) -> Option<std::path::PathBuf> {
        self.inner.effect_output_root()
    }
}

#[derive(Default)]
struct BeforeIdleFailureLedger {
    inner: InMemoryRuntimeLedger,
    fail_idle: AtomicBool,
}

impl BeforeIdleFailureLedger {
    fn fail_idle_once(&self) {
        self.fail_idle.store(true, Ordering::SeqCst);
    }
}

impl RuntimeLedger for BeforeIdleFailureLedger {
    crate::runtime_ledger::forward_compare_and_append!();

    fn append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        payload: Value,
    ) -> io::Result<LedgerRecord> {
        if record_type == "agent_state_changed"
            && payload.get("state").and_then(Value::as_str) == Some("idle")
            && self.fail_idle.swap(false, Ordering::SeqCst)
        {
            return Err(io::Error::other("injected idle state append error"));
        }
        self.inner.append(run_id, durability, record_type, payload)
    }

    fn run_ids(&self) -> io::Result<Vec<RunId>> {
        self.inner.run_ids()
    }

    fn records_for_run(&self, run_id: &RunId) -> io::Result<Vec<LedgerRecord>> {
        self.inner.records_for_run(run_id)
    }

    fn effect_output_root(&self) -> Option<std::path::PathBuf> {
        self.inner.effect_output_root()
    }
}
