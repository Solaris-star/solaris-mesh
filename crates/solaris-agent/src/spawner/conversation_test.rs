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
use solaris_types::identity::{AgentId, OperationId, RunId, TaskId, TeamId};
use solaris_types::llm::{LlmEvent, LlmRequest};
use solaris_types::message::{ContentBlock, Role, StopReason, TokenUsage};
use solaris_types::permission::PermissionCeiling;
use solaris_types::resource::ResourceBudget;
use solaris_types::runtime::{AgentLifecycleState, TaskFailureClass};
use solaris_types::spawner::{
    AgentCollaborationContext, AgentConversationConfig, AgentConversationSpec, AgentOutcomeStatus, AgentTurnSpec,
    ForkOverrides,
};
use solaris_types::workflow::CollaborationStrategy;

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

#[derive(Default)]
struct AtomicMembershipFailureLedger {
    inner: InMemoryRuntimeLedger,
    atomic_calls: AtomicUsize,
    fail_on_call: AtomicUsize,
}

impl AtomicMembershipFailureLedger {
    fn fail_atomic_call(&self, call: usize) {
        self.fail_on_call.store(call, Ordering::SeqCst);
    }
}

impl RuntimeLedger for AtomicMembershipFailureLedger {
    fn logical_append_capability(&self) -> crate::runtime_ledger::LogicalAppendCapability {
        self.inner.logical_append_capability()
    }

    fn acquire_workflow_mutation_lease(
        &self,
        run_id: &RunId,
        owner_id: &str,
        now_unix_ms: i64,
    ) -> io::Result<crate::runtime_ledger::WorkflowMutationLease> {
        self.inner
            .acquire_workflow_mutation_lease(run_id, owner_id, now_unix_ms)
    }

    fn renew_workflow_mutation_lease(
        &self,
        lease: &crate::runtime_ledger::WorkflowMutationLease,
        now_unix_ms: i64,
    ) -> io::Result<crate::runtime_ledger::WorkflowMutationLease> {
        self.inner.renew_workflow_mutation_lease(lease, now_unix_ms)
    }

    fn commit_workflow_restore(
        &self,
        lease: &crate::runtime_ledger::WorkflowMutationLease,
        expected_sequence: u64,
        now_unix_ms: i64,
    ) -> io::Result<crate::runtime_ledger::WorkflowRestoreCommit> {
        self.inner
            .commit_workflow_restore(lease, expected_sequence, now_unix_ms)
    }

    fn release_workflow_mutation_lease(&self, lease: &crate::runtime_ledger::WorkflowMutationLease) -> io::Result<()> {
        self.inner.release_workflow_mutation_lease(lease)
    }

    fn compare_and_append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        identity_fields: &[&str],
        payload: Value,
    ) -> io::Result<LedgerRecord> {
        self.inner
            .compare_and_append(run_id, durability, record_type, identity_fields, payload)
    }

    fn append_under_workflow_lease(
        &self,
        lease: &crate::runtime_ledger::WorkflowMutationLease,
        now_unix_ms: i64,
        durability: DurabilityClass,
        record_type: &str,
        payload: Value,
    ) -> io::Result<LedgerRecord> {
        self.inner
            .append_under_workflow_lease(lease, now_unix_ms, durability, record_type, payload)
    }

    fn compare_and_append_under_workflow_lease(
        &self,
        lease: &crate::runtime_ledger::WorkflowMutationLease,
        now_unix_ms: i64,
        durability: DurabilityClass,
        record_type: &str,
        identity_fields: &[&str],
        payload: Value,
    ) -> io::Result<LedgerRecord> {
        self.inner.compare_and_append_under_workflow_lease(
            lease,
            now_unix_ms,
            durability,
            record_type,
            identity_fields,
            payload,
        )
    }

    fn admit_collaboration_tasks_for_root(
        &self,
        root_run_id: &RunId,
        run_id: &RunId,
        max_tasks: usize,
        tasks: &[solaris_types::runtime::TaskRecord],
    ) -> io::Result<Vec<LedgerRecord>> {
        self.inner
            .admit_collaboration_tasks_for_root(root_run_id, run_id, max_tasks, tasks)
    }

    fn supports_atomic_task_metadata_admission(&self) -> bool {
        true
    }

    fn append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        payload: Value,
    ) -> io::Result<LedgerRecord> {
        self.inner.append(run_id, durability, record_type, payload)
    }

    fn admit_tasks_and_append(
        &self,
        root_run_id: &RunId,
        run_id: &RunId,
        max_tasks: usize,
        tasks: &[solaris_types::runtime::TaskRecord],
        records: &[(DurabilityClass, String, Value)],
    ) -> io::Result<Vec<LedgerRecord>> {
        let call = self.atomic_calls.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fail_on_call.load(Ordering::SeqCst) == call {
            return Err(io::Error::other("injected collaboration membership commit failure"));
        }
        self.inner
            .admit_tasks_and_append(root_run_id, run_id, max_tasks, tasks, records)
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

#[tokio::test]
async fn collaboration_open_membership_commit_failure_aborts_reserved_agent_and_is_retryable_by_exact_open() {
    let ledger = Arc::new(AtomicMembershipFailureLedger::default());
    ledger.fail_atomic_call(2);
    let harness = Harness::with_ledger(ledger.clone());
    let mut spec = harness.spec();
    let child_id = AgentConversationService::expected_agent_id(&spec);
    let team_id = TeamId::from("conversation-atomic-team");
    spec.overrides.collaboration = Some(AgentCollaborationContext {
        team_id: team_id.clone(),
        strategy: CollaborationStrategy::Team,
        coordinator_agent_id: harness.parent_id.clone(),
        max_pending_messages: 8,
        max_message_bytes: 1_024,
    });

    let error = harness
        .service
        .open(spec.clone())
        .await
        .expect_err("the second atomic collaboration commit is injected to fail");
    assert_eq!(error.failure_class, TaskFailureClass::ReconciliationRequired);
    assert!(
        error
            .message
            .contains("injected collaboration membership commit failure"),
        "unexpected failure path: {} (atomic calls={})",
        error.message,
        ledger.atomic_calls.load(Ordering::SeqCst)
    );

    let runtime = harness.service.spawner.lifecycle_runtime();
    assert!(
        runtime.agents().get(&child_id).is_none(),
        "failed open must remove the Reserved child"
    );
    let team = runtime
        .teams()
        .get(&team_id)
        .expect("the safe Team shell was committed first");
    assert!(team.members.contains(&harness.parent_id));
    assert!(!team.members.contains(&child_id));
    let records = ledger.records_for_run(&harness.run_id).unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|record| record.record_type == "collaboration_team_prepared")
            .count(),
        1,
        "only the durable Team shell may survive the failed membership transaction"
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| record.record_type == "collaboration_batch_prepared")
            .count(),
        0,
        "failed membership transaction must not append a full collaboration batch"
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| record.record_type == "agent_conversation_opened")
            .count(),
        0
    );
    assert!(records.iter().any(|record| record.record_type == "agent_spawn_aborted"));

    let handle = harness
        .service
        .open(spec.clone())
        .await
        .expect("exact retry must reacquire the abandoned opening claim and complete the membership batch");
    assert_eq!(handle.agent_id, child_id);
    let team = runtime.teams().get(&team_id).unwrap();
    assert!(team.members.contains(&harness.parent_id));
    assert!(team.members.contains(&child_id));
    let marker_count = ledger
        .records_for_run(&harness.run_id)
        .unwrap()
        .iter()
        .filter(|record| record.record_type == "collaboration_batch_prepared")
        .count();
    assert_eq!(marker_count, 1);

    let replay = harness.service.open(spec).await.expect("exact open replay succeeds");
    assert_eq!(replay.agent_id, child_id);
    assert_eq!(
        ledger
            .records_for_run(&harness.run_id)
            .unwrap()
            .iter()
            .filter(|record| record.record_type == "collaboration_batch_prepared")
            .count(),
        marker_count,
        "exact replay must not append another Team/membership batch"
    );
}
