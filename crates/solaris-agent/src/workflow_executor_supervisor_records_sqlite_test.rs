use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};

use async_trait::async_trait;
use solaris_config::config::{CliArgs, Config};
use solaris_providers::{LlmProvider, ProviderError};
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::{AgentId, RunId};
use solaris_types::llm::{LlmEvent, LlmRequest};
use solaris_types::runtime::{AgentLifecycleState, AgentRecord, TaskFailureClass};
use tokio::sync::mpsc;

use crate::collaboration_runtime::CollaborationRuntime;
use crate::resource_policy::ResourcePolicy;
use crate::role_registry::AgentRoleRegistry;
use crate::runtime_ledger::{JsonlRuntimeLedger, LedgerRecord, RuntimeLedger, SqliteRuntimeLedger};
use crate::scheduler::Scheduler;
use crate::spawner::AgentSpawner;

use super::*;

struct UnusedProvider;

#[async_trait]
impl LlmProvider for UnusedProvider {
    async fn stream(&self, _request: &LlmRequest) -> Result<mpsc::Receiver<LlmEvent>, ProviderError> {
        panic!("Supervisor record persistence must not call the Provider")
    }
}

struct ReadBarrierLedger {
    inner: Arc<SqliteRuntimeLedger>,
    barrier: Arc<Barrier>,
    first_read: AtomicBool,
}

impl ReadBarrierLedger {
    fn new(inner: Arc<SqliteRuntimeLedger>, barrier: Arc<Barrier>) -> Self {
        Self {
            inner,
            barrier,
            first_read: AtomicBool::new(true),
        }
    }
}

impl RuntimeLedger for ReadBarrierLedger {
    crate::runtime_ledger::forward_workflow_mutation_lease!();

    fn logical_append_capability(&self) -> crate::runtime_ledger::LogicalAppendCapability {
        self.inner.logical_append_capability()
    }

    fn compare_and_append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        identity_fields: &[&str],
        payload: Value,
    ) -> std::io::Result<LedgerRecord> {
        if self.first_read.swap(false, Ordering::SeqCst) {
            self.barrier.wait();
        }
        self.inner
            .compare_and_append(run_id, durability, record_type, identity_fields, payload)
    }

    fn append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        payload: Value,
    ) -> std::io::Result<LedgerRecord> {
        self.inner.append(run_id, durability, record_type, payload)
    }

    fn run_ids(&self) -> std::io::Result<Vec<RunId>> {
        self.inner.run_ids()
    }

    fn records_for_run(&self, run_id: &RunId) -> std::io::Result<Vec<LedgerRecord>> {
        self.inner.records_for_run(run_id)
    }

    fn effect_output_root(&self) -> Option<std::path::PathBuf> {
        self.inner.effect_output_root()
    }
}

fn test_config() -> Config {
    let mut config = Config::resolve(&CliArgs {
        provider: Some("anthropic".into()),
        api_key: Some("test".into()),
        base_url: None,
        model: Some("test-model".into()),
        max_tokens: None,
        thinking: None,
        thinking_budget: None,
        max_turns: None,
        max_tool_call_malformed_turns: None,
        max_tool_call_failure_turns: None,
        system_prompt: None,
        profile: None,
        auto_approve: true,
        project_dir: None,
    })
    .unwrap();
    config.session.enabled = false;
    config
}

fn supervisor_host(
    path: &Path,
    root_run: &RunId,
    root_agent: &AgentId,
    barrier: Arc<Barrier>,
) -> AgentWorkflowExecutor {
    let sqlite = Arc::new(SqliteRuntimeLedger::open(path).unwrap());
    let ledger: Arc<dyn RuntimeLedger> = Arc::new(ReadBarrierLedger::new(sqlite, barrier));
    supervisor_host_with_ledger(ledger, root_run, root_agent)
}

fn supervisor_host_with_ledger(
    ledger: Arc<dyn RuntimeLedger>,
    root_run: &RunId,
    root_agent: &AgentId,
) -> AgentWorkflowExecutor {
    let runtime = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(2)),
        ledger,
    ));
    runtime.agents().upsert(AgentRecord {
        run_id: root_run.clone(),
        agent_id: root_agent.clone(),
        team_id: None,
        parent_agent_id: None,
        state: AgentLifecycleState::Active,
    });
    let spawner = Arc::new(
        AgentSpawner::new(Arc::new(UnusedProvider), test_config(), std::env::temp_dir()).with_runtime_context(
            runtime,
            root_run.clone(),
            root_agent.clone(),
        ),
    );
    AgentWorkflowExecutor::new(spawner, Arc::new(AgentRoleRegistry::default()))
}

fn decision_payload(body: &str) -> Value {
    json!({
        "workflow_run_id": "workflow-run",
        "workflow_id": "workflow:test",
        "node_id": "node",
        "attempt_id": "attempt",
        "round": 0,
        "body": body,
    })
}

fn concurrent_persist(
    first: AgentWorkflowExecutor,
    second: AgentWorkflowExecutor,
    first_payload: Value,
    second_payload: Value,
) -> [Result<(), WorkflowNodeError>; 2] {
    let first = std::thread::spawn(move || {
        first.persist_unique_supervisor_record(
            SUPERVISOR_DECISION_RECORD,
            &["workflow_run_id", "workflow_id", "node_id", "attempt_id", "round"],
            first_payload,
        )
    });
    let second = std::thread::spawn(move || {
        second.persist_unique_supervisor_record(
            SUPERVISOR_DECISION_RECORD,
            &["workflow_run_id", "workflow_id", "node_id", "attempt_id", "round"],
            second_payload,
        )
    });
    [first.join().unwrap(), second.join().unwrap()]
}

#[test]
fn sqlite_supervisor_same_identity_same_payload_commits_once_across_hosts() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ledger.sqlite3");
    let root_run = RunId::from("supervisor-logical-append-same");
    let root_agent = AgentId::from("coordinator");
    let barrier = Arc::new(Barrier::new(2));
    let first = supervisor_host(&path, &root_run, &root_agent, Arc::clone(&barrier));
    let second = supervisor_host(&path, &root_run, &root_agent, barrier);
    let payload = decision_payload("same");

    let results = concurrent_persist(first, second, payload.clone(), payload);

    assert!(results.iter().all(Result::is_ok), "{results:?}");
    let records = SqliteRuntimeLedger::open(&path)
        .unwrap()
        .records_for_run(&root_run)
        .unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|record| record.record_type == SUPERVISOR_DECISION_RECORD)
            .count(),
        1,
        "the same logical Supervisor record must commit once"
    );
}

#[test]
fn sqlite_supervisor_same_identity_conflicting_payload_reconciles_across_hosts() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ledger.sqlite3");
    let root_run = RunId::from("supervisor-logical-append-conflict");
    let root_agent = AgentId::from("coordinator");
    let barrier = Arc::new(Barrier::new(2));
    let first = supervisor_host(&path, &root_run, &root_agent, Arc::clone(&barrier));
    let second = supervisor_host(&path, &root_run, &root_agent, barrier);

    let results = concurrent_persist(first, second, decision_payload("first"), decision_payload("second"));

    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1, "{results:?}");
    let error = results.iter().find_map(|result| result.as_ref().err()).unwrap();
    assert_eq!(error.failure_class, TaskFailureClass::ReconciliationRequired);
    let records = SqliteRuntimeLedger::open(&path)
        .unwrap()
        .records_for_run(&root_run)
        .unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|record| record.record_type == SUPERVISOR_DECISION_RECORD)
            .count(),
        1,
        "a conflicting logical Supervisor record must not commit"
    );
}

#[test]
fn jsonl_supervisor_logical_persistence_fails_closed() {
    let directory = tempfile::tempdir().unwrap();
    let ledger = Arc::new(JsonlRuntimeLedger::open(directory.path().join("ledger.jsonl")).unwrap());
    let root_run = RunId::from("supervisor-jsonl-unsupported");
    let root_agent = AgentId::from("coordinator");
    let executor = supervisor_host_with_ledger(ledger.clone(), &root_run, &root_agent);

    let error = executor
        .persist_unique_supervisor_record(
            SUPERVISOR_DECISION_RECORD,
            &["workflow_run_id", "workflow_id", "node_id", "attempt_id", "round"],
            decision_payload("body"),
        )
        .unwrap_err();

    assert_eq!(error.failure_class, TaskFailureClass::ReconciliationRequired);
    assert!(ledger.records_for_run(&root_run).unwrap().is_empty());
}
