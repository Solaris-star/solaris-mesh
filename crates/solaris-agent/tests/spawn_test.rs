mod common;

use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use chrono::Utc;
use common::{MockLlmProvider, test_config};
use rusqlite::Connection;
use solaris_agent::permission_engine::PermissionContext;
use solaris_agent::relationship_store::AgentRelationship;
use solaris_agent::session::{Session, SessionManager};
use solaris_agent::spawner::{
    AgentOutcomeStatus, AgentSpawnService, AgentSpawnSpec, AgentSpawner, ForkOverrides, SubAgentConfig,
};
use solaris_agent::supervisor::SupervisorCoordinator;
use solaris_providers::{LlmProvider, ProviderError};
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::{ChildAgentKey, OperationId, TaskId};
use solaris_types::llm::{LlmEvent, LlmRequest};
use solaris_types::message::{ContentBlock, Message, Role, StopReason, TokenUsage};
use solaris_types::permission::{PermissionCeiling, PermissionMode};
use solaris_types::resource::ResourceBudget;
use solaris_types::runtime::{AgentLifecycleState, TaskFailureClass, TaskRecord, TaskState};
use tempfile::tempdir;
use tokio::sync::mpsc;

// ---------------------------------------------------------------------------
// Helper: build a minimal SubAgentConfig for testing
// ---------------------------------------------------------------------------

fn make_sub_config(name: &str) -> SubAgentConfig {
    SubAgentConfig {
        name: name.to_string(),
        prompt: format!("Task for {}", name),
        max_turns: 5,
        max_tokens: 1024,
        system_prompt: None,
    }
}

fn make_spawn_spec(spawner: &AgentSpawner, operation: &str, task: &str) -> AgentSpawnSpec {
    AgentSpawnSpec {
        run_id: spawner.run_id().clone(),
        parent_agent_id: spawner.parent_agent_id().clone(),
        task_id: TaskId::from(task),
        role_key: "worker".into(),
        stable_task_key: task.into(),
        operation_id: OperationId::from(operation),
        expected_task_revision: None,
        config: make_sub_config("worker"),
        overrides: ForkOverrides::default(),
        permission_ceiling: PermissionCeiling::unrestricted(),
        resource_budget: ResourceBudget::default(),
        context_policy: Some("isolated".into()),
        recursion_limit: Some(1),
    }
}

fn max_tokens_turn(text: &str) -> Vec<LlmEvent> {
    vec![
        LlmEvent::TextDelta(text.to_owned()),
        LlmEvent::Done {
            stop_reason: StopReason::MaxTokens,
            usage: TokenUsage::default(),
        },
    ]
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Single sub-agent executes and returns the expected text result.
#[tokio::test]
async fn test_spawn_single_agent() {
    let provider = Arc::new(MockLlmProvider::with_text_response("Sub-agent done"));
    let spawner = AgentSpawner::new(provider, test_config(), std::env::temp_dir());

    let result = spawner.spawn_one(make_sub_config("agent-1")).await;

    assert_eq!(result.text, "Sub-agent done");
    assert!(!result.is_error, "expected no error, got: {}", result.text);
    assert_eq!(result.turns, 1);
    assert_eq!(result.name, "agent-1");
    assert_eq!(result.status, AgentOutcomeStatus::Completed);
}

#[tokio::test]
async fn spawn_one_preserves_internal_terminal_failure_status() {
    let provider = Arc::new(MockLlmProvider::with_turns(vec![
        max_tokens_turn("part 1 "),
        max_tokens_turn("part 2 "),
        max_tokens_turn("part 3 "),
        max_tokens_turn("part 4"),
    ]));
    let spawner = AgentSpawner::new(provider, test_config(), std::env::temp_dir());

    let result = spawner.spawn_one(make_sub_config("bounded-agent")).await;

    assert_eq!(result.status, AgentOutcomeStatus::Failed);
    assert!(result.is_error);
    assert_eq!(result.text, "part 1 part 2 part 3 part 4");
}

#[tokio::test]
async fn spawn_fork_preserves_internal_terminal_failure_status() {
    let provider = Arc::new(MockLlmProvider::with_turns(vec![
        max_tokens_turn("part 1 "),
        max_tokens_turn("part 2 "),
        max_tokens_turn("part 3 "),
        max_tokens_turn("part 4"),
    ]));
    let spawner = AgentSpawner::new(provider, test_config(), std::env::temp_dir());

    let result = spawner
        .spawn_fork_with_operation(
            make_sub_config("bounded-fork"),
            ForkOverrides::default(),
            OperationId::from("bounded-fork"),
            PermissionCeiling::unrestricted(),
        )
        .await;

    assert_eq!(result.status, AgentOutcomeStatus::Failed);
    assert!(result.is_error);
    assert_eq!(result.text, "part 1 part 2 part 3 part 4");
}

#[tokio::test]
async fn spawn_fork_preserves_completed_status() {
    let provider = Arc::new(MockLlmProvider::with_text_response("fork done"));
    let spawner = AgentSpawner::new(provider, test_config(), std::env::temp_dir());

    let result = spawner
        .spawn_fork_with_operation(
            make_sub_config("completed-fork"),
            ForkOverrides::default(),
            OperationId::from("completed-fork"),
            PermissionCeiling::unrestricted(),
        )
        .await;

    assert_eq!(result.status, AgentOutcomeStatus::Completed);
    assert!(!result.is_error);
    assert_eq!(result.text, "fork done");
}

/// Parallel sub-agents all complete successfully and return distinct results.
#[tokio::test]
async fn test_spawn_parallel_agents() {
    // Provide one turn sequence per sub-agent; each stream() call pops one entry.
    let make_turn = |text: &str| {
        vec![
            LlmEvent::TextDelta(text.to_string()),
            LlmEvent::Done {
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage {
                    input_tokens: 10,
                    output_tokens: 5,
                    cache_creation_tokens: 0,
                    cache_read_tokens: 0,
                },
            },
        ]
    };

    let provider = Arc::new(MockLlmProvider::with_turns(vec![
        make_turn("result-A"),
        make_turn("result-B"),
        make_turn("result-C"),
    ]));

    let spawner = AgentSpawner::new(provider, test_config(), std::env::temp_dir());

    let sub_configs = vec![
        make_sub_config("agent-A"),
        make_sub_config("agent-B"),
        make_sub_config("agent-C"),
    ];

    let results = spawner.spawn_parallel(sub_configs).await;

    assert_eq!(results.len(), 3, "expected 3 results from 3 sub-agents");

    for result in &results {
        assert!(
            !result.is_error,
            "sub-agent '{}' returned an error: {}",
            result.name, result.text
        );
    }

    // Each result should contain one of the expected texts (order may vary due
    // to concurrent scheduling, so we just verify the full set is covered).
    let texts: std::collections::HashSet<&str> = results.iter().map(|r| r.text.as_str()).collect();
    assert!(texts.contains("result-A"), "missing result-A");
    assert!(texts.contains("result-B"), "missing result-B");
    assert!(texts.contains("result-C"), "missing result-C");
}

/// The same provider Arc is reused across sequentially spawned sub-agents.
#[tokio::test]
async fn test_spawn_shares_provider() {
    // Two turns: one for each sequential sub-agent call.
    let provider = Arc::new(MockLlmProvider::with_turns(vec![
        vec![
            LlmEvent::TextDelta("first".to_string()),
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
            LlmEvent::TextDelta("second".to_string()),
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

    // Both sub-agents share the same underlying provider via Arc.
    let provider_dyn: Arc<dyn solaris_providers::LlmProvider> = provider;
    let spawner = AgentSpawner::new(Arc::clone(&provider_dyn), test_config(), std::env::temp_dir());

    let result1 = spawner.spawn_one(make_sub_config("seq-1")).await;
    let result2 = spawner.spawn_one(make_sub_config("seq-2")).await;

    assert!(!result1.is_error, "seq-1 errored: {}", result1.text);
    assert!(!result2.is_error, "seq-2 errored: {}", result2.text);
    assert_eq!(result1.text, "first");
    assert_eq!(result2.text, "second");
}

/// An LLM error event causes the sub-agent result to be marked as an error.
#[tokio::test]
async fn test_spawn_agent_error_captured() {
    // Emit an Error event — the engine converts this to AgentError::ApiError,
    // which spawner catches and stores in SubAgentResult::is_error.
    let provider = Arc::new(MockLlmProvider::with_events(vec![LlmEvent::Error(
        "provider failed".to_string(),
    )]));

    let spawner = AgentSpawner::new(provider, test_config(), std::env::temp_dir());

    let result = spawner.spawn_one(make_sub_config("error-agent")).await;

    assert!(result.is_error, "expected is_error=true");
    assert!(
        result.text.to_lowercase().contains("error"),
        "expected error message to contain 'error', got: {}",
        result.text
    );
}

#[tokio::test]
async fn child_spawn_resumes_and_persists_stable_session() {
    let directory = tempdir().expect("tempdir");
    let provider = Arc::new(MockLlmProvider::with_text_response("continued response"));
    let mut config = test_config();
    config.session.enabled = true;
    config.session.directory = directory.path().to_string_lossy().into_owned();
    let spawner = AgentSpawner::new(provider, config.clone(), std::env::temp_dir());
    let operation_id = OperationId::from("persistent-operation");
    let resource_manager = spawner.resource_manager();
    drop(
        resource_manager
            .try_acquire_agent(1)
            .expect("original process counted the child"),
    );
    let runtime = spawner.lifecycle_runtime();
    let reservation = runtime
        .reserve_spawn(
            spawner.run_id().clone(),
            spawner.parent_agent_id().clone(),
            operation_id.clone(),
            &PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted()),
            PermissionCeiling::unrestricted(),
        )
        .expect("reserve interrupted child");
    runtime.commit_spawn(&reservation).expect("commit interrupted child");
    let key = ChildAgentKey {
        run_id: spawner.run_id().clone(),
        parent_agent_id: spawner.parent_agent_id().clone(),
        role_key: operation_id.as_str().to_owned(),
        stable_task_key: operation_id.as_str().to_owned(),
        spawn_operation_id: operation_id.clone(),
    };
    let session_id = key.session_id();
    let now = Utc::now();
    let mut interrupted = Session {
        id: session_id.clone(),
        run_id: Some(spawner.run_id().to_string()),
        created_at: now,
        updated_at: now,
        provider: config.provider_label.clone(),
        model: config.model.clone(),
        cwd: std::env::temp_dir().to_string_lossy().into_owned(),
        total_usage: TokenUsage::default(),
        messages: Vec::new(),
        runtime_state: None,
    };
    interrupted.messages.push(Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: "before crash".into(),
        }],
    ));
    fs::write(
        directory.path().join("interrupted-child.json"),
        serde_json::to_vec_pretty(&interrupted).unwrap(),
    )
    .unwrap();
    let manager = SessionManager::new(directory.path().to_path_buf(), config.session.max_sessions);

    let result = spawner
        .spawn_one_with_operation(make_sub_config("persistent-agent"), operation_id)
        .await;

    assert!(!result.is_error, "spawn failed: {}", result.text);
    let resumed = manager.load(&session_id).expect("load resumed session");
    assert_eq!(resumed.run_id.as_deref(), Some(spawner.run_id().as_str()));
    let connection = Connection::open(directory.path().join("session.sqlite3")).unwrap();
    let run_reference_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM session_run_references
             WHERE session_id = ?1 AND run_id = ?2 AND reference_kind = 'session'",
            rusqlite::params![&session_id, spawner.run_id().as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(run_reference_count, 1);
    assert!(resumed.messages.iter().any(|message| {
        message
            .content
            .iter()
            .any(|block| matches!(block, ContentBlock::Text { text } if text == "before crash"))
    }));
    assert!(resumed.messages.iter().any(|message| {
        message
            .content
            .iter()
            .any(|block| matches!(block, ContentBlock::Text { text } if text == "continued response"))
    }));
    assert_eq!(resource_manager.usage().total_descendants, 1);
    assert_eq!(resource_manager.usage().active_agents, 0);
    assert_eq!(
        runtime
            .projection()
            .agents
            .iter()
            .filter(|agent| agent.parent_agent_id.as_ref() == Some(spawner.parent_agent_id()))
            .count(),
        1
    );
}

#[tokio::test]
async fn new_child_session_stores_run_identity_and_reference() {
    let directory = tempdir().expect("tempdir");
    let provider = Arc::new(MockLlmProvider::with_text_response("child complete"));
    let mut config = test_config();
    config.session.enabled = true;
    config.session.directory = directory.path().to_string_lossy().into_owned();
    let spawner = AgentSpawner::new(provider, config, std::env::temp_dir());

    let result = spawner
        .spawn_one_with_operation(make_sub_config("new-child"), OperationId::from("new-child-session"))
        .await;

    assert!(!result.is_error, "spawn failed: {}", result.text);
    let connection = Connection::open(directory.path().join("session.sqlite3")).unwrap();
    let (session_count, missing_run_count, reference_count, active_lease_count): (i64, i64, i64, i64) = connection
        .query_row(
            "SELECT
                COUNT(*),
                SUM(CASE WHEN sessions.run_id IS NULL OR sessions.run_id = '' THEN 1 ELSE 0 END),
                SUM(CASE WHEN session_run_references.run_id = sessions.run_id
                          AND session_run_references.reference_kind = 'session'
                         THEN 1 ELSE 0 END),
                SUM(CASE WHEN session_leases.owner_id IS NOT NULL THEN 1 ELSE 0 END)
             FROM sessions
             LEFT JOIN session_run_references
               ON session_run_references.session_id = sessions.session_id
             LEFT JOIN session_leases
               ON session_leases.session_id = sessions.session_id",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(session_count, 1);
    assert_eq!(missing_run_count, 0);
    assert_eq!(reference_count, 1);
    assert_eq!(active_lease_count, 0);
}

#[tokio::test]
async fn legacy_child_identity_reuses_v1_relationship_and_session() {
    let directory = tempdir().expect("tempdir");
    let calls = Arc::new(AtomicUsize::new(0));
    let provider = Arc::new(CountingProvider {
        calls: Arc::clone(&calls),
    });
    let mut config = test_config();
    config.session.enabled = true;
    config.session.directory = directory.path().to_string_lossy().into_owned();
    let spawner = AgentSpawner::new(provider, config.clone(), std::env::temp_dir());
    let spec = make_spawn_spec(&spawner, "legacy-operation", "legacy-task");
    let key = ChildAgentKey {
        run_id: spec.run_id.clone(),
        parent_agent_id: spec.parent_agent_id.clone(),
        role_key: spec.role_key.clone(),
        stable_task_key: spec.stable_task_key.clone(),
        spawn_operation_id: spec.operation_id.clone(),
    };
    let legacy_agent_id = key.legacy_agent_id();
    let relationship = AgentRelationship {
        run_id: spec.run_id.clone(),
        parent_agent_id: spec.parent_agent_id.clone(),
        child_agent_id: legacy_agent_id.clone(),
        child_identity_version: 0,
        role_key: spec.role_key.clone(),
        stable_task_key: spec.stable_task_key.clone(),
        spawn_operation_id: spec.operation_id.clone(),
    };
    let runtime = spawner.lifecycle_runtime();
    runtime
        .ledger()
        .append(
            &spec.run_id,
            DurabilityClass::SyncCritical,
            "agent_spawn_committed",
            serde_json::to_value(&relationship).unwrap(),
        )
        .unwrap();
    runtime.restore_projection(&spec.run_id).unwrap();

    let legacy_session_id = key.legacy_session_id();
    let now = Utc::now();
    let mut session = Session {
        id: legacy_session_id.clone(),
        run_id: Some(spec.run_id.to_string()),
        created_at: now,
        updated_at: now,
        provider: config.provider_label.clone(),
        model: config.model.clone(),
        cwd: std::env::temp_dir().to_string_lossy().into_owned(),
        total_usage: TokenUsage::default(),
        messages: Vec::new(),
        runtime_state: None,
    };
    session.messages.push(Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: "legacy context".into(),
        }],
    ));
    fs::write(
        directory.path().join("interrupted-legacy-child.json"),
        serde_json::to_vec_pretty(&session).unwrap(),
    )
    .unwrap();
    let manager = SessionManager::new(directory.path().to_path_buf(), config.session.max_sessions);

    let handle = spawner.spawn(spec).await.unwrap();
    assert_eq!(handle.agent_id, legacy_agent_id);
    assert_eq!(handle.identity_version, ChildAgentKey::LEGACY_IDENTITY_VERSION);
    let outcome = spawner.join(&handle).await.unwrap();
    assert_eq!(outcome.status, AgentOutcomeStatus::Completed);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(
        manager
            .load(&legacy_session_id)
            .unwrap()
            .messages
            .iter()
            .any(|message| {
                message
                    .content
                    .iter()
                    .any(|block| matches!(block, ContentBlock::Text { text } if text == "legacy context"))
            })
    );
    assert_eq!(
        runtime
            .projection()
            .agents
            .iter()
            .filter(|agent| agent.parent_agent_id.as_ref() == Some(spawner.parent_agent_id()))
            .count(),
        1
    );
    let handle_records = runtime
        .ledger()
        .records_for_run(spawner.run_id())
        .unwrap()
        .into_iter()
        .filter(|record| record.record_type == "agent_handle_issued")
        .collect::<Vec<_>>();
    assert_eq!(handle_records.len(), 1);
    assert_eq!(handle_records[0].payload["agent_id"], legacy_agent_id.as_str());
}

struct SlowProvider;

#[async_trait]
impl LlmProvider for SlowProvider {
    async fn stream(&self, _request: &LlmRequest) -> Result<mpsc::Receiver<LlmEvent>, ProviderError> {
        let (tx, rx) = mpsc::channel(1);
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            drop(tx);
        });
        Ok(rx)
    }
}

struct CountingProvider {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl LlmProvider for CountingProvider {
    async fn stream(&self, _request: &LlmRequest) -> Result<mpsc::Receiver<LlmEvent>, ProviderError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = mpsc::channel(2);
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            let _ = tx.send(LlmEvent::TextDelta("once".into())).await;
            let _ = tx
                .send(LlmEvent::Done {
                    stop_reason: StopReason::EndTurn,
                    usage: TokenUsage::default(),
                })
                .await;
        });
        Ok(rx)
    }
}

#[tokio::test]
async fn concurrent_same_operation_executes_provider_once() {
    let calls = Arc::new(AtomicUsize::new(0));
    let spawner = Arc::new(AgentSpawner::new(
        Arc::new(CountingProvider {
            calls: Arc::clone(&calls),
        }),
        test_config(),
        std::env::temp_dir(),
    ));
    let left = {
        let spawner = Arc::clone(&spawner);
        tokio::spawn(async move {
            spawner
                .spawn_one_with_operation(make_sub_config("same-operation"), OperationId::from("same-operation"))
                .await
        })
    };
    let right = {
        let spawner = Arc::clone(&spawner);
        tokio::spawn(async move {
            spawner
                .spawn_one_with_operation(make_sub_config("same-operation"), OperationId::from("same-operation"))
                .await
        })
    };

    let (left, right) = tokio::join!(left, right);
    let left = left.expect("left task");
    let right = right.expect("right task");
    assert_eq!(left.text, "once");
    assert_eq!(right.text, "once");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn completed_spawn_reuses_durable_outcome_without_new_provider_or_descendant() {
    let calls = Arc::new(AtomicUsize::new(0));
    let spawner = AgentSpawner::new(
        Arc::new(CountingProvider {
            calls: Arc::clone(&calls),
        }),
        test_config(),
        std::env::temp_dir(),
    );
    let operation_id = OperationId::from("completed-operation");

    let first = spawner
        .spawn_one_with_operation(make_sub_config("completed-operation"), operation_id.clone())
        .await;
    let second = spawner
        .spawn_one_with_operation(make_sub_config("completed-operation"), operation_id)
        .await;

    assert_eq!(first.name, second.name);
    assert_eq!(first.text, second.text);
    assert_eq!(first.is_error, second.is_error);
    assert_eq!(first.turns, second.turns);
    assert_eq!(first.usage.input_tokens, second.usage.input_tokens);
    assert_eq!(first.usage.output_tokens, second.usage.output_tokens);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(spawner.resource_manager().usage().total_descendants, 1);
}

#[tokio::test]
async fn typed_handle_rejects_changed_spec_and_assigns_the_real_task() {
    let spawner = AgentSpawner::new(
        Arc::new(MockLlmProvider::with_text_response("done")),
        test_config(),
        std::env::temp_dir(),
    );
    let runtime = spawner.lifecycle_runtime();
    let spec = make_spawn_spec(&spawner, "typed-operation", "workflow:run:node");
    runtime
        .register_runtime_task(
            &spec.run_id,
            TaskRecord {
                run_id: spec.run_id.clone(),
                task_id: spec.task_id.clone(),
                revision: 0,
                task_key: Some(spec.stable_task_key.clone()),
                team_id: None,
                workflow_id: Some("workflow".into()),
                node_id: Some("node".into()),
                role: Some("worker".into()),
                depends_on: Vec::new(),
                content: None,
                expected_write_scope: Vec::new(),
                owner_agent_id: None,
                state: TaskState::Queued,
                outcome_ref: None,
                failure_class: None,
            },
        )
        .unwrap();

    let handle = spawner.spawn(spec.clone()).await.expect("spawn typed handle");
    assert_eq!(
        runtime.tasks().get(&spec.task_id).unwrap().owner_agent_id,
        Some(handle.agent_id)
    );
    let mut changed = spec;
    changed.config.prompt = "different prompt".into();
    let error = spawner.spawn(changed).await.expect_err("changed spec must fail closed");
    assert!(error.message.contains("different AgentSpawnSpec"));
}

#[tokio::test]
async fn typed_handle_cancel_prevents_join_execution() {
    let calls = Arc::new(AtomicUsize::new(0));
    let spawner = AgentSpawner::new(
        Arc::new(CountingProvider {
            calls: Arc::clone(&calls),
        }),
        test_config(),
        std::env::temp_dir(),
    );
    let runtime = spawner.lifecycle_runtime();
    let run_id = spawner.run_id().clone();
    let parent_agent_id = spawner.parent_agent_id().clone();
    let handle = spawner
        .spawn(make_spawn_spec(&spawner, "cancel-handle", "task-cancel"))
        .await
        .expect("spawn handle");
    spawner.cancel(&handle).await.expect("cancel handle");
    let outcome = spawner.join(&handle).await.expect("join cancelled handle");

    assert_eq!(outcome.status, AgentOutcomeStatus::Cancelled);
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    let restored = AgentSpawner::new(
        Arc::new(CountingProvider {
            calls: Arc::clone(&calls),
        }),
        test_config(),
        std::env::temp_dir(),
    )
    .with_runtime_context(runtime, run_id, parent_agent_id);
    let restored_outcome = restored
        .join(&handle)
        .await
        .expect("join cancelled handle after restart");
    assert_eq!(restored_outcome.status, AgentOutcomeStatus::Cancelled);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn typed_role_wall_time_budget_terminates_the_child() {
    let spawner = AgentSpawner::new(Arc::new(SlowProvider), test_config(), std::env::temp_dir());
    let mut spec = make_spawn_spec(&spawner, "role-timeout", "task-role-timeout");
    spec.resource_budget.max_wall_time_ms = Some(5);
    let handle = spawner.spawn(spec).await.expect("spawn handle");

    let outcome = spawner.join(&handle).await.expect("join handle");

    assert_eq!(outcome.status, AgentOutcomeStatus::Failed);
    assert!(outcome.output.to_string().contains("wall-time budget"));
}

#[tokio::test]
async fn cancelling_spawn_marks_committed_child_cancelled() {
    let spawner = Arc::new(AgentSpawner::new(
        Arc::new(SlowProvider),
        test_config(),
        std::env::temp_dir(),
    ));
    let runtime = spawner.lifecycle_runtime();
    let run_id = spawner.run_id().clone();
    let task_spawner = Arc::clone(&spawner);
    let task = tokio::spawn(async move {
        task_spawner
            .spawn_one_with_operation(make_sub_config("cancelled"), OperationId::from("cancelled-operation"))
            .await
    });

    for _ in 0..100 {
        if runtime
            .projection()
            .agents
            .iter()
            .any(|agent| agent.state == AgentLifecycleState::Active)
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    task.abort();
    let _ = task.await;
    tokio::task::yield_now().await;

    let child = runtime
        .projection()
        .agents
        .into_iter()
        .find(|agent| agent.parent_agent_id.as_ref() == Some(spawner.parent_agent_id()))
        .expect("child agent");
    assert_eq!(child.state, AgentLifecycleState::Cancelled);
    let records = runtime.ledger().records_for_run(&run_id).unwrap();
    assert!(records.iter().any(|record| record.record_type == "agent_outcome"));
    assert!(
        records
            .iter()
            .any(|record| record.record_type == "agent_spawn_cancelled")
    );
}

#[tokio::test]
async fn supervisor_does_not_retry_unknown_worker_outcome() {
    let provider = Arc::new(MockLlmProvider::with_turns(vec![
        vec![LlmEvent::Error("retry".into())],
        vec![
            LlmEvent::TextDelta("done".into()),
            LlmEvent::Done {
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::default(),
            },
        ],
    ]));
    let spawner = Arc::new(AgentSpawner::new(provider, test_config(), std::env::temp_dir()));
    let runtime = spawner.lifecycle_runtime();
    let run_id = spawner.run_id().clone();
    let spec = AgentSpawnSpec {
        run_id: run_id.clone(),
        parent_agent_id: spawner.parent_agent_id().clone(),
        task_id: TaskId::from("task-supervised"),
        role_key: "worker".into(),
        stable_task_key: "task-supervised".into(),
        operation_id: OperationId::from("supervised"),
        expected_task_revision: None,
        config: make_sub_config("worker"),
        overrides: ForkOverrides::default(),
        permission_ceiling: PermissionCeiling::unrestricted(),
        resource_budget: ResourceBudget::default(),
        context_policy: Some("isolated".into()),
        recursion_limit: Some(1),
    };
    let result = SupervisorCoordinator::new(Arc::clone(&spawner)).execute(spec, 2).await;

    assert!(result.is_error);
    assert_eq!(result.status, AgentOutcomeStatus::OutcomeUnknown);
    assert_eq!(result.failure_class, Some(TaskFailureClass::OutcomeUnknown));
    let records = runtime.ledger().records_for_run(&run_id).unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|record| record.record_type == "supervisor_assignment")
            .count(),
        1
    );
    assert!(
        records
            .iter()
            .any(|record| record.record_type == "supervisor_attempt_failed")
    );
    assert!(!records.iter().any(|record| record.record_type == "supervisor_settled"));
    assert_eq!(
        records
            .iter()
            .filter(|record| record.record_type == "agent_spawn_intent")
            .count(),
        1,
        "Supervisor retries must preserve one logical child identity",
    );
}

#[tokio::test]
async fn plan_mode_allows_scoped_agent_lifecycle_through_effect_pipeline() {
    let spawner = AgentSpawner::new(
        Arc::new(MockLlmProvider::with_text_response("planned")),
        test_config(),
        std::env::temp_dir(),
    )
    .with_permission_context(PermissionContext::new(PermissionMode::Plan, PermissionCeiling::plan()));
    let runtime = spawner.lifecycle_runtime();
    let run_id = spawner.run_id().clone();

    let result = spawner
        .spawn_one_with_operation(make_sub_config("planner"), OperationId::from("plan-spawn"))
        .await;

    assert!(!result.is_error, "{}", result.text);
    let records = runtime.ledger().records_for_run(&run_id).unwrap();
    assert!(records.iter().any(|record| record.record_type == "effect_intent"));
    assert!(records.iter().any(|record| record.record_type == "effect_outcome"));
}
