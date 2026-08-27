use super::*;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use solaris_types::identity::{AgentId, TeamId};

use crate::runtime_ledger::RuntimeLedger;

#[test]
fn child_resource_budget_defaults_keep_spawn_limits_finite() {
    let budget = resource_budget_from_env(&[]);

    assert_eq!(budget.max_spawn_depth, Some(8));
    assert_eq!(budget.max_total_descendants_per_run, Some(256));
}

#[test]
fn child_resource_budget_preserves_explicit_spawn_limits() {
    let budget = resource_budget_from_env(&[
        ("SOLARIS_MAX_SPAWN_DEPTH".to_owned(), "3".to_owned()),
        ("SOLARIS_MAX_TOTAL_DESCENDANTS".to_owned(), "17".to_owned()),
    ]);

    assert_eq!(budget.max_spawn_depth, Some(3));
    assert_eq!(budget.max_total_descendants_per_run, Some(17));
}

#[test]
fn child_resource_budget_environment_agent_limit_is_effectively_clamped() {
    for (configured, expected) in [("0", 1), ("65", 64), ("500", 64)] {
        let budget = resource_budget_from_env(&[("SOLARIS_MAX_ACTIVE_AGENTS".to_owned(), configured.to_owned())]);
        let manager = crate::resource_manager::ResourceManager::new(budget);

        assert_eq!(manager.effective_agent_limit(), expected);
    }
}

#[cfg(test)]
mod phase7_tests {
    use super::{ForkOverrides, SubAgentConfig, build_tool_registry};
    use solaris_types::permission::{ExecutionBoundary, PermissionCeiling, PermissionMode};

    use crate::permission_engine::PermissionContext;

    fn permissions() -> PermissionContext {
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted())
    }

    #[test]
    fn tc_7_1_fork_overrides_default_values() {
        let o = ForkOverrides::default();
        assert!(o.model.is_none());
        assert!(o.effort.is_none());
        assert!(o.allowed_tools.is_empty());
        assert!(!o.inherit_capabilities);
    }

    #[test]
    fn tc_7_40_build_tool_registry_empty_allowed_registers_none() {
        let registry = build_tool_registry(&[], false, &std::env::temp_dir(), &[], &permissions());
        for name in &["Read", "Write", "Edit", "ExecCommand", "Grep", "Glob"] {
            assert!(registry.get(name).is_none(), "tool '{name}' must not be registered");
        }
    }

    #[test]
    fn explicit_inheritance_registers_parent_tools() {
        let registry = build_tool_registry(&[], true, &std::env::temp_dir(), &[], &permissions());
        for name in &["Read", "Write", "Edit", "ExecCommand", "Grep", "Glob"] {
            assert!(registry.get(name).is_some(), "tool '{name}' should be inherited");
        }
    }

    #[test]
    fn tc_7_43_build_tool_registry_filters_to_allowed() {
        let allowed = vec!["ExecCommand".to_string(), "Read".to_string()];
        let registry = build_tool_registry(&allowed, false, &std::env::temp_dir(), &[], &permissions());
        assert!(registry.get("ExecCommand").is_some());
        assert!(registry.get("Read").is_some());
        assert!(registry.get("Write").is_none());
    }

    #[tokio::test]
    async fn child_search_tools_observe_shared_runtime_path_policy_and_mode_changes() {
        let workspace = tempfile::tempdir().unwrap();
        let runtime = workspace.path().join(".solaris").join("runtime");
        std::fs::create_dir_all(&runtime).unwrap();
        let secret = runtime.join("secret.txt");
        let hard_link = workspace.path().join("runtime-hard-link.txt");
        std::fs::write(&secret, "runtime-secret-marker").unwrap();
        std::fs::hard_link(&secret, &hard_link).unwrap();
        std::fs::write(workspace.path().join("public.txt"), "public-marker").unwrap();
        let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
        permissions.set_boundary(ExecutionBoundary::workspace(workspace.path().to_string_lossy()));
        permissions.register_protected_paths(&runtime, vec![secret]).unwrap();
        let registry = build_tool_registry(&[], true, workspace.path(), &[], &permissions);
        let glob = registry.get("Glob").unwrap();
        let read = registry.get("Read").unwrap();

        let auto = glob
            .execute(serde_json::json!({"pattern": "**/*.txt", "path": "."}))
            .await;
        assert!(auto.content.contains("public.txt"));
        assert!(!auto.content.contains("secret.txt"));
        assert!(read.execute(serde_json::json!({"file_path": hard_link})).await.is_error);

        permissions.set_mode(PermissionMode::Bypass);
        let bypass = glob
            .execute(serde_json::json!({"pattern": "**/*.txt", "path": "."}))
            .await;
        assert!(bypass.content.contains("secret.txt"));
        assert!(!read.execute(serde_json::json!({"file_path": hard_link})).await.is_error);
    }

    #[test]
    fn tc_7_sub_agent_config_original_fields_intact() {
        let config = SubAgentConfig {
            name: "test-agent".to_string(),
            prompt: "do the task".to_string(),
            max_turns: 5,
            max_tokens: 1024,
            system_prompt: Some("you are helpful".to_string()),
        };
        assert_eq!(config.name, "test-agent");
        assert_eq!(config.max_turns, 5);
    }
}

struct NeverCalledProvider;

#[async_trait::async_trait]
impl solaris_providers::LlmProvider for NeverCalledProvider {
    async fn stream(
        &self,
        _request: &solaris_types::llm::LlmRequest,
    ) -> Result<tokio::sync::mpsc::Receiver<solaris_types::llm::LlmEvent>, solaris_providers::ProviderError> {
        panic!("disabled multi-agent policy must reject before the provider is called")
    }
}

#[tokio::test]
async fn disabled_multi_agent_policy_rejects_legacy_spawn_before_provider_execution() {
    let config = solaris_config::config::Config::resolve(&solaris_config::config::CliArgs {
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
    let spawner = AgentSpawner::new(Arc::new(NeverCalledProvider), config, std::env::temp_dir());
    *spawner
        .multi_agent_policy_state()
        .write()
        .unwrap_or_else(|error| error.into_inner()) = solaris_types::workflow::MultiAgentPolicy::Disabled;

    let result = spawner
        .spawn_one(SubAgentConfig {
            name: "denied-child".into(),
            prompt: "must not run".into(),
            max_turns: 1,
            max_tokens: 32,
            system_prompt: None,
        })
        .await;

    assert!(result.is_error);
    assert!(result.text.contains("multi-agent policy is disabled"));
}

#[tokio::test]
async fn single_collaboration_keeps_work_in_parent_without_registering_child() {
    let config = solaris_config::config::Config::resolve(&solaris_config::config::CliArgs {
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
    let spawner = AgentSpawner::new(Arc::new(NeverCalledProvider), config, std::env::temp_dir());
    let request = ParsedSpawnRequest {
        strategy: solaris_types::workflow::CollaborationSelection::Fixed(
            solaris_types::workflow::CollaborationStrategy::Single,
        ),
        tasks: vec![solaris_types::workflow::CollaborationTaskInput {
            id: Some("single-task".into()),
            name: "single-task".into(),
            prompt: "the parent should execute this".into(),
            role: None,
            depends_on: Vec::new(),
            expected_output: None,
            resource_budget: None,
        }],
    };

    let agents_before = spawner.lifecycle_runtime().agents().snapshot();
    let result = spawner.spawn_collaboration(request).await;

    assert_eq!(result.status, solaris_types::workflow::CollaborationRunStatus::Failed);
    assert_eq!(result.created, 0);
    assert_eq!(result.reattached, 0);
    assert_eq!(result.tasks.len(), 1);
    assert_eq!(result.tasks[0].status, TaskState::Skipped);
    assert!(result.tasks[0].agent_id.is_none());
    assert!(result.summary.contains("did not create a Child Agent"));
    assert_eq!(
        spawner.lifecycle_runtime().agents().snapshot().len(),
        agents_before.len()
    );
    assert!(spawner.lifecycle_runtime().tasks().snapshot().is_empty());
}

struct ReviewerProvider {
    calls: Arc<AtomicUsize>,
    responses: Arc<std::sync::Mutex<Vec<String>>>,
}

#[async_trait::async_trait]
impl solaris_providers::LlmProvider for ReviewerProvider {
    async fn stream(
        &self,
        _request: &solaris_types::llm::LlmRequest,
    ) -> Result<tokio::sync::mpsc::Receiver<solaris_types::llm::LlmEvent>, solaris_providers::ProviderError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let text = self
            .responses
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .pop()
            .unwrap_or_else(|| "fallback".to_owned());
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        sender
            .send(solaris_types::llm::LlmEvent::TextDelta(text))
            .await
            .unwrap();
        sender
            .send(solaris_types::llm::LlmEvent::Done {
                stop_reason: solaris_types::message::StopReason::EndTurn,
                usage: solaris_types::message::TokenUsage::default(),
            })
            .await
            .unwrap();
        Ok(receiver)
    }
}

#[tokio::test]
async fn independent_reviewer_adds_a_read_only_durable_task() {
    let calls = Arc::new(AtomicUsize::new(0));
    let provider = Arc::new(ReviewerProvider {
        calls: Arc::clone(&calls),
        responses: Arc::new(std::sync::Mutex::new(vec![
            "reviewed".to_owned(),
            "candidate".to_owned(),
        ])),
    });
    let mut config = solaris_config::config::Config::resolve(&solaris_config::config::CliArgs {
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
    let spawner = AgentSpawner::new(provider, config, std::env::temp_dir());
    let result = spawner
        .spawn_collaboration(ParsedSpawnRequest {
            strategy: solaris_types::workflow::CollaborationSelection::Fixed(
                solaris_types::workflow::CollaborationStrategy::IndependentReviewer,
            ),
            tasks: vec![solaris_types::workflow::CollaborationTaskInput {
                id: Some("candidate".to_owned()),
                name: "candidate".to_owned(),
                prompt: "produce a candidate".to_owned(),
                role: Some("author".to_owned()),
                depends_on: Vec::new(),
                expected_output: None,
                resource_budget: None,
            }],
        })
        .await;

    assert_eq!(
        result.status,
        solaris_types::workflow::CollaborationRunStatus::Completed
    );
    assert_eq!(result.tasks.len(), 2);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let reviewer = result
        .tasks
        .iter()
        .find(|task| task.id == INDEPENDENT_REVIEWER_TASK_ID)
        .expect("reviewer summary");
    assert_eq!(reviewer.status, TaskState::Completed);
    let reviewer_id = reviewer.agent_id.clone().expect("reviewer agent id");
    let reviewer_handle = spawner
        .lifecycle_runtime()
        .projection()
        .agents
        .into_iter()
        .find(|agent| agent.agent_id == reviewer_id)
        .expect("reviewer agent record");
    assert_eq!(reviewer_handle.parent_agent_id, Some(spawner.parent_agent_id().clone()));
    let reviewer_spawn = spawner
        .lifecycle_runtime()
        .ledger()
        .records_for_run(spawner.run_id())
        .unwrap()
        .into_iter()
        .find(|record| {
            record.record_type == "agent_handle_issued"
                && record.payload.get("agent_id").and_then(serde_json::Value::as_str) == Some(reviewer_id.as_str())
        })
        .expect("reviewer spawn handle");
    let spec = reviewer_spawn.payload.get("spec").expect("reviewer spec");
    assert_eq!(spec["overrides"]["inherit_capabilities"], false);
    assert_eq!(
        spec["overrides"]["allowed_tools"],
        serde_json::json!(["Read", "Grep", "Glob"])
    );
    assert_eq!(spec["permission_ceiling"]["workspace_mutation"], false);
    assert_eq!(spec["context_policy"], "isolated_verification");
}

#[tokio::test]
async fn collaboration_summary_reports_real_duplicate_call_rate() {
    let calls = Arc::new(AtomicUsize::new(0));
    let provider = Arc::new(ReviewerProvider {
        calls: Arc::clone(&calls),
        responses: Arc::new(std::sync::Mutex::new(vec![
            "reviewed".to_owned(),
            "candidate".to_owned(),
        ])),
    });
    let mut config = solaris_config::config::Config::resolve(&solaris_config::config::CliArgs {
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
    let resources = crate::resource_manager::ResourceManager::new(solaris_types::resource::ResourceBudget::default());
    let scope = "task:summary|env:summary";
    resources
        .record_tool_calls_once_checked(
            "summary-round-1",
            &[
                solaris_types::tool::ToolCallStat::new(
                    scope,
                    "read",
                    &serde_json::json!({"path": "/a"}),
                    solaris_types::tool::ToolResultStatus::Executed,
                ),
                solaris_types::tool::ToolCallStat::new(
                    scope,
                    "read",
                    &serde_json::json!({"path": "/a"}),
                    solaris_types::tool::ToolResultStatus::CacheHit,
                ),
            ],
        )
        .unwrap();
    let spawner = AgentSpawner::new(provider, config, std::env::temp_dir()).with_resource_manager(resources);
    let result = spawner
        .spawn_collaboration(ParsedSpawnRequest {
            strategy: solaris_types::workflow::CollaborationSelection::Fixed(
                solaris_types::workflow::CollaborationStrategy::IndependentReviewer,
            ),
            tasks: vec![solaris_types::workflow::CollaborationTaskInput {
                id: Some("candidate".to_owned()),
                name: "candidate".to_owned(),
                prompt: "produce a candidate".to_owned(),
                role: Some("author".to_owned()),
                depends_on: Vec::new(),
                expected_output: None,
                resource_budget: None,
            }],
        })
        .await;

    assert_eq!(
        result.status,
        solaris_types::workflow::CollaborationRunStatus::Completed
    );
    assert_eq!(result.tool_calls, 2);
    assert_eq!(result.duplicate_call_rate, Some(0.5));
}

struct SupervisorRetryProvider {
    attempts: std::sync::Mutex<Vec<bool>>,
}

#[async_trait::async_trait]
impl solaris_providers::LlmProvider for SupervisorRetryProvider {
    async fn stream(
        &self,
        _request: &solaris_types::llm::LlmRequest,
    ) -> Result<tokio::sync::mpsc::Receiver<solaris_types::llm::LlmEvent>, solaris_providers::ProviderError> {
        let succeeds = self
            .attempts
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .pop()
            .unwrap_or(true);
        if succeeds {
            let (sender, receiver) = tokio::sync::mpsc::channel(2);
            sender
                .send(solaris_types::llm::LlmEvent::TextDelta("retried".to_owned()))
                .await
                .unwrap();
            sender
                .send(solaris_types::llm::LlmEvent::Done {
                    stop_reason: solaris_types::message::StopReason::EndTurn,
                    usage: solaris_types::message::TokenUsage::default(),
                })
                .await
                .unwrap();
            Ok(receiver)
        } else {
            Err(solaris_providers::ProviderError::RateLimited {
                retry_after_ms: 1,
                body: None,
            })
        }
    }
}

#[tokio::test]
async fn supervisor_retries_only_retryable_direct_spawn_tasks() {
    let mut config = solaris_config::config::Config::resolve(&solaris_config::config::CliArgs {
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
    let spawner = AgentSpawner::new(
        Arc::new(SupervisorRetryProvider {
            // `pop` makes the first call fail and the second call succeed.
            attempts: std::sync::Mutex::new(vec![true, false]),
        }),
        config,
        std::env::temp_dir(),
    );
    let result = spawner
        .spawn_collaboration(ParsedSpawnRequest {
            strategy: solaris_types::workflow::CollaborationSelection::Fixed(
                solaris_types::workflow::CollaborationStrategy::Supervisor,
            ),
            tasks: vec![solaris_types::workflow::CollaborationTaskInput {
                id: Some("retryable".to_owned()),
                name: "retryable".to_owned(),
                prompt: "perform a transient operation".to_owned(),
                role: Some("worker".to_owned()),
                depends_on: Vec::new(),
                expected_output: None,
                resource_budget: None,
            }],
        })
        .await;

    assert_eq!(
        result.status,
        solaris_types::workflow::CollaborationRunStatus::Completed
    );
    assert_eq!(result.tasks.len(), 1);
    assert_eq!(result.tasks[0].status, TaskState::Completed);
    assert_eq!(result.tasks[0].retries, 1);
    let records = spawner
        .lifecycle_runtime()
        .ledger()
        .records_for_run(spawner.run_id())
        .unwrap();
    assert!(
        records
            .iter()
            .any(|record| record.record_type == "supervisor_round_started")
    );
    assert!(
        records
            .iter()
            .any(|record| record.record_type == "supervisor_round_completed")
    );
    assert!(records.iter().any(|record| record.record_type == "supervisor_retry"));
    assert_eq!(
        records
            .iter()
            .filter(|record| record.record_type == "task_created")
            .count(),
        1
    );
}

#[test]
fn supervisor_retry_intent_recovers_failed_projection_after_crash_window() {
    let mut config = solaris_config::config::Config::resolve(&solaris_config::config::CliArgs {
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
    let spawner = AgentSpawner::new(Arc::new(NeverCalledProvider), config, std::env::temp_dir());
    let ledger = Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default());
    let runtime = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::<()>::new(ResourcePolicy::new(2)),
        ledger.clone(),
    ));
    let run_id = spawner.run_id().clone();
    let parent = spawner.parent_agent_id().clone();
    let child = AgentId::from("retry-crash-child");
    let team_id = TeamId::from("retry-crash-team");
    let task_id = TaskId::new(format!("collaboration:{run_id}:worker"));
    runtime.agents().upsert(solaris_types::runtime::AgentRecord {
        run_id: run_id.clone(),
        agent_id: parent.clone(),
        team_id: None,
        parent_agent_id: None,
        state: solaris_types::runtime::AgentLifecycleState::Active,
    });
    runtime.agents().upsert(solaris_types::runtime::AgentRecord {
        run_id: run_id.clone(),
        agent_id: child.clone(),
        team_id: Some(team_id.clone()),
        parent_agent_id: Some(parent.clone()),
        state: solaris_types::runtime::AgentLifecycleState::Failed,
    });
    let collaboration = AgentCollaborationContext {
        team_id: team_id.clone(),
        strategy: solaris_types::workflow::CollaborationStrategy::Supervisor,
        coordinator_agent_id: parent.clone(),
        max_pending_messages: 8,
        max_message_bytes: 1024,
    };
    runtime
        .ensure_collaboration_team(run_id.clone(), "supervisor", &collaboration)
        .unwrap();
    runtime.join_team(&run_id, &team_id, parent.clone()).unwrap();
    runtime.join_team(&run_id, &team_id, child.clone()).unwrap();
    let failed = TaskRecord {
        run_id: run_id.clone(),
        task_id: task_id.clone(),
        revision: 0,
        task_key: Some("collaboration:worker".to_owned()),
        team_id: Some(team_id),
        workflow_id: None,
        node_id: None,
        role: Some("worker".to_owned()),
        depends_on: Vec::new(),
        content: None,
        expected_write_scope: Vec::new(),
        owner_agent_id: Some(child),
        state: TaskState::Failed,
        outcome_ref: Some("agent-outcome:attempt-0".to_owned()),
        failure_class: Some(TaskFailureClass::Retryable),
    };
    ledger
        .append(
            &run_id,
            DurabilityClass::SyncCritical,
            "task_created",
            serde_json::to_value(&failed).unwrap(),
        )
        .unwrap();
    runtime.tasks().upsert(failed);
    let spawner = spawner.with_runtime_context(runtime.clone(), run_id.clone(), parent);
    let next_operation_id = collaboration_operation_id(&run_id, "worker", 1);
    spawner
        .record_supervisor_retry(
            "worker",
            0,
            TaskFailureClass::Retryable,
            &next_operation_id,
            "transient",
        )
        .unwrap();

    let attempts = spawner
        .recover_supervisor_retry_intents(&HashMap::from([("worker".to_owned(), task_id.clone())]))
        .unwrap();
    assert_eq!(attempts.get("worker"), Some(&1));
    let recovered = runtime.tasks().get(&task_id).unwrap();
    assert_eq!(recovered.state, TaskState::Queued);
    assert!(recovered.owner_agent_id.is_none());
    assert_eq!(recovered.revision, 1);
}

#[tokio::test]
async fn stale_root_session_fence_rejects_spawn_before_provider_execution() {
    let workspace = tempfile::tempdir().unwrap();
    let sessions = workspace.path().join("sessions");
    let owner = crate::session::SessionManager::new(sessions.clone(), 20);
    owner
        .create_active_session("provider", "model", "workspace", Some("spawn-fence"), "spawn-run")
        .unwrap();
    let fence = owner.active_fence("spawn-fence").unwrap();
    let config = solaris_config::config::Config::resolve(&solaris_config::config::CliArgs {
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
        project_dir: Some(workspace.path().to_path_buf()),
    })
    .unwrap();
    let spawner = AgentSpawner::new(Arc::new(NeverCalledProvider), config, workspace.path().to_path_buf())
        .with_session_fence_state(Arc::new(RwLock::new(vec![fence])));
    let connection = rusqlite::Connection::open(sessions.join("session.sqlite3")).unwrap();
    connection
        .execute(
            "UPDATE session_leases SET heartbeat_at_ms = 0, expires_at_ms = 0
             WHERE session_id = 'spawn-fence'",
            [],
        )
        .unwrap();
    drop(connection);
    let replacement = crate::session::SessionManager::new(sessions, 20);
    replacement.load_active_session("spawn-fence").unwrap();

    let result = spawner
        .spawn_one(SubAgentConfig {
            name: "fenced-child".into(),
            prompt: "must not run".into(),
            max_turns: 1,
            max_tokens: 32,
            system_prompt: None,
        })
        .await;

    assert!(result.is_error);
    assert!(result.text.contains("session lease"), "{}", result.text);
    replacement.release_active_session().unwrap();
}
