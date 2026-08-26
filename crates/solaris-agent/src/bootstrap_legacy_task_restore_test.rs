use solaris_types::identity::{AgentId, RunId, TaskId};
use solaris_types::runtime::{TaskRecord, TaskState};

use crate::runtime_ledger::SqliteRuntimeLedger;

#[tokio::test]
async fn bootstrap_does_not_revive_a_settled_legacy_task_after_handoff_restore() {
    let workspace = tempfile::tempdir().unwrap();
    let run_id = RunId::from("legacy-bootstrap-task-run");
    let task_id = TaskId::from("legacy-bootstrap-task");
    let runtime_directory = workspace.path().join(".solaris").join("runtime");
    std::fs::create_dir_all(&runtime_directory).unwrap();
    let ledger = SqliteRuntimeLedger::open(runtime_directory.join("ledger.sqlite3")).unwrap();
    ledger
        .append(
            &run_id,
            DurabilityClass::SyncCritical,
            "task_created",
            serde_json::to_value(TaskRecord {
                run_id: run_id.clone(),
                task_id: task_id.clone(),
                revision: 0,
                task_key: Some("legacy-bootstrap-task".to_owned()),
                team_id: None,
                workflow_id: None,
                node_id: None,
                role: Some("worker".to_owned()),
                depends_on: Vec::new(),
                content: None,
                expected_write_scope: Vec::new(),
                owner_agent_id: None,
                state: TaskState::Queued,
                outcome_ref: None,
                failure_class: None,
            })
            .unwrap(),
        )
        .unwrap();
    ledger
        .append(
            &run_id,
            DurabilityClass::SyncCritical,
            "task_assigned",
            serde_json::json!({
                "task_id": task_id,
                "agent_id": "legacy-owner-a",
            }),
        )
        .unwrap();
    ledger
        .append(
            &run_id,
            DurabilityClass::SyncCritical,
            "task_handoff",
            serde_json::json!({
                "task_id": task_id,
                "from": "legacy-owner-a",
                "to": "legacy-owner-b",
            }),
        )
        .unwrap();
    ledger
        .append(
            &run_id,
            DurabilityClass::SyncCritical,
            "task_settled",
            serde_json::json!({
                "task_id": task_id,
                "state": TaskState::Completed,
            }),
        )
        .unwrap();
    drop(ledger);

    let mut config = Config::resolve(&CliArgs {
        provider: Some("anthropic".into()),
        api_key: Some("test".into()),
        base_url: Some("https://provider.example.test".into()),
        model: Some("test-model".into()),
        max_tokens: None,
        thinking: None,
        thinking_budget: None,
        max_turns: None,
        max_tool_call_malformed_turns: None,
        max_tool_call_failure_turns: None,
        system_prompt: None,
        profile: None,
        auto_approve: false,
        project_dir: Some(workspace.path().to_path_buf()),
    })
    .unwrap();
    config.session.directory = workspace
        .path()
        .join(".solaris")
        .join("sessions")
        .to_string_lossy()
        .into_owned();
    let mut bootstrap = AgentBootstrap::new(config, workspace.path().to_string_lossy(), Arc::new(NullSink))
        .permission_mode(PermissionMode::Plan)
        .provider(Arc::new(FailingProvider));
    bootstrap.run_id = run_id.clone();
    bootstrap.root_agent_id = root_agent_id_for_run(&run_id);

    let result = bootstrap.build().await.unwrap();
    let task = result.collaboration_runtime.tasks().get(&task_id).unwrap();
    assert_eq!(task.state, TaskState::Completed);
    assert_eq!(task.owner_agent_id, Some(AgentId::from("legacy-owner-b")));
}
