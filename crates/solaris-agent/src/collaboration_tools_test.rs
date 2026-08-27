use super::*;
use crate::resource_policy::ResourcePolicy;
use crate::scheduler::Scheduler;

#[test]
fn create_team_task_effect_uses_the_durable_team_scoped_task_id() {
    let runtime = Arc::new(CollaborationRuntime::new(Scheduler::new(ResourcePolicy::new(1))));
    let tool = CreateTeamTaskTool::new(runtime, RunId::from("run"), AgentId::from("coordinator"), 256);
    let team_a = tool.describe_effect(&json!({"team_id": "alpha", "task_id": "review"}));
    let team_b = tool.describe_effect(&json!({"team_id": "beta", "task_id": "review"}));

    assert_eq!(
        team_a.resources.mesh_resources,
        vec!["mesh:team:alpha".to_owned(), "mesh:task:team:alpha:review".to_owned(),]
    );
    assert_eq!(
        team_b.resources.mesh_resources,
        vec!["mesh:team:beta".to_owned(), "mesh:task:team:beta:review".to_owned(),]
    );
    assert_ne!(team_a.resources, team_b.resources);
}

#[tokio::test]
async fn typed_team_task_requires_key_and_preserves_content_scope_and_dependencies() {
    let runtime = Arc::new(CollaborationRuntime::new(Scheduler::new(ResourcePolicy::new(1))));
    let run_id = RunId::from("typed-task-run");
    let coordinator = AgentId::from("coordinator");
    runtime.agents().upsert(solaris_types::runtime::AgentRecord {
        run_id: run_id.clone(),
        agent_id: coordinator.clone(),
        team_id: None,
        parent_agent_id: None,
        state: solaris_types::runtime::AgentLifecycleState::Active,
    });
    let team_id = TeamId::from("typed-task-team");
    runtime
        .create_collaboration_team(
            run_id.clone(),
            team_id.clone(),
            "typed",
            solaris_types::workflow::CollaborationStrategy::Supervisor,
            Some(coordinator.clone()),
        )
        .unwrap();
    runtime.join_team(&run_id, &team_id, coordinator.clone()).unwrap();
    let tool = CreateTeamTaskTool::new(Arc::clone(&runtime), run_id, coordinator, 256);
    let typed = json!({
        "team_id": team_id,
        "task_id": "review",
        "content": {"request": "review changes"},
        "expected_write_scope": ["src/**"],
        "depends_on": ["research"]
    });

    assert!(tool.execute(typed.clone()).await.is_error);
    let mut keyed = typed;
    keyed["task_key"] = json!("review-v1");
    let created = tool.execute(keyed).await;

    assert!(!created.is_error, "{}", created.content);
    let task_id = TaskId::from("team:typed-task-team:review");
    let task = runtime.tasks().get(&task_id).unwrap();
    assert_eq!(task.revision, 0);
    assert_eq!(task.task_key.as_deref(), Some("review-v1"));
    assert_eq!(task.content, Some(json!({"request": "review changes"})));
    assert_eq!(task.expected_write_scope, vec!["src/**"]);
    assert_eq!(task.depends_on, vec![TaskId::from("team:typed-task-team:research")]);
}

#[test]
fn handoff_schema_requires_expected_revision() {
    let runtime = Arc::new(CollaborationRuntime::new(Scheduler::new(ResourcePolicy::new(1))));
    let tool = HandoffTaskTool::new(runtime, RunId::from("run"), AgentId::from("owner"));
    let schema = tool.input_schema();
    let required = schema["required"].as_array().unwrap();

    assert!(required.contains(&json!("expected_revision")));
}

#[test]
fn message_tools_declare_idempotent_replay() {
    let runtime = Arc::new(CollaborationRuntime::new(Scheduler::new(ResourcePolicy::new(1))));
    let send = SendAgentMessageTool::new(Arc::clone(&runtime), RunId::from("run"), AgentId::from("sender"));
    let broadcast = BroadcastTeamMessageTool::new(runtime, RunId::from("run"), AgentId::from("sender"));

    assert_eq!(
        send.describe_effect(&json!({"to_agent_id": "target", "body": {}}))
            .replay_policy,
        EffectReplayPolicy::Idempotent
    );
    assert_eq!(
        broadcast
            .describe_effect(&json!({"team_id": "team", "body": {}}))
            .replay_policy,
        EffectReplayPolicy::Idempotent
    );
}

#[tokio::test]
async fn send_agent_message_tool_uses_team_pending_limit() {
    let runtime = Arc::new(CollaborationRuntime::new(Scheduler::new(ResourcePolicy::new(2))));
    let run_id = RunId::from("tool-message-limit-run");
    let sender = AgentId::from("tool-message-sender");
    let receiver = AgentId::from("tool-message-receiver");
    for agent_id in [&sender, &receiver] {
        runtime.agents().upsert(solaris_types::runtime::AgentRecord {
            run_id: run_id.clone(),
            agent_id: agent_id.clone(),
            team_id: None,
            parent_agent_id: None,
            state: solaris_types::runtime::AgentLifecycleState::Active,
        });
    }
    let team_id = TeamId::from("tool-message-limit-team");
    let collaboration = solaris_types::spawner::AgentCollaborationContext {
        team_id: team_id.clone(),
        strategy: solaris_types::workflow::CollaborationStrategy::Team,
        coordinator_agent_id: sender.clone(),
        max_pending_messages: 1,
        max_message_bytes: solaris_types::workflow::CollaborationRuntimeConfig::DEFAULT_MAX_MESSAGE_BYTES,
    };
    runtime
        .ensure_collaboration_team(run_id.clone(), "tool limits", &collaboration)
        .unwrap();
    runtime.join_team(&run_id, &team_id, sender.clone()).unwrap();
    runtime.join_team(&run_id, &team_id, receiver.clone()).unwrap();
    let tool = SendAgentMessageTool::new(Arc::clone(&runtime), run_id.clone(), sender);
    let input = json!({
        "team_id": team_id,
        "to_agent_id": receiver,
        "kind": "tool-test",
        "body": {"value": 1}
    });

    assert!(!tool.execute(input.clone()).await.is_error);
    let rejected = tool.execute(input).await;

    assert!(rejected.is_error);
    assert!(rejected.content.contains("max_pending_messages"));
    assert_eq!(
        runtime
            .ledger()
            .records_for_run(&run_id)
            .unwrap()
            .iter()
            .filter(|record| record.record_type == "message_delivered")
            .count(),
        1
    );
}

#[tokio::test]
async fn prepared_send_uses_stable_effect_id_and_rejects_tampered_replay() {
    let runtime = Arc::new(CollaborationRuntime::new(Scheduler::new(ResourcePolicy::new(2))));
    let run_id = RunId::from("prepared-send-run");
    let sender = AgentId::from("sender");
    let receiver = AgentId::from("receiver");
    for agent_id in [&sender, &receiver] {
        runtime.agents().upsert(solaris_types::runtime::AgentRecord {
            run_id: run_id.clone(),
            agent_id: agent_id.clone(),
            team_id: None,
            parent_agent_id: None,
            state: solaris_types::runtime::AgentLifecycleState::Active,
        });
    }
    let tool = SendAgentMessageTool::new(Arc::clone(&runtime), run_id.clone(), sender);
    let input = json!({"to_agent_id": receiver, "body": {"value": 1}});

    let first = tool
        .prepare_execution(input.clone(), ToolExecutionContext::new("effect-one"))
        .unwrap()
        .execute()
        .await;
    let replay = tool
        .prepare_execution(input, ToolExecutionContext::new("effect-one"))
        .unwrap()
        .execute()
        .await;
    let tampered = tool
        .prepare_execution(
            json!({"to_agent_id": receiver, "body": {"value": 2}}),
            ToolExecutionContext::new("effect-one"),
        )
        .unwrap()
        .execute()
        .await;

    assert!(!first.is_error);
    assert_eq!(first.content, replay.content);
    assert!(tampered.is_error);
    assert!(tampered.content.contains("different content"));
    assert_eq!(
        runtime
            .ledger()
            .records_for_run(&run_id)
            .unwrap()
            .iter()
            .filter(|record| record.record_type == "message_delivered")
            .count(),
        1
    );
}

#[tokio::test]
async fn prepared_broadcast_replays_after_capacity_is_full() {
    let runtime = Arc::new(CollaborationRuntime::new(Scheduler::new(ResourcePolicy::new(3))));
    let run_id = RunId::from("prepared-broadcast-run");
    let sender = AgentId::from("sender");
    let first = AgentId::from("first");
    let second = AgentId::from("second");
    for agent_id in [&sender, &first, &second] {
        runtime.agents().upsert(solaris_types::runtime::AgentRecord {
            run_id: run_id.clone(),
            agent_id: agent_id.clone(),
            team_id: None,
            parent_agent_id: None,
            state: solaris_types::runtime::AgentLifecycleState::Active,
        });
    }
    let team_id = TeamId::from("prepared-broadcast-team");
    let collaboration = solaris_types::spawner::AgentCollaborationContext {
        team_id: team_id.clone(),
        strategy: solaris_types::workflow::CollaborationStrategy::Team,
        coordinator_agent_id: sender.clone(),
        max_pending_messages: 1,
        max_message_bytes: 1_024,
    };
    runtime
        .ensure_collaboration_team(run_id.clone(), "prepared", &collaboration)
        .unwrap();
    for agent_id in [sender.clone(), first.clone(), second.clone()] {
        runtime.join_team(&run_id, &team_id, agent_id).unwrap();
    }
    let tool = BroadcastTeamMessageTool::new(Arc::clone(&runtime), run_id.clone(), sender);
    let input = json!({"team_id": team_id, "body": {"task": 1}});

    let first_result = tool
        .prepare_execution(input.clone(), ToolExecutionContext::new("broadcast-effect"))
        .unwrap()
        .execute()
        .await;
    let replay = tool
        .prepare_execution(input, ToolExecutionContext::new("broadcast-effect"))
        .unwrap()
        .execute()
        .await;

    assert!(!first_result.is_error);
    assert_eq!(first_result.content, replay.content);
    assert_eq!(runtime.messages().inbox(&first).len(), 1);
    assert_eq!(runtime.messages().inbox(&second).len(), 1);
    assert_eq!(
        runtime
            .ledger()
            .records_for_run(&run_id)
            .unwrap()
            .iter()
            .filter(|record| record.record_type == "messages_broadcast")
            .count(),
        1
    );
}

#[tokio::test]
async fn create_team_task_cannot_bypass_the_run_quota() {
    let runtime = Arc::new(CollaborationRuntime::new(Scheduler::new(ResourcePolicy::new(1))));
    let run_id = RunId::from("team-task-quota-run");
    let coordinator = AgentId::from("coordinator");
    runtime.agents().upsert(solaris_types::runtime::AgentRecord {
        run_id: run_id.clone(),
        agent_id: coordinator.clone(),
        team_id: None,
        parent_agent_id: None,
        state: solaris_types::runtime::AgentLifecycleState::Active,
    });
    let team_id = TeamId::from("team-task-quota");
    runtime
        .create_collaboration_team(
            run_id.clone(),
            team_id.clone(),
            "quota",
            solaris_types::workflow::CollaborationStrategy::Supervisor,
            Some(coordinator.clone()),
        )
        .unwrap();
    runtime.join_team(&run_id, &team_id, coordinator.clone()).unwrap();
    let tool = CreateTeamTaskTool::new(Arc::clone(&runtime), run_id, coordinator, 1);

    let first = tool.execute(json!({"team_id": team_id, "task_id": "first"})).await;
    let second = tool.execute(json!({"team_id": team_id, "task_id": "second"})).await;

    assert!(!first.is_error, "{}", first.content);
    assert!(second.is_error);
    assert!(second.content.contains("at most 1"), "{}", second.content);
    assert_eq!(runtime.tasks().snapshot().len(), 1);
}
