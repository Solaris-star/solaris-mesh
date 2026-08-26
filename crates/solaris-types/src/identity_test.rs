use super::*;

#[test]
fn child_agent_key_is_stable_and_unambiguous() {
    let key = ChildAgentKey {
        run_id: RunId::from("run-1"),
        parent_agent_id: AgentId::from("agent-2"),
        role_key: "researcher".into(),
        stable_task_key: "task-3".into(),
        spawn_operation_id: OperationId::from("op-3"),
    };

    assert_eq!(key.stable_string(), "v2:5:run-1:7:agent-2:10:researcher:6:task-3");
}

#[test]
fn child_agent_session_id_is_stable_and_path_safe() {
    let key = ChildAgentKey {
        run_id: RunId::from("run:1"),
        parent_agent_id: AgentId::from("agent/2"),
        role_key: "role".into(),
        stable_task_key: "task".into(),
        spawn_operation_id: OperationId::from("op\\3"),
    };

    let first = key.session_id();
    let second = key.session_id();

    assert_eq!(first, second);
    assert!(first.starts_with("mesh-child-"));
    assert!(first.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '-'));
    assert_eq!(key.identity_version_for_agent_id(&key.agent_id()), Some(2));
    assert_eq!(key.identity_version_for_agent_id(&key.legacy_agent_id()), Some(1));
    assert_eq!(
        key.session_id_for_agent_id(&key.legacy_agent_id()),
        Some(key.legacy_session_id())
    );
}

#[test]
fn different_child_agent_keys_have_different_session_ids() {
    let first = ChildAgentKey {
        run_id: RunId::from("run-1"),
        parent_agent_id: AgentId::from("agent-2"),
        role_key: "role".into(),
        stable_task_key: "task-3".into(),
        spawn_operation_id: OperationId::from("op-3"),
    };
    let second = ChildAgentKey {
        stable_task_key: "task-4".into(),
        ..first.clone()
    };

    assert_ne!(first.session_id(), second.session_id());
}

#[test]
fn delimiter_placement_cannot_collide_child_identity() {
    let first = ChildAgentKey {
        run_id: RunId::from("run-1"),
        parent_agent_id: AgentId::from("agent-2"),
        role_key: "a:b".into(),
        stable_task_key: "c".into(),
        spawn_operation_id: OperationId::from("op-1"),
    };
    let second = ChildAgentKey {
        role_key: "a".into(),
        stable_task_key: "b:c".into(),
        spawn_operation_id: OperationId::from("op-2"),
        ..first.clone()
    };

    assert_ne!(first.stable_string(), second.stable_string());
    assert_ne!(first.stable_digest(), second.stable_digest());
    assert_ne!(first.agent_id(), second.agent_id());
    assert_ne!(first.session_id(), second.session_id());
}

#[test]
fn retry_operation_id_does_not_change_logical_child_identity() {
    let first = ChildAgentKey {
        run_id: RunId::from("run-1"),
        parent_agent_id: AgentId::from("agent-2"),
        role_key: "role".into(),
        stable_task_key: "task".into(),
        spawn_operation_id: OperationId::from("attempt-1"),
    };
    let second = ChildAgentKey {
        spawn_operation_id: OperationId::from("attempt-2"),
        ..first.clone()
    };

    assert_eq!(first, second);
    assert_eq!(first.agent_id(), second.agent_id());
    assert_eq!(first.session_id(), second.session_id());
}
