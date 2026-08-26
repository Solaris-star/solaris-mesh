use super::*;
use serde_json::json;

#[test]
fn workflow_node_defaults_to_inherit_collaboration() {
    let node: WorkflowNode = serde_json::from_value(serde_json::json!({
        "id": "implement"
    }))
    .unwrap();

    assert_eq!(node.collaboration, CollaborationSelection::Inherit);
    assert_eq!(node.permission_ceiling, PermissionCeiling::unrestricted());
}

#[test]
fn workflow_definition_defaults_to_schema_version_one() {
    let definition: WorkflowDefinition = serde_json::from_value(json!({
        "id": "legacy",
        "version": "1",
        "description": "Legacy workflow",
        "nodes": []
    }))
    .unwrap();

    assert_eq!(definition.schema_version, 1);
}

#[test]
fn multi_agent_policy_defaults_to_on_demand_and_migrates_old_names() {
    assert_eq!(MultiAgentPolicy::default(), MultiAgentPolicy::OnDemand);
    assert_eq!(
        serde_json::from_str::<MultiAgentPolicy>(r#""explicit""#).unwrap(),
        MultiAgentPolicy::OnDemand
    );
    assert_eq!(
        serde_json::from_str::<MultiAgentPolicy>(r#""adaptive""#).unwrap(),
        MultiAgentPolicy::OnDemand
    );
    assert_eq!(
        serde_json::to_string(&MultiAgentPolicy::OnDemand).unwrap(),
        r#""on_demand""#
    );
}

#[test]
fn collaboration_strategy_has_stable_wire_names() {
    let cases = [
        (CollaborationStrategy::Single, "single"),
        (CollaborationStrategy::Supervisor, "supervisor"),
        (CollaborationStrategy::Team, "team"),
        (CollaborationStrategy::Fanout, "fanout"),
        (CollaborationStrategy::IndependentReviewer, "independent_reviewer"),
    ];

    for (strategy, expected) in cases {
        assert_eq!(strategy.as_str(), expected);
        assert_eq!(strategy.to_string(), expected);
        assert_eq!(serde_json::to_value(strategy).unwrap(), json!(expected));
    }
}

#[test]
fn collaboration_selection_preserves_v1_wire_shape() {
    let cases = [
        (CollaborationSelection::Auto, json!({"mode": "auto"})),
        (
            CollaborationSelection::Fixed(CollaborationStrategy::Team),
            json!({"mode": "fixed", "strategy": "team"}),
        ),
        (CollaborationSelection::Inherit, json!({"mode": "inherit"})),
    ];

    for (selection, expected) in cases {
        let serialized = serde_json::to_value(&selection).unwrap();
        assert_eq!(serialized, expected);
        assert_eq!(
            serde_json::from_value::<CollaborationSelection>(serialized).unwrap(),
            selection
        );
    }
}

#[test]
fn configured_collaboration_round_trips_full_runtime_config() {
    let selection = CollaborationSelection::Configured(CollaborationRuntimeConfig {
        strategy: CollaborationStrategy::IndependentReviewer,
        worker_roles: vec![
            WorkerRolePolicy {
                role: "author".into(),
                max_concurrent: 1,
                max_total: 1,
            },
            WorkerRolePolicy {
                role: "reviewer".into(),
                max_concurrent: 1,
                max_total: 1,
            },
        ],
        primary_role: Some("author".into()),
        reviewer_role: Some("reviewer".into()),
        ..CollaborationRuntimeConfig::default()
    });

    let value = serde_json::to_value(&selection).unwrap();
    assert_eq!(value["mode"], "configured");
    assert_eq!(value["strategy"]["strategy"], "independent_reviewer");
    assert_eq!(value["strategy"]["max_concurrent_workers"], 4);
    assert_eq!(value["strategy"]["max_tasks"], 32);
    assert_eq!(value["strategy"]["max_coordinator_rounds"], 8);
    assert_eq!(value["strategy"]["max_pending_messages"], 64);
    assert_eq!(value["strategy"]["max_message_bytes"], 65_536);
    assert_eq!(
        serde_json::from_value::<CollaborationSelection>(value).unwrap(),
        selection
    );
}

#[test]
fn configured_collaboration_deserialization_applies_runtime_defaults() {
    let selection: CollaborationSelection = serde_json::from_value(json!({
        "mode": "configured",
        "strategy": {
            "strategy": "supervisor",
            "worker_roles": [{
                "role": "worker",
                "max_concurrent": 2,
                "max_total": 4
            }]
        }
    }))
    .unwrap();

    let CollaborationSelection::Configured(config) = selection else {
        panic!("expected configured collaboration");
    };
    assert_eq!(config.max_concurrent_workers, 4);
    assert_eq!(config.max_tasks, 32);
    assert_eq!(config.max_coordinator_rounds, 8);
    assert_eq!(config.max_pending_messages, 64);
    assert_eq!(config.max_message_bytes, 65_536);
    assert!(config.primary_role.is_none());
    assert!(config.reviewer_role.is_none());
}

#[test]
fn legacy_agent_collaboration_context_uses_message_limit_defaults() {
    let context: crate::spawner::AgentCollaborationContext = serde_json::from_value(json!({
        "team_id": "legacy-team",
        "strategy": "team",
        "coordinator_agent_id": "legacy-coordinator"
    }))
    .unwrap();

    assert_eq!(
        context.max_pending_messages,
        CollaborationRuntimeConfig::DEFAULT_MAX_PENDING_MESSAGES
    );
    assert_eq!(
        context.max_message_bytes,
        CollaborationRuntimeConfig::DEFAULT_MAX_MESSAGE_BYTES
    );
    assert_eq!(
        serde_json::to_value(&context).unwrap(),
        json!({
            "team_id": "legacy-team",
            "strategy": "team",
            "coordinator_agent_id": "legacy-coordinator"
        })
    );
}

#[test]
fn non_default_agent_collaboration_message_limits_remain_on_the_wire() {
    let context = crate::spawner::AgentCollaborationContext {
        team_id: crate::identity::TeamId::from("limited-team"),
        strategy: CollaborationStrategy::Team,
        coordinator_agent_id: crate::identity::AgentId::from("limited-coordinator"),
        max_pending_messages: 1,
        max_message_bytes: 1_024,
    };

    let value = serde_json::to_value(context).unwrap();

    assert_eq!(value["max_pending_messages"], 1);
    assert_eq!(value["max_message_bytes"], 1_024);
}

#[test]
fn worker_role_limits_are_required_on_the_wire() {
    for missing_field in ["max_concurrent", "max_total"] {
        let mut role = json!({
            "role": "worker",
            "max_concurrent": 1,
            "max_total": 2
        });
        role.as_object_mut().unwrap().remove(missing_field);
        let value = json!({
            "mode": "configured",
            "strategy": {
                "strategy": "supervisor",
                "worker_roles": [role]
            }
        });

        assert!(serde_json::from_value::<CollaborationSelection>(value).is_err());
    }
}
