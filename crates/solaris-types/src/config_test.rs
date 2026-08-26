use super::{ConfigField, ConfigFieldResult, ConfigFieldStatus, RuntimeConfigUpdate};

#[test]
fn field_result_uses_stable_snake_case_values() {
    let value = serde_json::to_value(ConfigFieldResult::new(
        ConfigField::ThinkingBudget,
        ConfigFieldStatus::Unsupported,
        "provider does not support thinking",
    ))
    .unwrap();

    assert_eq!(value["field"], "thinking_budget");
    assert_eq!(value["status"], "unsupported");
    assert_eq!(value["message"], "provider does not support thinking");
}

#[test]
fn runtime_update_deserializes_the_typed_agent_limit() {
    let update: RuntimeConfigUpdate = serde_json::from_value(serde_json::json!({
        "multi_agent_policy": "proactive",
        "max_active_agents": 4
    }))
    .unwrap();

    assert_eq!(
        update.multi_agent_policy,
        Some(crate::workflow::MultiAgentPolicy::Proactive)
    );
    assert_eq!(update.max_active_agents, Some(4));
}
