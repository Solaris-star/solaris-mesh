use serde_json::json;

use crate::identity::RunId;
use crate::runtime::TaskFailureClass;

use super::{
    AgentConversationError, AgentConversationHandle, AgentOutcomeStatus, AgentSpawnError, AgentSpawnSpec,
    AgentTurnSpec, OutcomeBlobRef, SubAgentResult,
};

#[test]
fn conversation_turn_identity_is_stable_and_input_sensitive() {
    let handle = AgentConversationHandle::for_test("conversation-a", "agent-a", "session-a");
    let first = AgentTurnSpec {
        turn_id: "turn-1".to_owned(),
        prompt: "inspect the workspace".to_owned(),
    };
    let replay = first.clone();
    let changed = AgentTurnSpec {
        prompt: "modify the workspace".to_owned(),
        ..first.clone()
    };

    assert_eq!(handle.turn_identity(&first), handle.turn_identity(&replay));
    assert_ne!(
        handle.turn_identity(&first).input_digest,
        handle.turn_identity(&changed).input_digest
    );
}

#[test]
fn conversation_turn_identity_binds_the_full_open_identity_domain() {
    let handle = AgentConversationHandle::for_test("conversation-a", "agent-a", "session-a");
    let turn = AgentTurnSpec {
        turn_id: "turn-1".to_owned(),
        prompt: "inspect".to_owned(),
    };
    let baseline = handle.turn_identity(&turn);

    let mut variants = Vec::new();
    let mut changed = handle.clone();
    changed.run_id = "another-run".into();
    variants.push(changed);
    let mut changed = handle.clone();
    changed.parent_agent_id = "another-parent".into();
    variants.push(changed);
    let mut changed = handle.clone();
    changed.agent_id = "another-agent".into();
    variants.push(changed);
    let mut changed = handle.clone();
    changed.session_id = "another-session".to_owned();
    variants.push(changed);
    let mut changed = handle.clone();
    changed.operation_id = "another-open-operation".into();
    variants.push(changed);
    let mut changed = handle;
    changed.spec_digest = "another-spec-digest".to_owned();
    variants.push(changed);

    for variant in variants {
        let identity = variant.turn_identity(&turn);
        assert_ne!(identity.operation_id, baseline.operation_id);
        assert_ne!(identity.message_id, baseline.message_id);
    }
}

#[test]
fn old_agent_spawn_spec_wire_defaults_task_revision_without_changing_round_trip() {
    let old = json!({
        "run_id": "run",
        "parent_agent_id": "parent",
        "task_id": "task",
        "role_key": "worker",
        "stable_task_key": "stable",
        "operation_id": "operation",
        "config": {
            "name": "worker",
            "prompt": "work",
            "max_turns": 1,
            "max_tokens": 32,
            "system_prompt": null
        },
        "overrides": {
            "model": null,
            "effort": null,
            "allowed_tools": [],
            "inherit_capabilities": false
        },
        "permission_ceiling": {
            "agent_lifecycle": true,
            "mesh_state_mutation": true,
            "workspace_mutation": false,
            "process": false,
            "network": false,
            "external_side_effect": false
        },
        "resource_budget": {
            "max_active_agents": null,
            "max_concurrent_effects": null,
            "max_spawn_depth": null,
            "max_total_descendants_per_run": null,
            "max_turns": null,
            "max_tokens": null,
            "max_wall_time_ms": null,
            "max_cost": null,
            "max_process_output_bytes": null
        }
    });

    let spec: AgentSpawnSpec = serde_json::from_value(old.clone()).expect("old wire must deserialize");
    assert_eq!(spec.expected_task_revision, None);
    assert_eq!(serde_json::to_value(spec).unwrap(), old);
}

#[test]
fn typed_spawn_failure_classes_have_stable_wire_values() {
    let error = AgentSpawnError::outcome_unknown("unknown");
    assert_eq!(error.failure_class, TaskFailureClass::OutcomeUnknown);
    assert_eq!(
        serde_json::to_value(error).unwrap(),
        json!({"failure_class":"outcome_unknown","message":"unknown"})
    );
    assert_eq!(
        serde_json::to_value(AgentOutcomeStatus::OutcomeUnknown).unwrap(),
        json!("outcome_unknown")
    );
}

#[test]
fn typed_spawn_error_constructors_cover_every_failure_class() {
    for (error, class, wire) in [
        (
            AgentSpawnError::retryable("m"),
            TaskFailureClass::Retryable,
            "retryable",
        ),
        (
            AgentSpawnError::non_retryable("m"),
            TaskFailureClass::NonRetryable,
            "non_retryable",
        ),
        (
            AgentSpawnError::permission_denied("m"),
            TaskFailureClass::PermissionDenied,
            "permission_denied",
        ),
        (AgentSpawnError::max_turns("m"), TaskFailureClass::MaxTurns, "max_turns"),
        (
            AgentSpawnError::non_convergent("m"),
            TaskFailureClass::NonConvergent,
            "non_convergent",
        ),
        (
            AgentSpawnError::cancelled("m"),
            TaskFailureClass::Cancelled,
            "cancelled",
        ),
        (
            AgentSpawnError::outcome_unknown("m"),
            TaskFailureClass::OutcomeUnknown,
            "outcome_unknown",
        ),
        (
            AgentSpawnError::reconciliation_required("m"),
            TaskFailureClass::ReconciliationRequired,
            "reconciliation_required",
        ),
        (
            AgentSpawnError::side_effect_unknown("m"),
            TaskFailureClass::SideEffectUnknown,
            "side_effect_unknown",
        ),
    ] {
        assert_eq!(error.failure_class, class);
        assert_eq!(
            serde_json::to_value(error).unwrap(),
            json!({"failure_class":wire,"message":"m"})
        );
    }
}

#[test]
fn typed_conversation_error_constructors_cover_every_failure_class() {
    for (error, class) in [
        (AgentConversationError::retryable("m"), TaskFailureClass::Retryable),
        (
            AgentConversationError::non_retryable("m"),
            TaskFailureClass::NonRetryable,
        ),
        (
            AgentConversationError::permission_denied("m"),
            TaskFailureClass::PermissionDenied,
        ),
        (AgentConversationError::max_turns("m"), TaskFailureClass::MaxTurns),
        (
            AgentConversationError::non_convergent("m"),
            TaskFailureClass::NonConvergent,
        ),
        (AgentConversationError::cancelled("m"), TaskFailureClass::Cancelled),
        (
            AgentConversationError::outcome_unknown("m"),
            TaskFailureClass::OutcomeUnknown,
        ),
        (
            AgentConversationError::reconciliation_required("m"),
            TaskFailureClass::ReconciliationRequired,
        ),
        (
            AgentConversationError::side_effect_unknown("m"),
            TaskFailureClass::SideEffectUnknown,
        ),
    ] {
        assert_eq!(error.failure_class, class);
    }
}

#[test]
fn agent_outcome_and_sub_agent_result_carry_optional_failure_class() {
    let result: SubAgentResult = serde_json::from_value(json!({
        "name": "worker",
        "status": "failed",
        "text": "boom",
        "usage": {"input_tokens":0,"output_tokens":0},
        "turns": 1,
        "is_error": true
    }))
    .expect("legacy SubAgentResult wire must remain readable");
    assert_eq!(result.failure_class, None);

    let classified: SubAgentResult = serde_json::from_value(json!({
        "name": "worker",
        "status": "failed",
        "failure_class": "permission_denied",
        "text": "denied",
        "usage": {"input_tokens":0,"output_tokens":0},
        "turns": 1,
        "is_error": true
    }))
    .unwrap();
    assert_eq!(classified.failure_class, Some(TaskFailureClass::PermissionDenied));
}

#[test]
fn outcome_blob_reference_has_a_provider_neutral_wire_shape() {
    let reference = OutcomeBlobRef {
        reference: "sha256-abc.blob".into(),
        bytes: 42,
        digest: "abc".into(),
        run_id: Some(RunId::from("run")),
        status: Some(AgentOutcomeStatus::Completed),
    };

    assert_eq!(
        serde_json::to_value(&reference).unwrap(),
        json!({
            "reference":"sha256-abc.blob", "bytes":42, "digest":"abc", "run_id":"run", "status":"completed"
        })
    );
    assert_eq!(
        serde_json::from_value::<OutcomeBlobRef>(serde_json::to_value(&reference).unwrap()).unwrap(),
        reference
    );
    let legacy: OutcomeBlobRef = serde_json::from_value(json!({
        "reference":"sha256-legacy.blob", "bytes":7, "digest":"legacy"
    }))
    .unwrap();
    assert_eq!(legacy.status, None);
}
