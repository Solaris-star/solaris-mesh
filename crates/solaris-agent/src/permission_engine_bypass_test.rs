use solaris_types::effect::{EffectClass, EffectDescriptor, EffectReplayPolicy, EffectRequest, ResourceFootprint};
use solaris_types::identity::{EffectId, OperationId, RunId};
use solaris_types::permission::{
    ExecutionBoundary, PermissionCeiling, PermissionDecision, PermissionMode, PermissionRule,
};

use super::{PermissionContext, PermissionEvaluation};

fn request(capability: &str, class: EffectClass, resources: ResourceFootprint) -> EffectRequest {
    EffectRequest {
        effect_id: EffectId::new(format!("effect-{capability}")),
        operation_id: OperationId::new(format!("operation-{capability}")),
        capability: capability.to_owned(),
        descriptor: EffectDescriptor {
            class,
            action: "test bypass access".to_owned(),
            resources,
            replay_policy: EffectReplayPolicy::Never,
        },
        effective_input: serde_json::Value::Null,
        input_digest: None,
    }
}

fn evaluate(context: &PermissionContext, capability: &str, request: &EffectRequest) -> PermissionEvaluation {
    context.evaluate_effect(&RunId::new("bypass-run"), capability, request)
}

#[test]
fn bypass_ignores_workspace_boundary_and_permission_ceiling() {
    let context = PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::plan());
    context.set_boundary(ExecutionBoundary::workspace("workspace"));

    let mut process = ResourceFootprint {
        process_commands: vec!["external-program".to_owned()],
        ..ResourceFootprint::default()
    };
    process.declare_uncontained_process_access();
    let requests = [
        (
            "Read",
            request(
                "Read",
                EffectClass::ReadOnly,
                ResourceFootprint {
                    file_reads: vec!["outside/private.txt".to_owned()],
                    ..ResourceFootprint::default()
                },
            ),
        ),
        (
            "Write",
            request(
                "Write",
                EffectClass::WorkspaceMutation,
                ResourceFootprint {
                    file_writes: vec!["outside/changed.txt".to_owned()],
                    ..ResourceFootprint::default()
                },
            ),
        ),
        ("ExecCommand", request("ExecCommand", EffectClass::Process, process)),
        (
            "Network",
            request(
                "Network",
                EffectClass::Network,
                ResourceFootprint {
                    network_domains: vec!["https://outside.example.test".to_owned()],
                    ..ResourceFootprint::default()
                },
            ),
        ),
        (
            "External",
            request(
                "External",
                EffectClass::ExternalSideEffect,
                ResourceFootprint {
                    external_resources: vec!["outside:resource".to_owned()],
                    ..ResourceFootprint::default()
                },
            ),
        ),
    ];

    for (capability, request) in requests {
        assert_eq!(
            evaluate(&context, capability, &request).decision,
            PermissionDecision::Allow,
            "Bypass must not retain an ordinary boundary or ceiling denial for {capability}"
        );
    }
}

#[test]
fn bypass_still_honors_explicit_deny_rules() {
    let context = PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted());
    context.add_rule(PermissionRule {
        capability: Some("ExecCommand".to_owned()),
        action: None,
        effect_class: None,
        resource_prefixes: Vec::new(),
        decision: PermissionDecision::Deny,
    });
    let mut resources = ResourceFootprint::default();
    resources.declare_uncontained_process_access();
    let request = request("ExecCommand", EffectClass::Process, resources);

    let result = evaluate(&context, "ExecCommand", &request);

    assert_eq!(result.decision, PermissionDecision::Deny);
    assert!(result.matched_rule_index.is_some());
}

#[test]
fn bypass_does_not_override_a_capability_mismatch() {
    let context = PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted());
    let request = request("Read", EffectClass::ReadOnly, ResourceFootprint::default());

    let result = evaluate(&context, "Write", &request);

    assert_eq!(result.decision, PermissionDecision::Deny);
    assert!(result.reason.contains("does not match"));
}
