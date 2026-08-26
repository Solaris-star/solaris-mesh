use super::*;
use crate::effect::EffectClass;

#[test]
fn child_ceiling_can_only_narrow_parent() {
    let parent = PermissionCeiling {
        agent_lifecycle: true,
        mesh_state_mutation: true,
        workspace_mutation: true,
        process: false,
        network: true,
        external_side_effect: false,
    };
    let requested = PermissionCeiling::unrestricted();
    let effective = parent.intersect(requested);

    assert!(effective.allows(EffectClass::WorkspaceMutation));
    assert!(!effective.allows(EffectClass::Process));
    assert!(!effective.allows(EffectClass::ExternalSideEffect));
}

#[test]
fn plan_ceiling_blocks_user_workspace_mutation_and_processes() {
    let ceiling = PermissionCeiling::plan();
    assert!(ceiling.allows(EffectClass::ReadOnly));
    assert!(ceiling.allows(EffectClass::AgentLifecycle));
    assert!(ceiling.allows(EffectClass::MeshStateMutation));
    assert!(ceiling.allows(EffectClass::Network));
    assert!(!ceiling.allows(EffectClass::WorkspaceMutation));
    assert!(!ceiling.allows(EffectClass::Process));
}
