use std::fs::File;
use std::path::PathBuf;

use super::ProcessLaunchPolicy;
use crate::ProtectedObjectIdentity;

#[test]
fn default_policy_preserves_ambient_access() {
    assert_eq!(ProcessLaunchPolicy::default(), ProcessLaunchPolicy::Ambient);
}

#[test]
fn workspace_policy_owns_the_protected_path_snapshot() {
    let workspace = PathBuf::from("workspace");
    let protected = PathBuf::from("runtime");
    let policy = ProcessLaunchPolicy::workspace_sandbox(&workspace, [protected.clone()]);

    assert_eq!(
        policy,
        ProcessLaunchPolicy::WorkspaceSandbox {
            workspace_root: workspace,
            workspace_capability: None,
            protected_roots: vec![protected],
            protected_object_identities: Vec::new(),
            network_proxy: Default::default(),
        }
    );
}

#[test]
fn network_constructor_keeps_only_exact_validated_destinations() {
    let policy =
        ProcessLaunchPolicy::workspace_sandbox_with_network("workspace", [], [], ["https://api.example.test/v1"])
            .unwrap();
    let ProcessLaunchPolicy::WorkspaceSandbox { network_proxy, .. } = policy else {
        panic!("expected workspace sandbox");
    };
    assert!(network_proxy.permits("api.example.test", 443));
    assert!(!network_proxy.permits("other.example.test", 443));
}

#[test]
fn workspace_policy_carries_opened_protected_object_history() {
    let directory = tempfile::tempdir().unwrap();
    let state = directory.path().join("state");
    let alias = directory.path().join("alias");
    std::fs::write(&state, b"old-state").unwrap();
    std::fs::hard_link(&state, &alias).unwrap();
    let identity = ProtectedObjectIdentity::from_file(&File::open(&state).unwrap()).unwrap();

    let policy = ProcessLaunchPolicy::workspace_sandbox_with_identities(directory.path(), [], [identity]);

    assert!(matches!(
        policy,
        ProcessLaunchPolicy::WorkspaceSandbox {
            protected_object_identities,
            ..
        } if protected_object_identities == vec![identity]
    ));
    assert_eq!(
        ProtectedObjectIdentity::from_file(&File::open(alias).unwrap()).unwrap(),
        identity
    );
}
