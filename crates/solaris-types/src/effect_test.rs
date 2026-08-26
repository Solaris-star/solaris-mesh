use super::*;

#[test]
fn read_only_descriptor_defaults_to_replay_safe() {
    let descriptor = EffectDescriptor::read_only("read file");
    assert_eq!(descriptor.class, EffectClass::ReadOnly);
    assert_eq!(descriptor.replay_policy, EffectReplayPolicy::ReplaySafe);
}

#[test]
fn audit_projection_preserves_identity_without_serializing_effect_secrets() {
    let secret = "super-secret-token";
    let descriptor = EffectDescriptor {
        class: EffectClass::Process,
        action: format!("deploy with {secret}"),
        resources: ResourceFootprint {
            unrestricted_file_reads: true,
            unrestricted_process: true,
            file_reads: vec![format!("credentials/{secret}")],
            network_domains: vec![format!("{secret}.example.test")],
            process_commands: vec![format!("deploy --token {secret}")],
            process_invocations: vec![ProcessInvocation {
                executable: "trusted-shell".into(),
                argv: vec!["-c".into(), format!("deploy --token {secret}")],
            }],
            external_resources: vec![format!("header:Authorization={secret}")],
            ..ResourceFootprint::default()
        },
        replay_policy: EffectReplayPolicy::ReconcileRequired,
    };

    let first = EffectAuditProjection::from_descriptor(&descriptor);
    let second = EffectAuditProjection::from_descriptor(&descriptor);
    let serialized = serde_json::to_string(&first).unwrap();

    assert_eq!(first, second);
    assert_eq!(first.version, EffectAuditProjection::VERSION);
    assert!(!serialized.contains(secret));
    assert!(!serialized.contains("deploy --token"));
    assert!(!serialized.contains("credentials/"));
    assert!(!serialized.contains("Authorization"));
    assert_eq!(first.class, EffectClass::Process);
    assert_eq!(first.action.summary, "process effect");
    assert!(first.action.digest.starts_with("sha256:"));
    assert!(first.descriptor_digest.starts_with("sha256:"));
    assert_eq!(first.executables.len(), 1);
    assert_eq!(first.executables[0].argv_count, 2);
    assert!(first.executables[0].executable_digest.starts_with("sha256:"));
    assert!(first.executables[0].invocation_digest.starts_with("sha256:"));
    assert!(first.resources.iter().any(|resource| {
        resource.kind == EffectResourceKind::ProcessCommand
            && resource.count == 1
            && resource
                .digest
                .as_deref()
                .is_some_and(|digest| digest.starts_with("sha256:"))
    }));
    assert!(first.resources.iter().any(|resource| {
        resource.kind == EffectResourceKind::FileRead && resource.count == 1 && resource.unrestricted
    }));
}

#[test]
fn audit_projection_uses_set_semantics_for_resource_collections() {
    let descriptor = EffectDescriptor {
        class: EffectClass::Process,
        action: "stable action".into(),
        resources: ResourceFootprint {
            file_reads: vec!["b".into(), "a".into(), "a".into()],
            file_writes: vec!["z".into(), "z".into()],
            network_domains: vec!["b.example".into(), "a.example".into()],
            process_commands: vec!["second".into(), "first".into(), "first".into()],
            process_invocations: vec![
                ProcessInvocation {
                    executable: "b-shell".into(),
                    argv: vec!["-c".into(), "second".into()],
                },
                ProcessInvocation {
                    executable: "a-shell".into(),
                    argv: vec!["-c".into(), "first".into()],
                },
                ProcessInvocation {
                    executable: "a-shell".into(),
                    argv: vec!["-c".into(), "first".into()],
                },
            ],
            external_resources: vec!["external:b".into(), "external:a".into()],
            mesh_resources: vec!["mesh:b".into(), "mesh:a".into(), "mesh:a".into()],
            ..Default::default()
        },
        replay_policy: EffectReplayPolicy::Never,
    };
    let mut reordered = descriptor.clone();
    reordered.resources.file_reads = vec!["a".into(), "b".into()];
    reordered.resources.file_writes = vec!["z".into()];
    reordered.resources.network_domains.reverse();
    reordered.resources.process_commands = vec!["first".into(), "second".into()];
    reordered.resources.process_invocations.reverse();
    reordered.resources.process_invocations.dedup();
    reordered.resources.external_resources.reverse();
    reordered.resources.mesh_resources = vec!["mesh:a".into(), "mesh:b".into()];

    let first = EffectAuditProjection::from_descriptor(&descriptor);
    let second = EffectAuditProjection::from_descriptor(&reordered);

    assert_eq!(first, second);
    assert_eq!(
        first
            .resources
            .iter()
            .find(|resource| resource.kind == EffectResourceKind::FileRead)
            .unwrap()
            .count,
        2
    );
    assert_eq!(first.executables.len(), 2);
}

#[test]
fn audit_projection_descriptor_digest_has_a_versioned_fixed_vector() {
    let descriptor = EffectDescriptor {
        class: EffectClass::Network,
        action: "fixed-vector".into(),
        resources: ResourceFootprint {
            network_domains: vec!["api.example.test".into()],
            ..Default::default()
        },
        replay_policy: EffectReplayPolicy::Idempotent,
    };

    assert_eq!(
        EffectAuditProjection::from_descriptor(&descriptor).descriptor_digest,
        "sha256:d1251057f151213d57ffdfcd6692260ba7f7706d6809211735d850c2e3079558"
    );
}
