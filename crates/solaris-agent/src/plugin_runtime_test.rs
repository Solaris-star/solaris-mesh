use std::sync::{Arc, Mutex};

use solaris_types::plugin::{
    ImplementationIdentity, PluginCapabilities, PluginCommandContributionDefinition, PluginContributionKind,
    PluginDefinition, PluginScope, PluginSource,
};

use super::*;

fn local_definition(path: &str) -> PluginDefinition {
    PluginDefinition {
        id: "local".into(),
        version: "1".into(),
        source: PluginSource::Local { path: path.into() },
        materialized_path: None,
        capabilities: PluginCapabilities::default(),
        compatibility: Default::default(),
        requested_paths: Vec::new(),
        requires_services: Vec::new(),
        resources: Default::default(),
        command_tools: Vec::new(),
        command_contributions: Vec::new(),
    }
}

#[test]
fn plugin_manifest_rejects_result_limits_that_cannot_be_safely_truncated() {
    let mut definition = local_definition("plugin");
    definition.capabilities.providers.push("acme".to_owned());
    definition
        .command_contributions
        .push(PluginCommandContributionDefinition {
            kind: PluginContributionKind::Provider,
            name: "acme".to_owned(),
            command: "runner".to_owned(),
            args: Vec::new(),
            input_schema: serde_json::json!({"type": "object"}),
            max_result_size: 1,
            timeout_ms: 1_000,
        });

    let error = validate_plugin_definition(&definition).unwrap_err();
    assert!(error.contains("max_result_size must be at least 2"));
}

#[test]
fn relative_local_plugin_resolves_inside_trusted_authority() {
    let root = tempfile::tempdir().unwrap();
    let plugin_dir = root.path().join("plugin");
    std::fs::create_dir(&plugin_dir).unwrap();
    let resolver = PluginResolver::new(
        root.path(),
        PluginTrustPolicy {
            trusted_local_roots: vec![root.path().to_path_buf()],
            ..Default::default()
        },
    );
    let resolved = resolver.resolve(local_definition("plugin")).unwrap();
    let expected = plugin_dir.canonicalize().unwrap().to_string_lossy().into_owned();
    assert_eq!(resolved.authority_root, Some(expected));
}

#[test]
fn local_plugin_cannot_escape_trusted_authority() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("workspace");
    let outside = parent.path().join("outside");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    let resolver = PluginResolver::new(
        &root,
        PluginTrustPolicy {
            trusted_local_roots: vec![root.clone()],
            ..Default::default()
        },
    );
    let error = resolver.resolve(local_definition("../outside")).unwrap_err();
    assert!(error.contains("escapes trusted authority roots"));
}

#[test]
fn plugin_digest_rejects_directories_beyond_the_scan_limit() {
    let root = tempfile::tempdir().unwrap();
    let plugin_dir = root.path().join("plugin");
    let mut current = plugin_dir.clone();
    for index in 0..=MAX_PLUGIN_DEPTH {
        current = current.join(format!("level-{index}"));
    }
    std::fs::create_dir_all(&current).unwrap();
    std::fs::write(current.join("runner"), b"test").unwrap();
    let resolver = PluginResolver::new(
        root.path(),
        PluginTrustPolicy {
            trusted_local_roots: vec![root.path().to_path_buf()],
            ..Default::default()
        },
    );

    let error = resolver.resolve(local_definition("plugin")).unwrap_err();

    assert!(error.contains("maximum depth"));
}

#[test]
fn plugin_digest_rejects_more_files_than_the_scan_limit() {
    let root = tempfile::tempdir().unwrap();
    let plugin_dir = root.path().join("plugin");
    std::fs::create_dir(&plugin_dir).unwrap();
    for index in 0..=MAX_PLUGIN_FILES {
        std::fs::write(plugin_dir.join(format!("file-{index:04}")), b"test").unwrap();
    }
    let resolver = PluginResolver::new(
        root.path(),
        PluginTrustPolicy {
            trusted_local_roots: vec![root.path().to_path_buf()],
            ..Default::default()
        },
    );

    let error = resolver.resolve(local_definition("plugin")).unwrap_err();

    assert!(error.contains("more than"));
}

#[cfg(unix)]
#[test]
fn plugin_digest_rejects_symlinked_files() {
    use std::os::unix::fs::symlink;

    let root = tempfile::tempdir().unwrap();
    let plugin_dir = root.path().join("plugin");
    let outside = root.path().join("outside");
    std::fs::create_dir(&plugin_dir).unwrap();
    std::fs::write(&outside, b"outside").unwrap();
    symlink(&outside, plugin_dir.join("runner")).unwrap();
    let resolver = PluginResolver::new(
        root.path(),
        PluginTrustPolicy {
            trusted_local_roots: vec![root.path().to_path_buf()],
            ..Default::default()
        },
    );

    let error = resolver.resolve(local_definition("plugin")).unwrap_err();

    assert!(error.contains("symbolic link"));
}

#[test]
fn manifest_source_cannot_grant_its_own_remote_trust() {
    let root = tempfile::tempdir().unwrap();
    let mut definition = local_definition("plugin");
    definition.source = PluginSource::Git {
        repository: "https://example.test/acme/plugin".into(),
        reference: "main".into(),
    };
    definition.materialized_path = Some("plugin".into());
    std::fs::create_dir(root.path().join("plugin")).unwrap();
    let resolver = PluginResolver::new(
        root.path(),
        PluginTrustPolicy {
            trusted_local_roots: vec![root.path().to_path_buf()],
            ..Default::default()
        },
    );
    let error = resolver.resolve(definition).unwrap_err();
    assert!(error.contains("not pre-trusted"));
}

#[test]
fn incompatible_runtime_api_is_rejected_before_resolution() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("plugin")).unwrap();
    let mut definition = local_definition("plugin");
    definition.compatibility.runtime_api_version = Some(PLUGIN_RUNTIME_API_VERSION + 1);
    let resolver = PluginResolver::new(
        root.path(),
        PluginTrustPolicy {
            trusted_local_roots: vec![root.path().to_path_buf()],
            ..Default::default()
        },
    );
    let error = resolver.resolve(definition).unwrap_err();
    assert!(error.contains("requires runtime API"));
}

#[test]
fn capability_resolution_inherits_and_child_overrides() {
    let resolver = CapabilityResolver::default();
    resolver.define_scope("runtime", None);
    resolver.define_scope("agent", Some("runtime".into()));
    resolver
        .bind(
            "runtime",
            CapabilityBinding {
                capability: "provider".into(),
                implementation: ImplementationIdentity {
                    implementation_id: "base".into(),
                    version: None,
                    digest: None,
                },
                plugin_id: "base-plugin".into(),
            },
        )
        .unwrap();
    assert_eq!(resolver.resolve("agent", "provider").unwrap().plugin_id, "base-plugin");
    resolver
        .bind(
            "agent",
            CapabilityBinding {
                capability: "provider".into(),
                implementation: ImplementationIdentity {
                    implementation_id: "agent".into(),
                    version: None,
                    digest: None,
                },
                plugin_id: "agent-plugin".into(),
            },
        )
        .unwrap();
    assert_eq!(resolver.resolve("agent", "provider").unwrap().plugin_id, "agent-plugin");
}

#[test]
fn activation_resources_dispose_in_reverse_order() {
    let plugin = Arc::new(ResolvedPluginDefinition {
        definition: PluginDefinition {
            id: "test".into(),
            version: "1".into(),
            source: PluginSource::HostBundled { id: "test".into() },
            materialized_path: None,
            capabilities: PluginCapabilities::default(),
            compatibility: Default::default(),
            requested_paths: Vec::new(),
            requires_services: Vec::new(),
            resources: Default::default(),
            command_tools: Vec::new(),
            command_contributions: Vec::new(),
        },
        identity: ResolvedPluginIdentity {
            plugin_id: "test".into(),
            source: PluginSource::HostBundled { id: "test".into() },
            implementation: ImplementationIdentity {
                implementation_id: "test".into(),
                version: Some("1".into()),
                digest: Some("abc".into()),
            },
        },
        authority_root: None,
    });
    let activation = PluginActivation::new(PluginScope::Global, plugin, Default::default());
    let order = Arc::new(Mutex::new(Vec::new()));
    for value in [1, 2, 3] {
        let order = Arc::clone(&order);
        activation.add_resource(ScopedResource::new(format!("r{value}"), move || {
            order.lock().unwrap().push(value);
        }));
    }
    activation.deactivate();
    assert_eq!(*order.lock().unwrap(), vec![3, 2, 1]);
}
fn resolved_host_plugin(id: &str, capabilities: PluginCapabilities) -> ResolvedPluginDefinition {
    ResolvedPluginDefinition {
        definition: PluginDefinition {
            id: id.into(),
            version: "1".into(),
            source: PluginSource::HostBundled { id: id.into() },
            materialized_path: None,
            capabilities,
            compatibility: Default::default(),
            requested_paths: Vec::new(),
            requires_services: Vec::new(),
            resources: Default::default(),
            command_tools: Vec::new(),
            command_contributions: Vec::new(),
        },
        identity: ResolvedPluginIdentity {
            plugin_id: id.into(),
            source: PluginSource::HostBundled { id: id.into() },
            implementation: ImplementationIdentity {
                implementation_id: format!("host:{id}"),
                version: Some("1".into()),
                digest: Some(format!("digest:{id}")),
            },
        },
        authority_root: None,
    }
}

#[test]
fn capability_batch_collision_is_transactional() {
    let resolver = CapabilityResolver::default();
    resolver.define_scope("run:r", None);
    resolver
        .bind(
            "run:r",
            CapabilityBinding {
                capability: "tool:shared".into(),
                implementation: ImplementationIdentity {
                    implementation_id: "first".into(),
                    version: None,
                    digest: None,
                },
                plugin_id: "first".into(),
            },
        )
        .unwrap();

    let error = resolver
        .bind_batch(
            "run:r",
            vec![
                CapabilityBinding {
                    capability: "tool:new".into(),
                    implementation: ImplementationIdentity {
                        implementation_id: "second".into(),
                        version: None,
                        digest: None,
                    },
                    plugin_id: "second".into(),
                },
                CapabilityBinding {
                    capability: "tool:shared".into(),
                    implementation: ImplementationIdentity {
                        implementation_id: "second".into(),
                        version: None,
                        digest: None,
                    },
                    plugin_id: "second".into(),
                },
            ],
        )
        .unwrap_err();
    assert!(error.contains("already bound"));
    assert!(resolver.resolve("run:r", "tool:new").is_none());
    assert_eq!(resolver.resolve("run:r", "tool:shared").unwrap().plugin_id, "first");
}

#[test]
fn deactivate_removes_scope_capability_bindings() {
    let runtime = PluginRuntime::default();
    runtime.initialize_scope_tree("workspace", "run", "root");
    let plugin = runtime.install(resolved_host_plugin(
        "hot",
        PluginCapabilities {
            tools: vec!["hot_tool".into()],
            ..Default::default()
        },
    ));
    let activation_id = "run:run:hot";
    runtime
        .activate(
            activation_id,
            PluginScope::Run { run_id: "run".into() },
            &plugin.definition.id,
        )
        .unwrap();
    assert_eq!(
        runtime
            .capability_resolver()
            .resolve("run:run", "tool:hot_tool")
            .unwrap()
            .plugin_id,
        "hot"
    );
    assert!(runtime.deactivate(activation_id));
    assert!(
        runtime
            .capability_resolver()
            .resolve("run:run", "tool:hot_tool")
            .is_none()
    );
}

#[test]
fn activation_rejects_declared_capability_without_callable_implementation() {
    let runtime = PluginRuntime::default();
    runtime.initialize_scope_tree("workspace", "run", "root");
    let plugin = runtime.install(resolved_host_plugin(
        "extensions",
        PluginCapabilities {
            providers: vec!["acme".into()],
            collaboration_strategies: vec!["review-pair".into()],
            storage_backends: vec!["sqlite".into()],
            ..Default::default()
        },
    ));
    let error = runtime
        .activate(
            "run:run:extensions",
            PluginScope::Run { run_id: "run".into() },
            &plugin.definition.id,
        )
        .err()
        .unwrap();
    assert!(error.contains("requires exactly one callable implementation"));
}

#[test]
fn activation_binds_and_exposes_callable_contributions() {
    let root = tempfile::tempdir().unwrap();
    let executable = root.path().join("runner");
    std::fs::write(&executable, b"test").unwrap();
    let definitions = [
        (PluginContributionKind::Provider, "acme"),
        (PluginContributionKind::CollaborationStrategy, "review-pair"),
        (PluginContributionKind::StorageBackend, "sqlite"),
        (PluginContributionKind::Hook, "before-tool"),
    ];
    let plugin = ResolvedPluginDefinition {
        definition: PluginDefinition {
            id: "extensions".into(),
            version: "1".into(),
            source: PluginSource::Local {
                path: root.path().to_string_lossy().into_owned(),
            },
            materialized_path: None,
            capabilities: PluginCapabilities {
                providers: vec!["acme".into()],
                collaboration_strategies: vec!["review-pair".into()],
                storage_backends: vec!["sqlite".into()],
                hooks: vec!["before-tool".into()],
                ..Default::default()
            },
            compatibility: Default::default(),
            requested_paths: Vec::new(),
            requires_services: Vec::new(),
            resources: Default::default(),
            command_tools: Vec::new(),
            command_contributions: definitions
                .into_iter()
                .map(|(kind, name)| PluginCommandContributionDefinition {
                    kind,
                    name: name.into(),
                    command: "runner".into(),
                    args: Vec::new(),
                    input_schema: serde_json::json!({"type": "object"}),
                    max_result_size: 1024,
                    timeout_ms: 1_000,
                })
                .collect(),
        },
        identity: ResolvedPluginIdentity {
            plugin_id: "extensions".into(),
            source: PluginSource::Local {
                path: root.path().to_string_lossy().into_owned(),
            },
            implementation: ImplementationIdentity {
                implementation_id: "local:extensions".into(),
                version: Some("1".into()),
                digest: Some("digest:extensions".into()),
            },
        },
        authority_root: Some(root.path().canonicalize().unwrap().to_string_lossy().into_owned()),
    };
    let runtime = PluginRuntime::default();
    runtime.initialize_scope_tree("workspace", "run", "root");
    runtime.install(plugin);
    runtime
        .activate(
            "run:run:extensions",
            PluginScope::Run { run_id: "run".into() },
            "extensions",
        )
        .unwrap();

    for capability in [
        "provider:acme",
        "strategy:review-pair",
        "storage:sqlite",
        "hook:before-tool",
    ] {
        assert_eq!(
            runtime
                .capability_resolver()
                .resolve("run:run", capability)
                .unwrap()
                .plugin_id,
            "extensions"
        );
        let callable = runtime.resolve_command_contribution("agent:root", capability).unwrap();
        assert_eq!(callable.name(), capability.split_once(':').unwrap().1);
    }
}
