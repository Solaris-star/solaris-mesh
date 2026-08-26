use solaris_types::effect::{
    EffectClass, EffectDescriptor, EffectReplayPolicy, EffectRequest, ProcessInvocation, ResourceFootprint,
};
use solaris_types::identity::{EffectId, OperationId, RunId};
use solaris_types::permission::{
    AdditionalPermissions, CapabilityLease, ExecutionBoundary, LeaseScope, PermissionCeiling, PermissionDecision,
    PermissionMode, PermissionRule,
};

use super::*;

#[cfg(windows)]
fn create_windows_junction(target: &std::path::Path, junction: &std::path::Path) {
    fn powershell_literal(path: &std::path::Path) -> String {
        format!("'{}'", path.to_string_lossy().replace('\'', "''"))
    }

    let shell = solaris_config::shell::resolve_shell(Some("powershell")).expect("resolve PowerShell");
    let script = format!(
        "$ErrorActionPreference = 'Stop'; \
         $item = New-Item -ItemType Junction -Path {} -Target {}; \
         if ($item.LinkType -ne 'Junction') {{ throw 'expected a junction' }}",
        powershell_literal(junction),
        powershell_literal(target)
    );
    let mut command = solaris_config::shell::shell_command_builder(&shell, &script, false);
    let output = tokio_test::block_on(command.output()).expect("run junction creation");
    assert!(
        output.status.success(),
        "junction creation failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn effect(class: EffectClass) -> EffectDescriptor {
    EffectDescriptor {
        class,
        action: "test".into(),
        resources: ResourceFootprint::default(),
        replay_policy: EffectReplayPolicy::ReconcileRequired,
    }
}

fn effect_request(capability: &str, class: EffectClass, resources: ResourceFootprint) -> EffectRequest {
    EffectRequest {
        effect_id: EffectId::new("effect-1"),
        operation_id: OperationId::new("operation-1"),
        capability: capability.into(),
        descriptor: EffectDescriptor {
            class,
            action: "test".into(),
            resources,
            replay_policy: EffectReplayPolicy::ReconcileRequired,
        },
        effective_input: serde_json::Value::Null,
        input_digest: None,
    }
}

#[test]
fn plan_allows_agent_lifecycle_but_denies_workspace_mutation() {
    let engine = PermissionEngine::with_ceilings(
        PermissionMode::Plan,
        PermissionCeiling::unrestricted(),
        PermissionCeiling::plan(),
        PermissionCeiling::unrestricted(),
    );

    assert_eq!(
        engine.evaluate("Spawn", &effect(EffectClass::AgentLifecycle)).decision,
        PermissionDecision::Allow
    );
    assert_eq!(
        engine
            .evaluate("Write", &effect(EffectClass::WorkspaceMutation))
            .decision,
        PermissionDecision::Deny
    );
    assert_eq!(
        engine.evaluate("RemoteTool", &effect(EffectClass::Network)).decision,
        PermissionDecision::Deny
    );
}

#[test]
fn plan_ceiling_cannot_be_widened_by_an_explicit_allow_rule() {
    let engine = PermissionEngine::new(PermissionMode::Plan);
    engine.add_rule(PermissionRule {
        capability: Some("ExecCommand".into()),
        action: None,
        effect_class: Some(EffectClass::Process),
        resource_prefixes: Vec::new(),
        decision: PermissionDecision::Allow,
    });

    assert_eq!(
        engine.evaluate("ExecCommand", &effect(EffectClass::Process)).decision,
        PermissionDecision::Deny
    );
}

#[test]
fn later_rule_overrides_earlier_rule() {
    let engine = PermissionEngine::new(PermissionMode::Auto);
    engine.add_rule(PermissionRule {
        capability: Some("ExecCommand".into()),
        action: None,
        effect_class: None,
        resource_prefixes: Vec::new(),
        decision: PermissionDecision::Allow,
    });
    engine.add_rule(PermissionRule {
        capability: Some("ExecCommand".into()),
        action: None,
        effect_class: Some(EffectClass::Process),
        resource_prefixes: Vec::new(),
        decision: PermissionDecision::Deny,
    });

    let result = engine.evaluate("ExecCommand", &effect(EffectClass::Process));
    assert_eq!(result.decision, PermissionDecision::Deny);
    assert_eq!(result.matched_rule_index, Some(1));
}

#[test]
fn later_allow_rule_cannot_override_earlier_deny() {
    let engine = PermissionEngine::new(PermissionMode::Auto);
    engine.add_rule(PermissionRule {
        capability: Some("ExecCommand".into()),
        action: None,
        effect_class: Some(EffectClass::Process),
        resource_prefixes: Vec::new(),
        decision: PermissionDecision::Deny,
    });
    engine.add_rule(PermissionRule {
        capability: Some("ExecCommand".into()),
        action: None,
        effect_class: None,
        resource_prefixes: Vec::new(),
        decision: PermissionDecision::Allow,
    });

    let result = engine.evaluate("ExecCommand", &effect(EffectClass::Process));
    assert_eq!(result.decision, PermissionDecision::Deny);
    assert_eq!(result.matched_rule_index, Some(0));
}

#[test]
fn configured_process_boundary_does_not_implicitly_authorize_auto_connection() {
    let context = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    context.set_boundary(ExecutionBoundary::workspace("workspace"));
    let request = effect_request(
        "McpConnect",
        EffectClass::Process,
        ResourceFootprint {
            process_commands: vec!["node mcp-server.js".into()],
            ..Default::default()
        },
    );
    context.allow_configured_effect_for("test:mcp", "McpConnect", &request.descriptor);
    context.set_mode(PermissionMode::Auto);
    assert_eq!(
        context
            .evaluate_effect(&RunId::new("run"), "McpConnect", &request)
            .decision,
        PermissionDecision::Ask
    );

    context.set_mode(PermissionMode::Plan);
    assert_eq!(
        context
            .evaluate_effect(&RunId::new("run"), "McpConnect", &request)
            .decision,
        PermissionDecision::Deny
    );
}

#[test]
fn configured_process_boundary_freezes_executable_and_argv() {
    let context = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    context.set_boundary(ExecutionBoundary::default());
    let approved = effect_request(
        "ExecCommand",
        EffectClass::Process,
        ResourceFootprint {
            process_commands: vec!["runner".into()],
            process_invocations: vec![ProcessInvocation {
                executable: "runner".into(),
                argv: vec!["--safe".into()],
            }],
            ..Default::default()
        },
    );
    context.allow_configured_effect_for("test:runner", "ExecCommand", &approved.descriptor);
    context.add_rule(PermissionRule {
        capability: Some("ExecCommand".into()),
        action: None,
        effect_class: Some(EffectClass::Process),
        resource_prefixes: Vec::new(),
        decision: PermissionDecision::Allow,
    });
    let changed = effect_request(
        "ExecCommand",
        EffectClass::Process,
        ResourceFootprint {
            process_commands: vec!["runner".into()],
            process_invocations: vec![ProcessInvocation {
                executable: "runner".into(),
                argv: vec!["--unsafe".into()],
            }],
            ..Default::default()
        },
    );

    assert_eq!(
        context
            .evaluate_effect(&RunId::from("run"), "ExecCommand", &approved)
            .decision,
        PermissionDecision::Allow
    );
    assert_eq!(
        context
            .evaluate_effect(&RunId::from("run"), "ExecCommand", &changed)
            .decision,
        PermissionDecision::Ask
    );
}

#[test]
fn switching_modes_preserves_explicit_provider_and_mcp_denies() {
    let context = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    for capability in ["ProviderRequest", "McpConnect"] {
        context.add_rule(PermissionRule {
            capability: Some(capability.into()),
            action: None,
            effect_class: None,
            resource_prefixes: Vec::new(),
            decision: PermissionDecision::Deny,
        });
    }

    context.set_mode(PermissionMode::Plan);
    context.set_mode(PermissionMode::Auto);

    assert_eq!(
        context
            .evaluate_effect(
                &RunId::from("run"),
                "ProviderRequest",
                &effect_request("ProviderRequest", EffectClass::Network, ResourceFootprint::default()),
            )
            .decision,
        PermissionDecision::Deny
    );
    assert_eq!(
        context
            .evaluate_effect(
                &RunId::from("run"),
                "McpConnect",
                &effect_request("McpConnect", EffectClass::Network, ResourceFootprint::default()),
            )
            .decision,
        PermissionDecision::Deny
    );
}

#[test]
fn removing_generated_plugin_rules_preserves_explicit_deny() {
    let context = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    context.add_rule(PermissionRule {
        capability: Some("PluginProvider:x".into()),
        action: None,
        effect_class: Some(EffectClass::Process),
        resource_prefixes: Vec::new(),
        decision: PermissionDecision::Deny,
    });
    context.set_generated_rules(
        "plugin:test",
        vec![PermissionRule {
            capability: Some("PluginProvider:x".into()),
            action: None,
            effect_class: Some(EffectClass::Process),
            resource_prefixes: Vec::new(),
            decision: PermissionDecision::Allow,
        }],
    );
    assert_eq!(
        context
            .evaluate_effect(
                &RunId::from("run"),
                "PluginProvider:x",
                &effect_request("PluginProvider:x", EffectClass::Process, ResourceFootprint::default()),
            )
            .decision,
        PermissionDecision::Deny
    );
    context.remove_generated_rules("plugin:test");
    assert_eq!(
        context
            .evaluate_effect(
                &RunId::from("run"),
                "PluginProvider:x",
                &effect_request("PluginProvider:x", EffectClass::Process, ResourceFootprint::default()),
            )
            .decision,
        PermissionDecision::Deny
    );
}

#[test]
fn allow_rule_must_cover_every_requested_resource() {
    let engine = PermissionEngine::new(PermissionMode::Auto);
    engine.add_rule(PermissionRule {
        capability: Some("Read".into()),
        action: None,
        effect_class: Some(EffectClass::ReadOnly),
        resource_prefixes: vec!["workspace/allowed".into()],
        decision: PermissionDecision::Allow,
    });
    let descriptor = EffectDescriptor {
        class: EffectClass::ReadOnly,
        action: "read".into(),
        resources: ResourceFootprint {
            file_reads: vec!["workspace/allowed/a.txt".into(), "workspace/private/b.txt".into()],
            ..ResourceFootprint::default()
        },
        replay_policy: EffectReplayPolicy::ReplaySafe,
    };

    let evaluation = engine.evaluate("Read", &descriptor);
    assert_eq!(evaluation.matched_rule_index, None);
}

#[test]
fn deny_rule_matches_when_any_requested_resource_is_denied() {
    let engine = PermissionEngine::new(PermissionMode::Auto);
    engine.add_rule(PermissionRule {
        capability: Some("Read".into()),
        action: None,
        effect_class: Some(EffectClass::ReadOnly),
        resource_prefixes: vec!["workspace/private".into()],
        decision: PermissionDecision::Deny,
    });
    let descriptor = EffectDescriptor {
        class: EffectClass::ReadOnly,
        action: "read".into(),
        resources: ResourceFootprint {
            file_reads: vec!["workspace/public/a.txt".into(), "workspace/private/b.txt".into()],
            ..ResourceFootprint::default()
        },
        replay_policy: EffectReplayPolicy::ReplaySafe,
    };

    assert_eq!(engine.evaluate("Read", &descriptor).decision, PermissionDecision::Deny);
}

#[test]
fn resource_rules_do_not_expand_to_sibling_paths_or_lookalike_domains_and_namespaces() {
    let engine = PermissionEngine::new(PermissionMode::Auto);
    engine.add_rule(PermissionRule {
        capability: None,
        action: None,
        effect_class: None,
        resource_prefixes: vec!["workspace/safe".into(), "api.example.com".into(), "mcp:foo".into()],
        decision: PermissionDecision::Allow,
    });

    for resources in [
        ResourceFootprint {
            file_reads: vec!["workspace/safe-escape/secret.txt".into()],
            ..Default::default()
        },
        ResourceFootprint {
            network_domains: vec!["api.example.com.evil".into()],
            ..Default::default()
        },
        ResourceFootprint {
            external_resources: vec!["mcp:foobar".into()],
            ..Default::default()
        },
    ] {
        assert_eq!(
            engine
                .evaluate(
                    "Read",
                    &effect_request("Read", EffectClass::ReadOnly, resources).descriptor
                )
                .matched_rule_index,
            None
        );
    }

    let exact = engine.evaluate(
        "Read",
        &effect_request(
            "Read",
            EffectClass::ReadOnly,
            ResourceFootprint {
                file_reads: vec!["workspace/safe/ok.txt".into()],
                network_domains: vec!["api.example.com".into()],
                external_resources: vec!["mcp:foo:tool".into()],
                ..Default::default()
            },
        )
        .descriptor,
    );
    assert_eq!(exact.decision, PermissionDecision::Allow);

    let subdomain = engine.evaluate(
        "Read",
        &effect_request(
            "Read",
            EffectClass::ReadOnly,
            ResourceFootprint {
                network_domains: vec!["v1.api.example.com".into()],
                ..Default::default()
            },
        )
        .descriptor,
    );
    assert_eq!(subdomain.matched_rule_index, None);
}

#[test]
fn external_boundary_requires_a_namespace_separator() {
    let boundary = ExecutionBoundary {
        external_resource_prefixes: vec!["mcp:foo".into()],
        ..Default::default()
    };
    let descriptor = |resource: &str| EffectDescriptor {
        class: EffectClass::ExternalSideEffect,
        action: "call".into(),
        resources: ResourceFootprint {
            external_resources: vec![resource.into()],
            ..Default::default()
        },
        replay_policy: EffectReplayPolicy::ReconcileRequired,
    };

    assert!(boundary_allows(&boundary, &descriptor("mcp:foo")));
    assert!(boundary_allows(&boundary, &descriptor("mcp:foo:tool")));
    assert!(!boundary_allows(&boundary, &descriptor("mcp:foobar")));
}

#[test]
fn bypass_never_overrides_explicit_deny() {
    let engine = PermissionEngine::new(PermissionMode::Bypass);
    engine.add_rule(PermissionRule {
        capability: Some("Danger".into()),
        action: None,
        effect_class: None,
        resource_prefixes: Vec::new(),
        decision: PermissionDecision::Deny,
    });

    assert_eq!(
        engine
            .evaluate("Danger", &effect(EffectClass::ExternalSideEffect))
            .decision,
        PermissionDecision::Deny
    );
}

#[test]
fn bypass_can_start_process_outside_workspace_boundary() {
    let context = PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted());
    context.set_boundary(ExecutionBoundary::workspace("workspace"));
    let request = effect_request(
        "ExecCommand",
        EffectClass::Process,
        ResourceFootprint {
            process_commands: vec!["cargo test".into()],
            ..ResourceFootprint::default()
        },
    );

    assert_eq!(
        context
            .evaluate_effect(&RunId::new("run-1"), "ExecCommand", &request)
            .decision,
        PermissionDecision::Allow
    );
}

#[test]
fn bypass_can_access_external_resource_outside_workspace_boundary() {
    let context = PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted());
    context.set_boundary(ExecutionBoundary::workspace("workspace"));
    let request = effect_request(
        "UpdateTicket",
        EffectClass::ExternalSideEffect,
        ResourceFootprint {
            external_resources: vec!["ticket:123".into()],
            ..ResourceFootprint::default()
        },
    );

    assert_eq!(
        context
            .evaluate_effect(&RunId::new("run-1"), "UpdateTicket", &request)
            .decision,
        PermissionDecision::Allow
    );
}

#[test]
fn empty_boundary_requires_auto_approval_for_declared_resources() {
    let context = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    context.set_boundary(ExecutionBoundary::default());
    let request = effect_request(
        "Write",
        EffectClass::WorkspaceMutation,
        ResourceFootprint {
            file_writes: vec!["workspace/file.txt".into()],
            ..ResourceFootprint::default()
        },
    );

    assert_eq!(
        context
            .evaluate_effect(&RunId::new("run-1"), "Write", &request)
            .decision,
        PermissionDecision::Ask
    );
}

#[test]
fn workspace_boundary_requires_auto_approval_outside_root_and_allows_inside_root() {
    let directory = tempfile::tempdir().expect("tempdir");
    let workspace = directory.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let context = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    context.set_boundary(ExecutionBoundary::workspace(workspace.to_string_lossy()));
    let outside = effect_request(
        "Write",
        EffectClass::WorkspaceMutation,
        ResourceFootprint {
            file_writes: vec![directory.path().join("other/file.txt").to_string_lossy().into_owned()],
            ..ResourceFootprint::default()
        },
    );
    let inside = effect_request(
        "Write",
        EffectClass::WorkspaceMutation,
        ResourceFootprint {
            file_writes: vec![workspace.join("file.txt").to_string_lossy().into_owned()],
            ..ResourceFootprint::default()
        },
    );

    assert_eq!(
        context
            .evaluate_effect(&RunId::new("run-1"), "Write", &outside)
            .decision,
        PermissionDecision::Ask
    );
    assert_eq!(
        context.evaluate_effect(&RunId::new("run-1"), "Write", &inside).decision,
        PermissionDecision::Allow
    );
}

#[test]
fn auto_can_use_effect_scoped_lease_for_boundary_escalation() {
    let context = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    context.set_boundary(ExecutionBoundary::workspace("workspace"));
    let mut resources = ResourceFootprint {
        process_commands: vec!["cargo test".into()],
        ..ResourceFootprint::default()
    };
    resources.declare_uncontained_process_access();
    let request = effect_request("ExecCommand", EffectClass::Process, resources);
    let run_id = RunId::new("run-1");

    assert_eq!(
        context.evaluate_effect(&run_id, "ExecCommand", &request).decision,
        PermissionDecision::Ask
    );
    context.issue_lease(CapabilityLease {
        lease_id: "test-lease".into(),
        capability: "ExecCommand".into(),
        action: Some("test".into()),
        scope: LeaseScope::Effect {
            effect_id: request.effect_id.clone(),
        },
        grants: AdditionalPermissions {
            process_command_prefix: Some("cargo test".into()),
            unrestricted_file_reads: true,
            unrestricted_file_writes: true,
            unrestricted_network: true,
            unrestricted_process: true,
            ..AdditionalPermissions::default()
        },
        max_uses: Some(1),
        expires_at_unix_ms: None,
    });

    let approved = context.evaluate_effect(&run_id, "ExecCommand", &request);
    assert_eq!(approved.decision, PermissionDecision::Allow);
    assert!(approved.matched_lease);
}

#[test]
fn configured_network_and_process_resources_do_not_widen_other_boundaries() {
    let context = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    context.set_boundary(ExecutionBoundary::workspace("workspace"));
    let allowed_network = effect_request(
        "ProviderRequest",
        EffectClass::Network,
        ResourceFootprint {
            network_domains: vec!["https://provider.example.test/v1".into()],
            ..Default::default()
        },
    );
    let allowed_process = effect_request(
        "McpConnect",
        EffectClass::Process,
        ResourceFootprint {
            process_commands: vec!["node mcp-server.js".into()],
            ..Default::default()
        },
    );
    let base_boundary = context.boundary();
    context.allow_configured_effect_for("runtime-config", "ProviderRequest", &allowed_network.descriptor);
    context.allow_configured_effect_for("runtime-config", "McpConnect", &allowed_process.descriptor);

    assert_eq!(context.boundary(), base_boundary);

    assert_eq!(
        context
            .evaluate_effect(&RunId::new("run"), "ProviderRequest", &allowed_network)
            .decision,
        PermissionDecision::Allow
    );
    assert_eq!(
        context
            .evaluate_effect(&RunId::new("run"), "McpConnect", &allowed_process)
            .decision,
        PermissionDecision::Ask
    );
    let denied = effect_request(
        "ProviderRequest",
        EffectClass::Network,
        ResourceFootprint {
            network_domains: vec!["https://other.example.test".into()],
            ..Default::default()
        },
    );
    assert_eq!(
        context
            .evaluate_effect(&RunId::new("run"), "ProviderRequest", &denied)
            .decision,
        PermissionDecision::Ask
    );
    assert_eq!(
        context
            .evaluate_effect(
                &RunId::new("run"),
                "AutoCompact",
                &effect_request(
                    "AutoCompact",
                    EffectClass::Network,
                    allowed_network.descriptor.resources.clone(),
                ),
            )
            .decision,
        PermissionDecision::Ask
    );
}

#[test]
fn evaluation_rejects_a_capability_argument_that_disagrees_with_the_request() {
    let context = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    let request = effect_request("ProviderRequest", EffectClass::Network, ResourceFootprint::default());

    let evaluation = context.evaluate_effect(&RunId::new("run"), "ExecCommand", &request);

    assert_eq!(evaluation.decision, PermissionDecision::Deny);
    assert!(evaluation.reason.contains("does not match"));
}

#[test]
fn configured_provider_credential_outside_workspace_is_allowed_only_for_provider_request() {
    let directory = tempfile::tempdir().expect("tempdir");
    let workspace = directory.path().join("workspace");
    let credential = directory.path().join("credentials/provider.json");
    std::fs::create_dir_all(&workspace).expect("workspace");
    std::fs::create_dir_all(credential.parent().expect("credential parent")).expect("credential directory");
    std::fs::write(&credential, "{}").expect("credential");
    let credential = credential.canonicalize().expect("canonical credential");
    let context = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    context.set_boundary(ExecutionBoundary::workspace(
        workspace.canonicalize().expect("canonical workspace").to_string_lossy(),
    ));
    let descriptor = EffectDescriptor {
        class: EffectClass::Network,
        action: "request provider".into(),
        resources: ResourceFootprint {
            file_reads: vec![credential.to_string_lossy().into_owned()],
            network_domains: vec!["https://provider.example.test".into()],
            ..Default::default()
        },
        replay_policy: EffectReplayPolicy::Never,
    };
    context.allow_configured_effect_for("config:provider", "ProviderRequest", &descriptor);

    let provider_request = EffectRequest {
        effect_id: EffectId::new("provider-effect"),
        operation_id: OperationId::new("provider-operation"),
        capability: "ProviderRequest".into(),
        descriptor: descriptor.clone(),
        effective_input: serde_json::Value::Null,
        input_digest: None,
    };
    assert_eq!(
        context
            .evaluate_effect(&RunId::new("run"), "ProviderRequest", &provider_request)
            .decision,
        PermissionDecision::Allow
    );

    let auto_compact = EffectRequest {
        effect_id: EffectId::new("compact-effect"),
        operation_id: OperationId::new("compact-operation"),
        capability: "AutoCompact".into(),
        descriptor,
        effective_input: serde_json::Value::Null,
        input_digest: None,
    };
    assert_eq!(
        context
            .evaluate_effect(&RunId::new("run"), "AutoCompact", &auto_compact)
            .decision,
        PermissionDecision::Ask
    );
}

#[test]
fn configured_effect_resources_do_not_authorize_another_capability() {
    let context = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    context.set_boundary(ExecutionBoundary::workspace("workspace"));
    let mut resources = ResourceFootprint {
        process_commands: vec!["plugin-command".into()],
        ..ResourceFootprint::default()
    };
    resources.declare_uncontained_process_access();
    let plugin_request = effect_request("PluginTool:test", EffectClass::Process, resources);
    context.allow_configured_effect_for("plugin:test", "PluginTool:test", &plugin_request.descriptor);
    context.set_generated_rules(
        "plugin:test",
        vec![PermissionRule {
            capability: Some("PluginTool:test".into()),
            action: None,
            effect_class: Some(EffectClass::Process),
            resource_prefixes: Vec::new(),
            decision: PermissionDecision::Allow,
        }],
    );

    assert_eq!(
        context
            .evaluate_effect(&RunId::new("run"), "PluginTool:test", &plugin_request)
            .decision,
        PermissionDecision::Allow
    );
    assert_eq!(
        context
            .evaluate_effect(&RunId::new("run"), "ExecCommand", &plugin_request)
            .decision,
        PermissionDecision::Deny
    );

    for (capability, request) in [
        (
            "Network",
            effect_request(
                "Network",
                EffectClass::Network,
                ResourceFootprint {
                    network_domains: vec!["https://plugin.example.test".into()],
                    ..Default::default()
                },
            ),
        ),
        (
            "Read",
            effect_request(
                "Read",
                EffectClass::ReadOnly,
                ResourceFootprint {
                    file_reads: vec!["outside/secret.txt".into()],
                    ..Default::default()
                },
            ),
        ),
        (
            "Write",
            effect_request(
                "Write",
                EffectClass::WorkspaceMutation,
                ResourceFootprint {
                    file_writes: vec!["outside/changed.txt".into()],
                    ..Default::default()
                },
            ),
        ),
    ] {
        assert_eq!(
            context
                .evaluate_effect(&RunId::new("run"), capability, &request)
                .decision,
            PermissionDecision::Ask,
            "configured plugin process resources must not authorize {capability}"
        );
    }
}

#[test]
fn workspace_boundary_requires_approval_for_parent_traversal_and_nonexistent_outside_targets() {
    let directory = tempfile::tempdir().expect("tempdir");
    let workspace = directory.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let context = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    context.set_boundary(ExecutionBoundary::workspace(workspace.to_string_lossy()));
    let escaped = workspace.join("..").join("outside.txt");
    let request = effect_request(
        "Write",
        EffectClass::WorkspaceMutation,
        ResourceFootprint {
            file_writes: vec![escaped.to_string_lossy().into_owned()],
            ..Default::default()
        },
    );

    assert_eq!(
        context.evaluate_effect(&RunId::new("run"), "Write", &request).decision,
        PermissionDecision::Ask
    );
}

#[cfg(unix)]
#[test]
fn workspace_boundary_requires_approval_for_symlink_escape() {
    use std::os::unix::fs::symlink;

    let directory = tempfile::tempdir().expect("tempdir");
    let workspace = directory.path().join("workspace");
    let outside = directory.path().join("outside");
    std::fs::create_dir(&workspace).expect("workspace");
    std::fs::create_dir(&outside).expect("outside");
    symlink(&outside, workspace.join("link")).expect("symlink");
    let context = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    context.set_boundary(ExecutionBoundary::workspace(workspace.to_string_lossy()));
    let request = effect_request(
        "Write",
        EffectClass::WorkspaceMutation,
        ResourceFootprint {
            file_writes: vec![workspace.join("link/file.txt").to_string_lossy().into_owned()],
            ..Default::default()
        },
    );

    assert_eq!(
        context.evaluate_effect(&RunId::new("run"), "Write", &request).decision,
        PermissionDecision::Ask
    );
}

#[cfg(windows)]
#[test]
fn workspace_boundary_requires_approval_for_junction_escape() {
    let directory = tempfile::tempdir().expect("tempdir");
    let workspace = directory.path().join("workspace");
    let outside = directory.path().join("outside");
    std::fs::create_dir(&workspace).expect("workspace");
    std::fs::create_dir(&outside).expect("outside");
    create_windows_junction(&outside, &workspace.join("link"));
    let context = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    context.set_boundary(ExecutionBoundary::workspace(workspace.to_string_lossy()));
    let request = effect_request(
        "Write",
        EffectClass::WorkspaceMutation,
        ResourceFootprint {
            file_writes: vec![workspace.join("link/file.txt").to_string_lossy().into_owned()],
            ..Default::default()
        },
    );

    assert_eq!(
        context.evaluate_effect(&RunId::new("run"), "Write", &request).decision,
        PermissionDecision::Ask
    );
}

#[test]
fn configured_process_boundary_requires_an_exact_safe_command() {
    let context = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    context.set_boundary(ExecutionBoundary::workspace("workspace"));
    let capability = "mcp-connect:v1:4:test";
    let configured = effect_request(
        capability,
        EffectClass::Process,
        ResourceFootprint {
            process_commands: vec!["node mcp-server.js".into()],
            ..Default::default()
        },
    );
    context.allow_configured_effect_for("test:mcp", capability, &configured.descriptor);
    context.set_generated_rules(
        "test:mcp",
        vec![PermissionRule {
            capability: Some(capability.to_owned()),
            action: None,
            effect_class: Some(EffectClass::Process),
            resource_prefixes: Vec::new(),
            decision: PermissionDecision::Allow,
        }],
    );
    assert_eq!(
        context
            .evaluate_effect(&RunId::new("run"), capability, &configured)
            .decision,
        PermissionDecision::Allow
    );
    for command in [
        "node mcp-server.js && whoami",
        "node mcp-server.js > output.txt",
        "node mcp-server.js-extra",
    ] {
        let request = effect_request(
            capability,
            EffectClass::Process,
            ResourceFootprint {
                process_commands: vec![command.into()],
                ..Default::default()
            },
        );
        assert_eq!(
            context
                .evaluate_effect(&RunId::new("run"), capability, &request)
                .decision,
            PermissionDecision::Ask,
            "{command}"
        );
    }
}

#[test]
fn narrowed_boundary_cannot_be_expanded_by_inherited_configured_effects() {
    let directory = tempfile::tempdir().unwrap();
    let parent_root = directory.path().join("parent");
    let child_root = parent_root.join("child");
    let outside_path = parent_root.join("outside.txt");
    std::fs::create_dir_all(&child_root).unwrap();
    let parent = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    parent.set_boundary(ExecutionBoundary::workspace(parent_root.to_string_lossy()));
    let outside = effect_request(
        "Write",
        EffectClass::WorkspaceMutation,
        ResourceFootprint {
            file_writes: vec![outside_path.to_string_lossy().into_owned()],
            ..Default::default()
        },
    );
    parent.allow_configured_effect_for("test:write", "Write", &outside.descriptor);
    assert!(parent.current_boundary_allows_request(&outside));

    let child = parent.narrowed_with_boundary(
        PermissionCeiling::unrestricted(),
        ExecutionBoundary::workspace(child_root.to_string_lossy()),
    );
    assert!(!child.current_boundary_allows_request(&outside));
    parent.issue_lease(CapabilityLease {
        lease_id: "outside-write".into(),
        capability: "Write".into(),
        action: Some("test".into()),
        scope: LeaseScope::Effect {
            effect_id: outside.effect_id.clone(),
        },
        grants: AdditionalPermissions {
            file_writes: vec![outside_path.to_string_lossy().into_owned()],
            ..Default::default()
        },
        max_uses: Some(1),
        expires_at_unix_ms: None,
    });
    assert_eq!(
        child
            .evaluate_effect(&RunId::from("child-run"), "Write", &outside)
            .decision,
        PermissionDecision::Deny
    );

    child.set_boundary(ExecutionBoundary::unrestricted());
    assert!(!child.current_boundary_allows_request(&outside));
}

#[path = "permission_engine_protected_test.rs"]
mod permission_engine_protected_test;
