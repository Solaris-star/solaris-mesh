#[test]
fn scoped_provider_rejection_releases_run_reservation() {
    let run_resources = ResourceManager::new(solaris_types::resource::ResourceBudget::default());
    run_resources.set_provider_signals(solaris_types::provider_contract::ProviderSignals {
        requests_per_minute: Some(1),
        ..Default::default()
    });
    let scoped_resources = ResourceManager::new(solaris_types::resource::ResourceBudget::default());
    scoped_resources.set_provider_signals(solaris_types::provider_contract::ProviderSignals {
        requests_per_minute: Some(0),
        ..Default::default()
    });
    let context = EffectExecutionContext::new(
        RunId::from("provider-scope-run"),
        AgentId::from("provider-scope-agent"),
        Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default()),
        PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    )
    .with_resource_manager(Arc::clone(&run_resources))
    .with_scoped_resource_manager(scoped_resources);

    assert!(context.acquire_provider_request(1).is_err());
    assert!(run_resources.acquire_provider_request(1).is_ok());
}

#[test]
fn completed_model_usage_that_crosses_budget_is_recorded_and_blocks_the_next_operation() {
    let resources = ResourceManager::new(solaris_types::resource::ResourceBudget {
        max_tokens: Some(10),
        ..Default::default()
    });
    let context = EffectExecutionContext::new(
        RunId::from("usage-crossing-run"),
        AgentId::from("usage-crossing-agent"),
        Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default()),
        PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    )
    .with_resource_manager(Arc::clone(&resources));
    let effect_id = EffectId::from("provider-request-crossing-budget");

    context
        .record_model_usage_once(
            &effect_id,
            &solaris_types::message::TokenUsage {
                input_tokens: 8,
                output_tokens: 3,
                ..Default::default()
            },
            true,
        )
        .expect("a completed provider response must not be rejected retroactively");

    assert_eq!(resources.usage().tokens, 11);
    assert_eq!(resources.usage().turns, 1);
    let error = context
        .ensure_runtime_budget_available()
        .expect_err("the next operation must be blocked after the budget is crossed");
    assert!(error.contains("token budget exhausted"));
}

#[test]
fn stable_effect_attempt_reuses_pending_identity_and_advances_after_outcome() {
    let context = EffectExecutionContext::new(
        RunId::from("attempt-run"),
        AgentId::from("attempt-agent"),
        Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default()),
        PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let first_id = context.stable_effect_attempt_id("mcp-connect:test:digest").unwrap();
    assert_eq!(first_id, "mcp-connect:test:digest:attempt:1");
    let request = context.effect_request(
        &first_id,
        "McpConnect",
        &json!({}),
        EffectDescriptor {
            class: solaris_types::effect::EffectClass::Process,
            action: "connect".into(),
            resources: ResourceFootprint::default(),
            replay_policy: solaris_types::effect::EffectReplayPolicy::ReconcileRequired,
        },
    );
    context.record_effect_intent(&request).unwrap();
    assert_eq!(
        context.stable_effect_attempt_id("mcp-connect:test:digest").unwrap(),
        first_id
    );
    assert!(matches!(
        context.recover_effect(&request).unwrap(),
        EffectRecoveryDecision::Reconcile { .. }
    ));
    context.record_effect_outcome(&request, false, "connected").unwrap();
    assert_eq!(
        context.stable_effect_attempt_id("mcp-connect:test:digest").unwrap(),
        "mcp-connect:test:digest:attempt:2"
    );
}

#[test]
fn tool_intent_records_the_prepared_implementation_identity() {
    let run_id = RunId::from("prepared-tool-intent-run");
    let ledger = Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default());
    let context = EffectExecutionContext::new(
        run_id.clone(),
        AgentId::from("prepared-tool-agent"),
        ledger.clone(),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let request = context.effect_request(
        "prepared-tool-call",
        "PreparedPlugin",
        &json!({}),
        EffectDescriptor {
            class: solaris_types::effect::EffectClass::Process,
            action: "execute prepared plugin".into(),
            resources: ResourceFootprint::default(),
            replay_policy: EffectReplayPolicy::Never,
        },
    );
    let implementation = ImplementationIdentity {
        implementation_id: "plugin-executable:prepared".into(),
        version: None,
        digest: Some("prepared-digest".into()),
    };

    context
        .record_effect_intent_with_tool_implementation(&request, &implementation)
        .unwrap();

    let intent = ledger.records_for_run(&run_id).unwrap().pop().unwrap();
    let environment: OperationEnvironmentSnapshot =
        serde_json::from_value(intent.payload["environment"].clone()).unwrap();
    assert_eq!(environment.tools.len(), 1);
    assert_eq!(
        environment.tools[0].name,
        format!("{PREPARED_IMPLEMENTATION_SNAPSHOT_PREFIX}PreparedPlugin")
    );
    assert_eq!(environment.tools[0].implementation, implementation);
    assert!(context.environment().tools.is_empty());
}

#[test]
fn failed_tool_intent_append_does_not_change_shared_environment() {
    let initial = OperationEnvironmentSnapshot {
        permission_fingerprint: Some("unchanged".into()),
        ..Default::default()
    };
    let context = EffectExecutionContext::new(
        RunId::from("failed-tool-intent-run"),
        AgentId::from("failed-tool-intent-agent"),
        Arc::new(RejectingRuntimeLedger),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        initial.clone(),
    );
    let request = context.effect_request(
        "failed-tool-call",
        "PreparedPlugin",
        &json!({}),
        EffectDescriptor {
            class: solaris_types::effect::EffectClass::Process,
            action: "execute prepared plugin".into(),
            resources: ResourceFootprint::default(),
            replay_policy: EffectReplayPolicy::Never,
        },
    );
    let implementation = ImplementationIdentity {
        implementation_id: "plugin-executable:prepared".into(),
        version: None,
        digest: Some("prepared-digest".into()),
    };

    assert!(
        context
            .record_effect_intent_with_tool_implementation(&request, &implementation)
            .is_err()
    );
    assert_eq!(context.environment(), initial);
}

#[test]
fn compatibility_requires_reconcile_for_same_provider_with_changed_environment() {
    let provider = ImplementationIdentity {
        implementation_id: "provider:openai:openai-responses".into(),
        version: None,
        digest: Some("one".into()),
    };
    let recorded = OperationEnvironmentSnapshot {
        provider: Some(provider.clone()),
        permission_fingerprint: Some("a".into()),
        ..Default::default()
    };
    let current = OperationEnvironmentSnapshot {
        provider: Some(provider),
        permission_fingerprint: Some("b".into()),
        ..Default::default()
    };
    assert_eq!(
        compatibility_decision(&recorded, &current),
        CompatibilityDecision::ReconcileRequired
    );
}

#[test]
fn compatibility_rejects_provider_implementation_change() {
    let recorded = OperationEnvironmentSnapshot {
        provider: Some(ImplementationIdentity {
            implementation_id: "provider:a".into(),
            version: None,
            digest: None,
        }),
        ..Default::default()
    };
    let current = OperationEnvironmentSnapshot {
        provider: Some(ImplementationIdentity {
            implementation_id: "provider:b".into(),
            version: None,
            digest: None,
        }),
        ..Default::default()
    };
    assert_eq!(
        compatibility_decision(&recorded, &current),
        CompatibilityDecision::Incompatible
    );
}

#[test]
fn plugin_environment_order_is_stable_and_conflicting_identities_fail_closed() {
    let first = ImplementationIdentity {
        implementation_id: "plugin:a".into(),
        version: Some("1".into()),
        digest: Some("digest-a".into()),
    };
    let second = ImplementationIdentity {
        implementation_id: "plugin:b".into(),
        version: Some("1".into()),
        digest: Some("digest-b".into()),
    };
    assert_eq!(
        normalize_plugin_identities(vec![second.clone(), first.clone(), first.clone()]),
        vec![first.clone(), second.clone()]
    );
    let context = EffectExecutionContext::new(
        RunId::from("plugin-order-run"),
        AgentId::from("agent"),
        Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default()),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot {
            plugins: vec![second.clone(), first.clone(), first.clone()],
            ..Default::default()
        },
    );
    assert_eq!(context.environment().plugins, vec![first.clone(), second]);

    let conflict = ImplementationIdentity {
        implementation_id: first.implementation_id.clone(),
        version: Some("2".into()),
        digest: Some("digest-conflict".into()),
    };
    let plugins = normalize_plugin_identities(vec![conflict, first]);
    assert_eq!(plugins.len(), 2);
    let environment = OperationEnvironmentSnapshot {
        plugins,
        ..Default::default()
    };
    let error = validate_environment_plugin_identities(&environment).unwrap_err();
    assert!(error.contains("conflicting plugin implementation identity sha256:"));
    assert!(!error.contains("plugin:a"));
}

#[test]
fn once_approval_creates_effect_scoped_single_use_lease() {
    let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    let context = EffectExecutionContext::new(
        RunId::from("run"),
        AgentId::from("agent"),
        Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default()),
        permissions,
        OperationEnvironmentSnapshot::default(),
    );
    let request = context.effect_request(
        "call",
        "ExecCommand",
        &json!({"cmd":"cargo test"}),
        EffectDescriptor {
            class: solaris_types::effect::EffectClass::Process,
            action: "Execute cargo test".into(),
            resources: ResourceFootprint {
                process_commands: vec!["cargo test".into()],
                ..Default::default()
            },
            replay_policy: solaris_types::effect::EffectReplayPolicy::ReconcileRequired,
        },
    );
    let lease = context.issue_approval_lease(&request, false).unwrap();
    assert!(matches!(lease.scope, LeaseScope::Effect { .. }));
    assert_eq!(lease.max_uses, Some(1));
}

#[test]
fn durable_leases_project_write_edit_network_and_plugin_process_secrets() {
    let secret = "lease-super-secret-token";
    let cases = [
        (
            "Write",
            EffectDescriptor {
                class: solaris_types::effect::EffectClass::WorkspaceMutation,
                action: format!("write {secret}"),
                resources: ResourceFootprint {
                    file_writes: vec![format!("workspace/{secret}.txt")],
                    ..Default::default()
                },
                replay_policy: solaris_types::effect::EffectReplayPolicy::ReconcileRequired,
            },
        ),
        (
            "Edit",
            EffectDescriptor {
                class: solaris_types::effect::EffectClass::WorkspaceMutation,
                action: format!("edit {secret}"),
                resources: ResourceFootprint {
                    file_reads: vec![format!("workspace/{secret}.txt")],
                    file_writes: vec![format!("workspace/{secret}.txt")],
                    ..Default::default()
                },
                replay_policy: solaris_types::effect::EffectReplayPolicy::ReconcileRequired,
            },
        ),
        (
            "NetworkRequest",
            EffectDescriptor {
                class: solaris_types::effect::EffectClass::Network,
                action: format!("connect with {secret}"),
                resources: ResourceFootprint {
                    network_domains: vec![format!("{secret}.example.test")],
                    ..Default::default()
                },
                replay_policy: solaris_types::effect::EffectReplayPolicy::Never,
            },
        ),
        (
            "PluginTool:secret",
            EffectDescriptor {
                class: solaris_types::effect::EffectClass::Process,
                action: format!("invoke plugin {secret}"),
                resources: ResourceFootprint {
                    process_commands: vec![format!("plugin --token {secret}")],
                    process_invocations: vec![ProcessInvocation {
                        executable: format!("plugins/{secret}/runner"),
                        argv: vec!["--token".into(), secret.into()],
                    }],
                    external_resources: vec![format!("plugin:{secret}")],
                    ..Default::default()
                },
                replay_policy: solaris_types::effect::EffectReplayPolicy::Never,
            },
        ),
    ];

    for (index, (capability, descriptor)) in cases.into_iter().enumerate() {
        let run_id = RunId::new(format!("secret-lease-run-{index}"));
        let ledger: Arc<dyn RuntimeLedger> = Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default());
        let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
        permissions.set_boundary(ExecutionBoundary::default());
        let context = EffectExecutionContext::new(
            run_id.clone(),
            AgentId::from("agent"),
            Arc::clone(&ledger),
            permissions,
            OperationEnvironmentSnapshot::default(),
        );
        let request = context.effect_request(
            &format!("secret-lease-{index}"),
            capability,
            &json!({"user_input": secret}),
            descriptor.clone(),
        );

        context.issue_approval_lease(&request, true).unwrap();

        let records = ledger.records_for_run(&run_id).unwrap();
        let issued = records
            .iter()
            .find(|record| record.record_type == "capability_lease_issued")
            .unwrap();
        let durable = serde_json::to_string(&issued.payload).unwrap();
        let host = serde_json::to_string(&secret_safe_ledger_payload(issued.payload.clone())).unwrap();
        assert!(!durable.contains(secret), "durable {capability} lease leaked");
        assert!(!host.contains(secret), "Host {capability} lease leaked");
        assert!(durable.contains("sha256:"));
        assert_eq!(issued.payload["restorable"], false);
        assert_eq!(issued.payload["source_decision"], "host_approval");
        assert!(issued.payload.get("grants").is_none());
        assert!(issued.payload.get("action").is_none());
        assert_eq!(
            issued.payload["effect"]["descriptor_digest"],
            EffectAuditProjection::from_descriptor(&request.descriptor).descriptor_digest
        );

        let restored_permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
        restored_permissions.set_boundary(ExecutionBoundary::default());
        let restored = EffectExecutionContext::new(
            run_id,
            AgentId::from("agent"),
            ledger,
            restored_permissions,
            OperationEnvironmentSnapshot::default(),
        );
        assert_ne!(restored.evaluate(&request).decision, PermissionDecision::Allow);
    }
}

#[test]
fn redacted_leases_require_reapproval_and_once_consumption_survives_restart() {
    let run_id = RunId::from("lease-run");
    let ledger: Arc<dyn RuntimeLedger> = Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default());
    let restricted_permissions = || {
        let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
        permissions.set_boundary(ExecutionBoundary::default());
        permissions
    };
    let request_for = |context: &EffectExecutionContext, call: &str| {
        context.effect_request(
            call,
            "Write",
            &json!({"file_path":"workspace/file.txt"}),
            EffectDescriptor {
                class: solaris_types::effect::EffectClass::WorkspaceMutation,
                action: "Write workspace file".into(),
                resources: ResourceFootprint {
                    file_writes: vec!["workspace/file.txt".into()],
                    ..Default::default()
                },
                replay_policy: solaris_types::effect::EffectReplayPolicy::ReconcileRequired,
            },
        )
    };

    let permissions = restricted_permissions();
    let first = EffectExecutionContext::new(
        run_id.clone(),
        AgentId::from("agent"),
        Arc::clone(&ledger),
        permissions,
        OperationEnvironmentSnapshot::default(),
    );
    let always_request = request_for(&first, "always");
    let always_lease = first.issue_approval_lease(&always_request, true).unwrap();
    assert!(matches!(always_lease.scope, LeaseScope::Run { .. }));

    let restored_permissions = restricted_permissions();
    let restored = EffectExecutionContext::new(
        run_id.clone(),
        AgentId::from("agent"),
        Arc::clone(&ledger),
        restored_permissions,
        OperationEnvironmentSnapshot::default(),
    );
    assert_ne!(
        restored.evaluate(&request_for(&restored, "after-restart")).decision,
        PermissionDecision::Allow
    );
    let issued = ledger
        .records_for_run(&run_id)
        .unwrap()
        .into_iter()
        .find(|record| record.record_type == "capability_lease_issued")
        .unwrap();
    assert_eq!(issued.payload["restorable"], false);

    let once_run = RunId::from("once-lease-run");
    let once_ledger: Arc<dyn RuntimeLedger> = Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default());
    let once = EffectExecutionContext::new(
        once_run.clone(),
        AgentId::from("agent"),
        Arc::clone(&once_ledger),
        restricted_permissions(),
        OperationEnvironmentSnapshot::default(),
    );
    let once_request = request_for(&once, "once");
    once.issue_approval_lease(&once_request, false).unwrap();
    assert!(
        once.revalidate_environment(&once_request, &OperationEnvironmentSnapshot::default())
            .is_ok()
    );

    let after_consumption = EffectExecutionContext::new(
        once_run,
        AgentId::from("agent"),
        Arc::clone(&once_ledger),
        restricted_permissions(),
        OperationEnvironmentSnapshot::default(),
    );
    assert_ne!(
        after_consumption.evaluate(&once_request).decision,
        PermissionDecision::Allow
    );
    assert!(
        once_ledger
            .records_for_run(&RunId::from("once-lease-run"))
            .unwrap()
            .iter()
            .any(|record| record.record_type == "capability_lease_consumed")
    );
}

#[test]
fn revalidation_keeps_original_operation_ceiling() {
    let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    let context = EffectExecutionContext::new(
        RunId::from("run"),
        AgentId::from("agent"),
        Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default()),
        permissions,
        OperationEnvironmentSnapshot::default(),
    );
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(WriteTool::new(None)));
    let input = json!({"file_path":"workspace/file.txt", "content":"test"});
    let descriptor = registry.get("Write").unwrap().describe_effect(&input);
    let request = context.effect_request("call", "Write", &input, descriptor);
    context.remember_approved_request(request, PermissionMode::Auto, PermissionCeiling::plan());
    let approved = context.take_approved_request_for_call("call").unwrap();

    let error = context.revalidate(&registry, &approved).unwrap_err();
    assert!(error.contains("permission no longer allows effect"));
}

#[test]
fn approved_request_is_consumed_atomically_once() {
    let context = EffectExecutionContext::new(
        RunId::from("one-shot-approved-run"),
        AgentId::from("agent"),
        Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default()),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let input = json!({});
    let request = context.effect_request(
        "call",
        "Read",
        &input,
        EffectDescriptor {
            class: solaris_types::effect::EffectClass::ReadOnly,
            action: "read".into(),
            resources: ResourceFootprint::default(),
            replay_policy: EffectReplayPolicy::ReplaySafe,
        },
    );
    context.remember_approved_request(request, PermissionMode::Bypass, PermissionCeiling::unrestricted());

    assert!(context.take_approved_request_for_call("call").is_some());
    assert!(context.take_approved_request_for_call("call").is_none());
}

#[test]
fn revalidation_observes_a_runtime_mode_reduction() {
    let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    permissions.set_boundary(ExecutionBoundary::unrestricted());
    let context = EffectExecutionContext::new(
        RunId::from("run-mode-reduction"),
        AgentId::from("agent"),
        Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default()),
        permissions.clone(),
        OperationEnvironmentSnapshot::default(),
    );
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(WriteTool::new(None)));
    let input = json!({"file_path":"workspace/file.txt", "content":"test"});
    let descriptor = registry.get("Write").unwrap().describe_effect(&input);
    let request = context.effect_request("call", "Write", &input, descriptor);
    context.remember_approved_request(request, PermissionMode::Auto, PermissionCeiling::unrestricted());
    let approved = context.take_approved_request_for_call("call").unwrap();

    permissions.set_mode(PermissionMode::Plan);
    let error = context.revalidate(&registry, &approved).unwrap_err();
    assert!(error.contains("permission no longer allows effect"));
}

#[test]
fn revalidation_rejects_tool_or_plugin_replacement_after_approval() {
    let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    let approved_tool = ToolImplementationSnapshot {
        name: "Write".into(),
        implementation: ImplementationIdentity {
            implementation_id: "tool:Write".into(),
            version: Some(env!("CARGO_PKG_VERSION").into()),
            digest: Some(stable_digest_value(&WriteTool::new(None).input_schema())),
        },
        schema_digest: Some(stable_digest_value(&WriteTool::new(None).input_schema())),
        replay_policy: solaris_types::effect::EffectReplayPolicy::ReconcileRequired,
    };
    let approved_plugin = ImplementationIdentity {
        implementation_id: "plugin:test".into(),
        version: Some("1".into()),
        digest: Some("plugin-a".into()),
    };
    let context = EffectExecutionContext::new(
        RunId::from("run"),
        AgentId::from("agent"),
        Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default()),
        permissions,
        OperationEnvironmentSnapshot {
            tools: vec![approved_tool.clone()],
            plugins: vec![approved_plugin.clone()],
            ..Default::default()
        },
    );
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(WriteTool::new(None)));
    let input = json!({"file_path":"workspace/file.txt", "content":"test"});
    let descriptor = registry.get("Write").unwrap().describe_effect(&input);
    let request = context.effect_request("call", "Write", &input, descriptor);
    context.remember_approved_request(request, PermissionMode::Auto, PermissionCeiling::unrestricted());
    let approved = context.take_approved_request_for_call("call").unwrap();

    let mut stale_tool_approval = (*approved).clone();
    stale_tool_approval.environment.tools[0].implementation.digest = Some("schema-b".into());
    assert!(
        context
            .revalidate(&registry, &stale_tool_approval)
            .unwrap_err()
            .contains("tool implementation changed")
    );

    let mut changed_plugin = approved.environment.clone();
    changed_plugin.tools = vec![approved.environment.tools[0].clone()];
    changed_plugin.plugins[0].digest = Some("plugin-b".into());
    context.set_environment(changed_plugin);
    assert!(
        context
            .revalidate(&registry, &approved)
            .unwrap_err()
            .contains("plugin implementation set changed")
    );
}

#[test]
fn dropped_effect_guard_records_cancelled_outcome() {
    let ledger = Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default());
    let run_id = RunId::from("cancelled-effect-run");
    let context = EffectExecutionContext::new(
        run_id.clone(),
        AgentId::from("agent"),
        ledger.clone(),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let request = context.effect_request(
        "cancelled-call",
        "ExternalCommand",
        &json!({}),
        EffectDescriptor {
            class: solaris_types::effect::EffectClass::Process,
            action: "run external command".into(),
            resources: ResourceFootprint::default(),
            replay_policy: solaris_types::effect::EffectReplayPolicy::Never,
        },
    );
    context.record_effect_intent(&request).unwrap();

    drop(EffectOutcomeGuard::new(context, request, "execution future cancelled"));

    let records = ledger.records_for_run(&run_id).unwrap();
    assert_eq!(records[0].record_type, "effect_intent");
    assert_eq!(records[1].record_type, "effect_outcome");
    assert_eq!(records[1].payload["is_error"], true);
    assert_eq!(records[1].payload["status"], "outcome_unknown");
}

#[test]
fn dropped_effect_guard_logs_safe_identities_when_terminal_outcome_fails() {
    let sentinel = "super-secret-token-terminal-outcome";
    let context = EffectExecutionContext::new(
        RunId::from(format!("run-{sentinel}")),
        AgentId::from("agent"),
        Arc::new(RejectingRuntimeLedger),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let request = context.effect_request(
        &format!("call-{sentinel}"),
        "ExternalCommand",
        &json!({}),
        EffectDescriptor::read_only("cancelled effect"),
    );
    let buffer = TestLogBuffer::default();
    let subscriber = TestSubscriber(buffer.clone());

    tracing::subscriber::with_default(subscriber, || {
        drop(EffectOutcomeGuard::new(
            context,
            request,
            format!("cancelled because {sentinel}"),
        ));
    });
    let logs = buffer.0.lock().unwrap().clone();

    assert!(logs.contains("terminal effect outcome persistence failed"), "{logs}");
    assert!(logs.contains("run_identity=sha256:"), "{logs}");
    assert!(logs.contains("effect_identity=sha256:"), "{logs}");
    assert!(!logs.contains(sentinel));
    assert!(!logs.contains("super-secret-token-ledger-error"));
}
