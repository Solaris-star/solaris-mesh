struct StableCapabilityTool {
    display_name: String,
    capability: String,
    implementation: solaris_types::plugin::ImplementationIdentity,
}

#[async_trait::async_trait]
impl solaris_tools::Tool for StableCapabilityTool {
    fn name(&self) -> &str {
        &self.display_name
    }

    fn permission_capability(&self) -> &str {
        &self.capability
    }

    fn description(&self) -> &str {
        "stable capability test tool"
    }

    fn input_schema(&self) -> solaris_types::tool::JsonSchema {
        json!({"type": "object"})
    }

    fn is_concurrency_safe(&self, _input: &serde_json::Value) -> bool {
        false
    }

    async fn execute(&self, _input: serde_json::Value) -> solaris_types::tool::ToolResult {
        solaris_types::tool::ToolResult {
            content: "executed".into(),
            is_error: false,
        }
    }

    fn describe_effect(&self, _input: &serde_json::Value) -> EffectDescriptor {
        EffectDescriptor {
            class: EffectClass::ExternalSideEffect,
            action: "call stable MCP tool".into(),
            resources: ResourceFootprint {
                external_resources: vec!["mcp:test-server:test-tool".into()],
                ..Default::default()
            },
            replay_policy: EffectReplayPolicy::ReconcileRequired,
        }
    }

    fn implementation_identity(&self) -> Option<solaris_types::plugin::ImplementationIdentity> {
        Some(self.implementation.clone())
    }

    fn category(&self) -> ToolCategory {
        ToolCategory::Mcp
    }
}

fn stable_capability_fixture(
    display_name: &str,
    run_label: &str,
) -> (
    ToolRegistry,
    EffectExecutionContext,
    Arc<InMemoryRuntimeLedger>,
    ContentBlock,
) {
    let capability = "mcp:11:test-server:9:test-tool".to_owned();
    let implementation = solaris_types::plugin::ImplementationIdentity {
        implementation_id: "mcp:test-server:test-tool".into(),
        version: Some("test".into()),
        digest: Some("sha256:test".into()),
    };
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(StableCapabilityTool {
        display_name: display_name.into(),
        capability: capability.clone(),
        implementation: implementation.clone(),
    }));
    let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    permissions.set_boundary(ExecutionBoundary::unrestricted());
    permissions.add_rule(PermissionRule {
        capability: Some(capability),
        action: None,
        effect_class: Some(EffectClass::ExternalSideEffect),
        resource_prefixes: Vec::new(),
        decision: PermissionDecision::Allow,
    });
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let context = EffectExecutionContext::new(
        RunId::from(run_label),
        AgentId::from(format!("{run_label}-agent")),
        ledger.clone(),
        permissions,
        OperationEnvironmentSnapshot {
            tools: vec![solaris_types::runtime::ToolImplementationSnapshot {
                name: display_name.into(),
                implementation,
                schema_digest: None,
                replay_policy: EffectReplayPolicy::ReconcileRequired,
            }],
            ..Default::default()
        },
    );
    let call = ContentBlock::ToolUse {
        id: format!("{run_label}-call"),
        name: display_name.into(),
        input: json!({}),
        extra: None,
    };
    (registry, context, ledger, call)
}

#[tokio::test]
async fn stable_capability_survives_root_child_and_fork_display_name_changes() {
    for (run_label, display_name) in [
        ("root-capability", "test-tool"),
        ("child-capability", "mcp__test-server_test-tool"),
        ("fork-capability", "mcp__test-server_test-tool_2"),
    ] {
        let (registry, context, ledger, call) = stable_capability_fixture(display_name, run_label);
        let mut confirmer = ToolConfirmer::new(false, Vec::new());
        confirmer.set_interactive(false);
        let outcome = execute_tool_calls_with_policy_context(
            &registry,
            &[call],
            &Arc::new(Mutex::new(confirmer)),
            PermissionMode::Auto,
            PermissionCeiling::unrestricted(),
            &context,
            None,
            solaris_compact::CompactLevel::Off,
            false,
        )
        .await
        .unwrap();

        assert!(matches!(
            outcome.results.as_slice(),
            [ContentBlock::ToolResult { is_error: false, content, .. }] if content == "executed"
        ));
        let records = ledger.records_for_run(context.run_id()).unwrap();
        let decision = records
            .iter()
            .find(|record| record.record_type == "permission_decision")
            .unwrap();
        assert_eq!(decision.payload["capability"], "mcp:11:test-server:9:test-tool");
        assert_eq!(decision.payload["decision"], "allow");
    }
}

#[tokio::test]
async fn approval_lease_uses_stable_capability_instead_of_display_name() {
    let (registry, context, ledger, call) = stable_capability_fixture("renamed-tool", "stable-lease");
    context.permissions().replace_rules(Vec::new());
    let outcome = execute_tool_calls_with_policy_context(
        &registry,
        &[call],
        &Arc::new(Mutex::new(ToolConfirmer::new(true, Vec::new()))),
        PermissionMode::Auto,
        PermissionCeiling::unrestricted(),
        &context,
        None,
        solaris_compact::CompactLevel::Off,
        false,
    )
    .await
    .unwrap();

    assert!(matches!(
        outcome.results.as_slice(),
        [ContentBlock::ToolResult { is_error: false, .. }]
    ));
    let records = ledger.records_for_run(context.run_id()).unwrap();
    let lease = records
        .iter()
        .find(|record| record.record_type == "capability_lease_issued")
        .unwrap();
    assert_eq!(lease.payload["capability"], "mcp:11:test-server:9:test-tool");
}
