struct ExplicitOutcomeTool {
    status: solaris_types::tool::ToolResultStatus,
    calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    replay_policy: EffectReplayPolicy,
}

#[async_trait::async_trait]
impl solaris_tools::Tool for ExplicitOutcomeTool {
    fn name(&self) -> &str {
        "ExplicitOutcome"
    }

    fn description(&self) -> &str {
        "Return an explicit test outcome"
    }

    fn input_schema(&self) -> serde_json::Value {
        json!({"type": "object"})
    }

    fn is_concurrency_safe(&self, _input: &serde_json::Value) -> bool {
        false
    }

    async fn execute(&self, _input: serde_json::Value) -> solaris_types::tool::ToolResult {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        solaris_types::tool::ToolResult {
            content: format!("{:?}", self.status),
            is_error: self.status.is_error(),
        }
    }

    fn classify_result(
        &self,
        _input: &serde_json::Value,
        _result: &solaris_types::tool::ToolResult,
    ) -> solaris_types::tool::ToolResultStatus {
        self.status
    }

    fn describe_effect(&self, _input: &serde_json::Value) -> EffectDescriptor {
        EffectDescriptor {
            class: EffectClass::ReadOnly,
            action: "explicit test outcome".into(),
            resources: ResourceFootprint::default(),
            replay_policy: self.replay_policy,
        }
    }

    fn category(&self) -> ToolCategory {
        ToolCategory::Info
    }
}

fn explicit_outcome_fixture(
    status: solaris_types::tool::ToolResultStatus,
    replay_policy: EffectReplayPolicy,
    permission: PermissionContext,
) -> (
    ToolRegistry,
    EffectExecutionContext,
    ContentBlock,
    std::sync::Arc<std::sync::atomic::AtomicUsize>,
) {
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(ExplicitOutcomeTool {
        status,
        calls: std::sync::Arc::clone(&calls),
        replay_policy,
    }));
    let context = EffectExecutionContext::new(
        RunId::from(format!("status-{status:?}")),
        AgentId::from("status-agent"),
        std::sync::Arc::new(InMemoryRuntimeLedger::default()),
        permission,
        OperationEnvironmentSnapshot::default(),
    );
    let call = ContentBlock::ToolUse {
        id: "status-call".into(),
        name: "ExplicitOutcome".into(),
        input: json!({}),
        extra: None,
    };
    (registry, context, call, calls)
}

async fn execute_status_fixture(
    registry: &ToolRegistry,
    context: &EffectExecutionContext,
    call: &ContentBlock,
) -> ToolCallOutcome {
    execute_tool_calls_with_policy_context(
        registry,
        std::slice::from_ref(call),
        &std::sync::Arc::new(Mutex::new(ToolConfirmer::new(true, Vec::new()))),
        PermissionMode::Bypass,
        PermissionCeiling::unrestricted(),
        context,
        None,
        solaris_compact::CompactLevel::Off,
        false,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn exec_process_recovery_is_one_unknown_outcome_stops_the_turn_and_cold_recovery_never_retries() {
    let _recovery_test_guard = solaris_process::isolate_process_recoveries_for_test();
    let workspace = tempfile::tempdir().unwrap();
    let exec = solaris_tools::exec_command::ExecCommandTool::new(workspace.path().to_path_buf())
        .with_process_failure_fixtures_for_test([
            ("SOLARIS_SANDBOX_FIXTURE_FAIL_AFTER_SPAWN".to_owned(), "1".to_owned()),
            (
                "SOLARIS_SANDBOX_FIXTURE_POST_SPAWN_TERMINATION_UNKNOWN".to_owned(),
                "1".to_owned(),
            ),
            (
                "SOLARIS_SANDBOX_FIXTURE_RECOVERY_TERMINATION_UNKNOWN".to_owned(),
                "1".to_owned(),
            ),
        ]);
    let later_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(exec));
    registry.register(Box::new(ExplicitOutcomeTool {
        status: solaris_types::tool::ToolResultStatus::Executed,
        calls: std::sync::Arc::clone(&later_calls),
        replay_policy: EffectReplayPolicy::ReplaySafe,
    }));
    let ledger = std::sync::Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("exec-process-recovery-chain");
    let agent_id = AgentId::from("exec-process-recovery-agent");
    let permissions = PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted());
    let context = EffectExecutionContext::new(
        run_id.clone(),
        agent_id.clone(),
        ledger.clone(),
        permissions.clone(),
        OperationEnvironmentSnapshot::default(),
    );
    #[cfg(windows)]
    let exec_input = json!({"cmd": "Start-Sleep -Seconds 30", "shell": "powershell"});
    #[cfg(not(windows))]
    let exec_input = json!({"cmd": "sleep 30", "shell": "sh"});
    let exec_call = ContentBlock::ToolUse {
        id: "exec-recovery-call".to_owned(),
        name: "ExecCommand".to_owned(),
        input: exec_input.clone(),
        extra: None,
    };
    let later_call = ContentBlock::ToolUse {
        id: "later-call".to_owned(),
        name: "ExplicitOutcome".to_owned(),
        input: json!({}),
        extra: None,
    };

    let outcome = execute_tool_calls_with_policy_context(
        &registry,
        &[exec_call.clone(), later_call],
        &std::sync::Arc::new(Mutex::new(ToolConfirmer::new(true, Vec::new()))),
        PermissionMode::Bypass,
        PermissionCeiling::unrestricted(),
        &context,
        None,
        solaris_compact::CompactLevel::Off,
        false,
    )
    .await
    .unwrap();

    assert_eq!(
        outcome.statuses,
        [
            solaris_types::tool::ToolResultStatus::OutcomeUnknown,
            solaris_types::tool::ToolResultStatus::Aborted,
        ]
    );
    assert_eq!(later_calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    let effect_id = context.effect_id_for_call("exec-recovery-call");
    let outcomes = ledger
        .records_for_run(&run_id)
        .unwrap()
        .into_iter()
        .filter(|record| {
            record.record_type == "effect_outcome" && record.payload["effect_id"].as_str() == Some(effect_id.as_str())
        })
        .collect::<Vec<_>>();
    assert_eq!(outcomes.len(), 1, "one effect may have only one terminal outcome");
    assert_eq!(outcomes[0].payload["status"], "outcome_unknown");
    assert_eq!(
        outcomes[0].payload["process_recovery"]["schema"],
        "solaris/process-recovery/v1"
    );
    assert_eq!(outcomes[0].payload["process_recovery"]["kind"], "started_child");
    assert!(outcomes[0].payload["process_recovery"]["id"].is_u64());
    assert!(
        outcomes[0].payload["process_recovery"]["ref"]
            .as_str()
            .is_some_and(|value| value.starts_with("solaris://process-recovery/"))
    );

    let cold = EffectExecutionContext::new(
        run_id.clone(),
        agent_id,
        ledger,
        permissions,
        OperationEnvironmentSnapshot::default(),
    );
    let recovered = execute_status_fixture(&registry, &cold, &exec_call).await;
    assert_eq!(
        recovered.statuses,
        [solaris_types::tool::ToolResultStatus::OutcomeUnknown]
    );
}

#[tokio::test]
async fn tool_call_outcome_preserves_noop_and_timeout() {
    for status in [
        solaris_types::tool::ToolResultStatus::Noop,
        solaris_types::tool::ToolResultStatus::Timeout,
    ] {
        let (registry, context, call, _) = explicit_outcome_fixture(
            status,
            EffectReplayPolicy::ReplaySafe,
            PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        );

        let outcome = execute_status_fixture(&registry, &context, &call).await;

        assert_eq!(outcome.statuses, [status]);
    }
}

#[tokio::test]
async fn policy_denial_has_denied_status() {
    let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    permissions.add_rule(PermissionRule {
        capability: Some("ExplicitOutcome".into()),
        action: None,
        effect_class: Some(EffectClass::ReadOnly),
        resource_prefixes: Vec::new(),
        decision: PermissionDecision::Deny,
    });
    let (registry, context, call, calls) = explicit_outcome_fixture(
        solaris_types::tool::ToolResultStatus::Executed,
        EffectReplayPolicy::ReplaySafe,
        permissions,
    );

    let outcome = execute_tool_calls_with_policy_context(
        &registry,
        &[call],
        &std::sync::Arc::new(Mutex::new(ToolConfirmer::new(true, Vec::new()))),
        PermissionMode::Auto,
        PermissionCeiling::unrestricted(),
        &context,
        None,
        solaris_compact::CompactLevel::Off,
        false,
    )
    .await
    .unwrap();

    assert_eq!(outcome.statuses, [solaris_types::tool::ToolResultStatus::Denied]);
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn successful_recovery_reuse_is_a_cache_hit() {
    let (registry, context, call, calls) = explicit_outcome_fixture(
        solaris_types::tool::ToolResultStatus::Executed,
        EffectReplayPolicy::ReplaySafe,
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
    );

    let first = execute_status_fixture(&registry, &context, &call).await;
    let second = execute_status_fixture(&registry, &context, &call).await;

    assert_eq!(first.statuses, [solaris_types::tool::ToolResultStatus::Executed]);
    assert_eq!(second.statuses, [solaris_types::tool::ToolResultStatus::CacheHit]);
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}

struct ModifierOutcomeTool {
    calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl solaris_tools::Tool for ModifierOutcomeTool {
    fn name(&self) -> &str {
        "ModifierOutcome"
    }

    fn description(&self) -> &str {
        "Return a context modifier for durable recovery"
    }

    fn input_schema(&self) -> serde_json::Value {
        json!({"type": "object"})
    }

    fn is_concurrency_safe(&self, _input: &serde_json::Value) -> bool {
        false
    }

    async fn execute(&self, _input: serde_json::Value) -> solaris_types::tool::ToolResult {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        solaris_types::tool::ToolResult {
            content: "modifier-visible-output".to_owned(),
            is_error: false,
        }
    }

    fn context_modifier_for(&self, _input: &serde_json::Value) -> Option<solaris_types::skill_types::ContextModifier> {
        Some(solaris_types::skill_types::ContextModifier {
            model: Some("recovered-model".to_owned()),
            effort: Some(solaris_types::skill_types::EffortLevel::High),
            allowed_tools: vec!["Read".to_owned()],
            plan_mode_transition: Some(solaris_types::skill_types::PlanModeTransition::Exit {
                plan_content: Some("# Recovered plan".to_owned()),
            }),
        })
    }

    fn describe_effect(&self, _input: &serde_json::Value) -> EffectDescriptor {
        EffectDescriptor::read_only("durable modifier outcome")
    }

    fn category(&self) -> ToolCategory {
        ToolCategory::Info
    }
}

#[tokio::test]
async fn recovered_tool_outcome_restores_modifier_without_reexecution() {
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(ModifierOutcomeTool {
        calls: std::sync::Arc::clone(&calls),
    }));
    let context = EffectExecutionContext::new(
        RunId::from("modifier-recovery"),
        AgentId::from("modifier-agent"),
        std::sync::Arc::new(InMemoryRuntimeLedger::default()),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let call = ContentBlock::ToolUse {
        id: "modifier-call".to_owned(),
        name: "ModifierOutcome".to_owned(),
        input: json!({}),
        extra: None,
    };

    let first = execute_status_fixture(&registry, &context, &call).await;
    let recovered = execute_status_fixture(&registry, &context, &call).await;

    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(first.statuses, [solaris_types::tool::ToolResultStatus::Executed]);
    assert_eq!(recovered.statuses, [solaris_types::tool::ToolResultStatus::CacheHit]);
    for result in [&first.results[0], &recovered.results[0]] {
        assert!(matches!(
            result,
            ContentBlock::ToolResult { content, is_error: false, .. }
                if content == "modifier-visible-output"
        ));
    }
    assert_eq!(first.modifiers, recovered.modifiers);
    let modifier = recovered.modifiers[0].as_ref().unwrap();
    assert_eq!(modifier.model.as_deref(), Some("recovered-model"));
    assert!(matches!(
        modifier.plan_mode_transition,
        Some(solaris_types::skill_types::PlanModeTransition::Exit { .. })
    ));
}

#[tokio::test]
async fn legacy_modifier_outcome_requires_reconciliation() {
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(ModifierOutcomeTool {
        calls: std::sync::Arc::clone(&calls),
    }));
    let context = EffectExecutionContext::new(
        RunId::from("legacy-modifier-recovery"),
        AgentId::from("legacy-modifier-agent"),
        std::sync::Arc::new(InMemoryRuntimeLedger::default()),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let call = ContentBlock::ToolUse {
        id: "legacy-modifier-call".to_owned(),
        name: "ModifierOutcome".to_owned(),
        input: json!({}),
        extra: None,
    };
    let ContentBlock::ToolUse { id, name, input, .. } = &call else {
        unreachable!()
    };
    let request = context.effect_request(id, name, input, describe_effect(&registry, name, input));
    context.record_effect_intent(&request).unwrap();
    context
        .record_effect_outcome(&request, false, "legacy modifier output")
        .unwrap();

    let recovered = execute_status_fixture(&registry, &context, &call).await;

    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(
        recovered.statuses,
        [solaris_types::tool::ToolResultStatus::OutcomeUnknown]
    );
    assert_eq!(recovered.modifiers, [None]);
}

#[tokio::test]
async fn unresolved_prior_side_effect_has_outcome_unknown_status() {
    let (registry, context, call, calls) = explicit_outcome_fixture(
        solaris_types::tool::ToolResultStatus::Executed,
        EffectReplayPolicy::ReconcileRequired,
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
    );
    let ContentBlock::ToolUse { id, name, input, .. } = &call else {
        unreachable!()
    };
    let request = context.effect_request(id, name, input, describe_effect(&registry, name, input));
    context.record_effect_intent(&request).unwrap();

    let outcome = execute_status_fixture(&registry, &context, &call).await;

    assert_eq!(
        outcome.statuses,
        [solaris_types::tool::ToolResultStatus::OutcomeUnknown]
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn removed_tool_with_existing_intent_stops_later_tool_execution() {
    let (registry, context, later_call, calls) = explicit_outcome_fixture(
        solaris_types::tool::ToolResultStatus::Executed,
        EffectReplayPolicy::ReplaySafe,
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
    );
    let missing_call = ContentBlock::ToolUse {
        id: "removed-tool-call".to_owned(),
        name: "RemovedTool".to_owned(),
        input: json!({"path": "workspace/file"}),
        extra: None,
    };
    let ContentBlock::ToolUse { id, name, input, .. } = &missing_call else {
        unreachable!()
    };
    let request = context.effect_request(
        id,
        name,
        input,
        EffectDescriptor {
            class: EffectClass::WorkspaceMutation,
            action: "removed side-effecting tool".to_owned(),
            resources: ResourceFootprint::default(),
            replay_policy: EffectReplayPolicy::ReconcileRequired,
        },
    );
    context.record_effect_intent(&request).unwrap();

    let outcome = execute_tool_calls_with_policy_context(
        &registry,
        &[missing_call, later_call],
        &std::sync::Arc::new(Mutex::new(ToolConfirmer::new(true, Vec::new()))),
        PermissionMode::Bypass,
        PermissionCeiling::unrestricted(),
        &context,
        None,
        solaris_compact::CompactLevel::Off,
        false,
    )
    .await
    .unwrap();

    assert_eq!(
        outcome.statuses,
        [
            solaris_types::tool::ToolResultStatus::OutcomeUnknown,
            solaris_types::tool::ToolResultStatus::Aborted,
        ]
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
}

struct FailingHistoryLedger;

impl crate::runtime_ledger::RuntimeLedger for FailingHistoryLedger {
    crate::runtime_ledger::unsupported_compare_and_append!();

    fn append(
        &self,
        _run_id: &RunId,
        _durability: solaris_types::effect::DurabilityClass,
        _record_type: &str,
        _payload: serde_json::Value,
    ) -> std::io::Result<crate::runtime_ledger::LedgerRecord> {
        Err(std::io::Error::other("history unavailable"))
    }

    fn run_ids(&self) -> std::io::Result<Vec<RunId>> {
        Ok(Vec::new())
    }

    fn records_for_run(&self, _run_id: &RunId) -> std::io::Result<Vec<crate::runtime_ledger::LedgerRecord>> {
        Err(std::io::Error::other("history unavailable"))
    }
}

#[tokio::test]
async fn recovery_history_error_is_outcome_unknown_without_tool_execution() {
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(ExplicitOutcomeTool {
        status: solaris_types::tool::ToolResultStatus::Executed,
        calls: std::sync::Arc::clone(&calls),
        replay_policy: EffectReplayPolicy::ReplaySafe,
    }));
    let context = EffectExecutionContext::new(
        RunId::from("failing-history"),
        AgentId::from("status-agent"),
        std::sync::Arc::new(FailingHistoryLedger),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let call = ContentBlock::ToolUse {
        id: "history-call".to_owned(),
        name: "ExplicitOutcome".to_owned(),
        input: json!({}),
        extra: None,
    };

    let outcome = execute_status_fixture(&registry, &context, &call).await;

    assert_eq!(
        outcome.statuses,
        [solaris_types::tool::ToolResultStatus::OutcomeUnknown]
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn approval_recovery_history_error_emits_outcome_unknown_without_execution() {
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(ExplicitOutcomeTool {
        status: solaris_types::tool::ToolResultStatus::Executed,
        calls: std::sync::Arc::clone(&calls),
        replay_policy: EffectReplayPolicy::ReplaySafe,
    }));
    let context = EffectExecutionContext::new(
        RunId::from("approval-failing-history"),
        AgentId::from("status-agent"),
        std::sync::Arc::new(FailingHistoryLedger),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let call = ContentBlock::ToolUse {
        id: "approval-history-call".to_owned(),
        name: "ExplicitOutcome".to_owned(),
        input: json!({}),
        extra: None,
    };
    let events = std::sync::Arc::new(CapturedStatusEvents::default());
    let writer: std::sync::Arc<dyn ProtocolEmitter> = events.clone();

    let outcome = execute_tool_calls_with_approval_context(
        &registry,
        &[call],
        &std::sync::Arc::new(solaris_protocol::ToolApprovalManager::new()),
        &writer,
        "message-history-error",
        false,
        &[],
        PermissionCeiling::unrestricted(),
        &context,
        None,
        solaris_compact::CompactLevel::Off,
        false,
    )
    .await
    .unwrap();

    assert_eq!(
        outcome.statuses,
        [solaris_types::tool::ToolResultStatus::OutcomeUnknown]
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert!(
        events
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|event| event["type"] == "tool_result" && event["status"] == "outcome_unknown")
    );
}

#[derive(Default)]
struct CapturedStatusEvents(std::sync::Mutex<Vec<serde_json::Value>>);

impl ProtocolEmitter for CapturedStatusEvents {
    fn emit(&self, event: &ProtocolEvent) -> std::io::Result<()> {
        self.0
            .lock()
            .unwrap()
            .push(serde_json::to_value(event).map_err(std::io::Error::other)?);
        Ok(())
    }
}

struct TypedSandboxFailureAuthorizer(solaris_process::SandboxReport);

impl solaris_process::ProcessSpawnAuthorizer for TypedSandboxFailureAuthorizer {
    fn authorize_and_spawn(
        &self,
        _spawn: solaris_process::ProcessSpawn,
    ) -> std::io::Result<solaris_process::ManagedChild> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            solaris_process::SandboxError::NetworkProxyUnavailable { report: self.0 },
        ))
    }
}

struct TypedSandboxExecCommand {
    inner: solaris_tools::exec_command::ExecCommandTool,
    report: solaris_process::SandboxReport,
}

#[async_trait::async_trait]
impl solaris_tools::Tool for TypedSandboxExecCommand {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn description(&self) -> &str {
        self.inner.description()
    }

    fn input_schema(&self) -> serde_json::Value {
        self.inner.input_schema()
    }

    fn is_concurrency_safe(&self, input: &serde_json::Value) -> bool {
        self.inner.is_concurrency_safe(input)
    }

    async fn execute(&self, input: serde_json::Value) -> solaris_types::tool::ToolResult {
        self.inner.execute(input).await
    }

    fn prepare_effect(
        &self,
        effect_id: &str,
        input: &serde_json::Value,
    ) -> Result<solaris_tools::PreparedToolEffect, String> {
        self.inner.prepare_effect(effect_id, input)
    }

    fn revalidate_effect(
        &self,
        input: &serde_json::Value,
        context: &solaris_tools::ToolExecutionContext,
    ) -> EffectDescriptor {
        self.inner.revalidate_effect(input, context)
    }

    fn prepare_execution<'a>(
        &'a self,
        input: serde_json::Value,
        context: solaris_tools::ToolExecutionContext,
    ) -> Result<solaris_tools::PreparedToolExecution<'a>, String> {
        let authorization = solaris_process::ProcessSpawnAuthorization::new(std::sync::Arc::new(
            TypedSandboxFailureAuthorizer(self.report),
        ));
        self.inner.prepare_execution(
            input,
            context
                .with_process_launch_policy(solaris_process::ProcessLaunchPolicy::Ambient)
                .with_process_spawn_authorization(authorization),
        )
    }

    fn describe_effect(&self, input: &serde_json::Value) -> EffectDescriptor {
        self.inner.describe_effect(input)
    }

    fn category(&self) -> ToolCategory {
        self.inner.category()
    }
}

#[tokio::test]
async fn exec_command_sandbox_report_reaches_agent_outcome_and_protocol_host_event() {
    let report = solaris_process::SandboxReport::new(
        solaris_process::SandboxEnforcement::Unavailable,
        solaris_process::SandboxBackend::WindowsAppContainer,
        solaris_process::SandboxReason::NetworkProxyUnavailable,
    );
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(TypedSandboxExecCommand {
        inner: solaris_tools::exec_command::ExecCommandTool::new(std::env::temp_dir()),
        report,
    }));
    let context = EffectExecutionContext::new(
        RunId::from("typed-sandbox-report"),
        AgentId::from("typed-sandbox-agent"),
        std::sync::Arc::new(InMemoryRuntimeLedger::default()),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let call = ContentBlock::ToolUse {
        id: "typed-sandbox-call".to_owned(),
        name: "ExecCommand".to_owned(),
        input: json!({"cmd": "echo must-not-run"}),
        extra: None,
    };
    let events = std::sync::Arc::new(CapturedStatusEvents::default());
    let writer: std::sync::Arc<dyn ProtocolEmitter> = events.clone();

    let outcome = execute_tool_calls_with_approval_context(
        &registry,
        std::slice::from_ref(&call),
        &std::sync::Arc::new(solaris_protocol::ToolApprovalManager::new()),
        &writer,
        "typed-sandbox-message",
        true,
        &[],
        PermissionCeiling::unrestricted(),
        &context,
        None,
        solaris_compact::CompactLevel::Off,
        false,
    )
    .await
    .unwrap();

    assert_eq!(outcome.statuses, [solaris_types::tool::ToolResultStatus::Denied]);
    assert_eq!(
        outcome
            .metadata
            .get("typed-sandbox-call")
            .and_then(|metadata| metadata.sandbox_report),
        Some(report)
    );

    let replay = execute_tool_calls_with_approval_context(
        &registry,
        &[call],
        &std::sync::Arc::new(solaris_protocol::ToolApprovalManager::new()),
        &writer,
        "typed-sandbox-replay",
        true,
        &[],
        PermissionCeiling::unrestricted(),
        &context,
        None,
        solaris_compact::CompactLevel::Off,
        false,
    )
    .await
    .unwrap();
    assert_eq!(replay.statuses, [solaris_types::tool::ToolResultStatus::Denied]);
    assert_eq!(
        replay
            .metadata
            .get("typed-sandbox-call")
            .and_then(|metadata| metadata.sandbox_report),
        Some(report)
    );

    let events = events.0.lock().unwrap();
    let matching = events
        .iter()
        .filter(|event| event["type"] == "tool_result" && event["call_id"] == "typed-sandbox-call")
        .collect::<Vec<_>>();
    assert_eq!(matching.len(), 2);
    let event = matching.last().unwrap();
    assert_eq!(event["status"], "denied");
    assert_eq!(event["metadata"]["sandbox_report"]["backend"], "windows_app_container");
    assert_eq!(event["metadata"]["sandbox_report"]["enforcement"], "unavailable");
    assert_eq!(
        event["metadata"]["sandbox_report"]["reason"],
        "network_proxy_unavailable"
    );
}

#[tokio::test]
async fn json_approval_denial_emits_explicit_denied_terminal_status() {
    let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    permissions.add_rule(PermissionRule {
        capability: Some("ExplicitOutcome".into()),
        action: None,
        effect_class: Some(EffectClass::ReadOnly),
        resource_prefixes: Vec::new(),
        decision: PermissionDecision::Deny,
    });
    let (registry, context, call, _) = explicit_outcome_fixture(
        solaris_types::tool::ToolResultStatus::Executed,
        EffectReplayPolicy::ReplaySafe,
        permissions,
    );
    let events = std::sync::Arc::new(CapturedStatusEvents::default());
    let writer: std::sync::Arc<dyn ProtocolEmitter> = events.clone();

    let outcome = execute_tool_calls_with_approval_context(
        &registry,
        &[call],
        &std::sync::Arc::new(solaris_protocol::ToolApprovalManager::new()),
        &writer,
        "message-1",
        false,
        &[],
        PermissionCeiling::unrestricted(),
        &context,
        None,
        solaris_compact::CompactLevel::Off,
        false,
    )
    .await
    .unwrap();

    assert_eq!(outcome.statuses, [solaris_types::tool::ToolResultStatus::Denied]);
    let events = events.0.lock().unwrap();
    assert!(
        events
            .iter()
            .any(|event| { event["type"] == "tool_cancelled" && event["status"] == "denied" })
    );
    assert!(
        events
            .iter()
            .any(|event| { event["type"] == "tool_result" && event["status"] == "denied" })
    );
}

#[tokio::test]
async fn cancelled_json_approval_emits_aborted_terminal_status() {
    let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    permissions.add_rule(PermissionRule {
        capability: Some("ExplicitOutcome".into()),
        action: None,
        effect_class: Some(EffectClass::ReadOnly),
        resource_prefixes: Vec::new(),
        decision: PermissionDecision::Ask,
    });
    let (registry, context, call, _) = explicit_outcome_fixture(
        solaris_types::tool::ToolResultStatus::Executed,
        EffectReplayPolicy::ReplaySafe,
        permissions,
    );
    let registry = std::sync::Arc::new(registry);
    let context = std::sync::Arc::new(context);
    let approval_manager = std::sync::Arc::new(solaris_protocol::ToolApprovalManager::new());
    let events = std::sync::Arc::new(CapturedStatusEvents::default());
    let writer: std::sync::Arc<dyn ProtocolEmitter> = events.clone();
    let execution = {
        let registry = registry.clone();
        let context = context.clone();
        let approval_manager = approval_manager.clone();
        let writer = writer.clone();
        tokio::spawn(async move {
            execute_tool_calls_with_approval_context(
                &registry,
                &[call],
                &approval_manager,
                &writer,
                "message-cancelled",
                false,
                &[],
                PermissionCeiling::unrestricted(),
                &context,
                None,
                solaris_compact::CompactLevel::Off,
                false,
            )
            .await
        })
    };
    for _ in 0..100 {
        if approval_manager.pending_count() == 1 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(approval_manager.pending_count(), 1);

    approval_manager.drop_pending("status-call");
    let result = execution.await.unwrap();

    assert!(matches!(result, Err(ExecutionControl::Quit)));
    let events = events.0.lock().unwrap();
    assert!(
        events
            .iter()
            .any(|event| { event["type"] == "tool_cancelled" && event["status"] == "aborted" })
    );
    assert!(
        events
            .iter()
            .any(|event| { event["type"] == "tool_result" && event["status"] == "aborted" })
    );
}
