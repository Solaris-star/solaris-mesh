#[test]
fn durable_task_phases_record_stable_call_identity_without_payloads() {
    let ledger = Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default());
    let context = EffectExecutionContext::new(
        RunId::from("task-phase-run"),
        AgentId::from("task-phase-agent"),
        ledger.clone(),
        PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );

    context
        .record_task_phase("message-1", DurableTaskPhase::ProviderInFlight, Some("provider-call-1"))
        .unwrap();
    context
        .record_task_phase("message-1", DurableTaskPhase::OutcomeUnknown, Some("tool-call-1"))
        .unwrap();

    let records = ledger.records_for_run(&RunId::from("task-phase-run")).unwrap();
    assert_eq!(records.len(), 2);
    assert!(records.iter().all(|record| record.record_type == "agent_task_phase"));
    assert_eq!(records[0].payload["phase"], "provider_in_flight");
    assert_eq!(records[0].payload["call_id"], "provider-call-1");
    assert_eq!(records[1].payload["phase"], "outcome_unknown");
    assert_eq!(records[1].payload["call_id"], "tool-call-1");
    let serialized = serde_json::to_string(&records).unwrap();
    assert!(!serialized.contains("prompt"));
    assert!(!serialized.contains("tool_input"));
}

#[test]
fn run_tool_statistics_dedupe_per_agent_instead_of_per_provider_local_call_id() {
    let ledger = Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default());
    let run_id = RunId::from("shared-tool-statistics-run");
    let resources = crate::resource_manager::ResourceManager::new(solaris_types::resource::ResourceBudget::default());
    resources
        .attach_ledger(
            run_id.clone(),
            Arc::clone(&ledger) as Arc<dyn crate::runtime_ledger::RuntimeLedger>,
        )
        .unwrap();
    let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    let first = EffectExecutionContext::new(
        run_id.clone(),
        AgentId::from("child-a"),
        Arc::clone(&ledger) as Arc<dyn crate::runtime_ledger::RuntimeLedger>,
        permissions.clone(),
        OperationEnvironmentSnapshot::default(),
    )
    .with_resource_manager(Arc::clone(&resources));
    let second = EffectExecutionContext::new(
        run_id,
        AgentId::from("child-b"),
        ledger as Arc<dyn crate::runtime_ledger::RuntimeLedger>,
        permissions,
        OperationEnvironmentSnapshot::default(),
    )
    .with_resource_manager(Arc::clone(&resources));

    first
        .record_tool_calls_once(
            "provider-local-round",
            &[solaris_types::tool::ToolCallStat::new(
                "task:t|env:e",
                "read",
                &serde_json::json!({"path": "a"}),
                solaris_types::tool::ToolResultStatus::Executed,
            )],
        )
        .unwrap();
    first
        .record_tool_calls_once(
            "provider-local-round",
            &[solaris_types::tool::ToolCallStat::new(
                "task:t|env:e",
                "read",
                &serde_json::json!({"path": "a"}),
                solaris_types::tool::ToolResultStatus::Executed,
            )],
        )
        .unwrap();
    second
        .record_tool_calls_once(
            "provider-local-round",
            &[solaris_types::tool::ToolCallStat::new(
                "task:t|env:e",
                "noop",
                &serde_json::json!({}),
                solaris_types::tool::ToolResultStatus::Noop,
            )],
        )
        .unwrap();

    let usage = resources.usage();
    assert_eq!(usage.tool_calls, 2);
    assert_eq!(usage.useful_tool_calls, 1);
    assert_eq!(usage.useful_call_rate, Some(0.5));
}
