fn hook_recovery_fixture(
    run_id: &str,
) -> (
    ToolRegistry,
    EffectExecutionContext,
    ContentBlock,
    std::sync::Arc<std::sync::atomic::AtomicUsize>,
    std::sync::Arc<InMemoryRuntimeLedger>,
) {
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(ExplicitOutcomeTool {
        status: solaris_types::tool::ToolResultStatus::Executed,
        calls: std::sync::Arc::clone(&calls),
        replay_policy: EffectReplayPolicy::ReplaySafe,
    }));
    let ledger = std::sync::Arc::new(InMemoryRuntimeLedger::default());
    let context = EffectExecutionContext::new(
        RunId::from(run_id),
        AgentId::from("hook-recovery-agent"),
        ledger.clone(),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let call = ContentBlock::ToolUse {
        id: "parent-tool-call".to_owned(),
        name: "ExplicitOutcome".to_owned(),
        input: json!({"path": "workspace/input"}),
        extra: None,
    };
    (registry, context, call, calls, ledger)
}

fn durable_post_hook(name: &str, command: String, timeout_ms: u64) -> HooksConfig {
    HooksConfig {
        post_tool_use: vec![HookDef {
            name: name.to_owned(),
            tool_match: vec!["ExplicitOutcome".to_owned()],
            file_match: Vec::new(),
            command,
            timeout_ms,
            network: Default::default(),
        }],
        ..Default::default()
    }
}

fn durable_pre_hook(name: &str, command: String, timeout_ms: u64) -> HooksConfig {
    HooksConfig {
        pre_tool_use: vec![HookDef {
            name: name.to_owned(),
            tool_match: vec!["ExplicitOutcome".to_owned()],
            file_match: Vec::new(),
            command,
            timeout_ms,
            network: Default::default(),
        }],
        ..Default::default()
    }
}

fn append_hook_marker_command(marker: &std::path::Path) -> String {
    #[cfg(windows)]
    {
        format!(
            "Add-Content -LiteralPath '{}' -Value launched",
            marker.to_string_lossy().replace('\'', "''")
        )
    }
    #[cfg(not(windows))]
    {
        format!(
            "printf 'launched\\n' >> '{}'",
            marker.to_string_lossy().replace('\'', "'\\''")
        )
    }
}

fn sleeping_hook_command() -> String {
    #[cfg(windows)]
    {
        "Start-Sleep -Seconds 30".to_owned()
    }
    #[cfg(not(windows))]
    {
        "sleep 30".to_owned()
    }
}

async fn execute_hook_policy(
    registry: &ToolRegistry,
    context: &EffectExecutionContext,
    calls: &[ContentBlock],
    hooks: Option<&mut HookEngine>,
) -> ToolCallOutcome {
    execute_tool_calls_with_policy_context(
        registry,
        calls,
        &std::sync::Arc::new(Mutex::new(ToolConfirmer::new(true, Vec::new()))),
        PermissionMode::Bypass,
        PermissionCeiling::unrestricted(),
        context,
        hooks,
        solaris_compact::CompactLevel::Off,
        false,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn saved_tool_outcome_runs_missing_post_hook_once_then_reuses_it() {
    let directory = tempfile::tempdir().unwrap();
    let marker = directory.path().join("post-hook-count.txt");
    let (registry, context, call, calls, _) = hook_recovery_fixture("post-hook-after-tool-outcome");

    let first = execute_hook_policy(&registry, &context, std::slice::from_ref(&call), None).await;
    assert_eq!(first.statuses, [ToolStatus::Executed]);

    let mut hooks = HookEngine::new(
        durable_post_hook("post-once", append_hook_marker_command(&marker), 5_000),
        directory.path().to_path_buf(),
    );
    hooks.set_executor(std::sync::Arc::new(EffectHookExecutor::new(context.clone())));
    let recovered = execute_hook_policy(&registry, &context, std::slice::from_ref(&call), Some(&mut hooks)).await;
    let recovered_again = execute_hook_policy(&registry, &context, std::slice::from_ref(&call), Some(&mut hooks)).await;

    assert_eq!(recovered.statuses, [ToolStatus::CacheHit]);
    assert_eq!(
        recovered_again.statuses,
        [ToolStatus::CacheHit],
        "recovered results: {:?}",
        recovered_again.results
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(std::fs::read_to_string(marker).unwrap().matches("launched").count(), 1);
}

#[tokio::test]
async fn completed_parent_reuses_pre_hook_without_running_it_late() {
    let directory = tempfile::tempdir().unwrap();
    let marker = directory.path().join("pre-hook-once.txt");
    let (registry, context, call, calls, _) = hook_recovery_fixture("completed-parent-pre-hook");
    let mut hooks = HookEngine::new(
        durable_pre_hook("pre-once", append_hook_marker_command(&marker), 5_000),
        directory.path().to_path_buf(),
    );
    hooks.set_executor(std::sync::Arc::new(EffectHookExecutor::new(context.clone())));

    let first = execute_hook_policy(&registry, &context, std::slice::from_ref(&call), Some(&mut hooks)).await;
    let recovered = execute_hook_policy(&registry, &context, std::slice::from_ref(&call), Some(&mut hooks)).await;

    assert_eq!(first.statuses, [ToolStatus::Executed]);
    assert_eq!(recovered.statuses, [ToolStatus::CacheHit]);
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(std::fs::read_to_string(marker).unwrap().matches("launched").count(), 1);
}

#[tokio::test]
async fn pre_hook_added_after_parent_outcome_requires_reconciliation_without_late_execution() {
    let directory = tempfile::tempdir().unwrap();
    let marker = directory.path().join("late-pre-hook.txt");
    let (registry, context, call, calls, _) = hook_recovery_fixture("late-pre-hook");
    let mut original = HookEngine::new(HooksConfig::default(), directory.path().to_path_buf());
    original.set_executor(std::sync::Arc::new(EffectHookExecutor::new(context.clone())));
    let first = execute_hook_policy(
        &registry,
        &context,
        std::slice::from_ref(&call),
        Some(&mut original),
    )
    .await;
    assert_eq!(first.statuses, [ToolStatus::Executed]);

    let mut changed = HookEngine::new(
        durable_pre_hook("late-pre", append_hook_marker_command(&marker), 5_000),
        directory.path().to_path_buf(),
    );
    changed.set_executor(std::sync::Arc::new(EffectHookExecutor::new(context.clone())));
    let later = ContentBlock::ToolUse {
        id: "must-abort-after-late-pre".to_owned(),
        name: "ExplicitOutcome".to_owned(),
        input: json!({}),
        extra: None,
    };
    let recovered = execute_hook_policy(&registry, &context, &[call, later], Some(&mut changed)).await;

    assert_eq!(recovered.statuses, [ToolStatus::OutcomeUnknown, ToolStatus::Aborted]);
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(!marker.exists());
}

#[tokio::test]
async fn changed_hook_definition_reconciles_without_using_a_new_identity() {
    let directory = tempfile::tempdir().unwrap();
    let first_marker = directory.path().join("first-hook.txt");
    let changed_marker = directory.path().join("changed-hook.txt");
    let (registry, context, call, calls, _) = hook_recovery_fixture("changed-post-hook-definition");
    let _ = execute_hook_policy(&registry, &context, std::slice::from_ref(&call), None).await;

    let mut original = HookEngine::new(
        durable_post_hook("stable-slot", append_hook_marker_command(&first_marker), 5_000),
        directory.path().to_path_buf(),
    );
    original.set_executor(std::sync::Arc::new(EffectHookExecutor::new(context.clone())));
    let first_recovery =
        execute_hook_policy(&registry, &context, std::slice::from_ref(&call), Some(&mut original)).await;
    assert_eq!(first_recovery.statuses, [ToolStatus::CacheHit]);

    let mut changed = HookEngine::new(
        durable_post_hook("stable-slot", append_hook_marker_command(&changed_marker), 5_000),
        directory.path().to_path_buf(),
    );
    changed.set_executor(std::sync::Arc::new(EffectHookExecutor::new(context.clone())));
    let changed_recovery =
        execute_hook_policy(&registry, &context, std::slice::from_ref(&call), Some(&mut changed)).await;

    assert_eq!(changed_recovery.statuses, [ToolStatus::OutcomeUnknown]);
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(first_marker.exists());
    assert!(!changed_marker.exists());
}

#[tokio::test]
async fn stable_pre_hook_reuses_completion_and_input_change_reconciles() {
    let directory = tempfile::tempdir().unwrap();
    let marker = directory.path().join("pre-hook-count.txt");
    let context = EffectExecutionContext::new(
        RunId::from("stable-pre-hook-input"),
        AgentId::from("stable-pre-hook-agent"),
        std::sync::Arc::new(InMemoryRuntimeLedger::default()),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let mut hooks = HookEngine::new(
        HooksConfig {
            pre_tool_use: vec![HookDef {
                name: "pre-once".to_owned(),
                tool_match: vec!["Read".to_owned()],
                file_match: Vec::new(),
                command: append_hook_marker_command(&marker),
                timeout_ms: 5_000,
                network: Default::default(),
            }],
            ..Default::default()
        },
        directory.path().to_path_buf(),
    );
    hooks.set_executor(std::sync::Arc::new(EffectHookExecutor::new(context)));

    hooks
        .run_pre_tool_use_for_call("parent-pre-call", "Read", &json!({"path": "a"}))
        .await
        .unwrap();
    hooks
        .run_pre_tool_use_for_call("parent-pre-call", "Read", &json!({"path": "a"}))
        .await
        .unwrap();
    let changed = hooks
        .run_pre_tool_use_for_call("parent-pre-call", "Read", &json!({"path": "b"}))
        .await
        .unwrap_err();

    assert!(changed.is_outcome_unknown());
    assert_eq!(std::fs::read_to_string(marker).unwrap().matches("launched").count(), 1);
}

#[tokio::test]
async fn hook_history_read_failure_is_outcome_unknown_before_process_start() {
    let directory = tempfile::tempdir().unwrap();
    let marker = directory.path().join("history-failure-must-not-run.txt");
    let context = EffectExecutionContext::new(
        RunId::from("hook-history-failure"),
        AgentId::from("hook-history-agent"),
        std::sync::Arc::new(FailingHistoryLedger),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let mut hooks = HookEngine::new(
        HooksConfig {
            pre_tool_use: vec![HookDef {
                name: "history-failure".to_owned(),
                tool_match: vec!["Read".to_owned()],
                file_match: Vec::new(),
                command: append_hook_marker_command(&marker),
                timeout_ms: 5_000,
                network: Default::default(),
            }],
            ..Default::default()
        },
        directory.path().to_path_buf(),
    );
    hooks.set_executor(std::sync::Arc::new(EffectHookExecutor::new(context)));

    let error = hooks
        .run_pre_tool_use_for_call("history-parent", "Read", &json!({}))
        .await
        .unwrap_err();

    assert!(error.is_outcome_unknown());
    assert!(!marker.exists());
}

#[tokio::test]
async fn durable_hook_setup_failure_is_outcome_unknown() {
    let directory = tempfile::tempdir().unwrap();
    let (_, context, _, _, _) = hook_recovery_fixture("durable-hook-setup-failure");
    let definition = HookDef {
        name: "missing-shell".to_owned(),
        tool_match: vec!["Read".to_owned()],
        file_match: Vec::new(),
        command: "must not run".to_owned(),
        timeout_ms: 1_000,
        network: Default::default(),
    };
    let error = EffectHookExecutor::new(context)
        .execute(HookInvocation {
            identity: Some(solaris_config::hooks::HookInvocationIdentity {
                parent_call_id: "parent-tool-call".to_owned(),
                stage: solaris_config::hooks::HookStage::PostToolUse,
                ordinal: 0,
            }),
            definition,
            effective_input: json!({"tool_name": "Read"}),
            completed_parent_recovery: true,
            hook_name: "missing-shell".to_owned(),
            command: "must not run".to_owned(),
            executable: directory.path().join("missing-shell"),
            argv: Vec::new(),
            cwd: directory.path().to_path_buf(),
            env: std::collections::HashMap::new(),
            timeout_ms: 1_000,
            network: Default::default(),
        })
        .await
        .unwrap_err();

    assert!(error.is_outcome_unknown());
}

#[tokio::test]
async fn removed_post_hook_definition_cannot_bypass_prior_stage_history() {
    let directory = tempfile::tempdir().unwrap();
    let marker = directory.path().join("removed-post-hook.txt");
    let (registry, context, call, calls, _) = hook_recovery_fixture("removed-post-hook-definition");
    let _ = execute_hook_policy(&registry, &context, std::slice::from_ref(&call), None).await;
    let mut hooks = HookEngine::new(
        durable_post_hook("removed-post", append_hook_marker_command(&marker), 5_000),
        directory.path().to_path_buf(),
    );
    hooks.set_executor(std::sync::Arc::new(EffectHookExecutor::new(context.clone())));
    let completed = execute_hook_policy(&registry, &context, std::slice::from_ref(&call), Some(&mut hooks)).await;
    assert_eq!(completed.statuses, [ToolStatus::CacheHit]);

    drop(hooks);
    let mut restarted = HookEngine::new(HooksConfig::default(), directory.path().to_path_buf());
    restarted.set_executor(std::sync::Arc::new(EffectHookExecutor::new(context.clone())));
    let removed = execute_hook_policy(
        &registry,
        &context,
        std::slice::from_ref(&call),
        Some(&mut restarted),
    )
    .await;

    assert_eq!(removed.statuses, [ToolStatus::OutcomeUnknown]);
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(std::fs::read_to_string(marker).unwrap().matches("launched").count(), 1);
}

#[tokio::test]
async fn cancelled_post_hook_intent_is_outcome_unknown_and_aborts_later_tool() {
    let directory = tempfile::tempdir().unwrap();
    let (registry, context, call, calls, ledger) = hook_recovery_fixture("cancelled-post-hook");
    let _ = execute_hook_policy(&registry, &context, std::slice::from_ref(&call), None).await;
    let registry = std::sync::Arc::new(registry);
    let context = std::sync::Arc::new(context);
    let mut hooks = HookEngine::new(
        durable_post_hook("post-crash", sleeping_hook_command(), 60_000),
        directory.path().to_path_buf(),
    );
    hooks.set_executor(std::sync::Arc::new(EffectHookExecutor::new((*context).clone())));
    let task_registry = registry.clone();
    let task_context = context.clone();
    let task_call = call.clone();
    let task = tokio::spawn(async move {
        execute_hook_policy(
            &task_registry,
            &task_context,
            std::slice::from_ref(&task_call),
            Some(&mut hooks),
        )
        .await
    });

    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let has_hook_intent = ledger.records_for_run(context.run_id()).unwrap().iter().any(|record| {
                record.record_type == "effect_intent" && record.payload["capability"] == "HookCommand:post-crash"
            });
            if has_hook_intent {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("post-hook intent should be persisted before timeout");
    task.abort();
    match task.await {
        Err(error) => assert!(error.is_cancelled()),
        Ok(_) => panic!("cancelled post-hook task unexpectedly completed"),
    }

    let mut recovery_hooks = HookEngine::new(
        durable_post_hook("post-crash", sleeping_hook_command(), 60_000),
        directory.path().to_path_buf(),
    );
    recovery_hooks.set_executor(std::sync::Arc::new(EffectHookExecutor::new((*context).clone())));
    let later_call = ContentBlock::ToolUse {
        id: "later-tool-call".to_owned(),
        name: "ExplicitOutcome".to_owned(),
        input: json!({"path": "workspace/later"}),
        extra: None,
    };
    let recovered = execute_hook_policy(&registry, &context, &[call, later_call], Some(&mut recovery_hooks)).await;

    assert_eq!(recovered.statuses, [ToolStatus::OutcomeUnknown, ToolStatus::Aborted]);
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}

struct OutcomeUnknownHookExecutor;

#[async_trait]
impl HookExecutor for OutcomeUnknownHookExecutor {
    async fn execute(&self, invocation: HookInvocation) -> Result<HookExecutionResult, HookError> {
        Err(HookError::OutcomeUnknown {
            hook_name: invocation.hook_name,
            reason: "injected pending durable intent".to_owned(),
        })
    }
}

fn unknown_pre_hook_engine(directory: &std::path::Path) -> HookEngine {
    let mut hooks = HookEngine::new(
        HooksConfig {
            pre_tool_use: vec![HookDef {
                name: "unknown-pre".to_owned(),
                tool_match: vec!["ExplicitOutcome".to_owned()],
                file_match: Vec::new(),
                command: "must-not-run".to_owned(),
                timeout_ms: 1_000,
                network: Default::default(),
            }],
            ..Default::default()
        },
        directory.to_path_buf(),
    );
    hooks.set_executor(std::sync::Arc::new(OutcomeUnknownHookExecutor));
    hooks
}

#[tokio::test]
async fn policy_and_host_approval_paths_both_stop_on_unknown_pre_hook() {
    let directory = tempfile::tempdir().unwrap();
    let (registry, context, call, calls, _) = hook_recovery_fixture("unknown-pre-policy");
    let later_call = ContentBlock::ToolUse {
        id: "later-policy-call".to_owned(),
        name: "ExplicitOutcome".to_owned(),
        input: json!({}),
        extra: None,
    };
    let mut hooks = unknown_pre_hook_engine(directory.path());
    let policy = execute_hook_policy(&registry, &context, &[call, later_call], Some(&mut hooks)).await;
    assert_eq!(policy.statuses, [ToolStatus::OutcomeUnknown, ToolStatus::Aborted]);
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);

    let (registry, context, call, calls, _) = hook_recovery_fixture("unknown-pre-approval");
    let later_call = ContentBlock::ToolUse {
        id: "later-approval-call".to_owned(),
        name: "ExplicitOutcome".to_owned(),
        input: json!({}),
        extra: None,
    };
    let mut hooks = unknown_pre_hook_engine(directory.path());
    let approval_manager = std::sync::Arc::new(solaris_protocol::ToolApprovalManager::new());
    approval_manager.set_mode(PermissionMode::Bypass);
    let events = std::sync::Arc::new(CapturedStatusEvents::default());
    let writer: std::sync::Arc<dyn ProtocolEmitter> = events.clone();
    let approval = execute_tool_calls_with_approval_context(
        &registry,
        &[call, later_call],
        &approval_manager,
        &writer,
        "unknown-pre-message",
        false,
        &[],
        PermissionCeiling::unrestricted(),
        &context,
        Some(&mut hooks),
        solaris_compact::CompactLevel::Off,
        false,
    )
    .await
    .unwrap();

    assert_eq!(approval.statuses, [ToolStatus::OutcomeUnknown, ToolStatus::Aborted]);
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert!(
        events
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|event| { event["type"] == "tool_result" && event["status"] == "outcome_unknown" })
    );
}
