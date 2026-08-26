// -- truncate_display -----------------------------------------------------

#[test]
fn truncate_display_ascii_short_unchanged() {
    assert_eq!(truncate_display("hello", 10), "hello");
}

#[test]
fn truncate_display_ascii_truncated() {
    let result = truncate_display("hello world", 5);
    assert!(result.ends_with("..."));
    assert!(result.len() <= 20);
}

#[test]
fn truncate_display_cjk_does_not_panic() {
    // 200 CJK chars: each is 3 bytes, so byte index 200 falls mid-character
    let cjk: String = "你好世界测试".chars().cycle().take(200).collect();
    let result = truncate_display(&cjk, 50);
    assert!(result.ends_with("..."));
}

#[test]
fn truncate_display_mixed_cjk_ascii_does_not_panic() {
    let mixed = "abc你好def世界ghi测试".repeat(20);
    let result = truncate_display(&mixed, 30);
    assert!(result.ends_with("..."));
}

#[test]
fn host_tool_request_serialization_uses_secret_safe_effect_projection() {
    let secret = "super-secret-token";
    let input = json!({
        "cmd": format!("deploy --token {secret}"),
        "headers": {"Authorization": secret},
        "user_input": secret,
    });
    let tool = solaris_tools::exec_command::ExecCommandTool::new(std::env::temp_dir());
    let descriptor = tool.describe_effect(&input);
    let context = EffectExecutionContext::new(
        RunId::from("host-secret-safe-run"),
        AgentId::from("agent"),
        Arc::new(InMemoryRuntimeLedger::default()),
        PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let request = context.effect_request("secret-call", "ExecCommand", &input, descriptor);
    let event = ProtocolEvent::ToolRequest {
        msg_id: "message".into(),
        call_id: "secret-call".into(),
        run_id: Some(context.run_id().to_string()),
        agent_id: Some(context.agent_id().to_string()),
        operation_id: Some(request.operation_id.to_string()),
        effect_id: Some(request.effect_id.to_string()),
        tool: host_safe_tool_info("ExecCommand", ToolCategory::Exec, &request),
    };

    let serialized = serde_json::to_string(&event).unwrap();

    assert!(!serialized.contains(secret));
    assert!(!serialized.contains("deploy --token"));
    assert!(!serialized.contains("Authorization"));
    assert!(!serialized.contains("user_input"));
    assert!(serialized.contains("input_digest"));
    assert!(serialized.contains("sha256:"));
    assert!(serialized.contains("process effect"));
}

#[tokio::test]
async fn plan_mode_denies_hook_process_before_it_starts() {
    let temp = tempfile::tempdir().unwrap();
    let marker = temp.path().join("hook-must-not-run.txt");
    #[cfg(windows)]
    let command = format!(
        "Set-Content -LiteralPath '{}' -Value forbidden",
        marker.to_string_lossy().replace('\'', "''")
    );
    #[cfg(not(windows))]
    let command = format!(
        "printf forbidden > '{}'",
        marker.to_string_lossy().replace('\'', "'\\''")
    );

    let permissions = PermissionContext::new(PermissionMode::Plan, PermissionCeiling::unrestricted());
    permissions.set_boundary(ExecutionBoundary::unrestricted());
    let run_id = RunId::from("run-hook-denied");
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let context = EffectExecutionContext::new(
        run_id.clone(),
        AgentId::new("agent-hook-denied"),
        ledger.clone(),
        permissions,
        OperationEnvironmentSnapshot::default(),
    );
    let mut hooks = HookEngine::new(
        HooksConfig {
            pre_tool_use: vec![HookDef {
                name: "blocked-hook".into(),
                tool_match: vec!["Read".into()],
                file_match: vec![],
                command,
                timeout_ms: 5_000,
                network: Default::default(),
            }],
            ..Default::default()
        },
        temp.path().to_path_buf(),
    );
    hooks.set_executor(Arc::new(EffectHookExecutor::new(context)));

    let result = hooks.run_pre_tool_use("Read", &json!({})).await;
    assert!(result.is_err());
    assert!(!marker.exists());
    let records = ledger.records_for_run(&run_id).unwrap();
    assert!(records.iter().any(|record| record.record_type == "permission_decision"));
    assert!(!records.iter().any(|record| record.record_type == "effect_intent"));
}

#[tokio::test]
async fn hook_executable_error_does_not_expose_requested_path() {
    let sentinel = "super-secret-token-hook-shell";
    let executable = std::env::temp_dir().join(sentinel).join("missing-shell");
    let context = EffectExecutionContext::new(
        RunId::from("hook-shell-error-run"),
        AgentId::from("agent"),
        Arc::new(InMemoryRuntimeLedger::default()),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let error = EffectHookExecutor::new(context)
        .execute(HookInvocation {
            identity: None,
            definition: HookDef {
                name: "missing-shell".into(),
                tool_match: Vec::new(),
                file_match: Vec::new(),
                command: "must not run".into(),
                timeout_ms: 1_000,
                network: Default::default(),
            },
            effective_input: json!({}),
            completed_parent_recovery: false,
            hook_name: "missing-shell".into(),
            command: "must not run".into(),
            executable,
            argv: Vec::new(),
            cwd: std::env::temp_dir(),
            env: std::collections::HashMap::new(),
            timeout_ms: 1_000,
            network: Default::default(),
        })
        .await
        .unwrap_err()
        .to_string();

    assert!(!error.contains(sentinel));
    assert!(error.contains("executable identity sha256:"));
}

#[tokio::test]
async fn approved_custom_shell_replacement_is_rejected_before_intent() {
    let directory = tempfile::tempdir().unwrap();
    let shell = solaris_config::shell::default_shell();
    let copied = directory
        .path()
        .join(shell.path.file_name().expect("shell should have a file name"));
    std::fs::copy(&shell.path, &copied).unwrap();
    let marker = directory.path().join("must-not-run");
    #[cfg(windows)]
    let command = format!(
        "Set-Content -LiteralPath '{}' -Value forbidden",
        marker.to_string_lossy().replace('\'', "''")
    );
    #[cfg(not(windows))]
    let command = format!(
        "printf forbidden > '{}'",
        marker.to_string_lossy().replace('\'', "'\\''")
    );
    let input = json!({"cmd": command, "shell": copied});
    let tool = solaris_tools::exec_command::ExecCommandTool::new(directory.path().to_path_buf());
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(tool));
    let run_id = RunId::from("custom-shell-replacement-run");
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let context = EffectExecutionContext::new(
        run_id.clone(),
        AgentId::from("custom-shell-agent"),
        ledger.clone(),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let prepared_effect = registry
        .get("ExecCommand")
        .unwrap()
        .prepare_effect(context.effect_id_for_call("custom-shell-call").as_str(), &input)
        .unwrap()
        .into_parts();
    let request = context.effect_request("custom-shell-call", "ExecCommand", &input, prepared_effect.0);
    let tool_execution = prepared_effect.1;
    let registration = context.remember_approved_request_with_tool_context(
        request,
        tool_execution,
        PermissionMode::Bypass,
        PermissionCeiling::unrestricted(),
    );
    std::fs::write(&copied, b"replacement").unwrap();
    let call = ContentBlock::ToolUse {
        id: "custom-shell-call".into(),
        name: "ExecCommand".into(),
        input,
        extra: None,
    };

    let (result, _, _, _) = execute_single_with_effect_context(
        &registry,
        &call,
        None,
        &context,
        solaris_compact::CompactLevel::Off,
        false,
        Some(registration),
    )
    .await;

    assert!(matches!(result, ContentBlock::ToolResult { is_error: true, .. }));
    assert!(!marker.exists());
    let records = ledger.records_for_run(&run_id).unwrap();
    assert!(!records.iter().any(|record| record.record_type == "effect_intent"));
}

#[tokio::test]
async fn policy_approval_identity_is_the_one_pinned_after_permit_wait() {
    use solaris_types::resource::ResourceBudget;

    use crate::resource_manager::ResourceManager;

    let directory = tempfile::tempdir().unwrap();
    let shell = solaris_config::shell::default_shell();
    let copied = directory
        .path()
        .join(shell.path.file_name().expect("shell should have a file name"));
    std::fs::copy(&shell.path, &copied).unwrap();
    let input = json!({"cmd": "echo forbidden", "shell": copied});
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(solaris_tools::exec_command::ExecCommandTool::new(
        directory.path().to_path_buf(),
    )));
    let resources = ResourceManager::new(ResourceBudget {
        max_concurrent_effects: Some(1),
        ..Default::default()
    });
    let held = resources.acquire_effect().await.unwrap();
    let run_id = RunId::from("exec-permission-pin-run");
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let context = EffectExecutionContext::new(
        run_id.clone(),
        AgentId::from("agent"),
        ledger.clone(),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    )
    .with_resource_manager(Arc::clone(&resources));
    let confirmer = Arc::new(Mutex::new(ToolConfirmer::new(true, Vec::new())));
    let calls = vec![ContentBlock::ToolUse {
        id: "exec-call".into(),
        name: "ExecCommand".into(),
        input,
        extra: None,
    }];
    let task = tokio::spawn(async move {
        execute_tool_calls_with_policy_context(
            &registry,
            &calls,
            &confirmer,
            PermissionMode::Bypass,
            PermissionCeiling::unrestricted(),
            &context,
            None,
            solaris_compact::CompactLevel::Off,
            false,
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if ledger
                .records_for_run(&run_id)
                .unwrap()
                .iter()
                .any(|record| record.record_type == "permission_decision")
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    std::fs::write(&copied, b"replacement implementation").unwrap();
    drop(held);

    let outcome = task.await.unwrap().unwrap();
    assert!(matches!(
        &outcome.results[0],
        ContentBlock::ToolResult { is_error: true, .. }
    ));
    let records = ledger.records_for_run(&run_id).unwrap();
    assert!(!records.iter().any(|record| record.record_type == "effect_intent"));
}

#[tokio::test]
async fn consecutive_exec_intents_each_record_the_pinned_shell_identity() {
    let shell = solaris_config::shell::default_shell();
    let expected = inspect_executable(&shell.path).unwrap();
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(solaris_tools::exec_command::ExecCommandTool::new(
        std::env::temp_dir(),
    )));
    let run_id = RunId::from("consecutive-exec-intents-run");
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let context = EffectExecutionContext::new(
        run_id.clone(),
        AgentId::from("agent"),
        ledger.clone(),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let calls = vec![
        ContentBlock::ToolUse {
            id: "exec-one".into(),
            name: "ExecCommand".into(),
            input: json!({"cmd": "echo one"}),
            extra: None,
        },
        ContentBlock::ToolUse {
            id: "exec-two".into(),
            name: "ExecCommand".into(),
            input: json!({"cmd": "echo two"}),
            extra: None,
        },
    ];
    let confirmer = Arc::new(Mutex::new(ToolConfirmer::new(true, Vec::new())));

    let outcome = execute_tool_calls_with_policy_context(
        &registry,
        &calls,
        &confirmer,
        PermissionMode::Bypass,
        PermissionCeiling::unrestricted(),
        &context,
        None,
        solaris_compact::CompactLevel::Off,
        false,
    )
    .await
    .unwrap();

    assert!(outcome.results.iter().all(|result| !block_is_error(result)));
    let intents = ledger
        .records_for_run(&run_id)
        .unwrap()
        .into_iter()
        .filter(|record| record.record_type == "effect_intent")
        .collect::<Vec<_>>();
    assert_eq!(intents.len(), 2);
    for intent in intents {
        let environment: OperationEnvironmentSnapshot =
            serde_json::from_value(intent.payload["environment"].clone()).unwrap();
        let implementation = &environment
            .tools
            .iter()
            .find(|tool| tool.name.ends_with("/ExecCommand"))
            .unwrap()
            .implementation;
        assert_eq!(
            implementation.implementation_id,
            format!("exec-shell:{}", expected.path_digest())
        );
        assert_eq!(implementation.digest.as_deref(), Some(expected.content_digest()));
    }
    assert!(context.environment().tools.is_empty());
}

#[tokio::test]
async fn completed_exec_call_reuses_output_without_running_again() {
    use solaris_config::shell::ShellKind;

    let directory = tempfile::tempdir().unwrap();
    let marker = directory.path().join("exec-recovery-marker.txt");
    let command = match solaris_config::shell::default_shell().kind {
        ShellKind::PowerShell => format!(
            "Add-Content -LiteralPath '{}' -Value run; Write-Output reusable",
            marker.to_string_lossy().replace('\'', "''")
        ),
        ShellKind::Cmd => format!("echo run>>\"{}\" & echo reusable", marker.display()),
        ShellKind::Bash | ShellKind::Zsh | ShellKind::Sh => format!(
            "printf 'run\\n' >> '{}'; printf reusable",
            marker.to_string_lossy().replace('\'', "'\\''")
        ),
    };
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(solaris_tools::exec_command::ExecCommandTool::new(
        directory.path().to_path_buf(),
    )));
    let run_id = RunId::from("exec-recovery-implementation-run");
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let context = EffectExecutionContext::new(
        run_id.clone(),
        AgentId::from("agent"),
        ledger.clone(),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let call = ContentBlock::ToolUse {
        id: "same-exec-call".into(),
        name: "ExecCommand".into(),
        input: json!({"cmd": command}),
        extra: None,
    };
    let confirmer = Arc::new(Mutex::new(ToolConfirmer::new(true, Vec::new())));

    for _ in 0..2 {
        let outcome = execute_tool_calls_with_policy_context(
            &registry,
            std::slice::from_ref(&call),
            &confirmer,
            PermissionMode::Bypass,
            PermissionCeiling::unrestricted(),
            &context,
            None,
            solaris_compact::CompactLevel::Off,
            false,
        )
        .await
        .unwrap();
        assert!(matches!(
            &outcome.results[0],
            ContentBlock::ToolResult { is_error: false, content, .. } if content.contains("reusable")
        ));
    }

    assert_eq!(std::fs::read_to_string(marker).unwrap().lines().count(), 1);
    let records = ledger.records_for_run(&run_id).unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|record| record.record_type == "effect_intent")
            .count(),
        1
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| record.record_type == "effect_outcome")
            .count(),
        1
    );
}

struct PluginToolFixture {
    _directory: tempfile::TempDir,
    executable: std::path::PathBuf,
    registry: ToolRegistry,
    context: EffectExecutionContext,
    ledger: Arc<InMemoryRuntimeLedger>,
    run_id: RunId,
    call: ContentBlock,
    implementation: solaris_types::plugin::ImplementationIdentity,
}

fn approved_plugin_tool_fixture(run_label: &str) -> PluginToolFixture {
    use solaris_types::plugin::{
        PluginCapabilities, PluginCommandToolDefinition, PluginDefinition, PluginSource, ResolvedPluginDefinition,
        ResolvedPluginIdentity,
    };
    use solaris_types::runtime::ToolImplementationSnapshot;

    let directory = tempfile::tempdir().unwrap();
    #[cfg(windows)]
    let source_executable = {
        let shell = solaris_config::shell::resolve_shell(Some("cmd")).expect("cmd should be available on Windows");
        shell
            .path
            .parent()
            .expect("cmd should have a parent directory")
            .join("whoami.exe")
    };
    #[cfg(not(windows))]
    let shell = solaris_config::shell::default_shell();
    #[cfg(not(windows))]
    let source_executable = shell.path.clone();
    let executable = directory.path().join(
        source_executable
            .file_name()
            .expect("test executable should have a file name"),
    );
    std::fs::copy(&source_executable, &executable).unwrap();
    #[cfg(windows)]
    let args = Vec::new();
    #[cfg(not(windows))]
    let args = shell.derive_exec_args("printf ok", false);
    let definition = PluginCommandToolDefinition {
        name: "PreparedPlugin".into(),
        description: "prepared plugin test".into(),
        input_schema: json!({"type": "object"}),
        command: executable
            .file_name()
            .expect("copied shell should have a file name")
            .to_string_lossy()
            .into_owned(),
        args,
        effect: EffectDescriptor {
            class: EffectClass::Process,
            action: "execute prepared plugin".into(),
            resources: ResourceFootprint::default(),
            replay_policy: EffectReplayPolicy::Never,
        },
        concurrency_safe: false,
        max_result_size: 1024,
        timeout_ms: 5_000,
    };
    let source = PluginSource::Local {
        path: directory.path().to_string_lossy().into_owned(),
    };
    let plugin = ResolvedPluginDefinition {
        definition: PluginDefinition {
            id: "prepared-plugin".into(),
            version: "1".into(),
            source: source.clone(),
            materialized_path: None,
            capabilities: PluginCapabilities {
                tools: vec![definition.name.clone()],
                ..Default::default()
            },
            compatibility: Default::default(),
            requested_paths: Vec::new(),
            requires_services: Vec::new(),
            resources: Default::default(),
            command_tools: vec![definition.clone()],
            command_contributions: Vec::new(),
        },
        identity: ResolvedPluginIdentity {
            plugin_id: "prepared-plugin".into(),
            source,
            implementation: solaris_types::plugin::ImplementationIdentity {
                implementation_id: "plugin:prepared-plugin".into(),
                version: Some("1".into()),
                digest: Some("plugin-digest".into()),
            },
        },
        authority_root: Some(directory.path().to_string_lossy().into_owned()),
    };
    let tool = crate::plugin_tool::PluginCommandTool::from_resolved(&plugin, definition).unwrap();
    let implementation = tool
        .implementation_identity()
        .expect("command plugin should expose its implementation identity");
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(tool));
    let run_id = RunId::from(run_label);
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let context = EffectExecutionContext::new(
        run_id.clone(),
        AgentId::from("prepared-plugin-agent"),
        ledger.clone(),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot {
            tools: vec![ToolImplementationSnapshot {
                name: "PreparedPlugin".into(),
                implementation: implementation.clone(),
                schema_digest: None,
                replay_policy: EffectReplayPolicy::Never,
            }],
            ..Default::default()
        },
    );
    let call = ContentBlock::ToolUse {
        id: "prepared-plugin-call".into(),
        name: "PreparedPlugin".into(),
        input: json!({}),
        extra: None,
    };
    let ContentBlock::ToolUse { id, name, input, .. } = &call else {
        unreachable!()
    };
    let request = context.effect_request(id, name, input, describe_effect(&registry, name, input));
    context.remember_approved_request(request, PermissionMode::Bypass, PermissionCeiling::unrestricted());

    PluginToolFixture {
        _directory: directory,
        executable,
        registry,
        context,
        ledger,
        run_id,
        call,
        implementation,
    }
}

#[tokio::test]
async fn replaced_plugin_executable_is_rejected_before_effect_intent() {
    let fixture = approved_plugin_tool_fixture("plugin-replaced-before-intent-run");
    std::fs::write(&fixture.executable, b"replacement").unwrap();

    let (result, _, _, _) = execute_single_with_effect_context(
        &fixture.registry,
        &fixture.call,
        None,
        &fixture.context,
        solaris_compact::CompactLevel::Off,
        false,
        None,
    )
    .await;

    assert!(matches!(
        result,
        ContentBlock::ToolResult { is_error: true, ref content, .. }
            if content.contains("implementation changed before execution")
    ));
    let records = fixture.ledger.records_for_run(&fixture.run_id).unwrap();
    assert!(
        records
            .iter()
            .any(|record| record.record_type == "effect_revalidation_failed")
    );
    assert!(!records.iter().any(|record| record.record_type == "effect_intent"));
    assert!(!records.iter().any(|record| record.record_type == "effect_outcome"));
}

#[cfg(unix)]
#[tokio::test]
async fn redirected_plugin_executable_is_rejected_before_effect_intent() {
    use std::os::unix::fs::symlink;

    let fixture = approved_plugin_tool_fixture("plugin-redirected-before-intent-run");
    let approved_backup = fixture.executable.with_extension("approved");
    let redirected = fixture.executable.with_extension("redirected");
    std::fs::copy(&fixture.executable, &redirected).unwrap();
    std::fs::rename(&fixture.executable, &approved_backup).unwrap();
    symlink(&redirected, &fixture.executable).unwrap();

    let (result, _, _, _) = execute_single_with_effect_context(
        &fixture.registry,
        &fixture.call,
        None,
        &fixture.context,
        solaris_compact::CompactLevel::Off,
        false,
        None,
    )
    .await;

    assert!(matches!(
        result,
        ContentBlock::ToolResult { is_error: true, ref content, .. }
            if content.contains("path identity changed before execution")
    ));
    let records = fixture.ledger.records_for_run(&fixture.run_id).unwrap();
    assert!(
        records
            .iter()
            .any(|record| record.record_type == "effect_revalidation_failed")
    );
    assert!(!records.iter().any(|record| record.record_type == "effect_intent"));
    assert!(!records.iter().any(|record| record.record_type == "effect_outcome"));
}

#[tokio::test]
async fn prepared_plugin_execution_records_pinned_identity_and_runs() {
    let fixture = approved_plugin_tool_fixture("plugin-prepared-execution-run");

    let (result, _, _, _) = execute_single_with_effect_context(
        &fixture.registry,
        &fixture.call,
        None,
        &fixture.context,
        solaris_compact::CompactLevel::Off,
        false,
        None,
    )
    .await;

    assert!(
        matches!(
            &result,
            ContentBlock::ToolResult { is_error: false, content, .. } if !content.is_empty()
        ),
        "unexpected prepared plugin result: {result:?}"
    );
    let records = fixture.ledger.records_for_run(&fixture.run_id).unwrap();
    let intent = records
        .iter()
        .find(|record| record.record_type == "effect_intent")
        .expect("prepared plugin should record effect intent");
    let environment: OperationEnvironmentSnapshot =
        serde_json::from_value(intent.payload["environment"].clone()).unwrap();
    let recorded = environment
        .tools
        .iter()
        .find(|tool| tool.name == "PreparedPlugin")
        .expect("prepared plugin identity should be recorded");
    assert_eq!(recorded.implementation, fixture.implementation);
    assert_eq!(records.last().unwrap().record_type, "effect_outcome");
}
