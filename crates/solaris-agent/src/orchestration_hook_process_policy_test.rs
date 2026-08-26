fn hook_marker_command(marker: &std::path::Path) -> String {
    #[cfg(windows)]
    {
        format!(
            "Set-Content -LiteralPath '{}' -Value launched",
            marker.to_string_lossy().replace('\'', "''")
        )
    }
    #[cfg(not(windows))]
    {
        format!(
            "printf launched > '{}'",
            marker.to_string_lossy().replace('\'', "'\\''")
        )
    }
}

async fn run_hook_process_policy_case(mode: PermissionMode) -> (bool, bool) {
    let workspace = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let marker = outside.path().join("hook-process-marker.txt");
    let permissions = PermissionContext::new(mode, PermissionCeiling::unrestricted());
    permissions.set_boundary(ExecutionBoundary::workspace(
        workspace.path().to_string_lossy().into_owned(),
    ));
    permissions.register_protected_paths(state.path(), Vec::new()).unwrap();
    let context = EffectExecutionContext::new(
        RunId::from(format!("hook-process-policy-{mode:?}")),
        AgentId::from("hook-process-policy-agent"),
        Arc::new(InMemoryRuntimeLedger::default()),
        permissions,
        OperationEnvironmentSnapshot::default(),
    );
    let mut hooks = HookEngine::new(
        HooksConfig {
            pre_tool_use: vec![HookDef {
                name: "process-policy-hook".into(),
                tool_match: vec!["Read".into()],
                file_match: Vec::new(),
                command: hook_marker_command(&marker),
                timeout_ms: 5_000,
                network: Default::default(),
            }],
            ..Default::default()
        },
        workspace.path().to_path_buf(),
    );
    hooks.set_executor(Arc::new(EffectHookExecutor::new(context)));

    let result = hooks.run_pre_tool_use("Read", &json!({})).await;
    (result.is_err(), marker.exists())
}

#[tokio::test]
async fn auto_configured_hook_cannot_start_with_ambient_access() {
    let (is_error, marker_exists) = run_hook_process_policy_case(PermissionMode::Auto).await;

    assert!(is_error);
    assert!(!marker_exists);
}

#[tokio::test]
async fn bypass_configured_hook_keeps_ambient_access() {
    let (is_error, marker_exists) = run_hook_process_policy_case(PermissionMode::Bypass).await;

    assert!(!is_error);
    assert!(marker_exists);
}

#[tokio::test]
async fn plan_configured_hook_is_rejected_before_spawn() {
    let (is_error, marker_exists) = run_hook_process_policy_case(PermissionMode::Plan).await;

    assert!(is_error);
    assert!(!marker_exists);
}
