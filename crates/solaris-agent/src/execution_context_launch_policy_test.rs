fn revalidated_exec_launch_policy(
    approved_mode: PermissionMode,
    current_mode: PermissionMode,
) -> Result<solaris_process::ProcessLaunchPolicy, String> {
    let workspace = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let permissions = PermissionContext::new(approved_mode, PermissionCeiling::unrestricted());
    permissions.set_boundary(ExecutionBoundary::unrestricted());
    permissions
        .register_protected_paths(state.path(), Vec::new())
        .unwrap();
    let context = EffectExecutionContext::new(
        RunId::from("launch-policy-run"),
        AgentId::from("launch-policy-agent"),
        Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default()),
        permissions.clone(),
        OperationEnvironmentSnapshot::default(),
    );
    let tool = solaris_tools::exec_command::ExecCommandTool::new(workspace.path().to_path_buf());
    let input = json!({"cmd": "echo launch-policy"});
    let (descriptor, tool_execution) = tool
        .prepare_effect("effect:launch-policy", &input)
        .unwrap()
        .into_parts();
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(tool));
    let request = context.effect_request("call", "ExecCommand", &input, descriptor);
    context.issue_approval_lease(&request, false).unwrap();
    let _registration = context.remember_approved_request_with_tool_context(
        request,
        tool_execution,
        approved_mode,
        PermissionCeiling::unrestricted(),
    );
    let approved = context.take_approved_request_for_call("call").unwrap();
    permissions.set_mode(current_mode);

    context
        .revalidate(&registry, &approved)
        .and_then(|execution| {
            execution
                .process_launch_policy()
                .cloned()
                .ok_or_else(|| "missing launch policy".to_owned())
        })
}

#[test]
fn mode_changes_choose_the_stricter_auto_sandbox() {
    for (approved, current) in [
        (PermissionMode::Auto, PermissionMode::Bypass),
        (PermissionMode::Bypass, PermissionMode::Auto),
    ] {
        let policy = revalidated_exec_launch_policy(approved, current).unwrap();
        assert!(matches!(
            policy,
            solaris_process::ProcessLaunchPolicy::WorkspaceSandbox { .. }
        ));
    }
}

#[test]
fn bypass_at_approval_and_execution_uses_ambient_policy() {
    assert_eq!(
        revalidated_exec_launch_policy(PermissionMode::Bypass, PermissionMode::Bypass).unwrap(),
        solaris_process::ProcessLaunchPolicy::Ambient
    );
}

#[test]
fn plan_at_either_boundary_rejects_exec() {
    for (approved, current) in [
        (PermissionMode::Plan, PermissionMode::Bypass),
        (PermissionMode::Bypass, PermissionMode::Plan),
    ] {
        assert!(revalidated_exec_launch_policy(approved, current).is_err());
    }
}
