use super::*;

use solaris_process::{SandboxEnforcement, platform_sandbox_report};
use solaris_types::permission::{ExecutionBoundary, PermissionDecision, PermissionRule};

pub(super) fn marker_command(marker: &Path) -> String {
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

async fn execute_process_policy_case(mode: PermissionMode) -> (ContentBlock, bool) {
    let workspace = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let marker = outside.path().join("plugin-process-marker.txt");
    let tool = command_tool(&marker_command(&marker), 1024, 5_000);
    let descriptor = tool.describe_effect(&serde_json::json!({}));
    let permissions = PermissionContext::new(mode, PermissionCeiling::unrestricted());
    let mut boundary = ExecutionBoundary::workspace(workspace.path().to_string_lossy().into_owned());
    boundary.unrestricted_file_reads = true;
    boundary.unrestricted_file_writes = true;
    boundary.unrestricted_network = true;
    boundary.unrestricted_process = true;
    boundary.unrestricted_external_side_effects = true;
    permissions.set_boundary(boundary);
    permissions.register_protected_paths(state.path(), Vec::new()).unwrap();
    permissions.allow_configured_effect_for("test:plugin", "ContainedPlugin", &descriptor);
    permissions.set_generated_rules(
        "test:plugin",
        vec![PermissionRule {
            capability: Some("ContainedPlugin".into()),
            action: None,
            effect_class: Some(EffectClass::Process),
            resource_prefixes: Vec::new(),
            decision: PermissionDecision::Allow,
        }],
    );
    let context = EffectExecutionContext::new(
        RunId::from(format!("plugin-process-policy-{mode:?}")),
        AgentId::from("root"),
        Arc::new(InMemoryRuntimeLedger::default()),
        permissions,
        OperationEnvironmentSnapshot::default(),
    );
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(tool));
    let confirmer = Arc::new(Mutex::new(ToolConfirmer::new(true, Vec::new())));
    let outcome = execute_tool_calls_with_policy_context(
        &registry,
        &[ContentBlock::ToolUse {
            id: "plugin-process-policy-call".into(),
            name: "ContainedPlugin".into(),
            input: serde_json::json!({}),
            extra: None,
        }],
        &confirmer,
        mode,
        PermissionCeiling::unrestricted(),
        &context,
        None,
        CompactLevel::Off,
        false,
    )
    .await
    .unwrap();
    (outcome.results.into_iter().next().unwrap(), marker.exists())
}

#[tokio::test]
async fn auto_plugin_command_cannot_start_with_ambient_access() {
    let (result, marker_exists) = execute_process_policy_case(PermissionMode::Auto).await;
    let ContentBlock::ToolResult { is_error, .. } = result else {
        panic!("plugin process must return a tool result")
    };

    assert!(is_error);
    assert!(!marker_exists);
}

#[tokio::test]
async fn bypass_plugin_command_keeps_ambient_access() {
    let (result, marker_exists) = execute_process_policy_case(PermissionMode::Bypass).await;
    let ContentBlock::ToolResult { is_error, content, .. } = result else {
        panic!("plugin process must return a tool result")
    };

    assert!(!is_error, "{content}");
    assert!(marker_exists);
}

#[tokio::test]
async fn plan_plugin_command_is_rejected_before_spawn() {
    let (result, marker_exists) = execute_process_policy_case(PermissionMode::Plan).await;
    let ContentBlock::ToolResult { is_error, .. } = result else {
        panic!("plugin process must return a tool result")
    };

    assert!(is_error);
    assert!(!marker_exists);
}

fn contribution_marker_script(marker: &Path) -> String {
    match solaris_config::shell::default_shell().kind {
        solaris_config::shell::ShellKind::PowerShell => format!(
            "Set-Content -LiteralPath '{}' -Value launched; [Console]::Out.Write('{{}}')",
            marker.to_string_lossy().replace('\'', "''")
        ),
        solaris_config::shell::ShellKind::Cmd => {
            format!("echo launched>\"{}\" & echo {{}}", marker.display())
        }
        _ => format!(
            "printf launched > '{}'; printf '{{}}'",
            marker.to_string_lossy().replace('\'', "'\\''")
        ),
    }
}

async fn execute_contribution_policy_case(mode: PermissionMode) -> (Result<Value, String>, bool) {
    let workspace = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let marker = outside.path().join("plugin-contribution-marker.txt");
    let shell = solaris_config::shell::default_shell();
    let executable = shell.path.clone();
    let implementation = ImplementationIdentity {
        implementation_id: "plugin:process-policy".into(),
        version: None,
        digest: Some("process-policy".into()),
    };
    let contribution = PluginCommandContribution {
        plugin_id: "process-policy".into(),
        implementation: implementation.clone(),
        definition: PluginCommandContributionDefinition {
            kind: PluginContributionKind::Hook,
            name: "process-policy".into(),
            command: executable.to_string_lossy().into_owned(),
            args: shell.derive_exec_args(&contribution_marker_script(&marker), false),
            input_schema: serde_json::json!({"type": "object"}),
            max_result_size: 1024,
            timeout_ms: 5_000,
        },
        approved_executable: approved_executable(&executable),
        executable_identity: executable_identity(&executable),
        provider_command_v1: false,
        executable,
    };
    let capability = contribution.capability();
    let descriptor = contribution.execution_boundary_descriptor();
    let permissions = PermissionContext::new(mode, PermissionCeiling::unrestricted());
    let mut boundary = ExecutionBoundary::workspace(workspace.path().to_string_lossy().into_owned());
    boundary.unrestricted_file_reads = true;
    boundary.unrestricted_file_writes = true;
    boundary.unrestricted_network = true;
    boundary.unrestricted_process = true;
    boundary.unrestricted_external_side_effects = true;
    permissions.set_boundary(boundary);
    permissions.register_protected_paths(state.path(), Vec::new()).unwrap();
    permissions.allow_configured_effect_for("test:contribution", &capability, &descriptor);
    permissions.set_generated_rules(
        "test:contribution",
        vec![PermissionRule {
            capability: Some(capability),
            action: None,
            effect_class: Some(EffectClass::Process),
            resource_prefixes: Vec::new(),
            decision: PermissionDecision::Allow,
        }],
    );
    let context = EffectExecutionContext::new(
        RunId::from(format!("plugin-contribution-policy-{mode:?}")),
        AgentId::from("root"),
        Arc::new(InMemoryRuntimeLedger::default()),
        permissions,
        OperationEnvironmentSnapshot {
            plugins: vec![implementation],
            ..Default::default()
        },
    );

    let result = contribution.invoke_authorized(serde_json::json!({}), &context).await;
    (result, marker.exists())
}

#[tokio::test]
async fn auto_plugin_contribution_cannot_write_outside_workspace() {
    let report = platform_sandbox_report();
    let (result, marker_exists) = execute_contribution_policy_case(PermissionMode::Auto).await;

    match report.enforcement() {
        SandboxEnforcement::Full => assert!(result.is_ok(), "{result:?}"),
        SandboxEnforcement::Partial | SandboxEnforcement::Unavailable => {
            let error = result.expect_err("Auto contribution must reject a non-Full process sandbox");
            assert!(
                error.contains("strict workspace sandbox enforcement is insufficient"),
                "{error}"
            );
        }
    }
    assert!(!marker_exists);
}

#[tokio::test]
async fn bypass_plugin_contribution_keeps_ambient_access() {
    let (result, marker_exists) = execute_contribution_policy_case(PermissionMode::Bypass).await;

    assert!(result.is_ok(), "{result:?}");
    assert!(marker_exists);
}

#[tokio::test]
async fn plan_plugin_contribution_is_rejected_before_spawn() {
    let (result, marker_exists) = execute_contribution_policy_case(PermissionMode::Plan).await;

    assert!(result.is_err());
    assert!(!marker_exists);
}
