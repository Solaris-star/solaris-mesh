use std::sync::Arc;

use serde_json::json;
use solaris_config::config::{CliArgs, Config};
use solaris_process::{
    ManagedChild, ProcessLaunchPolicy, ProcessSpawn, ProcessSpawnAuthorization, ProcessSpawnAuthorizer,
};
use solaris_tools::Tool;
use solaris_tools::exec_command::ExecCommandTool;
use solaris_types::permission::PermissionMode;

use crate::output::null_sink::NullSink;

use super::AgentBootstrap;

struct StrictPolicySpawnAuthorizer(ProcessLaunchPolicy);

impl ProcessSpawnAuthorizer for StrictPolicySpawnAuthorizer {
    fn authorize_and_spawn(&self, spawn: ProcessSpawn) -> std::io::Result<ManagedChild> {
        spawn(self.0.clone())
    }
}

#[tokio::test]
async fn default_workspace_runtime_layout_rejects_auto_exec_before_spawn() {
    let workspace = tempfile::tempdir().unwrap();
    let mut config = Config::resolve(&CliArgs {
        provider: Some("anthropic".to_owned()),
        api_key: Some("test".to_owned()),
        base_url: Some("https://provider.example.test".to_owned()),
        model: Some("test-model".to_owned()),
        max_tokens: None,
        thinking: None,
        thinking_budget: None,
        max_turns: None,
        max_tool_call_malformed_turns: None,
        max_tool_call_failure_turns: None,
        system_prompt: None,
        profile: None,
        auto_approve: true,
        project_dir: Some(workspace.path().to_path_buf()),
    })
    .unwrap();
    config.session.directory = workspace.path().join("sessions").to_string_lossy().into_owned();
    let mut bootstrap = AgentBootstrap::new(config, workspace.path().to_string_lossy(), Arc::new(NullSink));
    bootstrap.initialize_persistent_runtime(workspace.path()).unwrap();
    assert_eq!(bootstrap.permission_context.mode(), PermissionMode::Auto);
    let marker = workspace.path().join("must-not-launch");
    #[cfg(windows)]
    let command = format!(
        "Set-Content -LiteralPath '{}' -Value launched",
        marker.to_string_lossy().replace('\'', "''")
    );
    #[cfg(not(windows))]
    let command = format!("printf launched > '{}'", marker.to_string_lossy());
    let input = json!({"cmd": command});
    let tool = ExecCommandTool::new(workspace.path().to_path_buf());
    let policy = ProcessLaunchPolicy::workspace_sandbox(
        workspace.path(),
        bootstrap.permission_context.protected_paths_snapshot(),
    );
    let context = tool
        .prepare_effect("default-layout-effect", &input)
        .unwrap()
        .into_parts()
        .1
        .with_process_launch_policy(policy.clone())
        .with_process_spawn_authorization(ProcessSpawnAuthorization::new(Arc::new(StrictPolicySpawnAuthorizer(
            policy,
        ))));

    let result = tool.prepare_execution(input, context).unwrap().execute().await;

    assert!(result.is_error);
    assert!(!marker.exists());
}
