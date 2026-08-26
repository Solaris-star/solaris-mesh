use super::*;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use serde_json::json;
    use solaris_process::{
        ManagedChild, ProcessLaunchPolicy, ProcessSpawn, ProcessSpawnAuthorization, ProcessSpawnAuthorizer,
    };

    struct TestSpawnAuthorizer(ProcessLaunchPolicy);

    impl ProcessSpawnAuthorizer for TestSpawnAuthorizer {
        fn authorize_and_spawn(&self, spawn: ProcessSpawn) -> std::io::Result<ManagedChild> {
            spawn(self.0.clone())
        }
    }

    struct NetworkProxyUnavailableAuthorizer(solaris_process::SandboxReport);

    impl ProcessSpawnAuthorizer for NetworkProxyUnavailableAuthorizer {
        fn authorize_and_spawn(&self, _spawn: ProcessSpawn) -> std::io::Result<ManagedChild> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                solaris_process::SandboxError::NetworkProxyUnavailable { report: self.0 },
            ))
        }
    }

    fn authorized_context(context: ToolExecutionContext, policy: ProcessLaunchPolicy) -> ToolExecutionContext {
        context
            .with_process_launch_policy(policy.clone())
            .with_process_spawn_authorization(ProcessSpawnAuthorization::new(Arc::new(TestSpawnAuthorizer(policy))))
    }

    async fn execute_approved(tool: &ExecCommandTool, input: Value) -> ToolResult {
        let context = match tool.prepare_effect("test-approved-effect", &input) {
            Ok(prepared) => authorized_context(prepared.into_parts().1, ProcessLaunchPolicy::Ambient),
            Err(error) => {
                return ToolResult {
                    content: error,
                    is_error: true,
                };
            }
        };
        match tool.prepare_execution(input, context) {
            Ok(prepared) => prepared.execute().await,
            Err(error) => ToolResult {
                content: error,
                is_error: true,
            },
        }
    }

    #[test]
    fn description_warns_about_powershell_statement_chaining() {
        let tool = ExecCommandTool::new(std::env::temp_dir());

        assert!(tool.description().contains("PowerShell"));
        assert!(tool.description().contains("assignments or control-flow statements"));
    }

    #[tokio::test]
    async fn direct_execute_rejects_the_ambient_compatibility_path() {
        let tool = ExecCommandTool::new(std::env::temp_dir());

        let result = tool.execute(json!({"cmd": "echo must-not-run"})).await;

        assert!(result.is_error);
        assert_eq!(
            result.content,
            "ExecCommand requires an approved process spawn authorization"
        );
    }

    #[tokio::test]
    async fn execute_echo_returns_stdout() {
        let tool = ExecCommandTool::new(std::env::temp_dir());
        let input = json!({"cmd": "echo hello_exec_command"});
        let result = execute_approved(&tool, input).await;
        assert!(!result.is_error, "unexpected error: {}", result.content);
        assert!(result.content.contains("hello_exec_command"));
    }

    #[tokio::test]
    async fn execute_invalid_command_returns_error() {
        let tool = ExecCommandTool::new(std::env::temp_dir());
        let input = json!({"cmd": "nonexistent_command_xyz_123"});
        let result = execute_approved(&tool, input).await;
        assert!(result.is_error);
    }

    #[tokio::test]
    async fn unavailable_network_proxy_is_denied_with_typed_report_metadata() {
        let tool = ExecCommandTool::new(std::env::temp_dir());
        let input = json!({"cmd": "echo must-not-run"});
        let report = solaris_process::SandboxReport::new(
            solaris_process::SandboxEnforcement::Unavailable,
            solaris_process::SandboxBackend::WindowsAppContainer,
            solaris_process::SandboxReason::NetworkProxyUnavailable,
        );
        let context = tool
            .prepare_effect("sandbox-report", &input)
            .unwrap()
            .into_parts()
            .1
            .with_process_launch_policy(ProcessLaunchPolicy::Ambient)
            .with_process_spawn_authorization(ProcessSpawnAuthorization::new(Arc::new(
                NetworkProxyUnavailableAuthorizer(report),
            )));

        let result = tool
            .prepare_execution(input, context)
            .unwrap()
            .execute_classified()
            .await;

        assert_eq!(result.status, ToolResultStatus::Denied);
        assert_eq!(
            result.metadata.and_then(|metadata| metadata.sandbox_report),
            Some(report)
        );
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_network_policy_denies_exec_command_before_the_marker_is_created() {
        let workspace = tempfile::tempdir().unwrap();
        let marker = workspace.path().join("must-not-start");
        let command = format!(
            "Set-Content -LiteralPath '{}' -Value started",
            marker.to_string_lossy().replace('\'', "''")
        );
        let input = json!({
            "cmd": command,
            "shell": "powershell",
            "network_domains": ["https://allowed.example.test"]
        });
        let policy = ProcessLaunchPolicy::workspace_sandbox_with_network(
            workspace.path(),
            [],
            [],
            ["https://allowed.example.test"],
        )
        .unwrap();
        let tool = ExecCommandTool::new(workspace.path().to_path_buf());
        let context = authorized_context(
            tool.prepare_effect("windows-network-marker", &input)
                .unwrap()
                .into_parts()
                .1,
            policy,
        );

        let result = tool
            .prepare_execution(input, context)
            .unwrap()
            .execute_classified()
            .await;

        assert_eq!(result.status, ToolResultStatus::Denied);
        assert_eq!(
            result.metadata.and_then(|metadata| metadata.sandbox_report),
            Some(solaris_process::SandboxReport::new(
                solaris_process::SandboxEnforcement::Unavailable,
                solaris_process::SandboxBackend::WindowsAppContainer,
                solaris_process::SandboxReason::NetworkProxyUnavailable,
            ))
        );
        assert!(!marker.exists(), "network-bearing ExecCommand reached target spawn");
    }

    #[tokio::test]
    async fn invalid_shell_error_does_not_expose_requested_path() {
        let sentinel = "super-secret-token-shell";
        let shell = std::env::temp_dir().join(sentinel).join("missing-shell");
        let tool = ExecCommandTool::new(std::env::temp_dir());

        let result = execute_approved(&tool, json!({"cmd": "echo safe", "shell": shell})).await;

        assert!(result.is_error);
        assert!(!result.content.contains(sentinel));
        assert!(result.content.contains("Invalid shell selection"));
        assert!(result.content.contains("sha256:"));
    }

    #[tokio::test]
    async fn execute_respects_cwd() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("cwd_proof.txt"), "proof").unwrap();
        let tool = ExecCommandTool::new(dir.path().to_path_buf());
        let cmd = if cfg!(windows) {
            "type cwd_proof.txt"
        } else {
            "cat cwd_proof.txt"
        };
        let input = json!({"cmd": cmd});
        let result = execute_approved(&tool, input).await;
        assert!(!result.is_error, "unexpected error: {}", result.content);
        assert!(
            result.content.contains("proof"),
            "ExecCommandTool should execute in injected cwd, got: {}",
            result.content
        );
    }

    #[tokio::test]
    async fn execute_injects_runtime_env() {
        let tool = ExecCommandTool::new_with_env(
            std::env::temp_dir(),
            vec![("SOLARIS_MAX_ACTIVE_AGENTS".to_string(), "4".to_string())],
        );
        #[cfg(windows)]
        let input = json!({"cmd": "Write-Output $env:SOLARIS_MAX_ACTIVE_AGENTS", "shell": "powershell"});
        #[cfg(not(windows))]
        let input = json!({"cmd": "printf '%s' \"$SOLARIS_MAX_ACTIVE_AGENTS\"", "shell": "sh"});

        let result = execute_approved(&tool, input).await;

        assert!(!result.is_error, "unexpected error: {}", result.content);
        assert!(result.content.contains('4'));
    }

    #[tokio::test]
    async fn execute_does_not_inherit_secret_runtime_env() {
        let sentinel = "must-not-reach-child";
        let tool = ExecCommandTool::new_with_env(
            std::env::temp_dir(),
            vec![
                ("API_KEY".to_owned(), sentinel.to_owned()),
                ("AWS_SECRET_ACCESS_KEY".to_owned(), sentinel.to_owned()),
                ("GH_TOKEN".to_owned(), sentinel.to_owned()),
                ("SOLARIS_MAX_PRIVATE_TOKEN".to_owned(), sentinel.to_owned()),
            ],
        );
        #[cfg(windows)]
        let input = json!({
            "cmd": "Write-Output \"$env:API_KEY|$env:AWS_SECRET_ACCESS_KEY|$env:GH_TOKEN|$env:SOLARIS_MAX_PRIVATE_TOKEN\"",
            "shell": "powershell"
        });
        #[cfg(not(windows))]
        let input = json!({
            "cmd": "printf '%s|%s|%s|%s' \"$API_KEY\" \"$AWS_SECRET_ACCESS_KEY\" \"$GH_TOKEN\" \"$SOLARIS_MAX_PRIVATE_TOKEN\"",
            "shell": "sh"
        });

        let result = execute_approved(&tool, input).await;

        assert!(!result.is_error, "unexpected error: {}", result.content);
        assert!(!result.content.contains(sentinel));
    }

    #[tokio::test]
    async fn prepared_execution_rejects_shell_replaced_after_approval() {
        let source = solaris_config::shell::resolve_shell(Some("auto")).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let sentinel = "super-secret-token-shell";
        let secret_directory = directory.path().join(sentinel);
        std::fs::create_dir(&secret_directory).unwrap();
        let copied = secret_directory.join(source.path.file_name().unwrap());
        std::fs::copy(&source.path, &copied).unwrap();
        let tool = ExecCommandTool::new(directory.path().to_path_buf());
        let input = json!({"cmd": "echo should-not-run", "shell": copied});
        let (effect, execution) = tool
            .prepare_effect("approved-effect", &input)
            .expect("approval should capture executable identity")
            .into_parts();
        let execution = authorized_context(execution, ProcessLaunchPolicy::Ambient);
        assert!(
            effect
                .resources
                .external_resources
                .iter()
                .any(|resource| resource.starts_with("exec-shell-executable:sha256:"))
        );
        std::fs::write(&copied, b"replaced shell bytes").unwrap();

        let error = match tool.prepare_execution(input, execution) {
            Ok(_) => panic!("replacement must be rejected before intent"),
            Err(error) => error,
        };

        assert!(error.contains("changed before execution"));
        assert!(!error.contains(sentinel));
    }

    #[test]
    fn effect_aware_execution_rejects_missing_launch_policy() {
        let directory = tempfile::tempdir().unwrap();
        let marker = directory.path().join("must-not-launch");
        #[cfg(windows)]
        let command = format!(
            "Set-Content -LiteralPath '{}' -Value launched",
            marker.to_string_lossy().replace('\'', "''")
        );
        #[cfg(not(windows))]
        let command = format!("printf launched > '{}'", marker.to_string_lossy());
        let tool = ExecCommandTool::new(directory.path().to_path_buf());
        let input = json!({"cmd": command});
        let context = tool.prepare_effect("approved-effect", &input).unwrap().into_parts().1;

        let error = match tool.prepare_execution(input, context) {
            Ok(_) => panic!("missing engine launch policy must be rejected"),
            Err(error) => error,
        };

        assert_eq!(error, "approved process launch policy is missing");
        assert!(!marker.exists());
    }

    #[test]
    fn effect_aware_execution_rejects_missing_spawn_authorization() {
        let directory = tempfile::tempdir().unwrap();
        let marker = directory.path().join("must-not-launch");
        #[cfg(windows)]
        let command = format!(
            "Set-Content -LiteralPath '{}' -Value launched",
            marker.to_string_lossy().replace('\'', "''")
        );
        #[cfg(not(windows))]
        let command = format!("printf launched > '{}'", marker.to_string_lossy());
        let tool = ExecCommandTool::new(directory.path().to_path_buf());
        let input = json!({"cmd": command});
        let context = tool
            .prepare_effect("approved-effect", &input)
            .unwrap()
            .into_parts()
            .1
            .with_process_launch_policy(ProcessLaunchPolicy::Ambient);

        let error = match tool.prepare_execution(input, context) {
            Ok(_) => panic!("missing process spawn authorization must be rejected"),
            Err(error) => error,
        };

        assert_eq!(error, "approved process spawn authorization is missing");
        assert!(!marker.exists());
    }

    #[cfg(any(windows, target_os = "macos"))]
    #[tokio::test]
    async fn workspace_launch_policy_writes_only_inside_workspace() {
        let workspace = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let workspace_marker = workspace.path().join("workspace-marker");
        let outside_marker = outside.path().join("outside-marker");
        #[cfg(windows)]
        let command = format!(
            "Set-Content -LiteralPath '{}' -Value denied; Set-Content -LiteralPath '{}' -Value launched",
            outside_marker.to_string_lossy().replace('\'', "''"),
            workspace_marker.to_string_lossy().replace('\'', "''")
        );
        #[cfg(target_os = "macos")]
        let command = format!(
            "printf denied > '{}'; printf launched > '{}'",
            outside_marker.to_string_lossy().replace('\'', "'\\''"),
            workspace_marker.to_string_lossy().replace('\'', "'\\''")
        );
        let tool = ExecCommandTool::new(workspace.path().to_path_buf());
        let input = json!({"cmd": command});
        let policy = ProcessLaunchPolicy::workspace_sandbox(workspace.path(), [state.path().to_path_buf()]);
        let context = authorized_context(
            tool.prepare_effect("approved-effect", &input).unwrap().into_parts().1,
            policy,
        );

        let result = tool.prepare_execution(input, context).unwrap().execute().await;

        assert!(!result.is_error, "{}", result.content);
        assert!(workspace_marker.exists());
        assert!(!outside_marker.exists());
    }

    #[tokio::test]
    async fn execute_terminates_when_process_output_exceeds_budget() {
        let tool = ExecCommandTool::new_with_env(
            std::env::temp_dir(),
            vec![("SOLARIS_MAX_PROCESS_OUTPUT_BYTES".to_owned(), "1024".to_owned())],
        );
        #[cfg(windows)]
        let input = json!({
            "cmd": "while ($true) { [Console]::Out.Write('0123456789') }",
            "shell": "powershell",
            "timeout": 10000
        });
        #[cfg(not(windows))]
        let input = json!({
            "cmd": "while :; do printf '0123456789'; done",
            "shell": "sh",
            "timeout": 10000
        });

        let result = execute_approved(&tool, input).await;

        assert!(result.is_error);
        assert!(result.content.contains("output exceeded 1024 bytes"));
        assert!(result.content.len() < 4096);
    }

    #[test]
    fn effect_declares_resolved_shell_executable_and_exact_argv() {
        let tool = ExecCommandTool::new(std::env::temp_dir());
        let resolved = solaris_config::shell::resolve_shell(Some("auto")).unwrap();
        let expected_identity = solaris_process::inspect_executable(&resolved.path).unwrap();
        let first = tool.describe_effect(&json!({"cmd": "echo first"}));
        let second = tool.describe_effect(&json!({"cmd": "echo second"}));
        assert_eq!(first.resources.process_invocations.len(), 1);
        assert!(first.resources.process_invocations[0].executable.starts_with("sha256:"));
        assert_eq!(
            first.resources.process_invocations[0].executable,
            expected_identity.path_digest()
        );
        assert_ne!(
            first.resources.process_invocations[0].argv,
            second.resources.process_invocations[0].argv
        );
        assert!(first.resources.unrestricted_file_reads);
        assert!(first.resources.unrestricted_file_writes);
        assert!(!first.resources.unrestricted_network);
        assert!(first.resources.unrestricted_process);
    }

    #[test]
    fn effect_declares_only_explicit_network_domains() {
        let tool = ExecCommandTool::new(std::env::temp_dir());
        let offline = tool.describe_effect(&json!({"cmd": "echo offline"}));
        let online = tool.describe_effect(&json!({
            "cmd": "echo online",
            "network_domains": ["https://api.example.test/v1"]
        }));

        assert!(offline.resources.network_domains.is_empty());
        assert!(!offline.resources.unrestricted_network);
        assert_eq!(online.resources.network_domains, ["https://api.example.test/v1"]);
        assert!(!online.resources.unrestricted_network);
    }

    #[test]
    fn effect_audit_projection_hides_command_and_argv_but_keeps_stable_identity() {
        let tool = ExecCommandTool::new(std::env::temp_dir());
        let secret = "super-secret-token";
        let descriptor = tool.describe_effect(&json!({
            "cmd": format!("deploy --token {secret}"),
            "shell": "auto"
        }));

        assert!(descriptor.resources.process_commands[0].contains(secret));
        assert!(
            descriptor.resources.process_invocations[0]
                .argv
                .iter()
                .any(|argument| argument.contains(secret))
        );

        let first = solaris_types::effect::EffectAuditProjection::from_descriptor(&descriptor);
        let second = solaris_types::effect::EffectAuditProjection::from_descriptor(&descriptor);
        let serialized = serde_json::to_string(&first).unwrap();

        assert_eq!(first, second);
        assert!(!serialized.contains(secret));
        assert!(!serialized.contains("deploy --token"));
        assert!(first.descriptor_digest.starts_with("sha256:"));
        assert_eq!(first.executables.len(), 1);
        assert!(!first.executables[0].executable_digest.is_empty());
        assert!(first.executables[0].argv_count >= 2);
    }

    #[tokio::test]
    async fn execute_timeout_preserves_stdout_emitted_before_timeout() {
        let tool = ExecCommandTool::new(std::env::temp_dir());
        #[cfg(windows)]
        let cmd = "echo solaris_stdout_before_timeout & ping -t 127.0.0.1 >nul";
        #[cfg(not(windows))]
        let cmd = "printf 'solaris_stdout_before_timeout\\n'; sleep 5";
        let input = json!({
            "cmd": cmd,
            "shell": if cfg!(windows) { "cmd" } else { "sh" },
            "timeout": 1500
        });

        let result = execute_approved(&tool, input).await;

        assert!(result.is_error, "timeout should be an error: {}", result.content);
        assert!(
            result.content.contains("Command timed out after 1500ms"),
            "timeout message missing: {}",
            result.content
        );
        assert!(
            result.content.contains("STDOUT:\n") && result.content.contains("solaris_stdout_before_timeout"),
            "stdout emitted before timeout should be preserved, got: {}",
            result.content
        );
    }

    #[tokio::test]
    async fn prepared_execution_timeout_has_timeout_status() {
        let tool = ExecCommandTool::new(std::env::temp_dir());
        #[cfg(windows)]
        let input = json!({
            "cmd": "Start-Sleep -Seconds 5",
            "shell": "powershell",
            "timeout": 10
        });
        #[cfg(not(windows))]
        let input = json!({"cmd": "sleep 5", "shell": "sh", "timeout": 10});
        let context = authorized_context(
            tool.prepare_effect("timeout-status", &input).unwrap().into_parts().1,
            ProcessLaunchPolicy::Ambient,
        );

        let result = tool
            .prepare_execution(input, context)
            .unwrap()
            .execute_classified()
            .await;

        assert_eq!(result.status, solaris_types::tool::ToolResultStatus::Timeout);
    }

    #[tokio::test]
    async fn execute_timeout_preserves_stderr_emitted_before_timeout() {
        let tool = ExecCommandTool::new(std::env::temp_dir());
        #[cfg(windows)]
        let cmd = "echo solaris_stderr_before_timeout 1>&2 & ping -t 127.0.0.1 >nul";
        #[cfg(not(windows))]
        let cmd = "printf 'solaris_stderr_before_timeout\\n' >&2; sleep 5";
        let input = json!({
            "cmd": cmd,
            "shell": if cfg!(windows) { "cmd" } else { "sh" },
            "timeout": 1500
        });

        let result = execute_approved(&tool, input).await;

        assert!(result.is_error, "timeout should be an error: {}", result.content);
        assert!(
            result.content.contains("Command timed out after 1500ms"),
            "timeout message missing: {}",
            result.content
        );
        assert!(
            result.content.contains("STDERR:\n") && result.content.contains("solaris_stderr_before_timeout"),
            "stderr emitted before timeout should be preserved, got: {}",
            result.content
        );
    }

    #[tokio::test]
    async fn execute_timeout_omits_output_after_timeout() {
        let tool = ExecCommandTool::new(std::env::temp_dir());
        #[cfg(windows)]
        let cmd = "echo solaris_before_timeout & ping -n 6 127.0.0.1 >nul & echo solaris_after_timeout";
        #[cfg(not(windows))]
        let cmd = "printf 'solaris_before_timeout\\n'; sleep 5; printf 'solaris_after_timeout\\n'";
        let input = json!({
            "cmd": cmd,
            "shell": if cfg!(windows) { "cmd" } else { "sh" },
            "timeout": 1500
        });

        let result = execute_approved(&tool, input).await;

        assert!(result.is_error, "timeout should be an error: {}", result.content);
        assert!(
            result.content.contains("Command timed out after 1500ms"),
            "timeout message missing: {}",
            result.content
        );
        assert!(
            result.content.contains("solaris_before_timeout"),
            "output emitted before timeout should be preserved, got: {}",
            result.content
        );
        assert!(
            !result.content.contains("solaris_after_timeout"),
            "output after timeout should not be present, got: {}",
            result.content
        );
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn execute_powershell_write_output_returns_stdout() {
        let tool = ExecCommandTool::new(std::env::temp_dir());
        let input = json!({
            "cmd": "Write-Output solaris_powershell_stdout_probe",
            "shell": "powershell"
        });

        let result = execute_approved(&tool, input).await;

        assert!(!result.is_error, "unexpected error: {}", result.content);
        assert!(
            result.content.contains("STDOUT:\n") && result.content.contains("solaris_powershell_stdout_probe"),
            "PowerShell stdout should be preserved, got: {}",
            result.content
        );
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn execute_powershell_echo_quoted_message_returns_stdout() {
        let tool = ExecCommandTool::new(std::env::temp_dir());
        let input = json!({
            "cmd": "echo \"message\"",
            "shell": "powershell"
        });

        let result = execute_approved(&tool, input).await;

        assert!(!result.is_error, "unexpected error: {}", result.content);
        assert!(
            result.content.contains("STDOUT:\n") && result.content.contains("message"),
            "PowerShell quoted echo stdout should be preserved, got: {}",
            result.content
        );
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn execute_cmd_echo_returns_stdout() {
        let tool = ExecCommandTool::new(std::env::temp_dir());
        let input = json!({
            "cmd": "echo solaris_cmd_stdout_probe",
            "shell": "cmd"
        });

        let result = execute_approved(&tool, input).await;

        assert!(!result.is_error, "unexpected error: {}", result.content);
        assert!(
            result.content.contains("STDOUT:\n") && result.content.contains("solaris_cmd_stdout_probe"),
            "cmd stdout should be preserved, got: {}",
            result.content
        );
    }
}
