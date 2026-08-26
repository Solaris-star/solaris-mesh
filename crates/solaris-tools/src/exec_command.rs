use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use solaris_config::shell::{ResolvedShell, resolve_shell};
use solaris_process::{
    CommandRunner, DEFAULT_MAX_PROCESS_OUTPUT_BYTES, ExecutableIdentity, executable_path_identity,
    filter_resource_environment, inspect_executable, pin_executable, process_outcome_unknown,
    process_recovery_required, sandbox_report_from_error,
};
use solaris_protocol::events::ToolCategory;
use solaris_types::effect::{EffectClass, EffectDescriptor, EffectReplayPolicy, ProcessInvocation, ResourceFootprint};
use solaris_types::plugin::ImplementationIdentity;
use solaris_types::tool::{ClassifiedToolResult, JsonSchema, ToolResult, ToolResultMetadata, ToolResultStatus};

use crate::{PreparedToolEffect, PreparedToolExecution, Tool, ToolExecutionContext};

const DEFAULT_TIMEOUT_MS: u64 = 120_000;
const MAX_TIMEOUT_MS: u64 = 600_000;

pub struct ExecCommandTool {
    cwd: PathBuf,
    runtime_env: HashMap<String, String>,
    #[cfg(feature = "sandbox-test-fixtures")]
    process_failure_fixtures: HashMap<String, String>,
}

fn shell_identity(shell: &ResolvedShell) -> Result<ExecutableIdentity, String> {
    let canonical = shell.path.canonicalize().map_err(|_| {
        format!(
            "shell executable resolution failed for identity {}",
            executable_path_identity(&shell.path)
        )
    })?;
    inspect_executable(&canonical).map_err(|error| error.to_string())
}

fn effect_descriptor(
    cwd: &std::path::Path,
    input: &Value,
    arguments: Vec<String>,
    identity: &ExecutableIdentity,
) -> EffectDescriptor {
    let command = input.get("cmd").and_then(Value::as_str).unwrap_or("");
    let mut resources = ResourceFootprint {
        process_commands: vec![command.to_owned()],
        process_invocations: vec![ProcessInvocation {
            executable: identity.path_digest().to_owned(),
            argv: arguments,
        }],
        file_reads: vec![cwd.to_string_lossy().into_owned()],
        file_writes: vec![cwd.to_string_lossy().into_owned()],
        external_resources: vec![format!("exec-shell-executable:sha256:{}", identity.content_digest())],
        network_domains: requested_network_domains(input),
        ..Default::default()
    };
    resources.declare_sandboxed_process_access();
    EffectDescriptor {
        class: EffectClass::Process,
        action: format!("Execute command in {}", cwd.display()),
        resources,
        replay_policy: EffectReplayPolicy::ReconcileRequired,
    }
}

fn unavailable_effect_descriptor(cwd: &std::path::Path, input: &Value) -> EffectDescriptor {
    let command = input.get("cmd").and_then(Value::as_str).unwrap_or("");
    let mut resources = ResourceFootprint {
        process_commands: vec![command.to_owned()],
        file_reads: vec![cwd.to_string_lossy().into_owned()],
        file_writes: vec![cwd.to_string_lossy().into_owned()],
        network_domains: requested_network_domains(input),
        ..Default::default()
    };
    resources.declare_sandboxed_process_access();
    EffectDescriptor {
        class: EffectClass::Process,
        action: format!("Execute command in {}", cwd.display()),
        resources,
        replay_policy: EffectReplayPolicy::ReconcileRequired,
    }
}

fn requested_network_domains(input: &Value) -> Vec<String> {
    input
        .get("network_domains")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect()
}

fn safe_error_digest(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"solaris.exec-command/error/v1\0");
    hasher.update(value.as_bytes());
    format!("sha256:{:x}", hasher.finalize())
}

impl ExecCommandTool {
    pub fn new(cwd: PathBuf) -> Self {
        Self {
            cwd,
            runtime_env: HashMap::new(),
            #[cfg(feature = "sandbox-test-fixtures")]
            process_failure_fixtures: HashMap::new(),
        }
    }

    pub fn new_with_env(cwd: PathBuf, runtime_env: Vec<(String, String)>) -> Self {
        Self {
            cwd,
            runtime_env: filter_resource_environment(runtime_env).into_iter().collect(),
            #[cfg(feature = "sandbox-test-fixtures")]
            process_failure_fixtures: HashMap::new(),
        }
    }

    #[cfg(feature = "sandbox-test-fixtures")]
    pub fn with_process_failure_fixtures_for_test(
        mut self,
        fixtures: impl IntoIterator<Item = (String, String)>,
    ) -> Self {
        self.process_failure_fixtures = fixtures.into_iter().collect();
        self
    }
}

fn render_exit_result(exit_code: i32, stdout: &[u8], stderr: &[u8]) -> String {
    let stdout = String::from_utf8_lossy(stdout);
    let stderr = String::from_utf8_lossy(stderr);
    format!("Exit code: {exit_code}\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}")
}

fn render_timeout_result(timeout_ms: u64, stdout: &[u8], stderr: &[u8]) -> String {
    let stdout = String::from_utf8_lossy(stdout);
    let stderr = String::from_utf8_lossy(stderr);
    format!("Command timed out after {timeout_ms}ms\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}")
}

fn render_output_limit_result(limit: usize, stdout: &[u8], stderr: &[u8]) -> String {
    let stdout = String::from_utf8_lossy(stdout);
    let stderr = String::from_utf8_lossy(stderr);
    format!(
        "Command output exceeded {limit} bytes and the process tree was terminated\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}"
    )
}

#[async_trait]
impl Tool for ExecCommandTool {
    fn name(&self) -> &str {
        "ExecCommand"
    }

    fn description(&self) -> &str {
        "Executes a shell command and returns its output.\n\n\
         IMPORTANT: Do NOT use ExecCommand when a dedicated tool is available:\n\
         - File search: use Glob (not find or ls)\n\
         - Content search: use Grep (not grep or rg)\n\
         - Read files: use Read (not cat, head, or tail)\n\
         - Edit files: use Edit (not sed or awk)\n\
         - Write files: use Write (not echo or cat with heredoc)\n\n\
         # Instructions\n\
         - Use absolute paths to avoid working directory confusion.\n\
         - When issuing multiple independent commands, make parallel tool calls \
         instead of chaining them. Use `&&` only when commands depend on each other.\n\
         - Follow the displayed shell syntax. In PowerShell, separate assignments or control-flow statements \
         with `;` or newlines; do not place them after `&&`.\n\
         - You may specify an optional timeout in milliseconds (default 120000, max 600000).\n\n\
         # Git safety\n\
         - Never force push, reset --hard, or use --no-verify unless explicitly asked.\n\
         - Prefer creating new commits over amending existing ones."
    }

    fn input_schema(&self) -> JsonSchema {
        json!({
            "type": "object",
            "properties": {
                "cmd": {
                    "type": "string",
                    "description": "The command to execute"
                },
                "shell": {
                    "type": "string",
                    "description": "Optional shell override: auto, powershell, pwsh, cmd, bash, zsh, sh, or an executable path"
                },
                "timeout": {
                    "type": "integer",
                    "description": "Timeout in milliseconds (default 120000, max 600000)"
                },
                "network_domains": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "Exact HTTP(S) domains required by this command in Auto mode; direct network remains denied",
                    "maxItems": 32
                }
            },
            "required": ["cmd"]
        })
    }

    fn is_concurrency_safe(&self, _input: &Value) -> bool {
        false
    }

    async fn execute(&self, _input: Value) -> ToolResult {
        ToolResult {
            content: "ExecCommand requires an approved process spawn authorization".to_owned(),
            is_error: true,
        }
    }

    fn prepare_effect(&self, effect_id: &str, input: &Value) -> Result<PreparedToolEffect, String> {
        let shell = resolve_shell(input["shell"].as_str())
            .map_err(|error| format!("Invalid shell selection ({})", safe_error_digest(&error.to_string())))?;
        let identity = shell_identity(&shell)?;
        let arguments = shell.derive_exec_args(input.get("cmd").and_then(Value::as_str).unwrap_or(""), false);
        let descriptor = effect_descriptor(&self.cwd, input, arguments.clone(), &identity);
        Ok(PreparedToolEffect::new(
            descriptor,
            ToolExecutionContext::new(effect_id)
                .with_approved_process(identity, arguments, shell.kind.name().to_owned())
                .with_sandbox_workspace_root(&self.cwd),
        ))
    }

    fn prepare_execution<'a>(
        &'a self,
        input: Value,
        context: ToolExecutionContext,
    ) -> Result<PreparedToolExecution<'a>, String> {
        let _command = input["cmd"]
            .as_str()
            .ok_or_else(|| "Missing required parameter: cmd".to_owned())?;
        let expected_identity = context
            .approved_executable()
            .ok_or_else(|| "approved executable identity is missing".to_owned())?;
        let arguments = context
            .approved_process_arguments()
            .ok_or_else(|| "approved process arguments are missing".to_owned())?
            .to_vec();
        let shell_kind = context
            .approved_executable_kind()
            .ok_or_else(|| "approved executable kind is missing".to_owned())?
            .to_owned();
        let launch_policy = context
            .process_launch_policy()
            .cloned()
            .ok_or_else(|| "approved process launch policy is missing".to_owned())?;
        let spawn_authorization = context
            .process_spawn_authorization()
            .cloned()
            .ok_or_else(|| "approved process spawn authorization is missing".to_owned())?;
        let pinned =
            pin_executable(expected_identity.canonical_path(), expected_identity).map_err(|error| error.to_string())?;
        let actual_identity = pinned.identity().clone();
        tracing::info!(
            shell_kind,
            shell_path_identity = actual_identity.path_digest(),
            shell_content_identity = actual_identity.content_digest(),
            cwd_identity = %safe_error_digest(&self.cwd.to_string_lossy()),
            "ExecCommandTool executing"
        );

        let timeout_ms = input["timeout"]
            .as_u64()
            .unwrap_or(DEFAULT_TIMEOUT_MS)
            .min(MAX_TIMEOUT_MS);

        let timeout = Duration::from_millis(timeout_ms);

        let cwd = self.cwd.clone();
        let mut command_builder = pinned.command().map_err(|error| error.to_string())?;
        command_builder
            .args(arguments)
            .reset_to_safe_environment_with_overrides(&self.runtime_env)
            .current_dir(&cwd);
        #[cfg(feature = "sandbox-test-fixtures")]
        command_builder.envs(&self.process_failure_fixtures);

        let max_output_bytes = self
            .runtime_env
            .get("SOLARIS_MAX_PROCESS_OUTPUT_BYTES")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(DEFAULT_MAX_PROCESS_OUTPUT_BYTES)
            .max(1);
        let implementation = ImplementationIdentity {
            implementation_id: format!("exec-shell:{}", actual_identity.path_digest()),
            version: None,
            digest: Some(actual_identity.content_digest().to_owned()),
        };
        Ok(PreparedToolExecution::new_classified(
            Some(implementation),
            Box::pin(async move {
                let result = CommandRunner::new_pinned(command_builder)
                    .launch_policy(launch_policy)
                    .spawn_authorizer(spawn_authorization)
                    .timeout(timeout)
                    .max_output_bytes(max_output_bytes)
                    .run()
                    .await;

                match result {
                    Ok(result) if result.output_limit_exceeded => ClassifiedToolResult::new(
                        render_output_limit_result(max_output_bytes, &result.stdout, &result.stderr),
                        ToolResultStatus::Failed,
                    ),
                    Ok(result) if result.timed_out => ClassifiedToolResult::new(
                        render_timeout_result(timeout_ms, &result.stdout, &result.stderr),
                        ToolResultStatus::Timeout,
                    ),
                    Ok(result) => {
                        let exit_code = result.exit_code.unwrap_or(-1);
                        let status = if exit_code == 0 {
                            ToolResultStatus::Executed
                        } else {
                            ToolResultStatus::Failed
                        };
                        ClassifiedToolResult::new(render_exit_result(exit_code, &result.stdout, &result.stderr), status)
                    }
                    Err(err) => {
                        let sandbox_report = sandbox_report_from_error(&err);
                        let result = match process_recovery_required(&err) {
                            Some(recovery) => ClassifiedToolResult::new(
                                format!(
                                    "Command outcome requires reconciliation (ref={}, kind={})",
                                    recovery.reference(),
                                    recovery.kind().as_str()
                                ),
                                ToolResultStatus::OutcomeUnknown,
                            ),
                            None if process_outcome_unknown(&err) => ClassifiedToolResult::new(
                                "Command outcome is unknown and requires reconciliation".to_owned(),
                                ToolResultStatus::OutcomeUnknown,
                            ),
                            None if sandbox_report.is_some() => ClassifiedToolResult::new(
                                "Command execution denied because strict sandbox enforcement is unavailable".to_owned(),
                                ToolResultStatus::Denied,
                            ),
                            None => ClassifiedToolResult::new(
                                format!(
                                    "Failed to execute approved command ({})",
                                    safe_error_digest(&err.to_string())
                                ),
                                ToolResultStatus::Failed,
                            ),
                        };
                        match sandbox_report {
                            Some(report) => result.with_metadata(ToolResultMetadata::sandbox_report(report)),
                            None => result,
                        }
                    }
                }
            }),
        ))
    }

    fn describe_effect(&self, input: &Value) -> EffectDescriptor {
        let Ok(shell) = resolve_shell(input.get("shell").and_then(Value::as_str)) else {
            return unavailable_effect_descriptor(&self.cwd, input);
        };
        let Ok(identity) = shell_identity(&shell) else {
            return unavailable_effect_descriptor(&self.cwd, input);
        };
        effect_descriptor(
            &self.cwd,
            input,
            shell.derive_exec_args(input.get("cmd").and_then(Value::as_str).unwrap_or(""), false),
            &identity,
        )
    }

    fn revalidate_effect(&self, input: &Value, context: &ToolExecutionContext) -> EffectDescriptor {
        let Some(identity) = context.approved_executable() else {
            return unavailable_effect_descriptor(&self.cwd, input);
        };
        let Some(arguments) = context.approved_process_arguments() else {
            return unavailable_effect_descriptor(&self.cwd, input);
        };
        effect_descriptor(&self.cwd, input, arguments.to_vec(), identity)
    }

    fn category(&self) -> ToolCategory {
        ToolCategory::Exec
    }

    fn describe(&self, input: &Value) -> String {
        let cmd = input.get("cmd").and_then(|v| v.as_str()).unwrap_or("");
        format!("Execute: {}", crate::truncate_utf8(cmd, 80))
    }
}

#[cfg(test)]
#[path = "exec_command_test.rs"]
mod exec_command_test;
