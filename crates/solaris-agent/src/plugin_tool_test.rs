use super::*;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::OnceLock;

use solaris_compact::CompactLevel;
use solaris_tools::registry::ToolRegistry;
use solaris_types::effect::{EffectReplayPolicy, ResourceFootprint};
use solaris_types::identity::{AgentId, RunId};
use solaris_types::message::ContentBlock;
use solaris_types::permission::{PermissionCeiling, PermissionMode};
use solaris_types::runtime::OperationEnvironmentSnapshot;

use crate::confirm::ToolConfirmer;
use crate::orchestration::execute_tool_calls_with_policy_context;
use crate::permission_engine::PermissionContext;
use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};

const PLUGIN_PROVIDER_HELPER_ARGUMENT: &str = "--solaris-plugin-provider-test-helper";
const PLUGIN_DESCENDANT_WAIT_HELPER_ARGUMENT: &str = "--solaris-plugin-descendant-wait-test-helper";
const PLUGIN_DESCENDANT_OUTPUT_HELPER_ARGUMENT: &str = "--solaris-plugin-descendant-output-test-helper";

struct PluginHelperBinary {
    _directory: tempfile::TempDir,
    executable: PathBuf,
}

static PLUGIN_HELPER_BINARY: OnceLock<PluginHelperBinary> = OnceLock::new();

fn plugin_helper_executable() -> PathBuf {
    PLUGIN_HELPER_BINARY
        .get_or_init(|| {
            let directory = tempfile::tempdir().expect("create plugin helper directory");
            let executable = directory
                .path()
                .join(format!("plugin-process-helper{}", std::env::consts::EXE_SUFFIX));
            let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests")
                .join("fixtures")
                .join("plugin_process_helper.rs");
            let rustc = std::env::var_os("RUSTC")
                .map(PathBuf::from)
                .filter(|path| path.is_file())
                .or_else(|| {
                    std::env::var_os("PATH").and_then(|path| {
                        std::env::split_paths(&path)
                            .map(|directory| directory.join(format!("rustc{}", std::env::consts::EXE_SUFFIX)))
                            .find(|path| path.is_file())
                    })
                })
                .expect("Rust compiler is required for the plugin process helper");
            let status = std::process::Command::new(rustc)
                .args([
                    "--edition=2024",
                    "-C",
                    "opt-level=s",
                    "-C",
                    "panic=abort",
                    "-C",
                    "strip=symbols",
                    "-o",
                ])
                .arg(&executable)
                .arg(source)
                .status()
                .expect("start Rust compiler for the plugin process helper");
            assert!(status.success(), "Rust compiler failed for the plugin process helper");
            PluginHelperBinary {
                _directory: directory,
                executable,
            }
        })
        .executable
        .clone()
}

fn approved_executable(path: &Path) -> ExecutableIdentity {
    inspect_executable(path).expect("test executable should be inspectable")
}

fn write_test_executable(path: &Path, contents: &[u8]) {
    std::fs::write(path, contents).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
}

struct TestBypassSpawnAuthorizer;

impl solaris_process::ProcessSpawnAuthorizer for TestBypassSpawnAuthorizer {
    fn authorize_and_spawn(
        &self,
        spawn: solaris_process::ProcessSpawn,
    ) -> std::io::Result<solaris_process::ManagedChild> {
        spawn(ProcessLaunchPolicy::Ambient)
    }
}

fn authorized_tool_context(context: ToolExecutionContext) -> ToolExecutionContext {
    context
        .with_process_launch_policy(ProcessLaunchPolicy::Ambient)
        .with_process_spawn_authorization(ProcessSpawnAuthorization::new(Arc::new(TestBypassSpawnAuthorizer)))
}

fn command_tool(script: &str, max_result_size: usize, timeout_ms: u64) -> PluginCommandTool {
    let shell = solaris_config::shell::default_shell();
    let args = shell.derive_exec_args(script, false);
    let executable = shell.path.canonicalize().unwrap();
    PluginCommandTool {
        plugin_id: "contained".into(),
        definition: PluginCommandToolDefinition {
            name: "ContainedPlugin".into(),
            description: "contained plugin test".into(),
            input_schema: serde_json::json!({"type": "object"}),
            command: executable.to_string_lossy().into_owned(),
            args,
            effect: EffectDescriptor {
                class: EffectClass::Process,
                action: "run contained plugin test".into(),
                resources: ResourceFootprint::default(),
                replay_policy: EffectReplayPolicy::Never,
            },
            concurrency_safe: false,
            max_result_size,
            timeout_ms,
        },
        approved_executable: approved_executable(&executable),
        executable_identity: executable_identity(&executable),
        executable,
    }
}

fn helper_command_tool(helper: &str, marker: &Path, max_result_size: usize, timeout_ms: u64) -> PluginCommandTool {
    let executable = plugin_helper_executable();
    let approved_executable = approved_executable(&executable);
    PluginCommandTool {
        plugin_id: "contained".into(),
        definition: PluginCommandToolDefinition {
            name: "ContainedPlugin".into(),
            description: "contained plugin test".into(),
            input_schema: serde_json::json!({"type": "object"}),
            command: executable.to_string_lossy().into_owned(),
            args: vec![helper.to_owned(), marker.to_string_lossy().into_owned()],
            effect: EffectDescriptor {
                class: EffectClass::Process,
                action: "run contained plugin test".into(),
                resources: ResourceFootprint::default(),
                replay_policy: EffectReplayPolicy::Never,
            },
            concurrency_safe: false,
            max_result_size,
            timeout_ms,
        },
        executable_identity: executable_identity(&executable),
        approved_executable,
        executable,
    }
}

fn command_tool_context(run_label: &str) -> (EffectExecutionContext, Arc<InMemoryRuntimeLedger>, RunId) {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from(run_label);
    let context = EffectExecutionContext::new(
        run_id.clone(),
        AgentId::from("root"),
        ledger.clone(),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    (context, ledger, run_id)
}

async fn execute_command_tool(tool: PluginCommandTool, context: &EffectExecutionContext) -> ContentBlock {
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(tool));
    let calls = vec![ContentBlock::ToolUse {
        id: "contained-call".into(),
        name: "ContainedPlugin".into(),
        input: serde_json::json!({}),
        extra: None,
    }];
    let confirmer = Arc::new(Mutex::new(ToolConfirmer::new(true, Vec::new())));
    let outcome = execute_tool_calls_with_policy_context(
        &registry,
        &calls,
        &confirmer,
        PermissionMode::Bypass,
        PermissionCeiling::unrestricted(),
        context,
        None,
        CompactLevel::Off,
        false,
    )
    .await
    .expect("plugin tool execution should return a terminal result");
    outcome
        .results
        .into_iter()
        .next()
        .expect("plugin tool execution should return one result")
}

#[tokio::test]
async fn orchestration_passes_plugin_input_without_internal_metadata() {
    #[cfg(windows)]
    let script = "$value = [Console]::In.ReadToEnd(); [Console]::Out.Write($value)";
    #[cfg(not(windows))]
    let script = "cat";
    let mut tool = command_tool(script, 1024, 5_000);
    tool.definition.input_schema = serde_json::json!({
        "type": "object",
        "properties": {},
        "additionalProperties": false
    });
    let (context, _, _) = command_tool_context("plugin-input-metadata-run");

    let result = execute_command_tool(tool, &context).await;

    assert!(matches!(
        result,
        ContentBlock::ToolResult { is_error: false, ref content, .. }
            if content == "{}" && !content.contains("__solaris_effect_id")
    ));
}

#[tokio::test]
async fn direct_command_tool_execution_fails_closed_without_spawn_authorization() {
    let directory = tempfile::tempdir().unwrap();
    let marker = directory.path().join("direct-plugin-marker.txt");
    let tool = command_tool(&plugin_tool_process_policy_test::marker_command(&marker), 1024, 5_000);

    let result = tool.execute(serde_json::json!({})).await;

    assert!(result.is_error);
    assert!(result.content.contains("approved process spawn authorization"));
    assert!(!marker.exists());
}

#[test]
fn prepared_command_rejects_ambient_policy_without_spawn_authorization() {
    let tool = command_tool("", 1024, 1_000);
    let context =
        solaris_tools::ToolExecutionContext::new("test").with_process_launch_policy(ProcessLaunchPolicy::Ambient);

    let error = match tool.prepare_execution(serde_json::json!({}), context) {
        Ok(_) => panic!("plugin process preparation must require final spawn authorization"),
        Err(error) => error,
    };

    assert!(error.contains("approved process spawn authorization"));
}

#[test]
fn prepared_command_uses_the_identity_of_the_pinned_executable() {
    let mut tool = command_tool("", 1024, 1_000);
    tool.executable_identity = ImplementationIdentity {
        implementation_id: "stale-cached-identity".to_owned(),
        version: None,
        digest: Some("stale-digest".to_owned()),
    };
    let expected = plugin_executable_identity(&tool.approved_executable);

    let prepared = tool
        .prepare_execution(
            serde_json::json!({}),
            authorized_tool_context(solaris_tools::ToolExecutionContext::new("test")),
        )
        .unwrap();

    assert_eq!(prepared.implementation(), Some(&expected));
}

#[test]
fn prepared_command_rejects_a_different_path_with_identical_content() {
    let directory = tempfile::tempdir().unwrap();
    let approved_path = directory.path().join("approved");
    let redirected_path = directory.path().join("redirected");
    write_test_executable(&approved_path, b"identical executable bytes");
    write_test_executable(&redirected_path, b"identical executable bytes");
    let approved = approved_executable(&approved_path);
    let mut tool = command_tool("", 1024, 1_000);
    tool.executable = redirected_path;
    tool.executable_identity = plugin_executable_identity(&approved);
    tool.approved_executable = approved;

    let error = match tool.prepare_execution(
        serde_json::json!({}),
        authorized_tool_context(solaris_tools::ToolExecutionContext::new("test")),
    ) {
        Ok(_) => panic!("redirected executable should be rejected"),
        Err(error) => error,
    };

    assert!(error.contains("path identity changed before execution"));
    assert!(!error.contains(directory.path().to_string_lossy().as_ref()));
}

#[cfg(unix)]
#[tokio::test]
async fn prepared_command_runs_frozen_snapshot_and_records_approved_identity() {
    let directory = tempfile::tempdir().unwrap();
    let shell = solaris_config::shell::default_shell();
    let executable = directory.path().join("approved-shell");
    std::fs::copy(&shell.path, &executable).unwrap();
    let args = shell.derive_exec_args("printf 'approved:%s' \"${CARGO_PKG_NAME-unset}\"", false);
    let approved = approved_executable(&executable);
    let tool = PluginCommandTool {
        plugin_id: "snapshot".into(),
        definition: PluginCommandToolDefinition {
            name: "SnapshotPlugin".into(),
            description: "snapshot test".into(),
            input_schema: serde_json::json!({"type": "object"}),
            command: executable.to_string_lossy().into_owned(),
            args,
            effect: EffectDescriptor {
                class: EffectClass::Process,
                action: "execute snapshot test".into(),
                resources: ResourceFootprint::default(),
                replay_policy: EffectReplayPolicy::Never,
            },
            concurrency_safe: false,
            max_result_size: 1024,
            timeout_ms: 5_000,
        },
        executable_identity: plugin_executable_identity(&approved),
        approved_executable: approved,
        executable: executable.clone(),
    };
    let prepared = tool
        .prepare_execution(
            serde_json::json!({}),
            authorized_tool_context(solaris_tools::ToolExecutionContext::new("test")),
        )
        .unwrap();
    let pinned_identity = prepared
        .implementation()
        .expect("prepared plugin should expose pinned identity")
        .clone();
    let (context, ledger, run_id) = command_tool_context("plugin-snapshot-run");
    let request = context.effect_request(
        "snapshot-call",
        tool.name(),
        &serde_json::json!({}),
        tool.describe_effect(&serde_json::json!({})),
    );
    context
        .record_effect_intent_with_tool_implementation(&request, &pinned_identity)
        .unwrap();
    std::fs::write(&executable, b"replacement must never execute").unwrap();

    let result = prepared.execute().await;

    assert!(!result.is_error, "{}", result.content);
    assert_eq!(result.content, "approved:unset");
    let intent = ledger
        .records_for_run(&run_id)
        .unwrap()
        .into_iter()
        .find(|record| record.record_type == "effect_intent")
        .unwrap();
    let environment: OperationEnvironmentSnapshot =
        serde_json::from_value(intent.payload["environment"].clone()).unwrap();
    assert_eq!(environment.tools[0].implementation, pinned_identity);
}

async fn wait_for_pid_file(path: &Path) -> u32 {
    for _ in 0..200 {
        if let Ok(value) = std::fs::read_to_string(path)
            && let Ok(pid) = value.trim().parse()
        {
            return pid;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("descendant process did not publish its pid");
}

async fn assert_process_exits(pid: u32) {
    for _ in 0..40 {
        if !process_alive(pid) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("descendant process {pid} was still alive after plugin termination");
}

#[cfg(unix)]
fn process_alive(pid: u32) -> bool {
    let Ok(target) = i32::try_from(pid) else {
        return false;
    };
    let rc = unsafe { libc::kill(target, 0) };
    rc == 0 || !matches!(std::io::Error::last_os_error().raw_os_error(), Some(libc::ESRCH))
}

#[cfg(windows)]
fn process_alive(pid: u32) -> bool {
    use windows_sys::Win32::Foundation::{CloseHandle, WAIT_TIMEOUT};
    use windows_sys::Win32::System::Threading::{OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, WaitForSingleObject};

    const SYNCHRONIZE: u32 = 0x0010_0000;
    let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION | SYNCHRONIZE, 0, pid) };
    if handle.is_null() {
        return false;
    }
    let wait_result = unsafe { WaitForSingleObject(handle, 0) };
    unsafe { CloseHandle(handle) };
    wait_result == WAIT_TIMEOUT
}

fn assert_terminal_error_outcome(ledger: &InMemoryRuntimeLedger, run_id: &RunId) {
    let records = ledger.records_for_run(run_id).unwrap();
    let terminal = records
        .last()
        .expect("plugin effect should have a terminal ledger record");
    assert_eq!(terminal.record_type, "effect_outcome");
    assert_eq!(terminal.payload["is_error"], true);
}

#[test]
fn external_process_cannot_claim_read_only() {
    let definition = PluginCommandToolDefinition {
        name: "x".into(),
        description: "x".into(),
        input_schema: serde_json::json!({}),
        command: "x".into(),
        args: vec![],
        effect: EffectDescriptor {
            class: EffectClass::ReadOnly,
            action: "read ${input.path}".into(),
            resources: ResourceFootprint::default(),
            replay_policy: EffectReplayPolicy::ReplaySafe,
        },
        concurrency_safe: true,
        max_result_size: 100,
        timeout_ms: 1_000,
    };
    let executable = solaris_config::shell::default_shell().path;
    let tool = PluginCommandTool {
        plugin_id: "p".into(),
        definition,
        approved_executable: approved_executable(&executable),
        executable_identity: executable_identity(&executable),
        executable,
    };
    let effect = tool.rendered_effect(&serde_json::json!({"path": "a"}));
    assert_eq!(effect.class, EffectClass::Process);
    assert_eq!(effect.action, "read a");
    assert!(!effect.resources.process_commands.is_empty());
    assert_eq!(effect.resources.process_invocations.len(), 1);
    assert_eq!(effect.resources.process_invocations[0].argv, tool.definition.args);
}

#[tokio::test]
async fn contribution_execution_records_permission_intent_and_outcome() {
    let executable = plugin_helper_executable();
    let args = vec![PLUGIN_PROVIDER_HELPER_ARGUMENT.to_owned()];
    let implementation = ImplementationIdentity {
        implementation_id: "plugin:test".to_owned(),
        version: Some("1".to_owned()),
        digest: Some("digest".to_owned()),
    };
    let contribution = PluginCommandContribution {
        plugin_id: "test".to_owned(),
        implementation: implementation.clone(),
        definition: PluginCommandContributionDefinition {
            kind: PluginContributionKind::Provider,
            name: "test".to_owned(),
            command: executable.to_string_lossy().into_owned(),
            args,
            input_schema: serde_json::json!({"type": "object"}),
            max_result_size: 1024,
            timeout_ms: 1_000,
        },
        approved_executable: approved_executable(&executable),
        executable_identity: executable_identity(&executable),
        provider_command_v1: false,
        executable,
    };
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("plugin-effect-run");
    let context = EffectExecutionContext::new(
        run_id.clone(),
        AgentId::from("root"),
        ledger.clone(),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot {
            plugins: vec![implementation],
            ..Default::default()
        },
    );

    let output = contribution
        .invoke_authorized(serde_json::json!({"prompt": "hello"}), &context)
        .await
        .unwrap();

    assert_eq!(output, serde_json::json!({"ok": true}));
    assert_eq!(
        ledger
            .records_for_run(&run_id)
            .unwrap()
            .into_iter()
            .map(|record| record.record_type)
            .collect::<Vec<_>>(),
        vec!["permission_decision", "effect_intent", "effect_outcome"]
    );

    let recovered = contribution
        .invoke_authorized(serde_json::json!({"prompt": "hello"}), &context)
        .await
        .unwrap();
    assert_eq!(recovered, serde_json::json!({"ok": true}));
    assert_eq!(
        ledger
            .records_for_run(&run_id)
            .unwrap()
            .into_iter()
            .filter(|record| record.record_type == "effect_intent")
            .count(),
        1
    );
    assert_eq!(
        ledger
            .records_for_run(&run_id)
            .unwrap()
            .into_iter()
            .filter(|record| record.record_type == "permission_decision")
            .count(),
        1
    );
}

#[tokio::test]
async fn contribution_without_approved_plugin_identity_fails_before_intent() {
    let implementation = ImplementationIdentity {
        implementation_id: "plugin:missing".to_owned(),
        version: None,
        digest: None,
    };
    let executable = solaris_config::shell::default_shell().path;
    let contribution = PluginCommandContribution {
        plugin_id: "missing".to_owned(),
        implementation,
        definition: PluginCommandContributionDefinition {
            kind: PluginContributionKind::Hook,
            name: "missing".to_owned(),
            command: "missing".to_owned(),
            args: Vec::new(),
            input_schema: serde_json::json!({"type": "object"}),
            max_result_size: 1024,
            timeout_ms: 1_000,
        },
        approved_executable: approved_executable(&executable),
        executable_identity: executable_identity(&executable),
        provider_command_v1: false,
        executable,
    };
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("plugin-stale-run");
    let context = EffectExecutionContext::new(
        run_id.clone(),
        AgentId::from("root"),
        ledger.clone(),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );

    let error = contribution
        .invoke_authorized(serde_json::json!({}), &context)
        .await
        .unwrap_err();

    assert!(error.contains("implementation disappeared"));
    assert_eq!(
        ledger
            .records_for_run(&run_id)
            .unwrap()
            .into_iter()
            .map(|record| record.record_type)
            .collect::<Vec<_>>(),
        vec!["permission_decision", "effect_revalidation_failed"]
    );
}

#[tokio::test]
async fn contribution_rejects_replaced_executable_before_intent() {
    let root = tempfile::tempdir().unwrap();
    let executable = root
        .path()
        .join(if cfg!(windows) { "replace.cmd" } else { "replace.sh" });
    write_test_executable(&executable, b"original");
    let implementation = ImplementationIdentity {
        implementation_id: "plugin:replace".into(),
        version: Some("1".into()),
        digest: Some("plugin-digest".into()),
    };
    let contribution = PluginCommandContribution {
        plugin_id: "replace".into(),
        implementation: implementation.clone(),
        definition: PluginCommandContributionDefinition {
            kind: PluginContributionKind::Hook,
            name: "replace".into(),
            command: executable.to_string_lossy().into_owned(),
            args: Vec::new(),
            input_schema: serde_json::json!({"type": "object"}),
            max_result_size: 1024,
            timeout_ms: 1_000,
        },
        approved_executable: approved_executable(&executable),
        executable_identity: executable_identity(&executable),
        provider_command_v1: false,
        executable: executable.clone(),
    };
    std::fs::write(&executable, "replacement").unwrap();
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("plugin-replace-run");
    let context = EffectExecutionContext::new(
        run_id.clone(),
        AgentId::from("root"),
        ledger.clone(),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot {
            plugins: vec![implementation],
            ..Default::default()
        },
    );

    let error = contribution
        .invoke_authorized(serde_json::json!({}), &context)
        .await
        .unwrap_err();
    assert!(error.contains("implementation changed"));
    assert_eq!(
        ledger
            .records_for_run(&run_id)
            .unwrap()
            .into_iter()
            .map(|record| record.record_type)
            .collect::<Vec<_>>(),
        vec!["effect_revalidation_failed"]
    );
}

#[tokio::test]
async fn command_tool_timeout_uses_contained_process_runner() {
    let shell = solaris_config::shell::default_shell();
    #[cfg(windows)]
    let script = "Start-Sleep -Seconds 5; [Console]::Out.Write('{\"ok\":true}')";
    #[cfg(not(windows))]
    let script = "sleep 5; printf '{\"ok\":true}'";
    let args = shell.derive_exec_args(script, false);
    let executable = shell.path;
    let tool = PluginCommandTool {
        plugin_id: "timeout".into(),
        definition: PluginCommandToolDefinition {
            name: "TimeoutPlugin".into(),
            description: "timeout test".into(),
            input_schema: serde_json::json!({"type": "object"}),
            command: executable.to_string_lossy().into_owned(),
            args,
            effect: EffectDescriptor {
                class: EffectClass::Process,
                action: "run timeout test".into(),
                resources: ResourceFootprint::default(),
                replay_policy: EffectReplayPolicy::ReconcileRequired,
            },
            concurrency_safe: false,
            max_result_size: 1024,
            timeout_ms: 100,
        },
        approved_executable: approved_executable(&executable),
        executable_identity: executable_identity(&executable),
        executable,
    };

    let input = serde_json::json!({});
    let execution = tool.prepare_effect("timeout-test", &input).unwrap().into_parts().1;
    let result = tool
        .prepare_execution(input, authorized_tool_context(execution))
        .unwrap()
        .execute()
        .await;

    assert!(result.is_error);
    assert!(result.content.contains("timed out after 100ms"), "{}", result.content);
}

#[tokio::test]
async fn command_tool_timeout_kills_descendant_and_records_terminal_outcome() {
    let directory = tempfile::tempdir().unwrap();
    let marker = directory.path().join("timeout-descendant.pid");
    let tool = helper_command_tool(PLUGIN_DESCENDANT_WAIT_HELPER_ARGUMENT, &marker, 1024, 5_000);
    let (context, ledger, run_id) = command_tool_context("plugin-tool-timeout-run");

    let result = execute_command_tool(tool, &context).await;
    assert!(
        matches!(
            &result,
            ContentBlock::ToolResult { is_error: true, content, .. }
                if content.contains("timed out after 5000ms")
        ),
        "unexpected result: {result:?}"
    );
    let descendant_pid = wait_for_pid_file(&marker).await;
    assert_process_exits(descendant_pid).await;
    assert_terminal_error_outcome(ledger.as_ref(), &run_id);
}

#[tokio::test]
async fn command_tool_stdout_limit_kills_descendant_and_records_terminal_outcome() {
    assert_command_tool_output_limit(false).await;
}

#[tokio::test]
async fn command_tool_stderr_limit_kills_descendant_and_records_terminal_outcome() {
    assert_command_tool_output_limit(true).await;
}

async fn assert_command_tool_output_limit(stderr: bool) {
    let directory = tempfile::tempdir().unwrap();
    let marker = directory.path().join(if stderr {
        "stderr-descendant.pid"
    } else {
        "stdout-descendant.pid"
    });
    let mut tool = helper_command_tool(PLUGIN_DESCENDANT_OUTPUT_HELPER_ARGUMENT, &marker, 64, 10_000);
    tool.definition
        .args
        .push(if stderr { "stderr" } else { "stdout" }.to_owned());
    let run_label = if stderr {
        "plugin-tool-stderr-limit-run"
    } else {
        "plugin-tool-stdout-limit-run"
    };
    let (context, ledger, run_id) = command_tool_context(run_label);

    let result = execute_command_tool(tool, &context).await;
    assert!(
        matches!(
            &result,
            ContentBlock::ToolResult { is_error: true, content, .. }
                if content.contains("output exceeded 64 bytes")
        ),
        "unexpected result: {result:?}"
    );
    let descendant_pid = wait_for_pid_file(&marker).await;
    assert_process_exits(descendant_pid).await;
    assert_terminal_error_outcome(ledger.as_ref(), &run_id);
}

#[tokio::test]
async fn cancelling_command_tool_kills_descendant_and_records_terminal_outcome() {
    let directory = tempfile::tempdir().unwrap();
    let marker = directory.path().join("cancel-descendant.pid");
    let tool = helper_command_tool(PLUGIN_DESCENDANT_WAIT_HELPER_ARGUMENT, &marker, 1024, 30_000);
    let (context, ledger, run_id) = command_tool_context("plugin-tool-cancel-run");
    let task = tokio::spawn(async move { execute_command_tool(tool, &context).await });
    let descendant_pid = wait_for_pid_file(&marker).await;

    task.abort();
    let error = task.await.expect_err("plugin tool execution should be cancelled");

    assert!(error.is_cancelled());
    assert_process_exits(descendant_pid).await;
    assert_terminal_error_outcome(ledger.as_ref(), &run_id);
}

#[tokio::test]
async fn contribution_output_limit_kills_process_and_records_failure() {
    let shell = solaris_config::shell::default_shell();
    #[cfg(windows)]
    let script = "$null = [Console]::In.ReadToEnd(); while ($true) { [Console]::Out.Write('0123456789') }";
    #[cfg(unix)]
    let script = "cat >/dev/null; while :; do printf '0123456789'; done";
    let args = shell.derive_exec_args(script, false);
    let executable = shell.path;
    let implementation = ImplementationIdentity {
        implementation_id: "plugin:bounded".into(),
        version: None,
        digest: Some("bounded".into()),
    };
    let contribution = PluginCommandContribution {
        plugin_id: "bounded".into(),
        implementation: implementation.clone(),
        definition: PluginCommandContributionDefinition {
            kind: PluginContributionKind::Hook,
            name: "bounded".into(),
            command: executable.to_string_lossy().into_owned(),
            args,
            input_schema: serde_json::json!({"type": "object"}),
            max_result_size: 32,
            timeout_ms: 5_000,
        },
        approved_executable: approved_executable(&executable),
        executable_identity: executable_identity(&executable),
        provider_command_v1: false,
        executable,
    };
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("plugin-output-limit-run");
    let context = EffectExecutionContext::new(
        run_id.clone(),
        AgentId::from("root"),
        ledger.clone(),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot {
            plugins: vec![implementation],
            ..Default::default()
        },
    );

    let error = contribution
        .invoke_authorized(serde_json::json!({}), &context)
        .await
        .unwrap_err();

    assert!(error.contains("output exceeded 32 bytes"), "{error}");
    let records = ledger.records_for_run(&run_id).unwrap();
    assert_eq!(records.last().unwrap().record_type, "effect_outcome");
    assert_eq!(records.last().unwrap().payload["is_error"], true);
}

#[path = "plugin_tool_process_policy_test.rs"]
mod plugin_tool_process_policy_test;
