#[tokio::test]
async fn auto_mcp_connection_is_denied_before_process_spawn_outside_boundary() {
    use solaris_types::identity::{AgentId, RunId};
    use solaris_types::permission::{ExecutionBoundary, PermissionCeiling, PermissionMode};
    use solaris_types::runtime::OperationEnvironmentSnapshot;

    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};

    let parent = tempfile::tempdir().unwrap();
    let workspace = parent.path().join("workspace");
    let executable = parent.path().join(if cfg!(windows) {
        "must-not-start.exe"
    } else {
        "must-not-start"
    });
    std::fs::create_dir(&workspace).unwrap();
    std::fs::write(&executable, b"must not start").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("mcp-denied-run");
    let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    permissions.set_boundary(ExecutionBoundary::workspace(
        workspace.canonicalize().unwrap().to_string_lossy(),
    ));
    let context = EffectExecutionContext::new(
        run_id.clone(),
        AgentId::from("root"),
        ledger.clone(),
        permissions,
        OperationEnvironmentSnapshot::default(),
    );
    let config = McpServerConfig {
        transport: TransportType::Stdio,
        command: Some(executable.to_string_lossy().into_owned()),
        args: None,
        env: None,
        url: None,
        headers: None,
        network: Default::default(),
        deferred: None,
        startup_timeout_ms: Some(50),
    };
    let mut manager = McpManager::new();

    let error = connect_mcp_server_authorized(&mut manager, "blocked", &config, &context, &mcp_identity_key())
        .await
        .unwrap_err();

    assert!(error.contains("permission denied"));
    assert_eq!(
        ledger
            .records_for_run(&run_id)
            .unwrap()
            .into_iter()
            .map(|record| record.record_type)
            .collect::<Vec<_>>(),
        vec!["permission_decision"]
    );
}

#[test]
fn mcp_connection_capability_is_stable_and_scoped_to_one_server() {
    assert_eq!(mcp_connection_capability("alpha"), "mcp-connect:v1:5:alpha");
    assert_eq!(mcp_connection_capability("太阳"), "mcp-connect:v1:6:太阳");
    assert_ne!(mcp_connection_capability("alpha"), mcp_connection_capability("beta"));
}

#[test]
fn one_server_connection_grant_does_not_authorize_another_server_capability() {
    let config = McpServerConfig {
        transport: TransportType::StreamableHttp,
        command: None,
        args: None,
        env: None,
        url: Some("https://mcp.example.test/rpc".into()),
        headers: None,
        network: Default::default(),
        deferred: None,
        startup_timeout_ms: None,
    };
    let descriptor = mcp_connection_effect_descriptor("alpha", &config, &mcp_identity_key());
    let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    allow_mcp_connection_for(&permissions, "test:alpha", "alpha", &config, &mcp_identity_key());
    let request = |capability: String| EffectRequest {
        effect_id: EffectId::new(format!("effect:{capability}")),
        operation_id: OperationId::new(format!("operation:{capability}")),
        capability: capability.clone(),
        descriptor: descriptor.clone(),
        effective_input: serde_json::Value::Null,
        input_digest: None,
    };
    let alpha = mcp_connection_capability("alpha");
    let beta = mcp_connection_capability("beta");

    assert_eq!(
        permissions
            .evaluate_effect(&RunId::from("run"), &alpha, &request(alpha.clone()))
            .decision,
        PermissionDecision::Allow
    );
    assert_ne!(
        permissions
            .evaluate_effect(&RunId::from("run"), &beta, &request(beta.clone()))
            .decision,
        PermissionDecision::Allow
    );
}

#[tokio::test]
async fn mcp_executable_approved_before_permit_cannot_switch_before_pin() {
    use solaris_types::identity::{AgentId, RunId};
    use solaris_types::permission::{PermissionCeiling, PermissionMode};
    use solaris_types::resource::ResourceBudget;
    use solaris_types::runtime::OperationEnvironmentSnapshot;

    use crate::resource_manager::ResourceManager;
    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};

    let directory = tempfile::tempdir().unwrap();
    let shell = solaris_config::shell::default_shell();
    let executable = directory
        .path()
        .join(shell.path.file_name().expect("shell should have a file name"));
    std::fs::copy(&shell.path, &executable).unwrap();
    let resources = ResourceManager::new(ResourceBudget {
        max_concurrent_effects: Some(1),
        ..Default::default()
    });
    let held = resources.acquire_effect().await.unwrap();
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("mcp-switch-before-pin-run");
    let context = EffectExecutionContext::new(
        run_id.clone(),
        AgentId::from("root"),
        ledger.clone(),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    )
    .with_resource_manager(Arc::clone(&resources));
    let config = McpServerConfig {
        transport: TransportType::Stdio,
        command: Some(executable.to_string_lossy().into_owned()),
        args: None,
        env: None,
        url: None,
        headers: None,
        network: Default::default(),
        deferred: None,
        startup_timeout_ms: Some(100),
    };
    let identity_key = mcp_identity_key();
    let task =
        tokio::spawn(async move { prepare_mcp_server_authorized("switched", &config, &context, &identity_key).await });
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
    std::fs::write(&executable, b"replacement implementation").unwrap();
    drop(held);

    let error = match task.await.unwrap() {
        Ok(_) => panic!("replacement executable must not connect"),
        Err(error) => error,
    };
    assert!(error.contains("implementation changed before execution"));
    let records = ledger.records_for_run(&run_id).unwrap();
    assert!(!records.iter().any(|record| record.record_type == "effect_intent"));
}

#[tokio::test]
async fn oversized_mcp_executable_is_rejected_without_exposing_its_path() {
    use solaris_types::identity::{AgentId, RunId};
    use solaris_types::permission::{PermissionCeiling, PermissionMode};
    use solaris_types::runtime::OperationEnvironmentSnapshot;

    let directory = tempfile::tempdir().unwrap();
    let sentinel = "mcp-oversized-secret-path";
    let executable = directory.path().join(if cfg!(windows) {
        format!("{sentinel}.exe")
    } else {
        sentinel.into()
    });
    let file = std::fs::File::create(&executable).unwrap();
    file.set_len(64 * 1024 * 1024 + 1).unwrap();
    drop(file);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let context = EffectExecutionContext::new(
        RunId::from("oversized-mcp-run"),
        AgentId::from("root"),
        Arc::new(InMemoryRuntimeLedger::default()),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let config = McpServerConfig {
        transport: TransportType::Stdio,
        command: Some(executable.to_string_lossy().into_owned()),
        args: None,
        env: None,
        url: None,
        headers: None,
        network: Default::default(),
        deferred: None,
        startup_timeout_ms: Some(100),
    };
    let mut manager = McpManager::new();

    let error = connect_mcp_server_authorized(&mut manager, "oversized", &config, &context, &mcp_identity_key())
        .await
        .unwrap_err();

    assert!(
        error.contains("executable exceeds size limit"),
        "unexpected error: {error}"
    );
    assert!(!error.contains(sentinel));
    assert!(manager.server_names().is_empty());
}

const MCP_TEST_PROCESS_OUTPUT_LIMIT_BYTES: usize = 64 * 1024;
const MCP_HELPER_COMPILE_WATCHDOG: std::time::Duration = std::time::Duration::from_secs(15);

fn pinned_test_command(executable: &std::path::Path) -> solaris_process::PinnedCommand {
    let identity = solaris_process::inspect_executable(executable)
        .expect("bounded MCP test executable inspection must succeed");
    solaris_process::pin_executable(executable, &identity)
        .and_then(solaris_process::PinnedExecutable::command)
        .expect("bounded MCP test executable pin must succeed")
}

async fn run_bounded_test_process(
    runner: solaris_process::CommandRunner,
    timeout: std::time::Duration,
) -> solaris_process::CommandResult {
    runner
        .timeout(timeout)
        .max_output_bytes(MCP_TEST_PROCESS_OUTPUT_LIMIT_BYTES)
        .run()
        .await
        .expect("bounded MCP test process must start")
}

fn assert_bounded_test_process_succeeded(result: &solaris_process::CommandResult, process_kind: &str) {
    assert!(
        !result.timed_out && !result.output_limit_exceeded && result.exit_code == Some(0),
        "{process_kind} failed: exit_code={:?}, timed_out={}, output_limit_exceeded={}, stdout_bytes={}, stderr_bytes={}, stderr={}",
        result.exit_code,
        result.timed_out,
        result.output_limit_exceeded,
        result.stdout.len(),
        result.stderr.len(),
        String::from_utf8_lossy(&result.stderr)
    );
}

async fn compile_mcp_watchdog_helper(directory: &tempfile::TempDir) -> std::path::PathBuf {
    let executable = directory
        .path()
        .join(format!("mcp-watchdog-helper{}", std::env::consts::EXE_SUFFIX));
    let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("mcp_watchdog_helper.rs");
    let mut command = tokio::process::Command::new(rustc_for_stdio_helper());
    command
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
        .arg(source);
    let result = run_bounded_test_process(
        solaris_process::CommandRunner::new(command),
        MCP_HELPER_COMPILE_WATCHDOG,
    )
    .await;
    assert_bounded_test_process_succeeded(&result, "Rust MCP watchdog helper compilation");
    executable
}

#[tokio::test]
async fn bounded_mcp_test_process_watchdog_terminates_descendant_tree() {
    let directory = tempfile::tempdir().unwrap();
    let pid_path = directory.path().join("descendant.pid");
    let helper = compile_mcp_watchdog_helper(&directory).await;
    let mut child = pinned_test_command(&helper);
    child
        .args(["--solaris-mcp-watchdog-controller", &pid_path.to_string_lossy()])
        .current_dir(directory.path())
        .stdin(std::process::Stdio::null());

    let result = run_bounded_test_process(
        solaris_process::CommandRunner::new_pinned(child),
        std::time::Duration::from_secs(5),
    )
    .await;

    assert!(result.timed_out, "watchdog probe must reach its bounded deadline");
    let descendant_pid = std::fs::read_to_string(pid_path)
        .expect("watchdog controller must publish its descendant pid")
        .trim()
        .parse::<u32>()
        .expect("watchdog descendant pid must be numeric");
    assert_mcp_watchdog_process_exits(descendant_pid).await;
}

async fn assert_mcp_watchdog_process_exits(pid: u32) {
    for _ in 0..40 {
        if !mcp_watchdog_process_alive(pid) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("watchdog descendant process {pid} was still alive after termination");
}

#[cfg(unix)]
fn mcp_watchdog_process_alive(pid: u32) -> bool {
    let Ok(target) = i32::try_from(pid) else {
        return false;
    };
    let result = unsafe { libc::kill(target, 0) };
    result == 0 || !matches!(std::io::Error::last_os_error().raw_os_error(), Some(libc::ESRCH))
}

#[cfg(windows)]
fn mcp_watchdog_process_alive(pid: u32) -> bool {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn production_bootstrap_connects_stdio_servers_concurrently() {
    assert_production_bootstrap_connects_stdio_servers_concurrently().await;
}

async fn assert_production_bootstrap_connects_stdio_servers_concurrently() {
    use solaris_types::identity::{AgentId, RunId};
    use solaris_types::permission::{PermissionCeiling, PermissionMode};
    use solaris_types::runtime::OperationEnvironmentSnapshot;
    use std::io::{Read, Write};

    let workspace = tempfile::tempdir().unwrap();
    let helper_directory = tempfile::tempdir().unwrap();
    let helper = compile_stdio_barrier_helper(&helper_directory).await;
    let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
    let barrier_address = listener.local_addr().unwrap().to_string();
    let mut config = Config::resolve(&CliArgs {
        provider: Some("anthropic".into()),
        api_key: Some("sk-test".into()),
        base_url: None,
        model: Some("claude-sonnet-4-20250514".into()),
        max_tokens: Some(256),
        thinking: None,
        thinking_budget: None,
        max_turns: None,
        max_tool_call_malformed_turns: None,
        max_tool_call_failure_turns: None,
        system_prompt: None,
        profile: None,
        auto_approve: true,
        project_dir: None,
    })
    .unwrap();
    for (name, slot) in [("first", "1"), ("second", "2")] {
        config.mcp.servers.insert(
            name.into(),
            McpServerConfig {
                transport: TransportType::Stdio,
                command: Some(helper.to_string_lossy().into_owned()),
                args: None,
                env: Some(HashMap::from([
                    ("SOLARIS_TEST_MCP_BARRIER_ADDR".into(), barrier_address.clone()),
                    ("SOLARIS_TEST_MCP_BARRIER_SLOT".into(), slot.into()),
                ])),
                url: None,
                headers: None,
                network: Default::default(),
                deferred: None,
                startup_timeout_ms: Some(1_000),
            },
        );
    }
    let barrier = std::thread::spawn(move || {
        let mut peers = Vec::new();
        let mut slots = Vec::new();
        for _ in 0..2 {
            let (mut peer, _) = listener.accept().unwrap();
            let mut slot = [0_u8; 1];
            peer.read_exact(&mut slot).unwrap();
            slots.push(slot[0]);
            peers.push(peer);
        }
        slots.sort_unstable();
        for mut peer in peers {
            peer.write_all(b"1").unwrap();
        }
        slots
    });
    let output = Arc::new(RecordingBootstrapOutput::default());
    let bootstrap = AgentBootstrap::new(config, workspace.path().to_string_lossy(), output.clone());
    let context = EffectExecutionContext::new(
        RunId::from("stdio-concurrency-run"),
        AgentId::from("root"),
        Arc::new(InMemoryRuntimeLedger::default()),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let mut registry = ToolRegistry::new();

    let connected = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        bootstrap.connect_mcp(&mut registry, &[], &context, &mcp_identity_key()),
    )
    .await
    .expect("both real MCP stdio helpers must reach the barrier concurrently");

    {
        let connection_errors = output.errors.lock().unwrap();
        let timeout_errors = connection_errors.iter().filter(|error| error.contains("timed out")).count();
        assert!(
            connection_errors.is_empty(),
            "real MCP helpers failed: count={}, timeout_count={timeout_errors}",
            connection_errors.len()
        );
    }
    let manager = connected.manager.expect("both real MCP helpers must produce a manager");
    let mut server_names = manager.server_names();
    server_names.sort_unstable();
    assert_eq!(server_names, vec!["first", "second"]);
    assert_eq!(barrier.join().unwrap(), b"12");
    manager.shutdown().await;
}

async fn compile_stdio_barrier_helper(directory: &tempfile::TempDir) -> std::path::PathBuf {
    let executable = directory
        .path()
        .join(format!("mcp-stdio-barrier-helper{}", std::env::consts::EXE_SUFFIX));
    let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("mcp_stdio_barrier_helper.rs");
    let mut command = tokio::process::Command::new(rustc_for_stdio_helper());
    command
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
        .arg(source);
    let result = run_bounded_test_process(
        solaris_process::CommandRunner::new(command),
        MCP_HELPER_COMPILE_WATCHDOG,
    )
    .await;
    assert_bounded_test_process_succeeded(&result, "Rust MCP test helper compilation");
    executable
}

fn rustc_for_stdio_helper() -> std::path::PathBuf {
    let executable_name = format!("rustc{}", std::env::consts::EXE_SUFFIX);
    if let Some(configured) = std::env::var_os("RUSTC") {
        let configured = std::path::PathBuf::from(configured);
        if configured.is_file() {
            return configured;
        }
    }
    if let Some(path) = std::env::var_os("PATH").and_then(|path| {
        std::env::split_paths(&path)
            .map(|directory| directory.join(&executable_name))
            .find(|candidate| candidate.is_file())
    }) {
        return path;
    }
    let cargo_home = std::env::var_os("CARGO_HOME").map(std::path::PathBuf::from).or_else(|| {
        std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" })
            .map(std::path::PathBuf::from)
            .map(|home| home.join(".cargo"))
    });
    cargo_home
        .map(|home| home.join("bin").join(executable_name))
        .filter(|candidate| candidate.is_file())
        .expect("Rust compiler is required for the MCP stdio concurrency test")
}

#[tokio::test]
async fn production_bootstrap_connects_a_real_stdio_shell_server() {
    use solaris_config::shell::ShellKind;
    use solaris_types::identity::{AgentId, RunId};
    use solaris_types::permission::{PermissionCeiling, PermissionMode};
    use solaris_types::runtime::OperationEnvironmentSnapshot;

    let workspace = tempfile::tempdir().unwrap();
    let shell = solaris_config::shell::default_shell();
    let script = match shell.kind {
        ShellKind::PowerShell => {
            r#"$null = [Console]::In.ReadLine(); [Console]::Out.WriteLine('{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-03-26","capabilities":{},"serverInfo":null}}'); $null = [Console]::In.ReadLine(); $null = [Console]::In.ReadLine(); [Console]::Out.WriteLine('{"jsonrpc":"2.0","id":2,"result":{"tools":[]}}')"#
        }
        ShellKind::Cmd => {
            r#"set /p _=& echo {"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-03-26","capabilities":{},"serverInfo":null}}& set /p _=& set /p _=& echo {"jsonrpc":"2.0","id":2,"result":{"tools":[]}}"#
        }
        ShellKind::Bash | ShellKind::Zsh | ShellKind::Sh => {
            r#"read -r _; printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-03-26","capabilities":{},"serverInfo":null}}'; read -r _; read -r _; printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"tools":[]}}'"#
        }
    };
    let mut config = Config::resolve(&CliArgs {
        provider: Some("anthropic".into()),
        api_key: Some("sk-test".into()),
        base_url: None,
        model: Some("claude-sonnet-4-20250514".into()),
        max_tokens: Some(256),
        thinking: None,
        thinking_budget: None,
        max_turns: None,
        max_tool_call_malformed_turns: None,
        max_tool_call_failure_turns: None,
        system_prompt: None,
        profile: None,
        auto_approve: true,
        project_dir: None,
    })
    .unwrap();
    config.mcp.servers.insert(
        "shell-smoke".into(),
        McpServerConfig {
            transport: TransportType::Stdio,
            command: Some(shell.path.to_string_lossy().into_owned()),
            args: Some(shell.derive_exec_args(script, false)),
            env: None,
            url: None,
            headers: None,
            network: Default::default(),
            deferred: None,
            startup_timeout_ms: Some(5_000),
        },
    );
    let output = Arc::new(RecordingBootstrapOutput::default());
    let bootstrap = AgentBootstrap::new(config, workspace.path().to_string_lossy(), output.clone());
    let context = EffectExecutionContext::new(
        RunId::from("stdio-shell-smoke-run"),
        AgentId::from("root"),
        Arc::new(InMemoryRuntimeLedger::default()),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let mut registry = ToolRegistry::new();

    let connected = bootstrap
        .connect_mcp(&mut registry, &[], &context, &mcp_identity_key())
        .await;

    assert_eq!(connected.manager.unwrap().server_names(), vec!["shell-smoke"]);
    assert!(output.errors.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mcp_connection_records_secret_safe_identity_and_redacted_failure() {
    use solaris_types::identity::{AgentId, RunId};
    use solaris_types::permission::{PermissionCeiling, PermissionMode};
    use solaris_types::runtime::OperationEnvironmentSnapshot;

    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};

    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("mcp-secret-safe-run");
    let context = EffectExecutionContext::new(
        run_id.clone(),
        AgentId::from("root"),
        ledger.clone(),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let secret = "mcp-header-sentinel\n";
    let config = McpServerConfig {
        transport: TransportType::StreamableHttp,
        command: None,
        args: None,
        env: Some(HashMap::from([("API_TOKEN".into(), "mcp-env-sentinel".into())])),
        url: Some("https://user:mcp-url-sentinel@example.test/mcp?token=mcp-query-sentinel".into()),
        headers: Some(HashMap::from([("Authorization".into(), secret.into())])),
        network: Default::default(),
        deferred: None,
        startup_timeout_ms: Some(50),
    };
    let mut manager = McpManager::new();

    let error = connect_mcp_server_authorized(&mut manager, "redacted", &config, &context, &mcp_identity_key())
        .await
        .unwrap_err();
    let records = serde_json::to_string(&ledger.records_for_run(&run_id).unwrap()).unwrap();

    assert!(error.contains("Authorization"));
    assert!(!error.contains("mcp-header-sentinel"));
    assert!(!records.contains("mcp-header-sentinel"));
    assert!(!records.contains("mcp-env-sentinel"));
    assert!(!records.contains("mcp-url-sentinel"));
    assert!(!records.contains("mcp-query-sentinel"));
}

#[tokio::test]
async fn plan_bootstrap_does_not_start_or_connect_any_mcp_transport() {
    use solaris_config::shell::ShellKind;
    use solaris_types::permission::PermissionMode;

    let workspace = tempfile::tempdir().unwrap();
    let marker = workspace.path().join("mcp-must-not-start");
    let shell = solaris_config::shell::default_shell();
    let script = match shell.kind {
        ShellKind::PowerShell => format!(
            "Set-Content -LiteralPath '{}' -Value forbidden; Start-Sleep -Seconds 60",
            marker.to_string_lossy().replace('\'', "''")
        ),
        ShellKind::Cmd => format!(">\"{}\" echo forbidden & ping -n 61 127.0.0.1 >nul", marker.display()),
        ShellKind::Bash | ShellKind::Zsh | ShellKind::Sh => format!(
            "printf forbidden > '{}'; sleep 60",
            marker.to_string_lossy().replace('\'', "'\\''")
        ),
    };
    let mut config = Config::resolve(&CliArgs {
        provider: Some("anthropic".into()),
        api_key: Some("sk-test".into()),
        base_url: None,
        model: Some("claude-sonnet-4-20250514".into()),
        max_tokens: Some(256),
        thinking: None,
        thinking_budget: None,
        max_turns: None,
        max_tool_call_malformed_turns: None,
        max_tool_call_failure_turns: None,
        system_prompt: None,
        profile: None,
        auto_approve: true,
        project_dir: None,
    })
    .unwrap();
    config.mcp.servers.insert(
        "stdio".into(),
        McpServerConfig {
            transport: TransportType::Stdio,
            command: Some(shell.path.to_string_lossy().into_owned()),
            args: Some(shell.derive_exec_args(&script, false)),
            env: None,
            url: None,
            headers: None,
            network: Default::default(),
            deferred: None,
            startup_timeout_ms: Some(60_000),
        },
    );
    for (name, transport) in [("sse", TransportType::Sse), ("http", TransportType::StreamableHttp)] {
        config.mcp.servers.insert(
            name.into(),
            McpServerConfig {
                transport,
                command: None,
                args: None,
                env: None,
                url: Some(format!("http://127.0.0.1:9/{name}")),
                headers: None,
                network: Default::default(),
                deferred: None,
                startup_timeout_ms: Some(60_000),
            },
        );
    }
    let output = Arc::new(RecordingBootstrapOutput::default());

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        AgentBootstrap::new(config, workspace.path().to_string_lossy(), output.clone())
            .provider(Arc::new(FailingProvider))
            .permission_mode(PermissionMode::Plan)
            .build(),
    )
    .await
    .expect("Plan bootstrap must not wait for MCP startup")
    .unwrap();

    assert!(!marker.exists());
    assert!(!result.has_mcp);
    assert!(result.mcp_managers.is_empty());
    assert!(result.mcp_identity_key.is_none());
    assert!(!workspace.path().join(".solaris/runtime/mcp-identity-key.json").exists());
    assert!(output.errors.lock().unwrap().is_empty());
}

async fn assert_stdio_mode_switch_does_not_escape_auto_sandbox(
    approved_mode: solaris_types::permission::PermissionMode,
    current_mode: solaris_types::permission::PermissionMode,
    run_label: &str,
) {
    use solaris_config::shell::ShellKind;
    use solaris_types::identity::{AgentId, RunId};
    use solaris_types::permission::{ExecutionBoundary, PermissionCeiling};
    use solaris_types::resource::ResourceBudget;
    use solaris_types::runtime::OperationEnvironmentSnapshot;

    use crate::resource_manager::ResourceManager;
    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};

    let workspace = tempfile::tempdir().unwrap();
    let runtime_root = workspace.path().join(".solaris").join("runtime");
    std::fs::create_dir_all(&runtime_root).unwrap();
    let protected = runtime_root.join("ledger.sqlite3");
    std::fs::write(&protected, b"state").unwrap();
    let outside = tempfile::tempdir().unwrap();
    let marker = outside.path().join("mcp-sandbox-escape-marker");
    let shell = solaris_config::shell::default_shell();
    let script = match shell.kind {
        ShellKind::PowerShell => format!(
            "Set-Content -LiteralPath '{}' -Value escaped; exit 7",
            marker.to_string_lossy().replace('\'', "''")
        ),
        ShellKind::Cmd => format!(">\"{}\" echo escaped & exit /b 7", marker.display()),
        ShellKind::Bash | ShellKind::Zsh | ShellKind::Sh => format!(
            "printf escaped > '{}'; exit 7",
            marker.to_string_lossy().replace('\'', "'\\''")
        ),
    };
    let config = McpServerConfig {
        transport: TransportType::Stdio,
        command: Some(shell.path.to_string_lossy().into_owned()),
        args: Some(shell.derive_exec_args(&script, false)),
        env: None,
        url: None,
        headers: None,
        network: Default::default(),
        deferred: None,
        startup_timeout_ms: Some(1_000),
    };
    let permissions = PermissionContext::new(approved_mode, PermissionCeiling::unrestricted());
    permissions.set_boundary(ExecutionBoundary::workspace(
        workspace.path().canonicalize().unwrap().to_string_lossy(),
    ));
    permissions
        .register_protected_paths(&runtime_root, vec![protected])
        .unwrap();
    let identity_key = mcp_identity_key();
    allow_mcp_connection_for(
        &permissions,
        "test:mcp:switched",
        "switched",
        &config,
        &identity_key,
    );
    let resources = ResourceManager::new(ResourceBudget {
        max_concurrent_effects: Some(1),
        ..Default::default()
    });
    let held = resources.acquire_effect().await.unwrap();
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from(run_label);
    let context = EffectExecutionContext::new(
        run_id.clone(),
        AgentId::from("root"),
        ledger.clone(),
        permissions.clone(),
        OperationEnvironmentSnapshot::default(),
    )
    .with_resource_manager(resources);
    let task =
        tokio::spawn(async move { prepare_mcp_server_authorized("switched", &config, &context, &identity_key).await });
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
    permissions.set_mode(current_mode);
    drop(held);

    let result = task.await.unwrap();
    assert!(result.is_err(), "sandboxed stdio MCP connection must be refused");
    assert!(!marker.exists(), "stdio MCP process escaped the stricter Auto policy");
}

#[tokio::test]
async fn stdio_rechecks_bypass_to_auto_before_spawn() {
    use solaris_types::permission::PermissionMode;

    assert_stdio_mode_switch_does_not_escape_auto_sandbox(
        PermissionMode::Bypass,
        PermissionMode::Auto,
        "mcp-bypass-to-auto",
    )
    .await;
}

#[tokio::test]
async fn stdio_rechecks_auto_to_bypass_before_spawn() {
    use solaris_types::permission::PermissionMode;

    assert_stdio_mode_switch_does_not_escape_auto_sandbox(
        PermissionMode::Auto,
        PermissionMode::Bypass,
        "mcp-auto-to-bypass",
    )
    .await;
}

#[tokio::test]
async fn entering_plan_revokes_configured_grant_for_a_server_that_failed_to_connect() {
    use solaris_types::permission::PermissionMode;

    let workspace = tempfile::tempdir().unwrap();
    let mut config = Config::resolve(&CliArgs {
        provider: Some("anthropic".into()),
        api_key: Some("sk-test".into()),
        base_url: None,
        model: Some("claude-sonnet-4-20250514".into()),
        max_tokens: Some(256),
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
    config.mcp.servers.insert(
        "offline".into(),
        McpServerConfig {
            transport: TransportType::StreamableHttp,
            command: None,
            args: None,
            env: None,
            url: Some("http://127.0.0.1:9/mcp".into()),
            headers: None,
            network: Default::default(),
            deferred: None,
            startup_timeout_ms: Some(50),
        },
    );
    let output = Arc::new(RecordingBootstrapOutput::default());
    let mut result = AgentBootstrap::new(config, workspace.path().to_string_lossy(), output)
        .provider(Arc::new(FailingProvider))
        .permission_mode(PermissionMode::Bypass)
        .build()
        .await
        .unwrap();

    assert!(!result.has_mcp);
    assert!(
        result
            .execution_context
            .permissions()
            .has_configured_effect_source("config:mcp:offline")
    );

    result.engine.set_permission_mode(PermissionMode::Plan);

    assert!(
        !result
            .execution_context
            .permissions()
            .has_configured_effect_source("config:mcp:offline"),
        "Plan mode must revoke exact grants even when no manager was created"
    );
}
