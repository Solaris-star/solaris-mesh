#![cfg(target_os = "linux")]

use std::time::Duration;

use solaris_process::{
    CommandRunner, ProcessLaunchPolicy, ProtectedObjectIdentity, SandboxEnforcement, SandboxError, inspect_executable,
    pin_executable, platform_sandbox_report,
};
#[cfg(feature = "sandbox-test-fixtures")]
use solaris_process::{
    SandboxReason, drain_process_recoveries, isolate_process_recoveries_for_test, pending_process_recoveries,
    process_outcome_unknown, process_recovery_required,
};

#[path = "support/sandbox_proxy.rs"]
mod sandbox_proxy;

#[test]
fn platform_probe_reports_full_only_after_a_real_handshake() {
    assert_eq!(platform_sandbox_report().enforcement(), SandboxEnforcement::Full);
}

#[tokio::test]
async fn workspace_sandbox_allows_workspace_but_denies_external_state() {
    let workspace = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let workspace_marker = workspace.path().join("workspace-marker");
    let state_marker = state.path().join("state-marker");
    let script = format!(
        "printf allowed > '{}'; printf denied > '{}'",
        workspace_marker.to_string_lossy(),
        state_marker.to_string_lossy()
    );
    let policy = ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []);

    let result = CommandRunner::new_pinned(pinned_shell_command(&script))
        .launch_policy(policy)
        .run()
        .await
        .expect("supported Linux must enforce the sandbox");

    assert!(workspace_marker.exists());
    assert!(!state_marker.exists());
    assert_ne!(result.exit_code, Some(0));
}

#[tokio::test]
async fn workspace_sandbox_writes_workspace_but_hides_nested_solaris_state() {
    let workspace = tempfile::tempdir().unwrap();
    let protected = workspace.path().join(".solaris");
    let protected_secret = protected.join("runtime/secret");
    let workspace_marker = workspace.path().join("workspace-write");
    std::fs::create_dir_all(protected_secret.parent().unwrap()).unwrap();
    std::fs::write(&protected_secret, b"runtime-secret").unwrap();
    let protected_identity =
        ProtectedObjectIdentity::from_file(&std::fs::File::open(&protected_secret).unwrap()).unwrap();
    let script = format!(
        "printf allowed > '{}'; if cat '{}' 2>/dev/null | grep -q runtime-secret; then exit 40; fi; if printf denied > '{}' 2>/dev/null; then exit 41; fi; exit 0",
        workspace_marker.to_string_lossy(),
        protected_secret.to_string_lossy(),
        protected_secret.to_string_lossy(),
    );
    let policy =
        ProcessLaunchPolicy::workspace_sandbox_with_identities(workspace.path(), [protected], [protected_identity]);

    let result = CommandRunner::new_pinned(pinned_shell_command(&script))
        .launch_policy(policy)
        .run()
        .await
        .unwrap();

    assert_eq!(result.exit_code, Some(0));
    assert_eq!(std::fs::read_to_string(workspace_marker).unwrap(), "allowed");
    assert_eq!(std::fs::read_to_string(protected_secret).unwrap(), "runtime-secret");
}

#[tokio::test]
async fn workspace_sandbox_denies_real_home_and_host_process_files() {
    let workspace = tempfile::tempdir().unwrap();
    let home_root = std::env::var_os("HOME").expect("Linux process tests require HOME");
    let real_home = tempfile::Builder::new()
        .prefix("solaris-real-home-probe-")
        .tempdir_in(home_root)
        .unwrap();
    let home_secret = real_home.path().join("secret");
    std::fs::write(&home_secret, "secret-home-data").unwrap();
    let script = format!(
        "if cat '{}' >/dev/null 2>&1; then printf home-exposed; else printf home-denied; fi; if cat /proc/{}/cmdline >/dev/null 2>&1; then printf proc-exposed; else printf proc-denied; fi",
        home_secret.to_string_lossy(),
        std::process::id()
    );
    let policy = ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []);

    let result = CommandRunner::new_pinned(pinned_shell_command(&script))
        .launch_policy(policy)
        .run()
        .await
        .expect("supported Linux must enforce the sandbox");
    let stdout = String::from_utf8_lossy(&result.stdout);

    assert_eq!(result.exit_code, Some(0));
    assert!(stdout.contains("home-denied"));
    assert!(stdout.contains("proc-denied"));
    assert!(!stdout.contains("exposed"));
}

#[test]
fn sandbox_network_child_helper() {
    let Some(address) = std::env::var_os("SOLARIS_SANDBOX_TCP_ADDRESS") else {
        return;
    };
    let marker = std::env::var_os("SOLARIS_SANDBOX_NETWORK_MARKER").unwrap();
    let exposed = std::net::TcpStream::connect(address.to_string_lossy().as_ref()).is_ok();
    std::fs::write(marker, if exposed { "exposed" } else { "denied" }).unwrap();
}

#[test]
fn sandbox_proxy_environment_child_probe() {
    sandbox_proxy::child_probe();
}

#[tokio::test]
async fn workspace_sandbox_denies_host_loopback() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let marker = workspace.path().join("network-result");
    let mut command = pinned_test_command("sandbox_network_child_helper");
    command
        .env(
            "SOLARIS_SANDBOX_TCP_ADDRESS",
            listener.local_addr().unwrap().to_string(),
        )
        .env("SOLARIS_SANDBOX_NETWORK_MARKER", &marker);
    let policy = ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []);

    let result = CommandRunner::new_pinned(command)
        .launch_policy(policy)
        .run()
        .await
        .unwrap();

    assert_eq!(result.exit_code, Some(0));
    assert_eq!(result.sandbox_report.enforcement(), SandboxEnforcement::Full);
    assert_eq!(std::fs::read_to_string(marker).unwrap(), "denied");
}

#[tokio::test]
async fn empty_network_policy_removes_inherited_proxy_environment() {
    let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let marker = workspace.path().join("empty-proxy-environment");
    let mut command = pinned_test_command("sandbox_proxy_environment_child_probe");
    sandbox_proxy::configure_probe(
        &mut command,
        sandbox_proxy::MODE_ABSENT,
        &marker,
        listener.local_addr().unwrap(),
    );

    let result = CommandRunner::new_pinned(command)
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []))
        .run()
        .await
        .unwrap();

    assert_eq!(result.exit_code, Some(0));
    assert_eq!(result.sandbox_report.enforcement(), SandboxEnforcement::Full);
    assert_eq!(std::fs::read(marker).unwrap(), b"verified");
}

#[cfg(feature = "sandbox-test-fixtures")]
#[tokio::test]
async fn approved_domain_policy_uses_only_the_host_proxy_bridge() {
    let origin = sandbox_proxy::OriginProbe::spawn();
    let workspace = tempfile::tempdir().unwrap();
    let marker = workspace.path().join("approved-domain-proxy");
    let mut command = pinned_test_command("sandbox_proxy_environment_child_probe");
    sandbox_proxy::configure_probe(&mut command, sandbox_proxy::MODE_ENABLED, &marker, origin.address());
    let policy = ProcessLaunchPolicy::workspace_sandbox_with_network_test_connector(
        workspace.path(),
        [],
        [],
        ["http://allowed.example.test"],
        "allowed.example.test",
        80,
        "93.184.216.34".parse().unwrap(),
        origin.address(),
    )
    .unwrap();

    let result = CommandRunner::new_pinned(command)
        .launch_policy(policy)
        .run()
        .await
        .unwrap();

    assert_eq!(result.exit_code, Some(0));
    assert_eq!(result.sandbox_report.enforcement(), SandboxEnforcement::Full);
    assert_eq!(std::fs::read(marker).unwrap(), b"verified");
    origin.assert_received();
}

#[test]
fn sandbox_unix_socket_child_helper() {
    use std::os::unix::net::UnixStream;

    let Some(socket) = std::env::var_os("SOLARIS_SANDBOX_UNIX_SOCKET") else {
        return;
    };
    let marker = std::env::var_os("SOLARIS_SANDBOX_UNIX_MARKER").unwrap();
    let exposed = UnixStream::connect(socket).is_ok();
    std::fs::write(marker, if exposed { "exposed" } else { "denied" }).unwrap();
}

#[tokio::test]
async fn workspace_sandbox_hides_protected_host_unix_socket() {
    use std::os::unix::net::UnixListener;

    let workspace = tempfile::tempdir().unwrap();
    let protected = tempfile::tempdir().unwrap();
    let socket = protected.path().join("runtime.sock");
    let _listener = UnixListener::bind(&socket).unwrap();
    let marker = workspace.path().join("unix-result");
    let mut command = pinned_test_command("sandbox_unix_socket_child_helper");
    command
        .env("SOLARIS_SANDBOX_UNIX_SOCKET", &socket)
        .env("SOLARIS_SANDBOX_UNIX_MARKER", &marker);
    let policy = ProcessLaunchPolicy::workspace_sandbox(workspace.path(), [protected.path().to_path_buf()]);

    let result = CommandRunner::new_pinned(command)
        .launch_policy(policy)
        .run()
        .await
        .unwrap();

    assert_eq!(result.exit_code, Some(0));
    assert_eq!(std::fs::read_to_string(marker).unwrap(), "denied");
}

#[tokio::test]
async fn workspace_host_unix_socket_is_rejected_before_target_spawn() {
    use std::os::unix::net::UnixListener;

    let workspace = tempfile::tempdir().unwrap();
    let socket = workspace.path().join("host.sock");
    let _listener = UnixListener::bind(&socket).unwrap();
    let target_marker = workspace.path().join("socket-target-started");
    let mut command = pinned_test_command("sandbox_hardlink_child_helper");
    command
        .env("SOLARIS_SANDBOX_HARDLINK_CHILD_MARKER", &target_marker)
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []));

    let error = match command.spawn() {
        Ok(_) => panic!("a host Unix socket in the workspace must fail before spawn"),
        Err(error) => error,
    };

    assert_eq!(sandbox_error(&error), Some(&SandboxError::HostSocketExposed));
    assert!(!target_marker.exists());
}

#[test]
#[allow(clippy::zombie_processes)]
fn sandbox_descendant_parent_helper() {
    // This helper must exit without waiting so the sandbox has to reap or kill
    // the background descendant rather than relying on cooperative cleanup.
    let Some(marker) = std::env::var_os("SOLARIS_SANDBOX_DESCENDANT_MARKER") else {
        return;
    };
    std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "sandbox_descendant_writer_helper", "--nocapture"])
        .env("SOLARIS_SANDBOX_DESCENDANT_MARKER", marker)
        .spawn()
        .unwrap();
}

#[test]
fn sandbox_descendant_writer_helper() {
    let Some(marker) = std::env::var_os("SOLARIS_SANDBOX_DESCENDANT_MARKER") else {
        return;
    };
    std::thread::sleep(Duration::from_millis(500));
    std::fs::write(marker, "escaped").unwrap();
}

#[tokio::test]
async fn ambient_true_spawns_and_exits_without_pre_exec_deadlock() {
    let target = std::path::Path::new("/bin/true");
    let identity = inspect_executable(target).unwrap();
    let command = pin_executable(target, &identity).unwrap().command().unwrap();

    let status = tokio::time::timeout(Duration::from_secs(1), async move {
        let mut child = command.spawn()?;
        child.wait().await
    })
    .await
    .expect("guardian spawn and wait exceeded the one-second watchdog")
    .unwrap();

    assert!(status.success());
}

#[cfg(feature = "sandbox-test-fixtures")]
#[tokio::test]
async fn target_does_not_run_before_guardian_release() {
    let workspace = tempfile::tempdir().unwrap();
    let accepted = workspace.path().join("plan-accepted");
    let release = workspace.path().join("release");
    let target_marker = workspace.path().join("target-ran");
    let mut command = pinned_test_command("sandbox_hardlink_child_helper");
    command
        .env("SOLARIS_GUARDIAN_FIXTURE_PLAN_ACCEPTED", &accepted)
        .env("SOLARIS_GUARDIAN_FIXTURE_RELEASE_GATE", &release)
        .env("SOLARIS_SANDBOX_HARDLINK_CHILD_MARKER", &target_marker)
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []));
    let spawn = tokio::task::spawn_blocking(move || command.spawn());
    tokio::time::timeout(Duration::from_secs(2), async {
        while !accepted.is_file() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("guardian never accepted the target plan");

    assert!(!target_marker.exists());
    std::fs::write(&release, b"release").unwrap();
    let mut child = spawn.await.unwrap().unwrap();
    let status = child.wait().await.unwrap();

    assert!(status.success());
    assert!(target_marker.is_file());
}

#[tokio::test]
async fn workspace_sandbox_leaves_no_background_descendant() {
    let workspace = tempfile::tempdir().unwrap();
    let marker = workspace.path().join("descendant-escaped");
    let mut command = pinned_test_command("sandbox_descendant_parent_helper");
    command.env("SOLARIS_SANDBOX_DESCENDANT_MARKER", &marker);
    let policy = ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []);

    let result = CommandRunner::new_pinned(command)
        .launch_policy(policy)
        .post_process_drain(Duration::from_secs(1))
        .run()
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(700)).await;

    assert_eq!(result.exit_code, Some(0));
    assert!(!marker.exists());
}

#[cfg(feature = "sandbox-test-fixtures")]
#[tokio::test]
async fn target_cannot_run_before_containment_attach_completes() {
    let workspace = tempfile::tempdir().unwrap();
    let marker = workspace.path().join("target-ran-before-containment");
    let mut command = pinned_test_command("sandbox_hardlink_child_helper");
    command
        .env("SOLARIS_SANDBOX_HARDLINK_CHILD_MARKER", &marker)
        .env("SOLARIS_SANDBOX_FIXTURE_FAIL_CONTAINMENT_ATTACH", "1")
        .launch_policy(ProcessLaunchPolicy::Ambient);

    let error = command
        .spawn()
        .expect_err("injected attach failure must reject the child before target execution");
    assert!(error.to_string().contains("injected containment attach failure"));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!marker.exists());
}

#[cfg(feature = "sandbox-test-fixtures")]
#[tokio::test]
async fn abnormal_sync_proof_retains_guardian_until_recovery() {
    let _recovery_test_guard = isolate_process_recoveries_for_test();
    let workspace = tempfile::tempdir().unwrap();
    let gate = workspace.path().join("allow-sync-proof");
    let mut command = pinned_test_command("sandbox_hardlink_child_helper");
    command
        .env("SOLARIS_GUARDIAN_FIXTURE_DRAIN_ERROR_GATE", &gate)
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []));

    let mut child = command.spawn().unwrap();
    let error = child
        .wait()
        .await
        .expect_err("an abnormal sync proof must not finalize or reap the guardian");
    let recovery = process_recovery_required(&error).expect("the unreaped guardian must retain recovery ownership");

    assert!(pending_process_recoveries().contains(&recovery));
    std::fs::write(&gate, b"retry").unwrap();
    drain_process_recoveries(Duration::from_secs(5)).await.unwrap();
    assert!(!pending_process_recoveries().contains(&recovery));
}

#[cfg(feature = "sandbox-test-fixtures")]
#[tokio::test]
async fn guardian_exec_error_recovery_does_not_retry_the_broken_control_socket() {
    let _recovery_test_guard = isolate_process_recoveries_for_test();
    let workspace = tempfile::tempdir().unwrap();
    let mut command = pinned_test_command("sandbox_hardlink_child_helper");
    command
        .env("SOLARIS_GUARDIAN_FIXTURE_EXEC_ERROR_DELAY_MS", "500")
        .env("SOLARIS_SANDBOX_FIXTURE_POST_SPAWN_TERMINATION_UNKNOWN", "1")
        .env("SOLARIS_SANDBOX_FIXTURE_RECOVERY_TERMINATION_UNKNOWN", "1")
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []));

    let error = command
        .spawn()
        .expect_err("injected EXEC_ERROR must reject the released launch");
    let recovery = process_recovery_required(&error).expect("delayed guardian exit must retain recovery ownership");

    assert!(process_outcome_unknown(&error));
    assert!(pending_process_recoveries().contains(&recovery));
    tokio::time::sleep(Duration::from_millis(600)).await;
    drain_process_recoveries(Duration::from_secs(5)).await.unwrap();
    assert!(!pending_process_recoveries().contains(&recovery));
}

#[tokio::test]
async fn managed_wait_kills_descendant_before_reaping_fast_root() {
    let workspace = tempfile::tempdir().unwrap();
    let marker = workspace.path().join("managed-wait-descendant-escaped");
    let mut command = pinned_test_command("sandbox_descendant_parent_helper");
    command
        .env("SOLARIS_SANDBOX_DESCENDANT_MARKER", &marker)
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []));

    let mut child = command.spawn().unwrap();
    let status = child.wait().await.unwrap();
    tokio::time::sleep(Duration::from_millis(700)).await;

    assert!(status.success());
    assert!(!marker.exists());
}

#[tokio::test]
async fn ambient_wait_kills_descendant_after_normal_root_exit() {
    let directory = tempfile::tempdir().unwrap();
    let marker = directory.path().join("ambient-descendant-escaped");
    let mut command = pinned_test_command("sandbox_descendant_parent_helper");
    command
        .env("SOLARIS_SANDBOX_DESCENDANT_MARKER", &marker)
        .launch_policy(ProcessLaunchPolicy::Ambient);

    let mut child = command.spawn().unwrap();
    let status = child.wait().await.unwrap();
    tokio::time::sleep(Duration::from_millis(700)).await;

    assert!(status.success());
    assert!(!marker.exists());
}

#[test]
fn sandbox_hardlink_child_helper() {
    if let Some(marker) = std::env::var_os("SOLARIS_SANDBOX_HARDLINK_CHILD_MARKER") {
        std::fs::write(marker, "child-started").unwrap();
    }
    let Some(target) = std::env::var_os("SOLARIS_SANDBOX_HARDLINK_TARGET") else {
        return;
    };
    std::fs::write(target, "overwritten-by-sandbox-child").unwrap();
}

#[tokio::test]
async fn workspace_hardlink_cannot_modify_a_protected_file() {
    use std::os::unix::fs::PermissionsExt;

    let workspace = tempfile::tempdir().unwrap();
    let protected = tempfile::tempdir().unwrap();
    let protected_file = protected.path().join("runtime-state.db");
    let workspace_alias = workspace.path().join("preexisting-runtime-alias.db");
    let child_marker = workspace.path().join("hardlink-child-started");
    let child_executable = workspace.path().join("hardlink-probe-child");
    std::fs::write(&protected_file, "protected-content").unwrap();
    std::fs::hard_link(&protected_file, &workspace_alias).unwrap();
    std::fs::copy(std::env::current_exe().unwrap(), &child_executable).unwrap();
    std::fs::set_permissions(&child_executable, std::fs::Permissions::from_mode(0o700)).unwrap();
    let policy = ProcessLaunchPolicy::workspace_sandbox(workspace.path(), [protected.path().to_path_buf()]);

    let identity = inspect_executable(&child_executable).unwrap();
    let pinned = pin_executable(&child_executable, &identity).unwrap();
    let mut command = pinned.command().unwrap();
    command
        .arg("--exact")
        .arg("sandbox_hardlink_child_helper")
        .arg("--nocapture")
        .env("SOLARIS_SANDBOX_HARDLINK_CHILD_MARKER", &child_marker)
        .env("SOLARIS_SANDBOX_HARDLINK_TARGET", &workspace_alias)
        .launch_policy(policy);

    let error = match command.spawn() {
        Ok(_) => panic!("a workspace hardlink to protected state must fail before spawn"),
        Err(error) => error,
    };
    assert_eq!(sandbox_error(&error), Some(&SandboxError::ProtectedObjectAlias));

    assert!(!child_marker.exists());
    assert_eq!(std::fs::read_to_string(protected_file).unwrap(), "protected-content");
}

#[tokio::test]
async fn workspace_hardlink_to_an_unprotected_external_file_is_rejected() {
    let workspace = tempfile::tempdir().unwrap();
    let external = tempfile::tempdir().unwrap();
    let external_file = external.path().join("outside");
    let workspace_alias = workspace.path().join("outside-alias");
    let child_marker = workspace.path().join("external-hardlink-child-started");
    std::fs::write(&external_file, b"outside").unwrap();
    std::fs::hard_link(&external_file, &workspace_alias).unwrap();
    let mut command = pinned_test_command("sandbox_hardlink_child_helper");
    command
        .env("SOLARIS_SANDBOX_HARDLINK_CHILD_MARKER", &child_marker)
        .env("SOLARIS_SANDBOX_HARDLINK_TARGET", &workspace_alias)
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []));

    let error = match command.spawn() {
        Ok(_) => panic!("external hardlink must fail before spawn"),
        Err(error) => error,
    };

    assert_eq!(sandbox_error(&error), Some(&SandboxError::ExternalHardlink));
    assert!(!child_marker.exists());
    assert_eq!(std::fs::read_to_string(external_file).unwrap(), "outside");
}

#[tokio::test]
async fn workspace_hardlinks_are_allowed_when_all_names_are_in_the_workspace() {
    let workspace = tempfile::tempdir().unwrap();
    let original = workspace.path().join("original");
    let alias = workspace.path().join("alias");
    let child_marker = workspace.path().join("internal-hardlink-child-started");
    std::fs::write(&original, b"inside").unwrap();
    std::fs::hard_link(&original, &alias).unwrap();
    let mut command = pinned_test_command("sandbox_hardlink_child_helper");
    command
        .env("SOLARIS_SANDBOX_HARDLINK_CHILD_MARKER", &child_marker)
        .env("SOLARIS_SANDBOX_HARDLINK_TARGET", &alias);

    let result = CommandRunner::new_pinned(command)
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []))
        .run()
        .await
        .unwrap();

    assert_eq!(result.exit_code, Some(0));
    assert!(child_marker.exists());
    assert_eq!(
        std::fs::read_to_string(original).unwrap(),
        "overwritten-by-sandbox-child"
    );
}

#[tokio::test]
async fn retained_identity_rejects_a_hardlink_to_a_rotated_protected_file() {
    let workspace = tempfile::tempdir().unwrap();
    let protected = tempfile::tempdir().unwrap();
    let protected_file = protected.path().join("session.sqlite3");
    let workspace_alias = workspace.path().join("old-session.sqlite3");
    let child_marker = workspace.path().join("historical-hardlink-child-started");
    std::fs::write(&protected_file, b"old-state").unwrap();
    std::fs::hard_link(&protected_file, &workspace_alias).unwrap();
    let retained = ProtectedObjectIdentity::from_file(&std::fs::File::open(&protected_file).unwrap()).unwrap();
    std::fs::remove_file(&protected_file).unwrap();
    std::fs::write(&protected_file, b"new-state").unwrap();
    let mut command = pinned_test_command("sandbox_hardlink_child_helper");
    command
        .env("SOLARIS_SANDBOX_HARDLINK_CHILD_MARKER", &child_marker)
        .env("SOLARIS_SANDBOX_HARDLINK_TARGET", &workspace_alias)
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox_with_identities(
            workspace.path(),
            [protected.path().to_path_buf()],
            [retained],
        ));

    let error = match command.spawn() {
        Ok(_) => panic!("retained protected identity must fail before spawn"),
        Err(error) => error,
    };

    assert_eq!(sandbox_error(&error), Some(&SandboxError::ProtectedObjectAlias));
    assert!(!child_marker.exists());
    assert_eq!(std::fs::read_to_string(workspace_alias).unwrap(), "old-state");
    assert_eq!(std::fs::read_to_string(protected_file).unwrap(), "new-state");
}

#[cfg(feature = "sandbox-test-fixtures")]
#[test]
fn helper_early_exit_fails_before_the_target_starts() {
    let workspace = tempfile::tempdir().unwrap();
    let target_marker = workspace.path().join("early-target-started");
    let mut command = pinned_test_command("sandbox_hardlink_child_helper");
    command
        .env("SOLARIS_SANDBOX_FIXTURE_BEHAVIOR", "early-exit")
        .env("SOLARIS_SANDBOX_HARDLINK_CHILD_MARKER", &target_marker)
        .launch_policy(fixture_policy(workspace.path()));

    let error = match command.spawn() {
        Ok(_) => panic!("early helper exit must fail before target spawn"),
        Err(error) => error,
    };

    assert_start_confirmation_failed(&error);
    assert!(!target_marker.exists());
}

#[cfg(feature = "sandbox-test-fixtures")]
#[test]
fn partial_start_marker_fails_before_the_target_starts() {
    let workspace = tempfile::tempdir().unwrap();
    let target_marker = workspace.path().join("partial-target-started");
    let mut command = pinned_test_command("sandbox_hardlink_child_helper");
    command
        .env("SOLARIS_SANDBOX_FIXTURE_BEHAVIOR", "partial-marker")
        .env("SOLARIS_SANDBOX_HARDLINK_CHILD_MARKER", &target_marker)
        .launch_policy(fixture_policy(workspace.path()));

    let error = match command.spawn() {
        Ok(_) => panic!("partial helper marker must fail before target spawn"),
        Err(error) => error,
    };

    assert_start_confirmation_failed(&error);
    assert!(!target_marker.exists());
}

#[cfg(feature = "sandbox-test-fixtures")]
#[tokio::test]
async fn handshake_timeout_kills_the_helper_descendant() {
    let workspace = tempfile::tempdir().unwrap();
    let target_marker = workspace.path().join("timeout-target-started");
    let descendant_marker = workspace.path().join("timeout-descendant-escaped");
    let mut command = pinned_test_command("sandbox_hardlink_child_helper");
    command
        .env("SOLARIS_SANDBOX_FIXTURE_BEHAVIOR", "timeout-with-descendant")
        .env("SOLARIS_SANDBOX_FIXTURE_DESCENDANT_MARKER", &descendant_marker)
        .env("SOLARIS_SANDBOX_HARDLINK_CHILD_MARKER", &target_marker)
        .launch_policy(fixture_policy(workspace.path()));

    let error = match command.spawn() {
        Ok(_) => panic!("helper timeout must fail before target spawn"),
        Err(error) => error,
    };
    assert_start_confirmation_failed(&error);
    tokio::time::sleep(Duration::from_secs(2)).await;

    assert!(!target_marker.exists());
    assert!(!descendant_marker.exists());
}

#[cfg(feature = "sandbox-test-fixtures")]
fn fixture_policy(workspace: &std::path::Path) -> ProcessLaunchPolicy {
    ProcessLaunchPolicy::workspace_sandbox_with_test_helper(
        workspace,
        [],
        [],
        env!("CARGO_BIN_EXE_solaris-process-sandbox-test-helper"),
    )
}

#[cfg(feature = "sandbox-test-fixtures")]
fn assert_start_confirmation_failed(error: &std::io::Error) {
    let Some(SandboxError::InsufficientEnforcement { report }) = sandbox_error(error) else {
        panic!("expected structured sandbox enforcement error");
    };
    assert_eq!(report.enforcement(), SandboxEnforcement::Unavailable);
    assert_eq!(report.reason(), SandboxReason::StartConfirmationFailed);
}

fn pinned_shell_command(script: &str) -> solaris_process::PinnedCommand {
    let shell = std::env::var_os("SHELL").expect("SHELL must identify the Linux shell for process tests");
    let shell = std::path::PathBuf::from(shell).canonicalize().unwrap();
    let identity = inspect_executable(&shell).unwrap();
    let mut command = pin_executable(&shell, &identity).unwrap().command().unwrap();
    command.args(["-c", script]);
    command
}

fn pinned_test_command(test_name: &str) -> solaris_process::PinnedCommand {
    let executable = std::env::current_exe().unwrap();
    let identity = inspect_executable(&executable).unwrap();
    let mut command = pin_executable(&executable, &identity).unwrap().command().unwrap();
    command.args(["--exact", test_name, "--nocapture"]);
    command
}

fn sandbox_error(error: &std::io::Error) -> Option<&SandboxError> {
    error.get_ref()?.downcast_ref::<SandboxError>()
}
