#![cfg(windows)]

#[path = "support/windows.rs"]
mod windows_support;

use std::io::{Read, Write};
use std::path::Path;

use solaris_process::{
    CommandRunner, ProcessLaunchPolicy, SandboxBackend, SandboxEnforcement, SandboxError, SandboxReason,
    WorkspaceRootAuthority, inspect_executable, pin_executable, platform_sandbox_report, sandbox_report_from_error,
};
#[cfg(feature = "sandbox-test-fixtures")]
use solaris_process::{
    ProcessFinalizationError, ProcessFinalizationStage, ProcessRecoveryKind, ProcessRecoveryState,
    process_recovery_required, retry_process_recovery,
};

const WORKSPACE_FILE_KEY: &str = "SOLARIS_WINDOWS_SANDBOX_WORKSPACE_FILE";
const EXTERNAL_FILE_KEY: &str = "SOLARIS_WINDOWS_SANDBOX_EXTERNAL_FILE";
const DESCENDANT_LEVEL_KEY: &str = "SOLARIS_WINDOWS_SANDBOX_DESCENDANT_LEVEL";
const DESCENDANT_MARKER_KEY: &str = "SOLARIS_WINDOWS_SANDBOX_DESCENDANT_MARKER";
const DESCENDANT_SPAWNED_KEY: &str = "SOLARIS_WINDOWS_SANDBOX_DESCENDANT_SPAWNED";
const DELAYED_MARKER_KEY: &str = "SOLARIS_WINDOWS_SANDBOX_DELAYED_MARKER";
const DELAY_MILLIS_KEY: &str = "SOLARIS_WINDOWS_SANDBOX_DELAY_MILLIS";
const APPLICATION_PACKAGE_PROBE_KEY: &str = "SOLARIS_WINDOWS_SANDBOX_APPLICATION_PACKAGE_PROBE";
const NETWORK_PORT_KEY: &str = "SOLARIS_WINDOWS_SANDBOX_NETWORK_PORT";
const PROTECTED_FILE_KEY: &str = "SOLARIS_WINDOWS_SANDBOX_PROTECTED_FILE";
const STDIO_PROBE_KEY: &str = "SOLARIS_WINDOWS_SANDBOX_STDIO_PROBE";
const TARGET_MARKER_KEY: &str = "SOLARIS_WINDOWS_SANDBOX_TARGET_MARKER";
const ACL_RENAME_TARGET_KEY: &str = "SOLARIS_WINDOWS_SANDBOX_ACL_RENAME_TARGET";
const ACL_RENAME_DESTINATION_KEY: &str = "SOLARIS_WINDOWS_SANDBOX_ACL_RENAME_DESTINATION";

#[test]
fn sandbox_workspace_child_probe() {
    let Some(workspace_file) = std::env::var_os(WORKSPACE_FILE_KEY) else {
        return;
    };
    let external_file = std::env::var_os(EXTERNAL_FILE_KEY).expect("external probe path");

    std::fs::write(workspace_file, b"workspace-write").expect("workspace must be writable");
    assert!(
        std::fs::read(&external_file).is_err(),
        "an AppContainer child must not read an out-of-workspace file"
    );
    assert!(
        std::fs::write(external_file, b"overwritten").is_err(),
        "an AppContainer child must not write an out-of-workspace file"
    );
}

#[test]
fn sandbox_stdio_child_probe() {
    if std::env::var_os(STDIO_PROBE_KEY).is_none() {
        return;
    }
    let mut input = String::new();
    std::io::stdin().read_to_string(&mut input).unwrap();
    writeln!(std::io::stdout(), "sandbox-stdout:{input}").unwrap();
    writeln!(std::io::stderr(), "sandbox-stderr:{input}").unwrap();
}

#[test]
fn sandbox_application_package_child_probe() {
    if std::env::var_os(APPLICATION_PACKAGE_PROBE_KEY).is_none() {
        return;
    }
    use std::os::windows::ffi::OsStrExt;

    use windows_sys::Win32::Foundation::{CloseHandle, LocalFree};
    use windows_sys::Win32::Security::Authorization::ConvertStringSidToSidW;
    use windows_sys::Win32::Security::{
        CheckTokenMembershipEx, DuplicateToken, SecurityImpersonation, TOKEN_DUPLICATE, TOKEN_QUERY,
    };
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    let mut token = std::ptr::null_mut();
    assert_ne!(
        unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY | TOKEN_DUPLICATE, &mut token) },
        0
    );
    let mut sid_text = std::ffi::OsStr::new("S-1-15-2-1")
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let mut sid = std::ptr::null_mut();
    assert_ne!(unsafe { ConvertStringSidToSidW(sid_text.as_mut_ptr(), &mut sid) }, 0);
    let mut impersonation = std::ptr::null_mut();
    assert_ne!(
        unsafe { DuplicateToken(token, SecurityImpersonation, &mut impersonation) },
        0
    );
    let mut member = 0;
    let checked = unsafe { CheckTokenMembershipEx(impersonation, sid, 1, &mut member) };
    assert_ne!(checked, 0, "{}", std::io::Error::last_os_error());
    unsafe {
        LocalFree(sid);
        CloseHandle(impersonation);
        CloseHandle(token);
    }
    assert_eq!(
        member, 0,
        "sandbox target must not inherit broad ALL APPLICATION PACKAGES access"
    );
}

#[test]
fn sandbox_protected_child_probe() {
    let Some(protected_file) = std::env::var_os(PROTECTED_FILE_KEY) else {
        return;
    };
    if let Ok(delay) = std::env::var(DELAY_MILLIS_KEY) {
        std::thread::sleep(std::time::Duration::from_millis(delay.parse().unwrap()));
    }
    assert!(
        std::fs::read(&protected_file).is_err(),
        "protected state must not be readable"
    );
    assert!(
        std::fs::write(&protected_file, b"overwritten").is_err(),
        "protected state must not be writable"
    );
}

#[test]
fn sandbox_network_child_probe() {
    let Some(port) = std::env::var_os(NETWORK_PORT_KEY) else {
        return;
    };
    let port = port.to_string_lossy().parse::<u16>().unwrap();
    let address = std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, port));
    assert!(
        std::net::TcpStream::connect_timeout(&address, std::time::Duration::from_secs(1)).is_err(),
        "strict Auto must not reach a host loopback listener"
    );
}

#[test]
#[allow(clippy::zombie_processes)]
fn sandbox_descendant_child_probe() {
    let Some(marker) = std::env::var_os(DESCENDANT_MARKER_KEY) else {
        return;
    };
    if std::env::var_os(DESCENDANT_LEVEL_KEY).as_deref() == Some(std::ffi::OsStr::new("child")) {
        let spawned = std::env::var_os(DESCENDANT_SPAWNED_KEY).unwrap();
        let mut marker = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(marker)
            .unwrap();
        marker.write_all(b"armed").unwrap();
        marker.flush().unwrap();
        std::fs::write(spawned, b"spawned").unwrap();
        let delay = std::env::var(DELAY_MILLIS_KEY)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(2_000);
        std::thread::sleep(std::time::Duration::from_millis(delay));
        marker.write_all(b"escaped").unwrap();
        marker.flush().unwrap();
        return;
    }
    let spawned = std::env::var_os(DESCENDANT_SPAWNED_KEY).unwrap();
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "sandbox_descendant_child_probe", "--nocapture"])
        .env(DESCENDANT_LEVEL_KEY, "child")
        .spawn()
        .expect("sandbox root must be able to create a contained descendant");
    assert!(child.id() > 0);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while !Path::new(&spawned).exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "sandbox descendant did not become ready"
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

#[test]
fn sandbox_marker_child_probe() {
    if let Some(marker) = std::env::var_os(TARGET_MARKER_KEY) {
        std::fs::write(marker, b"target-started").unwrap();
    }
}

#[test]
fn sandbox_acl_replacement_child_probe() {
    let Some(target) = std::env::var_os(ACL_RENAME_TARGET_KEY) else {
        return;
    };
    let destination = std::env::var_os(ACL_RENAME_DESTINATION_KEY).expect("ACL rename destination");
    std::fs::rename(&target, destination).expect("sandbox child must rename a writable file");
    std::fs::write(target, b"replacement").expect("sandbox child must create a replacement file");
}

#[test]
fn sandbox_delayed_write_child_probe() {
    let Some(marker) = std::env::var_os(DELAYED_MARKER_KEY) else {
        return;
    };
    let delay = std::env::var(DELAY_MILLIS_KEY).unwrap().parse::<u64>().unwrap();
    std::thread::sleep(std::time::Duration::from_millis(delay));
    std::fs::write(marker, b"written").unwrap();
}

#[tokio::test]
async fn workspace_sandbox_writes_workspace_and_denies_external_read() {
    let workspace = tempfile::tempdir().unwrap();
    let external = tempfile::tempdir().unwrap();
    let workspace_file = workspace.path().join("written-by-child");
    let external_file = external.path().join("outside");
    std::fs::write(&external_file, b"outside").unwrap();

    let mut command = pinned_test_command(workspace.path(), "sandbox_workspace_child_probe");
    command
        .env(WORKSPACE_FILE_KEY, &workspace_file)
        .env(EXTERNAL_FILE_KEY, &external_file)
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []));

    let mut child = command.spawn().expect("Windows strict Auto must start through Full");
    assert_eq!(child.sandbox_report().enforcement(), SandboxEnforcement::Full);
    assert!(child.wait().await.unwrap().success());
    assert_eq!(std::fs::read(&workspace_file).unwrap(), b"workspace-write");
    assert_eq!(std::fs::read(&external_file).unwrap(), b"outside");
}

#[test]
fn platform_report_is_full_only_after_windows_probe() {
    let report = platform_sandbox_report();
    assert_eq!(
        report.enforcement(),
        SandboxEnforcement::Full,
        "probe report: {report:?}"
    );
    assert_eq!(report.backend(), SandboxBackend::WindowsAppContainer);
    assert_eq!(report.reason(), solaris_process::SandboxReason::Enforced);
}

#[tokio::test]
async fn workspace_sandbox_denies_external_file_with_everyone_acl() {
    let workspace = tempfile::tempdir().unwrap();
    let external = tempfile::tempdir().unwrap();
    let workspace_file = workspace.path().join("written-by-child");
    let external_file = external.path().join("everyone-readable");
    std::fs::write(&external_file, b"outside").unwrap();
    windows_support::grant_everyone_full(external.path()).unwrap();
    windows_support::grant_everyone_full(&external_file).unwrap();
    let mut command = pinned_test_command(workspace.path(), "sandbox_workspace_child_probe");
    command
        .env(WORKSPACE_FILE_KEY, &workspace_file)
        .env(EXTERNAL_FILE_KEY, &external_file)
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []));

    let mut child = command.spawn().expect("Windows strict Auto must start through Full");
    assert_eq!(child.sandbox_report().enforcement(), SandboxEnforcement::Full);
    assert!(child.wait().await.unwrap().success());
    assert_eq!(std::fs::read(&workspace_file).unwrap(), b"workspace-write");
    assert_eq!(std::fs::read(&external_file).unwrap(), b"outside");
}

#[tokio::test]
async fn workspace_sandbox_restores_acl_after_exit() {
    let workspace = tempfile::tempdir().unwrap();
    let external = tempfile::tempdir().unwrap();
    let protected = workspace.path().join(".solaris");
    std::fs::create_dir(&protected).unwrap();
    let protected_file = protected.join("session.sqlite3");
    std::fs::write(&protected_file, b"runtime-state").unwrap();
    let workspace_acl = windows_support::dacl_sddl(workspace.path()).unwrap();
    let protected_acl = windows_support::dacl_sddl(&protected_file).unwrap();
    let workspace_file = workspace.path().join("created-by-child");
    let external_file = external.path().join("outside");
    std::fs::write(&external_file, b"outside").unwrap();
    let mut command = pinned_test_command(workspace.path(), "sandbox_workspace_child_probe");
    command
        .env(WORKSPACE_FILE_KEY, &workspace_file)
        .env(EXTERNAL_FILE_KEY, &external_file)
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(
            workspace.path(),
            [protected.clone()],
        ));

    let mut child = command.spawn().expect("Windows strict Auto must start through Full");
    assert!(child.wait().await.unwrap().success());
    drop(child);
    assert_eq!(windows_support::dacl_sddl(workspace.path()).unwrap(), workspace_acl);
    assert_eq!(windows_support::dacl_sddl(&protected_file).unwrap(), protected_acl);
    assert!(
        !windows_support::dacl_sddl(&workspace_file)
            .unwrap()
            .contains("S-1-15-2-"),
        "a new workspace file retained the ephemeral AppContainer SID"
    );
}

#[tokio::test]
async fn workspace_sandbox_restores_renamed_object_without_touching_replacement() {
    let workspace = tempfile::tempdir().unwrap();
    let original = workspace.path().join("acl-target");
    let moved = workspace.path().join("acl-target-moved");
    std::fs::write(&original, b"original").unwrap();
    windows_support::grant_everyone_full(&original).unwrap();
    let original_acl = windows_support::dacl_sddl(&original).unwrap();
    let mut command = pinned_test_command(workspace.path(), "sandbox_acl_replacement_child_probe");
    command
        .env(ACL_RENAME_TARGET_KEY, &original)
        .env(ACL_RENAME_DESTINATION_KEY, &moved)
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []));

    let mut child = command.spawn().expect("Windows strict Auto must start through Full");
    assert!(child.wait().await.unwrap().success());

    assert_eq!(std::fs::read(&moved).unwrap(), b"original");
    assert_eq!(windows_support::dacl_sddl(&moved).unwrap(), original_acl);
    assert_eq!(std::fs::read(&original).unwrap(), b"replacement");
    let replacement_acl = windows_support::dacl_sddl(&original).unwrap();
    assert_ne!(
        replacement_acl, original_acl,
        "cleanup must not restore the retained object's DACL onto its path replacement"
    );
    assert!(
        !replacement_acl.contains("S-1-15-2-"),
        "the replacement must not retain the ephemeral AppContainer SID"
    );
}

#[cfg(feature = "sandbox-test-fixtures")]
#[tokio::test]
async fn managed_child_wait_reports_acl_cleanup_failure_and_drop_retries() {
    let workspace = tempfile::tempdir().unwrap();
    let workspace_acl = windows_support::dacl_sddl(workspace.path()).unwrap();
    let marker = workspace.path().join("cleanup-failure-target");
    let mut command = pinned_test_command(workspace.path(), "sandbox_delayed_write_child_probe");
    command
        .env("SOLARIS_SANDBOX_FIXTURE_FAIL_ACL_CLEANUP", "1")
        .env(DELAYED_MARKER_KEY, &marker)
        .env(DELAY_MILLIS_KEY, "200")
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []));

    let mut child = command.spawn().expect("Windows strict Auto must start through Full");
    let error = child
        .wait()
        .await
        .expect_err("an ACL cleanup failure must be visible to the caller");
    assert!(matches!(
        sandbox_error(&error),
        Some(SandboxError::CleanupFailed { .. })
    ));
    drop(child);
    assert_eq!(windows_support::dacl_sddl(workspace.path()).unwrap(), workspace_acl);
}

#[cfg(feature = "sandbox-test-fixtures")]
#[tokio::test]
async fn command_runner_preserves_stdin_and_cleanup_failures() {
    let workspace = tempfile::tempdir().unwrap();
    let workspace_acl = windows_support::dacl_sddl(workspace.path()).unwrap();
    let marker = workspace.path().join("stdin-cleanup-target");
    let mut command = pinned_test_command(workspace.path(), "sandbox_delayed_write_child_probe");
    command
        .env("SOLARIS_SANDBOX_FIXTURE_FAIL_STDIN", "1")
        .env("SOLARIS_SANDBOX_FIXTURE_FAIL_ACL_CLEANUP", "1")
        .env(DELAYED_MARKER_KEY, &marker)
        .env(DELAY_MILLIS_KEY, "200");

    let error = CommandRunner::new_pinned(command)
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []))
        .stdin_bytes(b"fixture input".to_vec())
        .run()
        .await
        .expect_err("stdin and cleanup failures must both be visible");

    assert_finalization_stages(
        &error,
        &[
            ProcessFinalizationStage::Stdin,
            ProcessFinalizationStage::SandboxCleanup,
        ],
    );
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    assert_eq!(windows_support::dacl_sddl(workspace.path()).unwrap(), workspace_acl);
}

#[cfg(feature = "sandbox-test-fixtures")]
#[tokio::test]
async fn command_runner_preserves_termination_and_cleanup_failures() {
    let workspace = tempfile::tempdir().unwrap();
    let workspace_acl = windows_support::dacl_sddl(workspace.path()).unwrap();
    let marker = workspace.path().join("termination-cleanup-target");
    let mut command = pinned_test_command(workspace.path(), "sandbox_delayed_write_child_probe");
    command
        .env("SOLARIS_SANDBOX_FIXTURE_FAIL_TERMINATION", "1")
        .env("SOLARIS_SANDBOX_FIXTURE_FAIL_ACL_CLEANUP", "1")
        .env(DELAYED_MARKER_KEY, &marker)
        .env(DELAY_MILLIS_KEY, "200");

    let error = CommandRunner::new_pinned(command)
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []))
        .run()
        .await
        .expect_err("termination and cleanup failures must both be visible");

    let aggregate = error
        .get_ref()
        .and_then(|source| source.downcast_ref::<ProcessFinalizationError>())
        .expect("termination uncertainty must retain every recovery failure");
    assert_eq!(
        aggregate
            .failures()
            .iter()
            .map(|failure| failure.stage())
            .collect::<Vec<_>>(),
        [
            ProcessFinalizationStage::Terminate,
            ProcessFinalizationStage::Recovery,
            ProcessFinalizationStage::Reconciliation,
        ]
    );
    let recovery = process_recovery_required(&error).expect("error must carry its stable process recovery ID");
    assert_eq!(recovery.kind(), ProcessRecoveryKind::StartedChild);
    assert_eq!(
        retry_process_recovery(recovery.id()).unwrap(),
        ProcessRecoveryState::Complete
    );
    assert_eq!(windows_support::dacl_sddl(workspace.path()).unwrap(), workspace_acl);
}

#[cfg(feature = "sandbox-test-fixtures")]
#[tokio::test]
async fn command_runner_preserves_wait_and_cleanup_failures() {
    let workspace = tempfile::tempdir().unwrap();
    let workspace_acl = windows_support::dacl_sddl(workspace.path()).unwrap();
    let marker = workspace.path().join("wait-cleanup-target");
    let mut command = pinned_test_command(workspace.path(), "sandbox_delayed_write_child_probe");
    command
        .env("SOLARIS_SANDBOX_FIXTURE_FAIL_WAIT", "1")
        .env("SOLARIS_SANDBOX_FIXTURE_FAIL_ACL_CLEANUP", "1")
        .env(DELAYED_MARKER_KEY, &marker)
        .env(DELAY_MILLIS_KEY, "200");

    let error = CommandRunner::new_pinned(command)
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []))
        .run()
        .await
        .expect_err("wait and cleanup failures must both be visible");

    assert_finalization_stages(
        &error,
        &[ProcessFinalizationStage::Wait, ProcessFinalizationStage::SandboxCleanup],
    );
    assert_eq!(windows_support::dacl_sddl(workspace.path()).unwrap(), workspace_acl);
}

#[cfg(feature = "sandbox-test-fixtures")]
#[tokio::test]
async fn command_runner_returns_single_primary_error_unchanged_after_successful_cleanup() {
    let workspace = tempfile::tempdir().unwrap();
    let workspace_acl = windows_support::dacl_sddl(workspace.path()).unwrap();
    let marker = workspace.path().join("stdin-primary-target");
    let mut command = pinned_test_command(workspace.path(), "sandbox_delayed_write_child_probe");
    command
        .env("SOLARIS_SANDBOX_FIXTURE_FAIL_STDIN", "1")
        .env(DELAYED_MARKER_KEY, &marker)
        .env(DELAY_MILLIS_KEY, "200");

    let error = CommandRunner::new_pinned(command)
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []))
        .stdin_bytes(b"fixture input".to_vec())
        .run()
        .await
        .expect_err("the injected primary error must be returned");

    assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
    assert!(
        error
            .get_ref()
            .and_then(|source| source.downcast_ref::<ProcessFinalizationError>())
            .is_none(),
        "one primary error with successful cleanup must be returned unchanged"
    );
    assert_eq!(windows_support::dacl_sddl(workspace.path()).unwrap(), workspace_acl);
}

#[cfg(feature = "sandbox-test-fixtures")]
#[tokio::test]
async fn command_runner_spawn_failure_still_reports_cleanup_failure() {
    let workspace = tempfile::tempdir().unwrap();
    let workspace_acl = windows_support::dacl_sddl(workspace.path()).unwrap();
    let marker = workspace.path().join("must-not-spawn");
    let mut command = pinned_test_command(workspace.path(), "sandbox_delayed_write_child_probe");
    command
        .env("SOLARIS_SANDBOX_FIXTURE_FAIL_SPAWN", "1")
        .env("SOLARIS_SANDBOX_FIXTURE_FAIL_ACL_CLEANUP", "1")
        .env(DELAYED_MARKER_KEY, &marker)
        .env(DELAY_MILLIS_KEY, "200");

    let error = CommandRunner::new_pinned(command)
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []))
        .run()
        .await
        .expect_err("spawn and cleanup failures must both be visible");

    assert_finalization_stages(
        &error,
        &[
            ProcessFinalizationStage::Spawn,
            ProcessFinalizationStage::SandboxCleanup,
        ],
    );
    assert!(!marker.exists());
    assert_eq!(windows_support::dacl_sddl(workspace.path()).unwrap(), workspace_acl);
}

#[tokio::test]
async fn workspace_sandbox_preserves_stdio_pipes() {
    let workspace = tempfile::tempdir().unwrap();
    let mut command = pinned_test_command(workspace.path(), "sandbox_stdio_child_probe");
    command.env(STDIO_PROBE_KEY, "1");

    let result = CommandRunner::new_pinned(command)
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []))
        .stdin_bytes(b"sandbox-input".to_vec())
        .run()
        .await
        .expect("Full Windows sandbox must preserve configured pipes");

    assert_eq!(result.sandbox_report.enforcement(), SandboxEnforcement::Full);
    assert_eq!(result.exit_code, Some(0));
    assert!(String::from_utf8_lossy(&result.stdout).contains("sandbox-stdout:sandbox-input"));
    assert!(String::from_utf8_lossy(&result.stderr).contains("sandbox-stderr:sandbox-input"));
}

#[tokio::test]
async fn workspace_sandbox_token_excludes_broad_application_package_group() {
    let workspace = tempfile::tempdir().unwrap();
    let mut command = pinned_test_command(workspace.path(), "sandbox_application_package_child_probe");
    command
        .env(APPLICATION_PACKAGE_PROBE_KEY, "1")
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []));

    let mut child = command.spawn().expect("Windows strict Auto must start through Full");
    assert_eq!(child.sandbox_report().enforcement(), SandboxEnforcement::Full);
    assert!(child.wait().await.unwrap().success());
}

#[tokio::test]
async fn workspace_sandbox_denies_nested_protected_state() {
    let workspace = tempfile::tempdir().unwrap();
    let protected = workspace.path().join(".solaris");
    std::fs::create_dir(&protected).unwrap();
    let protected_file = protected.join("session.sqlite3");
    std::fs::write(&protected_file, b"runtime-state").unwrap();
    let mut command = pinned_test_command(workspace.path(), "sandbox_protected_child_probe");
    command
        .env(PROTECTED_FILE_KEY, &protected_file)
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(
            workspace.path(),
            [protected.clone()],
        ));

    let mut child = command
        .spawn()
        .expect("nested protected state must remain representable");
    assert_eq!(child.sandbox_report().enforcement(), SandboxEnforcement::Full);
    assert!(child.wait().await.unwrap().success());
    assert_eq!(std::fs::read(&protected_file).unwrap(), b"runtime-state");
}

#[tokio::test]
async fn workspace_sandbox_denies_host_loopback_network() {
    let workspace = tempfile::tempdir().unwrap();
    let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    let mut command = pinned_test_command(workspace.path(), "sandbox_network_child_probe");
    command
        .env(NETWORK_PORT_KEY, port.to_string())
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []));

    let mut child = command.spawn().expect("Windows strict Auto must start through Full");
    assert_eq!(child.sandbox_report().enforcement(), SandboxEnforcement::Full);
    assert!(child.wait().await.unwrap().success());
    drop(listener);
}

#[test]
fn approved_domain_policy_fails_closed_before_the_target_starts() {
    let workspace = tempfile::tempdir().unwrap();
    let marker = workspace.path().join("target-started");
    let mut command = pinned_test_command(workspace.path(), "sandbox_marker_child_probe");
    command.env(TARGET_MARKER_KEY, &marker).launch_policy(
        ProcessLaunchPolicy::workspace_sandbox_with_network(workspace.path(), [], [], ["https://allowed.example.test"])
            .unwrap(),
    );

    let error = match command.spawn() {
        Ok(_) => panic!("Windows approved-domain policy must fail closed until a verified Full transport is available"),
        Err(error) => error,
    };
    let report = solaris_process::SandboxReport::new(
        SandboxEnforcement::Unavailable,
        SandboxBackend::WindowsAppContainer,
        SandboxReason::NetworkProxyUnavailable,
    );
    assert_eq!(
        sandbox_error(&error),
        Some(&SandboxError::NetworkProxyUnavailable { report })
    );
    assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
    assert_eq!(sandbox_report_from_error(&error), Some(report));
    assert!(!marker.exists());
}

#[tokio::test]
async fn workspace_sandbox_kills_descendants_after_root_exit() {
    let workspace = tempfile::tempdir().unwrap();
    let marker = workspace.path().join("descendant-escaped");
    let spawned = workspace.path().join("descendant-spawned");
    let mut command = pinned_test_command(workspace.path(), "sandbox_descendant_child_probe");
    command
        .env(DESCENDANT_MARKER_KEY, &marker)
        .env(DESCENDANT_SPAWNED_KEY, &spawned)
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []));

    let mut child = command.spawn().expect("Windows strict Auto must start through Full");
    assert_eq!(child.sandbox_report().enforcement(), SandboxEnforcement::Full);
    assert!(child.wait().await.unwrap().success());
    assert_eq!(std::fs::read(&spawned).unwrap(), b"spawned");
    assert_eq!(std::fs::read(&marker).unwrap(), b"armed");
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    assert_eq!(
        std::fs::read(&marker).unwrap(),
        b"armed",
        "a descendant survived Job termination"
    );
}

#[tokio::test]
async fn command_runner_kills_sandbox_descendants_after_root_exit() {
    let workspace = tempfile::tempdir().unwrap();
    let marker = workspace.path().join("runner-descendant-escaped");
    let spawned = workspace.path().join("runner-descendant-spawned");
    let mut command = pinned_test_command(workspace.path(), "sandbox_descendant_child_probe");
    command
        .env(DESCENDANT_MARKER_KEY, &marker)
        .env(DESCENDANT_SPAWNED_KEY, &spawned)
        .env(DELAY_MILLIS_KEY, "100");

    let result = CommandRunner::new_pinned(command)
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []))
        .run()
        .await
        .expect("Windows strict Auto command runner must start through Full");

    assert_eq!(result.sandbox_report.enforcement(), SandboxEnforcement::Full);
    assert_eq!(result.exit_code, Some(0));
    assert_eq!(std::fs::read(&spawned).unwrap(), b"spawned");
    assert_eq!(std::fs::read(&marker).unwrap(), b"armed");
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    assert_eq!(
        std::fs::read(&marker).unwrap(),
        b"armed",
        "CommandRunner released a sandbox descendant"
    );
}

#[tokio::test]
async fn concurrent_workspace_sandboxes_keep_each_acl_lease_active() {
    let workspace = tempfile::tempdir().unwrap();
    let first_marker = workspace.path().join("first-marker");
    let second_marker = workspace.path().join("second-marker");
    let mut first = pinned_test_command_named(
        workspace.path(),
        "first-sandbox-target.exe",
        "sandbox_delayed_write_child_probe",
    );
    first
        .env(DELAYED_MARKER_KEY, &first_marker)
        .env(DELAY_MILLIS_KEY, "200")
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []));
    let mut second = pinned_test_command_named(
        workspace.path(),
        "second-sandbox-target.exe",
        "sandbox_delayed_write_child_probe",
    );
    second
        .env(DELAYED_MARKER_KEY, &second_marker)
        .env(DELAY_MILLIS_KEY, "1200")
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []));

    let mut first = first.spawn().unwrap();
    let mut second = second.spawn().unwrap();
    assert!(first.wait().await.unwrap().success());
    drop(first);
    assert!(second.wait().await.unwrap().success());
    drop(second);
    assert_eq!(std::fs::read(&first_marker).unwrap(), b"written");
    assert_eq!(std::fs::read(&second_marker).unwrap(), b"written");
}

#[tokio::test]
async fn managed_child_keeps_workspace_root_guarded_until_drop() {
    let parent = tempfile::tempdir().unwrap();
    let workspace = parent.path().join("workspace");
    let renamed = parent.path().join("renamed");
    std::fs::create_dir(&workspace).unwrap();
    let marker = workspace.join("delayed-marker");
    let mut command = pinned_test_command(&workspace, "sandbox_delayed_write_child_probe");
    command
        .env(DELAYED_MARKER_KEY, &marker)
        .env(DELAY_MILLIS_KEY, "200")
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(&workspace, []));

    let mut child = command.spawn().expect("Windows strict Auto must start through Full");
    assert_eq!(child.sandbox_report().enforcement(), SandboxEnforcement::Full);
    assert_eq!(
        std::fs::rename(&workspace, &renamed).unwrap_err().raw_os_error(),
        Some(32)
    );
    assert!(child.wait().await.unwrap().success());
    assert_eq!(
        std::fs::rename(&workspace, &renamed).unwrap_err().raw_os_error(),
        Some(32)
    );

    drop(child);
    std::fs::rename(&workspace, &renamed).unwrap();
    assert_eq!(std::fs::read(renamed.join("delayed-marker")).unwrap(), b"written");
}

#[tokio::test]
async fn concurrent_workspace_sandboxes_keep_protected_acl_active() {
    let workspace = tempfile::tempdir().unwrap();
    let protected = workspace.path().join(".solaris");
    std::fs::create_dir(&protected).unwrap();
    let protected_file = protected.join("session.sqlite3");
    std::fs::write(&protected_file, b"runtime-state").unwrap();
    let mut first = pinned_test_command_named(
        workspace.path(),
        "first-protected-target.exe",
        "sandbox_protected_child_probe",
    );
    first
        .env(PROTECTED_FILE_KEY, &protected_file)
        .env(DELAY_MILLIS_KEY, "200")
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(
            workspace.path(),
            [protected.clone()],
        ));
    let mut second = pinned_test_command_named(
        workspace.path(),
        "second-protected-target.exe",
        "sandbox_protected_child_probe",
    );
    second
        .env(PROTECTED_FILE_KEY, &protected_file)
        .env(DELAY_MILLIS_KEY, "1200")
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(workspace.path(), [protected]));

    let mut first = first.spawn().unwrap();
    let mut second = second.spawn().unwrap();
    assert!(first.wait().await.unwrap().success());
    drop(first);
    assert!(second.wait().await.unwrap().success());
    drop(second);
    assert_eq!(std::fs::read(&protected_file).unwrap(), b"runtime-state");
}

#[test]
fn workspace_sandbox_rejects_external_hardlink_before_spawn() {
    let workspace = tempfile::tempdir().unwrap();
    let external = tempfile::tempdir().unwrap();
    let source = workspace.path().join("workspace-hardlink-source");
    std::fs::write(&source, b"shared-inode").unwrap();
    std::fs::hard_link(&source, external.path().join("external-hardlink")).unwrap();
    let marker = workspace.path().join("must-not-launch");
    let external_file = external.path().join("external-file");
    std::fs::write(&external_file, b"outside").unwrap();
    let mut command = pinned_test_command(workspace.path(), "sandbox_workspace_child_probe");
    command
        .env(WORKSPACE_FILE_KEY, &marker)
        .env(EXTERNAL_FILE_KEY, &external_file)
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []));

    let error = match command.spawn() {
        Ok(_) => panic!("an external hardlink must fail before target spawn"),
        Err(error) => error,
    };
    assert_eq!(sandbox_error(&error), Some(&SandboxError::ExternalHardlink));
    assert!(!marker.exists());
}

#[tokio::test]
async fn workspace_sandbox_allows_hardlinks_wholly_inside_workspace() {
    let workspace = tempfile::tempdir().unwrap();
    let external = tempfile::tempdir().unwrap();
    let source = workspace.path().join("workspace-hardlink-source");
    let alias = workspace.path().join("workspace-hardlink-alias");
    std::fs::write(&source, b"shared-inode").unwrap();
    std::fs::hard_link(&source, &alias).unwrap();
    let marker = workspace.path().join("launched");
    let external_file = external.path().join("external-file");
    std::fs::write(&external_file, b"outside").unwrap();
    let mut command = pinned_test_command(workspace.path(), "sandbox_workspace_child_probe");
    command
        .env(WORKSPACE_FILE_KEY, &marker)
        .env(EXTERNAL_FILE_KEY, &external_file)
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []));

    let mut child = command.spawn().expect("all hardlinks are inside the workspace");
    assert!(child.wait().await.unwrap().success());
    assert_eq!(std::fs::read(&marker).unwrap(), b"workspace-write");
}

#[test]
fn workspace_sandbox_rejects_rotated_protected_identity_before_spawn() {
    let workspace = tempfile::tempdir().unwrap();
    let external = tempfile::tempdir().unwrap();
    let protected = workspace.path().join(".solaris");
    std::fs::create_dir(&protected).unwrap();
    let old_state = protected.join("old-session.sqlite3");
    std::fs::write(&old_state, b"old-state").unwrap();
    let old_file = std::fs::File::open(&old_state).unwrap();
    let old_identity = solaris_process::ProtectedObjectIdentity::from_file(&old_file).unwrap();
    drop(old_file);
    std::fs::hard_link(&old_state, workspace.path().join("rotated-state-alias")).unwrap();
    std::fs::remove_file(&old_state).unwrap();
    std::fs::write(protected.join("session.sqlite3"), b"new-state").unwrap();
    let marker = workspace.path().join("must-not-launch");
    let external_file = external.path().join("external-file");
    std::fs::write(&external_file, b"outside").unwrap();
    let mut command = pinned_test_command(workspace.path(), "sandbox_workspace_child_probe");
    command
        .env(WORKSPACE_FILE_KEY, &marker)
        .env(EXTERNAL_FILE_KEY, &external_file)
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox_with_identities(
            workspace.path(),
            [protected],
            [old_identity],
        ));

    let error = match command.spawn() {
        Ok(_) => panic!("a retained protected identity must fail before target spawn"),
        Err(error) => error,
    };
    assert_eq!(sandbox_error(&error), Some(&SandboxError::ProtectedObjectAlias));
    assert!(!marker.exists());
}

#[test]
fn workspace_sandbox_rejects_junction_before_spawn() {
    let workspace = tempfile::tempdir().unwrap();
    let external = tempfile::tempdir().unwrap();
    let external_file = external.path().join("outside");
    std::fs::write(&external_file, b"outside").unwrap();
    let junction = workspace.path().join("junction-escape");
    windows_support::create_junction(&junction, external.path()).unwrap();
    let marker = workspace.path().join("must-not-launch");
    let mut command = pinned_test_command(workspace.path(), "sandbox_workspace_child_probe");
    command
        .env(WORKSPACE_FILE_KEY, &marker)
        .env(EXTERNAL_FILE_KEY, &external_file)
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []));

    let error = match command.spawn() {
        Ok(_) => panic!("a Windows junction must fail before target spawn"),
        Err(error) => error,
    };
    assert_eq!(sandbox_error(&error), Some(&SandboxError::ReparsePointExposed));
    assert!(!marker.exists());
    std::fs::remove_dir(&junction).unwrap();
    assert_eq!(std::fs::read(&external_file).unwrap(), b"outside");
}

#[test]
fn workspace_authority_rejects_a_junction_root() {
    let parent = tempfile::tempdir().unwrap();
    let target = parent.path().join("target");
    let junction = parent.path().join("junction");
    std::fs::create_dir(&target).unwrap();
    windows_support::create_junction(&junction, &target).unwrap();

    let error = WorkspaceRootAuthority::capture(&junction).unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    std::fs::remove_dir(&junction).unwrap();
}

#[test]
fn sealed_workspace_authority_guards_every_ancestor_until_release() {
    let parent = tempfile::tempdir().unwrap();
    let ancestor = parent.path().join("ancestor");
    let workspace = ancestor.join("workspace");
    let renamed = parent.path().join("renamed-ancestor");
    std::fs::create_dir_all(&workspace).unwrap();
    let authority = WorkspaceRootAuthority::capture(&workspace).unwrap();
    let capability = authority.seal(&workspace).unwrap();

    let error = std::fs::rename(&ancestor, &renamed).unwrap_err();
    assert_eq!(error.raw_os_error(), Some(32), "expected ERROR_SHARING_VIOLATION");

    drop(capability);
    std::fs::rename(&ancestor, &renamed).unwrap();
}

#[cfg(feature = "sandbox-test-fixtures")]
#[test]
fn helper_early_exit_fails_before_windows_target_starts() {
    assert_windows_helper_failure("early-exit");
}

#[cfg(feature = "sandbox-test-fixtures")]
#[test]
fn partial_marker_fails_before_windows_target_starts() {
    assert_windows_helper_failure("partial-marker");
}

#[cfg(feature = "sandbox-test-fixtures")]
#[tokio::test]
async fn handshake_timeout_kills_windows_helper_descendant() {
    let workspace = tempfile::tempdir().unwrap();
    let target_marker = workspace.path().join("target-started");
    let descendant_marker = workspace.path().join("helper-descendant-escaped");
    let mut command = pinned_test_command(workspace.path(), "sandbox_marker_child_probe");
    command
        .env("SOLARIS_SANDBOX_FIXTURE_BEHAVIOR", "timeout-with-descendant")
        .env("SOLARIS_SANDBOX_FIXTURE_DESCENDANT_MARKER", &descendant_marker)
        .env(TARGET_MARKER_KEY, &target_marker)
        .launch_policy(fixture_policy(workspace.path()));

    let error = match command.spawn() {
        Ok(_) => panic!("a handshake timeout must fail before target spawn"),
        Err(error) => error,
    };
    assert_start_confirmation_failed(&error);
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert!(!target_marker.exists());
    assert!(!descendant_marker.exists(), "a failed helper left a descendant running");
}

fn pinned_test_command(workspace: &Path, test_name: &str) -> solaris_process::PinnedCommand {
    pinned_test_command_named(workspace, "windows-sandbox-test.exe", test_name)
}

fn pinned_test_command_named(workspace: &Path, name: &str, test_name: &str) -> solaris_process::PinnedCommand {
    let executable = workspace.join(name);
    std::fs::copy(std::env::current_exe().unwrap(), &executable).unwrap();
    let identity = inspect_executable(&executable).unwrap();
    let mut command = pin_executable(&executable, &identity).unwrap().command().unwrap();
    command.args(["--exact", test_name, "--nocapture"]);
    command
}

fn sandbox_error(error: &std::io::Error) -> Option<&SandboxError> {
    error.get_ref().and_then(|source| source.downcast_ref::<SandboxError>())
}

#[cfg(feature = "sandbox-test-fixtures")]
fn assert_finalization_stages(error: &std::io::Error, expected: &[ProcessFinalizationStage]) {
    let aggregate = error
        .get_ref()
        .and_then(|source| source.downcast_ref::<ProcessFinalizationError>())
        .expect("expected a structured multi-stage finalization error");
    assert_eq!(
        aggregate
            .failures()
            .iter()
            .map(|failure| failure.stage())
            .collect::<Vec<_>>(),
        expected
    );
    let cleanup = aggregate
        .failures()
        .iter()
        .find(|failure| failure.stage() == ProcessFinalizationStage::SandboxCleanup)
        .expect("cleanup failure must be retained");
    assert!(matches!(
        sandbox_error(cleanup.error()),
        Some(SandboxError::CleanupFailed { .. })
    ));
}

#[cfg(feature = "sandbox-test-fixtures")]
fn fixture_policy(workspace: &Path) -> ProcessLaunchPolicy {
    ProcessLaunchPolicy::workspace_sandbox_with_test_helper(
        workspace,
        [],
        [],
        env!("CARGO_BIN_EXE_solaris-process-sandbox-test-helper"),
    )
}

#[cfg(feature = "sandbox-test-fixtures")]
fn assert_windows_helper_failure(behavior: &str) {
    let workspace = tempfile::tempdir().unwrap();
    let target_marker = workspace.path().join("target-started");
    let mut command = pinned_test_command(workspace.path(), "sandbox_marker_child_probe");
    command
        .env("SOLARIS_SANDBOX_FIXTURE_BEHAVIOR", behavior)
        .env(TARGET_MARKER_KEY, &target_marker)
        .launch_policy(fixture_policy(workspace.path()));
    let error = match command.spawn() {
        Ok(_) => panic!("a failed helper must not return a managed child"),
        Err(error) => error,
    };
    assert_start_confirmation_failed(&error);
    assert!(!target_marker.exists());
}

#[cfg(feature = "sandbox-test-fixtures")]
fn assert_start_confirmation_failed(error: &std::io::Error) {
    let Some(SandboxError::InsufficientEnforcement { report }) = sandbox_error(error) else {
        panic!("expected a structured Windows sandbox enforcement error");
    };
    assert_eq!(report.enforcement(), SandboxEnforcement::Partial);
    assert_eq!(report.reason(), SandboxReason::StartConfirmationFailed);
}
