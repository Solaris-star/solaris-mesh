#![cfg(target_os = "macos")]

use std::ffi::CString;
use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::os::fd::FromRawFd;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::time::Duration;

use solaris_process::{
    CommandRunner, ProcessLaunchPolicy, ProtectedObjectIdentity, SandboxBackend, SandboxEnforcement, SandboxError,
    SandboxReason, inspect_executable, pin_executable, platform_sandbox_report,
};

#[path = "support/sandbox_proxy.rs"]
mod sandbox_proxy;

const TARGET_MARKER_KEY: &str = "SOLARIS_MACOS_SANDBOX_TARGET_MARKER";
const EXTERNAL_SECRET_KEY: &str = "SOLARIS_MACOS_SANDBOX_EXTERNAL_SECRET";
const EXTERNAL_WRITE_KEY: &str = "SOLARIS_MACOS_SANDBOX_EXTERNAL_WRITE";
const SYMLINK_KEY: &str = "SOLARIS_MACOS_SANDBOX_EXTERNAL_SYMLINK";
const PROTECTED_SECRET_KEY: &str = "SOLARIS_MACOS_SANDBOX_PROTECTED_SECRET";
const LOOPBACK_KEY: &str = "SOLARIS_MACOS_SANDBOX_LOOPBACK";
const ESCAPE_MARKER_KEY: &str = "SOLARIS_MACOS_SANDBOX_ESCAPE_MARKER";

#[test]
fn platform_probe_reports_full_only_after_real_seatbelt_checks() {
    let report = platform_sandbox_report();

    assert_eq!(report.enforcement(), SandboxEnforcement::Full);
    assert_eq!(report.backend(), SandboxBackend::MacOsSeatbelt);
    assert_eq!(report.reason(), SandboxReason::Enforced);
}

#[test]
fn sandbox_filesystem_child_probe() {
    let Some(marker) = std::env::var_os(TARGET_MARKER_KEY) else {
        return;
    };
    let external_secret = PathBuf::from(std::env::var_os(EXTERNAL_SECRET_KEY).unwrap());
    let external_write = PathBuf::from(std::env::var_os(EXTERNAL_WRITE_KEY).unwrap());
    let symlink = PathBuf::from(std::env::var_os(SYMLINK_KEY).unwrap());
    let workspace_write = std::fs::write(&marker, b"workspace-write").is_ok();
    let external_read = std::fs::read(&external_secret).is_ok();
    let external_write = std::fs::write(&external_write, b"exposed").is_ok();
    let symlink_read = std::fs::read(&symlink).is_ok();
    assert!(workspace_write);
    assert!(!external_read);
    assert!(!external_write);
    assert!(!symlink_read);
}

#[tokio::test]
async fn workspace_is_writable_but_external_paths_and_symlinks_are_denied() {
    use std::os::unix::fs::symlink;

    let workspace = tempfile::tempdir().unwrap();
    let external = tempfile::tempdir().unwrap();
    let marker = workspace.path().join("workspace-write");
    let external_secret = external.path().join("host-secret");
    let external_write = external.path().join("host-write");
    let symlink_path = workspace.path().join("external-alias");
    std::fs::write(&external_secret, b"secret").unwrap();
    symlink(&external_secret, &symlink_path).unwrap();
    let mut command = pinned_test_command("sandbox_filesystem_child_probe");
    command
        .env(TARGET_MARKER_KEY, &marker)
        .env(EXTERNAL_SECRET_KEY, &external_secret)
        .env(EXTERNAL_WRITE_KEY, &external_write)
        .env(SYMLINK_KEY, &symlink_path);

    let result = CommandRunner::new_pinned(command)
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []))
        .run()
        .await
        .unwrap();

    assert_eq!(result.exit_code, Some(0));
    assert_eq!(result.sandbox_report.enforcement(), SandboxEnforcement::Full);
    assert_eq!(std::fs::read(&marker).unwrap(), b"workspace-write");
    assert_eq!(std::fs::read(&external_secret).unwrap(), b"secret");
    assert!(!external_write.exists());
}

#[test]
fn sandbox_protected_state_child_probe() {
    let Some(marker) = std::env::var_os(TARGET_MARKER_KEY) else {
        return;
    };
    let protected = PathBuf::from(std::env::var_os(PROTECTED_SECRET_KEY).unwrap());
    assert!(std::fs::write(marker, b"workspace-write").is_ok());
    assert!(is_sandbox_denial(std::fs::read(&protected).unwrap_err()));
    assert!(is_sandbox_denial(
        std::fs::OpenOptions::new().append(true).open(protected).unwrap_err()
    ));
}

#[tokio::test]
async fn nested_runtime_state_is_hidden_and_unchanged() {
    let workspace = tempfile::tempdir().unwrap();
    let protected_root = workspace.path().join(".solaris");
    let protected_secret = protected_root.join("runtime/secret");
    let marker = workspace.path().join("workspace-write");
    std::fs::create_dir_all(protected_secret.parent().unwrap()).unwrap();
    std::fs::write(&protected_secret, b"runtime-secret").unwrap();
    let protected_identity =
        ProtectedObjectIdentity::from_file(&std::fs::File::open(&protected_secret).unwrap()).unwrap();
    let mut command = pinned_test_command("sandbox_protected_state_child_probe");
    command
        .env(TARGET_MARKER_KEY, &marker)
        .env(PROTECTED_SECRET_KEY, &protected_secret);
    let policy = ProcessLaunchPolicy::workspace_sandbox_with_identities(
        workspace.path(),
        [protected_root],
        [protected_identity],
    );

    let result = CommandRunner::new_pinned(command)
        .launch_policy(policy)
        .run()
        .await
        .unwrap();

    assert_eq!(result.exit_code, Some(0));
    assert_eq!(result.sandbox_report.enforcement(), SandboxEnforcement::Full);
    assert_eq!(std::fs::read(&protected_secret).unwrap(), b"runtime-secret");
}

#[test]
fn sandbox_network_child_probe() {
    let Some(marker) = std::env::var_os(TARGET_MARKER_KEY) else {
        return;
    };
    let loopback = std::env::var(LOOPBACK_KEY).unwrap().parse::<SocketAddr>().unwrap();
    let public = SocketAddr::from(([93, 184, 216, 34], 80));
    assert!(connection_is_sandbox_denied(loopback));
    assert!(connection_is_sandbox_denied(public));
    std::fs::write(marker, b"network-denied").unwrap();
}

#[tokio::test]
async fn direct_loopback_and_ip_network_are_denied() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let marker = workspace.path().join("network-denied");
    let mut command = pinned_test_command("sandbox_network_child_probe");
    command
        .env(TARGET_MARKER_KEY, &marker)
        .env(LOOPBACK_KEY, listener.local_addr().unwrap().to_string());

    let result = CommandRunner::new_pinned(command)
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []))
        .run()
        .await
        .unwrap();

    assert_eq!(result.exit_code, Some(0));
    assert_eq!(result.sandbox_report.enforcement(), SandboxEnforcement::Full);
    assert_eq!(std::fs::read(marker).unwrap(), b"network-denied");
}

#[test]
fn sandbox_proxy_child_probe() {
    if std::env::var_os(sandbox_proxy::PROBE_MODE_KEY).is_none() {
        return;
    }
    sandbox_proxy::child_probe();
    let proxy = std::env::var("HTTP_PROXY")
        .unwrap()
        .strip_prefix("http://")
        .unwrap()
        .parse::<SocketAddr>()
        .unwrap();
    assert_proxy_denied(proxy, "denied.invalid", 443);
    assert_proxy_denied(proxy, "93.184.216.34", 80);
    assert_proxy_denied(proxy, "127.0.0.1", 65_534);
}

#[cfg(feature = "sandbox-test-fixtures")]
#[tokio::test]
async fn approved_domain_works_only_through_the_host_proxy() {
    let origin = sandbox_proxy::OriginProbe::spawn();
    let workspace = tempfile::tempdir().unwrap();
    let marker = workspace.path().join("proxy-verified");
    let mut command = pinned_test_command("sandbox_proxy_child_probe");
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
fn sandbox_marker_child_probe() {
    let Some(marker) = std::env::var_os(TARGET_MARKER_KEY) else {
        return;
    };
    std::fs::write(marker, b"target-started").unwrap();
}

#[test]
fn workspace_path_replacement_is_rejected_before_target_exec() {
    let parent = tempfile::tempdir().unwrap();
    let workspace = parent.path().join("workspace");
    let retained = parent.path().join("retained");
    let marker = retained.join("target-started");
    std::fs::create_dir(&workspace).unwrap();
    let policy = ProcessLaunchPolicy::workspace_sandbox(&workspace, []);
    let mut command = pinned_test_command("sandbox_marker_child_probe");
    command.env(TARGET_MARKER_KEY, &marker).launch_policy(policy);
    std::fs::rename(&workspace, &retained).unwrap();
    std::fs::create_dir(&workspace).unwrap();

    let error = command.spawn().expect_err("replaced workspace path must be rejected");

    let report = sandbox_report(&error).expect("expected a structured workspace binding report");
    assert_eq!(report.enforcement(), SandboxEnforcement::Unavailable);
    assert_eq!(report.backend(), SandboxBackend::MacOsSeatbelt);
    assert_eq!(report.reason(), SandboxReason::WorkspaceObjectBindingUnavailable);
    assert!(!marker.exists());
}

#[test]
fn external_hardlink_is_rejected_before_target_exec() {
    let workspace = tempfile::tempdir().unwrap();
    let external = tempfile::tempdir().unwrap();
    let external_file = external.path().join("host-state");
    let alias = workspace.path().join("host-state-alias");
    let marker = workspace.path().join("target-started");
    std::fs::write(&external_file, b"host-state").unwrap();
    std::fs::hard_link(&external_file, &alias).unwrap();
    let mut command = pinned_test_command("sandbox_marker_child_probe");
    command
        .env(TARGET_MARKER_KEY, &marker)
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []));

    let error = command.spawn().expect_err("external hardlink must be rejected");

    assert_eq!(sandbox_error(&error), Some(&SandboxError::ExternalHardlink));
    assert!(!marker.exists());
    assert_eq!(std::fs::read(external_file).unwrap(), b"host-state");
}

#[test]
fn workspace_fifo_is_rejected_before_target_exec() {
    let workspace = tempfile::tempdir().unwrap();
    let fifo = workspace.path().join("host.fifo");
    let marker = workspace.path().join("target-started");
    let fifo_name = CString::new(fifo.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(fifo_name.as_ptr(), 0o600) }, 0);
    let fifo_fd = unsafe { libc::open(fifo_name.as_ptr(), libc::O_RDWR | libc::O_NONBLOCK) };
    assert_ne!(fifo_fd, -1);
    let _host_endpoint = unsafe { std::fs::File::from_raw_fd(fifo_fd) };
    let mut command = pinned_test_command("sandbox_marker_child_probe");
    command
        .env(TARGET_MARKER_KEY, &marker)
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []));

    let error = command.spawn().expect_err("workspace FIFO must be rejected");

    assert_eq!(sandbox_error(&error), Some(&SandboxError::HostSocketExposed));
    assert!(!marker.exists());
}

#[test]
#[allow(clippy::zombie_processes)]
fn sandbox_escape_child_probe() {
    let Some(marker) = std::env::var_os(ESCAPE_MARKER_KEY) else {
        return;
    };
    let marker = CString::new(marker.as_bytes()).unwrap();
    let program = CString::new("/usr/bin/true").unwrap();
    let mut descriptors = [-1_i32; 2];
    assert_eq!(unsafe { libc::pipe(descriptors.as_mut_ptr()) }, 0);
    let pid = unsafe { libc::fork() };
    assert_ne!(pid, -1);
    if pid == 0 {
        unsafe {
            let _ = libc::close(descriptors[0]);
            let setsid_denied = libc::setsid() == -1;
            let setpgid_denied = libc::setpgid(0, 0) == -1;
            let mut spawned = 0;
            let mut argv = [program.as_ptr().cast_mut(), std::ptr::null_mut()];
            let mut environment = [std::ptr::null_mut::<libc::c_char>()];
            let spawn_denied = libc::posix_spawn(
                &mut spawned,
                program.as_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                argv.as_mut_ptr(),
                environment.as_mut_ptr(),
            ) != 0;
            if !spawn_denied {
                let _ = libc::waitpid(spawned, std::ptr::null_mut(), 0);
            }
            let denied = u8::from(setsid_denied && setpgid_denied && spawn_denied);
            let _ = libc::write(descriptors[1], std::ptr::addr_of!(denied).cast(), 1);
            let _ = libc::close(descriptors[1]);
            libc::sleep(1);
            let file = libc::open(marker.as_ptr(), libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC, 0o600);
            if file != -1 {
                let escaped = b"escaped";
                let _ = libc::write(file, escaped.as_ptr().cast(), escaped.len());
                let _ = libc::close(file);
            }
            libc::_exit(0);
        }
    }
    unsafe {
        let _ = libc::close(descriptors[1]);
        let mut denied = 0_u8;
        assert_eq!(libc::read(descriptors[0], std::ptr::addr_of_mut!(denied).cast(), 1), 1);
        let _ = libc::close(descriptors[0]);
        assert_eq!(denied, 1, "a descendant escaped the managed process group");
    }
}

#[tokio::test]
async fn descendants_cannot_change_session_or_survive_managed_wait() {
    let workspace = tempfile::tempdir().unwrap();
    let marker = workspace.path().join("descendant-escaped");
    let mut command = pinned_test_command("sandbox_escape_child_probe");
    command.env(ESCAPE_MARKER_KEY, &marker);

    let result = CommandRunner::new_pinned(command)
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []))
        .post_process_drain(Duration::from_secs(2))
        .run()
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1_200)).await;

    assert_eq!(result.exit_code, Some(0));
    assert_eq!(result.sandbox_report.enforcement(), SandboxEnforcement::Full);
    assert!(!marker.exists());
}

#[cfg(feature = "sandbox-test-fixtures")]
#[test]
fn malformed_start_handshake_fails_before_target_exec() {
    let workspace = tempfile::tempdir().unwrap();
    let marker = workspace.path().join("target-started");
    let mut command = pinned_test_command("sandbox_marker_child_probe");
    command
        .env("SOLARIS_SANDBOX_FIXTURE_BEHAVIOR", "partial-marker")
        .env(TARGET_MARKER_KEY, &marker)
        .launch_policy(ProcessLaunchPolicy::workspace_sandbox_with_test_helper(
            workspace.path(),
            [],
            [],
            env!("CARGO_BIN_EXE_solaris-process-sandbox-test-helper"),
        ));

    let error = command
        .spawn()
        .expect_err("partial helper marker must reject the target");

    let report = sandbox_report(&error).expect("expected a structured handshake report");
    assert_eq!(report.enforcement(), SandboxEnforcement::Unavailable);
    assert_eq!(report.backend(), SandboxBackend::MacOsSeatbelt);
    assert_eq!(report.reason(), SandboxReason::StartConfirmationFailed);
    assert!(!marker.exists());
}

fn pinned_test_command(test_name: &str) -> solaris_process::PinnedCommand {
    let executable = std::env::current_exe().unwrap();
    let identity = inspect_executable(&executable).unwrap();
    let mut command = pin_executable(&executable, &identity).unwrap().command().unwrap();
    command.args(["--exact", test_name, "--nocapture"]);
    command
}

fn sandbox_error(error: &std::io::Error) -> Option<&SandboxError> {
    error.get_ref().and_then(|source| source.downcast_ref::<SandboxError>())
}

fn sandbox_report(error: &std::io::Error) -> Option<solaris_process::SandboxReport> {
    solaris_process::sandbox_report_from_error(error)
}

fn is_sandbox_denial(error: std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::PermissionDenied
        || matches!(error.raw_os_error(), Some(libc::EPERM | libc::EACCES))
}

fn connection_is_sandbox_denied(address: SocketAddr) -> bool {
    TcpStream::connect_timeout(&address, Duration::from_secs(1)).is_err_and(is_sandbox_denial)
}

fn assert_proxy_denied(proxy: SocketAddr, host: &str, port: u16) {
    let mut stream = TcpStream::connect_timeout(&proxy, Duration::from_secs(1)).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let request = format!("CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n\r\n");
    stream.write_all(request.as_bytes()).unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).unwrap();
    assert!(response.starts_with(b"HTTP/1.1 403"));
}
