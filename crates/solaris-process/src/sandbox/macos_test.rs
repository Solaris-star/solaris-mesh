use std::ffi::OsString;
use std::fs::File;
use std::net::{Ipv4Addr, SocketAddr};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::path::Path;

use tokio::process::Command;

use super::{
    MacOsWrapperFiles, SandboxBackend, SandboxEnforcement, SandboxReason, append_proxy_environment,
    bind_private_proxy_listener, build_seatbelt_argv, descriptor_sanitize_limit, explicit_environment,
    pinned_target_fd, replace_pinned_command, workspace_binding_error,
};

#[test]
fn duplicated_target_survives_original_command_drop_and_fd_reuse() {
    let source = File::open("/usr/bin/true").unwrap();
    let original_fd = source.as_raw_fd();
    let mut command = Command::new(format!("/dev/fd/{original_fd}"));
    unsafe {
        command.pre_exec(move || {
            if libc::fcntl(source.as_raw_fd(), libc::F_SETFD, 0) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    assert_eq!(pinned_target_fd(&command).unwrap(), original_fd);
    let wrapper_files = MacOsWrapperFiles {
        seatbelt: File::open("/dev/null").unwrap(),
        helper: File::open("/dev/null").unwrap(),
        status_writer: File::open("/dev/null").unwrap(),
        proxy_listener: None,
    };
    replace_pinned_command(
        &mut command,
        wrapper_files,
        descriptor_sanitize_limit().unwrap(),
        None,
        |target_fd| Command::new(format!("/dev/fd/{target_fd}")),
    )
    .unwrap();

    let (socket, _peer) = UnixStream::pair().unwrap();
    assert_ne!(unsafe { libc::dup2(socket.as_raw_fd(), original_fd) }, -1);
    let mut reused: libc::stat = unsafe { std::mem::zeroed() };
    assert_eq!(unsafe { libc::fstat(original_fd, &mut reused) }, 0);
    assert_eq!(reused.st_mode & libc::S_IFMT, libc::S_IFSOCK);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let status = runtime.block_on(async move { command.spawn().unwrap().wait().await.unwrap() });

    assert!(status.success());
    if socket.as_raw_fd() != original_fd {
        assert_eq!(unsafe { libc::close(original_fd) }, 0);
    }
}

#[test]
fn workspace_binding_failure_keeps_a_structured_macos_report() {
    let error = workspace_binding_error();
    let report = super::super::sandbox_report_from_error(&error).unwrap();

    assert_eq!(report.enforcement(), SandboxEnforcement::Unavailable);
    assert_eq!(report.backend(), SandboxBackend::MacOsSeatbelt);
    assert_eq!(report.reason(), SandboxReason::WorkspaceObjectBindingUnavailable);
}

#[test]
fn private_proxy_listeners_use_distinct_ephemeral_ipv4_ports() {
    let first = bind_private_proxy_listener().unwrap();
    let second = bind_private_proxy_listener().unwrap();
    let first_address = first.local_addr().unwrap();
    let second_address = second.local_addr().unwrap();

    assert!(matches!(
        first_address,
        SocketAddr::V4(address) if *address.ip() == Ipv4Addr::LOCALHOST && address.port() != 0
    ));
    assert!(matches!(
        second_address,
        SocketAddr::V4(address) if *address.ip() == Ipv4Addr::LOCALHOST && address.port() != 0
    ));
    assert_ne!(first_address.port(), second_address.port());
    assert_ne!(
        unsafe { libc::fcntl(first.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
        0
    );
}

#[test]
fn proxy_environment_uses_the_prebound_listener_port() {
    let mut command = Command::new("/bin/echo");
    command
        .env_clear()
        .env("HTTP_PROXY", "http://untrusted.invalid:8080")
        .env("NO_PROXY", "*");

    let environment = explicit_environment(
        &command,
        Path::new("/private/home"),
        Path::new("/private/tmp"),
        Some(41_237),
        Some(Path::new("/private/home/.solaris-network-proxy-ca.pem")),
    )
    .unwrap();

    for key in [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ] {
        assert_eq!(
            environment
                .iter()
                .find(|(candidate, _)| candidate == key)
                .map(|(_, value)| value),
            Some(&OsString::from("http://127.0.0.1:41237"))
        );
    }
    for key in ["NO_PROXY", "no_proxy"] {
        assert_eq!(
            environment
                .iter()
                .find(|(candidate, _)| candidate == key)
                .map(|(_, value)| value),
            Some(&OsString::new())
        );
    }
    for key in ["SSL_CERT_FILE", "NODE_EXTRA_CA_CERTS", "REQUESTS_CA_BUNDLE"] {
        assert_eq!(
            environment
                .iter()
                .find(|(candidate, _)| candidate == key)
                .map(|(_, value)| value),
            Some(&OsString::from("/private/home/.solaris-network-proxy-ca.pem"))
        );
    }
}

#[test]
fn zero_proxy_port_is_rejected() {
    assert!(
        append_proxy_environment(
            &mut Vec::new(),
            0,
            Path::new("/private/home/.solaris-network-proxy-ca.pem")
        )
        .is_err()
    );
}

#[test]
fn seatbelt_argv_passes_the_listener_descriptor_instead_of_a_fixed_port() {
    let argv = build_seatbelt_argv(
        "(version 1)",
        Path::new("/work/repo"),
        9,
        Path::new("/private/denied"),
        45_001,
        10,
        11,
        &[OsString::from("argument")],
        Some((Path::new("/private/state/network-proxy.sock"), 12)),
    );

    let listener_flag = argv
        .iter()
        .position(|argument| argument == "--proxy-listener-fd")
        .unwrap();
    assert_eq!(argv[listener_flag + 1], "12");
    assert!(!argv.iter().any(|argument| argument == "--proxy-port"));
    assert!(!argv.iter().any(|argument| argument == "29347"));
}
