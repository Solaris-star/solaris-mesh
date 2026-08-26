use std::ffi::OsString;
use std::net::{Ipv4Addr, TcpListener};
use std::os::fd::IntoRawFd;

use tempfile::tempdir;

use super::{Invocation, SpawnedTarget, prepare_proxy_listener};

fn invocation(proxy_descriptor: i32) -> Vec<OsString> {
    [
        "--workspace",
        "/work/repo",
        "--status-fd",
        "7",
        "--denied-probe",
        "/private/denied",
        "--denied-port",
        "45001",
        "--proxy-socket",
        "/private/state/network-proxy.sock",
        "--proxy-listener-fd",
        &proxy_descriptor.to_string(),
        "--",
        "/dev/fd/8",
        "argument",
    ]
    .into_iter()
    .map(OsString::from)
    .collect()
}

#[test]
fn invocation_accepts_an_explicit_proxy_listener_descriptor() {
    let invocation = Invocation::parse(invocation(9)).unwrap();

    assert_eq!(invocation.proxy.unwrap().1, 9);
}

#[test]
fn invocation_rejects_reusing_the_status_descriptor_for_the_proxy() {
    assert!(Invocation::parse(invocation(7)).is_err());
}

#[test]
fn invocation_rejects_the_legacy_fixed_proxy_port_contract() {
    let mut arguments = invocation(9);
    let flag = arguments
        .iter()
        .position(|argument| argument == "--proxy-listener-fd")
        .unwrap();
    arguments[flag] = OsString::from("--proxy-port");

    assert!(Invocation::parse(arguments).is_err());
}

#[test]
fn inherited_proxy_listener_must_be_ipv4_loopback() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    let descriptor = listener.into_raw_fd();

    let listener = prepare_proxy_listener(descriptor).unwrap();

    assert_eq!(listener.local_addr().unwrap().port(), port);
}

#[test]
fn spawn_reports_execve_failure_through_the_cloexec_pipe() {
    let directory = tempdir().unwrap();
    let missing_target = directory.path().join("missing-target");

    let error = match SpawnedTarget::spawn(&missing_target, &[], &[]) {
        Ok(mut child) => {
            child.terminate();
            panic!("missing executable unexpectedly started");
        }
        Err(error) => error,
    };

    assert_eq!(error.raw_os_error(), Some(libc::ENOENT));
}
