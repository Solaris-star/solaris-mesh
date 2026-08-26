use std::net::{Ipv4Addr, SocketAddrV4, TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use super::{HOST_CONNECT_TIMEOUT, LocalProxy, MAX_ACTIVE_CONNECTIONS, connect_host_socket};

#[test]
fn prebound_listener_must_be_ipv4_loopback() {
    let state = tempfile::tempdir().unwrap();
    let wildcard = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)).unwrap();
    let error = LocalProxy::start_with_listener(&state.path().join("host.sock"), wildcard)
        .err()
        .expect("wildcard listener must be rejected");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);

    let loopback = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
    let proxy = LocalProxy::start_with_listener(&state.path().join("host.sock"), loopback).unwrap();
    drop(proxy);
}

#[test]
fn missing_host_socket_does_not_delay_shutdown() {
    let state = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let proxy = LocalProxy::start_with_listener(&state.path().join("missing.sock"), listener).unwrap();
    let clients = (0..MAX_ACTIVE_CONNECTIONS)
        .map(|_| TcpStream::connect(address).unwrap())
        .collect::<Vec<_>>();
    std::thread::sleep(Duration::from_millis(50));

    let started = Instant::now();
    drop(proxy);

    assert!(started.elapsed() < Duration::from_secs(1));
    drop(clients);
}

#[test]
fn full_host_socket_backlog_does_not_delay_shutdown() {
    let state = tempfile::tempdir().unwrap();
    let host_socket = state.path().join("host.sock");
    let host_listener = UnixListener::bind(&host_socket).unwrap();
    assert_eq!(unsafe { libc::listen(host_listener.as_raw_fd(), 1) }, 0);
    let stopped = AtomicBool::new(false);
    let mut backlog = Vec::<UnixStream>::new();
    let mut saturated = false;
    for _ in 0..16 {
        match connect_host_socket(&host_socket, &stopped, Instant::now() + Duration::from_millis(50)) {
            Ok(stream) => backlog.push(stream),
            Err(error) if error.kind() == std::io::ErrorKind::TimedOut => {
                saturated = true;
                break;
            }
            Err(error) => panic!("unexpected backlog fill error: {error}"),
        }
    }
    assert!(saturated, "Unix socket backlog did not fill during the test");

    let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let proxy = LocalProxy::start_with_listener(&host_socket, listener).unwrap();
    let clients = (0..MAX_ACTIVE_CONNECTIONS)
        .map(|_| TcpStream::connect(address).unwrap())
        .collect::<Vec<_>>();
    std::thread::sleep(Duration::from_millis(HOST_CONNECT_TIMEOUT.as_millis().min(100) as u64));

    let started = Instant::now();
    drop(proxy);

    assert!(started.elapsed() < Duration::from_secs(1));
    drop(clients);
    drop(backlog);
    drop(host_listener);
}
