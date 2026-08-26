use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use rcgen::{CertificateParams, ExtendedKeyUsagePurpose, KeyPair};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName};
use rustls::{ClientConfig, ClientConnection, RootCertStore, ServerConfig, ServerConnection, StreamOwned};

use super::host_tls::client_config;
use super::{
    DestinationConnector, HostNetworkProxy, MAX_ACTIVE_PROXY_CONNECTIONS, MAX_PENDING_PROXY_CONNECTIONS,
    connect_public_with,
};
use crate::NetworkProxyPolicy;

static HOST_TEST_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn host_proxy_rejects_connect_when_the_destination_is_not_approved() {
    let _lock = lock_host_tests();
    let state = tempfile::tempdir().unwrap();
    let socket = state.path().join("proxy.sock");
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let connector = Arc::new(RecordingConnector::without_destination(Arc::clone(&attempts)));
    let policy = NetworkProxyPolicy::from_permission_domains(["https://allowed.example.test"]).unwrap();
    let _proxy = HostNetworkProxy::start_with_connector(&socket, policy, connector).unwrap();
    let mut client = UnixStream::connect(socket).unwrap();
    client
        .write_all(b"CONNECT denied.example.test:443 HTTP/1.1\r\nHost: denied.example.test:443\r\n\r\n")
        .unwrap();
    let mut response = String::new();
    client.read_to_string(&mut response).unwrap();

    assert!(response.starts_with("HTTP/1.1 403"), "{response}");
    assert!(attempts.lock().unwrap().is_empty());
}

#[test]
fn approved_connect_terminates_tls_and_verifies_the_upstream_certificate() {
    let _lock = lock_host_tests();
    let host = "allowed.example.test";
    let (origin_address, origin_root, origin) = spawn_tls_origin(host);
    let state = tempfile::tempdir().unwrap();
    let socket = state.path().join("proxy.sock");
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let connector = Arc::new(RecordingConnector::to(origin_address, Arc::clone(&attempts)));
    let policy = NetworkProxyPolicy::from_permission_domains([format!("https://{host}")]).unwrap();
    let upstream_config = trusted_client_config(origin_root);
    let proxy = HostNetworkProxy::start_with_connector_and_upstream_config(&socket, policy, connector, upstream_config)
        .unwrap();
    let mut tls = connect_tls_client(&socket, &proxy, host);
    tls.write_all(b"GET /approved HTTP/1.1\r\nHost: allowed.example.test\r\n\r\n")
        .unwrap();
    tls.conn.send_close_notify();
    tls.flush().unwrap();
    let mut response = String::new();
    tls.read_to_string(&mut response).unwrap();
    let request = origin.join().unwrap();

    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert_eq!(*attempts.lock().unwrap(), [(host.to_owned(), 443)]);
    assert!(request.starts_with(b"GET /approved HTTP/1.1\r\n"));
    assert!(
        request
            .windows(b"Host: allowed.example.test\r\n".len())
            .any(|window| window == b"Host: allowed.example.test\r\n")
    );
}

#[test]
fn tls_pipeline_cannot_change_the_connect_host() {
    let _lock = lock_host_tests();
    let host = "allowed.example.test";
    let (origin_address, origin_root, origin) = spawn_tls_origin(host);
    let state = tempfile::tempdir().unwrap();
    let socket = state.path().join("proxy.sock");
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let connector = Arc::new(RecordingConnector::to(origin_address, Arc::clone(&attempts)));
    let policy = NetworkProxyPolicy::from_permission_domains([
        format!("https://{host}"),
        "https://other.example.test".to_owned(),
    ])
    .unwrap();
    let proxy = HostNetworkProxy::start_with_connector_and_upstream_config(
        &socket,
        policy,
        connector,
        trusted_client_config(origin_root),
    )
    .unwrap();
    let mut tls = connect_tls_client(&socket, &proxy, host);
    tls.write_all(
        b"GET /first HTTP/1.1\r\nHost: allowed.example.test\r\n\r\n\
          GET /second HTTP/1.1\r\nHost: other.example.test\r\n\r\n",
    )
    .unwrap();
    tls.conn.send_close_notify();
    tls.flush().unwrap();
    let mut response = String::new();
    tls.read_to_string(&mut response).unwrap();
    let request = origin.join().unwrap();

    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.contains("HTTP/1.1 403"), "{response}");
    assert_eq!(*attempts.lock().unwrap(), [(host.to_owned(), 443)]);
    assert!(request.starts_with(b"GET /first HTTP/1.1\r\n"));
    assert!(
        request
            .windows("other.example.test".len())
            .all(|window| window != b"other.example.test")
    );
}

#[test]
fn dropping_proxy_interrupts_a_stalled_tls_handshake() {
    let _lock = lock_host_tests();
    let state = tempfile::tempdir().unwrap();
    let socket = state.path().join("proxy.sock");
    let policy = NetworkProxyPolicy::from_permission_domains(["https://allowed.example.test"]).unwrap();
    let proxy = HostNetworkProxy::start(&socket, policy).unwrap();
    let mut client = UnixStream::connect(&socket).unwrap();
    client
        .write_all(b"CONNECT allowed.example.test:443 HTTP/1.1\r\nHost: allowed.example.test:443\r\n\r\n")
        .unwrap();
    let response = read_header(&mut client);
    assert!(response.starts_with(b"HTTP/1.1 200"));
    wait_until(|| proxy.active_connection_count() == 1);

    let started = Instant::now();
    drop(proxy);

    assert!(started.elapsed() < Duration::from_secs(1));
}

#[test]
fn pipelined_request_cannot_change_the_approved_host() {
    let _lock = lock_host_tests();
    let (origin_address, origin) = spawn_origin();
    let state = tempfile::tempdir().unwrap();
    let socket = state.path().join("proxy.sock");
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let connector = Arc::new(RecordingConnector::to(origin_address, Arc::clone(&attempts)));
    let policy = NetworkProxyPolicy::from_permission_domains([
        "http://first.example.test:8080",
        "http://second.example.test:8080",
    ])
    .unwrap();
    let _proxy = HostNetworkProxy::start_with_connector(&socket, policy, connector).unwrap();
    let mut client = UnixStream::connect(socket).unwrap();
    client
        .write_all(
            b"GET http://first.example.test:8080/first HTTP/1.1\r\nHost: first.example.test:8080\r\n\r\n\
              GET http://second.example.test:8080/second HTTP/1.1\r\nHost: second.example.test:8080\r\n\r\n",
        )
        .unwrap();
    let mut response = String::new();
    client.read_to_string(&mut response).unwrap();
    let request = origin.join().unwrap();

    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.contains("HTTP/1.1 403"), "{response}");
    assert_eq!(*attempts.lock().unwrap(), [("first.example.test".to_owned(), 8080)]);
    assert!(request.starts_with(b"GET /first HTTP/1.1\r\n"));
    assert!(
        request
            .windows("second.example.test".len())
            .all(|window| window != b"second.example.test")
    );
}

#[test]
fn proxy_never_treats_an_approved_public_name_as_host_loopback() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    let state = tempfile::tempdir().unwrap();
    let socket = state.path().join("proxy.sock");
    let policy = NetworkProxyPolicy::from_permission_domains([format!("http://localhost:{port}")]);
    assert!(policy.is_err());
    drop(listener);
    assert!(!socket.exists());
}

#[test]
fn host_proxy_refuses_connections_beyond_the_shared_workers_and_queue() {
    let _lock = lock_host_tests();
    let state = tempfile::tempdir().unwrap();
    let socket = state.path().join("proxy.sock");
    let policy = NetworkProxyPolicy::from_permission_domains(["https://allowed.example.test"]).unwrap();
    let proxy = HostNetworkProxy::start(&socket, policy).unwrap();

    let mut clients = Vec::new();
    for _ in 0..MAX_ACTIVE_PROXY_CONNECTIONS {
        clients.push(UnixStream::connect(&socket).unwrap());
    }
    wait_until(|| proxy.active_connection_count() == MAX_ACTIVE_PROXY_CONNECTIONS);
    for _ in 0..MAX_PENDING_PROXY_CONNECTIONS {
        clients.push(UnixStream::connect(&socket).unwrap());
    }
    wait_until(|| proxy.pending_connection_count() == MAX_PENDING_PROXY_CONNECTIONS);

    let mut rejected = UnixStream::connect(&socket).unwrap();
    rejected.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    rejected
        .write_all(b"CONNECT allowed.example.test:443 HTTP/1.1\r\nHost: allowed.example.test:443\r\n\r\n")
        .unwrap();
    let mut response = String::new();
    rejected.read_to_string(&mut response).unwrap();
    assert!(response.starts_with("HTTP/1.1 503"), "{response}");

    let started = Instant::now();
    drop(proxy);
    assert!(started.elapsed() < Duration::from_secs(2));
    drop(clients);
}

#[test]
fn dropping_host_proxy_stops_resolution_and_address_attempts() {
    let _lock = lock_host_tests();
    let state = tempfile::tempdir().unwrap();
    let socket = state.path().join("proxy.sock");
    let running = Arc::new(AtomicUsize::new(0));
    let connector = Arc::new(BlockingConnector {
        running: Arc::clone(&running),
    });
    let policy = NetworkProxyPolicy::from_permission_domains(["http://allowed.example.test:8080"]).unwrap();
    let proxy = HostNetworkProxy::start_with_connector(&socket, policy, connector).unwrap();
    let worker_liveness = proxy.worker_liveness();
    let mut client = UnixStream::connect(&socket).unwrap();
    client
        .write_all(b"GET http://allowed.example.test:8080/ HTTP/1.1\r\nHost: allowed.example.test:8080\r\n\r\n")
        .unwrap();
    wait_until(|| running.load(Ordering::Acquire) == 1);

    let started = Instant::now();
    drop(proxy);

    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(running.load(Ordering::Acquire), 0);
    assert_eq!(worker_liveness.load(Ordering::Acquire), 0);
}

#[test]
fn multiple_blackhole_addresses_share_one_total_deadline() {
    let addresses = vec![SocketAddr::from((Ipv4Addr::LOCALHOST, 9)); MAX_ACTIVE_PROXY_CONNECTIONS];
    let stopped = std::sync::atomic::AtomicBool::new(false);
    let mut attempts = 0;
    let started = Instant::now();
    let error = connect_public_with(
        &addresses,
        &stopped,
        Instant::now() + Duration::from_millis(120),
        |_, timeout| {
            attempts += 1;
            std::thread::sleep(timeout);
            Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "simulated blackhole"))
        },
    )
    .unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
    assert!(attempts <= 2);
    assert!(started.elapsed() < Duration::from_millis(500));
}

#[test]
fn proxy_instances_share_one_bounded_worker_pool() {
    let _lock = lock_host_tests();
    let first_state = tempfile::tempdir().unwrap();
    let second_state = tempfile::tempdir().unwrap();
    let first_socket = first_state.path().join("proxy.sock");
    let second_socket = second_state.path().join("proxy.sock");
    let policy = NetworkProxyPolicy::from_permission_domains(["https://allowed.example.test"]).unwrap();
    let first = HostNetworkProxy::start(&first_socket, policy.clone()).unwrap();
    let second = HostNetworkProxy::start(&second_socket, policy).unwrap();
    let first_liveness = first.worker_liveness();
    let second_liveness = second.worker_liveness();
    wait_until(|| first_liveness.load(Ordering::Acquire) == MAX_ACTIVE_PROXY_CONNECTIONS);

    assert!(Arc::ptr_eq(&first_liveness, &second_liveness));
    assert_eq!(first_liveness.load(Ordering::Acquire), MAX_ACTIVE_PROXY_CONNECTIONS);
    drop(first);
    assert_eq!(second_liveness.load(Ordering::Acquire), MAX_ACTIVE_PROXY_CONNECTIONS);
    drop(second);
    assert_eq!(second_liveness.load(Ordering::Acquire), 0);
}

struct RecordingConnector {
    destination: Option<SocketAddr>,
    attempts: Arc<Mutex<Vec<(String, u16)>>>,
}

impl RecordingConnector {
    fn to(destination: SocketAddr, attempts: Arc<Mutex<Vec<(String, u16)>>>) -> Self {
        Self {
            destination: Some(destination),
            attempts,
        }
    }

    fn without_destination(attempts: Arc<Mutex<Vec<(String, u16)>>>) -> Self {
        Self {
            destination: None,
            attempts,
        }
    }
}

impl DestinationConnector for RecordingConnector {
    fn connect(
        &self,
        host: &str,
        port: u16,
        _stopped: &std::sync::atomic::AtomicBool,
        _deadline: Instant,
    ) -> std::io::Result<TcpStream> {
        self.attempts.lock().unwrap().push((host.to_owned(), port));
        TcpStream::connect(
            self.destination
                .ok_or_else(|| std::io::Error::other("unexpected destination connection"))?,
        )
    }
}

struct BlockingConnector {
    running: Arc<AtomicUsize>,
}

impl DestinationConnector for BlockingConnector {
    fn connect(
        &self,
        _host: &str,
        _port: u16,
        stopped: &std::sync::atomic::AtomicBool,
        deadline: Instant,
    ) -> std::io::Result<TcpStream> {
        let _running = RunningConnector::new(Arc::clone(&self.running));
        while !stopped.load(Ordering::Acquire) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        if stopped.load(Ordering::Acquire) {
            Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "proxy is stopping",
            ))
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "simulated DNS and address attempts timed out",
            ))
        }
    }
}

struct RunningConnector(Arc<AtomicUsize>);

impl RunningConnector {
    fn new(running: Arc<AtomicUsize>) -> Self {
        running.fetch_add(1, Ordering::AcqRel);
        Self(running)
    }
}

impl Drop for RunningConnector {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

fn spawn_origin() -> (SocketAddr, JoinHandle<Vec<u8>>) {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let mut request = Vec::new();
        stream.read_to_end(&mut request).unwrap();
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .unwrap();
        request
    });
    (address, server)
}

fn lock_host_tests() -> MutexGuard<'static, ()> {
    HOST_TEST_LOCK.lock().unwrap_or_else(|error| error.into_inner())
}

fn wait_until(mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !predicate() {
        assert!(
            Instant::now() < deadline,
            "condition did not become true before timeout"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn trusted_client_config(root: CertificateDer<'static>) -> Arc<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(root).unwrap();
    client_config(roots).unwrap()
}

fn connect_tls_client(
    socket: &std::path::Path,
    proxy: &HostNetworkProxy,
    host: &str,
) -> StreamOwned<ClientConnection, UnixStream> {
    let mut stream = UnixStream::connect(socket).unwrap();
    stream
        .write_all(format!("CONNECT {host}:443 HTTP/1.1\r\nHost: {host}:443\r\n\r\n").as_bytes())
        .unwrap();
    let response = read_header(&mut stream);
    assert!(
        response.starts_with(b"HTTP/1.1 200"),
        "{}",
        String::from_utf8_lossy(&response)
    );
    let mut roots = RootCertStore::empty();
    roots.add(proxy.ca_certificate_der()).unwrap();
    let mut config = ClientConfig::builder_with_provider(rustls::crypto::ring::default_provider().into())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    let server_name = ServerName::try_from(host.to_owned()).unwrap();
    let connection = ClientConnection::new(Arc::new(config), server_name).unwrap();
    StreamOwned::new(connection, stream)
}

fn read_header(reader: &mut impl Read) -> Vec<u8> {
    let mut response = Vec::new();
    while !response.ends_with(b"\r\n\r\n") {
        let mut byte = [0_u8; 1];
        assert_eq!(reader.read(&mut byte).unwrap(), 1);
        response.push(byte[0]);
        assert!(response.len() < 4096);
    }
    response
}

fn spawn_tls_origin(host: &str) -> (SocketAddr, CertificateDer<'static>, JoinHandle<Vec<u8>>) {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec![host.to_owned()]).unwrap();
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let certificate = params.self_signed(&key).unwrap();
    let root = certificate.der().clone();
    let private_key = PrivatePkcs8KeyDer::from(key.serialize_der());
    let mut config = ServerConfig::builder_with_provider(rustls::crypto::ring::default_provider().into())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![certificate.der().clone()], private_key.into())
        .unwrap();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    let config = Arc::new(config);
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        stream.set_write_timeout(Some(Duration::from_secs(2))).unwrap();
        let connection = ServerConnection::new(config).unwrap();
        let mut tls = StreamOwned::new(connection, stream);
        let request = read_header(&mut tls);
        tls.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .unwrap();
        tls.conn.send_close_notify();
        tls.flush().unwrap();
        request
    });
    (address, root, server)
}
