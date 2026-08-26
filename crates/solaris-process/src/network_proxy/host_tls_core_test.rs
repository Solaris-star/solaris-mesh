use std::io::{Read, Write};
use std::net::{Ipv4Addr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ClientConnection, RootCertStore, ServerConnection, StreamOwned};

use super::{
    CertificateAuthority, TlsRequestState, client_config, complete_client_handshake, complete_server_handshake,
    default_upstream_config,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(2);

#[test]
fn generated_authority_serves_only_the_exact_sni_and_http_11_alpn() {
    let authority = CertificateAuthority::generate().unwrap();
    let server_config = authority.server_config("allowed.example.test").unwrap();
    let root = authority.certificate_der();
    let upstream_config = default_upstream_config().unwrap();

    assert!(authority.certificate_pem().starts_with(b"-----BEGIN CERTIFICATE-----"));
    assert!(
        !authority
            .certificate_pem()
            .windows(11)
            .any(|window| window == b"PRIVATE KEY")
    );
    assert_eq!(upstream_config.alpn_protocols, [b"http/1.1".to_vec()]);
    assert!(tls_handshake_fails(
        Arc::clone(&server_config),
        client_with_root(root.clone(), Some(b"http/1.1")),
        "different.example.test",
    ));
    assert!(tls_handshake_fails(
        server_config,
        client_with_root(root, None),
        "allowed.example.test",
    ));
}

#[test]
fn tls_request_state_rejects_a_second_cross_host_request_without_relaying_it() {
    let host = "allowed.example.test";
    let authority = CertificateAuthority::generate().unwrap();
    let server_config = authority.server_config(host).unwrap();
    let client_config = client_with_root(authority.certificate_der(), Some(b"http/1.1"));
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        set_timeouts(&socket);
        let stopped = AtomicBool::new(false);
        let mut connection = ServerConnection::new(server_config).unwrap();
        complete_server_handshake(&mut connection, &mut socket, &stopped, Instant::now() + TEST_TIMEOUT).unwrap();
        let mut state = TlsRequestState::from_connection(host, 443, &connection).unwrap();
        let mut tls = StreamOwned::new(connection, socket);
        let first = state.next(&mut tls, &stopped).unwrap().unwrap();
        assert!(first.header.starts_with(b"GET /first HTTP/1.1\r\n"));
        tls.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .unwrap();
        tls.flush().unwrap();

        let error = state
            .next(&mut tls, &stopped)
            .err()
            .expect("cross-host request must be rejected");
        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    });

    let mut socket = TcpStream::connect(address).unwrap();
    set_timeouts(&socket);
    let stopped = AtomicBool::new(false);
    let server_name = ServerName::try_from(host.to_owned()).unwrap();
    let mut connection = ClientConnection::new(client_config, server_name).unwrap();
    complete_client_handshake(&mut connection, &mut socket, &stopped, Instant::now() + TEST_TIMEOUT).unwrap();
    let mut tls = StreamOwned::new(connection, socket);
    tls.write_all(
        b"GET /first HTTP/1.1\r\nHost: allowed.example.test\r\n\r\n\
          GET /second HTTP/1.1\r\nHost: different.example.test\r\n\r\n",
    )
    .unwrap();
    tls.conn.send_close_notify();
    tls.flush().unwrap();
    let mut response = Vec::new();
    tls.read_to_end(&mut response).unwrap();
    server.join().unwrap();

    assert!(response.starts_with(b"HTTP/1.1 200"));
    assert!(response.windows(12).any(|window| window == b"HTTP/1.1 403"));
}

#[test]
fn handshake_loop_checks_stop_before_touching_the_transport() {
    let authority = CertificateAuthority::generate().unwrap();
    let mut connection = ServerConnection::new(authority.server_config("allowed.example.test").unwrap()).unwrap();
    let stopped = AtomicBool::new(true);
    let mut transport = std::io::Cursor::new(Vec::<u8>::new());

    let error = complete_server_handshake(&mut connection, &mut transport, &stopped, Instant::now() + TEST_TIMEOUT)
        .expect_err("stopped handshake must fail");

    assert_eq!(error.kind(), std::io::ErrorKind::Interrupted);
    assert_eq!(transport.position(), 0);
}

fn tls_handshake_fails(server_config: Arc<rustls::ServerConfig>, client_config: Arc<ClientConfig>, host: &str) -> bool {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        set_timeouts(&socket);
        let stopped = AtomicBool::new(false);
        let mut connection = ServerConnection::new(server_config).unwrap();
        complete_server_handshake(&mut connection, &mut socket, &stopped, Instant::now() + TEST_TIMEOUT).is_err()
    });
    let mut socket = TcpStream::connect(address).unwrap();
    set_timeouts(&socket);
    let stopped = AtomicBool::new(false);
    let server_name = ServerName::try_from(host.to_owned()).unwrap();
    let mut connection = ClientConnection::new(client_config, server_name).unwrap();
    let _ = complete_client_handshake(&mut connection, &mut socket, &stopped, Instant::now() + TEST_TIMEOUT);
    server.join().unwrap()
}

fn client_with_root(root: rustls::pki_types::CertificateDer<'static>, alpn: Option<&[u8]>) -> Arc<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(root).unwrap();
    if alpn == Some(b"http/1.1") {
        return client_config(roots).unwrap();
    }
    let mut config = ClientConfig::builder_with_provider(rustls::crypto::ring::default_provider().into())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = alpn.into_iter().map(<[u8]>::to_vec).collect();
    Arc::new(config)
}

fn set_timeouts(stream: &TcpStream) {
    stream.set_read_timeout(Some(TEST_TIMEOUT)).unwrap();
    stream.set_write_timeout(Some(TEST_TIMEOUT)).unwrap();
}
