use std::ffi::OsString;
use std::io::{Read, Write};
#[cfg(feature = "sandbox-test-fixtures")]
use std::net::TcpListener;
use std::net::{Ipv4Addr, Shutdown, SocketAddr, TcpStream};
use std::path::Path;
#[cfg(feature = "sandbox-test-fixtures")]
use std::thread::JoinHandle;
use std::time::Duration;

use solaris_process::PinnedCommand;

pub(crate) const PROBE_MODE_KEY: &str = "SOLARIS_SANDBOX_PROXY_PROBE_MODE";
pub(crate) const PROBE_MARKER_KEY: &str = "SOLARIS_SANDBOX_PROXY_PROBE_MARKER";
pub(crate) const DIRECT_ADDRESS_KEY: &str = "SOLARIS_SANDBOX_PROXY_DIRECT_ADDRESS";
pub(crate) const MODE_ABSENT: &str = "absent";
pub(crate) const MODE_ENABLED: &str = "enabled";

const APPROVED_BODY: &[u8] = b"solaris-proxy-approved";
#[cfg(feature = "sandbox-test-fixtures")]
const APPROVED_HOST: &str = "allowed.example.test";
#[cfg(feature = "sandbox-test-fixtures")]
const MAX_ORIGIN_REQUEST_BYTES: usize = 16 * 1024;

const PROXY_KEYS: &[&str] = &[
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
];
const FORWARD_PROXY_KEYS: &[&str] = &[
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
];

pub(crate) fn configure_probe(command: &mut PinnedCommand, mode: &str, marker: &Path, direct_address: SocketAddr) {
    for key in PROXY_KEYS {
        command.env(key, "http://untrusted-proxy.invalid:9999");
    }
    command
        .env(PROBE_MODE_KEY, mode)
        .env(PROBE_MARKER_KEY, marker)
        .env(DIRECT_ADDRESS_KEY, direct_address.to_string());
}

#[cfg(feature = "sandbox-test-fixtures")]
pub(crate) struct OriginProbe {
    address: SocketAddr,
    request: JoinHandle<Vec<u8>>,
}

#[cfg(feature = "sandbox-test-fixtures")]
impl OriginProbe {
    pub(crate) fn spawn() -> Self {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let request = std::thread::spawn(move || serve_approved_request(listener));
        Self { address, request }
    }

    pub(crate) fn address(&self) -> SocketAddr {
        self.address
    }

    pub(crate) fn assert_received(self) {
        let request = self.request.join().expect("proxy origin thread must not panic");
        assert!(request.starts_with(b"GET /approved HTTP/1.1\r\n"));
        assert!(
            request
                .windows(b"Host: allowed.example.test\r\n".len())
                .any(|window| window == b"Host: allowed.example.test\r\n")
        );
    }
}

pub(crate) fn child_probe() {
    let Some(mode) = std::env::var_os(PROBE_MODE_KEY) else {
        return;
    };
    let marker = std::env::var_os(PROBE_MARKER_KEY).expect("proxy probe marker");
    let direct_address = std::env::var(DIRECT_ADDRESS_KEY)
        .expect("direct network probe address")
        .parse::<SocketAddr>()
        .unwrap();
    assert!(
        TcpStream::connect_timeout(&direct_address, Duration::from_secs(1)).is_err(),
        "sandbox child reached host loopback without the proxy"
    );

    match mode.to_string_lossy().as_ref() {
        MODE_ABSENT => assert_proxy_environment_absent(),
        MODE_ENABLED => exercise_proxy_requests(),
        other => panic!("unknown proxy probe mode: {other}"),
    }
    std::fs::write(marker, b"verified").unwrap();
}

fn assert_proxy_environment_absent() {
    for key in PROXY_KEYS {
        assert!(
            std::env::var_os(key).is_none(),
            "{key} must not survive an empty network policy"
        );
    }
}

fn exercise_proxy_requests() {
    let proxy = std::env::var("HTTP_PROXY").expect("approved-domain policy must provide HTTP_PROXY");
    for key in FORWARD_PROXY_KEYS {
        assert_eq!(
            std::env::var(key).as_deref(),
            Ok(proxy.as_str()),
            "{key} must use the one Host proxy"
        );
    }
    for key in ["NO_PROXY", "no_proxy"] {
        assert_eq!(
            std::env::var_os(key),
            Some(OsString::new()),
            "{key} must not bypass the Host proxy"
        );
    }

    let address = proxy
        .strip_prefix("http://")
        .expect("sandbox proxy must use an HTTP proxy URL")
        .parse::<SocketAddr>()
        .expect("sandbox proxy URL must contain a socket address");
    assert_eq!(address.ip(), Ipv4Addr::LOCALHOST);

    let approved = send_proxy_request(
        address,
        b"GET http://allowed.example.test/approved HTTP/1.1\r\nHost: allowed.example.test\r\nConnection: close\r\n\r\n",
    );
    assert!(
        approved.starts_with(b"HTTP/1.1 200") && approved.ends_with(APPROVED_BODY),
        "unexpected approved proxy response: {}",
        String::from_utf8_lossy(&approved)
    );

    let denied = send_proxy_request(
        address,
        b"CONNECT denied.invalid:443 HTTP/1.1\r\nHost: denied.invalid:443\r\n\r\n",
    );
    assert!(
        denied.starts_with(b"HTTP/1.1 403"),
        "unexpected denied proxy response: {}",
        String::from_utf8_lossy(&denied)
    );
}

fn send_proxy_request(address: SocketAddr, request: &[u8]) -> Vec<u8> {
    let mut client = TcpStream::connect_timeout(&address, Duration::from_secs(1)).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    client.write_all(request).unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    response
}

#[cfg(feature = "sandbox-test-fixtures")]
fn serve_approved_request(listener: TcpListener) -> Vec<u8> {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        assert!(
            std::time::Instant::now() < deadline,
            "Host proxy did not reach the deterministic origin"
        );
        match listener.accept() {
            Ok((mut stream, _)) => {
                stream.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
                stream.set_write_timeout(Some(Duration::from_secs(1))).unwrap();
                let mut request = Vec::new();
                Read::by_ref(&mut stream)
                    .take((MAX_ORIGIN_REQUEST_BYTES + 1) as u64)
                    .read_to_end(&mut request)
                    .unwrap();
                assert!(
                    request.len() <= MAX_ORIGIN_REQUEST_BYTES,
                    "origin request exceeded the test limit"
                );
                if request.is_empty() {
                    continue;
                }
                assert!(
                    request
                        .windows(APPROVED_HOST.len())
                        .any(|window| window == APPROVED_HOST.as_bytes()),
                    "Host proxy sent an unexpected origin request"
                );
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    APPROVED_BODY.len()
                );
                stream.write_all(response.as_bytes()).unwrap();
                stream.write_all(APPROVED_BODY).unwrap();
                return request;
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(error) => panic!("proxy origin accept failed: {error}"),
        }
    }
}
