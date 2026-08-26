use super::{ProxyRequest, ProxyRequestError};
use crate::NetworkProxyPolicy;

fn policy() -> NetworkProxyPolicy {
    NetworkProxyPolicy::from_permission_domains(["https://api.example.test", "http://plain.example.test:8080"]).unwrap()
}

#[test]
fn permits_only_exact_approved_connect_target() {
    let request = ProxyRequest::parse(
        b"CONNECT api.example.test:443 HTTP/1.1\r\nHost: api.example.test:443\r\n\r\n",
        &policy(),
    )
    .unwrap();
    assert!(request.connect);
    assert_eq!(request.host, "api.example.test");
    assert_eq!(request.port, 443);

    assert_eq!(
        ProxyRequest::parse(b"CONNECT other.example.test:443 HTTP/1.1\r\n\r\n", &policy()),
        Err(ProxyRequestError::Denied)
    );
    assert_eq!(
        ProxyRequest::parse(b"CONNECT api.example.test:80 HTTP/1.1\r\n\r\n", &policy()),
        Err(ProxyRequestError::Denied)
    );
    assert_eq!(
        ProxyRequest::parse(
            b"CONNECT api.example.test:443 HTTP/1.1\r\nHost: other.example.test:443\r\n\r\n",
            &policy(),
        ),
        Err(ProxyRequestError::Invalid)
    );
}

#[test]
fn absolute_http_target_and_host_header_must_match() {
    let request = ProxyRequest::parse(
        b"GET http://plain.example.test:8080/a HTTP/1.1\r\nHost: plain.example.test:8080\r\n\r\n",
        &policy(),
    )
    .unwrap();
    assert!(!request.connect);
    assert_eq!(request.header_bytes, 80);

    assert_eq!(
        ProxyRequest::parse(
            b"GET http://plain.example.test:8080/a HTTP/1.1\r\nHost: api.example.test\r\n\r\n",
            &policy(),
        ),
        Err(ProxyRequestError::Invalid)
    );
}

#[test]
fn rejects_origin_form_ip_literals_userinfo_and_header_smuggling() {
    for request in [
        &b"GET / HTTP/1.1\r\nHost: api.example.test\r\n\r\n"[..],
        &b"CONNECT 127.0.0.1:443 HTTP/1.1\r\n\r\n"[..],
        &b"GET http://token@plain.example.test:8080/ HTTP/1.1\r\nHost: plain.example.test:8080\r\n\r\n"[..],
        &b"GET http://plain.example.test:8080/ HTTP/1.1\r\nHost: plain.example.test:8080\r\nHost: api.example.test\r\n\r\n"[..],
        &b"GET http://plain.example.test:8080/ HTTP/1.1\r\n Host: plain.example.test:8080\r\n\r\n"[..],
    ] {
        assert_eq!(ProxyRequest::parse(request, &policy()), Err(ProxyRequestError::Invalid));
    }
}

#[test]
fn incomplete_and_oversized_headers_fail_closed() {
    assert_eq!(
        ProxyRequest::parse(b"CONNECT api.example.test:443 HTTP/1.1\r\n", &policy()),
        Err(ProxyRequestError::Incomplete)
    );
    let oversized = vec![b'a'; super::super::MAX_PROXY_HEADER_BYTES + 1];
    assert_eq!(
        ProxyRequest::parse(&oversized, &policy()),
        Err(ProxyRequestError::TooLarge)
    );
}
