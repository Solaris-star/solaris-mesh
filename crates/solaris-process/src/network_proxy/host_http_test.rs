use std::io::{self, Cursor};
use std::sync::atomic::AtomicBool;

use super::{
    MAX_PROXY_BODY_BYTES, forward_request_body, prepare_plain_request, prepare_tls_request, read_header,
    validate_connect_header,
};

#[test]
fn tls_request_requires_the_connect_authority() {
    let error = prepare_tls_request(
        b"GET / HTTP/1.1\r\nHost: other.example.test\r\n\r\n",
        "allowed.example.test",
        443,
    )
    .err()
    .expect("cross-host request must fail");

    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
}

#[test]
fn tls_request_requires_an_explicit_nonstandard_port() {
    assert!(
        prepare_tls_request(
            b"GET / HTTP/1.1\r\nHost: allowed.example.test\r\n\r\n",
            "allowed.example.test",
            8443,
        )
        .is_err()
    );
    assert!(
        prepare_tls_request(
            b"GET / HTTP/1.1\r\nHost: allowed.example.test:8443\r\n\r\n",
            "allowed.example.test",
            8443,
        )
        .is_ok()
    );
}

#[test]
fn request_body_length_has_a_fixed_limit() {
    let header = format!(
        "POST http://allowed.example.test/upload HTTP/1.1\r\nHost: allowed.example.test\r\nContent-Length: {}\r\n\r\n",
        MAX_PROXY_BODY_BYTES + 1
    );

    assert!(prepare_plain_request(header.as_bytes()).is_err());
}

#[test]
fn connect_rejects_a_request_body_and_protocol_upgrade() {
    assert!(validate_connect_header(b"CONNECT allowed.example.test:443 HTTP/1.1\r\n\r\n").is_err());
    assert!(
        validate_connect_header(
            b"CONNECT allowed.example.test:443 HTTP/1.1\r\nHost: allowed.example.test:443\r\nContent-Length: 1\r\n\r\n"
        )
        .is_err()
    );
    assert!(
        validate_connect_header(
            b"CONNECT allowed.example.test:443 HTTP/1.1\r\nHost: allowed.example.test:443\r\nUpgrade: websocket\r\n\r\n"
        )
        .is_err()
    );
}

#[test]
fn shared_framing_reads_one_header_and_forwards_only_its_body() {
    let mut input = Cursor::new(
        b"POST http://allowed.example.test/upload HTTP/1.1\r\nHost: allowed.example.test\r\nContent-Length: 4\r\n\r\nbodyNEXT"
            .to_vec(),
    );
    let mut buffered = Vec::new();
    let header = read_header(&mut input, &mut buffered).unwrap().unwrap();
    let prepared = prepare_plain_request(&header).unwrap();
    let mut forwarded = Vec::new();

    forward_request_body(
        &mut input,
        &mut forwarded,
        &mut buffered,
        prepared.body,
        &AtomicBool::new(false),
    )
    .unwrap();

    assert!(prepared.header.starts_with(b"POST /upload HTTP/1.1\r\n"));
    assert_eq!(forwarded, b"body");
}

#[test]
fn shared_framing_validates_and_forwards_chunked_bodies() {
    let mut input = Cursor::new(b"4\r\nbody\r\n0\r\n\r\nNEXT".to_vec());
    let mut buffered = Vec::new();
    let prepared = prepare_plain_request(
        b"POST http://allowed.example.test/upload HTTP/1.1\r\nHost: allowed.example.test\r\nTransfer-Encoding: chunked\r\n\r\n",
    )
    .unwrap();
    let mut forwarded = Vec::new();

    forward_request_body(
        &mut input,
        &mut forwarded,
        &mut buffered,
        prepared.body,
        &AtomicBool::new(false),
    )
    .unwrap();

    assert_eq!(forwarded, b"4\r\nbody\r\n0\r\n\r\n");
}
