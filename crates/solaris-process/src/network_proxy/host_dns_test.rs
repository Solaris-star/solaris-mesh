use std::io::ErrorKind;
use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use super::*;

#[test]
fn resolver_config_has_bounded_nameservers_and_search_suffixes() {
    let servers = parse_resolver_config("nameserver 192.0.2.1\nsearch one.test two.test\n").unwrap();
    assert_eq!(servers, [SocketAddr::from((Ipv4Addr::new(192, 0, 2, 1), 53))]);

    let too_many_servers = (1..=MAX_NAME_SERVERS + 1)
        .map(|index| format!("nameserver 192.0.2.{index}\n"))
        .collect::<String>();
    assert_eq!(
        parse_resolver_config(&too_many_servers).unwrap_err().kind(),
        ErrorKind::InvalidData
    );

    let too_many_suffixes = format!(
        "nameserver 192.0.2.1\nsearch {}\n",
        (0..=MAX_SEARCH_SUFFIXES)
            .map(|index| format!("s{index}.test"))
            .collect::<Vec<_>>()
            .join(" ")
    );
    assert_eq!(
        parse_resolver_config(&too_many_suffixes).unwrap_err().kind(),
        ErrorKind::InvalidData
    );
}

#[test]
fn resolver_config_file_size_is_bounded() {
    let state = tempfile::tempdir().unwrap();
    let path = state.path().join("resolv.conf");
    std::fs::write(&path, vec![b' '; MAX_RESOLVER_CONFIG_BYTES as usize + 1]).unwrap();
    assert_eq!(read_resolver_config(&path).unwrap_err().kind(), ErrorKind::InvalidData);
}

#[test]
fn query_encodes_one_absolute_question() {
    let query = build_query("api.example.test.", DNS_TYPE_A, 41).unwrap();
    let (name, consumed) = parse_name(&query, 12).unwrap();
    assert_eq!(name, "api.example.test");
    assert_eq!(read_u16(&query, 12 + consumed).unwrap(), DNS_TYPE_A);
    assert_eq!(read_u16(&query, 12 + consumed + 2).unwrap(), DNS_CLASS_IN);
    assert_eq!(query.last(), Some(&1));
}

#[test]
fn response_requires_matching_header_and_question() {
    let response = a_response(7, "api.example.test", [93, 184, 216, 34]);
    let parsed = parse_response(&response, 7, "api.example.test", DNS_TYPE_A).unwrap();
    assert!(matches!(parsed, ParsedResponse::Complete(_)));
    assert_eq!(
        parse_response(&response, 8, "api.example.test", DNS_TYPE_A)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );
    assert_eq!(
        parse_response(&response, 7, "other.example.test", DNS_TYPE_A)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );

    let mut not_a_response = response.clone();
    not_a_response[2..4].copy_from_slice(&0x0180_u16.to_be_bytes());
    assert_eq!(
        parse_response(&not_a_response, 7, "api.example.test", DNS_TYPE_A)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );

    let mut server_failure = response;
    server_failure[2..4].copy_from_slice(&0x8182_u16.to_be_bytes());
    assert_eq!(
        parse_response(&server_failure, 7, "api.example.test", DNS_TYPE_A)
            .unwrap_err()
            .kind(),
        ErrorKind::Other
    );
}

#[test]
fn compression_pointer_loops_are_rejected() {
    let mut packet = build_query("api.example.test", DNS_TYPE_A, 9).unwrap();
    packet[2..4].copy_from_slice(&0x8180_u16.to_be_bytes());
    packet[6..8].copy_from_slice(&1_u16.to_be_bytes());
    let answer_offset = packet.len();
    let pointer = 0xc000_u16 | u16::try_from(answer_offset).unwrap();
    packet.extend_from_slice(&pointer.to_be_bytes());
    packet.extend_from_slice(&DNS_TYPE_A.to_be_bytes());
    packet.extend_from_slice(&DNS_CLASS_IN.to_be_bytes());
    packet.extend_from_slice(&0_u32.to_be_bytes());
    packet.extend_from_slice(&4_u16.to_be_bytes());
    packet.extend_from_slice(&[93, 184, 216, 34]);

    assert_eq!(
        parse_response(&packet, 9, "api.example.test", DNS_TYPE_A)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );
}

#[test]
fn address_results_have_a_fixed_limit() {
    let response = DnsResponse {
        records: (0..=MAX_RESOLVED_ADDRESSES)
            .map(|index| DnsRecord::Address {
                owner: "api.example.test".to_owned(),
                address: IpAddr::V4(Ipv4Addr::new(93, 184, 216, index as u8)),
            })
            .collect(),
        name_error: false,
    };
    let error = match select_response(&response, "api.example.test", DNS_TYPE_A) {
        Ok(_) => panic!("too many DNS addresses were accepted"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ErrorKind::InvalidData);
}

#[test]
fn unresponsive_dns_uses_the_shared_deadline_and_stop_flag() {
    let server = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let address = server.local_addr().unwrap();
    let stopped = AtomicBool::new(false);
    let started = Instant::now();
    let error = resolve_with_servers(
        "api.example.test",
        80,
        &[address],
        &stopped,
        Instant::now() + Duration::from_millis(120),
    )
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::TimedOut);
    assert!(started.elapsed() < Duration::from_millis(500));

    stopped.store(true, Ordering::Release);
    let error = resolve_with_servers(
        "api.example.test",
        80,
        &[address],
        &stopped,
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Interrupted);
}

#[test]
fn any_loopback_dns_answer_rejects_the_destination() {
    let server = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    server.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    let address = server.local_addr().unwrap();
    let responder = std::thread::spawn(move || {
        for _ in 0..2 {
            let mut query = [0_u8; 512];
            let (received, peer) = server.recv_from(&mut query).unwrap();
            let query = &query[..received];
            let (_, consumed) = parse_name(query, 12).unwrap();
            let query_type = read_u16(query, 12 + consumed).unwrap();
            let mut response = query.to_vec();
            response[2..4].copy_from_slice(&0x8180_u16.to_be_bytes());
            if query_type == DNS_TYPE_A {
                response[6..8].copy_from_slice(&1_u16.to_be_bytes());
                response.extend_from_slice(&0xc00c_u16.to_be_bytes());
                response.extend_from_slice(&DNS_TYPE_A.to_be_bytes());
                response.extend_from_slice(&DNS_CLASS_IN.to_be_bytes());
                response.extend_from_slice(&60_u32.to_be_bytes());
                response.extend_from_slice(&4_u16.to_be_bytes());
                response.extend_from_slice(&Ipv4Addr::LOCALHOST.octets());
            }
            server.send_to(&response, peer).unwrap();
        }
    });
    let stopped = AtomicBool::new(false);
    let error = resolve_with_servers(
        "api.example.test",
        80,
        &[address],
        &stopped,
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap_err();
    responder.join().unwrap();

    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
}

fn a_response(transaction_id: u16, name: &str, address: [u8; 4]) -> Vec<u8> {
    let mut packet = build_query(name, DNS_TYPE_A, transaction_id).unwrap();
    packet[2..4].copy_from_slice(&0x8180_u16.to_be_bytes());
    packet[6..8].copy_from_slice(&1_u16.to_be_bytes());
    packet.extend_from_slice(&0xc00c_u16.to_be_bytes());
    packet.extend_from_slice(&DNS_TYPE_A.to_be_bytes());
    packet.extend_from_slice(&DNS_CLASS_IN.to_be_bytes());
    packet.extend_from_slice(&60_u32.to_be_bytes());
    packet.extend_from_slice(&4_u16.to_be_bytes());
    packet.extend_from_slice(&address);
    packet
}
