use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use super::{NetworkProxyPolicy, NetworkProxyPolicyError, is_public_destination, permission_domain_is_covered};

#[cfg(all(windows, feature = "sandbox-test-fixtures"))]
#[test]
fn windows_network_proof_connects_upstream_before_reporting_200() {
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::time::Duration;

    use super::HostNetworkProxy;

    let origin = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let origin_address = origin.local_addr().unwrap();
    let origin_thread = std::thread::spawn(move || {
        let (stream, _) = origin.accept().unwrap();
        let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
        std::thread::sleep(Duration::from_millis(100));
    });
    let policy = NetworkProxyPolicy::from_permission_domains(["example.test:443"])
        .unwrap()
        .with_test_connector(
            "example.test",
            443,
            "93.184.216.34".parse::<IpAddr>().unwrap(),
            origin_address,
        )
        .unwrap();
    let temporary = tempfile::tempdir().unwrap();
    let port = {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        listener.local_addr().unwrap().port()
    };
    let proxy = HostNetworkProxy::start_on_port(&temporary.path().join("proxy.state"), port, policy).unwrap();
    let proxy_address = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let mut client = TcpStream::connect_timeout(&proxy_address, Duration::from_secs(2)).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    client
        .write_all(b"CONNECT example.test:443 HTTP/1.1\r\nHost: example.test:443\r\nX-Solaris-Network-Proof: 1\r\n\r\n")
        .unwrap();
    client.flush().unwrap();
    let mut response = Vec::new();
    let mut byte = [0_u8; 1];
    while response.len() < 4096 {
        if client.read(&mut byte).unwrap() == 0 {
            break;
        }
        response.push(byte[0]);
        if response.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    let response = String::from_utf8(response).unwrap();
    assert!(response.starts_with("HTTP/1.1 200"));
    assert!(response.contains(&format!("X-Solaris-Upstream-Address: {origin_address}\r\n")));
    drop(proxy);
    origin_thread.join().unwrap();
}

#[test]
fn permission_domains_are_normalized_to_exact_host_and_port() {
    let policy = NetworkProxyPolicy::from_permission_domains([
        "https://API.Example.Test/v1",
        "http://plain.example.test:8080/path",
        "secure.example.test",
        "https://api.example.test/other",
    ])
    .unwrap();

    assert_eq!(policy.endpoints.len(), 3);
    assert!(policy.permits("api.example.test", 443));
    assert!(policy.permits("PLAIN.EXAMPLE.TEST.", 8080));
    assert!(policy.permits("secure.example.test", 443));
    assert!(!policy.permits("sub.api.example.test", 443));
    assert!(!policy.permits("api.example.test", 80));
}

#[test]
fn dangerous_or_ambiguous_permission_values_are_rejected() {
    let cases = [
        ("*.example.test", NetworkProxyPolicyError::Wildcard),
        ("https://token@example.test", NetworkProxyPolicyError::UserInfo),
        ("127.0.0.1", NetworkProxyPolicyError::IpLiteral),
        ("[::1]", NetworkProxyPolicyError::IpLiteral),
        ("http://169.254.169.254/latest", NetworkProxyPolicyError::IpLiteral),
        ("metadata.google.internal", NetworkProxyPolicyError::MetadataService),
        ("localhost", NetworkProxyPolicyError::MetadataService),
        ("service.localhost", NetworkProxyPolicyError::MetadataService),
        ("instance-data.ec2.internal", NetworkProxyPolicyError::MetadataService),
        ("file://example.test/socket", NetworkProxyPolicyError::InvalidDomain),
        ("unix:/tmp/proxy.sock", NetworkProxyPolicyError::InvalidDomain),
        ("https://example.test:0", NetworkProxyPolicyError::InvalidDomain),
        ("https://example.test.evil@safe.test", NetworkProxyPolicyError::UserInfo),
        ("https://example.test\\@evil.test", NetworkProxyPolicyError::UserInfo),
    ];
    for (value, expected) in cases {
        assert_eq!(
            NetworkProxyPolicy::from_permission_domains([value]).unwrap_err(),
            expected,
            "unexpected result for {value}"
        );
    }
}

#[test]
fn no_domains_keeps_the_policy_empty() {
    let policy = NetworkProxyPolicy::from_permission_domains(std::iter::empty::<&str>()).unwrap();
    assert!(policy.is_empty());
}

#[test]
fn permission_coverage_is_exact_by_normalized_host_and_port() {
    assert!(permission_domain_is_covered("api.example.test", "https://API.example.test/v1").unwrap());
    assert!(permission_domain_is_covered("http://api.example.test:8080/a", "http://api.example.test:8080/b").unwrap());
    assert!(!permission_domain_is_covered("v1.api.example.test", "api.example.test").unwrap());
    assert!(!permission_domain_is_covered("http://api.example.test", "https://api.example.test").unwrap());
    assert!(!permission_domain_is_covered("https://api.example.test:444", "api.example.test").unwrap());
}

#[test]
fn proxy_policy_rejects_more_than_thirty_two_endpoints() {
    let domains = (0..33)
        .map(|index| format!("host-{index}.example.test"))
        .collect::<Vec<_>>();

    assert_eq!(
        NetworkProxyPolicy::from_permission_domains(&domains).unwrap_err(),
        NetworkProxyPolicyError::TooManyEndpoints
    );
}

#[test]
fn destination_filter_rejects_host_and_special_use_addresses() {
    for address in [
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
        IpAddr::V4(Ipv4Addr::new(100, 64, 0, 1)),
        IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254)),
        IpAddr::V4(Ipv4Addr::new(192, 31, 196, 0)),
        IpAddr::V4(Ipv4Addr::new(192, 31, 196, 255)),
        IpAddr::V4(Ipv4Addr::new(192, 52, 193, 0)),
        IpAddr::V4(Ipv4Addr::new(192, 52, 193, 255)),
        IpAddr::V4(Ipv4Addr::new(192, 88, 99, 1)),
        IpAddr::V4(Ipv4Addr::new(192, 88, 99, 255)),
        IpAddr::V4(Ipv4Addr::new(192, 175, 48, 0)),
        IpAddr::V4(Ipv4Addr::new(192, 175, 48, 255)),
        IpAddr::V4(Ipv4Addr::new(198, 18, 0, 1)),
        IpAddr::V6(Ipv6Addr::LOCALHOST),
        IpAddr::V6("::2".parse().unwrap()),
        IpAddr::V6("64:ff9b::a00:1".parse().unwrap()),
        IpAddr::V6("64:ff9b:1::1".parse().unwrap()),
        IpAddr::V6("64:ff9b:1:ffff:ffff:ffff:ffff:ffff".parse().unwrap()),
        IpAddr::V6("100::1".parse().unwrap()),
        IpAddr::V6("100:0:0:1::".parse().unwrap()),
        IpAddr::V6("100:0:0:1:ffff:ffff:ffff:ffff".parse().unwrap()),
        IpAddr::V6("2001:1::1".parse().unwrap()),
        IpAddr::V6("2001:1ff:ffff:ffff:ffff:ffff:ffff:ffff".parse().unwrap()),
        IpAddr::V6("2002:0a00:1::1".parse().unwrap()),
        IpAddr::V6("2002:ffff:ffff:ffff:ffff:ffff:ffff:ffff".parse().unwrap()),
        IpAddr::V6("2620:4f:8000::".parse().unwrap()),
        IpAddr::V6("2620:4f:8000:ffff:ffff:ffff:ffff:ffff".parse().unwrap()),
        IpAddr::V6("3fff::1".parse().unwrap()),
        IpAddr::V6("3fff:fff:ffff:ffff:ffff:ffff:ffff:ffff".parse().unwrap()),
        IpAddr::V6("5f00::1".parse().unwrap()),
        IpAddr::V6("5f00:ffff:ffff:ffff:ffff:ffff:ffff:ffff".parse().unwrap()),
        IpAddr::V6("4000::1".parse().unwrap()),
        IpAddr::V6("fc00::1".parse().unwrap()),
        IpAddr::V6("fe80::1".parse().unwrap()),
        IpAddr::V6("2001:db8::1".parse().unwrap()),
        IpAddr::V6("::ffff:127.0.0.1".parse().unwrap()),
    ] {
        assert!(!is_public_destination(address), "accepted {address}");
    }
    for address in [
        IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)),
        IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)),
        IpAddr::V6("2606:4700:4700::1111".parse().unwrap()),
    ] {
        assert!(is_public_destination(address), "rejected {address}");
    }
}
