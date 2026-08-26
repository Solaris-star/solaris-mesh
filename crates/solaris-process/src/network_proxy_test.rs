use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use super::{NetworkProxyPolicy, NetworkProxyPolicyError, is_public_destination, permission_domain_is_covered};

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
