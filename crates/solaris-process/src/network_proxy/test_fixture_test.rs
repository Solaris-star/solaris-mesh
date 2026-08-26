use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use super::*;

const APPROVED_HOST: &str = "allowed.example.test";
const APPROVED_PORT: u16 = 80;
const PUBLIC_TEST_ADDRESS: IpAddr = IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34));

#[test]
fn fixture_routes_an_approved_public_result_to_the_local_origin() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let policy = fixture_policy(PUBLIC_TEST_ADDRESS, listener.local_addr().unwrap()).unwrap();
    let fixture = policy.test_fixture.as_ref().unwrap();

    let stream = fixture
        .connect(
            APPROVED_HOST,
            APPROVED_PORT,
            &AtomicBool::new(false),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();

    assert_eq!(stream.peer_addr().unwrap(), listener.local_addr().unwrap());
}

#[test]
fn fixture_rejects_non_public_dns_results_and_non_loopback_origins() {
    let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 8080));
    assert_eq!(
        fixture_policy(IpAddr::V4(Ipv4Addr::LOCALHOST), loopback).unwrap_err(),
        NetworkProxyPolicyError::InvalidTestFixture
    );
    assert_eq!(
        fixture_policy(PUBLIC_TEST_ADDRESS, SocketAddr::from((PUBLIC_TEST_ADDRESS, 8080))).unwrap_err(),
        NetworkProxyPolicyError::InvalidTestFixture
    );
    assert_eq!(
        fixture_policy(PUBLIC_TEST_ADDRESS, SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap_err(),
        NetworkProxyPolicyError::InvalidTestFixture
    );
}

#[test]
fn fixture_rechecks_the_exact_approved_host_before_connecting() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let policy = fixture_policy(PUBLIC_TEST_ADDRESS, listener.local_addr().unwrap()).unwrap();
    let error = policy
        .test_fixture
        .as_ref()
        .unwrap()
        .connect(
            "denied.example.test",
            APPROVED_PORT,
            &AtomicBool::new(false),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
}

fn fixture_policy(resolved_ip: IpAddr, destination: SocketAddr) -> Result<NetworkProxyPolicy, NetworkProxyPolicyError> {
    NetworkProxyPolicy::from_permission_domains(["http://allowed.example.test"])?.with_test_connector(
        APPROVED_HOST,
        APPROVED_PORT,
        resolved_ip,
        destination,
    )
}
