use std::ffi::OsString;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Ipv4Addr, TcpListener, TcpStream};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use super::{CONTROL_PROTOCOL_VERSION, Invocation, PackageIdentity, RuntimeIdentity, RuntimeLimits, run_with};

const NONCE: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const PACKAGE_FAMILY_NAME: &str = "Solaris.Mesh.NetworkProxy_8wekyb3d8bbwe";
const PACKAGE_MANIFEST: &str = include_str!("../../windows-network-proxy/AppxManifest.xml");
const PACKAGE_SCRIPT: &str = include_str!("../../windows-network-proxy/build-unsigned-package.ps1");

struct FakeIdentity {
    is_app_container: bool,
}

impl RuntimeIdentity for FakeIdentity {
    fn current(&self) -> std::io::Result<PackageIdentity> {
        Ok(PackageIdentity {
            family_name: PACKAGE_FAMILY_NAME.to_owned(),
            is_app_container: self.is_app_container,
        })
    }
}

fn available_port() -> u16 {
    TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn invocation(control_port: u16, proxy_port: u16) -> Invocation {
    Invocation {
        control_port,
        proxy_port,
        nonce: NONCE.to_owned(),
        domains: vec!["https://example.com".to_owned()],
    }
}

fn limits() -> RuntimeLimits {
    RuntimeLimits {
        accept_timeout: Duration::from_secs(2),
        handshake_timeout: Duration::from_secs(1),
    }
}

fn connect_until(port: u16) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match TcpStream::connect((Ipv4Addr::LOCALHOST, port)) {
            Ok(stream) => return stream,
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(10)),
            Err(error) => panic!("control listener did not start: {error}"),
        }
    }
}

fn spawn_proxy(invocation: Invocation) -> JoinHandle<std::io::Result<()>> {
    std::thread::spawn(move || run_with(&FakeIdentity { is_app_container: true }, invocation, limits()))
}

#[test]
fn invocation_rejects_unsafe_ports_nonce_and_missing_domains() {
    let cases = [
        vec![
            "--control-port",
            "8080",
            "--proxy-port",
            "50001",
            "--nonce",
            NONCE,
            "--domain",
            "https://example.com",
        ],
        vec![
            "--control-port",
            "50000",
            "--proxy-port",
            "50000",
            "--nonce",
            NONCE,
            "--domain",
            "https://example.com",
        ],
        vec![
            "--control-port",
            "50000",
            "--proxy-port",
            "50001",
            "--nonce",
            "short",
            "--domain",
            "https://example.com",
        ],
        vec!["--control-port", "50000", "--proxy-port", "50001", "--nonce", NONCE],
    ];
    for arguments in cases {
        assert!(Invocation::parse(arguments.into_iter().map(OsString::from)).is_err());
    }
}

#[test]
fn medium_integrity_package_identity_is_rejected_before_binding() {
    let control_port = available_port();
    let proxy_port = available_port();
    let error = run_with(
        &FakeIdentity {
            is_app_container: false,
        },
        invocation(control_port, proxy_port),
        limits(),
    )
    .unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    TcpListener::bind((Ipv4Addr::LOCALHOST, control_port)).unwrap();
    TcpListener::bind((Ipv4Addr::LOCALHOST, proxy_port)).unwrap();
}

#[test]
fn fake_activation_proves_identity_nonce_endpoint_and_denial() {
    let control_port = available_port();
    let proxy_port = available_port();
    let proxy = spawn_proxy(invocation(control_port, proxy_port));
    let mut control = connect_until(control_port);
    writeln!(
        control,
        "{{\"protocol\":{CONTROL_PROTOCOL_VERSION},\"nonce\":\"{NONCE}\"}}"
    )
    .unwrap();
    let mut control = BufReader::new(control);
    let mut ready = String::new();
    control.read_line(&mut ready).unwrap();
    let ready: serde_json::Value = serde_json::from_str(&ready).unwrap();
    assert_eq!(ready["protocol"], CONTROL_PROTOCOL_VERSION);
    assert_eq!(ready["nonce"], NONCE);
    assert_eq!(ready["package_family_name"], PACKAGE_FAMILY_NAME);
    assert_eq!(
        ready["application_user_model_id"],
        format!("{PACKAGE_FAMILY_NAME}!Proxy")
    );
    assert_eq!(ready["proxy_url"], format!("http://127.0.0.1:{proxy_port}"));
    assert!(
        ready["ca_certificate_pem"]
            .as_str()
            .unwrap()
            .starts_with("-----BEGIN CERTIFICATE-----")
    );

    let mut denied = TcpStream::connect((Ipv4Addr::LOCALHOST, proxy_port)).unwrap();
    denied
        .write_all(b"CONNECT denied.invalid:443 HTTP/1.1\r\nHost: denied.invalid:443\r\n\r\n")
        .unwrap();
    let mut response = [0_u8; 256];
    let count = denied.read(&mut response).unwrap();
    assert!(String::from_utf8_lossy(&response[..count]).starts_with("HTTP/1.1 403"));

    drop(control);
    assert!(proxy.join().unwrap().is_ok());
}

#[test]
fn fake_activation_rejects_the_wrong_nonce_without_exposing_it() {
    let control_port = available_port();
    let proxy_port = available_port();
    let proxy = spawn_proxy(invocation(control_port, proxy_port));
    let mut control = connect_until(control_port);
    writeln!(
        control,
        "{{\"protocol\":{CONTROL_PROTOCOL_VERSION},\"nonce\":\"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff\"}}"
    )
    .unwrap();
    drop(control);

    let error = proxy.join().unwrap().unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    assert!(!error.to_string().contains(NONCE));
}

#[test]
fn package_manifest_is_multi_instance_appcontainer_without_full_trust_or_firewall_bypass() {
    for expected in [
        "uap10:RuntimeBehavior=\"packagedClassicApp\"",
        "uap10:TrustLevel=\"appContainer\"",
        "uap10:Subsystem=\"console\"",
        "uap10:SupportsMultipleInstances=\"true\"",
        "<Capability Name=\"internetClient\" />",
        "<Capability Name=\"privateNetworkClientServer\" />",
    ] {
        assert!(
            PACKAGE_MANIFEST.contains(expected),
            "missing manifest contract: {expected}"
        );
    }
    for forbidden in [
        "loopbackExempt",
        "NetworkIsolationSetAppContainerConfig",
        "Certificate",
        "runFullTrust",
        "rescap:Capability",
        "windows.firewallRules",
    ] {
        assert!(!PACKAGE_MANIFEST.contains(forbidden));
    }
}

#[test]
fn package_builder_only_creates_and_verifies_an_unsigned_package() {
    for expected in [
        "MakeAppx.exe",
        " pack ",
        " unpack ",
        "AppxSignature.p7x",
        "signed = $false",
        "OID.2.25.311729368913984317654407730594956997722=1",
    ] {
        assert!(PACKAGE_SCRIPT.contains(expected), "missing package check: {expected}");
    }
    assert!(PACKAGE_SCRIPT.contains("$identity.SetAttribute('Publisher', $unsignedPublisher)"));
    for forbidden in [
        "Add-AppxPackage",
        "Import-Certificate",
        "Cert:\\",
        "New-SelfSignedCertificate",
        "signtool",
    ] {
        assert!(!PACKAGE_SCRIPT.contains(forbidden));
    }
}
