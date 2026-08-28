use process_security_environment_spec::process_security_environment_layout::{
    process_security_environment_buffer_has_identifier, root_as_process_security_environment,
};

use super::*;

const PROXY_URL: &str = "http://127.0.0.1:43127";
const PROXY_PFN: &str = "Solaris.Mesh.NetworkProxy_8wekyb3d8bbwe";

#[test]
fn encodes_minimal_proxy_only_psec_v1_environment() {
    let bytes = encode_proxy_policy(PROXY_URL, PROXY_PFN).expect("proxy policy should encode");

    assert!(process_security_environment_buffer_has_identifier(&bytes));
    let spec = root_as_process_security_environment(&bytes).expect("PSEC should decode");
    assert_eq!(spec.version().major(), 1);
    assert_eq!(spec.version().minor(), 0);
    assert!(
        spec.capabilities().is_none(),
        "the Target must not receive internetClient or privateNetworkClientServer; only the packaged proxy peer owns network capabilities"
    );
    assert!(spec.fs_read_write().is_none());
    assert!(spec.fs_read_only().is_none());
    assert!(spec.fs_deny().is_none());

    let network = spec.network_policy().expect("network policy should be present");
    assert_eq!(network.proxy().and_then(|proxy| proxy.url()), Some(PROXY_URL));
    assert_eq!(network.allowed_appcontainer_peer(), Some(PROXY_PFN));
    assert!(network.egress().is_none());
}

#[test]
fn rejects_proxy_and_direct_egress_combination() {
    let error = encode_policy(NetworkPolicyInput {
        proxy_url: Some(PROXY_URL),
        allowed_appcontainer_peer: Some(PROXY_PFN),
        direct_egress: true,
    })
    .expect_err("mixed routes must be rejected");

    assert_eq!(error, PsecCodecError::ConflictingNetworkRoutes);
}

#[test]
fn rejects_direct_egress_without_proxy() {
    let error = encode_policy(NetworkPolicyInput {
        proxy_url: None,
        allowed_appcontainer_peer: None,
        direct_egress: true,
    })
    .expect_err("direct egress must be rejected");

    assert_eq!(error, PsecCodecError::DirectEgressNotAllowed);
}

#[test]
fn rejects_invalid_proxy_urls_with_structured_reasons() {
    let cases = [
        ("https://127.0.0.1:43127", ProxyUrlError::Scheme),
        ("http://localhost:43127", ProxyUrlError::Host),
        ("http://[::1]:43127", ProxyUrlError::Host),
        ("http://user@127.0.0.1:43127", ProxyUrlError::UserInfo),
        ("http://127.0.0.1", ProxyUrlError::Host),
        ("http://127.0.0.1:", ProxyUrlError::MissingPort),
        ("http://127.0.0.1:0", ProxyUrlError::ZeroPort),
        ("http://127.0.0.1:65536", ProxyUrlError::InvalidPort),
        ("http://127.0.0.1:043127", ProxyUrlError::InvalidPort),
        ("http://127.0.0.1:43127/", ProxyUrlError::Path),
        ("http://127.0.0.1:43127?x=1", ProxyUrlError::Query),
        ("http://127.0.0.1:43127#x", ProxyUrlError::Fragment),
    ];

    for (proxy_url, reason) in cases {
        assert_eq!(
            encode_proxy_policy(proxy_url, PROXY_PFN),
            Err(PsecCodecError::InvalidProxyUrl { reason }),
            "unexpected result for proxy URL category {reason:?}"
        );
    }
}

#[test]
fn rejects_invalid_package_family_names_with_structured_reasons() {
    let cases = [
        ("", PackageFamilyNameError::Empty),
        ("SolarisMeshNetworkProxy", PackageFamilyNameError::MissingPublisherId),
        ("So_8wekyb3d8bbwe", PackageFamilyNameError::InvalidLength),
        (
            "Solaris_Mesh.NetworkProxy_8wekyb3d8bbwe",
            PackageFamilyNameError::InvalidNameCharacters,
        ),
        (
            "Solaris.Mesh.NetworkProxy_8wekyb3d8bbw",
            PackageFamilyNameError::InvalidPublisherIdLength,
        ),
    ];

    for (package_family_name, reason) in cases {
        assert_eq!(
            encode_proxy_policy(PROXY_URL, package_family_name),
            Err(PsecCodecError::InvalidPackageFamilyName { reason }),
            "unexpected result for package family name category {reason:?}"
        );
    }

    let long_name = format!("{}_8wekyb3d8bbwe", "a".repeat(51));
    assert_eq!(
        encode_proxy_policy(PROXY_URL, &long_name),
        Err(PsecCodecError::InvalidPackageFamilyName {
            reason: PackageFamilyNameError::InvalidLength,
        })
    );
}

#[test]
fn rejects_every_forbidden_crockford_publisher_id_letter() {
    for forbidden in ['i', 'l', 'o', 'u'] {
        let publisher_id = format!("8wekyb3d8bbw{forbidden}");
        let package_family_name = format!("Solaris.Mesh.NetworkProxy_{publisher_id}");

        assert_eq!(
            encode_proxy_policy(PROXY_URL, &package_family_name),
            Err(PsecCodecError::InvalidPackageFamilyName {
                reason: PackageFamilyNameError::InvalidPublisherIdCharacter,
            }),
            "PublisherId containing forbidden letter {forbidden:?} must fail"
        );
    }
}

#[test]
fn accepts_the_complete_crockford_publisher_id_alphabet() {
    for publisher_id in ["0123456789abc", "defghjkmnpqrs", "tvwxyz0123456", "8WEKYB3D8BBWE"] {
        let package_family_name = format!("Solaris.Mesh.NetworkProxy_{publisher_id}");
        encode_proxy_policy(PROXY_URL, &package_family_name)
            .unwrap_or_else(|error| panic!("valid PublisherId should encode: {error}"));
    }
}

#[test]
fn rejects_every_reserved_device_name_case_insensitively_and_with_extensions() {
    let reserved_names = [
        "con", "prn", "aux", "nul", "com1", "com2", "com3", "com4", "com5", "com6", "com7", "com8", "com9", "lpt1",
        "lpt2", "lpt3", "lpt4", "lpt5", "lpt6", "lpt7", "lpt8", "lpt9",
    ];

    for reserved in reserved_names {
        for name in [
            reserved.to_owned(),
            reserved.to_ascii_uppercase(),
            format!("{reserved}.extension"),
        ] {
            let package_family_name = format!("{name}_8wekyb3d8bbwe");
            assert_eq!(
                encode_proxy_policy(PROXY_URL, &package_family_name),
                Err(PsecCodecError::InvalidPackageFamilyName {
                    reason: PackageFamilyNameError::ReservedDeviceName,
                }),
                "reserved package name {name:?} must fail"
            );
        }
    }
}

#[test]
fn rejects_each_prohibited_package_name_form() {
    let cases = [
        ("abc.", PackageFamilyNameError::TrailingDot),
        ("XN--proxy", PackageFamilyNameError::PunycodePrefix),
        ("a.Xn--proxy", PackageFamilyNameError::PunycodeSegment),
    ];

    for (name, reason) in cases {
        let package_family_name = format!("{name}_8wekyb3d8bbwe");
        assert_eq!(
            encode_proxy_policy(PROXY_URL, &package_family_name),
            Err(PsecCodecError::InvalidPackageFamilyName { reason }),
            "prohibited package name {name:?} must fail"
        );
    }
}

#[test]
fn rejects_all_five_reported_package_family_name_counterexamples() {
    let cases = [
        (
            "Solaris.Mesh.NetworkProxy_iiiiiiiiiiiii",
            PackageFamilyNameError::InvalidPublisherIdCharacter,
        ),
        ("con_8wekyb3d8bbwe", PackageFamilyNameError::ReservedDeviceName),
        ("abc._8wekyb3d8bbwe", PackageFamilyNameError::TrailingDot),
        ("xn--proxy_8wekyb3d8bbwe", PackageFamilyNameError::PunycodePrefix),
        ("a.xn--proxy_8wekyb3d8bbwe", PackageFamilyNameError::PunycodeSegment),
    ];

    for (package_family_name, reason) in cases {
        assert_eq!(
            encode_proxy_policy(PROXY_URL, package_family_name),
            Err(PsecCodecError::InvalidPackageFamilyName { reason })
        );
    }
}
