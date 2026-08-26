use process_security_environment_spec::process_security_environment_layout::{
    NetworkPolicyT, ProcessSecurityEnvironmentT, ProxyInfoT, SchemaVersionT, finish_process_security_environment_buffer,
};

const PRIVATE_NETWORK_CAPABILITY: &str = "privateNetworkClientServer";
const LOOPBACK_PROXY_PREFIX: &str = "http://127.0.0.1:";
// Windows package string and PublisherId rules:
// https://learn.microsoft.com/windows/apps/desktop/modernize/package-identity-overview
const PACKAGE_NAME_MIN_LENGTH: usize = 3;
const PACKAGE_NAME_MAX_LENGTH: usize = 50;
const PACKAGE_FAMILY_NAME_MAX_LENGTH: usize = 64;
const PUBLISHER_ID_LENGTH: usize = 13;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(super) enum PsecCodecError {
    #[error("the PSEC proxy URL is not an exact IPv4 loopback endpoint")]
    InvalidProxyUrl { reason: ProxyUrlError },
    #[error("the PSEC proxy package family name is invalid")]
    InvalidPackageFamilyName { reason: PackageFamilyNameError },
    #[error("PSEC proxy and direct egress policy forms cannot be combined")]
    ConflictingNetworkRoutes,
    #[error("this PSEC encoder does not permit direct network egress")]
    DirectEgressNotAllowed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ProxyUrlError {
    Scheme,
    Host,
    UserInfo,
    MissingPort,
    InvalidPort,
    ZeroPort,
    Path,
    Query,
    Fragment,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PackageFamilyNameError {
    Empty,
    InvalidLength,
    MissingPublisherId,
    InvalidNameCharacters,
    ReservedDeviceName,
    TrailingDot,
    PunycodePrefix,
    PunycodeSegment,
    InvalidPublisherIdLength,
    InvalidPublisherIdCharacter,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct NetworkPolicyInput<'a> {
    proxy_url: Option<&'a str>,
    allowed_appcontainer_peer: Option<&'a str>,
    direct_egress: bool,
}

pub(super) fn encode_proxy_policy(proxy_url: &str, allowed_appcontainer_peer: &str) -> Result<Vec<u8>, PsecCodecError> {
    encode_policy(NetworkPolicyInput {
        proxy_url: Some(proxy_url),
        allowed_appcontainer_peer: Some(allowed_appcontainer_peer),
        direct_egress: false,
    })
}

fn encode_policy(input: NetworkPolicyInput<'_>) -> Result<Vec<u8>, PsecCodecError> {
    if input.proxy_url.is_some() && input.direct_egress {
        return Err(PsecCodecError::ConflictingNetworkRoutes);
    }
    if input.direct_egress {
        return Err(PsecCodecError::DirectEgressNotAllowed);
    }

    let proxy_url = input.proxy_url.ok_or(PsecCodecError::InvalidProxyUrl {
        reason: ProxyUrlError::Host,
    })?;
    validate_proxy_url(proxy_url)?;
    let allowed_appcontainer_peer =
        input
            .allowed_appcontainer_peer
            .ok_or(PsecCodecError::InvalidPackageFamilyName {
                reason: PackageFamilyNameError::Empty,
            })?;
    validate_package_family_name(allowed_appcontainer_peer)?;

    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(256);
    let mut proxy = ProxyInfoT::default();
    proxy.url = Some(proxy_url.to_owned());
    let mut network_policy = NetworkPolicyT::default();
    network_policy.proxy = Some(Box::new(proxy));
    network_policy.allowed_appcontainer_peer = Some(allowed_appcontainer_peer.to_owned());
    let mut spec = ProcessSecurityEnvironmentT::default();
    spec.version = SchemaVersionT { major: 1, minor: 0 };
    spec.capabilities = Some(PRIVATE_NETWORK_CAPABILITY.to_owned());
    spec.network_policy = Some(Box::new(network_policy));
    let root = spec.pack(&mut builder);
    finish_process_security_environment_buffer(&mut builder, root);
    Ok(builder.finished_data().to_vec())
}

fn validate_proxy_url(proxy_url: &str) -> Result<u16, PsecCodecError> {
    if proxy_url.contains('#') {
        return Err(invalid_proxy_url(ProxyUrlError::Fragment));
    }
    if proxy_url.contains('?') {
        return Err(invalid_proxy_url(ProxyUrlError::Query));
    }
    if proxy_url.contains('/') && !proxy_url.starts_with("http://") {
        return Err(invalid_proxy_url(ProxyUrlError::Scheme));
    }
    let authority = proxy_url
        .strip_prefix("http://")
        .ok_or_else(|| invalid_proxy_url(ProxyUrlError::Scheme))?;
    if authority.contains('@') {
        return Err(invalid_proxy_url(ProxyUrlError::UserInfo));
    }
    if authority.contains('/') {
        return Err(invalid_proxy_url(ProxyUrlError::Path));
    }
    let port_text = proxy_url
        .strip_prefix(LOOPBACK_PROXY_PREFIX)
        .ok_or_else(|| invalid_proxy_url(ProxyUrlError::Host))?;
    if port_text.is_empty() {
        return Err(invalid_proxy_url(ProxyUrlError::MissingPort));
    }
    if !port_text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid_proxy_url(ProxyUrlError::InvalidPort));
    }
    let port = port_text
        .parse::<u16>()
        .map_err(|_| invalid_proxy_url(ProxyUrlError::InvalidPort))?;
    if port == 0 {
        return Err(invalid_proxy_url(ProxyUrlError::ZeroPort));
    }
    if port_text != port.to_string() {
        return Err(invalid_proxy_url(ProxyUrlError::InvalidPort));
    }
    Ok(port)
}

const fn invalid_proxy_url(reason: ProxyUrlError) -> PsecCodecError {
    PsecCodecError::InvalidProxyUrl { reason }
}

fn validate_package_family_name(package_family_name: &str) -> Result<(), PsecCodecError> {
    if package_family_name.is_empty() {
        return Err(invalid_package_family_name(PackageFamilyNameError::Empty));
    }
    if package_family_name.len() > PACKAGE_FAMILY_NAME_MAX_LENGTH {
        return Err(invalid_package_family_name(PackageFamilyNameError::InvalidLength));
    }
    let (name, publisher_id) = package_family_name
        .rsplit_once('_')
        .ok_or_else(|| invalid_package_family_name(PackageFamilyNameError::MissingPublisherId))?;
    validate_package_name(name)?;
    validate_publisher_id(publisher_id)?;
    Ok(())
}

fn validate_package_name(name: &str) -> Result<(), PsecCodecError> {
    if !(PACKAGE_NAME_MIN_LENGTH..=PACKAGE_NAME_MAX_LENGTH).contains(&name.len()) {
        return Err(invalid_package_family_name(PackageFamilyNameError::InvalidLength));
    }
    if !name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'))
    {
        return Err(invalid_package_family_name(
            PackageFamilyNameError::InvalidNameCharacters,
        ));
    }

    let folded = name.to_ascii_lowercase();
    if is_reserved_device_package_name(&folded) {
        return Err(invalid_package_family_name(PackageFamilyNameError::ReservedDeviceName));
    }
    if folded.ends_with('.') {
        return Err(invalid_package_family_name(PackageFamilyNameError::TrailingDot));
    }
    if folded.starts_with("xn--") {
        return Err(invalid_package_family_name(PackageFamilyNameError::PunycodePrefix));
    }
    if folded.contains(".xn--") {
        return Err(invalid_package_family_name(PackageFamilyNameError::PunycodeSegment));
    }
    Ok(())
}

fn is_reserved_device_package_name(folded_name: &str) -> bool {
    const RESERVED_DEVICE_NAMES: [&str; 22] = [
        "con", "prn", "aux", "nul", "com1", "com2", "com3", "com4", "com5", "com6", "com7", "com8", "com9", "lpt1",
        "lpt2", "lpt3", "lpt4", "lpt5", "lpt6", "lpt7", "lpt8", "lpt9",
    ];

    RESERVED_DEVICE_NAMES.iter().any(|reserved| {
        folded_name == *reserved
            || folded_name
                .strip_prefix(reserved)
                .is_some_and(|suffix| suffix.starts_with('.'))
    })
}

fn validate_publisher_id(publisher_id: &str) -> Result<(), PsecCodecError> {
    if publisher_id.len() != PUBLISHER_ID_LENGTH {
        return Err(invalid_package_family_name(
            PackageFamilyNameError::InvalidPublisherIdLength,
        ));
    }
    if !publisher_id.to_ascii_lowercase().bytes().all(|byte| {
        matches!(
            byte,
            b'0'..=b'9' | b'a'..=b'h' | b'j'..=b'k' | b'm'..=b'n' | b'p'..=b't' | b'v'..=b'z'
        )
    }) {
        return Err(invalid_package_family_name(
            PackageFamilyNameError::InvalidPublisherIdCharacter,
        ));
    }
    Ok(())
}

const fn invalid_package_family_name(reason: PackageFamilyNameError) -> PsecCodecError {
    PsecCodecError::InvalidPackageFamilyName { reason }
}

#[cfg(test)]
#[path = "psec_codec_test.rs"]
mod psec_codec_test;
