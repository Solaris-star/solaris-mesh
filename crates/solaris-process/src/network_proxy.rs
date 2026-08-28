use std::collections::BTreeSet;
use std::net::IpAddr;

use thiserror::Error;

#[cfg(any(unix, windows))]
#[cfg_attr(windows, allow(dead_code))]
#[path = "network_proxy/host.rs"]
mod host;
#[cfg(any(unix, windows))]
#[path = "network_proxy/host_http.rs"]
mod host_http;
#[cfg(any(unix, windows))]
#[path = "network_proxy/host_tls_core.rs"]
mod host_tls_core;
#[cfg(any(unix, windows))]
#[path = "network_proxy/request.rs"]
mod request;
#[cfg(all(feature = "sandbox-test-fixtures", any(unix, test)))]
#[path = "network_proxy/test_fixture.rs"]
mod test_fixture;

#[cfg(all(feature = "sandbox-test-fixtures", any(unix, test)))]
use test_fixture::NetworkProxyTestFixture;

#[cfg(any(unix, windows))]
#[cfg_attr(windows, allow(unused_imports))]
pub(crate) use host::HostNetworkProxy;

const MAX_PERMISSION_VALUE_BYTES: usize = 2048;
const MAX_NETWORK_PROXY_ENDPOINTS: usize = 32;
#[cfg(any(unix, windows))]
pub(crate) const MAX_PROXY_HEADER_BYTES: usize = 64 * 1024;
#[cfg(any(unix, windows))]
pub(crate) const MAX_PROXY_HEADER_COUNT: usize = 128;

/// One exact host and TCP port approved for an Auto child process.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ApprovedNetworkEndpoint {
    host: String,
    port: u16,
}

/// Validated network destinations for one strict sandbox launch.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NetworkProxyPolicy {
    endpoints: Vec<ApprovedNetworkEndpoint>,
    #[cfg(all(feature = "sandbox-test-fixtures", any(unix, test)))]
    test_fixture: Option<NetworkProxyTestFixture>,
}

impl NetworkProxyPolicy {
    pub(crate) fn from_permission_domains(
        domains: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<Self, NetworkProxyPolicyError> {
        let mut endpoints = BTreeSet::new();
        for domain in domains {
            for endpoint in parse_permission_domain(domain.as_ref())? {
                endpoints.insert(endpoint);
                if endpoints.len() > MAX_NETWORK_PROXY_ENDPOINTS {
                    return Err(NetworkProxyPolicyError::TooManyEndpoints);
                }
            }
        }
        Ok(Self {
            endpoints: endpoints.into_iter().collect(),
            #[cfg(all(feature = "sandbox-test-fixtures", any(unix, test)))]
            test_fixture: None,
        })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.endpoints.is_empty()
    }

    #[cfg(windows)]
    pub(crate) fn windows_permission_domains(&self) -> impl Iterator<Item = String> + '_ {
        self.endpoints
            .iter()
            .map(|endpoint| format!("{}:{}", endpoint.host, endpoint.port))
    }

    #[cfg(windows)]
    pub(crate) fn windows_proof_endpoint(&self) -> Option<(String, u16)> {
        self.endpoints
            .first()
            .map(|endpoint| (endpoint.host.clone(), endpoint.port))
    }

    #[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
    pub(crate) fn external_runner_endpoints(&self) -> impl Iterator<Item = (&str, u16)> {
        self.endpoints
            .iter()
            .map(|endpoint| (endpoint.host.as_str(), endpoint.port))
    }

    #[cfg(any(unix, windows))]
    pub(crate) fn permits(&self, host: &str, port: u16) -> bool {
        let Ok(host) = normalize_host(host) else {
            return false;
        };
        self.endpoints
            .binary_search(&ApprovedNetworkEndpoint { host, port })
            .is_ok()
    }
}

/// Returns whether one configured network permission covers the exact HTTP(S)
/// host and port represented by another permission value.
pub fn permission_domain_is_covered(requested: &str, allowed: &str) -> Result<bool, NetworkProxyPolicyError> {
    let requested = parse_permission_domain(requested)?;
    let allowed = parse_permission_domain(allowed)?.into_iter().collect::<BTreeSet<_>>();
    Ok(requested.into_iter().all(|endpoint| allowed.contains(&endpoint)))
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum NetworkProxyPolicyError {
    #[error("network permission is not an exact HTTP(S) domain")]
    InvalidDomain,
    #[error("network permission contains an IP literal")]
    IpLiteral,
    #[error("network permission contains a wildcard")]
    Wildcard,
    #[error("network permission contains user information")]
    UserInfo,
    #[error("network permission names a metadata service")]
    MetadataService,
    #[error("network permission contains too many exact endpoints")]
    TooManyEndpoints,
    #[cfg(all(feature = "sandbox-test-fixtures", any(unix, test)))]
    #[doc(hidden)]
    #[error("network proxy test fixture is invalid")]
    InvalidTestFixture,
}

fn parse_permission_domain(value: &str) -> Result<Vec<ApprovedNetworkEndpoint>, NetworkProxyPolicyError> {
    if value.is_empty()
        || value.len() > MAX_PERMISSION_VALUE_BYTES
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(NetworkProxyPolicyError::InvalidDomain);
    }
    if value.contains('*') {
        return Err(NetworkProxyPolicyError::Wildcard);
    }
    let (authority, default_ports) = if let Some(rest) = value.strip_prefix("https://") {
        (authority(rest)?, &[443][..])
    } else if let Some(rest) = value.strip_prefix("http://") {
        (authority(rest)?, &[80][..])
    } else if value.contains("://") || value.contains(['/', '?', '#']) {
        return Err(NetworkProxyPolicyError::InvalidDomain);
    } else {
        (value, &[443][..])
    };
    if authority.contains('@') {
        return Err(NetworkProxyPolicyError::UserInfo);
    }
    if authority.starts_with('[') || authority.ends_with(']') {
        return Err(NetworkProxyPolicyError::IpLiteral);
    }
    let (host, ports) = match authority.rsplit_once(':') {
        Some((host, port)) if !host.contains(':') => {
            let port = port
                .parse::<u16>()
                .ok()
                .filter(|port| *port != 0)
                .ok_or(NetworkProxyPolicyError::InvalidDomain)?;
            (host, vec![port])
        }
        Some(_) => return Err(NetworkProxyPolicyError::IpLiteral),
        None => (authority, default_ports.to_vec()),
    };
    let host = normalize_host(host)?;
    Ok(ports
        .into_iter()
        .map(|port| ApprovedNetworkEndpoint {
            host: host.clone(),
            port,
        })
        .collect())
}

fn authority(value: &str) -> Result<&str, NetworkProxyPolicyError> {
    let end = value.find(['/', '?', '#']).unwrap_or(value.len());
    let authority = &value[..end];
    if authority.is_empty() {
        return Err(NetworkProxyPolicyError::InvalidDomain);
    }
    Ok(authority)
}

fn normalize_host(value: &str) -> Result<String, NetworkProxyPolicyError> {
    let host = value.strip_suffix('.').unwrap_or(value).to_ascii_lowercase();
    if host.parse::<IpAddr>().is_ok() {
        return Err(NetworkProxyPolicyError::IpLiteral);
    }
    if host == "localhost"
        || host.ends_with(".localhost")
        || matches!(
            host.as_str(),
            "metadata"
                | "instance-data"
                | "metadata.google.internal"
                | "metadata.azure.internal"
                | "instance-data.ec2.internal"
                | "metadata.aws.internal"
        )
    {
        return Err(NetworkProxyPolicyError::MetadataService);
    }
    if host.is_empty() || host.len() > 253 || !host.is_ascii() {
        return Err(NetworkProxyPolicyError::InvalidDomain);
    }
    for label in host.split('.') {
        if label.is_empty()
            || label.len() > 63
            || !label.as_bytes()[0].is_ascii_alphanumeric()
            || !label.as_bytes()[label.len() - 1].is_ascii_alphanumeric()
            || !label.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err(NetworkProxyPolicyError::InvalidDomain);
        }
    }
    Ok(host)
}

#[cfg(any(unix, windows))]
pub(crate) fn is_public_destination(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => {
            let octets = address.octets();
            !(address.is_unspecified()
                || address.is_loopback()
                || address.is_private()
                || address.is_link_local()
                || address.is_multicast()
                || address.is_broadcast()
                || address.is_documentation()
                || octets[0] == 0
                || (octets[0] == 100 && (64..=127).contains(&octets[1]))
                || (octets[0] == 192 && octets[1] == 0 && octets[2] == 0)
                || (octets[0] == 192 && octets[1] == 31 && octets[2] == 196)
                || (octets[0] == 192 && octets[1] == 52 && octets[2] == 193)
                || (octets[0] == 192 && octets[1] == 88 && octets[2] == 99)
                || (octets[0] == 192 && octets[1] == 175 && octets[2] == 48)
                || (octets[0] == 198 && matches!(octets[1], 18 | 19))
                || octets[0] >= 240)
        }
        IpAddr::V6(address) => {
            if let Some(address) = address.to_ipv4_mapped() {
                return is_public_destination(IpAddr::V4(address));
            }
            let segments = address.segments();
            !(address.is_unspecified()
                || address.is_loopback()
                || address.is_multicast()
                || segments[..6].iter().all(|segment| *segment == 0)
                || (segments[0] & 0xfe00) == 0xfc00
                || (segments[0] & 0xffc0) == 0xfe80
                || (segments[0] & 0xffc0) == 0xfec0
                || (segments[0] == 0x0064
                    && segments[1] == 0xff9b
                    && (segments[2..6].iter().all(|segment| *segment == 0) || segments[2] == 1))
                || (segments[0] == 0x0100 && segments[1..4].iter().all(|segment| *segment == 0))
                || (segments[0] == 0x0100 && segments[1] == 0 && segments[2] == 0 && segments[3] == 1)
                || (segments[0] == 0x2001 && segments[1] <= 0x01ff)
                || (segments[0] == 0x2001 && segments[1] == 0x0db8)
                || segments[0] == 0x2002
                || (segments[0] == 0x2620 && segments[1] == 0x004f && segments[2] == 0x8000)
                || (segments[0] == 0x3fff && (segments[1] & 0xf000) == 0)
                || segments[0] == 0x5f00
                || (segments[0] & 0xe000) != 0x2000)
        }
    }
}

#[cfg(test)]
#[path = "network_proxy_test.rs"]
mod network_proxy_test;
