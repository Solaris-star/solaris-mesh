use std::io;
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use super::{
    ApprovedNetworkEndpoint, NetworkProxyPolicy, NetworkProxyPolicyError, is_public_destination, normalize_host,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct NetworkProxyTestFixture {
    endpoint: ApprovedNetworkEndpoint,
    resolved_ip: IpAddr,
    destination: SocketAddr,
}

impl NetworkProxyPolicy {
    pub(crate) fn with_test_connector(
        mut self,
        host: &str,
        port: u16,
        resolved_ip: IpAddr,
        destination: SocketAddr,
    ) -> Result<Self, NetworkProxyPolicyError> {
        let endpoint = ApprovedNetworkEndpoint {
            host: normalize_host(host)?,
            port,
        };
        if port == 0
            || !self.endpoints.contains(&endpoint)
            || !is_public_destination(resolved_ip)
            || !destination.ip().is_loopback()
            || destination.port() == 0
        {
            return Err(NetworkProxyPolicyError::InvalidTestFixture);
        }
        self.test_fixture = Some(NetworkProxyTestFixture {
            endpoint,
            resolved_ip,
            destination,
        });
        Ok(self)
    }
}

impl NetworkProxyTestFixture {
    pub(super) fn connect(
        &self,
        host: &str,
        port: u16,
        stopped: &AtomicBool,
        deadline: Instant,
    ) -> io::Result<TcpStream> {
        if host != self.endpoint.host || port != self.endpoint.port {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "proxy test fixture destination is not approved",
            ));
        }
        if !is_public_destination(self.resolved_ip) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "proxy test fixture DNS result is not public",
            ));
        }
        if stopped.load(Ordering::Acquire) {
            return Err(io::Error::new(io::ErrorKind::Interrupted, "proxy is stopping"));
        }
        let timeout = deadline.saturating_duration_since(Instant::now());
        if timeout.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "proxy destination connection timed out",
            ));
        }
        TcpStream::connect_timeout(&self.destination, timeout)
    }
}

#[cfg(test)]
#[path = "test_fixture_test.rs"]
mod test_fixture_test;
