use std::collections::BTreeMap;
use std::io::{self, Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};
use rustls::crypto::ring::sign::any_supported_type;
#[cfg(test)]
use rustls::pki_types::CertificateDer;
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use rustls::{ClientConfig, ClientConnection, RootCertStore, ServerConfig, ServerConnection, StreamOwned};
use zeroize::{Zeroize, Zeroizing};

use super::host_http::{PreparedRequest, prepare_tls_request, read_header};

const BAD_REQUEST_RESPONSE: &[u8] = b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n";
const FORBIDDEN_RESPONSE: &[u8] = b"HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n";

pub(super) struct CertificateAuthority {
    certificate: Certificate,
    certificate_pem: Vec<u8>,
    key: Zeroizing<KeyPair>,
    server_configs: Mutex<BTreeMap<String, Arc<ServerConfig>>>,
}

impl CertificateAuthority {
    pub(super) fn generate() -> io::Result<Self> {
        let key = Zeroizing::new(KeyPair::generate().map_err(|_| ca_error())?);
        let mut params = CertificateParams::default();
        let mut distinguished_name = DistinguishedName::new();
        distinguished_name.push(DnType::CommonName, "Solaris Mesh Ephemeral Proxy CA");
        params.distinguished_name = distinguished_name;
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        params.use_authority_key_identifier_extension = true;
        let certificate = params.self_signed(&key).map_err(|_| ca_error())?;
        let certificate_pem = certificate.pem().into_bytes();
        Ok(Self {
            certificate,
            certificate_pem,
            key,
            server_configs: Mutex::new(BTreeMap::new()),
        })
    }

    pub(super) fn certificate_pem(&self) -> &[u8] {
        &self.certificate_pem
    }

    pub(super) fn server_config(&self, host: &str) -> io::Result<Arc<ServerConfig>> {
        let mut configs = self.server_configs.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(config) = configs.get(host) {
            return Ok(Arc::clone(config));
        }
        let leaf_key = Zeroizing::new(KeyPair::generate().map_err(|_| ca_error())?);
        let mut params = CertificateParams::new(vec![host.to_owned()]).map_err(|_| ca_error())?;
        let mut distinguished_name = DistinguishedName::new();
        distinguished_name.push(DnType::CommonName, host);
        params.distinguished_name = distinguished_name;
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        params.use_authority_key_identifier_extension = true;
        let leaf = params
            .signed_by(&*leaf_key, &self.certificate, &self.key)
            .map_err(|_| ca_error())?;

        let mut private_key_der = Zeroizing::new(leaf_key.serialize_der());
        let private_key = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(private_key_der.as_slice()));
        let signing_key = any_supported_type(&private_key);
        drop(private_key);
        private_key_der.zeroize();
        let signing_key = signing_key.map_err(|_| tls_configuration_error())?;
        let certified_key = Arc::new(CertifiedKey::new(vec![leaf.der().clone()], signing_key));
        certified_key.keys_match().map_err(|_| tls_configuration_error())?;
        let resolver = Arc::new(FixedCertificateResolver {
            host: host.to_owned(),
            certified_key,
        });
        let mut config = ServerConfig::builder_with_provider(rustls::crypto::ring::default_provider().into())
            .with_safe_default_protocol_versions()
            .map_err(|_| tls_configuration_error())?
            .with_no_client_auth()
            .with_cert_resolver(resolver);
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        let config = Arc::new(config);
        configs.insert(host.to_owned(), Arc::clone(&config));
        Ok(config)
    }

    #[cfg(test)]
    pub(super) fn certificate_der(&self) -> CertificateDer<'static> {
        self.certificate.der().clone()
    }
}

#[derive(Debug)]
struct FixedCertificateResolver {
    host: String,
    certified_key: Arc<CertifiedKey>,
}

impl ResolvesServerCert for FixedCertificateResolver {
    fn resolve(&self, client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        let sni_matches = client_hello.server_name() == Some(self.host.as_str());
        let offers_http_11 = client_hello
            .alpn()
            .is_some_and(|mut protocols| protocols.any(|protocol| protocol == b"http/1.1"));
        (sni_matches && offers_http_11).then(|| Arc::clone(&self.certified_key))
    }
}

pub(super) struct TlsRequestState {
    host: String,
    port: u16,
    buffered: Vec<u8>,
}

impl TlsRequestState {
    pub(super) fn from_connection(host: &str, port: u16, connection: &ServerConnection) -> io::Result<Self> {
        if connection.server_name() != Some(host) || connection.alpn_protocol() != Some(b"http/1.1".as_slice()) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "proxy TLS SNI or ALPN differs from CONNECT authority",
            ));
        }
        Ok(Self {
            host: host.to_owned(),
            port,
            buffered: Vec::with_capacity(4096),
        })
    }

    pub(super) fn next<S: Read + Write>(
        &mut self,
        tls: &mut StreamOwned<ServerConnection, S>,
        stopped: &AtomicBool,
    ) -> io::Result<Option<PreparedRequest>> {
        check_running(stopped)?;
        let header = match read_header(tls, &mut self.buffered) {
            Ok(Some(header)) => header,
            Ok(None) => {
                tls.conn.send_close_notify();
                let _ = tls.flush();
                return Ok(None);
            }
            Err(error) => {
                write_tls_response(tls, BAD_REQUEST_RESPONSE);
                return Err(error);
            }
        };
        match prepare_tls_request(&header, &self.host, self.port) {
            Ok(prepared) => Ok(Some(prepared)),
            Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
                write_tls_response(tls, FORBIDDEN_RESPONSE);
                Err(error)
            }
            Err(error) => {
                write_tls_response(tls, BAD_REQUEST_RESPONSE);
                Err(error)
            }
        }
    }

    #[cfg(any(unix, windows))]
    pub(super) fn buffered(&mut self) -> &mut Vec<u8> {
        &mut self.buffered
    }
}

fn write_tls_response<S: Read + Write>(stream: &mut StreamOwned<ServerConnection, S>, response: &[u8]) {
    if stream.write_all(response).is_ok() {
        stream.conn.send_close_notify();
        let _ = stream.flush();
    }
}

pub(super) fn complete_server_handshake<S: Read + Write>(
    connection: &mut ServerConnection,
    stream: &mut S,
    stopped: &AtomicBool,
    deadline: Instant,
) -> io::Result<()> {
    while connection.is_handshaking() {
        check_handshake_state(stopped, deadline)?;
        match connection.complete_io(stream) {
            Ok(_) => {}
            Err(error) if retryable_io(&error) => {}
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "proxy client TLS handshake failed",
                ));
            }
        }
    }
    Ok(())
}

pub(super) fn complete_client_handshake<S: Read + Write>(
    connection: &mut ClientConnection,
    stream: &mut S,
    stopped: &AtomicBool,
    deadline: Instant,
) -> io::Result<()> {
    while connection.is_handshaking() {
        check_handshake_state(stopped, deadline)?;
        match connection.complete_io(stream) {
            Ok(_) => {}
            Err(error) if retryable_io(&error) => {}
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "proxy upstream TLS handshake failed",
                ));
            }
        }
    }
    Ok(())
}

pub(super) fn default_upstream_config() -> io::Result<Arc<ClientConfig>> {
    let roots = RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    client_config(roots)
}

pub(super) fn client_config(roots: RootCertStore) -> io::Result<Arc<ClientConfig>> {
    let mut config = ClientConfig::builder_with_provider(rustls::crypto::ring::default_provider().into())
        .with_safe_default_protocol_versions()
        .map_err(|_| tls_configuration_error())?
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(config))
}

fn check_handshake_state(stopped: &AtomicBool, deadline: Instant) -> io::Result<()> {
    check_running(stopped)?;
    if Instant::now() >= deadline {
        return Err(io::Error::new(io::ErrorKind::TimedOut, "proxy TLS handshake timed out"));
    }
    Ok(())
}

fn check_running(stopped: &AtomicBool) -> io::Result<()> {
    if stopped.load(Ordering::Acquire) {
        Err(io::Error::new(io::ErrorKind::Interrupted, "proxy is stopping"))
    } else {
        Ok(())
    }
}

fn retryable_io(error: &io::Error) -> bool {
    matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut)
}

fn ca_error() -> io::Error {
    io::Error::other("proxy CA generation failed")
}

fn tls_configuration_error() -> io::Error {
    io::Error::other("proxy TLS configuration failed")
}

#[cfg(test)]
#[path = "host_tls_core_test.rs"]
mod host_tls_core_test;
