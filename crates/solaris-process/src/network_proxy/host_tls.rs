use std::fs::{File, OpenOptions};
use std::io::{self, Cursor, Read, Write};
use std::net::{Shutdown, TcpStream};
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

#[cfg(all(test, unix))]
use rustls::pki_types::CertificateDer;
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ClientConnection, ServerConnection, StreamOwned};

use super::super::host_http::{forward_request_body, validate_connect_header};
#[cfg(all(test, unix))]
pub(super) use super::super::host_tls_core::client_config;
pub(super) use super::super::host_tls_core::default_upstream_config;
use super::super::host_tls_core::{
    CertificateAuthority, TlsRequestState, complete_client_handshake, complete_server_handshake,
};
use super::{
    ActiveConnectionGuard, BAD_REQUEST_RESPONSE, DESTINATION_DEADLINE, DestinationConnector, IO_TIMEOUT,
    ProxyClientStream,
};

pub(super) const CA_CERTIFICATE_FILE_NAME: &str = "network-proxy-ca.pem";
const CONNECT_ESTABLISHED_RESPONSE: &[u8] = b"HTTP/1.1 200 Connection Established\r\n\r\n";
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const TLS_HANDSHAKE_POLL: Duration = Duration::from_millis(100);
const MAX_TLS_BUFFER_BYTES: usize = 256 * 1024;

pub(super) struct TlsAuthority {
    ca_path: PathBuf,
    ca_file: Option<File>,
    #[cfg(unix)]
    ca_identity: FileIdentity,
    core: CertificateAuthority,
    upstream_config: Arc<ClientConfig>,
}

impl TlsAuthority {
    pub(super) fn create(socket_path: &Path, upstream_config: Arc<ClientConfig>) -> io::Result<Self> {
        let parent = socket_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .ok_or_else(|| invalid_input("proxy socket has no parent directory"))?;
        let ca_path = parent.join(CA_CERTIFICATE_FILE_NAME);
        let core = CertificateAuthority::generate()?;
        #[cfg(unix)]
        let (ca_file, ca_identity) = create_ca_file(&ca_path, core.certificate_pem())?;
        #[cfg(windows)]
        let ca_file = create_ca_file(&ca_path, core.certificate_pem())?;
        Ok(Self {
            ca_path,
            ca_file: Some(ca_file),
            #[cfg(unix)]
            ca_identity,
            core,
            upstream_config,
        })
    }

    #[cfg(any(test, windows))]
    pub(super) fn certificate_path(&self) -> &Path {
        &self.ca_path
    }

    pub(super) fn verify_certificate_file(&self) -> io::Result<()> {
        self.verify_certificate_path(&self.ca_path)
    }

    #[cfg(target_os = "macos")]
    pub(super) fn create_certificate_alias(&self, alias: &Path) -> io::Result<()> {
        std::fs::hard_link(&self.ca_path, alias)?;
        self.verify_certificate_path(alias)
    }

    #[cfg(target_os = "macos")]
    pub(super) fn verify_certificate_alias(&self, alias: &Path) -> io::Result<()> {
        self.verify_certificate_path(alias)
    }

    fn verify_certificate_path(&self, path: &Path) -> io::Result<()> {
        #[cfg(unix)]
        {
            let path_metadata = std::fs::symlink_metadata(path)?;
            let file_metadata = self
                .ca_file
                .as_ref()
                .ok_or_else(|| io::Error::other("proxy CA certificate is closed"))?
                .metadata()?;
            if !path_metadata.file_type().is_file()
                || path_metadata.permissions().mode() & 0o222 != 0
                || !self.ca_identity.matches(&path_metadata)
                || !self.ca_identity.matches(&file_metadata)
                || std::fs::read(path)? != self.core.certificate_pem()
            {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "proxy CA certificate file identity changed",
                ));
            }
            Ok(())
        }
        #[cfg(windows)]
        {
            use std::io::{Seek, SeekFrom};

            let path_metadata = std::fs::symlink_metadata(path)?;
            let file = self
                .ca_file
                .as_ref()
                .ok_or_else(|| io::Error::other("proxy CA certificate is closed"))?;
            let file_metadata = file.metadata()?;
            let mut contents = Vec::new();
            let mut reader = file.try_clone()?;
            reader.seek(SeekFrom::Start(0))?;
            reader.read_to_end(&mut contents)?;
            if !path_metadata.file_type().is_file()
                || !path_metadata.permissions().readonly()
                || path_metadata.len() != file_metadata.len()
                || contents != self.core.certificate_pem()
            {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "proxy CA certificate file identity changed",
                ));
            }
            Ok(())
        }
    }

    #[cfg(all(test, unix))]
    pub(super) fn certificate_der(&self) -> CertificateDer<'static> {
        self.core.certificate_der()
    }
}

impl Drop for TlsAuthority {
    fn drop(&mut self) {
        #[cfg(unix)]
        if std::fs::symlink_metadata(&self.ca_path).is_ok_and(|metadata| self.ca_identity.matches(&metadata)) {
            let _ = std::fs::remove_file(&self.ca_path);
        }
        #[cfg(windows)]
        {
            self.ca_file.take();
            if let Ok(metadata) = std::fs::symlink_metadata(&self.ca_path) {
                clear_windows_readonly(&self.ca_path, &metadata);
                let _ = std::fs::remove_file(&self.ca_path);
            }
        }
    }
}

#[cfg(unix)]
#[derive(Clone, Copy)]
struct FileIdentity {
    device: u64,
    inode: u64,
    length: u64,
}

#[cfg(unix)]
impl FileIdentity {
    fn from_metadata(metadata: &std::fs::Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            length: metadata.len(),
        }
    }

    fn matches(self, metadata: &std::fs::Metadata) -> bool {
        metadata.dev() == self.device && metadata.ino() == self.inode && metadata.len() == self.length
    }
}

#[cfg(unix)]
fn create_ca_file(path: &Path, pem: &[u8]) -> io::Result<(File, FileIdentity)> {
    let mut writer = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o400)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)?;
    let created_identity = FileIdentity::from_metadata(&writer.metadata()?);
    let result = (|| {
        writer.write_all(pem)?;
        writer.sync_all()?;
        drop(writer);
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(path)?;
        let metadata = file.metadata()?;
        if !metadata.file_type().is_file() || metadata.permissions().mode() & 0o222 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "proxy CA certificate is not read-only",
            ));
        }
        Ok((file, FileIdentity::from_metadata(&metadata)))
    })();
    if result.is_err() && std::fs::symlink_metadata(path).is_ok_and(|metadata| created_identity.matches(&metadata)) {
        let _ = std::fs::remove_file(path);
    }
    result
}

#[cfg(windows)]
fn create_ca_file(path: &Path, pem: &[u8]) -> io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt;

    use windows_sys::Win32::Storage::FileSystem::{FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ};

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .share_mode(FILE_SHARE_READ)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)?;
    let result = (|| {
        file.write_all(pem)?;
        file.sync_all()?;
        let mut permissions = file.metadata()?.permissions();
        permissions.set_readonly(true);
        std::fs::set_permissions(path, permissions)?;
        Ok(())
    })();
    if let Err(error) = result {
        drop(file);
        let _ = std::fs::remove_file(path);
        return Err(error);
    }
    Ok(file)
}

#[cfg(windows)]
fn clear_windows_readonly(path: &Path, metadata: &std::fs::Metadata) {
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::fs::MetadataExt;

    use windows_sys::Win32::Storage::FileSystem::{FILE_ATTRIBUTE_READONLY, SetFileAttributesW};

    let path = path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    unsafe {
        SetFileAttributesW(path.as_ptr(), metadata.file_attributes() & !FILE_ATTRIBUTE_READONLY);
    }
}

pub(super) struct ConnectContext<'a> {
    pub(super) header: &'a [u8],
    pub(super) host: &'a str,
    pub(super) port: u16,
    pub(super) stopped: &'a AtomicBool,
    pub(super) connection: &'a ActiveConnectionGuard,
    pub(super) connector: &'a dyn DestinationConnector,
    pub(super) authority: &'a TlsAuthority,
}

pub(super) fn handle_connect(
    client: &mut ProxyClientStream,
    initial_tls_bytes: Vec<u8>,
    context: ConnectContext<'_>,
) -> io::Result<()> {
    if let Err(error) = validate_connect_header(context.header) {
        let _ = client.write_all(BAD_REQUEST_RESPONSE);
        return Err(error);
    }
    context.authority.verify_certificate_file()?;
    client.write_all(CONNECT_ESTABLISHED_RESPONSE)?;
    let config = context.authority.core.server_config(context.host)?;
    let mut server = ServerConnection::new(config).map_err(|_| tls_configuration_error())?;
    server.set_buffer_limit(Some(MAX_TLS_BUFFER_BYTES));
    let mut socket = PrefixedProxyStream::new(client, initial_tls_bytes);
    configure_handshake_timeouts(socket.inner)?;
    complete_server_handshake(
        &mut server,
        &mut socket,
        context.stopped,
        Instant::now() + TLS_HANDSHAKE_TIMEOUT,
    )?;
    let mut request_state = TlsRequestState::from_connection(context.host, context.port, &server)?;
    socket.inner.set_read_timeout(Some(IO_TIMEOUT))?;
    socket.inner.set_write_timeout(Some(IO_TIMEOUT))?;
    let mut tls = StreamOwned::new(server, socket);
    loop {
        let Some(prepared) = request_state.next(&mut tls, context.stopped)? else {
            return Ok(());
        };
        let deadline = Instant::now() + DESTINATION_DEADLINE;
        let upstream = context
            .connector
            .connect(context.host, context.port, context.stopped, deadline)?;
        let relay = TlsRelayContext {
            host: context.host,
            upstream_config: Arc::clone(&context.authority.upstream_config),
            stopped: context.stopped,
            connection: context.connection,
        };
        relay_tls_request(&mut tls, request_state.buffered(), prepared, upstream, &relay)?;
    }
}

struct TlsRelayContext<'a> {
    host: &'a str,
    upstream_config: Arc<ClientConfig>,
    stopped: &'a AtomicBool,
    connection: &'a ActiveConnectionGuard,
}

fn relay_tls_request(
    client: &mut impl ReadWrite,
    buffered: &mut Vec<u8>,
    prepared: super::super::host_http::PreparedRequest,
    mut upstream: TcpStream,
    context: &TlsRelayContext<'_>,
) -> io::Result<()> {
    configure_handshake_timeouts(&upstream)?;
    context.connection.set_upstream(&upstream, context.stopped)?;
    let server_name = ServerName::try_from(context.host.to_owned())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "proxy TLS server name is invalid"))?;
    let mut tls = ClientConnection::new(Arc::clone(&context.upstream_config), server_name)
        .map_err(|_| tls_configuration_error())?;
    tls.set_buffer_limit(Some(MAX_TLS_BUFFER_BYTES));
    if let Err(error) = complete_client_handshake(
        &mut tls,
        &mut upstream,
        context.stopped,
        Instant::now() + TLS_HANDSHAKE_TIMEOUT,
    ) {
        context.connection.clear_upstream();
        return Err(error);
    }
    if tls.alpn_protocol() != Some(b"http/1.1".as_slice()) {
        context.connection.clear_upstream();
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "proxy upstream did not negotiate HTTP/1.1",
        ));
    }
    upstream.set_read_timeout(Some(IO_TIMEOUT))?;
    upstream.set_write_timeout(Some(IO_TIMEOUT))?;
    let mut upstream_tls = StreamOwned::new(tls, upstream);
    let result = (|| {
        upstream_tls.write_all(&prepared.header)?;
        forward_request_body(client, &mut upstream_tls, buffered, prepared.body, context.stopped)?;
        upstream_tls.flush()?;
        upstream_tls.conn.send_close_notify();
        io::copy(&mut upstream_tls, client)?;
        Ok(())
    })();
    let _ = upstream_tls.sock.shutdown(Shutdown::Both);
    context.connection.clear_upstream();
    result
}

trait ReadWrite: Read + Write {}

impl<T: Read + Write> ReadWrite for T {}

struct PrefixedProxyStream<'a> {
    inner: &'a mut ProxyClientStream,
    prefix: Cursor<Vec<u8>>,
}

impl<'a> PrefixedProxyStream<'a> {
    fn new(inner: &'a mut ProxyClientStream, prefix: Vec<u8>) -> Self {
        Self {
            inner,
            prefix: Cursor::new(prefix),
        }
    }
}

impl Read for PrefixedProxyStream<'_> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let read = self.prefix.read(bytes)?;
        if read == 0 { self.inner.read(bytes) } else { Ok(read) }
    }
}

impl Write for PrefixedProxyStream<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.inner.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

fn configure_handshake_timeouts(stream: &impl SocketTimeouts) -> io::Result<()> {
    stream.set_read_timeout(Some(TLS_HANDSHAKE_POLL))?;
    stream.set_write_timeout(Some(TLS_HANDSHAKE_POLL))
}

trait SocketTimeouts {
    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()>;
    fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()>;
}

#[cfg(unix)]
impl SocketTimeouts for std::os::unix::net::UnixStream {
    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        std::os::unix::net::UnixStream::set_read_timeout(self, timeout)
    }

    fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        std::os::unix::net::UnixStream::set_write_timeout(self, timeout)
    }
}

impl SocketTimeouts for TcpStream {
    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        TcpStream::set_read_timeout(self, timeout)
    }

    fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        TcpStream::set_write_timeout(self, timeout)
    }
}

fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn tls_configuration_error() -> io::Error {
    io::Error::other("proxy TLS configuration failed")
}

#[cfg(all(test, unix))]
#[path = "host_tls_test.rs"]
mod host_tls_test;
