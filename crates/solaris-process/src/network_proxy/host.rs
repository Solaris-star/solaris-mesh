use std::collections::BTreeMap;
use std::io::{self, Read, Write};
#[cfg(windows)]
use std::net::{Ipv4Addr, TcpListener};
use std::net::{Shutdown, SocketAddr, TcpStream};
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
#[cfg(unix)]
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use rustls::ClientConfig;
#[cfg(all(test, unix))]
use rustls::pki_types::CertificateDer;

#[cfg(all(feature = "sandbox-test-fixtures", any(unix, test)))]
use super::NetworkProxyTestFixture;
use super::request::{ProxyRequest, ProxyRequestError};
use super::{MAX_PROXY_HEADER_BYTES, NetworkProxyPolicy};

#[path = "host_dns.rs"]
mod host_dns;
#[path = "host_tls.rs"]
mod host_tls;

use host_dns::{MAX_RESOLVED_ADDRESSES, resolve_public_addresses};
use host_tls::{ConnectContext, TlsAuthority, default_upstream_config, handle_connect};

use super::host_http::{forward_request_body, prepare_plain_request};

const IO_TIMEOUT: Duration = Duration::from_secs(30);
const DESTINATION_DEADLINE: Duration = Duration::from_secs(10);
const CONNECT_ATTEMPT_TIMEOUT: Duration = Duration::from_millis(500);
const LISTENER_POLL_INTERVAL: Duration = Duration::from_millis(5);
pub(crate) const MAX_ACTIVE_PROXY_CONNECTIONS: usize = 16;
pub(crate) const MAX_PENDING_PROXY_CONNECTIONS: usize = 16;
const OVERLOADED_RESPONSE: &[u8] = b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n";
const FORBIDDEN_RESPONSE: &[u8] = b"HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n";
const BAD_REQUEST_RESPONSE: &[u8] = b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n";
const HEADER_TOO_LARGE_RESPONSE: &[u8] = b"HTTP/1.1 431 Request Header Fields Too Large\r\nConnection: close\r\n\r\n";

static SHARED_EXECUTOR: OnceLock<Mutex<Weak<ProxyExecutor>>> = OnceLock::new();

#[cfg(unix)]
type ProxyListener = UnixListener;
#[cfg(windows)]
type ProxyListener = TcpListener;
#[cfg(unix)]
pub(super) type ProxyClientStream = UnixStream;
#[cfg(windows)]
pub(super) type ProxyClientStream = TcpStream;

pub(crate) struct HostNetworkProxy {
    #[cfg(unix)]
    socket_path: PathBuf,
    #[cfg(windows)]
    address: SocketAddr,
    stopped: Arc<AtomicBool>,
    active: Arc<ActiveConnections>,
    jobs: Arc<JobTracker>,
    #[cfg(all(test, unix))]
    pending: Arc<AtomicUsize>,
    listener: Option<JoinHandle<()>>,
    _executor: Arc<ProxyExecutor>,
    tls_authority: Arc<TlsAuthority>,
}

impl HostNetworkProxy {
    pub(crate) fn start(socket_path: &Path, policy: NetworkProxyPolicy) -> io::Result<Self> {
        let connector = Arc::new(SystemConnector::for_policy(&policy));
        Self::start_inner(socket_path, None, policy, connector, default_upstream_config()?)
    }

    #[cfg(windows)]
    pub(crate) fn start_on_port(socket_path: &Path, port: u16, policy: NetworkProxyPolicy) -> io::Result<Self> {
        if port == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "proxy port must be non-zero",
            ));
        }
        let connector = Arc::new(SystemConnector::for_policy(&policy));
        Self::start_inner(socket_path, Some(port), policy, connector, default_upstream_config()?)
    }

    fn start_inner(
        socket_path: &Path,
        requested_port: Option<u16>,
        policy: NetworkProxyPolicy,
        connector: Arc<dyn DestinationConnector>,
        upstream_config: Arc<ClientConfig>,
    ) -> io::Result<Self> {
        if policy.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "empty proxy policy"));
        }
        let tls_authority = Arc::new(TlsAuthority::create(socket_path, upstream_config)?);
        let listener = bind_listener(socket_path, requested_port)?;
        #[cfg(windows)]
        let address = listener.local_addr()?;
        if let Err(error) = listener.set_nonblocking(true) {
            remove_listener_path(socket_path);
            return Err(error);
        }
        let executor = match ProxyExecutor::shared() {
            Ok(executor) => executor,
            Err(error) => {
                remove_listener_path(socket_path);
                return Err(error);
            }
        };
        let stopped = Arc::new(AtomicBool::new(false));
        let active = Arc::new(ActiveConnections::default());
        let jobs = Arc::new(JobTracker::default());
        let pending = Arc::new(AtomicUsize::new(0));
        let sender = executor.sender()?;
        let listener_thread = std::thread::Builder::new()
            .name("solaris-network-proxy-listener".to_owned())
            .spawn({
                let stopped = Arc::clone(&stopped);
                let active = Arc::clone(&active);
                let jobs = Arc::clone(&jobs);
                let pending = Arc::clone(&pending);
                let policy = Arc::new(policy);
                let context = ListenerContext {
                    stopped,
                    active,
                    jobs,
                    pending,
                    policy,
                    connector,
                    tls_authority: Arc::clone(&tls_authority),
                };
                move || accept_connections(listener, sender, context)
            });
        let listener = match listener_thread {
            Ok(listener) => listener,
            Err(error) => {
                remove_listener_path(socket_path);
                return Err(error);
            }
        };
        Ok(Self {
            #[cfg(unix)]
            socket_path: socket_path.to_path_buf(),
            #[cfg(windows)]
            address,
            stopped,
            active,
            jobs,
            #[cfg(all(test, unix))]
            pending,
            listener: Some(listener),
            _executor: executor,
            tls_authority,
        })
    }

    #[cfg(all(test, unix))]
    fn start_with_connector(
        socket_path: &Path,
        policy: NetworkProxyPolicy,
        connector: Arc<dyn DestinationConnector>,
    ) -> io::Result<Self> {
        Self::start_inner(socket_path, None, policy, connector, default_upstream_config()?)
    }

    #[cfg(all(test, unix))]
    fn start_with_connector_and_upstream_config(
        socket_path: &Path,
        policy: NetworkProxyPolicy,
        connector: Arc<dyn DestinationConnector>,
        upstream_config: Arc<ClientConfig>,
    ) -> io::Result<Self> {
        Self::start_inner(socket_path, None, policy, connector, upstream_config)
    }

    #[cfg(target_os = "macos")]
    pub(crate) fn create_ca_certificate_alias(&self, alias: &Path) -> io::Result<()> {
        self.tls_authority.create_certificate_alias(alias)
    }

    #[cfg(target_os = "macos")]
    pub(crate) fn verify_ca_certificate_alias(&self, alias: &Path) -> io::Result<()> {
        self.tls_authority.verify_certificate_alias(alias)
    }

    pub(crate) fn verify_ca_certificate(&self) -> io::Result<()> {
        self.tls_authority.verify_certificate_file()
    }

    #[cfg(windows)]
    pub(crate) fn proxy_url(&self) -> String {
        format!("http://{}", self.address)
    }

    #[cfg(windows)]
    pub(crate) fn ca_certificate_path(&self) -> &Path {
        self.tls_authority.certificate_path()
    }

    #[cfg(all(test, unix))]
    fn ca_certificate_der(&self) -> CertificateDer<'static> {
        self.tls_authority.certificate_der()
    }

    #[cfg(all(test, unix))]
    pub(crate) fn active_connection_count(&self) -> usize {
        self.active.len()
    }

    #[cfg(all(test, unix))]
    pub(crate) fn pending_connection_count(&self) -> usize {
        self.pending.load(Ordering::Acquire)
    }

    #[cfg(all(test, unix))]
    pub(crate) fn worker_liveness(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self._executor.worker_liveness)
    }
}

impl Drop for HostNetworkProxy {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Release);
        self.active.shutdown_all();
        if let Some(listener) = self.listener.take() {
            let _ = listener.join();
        }
        self.jobs.wait_until_empty();
        #[cfg(unix)]
        let _ = std::fs::remove_file(&self.socket_path);
    }
}

struct ProxyExecutor {
    sender: Option<SyncSender<ProxyJob>>,
    workers: Mutex<Vec<JoinHandle<()>>>,
    worker_liveness: Arc<AtomicUsize>,
}

impl ProxyExecutor {
    fn shared() -> io::Result<Arc<Self>> {
        let shared = SHARED_EXECUTOR.get_or_init(|| Mutex::new(Weak::new()));
        let mut shared = shared.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(executor) = shared.upgrade() {
            return Ok(executor);
        }
        let executor = Self::new()?;
        *shared = Arc::downgrade(&executor);
        Ok(executor)
    }

    fn new() -> io::Result<Arc<Self>> {
        let (sender, receiver) = sync_channel(MAX_PENDING_PROXY_CONNECTIONS);
        let receiver = Arc::new(Mutex::new(receiver));
        let worker_liveness = Arc::new(AtomicUsize::new(0));
        let mut workers = Vec::with_capacity(MAX_ACTIVE_PROXY_CONNECTIONS);
        for index in 0..MAX_ACTIVE_PROXY_CONNECTIONS {
            match std::thread::Builder::new()
                .name(format!("solaris-network-proxy-worker-{index}"))
                .spawn({
                    let receiver = Arc::clone(&receiver);
                    let liveness = Arc::clone(&worker_liveness);
                    move || connection_worker(receiver, liveness)
                }) {
                Ok(worker) => workers.push(worker),
                Err(error) => {
                    drop(sender);
                    join_workers(workers);
                    return Err(error);
                }
            }
        }
        Ok(Arc::new(Self {
            sender: Some(sender),
            workers: Mutex::new(workers),
            worker_liveness,
        }))
    }

    fn sender(&self) -> io::Result<SyncSender<ProxyJob>> {
        self.sender
            .as_ref()
            .cloned()
            .ok_or_else(|| io::Error::other("proxy executor is stopping"))
    }
}

impl Drop for ProxyExecutor {
    fn drop(&mut self) {
        let _lifecycle = SHARED_EXECUTOR
            .get()
            .map(|shared| shared.lock().unwrap_or_else(|error| error.into_inner()));
        self.sender.take();
        let workers = std::mem::take(self.workers.get_mut().unwrap_or_else(|error| error.into_inner()));
        join_workers(workers);
        debug_assert_eq!(self.worker_liveness.load(Ordering::Acquire), 0);
    }
}

fn join_workers(workers: Vec<JoinHandle<()>>) {
    for worker in workers {
        let _ = worker.join();
    }
}

struct ProxyJob {
    client: ProxyClientStream,
    policy: Arc<NetworkProxyPolicy>,
    stopped: Arc<AtomicBool>,
    active: Arc<ActiveConnections>,
    tracker: Arc<JobTracker>,
    pending: Arc<AtomicUsize>,
    connector: Arc<dyn DestinationConnector>,
    tls_authority: Arc<TlsAuthority>,
}

impl Drop for ProxyJob {
    fn drop(&mut self) {
        self.tracker.complete();
    }
}

#[derive(Default)]
struct JobTracker {
    count: Mutex<usize>,
    empty: Condvar,
}

impl JobTracker {
    fn add(&self) {
        let mut count = self.count.lock().unwrap_or_else(|error| error.into_inner());
        *count += 1;
    }

    fn complete(&self) {
        let mut count = self.count.lock().unwrap_or_else(|error| error.into_inner());
        // ProxyJob is not Clone and is constructed only after add(), so its
        // single Drop must have one matching count entry.
        *count = count.checked_sub(1).expect("proxy job count is balanced");
        if *count == 0 {
            self.empty.notify_all();
        }
    }

    fn wait_until_empty(&self) {
        let mut count = self.count.lock().unwrap_or_else(|error| error.into_inner());
        while *count != 0 {
            count = self.empty.wait(count).unwrap_or_else(|error| error.into_inner());
        }
    }
}

struct ListenerContext {
    stopped: Arc<AtomicBool>,
    active: Arc<ActiveConnections>,
    jobs: Arc<JobTracker>,
    pending: Arc<AtomicUsize>,
    policy: Arc<NetworkProxyPolicy>,
    connector: Arc<dyn DestinationConnector>,
    tls_authority: Arc<TlsAuthority>,
}

fn accept_connections(listener: ProxyListener, sender: SyncSender<ProxyJob>, context: ListenerContext) {
    while !context.stopped.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, _)) => {
                context.jobs.add();
                context.pending.fetch_add(1, Ordering::AcqRel);
                let job = ProxyJob {
                    client: stream,
                    policy: Arc::clone(&context.policy),
                    stopped: Arc::clone(&context.stopped),
                    active: Arc::clone(&context.active),
                    tracker: Arc::clone(&context.jobs),
                    pending: Arc::clone(&context.pending),
                    connector: Arc::clone(&context.connector),
                    tls_authority: Arc::clone(&context.tls_authority),
                };
                match sender.try_send(job) {
                    Ok(()) => {}
                    Err(TrySendError::Full(mut job)) => {
                        context.pending.fetch_sub(1, Ordering::AcqRel);
                        let _ = job.client.set_write_timeout(Some(Duration::from_millis(100)));
                        let _ = job.client.write_all(OVERLOADED_RESPONSE);
                        let _ = job.client.shutdown(Shutdown::Both);
                    }
                    Err(TrySendError::Disconnected(job)) => {
                        context.pending.fetch_sub(1, Ordering::AcqRel);
                        let _ = job.client.shutdown(Shutdown::Both);
                        break;
                    }
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(LISTENER_POLL_INTERVAL);
            }
            Err(_) => break,
        }
    }
}

fn connection_worker(receiver: Arc<Mutex<Receiver<ProxyJob>>>, liveness: Arc<AtomicUsize>) {
    let _liveness = WorkerLiveness::new(liveness);
    loop {
        let received = receiver.lock().unwrap_or_else(|error| error.into_inner()).recv();
        let mut job = match received {
            Ok(job) => job,
            Err(_) => break,
        };
        job.pending.fetch_sub(1, Ordering::AcqRel);
        if job.stopped.load(Ordering::Acquire) {
            let _ = job.client.shutdown(Shutdown::Both);
            continue;
        }
        let _ = handle_connection(
            &mut job.client,
            &job.policy,
            &job.stopped,
            Arc::clone(&job.active),
            job.connector.as_ref(),
            &job.tls_authority,
        );
    }
}

struct WorkerLiveness(Arc<AtomicUsize>);

impl WorkerLiveness {
    fn new(liveness: Arc<AtomicUsize>) -> Self {
        liveness.fetch_add(1, Ordering::AcqRel);
        Self(liveness)
    }
}

impl Drop for WorkerLiveness {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

trait DestinationConnector: Send + Sync {
    fn connect(&self, host: &str, port: u16, stopped: &AtomicBool, deadline: Instant) -> io::Result<TcpStream>;
}

struct SystemConnector {
    #[cfg(all(feature = "sandbox-test-fixtures", any(unix, test)))]
    test_fixture: Option<NetworkProxyTestFixture>,
}

impl SystemConnector {
    fn for_policy(policy: &NetworkProxyPolicy) -> Self {
        #[cfg(not(all(feature = "sandbox-test-fixtures", any(unix, test))))]
        let _ = policy;
        Self {
            #[cfg(all(feature = "sandbox-test-fixtures", any(unix, test)))]
            test_fixture: policy.test_fixture.clone(),
        }
    }
}

impl DestinationConnector for SystemConnector {
    fn connect(&self, host: &str, port: u16, stopped: &AtomicBool, deadline: Instant) -> io::Result<TcpStream> {
        #[cfg(all(feature = "sandbox-test-fixtures", any(unix, test)))]
        if let Some(test_fixture) = &self.test_fixture {
            return test_fixture.connect(host, port, stopped, deadline);
        }
        let addresses = resolve_public_addresses(host, port, stopped, deadline)?;
        connect_public(&addresses, stopped, deadline)
    }
}

fn connect_public(addresses: &[SocketAddr], stopped: &AtomicBool, deadline: Instant) -> io::Result<TcpStream> {
    connect_public_with(addresses, stopped, deadline, TcpStream::connect_timeout)
}

fn connect_public_with(
    addresses: &[SocketAddr],
    stopped: &AtomicBool,
    deadline: Instant,
    mut connect: impl FnMut(&SocketAddr, Duration) -> io::Result<TcpStream>,
) -> io::Result<TcpStream> {
    if addresses.is_empty() || addresses.len() > MAX_RESOLVED_ADDRESSES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "proxy DNS result count is invalid",
        ));
    }
    let mut last_error = None;
    for address in addresses {
        if stopped.load(Ordering::Acquire) {
            return Err(io::Error::new(io::ErrorKind::Interrupted, "proxy is stopping"));
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "proxy destination connection timed out",
            ));
        }
        match connect(address, remaining.min(CONNECT_ATTEMPT_TIMEOUT)) {
            Ok(stream) => return Ok(stream),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| io::Error::new(io::ErrorKind::NotFound, "proxy DNS result is empty")))
}

#[derive(Default)]
struct ActiveConnections {
    next_id: AtomicU64,
    connections: Mutex<BTreeMap<u64, ActiveConnection>>,
}

struct ActiveConnection {
    client: ProxyClientStream,
    upstream: Option<TcpStream>,
}

impl ActiveConnections {
    fn register(
        self: &Arc<Self>,
        client: &ProxyClientStream,
        stopped: &AtomicBool,
    ) -> io::Result<ActiveConnectionGuard> {
        let client = client.try_clone()?;
        let mut connections = self.connections.lock().unwrap_or_else(|error| error.into_inner());
        if stopped.load(Ordering::Acquire) {
            let _ = client.shutdown(Shutdown::Both);
            return Err(io::Error::new(io::ErrorKind::Interrupted, "proxy is stopping"));
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        connections.insert(id, ActiveConnection { client, upstream: None });
        Ok(ActiveConnectionGuard {
            id,
            active: Arc::clone(self),
        })
    }

    fn set_upstream(&self, id: u64, upstream: &TcpStream, stopped: &AtomicBool) -> io::Result<()> {
        let upstream = upstream.try_clone()?;
        let mut connections = self.connections.lock().unwrap_or_else(|error| error.into_inner());
        if stopped.load(Ordering::Acquire) {
            let _ = upstream.shutdown(Shutdown::Both);
            return Err(io::Error::new(io::ErrorKind::Interrupted, "proxy is stopping"));
        }
        let connection = connections
            .get_mut(&id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "proxy connection is not active"))?;
        connection.upstream = Some(upstream);
        Ok(())
    }

    fn clear_upstream(&self, id: u64) {
        if let Some(connection) = self
            .connections
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get_mut(&id)
        {
            connection.upstream = None;
        }
    }

    fn remove(&self, id: u64) {
        self.connections
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&id);
    }

    fn shutdown_all(&self) {
        let connections = self.connections.lock().unwrap_or_else(|error| error.into_inner());
        for connection in connections.values() {
            let _ = connection.client.shutdown(Shutdown::Both);
            if let Some(upstream) = &connection.upstream {
                let _ = upstream.shutdown(Shutdown::Both);
            }
        }
    }

    #[cfg(all(test, unix))]
    fn len(&self) -> usize {
        self.connections.lock().unwrap_or_else(|error| error.into_inner()).len()
    }
}

struct ActiveConnectionGuard {
    id: u64,
    active: Arc<ActiveConnections>,
}

impl ActiveConnectionGuard {
    fn set_upstream(&self, upstream: &TcpStream, stopped: &AtomicBool) -> io::Result<()> {
        self.active.set_upstream(self.id, upstream, stopped)
    }

    fn clear_upstream(&self) {
        self.active.clear_upstream(self.id);
    }
}

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        self.active.remove(self.id);
    }
}

fn handle_connection(
    client: &mut ProxyClientStream,
    policy: &NetworkProxyPolicy,
    stopped: &AtomicBool,
    active: Arc<ActiveConnections>,
    connector: &dyn DestinationConnector,
    tls_authority: &TlsAuthority,
) -> io::Result<()> {
    let connection = active.register(client, stopped)?;
    client.set_read_timeout(Some(IO_TIMEOUT))?;
    client.set_write_timeout(Some(IO_TIMEOUT))?;
    let mut buffered = Vec::with_capacity(4096);
    let mut connection_target = None;
    loop {
        let Some((request, header)) = read_request_header(client, &mut buffered, policy)? else {
            return Ok(());
        };
        if request.connect {
            return handle_connect(
                client,
                std::mem::take(&mut buffered),
                ConnectContext {
                    header: &header,
                    host: &request.host,
                    port: request.port,
                    stopped,
                    connection: &connection,
                    connector,
                    authority: tls_authority,
                },
            );
        }
        if !is_plain_http_header(&header) {
            let _ = client.write_all(FORBIDDEN_RESPONSE);
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "proxy plaintext HTTPS request is denied",
            ));
        }
        let target = (request.host.clone(), request.port);
        if connection_target.as_ref().is_some_and(|approved| approved != &target) {
            let _ = client.write_all(FORBIDDEN_RESPONSE);
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "proxy keep-alive destination change is denied",
            ));
        }
        connection_target = Some(target);
        let prepared = match prepare_plain_request(&header) {
            Ok(prepared) => prepared,
            Err(error) => {
                let _ = client.write_all(BAD_REQUEST_RESPONSE);
                return Err(error);
            }
        };
        let deadline = Instant::now() + DESTINATION_DEADLINE;
        let mut upstream = connector.connect(&request.host, request.port, stopped, deadline)?;
        upstream.set_read_timeout(Some(IO_TIMEOUT))?;
        upstream.set_write_timeout(Some(IO_TIMEOUT))?;
        connection.set_upstream(&upstream, stopped)?;
        upstream.write_all(&prepared.header)?;
        forward_request_body(client, &mut upstream, &mut buffered, prepared.body, stopped)?;
        upstream.shutdown(Shutdown::Write)?;
        io::copy(&mut upstream, client)?;
        connection.clear_upstream();
    }
}

fn read_request_header(
    client: &mut ProxyClientStream,
    buffered: &mut Vec<u8>,
    policy: &NetworkProxyPolicy,
) -> io::Result<Option<(ProxyRequest, Vec<u8>)>> {
    loop {
        match ProxyRequest::parse(buffered, policy) {
            Ok(request) => {
                let header = buffered.drain(..request.header_bytes).collect();
                return Ok(Some((request, header)));
            }
            Err(ProxyRequestError::Incomplete) if buffered.len() <= MAX_PROXY_HEADER_BYTES => {}
            Err(ProxyRequestError::TooLarge) => {
                let _ = client.write_all(HEADER_TOO_LARGE_RESPONSE);
                return Err(io::Error::new(io::ErrorKind::InvalidData, "proxy header too large"));
            }
            Err(ProxyRequestError::Denied) => {
                let _ = client.write_all(FORBIDDEN_RESPONSE);
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "proxy destination denied",
                ));
            }
            Err(ProxyRequestError::Invalid | ProxyRequestError::Incomplete) => {
                let _ = client.write_all(BAD_REQUEST_RESPONSE);
                return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid proxy request"));
            }
        }
        let mut bytes = [0_u8; 4096];
        let read = client.read(&mut bytes)?;
        if read == 0 {
            if buffered.is_empty() {
                return Ok(None);
            }
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "proxy request closed"));
        }
        buffered.extend_from_slice(&bytes[..read]);
    }
}

fn is_plain_http_header(header: &[u8]) -> bool {
    let Some(line_end) = header.windows(2).position(|window| window == b"\r\n") else {
        return false;
    };
    let Ok(line) = std::str::from_utf8(&header[..line_end]) else {
        return false;
    };
    line.split(' ')
        .nth(1)
        .is_some_and(|target| target.starts_with("http://"))
}

#[cfg(unix)]
fn bind_listener(socket_path: &Path, requested_port: Option<u16>) -> io::Result<ProxyListener> {
    debug_assert!(requested_port.is_none());
    UnixListener::bind(socket_path)
}

#[cfg(windows)]
fn bind_listener(_socket_path: &Path, requested_port: Option<u16>) -> io::Result<ProxyListener> {
    TcpListener::bind((Ipv4Addr::LOCALHOST, requested_port.unwrap_or(0)))
}

#[cfg(unix)]
fn remove_listener_path(socket_path: &Path) {
    let _ = std::fs::remove_file(socket_path);
}

#[cfg(windows)]
fn remove_listener_path(_socket_path: &Path) {}

#[cfg(all(test, unix))]
#[path = "host_test.rs"]
mod host_test;
