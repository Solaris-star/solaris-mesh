use std::collections::BTreeMap;
use std::io::{self, Write};
use std::net::{Ipv4Addr, Shutdown, TcpListener, TcpStream};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

const MAX_ACTIVE_CONNECTIONS: usize = 16;
const MAX_PENDING_CONNECTIONS: usize = 16;
const WORKER_POLL_INTERVAL: Duration = Duration::from_millis(20);
const HOST_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const HOST_CONNECT_POLL_INTERVAL: Duration = Duration::from_millis(20);
const OVERLOADED_RESPONSE: &[u8] = b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n";

pub(super) struct LocalProxy {
    stopped: Arc<AtomicBool>,
    active: Arc<ActiveConnections>,
    listener: Option<JoinHandle<()>>,
    workers: Vec<JoinHandle<()>>,
}

impl LocalProxy {
    #[cfg(target_os = "linux")]
    pub(super) fn start(host_socket: &Path, port: u16) -> io::Result<Self> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, port))?;
        Self::start_with_listener(host_socket, listener)
    }

    pub(super) fn start_with_listener(host_socket: &Path, listener: TcpListener) -> io::Result<Self> {
        let local_address = listener.local_addr()?;
        if !matches!(local_address, std::net::SocketAddr::V4(address) if *address.ip() == Ipv4Addr::LOCALHOST && address.port() != 0)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "sandbox proxy listener must use a non-zero IPv4 loopback port",
            ));
        }
        listener.set_nonblocking(true)?;
        let stopped = Arc::new(AtomicBool::new(false));
        let active = Arc::new(ActiveConnections::default());
        let (sender, receiver) = sync_channel(MAX_PENDING_CONNECTIONS);
        let receiver = Arc::new(Mutex::new(receiver));
        let mut workers = Vec::with_capacity(MAX_ACTIVE_CONNECTIONS);
        for index in 0..MAX_ACTIVE_CONNECTIONS {
            let worker = std::thread::Builder::new()
                .name(format!("solaris-sandbox-proxy-worker-{index}"))
                .spawn({
                    let receiver = Arc::clone(&receiver);
                    let host_socket = host_socket.to_path_buf();
                    let stopped = Arc::clone(&stopped);
                    let active = Arc::clone(&active);
                    move || connection_worker(receiver, host_socket, stopped, active)
                });
            match worker {
                Ok(worker) => workers.push(worker),
                Err(error) => {
                    stopped.store(true, Ordering::Release);
                    drop(sender);
                    join_workers(workers);
                    return Err(error);
                }
            }
        }
        let listener_thread = std::thread::Builder::new()
            .name("solaris-sandbox-proxy-listener".to_owned())
            .spawn({
                let stopped = Arc::clone(&stopped);
                move || accept_connections(listener, sender, stopped)
            });
        let listener = match listener_thread {
            Ok(listener) => listener,
            Err(error) => {
                stopped.store(true, Ordering::Release);
                join_workers(workers);
                return Err(error);
            }
        };
        Ok(Self {
            stopped,
            active,
            listener: Some(listener),
            workers,
        })
    }
}

impl Drop for LocalProxy {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Release);
        self.active.shutdown_all();
        if let Some(listener) = self.listener.take() {
            let _ = listener.join();
        }
        join_workers(std::mem::take(&mut self.workers));
    }
}

fn join_workers(workers: Vec<JoinHandle<()>>) {
    for worker in workers {
        let _ = worker.join();
    }
}

fn accept_connections(listener: TcpListener, sender: SyncSender<TcpStream>, stopped: Arc<AtomicBool>) {
    while !stopped.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, _)) => match sender.try_send(stream) {
                Ok(()) => {}
                Err(TrySendError::Full(mut stream)) => {
                    let _ = stream.set_write_timeout(Some(Duration::from_millis(100)));
                    let _ = stream.write_all(OVERLOADED_RESPONSE);
                    let _ = stream.shutdown(Shutdown::Both);
                }
                Err(TrySendError::Disconnected(stream)) => {
                    let _ = stream.shutdown(Shutdown::Both);
                    break;
                }
            },
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(_) => break,
        }
    }
}

fn connection_worker(
    receiver: Arc<Mutex<Receiver<TcpStream>>>,
    host_socket: PathBuf,
    stopped: Arc<AtomicBool>,
    active: Arc<ActiveConnections>,
) {
    loop {
        if stopped.load(Ordering::Acquire) {
            break;
        }
        let received = receiver
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .recv_timeout(WORKER_POLL_INTERVAL);
        let stream = match received {
            Ok(stream) => stream,
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => break,
        };
        if stopped.load(Ordering::Acquire) {
            let _ = stream.shutdown(Shutdown::Both);
            break;
        }
        let _ = handle_connection(stream, &host_socket, &stopped, Arc::clone(&active));
    }
}

fn handle_connection(
    client: TcpStream,
    host_socket: &Path,
    stopped: &AtomicBool,
    active: Arc<ActiveConnections>,
) -> io::Result<()> {
    let connection = active.register(&client, stopped)?;
    let host = connect_host_socket(host_socket, stopped, Instant::now() + HOST_CONNECT_TIMEOUT)?;
    connection.set_host(&host, stopped)?;
    relay(client, host)
}

fn connect_host_socket(host_socket: &Path, stopped: &AtomicBool, deadline: Instant) -> io::Result<UnixStream> {
    let path = host_socket.as_os_str().as_bytes();
    let mut address = unsafe { std::mem::zeroed::<libc::sockaddr_un>() };
    if path.is_empty() || path.contains(&0) || path.len() >= address.sun_path.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "sandbox proxy Host socket path is invalid",
        ));
    }
    address.sun_family = libc::AF_UNIX as libc::sa_family_t;
    for (destination, source) in address.sun_path.iter_mut().zip(path) {
        *destination = *source as libc::c_char;
    }
    let address_length = std::mem::offset_of!(libc::sockaddr_un, sun_path) + path.len() + 1;
    #[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd", target_os = "openbsd"))]
    {
        address.sun_len = address_length.try_into().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "sandbox proxy Host socket path is too long",
            )
        })?;
    }

    let descriptor = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
    if descriptor == -1 {
        return Err(io::Error::last_os_error());
    }
    let descriptor = unsafe { OwnedFd::from_raw_fd(descriptor) };
    set_nonblocking(descriptor.as_raw_fd())?;
    let result = unsafe {
        libc::connect(
            descriptor.as_raw_fd(),
            std::ptr::from_ref(&address).cast::<libc::sockaddr>(),
            address_length as libc::socklen_t,
        )
    };
    if result == -1 {
        let error = io::Error::last_os_error();
        if !matches!(
            error.raw_os_error(),
            Some(libc::EINPROGRESS | libc::EALREADY | libc::EWOULDBLOCK)
        ) {
            return Err(error);
        }
        wait_for_connection(descriptor.as_raw_fd(), stopped, deadline)?;
    }
    let stream = UnixStream::from(descriptor);
    stream.set_nonblocking(false)?;
    Ok(stream)
}

fn set_nonblocking(descriptor: libc::c_int) -> io::Result<()> {
    let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFL) };
    if flags == -1 || unsafe { libc::fcntl(descriptor, libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn wait_for_connection(descriptor: libc::c_int, stopped: &AtomicBool, deadline: Instant) -> io::Result<()> {
    loop {
        let timeout = poll_timeout(stopped, deadline)?;
        let mut descriptor = libc::pollfd {
            fd: descriptor,
            events: libc::POLLOUT,
            revents: 0,
        };
        let ready = unsafe { libc::poll(std::ptr::from_mut(&mut descriptor), 1, timeout) };
        if ready == -1 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        if ready == 0 {
            continue;
        }
        let mut socket_error: libc::c_int = 0;
        let mut length = std::mem::size_of_val(&socket_error) as libc::socklen_t;
        if unsafe {
            libc::getsockopt(
                descriptor.fd,
                libc::SOL_SOCKET,
                libc::SO_ERROR,
                std::ptr::from_mut(&mut socket_error).cast(),
                &mut length,
            )
        } == -1
        {
            return Err(io::Error::last_os_error());
        }
        if socket_error == 0 {
            return Ok(());
        }
        return Err(io::Error::from_raw_os_error(socket_error));
    }
}

fn poll_timeout(stopped: &AtomicBool, deadline: Instant) -> io::Result<libc::c_int> {
    if stopped.load(Ordering::Acquire) {
        return Err(io::Error::new(io::ErrorKind::Interrupted, "sandbox proxy is stopping"));
    }
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "sandbox proxy Host connection timed out",
        ));
    }
    Ok(remaining
        .min(HOST_CONNECT_POLL_INTERVAL)
        .as_millis()
        .max(1)
        .min(libc::c_int::MAX as u128) as libc::c_int)
}

#[derive(Default)]
struct ActiveConnections {
    next_id: AtomicU64,
    connections: Mutex<BTreeMap<u64, ActiveConnection>>,
}

struct ActiveConnection {
    client: TcpStream,
    host: Option<UnixStream>,
}

impl ActiveConnections {
    fn register(self: &Arc<Self>, client: &TcpStream, stopped: &AtomicBool) -> io::Result<ActiveConnectionGuard> {
        let client = client.try_clone()?;
        let mut connections = self.connections.lock().unwrap_or_else(|error| error.into_inner());
        if stopped.load(Ordering::Acquire) {
            let _ = client.shutdown(Shutdown::Both);
            return Err(io::Error::new(io::ErrorKind::Interrupted, "proxy is stopping"));
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        connections.insert(id, ActiveConnection { client, host: None });
        Ok(ActiveConnectionGuard {
            id,
            active: Arc::clone(self),
        })
    }

    fn set_host(&self, id: u64, host: &UnixStream, stopped: &AtomicBool) -> io::Result<()> {
        let host = host.try_clone()?;
        let mut connections = self.connections.lock().unwrap_or_else(|error| error.into_inner());
        if stopped.load(Ordering::Acquire) {
            let _ = host.shutdown(Shutdown::Both);
            return Err(io::Error::new(io::ErrorKind::Interrupted, "proxy is stopping"));
        }
        let connection = connections
            .get_mut(&id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "proxy connection is not active"))?;
        connection.host = Some(host);
        Ok(())
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
            if let Some(host) = &connection.host {
                let _ = host.shutdown(Shutdown::Both);
            }
        }
    }
}

struct ActiveConnectionGuard {
    id: u64,
    active: Arc<ActiveConnections>,
}

impl ActiveConnectionGuard {
    fn set_host(&self, host: &UnixStream, stopped: &AtomicBool) -> io::Result<()> {
        self.active.set_host(self.id, host, stopped)
    }
}

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        self.active.remove(self.id);
    }
}

fn relay(client: TcpStream, host: UnixStream) -> io::Result<()> {
    let mut client_read = client.try_clone()?;
    let mut host_write = host.try_clone()?;
    let client_to_host = std::thread::spawn(move || {
        let result = io::copy(&mut client_read, &mut host_write);
        let _ = host_write.shutdown(Shutdown::Write);
        result
    });
    let mut host_read = host;
    let mut client_write = client;
    let host_to_client = io::copy(&mut host_read, &mut client_write);
    let _ = client_write.shutdown(Shutdown::Write);
    client_to_host
        .join()
        .map_err(|_| io::Error::other("sandbox proxy relay failed"))??;
    host_to_client?;
    Ok(())
}

#[cfg(test)]
#[path = "sandbox_helper_proxy_test.rs"]
mod sandbox_helper_proxy_test;
