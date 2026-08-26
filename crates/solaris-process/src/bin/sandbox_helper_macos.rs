use std::ffi::{CString, OsStr, OsString};
use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use super::sandbox_helper_proxy::LocalProxy;

const MAX_SANITIZED_DESCRIPTORS: libc::rlim_t = 65_536;

pub(super) fn run() -> io::Result<()> {
    let invocation = Invocation::parse(std::env::args_os().skip(1))?;
    let mut status = prepare_status(invocation.status_fd)?;
    probe_workspace_write(&invocation.workspace)?;
    probe_denied_path(&invocation.denied_probe)?;
    probe_denied_network(invocation.denied_port)?;
    let _proxy = invocation
        .proxy
        .as_ref()
        .map(|(socket, descriptor)| {
            let listener = prepare_proxy_listener(*descriptor)?;
            LocalProxy::start_with_listener(socket, listener)
        })
        .transpose()?;
    let environment = std::env::vars_os().collect::<Vec<_>>();
    mark_inherited_descriptors_cloexec(descriptor_sanitize_limit()?)?;
    let mut child = SpawnedTarget::spawn(&invocation.target, &invocation.target_args, &environment)?;
    publish_status(&mut status)?;
    mirror_status(child.wait()?)
}

struct Invocation {
    workspace: PathBuf,
    status_fd: i32,
    denied_probe: PathBuf,
    denied_port: u16,
    proxy: Option<(PathBuf, i32)>,
    target: PathBuf,
    target_args: Vec<OsString>,
}

impl Invocation {
    fn parse(arguments: impl IntoIterator<Item = OsString>) -> io::Result<Self> {
        let mut arguments = arguments.into_iter();
        let workspace = next_path(&mut arguments, "--workspace")?;
        if arguments.next().as_deref() != Some(OsStr::new("--status-fd")) {
            return Err(invalid_input());
        }
        let status_fd = arguments
            .next()
            .and_then(|value| value.to_str().and_then(|value| value.parse().ok()))
            .filter(|descriptor| *descriptor >= 3)
            .ok_or_else(invalid_input)?;
        let denied_probe = next_path(&mut arguments, "--denied-probe")?;
        if arguments.next().as_deref() != Some(OsStr::new("--denied-port")) {
            return Err(invalid_input());
        }
        let denied_port = arguments
            .next()
            .and_then(|value| value.to_str().and_then(|value| value.parse().ok()))
            .filter(|port| *port != 0)
            .ok_or_else(invalid_input)?;
        let proxy = match arguments.next().as_deref() {
            Some(value) if value == OsStr::new("--proxy-socket") => {
                let socket = arguments.next().map(PathBuf::from).ok_or_else(invalid_input)?;
                if arguments.next().as_deref() != Some(OsStr::new("--proxy-listener-fd")) {
                    return Err(invalid_input());
                }
                let descriptor = arguments
                    .next()
                    .and_then(|value| value.to_str().and_then(|value| value.parse::<i32>().ok()))
                    .filter(|descriptor| *descriptor >= 3 && *descriptor != status_fd)
                    .ok_or_else(invalid_input)?;
                if arguments.next().as_deref() != Some(OsStr::new("--")) {
                    return Err(invalid_input());
                }
                Some((socket, descriptor))
            }
            Some(value) if value == OsStr::new("--") => None,
            _ => return Err(invalid_input()),
        };
        let target = arguments.next().map(PathBuf::from).ok_or_else(invalid_input)?;
        if !workspace.is_absolute()
            || !denied_probe.is_absolute()
            || !target.is_absolute()
            || proxy.as_ref().is_some_and(|(socket, _)| !socket.is_absolute())
        {
            return Err(invalid_input());
        }
        Ok(Self {
            workspace,
            status_fd,
            denied_probe,
            denied_port,
            proxy,
            target,
            target_args: arguments.collect(),
        })
    }
}

fn prepare_proxy_listener(descriptor: i32) -> io::Result<TcpListener> {
    let mut metadata: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::fstat(descriptor, &mut metadata) } == -1 || metadata.st_mode & libc::S_IFMT != libc::S_IFSOCK {
        return Err(invalid_input());
    }
    // SAFETY: fstat confirmed that the transferred descriptor is an open socket.
    let listener = unsafe { TcpListener::from_raw_fd(descriptor) };
    let mut accepting = 0_i32;
    let mut accepting_size = std::mem::size_of_val(&accepting) as libc::socklen_t;
    if unsafe {
        libc::getsockopt(
            descriptor,
            libc::SOL_SOCKET,
            libc::SO_ACCEPTCONN,
            std::ptr::addr_of_mut!(accepting).cast(),
            &mut accepting_size,
        )
    } == -1
        || accepting != 1
    {
        return Err(invalid_input());
    }
    match listener.local_addr()? {
        SocketAddr::V4(address) if *address.ip() == Ipv4Addr::LOCALHOST && address.port() != 0 => {}
        _ => return Err(invalid_input()),
    }
    set_cloexec(listener.as_raw_fd())?;
    Ok(listener)
}

fn next_path(arguments: &mut impl Iterator<Item = OsString>, flag: &str) -> io::Result<PathBuf> {
    if arguments.next().as_deref() != Some(OsStr::new(flag)) {
        return Err(invalid_input());
    }
    arguments.next().map(PathBuf::from).ok_or_else(invalid_input)
}

fn probe_workspace_write(workspace: &Path) -> io::Result<()> {
    for attempt in 0..16_u8 {
        let path = workspace.join(format!(".solaris-sandbox-probe-{}-{attempt}", std::process::id()));
        let mut file = match std::fs::OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        };
        let result = file.write_all(b"workspace-write-probe").and_then(|_| file.sync_all());
        drop(file);
        let cleanup = std::fs::remove_file(path);
        result?;
        cleanup?;
        return Ok(());
    }
    Err(permission_denied())
}

fn probe_denied_path(path: &Path) -> io::Result<()> {
    match std::fs::File::open(path) {
        Err(error) if is_sandbox_denial(&error) => {}
        _ => return Err(permission_denied()),
    }
    match std::fs::OpenOptions::new().append(true).open(path) {
        Err(error) if is_sandbox_denial(&error) => Ok(()),
        _ => Err(permission_denied()),
    }
}

fn probe_denied_network(port: u16) -> io::Result<()> {
    let address = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    match TcpStream::connect_timeout(&address, Duration::from_secs(1)) {
        Err(error) if is_sandbox_denial(&error) => Ok(()),
        _ => Err(permission_denied()),
    }
}

fn is_sandbox_denial(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::PermissionDenied || matches!(error.raw_os_error(), Some(libc::EPERM | libc::EACCES))
}

fn prepare_status(descriptor: i32) -> io::Result<std::fs::File> {
    // SAFETY: the parent transferred ownership of this inherited descriptor
    // to the helper through the explicit invocation contract.
    let file = unsafe { std::fs::File::from_raw_fd(descriptor) };
    let mut metadata: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::fstat(descriptor, &mut metadata) } == -1 || metadata.st_mode & libc::S_IFMT != libc::S_IFIFO {
        return Err(invalid_input());
    }
    if unsafe { libc::fcntl(descriptor, libc::F_SETFD, libc::FD_CLOEXEC) } == -1 {
        return Err(permission_denied());
    }
    Ok(file)
}

fn publish_status(file: &mut std::fs::File) -> io::Result<()> {
    file.write_all(b"full\n")?;
    file.flush()
}

struct SpawnedTarget {
    pid: Option<libc::pid_t>,
}

impl SpawnedTarget {
    fn spawn(target: &Path, arguments: &[OsString], environment: &[(OsString, OsString)]) -> io::Result<Self> {
        let execution = PreparedExec::new(target, arguments, environment)?;
        let (mut error_reader, error_writer) = cloexec_pipe()?;
        let reader_fd = error_reader.as_raw_fd();
        let writer_fd = error_writer.as_raw_fd();
        // SAFETY: Darwin returns the non-null errno slot for the current
        // thread. Capturing its address before fork lets the child read the
        // execve error without calling Rust or another errno accessor after
        // fork. The slot remains part of the copied calling thread in the
        // child until exec or _exit.
        let errno_location = unsafe { libc::__error() };
        // SAFETY: the child branch immediately enters
        // `exec_target_after_fork`, whose contract permits only
        // async-signal-safe libc operations before exec or _exit. The parent
        // keeps ordinary Rust ownership of both pipe endpoints.
        let pid = unsafe { libc::fork() };
        if pid == -1 {
            return Err(io::Error::last_os_error());
        }
        if pid == 0 {
            // SAFETY: `execution` owns every CString referenced by its pointer
            // arrays, the pipe descriptors are valid in this forked child, and
            // `errno_location` was captured for this thread before fork.
            unsafe {
                exec_target_after_fork(&execution, reader_fd, writer_fd, errno_location);
            }
        }

        drop(error_writer);
        let mut child = Self { pid: Some(pid) };
        match read_exec_error(&mut error_reader) {
            Ok(None) => Ok(child),
            Ok(Some(error)) => {
                let _ = child.wait();
                Err(io::Error::from_raw_os_error(error))
            }
            Err(error) => Err(error),
        }
    }

    fn wait(&mut self) -> io::Result<i32> {
        let pid = self.pid.ok_or_else(invalid_input)?;
        loop {
            let mut status = 0;
            let result = unsafe { libc::waitpid(pid, &mut status, 0) };
            if result == pid {
                self.pid = None;
                return Ok(status);
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }

    fn terminate(&mut self) {
        let Some(pid) = self.pid else {
            return;
        };
        let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
        let _ = self.wait();
    }
}

impl Drop for SpawnedTarget {
    fn drop(&mut self) {
        self.terminate();
    }
}

/// Execute the prepared target in the child created by `fork`.
///
/// # Safety
///
/// This must run only in that child. `execution` must retain its CString
/// storage, both descriptors must refer to the inherited CLOEXEC pipe, and
/// `errno_location` must be the calling thread's Darwin errno slot captured
/// before fork. This function deliberately uses only `close`, `execve`, a
/// plain errno load, `write`, and `_exit` after fork. On exec failure the errno
/// bytes are reported through the pipe; a short write is treated as a failed
/// handshake by the parent.
unsafe fn exec_target_after_fork(
    execution: &PreparedExec,
    reader_fd: i32,
    writer_fd: i32,
    errno_location: *const libc::c_int,
) -> ! {
    let _ = unsafe { libc::close(reader_fd) };
    unsafe {
        libc::execve(
            execution.path.as_ptr(),
            execution.argv.as_ptr(),
            execution.environment.as_ptr(),
        );
    }
    let error = unsafe { errno_location.read() };
    let bytes = error.to_ne_bytes();
    let _ = unsafe { libc::write(writer_fd, bytes.as_ptr().cast(), bytes.len()) };
    unsafe { libc::_exit(125) };
}

struct PreparedExec {
    path: CString,
    argv: PointerVector,
    environment: PointerVector,
}

impl PreparedExec {
    fn new(target: &Path, arguments: &[OsString], environment: &[(OsString, OsString)]) -> io::Result<Self> {
        let path = os_to_cstring(target.as_os_str())?;
        let argv = std::iter::once(target.as_os_str())
            .chain(arguments.iter().map(OsString::as_os_str))
            .map(os_to_cstring)
            .collect::<io::Result<Vec<_>>>()?;
        let environment = environment
            .iter()
            .map(|(key, value)| environment_entry(key, value))
            .collect::<io::Result<Vec<_>>>()?;
        Ok(Self {
            path,
            argv: PointerVector::new(argv),
            environment: PointerVector::new(environment),
        })
    }
}

struct PointerVector {
    _values: Vec<CString>,
    pointers: Box<[*const libc::c_char]>,
}

impl PointerVector {
    fn new(values: Vec<CString>) -> Self {
        let pointers = values
            .iter()
            .map(|value| value.as_ptr())
            .chain(std::iter::once(std::ptr::null()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            _values: values,
            pointers,
        }
    }

    fn as_ptr(&self) -> *const *const libc::c_char {
        self.pointers.as_ptr()
    }
}

fn os_to_cstring(value: &OsStr) -> io::Result<CString> {
    CString::new(value.as_bytes()).map_err(|_| invalid_input())
}

fn environment_entry(key: &OsStr, value: &OsStr) -> io::Result<CString> {
    if key.as_bytes().contains(&b'=') {
        return Err(invalid_input());
    }
    let mut entry = key.as_bytes().to_vec();
    entry.push(b'=');
    entry.extend_from_slice(value.as_bytes());
    CString::new(entry).map_err(|_| invalid_input())
}

fn cloexec_pipe() -> io::Result<(std::fs::File, std::fs::File)> {
    let mut descriptors = [-1_i32; 2];
    if unsafe { libc::pipe(descriptors.as_mut_ptr()) } == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: pipe initialized both owned descriptors on success.
    let reader = unsafe { std::fs::File::from_raw_fd(descriptors[0]) };
    // SAFETY: pipe initialized both owned descriptors on success.
    let writer = unsafe { std::fs::File::from_raw_fd(descriptors[1]) };
    set_cloexec(reader.as_raw_fd())?;
    set_cloexec(writer.as_raw_fd())?;
    Ok((reader, writer))
}

fn read_exec_error(reader: &mut std::fs::File) -> io::Result<Option<i32>> {
    let mut bytes = [0_u8; std::mem::size_of::<i32>()];
    let mut used = 0;
    loop {
        match reader.read(&mut bytes[used..]) {
            Ok(0) if used == 0 => return Ok(None),
            Ok(0) => return Err(io::Error::new(io::ErrorKind::InvalidData, "truncated exec status")),
            Ok(read) => {
                used += read;
                if used == bytes.len() {
                    return Ok(Some(i32::from_ne_bytes(bytes)));
                }
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

fn descriptor_sanitize_limit() -> io::Result<i32> {
    let mut limit: libc::rlimit = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } == -1
        || limit.rlim_cur == libc::RLIM_INFINITY
        || limit.rlim_cur > MAX_SANITIZED_DESCRIPTORS
    {
        return Err(permission_denied());
    }
    i32::try_from(limit.rlim_cur).map_err(|_| permission_denied())
}

fn mark_inherited_descriptors_cloexec(limit: i32) -> io::Result<()> {
    for descriptor in 3..limit {
        let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
        if flags == -1 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EBADF) {
                continue;
            }
            return Err(error);
        }
        if unsafe { libc::fcntl(descriptor, libc::F_SETFD, flags | libc::FD_CLOEXEC) } == -1 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

fn set_cloexec(descriptor: i32) -> io::Result<()> {
    let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
    if flags == -1 || unsafe { libc::fcntl(descriptor, libc::F_SETFD, flags | libc::FD_CLOEXEC) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn mirror_status(status: i32) -> io::Result<()> {
    if libc::WIFEXITED(status) {
        std::process::exit(libc::WEXITSTATUS(status));
    }
    if libc::WIFSIGNALED(status) {
        std::process::exit(128 + libc::WTERMSIG(status));
    }
    Err(io::Error::other("sandbox target returned an unexpected wait status"))
}

fn invalid_input() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, "invalid sandbox helper invocation")
}

fn permission_denied() -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, "sandbox enforcement failed")
}

#[cfg(test)]
#[path = "sandbox_helper_macos_test.rs"]
mod sandbox_helper_macos_test;
