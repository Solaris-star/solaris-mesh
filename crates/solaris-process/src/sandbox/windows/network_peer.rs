use std::ffi::{OsStr, c_void};
use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, Shutdown, SocketAddr, TcpListener, TcpStream};
use std::os::windows::ffi::{OsStrExt, OsStringExt};
use std::path::PathBuf;
use std::ptr::NonNull;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use uuid::Uuid;
use windows_sys::Win32::Foundation::{
    CloseHandle, ERROR_INSUFFICIENT_BUFFER, ERROR_SUCCESS, HANDLE, WAIT_OBJECT_0, WAIT_TIMEOUT,
};
use windows_sys::Win32::Storage::Packaging::Appx::GetPackageFamilyName;
use windows_sys::Win32::System::Com::{
    CLSCTX_LOCAL_SERVER, COINIT_MULTITHREADED, CoCreateInstance, CoInitializeEx, CoUninitialize,
};
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation, SetInformationJobObject,
};
use windows_sys::Win32::System::Threading::{
    OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_TERMINATE, QueryFullProcessImageNameW, TerminateProcess,
    WaitForSingleObject,
};
use windows_sys::core::GUID;

use crate::network_proxy::NetworkProxyPolicy;
use crate::runner::inspect_executable;

const CONTROL_PROTOCOL_VERSION: u32 = 1;
const MAX_IDENTITY_BYTES: u64 = 4 * 1024;
const MAX_CONTROL_LINE_BYTES: usize = 72 * 1024;
const CONNECT_DEADLINE: Duration = Duration::from_secs(5);
const EXIT_DEADLINE_MS: u32 = 2_000;
const SYNCHRONIZE_ACCESS: u32 = 0x0010_0000;
const APPLICATION_ID: &str = "Proxy";
const ACTIVATION_ATTEMPTS: usize = 3;
#[cfg(not(any(test, all(feature = "sandbox-test-fixtures", debug_assertions))))]
const PACKAGED_PROXY_SHA256: Option<&str> = option_env!("SOLARIS_PACKAGED_WINDOWS_NETWORK_PROXY_SHA256");

const CLSID_APPLICATION_ACTIVATION_MANAGER: GUID = GUID::from_u128(0x45ba127d_10a8_46ea_8ab7_56ea9078943c);
const IID_APPLICATION_ACTIVATION_MANAGER: GUID = GUID::from_u128(0x2e941141_7f97_4756_ba1d_9decde894a3d);

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProxyIdentityFile {
    schema_version: u32,
    package_family_name: String,
    application_user_model_id: String,
}

#[derive(Serialize)]
struct ControlHello<'a> {
    protocol: u32,
    nonce: &'a str,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ControlReady {
    protocol: u32,
    nonce: String,
    package_family_name: String,
    application_user_model_id: String,
    proxy_url: String,
    ca_certificate_pem: String,
}

pub(super) struct WindowsNetworkPeer {
    package_family_name: String,
    proxy_url: String,
    ca_certificate_pem: Vec<u8>,
    control: TcpStream,
    process: ProcessHandle,
    _job: KillOnCloseJob,
}

impl WindowsNetworkPeer {
    pub(super) fn start(policy: &NetworkProxyPolicy) -> io::Result<Self> {
        if policy.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "empty Windows network proxy policy",
            ));
        }
        let (expected_binary_digest, identity) = trusted_proxy_identity()?;
        for _ in 0..ACTIVATION_ATTEMPTS {
            let control_port = dynamic_loopback_port()?;
            let proxy_port = dynamic_loopback_port()?;
            if control_port == proxy_port {
                continue;
            }
            let nonce = proxy_nonce();
            let arguments = activation_arguments(control_port, proxy_port, &nonce, policy)?;
            let pid = activate_packaged_proxy(&identity.application_user_model_id, &arguments)?;
            let process = ProcessHandle::open(pid)?;
            if let Err(error) = verify_activated_process(&process, &identity, &expected_binary_digest) {
                process.terminate();
                return Err(error);
            }
            let job = match KillOnCloseJob::assign(process.raw()) {
                Ok(job) => job,
                Err(error) => {
                    process.terminate();
                    return Err(error);
                }
            };
            match connect_control(control_port, &process, &identity, &nonce, proxy_port) {
                Ok((control, ready)) => {
                    return Ok(Self {
                        package_family_name: identity.package_family_name,
                        proxy_url: ready.proxy_url,
                        ca_certificate_pem: ready.ca_certificate_pem.into_bytes(),
                        control,
                        process,
                        _job: job,
                    });
                }
                Err(error) => {
                    process.terminate();
                    if error.kind() != io::ErrorKind::AddrInUse {
                        return Err(error);
                    }
                }
            }
        }
        Err(io::Error::new(
            io::ErrorKind::AddrInUse,
            "unable to reserve packaged proxy control ports",
        ))
    }

    pub(super) fn package_family_name(&self) -> &str {
        &self.package_family_name
    }

    pub(super) fn proxy_url(&self) -> &str {
        &self.proxy_url
    }

    pub(super) fn ca_certificate_pem(&self) -> &[u8] {
        &self.ca_certificate_pem
    }

    pub(super) fn verify_alive(&self) -> io::Result<()> {
        // SAFETY: process is a valid synchronization handle; zero timeout only
        // observes whether the packaged proxy has already exited.
        match unsafe { WaitForSingleObject(self.process.raw(), 0) } {
            WAIT_TIMEOUT => Ok(()),
            WAIT_OBJECT_0 => Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "packaged Windows network proxy exited",
            )),
            _ => Err(io::Error::last_os_error()),
        }
    }

    #[cfg(feature = "sandbox-test-fixtures")]
    pub(super) fn terminate_for_test(&self) {
        self.process.terminate();
    }
}

impl Drop for WindowsNetworkPeer {
    fn drop(&mut self) {
        let _ = self.control.shutdown(Shutdown::Both);
        // SAFETY: process is a valid synchronization handle owned by ProcessHandle.
        let wait = unsafe { WaitForSingleObject(self.process.raw(), EXIT_DEADLINE_MS) };
        if wait == WAIT_TIMEOUT {
            self.process.terminate();
            // SAFETY: same valid process handle; wait is bounded after termination.
            let _ = unsafe { WaitForSingleObject(self.process.raw(), EXIT_DEADLINE_MS) };
        }
    }
}

fn trusted_proxy_identity() -> io::Result<(String, ProxyIdentityFile)> {
    for binary in proxy_binary_candidates() {
        if !binary.is_file() {
            continue;
        }
        let identity_path = binary.with_file_name("solaris-windows-network-proxy.identity.json");
        let metadata = match std::fs::symlink_metadata(&identity_path) {
            Ok(metadata)
                if metadata.is_file()
                    && !metadata.file_type().is_symlink()
                    && metadata.len() <= MAX_IDENTITY_BYTES
                    && crate::sandbox::helper::windows_package_file_is_trusted(&identity_path) =>
            {
                metadata
            }
            _ => continue,
        };
        let _ = metadata;
        if !crate::sandbox::helper::windows_package_file_is_trusted(&binary) {
            continue;
        }
        let bytes = std::fs::read(&identity_path)?;
        let identity: ProxyIdentityFile = serde_json::from_slice(&bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid packaged proxy identity"))?;
        if identity.schema_version != 1
            || identity.package_family_name.is_empty()
            || identity.application_user_model_id != format!("{}!{APPLICATION_ID}", identity.package_family_name)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid packaged proxy identity",
            ));
        }
        let inspected = inspect_executable(&binary)
            .map_err(|error| io::Error::other(format!("inspect packaged proxy: {error}")))?;
        let actual_digest = inspected.content_digest().to_owned();
        #[cfg(any(test, all(feature = "sandbox-test-fixtures", debug_assertions)))]
        let expected_digest = actual_digest.clone();
        #[cfg(not(any(test, all(feature = "sandbox-test-fixtures", debug_assertions))))]
        let expected_digest = packaged_proxy_expected_digest()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "packaged Windows network proxy digest is unavailable",
                )
            })?
            .to_owned();
        if actual_digest != expected_digest {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "packaged Windows network proxy digest does not match the release identity",
            ));
        }
        return Ok((expected_digest, identity));
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        "installed packaged Windows network proxy identity is unavailable",
    ))
}

#[cfg(not(any(test, all(feature = "sandbox-test-fixtures", debug_assertions))))]
fn packaged_proxy_expected_digest() -> Option<&'static str> {
    let digest = PACKAGED_PROXY_SHA256?;
    (digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
    .then_some(digest)
}

fn proxy_binary_candidates() -> Vec<PathBuf> {
    let Some(current) = std::env::current_exe().ok() else {
        return Vec::new();
    };
    let Some(parent) = current.parent() else {
        return Vec::new();
    };
    let name = format!("solaris-windows-network-proxy{}", std::env::consts::EXE_SUFFIX);
    let mut candidates = vec![parent.join(&name)];
    if let Some(root) = parent.parent() {
        candidates.push(root.join(name));
    }
    candidates
}

fn dynamic_loopback_port() -> io::Result<u16> {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    let port = listener.local_addr()?.port();
    if port < 49_152 {
        return Err(io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            "Windows packaged proxy requires a dynamic port",
        ));
    }
    Ok(port)
}

fn proxy_nonce() -> String {
    let first = Uuid::now_v7().as_u128();
    let second = Uuid::now_v7().as_u128();
    format!("{first:032x}{second:032x}")
}

fn activation_arguments(
    control_port: u16,
    proxy_port: u16,
    nonce: &str,
    policy: &NetworkProxyPolicy,
) -> io::Result<String> {
    let mut arguments = format!("--control-port {control_port} --proxy-port {proxy_port} --nonce {nonce}");
    for domain in policy.windows_permission_domains() {
        if domain.chars().any(char::is_whitespace) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid packaged proxy domain",
            ));
        }
        arguments.push_str(" --domain ");
        arguments.push_str(&domain);
    }
    Ok(arguments)
}

#[repr(C)]
struct ApplicationActivationManager {
    vtable: *const ApplicationActivationManagerVtable,
}

#[repr(C)]
struct ApplicationActivationManagerVtable {
    query_interface: unsafe extern "system" fn(*mut ApplicationActivationManager, *const GUID, *mut *mut c_void) -> i32,
    add_ref: unsafe extern "system" fn(*mut ApplicationActivationManager) -> u32,
    release: unsafe extern "system" fn(*mut ApplicationActivationManager) -> u32,
    activate_application:
        unsafe extern "system" fn(*mut ApplicationActivationManager, *const u16, *const u16, u32, *mut u32) -> i32,
    activate_for_file: unsafe extern "system" fn(),
    activate_for_protocol: unsafe extern "system" fn(),
}

fn activate_packaged_proxy(aumid: &str, arguments: &str) -> io::Result<u32> {
    let aumid = aumid.to_owned();
    let arguments = arguments.to_owned();
    std::thread::Builder::new()
        .name("solaris-windows-proxy-activation".to_owned())
        .spawn(move || activate_packaged_proxy_on_com_thread(&aumid, &arguments))
        .map_err(|error| io::Error::other(format!("start packaged proxy activation thread: {error}")))?
        .join()
        .map_err(|_| io::Error::other("packaged proxy activation thread panicked"))?
}

fn activate_packaged_proxy_on_com_thread(aumid: &str, arguments: &str) -> io::Result<u32> {
    // SAFETY: fresh thread has no COM apartment initialized by Solaris.
    let initialized = unsafe { CoInitializeEx(std::ptr::null(), COINIT_MULTITHREADED as u32) };
    hresult("CoInitializeEx", initialized)?;
    struct ComGuard;
    impl Drop for ComGuard {
        fn drop(&mut self) {
            // SAFETY: balances successful CoInitializeEx on this thread.
            unsafe { CoUninitialize() };
        }
    }
    let _com = ComGuard;
    let mut raw = std::ptr::null_mut::<c_void>();
    // SAFETY: CLSID/IID are the Windows ApplicationActivationManager contract
    // and raw is a live out-parameter for the interface pointer.
    let created = unsafe {
        CoCreateInstance(
            &CLSID_APPLICATION_ACTIVATION_MANAGER,
            std::ptr::null_mut(),
            CLSCTX_LOCAL_SERVER,
            &IID_APPLICATION_ACTIVATION_MANAGER,
            &mut raw,
        )
    };
    hresult("CoCreateInstance(ApplicationActivationManager)", created)?;
    let manager = NonNull::new(raw.cast::<ApplicationActivationManager>())
        .ok_or_else(|| io::Error::other("ApplicationActivationManager returned a null interface"))?;
    struct ActivationManager(NonNull<ApplicationActivationManager>);
    impl Drop for ActivationManager {
        fn drop(&mut self) {
            // SAFETY: pointer came from a successful CoCreateInstance call.
            unsafe { ((*self.0.as_ref().vtable).release)(self.0.as_ptr()) };
        }
    }
    let manager = ActivationManager(manager);
    let aumid = wide_nul(OsStr::new(aumid))?;
    let arguments = wide_nul(OsStr::new(arguments))?;
    let mut pid = 0_u32;
    // SAFETY: manager is a live COM interface; both strings are NUL-terminated
    // and pid is a live out-parameter.
    let activated = unsafe {
        ((*manager.0.as_ref().vtable).activate_application)(
            manager.0.as_ptr(),
            aumid.as_ptr(),
            arguments.as_ptr(),
            0,
            &mut pid,
        )
    };
    hresult("IApplicationActivationManager::ActivateApplication", activated)?;
    if pid == 0 {
        return Err(io::Error::other("packaged proxy activation returned no process id"));
    }
    Ok(pid)
}

fn hresult(function: &str, value: i32) -> io::Result<()> {
    if value < 0 {
        Err(io::Error::other(format!(
            "{function} failed with HRESULT 0x{:08X}",
            value as u32
        )))
    } else {
        Ok(())
    }
}

struct ProcessHandle(HANDLE);

// Windows process handles are kernel object references and may be waited on or
// terminated from a different thread than the one that opened them.
unsafe impl Send for ProcessHandle {}

impl ProcessHandle {
    fn open(pid: u32) -> io::Result<Self> {
        // SAFETY: OpenProcess validates pid/access rights; returned handle is owned below.
        let handle = unsafe {
            OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_TERMINATE | SYNCHRONIZE_ACCESS,
                0,
                pid,
            )
        };
        if handle.is_null() {
            Err(io::Error::last_os_error())
        } else {
            Ok(Self(handle))
        }
    }

    fn raw(&self) -> HANDLE {
        self.0
    }

    fn terminate(&self) {
        // SAFETY: valid process handle with PROCESS_TERMINATE access.
        unsafe {
            let _ = TerminateProcess(self.0, 125);
        }
    }
}

impl Drop for ProcessHandle {
    fn drop(&mut self) {
        // SAFETY: this object owns the process handle.
        unsafe { CloseHandle(self.0) };
    }
}

struct KillOnCloseJob(HANDLE);

// Job Object handles are likewise process-independent kernel handles.
unsafe impl Send for KillOnCloseJob {}

impl KillOnCloseJob {
    fn assign(process: HANDLE) -> io::Result<Self> {
        // SAFETY: creating an unnamed job has no pointer lifetime requirements.
        let job = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if job.is_null() {
            return Err(io::Error::last_os_error());
        }
        let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        // SAFETY: job is valid and limits is the documented structure.
        if unsafe {
            SetInformationJobObject(
                job,
                JobObjectExtendedLimitInformation,
                (&raw const limits).cast(),
                u32::try_from(std::mem::size_of_val(&limits))
                    .map_err(|_| io::Error::other("Job Object limit structure is too large"))?,
            )
        } == 0
        {
            // SAFETY: closes the newly created job on failure.
            unsafe { CloseHandle(job) };
            return Err(io::Error::last_os_error());
        }
        // SAFETY: process and job are live handles.
        if unsafe { AssignProcessToJobObject(job, process) } == 0 {
            // SAFETY: closes the newly created job on failure.
            unsafe { CloseHandle(job) };
            return Err(io::Error::last_os_error());
        }
        Ok(Self(job))
    }
}

impl Drop for KillOnCloseJob {
    fn drop(&mut self) {
        // SAFETY: this object owns the job handle; kill-on-close contains descendants.
        unsafe { CloseHandle(self.0) };
    }
}

fn verify_activated_process(
    process: &ProcessHandle,
    identity: &ProxyIdentityFile,
    expected_binary_digest: &str,
) -> io::Result<()> {
    if process_package_family_name(process.raw())? != identity.package_family_name {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "packaged proxy family identity mismatch",
        ));
    }
    let image = process_image_path(process.raw())?;
    let actual = inspect_executable(&image)
        .map_err(|error| io::Error::other(format!("inspect activated packaged proxy: {error}")))?;
    if expected_binary_digest != actual.content_digest() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "packaged proxy executable digest mismatch",
        ));
    }
    Ok(())
}

fn process_package_family_name(process: HANDLE) -> io::Result<String> {
    let mut length = 0_u32;
    // SAFETY: documented sizing call with null output buffer.
    let result = unsafe { GetPackageFamilyName(process, &mut length, std::ptr::null_mut()) };
    if result != ERROR_INSUFFICIENT_BUFFER || length < 2 {
        return Err(io::Error::from_raw_os_error(result as i32));
    }
    let mut buffer = vec![0_u16; length as usize];
    // SAFETY: buffer has `length` writable UTF-16 units.
    let result = unsafe { GetPackageFamilyName(process, &mut length, buffer.as_mut_ptr()) };
    if result != ERROR_SUCCESS || length < 2 || length as usize > buffer.len() {
        return Err(io::Error::from_raw_os_error(result as i32));
    }
    let value = &buffer[..length as usize];
    let value = value.strip_suffix(&[0]).unwrap_or(value);
    String::from_utf16(value).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid package family name"))
}

fn process_image_path(process: HANDLE) -> io::Result<PathBuf> {
    let mut buffer = vec![0_u16; 32_768];
    let mut length = u32::try_from(buffer.len()).map_err(|_| io::Error::other("process image buffer is too large"))?;
    // SAFETY: buffer is writable for length UTF-16 units and process has query access.
    if unsafe { QueryFullProcessImageNameW(process, 0, buffer.as_mut_ptr(), &mut length) } == 0 {
        return Err(io::Error::last_os_error());
    }
    buffer.truncate(length as usize);
    Ok(PathBuf::from(std::ffi::OsString::from_wide(&buffer)))
}

fn connect_control(
    port: u16,
    process: &ProcessHandle,
    identity: &ProxyIdentityFile,
    nonce: &str,
    proxy_port: u16,
) -> io::Result<(TcpStream, ControlReady)> {
    let deadline = Instant::now() + CONNECT_DEADLINE;
    let address = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let mut stream = loop {
        // SAFETY: valid synchronization handle; zero timeout is a state check.
        if unsafe { WaitForSingleObject(process.raw(), 0) } == WAIT_OBJECT_0 {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "packaged proxy exited before handshake",
            ));
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "packaged proxy handshake timed out",
            ));
        }
        match TcpStream::connect_timeout(&address, remaining.min(Duration::from_millis(100))) {
            Ok(stream) => break stream,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::ConnectionRefused | io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                ) =>
            {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => return Err(error),
        }
    };
    stream.set_read_timeout(Some(CONNECT_DEADLINE))?;
    stream.set_write_timeout(Some(CONNECT_DEADLINE))?;
    serde_json::to_writer(
        &mut stream,
        &ControlHello {
            protocol: CONTROL_PROTOCOL_VERSION,
            nonce,
        },
    )
    .map_err(|_| io::Error::other("packaged proxy handshake encoding failed"))?;
    stream.write_all(b"\n")?;
    stream.flush()?;
    let line = read_bounded_line(&mut stream, MAX_CONTROL_LINE_BYTES)?;
    let ready: ControlReady = serde_json::from_slice(&line)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid packaged proxy handshake"))?;
    if ready.protocol != CONTROL_PROTOCOL_VERSION
        || ready.nonce != nonce
        || ready.package_family_name != identity.package_family_name
        || ready.application_user_model_id != identity.application_user_model_id
        || ready.proxy_url != format!("http://127.0.0.1:{proxy_port}")
        || ready.ca_certificate_pem.is_empty()
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "packaged proxy handshake identity mismatch",
        ));
    }
    stream.set_read_timeout(None)?;
    stream.set_write_timeout(None)?;
    Ok((stream, ready))
}

fn read_bounded_line(stream: &mut TcpStream, limit: usize) -> io::Result<Vec<u8>> {
    let mut output = Vec::new();
    let mut byte = [0_u8; 1];
    while output.len() <= limit {
        let read = stream.read(&mut byte)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "packaged proxy closed during handshake",
            ));
        }
        output.push(byte[0]);
        if byte[0] == b'\n' {
            return Ok(output);
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "packaged proxy handshake is too large",
    ))
}

fn wide_nul(value: &OsStr) -> io::Result<Vec<u16>> {
    let mut wide = value.encode_wide().collect::<Vec<_>>();
    if wide.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Windows activation argument contains NUL",
        ));
    }
    wide.push(0);
    Ok(wide)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activation_arguments_use_only_normalized_exact_endpoints() {
        let policy =
            NetworkProxyPolicy::from_permission_domains(["https://Example.COM:8443", "example.org:443"]).unwrap();
        let args = activation_arguments(50_001, 50_002, &"a".repeat(64), &policy).unwrap();
        assert!(args.contains("--domain example.com:8443"));
        assert!(args.contains("--domain example.org:443"));
        assert!(!args.contains("Example.COM"));
    }

    #[test]
    fn proxy_nonce_is_exactly_sixty_four_hex_digits() {
        let nonce = proxy_nonce();
        assert_eq!(nonce.len(), 64);
        assert!(nonce.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }
}
