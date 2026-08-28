use std::ffi::{OsStr, OsString};
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::os::windows::ffi::OsStrExt;
use std::path::PathBuf;
use std::time::Duration;

use sha2::{Digest, Sha256};

use windows_sys::Win32::Foundation::{CloseHandle, LocalFree, TRUE, WAIT_OBJECT_0, WAIT_TIMEOUT};
use windows_sys::Win32::Security::Authorization::ConvertStringSidToSidW;
use windows_sys::Win32::Security::{
    EqualSid, GetTokenInformation, PSID, SECURITY_CAPABILITIES, TOKEN_QUERY, TokenIsAppContainer,
};
use windows_sys::Win32::System::Console::{GetStdHandle, STD_ERROR_HANDLE, STD_INPUT_HANDLE, STD_OUTPUT_HANDLE};
use windows_sys::Win32::System::JobObjects::IsProcessInJob;
use windows_sys::Win32::System::Threading::{
    CREATE_SUSPENDED, CREATE_UNICODE_ENVIRONMENT, CreateProcessW, DeleteProcThreadAttributeList,
    EXTENDED_STARTUPINFO_PRESENT, GetExitCodeProcess, InitializeProcThreadAttributeList, OpenProcessToken,
    PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES, PROCESS_INFORMATION, ResumeThread, STARTF_USESTDHANDLES,
    STARTUPINFOEXW, TerminateProcess, UpdateProcThreadAttribute, WaitForSingleObject,
};

const TARGET_START_GRACE_MS: u32 = 100;
const NETWORK_PROOF_TIMEOUT_MS: u32 = 15_000;
const NETWORK_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const NETWORK_FAILURE_MARKER: &[u8] = b"network-unavailable\n";

pub(super) fn run() -> io::Result<()> {
    let invocation = Invocation::parse(std::env::args_os().skip(1)).map_err(|error| stage("parse", error))?;
    let sid = LocalSid::parse(&invocation.sid).map_err(|error| stage("sid", error))?;
    let target_path = conventional_path(&invocation.target);
    let current_dir_path = conventional_path(&invocation.current_dir);
    let mut command_line =
        command_line(target_path.as_os_str(), &invocation.target_args).map_err(|error| stage("command-line", error))?;
    let mut environment = environment_block().map_err(|error| stage("environment", error))?;
    let current_dir = wide_nul(current_dir_path.as_os_str()).map_err(|error| stage("current-dir", error))?;
    let target = wide_nul(target_path.as_os_str()).map_err(|error| stage("target", error))?;
    let security_environment = invocation
        .security_environment
        .as_ref()
        .map(|(path, digest)| load_security_environment(path, digest))
        .transpose()
        .map_err(|error| stage("security-environment", error))?;
    let mut attributes = AttributeList::new(
        sid.get(),
        security_environment
            .as_ref()
            .map(crate::windows_psec_runtime::SecurityEnvironment::raw),
    )
    .map_err(|error| stage("attributes", error))?;
    let mut startup = STARTUPINFOEXW::default();
    configure_standard_handles(&mut startup.StartupInfo)?;
    startup.StartupInfo.cb = u32::try_from(std::mem::size_of::<STARTUPINFOEXW>()).map_err(|_| invalid_input())?;
    startup.lpAttributeList = attributes.get();
    if let Some(network_proof) = invocation.network_proof.as_ref()
        && let Err(error) = run_network_preflight(network_proof, sid.get(), &mut attributes, &mut environment)
    {
        publish_marker(&invocation.status, NETWORK_FAILURE_MARKER)
            .map_err(|publish_error| stage("network-proof-publish", publish_error))?;
        return Err(stage("network-proof", error));
    }
    let mut process = PROCESS_INFORMATION::default();
    let created = unsafe {
        CreateProcessW(
            target.as_ptr(),
            command_line.as_mut_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            TRUE,
            CREATE_SUSPENDED | CREATE_UNICODE_ENVIRONMENT | EXTENDED_STARTUPINFO_PRESENT,
            environment.as_mut_ptr().cast(),
            current_dir.as_ptr(),
            &startup.StartupInfo,
            &mut process,
        )
    };
    if created == 0 {
        return Err(stage("create-process", io::Error::last_os_error()));
    }
    let process = ChildProcess::new(process);
    verify_appcontainer(process.process, sid.get()).map_err(|error| stage("verify-token", error))?;
    verify_job_membership(process.process).map_err(|error| stage("verify-job", error))?;
    if unsafe { ResumeThread(process.thread) } == u32::MAX {
        return Err(stage("resume", io::Error::last_os_error()));
    }
    match unsafe { WaitForSingleObject(process.process, TARGET_START_GRACE_MS) } {
        WAIT_OBJECT_0 => {
            let exit_code = target_exit_code(process.process)?;
            if exit_code != 0 {
                return Err(stage(
                    "target-start",
                    io::Error::other(format!(
                        "sandbox target exited during initialization with status 0x{exit_code:08X}"
                    )),
                ));
            }
            publish_status(&invocation.status).map_err(|error| stage("publish", error))?;
            std::process::exit(0);
        }
        WAIT_TIMEOUT => {
            publish_status(&invocation.status).map_err(|error| stage("publish", error))?;
        }
        _ => return Err(stage("startup-wait", io::Error::last_os_error())),
    }
    if unsafe { WaitForSingleObject(process.process, u32::MAX) } != WAIT_OBJECT_0 {
        return Err(io::Error::last_os_error());
    }
    std::process::exit(target_exit_code(process.process)? as i32);
}

pub(super) fn run_network_proof_child() -> io::Result<()> {
    let mut arguments = std::env::args_os().skip(1);
    require_flag(arguments.next(), "--solaris-windows-network-proof")?;
    require_flag(arguments.next(), "--approved-host")?;
    let approved_host = arguments
        .next()
        .and_then(|value| value.into_string().ok())
        .filter(|value| !value.is_empty() && !value.chars().any(char::is_whitespace))
        .ok_or_else(invalid_input)?;
    if arguments.next().is_some() {
        return Err(invalid_input());
    }
    let proxy = std::env::var("HTTP_PROXY").map_err(|_| invalid_input())?;
    let proxy_address = proxy
        .strip_prefix("http://127.0.0.1:")
        .and_then(|port| port.parse::<u16>().ok())
        .filter(|port| *port != 0)
        .map(|port| SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, port)))
        .ok_or_else(invalid_input)?;

    let (approved_status, direct_address) = proxy_connect_proof(proxy_address, &approved_host)?;
    if approved_status != 200 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "approved destination is not reachable through the packaged proxy",
        ));
    }
    let direct_address = direct_address.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "packaged proxy did not report the connected upstream address",
        )
    })?;
    if proxy_connect_proof(proxy_address, "denied.invalid:443")?.0 != 403 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "unapproved destination was not rejected by the packaged proxy",
        ));
    }
    match TcpStream::connect_timeout(&direct_address, Duration::from_secs(2)) {
        Ok(stream) => {
            let _ = stream.shutdown(Shutdown::Both);
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "direct network egress is reachable from the PSEC target",
            ))
        }
        Err(_) => Ok(()),
    }
}

fn proxy_connect_proof(proxy: SocketAddr, authority: &str) -> io::Result<(u16, Option<SocketAddr>)> {
    let mut stream = TcpStream::connect_timeout(&proxy, NETWORK_CONNECT_TIMEOUT)?;
    stream.set_read_timeout(Some(NETWORK_CONNECT_TIMEOUT))?;
    stream.set_write_timeout(Some(NETWORK_CONNECT_TIMEOUT))?;
    write!(
        stream,
        "CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\nX-Solaris-Network-Proof: 1\r\n\r\n"
    )?;
    stream.flush()?;
    let mut response = Vec::with_capacity(256);
    let mut byte = [0_u8; 1];
    while response.len() < 1024 {
        if stream.read(&mut byte)? == 0 {
            break;
        }
        response.push(byte[0]);
        if response.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    let header = std::str::from_utf8(&response)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "proxy returned invalid HTTP"))?;
    let status = header
        .strip_prefix("HTTP/1.1 ")
        .and_then(|value| value.get(..3))
        .and_then(|value| value.parse::<u16>().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "proxy returned invalid HTTP status"))?;
    let upstream = header
        .split("\r\n")
        .find_map(|line| line.strip_prefix("X-Solaris-Upstream-Address: "))
        .map(str::parse::<SocketAddr>)
        .transpose()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "proxy returned invalid upstream address"))?;
    Ok((status, upstream))
}

fn run_network_preflight(
    proof: &NetworkProof,
    sid: PSID,
    attributes: &mut AttributeList,
    environment: &mut [u16],
) -> io::Result<()> {
    let local_appdata = std::env::var_os("SOLARIS_SANDBOX_CONTROL_TARGET_LOCALAPPDATA")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .ok_or_else(invalid_input)?;
    let source = std::env::current_exe()?;
    let proof_path = local_appdata.join(format!("solaris-network-proof-{}.exe", std::process::id()));
    let source_bytes = std::fs::read(&source)?;
    let mut proof_file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&proof_path)?;
    proof_file.write_all(&source_bytes)?;
    proof_file.sync_all()?;
    drop(proof_file);
    struct ProofFile(PathBuf);
    impl Drop for ProofFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    let _proof_file = ProofFile(proof_path.clone());
    if Sha256::digest(std::fs::read(&proof_path)?) != Sha256::digest(&source_bytes) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "network proof executable digest changed after copy",
        ));
    }

    let proof_args = [
        OsString::from("--solaris-windows-network-proof"),
        OsString::from("--approved-host"),
        OsString::from(&proof.approved_host),
    ];
    let conventional = conventional_path(&proof_path);
    let mut command_line = command_line(conventional.as_os_str(), &proof_args)?;
    let target = wide_nul(conventional.as_os_str())?;
    let current_dir = wide_nul(local_appdata.as_os_str())?;
    let mut startup = STARTUPINFOEXW::default();
    configure_standard_handles(&mut startup.StartupInfo)?;
    startup.StartupInfo.cb = u32::try_from(std::mem::size_of::<STARTUPINFOEXW>()).map_err(|_| invalid_input())?;
    startup.lpAttributeList = attributes.get();
    let mut process = PROCESS_INFORMATION::default();
    // SAFETY: command-line/environment/startup buffers remain live through the
    // call; the proof executable is an exact copy of this trusted helper.
    if unsafe {
        CreateProcessW(
            target.as_ptr(),
            command_line.as_mut_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            TRUE,
            CREATE_SUSPENDED | CREATE_UNICODE_ENVIRONMENT | EXTENDED_STARTUPINFO_PRESENT,
            environment.as_mut_ptr().cast(),
            current_dir.as_ptr(),
            &startup.StartupInfo,
            &mut process,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    let process = ChildProcess::new(process);
    verify_appcontainer(process.process, sid)?;
    verify_job_membership(process.process)?;
    if unsafe { ResumeThread(process.thread) } == u32::MAX {
        return Err(io::Error::last_os_error());
    }
    match unsafe { WaitForSingleObject(process.process, NETWORK_PROOF_TIMEOUT_MS) } {
        WAIT_OBJECT_0 => {
            let exit_code = target_exit_code(process.process)?;
            if exit_code != 0 {
                return Err(io::Error::other(format!(
                    "network proof exited with status 0x{exit_code:08X}"
                )));
            }
            Ok(())
        }
        WAIT_TIMEOUT => Err(io::Error::new(io::ErrorKind::TimedOut, "network proof timed out")),
        _ => Err(io::Error::last_os_error()),
    }
}

fn load_security_environment(
    path: &std::path::Path,
    expected_digest: &str,
) -> io::Result<crate::windows_psec_runtime::SecurityEnvironment> {
    if expected_digest.len() != 64 || !expected_digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(invalid_input());
    }
    let specification = std::fs::read(path)?;
    let digest = format!("{:x}", Sha256::digest(&specification));
    if !digest.eq_ignore_ascii_case(expected_digest) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "process security environment digest mismatch",
        ));
    }
    let api = crate::windows_psec_runtime::SecurityEnvironmentApi::load()?;
    let _support_flags = api.query_support()?;
    api.create(&specification)
}

fn target_exit_code(process: windows_sys::Win32::Foundation::HANDLE) -> io::Result<u32> {
    let mut exit_code = 125_u32;
    if unsafe { GetExitCodeProcess(process, &mut exit_code) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(exit_code)
}

#[derive(Debug)]
struct NetworkProof {
    approved_host: String,
}

struct Invocation {
    sid: OsString,
    security_environment: Option<(PathBuf, String)>,
    network_proof: Option<NetworkProof>,
    status: PathBuf,
    current_dir: PathBuf,
    target: PathBuf,
    target_args: Vec<OsString>,
}

impl Invocation {
    fn parse(arguments: impl IntoIterator<Item = OsString>) -> io::Result<Self> {
        let mut arguments = arguments.into_iter();
        require_flag(arguments.next(), "--windows-appcontainer")?;
        let sid = arguments.next().ok_or_else(invalid_input)?;
        let mut next = arguments.next().ok_or_else(invalid_input)?;
        let security_environment = if next == OsStr::new("--security-environment") {
            let path = arguments.next().map(PathBuf::from).ok_or_else(invalid_input)?;
            require_flag(arguments.next(), "--security-environment-sha256")?;
            let digest = arguments
                .next()
                .and_then(|value| value.into_string().ok())
                .ok_or_else(invalid_input)?;
            next = arguments.next().ok_or_else(invalid_input)?;
            Some((path, digest))
        } else {
            None
        };
        let network_proof = if next == OsStr::new("--network-proof-host") {
            let approved_host = arguments
                .next()
                .and_then(|value| value.into_string().ok())
                .filter(|value| !value.is_empty() && !value.chars().any(char::is_whitespace))
                .ok_or_else(invalid_input)?;
            next = arguments.next().ok_or_else(invalid_input)?;
            Some(NetworkProof { approved_host })
        } else {
            None
        };
        if network_proof.is_some() && security_environment.is_none() {
            return Err(invalid_input());
        }
        require_flag(Some(next), "--status")?;
        let status = arguments.next().map(PathBuf::from).ok_or_else(invalid_input)?;
        require_flag(arguments.next(), "--current-dir")?;
        let current_dir = arguments.next().map(PathBuf::from).ok_or_else(invalid_input)?;
        require_flag(arguments.next(), "--")?;
        let target = arguments.next().map(PathBuf::from).ok_or_else(invalid_input)?;
        if !status.is_absolute()
            || !current_dir.is_absolute()
            || !target.is_absolute()
            || security_environment
                .as_ref()
                .is_some_and(|(path, _)| !path.is_absolute())
        {
            return Err(invalid_input());
        }
        Ok(Self {
            sid,
            security_environment,
            network_proof,
            status,
            current_dir,
            target,
            target_args: arguments.collect(),
        })
    }
}

struct LocalSid(PSID);

impl LocalSid {
    fn parse(value: &OsStr) -> io::Result<Self> {
        let value = wide_nul(value)?;
        let mut sid = std::ptr::null_mut();
        if unsafe { ConvertStringSidToSidW(value.as_ptr(), &mut sid) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self(sid))
    }

    fn get(&self) -> PSID {
        self.0
    }
}

impl Drop for LocalSid {
    fn drop(&mut self) {
        unsafe {
            LocalFree(self.0);
        }
    }
}

struct AttributeList {
    storage: Vec<usize>,
    _capabilities: Box<SECURITY_CAPABILITIES>,
    _security_environment: Option<Box<windows_sys::Win32::Foundation::HANDLE>>,
}

impl AttributeList {
    fn new(sid: PSID, security_environment: Option<windows_sys::Win32::Foundation::HANDLE>) -> io::Result<Self> {
        let attribute_count = 1 + u32::from(security_environment.is_some());
        let mut bytes = 0_usize;
        unsafe {
            InitializeProcThreadAttributeList(std::ptr::null_mut(), attribute_count, 0, &mut bytes);
        }
        if bytes == 0 {
            return Err(io::Error::last_os_error());
        }
        let words = bytes.div_ceil(std::mem::size_of::<usize>());
        let mut this = Self {
            storage: vec![0_usize; words],
            _capabilities: Box::new(SECURITY_CAPABILITIES {
                AppContainerSid: sid,
                Capabilities: std::ptr::null_mut(),
                CapabilityCount: 0,
                Reserved: 0,
            }),
            _security_environment: security_environment.map(Box::new),
        };
        if unsafe { InitializeProcThreadAttributeList(this.get(), attribute_count, 0, &mut bytes) } == 0 {
            return Err(io::Error::last_os_error());
        }
        if unsafe {
            UpdateProcThreadAttribute(
                this.get(),
                0,
                PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES as usize,
                (&raw const *this._capabilities).cast(),
                std::mem::size_of::<SECURITY_CAPABILITIES>(),
                std::ptr::null_mut(),
                std::ptr::null(),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        let environment_value = this._security_environment.as_ref().map(|environment| {
            (&**environment as *const windows_sys::Win32::Foundation::HANDLE).cast::<std::ffi::c_void>()
        });
        if let Some(environment_value) = environment_value {
            let attribute_list = this.get();
            if unsafe {
                UpdateProcThreadAttribute(
                    attribute_list,
                    0,
                    crate::windows_psec_runtime::PROC_THREAD_ATTRIBUTE_SECURITY_ENVIRONMENT,
                    environment_value,
                    std::mem::size_of::<windows_sys::Win32::Foundation::HANDLE>(),
                    std::ptr::null_mut(),
                    std::ptr::null(),
                )
            } == 0
            {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(this)
    }

    fn get(&mut self) -> windows_sys::Win32::System::Threading::LPPROC_THREAD_ATTRIBUTE_LIST {
        self.storage.as_mut_ptr().cast()
    }
}

impl Drop for AttributeList {
    fn drop(&mut self) {
        unsafe {
            DeleteProcThreadAttributeList(self.get());
        }
    }
}

struct ChildProcess {
    process: windows_sys::Win32::Foundation::HANDLE,
    thread: windows_sys::Win32::Foundation::HANDLE,
}

impl ChildProcess {
    fn new(process: PROCESS_INFORMATION) -> Self {
        Self {
            process: process.hProcess,
            thread: process.hThread,
        }
    }
}

impl Drop for ChildProcess {
    fn drop(&mut self) {
        unsafe {
            TerminateProcess(self.process, 125);
            CloseHandle(self.thread);
            CloseHandle(self.process);
        }
    }
}

fn verify_appcontainer(process: windows_sys::Win32::Foundation::HANDLE, expected_sid: PSID) -> io::Result<()> {
    let mut token = std::ptr::null_mut();
    if unsafe { OpenProcessToken(process, TOKEN_QUERY, &mut token) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let token = OwnedHandle(token);
    let mut is_appcontainer = 0_u32;
    let mut returned = 0_u32;
    if unsafe {
        GetTokenInformation(
            token.0,
            TokenIsAppContainer,
            (&raw mut is_appcontainer).cast(),
            u32::try_from(std::mem::size_of_val(&is_appcontainer)).map_err(|_| invalid_input())?,
            &mut returned,
        )
    } == 0
        || is_appcontainer != 1
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "target is not an AppContainer",
        ));
    }
    let mut needed = 0_u32;
    unsafe {
        GetTokenInformation(
            token.0,
            windows_sys::Win32::Security::TokenAppContainerSid,
            std::ptr::null_mut(),
            0,
            &mut needed,
        );
    }
    if needed == 0 {
        return Err(io::Error::last_os_error());
    }
    let mut storage = vec![0_u8; needed as usize];
    if unsafe {
        GetTokenInformation(
            token.0,
            windows_sys::Win32::Security::TokenAppContainerSid,
            storage.as_mut_ptr().cast(),
            needed,
            &mut needed,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    let token_sid = unsafe { *(storage.as_ptr() as *const PSID) };
    if token_sid.is_null() || unsafe { EqualSid(token_sid, expected_sid) } == 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "AppContainer SID mismatch",
        ));
    }
    Ok(())
}

fn verify_job_membership(process: windows_sys::Win32::Foundation::HANDLE) -> io::Result<()> {
    let mut in_job = 0;
    if unsafe { IsProcessInJob(process, std::ptr::null_mut(), &mut in_job) } == 0 {
        return Err(io::Error::last_os_error());
    }
    if in_job == 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "target is outside Job containment",
        ));
    }
    Ok(())
}

struct OwnedHandle(windows_sys::Win32::Foundation::HANDLE);

impl Drop for OwnedHandle {
    fn drop(&mut self) {
        unsafe {
            CloseHandle(self.0);
        }
    }
}

fn publish_status(path: &PathBuf) -> io::Result<()> {
    publish_marker(path, b"full\n")
}

fn publish_marker(path: &PathBuf, marker: &[u8]) -> io::Result<()> {
    let mut file = std::fs::OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(marker)?;
    file.sync_all()
}

fn configure_standard_handles(startup: &mut windows_sys::Win32::System::Threading::STARTUPINFOW) -> io::Result<()> {
    startup.cb = u32::try_from(std::mem::size_of_val(startup)).map_err(|_| invalid_input())?;
    startup.dwFlags = STARTF_USESTDHANDLES;
    startup.hStdInput = unsafe { GetStdHandle(STD_INPUT_HANDLE) };
    startup.hStdOutput = unsafe { GetStdHandle(STD_OUTPUT_HANDLE) };
    startup.hStdError = unsafe { GetStdHandle(STD_ERROR_HANDLE) };
    Ok(())
}

fn command_line(target: &OsStr, arguments: &[OsString]) -> io::Result<Vec<u16>> {
    let mut line = Vec::new();
    for argument in std::iter::once(target).chain(arguments.iter().map(OsString::as_os_str)) {
        if !line.is_empty() {
            line.push(b' ' as u16);
        }
        push_quoted_argument(&mut line, argument)?;
    }
    line.push(0);
    Ok(line)
}

fn environment_block() -> io::Result<Vec<u16>> {
    let target_appdata = std::env::var_os("SOLARIS_SANDBOX_CONTROL_TARGET_APPDATA").ok_or_else(invalid_input)?;
    let target_local_appdata =
        std::env::var_os("SOLARIS_SANDBOX_CONTROL_TARGET_LOCALAPPDATA").ok_or_else(invalid_input)?;
    let mut environment = std::env::vars_os()
        .filter(|(key, _)| {
            let key = key.to_string_lossy();
            !key.starts_with("SOLARIS_SANDBOX_CONTROL_")
                && !key.eq_ignore_ascii_case("APPDATA")
                && !key.eq_ignore_ascii_case("LOCALAPPDATA")
        })
        .collect::<Vec<_>>();
    environment.push((OsString::from("APPDATA"), target_appdata));
    environment.push((OsString::from("LOCALAPPDATA"), target_local_appdata));
    environment.sort_by(|left, right| {
        left.0
            .to_string_lossy()
            .to_lowercase()
            .cmp(&right.0.to_string_lossy().to_lowercase())
    });
    let mut block = Vec::new();
    for (key, value) in environment {
        let key = key.encode_wide().collect::<Vec<_>>();
        let value = value.encode_wide().collect::<Vec<_>>();
        if key.is_empty() || key.contains(&0) || key.contains(&(b'=' as u16)) || value.contains(&0) {
            return Err(invalid_input());
        }
        block.extend(key);
        block.push(b'=' as u16);
        block.extend(value);
        block.push(0);
    }
    block.push(0);
    Ok(block)
}

fn push_quoted_argument(line: &mut Vec<u16>, argument: &OsStr) -> io::Result<()> {
    let units = argument.encode_wide().collect::<Vec<_>>();
    if units.contains(&0) {
        return Err(invalid_input());
    }
    line.push(b'"' as u16);
    let mut slashes = 0_usize;
    for unit in units {
        if unit == b'\\' as u16 {
            slashes += 1;
            continue;
        }
        if unit == b'"' as u16 {
            line.extend(std::iter::repeat_n(b'\\' as u16, slashes * 2 + 1));
        } else {
            line.extend(std::iter::repeat_n(b'\\' as u16, slashes));
        }
        slashes = 0;
        line.push(unit);
    }
    line.extend(std::iter::repeat_n(b'\\' as u16, slashes * 2));
    line.push(b'"' as u16);
    Ok(())
}

fn wide_nul(value: &OsStr) -> io::Result<Vec<u16>> {
    let mut wide = value.encode_wide().collect::<Vec<_>>();
    if wide.contains(&0) {
        return Err(invalid_input());
    }
    wide.push(0);
    Ok(wide)
}

fn conventional_path(path: &std::path::Path) -> PathBuf {
    let value = path.as_os_str().to_string_lossy();
    if let Some(rest) = value.strip_prefix(r"\\?\UNC\") {
        PathBuf::from(format!(r"\\{rest}"))
    } else if let Some(rest) = value.strip_prefix(r"\\?\") {
        PathBuf::from(rest)
    } else {
        path.to_path_buf()
    }
}

fn require_flag(value: Option<OsString>, expected: &str) -> io::Result<()> {
    if value.as_deref() == Some(OsStr::new(expected)) {
        Ok(())
    } else {
        Err(invalid_input())
    }
}

fn invalid_input() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, "invalid Windows sandbox helper invocation")
}

fn stage(name: &str, error: io::Error) -> io::Error {
    io::Error::new(error.kind(), format!("{name}: {error}"))
}
