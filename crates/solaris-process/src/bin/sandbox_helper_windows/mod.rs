use std::ffi::{OsStr, OsString};
use std::io::{self, Write};
use std::os::windows::ffi::OsStrExt;
use std::path::PathBuf;

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
    let mut attributes = AttributeList::new(sid.get()).map_err(|error| stage("attributes", error))?;
    let mut startup = STARTUPINFOEXW::default();
    configure_standard_handles(&mut startup.StartupInfo)?;
    startup.StartupInfo.cb = u32::try_from(std::mem::size_of::<STARTUPINFOEXW>()).map_err(|_| invalid_input())?;
    startup.lpAttributeList = attributes.get();
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

fn target_exit_code(process: windows_sys::Win32::Foundation::HANDLE) -> io::Result<u32> {
    let mut exit_code = 125_u32;
    if unsafe { GetExitCodeProcess(process, &mut exit_code) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(exit_code)
}

struct Invocation {
    sid: OsString,
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
        require_flag(arguments.next(), "--status")?;
        let status = arguments.next().map(PathBuf::from).ok_or_else(invalid_input)?;
        require_flag(arguments.next(), "--current-dir")?;
        let current_dir = arguments.next().map(PathBuf::from).ok_or_else(invalid_input)?;
        require_flag(arguments.next(), "--")?;
        let target = arguments.next().map(PathBuf::from).ok_or_else(invalid_input)?;
        if !status.is_absolute() || !current_dir.is_absolute() || !target.is_absolute() {
            return Err(invalid_input());
        }
        Ok(Self {
            sid,
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
}

impl AttributeList {
    fn new(sid: PSID) -> io::Result<Self> {
        let mut bytes = 0_usize;
        unsafe {
            InitializeProcThreadAttributeList(std::ptr::null_mut(), 1, 0, &mut bytes);
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
        };
        if unsafe { InitializeProcThreadAttributeList(this.get(), 1, 0, &mut bytes) } == 0 {
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
    let mut file = std::fs::OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(b"full\n")?;
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
