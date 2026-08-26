use std::ffi::{OsStr, OsString};
use std::io;
use std::os::fd::RawFd;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::process::CommandExt;
use std::process::{Command, ExitStatus};
use std::time::Duration;

const GUARDIAN_ARGUMENT: &str = "--process-guardian-v2";
const CONTROL_ARGUMENT: &str = "--control-fd";
const PROTOCOL_VERSION: u16 = 2;
const REQUIRE_VERIFIED_DRAIN: u8 = 1;
const MAX_PLAN_BYTES: usize = 256 * 1024;

const READY: u8 = b'R';
const PLAN: u8 = b'P';
const PLAN_ACCEPTED: u8 = b'A';
const RELEASE: u8 = b'L';
const EXEC_OK: u8 = b'O';
const EXEC_ERROR: u8 = b'E';
const TERMINATE: u8 = b'T';
const TARGET_EXITED: u8 = b'X';
const DRAINED: u8 = b'D';
const FINALIZE: u8 = b'F';

pub(super) fn matches(arguments: &[OsString]) -> bool {
    arguments.get(1).is_some_and(|argument| argument == GUARDIAN_ARGUMENT)
}

pub(super) fn run(arguments: Vec<OsString>) -> io::Result<()> {
    let control = parse_control_descriptor(arguments.into_iter().skip(2))?;
    set_cloexec(control)?;
    send_packet(control, &[READY])?;
    let plan = recv_packet(control)?;
    let plan = decode_plan(&plan)?;
    send_packet(control, &[PLAN_ACCEPTED])?;
    if recv_one(control)? != RELEASE {
        return Err(permission_denied("guardian launch was not released"));
    }

    #[cfg(feature = "sandbox-test-fixtures")]
    if let Some(delay) = injected_exec_error_delay() {
        send_packet(control, &[EXEC_ERROR])?;
        std::thread::sleep(delay);
        return Err(io::Error::other("injected guardian target execution failure"));
    }

    let target_descriptors = inherited_target_descriptors(control)?;
    let mut command = Command::new(plan.target);
    command.args(plan.arguments).process_group(0);
    #[cfg(target_os = "linux")]
    {
        let guardian_pid = unsafe { libc::getpid() };
        unsafe {
            command.pre_exec(move || {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) == -1 {
                    return Err(io::Error::last_os_error());
                }
                if libc::getppid() != guardian_pid {
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "guardian exited before target exec",
                    ));
                }
                Ok(())
            });
        }
    }
    let child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            let _ = send_packet(control, &[EXEC_ERROR]);
            return Err(error);
        }
    };
    if let Err(error) = close_target_descriptors(&target_descriptors) {
        let _ = terminate_and_reap_target(child, plan.verified_drain);
        let _ = send_packet(control, &[EXEC_ERROR]);
        return Err(error);
    }
    if let Err(error) = send_packet(control, &[EXEC_OK]) {
        let _ = terminate_and_reap_target(child, plan.verified_drain);
        return Err(error);
    }
    supervise_target(control, child, plan.verified_drain).and_then(mirror_status)
}

#[cfg(feature = "sandbox-test-fixtures")]
fn injected_exec_error_delay() -> Option<Duration> {
    std::env::var("SOLARIS_GUARDIAN_FIXTURE_EXEC_ERROR_DELAY_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
}

#[allow(clippy::zombie_processes)]
fn supervise_target(control: RawFd, child: std::process::Child, verified_drain: bool) -> io::Result<ExitStatus> {
    let pid = libc::pid_t::try_from(child.id()).map_err(|_| invalid_input())?;
    std::mem::forget(child);
    match supervise_target_inner(control, pid, verified_drain) {
        Ok(status) => Ok(status),
        Err(primary) => {
            if let Err(cleanup) = terminate_and_reap_target_pid(pid, verified_drain) {
                return Err(io::Error::other(format!(
                    "guardian supervision failed ({primary}); target cleanup also failed ({cleanup})"
                )));
            }
            Err(primary)
        }
    }
}

fn supervise_target_inner(control: RawFd, pid: libc::pid_t, verified_drain: bool) -> io::Result<ExitStatus> {
    let mut target_status = None;
    let mut exit_reported = false;
    let mut terminate_requested = false;
    let mut drain_reported = false;

    loop {
        if target_status.is_none() {
            target_status = target_wait_status(pid)?;
        }
        if target_status.is_some() && !exit_reported {
            send_packet(control, &[TARGET_EXITED])?;
            exit_reported = true;
        }
        if terminate_requested
            && target_status.is_some()
            && !drain_reported
            && target_group_is_drained(pid, verified_drain)?
        {
            send_packet(control, &[DRAINED])?;
            drain_reported = true;
        }

        let packet = match poll_packet(control, Duration::from_millis(10)) {
            Ok(packet) => packet,
            Err(error) if error.kind() == io::ErrorKind::BrokenPipe => {
                return terminate_and_reap_target_pid(pid, verified_drain);
            }
            Err(error) => return Err(error),
        };
        match packet {
            Some(packet) if packet.as_slice() == [TERMINATE] => {
                terminate_requested = true;
                signal_target_group(pid)?;
            }
            Some(packet) if packet.as_slice() == [FINALIZE] && drain_reported => {
                return reap_target(pid);
            }
            Some(packet) if packet.as_slice() == [FINALIZE] => {
                return Err(permission_denied("guardian finalized before process tree drain"));
            }
            Some(_) => return Err(invalid_input()),
            None => {}
        }
    }
}

#[allow(clippy::zombie_processes)]
fn terminate_and_reap_target(child: std::process::Child, verified_drain: bool) -> io::Result<ExitStatus> {
    let pid = libc::pid_t::try_from(child.id()).map_err(|_| invalid_input())?;
    std::mem::forget(child);
    terminate_and_reap_target_pid(pid, verified_drain)
}

fn terminate_and_reap_target_pid(pid: libc::pid_t, verified_drain: bool) -> io::Result<ExitStatus> {
    signal_target_group(pid)?;
    while target_wait_status(pid)?.is_none() || !target_group_is_drained(pid, verified_drain)? {
        std::thread::sleep(Duration::from_millis(10));
    }
    reap_target(pid)
}

fn inherited_target_descriptors(control: RawFd) -> io::Result<Vec<RawFd>> {
    let directory = std::fs::read_dir("/proc/self/fd").or_else(|_| std::fs::read_dir("/dev/fd"))?;
    let mut descriptors = Vec::new();
    for entry in directory {
        let descriptor = entry?
            .file_name()
            .to_str()
            .and_then(|value| value.parse::<RawFd>().ok());
        let Some(descriptor) = descriptor.filter(|descriptor| *descriptor > 2 && *descriptor != control) else {
            continue;
        };
        let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
        if flags >= 0 && flags & libc::FD_CLOEXEC == 0 {
            descriptors.push(descriptor);
        } else if flags == -1 && io::Error::last_os_error().raw_os_error() != Some(libc::EBADF) {
            return Err(io::Error::last_os_error());
        }
    }
    descriptors.sort_unstable();
    descriptors.dedup();
    Ok(descriptors)
}

fn close_target_descriptors(descriptors: &[RawFd]) -> io::Result<()> {
    let mut first_error = None;
    for descriptor in descriptors {
        if unsafe { libc::close(*descriptor) } == -1 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::EBADF) && first_error.is_none() {
                first_error = Some(error);
            }
        }
    }
    first_error.map_or(Ok(()), Err)
}

#[derive(Debug)]
struct TargetPlan {
    target: OsString,
    arguments: Vec<OsString>,
    verified_drain: bool,
}

fn decode_plan(packet: &[u8]) -> io::Result<TargetPlan> {
    if packet.first() != Some(&PLAN) || packet.len() > MAX_PLAN_BYTES || packet.len() < 8 {
        return Err(invalid_input());
    }
    let version = u16::from_be_bytes([packet[1], packet[2]]);
    if version != PROTOCOL_VERSION {
        return Err(invalid_input());
    }
    let flags = packet[3];
    if flags & !REQUIRE_VERIFIED_DRAIN != 0 {
        return Err(invalid_input());
    }
    let count = u32::from_be_bytes([packet[4], packet[5], packet[6], packet[7]]);
    let count = usize::try_from(count).map_err(|_| invalid_input())?;
    if count == 0 || count > 16_384 {
        return Err(invalid_input());
    }
    let mut cursor = 8;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let length = read_u32(packet, &mut cursor)?;
        let length = usize::try_from(length).map_err(|_| invalid_input())?;
        let end = cursor.checked_add(length).ok_or_else(invalid_input)?;
        let bytes = packet.get(cursor..end).ok_or_else(invalid_input)?;
        if bytes.contains(&0) {
            return Err(invalid_input());
        }
        values.push(OsString::from_vec(bytes.to_vec()));
        cursor = end;
    }
    if cursor != packet.len() {
        return Err(invalid_input());
    }
    let target = values.remove(0);
    Ok(TargetPlan {
        target,
        arguments: values,
        verified_drain: flags & REQUIRE_VERIFIED_DRAIN != 0,
    })
}

fn read_u32(packet: &[u8], cursor: &mut usize) -> io::Result<u32> {
    let end = cursor.checked_add(4).ok_or_else(invalid_input)?;
    let bytes: [u8; 4] = packet
        .get(*cursor..end)
        .ok_or_else(invalid_input)?
        .try_into()
        .map_err(|_| invalid_input())?;
    *cursor = end;
    Ok(u32::from_be_bytes(bytes))
}

fn target_wait_status(pid: libc::pid_t) -> io::Result<Option<i32>> {
    let mut info = std::mem::MaybeUninit::<libc::siginfo_t>::zeroed();
    let result = unsafe {
        libc::waitid(
            libc::P_PID,
            pid as libc::id_t,
            info.as_mut_ptr(),
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    };
    if result == -1 {
        return Err(io::Error::last_os_error());
    }
    let info = unsafe { info.assume_init() };
    if unsafe { info.si_pid() } == 0 {
        return Ok(None);
    }
    Ok(Some(status_from_siginfo(&info)))
}

fn status_from_siginfo(info: &libc::siginfo_t) -> i32 {
    let status = unsafe { info.si_status() };
    match info.si_code {
        libc::CLD_EXITED => status << 8,
        libc::CLD_DUMPED => (status & 0x7f) | 0x80,
        _ => status & 0x7f,
    }
}

fn reap_target(pid: libc::pid_t) -> io::Result<ExitStatus> {
    let mut status = 0;
    loop {
        let result = unsafe { libc::waitpid(pid, &mut status, 0) };
        if result == pid {
            use std::os::unix::process::ExitStatusExt;

            return Ok(ExitStatus::from_raw(status));
        }
        if result == -1 && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
            continue;
        }
        return Err(io::Error::last_os_error());
    }
}

fn signal_target_group(pid: libc::pid_t) -> io::Result<()> {
    if pid <= 1 {
        return Err(invalid_input());
    }
    let result = unsafe { libc::kill(-pid, libc::SIGKILL) };
    if result == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(error)
    }
}

#[cfg(target_os = "linux")]
fn target_group_is_drained(pid: libc::pid_t, _verified_drain: bool) -> io::Result<bool> {
    for entry in std::fs::read_dir("/proc")? {
        let entry = entry?;
        let Some(candidate) = entry
            .file_name()
            .to_str()
            .and_then(|value| value.parse::<libc::pid_t>().ok())
        else {
            continue;
        };
        if candidate == pid {
            continue;
        }
        match process_group(candidate) {
            Ok(group) if group == pid => return Ok(false),
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(true)
}

#[cfg(target_os = "linux")]
fn process_group(pid: libc::pid_t) -> io::Result<libc::pid_t> {
    let value = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let end = value.rfind(')').ok_or_else(invalid_input)?;
    value[end + 1..]
        .split_whitespace()
        .nth(2)
        .ok_or_else(invalid_input)?
        .parse()
        .map_err(|_| invalid_input())
}

#[cfg(target_os = "macos")]
fn target_group_is_drained(pid: libc::pid_t, _verified_drain: bool) -> io::Result<bool> {
    const PROC_PGRP_ONLY: u32 = 2;
    #[link(name = "proc")]
    unsafe extern "C" {
        fn proc_listpids(kind: u32, type_info: u32, buffer: *mut libc::c_void, buffer_size: i32) -> i32;
    }
    let bytes = unsafe { proc_listpids(PROC_PGRP_ONLY, pid as u32, std::ptr::null_mut(), 0) };
    if bytes < 0 {
        return Err(io::Error::last_os_error());
    }
    let slots = usize::try_from(bytes).map_err(|_| invalid_input())? / std::mem::size_of::<libc::pid_t>() + 8;
    let mut pids = vec![0; slots];
    let capacity = i32::try_from(std::mem::size_of_val(pids.as_slice())).map_err(|_| invalid_input())?;
    let bytes = unsafe { proc_listpids(PROC_PGRP_ONLY, pid as u32, pids.as_mut_ptr().cast(), capacity) };
    if bytes < 0 {
        return Err(io::Error::last_os_error());
    }
    let count = usize::try_from(bytes).map_err(|_| invalid_input())? / std::mem::size_of::<libc::pid_t>();
    Ok(pids[..count]
        .iter()
        .all(|candidate| *candidate == 0 || *candidate == pid))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn target_group_is_drained(_pid: libc::pid_t, verified_drain: bool) -> io::Result<bool> {
    if verified_drain {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "guardian cannot prove process-group drain on this platform",
        ))
    } else {
        Ok(true)
    }
}

fn poll_packet(descriptor: RawFd, timeout: Duration) -> io::Result<Option<Vec<u8>>> {
    let timeout = i32::try_from(timeout.as_millis()).unwrap_or(i32::MAX);
    let mut poll = libc::pollfd {
        fd: descriptor,
        events: libc::POLLIN,
        revents: 0,
    };
    loop {
        let result = unsafe { libc::poll(&mut poll, 1, timeout) };
        if result == 0 {
            return Ok(None);
        }
        if result == 1 {
            return recv_packet(descriptor).map(Some);
        }
        if io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
            return Err(io::Error::last_os_error());
        }
    }
}

fn recv_one(descriptor: RawFd) -> io::Result<u8> {
    let packet = recv_packet(descriptor)?;
    if packet.len() == 1 {
        Ok(packet[0])
    } else {
        Err(invalid_input())
    }
}

fn recv_packet(descriptor: RawFd) -> io::Result<Vec<u8>> {
    let mut packet = vec![0; MAX_PLAN_BYTES];
    loop {
        let read = unsafe { libc::recv(descriptor, packet.as_mut_ptr().cast(), packet.len(), 0) };
        if read > 0 {
            packet.truncate(usize::try_from(read).map_err(|_| invalid_input())?);
            return Ok(packet);
        }
        if read == -1 && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
            continue;
        }
        return Err(if read == -1 {
            io::Error::last_os_error()
        } else {
            io::Error::new(io::ErrorKind::BrokenPipe, "guardian control channel closed")
        });
    }
}

fn send_packet(descriptor: RawFd, packet: &[u8]) -> io::Result<()> {
    loop {
        let written = unsafe { libc::send(descriptor, packet.as_ptr().cast(), packet.len(), 0) };
        if written == packet.len() as isize {
            return Ok(());
        }
        if written == -1 && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
            continue;
        }
        return Err(if written == -1 {
            io::Error::last_os_error()
        } else {
            io::Error::new(io::ErrorKind::WriteZero, "guardian packet write was incomplete")
        });
    }
}

fn set_cloexec(descriptor: RawFd) -> io::Result<()> {
    let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
    if flags == -1 || unsafe { libc::fcntl(descriptor, libc::F_SETFD, flags | libc::FD_CLOEXEC) } == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn parse_control_descriptor(mut arguments: impl Iterator<Item = OsString>) -> io::Result<RawFd> {
    if arguments.next().as_deref() != Some(OsStr::new(CONTROL_ARGUMENT)) {
        return Err(invalid_input());
    }
    let descriptor = arguments
        .next()
        .and_then(|value| value.to_str().and_then(|value| value.parse().ok()))
        .filter(|descriptor| *descriptor >= 3)
        .ok_or_else(invalid_input)?;
    if arguments.next().is_some() {
        return Err(invalid_input());
    }
    Ok(descriptor)
}

fn mirror_status(status: ExitStatus) -> io::Result<()> {
    if let Some(code) = status.code() {
        std::process::exit(code);
    }
    use std::os::unix::process::ExitStatusExt;

    let signal = status.signal().unwrap_or(libc::SIGABRT);
    unsafe {
        libc::signal(signal, libc::SIG_DFL);
        libc::raise(signal);
        libc::_exit(128 + signal);
    }
}

fn invalid_input() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, "invalid Unix guardian invocation")
}

fn permission_denied(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message)
}

#[cfg(test)]
#[path = "sandbox_helper_unix_guardian_test.rs"]
mod sandbox_helper_unix_guardian_test;
