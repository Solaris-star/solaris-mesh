use std::ffi::CString;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::time::Duration;

use tokio::io::unix::AsyncFd;
use tokio::process::{Child, Command};

use crate::recovery::outcome_unknown_error;
use crate::sandbox::trusted_sandbox_helper;

const GUARDIAN_ARGUMENT: &str = "--process-guardian-v2";
const CONTROL_ARGUMENT: &str = "--control-fd";
const PROTOCOL_VERSION: u16 = 2;
const REQUIRE_VERIFIED_DRAIN: u8 = 1;
const MAX_PLAN_BYTES: usize = 256 * 1024;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

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

pub(crate) struct GuardianLaunch {
    control: OwnedFd,
    plan: Vec<u8>,
    #[cfg(feature = "sandbox-test-fixtures")]
    release_fixture: Option<GuardianReleaseFixture>,
}

#[cfg(feature = "sandbox-test-fixtures")]
struct GuardianReleaseFixture {
    accepted_marker: std::path::PathBuf,
    release_gate: std::path::PathBuf,
}

pub(crate) struct GuardianControl {
    control: Option<AsyncFd<OwnedFd>>,
    #[cfg(feature = "sandbox-test-fixtures")]
    release_fixture: Option<GuardianReleaseFixture>,
    released: bool,
    target_exited: bool,
    drained: bool,
    failed: bool,
    finalized: bool,
}

impl GuardianLaunch {
    pub(crate) fn prepare(command: &mut Command, verified_drain: bool) -> io::Result<Self> {
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        if verified_drain {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "strict process containment has no verified Unix drain proof",
            ));
        }
        let helper = trusted_sandbox_helper(None, None)?;
        let helper_file = helper.duplicate_unix_snapshot().map_err(io::Error::other)?;
        let plan = encode_plan(command, verified_drain)?;
        #[cfg(feature = "sandbox-test-fixtures")]
        let release_fixture = release_fixture(command);
        let (parent, child) = socket_pair()?;
        let helper_descriptor = helper_file.as_raw_fd();
        let control_descriptor = child.as_raw_fd();
        let helper_path = executable_fd_path_owned(helper_descriptor);
        let guardian_argument = CString::new(GUARDIAN_ARGUMENT).expect("guardian argument has no NUL");
        let control_argument = CString::new(CONTROL_ARGUMENT).expect("control argument has no NUL");
        let control_value = CString::new(control_descriptor.to_string()).expect("descriptor has no NUL");
        command.process_group(0);
        unsafe {
            command.pre_exec(move || {
                clear_cloexec(helper_file.as_raw_fd())?;
                clear_cloexec(child.as_raw_fd())?;
                let arguments = [
                    helper_path.as_ptr(),
                    guardian_argument.as_ptr(),
                    control_argument.as_ptr(),
                    control_value.as_ptr(),
                    std::ptr::null(),
                ];
                libc::execv(helper_path.as_ptr(), arguments.as_ptr());
                Err(io::Error::last_os_error())
            });
        }
        Ok(Self {
            control: parent,
            plan,
            #[cfg(feature = "sandbox-test-fixtures")]
            release_fixture,
        })
    }

    pub(crate) fn attach_before_release(self, child: &mut Child) -> io::Result<GuardianControl> {
        let child_id = child
            .id()
            .ok_or_else(|| io::Error::other("guardian child has no pid"))?;
        let child_id = libc::pid_t::try_from(child_id).map_err(|_| invalid_input())?;
        if unsafe { libc::getpgid(child_id) } != child_id {
            return Err(io::Error::other("guardian did not enter its dedicated process group"));
        }
        expect_packet(self.control.as_raw_fd(), READY, HANDSHAKE_TIMEOUT)?;
        send_packet(self.control.as_raw_fd(), &self.plan)?;
        expect_packet(self.control.as_raw_fd(), PLAN_ACCEPTED, HANDSHAKE_TIMEOUT)?;
        set_nonblocking(self.control.as_raw_fd())?;
        Ok(GuardianControl {
            control: Some(AsyncFd::new(self.control)?),
            #[cfg(feature = "sandbox-test-fixtures")]
            release_fixture: self.release_fixture,
            released: false,
            target_exited: false,
            drained: false,
            failed: false,
            finalized: false,
        })
    }
}

impl GuardianControl {
    pub(crate) fn release_target(&mut self) -> io::Result<()> {
        #[cfg(feature = "sandbox-test-fixtures")]
        if let Some(fixture) = &self.release_fixture {
            wait_for_fixture_release(fixture)?;
        }
        // Sending RELEASE can succeed even when the response is lost. Record
        // the irreversible boundary before the send so no error path can make
        // a possibly executed target look safe to retry.
        self.released = true;
        let descriptor = self.control_descriptor().map_err(outcome_unknown_error)?;
        if let Err(error) = send_packet(descriptor, &[RELEASE]) {
            self.invalidate_control();
            return Err(outcome_unknown_error(error));
        }
        let response = match recv_one_with_timeout(descriptor, HANDSHAKE_TIMEOUT) {
            Ok(response) => response,
            Err(error) => {
                self.invalidate_control();
                return Err(outcome_unknown_error(error));
            }
        };
        match response {
            EXEC_OK => {}
            EXEC_ERROR => {
                self.invalidate_control();
                return Err(outcome_unknown_error(io::Error::other(
                    "guardian could not start the released target",
                )));
            }
            _ => {
                self.invalidate_control();
                return Err(outcome_unknown_error(invalid_input()));
            }
        }
        Ok(())
    }

    pub(crate) const fn target_was_released(&self) -> bool {
        self.released
    }

    pub(crate) const fn control_is_lost(&self) -> bool {
        self.control.is_none()
    }
}

#[cfg(feature = "sandbox-test-fixtures")]
fn release_fixture(command: &Command) -> Option<GuardianReleaseFixture> {
    let command = command.as_std();
    let value = |key: &str| {
        command
            .get_envs()
            .find(|(candidate, _)| *candidate == std::ffi::OsStr::new(key))
            .and_then(|(_, value)| value)
            .map(std::path::PathBuf::from)
    };
    Some(GuardianReleaseFixture {
        accepted_marker: value("SOLARIS_GUARDIAN_FIXTURE_PLAN_ACCEPTED")?,
        release_gate: value("SOLARIS_GUARDIAN_FIXTURE_RELEASE_GATE")?,
    })
}

#[cfg(feature = "sandbox-test-fixtures")]
fn wait_for_fixture_release(fixture: &GuardianReleaseFixture) -> io::Result<()> {
    std::fs::write(&fixture.accepted_marker, b"accepted")?;
    let deadline = std::time::Instant::now() + HANDSHAKE_TIMEOUT;
    while !fixture.release_gate.is_file() {
        if std::time::Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "guardian test release gate timed out",
            ));
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    Ok(())
}

impl GuardianControl {
    pub(crate) async fn wait_target_exit(&mut self) -> io::Result<()> {
        while !self.target_exited && !self.failed {
            self.consume_available()?;
            if self.target_exited || self.failed {
                break;
            }
            let Some(control) = self.control.as_ref() else {
                return Err(outcome_unknown_error(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "guardian control channel was lost after target release",
                )));
            };
            let mut readiness = match control.readable().await {
                Ok(readiness) => readiness,
                Err(error) => {
                    self.invalidate_control();
                    return Err(outcome_unknown_error(error));
                }
            };
            readiness.clear_ready();
        }
        if self.failed {
            Err(io::Error::other("guardian target execution failed"))
        } else {
            Ok(())
        }
    }

    pub(crate) fn target_exited(&mut self) -> io::Result<bool> {
        self.consume_available()?;
        Ok(self.target_exited || self.failed)
    }

    pub(crate) fn terminate(&mut self) -> io::Result<()> {
        if self.released && !self.drained {
            let Some(descriptor) = self.control.as_ref().map(|control| control.get_ref().as_raw_fd()) else {
                return Ok(());
            };
            if send_packet(descriptor, &[TERMINATE]).is_err() {
                // Dropping the channel is itself a guardian termination
                // request. Recovery must not retry a permanently broken fd.
                self.invalidate_control();
            }
        }
        Ok(())
    }

    pub(crate) fn is_drained(&mut self) -> io::Result<bool> {
        self.consume_available()?;
        Ok(self.drained)
    }

    pub(crate) fn finalize(&mut self) -> io::Result<()> {
        if self.control.is_none() {
            self.finalized = true;
            return Ok(());
        }
        if !self.drained {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "guardian cannot finalize before process-group drain",
            ));
        }
        if !self.finalized {
            let descriptor = self.control_descriptor()?;
            send_packet(descriptor, &[FINALIZE])?;
            self.finalized = true;
        }
        Ok(())
    }

    fn consume_available(&mut self) -> io::Result<()> {
        let Some(descriptor) = self.control.as_ref().map(|control| control.get_ref().as_raw_fd()) else {
            return Ok(());
        };
        loop {
            match recv_packet(descriptor, libc::MSG_DONTWAIT) {
                Ok(packet) if packet.as_slice() == [TARGET_EXITED] => self.target_exited = true,
                Ok(packet) if packet.as_slice() == [DRAINED] => self.drained = true,
                Ok(packet) if packet.as_slice() == [EXEC_ERROR] => {
                    self.failed = true;
                    self.invalidate_control();
                    return Err(outcome_unknown_error(io::Error::other(
                        "guardian reported target execution failure after release",
                    )));
                }
                Ok(_) => {
                    self.invalidate_control();
                    return Err(outcome_unknown_error(invalid_input()));
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(error) => {
                    self.invalidate_control();
                    return Err(outcome_unknown_error(error));
                }
            }
        }
    }

    fn control_descriptor(&self) -> io::Result<RawFd> {
        self.control
            .as_ref()
            .map(|control| control.get_ref().as_raw_fd())
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "guardian control channel is closed"))
    }

    fn invalidate_control(&mut self) {
        self.control.take();
    }
}

fn encode_plan(command: &Command, verified_drain: bool) -> io::Result<Vec<u8>> {
    let command = command.as_std();
    let values = std::iter::once(command.get_program())
        .chain(command.get_args())
        .collect::<Vec<_>>();
    let count = u32::try_from(values.len()).map_err(|_| invalid_input())?;
    let mut plan = Vec::new();
    plan.push(PLAN);
    plan.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
    plan.push(u8::from(verified_drain) * REQUIRE_VERIFIED_DRAIN);
    plan.extend_from_slice(&count.to_be_bytes());
    for value in values {
        let bytes = value.as_bytes();
        if bytes.contains(&0) {
            return Err(invalid_input());
        }
        let length = u32::try_from(bytes.len()).map_err(|_| invalid_input())?;
        plan.extend_from_slice(&length.to_be_bytes());
        plan.extend_from_slice(bytes);
        if plan.len() > MAX_PLAN_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "guardian plan is too large",
            ));
        }
    }
    Ok(plan)
}

fn socket_pair() -> io::Result<(OwnedFd, OwnedFd)> {
    let mut descriptors = [-1; 2];
    #[cfg(target_os = "linux")]
    let socket_kind = libc::SOCK_SEQPACKET | libc::SOCK_CLOEXEC;
    #[cfg(not(target_os = "linux"))]
    let socket_kind = libc::SOCK_SEQPACKET;
    let result = unsafe { libc::socketpair(libc::AF_UNIX, socket_kind, 0, descriptors.as_mut_ptr()) };
    if result == -1 {
        return Err(io::Error::last_os_error());
    }
    let pair = unsafe {
        (
            OwnedFd::from_raw_fd(descriptors[0]),
            OwnedFd::from_raw_fd(descriptors[1]),
        )
    };
    set_cloexec(pair.0.as_raw_fd())?;
    set_cloexec(pair.1.as_raw_fd())?;
    Ok(pair)
}

fn expect_packet(descriptor: RawFd, expected: u8, timeout: Duration) -> io::Result<()> {
    if recv_one_with_timeout(descriptor, timeout)? == expected {
        Ok(())
    } else {
        Err(invalid_input())
    }
}

fn recv_one_with_timeout(descriptor: RawFd, timeout: Duration) -> io::Result<u8> {
    let timeout = i32::try_from(timeout.as_millis()).unwrap_or(i32::MAX);
    let mut poll = libc::pollfd {
        fd: descriptor,
        events: libc::POLLIN,
        revents: 0,
    };
    loop {
        let result = unsafe { libc::poll(&mut poll, 1, timeout) };
        if result == 1 {
            let packet = recv_packet(descriptor, 0)?;
            return if packet.len() == 1 {
                Ok(packet[0])
            } else {
                Err(invalid_input())
            };
        }
        if result == 0 {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "guardian handshake timed out"));
        }
        if io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
            return Err(io::Error::last_os_error());
        }
    }
}

fn recv_packet(descriptor: RawFd, flags: libc::c_int) -> io::Result<Vec<u8>> {
    let mut packet = vec![0; MAX_PLAN_BYTES];
    loop {
        let read = unsafe { libc::recv(descriptor, packet.as_mut_ptr().cast(), packet.len(), flags) };
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

fn executable_fd_path_owned(descriptor: RawFd) -> CString {
    #[cfg(target_os = "linux")]
    let path = format!("/proc/self/fd/{descriptor}");
    #[cfg(not(target_os = "linux"))]
    let path = format!("/dev/fd/{descriptor}");
    CString::new(path).expect("fd path has no NUL")
}

fn clear_cloexec(descriptor: RawFd) -> io::Result<()> {
    let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
    if flags == -1 || unsafe { libc::fcntl(descriptor, libc::F_SETFD, flags & !libc::FD_CLOEXEC) } == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
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

fn set_nonblocking(descriptor: RawFd) -> io::Result<()> {
    let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFL) };
    if flags == -1 || unsafe { libc::fcntl(descriptor, libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn invalid_input() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, "invalid Unix guardian protocol")
}

#[cfg(test)]
#[path = "unix_guardian_test.rs"]
mod unix_guardian_test;
