#![cfg(any(target_os = "linux", target_os = "macos"))]

use std::fs::File;
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const TARGET_MARKER_KEY: &str = "SOLARIS_GUARDIAN_PROTOCOL_TARGET_MARKER";
const WATCHDOG: Duration = Duration::from_secs(5);

#[test]
fn guardian_protocol_target_probe() {
    let Some(marker) = std::env::var_os(TARGET_MARKER_KEY) else {
        return;
    };
    let marker = PathBuf::from(marker);
    let temporary = marker.with_extension("tmp");
    std::fs::write(&temporary, std::process::id().to_string()).unwrap();
    std::fs::rename(temporary, marker).unwrap();
    loop {
        std::thread::park();
    }
}

struct Guardian {
    child: Child,
    control: Option<File>,
    target: Option<libc::pid_t>,
}

impl Drop for Guardian {
    fn drop(&mut self) {
        self.control.take();
        if let Some(target) = self.target {
            unsafe { libc::kill(-target, libc::SIGKILL) };
        }
        // Let the guardian reap its target even when an assertion fails.
        let deadline = Instant::now() + WATCHDOG;
        while Instant::now() < deadline {
            if self.child.try_wait().is_ok_and(|status| status.is_some()) {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn control_pair() -> (File, OwnedFd) {
    let mut descriptors = [-1; 2];
    let kind = if cfg!(target_os = "macos") {
        libc::SOCK_STREAM
    } else {
        libc::SOCK_SEQPACKET
    };
    assert_eq!(
        unsafe { libc::socketpair(libc::AF_UNIX, kind, 0, descriptors.as_mut_ptr()) },
        0
    );
    let parent = unsafe { File::from_raw_fd(descriptors[0]) };
    let helper = unsafe { OwnedFd::from_raw_fd(descriptors[1]) };
    for descriptor in [parent.as_raw_fd(), helper.as_raw_fd()] {
        assert_eq!(unsafe { libc::fcntl(descriptor, libc::F_SETFD, libc::FD_CLOEXEC) }, 0);
        let timeout = libc::timeval {
            tv_sec: WATCHDOG.as_secs() as libc::time_t,
            tv_usec: 0,
        };
        for option in [libc::SO_RCVTIMEO, libc::SO_SNDTIMEO] {
            assert_eq!(
                unsafe {
                    libc::setsockopt(
                        descriptor,
                        libc::SOL_SOCKET,
                        option,
                        (&timeout as *const libc::timeval).cast(),
                        std::mem::size_of_val(&timeout) as libc::socklen_t,
                    )
                },
                0
            );
        }
    }
    (parent, helper)
}

fn write_packet(control: &mut File, packet: &[u8]) {
    let bytes = if cfg!(target_os = "macos") {
        let mut frame = (packet.len() as u32).to_be_bytes().to_vec();
        frame.extend_from_slice(packet);
        frame
    } else {
        packet.to_vec()
    };
    control.write_all(&bytes).unwrap();
}

fn expect_status(control: &mut File, expected: u8) {
    if cfg!(target_os = "macos") {
        let mut length = [0; 4];
        control.read_exact(&mut length).unwrap();
        assert_eq!(u32::from_be_bytes(length), 1);
    }
    let mut packet = [0];
    control.read_exact(&mut packet).unwrap();
    assert_eq!(packet, [expected]);
}

fn released_guardian(marker: &std::path::Path) -> Guardian {
    let (control, helper) = control_pair();
    let descriptor = helper.as_raw_fd();
    let mut command = Command::new(env!("CARGO_BIN_EXE_solaris-process-sandbox-helper"));
    command
        .args(["--process-guardian-v2", "--control-fd", &descriptor.to_string()])
        .env(TARGET_MARKER_KEY, marker)
        .stdout(Stdio::null())
        .process_group(0);
    unsafe {
        command.pre_exec(move || {
            if libc::fcntl(descriptor, libc::F_SETFD, 0) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = command.spawn().unwrap();
    drop(helper);
    let mut guardian = Guardian {
        child,
        control: Some(control),
        target: None,
    };
    let control = guardian.control.as_mut().unwrap();
    expect_status(control, b'R');
    let executable = std::env::current_exe().unwrap();
    let values: [&[u8]; 4] = [
        executable.as_os_str().as_bytes(),
        b"--exact",
        b"guardian_protocol_target_probe",
        b"--nocapture",
    ];
    let mut plan = vec![b'P', 0, 2, 1];
    plan.extend_from_slice(&(values.len() as u32).to_be_bytes());
    for value in values {
        plan.extend_from_slice(&(value.len() as u32).to_be_bytes());
        plan.extend_from_slice(value);
    }
    write_packet(control, &plan);
    expect_status(control, b'A');
    write_packet(control, b"L");
    expect_status(control, b'O');
    let deadline = Instant::now() + WATCHDOG;
    let target = loop {
        if let Ok(value) = std::fs::read_to_string(marker) {
            break value.parse::<libc::pid_t>().unwrap();
        }
        assert!(
            guardian.child.try_wait().unwrap().is_none(),
            "guardian exited before target readiness"
        );
        assert!(
            Instant::now() < deadline,
            "guardian target did not start before watchdog"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    assert!(target > 1);
    assert_eq!(unsafe { libc::getpgid(target) }, target);
    assert_eq!(unsafe { libc::kill(target, 0) }, 0);
    guardian.target = Some(target);
    guardian
}

fn assert_failed_and_reaped(mut guardian: Guardian) {
    let deadline = Instant::now() + WATCHDOG;
    let status = loop {
        if let Some(status) = guardian.child.try_wait().unwrap() {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "guardian did not clean up after invalid control data"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    assert!(!status.success(), "malformed control must fail the guardian");
    let target = guardian.target.unwrap();
    for pid in [target, -target] {
        assert_eq!(
            unsafe { libc::kill(pid, 0) },
            -1,
            "target/group must be gone, including zombies"
        );
        assert_eq!(std::io::Error::last_os_error().raw_os_error(), Some(libc::ESRCH));
    }
    guardian.target = None;
}

#[test]
fn malformed_post_release_command_kills_and_reaps_real_target() {
    let directory = tempfile::tempdir().unwrap();
    let mut guardian = released_guardian(&directory.path().join("target"));
    write_packet(guardian.control.as_mut().unwrap(), b"?");
    assert_failed_and_reaped(guardian);
}

#[cfg(target_os = "macos")]
#[test]
fn truncated_post_release_stream_frame_kills_and_reaps_real_target() {
    for fragment in [&[0, 0][..], &[0, 0, 0, 2, b'T'][..]] {
        let directory = tempfile::tempdir().unwrap();
        let mut guardian = released_guardian(&directory.path().join("target"));
        let control = guardian.control.as_mut().unwrap();
        control.write_all(fragment).unwrap();
        assert_eq!(unsafe { libc::shutdown(control.as_raw_fd(), libc::SHUT_WR) }, 0);
        assert_failed_and_reaped(guardian);
    }
}
