use std::os::unix::ffi::OsStrExt;

use tokio::process::Command;

use std::os::fd::AsRawFd;

use tokio::io::unix::AsyncFd;

use super::{
    DRAINED, EXEC_ERROR, FINALIZE, GuardianControl, HANDSHAKE_TIMEOUT, MAX_PLAN_BYTES, PLAN, PROTOCOL_VERSION,
    PacketReader, encode_plan, send_packet, set_nonblocking, socket_pair,
};
use crate::process_outcome_unknown;

fn test_control(parent: std::os::fd::OwnedFd) -> GuardianControl {
    set_nonblocking(parent.as_raw_fd()).unwrap();
    GuardianControl {
        control: Some(AsyncFd::new(parent).unwrap()),
        reader: PacketReader::new(),
        #[cfg(feature = "sandbox-test-fixtures")]
        release_fixture: None,
        released: false,
        target_exited: false,
        drained: false,
        failed: false,
        finalized: false,
    }
}

fn recv_packet(descriptor: std::os::fd::RawFd, flags: libc::c_int) -> std::io::Result<Vec<u8>> {
    if flags == libc::MSG_DONTWAIT {
        PacketReader::new()
            .poll_packet(descriptor, std::time::Duration::ZERO)?
            .ok_or_else(|| std::io::ErrorKind::WouldBlock.into())
    } else {
        PacketReader::new().recv_packet(descriptor, HANDSHAKE_TIMEOUT)
    }
}

#[test]
fn plan_preserves_non_utf8_program_and_arguments() {
    let program = std::ffi::OsStr::from_bytes(b"/tmp/target-\xff");
    let argument = std::ffi::OsStr::from_bytes(b"argument-\xfe");
    let mut command = Command::new(program);
    command.arg(argument);

    let plan = encode_plan(&command, true).unwrap();

    assert_eq!(plan[0], PLAN);
    assert_eq!(&plan[1..3], &PROTOCOL_VERSION.to_be_bytes());
    assert_eq!(plan[3], super::REQUIRE_VERIFIED_DRAIN);
    assert!(
        plan.windows(program.as_bytes().len())
            .any(|bytes| bytes == program.as_bytes())
    );
    assert!(
        plan.windows(argument.as_bytes().len())
            .any(|bytes| bytes == argument.as_bytes())
    );
}

#[test]
fn oversized_plan_is_rejected_before_spawn() {
    let mut command = Command::new("/bin/true");
    command.arg("x".repeat(MAX_PLAN_BYTES));

    let error = encode_plan(&command, false).unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
fn control_sockets_use_platform_transport_nonblocking_and_close_on_exec() {
    let pair = socket_pair().unwrap();
    for descriptor in [pair.0.as_raw_fd(), pair.1.as_raw_fd()] {
        let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
        assert!(flags >= 0);
        assert_ne!(flags & libc::FD_CLOEXEC, 0);
        let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFL) };
        assert!(flags >= 0);
        assert_ne!(flags & libc::O_NONBLOCK, 0);
        #[cfg(target_os = "macos")]
        {
            let mut enabled: libc::c_int = 0;
            let mut length = std::mem::size_of_val(&enabled) as libc::socklen_t;
            assert_eq!(
                unsafe {
                    libc::getsockopt(
                        descriptor,
                        libc::SOL_SOCKET,
                        libc::SO_NOSIGPIPE,
                        (&mut enabled as *mut libc::c_int).cast(),
                        &mut length,
                    )
                },
                0
            );
            assert_eq!(enabled, 1);
        }
        let mut kind: libc::c_int = 0;
        let mut length = std::mem::size_of_val(&kind) as libc::socklen_t;
        assert_eq!(
            unsafe {
                libc::getsockopt(
                    descriptor,
                    libc::SOL_SOCKET,
                    libc::SO_TYPE,
                    (&mut kind as *mut libc::c_int).cast(),
                    &mut length,
                )
            },
            0
        );
        assert_eq!(
            kind,
            if cfg!(target_os = "macos") {
                libc::SOCK_STREAM
            } else {
                libc::SOCK_SEQPACKET
            }
        );
    }
}

#[tokio::test]
async fn finalize_send_failure_closes_channel_before_recovery_can_retry() {
    let (parent, helper) = socket_pair().unwrap();
    drop(helper);
    let mut control = test_control(parent);
    control.released = true;
    control.drained = true;

    assert!(control.finalize().is_err());
    assert!(control.control_is_lost());
    assert!(control.finalize().is_ok());
}

#[tokio::test]
async fn multibyte_control_message_cannot_claim_process_group_drain() {
    let (parent, helper) = socket_pair().unwrap();
    let mut control = test_control(parent);
    control.released = true;
    send_packet(helper.as_raw_fd(), &[DRAINED, super::TARGET_EXITED]).unwrap();

    let error = control.is_drained().unwrap_err();

    assert!(process_outcome_unknown(&error));
    assert!(!control.drained);
    assert!(control.control_is_lost());
}

#[tokio::test]
async fn process_group_drain_does_not_finalize_before_sandbox_proof() {
    let (parent, helper) = socket_pair().unwrap();
    let mut control = test_control(parent);
    control.released = true;
    control.target_exited = true;
    send_packet(helper.as_raw_fd(), &[DRAINED]).unwrap();

    assert!(control.is_drained().unwrap());
    let error = recv_packet(helper.as_raw_fd(), libc::MSG_DONTWAIT).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);

    control.finalize().unwrap();
    assert_eq!(recv_packet(helper.as_raw_fd(), 0).unwrap(), [FINALIZE]);
}

#[tokio::test]
async fn release_send_failure_is_already_released_and_outcome_unknown() {
    let (parent, helper) = socket_pair().unwrap();
    drop(helper);
    let mut control = test_control(parent);

    let error = control.release_target().unwrap_err();

    assert!(control.target_was_released());
    assert!(process_outcome_unknown(&error));
    assert!(control.control_is_lost());
    assert!(control.terminate().is_ok());
    assert!(control.terminate().is_ok());
}

#[tokio::test]
async fn release_recv_eof_is_outcome_unknown_and_stops_using_the_socket() {
    let (parent, helper) = socket_pair().unwrap();
    let helper_thread = std::thread::spawn(move || {
        assert_eq!(recv_packet(helper.as_raw_fd(), 0).unwrap(), [super::RELEASE]);
    });
    let mut control = test_control(parent);

    let error = control.release_target().unwrap_err();
    helper_thread.join().unwrap();

    assert!(control.target_was_released());
    assert!(process_outcome_unknown(&error));
    assert!(control.control_is_lost());
    assert!(control.terminate().is_ok());
}

#[tokio::test]
async fn release_bad_packet_is_outcome_unknown_and_stops_using_the_socket() {
    let (parent, helper) = socket_pair().unwrap();
    let helper_thread = std::thread::spawn(move || {
        assert_eq!(recv_packet(helper.as_raw_fd(), 0).unwrap(), [super::RELEASE]);
        send_packet(helper.as_raw_fd(), b"?").unwrap();
    });
    let mut control = test_control(parent);

    let error = control.release_target().unwrap_err();
    helper_thread.join().unwrap();

    assert!(control.target_was_released());
    assert!(process_outcome_unknown(&error));
    assert!(control.control_is_lost());
    assert!(control.terminate().is_ok());
}

#[tokio::test]
async fn exec_error_is_outcome_unknown_and_stops_using_the_socket() {
    let (parent, helper) = socket_pair().unwrap();
    let helper_thread = std::thread::spawn(move || {
        assert_eq!(recv_packet(helper.as_raw_fd(), 0).unwrap(), [super::RELEASE]);
        send_packet(helper.as_raw_fd(), &[EXEC_ERROR]).unwrap();
    });
    let mut control = test_control(parent);

    let error = control.release_target().unwrap_err();
    helper_thread.join().unwrap();

    assert!(control.target_was_released());
    assert!(process_outcome_unknown(&error));
    assert!(control.control_is_lost());
    assert!(control.terminate().is_ok());
}
