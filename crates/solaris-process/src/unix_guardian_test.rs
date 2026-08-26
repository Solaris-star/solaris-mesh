use std::os::unix::ffi::OsStrExt;

use tokio::process::Command;

use std::os::fd::AsRawFd;

use tokio::io::unix::AsyncFd;

use super::{
    DRAINED, EXEC_ERROR, FINALIZE, GuardianControl, MAX_PLAN_BYTES, PLAN, PROTOCOL_VERSION, encode_plan, recv_packet,
    send_packet, set_nonblocking, socket_pair,
};
use crate::process_outcome_unknown;

fn test_control(parent: std::os::fd::OwnedFd) -> GuardianControl {
    set_nonblocking(parent.as_raw_fd()).unwrap();
    GuardianControl {
        control: Some(AsyncFd::new(parent).unwrap()),
        #[cfg(feature = "sandbox-test-fixtures")]
        release_fixture: None,
        released: false,
        target_exited: false,
        drained: false,
        failed: false,
        finalized: false,
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
