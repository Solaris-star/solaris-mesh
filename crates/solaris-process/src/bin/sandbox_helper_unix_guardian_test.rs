use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;

use super::{
    PLAN, PROTOCOL_VERSION, REQUIRE_VERIFIED_DRAIN, close_target_descriptors, decode_plan,
    inherited_target_descriptors, set_cloexec,
};

#[test]
fn decoder_preserves_non_utf8_values() {
    let values: [&[u8]; 2] = [b"/tmp/target-\xff", b"argument-\xfe"];
    let mut plan = vec![PLAN];
    plan.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
    plan.push(REQUIRE_VERIFIED_DRAIN);
    plan.extend_from_slice(&(values.len() as u32).to_be_bytes());
    for value in values {
        plan.extend_from_slice(&(value.len() as u32).to_be_bytes());
        plan.extend_from_slice(value);
    }

    let decoded = decode_plan(&plan).unwrap();

    assert_eq!(decoded.target.as_bytes(), values[0]);
    assert_eq!(decoded.arguments.len(), 1);
    assert_eq!(decoded.arguments[0].as_bytes(), values[1]);
    assert!(decoded.verified_drain);
}

#[test]
fn decoder_rejects_trailing_or_mismatched_protocol_data() {
    let mut plan = vec![PLAN];
    plan.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
    plan.push(0);
    plan.extend_from_slice(&1_u32.to_be_bytes());
    plan.extend_from_slice(&4_u32.to_be_bytes());
    plan.extend_from_slice(b"true");
    plan.push(0);

    let error = decode_plan(&plan).unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
fn guardian_closes_target_only_descriptors_after_spawn() {
    let mut descriptors = [-1; 2];
    assert_eq!(unsafe { libc::pipe(descriptors.as_mut_ptr()) }, 0);
    let reader = unsafe { OwnedFd::from_raw_fd(descriptors[0]) };
    let writer = unsafe { OwnedFd::from_raw_fd(descriptors[1]) };
    set_cloexec(reader.as_raw_fd()).unwrap();
    let flags = unsafe { libc::fcntl(writer.as_raw_fd(), libc::F_GETFD) };
    assert!(flags >= 0);
    assert_eq!(
        unsafe { libc::fcntl(writer.as_raw_fd(), libc::F_SETFD, flags & !libc::FD_CLOEXEC) },
        0
    );
    let writer = writer.into_raw_fd();

    let inherited = inherited_target_descriptors(-1).unwrap();
    assert!(inherited.contains(&writer));
    close_target_descriptors(&inherited).unwrap();

    let mut byte = 0_u8;
    assert_eq!(
        unsafe { libc::read(reader.as_raw_fd(), (&mut byte as *mut u8).cast(), 1) },
        0
    );
}
