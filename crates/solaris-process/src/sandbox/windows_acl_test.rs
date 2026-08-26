use super::*;
#[cfg(feature = "sandbox-test-fixtures")]
use crate::{
    ProcessRecoveryKind, ProcessRecoveryState, isolate_process_recoveries_for_test, process_recovery_required,
    retry_process_recovery,
};

#[test]
fn restore_helper_win32_error_is_mapped_to_cleanup_with_source() {
    assert_cleanup_mapping(
        finish_restore_acl(Err(io::Error::from_raw_os_error(5))),
        AclCleanupOperation::Restore,
        5,
    );
}

#[test]
fn revoke_helper_win32_error_is_mapped_to_cleanup_with_source() {
    assert_cleanup_mapping(
        finish_revoke_acl(Err(io::Error::from_raw_os_error(87))),
        AclCleanupOperation::Revoke,
        87,
    );
}

fn assert_cleanup_mapping(result: io::Result<()>, expected_operation: AclCleanupOperation, code: i32) {
    let error = result.expect_err("operation must fail");
    let sandbox = error
        .get_ref()
        .and_then(|source| source.downcast_ref::<SandboxError>())
        .expect("cleanup category");
    let SandboxError::CleanupFailed { source } = sandbox else {
        panic!("expected cleanup failure category");
    };
    let operation = source
        .get_ref()
        .and_then(|source| source.downcast_ref::<AclCleanupOperationError>())
        .expect("operation context");
    assert_eq!(operation.operation, expected_operation);
    assert_eq!(operation.source.raw_os_error(), Some(code));
}

#[test]
#[cfg(feature = "sandbox-test-fixtures")]
fn partial_apply_cleanup_failure_retains_retryable_guard_until_restore() {
    let _recovery_test_guard = isolate_process_recoveries_for_test();
    let root = tempfile::tempdir().unwrap();
    let profile = AppContainerProfile::create().unwrap();
    let sid = copy_sid(profile.sid()).unwrap();
    let mut guard = AclGuard {
        leased: Vec::new(),
        sid,
        fail_cleanup_attempts: 0,
    };
    let root_path = root.path().to_path_buf();
    guard.acquire(root_path.clone(), AclMode::Writable).unwrap();
    let identity = guard.leased[0];
    let next_error = guard
        .acquire(root.path().join("missing-next-object"), AclMode::Writable)
        .expect_err("the next ACL acquisition must fail");
    guard.fail_cleanup_attempts_for_test(2);

    let error = match finish_acl_apply(guard, Err(next_error)) {
        Ok(_) => panic!("partial apply must fail"),
        Err(error) => error,
    };
    let recovery = process_recovery_required(&error).expect("cleanup ownership must be queryable");
    assert_eq!(recovery.kind(), ProcessRecoveryKind::WindowsAcl);
    assert!(acl_leases().lock().unwrap().contains_key(&identity));

    assert_eq!(
        retry_process_recovery(recovery.id()).unwrap(),
        ProcessRecoveryState::Complete
    );
    assert!(!acl_leases().lock().unwrap().contains_key(&identity));
}
