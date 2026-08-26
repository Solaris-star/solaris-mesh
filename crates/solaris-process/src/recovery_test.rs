use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use super::{
    ProcessRecovery, ProcessRecoveryFailureCategory, ProcessRecoveryKind, ProcessRecoveryState,
    drain_process_recoveries, isolate_process_recoveries_for_test, process_recovery_drain_failures,
    register_process_recovery, retry_process_recovery,
};

struct GatedFailureRecovery {
    allow_complete: Arc<AtomicBool>,
    error_kind: io::ErrorKind,
    sensitive_detail: &'static str,
}

impl ProcessRecovery for GatedFailureRecovery {
    fn kind(&self) -> ProcessRecoveryKind {
        ProcessRecoveryKind::StartedChild
    }

    fn retry(&mut self) -> io::Result<bool> {
        if self.allow_complete.load(Ordering::Acquire) {
            return Ok(true);
        }
        Err(io::Error::new(self.error_kind, self.sensitive_detail))
    }
}

#[tokio::test]
async fn drain_timeout_preserves_ids_and_safe_failure_categories() {
    let _recovery_test_guard = isolate_process_recoveries_for_test();
    let allow_complete = Arc::new(AtomicBool::new(false));
    let fixtures = [
        (
            io::ErrorKind::PermissionDenied,
            ProcessRecoveryFailureCategory::Permission,
            "secret-token-permission",
        ),
        (
            io::ErrorKind::BrokenPipe,
            ProcessRecoveryFailureCategory::Process,
            "secret-command-process",
        ),
        (
            io::ErrorKind::Other,
            ProcessRecoveryFailureCategory::System,
            "secret-path-system",
        ),
    ];
    let recoveries = fixtures
        .iter()
        .map(|(error_kind, _, sensitive_detail)| {
            register_process_recovery(GatedFailureRecovery {
                allow_complete: Arc::clone(&allow_complete),
                error_kind: *error_kind,
                sensitive_detail,
            })
        })
        .collect::<Vec<_>>();

    let error = drain_process_recoveries(Duration::from_millis(20)).await.unwrap_err();
    let failures = process_recovery_drain_failures(&error).expect("timeout must remain structured");

    assert_eq!(failures.len(), recoveries.len());
    for ((failure, recovery), (_, category, _)) in failures.iter().zip(&recoveries).zip(fixtures) {
        assert_eq!(failure.recovery(), *recovery);
        assert_eq!(failure.category(), category);
    }
    let safe_error = format!("{error:?} {error}");
    for recovery in &recoveries {
        assert!(safe_error.contains(&recovery.id().get().to_string()));
    }
    for (_, _, sensitive_detail) in fixtures {
        assert!(!safe_error.contains(sensitive_detail));
    }

    allow_complete.store(true, Ordering::Release);
    for recovery in recoveries {
        assert_eq!(
            retry_process_recovery(recovery.id()).unwrap(),
            ProcessRecoveryState::Complete
        );
    }
}
