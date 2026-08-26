use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use super::{normalize_retry_interval, retry_deadline};
use crate::recovery::{ProcessRecovery, register_process_recovery};
use crate::{
    ProcessRecoveryFailureCategory, ProcessRecoveryKind, ProcessRecoveryLifecycle, ProcessRecoveryState,
    blocking_process_recovery_error_for_test, isolate_process_recoveries_for_test, pending_process_recoveries,
    pending_process_recovery_error_for_test, process_recovery_required, retry_process_recovery,
};

struct CountingPendingRecovery {
    attempts: Arc<AtomicUsize>,
    complete: Arc<AtomicBool>,
}

impl ProcessRecovery for CountingPendingRecovery {
    fn kind(&self) -> ProcessRecoveryKind {
        ProcessRecoveryKind::StartedChild
    }

    fn retry(&mut self) -> std::io::Result<bool> {
        self.attempts.fetch_add(1, Ordering::AcqRel);
        Ok(self.complete.load(Ordering::Acquire))
    }
}

#[tokio::test]
async fn zero_interval_uses_a_future_deadline_and_retries_at_a_bounded_rate() {
    let _recovery_test_guard = isolate_process_recoveries_for_test();
    let normalized = normalize_retry_interval(Duration::ZERO);
    let before_deadline = tokio::time::Instant::now();
    assert_eq!(normalized, Duration::from_millis(1));
    assert!(retry_deadline(normalized) > before_deadline);

    let attempts = Arc::new(AtomicUsize::new(0));
    let complete = Arc::new(AtomicBool::new(false));
    let recovery = register_process_recovery(CountingPendingRecovery {
        attempts: Arc::clone(&attempts),
        complete: Arc::clone(&complete),
    });
    let mut lifecycle = ProcessRecoveryLifecycle::start(Duration::ZERO);

    tokio::time::timeout(Duration::from_secs(1), async {
        while attempts.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("zero interval worker never performed a real retry");
    tokio::time::sleep(Duration::from_millis(20)).await;
    let observed_attempts = attempts.load(Ordering::Acquire);
    assert!(observed_attempts > 0);
    assert!(observed_attempts < 100, "zero interval worker busy-looped");

    complete.store(true, Ordering::Release);
    tokio::time::timeout(Duration::from_secs(1), async {
        while pending_process_recoveries().contains(&recovery) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("normalized zero interval worker did not complete recovery");
    lifecycle.shutdown(Duration::from_millis(50)).await.unwrap();
}

#[tokio::test]
async fn host_lifecycle_retries_and_drains_typed_recovery_ownership() {
    let _recovery_test_guard = isolate_process_recoveries_for_test();
    let error = pending_process_recovery_error_for_test(ProcessRecoveryKind::StartedChild, 2);
    let recovery = process_recovery_required(&error).unwrap();
    let mut lifecycle = ProcessRecoveryLifecycle::start(Duration::from_millis(5));

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if !pending_process_recoveries()
                .iter()
                .any(|candidate| candidate.id() == recovery.id())
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    lifecycle.shutdown(Duration::from_millis(50)).await.unwrap();
}

#[tokio::test]
async fn host_shutdown_timeout_reports_typed_pending_id_without_releasing_it() {
    let _recovery_test_guard = isolate_process_recoveries_for_test();
    let error = pending_process_recovery_error_for_test(ProcessRecoveryKind::WindowsAcl, 8);
    let recovery = process_recovery_required(&error).unwrap();
    let mut lifecycle = ProcessRecoveryLifecycle::start(Duration::from_secs(60));

    let error = lifecycle.shutdown(Duration::ZERO).await.unwrap_err();

    assert!(error.pending().contains(&recovery));
    assert_eq!(error.failures().len(), 1);
    assert_eq!(error.failures()[0].recovery(), recovery);
    assert_eq!(error.failures()[0].category(), ProcessRecoveryFailureCategory::Deadline);
    assert!(error.to_string().contains(&recovery.id().get().to_string()));
    assert!(pending_process_recoveries().contains(&recovery));
    while retry_process_recovery(recovery.id()).unwrap() == ProcessRecoveryState::Pending {}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_deadline_covers_an_in_flight_blocking_retry_and_retains_ownership() {
    let _recovery_test_guard = isolate_process_recoveries_for_test();
    let marker =
        blocking_process_recovery_error_for_test(ProcessRecoveryKind::StartedChild, Duration::from_millis(500));
    let recovery = process_recovery_required(&marker).unwrap();
    let mut lifecycle = ProcessRecoveryLifecycle::start(Duration::from_millis(1));
    tokio::time::sleep(Duration::from_millis(25)).await;

    let started = std::time::Instant::now();
    let error = lifecycle.shutdown(Duration::from_millis(50)).await.unwrap_err();

    assert!(
        started.elapsed() < Duration::from_millis(250),
        "shutdown exceeded its one absolute deadline"
    );
    assert!(error.pending().contains(&recovery));
    assert!(pending_process_recoveries().contains(&recovery));
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        retry_process_recovery(recovery.id()).unwrap(),
        ProcessRecoveryState::Complete
    );
}
