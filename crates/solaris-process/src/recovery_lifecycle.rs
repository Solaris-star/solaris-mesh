use std::fmt;
use std::io;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::recovery::{
    drain_process_recoveries_before, process_recovery_drain_failures, process_recovery_failures_for,
    retry_process_recovery_before,
};
use crate::{ProcessRecoveryFailureRecord, ProcessRecoveryRecord, pending_process_recoveries};

const MIN_RETRY_INTERVAL: Duration = Duration::from_millis(1);

/// Owns the Host-side worker that retries retained process cleanup.
pub struct ProcessRecoveryLifecycle {
    stop: watch::Sender<bool>,
    worker: Option<JoinHandle<()>>,
}

impl ProcessRecoveryLifecycle {
    pub fn start(retry_interval: Duration) -> Self {
        let retry_interval = normalize_retry_interval(retry_interval);
        let (stop, mut stop_rx) = watch::channel(false);
        let worker = tokio::spawn(async move {
            let mut interval = tokio::time::interval(retry_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        retry_pending_once(retry_deadline(retry_interval)).await;
                    }
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            break;
                        }
                    }
                }
            }
        });
        Self {
            stop,
            worker: Some(worker),
        }
    }

    pub async fn shutdown(&mut self, timeout: Duration) -> Result<(), ProcessRecoveryLifecycleError> {
        let deadline = tokio::time::Instant::now() + timeout;
        let _ = self.stop.send(true);
        if let Some(mut worker) = self.worker.take() {
            match tokio::time::timeout_at(deadline, &mut worker).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::error!(
                        cancelled = error.is_cancelled(),
                        panic = error.is_panic(),
                        "process recovery worker stopped unexpectedly"
                    );
                }
                Err(_) => {
                    worker.abort();
                    return Err(ProcessRecoveryLifecycleError::new(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "process recovery worker did not stop before the shutdown deadline",
                    )));
                }
            }
        }
        match drain_process_recoveries_before(deadline).await {
            Ok(()) => Ok(()),
            Err(source) => Err(ProcessRecoveryLifecycleError::new(source)),
        }
    }
}

impl Drop for ProcessRecoveryLifecycle {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        if let Some(worker) = self.worker.take() {
            worker.abort();
        }
    }
}

async fn retry_pending_once(deadline: tokio::time::Instant) {
    for recovery in pending_process_recoveries() {
        if let Err(error) = retry_process_recovery_before(recovery.id(), deadline).await {
            tracing::warn!(
                recovery_id = recovery.id().get(),
                recovery_kind = recovery.kind().as_str(),
                error_kind = ?error.kind(),
                "process cleanup recovery attempt remains pending"
            );
        }
    }
}

fn normalize_retry_interval(retry_interval: Duration) -> Duration {
    retry_interval.max(MIN_RETRY_INTERVAL)
}

fn retry_deadline(retry_interval: Duration) -> tokio::time::Instant {
    tokio::time::Instant::now() + retry_interval
}

#[derive(Debug)]
pub struct ProcessRecoveryLifecycleError {
    pending: Vec<ProcessRecoveryRecord>,
    failures: Vec<ProcessRecoveryFailureRecord>,
    source: io::Error,
}

impl ProcessRecoveryLifecycleError {
    fn new(source: io::Error) -> Self {
        let pending = pending_process_recoveries();
        let failures = process_recovery_drain_failures(&source)
            .map(<[ProcessRecoveryFailureRecord]>::to_vec)
            .unwrap_or_else(|| process_recovery_failures_for(&pending));
        Self {
            pending,
            failures,
            source,
        }
    }

    pub fn pending(&self) -> &[ProcessRecoveryRecord] {
        &self.pending
    }

    pub fn failures(&self) -> &[ProcessRecoveryFailureRecord] {
        &self.failures
    }
}

impl fmt::Display for ProcessRecoveryLifecycleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} process cleanup recoveries require reconciliation:",
            self.pending.len()
        )?;
        for failure in &self.failures {
            write!(
                formatter,
                " {}={}",
                failure.recovery().id().get(),
                failure.category().as_str()
            )?;
        }
        Ok(())
    }
}

impl std::error::Error for ProcessRecoveryLifecycleError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

#[cfg(all(test, feature = "sandbox-test-fixtures"))]
#[path = "recovery_lifecycle_test.rs"]
mod recovery_lifecycle_test;
