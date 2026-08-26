use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use crate::{ProcessFinalizationError, SandboxError};

static NEXT_RECOVERY_ID: AtomicU64 = AtomicU64::new(1);
static PROCESS_RECOVERIES: OnceLock<Mutex<BTreeMap<ProcessRecoveryId, SharedRecovery>>> = OnceLock::new();
#[cfg(feature = "sandbox-test-fixtures")]
static PROCESS_RECOVERY_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

type SharedRecovery = Arc<RegisteredRecovery>;

struct RegisteredRecovery {
    kind: ProcessRecoveryKind,
    recovery: Mutex<Box<dyn ProcessRecovery>>,
    last_failure: Mutex<Option<ProcessRecoveryFailureCategory>>,
    in_flight: AtomicBool,
}

pub(crate) trait ProcessRecovery: Send {
    fn kind(&self) -> ProcessRecoveryKind;
    fn retry(&mut self) -> io::Result<bool>;
}

/// Stable identifier for process-scoped state that still requires cleanup.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProcessRecoveryId(u64);

impl ProcessRecoveryId {
    pub fn get(self) -> u64 {
        self.0
    }
}

/// The security state retained by a pending recovery.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProcessRecoveryKind {
    StartedChild,
    WindowsAcl,
}

impl ProcessRecoveryKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StartedChild => "started_child",
            Self::WindowsAcl => "windows_acl",
        }
    }
}

/// A queryable process-scoped recovery entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProcessRecoveryRecord {
    id: ProcessRecoveryId,
    kind: ProcessRecoveryKind,
}

impl ProcessRecoveryRecord {
    pub fn id(self) -> ProcessRecoveryId {
        self.id
    }

    pub fn kind(self) -> ProcessRecoveryKind {
        self.kind
    }

    pub fn reference(self) -> String {
        format!("solaris://process-recovery/{}", self.id.get())
    }
}

/// Result of one explicit recovery attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProcessRecoveryState {
    Pending,
    Complete,
}

/// Stable, non-sensitive category for a failed cleanup retry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProcessRecoveryFailureCategory {
    Permission,
    Process,
    Deadline,
    System,
}

impl ProcessRecoveryFailureCategory {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Permission => "permission",
            Self::Process => "process",
            Self::Deadline => "deadline",
            Self::System => "system",
        }
    }
}

/// Last safe failure category retained for one pending cleanup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProcessRecoveryFailureRecord {
    recovery: ProcessRecoveryRecord,
    category: ProcessRecoveryFailureCategory,
}

impl ProcessRecoveryFailureRecord {
    pub fn recovery(self) -> ProcessRecoveryRecord {
        self.recovery
    }

    pub fn category(self) -> ProcessRecoveryFailureCategory {
        self.category
    }
}

/// Structured marker included when an operation retained security state for retry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ProcessRecoveryRequired {
    recovery: ProcessRecoveryRecord,
}

impl ProcessRecoveryRequired {
    fn recovery(self) -> ProcessRecoveryRecord {
        self.recovery
    }
}

impl fmt::Display for ProcessRecoveryRequired {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "process cleanup recovery {} is required ({:?})",
            self.recovery.id.get(),
            self.recovery.kind
        )
    }
}

impl Error for ProcessRecoveryRequired {}

#[derive(Debug)]
struct ProcessOutcomeUnknown {
    source: io::Error,
}

impl fmt::Display for ProcessOutcomeUnknown {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("process launch outcome is unknown")
    }
}

impl Error for ProcessOutcomeUnknown {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

#[derive(Debug)]
struct ProcessRecoveryDrainTimeout {
    failures: Vec<ProcessRecoveryFailureRecord>,
}

impl fmt::Display for ProcessRecoveryDrainTimeout {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} process cleanup recoveries remain pending:",
            self.failures.len()
        )?;
        for failure in &self.failures {
            write!(
                formatter,
                " {}={}",
                failure.recovery.id().get(),
                failure.category.as_str()
            )?;
        }
        Ok(())
    }
}

impl Error for ProcessRecoveryDrainTimeout {}

pub(crate) fn register_process_recovery(recovery: impl ProcessRecovery + 'static) -> ProcessRecoveryRecord {
    let id = ProcessRecoveryId(NEXT_RECOVERY_ID.fetch_add(1, Ordering::Relaxed));
    let kind = recovery.kind();
    recovery_registry().insert(
        id,
        Arc::new(RegisteredRecovery {
            kind,
            recovery: Mutex::new(Box::new(recovery)),
            last_failure: Mutex::new(None),
            in_flight: AtomicBool::new(false),
        }),
    );
    ProcessRecoveryRecord { id, kind }
}

pub(crate) fn recovery_required_error(recovery: ProcessRecoveryRecord) -> io::Error {
    io::Error::other(ProcessRecoveryRequired { recovery })
}

#[cfg(any(unix, feature = "sandbox-test-fixtures"))]
pub(crate) fn outcome_unknown_error(source: io::Error) -> io::Error {
    io::Error::other(ProcessOutcomeUnknown { source })
}

#[cfg(feature = "sandbox-test-fixtures")]
pub fn process_outcome_unknown_error_for_test() -> io::Error {
    outcome_unknown_error(io::Error::other("injected released process launch failure"))
}

#[cfg(feature = "sandbox-test-fixtures")]
pub fn process_recovery_required_error_for_test(kind: ProcessRecoveryKind) -> io::Error {
    struct FixtureRecovery(ProcessRecoveryKind);

    impl ProcessRecovery for FixtureRecovery {
        fn kind(&self) -> ProcessRecoveryKind {
            self.0
        }

        fn retry(&mut self) -> io::Result<bool> {
            Ok(true)
        }
    }

    recovery_required_error(register_process_recovery(FixtureRecovery(kind)))
}

#[cfg(feature = "sandbox-test-fixtures")]
pub fn pending_process_recovery_error_for_test(kind: ProcessRecoveryKind, pending_attempts: usize) -> io::Error {
    struct FixtureRecovery {
        kind: ProcessRecoveryKind,
        pending_attempts: usize,
    }

    impl ProcessRecovery for FixtureRecovery {
        fn kind(&self) -> ProcessRecoveryKind {
            self.kind
        }

        fn retry(&mut self) -> io::Result<bool> {
            if self.pending_attempts == 0 {
                return Ok(true);
            }
            self.pending_attempts -= 1;
            Ok(false)
        }
    }

    recovery_required_error(register_process_recovery(FixtureRecovery { kind, pending_attempts }))
}

#[cfg(feature = "sandbox-test-fixtures")]
pub fn blocking_process_recovery_error_for_test(kind: ProcessRecoveryKind, delay: Duration) -> io::Error {
    struct FixtureRecovery {
        kind: ProcessRecoveryKind,
        delay: Option<Duration>,
    }

    impl ProcessRecovery for FixtureRecovery {
        fn kind(&self) -> ProcessRecoveryKind {
            self.kind
        }

        fn retry(&mut self) -> io::Result<bool> {
            if let Some(delay) = self.delay.take() {
                std::thread::sleep(delay);
                return Ok(false);
            }
            Ok(true)
        }
    }

    recovery_required_error(register_process_recovery(FixtureRecovery {
        kind,
        delay: Some(delay),
    }))
}

#[cfg(feature = "sandbox-test-fixtures")]
pub fn isolate_process_recoveries_for_test() -> impl Drop {
    let lock = PROCESS_RECOVERY_TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let baseline = pending_process_recoveries()
        .into_iter()
        .map(ProcessRecoveryRecord::id)
        .collect::<Vec<_>>();
    assert!(
        baseline.is_empty(),
        "a previous process recovery test leaked retained ownership"
    );
    ProcessRecoveryTestGuard { baseline, _lock: lock }
}

#[cfg(feature = "sandbox-test-fixtures")]
struct ProcessRecoveryTestGuard {
    baseline: Vec<ProcessRecoveryId>,
    _lock: MutexGuard<'static, ()>,
}

#[cfg(feature = "sandbox-test-fixtures")]
impl Drop for ProcessRecoveryTestGuard {
    fn drop(&mut self) {
        const MAX_CLEANUP_ATTEMPTS: usize = 1_024;

        for recovery in pending_process_recoveries()
            .into_iter()
            .filter(|recovery| !self.baseline.contains(&recovery.id()))
        {
            for attempt in 0..MAX_CLEANUP_ATTEMPTS {
                match retry_process_recovery(recovery.id()) {
                    Ok(ProcessRecoveryState::Complete) => break,
                    Ok(ProcessRecoveryState::Pending) if attempt + 1 < MAX_CLEANUP_ATTEMPTS => {}
                    Ok(ProcessRecoveryState::Pending) => tracing::error!(
                        recovery_id = recovery.id().get(),
                        recovery_kind = recovery.kind().as_str(),
                        "test recovery remained pending after bounded teardown"
                    ),
                    Err(error) => {
                        tracing::error!(
                            recovery_id = recovery.id().get(),
                            recovery_kind = recovery.kind().as_str(),
                            error_kind = ?error.kind(),
                            "test recovery teardown failed"
                        );
                        break;
                    }
                }
            }
        }
    }
}

pub(crate) fn retry_process_recoveries_by_kind(kind: ProcessRecoveryKind) -> io::Result<()> {
    let recoveries = pending_process_recoveries()
        .into_iter()
        .filter(|recovery| recovery.kind() == kind)
        .collect::<Vec<_>>();
    for recovery in recoveries {
        if retry_process_recovery(recovery.id())? == ProcessRecoveryState::Pending {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                ProcessRecoveryRequired { recovery },
            ));
        }
    }
    Ok(())
}

/// Lists every process-scoped recovery whose security state is still retained.
pub fn pending_process_recoveries() -> Vec<ProcessRecoveryRecord> {
    recovery_registry()
        .iter()
        .map(|(id, recovery)| ProcessRecoveryRecord {
            id: *id,
            kind: recovery.kind,
        })
        .collect()
}

/// Attempts one retained cleanup without discarding ownership on failure.
pub fn retry_process_recovery(id: ProcessRecoveryId) -> io::Result<ProcessRecoveryState> {
    let Some(recovery) = begin_recovery_attempt(id) else {
        return Ok(if recovery_registry().contains_key(&id) {
            ProcessRecoveryState::Pending
        } else {
            ProcessRecoveryState::Complete
        });
    };
    retry_acquired_recovery(id, recovery)
}

pub(crate) async fn retry_process_recovery_before(
    id: ProcessRecoveryId,
    deadline: tokio::time::Instant,
) -> io::Result<ProcessRecoveryState> {
    let Some(recovery) = begin_recovery_attempt(id) else {
        return Ok(if recovery_registry().contains_key(&id) {
            ProcessRecoveryState::Pending
        } else {
            ProcessRecoveryState::Complete
        });
    };
    let task_recovery = Arc::clone(&recovery);
    let task = tokio::task::spawn_blocking(move || retry_acquired_recovery(id, task_recovery));
    match tokio::time::timeout_at(deadline, task).await {
        Ok(Ok(result)) => result,
        Ok(Err(_)) => {
            set_last_recovery_failure(&recovery, ProcessRecoveryFailureCategory::System);
            Err(io::Error::other("process recovery task stopped unexpectedly"))
        }
        Err(_) => {
            set_last_recovery_failure(&recovery, ProcessRecoveryFailureCategory::Deadline);
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "process recovery attempt exceeded its deadline",
            ))
        }
    }
}

fn begin_recovery_attempt(id: ProcessRecoveryId) -> Option<SharedRecovery> {
    let recovery = recovery_registry().get(&id).cloned()?;
    recovery
        .in_flight
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .ok()
        .map(|_| recovery)
}

fn retry_acquired_recovery(id: ProcessRecoveryId, recovery: SharedRecovery) -> io::Result<ProcessRecoveryState> {
    let _in_flight = InFlightRecovery::new(Arc::clone(&recovery));
    let retry_result = {
        let mut pending = recovery_lock(&recovery);
        pending.retry()
    };
    let complete = match retry_result {
        Ok(complete) => complete,
        Err(error) => {
            set_last_recovery_failure(&recovery, recovery_failure_category(error.kind()));
            return Err(error);
        }
    };
    if !complete {
        return Ok(ProcessRecoveryState::Pending);
    }
    let mut registry = recovery_registry();
    if registry
        .get(&id)
        .is_some_and(|registered| Arc::ptr_eq(registered, &recovery))
    {
        registry.remove(&id);
    }
    Ok(ProcessRecoveryState::Complete)
}

struct InFlightRecovery {
    recovery: SharedRecovery,
}

impl InFlightRecovery {
    fn new(recovery: SharedRecovery) -> Self {
        Self { recovery }
    }
}

impl Drop for InFlightRecovery {
    fn drop(&mut self) {
        self.recovery.in_flight.store(false, Ordering::Release);
    }
}

/// Retries all retained cleanup work until it completes or the deadline expires.
pub async fn drain_process_recoveries(timeout: Duration) -> io::Result<()> {
    drain_process_recoveries_before(tokio::time::Instant::now() + timeout).await
}

pub(crate) async fn drain_process_recoveries_before(deadline: tokio::time::Instant) -> io::Result<()> {
    loop {
        let pending = pending_process_recoveries();
        if pending.is_empty() {
            return Ok(());
        }
        for recovery in pending {
            if tokio::time::Instant::now() >= deadline {
                return drain_timeout();
            }
            let _ = retry_process_recovery_before(recovery.id(), deadline).await;
        }
        let pending = pending_process_recoveries();
        if pending.is_empty() {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return drain_timeout_with(pending);
        }
        tokio::time::sleep_until((tokio::time::Instant::now() + Duration::from_millis(25)).min(deadline)).await;
    }
}

fn drain_timeout() -> io::Result<()> {
    drain_timeout_with(pending_process_recoveries())
}

fn drain_timeout_with(pending: Vec<ProcessRecoveryRecord>) -> io::Result<()> {
    let failures = process_recovery_failures_for(&pending);
    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        ProcessRecoveryDrainTimeout { failures },
    ))
}

/// Returns the stable per-recovery failures retained by a drain timeout.
pub fn process_recovery_drain_failures(error: &io::Error) -> Option<&[ProcessRecoveryFailureRecord]> {
    error
        .get_ref()?
        .downcast_ref::<ProcessRecoveryDrainTimeout>()
        .map(|timeout| timeout.failures.as_slice())
}

pub(crate) fn process_recovery_failures_for(pending: &[ProcessRecoveryRecord]) -> Vec<ProcessRecoveryFailureRecord> {
    let registry = recovery_registry();
    pending
        .iter()
        .map(|record| {
            let category = registry
                .get(&record.id())
                .and_then(|recovery| *recovery_failure_lock(recovery))
                .unwrap_or(ProcessRecoveryFailureCategory::Deadline);
            ProcessRecoveryFailureRecord {
                recovery: *record,
                category,
            }
        })
        .collect()
}

fn recovery_failure_category(kind: io::ErrorKind) -> ProcessRecoveryFailureCategory {
    match kind {
        io::ErrorKind::PermissionDenied => ProcessRecoveryFailureCategory::Permission,
        io::ErrorKind::NotFound
        | io::ErrorKind::BrokenPipe
        | io::ErrorKind::ConnectionAborted
        | io::ErrorKind::ConnectionRefused
        | io::ErrorKind::ConnectionReset
        | io::ErrorKind::NotConnected
        | io::ErrorKind::UnexpectedEof => ProcessRecoveryFailureCategory::Process,
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock => ProcessRecoveryFailureCategory::Deadline,
        _ => ProcessRecoveryFailureCategory::System,
    }
}

fn set_last_recovery_failure(recovery: &SharedRecovery, category: ProcessRecoveryFailureCategory) {
    *recovery_failure_lock(recovery) = Some(category);
}

/// Finds the structured recovery marker inside an execution error.
pub fn process_recovery_required(error: &io::Error) -> Option<ProcessRecoveryRecord> {
    find_process_recovery_required(error)
}

/// Returns whether an execution error proves only that a released process
/// launch may have run, even when all retained cleanup completed.
pub fn process_outcome_unknown(error: &io::Error) -> bool {
    find_process_outcome_unknown(error)
}

fn find_process_recovery_required(error: &(dyn Error + 'static)) -> Option<ProcessRecoveryRecord> {
    if let Some(required) = error.downcast_ref::<ProcessRecoveryRequired>() {
        return Some(required.recovery());
    }
    if let Some(finalization) = error.downcast_ref::<ProcessFinalizationError>() {
        return finalization
            .failures()
            .iter()
            .find_map(|failure| find_process_recovery_required(failure.error()));
    }
    if let Some(sandbox) = error.downcast_ref::<SandboxError>()
        && let Some(source) = sandbox.cleanup_source()
        && let Some(recovery) = find_process_recovery_required(source)
    {
        return Some(recovery);
    }
    if let Some(error) = error.downcast_ref::<io::Error>()
        && let Some(source) = error.get_ref()
        && let Some(recovery) = find_process_recovery_required(source)
    {
        return Some(recovery);
    }
    error.source().and_then(find_process_recovery_required)
}

fn find_process_outcome_unknown(error: &(dyn Error + 'static)) -> bool {
    if error.downcast_ref::<ProcessOutcomeUnknown>().is_some() {
        return true;
    }
    if let Some(finalization) = error.downcast_ref::<ProcessFinalizationError>()
        && finalization
            .failures()
            .iter()
            .any(|failure| find_process_outcome_unknown(failure.error()))
    {
        return true;
    }
    if let Some(sandbox) = error.downcast_ref::<SandboxError>()
        && let Some(source) = sandbox.cleanup_source()
        && find_process_outcome_unknown(source)
    {
        return true;
    }
    if let Some(error) = error.downcast_ref::<io::Error>()
        && let Some(source) = error.get_ref()
        && find_process_outcome_unknown(source)
    {
        return true;
    }
    error.source().is_some_and(find_process_outcome_unknown)
}

fn recovery_registry() -> MutexGuard<'static, BTreeMap<ProcessRecoveryId, SharedRecovery>> {
    PROCESS_RECOVERIES
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn recovery_lock(recovery: &SharedRecovery) -> MutexGuard<'_, Box<dyn ProcessRecovery>> {
    recovery
        .recovery
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn recovery_failure_lock(recovery: &SharedRecovery) -> MutexGuard<'_, Option<ProcessRecoveryFailureCategory>> {
    recovery
        .last_failure
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(all(test, feature = "sandbox-test-fixtures"))]
#[path = "recovery_test.rs"]
mod recovery_test;
