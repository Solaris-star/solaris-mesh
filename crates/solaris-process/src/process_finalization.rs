use std::error::Error;
use std::fmt;
use std::io;

/// A process stage whose error was retained while later cleanup still ran.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProcessFinalizationStage {
    Spawn,
    Wait,
    Terminate,
    Stdin,
    Stdout,
    Stderr,
    Release,
    SandboxCleanup,
    Recovery,
    Reconciliation,
}

impl ProcessFinalizationStage {
    const fn priority(self) -> u8 {
        match self {
            Self::Spawn => 0,
            Self::Wait => 1,
            Self::Terminate => 2,
            Self::Stdin => 3,
            Self::Stdout => 4,
            Self::Stderr => 5,
            Self::Release => 6,
            Self::SandboxCleanup => 7,
            Self::Recovery => 8,
            Self::Reconciliation => 9,
        }
    }
}

/// One original error retained in a multi-stage finalization failure.
#[derive(Debug)]
pub struct ProcessFinalizationFailure {
    stage: ProcessFinalizationStage,
    error: io::Error,
}

impl ProcessFinalizationFailure {
    pub fn stage(&self) -> ProcessFinalizationStage {
        self.stage
    }

    pub fn error(&self) -> &io::Error {
        &self.error
    }
}

/// Multiple process/finalization errors returned without discarding any one.
///
/// Failures are ordered by the stable [`ProcessFinalizationStage`] priority.
/// The outer [`io::ErrorKind`] is the sandbox-cleanup kind when cleanup failed,
/// because callers must not mistake an incomplete cleanup for an ordinary
/// process error. Every original error remains available through
/// [`Self::failures`]. A single failure is returned unchanged for compatibility.
#[derive(Debug)]
pub struct ProcessFinalizationError {
    failures: Vec<ProcessFinalizationFailure>,
}

impl ProcessFinalizationError {
    pub fn failures(&self) -> &[ProcessFinalizationFailure] {
        &self.failures
    }
}

impl fmt::Display for ProcessFinalizationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "process execution and finalization failed in {} stages",
            self.failures.len()
        )
    }
}

impl Error for ProcessFinalizationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.failures.first().map(|failure| failure.error() as _)
    }
}

pub(crate) struct FinalizationFailures {
    failures: Vec<ProcessFinalizationFailure>,
}

impl FinalizationFailures {
    pub(crate) const fn new() -> Self {
        Self { failures: Vec::new() }
    }

    pub(crate) fn record(&mut self, stage: ProcessFinalizationStage, result: io::Result<()>) -> bool {
        match result {
            Ok(()) => true,
            Err(error) => {
                self.failures.push(ProcessFinalizationFailure { stage, error });
                false
            }
        }
    }

    pub(crate) fn record_error(&mut self, stage: ProcessFinalizationStage, error: io::Error) {
        self.failures.push(ProcessFinalizationFailure { stage, error });
    }

    pub(crate) fn finish<T>(mut self, cleanup: io::Result<()>, value: T) -> io::Result<T> {
        self.record(ProcessFinalizationStage::SandboxCleanup, cleanup);
        self.failures.sort_by_key(|failure| failure.stage.priority());
        if self.failures.is_empty() {
            return Ok(value);
        }
        if self.failures.len() == 1 {
            return Err(self.failures.pop().expect("one finalization failure").error);
        }
        let kind = self
            .failures
            .iter()
            .find(|failure| failure.stage == ProcessFinalizationStage::SandboxCleanup)
            .unwrap_or_else(|| self.failures.first().expect("multiple finalization failures"))
            .error
            .kind();
        Err(io::Error::new(
            kind,
            ProcessFinalizationError {
                failures: self.failures,
            },
        ))
    }
}
