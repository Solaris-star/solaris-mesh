use serde::{Deserialize, Serialize};

/// Strength of one process-sandbox backend.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxEnforcement {
    Full,
    Partial,
    Unavailable,
}

impl SandboxEnforcement {
    /// Strict Auto process execution is allowed only when every promised
    /// boundary is fully enforced.
    pub const fn satisfies_strict_auto(self) -> bool {
        matches!(self, Self::Full)
    }
}

/// Backend that produced a sandbox report.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxBackend {
    None,
    LinuxBubblewrapLandlock,
    MacOsSeatbelt,
    WindowsAppContainer,
    ExternalRunner,
}

/// Non-sensitive reason for a sandbox report.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxReason {
    NotRequested,
    Enforced,
    PlatformRunnerUnavailable,
    BubblewrapUnavailable,
    HelperUnavailable,
    LandlockUnavailable,
    WorkspaceObjectBindingUnavailable,
    SeatbeltUnavailable,
    AppContainerUnavailable,
    NetworkProxyUnavailable,
    WorkspaceAclUnavailable,
    JobContainmentUnavailable,
    ExternalRunnerUnavailable,
    ExternalRunnerProtocolRejected,
    ExternalRunnerCapabilityInsufficient,
    ExecutableNotPinned,
    UnsafeLayout,
    IdentityScanFailed,
    IdentityBudgetExceeded,
    ProtectedObjectAlias,
    SetupFailed,
    StartConfirmationFailed,
}

/// Structured process-sandbox capability or execution report.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct SandboxReport {
    enforcement: SandboxEnforcement,
    backend: SandboxBackend,
    reason: SandboxReason,
}

impl SandboxReport {
    pub const fn new(enforcement: SandboxEnforcement, backend: SandboxBackend, reason: SandboxReason) -> Self {
        Self {
            enforcement,
            backend,
            reason,
        }
    }

    pub const fn enforcement(&self) -> SandboxEnforcement {
        self.enforcement
    }

    pub const fn backend(&self) -> SandboxBackend {
        self.backend
    }

    pub const fn reason(&self) -> SandboxReason {
        self.reason
    }
}

#[cfg(test)]
#[path = "sandbox_test.rs"]
mod sandbox_test;
