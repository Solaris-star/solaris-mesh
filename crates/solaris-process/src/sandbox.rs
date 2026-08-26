use std::io;
#[cfg(any(test, target_os = "linux", target_os = "macos", windows))]
use std::sync::OnceLock;

use thiserror::Error;
use tokio::process::{Child, Command};

use crate::launch_policy::ProcessLaunchPolicy;
use crate::recovery::process_outcome_unknown;
pub(crate) use crate::sandbox_report::{SandboxBackend, SandboxEnforcement, SandboxReason, SandboxReport};

#[cfg(any(test, all(unix, not(any(target_os = "linux", target_os = "macos")))))]
#[path = "sandbox/external.rs"]
mod external;
#[path = "sandbox/helper.rs"]
mod helper;
#[cfg(any(test, target_os = "linux", target_os = "macos", windows))]
#[path = "sandbox/identity.rs"]
mod identity;
#[path = "sandbox/layout.rs"]
mod layout;
#[cfg(target_os = "linux")]
#[path = "sandbox/linux.rs"]
mod linux;
#[cfg(target_os = "macos")]
#[path = "sandbox/macos.rs"]
mod macos;
#[cfg(any(test, target_os = "macos"))]
#[path = "sandbox/macos_profile.rs"]
mod macos_profile;
#[cfg(windows)]
#[path = "sandbox/windows.rs"]
mod windows;
#[cfg(windows)]
#[path = "sandbox/windows_acl.rs"]
mod windows_acl;

pub(crate) use helper::trusted_sandbox_helper;
use layout::{ResolvedSandboxLayout, validate_layout_with_identities};

#[cfg(target_os = "linux")]
static CONFIRMED_LINUX_SANDBOX: OnceLock<SandboxReport> = OnceLock::new();
#[cfg(target_os = "macos")]
static CONFIRMED_MACOS_SANDBOX: OnceLock<SandboxReport> = OnceLock::new();
#[cfg(windows)]
static CONFIRMED_WINDOWS_SANDBOX: OnceLock<SandboxReport> = OnceLock::new();

/// Reports the strict process-sandbox backend available on this host.
pub fn platform_sandbox_report() -> SandboxReport {
    #[cfg(target_os = "linux")]
    {
        cache_confirmed_full(&CONFIRMED_LINUX_SANDBOX, linux::probe_capability)
    }
    #[cfg(target_os = "macos")]
    {
        cache_confirmed_full(&CONFIRMED_MACOS_SANDBOX, macos::probe_capability)
    }
    #[cfg(windows)]
    {
        cache_confirmed_full(&CONFIRMED_WINDOWS_SANDBOX, windows::probe_capability)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
    {
        unavailable_platform_report()
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
const fn unavailable_platform_report() -> SandboxReport {
    SandboxReport::new(
        SandboxEnforcement::Unavailable,
        SandboxBackend::None,
        SandboxReason::PlatformRunnerUnavailable,
    )
}

#[derive(Debug, Error)]
pub enum SandboxError {
    #[error("workspace sandbox cannot separate protected state from writable roots")]
    UnrepresentableSandboxLayout,
    #[error("strict workspace sandbox requires a pinned executable")]
    ExecutableNotPinned,
    #[error("workspace aliases a protected filesystem object")]
    ProtectedObjectAlias,
    #[error("workspace file has a hardlink outside the workspace")]
    ExternalHardlink,
    #[error("workspace sandbox identity inspection failed")]
    IdentityInspectionFailed,
    #[error("workspace sandbox identity budget was exceeded")]
    IdentityBudgetExceeded,
    #[error("workspace contains a host Unix socket")]
    HostSocketExposed,
    #[error("workspace contains a Windows reparse point")]
    ReparsePointExposed,
    #[error("workspace sandbox preparation failed")]
    PreparationFailed,
    #[error("strict workspace sandbox enforcement is insufficient")]
    InsufficientEnforcement { report: SandboxReport },
    #[error("approved-domain proxy is unavailable for this strict sandbox")]
    NetworkProxyUnavailable { report: SandboxReport },
    #[error("workspace sandbox cleanup failed")]
    CleanupFailed {
        #[source]
        source: io::Error,
    },
}

impl SandboxError {
    pub(crate) fn cleanup_failed(source: io::Error) -> Self {
        Self::CleanupFailed { source }
    }

    pub fn cleanup_source(&self) -> Option<&io::Error> {
        match self {
            Self::CleanupFailed { source } => Some(source),
            _ => None,
        }
    }

    pub fn report(&self) -> Option<&SandboxReport> {
        match self {
            Self::InsufficientEnforcement { report } | Self::NetworkProxyUnavailable { report } => Some(report),
            _ => None,
        }
    }
}

impl PartialEq for SandboxError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::UnrepresentableSandboxLayout, Self::UnrepresentableSandboxLayout)
            | (Self::ExecutableNotPinned, Self::ExecutableNotPinned)
            | (Self::ProtectedObjectAlias, Self::ProtectedObjectAlias)
            | (Self::ExternalHardlink, Self::ExternalHardlink)
            | (Self::IdentityInspectionFailed, Self::IdentityInspectionFailed)
            | (Self::IdentityBudgetExceeded, Self::IdentityBudgetExceeded)
            | (Self::HostSocketExposed, Self::HostSocketExposed)
            | (Self::ReparsePointExposed, Self::ReparsePointExposed)
            | (Self::PreparationFailed, Self::PreparationFailed) => true,
            (Self::InsufficientEnforcement { report: left }, Self::InsufficientEnforcement { report: right }) => {
                left == right
            }
            (Self::NetworkProxyUnavailable { report: left }, Self::NetworkProxyUnavailable { report: right }) => {
                left == right
            }
            (Self::CleanupFailed { source: left }, Self::CleanupFailed { source: right }) => {
                left.kind() == right.kind()
                    && left.raw_os_error() == right.raw_os_error()
                    && left.to_string() == right.to_string()
            }
            _ => false,
        }
    }
}

impl Eq for SandboxError {}

/// Finds the structured strict-sandbox report retained anywhere in an
/// execution/finalization error chain.
pub fn sandbox_report_from_error(error: &io::Error) -> Option<SandboxReport> {
    find_sandbox_report(error)
}

fn find_sandbox_report(error: &(dyn std::error::Error + 'static)) -> Option<SandboxReport> {
    if let Some(sandbox) = error.downcast_ref::<SandboxError>() {
        if let Some(report) = sandbox.report() {
            return Some(*report);
        }
        if let Some(source) = sandbox.cleanup_source()
            && let Some(report) = find_sandbox_report(source)
        {
            return Some(report);
        }
    }
    if let Some(finalization) = error.downcast_ref::<crate::ProcessFinalizationError>()
        && let Some(report) = finalization
            .failures()
            .iter()
            .find_map(|failure| find_sandbox_report(failure.error()))
    {
        return Some(report);
    }
    if let Some(error) = error.downcast_ref::<io::Error>()
        && let Some(source) = error.get_ref()
        && let Some(report) = find_sandbox_report(source)
    {
        return Some(report);
    }
    error.source().and_then(find_sandbox_report)
}

pub(crate) trait SandboxRunner: Send {
    fn report(&self) -> SandboxReport;
    fn configure(&mut self, command: &mut Command) -> io::Result<SandboxCommandDisposition>;
    fn verify_before_spawn(&mut self) -> io::Result<()>;
    fn confirm_started(&mut self, child: &mut Child) -> io::Result<()>;
    fn guardian_requires_verified_process_tree_drain(&self) -> bool {
        self.report().reason() != SandboxReason::NotRequested
    }
    fn process_tree_is_drained(&mut self) -> io::Result<bool> {
        Ok(true)
    }
    fn cleanup(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SandboxCommandDisposition {
    Preserved,
    Replaced,
}

pub(crate) struct PreparedSandbox {
    runner: Box<dyn SandboxRunner>,
}

impl PreparedSandbox {
    #[cfg(test)]
    pub(crate) fn prepare(policy: &ProcessLaunchPolicy) -> io::Result<Self> {
        Self::prepare_for_executable(policy, true)
    }

    pub(crate) fn prepare_for_executable(policy: &ProcessLaunchPolicy, executable_is_pinned: bool) -> io::Result<Self> {
        Self::prepare_with_executable(policy, executable_is_pinned, None)
    }

    pub(crate) fn prepare_for_pinned_executable(
        policy: &ProcessLaunchPolicy,
        executable_identity: &crate::ExecutableIdentity,
    ) -> io::Result<Self> {
        Self::prepare_with_executable(policy, true, Some(executable_identity))
    }

    fn prepare_with_executable(
        policy: &ProcessLaunchPolicy,
        executable_is_pinned: bool,
        executable_identity: Option<&crate::ExecutableIdentity>,
    ) -> io::Result<Self> {
        match policy {
            ProcessLaunchPolicy::Ambient => Ok(Self {
                runner: Box::new(AmbientRunner),
            }),
            ProcessLaunchPolicy::WorkspaceSandbox {
                workspace_root,
                workspace_capability,
                protected_roots,
                protected_object_identities,
                network_proxy,
            } => prepare_workspace_sandbox(WorkspaceSandboxPreparation {
                workspace_root,
                workspace_capability: workspace_capability.as_ref(),
                protected_roots,
                protected_object_identities,
                network_proxy,
                executable_is_pinned,
                executable_identity,
                #[cfg(feature = "sandbox-test-fixtures")]
                helper_path: None,
            }),
            #[cfg(feature = "sandbox-test-fixtures")]
            ProcessLaunchPolicy::WorkspaceSandboxTest {
                workspace_root,
                workspace_capability,
                protected_roots,
                protected_object_identities,
                helper_path,
            } => prepare_workspace_sandbox(WorkspaceSandboxPreparation {
                workspace_root,
                workspace_capability: workspace_capability.as_ref(),
                protected_roots,
                protected_object_identities,
                network_proxy: &crate::NetworkProxyPolicy::default(),
                executable_is_pinned,
                executable_identity,
                helper_path: Some(helper_path),
            }),
        }
    }

    pub(crate) fn configure(&mut self, command: &mut Command) -> io::Result<SandboxCommandDisposition> {
        self.runner.configure(command)
    }

    pub(crate) fn requires_verified_process_tree_drain(&self) -> bool {
        self.runner.guardian_requires_verified_process_tree_drain()
    }

    pub(crate) fn verify_before_spawn(&mut self) -> io::Result<()> {
        self.runner.verify_before_spawn()?;
        let report = self.runner.report();
        if report.reason() == SandboxReason::NotRequested {
            Ok(())
        } else {
            require_full_enforcement(report)
        }
    }

    pub(crate) fn confirm_started(&mut self, child: &mut Child) -> io::Result<()> {
        self.runner.confirm_started(child)
    }

    pub(crate) fn process_tree_is_drained(&mut self) -> io::Result<bool> {
        self.runner.process_tree_is_drained()
    }

    pub(crate) fn cleanup(&mut self) -> io::Result<()> {
        self.runner.cleanup()
    }

    pub(crate) fn map_spawn_error(&self, error: io::Error) -> io::Error {
        let report = self.runner.report();
        if report.reason() == SandboxReason::NotRequested {
            return error;
        }
        insufficient_enforcement_error(SandboxReport::new(
            SandboxEnforcement::Unavailable,
            report.backend(),
            SandboxReason::SetupFailed,
        ))
    }

    pub(crate) fn map_containment_error(&self, error: io::Error) -> io::Error {
        // Once the guardian released the target, replacing this typed marker
        // with a generic sandbox setup error would make an unknown side effect
        // look safe to retry.
        if process_outcome_unknown(&error) {
            return error;
        }
        let report = self.runner.report();
        if report.backend() == SandboxBackend::WindowsAppContainer {
            return insufficient_enforcement_error(SandboxReport::new(
                SandboxEnforcement::Partial,
                SandboxBackend::WindowsAppContainer,
                SandboxReason::JobContainmentUnavailable,
            ));
        }
        if report.reason() != SandboxReason::NotRequested {
            let reason = if error.kind() == io::ErrorKind::Unsupported {
                SandboxReason::PlatformRunnerUnavailable
            } else {
                SandboxReason::SetupFailed
            };
            return insufficient_enforcement_error(SandboxReport::new(
                SandboxEnforcement::Unavailable,
                report.backend(),
                reason,
            ));
        }
        error
    }

    pub(crate) fn report(&self) -> SandboxReport {
        self.runner.report()
    }
}

struct WorkspaceSandboxPreparation<'a> {
    workspace_root: &'a std::path::Path,
    workspace_capability: Option<&'a crate::WorkspaceRootLaunchCapability>,
    protected_roots: &'a [std::path::PathBuf],
    protected_object_identities: &'a [crate::ProtectedObjectIdentity],
    network_proxy: &'a crate::NetworkProxyPolicy,
    executable_is_pinned: bool,
    executable_identity: Option<&'a crate::ExecutableIdentity>,
    #[cfg(feature = "sandbox-test-fixtures")]
    helper_path: Option<&'a std::path::PathBuf>,
}

fn prepare_workspace_sandbox(preparation: WorkspaceSandboxPreparation<'_>) -> io::Result<PreparedSandbox> {
    if !preparation.executable_is_pinned {
        return Err(sandbox_io_error(SandboxError::ExecutableNotPinned));
    }
    let workspace_capability = preparation.workspace_capability.ok_or_else(|| {
        insufficient_enforcement_error(SandboxReport::new(
            SandboxEnforcement::Unavailable,
            SandboxBackend::None,
            SandboxReason::WorkspaceObjectBindingUnavailable,
        ))
    })?;
    if workspace_capability.path() != preparation.workspace_root {
        return Err(sandbox_io_error(SandboxError::PreparationFailed));
    }
    let layout = validate_layout_with_identities(
        workspace_capability,
        preparation.protected_roots,
        preparation.protected_object_identities,
    )?;
    #[cfg(feature = "sandbox-test-fixtures")]
    let layout = {
        let mut layout = layout;
        layout._test_helper_path = preparation.helper_path.cloned();
        layout
    };
    let runner = prepare_platform_runner(layout, preparation.network_proxy, preparation.executable_identity)?;
    if runner.report().backend() != SandboxBackend::ExternalRunner {
        require_full_enforcement(runner.report())?;
    }
    Ok(PreparedSandbox { runner })
}

fn prepare_platform_runner(
    layout: ResolvedSandboxLayout,
    network_proxy: &crate::NetworkProxyPolicy,
    executable_identity: Option<&crate::ExecutableIdentity>,
) -> io::Result<Box<dyn SandboxRunner>> {
    #[cfg(target_os = "linux")]
    {
        let _ = executable_identity;
        linux::LinuxSandbox::prepare(layout, network_proxy.clone())
            .map(|runner| Box::new(runner) as Box<dyn SandboxRunner>)
    }
    #[cfg(target_os = "macos")]
    {
        let _ = executable_identity;
        macos::MacOsSandbox::prepare(layout, network_proxy.clone())
            .map(|runner| Box::new(runner) as Box<dyn SandboxRunner>)
    }
    #[cfg(windows)]
    {
        let _ = executable_identity;
        windows::WindowsSandbox::prepare(layout, network_proxy.clone())
            .map(|runner| Box::new(runner) as Box<dyn SandboxRunner>)
    }
    #[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
    {
        let executable_identity = executable_identity
            .cloned()
            .ok_or_else(|| sandbox_io_error(SandboxError::ExecutableNotPinned))?;
        external::ExternalSandbox::prepare(layout, network_proxy.clone(), executable_identity)
            .map(|runner| Box::new(runner) as Box<dyn SandboxRunner>)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (layout, network_proxy, executable_identity);
        Err(insufficient_enforcement_error(unavailable_platform_report()))
    }
}

fn require_full_enforcement(report: SandboxReport) -> io::Result<()> {
    if report.enforcement().satisfies_strict_auto() {
        Ok(())
    } else {
        Err(insufficient_enforcement_error(report))
    }
}

#[cfg(any(test, target_os = "linux", target_os = "macos", windows))]
fn cache_confirmed_full(cache: &OnceLock<SandboxReport>, probe: impl FnOnce() -> SandboxReport) -> SandboxReport {
    if let Some(report) = cache.get() {
        return *report;
    }
    let report = probe();
    if report.enforcement() == SandboxEnforcement::Full {
        let _ = cache.set(report);
        return cache.get().copied().unwrap_or(report);
    }
    report
}

fn insufficient_enforcement_error(report: SandboxReport) -> io::Error {
    sandbox_io_error(SandboxError::InsufficientEnforcement { report })
}

fn sandbox_io_error(error: SandboxError) -> io::Error {
    let unavailable = error
        .report()
        .is_some_and(|report| report.enforcement() == SandboxEnforcement::Unavailable);
    let kind = if unavailable {
        io::ErrorKind::Unsupported
    } else {
        io::ErrorKind::PermissionDenied
    };
    io::Error::new(kind, error)
}

struct AmbientRunner;

impl SandboxRunner for AmbientRunner {
    fn report(&self) -> SandboxReport {
        SandboxReport::new(
            SandboxEnforcement::Unavailable,
            SandboxBackend::None,
            SandboxReason::NotRequested,
        )
    }

    fn configure(&mut self, _command: &mut Command) -> io::Result<SandboxCommandDisposition> {
        Ok(SandboxCommandDisposition::Preserved)
    }

    fn verify_before_spawn(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn confirm_started(&mut self, _child: &mut Child) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
use identity::FileIdentity;
#[cfg(test)]
use identity::validate_workspace_object_links;
#[cfg(test)]
use layout::ensure_protected_roots_are_disjoint;
#[cfg(test)]
use layout::validate_layout;

#[cfg(test)]
#[path = "sandbox_test.rs"]
mod sandbox_test;
