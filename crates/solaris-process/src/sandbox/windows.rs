use std::ffi::{OsStr, OsString};
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use tokio::process::{Child, Command};

use super::identity::ProtectedIdentitySnapshot;
use super::layout::{ResolvedSandboxLayout, resolve_path};
use super::windows_acl::{AclGuard, AppContainerProfile, UserObjectAclGuard};
use super::{
    SandboxBackend, SandboxCommandDisposition, SandboxEnforcement, SandboxError, SandboxReason, SandboxReport,
    SandboxRunner, insufficient_enforcement_error, sandbox_io_error, trusted_sandbox_helper,
};
use crate::environment::is_network_proxy_environment_key;
use crate::network_proxy::NetworkProxyPolicy;
use crate::recovery::process_recovery_required;
use crate::runner::{PinnedExecutable, inspect_executable, pin_executable};

#[path = "windows/psec.rs"]
mod psec;
#[path = "windows/psec_codec.rs"]
#[allow(
    dead_code,
    reason = "the packaged PSEC launcher is implemented in the next Windows sandbox phase"
)]
mod psec_codec;

const START_CONFIRMATION_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) fn probe_capability() -> SandboxReport {
    if trusted_sandbox_helper(None, None).is_err() {
        return unavailable_report(SandboxReason::HelperUnavailable);
    }
    match std::thread::Builder::new()
        .name("solaris-windows-sandbox-probe".to_owned())
        .spawn(run_capability_probe)
    {
        Ok(probe) => probe
            .join()
            .unwrap_or_else(|_| partial_report(SandboxReason::StartConfirmationFailed)),
        Err(_) => partial_report(SandboxReason::SetupFailed),
    }
}

fn run_capability_probe() -> SandboxReport {
    let result = (|| -> io::Result<SandboxReport> {
        let workspace = private_directory("solaris-windows-sandbox-probe-")?;
        let packaged_helper = trusted_sandbox_helper(None, None)?;
        let source = packaged_helper.identity().canonical_path();
        let target = workspace.path().join("probe-target.exe");
        std::fs::copy(source, &target).map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?;
        let target = pin_support_executable(&target, SandboxReason::ExecutableNotPinned)?;
        let mut command = target
            .command()
            .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?;
        command
            .arg("--solaris-sandbox-probe-target")
            .current_dir(workspace.path())
            .launch_policy(crate::ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?;
        runtime.block_on(async move {
            let mut child = command.spawn()?;
            let report = child.sandbox_report();
            match tokio::time::timeout(START_CONFIRMATION_TIMEOUT, child.wait()).await {
                Ok(Ok(status)) if status.success() => Ok(report),
                Ok(_) => Ok(partial_report(SandboxReason::StartConfirmationFailed)),
                Err(_) => {
                    let _ = child.kill().await;
                    Ok(partial_report(SandboxReason::StartConfirmationFailed))
                }
            }
        })
    })();
    match result {
        Ok(report) => report,
        Err(error) => error
            .get_ref()
            .and_then(|source| source.downcast_ref::<SandboxError>())
            .and_then(|sandbox| sandbox.report().copied())
            .unwrap_or_else(|| partial_report(SandboxReason::SetupFailed)),
    }
}

pub(super) struct WindowsSandbox {
    // Restore temporary ACL changes before releasing the workspace component
    // handles which prevent path replacement.
    _acl: AclGuard,
    _user_objects: UserObjectAclGuard,
    layout: ResolvedSandboxLayout,
    identities: ProtectedIdentitySnapshot,
    helper: Option<PinnedExecutable>,
    profile: AppContainerProfile,
    private_home: tempfile::TempDir,
    private_tmp: tempfile::TempDir,
    _private_state: tempfile::TempDir,
    start_marker: PathBuf,
    report: SandboxReport,
    configured: bool,
}

impl WindowsSandbox {
    pub(super) fn prepare(layout: ResolvedSandboxLayout, network_policy: NetworkProxyPolicy) -> io::Result<Self> {
        if !network_policy.is_empty() {
            let capability = psec::probe_network_capability();
            debug_assert!(
                !capability.is_fully_proven(),
                "the probe-only PSEC path must not claim complete network enforcement"
            );
            return Err(sandbox_io_error(SandboxError::NetworkProxyUnavailable {
                report: network_proxy_unavailable_report(),
            }));
        }
        #[cfg(feature = "sandbox-test-fixtures")]
        let fixture_helper = layout._test_helper_path.as_deref();
        #[cfg(not(feature = "sandbox-test-fixtures"))]
        let fixture_helper = None;
        let helper = trusted_sandbox_helper(Some(&layout.workspace_root), fixture_helper)?;
        let mut identities =
            ProtectedIdentitySnapshot::capture(&layout.protected_roots, &layout.protected_object_identities)?;
        identities.verify_workspace(&layout.protected_roots, &layout.workspace_root)?;
        let profile = AppContainerProfile::create()
            .map_err(|_| insufficient_enforcement_error(partial_report(SandboxReason::AppContainerUnavailable)))?;
        let user_objects = UserObjectAclGuard::apply(profile.sid())
            .map_err(|_| insufficient_enforcement_error(partial_report(SandboxReason::AppContainerUnavailable)))?;
        let private_home = private_directory("solaris-windows-sandbox-home-")?;
        let private_tmp = private_directory("solaris-windows-sandbox-tmp-")?;
        let private_state = private_directory("solaris-windows-sandbox-state-")?;
        std::fs::create_dir_all(private_home.path().join("AppData").join("Roaming"))
            .and_then(|_| std::fs::create_dir_all(private_home.path().join("AppData").join("Local")))
            .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?;
        ensure_private_roots_are_disjoint(&layout, [private_home.path(), private_tmp.path(), private_state.path()])?;
        let private_roots = [private_home.path().to_path_buf(), private_tmp.path().to_path_buf()];
        let acl = AclGuard::apply(
            &layout.workspace_root,
            &layout.protected_roots,
            &private_roots,
            profile.sid(),
        )
        .map_err(|error| {
            if process_recovery_required(&error).is_some() {
                error
            } else {
                insufficient_enforcement_error(partial_report(SandboxReason::WorkspaceAclUnavailable))
            }
        })?;
        let start_marker = private_state.path().join("ready");
        Ok(Self {
            _acl: acl,
            _user_objects: user_objects,
            layout,
            identities,
            helper: Some(helper),
            profile,
            private_home,
            private_tmp,
            _private_state: private_state,
            start_marker,
            report: full_report(),
            configured: false,
        })
    }

    fn configure_inner(&mut self, command: &mut Command) -> io::Result<()> {
        use windows_sys::Win32::System::Threading::CREATE_SUSPENDED;

        if self.configured {
            return Err(sandbox_io_error(SandboxError::PreparationFailed));
        }
        #[cfg(feature = "sandbox-test-fixtures")]
        if command
            .as_std()
            .get_envs()
            .any(|(key, value)| key == "SOLARIS_SANDBOX_FIXTURE_FAIL_ACL_CLEANUP" && value.is_some())
        {
            self._acl.fail_next_cleanup_for_test();
        }
        let target = resolve_path(Path::new(command.as_std().get_program()))?;
        let target_args = command.as_std().get_args().map(OsStr::to_os_string).collect::<Vec<_>>();
        let current_dir = command
            .as_std()
            .get_current_dir()
            .map(resolve_path)
            .transpose()?
            .unwrap_or_else(|| self.layout.workspace_root.clone());
        if !current_dir.starts_with(&self.layout.workspace_root) {
            return Err(sandbox_io_error(SandboxError::UnrepresentableSandboxLayout));
        }
        let environment = explicit_environment(command, self.private_home.path(), self.private_tmp.path())?;
        let helper = self
            .helper
            .take()
            .ok_or_else(|| sandbox_io_error(SandboxError::PreparationFailed))?;
        let helper_path = helper.windows_execution_path().to_path_buf();
        let mut wrapper = helper
            .command()
            .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?;
        self.helper = Some(wrapper._executable);
        wrapper.command.env_clear().envs(environment);
        for key in ["LOCALAPPDATA", "APPDATA"] {
            if let Some(value) = std::env::var_os(key) {
                wrapper.command.env(key, value);
            }
        }
        wrapper
            .command
            .env(
                "SOLARIS_SANDBOX_CONTROL_TARGET_APPDATA",
                self.private_home.path().join("AppData").join("Roaming"),
            )
            .env(
                "SOLARIS_SANDBOX_CONTROL_TARGET_LOCALAPPDATA",
                self.private_home.path().join("AppData").join("Local"),
            );
        wrapper.command.arg("--windows-appcontainer").arg(
            self.profile
                .sid_string()
                .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?,
        );
        wrapper
            .command
            .arg("--status")
            .arg(&self.start_marker)
            .arg("--current-dir")
            .arg(&current_dir)
            .arg("--")
            .arg(&target)
            .args(target_args)
            .current_dir(&self.layout.workspace_root)
            .creation_flags(CREATE_SUSPENDED);
        if wrapper.command.as_std().get_program() != helper_path {
            return Err(sandbox_io_error(SandboxError::PreparationFailed));
        }
        *command = wrapper.command;
        self.configured = true;
        Ok(())
    }

    fn fail_start<T>(&mut self, reason: SandboxReason) -> io::Result<T> {
        self.report = partial_report(reason);
        Err(insufficient_enforcement_error(self.report))
    }

    fn wait_for_start(&mut self, child: &mut Child) -> io::Result<()> {
        let deadline = Instant::now() + START_CONFIRMATION_TIMEOUT;
        loop {
            match std::fs::read(&self.start_marker) {
                Ok(marker) => match classify_start_confirmation(&marker) {
                    StartConfirmation::Confirmed => return Ok(()),
                    StartConfirmation::Pending => {}
                    StartConfirmation::Invalid => {
                        return self.fail_start(SandboxReason::StartConfirmationFailed);
                    }
                },
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(_) => return self.fail_start(SandboxReason::StartConfirmationFailed),
            }
            match child.try_wait() {
                Ok(Some(_)) | Err(_) => return self.fail_start(SandboxReason::StartConfirmationFailed),
                Ok(None) => {}
            }
            if Instant::now() >= deadline {
                return self.fail_start(SandboxReason::StartConfirmationFailed);
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StartConfirmation {
    Pending,
    Confirmed,
    Invalid,
}

fn classify_start_confirmation(marker: &[u8]) -> StartConfirmation {
    if marker == b"full\n" {
        StartConfirmation::Confirmed
    } else if b"full\n".starts_with(marker) {
        StartConfirmation::Pending
    } else {
        StartConfirmation::Invalid
    }
}

impl SandboxRunner for WindowsSandbox {
    fn report(&self) -> SandboxReport {
        self.report
    }

    fn configure(&mut self, command: &mut Command) -> io::Result<SandboxCommandDisposition> {
        self.configure_inner(command)?;
        Ok(SandboxCommandDisposition::Replaced)
    }

    fn verify_before_spawn(&mut self) -> io::Result<()> {
        self.identities
            .verify_workspace(&self.layout.protected_roots, &self.layout.workspace_root)?;
        Ok(())
    }

    fn confirm_started(&mut self, child: &mut Child) -> io::Result<()> {
        self.wait_for_start(child)
    }

    fn cleanup(&mut self) -> io::Result<()> {
        self._acl.cleanup()
    }
}

fn explicit_environment(
    command: &Command,
    private_home: &Path,
    private_tmp: &Path,
) -> io::Result<Vec<(OsString, OsString)>> {
    let mut environment = command
        .as_std()
        .get_envs()
        .filter_map(|(key, value)| value.map(|value| (key.to_os_string(), value.to_os_string())))
        .filter(|(key, _)| {
            !is_network_proxy_environment_key(key)
                && !matches!(
                    key.to_string_lossy().to_ascii_uppercase().as_str(),
                    "HOME" | "USERPROFILE" | "TMP" | "TMPDIR" | "TEMP" | "SYSTEMROOT" | "WINDIR"
                )
        })
        .collect::<Vec<_>>();
    environment.extend([
        (OsString::from("HOME"), private_home.as_os_str().to_os_string()),
        (OsString::from("USERPROFILE"), private_home.as_os_str().to_os_string()),
        (OsString::from("TMP"), private_tmp.as_os_str().to_os_string()),
        (OsString::from("TMPDIR"), private_tmp.as_os_str().to_os_string()),
        (OsString::from("TEMP"), private_tmp.as_os_str().to_os_string()),
    ]);
    for key in ["SystemRoot", "WINDIR"] {
        if let Some(value) = std::env::var_os(key) {
            environment.push((OsString::from(key), value));
        }
    }
    for (key, value) in &environment {
        if key.to_string_lossy().contains('=')
            || key.to_string_lossy().contains('\0')
            || value.to_string_lossy().contains('\0')
        {
            return Err(sandbox_io_error(SandboxError::PreparationFailed));
        }
    }
    Ok(environment)
}

fn private_directory(prefix: &str) -> io::Result<tempfile::TempDir> {
    tempfile::Builder::new()
        .prefix(prefix)
        .tempdir()
        .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))
}

fn ensure_private_roots_are_disjoint<'a>(
    layout: &ResolvedSandboxLayout,
    roots: impl IntoIterator<Item = &'a Path>,
) -> io::Result<()> {
    let roots = roots.into_iter().map(resolve_path).collect::<io::Result<Vec<_>>>()?;
    if roots.iter().any(|root| {
        root.starts_with(&layout.workspace_root)
            || layout.workspace_root.starts_with(root)
            || layout
                .protected_roots
                .iter()
                .any(|protected| root.starts_with(protected) || protected.starts_with(root))
    }) {
        return Err(sandbox_io_error(SandboxError::UnrepresentableSandboxLayout));
    }
    Ok(())
}

fn pin_support_executable(path: &Path, reason: SandboxReason) -> io::Result<PinnedExecutable> {
    let identity = inspect_executable(path).map_err(|_| insufficient_enforcement_error(unavailable_report(reason)))?;
    pin_executable(path, &identity).map_err(|_| insufficient_enforcement_error(unavailable_report(reason)))
}

fn full_report() -> SandboxReport {
    SandboxReport::new(
        SandboxEnforcement::Full,
        SandboxBackend::WindowsAppContainer,
        SandboxReason::Enforced,
    )
}

const fn network_proxy_unavailable_report() -> SandboxReport {
    SandboxReport::new(
        SandboxEnforcement::Unavailable,
        SandboxBackend::WindowsAppContainer,
        SandboxReason::NetworkProxyUnavailable,
    )
}

fn unavailable_report(reason: SandboxReason) -> SandboxReport {
    SandboxReport::new(SandboxEnforcement::Unavailable, SandboxBackend::None, reason)
}

fn partial_report(reason: SandboxReason) -> SandboxReport {
    SandboxReport::new(SandboxEnforcement::Partial, SandboxBackend::WindowsAppContainer, reason)
}

#[cfg(test)]
#[path = "windows_test.rs"]
mod windows_test;
