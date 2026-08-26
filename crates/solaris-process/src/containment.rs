use std::io::{Error, Result};

use tokio::process::{Child, Command};

use crate::sandbox::SandboxCommandDisposition;
#[cfg(unix)]
use crate::unix_guardian::{GuardianControl, GuardianLaunch};
#[cfg(windows)]
use crate::windows_job::JobObject;

pub(crate) struct ChildContainment {
    #[cfg(unix)]
    guardian: Option<GuardianControl>,
    #[cfg(windows)]
    job: JobObject,
}

pub(crate) struct ContainmentLaunchGuard {
    #[cfg(unix)]
    guardian: Option<GuardianLaunch>,
}

impl ChildContainment {
    pub(crate) fn configure(command: &mut Command) -> Result<ContainmentLaunchGuard> {
        configure_command(command);
        ContainmentLaunchGuard::prepare(command)
    }

    #[cfg(unix)]
    pub(crate) fn configure_process_group(command: &mut Command) {
        configure_command(command);
    }

    pub(crate) fn attach(child: &mut Child, launch: ContainmentLaunchGuard) -> Result<Self> {
        attach_child(child, launch)
    }

    pub(crate) fn release_target(&mut self) -> Result<()> {
        #[cfg(unix)]
        if let Some(guardian) = self.guardian.as_mut() {
            guardian.release_target()?;
        }
        Ok(())
    }

    pub(crate) fn terminate(&mut self, child: &mut Child, child_id: Option<u32>) -> Result<()> {
        terminate_child(child, child_id, self)
    }

    pub(crate) fn terminate_for_recovery(&mut self, child: &mut Child, child_id: Option<u32>) -> Result<()> {
        terminate_child_for_recovery(child, child_id, self)
    }

    pub(crate) fn is_drained(&mut self, child: &mut Child, child_id: Option<u32>) -> Result<bool> {
        containment_is_drained(child, child_id, self)
    }

    pub(crate) fn finalize_after_drain(&mut self) -> Result<()> {
        #[cfg(unix)]
        if let Some(guardian) = self.guardian.as_mut() {
            guardian.finalize()?;
        }
        Ok(())
    }

    #[cfg(unix)]
    pub(crate) async fn wait_root_exit(&mut self) -> Result<()> {
        self.guardian
            .as_mut()
            .ok_or_else(|| Error::other("plain process-group containment has no guardian"))?
            .wait_target_exit()
            .await
    }

    #[cfg(unix)]
    pub(crate) fn has_guardian(&self) -> bool {
        self.guardian.is_some()
    }

    #[cfg(unix)]
    pub(crate) fn root_has_exited(&mut self, child: &mut Child) -> Result<bool> {
        let Some(guardian) = self.guardian.as_mut() else {
            return Ok(true);
        };
        if guardian.control_is_lost() {
            return child.try_wait().map(|status| status.is_some());
        }
        guardian.target_exited()
    }
}

impl ContainmentLaunchGuard {
    #[cfg(unix)]
    fn prepare(_command: &mut Command) -> Result<Self> {
        Ok(Self { guardian: None })
    }

    #[cfg(not(unix))]
    fn prepare(_command: &mut Command) -> Result<Self> {
        Ok(Self {})
    }

    pub(crate) fn ensure_final_command(
        &mut self,
        command: &mut Command,
        disposition: SandboxCommandDisposition,
        verified_drain: bool,
    ) -> Result<()> {
        #[cfg(unix)]
        {
            let _ = disposition;
            self.guardian = if verified_drain {
                Some(GuardianLaunch::prepare(command, true)?)
            } else {
                None
            };
            configure_command(command);
        }
        #[cfg(not(unix))]
        {
            let _ = verified_drain;
            if disposition == SandboxCommandDisposition::Replaced {
                *self = Self::prepare(command)?;
                configure_command(command);
            }
        }
        Ok(())
    }
}

impl Drop for ChildContainment {
    fn drop(&mut self) {}
}

#[cfg(unix)]
fn configure_command(command: &mut Command) {
    command.process_group(0);
}

#[cfg(windows)]
fn configure_command(command: &mut Command) {
    use windows_sys::Win32::System::Threading::CREATE_SUSPENDED;

    command.creation_flags(CREATE_SUSPENDED);
}

#[cfg(not(any(unix, windows)))]
fn configure_command(_command: &mut Command) {}

#[cfg(windows)]
fn attach_child(child: &mut Child, _launch: ContainmentLaunchGuard) -> Result<ChildContainment> {
    let pid = child.id().ok_or_else(|| Error::other("spawned child has no pid"))?;
    let raw_handle = child
        .raw_handle()
        .ok_or_else(|| Error::other("spawned child has no raw handle"))?;

    let job = JobObject::assign_and_resume(raw_handle, pid)
        .map_err(|error| Error::other(format!("windows job containment failed: {error}")))?;

    Ok(ChildContainment { job })
}

#[cfg(unix)]
fn attach_child(child: &mut Child, mut launch: ContainmentLaunchGuard) -> Result<ChildContainment> {
    let guardian = launch
        .guardian
        .take()
        .map(|guardian| guardian.attach_before_release(child))
        .transpose()?;
    Ok(ChildContainment { guardian })
}

#[cfg(not(any(unix, windows)))]
fn attach_child(_child: &mut Child, launch: ContainmentLaunchGuard) -> Result<ChildContainment> {
    let _ = launch;
    Ok(ChildContainment {})
}

#[cfg(unix)]
fn terminate_child_for_recovery(
    child: &mut Child,
    child_id: Option<u32>,
    containment: &mut ChildContainment,
) -> Result<()> {
    terminate_unix_child(child, child_id, containment)
}

#[cfg(windows)]
fn terminate_child_for_recovery(
    child: &mut Child,
    child_id: Option<u32>,
    containment: &mut ChildContainment,
) -> Result<()> {
    terminate_child(child, child_id, containment)
}

#[cfg(not(any(unix, windows)))]
fn terminate_child_for_recovery(
    child: &mut Child,
    child_id: Option<u32>,
    containment: &mut ChildContainment,
) -> Result<()> {
    terminate_child(child, child_id, containment)
}

#[cfg(unix)]
fn terminate_child(_child: &mut Child, _child_id: Option<u32>, containment: &mut ChildContainment) -> Result<()> {
    terminate_unix_child(_child, _child_id, containment)
}

#[cfg(unix)]
fn terminate_unix_child(child: &mut Child, child_id: Option<u32>, containment: &mut ChildContainment) -> Result<()> {
    let Some(guardian) = containment.guardian.as_mut() else {
        return signal_process_group(child_id);
    };
    if !guardian.target_was_released() {
        return child.start_kill();
    }
    if guardian.control_is_lost() {
        // Dropping the Host side of the control socket is the guardian's
        // fail-safe termination request. Let the guardian kill and reap the
        // target group, then observe the guardian child exit. Killing the
        // guardian here could orphan a target on Unix systems without
        // PR_SET_PDEATHSIG.
        return Ok(());
    }
    guardian.terminate()
}

#[cfg(unix)]
fn containment_is_drained(
    child: &mut Child,
    child_id: Option<u32>,
    containment: &mut ChildContainment,
) -> Result<bool> {
    let Some(guardian) = containment.guardian.as_mut() else {
        return process_group_is_drained(child_id);
    };
    if guardian.control_is_lost() {
        return child.try_wait().map(|status| status.is_some());
    }
    guardian.is_drained()
}

#[cfg(windows)]
fn containment_is_drained(
    _child: &mut Child,
    _child_id: Option<u32>,
    containment: &mut ChildContainment,
) -> Result<bool> {
    containment.job.is_empty()
}

#[cfg(not(any(unix, windows)))]
fn containment_is_drained(
    _child: &mut Child,
    _child_id: Option<u32>,
    _containment: &mut ChildContainment,
) -> Result<bool> {
    Ok(true)
}

#[cfg(windows)]
fn terminate_child(_child: &mut Child, _child_id: Option<u32>, containment: &mut ChildContainment) -> Result<()> {
    containment.job.terminate()
}

#[cfg(not(any(unix, windows)))]
fn terminate_child(child: &mut Child, _child_id: Option<u32>, _containment: &mut ChildContainment) -> Result<()> {
    child.start_kill()
}

#[cfg(unix)]
fn signal_process_group(child_id: Option<u32>) -> Result<()> {
    let pid = child_id
        .and_then(|pid| libc::pid_t::try_from(pid).ok())
        .filter(|pid| *pid > 1)
        .ok_or_else(|| Error::other("spawned child has no valid process-group id"))?;
    let result = unsafe { libc::kill(-pid, libc::SIGKILL) };
    if result == 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(error)
    }
}

#[cfg(unix)]
fn process_group_is_drained(child_id: Option<u32>) -> Result<bool> {
    let pid = child_id
        .and_then(|pid| libc::pid_t::try_from(pid).ok())
        .filter(|pid| *pid > 1)
        .ok_or_else(|| Error::other("spawned child has no valid process-group id"))?;
    let result = unsafe { libc::kill(-pid, 0) };
    if result == 0 {
        return Ok(false);
    }
    let error = std::io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::ESRCH) => Ok(true),
        Some(libc::EPERM) => Ok(false),
        _ => Err(error),
    }
}

#[cfg(test)]
#[path = "containment_test.rs"]
mod containment_test;
