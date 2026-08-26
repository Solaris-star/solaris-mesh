use std::ffi::OsStr;
use std::io::Result;
use std::path::Path;
use std::process::Stdio;
use std::time::{Duration, Instant};

use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};

use crate::containment::ChildContainment;
use crate::environment::{configure_explicit_process_environment, configure_safe_process_environment_with_overrides};
use crate::executable::ExecutableIdentity;
use crate::launch_policy::ProcessLaunchPolicy;
use crate::process_finalization::{FinalizationFailures, ProcessFinalizationStage};
use crate::recovery::{
    ProcessRecovery, ProcessRecoveryKind, ProcessRecoveryRecord, ProcessRecoveryState, recovery_required_error,
    register_process_recovery, retry_process_recovery,
};
use crate::runner::PinnedExecutable;
use crate::sandbox::{PreparedSandbox, SandboxReport};
use crate::spawn_authorization::ProcessSpawnAuthorization;

#[derive(Debug)]
pub struct PinnedCommand {
    pub(crate) command: Command,
    pub(crate) _executable: PinnedExecutable,
    pub(crate) launch_policy: ProcessLaunchPolicy,
    pub(crate) spawn_authorizer: Option<ProcessSpawnAuthorization>,
    pub(crate) stdin: Option<Stdio>,
    pub(crate) stdout: Option<Stdio>,
    pub(crate) stderr: Option<Stdio>,
}

impl PinnedCommand {
    pub fn executable_identity(&self) -> &ExecutableIdentity {
        self._executable.identity()
    }

    pub fn arg<S: AsRef<OsStr>>(&mut self, arg: S) -> &mut Self {
        self.command.arg(arg);
        self
    }

    pub fn args<I, S>(&mut self, args: I) -> &mut Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.command.args(args);
        self
    }

    pub fn env<K, V>(&mut self, key: K, value: V) -> &mut Self
    where
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        configure_explicit_process_environment(&mut self.command, [(key, value)]);
        self
    }

    pub fn envs<I, K, V>(&mut self, vars: I) -> &mut Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        configure_explicit_process_environment(&mut self.command, vars);
        self
    }

    /// Rebuilds the allowlisted baseline environment and applies only validated
    /// resource-limit or absolute private-temp overrides. Use [`Self::env`] or
    /// [`Self::envs`] afterwards for business values authorized for one process.
    pub fn reset_to_safe_environment_with_overrides<I, K, V>(&mut self, overrides: I) -> &mut Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        configure_safe_process_environment_with_overrides(&mut self.command, overrides);
        self
    }

    pub fn current_dir<P: AsRef<Path>>(&mut self, dir: P) -> &mut Self {
        self.command.current_dir(dir);
        self
    }

    pub fn stdin<T: Into<Stdio>>(&mut self, cfg: T) -> &mut Self {
        self.stdin = Some(cfg.into());
        self
    }

    pub fn stdout<T: Into<Stdio>>(&mut self, cfg: T) -> &mut Self {
        self.stdout = Some(cfg.into());
        self
    }

    pub fn stderr<T: Into<Stdio>>(&mut self, cfg: T) -> &mut Self {
        self.stderr = Some(cfg.into());
        self
    }

    pub fn kill_on_drop(&mut self, kill_on_drop: bool) -> &mut Self {
        self.command.kill_on_drop(kill_on_drop);
        self
    }

    pub fn launch_policy(&mut self, policy: ProcessLaunchPolicy) -> &mut Self {
        self.launch_policy = policy;
        self
    }

    /// Installs an effect-scoped check that runs immediately around creation of
    /// the retained executable process.
    pub fn spawn_authorizer(&mut self, authorizer: ProcessSpawnAuthorization) -> &mut Self {
        self.spawn_authorizer = Some(authorizer);
        self
    }

    pub fn spawn(self) -> Result<ManagedChild> {
        let policy = self.launch_policy.clone();
        self.spawn_with_policy(&policy)
    }

    pub(crate) fn spawn_with_policy(self, policy: &ProcessLaunchPolicy) -> Result<ManagedChild> {
        let spawn_authorizer = self.spawn_authorizer.clone();
        let Some(authorizer) = spawn_authorizer else {
            return ManagedChild::spawn_pinned_command_with_policy(self, policy);
        };
        authorizer.authorize_and_spawn(Box::new(move |policy| {
            ManagedChild::spawn_pinned_command_with_policy(self, &policy)
        }))
    }

    fn apply_deferred_stdio(&mut self) {
        if let Some(stdin) = self.stdin.take() {
            self.command.stdin(stdin);
        }
        if let Some(stdout) = self.stdout.take() {
            self.command.stdout(stdout);
        }
        if let Some(stderr) = self.stderr.take() {
            self.command.stderr(stderr);
        }
    }

    #[cfg(all(test, unix))]
    pub(crate) fn program_for_test(&self) -> &OsStr {
        self.command.as_std().get_program()
    }
}

/// A spawned child whose process-tree containment remains active for its
/// complete lifetime.
pub struct ManagedChild {
    child: Option<Child>,
    containment: Option<ChildContainment>,
    child_id: Option<u32>,
    sandbox: Option<PreparedSandbox>,
    sandbox_report: SandboxReport,
    finalized: bool,
    root_status: Option<std::process::ExitStatus>,
    root_exit_observed: bool,
    #[cfg(feature = "sandbox-test-fixtures")]
    test_failures: ProcessFailureFixtures,
}

#[cfg(feature = "sandbox-test-fixtures")]
#[derive(Default)]
struct ProcessFailureFixtures {
    containment_attach: bool,
    stdin: bool,
    terminate: bool,
    wait: bool,
    post_spawn_termination_unknown: bool,
    recovery_termination_unknown: bool,
}

impl ManagedChild {
    pub(crate) fn spawn_command(command: Command) -> Result<Self> {
        Self::spawn_command_with_policy_kind(command, &ProcessLaunchPolicy::Ambient, false)
    }

    pub(crate) fn spawn_command_with_policy(command: Command, policy: &ProcessLaunchPolicy) -> Result<Self> {
        Self::spawn_command_with_policy_kind(command, policy, false)
    }

    fn spawn_pinned_command_with_policy(mut pinned: PinnedCommand, policy: &ProcessLaunchPolicy) -> Result<Self> {
        let mut sandbox = PreparedSandbox::prepare_for_pinned_executable(policy, pinned._executable.identity())?;
        let mut containment_launch = ChildContainment::configure(&mut pinned.command)
            .map_err(|error| cleanup_after_error(&mut sandbox, ProcessFinalizationStage::Spawn, error))?;
        let disposition = sandbox
            .configure(&mut pinned.command)
            .map_err(|error| cleanup_after_error(&mut sandbox, ProcessFinalizationStage::Spawn, error))?;
        let verified_drain = sandbox.requires_verified_process_tree_drain();
        if let Err(error) = containment_launch.ensure_final_command(&mut pinned.command, disposition, verified_drain) {
            let error = sandbox.map_containment_error(error);
            return Err(cleanup_after_error(
                &mut sandbox,
                ProcessFinalizationStage::Spawn,
                error,
            ));
        }
        pinned.apply_deferred_stdio();
        Self::spawn_prepared_command(pinned.command, sandbox, containment_launch)
    }

    fn spawn_command_with_policy_kind(
        mut command: Command,
        policy: &ProcessLaunchPolicy,
        executable_is_pinned: bool,
    ) -> Result<Self> {
        let mut sandbox = PreparedSandbox::prepare_for_executable(policy, executable_is_pinned)?;
        let mut containment_launch = ChildContainment::configure(&mut command)
            .map_err(|error| cleanup_after_error(&mut sandbox, ProcessFinalizationStage::Spawn, error))?;
        let disposition = sandbox
            .configure(&mut command)
            .map_err(|error| cleanup_after_error(&mut sandbox, ProcessFinalizationStage::Spawn, error))?;
        let verified_drain = sandbox.requires_verified_process_tree_drain();
        if let Err(error) = containment_launch.ensure_final_command(&mut command, disposition, verified_drain) {
            let error = sandbox.map_containment_error(error);
            return Err(cleanup_after_error(
                &mut sandbox,
                ProcessFinalizationStage::Spawn,
                error,
            ));
        }
        Self::spawn_prepared_command(command, sandbox, containment_launch)
    }

    fn spawn_prepared_command(
        mut command: Command,
        mut sandbox: PreparedSandbox,
        containment_launch: crate::containment::ContainmentLaunchGuard,
    ) -> Result<Self> {
        #[cfg(feature = "sandbox-test-fixtures")]
        let mut test_failures = ProcessFailureFixtures::from_command(&command);
        #[cfg(feature = "sandbox-test-fixtures")]
        let fail_after_spawn = command_has_fixture(&command, "SOLARIS_SANDBOX_FIXTURE_FAIL_AFTER_SPAWN");
        command.kill_on_drop(true);
        if let Err(error) = sandbox.verify_before_spawn() {
            return Err(cleanup_after_error(
                &mut sandbox,
                ProcessFinalizationStage::Spawn,
                error,
            ));
        }
        #[cfg(feature = "sandbox-test-fixtures")]
        if command_has_fixture(&command, "SOLARIS_SANDBOX_FIXTURE_FAIL_SPAWN") {
            return Err(cleanup_after_error(
                &mut sandbox,
                ProcessFinalizationStage::Spawn,
                std::io::Error::other("injected process spawn failure"),
            ));
        }
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                let error = sandbox.map_spawn_error(error);
                return Err(cleanup_after_error(
                    &mut sandbox,
                    ProcessFinalizationStage::Spawn,
                    error,
                ));
            }
        };
        // The command owns pre-exec descriptor leases. The child inherited
        // those descriptors during spawn, so close the parent's copies before
        // a sandbox drain proof waits for EOF.
        drop(command);
        let child_id = child.id();
        #[cfg(feature = "sandbox-test-fixtures")]
        if std::mem::take(&mut test_failures.containment_attach) {
            return Err(finish_started_child_failure(
                child,
                None,
                child_id,
                sandbox,
                std::io::Error::other("injected containment attach failure"),
                &mut test_failures,
            ));
        }
        let mut containment = match ChildContainment::attach(&mut child, containment_launch) {
            Ok(containment) => containment,
            Err(error) => {
                let error = sandbox.map_containment_error(error);
                return Err(finish_started_child_failure(
                    child,
                    None,
                    child_id,
                    sandbox,
                    error,
                    #[cfg(feature = "sandbox-test-fixtures")]
                    &mut test_failures,
                ));
            }
        };
        if let Err(error) = containment.release_target() {
            let error = sandbox.map_containment_error(error);
            return Err(finish_started_child_failure(
                child,
                Some(containment),
                child_id,
                sandbox,
                error,
                #[cfg(feature = "sandbox-test-fixtures")]
                &mut test_failures,
            ));
        }
        #[cfg(feature = "sandbox-test-fixtures")]
        if fail_after_spawn {
            return Err(finish_started_child_failure(
                child,
                Some(containment),
                child_id,
                sandbox,
                std::io::Error::other("injected failure after process spawn"),
                &mut test_failures,
            ));
        }
        if let Err(error) = sandbox.confirm_started(&mut child) {
            return Err(finish_started_child_failure(
                child,
                Some(containment),
                child_id,
                sandbox,
                error,
                #[cfg(feature = "sandbox-test-fixtures")]
                &mut test_failures,
            ));
        }
        let sandbox_report = sandbox.report();
        Ok(Self {
            child: Some(child),
            containment: Some(containment),
            child_id,
            sandbox: Some(sandbox),
            sandbox_report,
            finalized: false,
            root_status: None,
            root_exit_observed: false,
            #[cfg(feature = "sandbox-test-fixtures")]
            test_failures,
        })
    }

    pub fn id(&self) -> Option<u32> {
        self.child.as_ref().and_then(Child::id)
    }

    pub fn sandbox_report(&self) -> SandboxReport {
        self.sandbox_report
    }

    pub fn take_stdin(&mut self) -> Option<ChildStdin> {
        self.child_mut().stdin.take()
    }

    pub fn take_stdout(&mut self) -> Option<ChildStdout> {
        self.child_mut().stdout.take()
    }

    pub fn take_stderr(&mut self) -> Option<ChildStderr> {
        self.child_mut().stderr.take()
    }

    pub(crate) async fn wait_root_ready(&mut self) -> Result<()> {
        if self.root_status.is_some() || self.root_exit_observed {
            return Ok(());
        }
        #[cfg(unix)]
        if let Some(containment) = self.containment.as_mut()
            && containment.has_guardian()
        {
            containment.wait_root_exit().await?;
            self.root_exit_observed = true;
            return Ok(());
        }
        self.root_status = Some(self.child_mut().wait().await?);
        Ok(())
    }

    pub(crate) async fn reap_root(&mut self) -> Result<std::process::ExitStatus> {
        let result = match self.root_status.take() {
            Some(status) => Ok(status),
            None => self.child_mut().wait().await,
        };
        self.root_exit_observed = false;
        #[cfg(feature = "sandbox-test-fixtures")]
        if result.is_ok() && std::mem::take(&mut self.test_failures.wait) {
            return Err(std::io::Error::other("injected process wait failure"));
        }
        result
    }

    pub(crate) async fn terminate_tree_and_confirm(&mut self, timeout: Duration) -> Result<()> {
        self.terminate_tree()?;
        let Some(containment) = self.containment.as_mut() else {
            return Ok(());
        };
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let child = self.child.as_mut().expect("managed child still owns its process");
            #[cfg(unix)]
            let root_exited = containment.root_has_exited(child)?;
            #[cfg(not(unix))]
            let root_exited = true;
            if root_exited && containment.is_drained(child, self.child_id)? {
                let sandbox_drained = self
                    .sandbox
                    .as_mut()
                    .map_or(Ok(true), PreparedSandbox::process_tree_is_drained)?;
                if sandbox_drained {
                    containment.finalize_after_drain()?;
                    return Ok(());
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "process tree drain could not be confirmed",
                ));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    pub(crate) fn terminate_tree(&mut self) -> Result<()> {
        let child = self.child.as_mut().expect("managed child still owns its process");
        let result = if let Some(containment) = self.containment.as_mut() {
            containment.terminate(child, self.child_id)?;
            Ok(())
        } else {
            child.start_kill()
        };
        apply_termination_failure_fixture(
            result,
            #[cfg(feature = "sandbox-test-fixtures")]
            &mut self.test_failures,
        )
    }

    pub(crate) fn cleanup_sandbox(&mut self) -> Result<()> {
        let result = self.sandbox.as_mut().map_or(Ok(()), PreparedSandbox::cleanup);
        if result.is_ok() {
            self.finalized = true;
        }
        result
    }

    pub(crate) fn finalize_sandbox(
        &mut self,
        tree_drain_confirmed: bool,
        failures: &mut FinalizationFailures,
    ) -> Result<()> {
        if tree_drain_confirmed {
            self.containment.take();
            return self.cleanup_sandbox();
        }
        let Some(recovery) = self.take_pending_cleanup() else {
            return Ok(());
        };
        retain_pending_cleanup(recovery, failures);
        Ok(())
    }

    fn take_pending_cleanup(&mut self) -> Option<PendingStartedChildCleanup> {
        if self.finalized {
            return None;
        }
        let child = self.child.take()?;
        let sandbox = self
            .sandbox
            .take()
            .expect("a managed child retaining its process also retains its sandbox");
        Some(PendingStartedChildCleanup::new(
            child,
            self.containment.take(),
            self.child_id,
            sandbox,
            #[cfg(feature = "sandbox-test-fixtures")]
            std::mem::take(&mut self.test_failures.recovery_termination_unknown),
        ))
    }

    fn child_mut(&mut self) -> &mut Child {
        self.child.as_mut().expect("managed child still owns its process")
    }

    #[cfg(feature = "sandbox-test-fixtures")]
    pub(crate) fn apply_stdin_failure_fixture(&mut self, result: Result<()>) -> Result<()> {
        if result.is_ok() && std::mem::take(&mut self.test_failures.stdin) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "injected process stdin writer failure",
            ));
        }
        result
    }

    pub async fn kill(&mut self) -> Result<()> {
        let mut failures = FinalizationFailures::new();
        let terminated = failures.record(
            ProcessFinalizationStage::Terminate,
            self.terminate_tree_and_confirm(STARTED_CHILD_EXIT_CONFIRMATION_TIMEOUT)
                .await,
        );
        if terminated {
            failures.record(ProcessFinalizationStage::Wait, self.reap_root().await.map(|_| ()));
        }
        let cleanup = self.finalize_sandbox(terminated, &mut failures);
        failures.finish(cleanup, ())
    }

    pub async fn wait(&mut self) -> Result<std::process::ExitStatus> {
        let mut failures = FinalizationFailures::new();
        let ready = self.wait_root_ready().await;
        let terminated = failures.record(
            ProcessFinalizationStage::Terminate,
            self.terminate_tree_and_confirm(STARTED_CHILD_EXIT_CONFIRMATION_TIMEOUT)
                .await,
        );
        let status = if terminated {
            let status_result = match ready {
                Ok(()) => self.reap_root().await,
                Err(error) => Err(error),
            };
            match status_result {
                Ok(status) => Some(status),
                Err(error) => {
                    failures.record_error(ProcessFinalizationStage::Wait, error);
                    None
                }
            }
        } else {
            if let Err(error) = ready {
                failures.record_error(ProcessFinalizationStage::Wait, error);
            }
            None
        };
        let cleanup = self.finalize_sandbox(terminated, &mut failures);
        let status = failures.finish(cleanup, status)?;
        status.ok_or_else(|| std::io::Error::other("process wait returned no status or error"))
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        self.kill().await
    }
}

fn cleanup_after_error(
    sandbox: &mut PreparedSandbox,
    stage: ProcessFinalizationStage,
    error: std::io::Error,
) -> std::io::Error {
    let mut failures = FinalizationFailures::new();
    failures.record_error(stage, error);
    failures
        .finish(sandbox.cleanup(), ())
        .expect_err("a primary process failure was recorded")
}

const STARTED_CHILD_EXIT_CONFIRMATION_TIMEOUT: Duration = Duration::from_secs(5);

struct PendingStartedChildCleanup {
    child: Option<Child>,
    containment: Option<ChildContainment>,
    child_id: Option<u32>,
    sandbox: Option<PreparedSandbox>,
    #[cfg(feature = "sandbox-test-fixtures")]
    recovery_termination_unknown: bool,
}

impl PendingStartedChildCleanup {
    fn new(
        child: Child,
        containment: Option<ChildContainment>,
        child_id: Option<u32>,
        sandbox: PreparedSandbox,
        #[cfg(feature = "sandbox-test-fixtures")] recovery_termination_unknown: bool,
    ) -> Self {
        Self {
            child: Some(child),
            containment,
            child_id,
            sandbox: Some(sandbox),
            #[cfg(feature = "sandbox-test-fixtures")]
            recovery_termination_unknown,
        }
    }

    fn terminate_inner(&mut self) -> Result<()> {
        let child = self.child.as_mut().expect("pending cleanup owns its child");
        if let Some(containment) = self.containment.as_mut() {
            containment.terminate(child, self.child_id)
        } else {
            child.start_kill()
        }
    }

    fn terminate_for_recovery(&mut self) -> Result<()> {
        let child = self.child.as_mut().expect("pending cleanup owns its child");
        if let Some(containment) = self.containment.as_mut() {
            containment.terminate_for_recovery(child, self.child_id)
        } else {
            child.start_kill()
        }
    }

    fn wait_for_exit(&mut self, timeout: Duration) -> Result<bool> {
        let deadline = Instant::now() + timeout;
        loop {
            if self.exit_and_containment_are_confirmed()? {
                return Ok(true);
            }
            if Instant::now() >= deadline {
                return Ok(false);
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn cleanup_after_confirmed_exit(&mut self) -> Result<()> {
        if !self.exit_and_containment_are_confirmed()? {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "sandbox cleanup requires confirmed child and containment exit",
            ));
        }
        self.containment.take();
        self.sandbox
            .as_mut()
            .expect("pending cleanup owns its sandbox")
            .cleanup()?;
        self.sandbox.take();
        self.child.take();
        Ok(())
    }

    fn exit_and_containment_are_confirmed(&mut self) -> Result<bool> {
        let child = self.child.as_mut().expect("pending cleanup owns its child");
        #[cfg(unix)]
        let root_exited = match self.containment.as_mut() {
            Some(containment) => containment.root_has_exited(child)?,
            None => child.try_wait()?.is_some(),
        };
        #[cfg(not(unix))]
        let root_exited = child.try_wait()?.is_some();
        if !root_exited {
            return Ok(false);
        }
        let drained = match self.containment.as_mut() {
            Some(containment) => containment.is_drained(child, self.child_id),
            None => Ok(true),
        }?;
        if !drained {
            return Ok(false);
        }
        let sandbox_drained = self
            .sandbox
            .as_mut()
            .expect("pending cleanup owns its sandbox")
            .process_tree_is_drained()?;
        if !sandbox_drained {
            return Ok(false);
        }
        if let Some(containment) = self.containment.as_mut() {
            containment.finalize_after_drain()?;
        }
        #[cfg(unix)]
        if child.try_wait()?.is_none() {
            return Ok(false);
        }
        Ok(true)
    }
}

impl ProcessRecovery for PendingStartedChildCleanup {
    fn kind(&self) -> ProcessRecoveryKind {
        ProcessRecoveryKind::StartedChild
    }

    fn retry(&mut self) -> Result<bool> {
        if self.child.is_none() || self.sandbox.is_none() {
            return Ok(true);
        }
        #[cfg(feature = "sandbox-test-fixtures")]
        let termination = if std::mem::take(&mut self.recovery_termination_unknown) {
            Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "injected recovery termination outcome unknown",
            ))
        } else {
            self.terminate_for_recovery()
        };
        #[cfg(not(feature = "sandbox-test-fixtures"))]
        let termination = self.terminate_for_recovery();
        match self.wait_for_exit(Duration::ZERO) {
            Ok(true) => {
                self.cleanup_after_confirmed_exit()?;
                Ok(true)
            }
            Ok(false) => match termination {
                Ok(()) => Ok(false),
                Err(error) => Err(error),
            },
            Err(error) => Err(error),
        }
    }
}

fn finish_started_child_failure(
    child: Child,
    containment: Option<ChildContainment>,
    child_id: Option<u32>,
    sandbox: PreparedSandbox,
    primary_error: std::io::Error,
    #[cfg(feature = "sandbox-test-fixtures")] test_failures: &mut ProcessFailureFixtures,
) -> std::io::Error {
    let mut failures = FinalizationFailures::new();
    failures.record_error(ProcessFinalizationStage::Spawn, primary_error);
    let mut recovery = PendingStartedChildCleanup::new(
        child,
        containment,
        child_id,
        sandbox,
        #[cfg(feature = "sandbox-test-fixtures")]
        std::mem::take(&mut test_failures.recovery_termination_unknown),
    );
    #[cfg(feature = "sandbox-test-fixtures")]
    let termination_was_not_attempted = std::mem::take(&mut test_failures.post_spawn_termination_unknown);
    #[cfg(not(feature = "sandbox-test-fixtures"))]
    let termination_was_not_attempted = false;
    let termination = if termination_was_not_attempted {
        Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "injected post-spawn termination outcome unknown",
        ))
    } else {
        apply_termination_failure_fixture(
            recovery.terminate_inner(),
            #[cfg(feature = "sandbox-test-fixtures")]
            test_failures,
        )
    };
    failures.record(ProcessFinalizationStage::Terminate, termination);
    let confirmation_timeout = if termination_was_not_attempted {
        Duration::ZERO
    } else {
        STARTED_CHILD_EXIT_CONFIRMATION_TIMEOUT
    };
    match recovery.wait_for_exit(confirmation_timeout) {
        Ok(true) => match recovery.cleanup_after_confirmed_exit() {
            Ok(()) => failures
                .finish(Ok(()), ())
                .expect_err("a started-child failure was recorded"),
            Err(cleanup_error) => retain_started_child_recovery(recovery, failures, Some(cleanup_error)),
        },
        Ok(false) => {
            failures.record_error(
                ProcessFinalizationStage::Wait,
                std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "spawned child exit could not be confirmed",
                ),
            );
            retain_started_child_recovery(recovery, failures, None)
        }
        Err(error) => {
            failures.record_error(ProcessFinalizationStage::Wait, error);
            retain_started_child_recovery(recovery, failures, None)
        }
    }
}

fn retain_started_child_recovery(
    recovery: PendingStartedChildCleanup,
    mut failures: FinalizationFailures,
    cleanup_error: Option<std::io::Error>,
) -> std::io::Error {
    retain_pending_cleanup(recovery, &mut failures);
    failures
        .finish(cleanup_error.map_or(Ok(()), Err), ())
        .expect_err("a started-child failure was recorded")
}

fn retain_pending_cleanup(
    recovery: PendingStartedChildCleanup,
    failures: &mut FinalizationFailures,
) -> Option<ProcessRecoveryRecord> {
    let record = register_process_recovery(recovery);
    match retry_process_recovery(record.id()) {
        Ok(ProcessRecoveryState::Complete) => return None,
        Ok(ProcessRecoveryState::Pending) => {}
        Err(error) => failures.record_error(ProcessFinalizationStage::Recovery, error),
    }
    failures.record_error(
        ProcessFinalizationStage::Reconciliation,
        recovery_required_error(record),
    );
    Some(record)
}

fn apply_termination_failure_fixture(
    result: Result<()>,
    #[cfg(feature = "sandbox-test-fixtures")] test_failures: &mut ProcessFailureFixtures,
) -> Result<()> {
    #[cfg(feature = "sandbox-test-fixtures")]
    if result.is_ok() && std::mem::take(&mut test_failures.terminate) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "injected process termination failure",
        ));
    }
    result
}

#[cfg(feature = "sandbox-test-fixtures")]
impl ProcessFailureFixtures {
    fn from_command(command: &Command) -> Self {
        Self {
            containment_attach: command_has_fixture(command, "SOLARIS_SANDBOX_FIXTURE_FAIL_CONTAINMENT_ATTACH"),
            stdin: command_has_fixture(command, "SOLARIS_SANDBOX_FIXTURE_FAIL_STDIN"),
            terminate: command_has_fixture(command, "SOLARIS_SANDBOX_FIXTURE_FAIL_TERMINATION"),
            wait: command_has_fixture(command, "SOLARIS_SANDBOX_FIXTURE_FAIL_WAIT"),
            post_spawn_termination_unknown: command_has_fixture(
                command,
                "SOLARIS_SANDBOX_FIXTURE_POST_SPAWN_TERMINATION_UNKNOWN",
            ),
            recovery_termination_unknown: command_has_fixture(
                command,
                "SOLARIS_SANDBOX_FIXTURE_RECOVERY_TERMINATION_UNKNOWN",
            ),
        }
    }
}

#[cfg(feature = "sandbox-test-fixtures")]
fn command_has_fixture(command: &Command, key: &str) -> bool {
    command
        .as_std()
        .get_envs()
        .any(|(candidate, value)| candidate == key && value.is_some())
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        let Some(mut recovery) = self.take_pending_cleanup() else {
            return;
        };
        let termination = recovery.terminate_inner();
        match recovery.wait_for_exit(STARTED_CHILD_EXIT_CONFIRMATION_TIMEOUT) {
            Ok(true) => match recovery.cleanup_after_confirmed_exit() {
                Ok(()) => {
                    if let Err(ref error) = termination {
                        tracing::warn!(
                            error_kind = ?error.kind(),
                            os_error = ?error.raw_os_error(),
                            "managed child termination reported an error after exit was confirmed"
                        );
                    }
                    return;
                }
                Err(error) => tracing::warn!(
                    error_kind = ?error.kind(),
                    os_error = ?error.raw_os_error(),
                    "managed child drop could not clean up a confirmed process exit"
                ),
            },
            Ok(false) => tracing::warn!("managed child exit was not confirmed during bounded drop cleanup"),
            Err(error) => tracing::warn!(
                error_kind = ?error.kind(),
                os_error = ?error.raw_os_error(),
                "managed child drop could not confirm process exit"
            ),
        }
        let mut failures = FinalizationFailures::new();
        failures.record(ProcessFinalizationStage::Terminate, termination);
        let pending = retain_pending_cleanup(recovery, &mut failures);
        if let Some(record) = pending {
            tracing::warn!(
                recovery_id = record.id().get(),
                recovery_kind = record.kind().as_str(),
                "managed child drop retained process recovery ownership"
            );
        }
    }
}

#[cfg(test)]
#[path = "command_test.rs"]
mod command_test;
