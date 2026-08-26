use std::io::Result;
use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use crate::command::{ManagedChild, PinnedCommand};
use crate::environment::configure_safe_process_environment;
use crate::launch_policy::ProcessLaunchPolicy;
use crate::output::{drain_reader, drain_reader_with_result, finish_stdin_writer, read_stream, take_output};
use crate::process_finalization::{FinalizationFailures, ProcessFinalizationStage};
use crate::sandbox::SandboxReport;
use crate::spawn_authorization::ProcessSpawnAuthorization;

pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(120);
pub const DEFAULT_POST_PROCESS_DRAIN: Duration = Duration::from_millis(250);
pub const DEFAULT_MAX_PROCESS_OUTPUT_BYTES: usize = 1024 * 1024;
const PROCESS_TREE_TERMINATION_TIMEOUT: Duration = Duration::from_secs(5);

/// Runs one process and buffers its stdout/stderr while it is running.
pub struct CommandRunner {
    command: RunnerCommand,
    launch_policy: Option<ProcessLaunchPolicy>,
    spawn_authorizer: Option<ProcessSpawnAuthorization>,
    stdin: Option<Vec<u8>>,
    timeout: Duration,
    post_process_drain: Duration,
    max_stdout_bytes: usize,
    max_stderr_bytes: usize,
    max_total_output_bytes: usize,
}

enum RunnerCommand {
    Unpinned(Command),
    Pinned(Box<PinnedCommand>),
}

impl RunnerCommand {
    fn stdin(&mut self, stdio: Stdio) {
        match self {
            Self::Unpinned(command) => {
                command.stdin(stdio);
            }
            Self::Pinned(command) => {
                command.stdin(stdio);
            }
        }
    }

    fn stdout(&mut self, stdio: Stdio) {
        match self {
            Self::Unpinned(command) => {
                command.stdout(stdio);
            }
            Self::Pinned(command) => {
                command.stdout(stdio);
            }
        }
    }

    fn stderr(&mut self, stdio: Stdio) {
        match self {
            Self::Unpinned(command) => {
                command.stderr(stdio);
            }
            Self::Pinned(command) => {
                command.stderr(stdio);
            }
        }
    }

    fn kill_on_drop(&mut self, kill_on_drop: bool) {
        match self {
            Self::Unpinned(command) => {
                command.kill_on_drop(kill_on_drop);
            }
            Self::Pinned(command) => {
                command.kill_on_drop(kill_on_drop);
            }
        }
    }

    fn spawn_with_policy(self, policy: Option<&ProcessLaunchPolicy>) -> Result<ManagedChild> {
        match (self, policy) {
            (Self::Unpinned(command), Some(policy)) => ManagedChild::spawn_command_with_policy(command, policy),
            (Self::Unpinned(command), None) => ManagedChild::spawn_command(command),
            (Self::Pinned(command), Some(policy)) => (*command).spawn_with_policy(policy),
            (Self::Pinned(command), None) => (*command).spawn(),
        }
    }

    fn spawn(
        self,
        policy: Option<&ProcessLaunchPolicy>,
        authorizer: Option<ProcessSpawnAuthorization>,
    ) -> Result<ManagedChild> {
        let Some(authorizer) = authorizer else {
            return self.spawn_with_policy(policy);
        };
        authorizer.authorize_and_spawn(Box::new(move |policy| self.spawn_with_policy(Some(&policy))))
    }
}

impl CommandRunner {
    pub fn new(command: Command) -> Self {
        let mut command = command;
        configure_safe_process_environment(&mut command);
        Self::with_command(RunnerCommand::Unpinned(command))
    }

    pub fn new_pinned(command: PinnedCommand) -> Self {
        Self::with_command(RunnerCommand::Pinned(Box::new(command)))
    }

    fn with_command(command: RunnerCommand) -> Self {
        Self {
            command,
            launch_policy: None,
            spawn_authorizer: None,
            stdin: None,
            timeout: DEFAULT_TIMEOUT,
            post_process_drain: DEFAULT_POST_PROCESS_DRAIN,
            max_stdout_bytes: DEFAULT_MAX_PROCESS_OUTPUT_BYTES,
            max_stderr_bytes: DEFAULT_MAX_PROCESS_OUTPUT_BYTES,
            max_total_output_bytes: DEFAULT_MAX_PROCESS_OUTPUT_BYTES,
        }
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn launch_policy(mut self, policy: ProcessLaunchPolicy) -> Self {
        self.launch_policy = Some(policy);
        self
    }

    /// Installs an effect-scoped check that selects the final launch policy at
    /// the synchronous process creation boundary.
    pub fn spawn_authorizer(mut self, authorizer: ProcessSpawnAuthorization) -> Self {
        self.spawn_authorizer = Some(authorizer);
        self
    }

    pub fn stdin_bytes(mut self, stdin: Vec<u8>) -> Self {
        self.stdin = Some(stdin);
        self
    }

    pub fn post_process_drain(mut self, drain: Duration) -> Self {
        self.post_process_drain = drain;
        self
    }

    pub fn max_output_bytes(mut self, limit: usize) -> Self {
        let limit = limit.max(1);
        self.max_stdout_bytes = limit;
        self.max_stderr_bytes = limit;
        self.max_total_output_bytes = limit;
        self
    }

    pub async fn run(mut self) -> Result<CommandResult> {
        if self.stdin.is_some() {
            self.command.stdin(Stdio::piped());
        }
        self.command.stdout(Stdio::piped());
        self.command.stderr(Stdio::piped());
        self.command.kill_on_drop(true);
        let mut child = self
            .command
            .spawn(self.launch_policy.as_ref(), self.spawn_authorizer.take())?;
        let sandbox_report = child.sandbox_report();
        let stdin_writer = self.stdin.take().and_then(|stdin| {
            child.take_stdin().map(|mut writer| {
                tokio::spawn(async move {
                    writer.write_all(&stdin).await?;
                    writer.shutdown().await
                })
            })
        });
        let stdout = Arc::new(Mutex::new(Vec::new()));
        let stderr = Arc::new(Mutex::new(Vec::new()));
        let total_output = Arc::new(AtomicUsize::new(0));
        let (limit_tx, mut limit_rx) = tokio::sync::mpsc::unbounded_channel();

        let stdout_reader = child.take_stdout().map(|reader| {
            read_stream(
                reader,
                Arc::clone(&stdout),
                self.max_stdout_bytes,
                self.max_total_output_bytes,
                Arc::clone(&total_output),
                limit_tx.clone(),
            )
        });
        let stderr_reader = child.take_stderr().map(|reader| {
            read_stream(
                reader,
                Arc::clone(&stderr),
                self.max_stderr_bytes,
                self.max_total_output_bytes,
                Arc::clone(&total_output),
                limit_tx,
            )
        });

        enum Completion {
            Exited(Result<()>),
            TimedOut,
            OutputLimit,
        }
        let completion = {
            let wait = child.wait_root_ready();
            tokio::pin!(wait);
            tokio::select! {
                status = &mut wait => Completion::Exited(status),
                _ = tokio::time::sleep(self.timeout) => Completion::TimedOut,
                Some(()) = limit_rx.recv() => Completion::OutputLimit,
            }
        };
        match completion {
            Completion::Exited(status) => {
                let mut failures = FinalizationFailures::new();
                let root_ready = match status {
                    Ok(()) => true,
                    Err(error) => {
                        failures.record_error(ProcessFinalizationStage::Wait, error);
                        false
                    }
                };
                let terminated = failures.record(
                    ProcessFinalizationStage::Terminate,
                    child.terminate_tree_and_confirm(PROCESS_TREE_TERMINATION_TIMEOUT).await,
                );
                let status = if root_ready && terminated {
                    match child.reap_root().await {
                        Ok(status) => Some(status),
                        Err(error) => {
                            failures.record_error(ProcessFinalizationStage::Wait, error);
                            None
                        }
                    }
                } else {
                    None
                };
                let stdin_result = finish_stdin_writer(stdin_writer).await;
                #[cfg(feature = "sandbox-test-fixtures")]
                let stdin_result = child.apply_stdin_failure_fixture(stdin_result);
                failures.record(ProcessFinalizationStage::Stdin, stdin_result);
                let (stdout_result, stderr_result) = tokio::join!(
                    drain_reader_with_result(stdout_reader, self.post_process_drain),
                    drain_reader_with_result(stderr_reader, self.post_process_drain)
                );
                failures.record(ProcessFinalizationStage::Stdout, stdout_result);
                failures.record(ProcessFinalizationStage::Stderr, stderr_result);
                let output_limit_exceeded = total_output.load(Ordering::Acquire) > self.max_total_output_bytes;
                let result = status.map(|status| CommandResult {
                    exit_code: if output_limit_exceeded { None } else { status.code() },
                    timed_out: false,
                    output_limit_exceeded,
                    sandbox_report,
                    stdout: take_output(stdout),
                    stderr: take_output(stderr),
                });
                let cleanup = child.finalize_sandbox(terminated, &mut failures);
                let result = failures.finish(cleanup, result)?;
                result.ok_or_else(|| std::io::Error::other("process wait returned no status or error"))
            }
            completion @ (Completion::TimedOut | Completion::OutputLimit) => {
                let mut failures = FinalizationFailures::new();
                let output_limit_exceeded = matches!(completion, Completion::OutputLimit);
                if let Some(writer) = stdin_writer {
                    writer.abort();
                }
                let terminated = failures.record(
                    ProcessFinalizationStage::Terminate,
                    child.terminate_tree_and_confirm(PROCESS_TREE_TERMINATION_TIMEOUT).await,
                );
                if terminated {
                    // Tree termination has already proved that the root process exited. Reaping
                    // only collects its retained status, so an output-reader drain budget must not
                    // turn scheduler delay into a false process failure under concurrent load.
                    let reap = async {
                        child.wait_root_ready().await?;
                        child.reap_root().await.map(|_| ())
                    }
                    .await;
                    failures.record(ProcessFinalizationStage::Wait, reap);
                }
                tokio::join!(
                    drain_reader(stdout_reader, self.post_process_drain),
                    drain_reader(stderr_reader, self.post_process_drain)
                );
                let cleanup = child.finalize_sandbox(terminated, &mut failures);
                failures.finish(
                    cleanup,
                    CommandResult {
                        exit_code: None,
                        timed_out: !output_limit_exceeded,
                        output_limit_exceeded,
                        sandbox_report,
                        stdout: take_output(stdout),
                        stderr: take_output(stderr),
                    },
                )
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandResult {
    pub exit_code: Option<i32>,
    pub timed_out: bool,
    pub output_limit_exceeded: bool,
    pub sandbox_report: SandboxReport,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}
