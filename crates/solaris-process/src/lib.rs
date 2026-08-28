mod command;
mod command_runner;
mod containment;
mod environment;
mod executable;
mod launch_policy;
mod network_proxy;
mod object_identity;
mod output;
mod process_finalization;
mod recovery;
mod recovery_lifecycle;
mod runner;
mod sandbox;
mod sandbox_report;
mod spawn_authorization;
#[cfg(unix)]
mod unix_guardian;
#[cfg(windows)]
mod windows_job;
#[cfg(windows)]
mod windows_psec_runtime;
mod workspace_root;

pub use command::{ManagedChild, PinnedCommand};
pub use command_runner::{
    CommandResult, CommandRunner, DEFAULT_MAX_PROCESS_OUTPUT_BYTES, DEFAULT_POST_PROCESS_DRAIN, DEFAULT_TIMEOUT,
};
pub use environment::{configure_safe_process_environment, filter_resource_environment};
pub use executable::{ExecutableError, ExecutableIdentity, executable_path_identity};
pub use launch_policy::ProcessLaunchPolicy;
pub use network_proxy::{NetworkProxyPolicy, NetworkProxyPolicyError, permission_domain_is_covered};
pub use object_identity::ProtectedObjectIdentity;
pub use process_finalization::{ProcessFinalizationError, ProcessFinalizationFailure, ProcessFinalizationStage};
pub use recovery::{
    ProcessRecoveryFailureCategory, ProcessRecoveryFailureRecord, ProcessRecoveryId, ProcessRecoveryKind,
    ProcessRecoveryRecord, ProcessRecoveryState, drain_process_recoveries, pending_process_recoveries,
    process_outcome_unknown, process_recovery_drain_failures, process_recovery_required, retry_process_recovery,
};
#[cfg(feature = "sandbox-test-fixtures")]
pub use recovery::{
    blocking_process_recovery_error_for_test, isolate_process_recoveries_for_test,
    pending_process_recovery_error_for_test, process_outcome_unknown_error_for_test,
    process_recovery_required_error_for_test,
};
pub use recovery_lifecycle::{ProcessRecoveryLifecycle, ProcessRecoveryLifecycleError};
pub use runner::{PinnedExecutable, inspect_executable, pin_executable, pin_executable_by_digest};
pub use sandbox::{SandboxError, platform_sandbox_report, sandbox_report_from_error};
pub use sandbox_report::{SandboxBackend, SandboxEnforcement, SandboxReason, SandboxReport};
pub use spawn_authorization::{ProcessSpawn, ProcessSpawnAuthorization, ProcessSpawnAuthorizer};
pub use workspace_root::{WorkspaceRootAuthority, WorkspaceRootLaunchCapability};
