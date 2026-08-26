#![cfg(all(windows, feature = "sandbox-test-fixtures"))]

use std::path::{Path, PathBuf};
use std::time::Duration;

use solaris_process::{
    ProcessFinalizationError, ProcessFinalizationStage, ProcessLaunchPolicy, ProcessRecoveryKind,
    drain_process_recoveries, inspect_executable, isolate_process_recoveries_for_test, pending_process_recoveries,
    pin_executable, process_recovery_required,
};

const TARGET_MARKER_KEY: &str = "SOLARIS_WINDOWS_RECOVERY_TARGET_MARKER";

#[test]
fn recovery_target_probe() {
    let Some(marker) = std::env::var_os(TARGET_MARKER_KEY) else {
        return;
    };
    std::fs::write(marker, b"started").unwrap();
}

#[tokio::test]
async fn post_spawn_failure_retains_child_containment_and_acl_until_explicit_drain() {
    let _recovery_test_guard = isolate_process_recoveries_for_test();
    let parent = tempfile::tempdir().unwrap();
    let workspace = parent.path().join("workspace");
    let renamed = parent.path().join("renamed-workspace");
    std::fs::create_dir(&workspace).unwrap();
    let descendant_marker = workspace.join("descendant-escaped");
    let target_marker = workspace.join("target-started");
    let mut command = pinned_test_command(&workspace, "recovery_target_probe");
    command
        .env("SOLARIS_SANDBOX_FIXTURE_BEHAVIOR", "timeout-with-descendant")
        .env("SOLARIS_SANDBOX_FIXTURE_DESCENDANT_MARKER", &descendant_marker)
        .env("SOLARIS_SANDBOX_FIXTURE_POST_SPAWN_TERMINATION_UNKNOWN", "1")
        .env("SOLARIS_SANDBOX_FIXTURE_RECOVERY_TERMINATION_UNKNOWN", "1")
        .env(TARGET_MARKER_KEY, &target_marker)
        .launch_policy(fixture_policy(&workspace));

    let error = match command.spawn() {
        Ok(_) => panic!("the injected start confirmation failure must not return a managed child"),
        Err(error) => error,
    };
    let recovery = process_recovery_required(&error).expect("the failure must require reconciliation");
    assert_eq!(recovery.kind(), ProcessRecoveryKind::StartedChild);
    assert!(pending_process_recoveries().contains(&recovery));
    assert_finalization_stages(
        &error,
        &[
            ProcessFinalizationStage::Spawn,
            ProcessFinalizationStage::Wait,
            ProcessFinalizationStage::Terminate,
            ProcessFinalizationStage::Recovery,
            ProcessFinalizationStage::Reconciliation,
        ],
    );
    let rename_error = std::fs::rename(&workspace, &renamed).unwrap_err();
    assert_eq!(
        rename_error.raw_os_error(),
        Some(32),
        "expected ERROR_SHARING_VIOLATION"
    );

    drain_process_recoveries(Duration::from_secs(5)).await.unwrap();
    assert!(!pending_process_recoveries().contains(&recovery));
    std::fs::rename(&workspace, &renamed).unwrap();
    std::fs::rename(&renamed, &workspace).unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(!target_marker.exists());
    assert!(!descendant_marker.exists());
}

fn pinned_test_command(workspace: &Path, test_name: &str) -> solaris_process::PinnedCommand {
    let executable = workspace.join("windows-failure-recovery-test.exe");
    std::fs::copy(std::env::current_exe().unwrap(), &executable).unwrap();
    let identity = inspect_executable(&executable).unwrap();
    let mut command = pin_executable(&executable, &identity).unwrap().command().unwrap();
    command.args(["--exact", test_name, "--nocapture"]);
    command
}

fn fixture_policy(workspace: &Path) -> ProcessLaunchPolicy {
    ProcessLaunchPolicy::workspace_sandbox_with_test_helper(
        workspace,
        [],
        [],
        PathBuf::from(env!("CARGO_BIN_EXE_solaris-process-sandbox-test-helper")),
    )
}

fn assert_finalization_stages(error: &std::io::Error, expected: &[ProcessFinalizationStage]) {
    let aggregate = error
        .get_ref()
        .and_then(|source| source.downcast_ref::<ProcessFinalizationError>())
        .expect("expected a structured multi-stage finalization error");
    assert_eq!(
        aggregate
            .failures()
            .iter()
            .map(|failure| failure.stage())
            .collect::<Vec<_>>(),
        expected
    );
}
