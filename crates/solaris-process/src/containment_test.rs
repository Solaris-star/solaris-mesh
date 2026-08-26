#[cfg(unix)]
#[test]
fn command_without_verified_drain_does_not_require_guardian_helper() {
    use super::{ContainmentLaunchGuard, SandboxCommandDisposition};

    let mut command = tokio::process::Command::new("/bin/true");
    let mut launch = ContainmentLaunchGuard::prepare(&mut command).unwrap();

    launch
        .ensure_final_command(&mut command, SandboxCommandDisposition::Replaced, false)
        .unwrap();

    assert!(launch.guardian.is_none());
}

#[cfg(unix)]
#[tokio::test]
async fn ambient_execution_uses_plain_process_group_without_packaged_helper() {
    let command = tokio::process::Command::new("/bin/true");

    let result = crate::CommandRunner::new(command)
        .launch_policy(crate::ProcessLaunchPolicy::Ambient)
        .run()
        .await
        .unwrap();

    assert_eq!(result.exit_code, Some(0));
    assert_eq!(result.sandbox_report.reason(), crate::SandboxReason::NotRequested);
}
