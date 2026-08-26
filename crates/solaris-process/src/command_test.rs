#[cfg(any(windows, target_os = "macos"))]
mod tests {
    use std::path::{Path, PathBuf};

    use crate::{ProcessLaunchPolicy, SandboxEnforcement, inspect_executable, pin_executable};
    #[cfg(target_os = "macos")]
    use crate::{SandboxError, SandboxReason};

    #[cfg(windows)]
    fn shell_path() -> PathBuf {
        let names = ["pwsh.exe", "powershell.exe"];
        std::env::var_os("PATH")
            .into_iter()
            .flat_map(|value| std::env::split_paths(&value).collect::<Vec<_>>())
            .flat_map(|root| names.map(|name| root.join(name)))
            .find(|path| path.is_file())
            .expect("PowerShell is required to run Windows process tests")
    }

    #[cfg(target_os = "macos")]
    fn shell_path() -> PathBuf {
        PathBuf::from("/bin/sh")
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn pinned_command_workspace_policy_runs_with_full_enforcement() {
        let workspace = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let marker = workspace.path().join("launched");
        let executable = shell_path().canonicalize().unwrap();
        let identity = inspect_executable(&executable).unwrap();
        let pinned = pin_executable(&executable, &identity).unwrap();
        let mut command = pinned.command().unwrap();
        configure_marker_command(&mut command, &marker);
        command.launch_policy(ProcessLaunchPolicy::workspace_sandbox(
            workspace.path(),
            [state.path().to_path_buf()],
        ));

        let mut child = command.spawn().expect("Windows strict Auto must use its Full runner");
        assert_eq!(child.sandbox_report().enforcement(), SandboxEnforcement::Full);
        assert!(child.wait().await.unwrap().success());
        assert_eq!(std::fs::read_to_string(marker).unwrap().trim(), "launched");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn pinned_command_workspace_policy_fails_before_spawn() {
        let workspace = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let marker = workspace.path().join("must-not-launch");
        let executable = shell_path().canonicalize().unwrap();
        let identity = inspect_executable(&executable).unwrap();
        let pinned = pin_executable(&executable, &identity).unwrap();
        let mut command = pinned.command().unwrap();
        configure_marker_command(&mut command, &marker);
        command.launch_policy(ProcessLaunchPolicy::workspace_sandbox(
            workspace.path(),
            [state.path().to_path_buf()],
        ));

        let error = match command.spawn() {
            Ok(_) => panic!("unsupported sandbox must fail before spawn"),
            Err(error) => error,
        };

        assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
        let sandbox_error = error
            .get_ref()
            .and_then(|source| source.downcast_ref::<SandboxError>())
            .expect("sandbox error must remain matchable");
        let report = sandbox_error
            .report()
            .expect("unavailable runner must include its report");
        assert_eq!(report.enforcement(), SandboxEnforcement::Unavailable);
        assert_eq!(report.reason(), SandboxReason::PlatformRunnerUnavailable);
        assert!(!marker.exists());
    }

    #[cfg(windows)]
    #[test]
    fn pinned_command_explicit_env_preserves_business_values_but_rejects_protected_keys() {
        let executable = shell_path().canonicalize().unwrap();
        let identity = inspect_executable(&executable).unwrap();
        let pinned = pin_executable(&executable, &identity).unwrap();
        let mut command = pinned.command().unwrap();
        let hostile = r"D:\hostile-process-environment";

        command
            .env("MCP_EXPLICIT_TOKEN", "configured")
            .env("TEMP", hostile)
            .envs([
                ("TMP", hostile),
                ("TMPDIR", hostile),
                ("SYSTEMROOT", hostile),
                ("WINDIR", hostile),
                ("SYSTEMDRIVE", hostile),
                ("COMSPEC", hostile),
                ("PATHEXT", hostile),
                ("PATH", hostile),
                ("HOME", hostile),
                ("USERPROFILE", hostile),
            ]);

        let environment = command
            .command
            .as_std()
            .get_envs()
            .filter_map(|(key, value)| value.map(|value| (key.to_string_lossy().to_uppercase(), value.to_owned())))
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(
            environment.get("MCP_EXPLICIT_TOKEN"),
            Some(&std::ffi::OsString::from("configured"))
        );
        for key in [
            "TEMP",
            "TMP",
            "TMPDIR",
            "SYSTEMROOT",
            "WINDIR",
            "SYSTEMDRIVE",
            "COMSPEC",
            "PATHEXT",
            "PATH",
            "HOME",
            "USERPROFILE",
        ] {
            assert_ne!(environment.get(key), Some(&std::ffi::OsString::from(hostile)), "{key}");
        }
    }

    #[cfg(windows)]
    fn configure_marker_command(command: &mut crate::PinnedCommand, marker: &Path) {
        let script = format!(
            "Set-Content -LiteralPath '{}' -Value launched",
            marker.to_string_lossy().replace('\'', "''")
        );
        command.args(["-NoProfile", "-Command", &script]);
    }

    #[cfg(target_os = "macos")]
    fn configure_marker_command(command: &mut crate::PinnedCommand, marker: &Path) {
        let script = format!("printf launched > '{}'", marker.to_string_lossy());
        command.args(["-c", &script]);
    }
}
