use super::*;

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct LogBuffer(Arc<Mutex<Vec<u8>>>);

    struct LogWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for LogWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for LogBuffer {
        type Writer = LogWriter;

        fn make_writer(&'writer self) -> Self::Writer {
            LogWriter(Arc::clone(&self.0))
        }
    }

    #[test]
    fn detect_shell_kind_recognizes_supported_shells() {
        assert_eq!(detect_shell_kind("bash"), Some(ShellKind::Bash));
        assert_eq!(detect_shell_kind("bash.exe"), Some(ShellKind::Bash));
        assert_eq!(detect_shell_kind("/bin/zsh"), Some(ShellKind::Zsh));
        assert_eq!(detect_shell_kind("/bin/sh"), Some(ShellKind::Sh));
        assert_eq!(detect_shell_kind("pwsh"), Some(ShellKind::PowerShell));
        assert_eq!(detect_shell_kind("powershell.exe"), Some(ShellKind::PowerShell));
        assert_eq!(
            detect_shell_kind(r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe"),
            Some(ShellKind::PowerShell)
        );
        assert_eq!(detect_shell_kind("cmd.exe"), Some(ShellKind::Cmd));
        assert_eq!(detect_shell_kind("fish"), None);
        assert_eq!(detect_shell_kind("nu"), None);
    }

    #[test]
    fn detect_shell_kind_recognizes_unicode_paths() {
        assert_eq!(detect_shell_kind(r"C:\用户\工具\pwsh.exe"), Some(ShellKind::PowerShell));
    }

    #[test]
    fn resolved_shell_logs_do_not_expose_the_executable_path() {
        let sentinel = "super-secret-token-shell-log";
        let shell = ResolvedShell::new(ShellKind::PowerShell, PathBuf::from(sentinel).join("powershell.exe"));
        let buffer = LogBuffer::default();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::DEBUG)
            .with_writer(buffer.clone())
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            log_configured_shell(&shell);
            log_default_shell(&shell);
        });
        let logs = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();

        assert!(!logs.contains(sentinel));
        assert!(logs.contains("shell_path_redacted=true"));
        assert!(logs.contains("resolved configured shell"));
        assert!(logs.contains("resolved default shell"));
    }

    #[test]
    fn shell_errors_and_anyhow_chain_do_not_expose_requested_values() {
        let sentinel = "super-secret-token-shell-error";
        let unsupported = resolve_shell(Some(sentinel)).unwrap_err();
        assert!(!unsupported.to_string().contains(sentinel));
        assert!(!format!("{unsupported:?}").contains(sentinel));
        assert!(unsupported.to_string().contains("sha256:"));

        let missing = std::env::temp_dir()
            .join(sentinel)
            .join(if cfg!(windows) { "powershell.exe" } else { "sh" });
        let config = ShellConfig {
            default: missing.to_string_lossy().into_owned(),
        };
        let bootstrap_result: anyhow::Result<ResolvedShell> = resolve_shell_config(&config).map_err(Into::into);
        let error = bootstrap_result.unwrap_err();
        let display = error.to_string();
        let chain = format!("{error:#}");
        let debug = format!("{error:?}");

        assert!(!display.contains(sentinel));
        assert!(!chain.contains(sentinel));
        assert!(!debug.contains(sentinel));
        assert!(display.contains("sha256:"));
    }

    #[test]
    fn derive_exec_args_uses_shell_specific_flags() {
        let bash = ResolvedShell::new(ShellKind::Bash, PathBuf::from("/bin/bash"));
        assert_eq!(bash.derive_exec_args("echo ok", false), vec!["-c", "echo ok"]);
        assert_eq!(bash.derive_exec_args("echo ok", true), vec!["-lc", "echo ok"]);

        let powershell = ResolvedShell::new(ShellKind::PowerShell, PathBuf::from("pwsh"));
        assert_eq!(
            powershell.derive_exec_args("Write-Output ok", false),
            vec![
                "-NoProfile",
                "-Command",
                "try { [Console]::OutputEncoding=[System.Text.Encoding]::UTF8 } catch {}\nWrite-Output ok"
            ]
        );

        let cmd = ResolvedShell::new(ShellKind::Cmd, PathBuf::from("cmd.exe"));
        assert_eq!(cmd.derive_exec_args("echo ok", false), vec!["/C", "echo ok"]);
    }

    #[tokio::test]
    async fn shell_command_runs_echo() {
        let output = shell_command("echo hello").await.expect("shell_command failed");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("hello"));
    }

    #[tokio::test]
    async fn shell_command_builder_allows_env_and_cwd() {
        let tmp = std::env::temp_dir();
        let shell = default_shell();
        let cmd_str = match shell.kind {
            ShellKind::PowerShell => "Write-Output $env:MY_VAR",
            ShellKind::Cmd => "echo %MY_VAR%",
            ShellKind::Bash | ShellKind::Zsh | ShellKind::Sh => "echo $MY_VAR",
        };
        let output = shell_command_builder(&shell, cmd_str, false)
            .env("MY_VAR", "test_value")
            .current_dir(&tmp)
            .output()
            .await
            .expect("builder failed");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("test_value"));
    }

    #[tokio::test]
    async fn shell_command_does_not_inherit_arbitrary_environment() {
        const CHILD_MODE: &str = "SOLARIS_CONFIG_SHELL_ENV_CHILD";
        const SECRET_KEY: &str = "SOLARIS_CONFIG_SHELL_SECRET";

        if std::env::var_os(CHILD_MODE).is_some() {
            let shell = default_shell();
            let script = match shell.kind {
                ShellKind::PowerShell => {
                    "if ($null -eq $env:SOLARIS_CONFIG_SHELL_SECRET) { 'missing' } else { 'present' }"
                }
                ShellKind::Cmd => "if defined SOLARIS_CONFIG_SHELL_SECRET (echo present) else (echo missing)",
                ShellKind::Bash | ShellKind::Zsh | ShellKind::Sh => {
                    "if [ -z \"${SOLARIS_CONFIG_SHELL_SECRET+x}\" ]; then printf missing; else printf present; fi"
                }
            };
            let output = shell_command(script).await.unwrap();
            assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "missing");
            return;
        }

        let output = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "shell::shell_test::tests::shell_command_does_not_inherit_arbitrary_environment",
                "--nocapture",
            ])
            .env(CHILD_MODE, "1")
            .env(SECRET_KEY, "shell-secret-must-not-pass")
            .output()
            .await
            .unwrap();

        assert!(output.status.success(), "shell child inherited arbitrary environment");
    }
}
