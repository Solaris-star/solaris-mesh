use super::*;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::process::Stdio;

    use crate::protocol::JsonRpcRequest;
    use crate::transport::McpTransport;
    use solaris_config::shell::ShellKind;
    use solaris_process::{inspect_executable, pin_executable};

    use super::{MAX_STDIO_FRAME_BYTES, StdioTransport};

    const STDERR_LEAK_CHILD_MODE: &str = "SOLARIS_MCP_STDERR_LEAK_CHILD";
    const STDERR_SECRET_KEY: &str = "MCP_STDERR_SECRET";
    const STDERR_SECRET: &str = "mcp-stderr-secret-must-not-escape";

    fn pinned_shell(script: &str) -> (solaris_process::PinnedCommand, Vec<String>, HashMap<String, String>) {
        let shell = solaris_config::shell::default_shell();
        let identity = inspect_executable(&shell.path).unwrap();
        let command = pin_executable(&shell.path, &identity).unwrap().command().unwrap();
        let args = shell.derive_exec_args(script, false);
        let env = ["PATH", "PATHEXT", "SYSTEMROOT", "WINDIR", "COMSPEC"]
            .into_iter()
            .filter_map(|key| std::env::var(key).ok().map(|value| (key.to_owned(), value)))
            .collect();
        (command, args, env)
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn stdio_child_expands_system_drive_before_using_windows_paths() {
        let workspace = tempfile::tempdir().unwrap();
        let relative_marker = workspace.path().join("%SystemDrive%");
        let marker = relative_marker.to_string_lossy().replace('\'', "''");
        let script = format!(
            "$null = [Console]::In.ReadLine(); \
             $expanded = [Environment]::ExpandEnvironmentVariables('%SystemDrive%\\ProgramData'); \
             $rooted = [IO.Path]::IsPathFullyQualified($expanded); \
             if (-not $rooted) {{ New-Item -ItemType Directory -Path '{marker}' -Force | Out-Null }}; \
             [Console]::Out.WriteLine('{{\"jsonrpc\":\"2.0\",\"id\":7,\"result\":{{\"rooted\":' + \
             $rooted.ToString().ToLowerInvariant() + '}}}}')"
        );
        let (mut command, args, env) = pinned_shell(&script);
        command.current_dir(workspace.path());
        let transport = StdioTransport::spawn(command, &args, &env).await.unwrap();
        let request = JsonRpcRequest::new(7, "tools/list", None);

        let response = transport.request(&request).await.unwrap();

        assert_eq!(response.result.unwrap()["rooted"], true);
        assert!(
            !relative_marker.exists(),
            "Windows treated %SystemDrive% as a relative directory"
        );
    }

    #[tokio::test]
    async fn stdio_child_receives_only_its_explicit_secret_environment() {
        let shell = solaris_config::shell::default_shell();
        let script = match shell.kind {
            ShellKind::PowerShell => {
                "$null = [Console]::In.ReadLine(); if ($env:MCP_EXPLICIT_TOKEN -ne 'configured' -or $null -ne $env:HTTP_PROXY) { exit 9 }; [Console]::Out.WriteLine('{\"jsonrpc\":\"2.0\",\"id\":7,\"result\":{\"environment\":true}}')"
            }
            ShellKind::Cmd => {
                r#"set /p _=& if not "%MCP_EXPLICIT_TOKEN%"=="configured" exit /b 9& if not "%HTTP_PROXY%"=="" exit /b 10& echo {"jsonrpc":"2.0","id":7,"result":{"environment":true}}"#
            }
            ShellKind::Bash | ShellKind::Zsh | ShellKind::Sh => {
                r#"read -r _; [ "$MCP_EXPLICIT_TOKEN" = configured ] || exit 9; [ -z "${HTTP_PROXY+x}" ] || exit 10; printf '%s\n' '{"jsonrpc":"2.0","id":7,"result":{"environment":true}}'"#
            }
        };
        let (command, args, mut env) = pinned_shell(script);
        env.insert("MCP_EXPLICIT_TOKEN".to_owned(), "configured".to_owned());
        let transport = StdioTransport::spawn(command, &args, &env).await.unwrap();
        let request = JsonRpcRequest::new(7, "tools/list", None);

        let response = transport.request(&request).await.unwrap();

        assert_eq!(response.result.unwrap()["environment"], true);
    }

    #[tokio::test]
    async fn stdio_mcp_env_cannot_replace_trusted_platform_environment() {
        let hostile = tempfile::tempdir().unwrap();
        let hostile = hostile.path().to_string_lossy().into_owned();
        let shell = solaris_config::shell::default_shell();
        let script = match shell.kind {
            ShellKind::PowerShell => {
                "$null = [Console]::In.ReadLine(); $bad = $env:MCP_MALICIOUS_ENV_SENTINEL; $protected = @($env:TEMP, $env:TMP, $env:TMPDIR, $env:SystemRoot, $env:WinDir, $env:SystemDrive, $env:ComSpec, $env:PATHEXT, $env:Path, $env:HOME, $env:USERPROFILE); if ($protected -contains $bad) { exit 9 }; if ($env:MCP_EXPLICIT_TOKEN -ne 'configured') { exit 10 }; [Console]::Out.WriteLine('{\"jsonrpc\":\"2.0\",\"id\":7,\"result\":{\"environment\":true}}')"
            }
            ShellKind::Cmd => {
                r#"set /p _=& if "%TEMP%"=="%MCP_MALICIOUS_ENV_SENTINEL%" exit /b 9& if "%TMP%"=="%MCP_MALICIOUS_ENV_SENTINEL%" exit /b 9& if "%TMPDIR%"=="%MCP_MALICIOUS_ENV_SENTINEL%" exit /b 9& if "%SYSTEMROOT%"=="%MCP_MALICIOUS_ENV_SENTINEL%" exit /b 9& if "%WINDIR%"=="%MCP_MALICIOUS_ENV_SENTINEL%" exit /b 9& if "%SYSTEMDRIVE%"=="%MCP_MALICIOUS_ENV_SENTINEL%" exit /b 9& if "%COMSPEC%"=="%MCP_MALICIOUS_ENV_SENTINEL%" exit /b 9& if "%PATHEXT%"=="%MCP_MALICIOUS_ENV_SENTINEL%" exit /b 9& if "%PATH%"=="%MCP_MALICIOUS_ENV_SENTINEL%" exit /b 9& if "%HOME%"=="%MCP_MALICIOUS_ENV_SENTINEL%" exit /b 9& if "%USERPROFILE%"=="%MCP_MALICIOUS_ENV_SENTINEL%" exit /b 9& if not "%MCP_EXPLICIT_TOKEN%"=="configured" exit /b 10& echo {"jsonrpc":"2.0","id":7,"result":{"environment":true}}"#
            }
            ShellKind::Bash | ShellKind::Zsh | ShellKind::Sh => {
                r#"read -r _; bad="$MCP_MALICIOUS_ENV_SENTINEL"; [ "$TEMP" != "$bad" ] && [ "$TMP" != "$bad" ] && [ "$TMPDIR" != "$bad" ] || exit 9; [ "$MCP_EXPLICIT_TOKEN" = configured ] || exit 10; printf '%s\n' '{"jsonrpc":"2.0","id":7,"result":{"environment":true}}'"#
            }
        };
        let (command, args, mut env) = pinned_shell(script);
        let protected_keys = if cfg!(windows) {
            vec![
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
            ]
        } else {
            vec!["TEMP", "TMP", "TMPDIR"]
        };
        for key in protected_keys {
            env.insert(key.to_owned(), hostile.clone());
        }
        env.insert("MCP_MALICIOUS_ENV_SENTINEL".to_owned(), hostile);
        env.insert("MCP_EXPLICIT_TOKEN".to_owned(), "configured".to_owned());
        let transport = StdioTransport::spawn(command, &args, &env).await.unwrap();

        let response = transport
            .request(&JsonRpcRequest::new(7, "tools/list", None))
            .await
            .unwrap();

        assert_eq!(response.result.unwrap()["environment"], true);
    }

    #[tokio::test]
    async fn stdio_child_explicit_secret_cannot_escape_through_parent_stderr() {
        if std::env::var_os(STDERR_LEAK_CHILD_MODE).is_some() {
            let shell = solaris_config::shell::default_shell();
            let script = match shell.kind {
                ShellKind::PowerShell => {
                    "$null = [Console]::In.ReadLine(); [Console]::Error.WriteLine($env:MCP_STDERR_SECRET); [Console]::Out.WriteLine('{\"jsonrpc\":\"2.0\",\"id\":7,\"result\":{}}')"
                }
                ShellKind::Cmd => {
                    r#"set /p _=& echo %MCP_STDERR_SECRET% 1>&2& echo {"jsonrpc":"2.0","id":7,"result":{}}"#
                }
                ShellKind::Bash | ShellKind::Zsh | ShellKind::Sh => {
                    r#"read -r _; printf '%s\n' "$MCP_STDERR_SECRET" >&2; printf '%s\n' '{"jsonrpc":"2.0","id":7,"result":{}}'"#
                }
            };
            let (command, args, mut env) = pinned_shell(script);
            env.insert(STDERR_SECRET_KEY.to_owned(), STDERR_SECRET.to_owned());
            let transport = StdioTransport::spawn(command, &args, &env).await.unwrap();
            let response = transport
                .request(&JsonRpcRequest::new(7, "tools/list", None))
                .await
                .unwrap();
            assert_eq!(response.id, Some(7));
            return;
        }

        let output = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "transport::stdio::stdio_test::tests::stdio_child_explicit_secret_cannot_escape_through_parent_stderr",
                "--nocapture",
            ])
            .env(STDERR_LEAK_CHILD_MODE, "1")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output()
            .await
            .unwrap();

        assert!(output.status.success());
        assert!(!String::from_utf8_lossy(&output.stderr).contains(STDERR_SECRET));
    }

    #[tokio::test]
    async fn malformed_frame_error_does_not_expose_secret_payload() {
        let secret = "stdio-secret-that-must-not-leak";
        let shell = solaris_config::shell::default_shell();
        let script = match shell.kind {
            ShellKind::PowerShell => format!("[Console]::Out.WriteLine('{secret}')"),
            ShellKind::Cmd => format!("echo {secret}"),
            ShellKind::Bash | ShellKind::Zsh | ShellKind::Sh => format!("printf '%s\\n' '{secret}'"),
        };
        let (command, args, env) = pinned_shell(&script);
        let transport = StdioTransport::spawn(command, &args, &env).await.unwrap();

        let error = transport.read_response().await.unwrap_err().to_string();

        assert!(!error.contains(secret));
        assert!(error.contains("bytes="));
        assert!(error.contains("digest=sha256:"));
    }

    #[tokio::test]
    async fn request_rejects_a_response_with_the_wrong_id() {
        let shell = solaris_config::shell::default_shell();
        let script = match shell.kind {
            ShellKind::PowerShell => {
                r#"$null = [Console]::In.ReadLine(); [Console]::Out.WriteLine('{"jsonrpc":"2.0","id":999,"result":{}}')"#
            }
            ShellKind::Cmd => r#"set /p _=& echo {"jsonrpc":"2.0","id":999,"result":{}}"#,
            ShellKind::Bash | ShellKind::Zsh | ShellKind::Sh => {
                r#"read -r _; printf '%s\n' '{"jsonrpc":"2.0","id":999,"result":{}}'"#
            }
        };
        let (command, args, env) = pinned_shell(script);
        let transport = StdioTransport::spawn(command, &args, &env).await.unwrap();
        let request = JsonRpcRequest::new(7, "tools/list", None);

        let error = transport.request(&request).await.unwrap_err();

        assert_eq!(
            error.to_string(),
            "Transport error: MCP response id did not match request"
        );
    }

    #[tokio::test]
    async fn request_skips_a_server_notification_before_its_response() {
        let shell = solaris_config::shell::default_shell();
        let notification = r#"{"jsonrpc":"2.0","method":"notifications/progress","params":{}}"#;
        let response = r#"{"jsonrpc":"2.0","id":7,"result":{}}"#;
        let script = match shell.kind {
            ShellKind::PowerShell => format!(
                "$null = [Console]::In.ReadLine(); [Console]::Out.WriteLine('{notification}'); [Console]::Out.WriteLine('{response}')"
            ),
            ShellKind::Cmd => format!("set /p _=& echo {notification}& echo {response}"),
            ShellKind::Bash | ShellKind::Zsh | ShellKind::Sh => {
                format!("read -r _; printf '%s\\n' '{notification}' '{response}'")
            }
        };
        let (command, args, env) = pinned_shell(&script);
        let transport = StdioTransport::spawn(command, &args, &env).await.unwrap();
        let request = JsonRpcRequest::new(7, "tools/list", None);

        let response = transport.request(&request).await.unwrap();

        assert_eq!(response.id, Some(7));
    }

    #[tokio::test]
    async fn json_rpc_error_does_not_expose_server_message() {
        let secret = "server-secret-that-must-not-leak";
        let shell = solaris_config::shell::default_shell();
        let response = format!(r#"{{"jsonrpc":"2.0","id":7,"error":{{"code":-1,"message":"{secret}"}}}}"#);
        let script = match shell.kind {
            ShellKind::PowerShell => {
                format!("$null = [Console]::In.ReadLine(); [Console]::Out.WriteLine('{response}')")
            }
            ShellKind::Cmd => format!("set /p _=& echo {response}"),
            ShellKind::Bash | ShellKind::Zsh | ShellKind::Sh => {
                format!("read -r _; printf '%s\\n' '{response}'")
            }
        };
        let (command, args, env) = pinned_shell(&script);
        let transport = StdioTransport::spawn(command, &args, &env).await.unwrap();
        let request = JsonRpcRequest::new(7, "tools/list", None);

        let error = transport.request(&request).await.unwrap_err().to_string();

        assert!(!error.contains(secret));
        assert!(error.contains("MCP server returned an error"));
    }

    #[tokio::test]
    async fn frame_without_newline_is_bounded_and_terminates_child() {
        let directory = tempfile::tempdir().unwrap();
        let marker = directory.path().join("mcp-descendant-must-not-survive");
        let shell = solaris_config::shell::default_shell();
        let count = MAX_STDIO_FRAME_BYTES + 1;
        let script = match shell.kind {
            ShellKind::PowerShell => format!(
                "$p = Start-Process -FilePath (Get-Process -Id $PID).Path -ArgumentList '-NoProfile', '-Command', \"Start-Sleep -Seconds 2; Set-Content -LiteralPath '{}' -Value survived\" -PassThru -WindowStyle Hidden; [Console]::Out.Write('x' * {count}); Start-Sleep -Seconds 30",
                marker.to_string_lossy().replace('\'', "''")
            ),
            ShellKind::Cmd => format!(
                "start /b cmd /c \"ping -n 3 127.0.0.1 >nul & echo survived>\\\"{}\\\"\" & for /L %i in (1,1,{count}) do @set /p =x<nul & ping -n 31 127.0.0.1 >nul",
                marker.display()
            ),
            ShellKind::Bash | ShellKind::Zsh | ShellKind::Sh => {
                format!(
                    "(sleep 2; printf survived > '{}') & yes x | tr -d '\\n' | head -c {count}; sleep 30",
                    marker.to_string_lossy().replace('\'', "'\\''")
                )
            }
        };
        let (command, args, env) = pinned_shell(&script);
        let transport = StdioTransport::spawn(command, &args, &env).await.unwrap();

        let error = tokio::time::timeout(std::time::Duration::from_secs(10), transport.read_response())
            .await
            .expect("bounded reader should fail promptly")
            .unwrap_err()
            .to_string();

        assert!(error.contains("frame exceeded"));
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        assert!(!marker.exists(), "MCP background descendant survived frame overflow");
    }
}
