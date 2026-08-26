use super::*;

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::process::Command;

    use super::{
        CommandRunner, PinPhase, executable_path_identity, inspect_executable, pin_executable, pin_executable_inner,
    };
    use crate::{ProcessLaunchPolicy, ProcessSpawn, ProcessSpawnAuthorization, ProcessSpawnAuthorizer, SandboxError};

    #[test]
    fn runner_timeout_stdout_helper() {
        if exact_helper_name() != Some("runner::runner_test::tests::runner_timeout_stdout_helper") {
            return;
        }
        emit_timeout_output("stdout");
    }

    #[test]
    fn runner_timeout_stderr_helper() {
        if exact_helper_name() != Some("runner::runner_test::tests::runner_timeout_stderr_helper") {
            return;
        }
        emit_timeout_output("stderr");
    }

    fn exact_helper_name() -> Option<&'static str> {
        let arguments = std::env::args().collect::<Vec<_>>();
        if !arguments.iter().any(|argument| argument == "--exact") {
            return None;
        }
        [
            "runner::runner_test::tests::runner_timeout_stdout_helper",
            "runner::runner_test::tests::runner_timeout_stderr_helper",
        ]
        .into_iter()
        .find(|name| arguments.iter().any(|argument| argument == name))
    }

    fn emit_timeout_output(stream: &str) {
        use std::io::Write;

        match stream {
            "stdout" => {
                println!("runner_stdout_before_timeout");
                std::io::stdout().flush().unwrap();
            }
            "stderr" => {
                eprintln!("runner_stderr_before_timeout");
                std::io::stderr().flush().unwrap();
            }
            other => panic!("unexpected timeout output stream: {other}"),
        }
        std::thread::sleep(Duration::from_secs(30));
    }

    fn timeout_output_command(stream: &str) -> Command {
        let mut command = Command::new(std::env::current_exe().unwrap());
        let helper = match stream {
            "stdout" => "runner::runner_test::tests::runner_timeout_stdout_helper",
            "stderr" => "runner::runner_test::tests::runner_timeout_stderr_helper",
            other => panic!("unexpected timeout output stream: {other}"),
        };
        command.args(["--exact", helper, "--nocapture"]);
        command
    }

    fn write_test_executable(path: &std::path::Path, contents: &[u8]) {
        std::fs::write(path, contents).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
    }

    #[cfg(unix)]
    #[test]
    fn executable_path_identity_preserves_non_utf8_unix_bytes() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let first = PathBuf::from(OsString::from_vec(vec![b's', b'h', 0xff]));
        let second = PathBuf::from(OsString::from_vec(vec![b's', b'h', 0xfe]));

        assert_eq!(first.to_string_lossy(), second.to_string_lossy());
        assert_ne!(executable_path_identity(&first), executable_path_identity(&second));
    }

    #[cfg(windows)]
    #[test]
    fn executable_path_identity_preserves_non_utf16_windows_units() {
        use std::ffi::OsString;
        use std::os::windows::ffi::OsStringExt;

        let first = PathBuf::from(OsString::from_wide(&[b's' as u16, b'h' as u16, 0xd800]));
        let second = PathBuf::from(OsString::from_wide(&[b's' as u16, b'h' as u16, 0xd801]));

        assert_eq!(first.to_string_lossy(), second.to_string_lossy());
        assert_ne!(executable_path_identity(&first), executable_path_identity(&second));
    }

    #[test]
    fn executable_errors_expose_only_a_digest_identity() {
        let sentinel = "super-secret-token-executable";
        let path = std::env::temp_dir().join(sentinel).join("missing-runner");

        let error = inspect_executable(&path).unwrap_err().to_string();

        assert!(!error.contains(sentinel));
        assert!(error.contains("executable pin open failed"));
        assert!(error.contains("sha256:"));
    }

    #[test]
    fn oversized_executable_is_rejected_with_matchable_category() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("oversized-secret-executable");
        let file = std::fs::File::create(&path).unwrap();
        file.set_len(super::MAX_EXECUTABLE_BYTES + 1).unwrap();
        drop(file);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        }

        let error = inspect_executable(&path).unwrap_err();

        assert_eq!(error.category(), "executable exceeds size limit");
        assert!(!error.to_string().contains("oversized-secret-executable"));
    }

    #[cfg(unix)]
    #[test]
    fn executable_without_execute_bit_is_rejected() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("not-executable");
        std::fs::write(&path, b"contents").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();

        let error = inspect_executable(&path).unwrap_err();

        assert_eq!(error.category(), "executable has no execute permission");
    }

    #[cfg(unix)]
    #[test]
    fn executable_fifo_is_rejected_without_blocking() {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("executable-fifo");
        let raw = CString::new(path.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(raw.as_ptr(), 0o700) }, 0);

        let error = inspect_executable(&path).unwrap_err();

        assert_eq!(error.category(), "executable is not a regular file");
    }

    #[cfg(unix)]
    #[test]
    fn executable_growth_during_read_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("growing-executable");
        write_test_executable(&path, b"approved");
        let requested_digest = executable_path_identity(&path);
        let mut file = super::open_executable(&path, &requested_digest).unwrap();

        let error = super::identity_from_open_file_inner(&mut file, &requested_digest, || {
            use std::io::Write;

            std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap()
                .write_all(b"-growth")
                .unwrap();
        })
        .unwrap_err();

        assert_eq!(error.category(), "executable changed size while reading");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_memfd_exec_flag_matches_kernel_abi() {
        assert_eq!(super::MFD_EXEC_FLAG, 0x0010);
    }

    #[test]
    fn pinned_executable_rejects_replaced_content_without_exposing_path() {
        let directory = tempfile::tempdir().unwrap();
        let sentinel = "super-secret-token-runner";
        let path = directory.path().join(sentinel);
        write_test_executable(&path, b"approved");
        let identity = inspect_executable(&path).unwrap();
        std::fs::write(&path, b"replacement").unwrap();

        let error = pin_executable(&path, &identity).unwrap_err().to_string();

        assert!(!error.contains(sentinel));
        assert!(error.contains("implementation changed before execution"));
        assert!(error.contains(identity.path_digest()));
    }

    #[test]
    fn pinned_executable_rejects_different_canonical_path_with_identical_content() {
        let directory = tempfile::tempdir().unwrap();
        let approved_path = directory.path().join("approved-secret-executable");
        let redirected_path = directory.path().join("redirected-secret-executable");
        write_test_executable(&approved_path, b"identical executable bytes");
        write_test_executable(&redirected_path, b"identical executable bytes");
        let approved = inspect_executable(&approved_path).unwrap();

        let error = pin_executable(&redirected_path, &approved).unwrap_err().to_string();

        assert!(error.contains("path identity changed before execution"));
        assert!(!error.contains("approved-secret-executable"));
        assert!(!error.contains("redirected-secret-executable"));
    }

    #[test]
    fn pinned_executable_reports_the_identity_it_actually_pinned() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("approved");
        write_test_executable(&path, b"approved executable bytes");
        let approved = inspect_executable(&path).unwrap();

        let pinned = pin_executable(&path, &approved).unwrap();

        assert_eq!(pinned.identity(), &approved);
    }

    #[test]
    fn pinned_executable_opens_the_entry_after_the_pre_open_phase() {
        let directory = tempfile::tempdir().unwrap();
        let entry = directory.path().join("entry");
        let approved_backup = directory.path().join("approved-backup");
        let replacement = directory.path().join("replacement");
        write_test_executable(&entry, b"approved executable bytes");
        write_test_executable(&replacement, b"replacement executable bytes");
        let approved = inspect_executable(&entry).unwrap();

        let error = pin_executable_inner(&entry, &approved, |phase| {
            if phase == PinPhase::BeforeOpen {
                std::fs::rename(&entry, &approved_backup).unwrap();
                std::fs::rename(&replacement, &entry).unwrap();
            }
        })
        .unwrap_err()
        .to_string();

        assert!(error.contains("implementation changed before execution"));
    }

    #[cfg(unix)]
    #[test]
    fn pinned_executable_identity_comes_from_the_open_fd_after_entry_swap() {
        let directory = tempfile::tempdir().unwrap();
        let entry = directory.path().join("entry");
        let moved = directory.path().join("moved-after-open");
        let replacement = directory.path().join("replacement");
        write_test_executable(&entry, b"identical executable bytes");
        write_test_executable(&replacement, b"identical executable bytes");
        let approved = inspect_executable(&entry).unwrap();

        let error = pin_executable_inner(&entry, &approved, |phase| {
            if phase == PinPhase::AfterOpen {
                std::fs::rename(&entry, &moved).unwrap();
                std::fs::rename(&replacement, &entry).unwrap();
            }
        })
        .unwrap_err()
        .to_string();

        assert!(error.contains("path identity changed before execution"));
    }

    #[cfg(windows)]
    #[test]
    fn pinned_executable_handle_blocks_entry_replacement_until_child_creation() {
        use std::cell::Cell;

        let directory = tempfile::tempdir().unwrap();
        let entry = directory.path().join("entry.exe");
        let moved = directory.path().join("moved-after-open.exe");
        write_test_executable(&entry, b"approved executable bytes");
        let approved = inspect_executable(&entry).unwrap();
        let replacement_was_blocked = Cell::new(false);

        let pinned = pin_executable_inner(&entry, &approved, |phase| {
            if phase == PinPhase::AfterOpen {
                replacement_was_blocked.set(std::fs::rename(&entry, &moved).is_err());
            }
        })
        .unwrap();

        assert!(replacement_was_blocked.get());
        assert_eq!(pinned.identity(), &approved);
    }

    #[cfg(windows)]
    #[test]
    fn pinned_command_keeps_the_windows_image_handle_until_spawn() {
        let directory = tempfile::tempdir().unwrap();
        let entry = directory.path().join("entry.exe");
        let moved = directory.path().join("moved.exe");
        std::fs::copy(std::env::current_exe().unwrap(), &entry).unwrap();
        let approved = inspect_executable(&entry).unwrap();
        let command = pin_executable(&entry, &approved).unwrap().command().unwrap();

        assert!(std::fs::write(&entry, b"replacement").is_err());
        assert!(std::fs::rename(&entry, &moved).is_err());

        drop(command);
        std::fs::rename(&entry, &moved).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pinned_executable_runs_frozen_snapshot_after_source_overwrite() {
        let directory = tempfile::tempdir().unwrap();
        let shell = std::env::var_os("SHELL").expect("SHELL must identify the platform shell for process tests");
        let source = directory.path().join("approved-shell");
        std::fs::copy(shell, &source).unwrap();
        let approved = inspect_executable(&source).unwrap();
        let pinned = pin_executable(&source, &approved).unwrap();
        let pinned_identity = pinned.identity().clone();
        std::fs::write(&source, b"replacement must never execute").unwrap();

        let mut command = pinned.command().unwrap();
        command.args(["-c", "printf approved-snapshot"]);
        let result = CommandRunner::new_pinned(command).run().await.unwrap();

        assert_eq!(result.exit_code, Some(0));
        assert_eq!(result.stdout, b"approved-snapshot");
        assert_eq!(pinned_identity, approved);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn prepared_snapshot_is_not_inherited_by_an_unrelated_exec() {
        use std::os::fd::RawFd;

        let directory = tempfile::tempdir().unwrap();
        let shell = std::env::var_os("SHELL").expect("SHELL must identify the platform shell for process tests");
        let source = directory.path().join("approved-shell");
        std::fs::copy(shell, &source).unwrap();
        let approved = inspect_executable(&source).unwrap();
        let pinned = pin_executable(&source, &approved).unwrap();
        let snapshot_fd_flags = pinned.snapshot_fd_flags();
        let mut target = pinned.command().unwrap();
        target.args(["-c", "printf child-only-snapshot"]);

        assert_ne!(snapshot_fd_flags & libc::FD_CLOEXEC, 0);
        let reserved: RawFd = std::path::Path::new(target.program_for_test())
            .file_name()
            .unwrap()
            .to_string_lossy()
            .parse()
            .unwrap();
        assert_ne!(unsafe { libc::fcntl(reserved, libc::F_GETFD) } & libc::FD_CLOEXEC, 0);

        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "runner::runner_test::tests::inherited_snapshot_probe_helper",
                "--nocapture",
            ])
            .env("SOLARIS_PROCESS_SNAPSHOT_PROBE", approved.content_digest())
            .status()
            .unwrap();
        assert!(status.success());

        let result = CommandRunner::new_pinned(target).run().await.unwrap();
        assert_eq!(result.exit_code, Some(0));
        assert_eq!(result.stdout, b"child-only-snapshot");
    }

    #[cfg(unix)]
    #[test]
    fn inherited_snapshot_probe_helper() {
        use sha2::{Digest, Sha256};
        use std::os::unix::fs::FileExt;

        let Some(expected_digest) = std::env::var("SOLARIS_PROCESS_SNAPSHOT_PROBE").ok() else {
            return;
        };
        let descriptor_root = if std::path::Path::new("/proc/self/fd").is_dir() {
            std::path::Path::new("/proc/self/fd")
        } else {
            std::path::Path::new("/dev/fd")
        };
        for entry in std::fs::read_dir(descriptor_root).unwrap() {
            let entry = entry.unwrap();
            let Ok(metadata) = entry.path().metadata() else {
                continue;
            };
            if !metadata.is_file() {
                continue;
            }
            let Ok(file) = std::fs::File::open(entry.path()) else {
                continue;
            };
            let mut hasher = Sha256::new();
            let mut offset = 0_u64;
            let mut buffer = [0_u8; 8192];
            loop {
                let read = file.read_at(&mut buffer, offset).unwrap();
                if read == 0 {
                    break;
                }
                hasher.update(&buffer[..read]);
                offset += read as u64;
            }
            if format!("{:x}", hasher.finalize()) != expected_digest {
                continue;
            }
            let descriptor: libc::c_int = entry.file_name().to_string_lossy().parse().unwrap();
            #[cfg(target_os = "macos")]
            let _ = unsafe { libc::fchflags(descriptor, 0) };
            let byte = b"x";
            let write_result = unsafe { libc::pwrite(descriptor, byte.as_ptr().cast(), byte.len(), 0) };
            let truncate_result = unsafe { libc::ftruncate(descriptor, 0) };
            assert_eq!(write_result, -1);
            assert_eq!(truncate_result, -1);
            panic!("an unrelated exec inherited the executable snapshot");
        }
    }

    #[cfg(unix)]
    #[test]
    fn pinned_executable_snapshot_is_private_and_removed_on_drop() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("approved");
        write_test_executable(&source, b"approved executable bytes");
        let approved = inspect_executable(&source).unwrap();
        let pinned = pin_executable(&source, &approved).unwrap();
        let snapshot_path = pinned.snapshot_test_path();
        let metadata = std::fs::metadata(&snapshot_path).unwrap();

        assert_ne!(pinned.snapshot_fd_flags() & libc::FD_CLOEXEC, 0);
        #[cfg(target_os = "linux")]
        {
            let required = libc::F_SEAL_WRITE | libc::F_SEAL_GROW | libc::F_SEAL_SHRINK | libc::F_SEAL_SEAL;
            assert_eq!(pinned.snapshot_linux_seals() & required, required);
        }
        assert_eq!(metadata.permissions().mode() & 0o777, 0o500);
        assert!(snapshot_path.exists());
        #[cfg(not(target_os = "linux"))]
        {
            assert_eq!(pinned.snapshot_link_count(), 0);
        }
        #[cfg(target_os = "macos")]
        {
            assert_eq!(libc::UF_IMMUTABLE, 0x0000_0002);
            assert_ne!(pinned.snapshot_macos_flags() & libc::UF_IMMUTABLE, 0);
            assert_eq!(pinned.snapshot_access_mode(), libc::O_RDWR);
        }
        #[cfg(all(not(target_os = "linux"), not(target_os = "macos")))]
        {
            assert_eq!(pinned.snapshot_access_mode(), libc::O_RDONLY);
            assert!(std::fs::OpenOptions::new().write(true).open(&snapshot_path).is_err());
        }
        drop(pinned);
        assert!(!snapshot_path.exists());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_snapshot_kernel_rejects_write_and_truncate_from_forked_dup() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("approved");
        write_test_executable(&source, b"approved executable bytes");
        let approved = inspect_executable(&source).unwrap();
        let pinned = pin_executable(&source, &approved).unwrap();
        let snapshot_fd = pinned.snapshot_raw_fd();

        let child = unsafe { libc::fork() };
        assert_ne!(child, -1);
        if child == 0 {
            let duplicate = unsafe { libc::dup(snapshot_fd) };
            let byte = [b'x'];
            let write_result = if duplicate == -1 {
                -1
            } else {
                unsafe { libc::pwrite(duplicate, byte.as_ptr().cast(), byte.len(), 0) }
            };
            let truncate_result = if duplicate == -1 {
                -1
            } else {
                unsafe { libc::ftruncate(duplicate, 0) }
            };
            if duplicate != -1 {
                unsafe { libc::close(duplicate) };
            }
            unsafe { libc::_exit(i32::from(write_result != -1 || truncate_result != -1)) };
        }

        let mut status = 0;
        assert_eq!(unsafe { libc::waitpid(child, &mut status, 0) }, child);
        assert!(libc::WIFEXITED(status));
        assert_eq!(libc::WEXITSTATUS(status), 0);
        assert_eq!(pinned.snapshot_macos_flags() & libc::UF_IMMUTABLE, libc::UF_IMMUTABLE);
        assert!(pinned.command().is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn pinned_executable_rejects_redirected_symlink_with_identical_content() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let approved_path = directory.path().join("approved");
        let entry = directory.path().join("entry");
        write_test_executable(&approved_path, b"identical executable bytes");
        symlink(&approved_path, &entry).unwrap();

        let error = inspect_executable(&entry).unwrap_err().to_string();

        assert!(error.contains("executable pin open failed"));
    }

    #[tokio::test]
    async fn runner_preserves_stdout_emitted_before_timeout() {
        let result = CommandRunner::new(timeout_output_command("stdout"))
            .timeout(Duration::from_secs(3))
            .run()
            .await
            .expect("runner should return timeout result");

        assert!(result.timed_out);
        assert_eq!(result.exit_code, None);
        assert!(
            String::from_utf8_lossy(&result.stdout).contains("runner_stdout_before_timeout"),
            "stdout was: {}",
            String::from_utf8_lossy(&result.stdout)
        );
    }

    #[tokio::test]
    async fn runner_preserves_stderr_emitted_before_timeout() {
        let result = CommandRunner::new(timeout_output_command("stderr"))
            .timeout(Duration::from_secs(3))
            .run()
            .await
            .expect("runner should return timeout result");

        assert!(result.timed_out);
        assert_eq!(result.exit_code, None);
        assert!(
            String::from_utf8_lossy(&result.stderr).contains("runner_stderr_before_timeout"),
            "stderr was: {}",
            String::from_utf8_lossy(&result.stderr)
        );
    }

    #[tokio::test]
    async fn runner_returns_exit_code_and_output_for_completed_command() {
        #[cfg(windows)]
        let script = "Write-Output runner_completed_stdout; Write-Error runner_completed_stderr; exit 7";
        #[cfg(not(windows))]
        let script = "printf 'runner_completed_stdout\n'; printf 'runner_completed_stderr\n' >&2; exit 7";

        let command = shell_command(script);
        let result = CommandRunner::new(command).run().await.expect("runner should complete");

        assert!(!result.timed_out);
        assert_eq!(result.exit_code, Some(7));
        assert!(
            String::from_utf8_lossy(&result.stdout).contains("runner_completed_stdout"),
            "stdout was: {}",
            String::from_utf8_lossy(&result.stdout)
        );
        assert!(
            String::from_utf8_lossy(&result.stderr).contains("runner_completed_stderr"),
            "stderr was: {}",
            String::from_utf8_lossy(&result.stderr)
        );
    }

    #[tokio::test]
    async fn runner_defaults_to_ambient_launch_policy() {
        let directory = tempfile::tempdir().unwrap();
        let marker = directory.path().join("ambient-marker");
        #[cfg(windows)]
        let script = format!(
            "Set-Content -LiteralPath '{}' -Value launched",
            marker.to_string_lossy().replace('\'', "''")
        );
        #[cfg(not(windows))]
        let script = format!("printf launched > '{}'", marker.to_string_lossy());

        let result = CommandRunner::new(shell_command(&script)).run().await.unwrap();

        assert_eq!(result.exit_code, Some(0));
        assert_eq!(std::fs::read_to_string(marker).unwrap().trim(), "launched");
    }

    struct DenySpawn;

    impl ProcessSpawnAuthorizer for DenySpawn {
        fn authorize_and_spawn(&self, _spawn: ProcessSpawn) -> std::io::Result<crate::ManagedChild> {
            Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "spawn denied by final authorization",
            ))
        }
    }

    #[tokio::test]
    async fn spawn_authorizer_runs_before_the_process_starts() {
        let directory = tempfile::tempdir().unwrap();
        let marker = directory.path().join("authorizer-marker");
        #[cfg(windows)]
        let script = format!(
            "Set-Content -LiteralPath '{}' -Value launched",
            marker.to_string_lossy().replace('\'', "''")
        );
        #[cfg(not(windows))]
        let script = format!("printf launched > '{}'", marker.to_string_lossy());

        let error = CommandRunner::new(shell_command(&script))
            .spawn_authorizer(ProcessSpawnAuthorization::new(Arc::new(DenySpawn)))
            .run()
            .await
            .unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
        assert!(!marker.exists());
    }

    #[tokio::test]
    async fn unpinned_workspace_sandbox_fails_before_spawn() {
        let workspace = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let marker = workspace.path().join("must-not-launch");
        #[cfg(windows)]
        let script = format!(
            "Set-Content -LiteralPath '{}' -Value launched",
            marker.to_string_lossy().replace('\'', "''")
        );
        #[cfg(not(windows))]
        let script = format!("printf launched > '{}'", marker.to_string_lossy());
        let policy = ProcessLaunchPolicy::workspace_sandbox(workspace.path(), [state.path().to_path_buf()]);

        let error = CommandRunner::new(shell_command(&script))
            .launch_policy(policy)
            .run()
            .await
            .unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
        assert_eq!(
            error.get_ref().and_then(|source| source.downcast_ref::<SandboxError>()),
            Some(&SandboxError::ExecutableNotPinned)
        );
        assert!(!marker.exists());
    }

    #[tokio::test]
    async fn runner_writes_configured_stdin_before_process_completion() {
        #[cfg(windows)]
        let command = shell_command("$value = [Console]::In.ReadToEnd(); [Console]::Out.Write($value)");
        #[cfg(not(windows))]
        let command = shell_command("cat");

        let result = CommandRunner::new(command)
            .stdin_bytes(b"plugin-input".to_vec())
            .run()
            .await
            .expect("runner should write stdin");

        assert_eq!(result.exit_code, Some(0));
        assert_eq!(result.stdout, b"plugin-input");
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn runner_does_not_hang_when_background_process_keeps_output_pipe_open() {
        let command = shell_command("printf 'background_parent_done\n'; sleep 5 &");

        let result = tokio::time::timeout(
            Duration::from_millis(700),
            CommandRunner::new(command)
                .post_process_drain(Duration::from_millis(50))
                .run(),
        )
        .await
        .expect("runner should return before the background child closes inherited output pipes")
        .expect("runner should complete successfully");

        assert!(!result.timed_out);
        assert_eq!(result.exit_code, Some(0));
        assert!(
            String::from_utf8_lossy(&result.stdout).contains("background_parent_done"),
            "stdout was: {}",
            String::from_utf8_lossy(&result.stdout)
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn runner_timeout_kills_process_group() {
        let result = CommandRunner::new(shell_command("sleep 5 & echo $!; wait"))
            .timeout(Duration::from_millis(300))
            .post_process_drain(Duration::from_millis(100))
            .run()
            .await
            .expect("runner should return timeout result");

        assert!(result.timed_out);
        let stdout = String::from_utf8_lossy(&result.stdout);
        let sleep_pid = stdout
            .lines()
            .find_map(|line| line.trim().parse::<u32>().ok())
            .expect("script should print background sleep pid");

        assert_process_exits(sleep_pid).await;
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn runner_timeout_kills_windows_job_descendant() {
        let script = "$p = Start-Process -FilePath (Get-Process -Id $PID).Path -ArgumentList '-NoProfile', '-Command', 'Start-Sleep -Seconds 10' -PassThru -WindowStyle Hidden; Write-Output $p.Id; Wait-Process -Id $p.Id";
        let result = CommandRunner::new(shell_command(script))
            .timeout(Duration::from_millis(5000))
            .post_process_drain(Duration::from_millis(250))
            .run()
            .await
            .expect("runner should return timeout result");

        assert!(result.timed_out);
        let stdout = String::from_utf8_lossy(&result.stdout);
        let sleep_pid = stdout
            .lines()
            .find_map(|line| line.trim().parse::<u32>().ok())
            .expect("script should print child process pid");

        assert_process_exits(sleep_pid).await;
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn runner_completed_command_kills_windows_background_descendant() {
        let script = "$p = Start-Process -FilePath (Get-Process -Id $PID).Path -ArgumentList '-NoProfile', '-Command', 'Start-Sleep -Seconds 10' -PassThru -WindowStyle Hidden; Write-Output $p.Id";
        let result = CommandRunner::new(shell_command(script))
            .run()
            .await
            .expect("runner should complete successfully");

        assert!(!result.timed_out);
        assert_eq!(result.exit_code, Some(0));
        let stdout = String::from_utf8_lossy(&result.stdout);
        let sleep_pid = stdout
            .lines()
            .find_map(|line| line.trim().parse::<u32>().ok())
            .expect("script should print child process pid");

        assert_process_exits(sleep_pid).await;
    }

    #[cfg(windows)]
    fn shell_command(script: &str) -> Command {
        let mut command = Command::new(powershell_command());
        command.args(["-NoProfile", "-Command", script]);
        command
    }

    #[tokio::test]
    async fn runner_terminates_process_tree_when_output_limit_is_exceeded() {
        #[cfg(windows)]
        let script = "while ($true) { [Console]::Out.Write('0123456789') }";
        #[cfg(not(windows))]
        let script = "while :; do printf '0123456789'; done";

        let result = CommandRunner::new(shell_command(script))
            .timeout(Duration::from_secs(10))
            .max_output_bytes(1024)
            .run()
            .await
            .expect("runner should return a bounded output result");

        assert!(result.output_limit_exceeded);
        assert!(!result.timed_out);
        assert!(result.stdout.len() <= 1024);
        assert!(result.stderr.len() <= 1024);
    }

    #[tokio::test]
    async fn root_exit_kills_background_descendant_before_late_output() {
        #[cfg(windows)]
        let script = "$p = Start-Process -FilePath (Get-Process -Id $PID).Path -ArgumentList '-NoProfile', '-Command', \"while (`$true) { [Console]::Out.Write('0123456789') }\" -PassThru -NoNewWindow; [Console]::Error.WriteLine($p.Id)";
        #[cfg(not(windows))]
        let script = "while :; do printf '0123456789'; done & echo $! >&2";

        let result = CommandRunner::new(shell_command(script))
            .timeout(Duration::from_secs(10))
            .post_process_drain(Duration::from_secs(2))
            .max_output_bytes(1024)
            .run()
            .await
            .expect("runner should terminate descendants after the root exits");

        assert!(!result.output_limit_exceeded);
        let descendant_pid = String::from_utf8_lossy(&result.stderr)
            .lines()
            .find_map(|line| line.trim().parse::<u32>().ok())
            .expect("parent should report background descendant pid");
        assert_process_exits(descendant_pid).await;
    }

    #[tokio::test]
    async fn dropping_runner_future_terminates_descendant_process_tree() {
        let directory = tempfile::tempdir().unwrap();
        let marker = directory.path().join("descendant.pid");
        #[cfg(windows)]
        let script = format!(
            "$p = Start-Process -FilePath (Get-Process -Id $PID).Path -ArgumentList '-NoProfile', '-Command', 'Start-Sleep -Seconds 10' -PassThru -WindowStyle Hidden; Set-Content -LiteralPath '{}' -Value $p.Id; Wait-Process -Id $p.Id",
            marker.to_string_lossy().replace('\'', "''")
        );
        #[cfg(not(windows))]
        let script = format!("sleep 10 & echo $! > '{}'; wait", marker.to_string_lossy());
        let task = tokio::spawn(
            CommandRunner::new(shell_command(&script))
                .timeout(Duration::from_secs(30))
                .run(),
        );
        let descendant_pid = wait_for_pid_file(&marker).await;

        task.abort();
        let error = task.await.expect_err("runner task should be cancelled");
        assert!(error.is_cancelled());
        assert_process_exits(descendant_pid).await;
    }

    #[cfg(windows)]
    fn powershell_command() -> &'static str {
        static COMMAND: std::sync::OnceLock<&'static str> = std::sync::OnceLock::new();

        COMMAND.get_or_init(|| {
            ["pwsh.exe", "powershell.exe"]
                .into_iter()
                .find(|candidate| {
                    std::process::Command::new(candidate)
                        .args(["-NoLogo", "-NoProfile", "-Command", "exit 0"])
                        .status()
                        .is_ok_and(|status| status.success())
                })
                .unwrap_or_else(|| panic!("PowerShell is required to run Windows process tests"))
        })
    }

    #[cfg(not(windows))]
    fn shell_command(script: &str) -> Command {
        let shell = std::env::var_os("SHELL").expect("SHELL must identify the platform shell for process tests");
        let mut command = Command::new(shell);
        command.args(["-c", script]);
        command
    }

    #[cfg(unix)]
    async fn assert_process_exits(pid: u32) {
        for _ in 0..20 {
            if !process_alive(pid) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        panic!("process {pid} was still alive after process-group timeout kill");
    }

    async fn wait_for_pid_file(path: &std::path::Path) -> u32 {
        for _ in 0..100 {
            if let Ok(value) = std::fs::read_to_string(path)
                && let Ok(pid) = value.trim().parse()
            {
                return pid;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        panic!("descendant process did not publish its pid");
    }

    #[cfg(unix)]
    fn process_alive(pid: u32) -> bool {
        let Ok(target) = i32::try_from(pid) else {
            return false;
        };

        let rc = unsafe { libc::kill(target, 0) };
        if rc == 0 {
            return true;
        }

        !matches!(std::io::Error::last_os_error().raw_os_error(), Some(libc::ESRCH))
    }

    #[cfg(windows)]
    async fn assert_process_exits(pid: u32) {
        for _ in 0..20 {
            if !process_alive(pid) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        panic!("process {pid} was still alive after job timeout kill");
    }

    #[cfg(windows)]
    fn process_alive(pid: u32) -> bool {
        use windows_sys::Win32::Foundation::{CloseHandle, WAIT_TIMEOUT};
        use windows_sys::Win32::System::Threading::{
            OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, WaitForSingleObject,
        };

        const SYNCHRONIZE: u32 = 0x0010_0000;

        let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION | SYNCHRONIZE, 0, pid) };
        if handle.is_null() {
            return false;
        }

        let wait_result = unsafe { WaitForSingleObject(handle, 0) };
        unsafe { CloseHandle(handle) };

        wait_result == WAIT_TIMEOUT
    }

    include!("runner_environment_test.rs");
}
