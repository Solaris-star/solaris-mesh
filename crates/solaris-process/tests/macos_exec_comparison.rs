#![cfg(target_os = "macos")]

use std::env;
use std::fs::{self, File, OpenOptions, Permissions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};
use solaris_process::{CommandRunner, inspect_executable, pin_executable};

const PROOF_KEY: &str = "SOLARIS_MACOS_EXEC_COMPARISON_PROOF";
const CASE_TIMEOUT: Duration = Duration::from_secs(3);

#[test]
fn macos_exec_comparison() {
    let directory = tempfile::tempdir().unwrap();
    let proof = directory.path().join("comparison-complete");
    let mut child = Command::new(env::current_exe().unwrap())
        .args(["--exact", "macos_exec_comparison_child", "--nocapture"])
        .env(PROOF_KEY, &proof)
        .process_group(0)
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "comparison subprocess failed");
            assert_eq!(fs::read(proof).unwrap(), b"all-four-cases-observed");
            return;
        }
        if Instant::now() >= deadline {
            let group = i32::try_from(child.id()).unwrap();
            assert!(group > 1);
            unsafe { libc::kill(-group, libc::SIGKILL) };
            let _ = child.kill();
            let _ = child.wait();
            panic!("macOS exec comparison exceeded its subprocess watchdog");
        }
        thread::sleep(Duration::from_millis(10));
    }
}

#[tokio::test]
async fn macos_exec_comparison_child() {
    let Some(proof) = env::var_os(PROOF_KEY) else {
        return;
    };
    let bytes = fs::read("/usr/bin/true").unwrap();
    assert!(!bytes.is_empty() && bytes.len() <= 64 * 1024 * 1024);
    let directory = tempfile::tempdir().unwrap();
    let named = directory.path().join("true-copy");
    let mut writer = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o700)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(&named)
        .unwrap();
    writer.write_all(&bytes).unwrap();
    writer.sync_all().unwrap();
    writer
        .set_permissions(Permissions::from_mode(0o500))
        .unwrap();
    // Open through the real filename: Darwin /dev/fd open duplicates the
    // existing file description and would not turn an O_RDWR fd read-only.
    let mut retained = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(&named)
        .unwrap();
    let original = writer.metadata().unwrap();
    let reopened = retained.metadata().unwrap();
    assert_eq!(
        (original.dev(), original.ino()),
        (reopened.dev(), reopened.ino())
    );
    drop(writer);
    verify_copy(&mut retained, &bytes, 1);
    let fd = retained.as_raw_fd();
    let descriptor_path = format!("/dev/fd/{fd}");

    run_case("named_copy", &named, None);
    verify_copy(&mut retained, &bytes, 1);
    run_case("linked_fd", Path::new(&descriptor_path), Some(fd));
    verify_copy(&mut retained, &bytes, 1);
    fs::remove_file(&named).unwrap();
    assert!(!named.exists());
    verify_copy(&mut retained, &bytes, 0);
    run_case("unlinked_fd", Path::new(&descriptor_path), Some(fd));
    verify_copy(&mut retained, &bytes, 0);

    // Exercise the public production pin/command path independently. This
    // uses its existing anonymous UF_IMMUTABLE snapshot, without policy edits.
    let approved = inspect_executable(Path::new("/usr/bin/true")).unwrap();
    assert_eq!(
        approved.content_digest(),
        Sha256::digest(&bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );
    let pinned = pin_executable(Path::new("/usr/bin/true"), &approved).unwrap();
    let command = pinned.command().unwrap();
    match tokio::time::timeout(CASE_TIMEOUT, CommandRunner::new_pinned(command).run()).await {
        Ok(Ok(result)) => println!(
            "macos_exec_comparison label=production_pin result=exit code={:?}",
            result.exit_code
        ),
        Ok(Err(error)) => emit_error("production_pin", &error),
        Err(_) => println!("macos_exec_comparison label=production_pin result=timeout"),
    }
    io::stdout().flush().unwrap();
    fs::write(proof, b"all-four-cases-observed").unwrap();
}

fn verify_copy(file: &mut File, expected: &[u8], links: u64) {
    let metadata = file.metadata().unwrap();
    assert!(metadata.is_file());
    assert_eq!(metadata.permissions().mode() & 0o777, 0o500);
    assert_eq!(metadata.nlink(), links);
    assert_eq!(metadata.len(), expected.len() as u64);
    let flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFL) };
    assert!(flags >= 0);
    assert_eq!(flags & libc::O_ACCMODE, libc::O_RDONLY);
    let flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFD) };
    assert!(flags >= 0);
    assert_ne!(flags & libc::FD_CLOEXEC, 0);
    file.seek(SeekFrom::Start(0)).unwrap();
    let mut actual = Vec::new();
    file.read_to_end(&mut actual).unwrap();
    assert!(actual == expected, "copied executable bytes changed");
}

fn run_case(label: &str, path: &Path, inherited: Option<RawFd>) {
    let mut command = Command::new(path);
    command
        .env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    // Force the same fork/exec path in all three controls. Only the intended
    // child's descriptor loses CLOEXEC; the parent retains its private fd.
    unsafe {
        command.pre_exec(move || {
            if let Some(descriptor) = inherited {
                let flags = libc::fcntl(descriptor, libc::F_GETFD);
                if flags == -1
                    || libc::fcntl(descriptor, libc::F_SETFD, flags & !libc::FD_CLOEXEC) == -1
                {
                    return Err(io::Error::last_os_error());
                }
            }
            Ok(())
        });
    }
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            emit_error(label, &error);
            return;
        }
    };
    let deadline = Instant::now() + CASE_TIMEOUT;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            println!(
                "macos_exec_comparison label={label} result=exit code={:?} signal={:?}",
                status.code(),
                status.signal()
            );
            return;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            println!("macos_exec_comparison label={label} result=timeout");
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn emit_error(label: &str, error: &io::Error) {
    println!(
        "macos_exec_comparison label={label} result=error kind={:?} raw_os_error={:?}",
        error.kind(),
        error.raw_os_error()
    );
}
