use std::ffi::OsString;
use std::io::Write;
#[cfg(unix)]
use std::os::fd::FromRawFd;
use std::path::PathBuf;
use std::time::Duration;

const BEHAVIOR_KEY: &str = "SOLARIS_SANDBOX_FIXTURE_BEHAVIOR";
const DESCENDANT_MARKER_KEY: &str = "SOLARIS_SANDBOX_FIXTURE_DESCENDANT_MARKER";

fn main() {
    if std::env::args_os().nth(1).as_deref() == Some(std::ffi::OsStr::new("--descendant")) {
        descendant();
        return;
    }
    let behavior = std::env::var(BEHAVIOR_KEY).unwrap_or_default();
    match behavior.as_str() {
        "early-exit" => std::process::exit(125),
        "partial-marker" => {
            let mut file = status_writer(std::env::args_os().skip(1)).unwrap_or_else(|| std::process::exit(125));
            file.write_all(b"ful").unwrap_or_else(|_| std::process::exit(125));
            file.flush().unwrap_or_else(|_| std::process::exit(125));
            std::process::exit(125);
        }
        "timeout-with-descendant" => {
            spawn_unwaited_descendant();
            std::thread::sleep(Duration::from_secs(60));
        }
        _ => std::process::exit(125),
    }
}

#[allow(clippy::zombie_processes)]
#[cfg(not(target_os = "macos"))]
fn spawn_unwaited_descendant() {
    // Deliberately leave this child uncooperative: the integration test
    // verifies that sandbox containment kills and reaps it after a timeout.
    let executable = std::env::current_exe().unwrap_or_else(|_| std::process::exit(125));
    std::process::Command::new(executable)
        .arg("--descendant")
        .spawn()
        .unwrap_or_else(|_| std::process::exit(125));
}

#[cfg(target_os = "macos")]
fn spawn_unwaited_descendant() {
    use std::os::unix::ffi::OsStrExt;

    let marker = std::env::var_os(DESCENDANT_MARKER_KEY).unwrap_or_else(|| std::process::exit(125));
    let marker = std::ffi::CString::new(marker.as_bytes()).unwrap_or_else(|_| std::process::exit(125));
    let pid = unsafe { libc::fork() };
    if pid == -1 {
        std::process::exit(125);
    }
    if pid == 0 {
        unsafe {
            libc::sleep(6);
            let descriptor = libc::open(marker.as_ptr(), libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC, 0o600);
            if descriptor == -1 {
                libc::_exit(125);
            }
            let bytes = b"escaped";
            let written = libc::write(descriptor, bytes.as_ptr().cast(), bytes.len());
            let _ = libc::close(descriptor);
            libc::_exit(if written == bytes.len() as isize { 0 } else { 125 });
        }
    }
}

fn status_writer(arguments: impl IntoIterator<Item = OsString>) -> Option<std::fs::File> {
    let mut arguments = arguments.into_iter();
    while let Some(argument) = arguments.next() {
        if argument == "--status" {
            let path = arguments.next().map(PathBuf::from)?;
            return std::fs::OpenOptions::new().write(true).create_new(true).open(path).ok();
        }
        #[cfg(unix)]
        if argument == "--status-fd" {
            let descriptor = arguments.next()?.to_str()?.parse::<i32>().ok()?;
            if descriptor < 3 {
                return None;
            }
            // SAFETY: the sandbox runner transferred this descriptor to the
            // fixture helper for the duration of this process.
            return Some(unsafe { std::fs::File::from_raw_fd(descriptor) });
        }
    }
    None
}

fn descendant() {
    let Some(marker) = std::env::var_os(DESCENDANT_MARKER_KEY) else {
        std::process::exit(125);
    };
    std::thread::sleep(Duration::from_secs(6));
    if std::fs::write(marker, b"escaped").is_err() {
        std::process::exit(125);
    }
}
