#[cfg(any(unix, windows))]
fn main() {
    #[cfg(unix)]
    {
        let arguments = std::env::args_os().collect::<Vec<_>>();
        if unix_guardian::matches(&arguments) {
            if unix_guardian::run(arguments).is_err() {
                std::process::exit(125);
            }
            return;
        }
    }
    #[cfg(target_os = "linux")]
    if std::env::args_os().skip(1).eq(["--solaris-sandbox-probe-descendant"]) {
        std::thread::sleep(std::time::Duration::from_secs(30));
        return;
    }
    if std::env::args_os().skip(1).eq(["--solaris-sandbox-probe-target"]) {
        #[cfg(target_os = "linux")]
        if spawn_linux_probe_descendant().is_err() {
            std::process::exit(125);
        }
        return;
    }
    #[cfg(target_os = "linux")]
    if linux::run().is_err() {
        eprintln!("solaris-sandbox-helper: setup failed");
        std::process::exit(125);
    }
    #[cfg(target_os = "macos")]
    if macos::run().is_err() {
        eprintln!("solaris-sandbox-helper: setup failed");
        std::process::exit(125);
    }
    #[cfg(windows)]
    if let Err(error) = windows::run() {
        eprintln!(
            "solaris-sandbox-helper: setup failed (kind={:?}, os={:?}, detail={})",
            error.kind(),
            error.raw_os_error(),
            error
        );
        std::process::exit(125);
    }
    #[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
    {
        eprintln!("solaris-sandbox-helper: unsupported platform invocation");
        std::process::exit(125);
    }
}

#[cfg(target_os = "linux")]
#[allow(clippy::zombie_processes)]
fn spawn_linux_probe_descendant() -> std::io::Result<()> {
    std::process::Command::new(std::env::current_exe()?)
        .arg("--solaris-sandbox-probe-descendant")
        .spawn()?;
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn main() {
    eprintln!("solaris-sandbox-helper: unsupported platform");
    std::process::exit(125);
}

#[cfg(windows)]
#[path = "sandbox_helper_windows/mod.rs"]
mod windows;

#[cfg(target_os = "linux")]
mod linux {
    use std::ffi::OsString;
    use std::io::{self, Write};
    use std::path::{Path, PathBuf};
    use std::process::{Command, ExitStatus};

    use landlock::{
        ABI, Access, AccessFs, BitFlags, CompatLevel, Compatible, PathBeneath, PathFd, Ruleset, RulesetAttr,
        RulesetCreatedAttr, RulesetStatus,
    };

    use super::sandbox_helper_proxy::LocalProxy;

    pub(super) fn run() -> io::Result<()> {
        let invocation = Invocation::parse(std::env::args_os().skip(1))?;
        let mut status = prepare_status(&invocation.status)?;
        enforce_landlock(&invocation.workspace)?;
        protect_control_descriptors()?;
        let _proxy = invocation
            .proxy
            .as_ref()
            .map(|(socket, port)| LocalProxy::start(socket, *port))
            .transpose()?;
        // The outer runner entered Bubblewrap with an explicit envp. Forward
        // exactly that bounded environment without consulting PATH.
        let environment = std::env::vars_os().collect::<Vec<_>>();
        let mut child = Command::new(&invocation.target);
        child.args(&invocation.target_args).env_clear().envs(environment);
        let mut child = child.spawn()?;
        // Do not acknowledge Full until exec of the pinned target succeeded.
        // A missing or invalid target therefore fails before the parent sees
        // the sandbox-start marker.
        publish_status(&mut status)?;
        mirror_status(child.wait()?)
    }

    struct Invocation {
        workspace: PathBuf,
        status: PathBuf,
        proxy: Option<(PathBuf, u16)>,
        target: PathBuf,
        target_args: Vec<OsString>,
    }

    impl Invocation {
        fn parse(arguments: impl IntoIterator<Item = OsString>) -> io::Result<Self> {
            let mut arguments = arguments.into_iter();
            if arguments.next().as_deref() != Some(std::ffi::OsStr::new("--workspace")) {
                return Err(invalid_input());
            }
            let workspace = arguments.next().map(PathBuf::from).ok_or_else(invalid_input)?;
            if arguments.next().as_deref() != Some(std::ffi::OsStr::new("--status")) {
                return Err(invalid_input());
            }
            let status = arguments.next().map(PathBuf::from).ok_or_else(invalid_input)?;
            let proxy = match arguments.next().as_deref() {
                Some(value) if value == std::ffi::OsStr::new("--proxy-socket") => {
                    let socket = arguments.next().map(PathBuf::from).ok_or_else(invalid_input)?;
                    if arguments.next().as_deref() != Some(std::ffi::OsStr::new("--proxy-port")) {
                        return Err(invalid_input());
                    }
                    let port = arguments
                        .next()
                        .and_then(|value| value.to_str().and_then(|value| value.parse::<u16>().ok()))
                        .filter(|port| *port != 0)
                        .ok_or_else(invalid_input)?;
                    if arguments.next().as_deref() != Some(std::ffi::OsStr::new("--")) {
                        return Err(invalid_input());
                    }
                    Some((socket, port))
                }
                Some(value) if value == std::ffi::OsStr::new("--") => None,
                _ => return Err(invalid_input()),
            };
            let target = arguments.next().map(PathBuf::from).ok_or_else(invalid_input)?;
            if !workspace.is_absolute()
                || !status.is_absolute()
                || !target.is_absolute()
                || proxy.as_ref().is_some_and(|(socket, _)| !socket.is_absolute())
            {
                return Err(invalid_input());
            }
            Ok(Self {
                workspace,
                status,
                proxy,
                target,
                target_args: arguments.collect(),
            })
        }
    }

    fn enforce_landlock(workspace: &Path) -> io::Result<()> {
        let abi = ABI::V3;
        let handled = AccessFs::from_all(abi);
        let read = AccessFs::from_read(abi);
        let write = AccessFs::from_all(abi);
        let mut ruleset = Ruleset::default()
            .set_compatibility(CompatLevel::HardRequirement)
            .handle_access(handled)
            .map_err(|_| permission_denied())?
            .create()
            .map_err(|_| permission_denied())?
            .set_compatibility(CompatLevel::HardRequirement);
        ruleset = add_rule(ruleset, Path::new("/"), read)?;
        for root in [
            workspace,
            Path::new("/tmp"),
            Path::new("/run"),
            Path::new("/home/solaris"),
            Path::new("/dev"),
        ] {
            ruleset = add_rule(ruleset, root, write)?;
        }
        let status = ruleset.restrict_self().map_err(|_| permission_denied())?;
        if status.ruleset != RulesetStatus::FullyEnforced || !status.no_new_privs {
            return Err(permission_denied());
        }
        Ok(())
    }

    fn add_rule(
        ruleset: landlock::RulesetCreated,
        path: &Path,
        access: BitFlags<AccessFs>,
    ) -> io::Result<landlock::RulesetCreated> {
        let descriptor = PathFd::new(path).map_err(|_| permission_denied())?;
        ruleset
            .add_rule(PathBeneath::new(descriptor, access).set_compatibility(CompatLevel::HardRequirement))
            .map_err(|_| permission_denied())
    }

    fn prepare_status(path: &Path) -> io::Result<std::fs::File> {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true).open(path)
    }

    fn protect_control_descriptors() -> io::Result<()> {
        if unsafe { libc::prctl(libc::PR_SET_DUMPABLE, 0) } == -1 {
            return Err(permission_denied());
        }
        Ok(())
    }

    fn publish_status(file: &mut std::fs::File) -> io::Result<()> {
        file.write_all(b"full\n")?;
        file.sync_all()
    }

    fn mirror_status(status: ExitStatus) -> io::Result<()> {
        if let Some(code) = status.code() {
            std::process::exit(code);
        }
        use std::os::unix::process::ExitStatusExt;

        std::process::exit(128 + status.signal().unwrap_or(1));
    }

    fn invalid_input() -> io::Error {
        io::Error::new(io::ErrorKind::InvalidInput, "invalid sandbox helper invocation")
    }

    fn permission_denied() -> io::Error {
        io::Error::new(io::ErrorKind::PermissionDenied, "sandbox enforcement failed")
    }
}

#[cfg(unix)]
#[path = "sandbox_helper_proxy.rs"]
mod sandbox_helper_proxy;

#[cfg(unix)]
#[path = "sandbox_helper_unix_guardian.rs"]
mod unix_guardian;

#[cfg(target_os = "macos")]
#[path = "sandbox_helper_macos.rs"]
mod macos;
