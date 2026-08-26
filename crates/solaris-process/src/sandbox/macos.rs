use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{self, Read};
use std::net::{Ipv4Addr, TcpListener};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use tokio::process::{Child, Command};

use super::identity::ProtectedIdentitySnapshot;
use super::layout::{ResolvedSandboxLayout, ensure_protected_roots_are_disjoint, resolve_path};
use super::macos_profile::{MacOsProfilePaths, build_profile};
use super::{
    SandboxBackend, SandboxCommandDisposition, SandboxEnforcement, SandboxError, SandboxReason, SandboxReport,
    SandboxRunner, insufficient_enforcement_error, sandbox_io_error, trusted_sandbox_helper,
};
use crate::NetworkProxyPolicy;
use crate::environment::{append_network_proxy_ca_environment, is_network_proxy_environment_key};
use crate::network_proxy::HostNetworkProxy;
use crate::runner::{PinnedExecutable, inspect_executable, pin_executable};

const SEATBELT_PATH: &str = "/usr/bin/sandbox-exec";
const PROBE_TARGET_ARGUMENT: &str = "--solaris-sandbox-probe-target";
const START_CONFIRMATION_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_SANITIZED_DESCRIPTORS: libc::rlim_t = 65_536;

fn prerequisite_report() -> SandboxReport {
    let seatbelt = Path::new(SEATBELT_PATH);
    if !seatbelt.is_file() || pin_support_executable(seatbelt, SandboxReason::SeatbeltUnavailable).is_err() {
        return unavailable_report(SandboxReason::SeatbeltUnavailable);
    }
    if trusted_sandbox_helper(None, None).is_err() {
        return unavailable_report(SandboxReason::HelperUnavailable);
    }
    let workspace = match private_directory("solaris-macos-sandbox-prerequisite-") {
        Ok(workspace) => workspace,
        Err(_) => return failed_macos_report(SandboxReason::SetupFailed),
    };
    let authority = match crate::WorkspaceRootAuthority::capture(workspace.path()) {
        Ok(authority) => authority,
        Err(_) => return failed_macos_report(SandboxReason::WorkspaceObjectBindingUnavailable),
    };
    if authority.seal(workspace.path()).is_err() {
        return failed_macos_report(SandboxReason::WorkspaceObjectBindingUnavailable);
    }
    full_report()
}

pub(super) fn probe_capability() -> SandboxReport {
    let prerequisite = prerequisite_report();
    if prerequisite.enforcement() != SandboxEnforcement::Full {
        return prerequisite;
    }
    match std::thread::Builder::new()
        .name("solaris-macos-sandbox-probe".to_owned())
        .spawn(run_capability_probe)
    {
        Ok(probe) => probe
            .join()
            .unwrap_or_else(|_| failed_macos_report(SandboxReason::StartConfirmationFailed)),
        Err(_) => failed_macos_report(SandboxReason::SetupFailed),
    }
}

fn run_capability_probe() -> SandboxReport {
    let result = (|| -> io::Result<SandboxReport> {
        let workspace = private_directory("solaris-macos-sandbox-probe-workspace-")?;
        let helper = trusted_sandbox_helper(None, None)?;
        let mut command = helper
            .command()
            .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?;
        command
            .arg(PROBE_TARGET_ARGUMENT)
            .current_dir(workspace.path())
            .launch_policy(crate::ProcessLaunchPolicy::workspace_sandbox(workspace.path(), []));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?;
        runtime.block_on(async move {
            let mut child = command.spawn()?;
            let report = child.sandbox_report();
            match tokio::time::timeout(START_CONFIRMATION_TIMEOUT, child.wait()).await {
                Ok(Ok(status)) if status.success() => Ok(report),
                Ok(Ok(_)) => Ok(failed_macos_report(SandboxReason::SetupFailed)),
                Ok(Err(error)) => Err(error),
                Err(_) => {
                    let _ = child.kill().await;
                    Ok(failed_macos_report(SandboxReason::StartConfirmationFailed))
                }
            }
        })
    })();
    match result {
        Ok(report) => report,
        Err(error) => {
            super::sandbox_report_from_error(&error).unwrap_or_else(|| failed_macos_report(SandboxReason::SetupFailed))
        }
    }
}

pub(super) struct MacOsSandbox {
    layout: ResolvedSandboxLayout,
    protected_identities: ProtectedIdentitySnapshot,
    seatbelt: PinnedExecutable,
    helper: PinnedExecutable,
    network_proxy: Option<HostNetworkProxy>,
    proxy_ca_path: Option<PathBuf>,
    private_home: tempfile::TempDir,
    private_tmp: tempfile::TempDir,
    private_state: tempfile::TempDir,
    _denied_root: tempfile::TempDir,
    denied_probe: PathBuf,
    denied_listener: TcpListener,
    status_reader: File,
    status_writer: Option<File>,
    proxy_listener: Option<TcpListener>,
    proxy_port: Option<u16>,
    report: SandboxReport,
    configured: bool,
}

impl MacOsSandbox {
    pub(super) fn prepare(layout: ResolvedSandboxLayout, network_policy: NetworkProxyPolicy) -> io::Result<Self> {
        let capability = prerequisite_report();
        if capability.enforcement() != SandboxEnforcement::Full {
            return Err(insufficient_enforcement_error(capability));
        }
        layout
            .workspace_capability
            .verify_macos_path_identity()
            .map_err(|_| workspace_binding_error())?;
        let seatbelt_path = Path::new(SEATBELT_PATH);
        #[cfg(feature = "sandbox-test-fixtures")]
        let fixture_helper = layout._test_helper_path.as_deref();
        #[cfg(not(feature = "sandbox-test-fixtures"))]
        let fixture_helper = None;
        let seatbelt = pin_support_executable(seatbelt_path, SandboxReason::SeatbeltUnavailable)?;
        let helper = trusted_sandbox_helper(Some(&layout.workspace_root), fixture_helper)?;
        let private_home = private_directory("solaris-macos-sandbox-home-")?;
        let private_tmp = private_directory("solaris-macos-sandbox-tmp-")?;
        let private_state = private_directory("solaris-macos-sandbox-state-")?;
        let denied_root = private_directory("solaris-macos-sandbox-denied-")?;
        ensure_private_sources_are_disjoint(
            &layout,
            [
                private_home.path(),
                private_tmp.path(),
                private_state.path(),
                denied_root.path(),
            ],
        )?;
        let denied_probe = denied_root.path().join("host-secret");
        std::fs::write(&denied_probe, b"ambient-readable")
            .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?;
        let denied_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?;
        let protected_identities =
            ProtectedIdentitySnapshot::capture(&layout.protected_roots, &layout.protected_object_identities)?;
        let (status_reader, status_writer) = start_channel()?;
        let (network_proxy, proxy_listener, proxy_port, proxy_ca_path) = if network_policy.is_empty() {
            (None, None, None, None)
        } else {
            let proxy = HostNetworkProxy::start(&private_state.path().join("network-proxy.sock"), network_policy)
                .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?;
            let proxy_ca_path = private_home.path().join(".solaris-network-proxy-ca.pem");
            proxy
                .create_ca_certificate_alias(&proxy_ca_path)
                .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?;
            let listener = bind_private_proxy_listener()?;
            let port = listener
                .local_addr()
                .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?
                .port();
            (Some(proxy), Some(listener), Some(port), Some(proxy_ca_path))
        };
        Ok(Self {
            layout,
            protected_identities,
            seatbelt,
            helper,
            private_home,
            private_tmp,
            private_state,
            _denied_root: denied_root,
            denied_probe,
            denied_listener,
            status_reader,
            status_writer: Some(status_writer),
            network_proxy,
            proxy_ca_path,
            proxy_listener,
            proxy_port,
            report: full_report(),
            configured: false,
        })
    }

    fn configure_inner(&mut self, command: &mut Command) -> io::Result<()> {
        if self.configured {
            return Err(sandbox_io_error(SandboxError::PreparationFailed));
        }
        let seatbelt_file = self
            .seatbelt
            .duplicate_macos_snapshot()
            .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?;
        let helper_file = self
            .helper
            .duplicate_macos_snapshot()
            .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?;
        let status_writer = self
            .status_writer
            .take()
            .ok_or_else(|| sandbox_io_error(SandboxError::PreparationFailed))?;
        let seatbelt_fd = seatbelt_file.as_raw_fd();
        let helper_fd = helper_file.as_raw_fd();
        let status_fd = status_writer.as_raw_fd();
        let descriptor_limit = descriptor_sanitize_limit()?;
        let current_dir = command
            .as_std()
            .get_current_dir()
            .map(resolve_path)
            .transpose()?
            .unwrap_or_else(|| self.layout.workspace_root.clone());
        if !current_dir.starts_with(&self.layout.workspace_root)
            || self
                .layout
                .protected_roots
                .iter()
                .any(|protected| current_dir.starts_with(protected))
        {
            return Err(sandbox_io_error(SandboxError::UnrepresentableSandboxLayout));
        }
        let target_args = command.as_std().get_args().map(OsStr::to_os_string).collect::<Vec<_>>();
        let proxy_ca_path = self.proxy_ca_path.as_deref();
        if let Some(proxy) = &self.network_proxy {
            proxy
                .verify_ca_certificate()
                .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?;
            proxy
                .verify_ca_certificate_alias(
                    proxy_ca_path.ok_or_else(|| sandbox_io_error(SandboxError::PreparationFailed))?,
                )
                .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?;
        }
        let environment = explicit_environment(
            command,
            self.private_home.path(),
            self.private_tmp.path(),
            self.proxy_port,
            proxy_ca_path,
        )?;
        let proxy_socket = self
            .proxy_port
            .map(|_| self.private_state.path().join("network-proxy.sock"));
        let profile = build_profile(&MacOsProfilePaths {
            workspace: &self.layout.workspace_root,
            private_home: self.private_home.path(),
            private_tmp: self.private_tmp.path(),
            private_state: self.private_state.path(),
            protected_roots: &self.layout.protected_roots,
            proxy_socket: proxy_socket.as_deref(),
            proxy_port: self.proxy_port,
            proxy_ca: proxy_ca_path,
        })?;
        let denied_port = self
            .denied_listener
            .local_addr()
            .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?
            .port();
        let proxy_listener = match self.proxy_port {
            Some(_) => Some(
                self.proxy_listener
                    .take()
                    .ok_or_else(|| sandbox_io_error(SandboxError::PreparationFailed))?,
            ),
            None => None,
        };
        let proxy_listener_fd = proxy_listener.as_ref().map(AsRawFd::as_raw_fd);
        let wrapper_path = PathBuf::from(format!("/dev/fd/{seatbelt_fd}"));
        replace_pinned_command(
            command,
            MacOsWrapperFiles {
                seatbelt: seatbelt_file,
                helper: helper_file,
                status_writer,
                proxy_listener,
            },
            descriptor_limit,
            proxy_listener_fd,
            |target_fd| {
                let argv = build_seatbelt_argv(
                    &profile,
                    &self.layout.workspace_root,
                    status_fd,
                    &self.denied_probe,
                    denied_port,
                    target_fd,
                    helper_fd,
                    &target_args,
                    proxy_socket.as_deref().zip(proxy_listener_fd),
                );
                let mut wrapper = Command::new(wrapper_path);
                wrapper
                    .args(argv.iter().skip(1))
                    .current_dir(current_dir)
                    .env_clear()
                    .envs(environment);
                wrapper
            },
        )?;
        self.configured = true;
        Ok(())
    }

    fn verify_identities(&mut self) -> io::Result<()> {
        if std::fs::read(&self.denied_probe).ok().as_deref() != Some(b"ambient-readable") {
            return Err(sandbox_io_error(SandboxError::PreparationFailed));
        }
        // Seatbelt consumes a path rule, so make the last same-object check as
        // close to sandbox-exec spawn as the process API permits. The retained
        // directory handle remains owned by `layout` through child cleanup.
        self.layout
            .workspace_capability
            .verify_macos_path_identity()
            .map_err(|_| workspace_binding_error())?;
        let workspace_directory = self
            .layout
            .workspace_capability
            .duplicate_macos_directory()
            .map_err(|_| workspace_binding_error())?;
        self.protected_identities.verify_macos_workspace(
            &self.layout.protected_roots,
            &self.layout.workspace_root,
            workspace_directory,
        )
    }

    fn wait_for_start(&mut self, child: &mut Child) -> io::Result<()> {
        let deadline = Instant::now() + START_CONFIRMATION_TIMEOUT;
        let mut observed = Vec::with_capacity(b"full\n".len());
        loop {
            let mut chunk = [0_u8; 16];
            match self.status_reader.read(&mut chunk) {
                Ok(0) => return self.fail_start(SandboxReason::StartConfirmationFailed),
                Ok(read) => {
                    observed.extend_from_slice(&chunk[..read]);
                    if observed == b"full\n" {
                        return Ok(());
                    }
                    if !b"full\n".starts_with(&observed) {
                        return self.fail_start(SandboxReason::StartConfirmationFailed);
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => return self.fail_start(SandboxReason::StartConfirmationFailed),
            }
            match child.try_wait() {
                Ok(Some(_)) | Err(_) => return self.fail_start(SandboxReason::StartConfirmationFailed),
                Ok(None) => {}
            }
            if Instant::now() >= deadline {
                return self.fail_start(SandboxReason::StartConfirmationFailed);
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn fail_start<T>(&mut self, reason: SandboxReason) -> io::Result<T> {
        self.report = failed_macos_report(reason);
        Err(insufficient_enforcement_error(self.report))
    }
}

impl SandboxRunner for MacOsSandbox {
    fn report(&self) -> SandboxReport {
        self.report
    }

    fn configure(&mut self, command: &mut Command) -> io::Result<SandboxCommandDisposition> {
        self.configure_inner(command)?;
        Ok(SandboxCommandDisposition::Replaced)
    }

    fn verify_before_spawn(&mut self) -> io::Result<()> {
        self.verify_identities()
    }

    fn confirm_started(&mut self, child: &mut Child) -> io::Result<()> {
        self.wait_for_start(child)
    }
}

fn full_report() -> SandboxReport {
    SandboxReport::new(
        SandboxEnforcement::Full,
        SandboxBackend::MacOsSeatbelt,
        SandboxReason::Enforced,
    )
}

fn unavailable_report(reason: SandboxReason) -> SandboxReport {
    SandboxReport::new(SandboxEnforcement::Unavailable, SandboxBackend::None, reason)
}

fn failed_macos_report(reason: SandboxReason) -> SandboxReport {
    SandboxReport::new(SandboxEnforcement::Unavailable, SandboxBackend::MacOsSeatbelt, reason)
}

fn workspace_binding_error() -> io::Error {
    insufficient_enforcement_error(failed_macos_report(SandboxReason::WorkspaceObjectBindingUnavailable))
}

fn pin_support_executable(path: &Path, reason: SandboxReason) -> io::Result<PinnedExecutable> {
    let identity = inspect_executable(path).map_err(|_| insufficient_enforcement_error(unavailable_report(reason)))?;
    pin_executable(path, &identity).map_err(|_| insufficient_enforcement_error(unavailable_report(reason)))
}

fn private_directory(prefix: &str) -> io::Result<tempfile::TempDir> {
    tempfile::Builder::new()
        .prefix(prefix)
        .tempdir()
        .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))
}

fn ensure_private_sources_are_disjoint<'a>(
    layout: &ResolvedSandboxLayout,
    private_roots: impl IntoIterator<Item = &'a Path>,
) -> io::Result<()> {
    let private_roots = private_roots
        .into_iter()
        .map(resolve_path)
        .collect::<io::Result<Vec<_>>>()?;
    if private_roots
        .iter()
        .any(|private| private.starts_with(&layout.workspace_root) || layout.workspace_root.starts_with(private))
    {
        return Err(sandbox_io_error(SandboxError::UnrepresentableSandboxLayout));
    }
    ensure_protected_roots_are_disjoint(&private_roots, &layout.protected_roots)
}

fn pinned_target_fd(command: &Command) -> io::Result<i32> {
    let path = Path::new(command.as_std().get_program());
    if path.parent().and_then(Path::file_name) != Some(OsStr::new("fd")) {
        return Err(sandbox_io_error(SandboxError::ExecutableNotPinned));
    }
    path.file_name()
        .and_then(|value| value.to_str())
        .and_then(|value| value.parse::<i32>().ok())
        .filter(|value| *value >= 3)
        .ok_or_else(|| sandbox_io_error(SandboxError::ExecutableNotPinned))
}

fn explicit_environment(
    command: &Command,
    private_home: &Path,
    private_tmp: &Path,
    proxy_port: Option<u16>,
    proxy_ca: Option<&Path>,
) -> io::Result<Vec<(OsString, OsString)>> {
    let mut environment = command
        .as_std()
        .get_envs()
        .filter_map(|(key, value)| value.map(|value| (key.to_os_string(), value.to_os_string())))
        .filter(|(key, _)| {
            !matches!(key.to_str(), Some("HOME" | "TMP" | "TMPDIR" | "TEMP"))
                && !key.as_bytes().starts_with(b"DYLD_")
                && !is_network_proxy_environment_key(key)
        })
        .collect::<Vec<_>>();
    environment.extend([
        (OsString::from("HOME"), private_home.as_os_str().to_os_string()),
        (OsString::from("TMP"), private_tmp.as_os_str().to_os_string()),
        (OsString::from("TMPDIR"), private_tmp.as_os_str().to_os_string()),
        (OsString::from("TEMP"), private_tmp.as_os_str().to_os_string()),
    ]);
    match (proxy_port, proxy_ca) {
        (Some(proxy_port), Some(proxy_ca)) => append_proxy_environment(&mut environment, proxy_port, proxy_ca)?,
        (None, None) => {}
        _ => return Err(sandbox_io_error(SandboxError::PreparationFailed)),
    }
    for (key, value) in &environment {
        if key.as_bytes().contains(&b'=') || key.as_bytes().contains(&0) || value.as_bytes().contains(&0) {
            return Err(sandbox_io_error(SandboxError::PreparationFailed));
        }
    }
    Ok(environment)
}

fn append_proxy_environment(
    environment: &mut Vec<(OsString, OsString)>,
    proxy_port: u16,
    proxy_ca: &Path,
) -> io::Result<()> {
    if proxy_port == 0 {
        return Err(sandbox_io_error(SandboxError::PreparationFailed));
    }
    let proxy = OsString::from(format!("http://127.0.0.1:{proxy_port}"));
    for key in [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ] {
        environment.push((OsString::from(key), proxy.clone()));
    }
    environment.push((OsString::from("NO_PROXY"), OsString::new()));
    environment.push((OsString::from("no_proxy"), OsString::new()));
    append_network_proxy_ca_environment(environment, proxy_ca);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn build_seatbelt_argv(
    profile: &str,
    workspace: &Path,
    status_fd: i32,
    denied_probe: &Path,
    denied_port: u16,
    target_fd: i32,
    helper_fd: i32,
    target_args: &[OsString],
    proxy: Option<(&Path, i32)>,
) -> Vec<OsString> {
    let mut argv = vec![
        OsString::from("sandbox-exec"),
        OsString::from("-p"),
        OsString::from(profile),
        OsString::from("--"),
        OsString::from(format!("/dev/fd/{helper_fd}")),
        OsString::from("--workspace"),
        workspace.as_os_str().to_os_string(),
        OsString::from("--status-fd"),
        OsString::from(status_fd.to_string()),
        OsString::from("--denied-probe"),
        denied_probe.as_os_str().to_os_string(),
        OsString::from("--denied-port"),
        OsString::from(denied_port.to_string()),
    ];
    if let Some((proxy_socket, proxy_listener_fd)) = proxy {
        argv.extend([
            OsString::from("--proxy-socket"),
            proxy_socket.as_os_str().to_os_string(),
            OsString::from("--proxy-listener-fd"),
            OsString::from(proxy_listener_fd.to_string()),
        ]);
    }
    argv.extend([OsString::from("--"), OsString::from(format!("/dev/fd/{target_fd}"))]);
    argv.extend(target_args.iter().cloned());
    argv
}

fn bind_private_proxy_listener() -> io::Result<TcpListener> {
    let listener =
        TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?;
    let address = listener
        .local_addr()
        .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?;
    if address.ip() != Ipv4Addr::LOCALHOST || address.port() == 0 {
        return Err(sandbox_io_error(SandboxError::PreparationFailed));
    }
    set_cloexec(listener.as_raw_fd())?;
    Ok(listener)
}

struct MacOsLaunchFiles {
    _seatbelt: File,
    _helper: File,
    _target: File,
    _status_writer: File,
    _proxy_listener: Option<TcpListener>,
}

struct MacOsWrapperFiles {
    seatbelt: File,
    helper: File,
    status_writer: File,
    proxy_listener: Option<TcpListener>,
}

fn replace_pinned_command(
    command: &mut Command,
    wrapper_files: MacOsWrapperFiles,
    descriptor_limit: i32,
    proxy_listener_fd: Option<i32>,
    build_wrapper: impl FnOnce(i32) -> Command,
) -> io::Result<()> {
    let target_file = duplicate_fd(pinned_target_fd(command)?)?;
    let target_fd = target_file.as_raw_fd();
    let mut wrapper = build_wrapper(target_fd);
    let launch_files = MacOsLaunchFiles {
        _seatbelt: wrapper_files.seatbelt,
        _helper: wrapper_files.helper,
        _target: target_file,
        _status_writer: wrapper_files.status_writer,
        _proxy_listener: wrapper_files.proxy_listener,
    };
    // SAFETY: `launch_files` owns every descriptor cleared below and is moved
    // into the hook, so each descriptor remains valid until this child reaches
    // exec. `descriptor_limit` was read before fork. The hook performs only
    // `fcntl` calls, raw-fd reads, integer iteration, and stack-only error
    // construction; it does not allocate, lock, or run user callbacks in the
    // post-fork child.
    unsafe {
        wrapper.pre_exec(move || {
            let _live_proxy_listener = &launch_files._proxy_listener;
            mark_inherited_descriptors_cloexec(descriptor_limit)?;
            clear_cloexec(launch_files._target.as_raw_fd())?;
            clear_cloexec(launch_files._seatbelt.as_raw_fd())?;
            clear_cloexec(launch_files._helper.as_raw_fd())?;
            clear_cloexec(launch_files._status_writer.as_raw_fd())?;
            if let Some(proxy_listener_fd) = proxy_listener_fd {
                clear_cloexec(proxy_listener_fd)?;
            }
            Ok(())
        });
    }
    *command = wrapper;
    Ok(())
}

fn duplicate_fd(descriptor: i32) -> io::Result<File> {
    let duplicate = unsafe { libc::fcntl(descriptor, libc::F_DUPFD_CLOEXEC, 3) };
    if duplicate == -1 {
        return Err(sandbox_io_error(SandboxError::PreparationFailed));
    }
    // SAFETY: F_DUPFD_CLOEXEC returned a fresh owned descriptor.
    Ok(unsafe { File::from_raw_fd(duplicate) })
}

fn clear_cloexec(descriptor: i32) -> io::Result<()> {
    if unsafe { libc::fcntl(descriptor, libc::F_SETFD, 0) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn start_channel() -> io::Result<(File, File)> {
    let mut descriptors = [-1_i32; 2];
    if unsafe { libc::pipe(descriptors.as_mut_ptr()) } == -1 {
        return Err(sandbox_io_error(SandboxError::PreparationFailed));
    }
    // SAFETY: pipe initialized both owned descriptors on success.
    let reader = unsafe { File::from_raw_fd(descriptors[0]) };
    // SAFETY: pipe initialized both owned descriptors on success.
    let writer = unsafe { File::from_raw_fd(descriptors[1]) };
    set_cloexec(reader.as_raw_fd())?;
    set_cloexec(writer.as_raw_fd())?;
    let flags = unsafe { libc::fcntl(reader.as_raw_fd(), libc::F_GETFL) };
    if flags == -1 || unsafe { libc::fcntl(reader.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1 {
        return Err(sandbox_io_error(SandboxError::PreparationFailed));
    }
    Ok((reader, writer))
}

fn set_cloexec(descriptor: i32) -> io::Result<()> {
    if unsafe { libc::fcntl(descriptor, libc::F_SETFD, libc::FD_CLOEXEC) } == -1 {
        return Err(sandbox_io_error(SandboxError::PreparationFailed));
    }
    Ok(())
}

fn descriptor_sanitize_limit() -> io::Result<i32> {
    let mut limit: libc::rlimit = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } == -1
        || limit.rlim_cur == libc::RLIM_INFINITY
        || limit.rlim_cur > MAX_SANITIZED_DESCRIPTORS
    {
        return Err(sandbox_io_error(SandboxError::PreparationFailed));
    }
    i32::try_from(limit.rlim_cur).map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))
}

fn mark_inherited_descriptors_cloexec(limit: i32) -> io::Result<()> {
    for descriptor in 3..limit {
        let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
        if flags == -1 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EBADF) {
                continue;
            }
            return Err(error);
        }
        if unsafe { libc::fcntl(descriptor, libc::F_SETFD, flags | libc::FD_CLOEXEC) } == -1 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "macos_test.rs"]
mod macos_test;
