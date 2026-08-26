use std::collections::HashSet;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use tokio::process::{Child, Command};

use super::identity::ProtectedIdentitySnapshot;
use super::layout::{ResolvedSandboxLayout, ensure_protected_roots_are_disjoint, resolve_path};
use super::{
    SandboxBackend, SandboxCommandDisposition, SandboxEnforcement, SandboxError, SandboxReason, SandboxReport,
    SandboxRunner, insufficient_enforcement_error, sandbox_io_error, trusted_sandbox_helper,
};
use crate::NetworkProxyPolicy;
use crate::environment::{append_network_proxy_ca_environment, is_network_proxy_environment_key};
use crate::network_proxy::HostNetworkProxy;
use crate::runner::{PinnedExecutable, inspect_executable, pin_executable};
use crate::sandbox_report::linux_sandbox_capability_report;

const BWRAP_CANDIDATES: &[&str] = &["/usr/bin/bwrap", "/bin/bwrap"];
const PROBE_TARGET_ARGUMENT: &str = "--solaris-sandbox-probe-target";
const LANDLOCK_CREATE_RULESET_VERSION_FLAG: libc::c_uint = 1;
const START_CONFIRMATION_TIMEOUT: Duration = Duration::from_secs(5);
const PROXY_PORT: u16 = 29_347;
const PROXY_CA_PATH: &str = "/__solaris/state/network-proxy-ca.pem";
const INTERNAL_DESTINATIONS: &[&str] = &[
    "/dev",
    "/proc",
    "/tmp",
    "/run",
    "/home/solaris",
    "/__solaris/state",
    "/__solaris/target",
    "/__solaris/runner",
];
const SYSTEM_DIRECTORIES: &[&str] = &["/bin", "/lib", "/lib64", "/sbin", "/usr", "/nix/store"];
const SYSTEM_FILES: &[&str] = &[
    "/etc/ca-certificates.conf",
    "/etc/hosts",
    "/etc/ld.so.cache",
    "/etc/localtime",
    "/etc/nsswitch.conf",
    "/etc/passwd",
    "/etc/group",
    "/etc/resolv.conf",
    "/etc/ssl/certs/ca-certificates.crt",
];

fn prerequisite_report() -> SandboxReport {
    linux_sandbox_capability_report(
        locate_bwrap().is_some(),
        trusted_sandbox_helper(None, None).is_ok(),
        landlock_v3_available(),
    )
}

pub(super) fn probe_capability() -> SandboxReport {
    let prerequisite = prerequisite_report();
    if prerequisite.enforcement() != SandboxEnforcement::Full {
        return prerequisite;
    }
    match std::thread::Builder::new()
        .name("solaris-sandbox-probe".to_owned())
        .spawn(run_capability_probe)
    {
        Ok(probe) => probe
            .join()
            .unwrap_or_else(|_| failed_linux_report(SandboxReason::StartConfirmationFailed)),
        Err(_) => failed_linux_report(SandboxReason::SetupFailed),
    }
}

fn run_capability_probe() -> SandboxReport {
    let result = (|| -> io::Result<SandboxReport> {
        let workspace = private_directory("solaris-sandbox-probe-workspace-")?;
        let helper = trusted_sandbox_helper(None, None)?;
        let mut command = helper
            .command()
            .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?;
        command
            .arg(PROBE_TARGET_ARGUMENT)
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
                Ok(_) => Ok(failed_linux_report(SandboxReason::StartConfirmationFailed)),
                Err(_) => {
                    let _ = child.kill().await;
                    Ok(failed_linux_report(SandboxReason::StartConfirmationFailed))
                }
            }
        })
    })();
    match result {
        Ok(report) => report,
        Err(error) => error
            .get_ref()
            .and_then(|source| source.downcast_ref::<SandboxError>())
            .and_then(|sandbox| sandbox.report().copied())
            .unwrap_or_else(|| failed_linux_report(SandboxReason::SetupFailed)),
    }
}

pub(super) struct LinuxSandbox {
    layout: ResolvedSandboxLayout,
    protected_identities: ProtectedIdentitySnapshot,
    bwrap: PinnedExecutable,
    helper: PinnedExecutable,
    network_proxy: Option<HostNetworkProxy>,
    private_home: tempfile::TempDir,
    private_tmp: tempfile::TempDir,
    private_state: tempfile::TempDir,
    _mask_sources: MaskSources,
    protected_masks: Vec<ProtectedMask>,
    start_marker: PathBuf,
    proxy_enabled: bool,
    report: SandboxReport,
    configured: bool,
    sync_read: Option<File>,
    #[cfg(feature = "sandbox-test-fixtures")]
    test_drain_error_gate: Option<PathBuf>,
}

struct LinuxLaunchFiles {
    _bwrap: File,
    _target: File,
    _helper: File,
    _workspace: File,
    _sync_write: File,
}

impl LinuxSandbox {
    pub(super) fn prepare(layout: ResolvedSandboxLayout, network_policy: NetworkProxyPolicy) -> io::Result<Self> {
        ensure_internal_destinations_are_available(&layout)?;
        let bwrap_path = locate_bwrap()
            .ok_or_else(|| insufficient_enforcement_error(unavailable_report(SandboxReason::BubblewrapUnavailable)))?;
        #[cfg(feature = "sandbox-test-fixtures")]
        let fixture_helper = layout._test_helper_path.as_deref();
        #[cfg(not(feature = "sandbox-test-fixtures"))]
        let fixture_helper = None;
        if !landlock_v3_available() {
            return Err(insufficient_enforcement_error(unavailable_report(
                SandboxReason::LandlockUnavailable,
            )));
        }
        let bwrap = pin_support_executable(&bwrap_path, SandboxReason::BubblewrapUnavailable)?;
        let helper = trusted_sandbox_helper(Some(&layout.workspace_root), fixture_helper)?;
        let private_home = private_directory("solaris-sandbox-home-")?;
        let private_tmp = private_directory("solaris-sandbox-tmp-")?;
        let private_state = private_directory("solaris-sandbox-state-")?;
        let mask_sources = mask_sources()?;
        let protected_masks = protected_masks(&layout.protected_roots, &mask_sources.directory, &mask_sources.file)?;
        ensure_private_sources_are_disjoint(
            &layout,
            [
                private_home.path(),
                private_tmp.path(),
                private_state.path(),
                mask_sources._root.path(),
            ],
        )?;
        let allowed_system_roots = system_mounts();
        ensure_protected_roots_are_disjoint(&allowed_system_roots, &layout.protected_roots)?;
        let protected_identities =
            ProtectedIdentitySnapshot::capture(&layout.protected_roots, &layout.protected_object_identities)?;
        let start_marker = private_state.path().join("ready");
        let proxy_enabled = !network_policy.is_empty();
        let network_proxy = proxy_enabled
            .then(|| HostNetworkProxy::start(&private_state.path().join("network-proxy.sock"), network_policy))
            .transpose()
            .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?;
        Ok(Self {
            layout,
            protected_identities,
            bwrap,
            helper,
            private_home,
            private_tmp,
            private_state,
            _mask_sources: mask_sources,
            protected_masks,
            start_marker,
            network_proxy,
            proxy_enabled,
            report: full_report(),
            configured: false,
            sync_read: None,
            #[cfg(feature = "sandbox-test-fixtures")]
            test_drain_error_gate: None,
        })
    }

    fn configure_inner(&mut self, command: &mut Command) -> io::Result<()> {
        if self.configured {
            return Err(sandbox_io_error(SandboxError::PreparationFailed));
        }
        #[cfg(feature = "sandbox-test-fixtures")]
        {
            self.test_drain_error_gate = command
                .as_std()
                .get_envs()
                .find(|(key, _)| *key == OsStr::new("SOLARIS_GUARDIAN_FIXTURE_DRAIN_ERROR_GATE"))
                .and_then(|(_, value)| value)
                .map(PathBuf::from);
        }
        let target_file = duplicate_fd(pinned_target_fd(command)?)?;
        let bwrap_file = self
            .bwrap
            .duplicate_linux_snapshot()
            .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?;
        let helper_file = self
            .helper
            .duplicate_linux_snapshot()
            .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?;
        let workspace_directory = self
            .layout
            .workspace_capability
            .duplicate_linux_directory()
            .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?;
        let (sync_read, sync_write) = cloexec_pipe()?;
        let bwrap_fd = bwrap_file.as_raw_fd();
        let target_fd = target_file.as_raw_fd();
        let helper_fd = helper_file.as_raw_fd();
        let workspace_fd = workspace_directory.as_raw_fd();
        let sync_fd = sync_write.as_raw_fd();
        rewind_data_fd(target_fd)?;
        rewind_data_fd(helper_fd)?;
        let current_dir = command
            .as_std()
            .get_current_dir()
            .map(|path| resolve_workspace_current_dir(&self.layout, path))
            .transpose()?
            .unwrap_or_else(|| self.layout.workspace_root.clone());
        if !current_dir.starts_with(&self.layout.workspace_root) {
            return Err(sandbox_io_error(SandboxError::UnrepresentableSandboxLayout));
        }
        let target_args = command.as_std().get_args().map(OsStr::to_os_string).collect::<Vec<_>>();
        if let Some(proxy) = &self.network_proxy {
            proxy
                .verify_ca_certificate()
                .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?;
        }
        let environment = explicit_environment(command, self.proxy_enabled)?;
        let argv = build_bwrap_argv(
            &self.layout,
            &current_dir,
            self.private_home.path(),
            self.private_tmp.path(),
            self.private_state.path(),
            &self.protected_masks,
            workspace_fd,
            target_fd,
            helper_fd,
            sync_fd,
            &target_args,
            self.proxy_enabled,
        )?;
        let wrapper_path = PathBuf::from(format!("/proc/self/fd/{bwrap_fd}"));
        let mut wrapper = Command::new(&wrapper_path);
        wrapper.args(argv.iter().skip(1)).env_clear().envs(environment);
        let launch_files = LinuxLaunchFiles {
            _bwrap: bwrap_file,
            _target: target_file,
            _helper: helper_file,
            _workspace: workspace_directory,
            _sync_write: sync_write,
        };
        unsafe {
            wrapper.pre_exec(move || {
                for descriptor in [
                    launch_files._bwrap.as_raw_fd(),
                    launch_files._workspace.as_raw_fd(),
                    launch_files._target.as_raw_fd(),
                    launch_files._helper.as_raw_fd(),
                    launch_files._sync_write.as_raw_fd(),
                ] {
                    clear_cloexec(descriptor)?;
                }
                Ok(())
            });
        }
        *command = wrapper;
        self.sync_read = Some(sync_read);
        self.configured = true;
        Ok(())
    }

    fn verify_identities(&mut self) -> io::Result<()> {
        let workspace_directory = self
            .layout
            .workspace_capability
            .duplicate_linux_directory()
            .map_err(|_| sandbox_io_error(SandboxError::IdentityInspectionFailed))?;
        self.protected_identities.verify_linux_workspace(
            &self.layout.protected_roots,
            &self.layout.workspace_root,
            workspace_directory,
        )
    }

    fn wait_for_start(&mut self, child: &mut Child) -> io::Result<()> {
        let deadline = Instant::now() + START_CONFIRMATION_TIMEOUT;
        loop {
            match std::fs::read(&self.start_marker) {
                Ok(status) if status == b"full\n" => return Ok(()),
                Ok(status) if b"full\n".starts_with(&status) => {}
                Ok(_) => return self.fail_start(SandboxReason::StartConfirmationFailed),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
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
        self.report = failed_linux_report(reason);
        Err(insufficient_enforcement_error(self.report))
    }
}

impl SandboxRunner for LinuxSandbox {
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

    fn process_tree_is_drained(&mut self) -> io::Result<bool> {
        #[cfg(feature = "sandbox-test-fixtures")]
        if self.test_drain_error_gate.as_ref().is_some_and(|gate| !gate.is_file()) {
            return Err(io::Error::other("injected sandbox drain proof failure"));
        }
        let descriptor = self
            .sync_read
            .as_ref()
            .ok_or_else(|| sandbox_io_error(SandboxError::PreparationFailed))?
            .as_raw_fd();
        sync_pipe_is_closed(descriptor)
    }
}

fn full_report() -> SandboxReport {
    SandboxReport::new(
        SandboxEnforcement::Full,
        SandboxBackend::LinuxBubblewrapLandlock,
        SandboxReason::Enforced,
    )
}

fn unavailable_report(reason: SandboxReason) -> SandboxReport {
    SandboxReport::new(SandboxEnforcement::Unavailable, SandboxBackend::None, reason)
}

fn failed_linux_report(reason: SandboxReason) -> SandboxReport {
    SandboxReport::new(
        SandboxEnforcement::Unavailable,
        SandboxBackend::LinuxBubblewrapLandlock,
        reason,
    )
}

fn locate_bwrap() -> Option<PathBuf> {
    BWRAP_CANDIDATES
        .iter()
        .map(PathBuf::from)
        .find(|candidate| candidate.is_file())
}

fn landlock_v3_available() -> bool {
    let abi = unsafe {
        libc::syscall(
            libc::SYS_landlock_create_ruleset,
            std::ptr::null::<libc::c_void>(),
            0,
            LANDLOCK_CREATE_RULESET_VERSION_FLAG,
        )
    };
    abi >= 3
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

fn ensure_internal_destinations_are_available(layout: &ResolvedSandboxLayout) -> io::Result<()> {
    let destinations = INTERNAL_DESTINATIONS.iter().map(Path::new).collect::<Vec<_>>();
    if destinations
        .iter()
        .any(|destination| destination.starts_with(&layout.workspace_root))
        || layout.protected_roots.iter().any(|protected| {
            destinations
                .iter()
                .any(|destination| destination.starts_with(protected))
        })
    {
        return Err(sandbox_io_error(SandboxError::UnrepresentableSandboxLayout));
    }
    Ok(())
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
    let program = command.as_std().get_program();
    let path = Path::new(program);
    let parent = path.parent().and_then(Path::file_name);
    let descriptor = path.file_name().and_then(|value| value.to_str());
    if parent != Some(OsStr::new("fd")) {
        return Err(sandbox_io_error(SandboxError::ExecutableNotPinned));
    }
    descriptor
        .and_then(|value| value.parse::<i32>().ok())
        .filter(|value| *value >= 3)
        .ok_or_else(|| sandbox_io_error(SandboxError::ExecutableNotPinned))
}

fn explicit_environment(command: &Command, proxy_enabled: bool) -> io::Result<Vec<(OsString, OsString)>> {
    let mut environment = command
        .as_std()
        .get_envs()
        .filter_map(|(key, value)| value.map(|value| (key.to_os_string(), value.to_os_string())))
        .filter(|(key, _)| {
            !matches!(key.to_str(), Some("HOME" | "TMP" | "TMPDIR" | "TEMP"))
                && !unsafe_wrapper_environment_key(key)
                && !is_network_proxy_environment_key(key)
        })
        .collect::<Vec<_>>();
    environment.extend([
        (OsString::from("HOME"), OsString::from("/home/solaris")),
        (OsString::from("TMP"), OsString::from("/tmp")),
        (OsString::from("TMPDIR"), OsString::from("/tmp")),
        (OsString::from("TEMP"), OsString::from("/tmp")),
    ]);
    if proxy_enabled {
        append_proxy_environment(&mut environment);
    }
    for (key, value) in &environment {
        if key.as_bytes().contains(&b'=') || key.as_bytes().contains(&0) || value.as_bytes().contains(&0) {
            return Err(sandbox_io_error(SandboxError::PreparationFailed));
        }
    }
    Ok(environment)
}

fn append_proxy_environment(environment: &mut Vec<(OsString, OsString)>) {
    let proxy = OsString::from(format!("http://127.0.0.1:{PROXY_PORT}"));
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
    append_network_proxy_ca_environment(environment, Path::new(PROXY_CA_PATH));
}

fn unsafe_wrapper_environment_key(key: &OsStr) -> bool {
    let key = key.as_bytes();
    key.starts_with(b"LD_")
        || key.starts_with(b"DYLD_")
        || key.starts_with(b"BWRAP_")
        || matches!(key, b"GLIBC_TUNABLES" | b"GCONV_PATH" | b"LOCPATH" | b"NLSPATH")
}

#[allow(clippy::too_many_arguments)]
fn build_bwrap_argv(
    layout: &ResolvedSandboxLayout,
    current_dir: &Path,
    private_home: &Path,
    private_tmp: &Path,
    private_state: &Path,
    protected_masks: &[ProtectedMask],
    workspace_fd: i32,
    target_fd: i32,
    helper_fd: i32,
    sync_fd: i32,
    target_args: &[OsString],
    proxy_enabled: bool,
) -> io::Result<Vec<OsString>> {
    let mut argv = vec![OsString::from("bwrap")];
    push_flags(
        &mut argv,
        &[
            "--unshare-user",
            "--unshare-pid",
            "--unshare-net",
            "--unshare-ipc",
            "--unshare-uts",
            "--disable-userns",
            "--assert-userns-disabled",
            "--cap-drop",
            "ALL",
            "--new-session",
            "--die-with-parent",
            "--hostname",
            "solaris",
        ],
    );
    argv.extend([OsString::from("--sync-fd"), OsString::from(sync_fd.to_string())]);
    let mut created = HashSet::new();
    for path in ["/dev", "/proc", "/tmp", "/run", "/home", "/home/solaris", "/__solaris"] {
        push_dir(&mut argv, Path::new(path), &mut created);
    }
    push_single_path(&mut argv, "--dev", Path::new("/dev"));
    push_single_path(&mut argv, "--proc", Path::new("/proc"));
    push_single_path(&mut argv, "--tmpfs", Path::new("/tmp"));
    push_single_path(&mut argv, "--tmpfs", Path::new("/run"));
    for path in SYSTEM_DIRECTORIES.iter().map(Path::new).filter(|path| path.is_dir()) {
        push_parent_dirs(&mut argv, path, &mut created);
        push_pair(&mut argv, "--ro-bind", path, path);
    }
    for path in SYSTEM_FILES.iter().map(Path::new).filter(|path| path.is_file()) {
        push_parent_dirs(&mut argv, path, &mut created);
        push_pair(&mut argv, "--ro-bind", path, path);
    }
    push_pair(&mut argv, "--bind", private_home, Path::new("/home/solaris"));
    push_pair(&mut argv, "--bind", private_tmp, Path::new("/tmp"));
    push_pair(&mut argv, "--bind", private_state, Path::new("/__solaris/state"));
    if proxy_enabled {
        push_pair(
            &mut argv,
            "--ro-bind",
            &private_state.join("network-proxy-ca.pem"),
            Path::new(PROXY_CA_PATH),
        );
    }
    push_parent_dirs(&mut argv, &layout.workspace_root, &mut created);
    argv.extend([
        OsString::from("--bind-fd"),
        OsString::from(workspace_fd.to_string()),
        layout.workspace_root.as_os_str().to_os_string(),
    ]);
    created.insert(layout.workspace_root.clone());
    for mask in protected_masks {
        if !mask.destination.starts_with(&layout.workspace_root) {
            push_parent_dirs(&mut argv, &mask.destination, &mut created);
        }
        push_pair(&mut argv, "--ro-bind", &mask.source, &mask.destination);
    }
    push_fd_file(&mut argv, target_fd, "/__solaris/target");
    push_fd_file(&mut argv, helper_fd, "/__solaris/runner");
    push_single_path(&mut argv, "--chdir", current_dir);
    argv.push(OsString::from("--"));
    argv.extend([
        OsString::from("/__solaris/runner"),
        OsString::from("--workspace"),
        layout.workspace_root.as_os_str().to_os_string(),
        OsString::from("--status"),
        OsString::from("/__solaris/state/ready"),
    ]);
    if proxy_enabled {
        argv.extend([
            OsString::from("--proxy-socket"),
            OsString::from("/__solaris/state/network-proxy.sock"),
            OsString::from("--proxy-port"),
            OsString::from(PROXY_PORT.to_string()),
        ]);
    }
    argv.extend([OsString::from("--"), OsString::from("/__solaris/target")]);
    argv.extend(target_args.iter().cloned());
    Ok(argv)
}

fn resolve_workspace_current_dir(layout: &ResolvedSandboxLayout, requested: &Path) -> io::Result<PathBuf> {
    let absolute = if requested.is_absolute() {
        requested.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?
            .join(requested)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            std::path::Component::RootDir => normalized.push(component.as_os_str()),
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                if !normalized.pop() {
                    return Err(sandbox_io_error(SandboxError::UnrepresentableSandboxLayout));
                }
            }
            std::path::Component::Normal(value) => normalized.push(value),
            std::path::Component::Prefix(_) => {
                return Err(sandbox_io_error(SandboxError::UnrepresentableSandboxLayout));
            }
        }
    }
    if normalized.starts_with(&layout.workspace_root) {
        Ok(normalized)
    } else {
        Err(sandbox_io_error(SandboxError::UnrepresentableSandboxLayout))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ProtectedMask {
    source: PathBuf,
    destination: PathBuf,
}

struct MaskSources {
    _root: tempfile::TempDir,
    directory: PathBuf,
    file: PathBuf,
}

fn mask_sources() -> io::Result<MaskSources> {
    let root = private_directory("solaris-sandbox-empty-")?;
    let directory = root.path().join("directory");
    let file = root.path().join("file");
    std::fs::create_dir(&directory).map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?;
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&file)
        .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?;
    Ok(MaskSources {
        _root: root,
        directory,
        file,
    })
}

fn protected_masks(
    protected_roots: &[PathBuf],
    empty_directory: &Path,
    empty_file: &Path,
) -> io::Result<Vec<ProtectedMask>> {
    protected_roots
        .iter()
        .map(|destination| {
            let metadata =
                std::fs::metadata(destination).map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?;
            Ok(ProtectedMask {
                source: if metadata.is_dir() {
                    empty_directory.to_path_buf()
                } else {
                    empty_file.to_path_buf()
                },
                destination: destination.clone(),
            })
        })
        .collect()
}

fn system_mounts() -> Vec<PathBuf> {
    SYSTEM_DIRECTORIES
        .iter()
        .chain(SYSTEM_FILES.iter())
        .map(PathBuf::from)
        .filter(|path| path.exists())
        .collect()
}

fn push_flags(argv: &mut Vec<OsString>, flags: &[&str]) {
    argv.extend(flags.iter().map(OsString::from));
}

fn push_fd_file(argv: &mut Vec<OsString>, descriptor: i32, destination: &str) {
    argv.extend([
        OsString::from("--ro-bind-data"),
        OsString::from(descriptor.to_string()),
        OsString::from(destination),
        OsString::from("--chmod"),
        OsString::from("0555"),
        OsString::from(destination),
    ]);
}

fn push_single_path(argv: &mut Vec<OsString>, flag: &str, path: &Path) {
    argv.extend([OsString::from(flag), path.as_os_str().to_os_string()]);
}

fn push_pair(argv: &mut Vec<OsString>, flag: &str, source: &Path, destination: &Path) {
    argv.extend([
        OsString::from(flag),
        source.as_os_str().to_os_string(),
        destination.as_os_str().to_os_string(),
    ]);
}

fn push_parent_dirs(argv: &mut Vec<OsString>, path: &Path, created: &mut HashSet<PathBuf>) {
    let mut parents = path
        .ancestors()
        .skip(1)
        .filter(|path| *path != Path::new("/"))
        .collect::<Vec<_>>();
    parents.reverse();
    for parent in parents {
        push_dir(argv, parent, created);
    }
}

fn push_dir(argv: &mut Vec<OsString>, path: &Path, created: &mut HashSet<PathBuf>) {
    if created.insert(path.to_path_buf()) {
        push_single_path(argv, "--dir", path);
    }
}

fn clear_cloexec(descriptor: i32) -> io::Result<()> {
    if unsafe { libc::fcntl(descriptor, libc::F_SETFD, 0) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn duplicate_fd(descriptor: i32) -> io::Result<File> {
    let duplicate = unsafe { libc::fcntl(descriptor, libc::F_DUPFD_CLOEXEC, 3) };
    if duplicate == -1 {
        return Err(sandbox_io_error(SandboxError::PreparationFailed));
    }
    Ok(unsafe { File::from_raw_fd(duplicate) })
}

fn rewind_data_fd(descriptor: i32) -> io::Result<()> {
    if unsafe { libc::lseek(descriptor, 0, libc::SEEK_SET) } == -1 {
        return Err(sandbox_io_error(SandboxError::PreparationFailed));
    }
    Ok(())
}

fn cloexec_pipe() -> io::Result<(File, File)> {
    let mut descriptors = [-1; 2];
    if unsafe { libc::pipe2(descriptors.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) } == -1 {
        return Err(sandbox_io_error(SandboxError::PreparationFailed));
    }
    Ok(unsafe { (File::from_raw_fd(descriptors[0]), File::from_raw_fd(descriptors[1])) })
}

fn sync_pipe_is_closed(descriptor: i32) -> io::Result<bool> {
    let mut byte = 0_u8;
    loop {
        let read = unsafe { libc::read(descriptor, std::ptr::addr_of_mut!(byte).cast(), 1) };
        if read == 0 {
            return Ok(true);
        }
        if read == -1 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            if error.kind() == io::ErrorKind::WouldBlock {
                return Ok(false);
            }
            return Err(error);
        }
        return Err(sandbox_io_error(SandboxError::PreparationFailed));
    }
}

#[cfg(test)]
#[path = "linux_test.rs"]
mod linux_test;
