use std::io;
use std::path::{Path, PathBuf};

const MAX_PROFILE_BYTES: usize = 64 * 1024;
const SYSTEM_READ_ROOTS: &[&str] = &[
    "/System",
    "/usr",
    "/bin",
    "/sbin",
    "/dev/fd",
    "/Library/Apple",
    "/private/var/db/dyld",
    "/private/var/db/timezone",
    "/private/var/select",
];
const SYSTEM_READ_FILES: &[&str] = &[
    "/dev/null",
    "/dev/random",
    "/dev/urandom",
    "/dev/zero",
    "/private/etc/group",
    "/private/etc/hosts",
    "/private/etc/localtime",
    "/private/etc/nsswitch.conf",
    "/private/etc/passwd",
    "/private/etc/resolv.conf",
    "/private/etc/ssl/cert.pem",
];

const BASE_PROFILE: &str = r#"(version 1)
(deny default)
(allow process-exec)
(allow process-fork)
(allow process-info* (target same-sandbox))
(allow signal (target same-sandbox))
(deny syscall-unix (syscall-number SYS_setsid))
(deny syscall-unix (syscall-number SYS_setpgid))
(deny syscall-unix (syscall-number SYS_posix_spawn))
(allow mach-lookup
  (global-name "com.apple.system.opendirectoryd.libinfo")
  (global-name "com.apple.system.opendirectoryd.membership"))
(allow sysctl-read
  (sysctl-name "hw.activecpu")
  (sysctl-name "hw.byteorder")
  (sysctl-name "hw.cacheconfig")
  (sysctl-name "hw.cachelinesize_compat")
  (sysctl-name "hw.cpufamily")
  (sysctl-name "hw.cpufrequency")
  (sysctl-name "hw.cputype")
  (sysctl-name "hw.logicalcpu")
  (sysctl-name "hw.logicalcpu_max")
  (sysctl-name "hw.machine")
  (sysctl-name "hw.memsize")
  (sysctl-name "hw.ncpu")
  (sysctl-name "hw.pagesize")
  (sysctl-name "hw.physicalcpu")
  (sysctl-name "hw.physicalcpu_max")
  (sysctl-name "kern.argmax")
  (sysctl-name "kern.hostname")
  (sysctl-name "kern.osproductversion")
  (sysctl-name "kern.osrelease")
  (sysctl-name "kern.ostype")
  (sysctl-name "kern.osversion")
  (sysctl-name "kern.version")
  (sysctl-name "machdep.cpu.brand_string")
  (sysctl-name-prefix "hw.optional.")
  (sysctl-name-prefix "machdep.cpu."))
(allow file-ioctl
  (literal "/dev/null")
  (literal "/dev/zero")
  (literal "/dev/random")
  (literal "/dev/urandom"))
(deny file-read*)
"#;

pub(super) struct MacOsProfilePaths<'a> {
    pub(super) workspace: &'a Path,
    pub(super) private_home: &'a Path,
    pub(super) private_tmp: &'a Path,
    pub(super) private_state: &'a Path,
    pub(super) protected_roots: &'a [PathBuf],
    pub(super) proxy_socket: Option<&'a Path>,
    pub(super) proxy_port: Option<u16>,
    pub(super) proxy_ca: Option<&'a Path>,
}

pub(super) fn build_profile(paths: &MacOsProfilePaths<'_>) -> io::Result<String> {
    let mut profile = String::from(BASE_PROFILE);
    match (paths.proxy_socket, paths.proxy_port, paths.proxy_ca) {
        (Some(socket), Some(port), Some(_)) => push_network_proxy_rules(&mut profile, socket, port)?,
        (None, None, None) => profile.push_str("(deny network*)\n"),
        _ => return Err(invalid_profile()),
    }
    profile.push_str("(allow file-read*\n");
    push_literal_filter(&mut profile, Path::new("/"))?;
    for root in SYSTEM_READ_ROOTS {
        push_path_filters(&mut profile, Path::new(root))?;
    }
    for file in SYSTEM_READ_FILES {
        push_literal_filter(&mut profile, Path::new(file))?;
    }
    for root in [
        paths.workspace,
        paths.private_home,
        paths.private_tmp,
        paths.private_state,
    ] {
        push_path_filters(&mut profile, root)?;
    }
    profile.push_str(")\n(deny file-write*)\n(allow file-write*\n");
    push_literal_filter(&mut profile, Path::new("/dev/null"))?;
    for root in [
        paths.workspace,
        paths.private_home,
        paths.private_tmp,
        paths.private_state,
    ] {
        push_path_filters(&mut profile, root)?;
    }
    profile.push_str(")\n");
    if let Some(proxy_ca) = paths.proxy_ca {
        push_deny_rule(&mut profile, "file-write*", proxy_ca)?;
        push_deny_rule(&mut profile, "file-write-unlink file-write-create", proxy_ca)?;
    }
    for protected in paths.protected_roots {
        push_deny_rule(&mut profile, "file-read*", protected)?;
        push_deny_rule(&mut profile, "file-write*", protected)?;
        push_deny_rule(&mut profile, "file-write-unlink file-write-create", protected)?;
        push_workspace_ancestor_guards(&mut profile, paths.workspace, protected)?;
    }
    if profile.len() > MAX_PROFILE_BYTES {
        return Err(invalid_profile());
    }
    Ok(profile)
}

fn push_network_proxy_rules(profile: &mut String, socket: &Path, port: u16) -> io::Result<()> {
    if port == 0 {
        return Err(invalid_profile());
    }
    profile.push_str("(allow system-socket (socket-domain AF_INET) (socket-type SOCK_STREAM))\n");
    profile.push_str("(allow system-socket (socket-domain AF_UNIX) (socket-type SOCK_STREAM))\n");
    profile.push_str("(allow network-inbound (local ip \"127.0.0.1:");
    profile.push_str(&port.to_string());
    profile.push_str("\"))\n");
    profile.push_str("(allow network-outbound (remote tcp \"127.0.0.1:");
    profile.push_str(&port.to_string());
    profile.push_str("\"))\n");
    profile.push_str("(allow network-outbound (remote unix-socket (literal ");
    profile.push_str(&sbpl_string(socket)?);
    profile.push_str(")))\n");
    Ok(())
}

fn push_workspace_ancestor_guards(profile: &mut String, workspace: &Path, protected: &Path) -> io::Result<()> {
    let mut ancestor = protected.parent();
    while let Some(path) = ancestor {
        if path == workspace || !path.starts_with(workspace) {
            break;
        }
        profile.push_str("(deny file-write-unlink file-write-create\n");
        push_literal_filter(profile, path)?;
        profile.push_str(")\n");
        ancestor = path.parent();
    }
    Ok(())
}

fn push_deny_rule(profile: &mut String, operations: &str, path: &Path) -> io::Result<()> {
    profile.push_str("(deny ");
    profile.push_str(operations);
    profile.push('\n');
    push_path_filters(profile, path)?;
    profile.push_str(")\n");
    Ok(())
}

fn push_path_filters(profile: &mut String, path: &Path) -> io::Result<()> {
    push_literal_filter(profile, path)?;
    profile.push_str("  (subpath ");
    profile.push_str(&sbpl_string(path)?);
    profile.push_str(")\n");
    Ok(())
}

fn push_literal_filter(profile: &mut String, path: &Path) -> io::Result<()> {
    profile.push_str("  (literal ");
    profile.push_str(&sbpl_string(path)?);
    profile.push_str(")\n");
    Ok(())
}

fn sbpl_string(path: &Path) -> io::Result<String> {
    let value = path.to_str().ok_or_else(invalid_profile)?;
    if value.chars().any(char::is_control) {
        return Err(invalid_profile());
    }
    let mut escaped = String::with_capacity(value.len() + 2);
    escaped.push('"');
    for character in value.chars() {
        if matches!(character, '\\' | '"') {
            escaped.push('\\');
        }
        escaped.push(character);
    }
    escaped.push('"');
    Ok(escaped)
}

fn invalid_profile() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, "invalid macOS sandbox profile")
}

#[cfg(test)]
#[path = "macos_profile_test.rs"]
mod macos_profile_test;
