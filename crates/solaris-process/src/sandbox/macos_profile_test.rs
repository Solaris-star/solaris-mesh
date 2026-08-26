use std::path::{Path, PathBuf};

use super::{MacOsProfilePaths, build_profile};

#[test]
fn strict_profile_denies_network_and_external_file_access() {
    let profile = profile(&[PathBuf::from("/work/repo/.solaris")]);

    assert!(profile.contains("(deny network*)"));
    assert!(profile.contains("(deny file-read*)"));
    assert!(profile.contains("(deny file-write*)"));
    assert!(!profile.contains("/outside/secret"));
}

#[test]
fn strict_profile_denies_process_group_escape_syscalls() {
    let profile = profile(&[]);

    assert!(profile.contains("(deny syscall-unix (syscall-number SYS_setsid))"));
    assert!(profile.contains("(deny syscall-unix (syscall-number SYS_setpgid))"));
    assert!(profile.contains("(deny syscall-unix (syscall-number SYS_posix_spawn))"));
}

#[test]
fn strict_profile_does_not_expose_process_arguments_through_sysctl() {
    let profile = profile(&[]);

    assert!(!profile.contains("(allow sysctl-read)"));
    assert!(!profile.contains("kern.proc"));
    assert!(!profile.contains("kern.procargs"));
    assert!(profile.contains("(sysctl-name \"hw.ncpu\")"));
}

#[test]
fn strict_profile_does_not_open_host_ipc_or_privileged_task_ports() {
    let profile = profile(&[]);

    for forbidden in [
        "mach-priv-task-port",
        "ipc-posix-shm",
        "ipc-posix-sem",
        "distributed-notification-post",
        "com.apple.SecurityServer",
        "com.apple.securityd.xpc",
        "com.apple.coreservices.launchservicesd",
    ] {
        assert!(
            !profile.contains(forbidden),
            "unexpected host IPC permission: {forbidden}"
        );
    }
}

#[test]
fn strict_profile_does_not_reopen_the_host_tty_or_device_tree() {
    let profile = profile(&[]);

    assert!(!profile.contains("(subpath \"/dev\")"));
    assert!(!profile.contains("/dev/tty"));
    assert!(profile.contains("(subpath \"/dev/fd\")"));
    assert!(profile.contains("(literal \"/dev/null\")"));
    assert!(profile.contains("(literal \"/dev/urandom\")"));
}

#[test]
fn strict_profile_reopens_only_workspace_and_private_roots() {
    let profile = profile(&[]);
    let read_deny = profile.find("(deny file-read*)").unwrap();
    let read_allow = profile.find("(allow file-read*").unwrap();
    let write_deny = profile.find("(deny file-write*)").unwrap();
    let write_allow = profile.find("(allow file-write*").unwrap();

    assert!(read_deny < read_allow);
    assert!(write_deny < write_allow);
    for path in ["/work/repo", "/private/home", "/private/tmp", "/private/state"] {
        assert!(profile.contains(&format!("(subpath \"{path}\")")));
    }
}

#[test]
fn protected_workspace_descendant_is_denied_after_workspace_allow() {
    let profile = profile(&[PathBuf::from("/work/repo/.solaris")]);
    let workspace_allow = profile.find("(subpath \"/work/repo\")").unwrap();
    let protected_deny = profile.rfind("(subpath \"/work/repo/.solaris\")").unwrap();

    assert!(workspace_allow < protected_deny);
    assert!(profile.contains("(deny file-write-unlink file-write-create"));
}

#[test]
fn protected_nested_root_prevents_renaming_its_workspace_ancestors() {
    let profile = profile(&[PathBuf::from("/work/repo/runtime/state/.solaris")]);

    for ancestor in ["/work/repo/runtime", "/work/repo/runtime/state"] {
        let rule = format!("(deny file-write-unlink file-write-create\n  (literal \"{ancestor}\")\n)");
        assert!(profile.contains(&rule), "missing ancestor rename guard: {ancestor}");
    }
    let workspace_rule = "(deny file-write-unlink file-write-create\n  (literal \"/work/repo\")\n)";
    assert!(!profile.contains(workspace_rule));
}

#[test]
fn sbpl_paths_escape_quotes_and_backslashes() {
    let paths = MacOsProfilePaths {
        workspace: Path::new("/work/a\\b\"c"),
        private_home: Path::new("/private/home"),
        private_tmp: Path::new("/private/tmp"),
        private_state: Path::new("/private/state"),
        protected_roots: &[],
        proxy_socket: None,
        proxy_port: None,
        proxy_ca: None,
    };

    let profile = build_profile(&paths).unwrap();

    assert!(profile.contains(r#"(subpath "/work/a\\b\"c")"#));
}

#[test]
fn sbpl_paths_reject_control_characters() {
    let paths = MacOsProfilePaths {
        workspace: Path::new("/work/repo\n(allow network*)"),
        private_home: Path::new("/private/home"),
        private_tmp: Path::new("/private/tmp"),
        private_state: Path::new("/private/state"),
        protected_roots: &[],
        proxy_socket: None,
        proxy_port: None,
        proxy_ca: None,
    };

    assert!(build_profile(&paths).is_err());
}

#[test]
fn proxy_profile_allows_only_the_private_frontend_and_host_socket() {
    let socket = Path::new("/private/state/network-proxy.sock");
    let profile = build_profile(&MacOsProfilePaths {
        workspace: Path::new("/work/repo"),
        private_home: Path::new("/private/home"),
        private_tmp: Path::new("/private/tmp"),
        private_state: Path::new("/private/state"),
        protected_roots: &[],
        proxy_socket: Some(socket),
        proxy_port: Some(29347),
        proxy_ca: Some(Path::new("/private/home/.solaris-network-proxy-ca.pem")),
    })
    .unwrap();

    assert!(profile.contains("(remote tcp \"127.0.0.1:29347\")"));
    assert!(profile.contains("(local ip \"127.0.0.1:29347\")"));
    assert!(profile.contains("(socket-domain AF_INET) (socket-type SOCK_STREAM)"));
    assert!(profile.contains("(socket-domain AF_UNIX) (socket-type SOCK_STREAM)"));
    assert!(profile.contains("/private/state/network-proxy.sock"));
    assert!(!profile.contains("(allow network*)"));
    assert!(!profile.contains("localhost"));
    assert!(!profile.contains("AF_INET6"));
    assert!(!profile.contains("SOCK_DGRAM"));
    assert!(!profile.contains("(allow network-bind"));
    let state_write_allow = profile.rfind("(subpath \"/private/state\")").unwrap();
    let ca_write_deny = profile
        .rfind("(literal \"/private/home/.solaris-network-proxy-ca.pem\")")
        .unwrap();
    assert!(state_write_allow < ca_write_deny);
    assert!(profile.contains("(deny file-write-unlink file-write-create"));
}

fn profile(protected_roots: &[PathBuf]) -> String {
    build_profile(&MacOsProfilePaths {
        workspace: Path::new("/work/repo"),
        private_home: Path::new("/private/home"),
        private_tmp: Path::new("/private/tmp"),
        private_state: Path::new("/private/state"),
        protected_roots,
        proxy_socket: None,
        proxy_port: None,
        proxy_ca: None,
    })
    .unwrap()
}
