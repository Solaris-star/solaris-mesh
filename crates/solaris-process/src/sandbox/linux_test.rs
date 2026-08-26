use std::ffi::OsString;
use std::path::{Path, PathBuf};

use super::{
    build_bwrap_argv, cloexec_pipe, ensure_internal_destinations_are_available, ensure_private_sources_are_disjoint,
    explicit_environment, mask_sources, protected_masks, rewind_data_fd, sync_pipe_is_closed,
};
use crate::sandbox::layout::ResolvedSandboxLayout;

fn layout(workspace_root: impl Into<PathBuf>, protected_roots: Vec<PathBuf>) -> ResolvedSandboxLayout {
    let workspace_root = workspace_root.into();
    ResolvedSandboxLayout {
        workspace_capability: crate::WorkspaceRootLaunchCapability::linux_test_placeholder(workspace_root.clone()),
        workspace_root,
        protected_roots,
        protected_object_identities: Vec::new(),
        _test_helper_path: None,
    }
}

#[test]
fn bind_data_descriptor_is_rewound_before_bubblewrap_reads_it() {
    use std::io::{Read, Write};
    use std::os::fd::AsRawFd;

    let mut file = tempfile::tempfile().unwrap();
    file.write_all(b"sealed-helper").unwrap();

    rewind_data_fd(file.as_raw_fd()).unwrap();

    let mut contents = Vec::new();
    file.read_to_end(&mut contents).unwrap();
    assert_eq!(contents, b"sealed-helper");
}

#[test]
fn sync_pipe_proves_drain_only_after_every_writer_closes() {
    use std::os::fd::AsRawFd;

    let (read, write) = cloexec_pipe().unwrap();

    assert!(!sync_pipe_is_closed(read.as_raw_fd()).unwrap());
    drop(write);
    assert!(sync_pipe_is_closed(read.as_raw_fd()).unwrap());
}

#[test]
fn invalid_sync_pipe_is_an_error_not_a_drain_proof() {
    use std::os::fd::AsRawFd;

    let (read, _write) = cloexec_pipe().unwrap();
    let descriptor = read.as_raw_fd();
    drop(read);

    let error = sync_pipe_is_closed(descriptor).unwrap_err();

    assert_eq!(error.raw_os_error(), Some(libc::EBADF));
}

#[test]
fn wrapper_environment_drops_loader_injection_but_keeps_business_values() {
    let mut command = tokio::process::Command::new("/proc/self/fd/41");
    command
        .env_clear()
        .env("LD_PRELOAD", "/workspace/inject.so")
        .env("GLIBC_TUNABLES", "glibc.malloc.check=3")
        .env("MCP_EXPLICIT_TOKEN", "configured");

    let environment = explicit_environment(&command, false).unwrap();

    assert!(!environment.iter().any(|(key, _)| key == "LD_PRELOAD"));
    assert!(!environment.iter().any(|(key, _)| key == "GLIBC_TUNABLES"));
    assert!(
        environment
            .iter()
            .any(|(key, value)| key == "MCP_EXPLICIT_TOKEN" && value == "configured")
    );
}

#[test]
fn proxy_environment_uses_only_the_internal_read_only_ca_path() {
    let mut command = tokio::process::Command::new("/proc/self/fd/41");
    command
        .env_clear()
        .env("SSL_CERT_FILE", "/host/leaked-ca.pem")
        .env("NODE_EXTRA_CA_CERTS", "/host/leaked-node-ca.pem");

    let environment = explicit_environment(&command, true).unwrap();

    for key in ["SSL_CERT_FILE", "NODE_EXTRA_CA_CERTS", "REQUESTS_CA_BUNDLE"] {
        assert_eq!(
            environment
                .iter()
                .find(|(candidate, _)| candidate == key)
                .map(|(_, value)| value),
            Some(&OsString::from("/__solaris/state/network-proxy-ca.pem"))
        );
    }
    assert!(!environment.iter().any(|(_, value)| value == "/host/leaked-ca.pem"));
}

#[test]
fn proxy_ca_is_overlaid_as_a_read_only_file_after_private_state_mount() {
    let layout = layout("/workspace", Vec::new());
    let argv = build_bwrap_argv(
        &layout,
        Path::new("/workspace"),
        Path::new("/private-home"),
        Path::new("/private-tmp"),
        Path::new("/private-state"),
        &[],
        40,
        41,
        42,
        43,
        &[],
        true,
    )
    .unwrap();
    let values = argv.iter().map(|value| value.to_string_lossy()).collect::<Vec<_>>();
    let state_mount = values
        .windows(3)
        .position(|values| values == ["--bind", "/private-state", "/__solaris/state"])
        .unwrap();
    let ca_mount = values
        .windows(3)
        .position(|values| {
            values
                == [
                    "--ro-bind",
                    "/private-state/network-proxy-ca.pem",
                    "/__solaris/state/network-proxy-ca.pem",
                ]
        })
        .unwrap();

    assert!(state_mount < ca_mount);
}

#[test]
fn private_control_state_cannot_be_reintroduced_through_the_workspace() {
    let layout = layout("/tmp", Vec::new());

    let error =
        ensure_private_sources_are_disjoint(&layout, [Path::new("/tmp/solaris-sandbox-state-private")]).unwrap_err();

    assert!(matches!(
        error.get_ref().and_then(|source| source.downcast_ref()),
        Some(crate::SandboxError::UnrepresentableSandboxLayout)
    ));
}

#[test]
fn workspace_cannot_shadow_the_internal_runner() {
    let layout = layout("/__solaris", Vec::new());

    let error = ensure_internal_destinations_are_available(&layout).unwrap_err();

    assert!(matches!(
        error.get_ref().and_then(|source| source.downcast_ref()),
        Some(crate::SandboxError::UnrepresentableSandboxLayout)
    ));
}

#[test]
fn strict_profile_contains_every_required_namespace_and_no_try_option() {
    let layout = layout("/workspace", vec![PathBuf::from("/runtime")]);
    let argv = build_bwrap_argv(
        &layout,
        Path::new("/workspace"),
        Path::new("/private-home"),
        Path::new("/private-tmp"),
        Path::new("/private-state"),
        &[],
        40,
        41,
        42,
        43,
        &[OsString::from("arg")],
        false,
    )
    .unwrap();
    let values = argv.iter().map(|value| value.to_string_lossy()).collect::<Vec<_>>();

    for required in [
        "--unshare-user",
        "--unshare-pid",
        "--unshare-net",
        "--unshare-ipc",
        "--unshare-uts",
        "--disable-userns",
        "--assert-userns-disabled",
        "--cap-drop",
        "--new-session",
        "--die-with-parent",
        "--sync-fd",
        "--bind-fd",
        "--ro-bind-data",
    ] {
        assert!(values.iter().any(|value| value == required), "missing {required}");
    }
    assert!(!values.iter().any(|value| value.ends_with("-try")));
    assert!(
        values
            .windows(3)
            .any(|values| values == ["--ro-bind-data", "41", "/__solaris/target"])
    );
    assert!(
        values
            .windows(3)
            .any(|values| values == ["--ro-bind-data", "42", "/__solaris/runner"])
    );
    assert!(values.windows(2).any(|values| values == ["--sync-fd", "43"]));
    assert_eq!(values.last().map(|value| value.as_ref()), Some("arg"));
}

#[test]
fn protected_root_is_masked_with_an_empty_read_only_bind() {
    let layout = layout("/workspace", vec![PathBuf::from("/runtime")]);
    let argv = build_bwrap_argv(
        &layout,
        Path::new("/workspace"),
        Path::new("/private-home"),
        Path::new("/private-tmp"),
        Path::new("/private-state"),
        &[super::ProtectedMask {
            source: PathBuf::from("/empty"),
            destination: PathBuf::from("/runtime"),
        }],
        40,
        41,
        42,
        43,
        &[],
        false,
    )
    .unwrap();
    let values = argv.iter().map(|value| value.to_string_lossy()).collect::<Vec<_>>();

    assert!(
        values
            .windows(3)
            .any(|values| values == ["--ro-bind", "/empty", "/runtime"])
    );
    assert!(!values.iter().any(|value| value == "/"));
}

#[test]
fn private_mount_precedes_a_workspace_nested_below_tmp() {
    let layout = layout("/tmp/workspace", Vec::new());
    let argv = build_bwrap_argv(
        &layout,
        Path::new("/tmp/workspace"),
        Path::new("/private-home"),
        Path::new("/private-tmp"),
        Path::new("/private-state"),
        &[],
        40,
        41,
        42,
        43,
        &[],
        false,
    )
    .unwrap();
    let values = argv.iter().map(|value| value.to_string_lossy()).collect::<Vec<_>>();
    let private_tmp = values
        .windows(3)
        .position(|values| values == ["--bind", "/private-tmp", "/tmp"])
        .unwrap();
    let workspace = values
        .windows(3)
        .position(|values| values == ["--bind-fd", "40", "/tmp/workspace"])
        .unwrap();

    assert!(private_tmp < workspace);
}

#[test]
fn protected_masks_use_an_object_with_the_same_file_kind() {
    let directory = tempfile::tempdir().unwrap();
    let protected_directory = directory.path().join("runtime");
    let protected_file = directory.path().join("session.sqlite3");
    let empty_directory = directory.path().join("empty");
    let empty_file = directory.path().join("empty-file");
    std::fs::create_dir_all(&protected_directory).unwrap();
    std::fs::write(&protected_file, b"state").unwrap();
    std::fs::create_dir_all(&empty_directory).unwrap();
    std::fs::write(&empty_file, b"").unwrap();

    let masks = protected_masks(
        &[protected_directory.clone(), protected_file.clone()],
        &empty_directory,
        &empty_file,
    )
    .unwrap();

    assert_eq!(masks[0].source, empty_directory);
    assert_eq!(masks[0].destination, protected_directory);
    assert_eq!(masks[1].source, empty_file);
    assert_eq!(masks[1].destination, protected_file);
}

#[test]
fn directory_mask_source_is_actually_empty() {
    let sources = mask_sources().unwrap();

    assert!(sources.directory.is_dir());
    assert!(sources.file.is_file());
    assert!(std::fs::read_dir(&sources.directory).unwrap().next().is_none());
}
