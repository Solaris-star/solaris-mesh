#[cfg(any(target_os = "linux", target_os = "macos"))]
use super::*;

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn sealed_workspace(path: &Path) -> crate::WorkspaceRootLaunchCapability {
    crate::WorkspaceRootAuthority::capture(path)
        .expect("capture workspace authority")
        .seal(path)
        .expect("seal workspace authority")
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn duplicate_workspace_directory(capability: &crate::WorkspaceRootLaunchCapability) -> std::io::Result<std::fs::File> {
    #[cfg(target_os = "linux")]
    {
        capability.duplicate_linux_directory()
    }
    #[cfg(target_os = "macos")]
    {
        capability.duplicate_macos_directory()
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn verify_retained_workspace(
    snapshot: &mut ProtectedIdentitySnapshot,
    workspace: &Path,
    directory: std::fs::File,
) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        snapshot.verify_linux_workspace(&[], workspace, directory)
    }
    #[cfg(target_os = "macos")]
    {
        snapshot.verify_macos_workspace(&[], workspace, directory)
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn retained_workspace_scan_detects_external_hardlink_after_path_replacement() {
    let root = tempfile::tempdir().expect("tempdir");
    let workspace = root.path().join("workspace");
    let retained = root.path().join("retained");
    let external = root.path().join("external");
    std::fs::create_dir(&workspace).expect("create workspace");
    std::fs::write(&external, b"protected").expect("write external file");
    std::fs::hard_link(&external, workspace.join("alias")).expect("create external hardlink");
    let capability = sealed_workspace(&workspace);
    std::fs::rename(&workspace, &retained).expect("replace authorized workspace path");
    std::fs::create_dir(&workspace).expect("create clean replacement");

    let mut snapshot = ProtectedIdentitySnapshot::capture(&[], &[]).expect("capture identities");
    let error = verify_retained_workspace(
        &mut snapshot,
        &workspace,
        duplicate_workspace_directory(&capability).expect("duplicate retained workspace directory"),
    )
    .expect_err("the retained object, not its clean replacement, must be inspected");

    assert_eq!(
        error.get_ref().and_then(|source| source.downcast_ref::<SandboxError>()),
        Some(&SandboxError::ExternalHardlink)
    );
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn retained_workspace_scan_detects_socket_after_path_replacement() {
    use std::os::unix::net::UnixListener;

    let root = tempfile::tempdir().expect("tempdir");
    let workspace = root.path().join("workspace");
    let retained = root.path().join("retained");
    std::fs::create_dir(&workspace).expect("create workspace");
    let _listener = UnixListener::bind(workspace.join("host.sock")).expect("bind workspace socket");
    let capability = sealed_workspace(&workspace);
    std::fs::rename(&workspace, &retained).expect("replace authorized workspace path");
    std::fs::create_dir(&workspace).expect("create clean replacement");

    let mut snapshot = ProtectedIdentitySnapshot::capture(&[], &[]).expect("capture identities");
    let error = verify_retained_workspace(
        &mut snapshot,
        &workspace,
        duplicate_workspace_directory(&capability).expect("duplicate retained workspace directory"),
    )
    .expect_err("the retained object, not its clean replacement, must be inspected");

    assert_eq!(
        error.get_ref().and_then(|source| source.downcast_ref::<SandboxError>()),
        Some(&SandboxError::HostSocketExposed)
    );
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn retained_workspace_scan_detects_fifo_after_path_replacement() {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let root = tempfile::tempdir().expect("tempdir");
    let workspace = root.path().join("workspace");
    let retained = root.path().join("retained");
    let fifo = workspace.join("host.fifo");
    std::fs::create_dir(&workspace).expect("create workspace");
    let fifo = CString::new(fifo.as_os_str().as_bytes()).expect("fifo path");
    assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
    let capability = sealed_workspace(&workspace);
    std::fs::rename(&workspace, &retained).expect("replace authorized workspace path");
    std::fs::create_dir(&workspace).expect("create clean replacement");

    let mut snapshot = ProtectedIdentitySnapshot::capture(&[], &[]).expect("capture identities");
    let error = verify_retained_workspace(
        &mut snapshot,
        &workspace,
        duplicate_workspace_directory(&capability).expect("duplicate retained workspace directory"),
    )
    .expect_err("the retained FIFO must be rejected even after path replacement");

    assert_eq!(
        error.get_ref().and_then(|source| source.downcast_ref::<SandboxError>()),
        Some(&SandboxError::HostSocketExposed)
    );
}
