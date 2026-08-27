#![cfg(windows)]

use std::path::PathBuf;
use std::process::Command;

use tempfile::tempdir;

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("solaris-cli must remain inside the workspace crates directory")
        .to_path_buf()
}

#[test]
fn windows_installer_copies_the_real_manifest_to_the_requested_layout() {
    let root = repository_root();
    let source = tempdir().unwrap();
    let destination = tempdir().unwrap();
    let helper = source.path().join("solaris-process-sandbox-helper.exe");
    std::fs::write(source.path().join("solaris.exe"), b"binary").unwrap();
    std::fs::write(&helper, b"helper").unwrap();
    let digest = Command::new("powershell.exe")
        .args(["-NoProfile", "-Command"])
        .arg(format!(
            "(Get-FileHash -LiteralPath '{}' -Algorithm SHA256).Hash.ToLowerInvariant()",
            helper.display()
        ))
        .output()
        .unwrap();
    assert!(digest.status.success());
    let digest = String::from_utf8(digest.stdout).unwrap();
    std::fs::write(
        source.path().join("solaris-process-sandbox-helper.exe.sha256"),
        format!("sha256:{}\n", digest.trim()),
    )
    .unwrap();
    std::fs::copy(
        root.join("solaris-extension.json"),
        source.path().join("solaris-extension.json"),
    )
    .unwrap();

    let status = Command::new("powershell.exe")
        .args(["-NoProfile", "-ExecutionPolicy", "Bypass", "-File"])
        .arg(root.join("packaging/windows/install-solaris.ps1"))
        .arg("-SourceDirectory")
        .arg(source.path())
        .arg("-InstallDirectory")
        .arg(destination.path())
        .status()
        .unwrap();
    assert!(status.success());

    let installed = std::fs::read(destination.path().join("solaris-extension.json")).unwrap();
    let source_manifest = std::fs::read(root.join("solaris-extension.json")).unwrap();
    assert_eq!(installed, source_manifest);
    for name in [
        "solaris.exe",
        "solaris-process-sandbox-helper.exe",
        "solaris-process-sandbox-helper.exe.sha256",
        "solaris-extension.json",
    ] {
        assert!(destination.path().join(name).is_file(), "missing installed {name}");
    }
}
