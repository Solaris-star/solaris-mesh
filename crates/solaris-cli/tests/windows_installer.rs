#![cfg(windows)]

use std::path::PathBuf;
use std::process::Command;

use serde_json::Value;
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

#[test]
fn windows_release_zip_and_entry_installer_preserve_the_real_manifest() {
    let root = repository_root();
    let source = tempdir().unwrap();
    let unpacked = tempdir().unwrap();
    let installed = tempdir().unwrap();
    let archive_root = tempdir().unwrap();
    let helper = source.path().join("solaris-process-sandbox-helper.exe");
    std::fs::write(source.path().join("solaris.exe"), b"release-binary").unwrap();
    std::fs::write(&helper, b"release-helper").unwrap();
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
    for (from, name) in [
        (root.join("solaris-extension.json"), "solaris-extension.json"),
        (
            root.join("packaging/windows/install-solaris.ps1"),
            "install-solaris.ps1",
        ),
        (
            root.join("packaging/windows/install-solaris.cmd"),
            "install-solaris.cmd",
        ),
    ] {
        std::fs::copy(from, source.path().join(name)).unwrap();
    }

    let archive = archive_root.path().join("solaris-release.zip");
    let create = Command::new("powershell.exe")
        .args(["-NoProfile", "-Command"])
        .arg(format!(
            "Compress-Archive -Path '{}\\*' -DestinationPath '{}' -Force",
            source.path().display(),
            archive.display()
        ))
        .status()
        .unwrap();
    assert!(create.success());
    assert!(archive.is_file());
    let expand = Command::new("powershell.exe")
        .args(["-NoProfile", "-Command"])
        .arg(format!(
            "Expand-Archive -LiteralPath '{}' -DestinationPath '{}' -Force",
            archive.display(),
            unpacked.path().display()
        ))
        .status()
        .unwrap();
    assert!(expand.success());

    let archived_manifest = std::fs::read(unpacked.path().join("solaris-extension.json")).unwrap();
    let repository_manifest = std::fs::read(root.join("solaris-extension.json")).unwrap();
    assert_eq!(archived_manifest, repository_manifest);
    let manifest: Value = serde_json::from_slice(&archived_manifest).unwrap();
    let adapter = &manifest["contributes"]["acpAdapters"][0];
    assert_eq!(adapter["cliCommand"], "solaris");
    assert_eq!(adapter["acpArgs"], serde_json::json!(["acp"]));

    let status = Command::new("powershell.exe")
        .args(["-NoProfile", "-Command"])
        .arg(format!(
            "& \"{}\" -InstallDirectory \"{}\"",
            unpacked.path().join("install-solaris.cmd").display(),
            installed.path().display()
        ))
        .status()
        .unwrap();
    assert!(status.success());
    assert_eq!(
        std::fs::read(installed.path().join("solaris-extension.json")).unwrap(),
        repository_manifest
    );
}
