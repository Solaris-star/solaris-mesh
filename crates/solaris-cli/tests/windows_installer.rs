#![cfg(windows)]

use std::path::{Path, PathBuf};
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

fn write_digest_manifest(path: &Path) {
    let digest = Command::new("powershell.exe")
        .args(["-NoProfile", "-Command"])
        .arg(format!(
            "(Get-FileHash -LiteralPath '{}' -Algorithm SHA256).Hash.ToLowerInvariant()",
            path.display()
        ))
        .output()
        .unwrap();
    assert!(digest.status.success());
    std::fs::write(
        path.with_file_name(format!("{}.sha256", path.file_name().unwrap().to_string_lossy())),
        format!("sha256:{}\n", String::from_utf8(digest.stdout).unwrap().trim()),
    )
    .unwrap();
}

fn package_architecture() -> &'static str {
    if cfg!(target_arch = "x86_64") {
        "x64"
    } else if cfg!(target_arch = "aarch64") {
        "arm64"
    } else {
        "x86"
    }
}

fn remove_test_package(package_name: &str) {
    let _ = Command::new("powershell.exe")
        .args(["-NoProfile", "-Command"])
        .arg(format!(
            "@(Get-AppxPackage -Name '{}' -ErrorAction SilentlyContinue) | ForEach-Object {{ Remove-AppxPackage -Package $_.PackageFullName -ErrorAction SilentlyContinue }}",
            package_name
        ))
        .status();
}

struct PackageCleanup(String);

impl PackageCleanup {
    fn new(package_name: String) -> Self {
        remove_test_package(&package_name);
        Self(package_name)
    }
}

impl Drop for PackageCleanup {
    fn drop(&mut self) {
        remove_test_package(&self.0);
    }
}

fn populate_release_source(root: &Path, source: &Path, binary: &[u8], helper: &[u8], package_name: &str) {
    std::fs::write(source.join("solaris.exe"), binary).unwrap();
    let helper_path = source.join("solaris-process-sandbox-helper.exe");
    std::fs::write(&helper_path, helper).unwrap();
    write_digest_manifest(&helper_path);

    let proxy_path = source.join("solaris-windows-network-proxy.exe");
    std::fs::copy(std::env::current_exe().unwrap(), &proxy_path).unwrap();
    write_digest_manifest(&proxy_path);
    let proxy_package = source.join("solaris-windows-network-proxy.msix");
    let package = Command::new("powershell.exe")
        .args(["-NoProfile", "-ExecutionPolicy", "Bypass", "-File"])
        .arg(root.join("crates/solaris-process/windows-network-proxy/build-unsigned-package.ps1"))
        .arg("-BinaryPath")
        .arg(&proxy_path)
        .arg("-OutputPath")
        .arg(&proxy_package)
        .arg("-PackageName")
        .arg(package_name)
        .arg("-Publisher")
        .arg("CN=Solaris Mesh")
        .arg("-Version")
        .arg("0.3.0.0")
        .arg("-Architecture")
        .arg(package_architecture())
        .status()
        .unwrap();
    assert!(package.success());
    write_digest_manifest(&proxy_package);

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
        std::fs::copy(from, source.join(name)).unwrap();
    }
}

fn assert_installed_payload(path: &Path) {
    for name in [
        "solaris.exe",
        "solaris-process-sandbox-helper.exe",
        "solaris-process-sandbox-helper.exe.sha256",
        "solaris-windows-network-proxy.exe",
        "solaris-windows-network-proxy.exe.sha256",
        "solaris-windows-network-proxy.msix",
        "solaris-windows-network-proxy.msix.sha256",
        "solaris-extension.json",
    ] {
        assert!(path.join(name).is_file(), "missing installed {name}");
    }
}

fn assert_registration_or_platform_blocker(destination: &Path, package_name: &str) {
    let identity_path = destination.join("solaris-windows-network-proxy.identity.json");
    let query = Command::new("powershell.exe")
        .args(["-NoProfile", "-Command"])
        .arg(format!(
            "$p=@(Get-AppxPackage -Name '{}' -ErrorAction SilentlyContinue)|Sort-Object Version -Descending|Select-Object -First 1; if($p){{ $p.PackageFamilyName }}",
            package_name
        ))
        .output()
        .unwrap();
    assert!(query.status.success());
    let registered_family = String::from_utf8(query.stdout).unwrap().trim().to_owned();
    if registered_family.is_empty() {
        assert!(
            !identity_path.exists(),
            "an identity must not be published without a registered package"
        );
    } else {
        let identity: Value = serde_json::from_slice(&std::fs::read(&identity_path).unwrap()).unwrap();
        assert_eq!(identity["schema_version"], 1);
        assert_eq!(identity["package_family_name"], registered_family);
        assert_eq!(
            identity["application_user_model_id"],
            format!("{registered_family}!Proxy")
        );
    }
}

#[test]
fn windows_installer_copies_the_real_manifest_to_the_requested_layout() {
    let root = repository_root();
    let source = tempdir().unwrap();
    let destination = tempdir().unwrap();
    let package_name = format!("Solaris.Mesh.NetworkProxy.TestA.{}", std::process::id());
    let _cleanup = PackageCleanup::new(package_name.clone());
    populate_release_source(&root, source.path(), b"binary", b"helper", &package_name);

    let status = Command::new("powershell.exe")
        .args(["-NoProfile", "-ExecutionPolicy", "Bypass", "-File"])
        .arg(root.join("packaging/windows/install-solaris.ps1"))
        .arg("-SourceDirectory")
        .arg(source.path())
        .arg("-InstallDirectory")
        .arg(destination.path())
        .arg("-NetworkProxyPackageName")
        .arg(&package_name)
        .status()
        .unwrap();
    assert!(status.success());

    let installed = std::fs::read(destination.path().join("solaris-extension.json")).unwrap();
    let source_manifest = std::fs::read(root.join("solaris-extension.json")).unwrap();
    assert_eq!(installed, source_manifest);
    assert_installed_payload(destination.path());
    assert_registration_or_platform_blocker(destination.path(), &package_name);
}

#[test]
fn windows_release_zip_and_entry_installer_preserve_the_real_manifest() {
    let root = repository_root();
    let source = tempdir().unwrap();
    let unpacked = tempdir().unwrap();
    let installed = tempdir().unwrap();
    let archive_root = tempdir().unwrap();
    let package_name = format!("Solaris.Mesh.NetworkProxy.TestB.{}", std::process::id());
    let _cleanup = PackageCleanup::new(package_name.clone());
    populate_release_source(
        &root,
        source.path(),
        b"release-binary",
        b"release-helper",
        &package_name,
    );

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
    assert_installed_payload(unpacked.path());

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
            "& \"{}\" -InstallDirectory \"{}\" -NetworkProxyPackageName '{}'",
            unpacked.path().join("install-solaris.cmd").display(),
            installed.path().display(),
            package_name
        ))
        .status()
        .unwrap();
    assert!(status.success());
    assert_eq!(
        std::fs::read(installed.path().join("solaris-extension.json")).unwrap(),
        repository_manifest
    );
    assert_installed_payload(installed.path());
    assert_registration_or_platform_blocker(installed.path(), &package_name);
}
