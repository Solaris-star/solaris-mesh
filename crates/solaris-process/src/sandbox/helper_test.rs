use super::{parse_manifest_digest, pin_packaged_candidate};
use crate::{SandboxError, SandboxReason};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

#[test]
fn packaged_helper_manifest_requires_one_exact_tagged_digest() {
    let digest = "a".repeat(64);
    let valid = format!("sha256:{digest}\n");
    assert_eq!(parse_manifest_digest(valid.as_bytes()), Some(digest.as_str()));

    for invalid in [
        digest.clone(),
        format!("sha256:{}", "A".repeat(64)),
        format!("sha256:{} extra", "a".repeat(64)),
        format!("sha256:{}", "a".repeat(63)),
    ] {
        assert!(parse_manifest_digest(invalid.as_bytes()).is_none());
    }
}

#[test]
fn packaged_helper_rejects_missing_mismatched_or_wrong_binary_manifest() {
    let directory = tempfile::tempdir().unwrap();
    let helper = directory.path().join(format!("helper{}", std::env::consts::EXE_SUFFIX));
    std::fs::copy(std::env::current_exe().unwrap(), &helper).unwrap();
    let manifest = directory.path().join("helper.sha256");

    let expected = "0".repeat(64);
    assert_helper_unavailable(pin_packaged_candidate(&helper, &manifest, None, &expected).unwrap_err());

    std::fs::write(&manifest, format!("sha256:{}\n", "1".repeat(64))).unwrap();
    assert_helper_unavailable(pin_packaged_candidate(&helper, &manifest, None, &expected).unwrap_err());

    std::fs::write(&manifest, format!("sha256:{expected}\n")).unwrap();
    assert_helper_unavailable(pin_packaged_candidate(&helper, &manifest, None, &expected).unwrap_err());
}

#[cfg(unix)]
#[test]
fn packaged_helper_rejects_every_owner_group_or_other_write_bit() {
    for (helper_mode, manifest_mode) in [(0o755, 0o444), (0o575, 0o444), (0o557, 0o444), (0o555, 0o644)] {
        let directory = tempfile::tempdir().unwrap();
        let helper = directory.path().join("helper");
        std::fs::copy(std::env::current_exe().unwrap(), &helper).unwrap();
        let digest = crate::inspect_executable(&helper).unwrap().content_digest().to_owned();
        let manifest = directory.path().join("helper.sha256");
        std::fs::write(&manifest, format!("sha256:{digest}\n")).unwrap();
        std::fs::set_permissions(&helper, std::fs::Permissions::from_mode(helper_mode)).unwrap();
        std::fs::set_permissions(&manifest, std::fs::Permissions::from_mode(manifest_mode)).unwrap();

        assert_helper_unavailable(pin_packaged_candidate(&helper, &manifest, None, &digest).unwrap_err());
    }
}

#[cfg(unix)]
#[test]
fn packaged_helper_accepts_read_only_manifest_and_non_writable_executable() {
    let directory = tempfile::tempdir().unwrap();
    let helper = directory.path().join("helper");
    std::fs::copy(std::env::current_exe().unwrap(), &helper).unwrap();
    let digest = crate::inspect_executable(&helper).unwrap().content_digest().to_owned();
    let manifest = directory.path().join("helper.sha256");
    std::fs::write(&manifest, format!("sha256:{digest}\n")).unwrap();
    std::fs::set_permissions(&helper, std::fs::Permissions::from_mode(0o555)).unwrap();
    std::fs::set_permissions(&manifest, std::fs::Permissions::from_mode(0o444)).unwrap();

    pin_packaged_candidate(&helper, &manifest, None, &digest).unwrap();
}

#[cfg(windows)]
#[test]
fn packaged_helper_rejects_default_writable_windows_dacl() {
    let directory = tempfile::tempdir().unwrap();
    let helper = directory.path().join("helper.exe");
    std::fs::copy(std::env::current_exe().unwrap(), &helper).unwrap();
    let digest = crate::inspect_executable(&helper).unwrap().content_digest().to_owned();
    let manifest = directory.path().join("helper.exe.sha256");
    std::fs::write(&manifest, format!("sha256:{digest}\n")).unwrap();

    assert_helper_unavailable(pin_packaged_candidate(&helper, &manifest, None, &digest).unwrap_err());
}

#[cfg(windows)]
#[test]
fn packaged_helper_accepts_read_execute_only_windows_dacl() {
    let directory = tempfile::tempdir().unwrap();
    let helper = directory.path().join("helper.exe");
    std::fs::copy(std::env::current_exe().unwrap(), &helper).unwrap();
    let digest = crate::inspect_executable(&helper).unwrap().content_digest().to_owned();
    let manifest = directory.path().join("helper.exe.sha256");
    std::fs::write(&manifest, format!("sha256:{digest}\n")).unwrap();
    tighten_test_dacl(&helper, "RX");
    tighten_test_dacl(&manifest, "R");

    pin_packaged_candidate(&helper, &manifest, None, &digest).unwrap();
}

#[cfg(windows)]
#[test]
fn packaged_helper_rejects_writes_for_current_user_users_or_everyone() {
    let current_user = std::env::var("USERNAME").unwrap();
    for principal in [current_user.as_str(), "*S-1-5-32-545", "*S-1-1-0"] {
        let directory = tempfile::tempdir().unwrap();
        let helper = directory.path().join("helper.exe");
        std::fs::copy(std::env::current_exe().unwrap(), &helper).unwrap();
        let digest = crate::inspect_executable(&helper).unwrap().content_digest().to_owned();
        let manifest = directory.path().join("helper.exe.sha256");
        std::fs::write(&manifest, format!("sha256:{digest}\n")).unwrap();
        tighten_test_dacl(&helper, "RX");
        tighten_test_dacl(&manifest, "R");
        grant_test_dacl(&helper, principal, "M");

        assert_helper_unavailable(pin_packaged_candidate(&helper, &manifest, None, &digest).unwrap_err());
    }
}

#[cfg(windows)]
fn tighten_test_dacl(path: &std::path::Path, rights: &str) {
    let user = std::env::var("USERNAME").unwrap();
    let grant = format!("{user}:({rights})");
    let status = std::process::Command::new("icacls.exe")
        .arg(path)
        .args(["/inheritance:r", "/grant:r", &grant])
        .status()
        .unwrap();
    assert!(status.success());
}

#[cfg(windows)]
fn grant_test_dacl(path: &std::path::Path, principal: &str, rights: &str) {
    let grant = format!("{principal}:({rights})");
    let status = std::process::Command::new("icacls.exe")
        .arg(path)
        .args(["/grant", &grant])
        .status()
        .unwrap();
    assert!(status.success());
}

fn assert_helper_unavailable(error: std::io::Error) {
    assert!(matches!(
        error.get_ref().and_then(|source| source.downcast_ref::<SandboxError>()),
        Some(SandboxError::InsufficientEnforcement { report })
            if report.reason() == SandboxReason::HelperUnavailable
    ));
}
