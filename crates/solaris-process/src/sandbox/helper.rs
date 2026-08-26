use std::io;
use std::path::{Path, PathBuf};

use crate::PinnedExecutable;
use crate::runner::pin_executable_by_digest;
#[cfg(any(test, all(feature = "sandbox-test-fixtures", debug_assertions)))]
use crate::runner::{inspect_executable, pin_executable};

use super::{
    SandboxBackend, SandboxEnforcement, SandboxError, SandboxReason, SandboxReport, insufficient_enforcement_error,
    sandbox_io_error,
};

const MAX_MANIFEST_BYTES: u64 = 80;
const PACKAGED_HELPER_SHA256: Option<&str> = option_env!("SOLARIS_PACKAGED_SANDBOX_HELPER_SHA256");

pub(crate) fn trusted_sandbox_helper(
    workspace_root: Option<&Path>,
    fixture_path: Option<&Path>,
) -> io::Result<PinnedExecutable> {
    #[cfg(any(test, all(feature = "sandbox-test-fixtures", debug_assertions)))]
    {
        let located = locate_sandbox_helper();
        if let Some(path) = fixture_path.or(located.as_deref()) {
            return pin_development_fixture(path, workspace_root);
        }
    }
    #[cfg(not(any(test, all(feature = "sandbox-test-fixtures", debug_assertions))))]
    let _ = fixture_path;

    let expected_digest = packaged_expected_digest().ok_or_else(helper_unavailable)?;

    for helper_path in sandbox_helper_candidates() {
        if !helper_path.is_file() {
            continue;
        }
        let manifest_path = helper_manifest_path(&helper_path);
        if !manifest_path.is_file() {
            continue;
        }
        return pin_packaged_candidate(&helper_path, &manifest_path, workspace_root, expected_digest);
    }
    Err(helper_unavailable())
}

#[cfg(any(test, all(feature = "sandbox-test-fixtures", debug_assertions)))]
fn locate_sandbox_helper() -> Option<PathBuf> {
    sandbox_helper_candidates()
        .into_iter()
        .find(|candidate| candidate.is_file())
}

fn sandbox_helper_candidates() -> Vec<PathBuf> {
    let Some(current) = std::env::current_exe().ok() else {
        return Vec::new();
    };
    let Some(parent) = current.parent() else {
        return Vec::new();
    };
    let name = format!("solaris-process-sandbox-helper{}", std::env::consts::EXE_SUFFIX);
    let mut candidates = vec![parent.join(&name)];
    if let Some(root) = parent.parent() {
        candidates.push(root.join(name));
    }
    candidates
}

fn helper_manifest_path(helper_path: &Path) -> PathBuf {
    let mut name = helper_path
        .file_name()
        .map_or_else(std::ffi::OsString::new, std::ffi::OsStr::to_os_string);
    name.push(".sha256");
    helper_path.with_file_name(name)
}

fn pin_packaged_candidate(
    helper_path: &Path,
    manifest_path: &Path,
    workspace_root: Option<&Path>,
    expected_digest: &str,
) -> io::Result<PinnedExecutable> {
    reject_workspace_package(helper_path, manifest_path, workspace_root)?;
    let metadata = std::fs::symlink_metadata(manifest_path).map_err(|_| helper_unavailable())?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() > MAX_MANIFEST_BYTES {
        return Err(helper_unavailable());
    }
    require_non_writable_package_file(manifest_path, &metadata)?;
    let helper_metadata = std::fs::symlink_metadata(helper_path).map_err(|_| helper_unavailable())?;
    if !helper_metadata.is_file() || helper_metadata.file_type().is_symlink() {
        return Err(helper_unavailable());
    }
    require_non_writable_package_file(helper_path, &helper_metadata)?;
    let manifest = std::fs::read(manifest_path).map_err(|_| helper_unavailable())?;
    let manifest_digest = parse_manifest_digest(&manifest).ok_or_else(helper_unavailable)?;
    if manifest_digest != expected_digest {
        return Err(helper_unavailable());
    }
    pin_executable_by_digest(helper_path, expected_digest).map_err(|_| helper_unavailable())
}

#[cfg(unix)]
fn require_non_writable_package_file(_path: &Path, metadata: &std::fs::Metadata) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    if metadata.permissions().mode() & 0o222 == 0 {
        Ok(())
    } else {
        Err(helper_unavailable())
    }
}

#[cfg(windows)]
fn require_non_writable_package_file(path: &Path, _metadata: &std::fs::Metadata) -> io::Result<()> {
    windows_dacl_has_no_writable_allow_ace(path)
        .then_some(())
        .ok_or_else(helper_unavailable)
}

#[cfg(not(any(unix, windows)))]
fn require_non_writable_package_file(_path: &Path, _metadata: &std::fs::Metadata) -> io::Result<()> {
    Err(helper_unavailable())
}

#[cfg(windows)]
fn windows_dacl_has_no_writable_allow_ace(path: &Path) -> bool {
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::AsRawHandle;

    use windows_sys::Win32::Foundation::{GENERIC_ALL, GENERIC_WRITE, LocalFree};
    use windows_sys::Win32::Security::Authorization::{GetSecurityInfo, SE_FILE_OBJECT};
    use windows_sys::Win32::Security::{ACCESS_ALLOWED_ACE, ACL, DACL_SECURITY_INFORMATION, GetAce};
    use windows_sys::Win32::Storage::FileSystem::{
        DELETE, FILE_APPEND_DATA, FILE_DELETE_CHILD, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
        FILE_WRITE_ATTRIBUTES, FILE_WRITE_DATA, FILE_WRITE_EA, READ_CONTROL, WRITE_DAC, WRITE_OWNER,
    };

    let file = match std::fs::OpenOptions::new()
        .access_mode(READ_CONTROL)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .open(path)
    {
        Ok(file) => file,
        Err(_) => return false,
    };
    let mut dacl = std::ptr::null_mut::<ACL>();
    let mut descriptor = std::ptr::null_mut();
    let result = unsafe {
        GetSecurityInfo(
            file.as_raw_handle(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &mut dacl,
            std::ptr::null_mut(),
            &mut descriptor,
        )
    };
    if result != 0 || descriptor.is_null() || dacl.is_null() {
        if !descriptor.is_null() {
            unsafe {
                LocalFree(descriptor);
            }
        }
        return false;
    }
    let ace_count = unsafe { (*dacl).AceCount };
    // FILE_GENERIC_WRITE contains shared read/synchronization rights, so using
    // it as an intersection mask would reject a read/execute-only ACE. Check
    // only rights that can mutate the file, its ACL, or its directory entry.
    let write_mask = GENERIC_ALL
        | GENERIC_WRITE
        | FILE_WRITE_DATA
        | FILE_APPEND_DATA
        | FILE_WRITE_EA
        | FILE_WRITE_ATTRIBUTES
        | WRITE_DAC
        | WRITE_OWNER
        | DELETE
        | FILE_DELETE_CHILD;
    let mut trusted = true;
    for index in 0..u32::from(ace_count) {
        let mut ace = std::ptr::null_mut();
        if unsafe { GetAce(dacl, index, &mut ace) } == 0 || ace.is_null() {
            trusted = false;
            break;
        }
        let allowed = matches!(
            unsafe { (*(ace.cast::<ACCESS_ALLOWED_ACE>())).Header.AceType },
            0 | 4 | 5 | 9 | 11
        );
        if allowed && unsafe { (*(ace.cast::<ACCESS_ALLOWED_ACE>())).Mask } & write_mask != 0 {
            trusted = false;
            break;
        }
    }
    unsafe {
        LocalFree(descriptor);
    }
    trusted
}

fn packaged_expected_digest() -> Option<&'static str> {
    let digest = PACKAGED_HELPER_SHA256?;
    (digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
    .then_some(digest)
}

fn reject_workspace_package(helper_path: &Path, manifest_path: &Path, workspace_root: Option<&Path>) -> io::Result<()> {
    let Some(workspace_root) = workspace_root else {
        return Ok(());
    };
    let workspace_root = workspace_root
        .canonicalize()
        .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?;
    for path in [helper_path, manifest_path] {
        let path = path.canonicalize().map_err(|_| helper_unavailable())?;
        if path.starts_with(&workspace_root) {
            return Err(helper_unavailable());
        }
    }
    Ok(())
}

fn parse_manifest_digest(bytes: &[u8]) -> Option<&str> {
    let value = std::str::from_utf8(bytes).ok()?.trim_end_matches(['\r', '\n']);
    let digest = value.strip_prefix("sha256:")?;
    (digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
    .then_some(digest)
}

#[cfg(any(test, all(feature = "sandbox-test-fixtures", debug_assertions)))]
fn pin_development_fixture(path: &Path, workspace_root: Option<&Path>) -> io::Result<PinnedExecutable> {
    if let Some(workspace_root) = workspace_root {
        let workspace_root = workspace_root
            .canonicalize()
            .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?;
        let fixture = path.canonicalize().map_err(|_| helper_unavailable())?;
        if fixture.starts_with(workspace_root) {
            return Err(helper_unavailable());
        }
    }
    let identity = inspect_executable(path).map_err(|_| helper_unavailable())?;
    pin_executable(path, &identity).map_err(|_| helper_unavailable())
}

fn helper_unavailable() -> io::Error {
    insufficient_enforcement_error(SandboxReport::new(
        SandboxEnforcement::Unavailable,
        SandboxBackend::None,
        SandboxReason::HelperUnavailable,
    ))
}

#[cfg(test)]
#[path = "helper_test.rs"]
mod helper_test;
