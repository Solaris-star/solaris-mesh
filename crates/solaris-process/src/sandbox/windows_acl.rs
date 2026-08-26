use std::collections::HashMap;
use std::error::Error;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::io;
use std::os::windows::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use windows_sys::Win32::Foundation::{ERROR_SUCCESS, LocalFree};
use windows_sys::Win32::Security::Authorization::{
    ConvertSidToStringSidW, EXPLICIT_ACCESS_W, GRANT_ACCESS, GetSecurityInfo, REVOKE_ACCESS, SE_FILE_OBJECT,
    SetEntriesInAclW, SetSecurityInfo, TRUSTEE_IS_SID, TRUSTEE_IS_USER, TRUSTEE_W,
};
use windows_sys::Win32::Security::Isolation::{CreateAppContainerProfile, DeleteAppContainerProfile};
use windows_sys::Win32::Security::{
    ACL, CONTAINER_INHERIT_ACE, DACL_SECURITY_INFORMATION, FreeSid, GetLengthSid, GetSecurityDescriptorControl,
    OBJECT_INHERIT_ACE, PROTECTED_DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID, SE_DACL_PROTECTED,
};
use windows_sys::Win32::Storage::FileSystem::{
    DELETE, FILE_ATTRIBUTE_REPARSE_POINT, FILE_DELETE_CHILD, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
    FILE_GENERIC_EXECUTE, FILE_GENERIC_READ, FILE_GENERIC_WRITE, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
    READ_CONTROL, WRITE_DAC,
};

use super::{SandboxError, sandbox_io_error};
use crate::ProtectedObjectIdentity;
use crate::process_finalization::{FinalizationFailures, ProcessFinalizationStage};
use crate::recovery::{
    ProcessRecovery, ProcessRecoveryKind, ProcessRecoveryState, recovery_required_error, register_process_recovery,
    retry_process_recoveries_by_kind, retry_process_recovery,
};

const MAX_ACL_OBJECTS: usize = 262_144;
static PROFILE_COUNTER: AtomicU64 = AtomicU64::new(1);
static ACL_LEASES: OnceLock<Mutex<HashMap<ProtectedObjectIdentity, AclLease>>> = OnceLock::new();

pub(super) struct AppContainerProfile {
    name: Vec<u16>,
    sid: PSID,
}

unsafe impl Send for AppContainerProfile {}

impl AppContainerProfile {
    pub(super) fn create() -> io::Result<Self> {
        let counter = PROFILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?
            .as_nanos() as u64;
        let name = format!("solaris.mesh.auto.{}.{}.{}", std::process::id(), counter, nanos);
        let name = wide_nul(OsStr::new(&name))?;
        let display = wide_nul(OsStr::new("Solaris Mesh strict Auto"))?;
        let description = wide_nul(OsStr::new("Ephemeral Solaris Mesh process sandbox"))?;
        let mut sid = std::ptr::null_mut();
        let result = unsafe {
            CreateAppContainerProfile(
                name.as_ptr(),
                display.as_ptr(),
                description.as_ptr(),
                std::ptr::null(),
                0,
                &mut sid,
            )
        };
        if result < 0 || sid.is_null() {
            return Err(io::Error::from_raw_os_error(result));
        }
        Ok(Self { name, sid })
    }

    pub(super) fn sid(&self) -> PSID {
        self.sid
    }

    pub(super) fn sid_string(&self) -> io::Result<OsString> {
        let mut value = std::ptr::null_mut();
        if unsafe { ConvertSidToStringSidW(self.sid, &mut value) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let length = unsafe { (0..).take_while(|index| *value.add(*index) != 0).count() };
        let result = OsString::from_wide(unsafe { std::slice::from_raw_parts(value, length) });
        unsafe {
            LocalFree(value.cast());
        }
        Ok(result)
    }
}

impl Drop for AppContainerProfile {
    fn drop(&mut self) {
        unsafe {
            DeleteAppContainerProfile(self.name.as_ptr());
            FreeSid(self.sid);
        }
    }
}

pub(super) struct AclGuard {
    leased: Vec<ProtectedObjectIdentity>,
    sid: Vec<u8>,
    #[cfg(feature = "sandbox-test-fixtures")]
    fail_cleanup_attempts: usize,
}

impl AclGuard {
    pub(super) fn apply(
        workspace: &Path,
        protected_roots: &[PathBuf],
        private_roots: &[PathBuf],
        sid: PSID,
    ) -> io::Result<Self> {
        retry_process_recoveries_by_kind(ProcessRecoveryKind::WindowsAcl)?;
        let sid = copy_sid(sid)?;
        let mut guard = Self {
            leased: Vec::new(),
            sid,
            #[cfg(feature = "sandbox-test-fixtures")]
            fail_cleanup_attempts: 0,
        };
        let apply_result = (|| {
            for root in protected_roots {
                for path in collect_tree(root)? {
                    guard.acquire(path, AclMode::Protected)?;
                }
            }
            let workspace_paths = collect_tree(workspace)?;
            for path in workspace_paths {
                if protected_roots.iter().any(|protected| path.starts_with(protected)) {
                    continue;
                }
                guard.acquire(path, AclMode::Writable)?;
            }
            for root in private_roots {
                for path in collect_tree(root)? {
                    guard.acquire(path, AclMode::Writable)?;
                }
            }
            Ok(())
        })();
        finish_acl_apply(guard, apply_result)
    }

    fn acquire(&mut self, path: PathBuf, mode: AclMode) -> io::Result<()> {
        let identity = acquire_acl_lease(&path, mode, &mut self.sid)?;
        self.leased.push(identity);
        Ok(())
    }

    pub(super) fn cleanup(&mut self) -> io::Result<()> {
        #[cfg(feature = "sandbox-test-fixtures")]
        if self.fail_cleanup_attempts > 0 {
            self.fail_cleanup_attempts -= 1;
            return Err(acl_cleanup_error(io::Error::other(
                "injected Windows ACL cleanup failure",
            )));
        }
        let mut first_error = None;
        let mut index = self.leased.len();
        while index > 0 {
            index -= 1;
            match release_acl_lease(self.leased[index], &mut self.sid) {
                Ok(()) => {
                    self.leased.remove(index);
                }
                Err(error) => {
                    first_error.get_or_insert_with(|| acl_cleanup_error(error));
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    #[cfg(feature = "sandbox-test-fixtures")]
    pub(super) fn fail_next_cleanup_for_test(&mut self) {
        self.fail_cleanup_attempts = self.fail_cleanup_attempts.max(1);
    }

    #[cfg(all(test, feature = "sandbox-test-fixtures"))]
    fn fail_cleanup_attempts_for_test(&mut self, attempts: usize) {
        self.fail_cleanup_attempts = attempts;
    }
}

struct PendingAclGuardCleanup {
    guard: Option<AclGuard>,
}

impl ProcessRecovery for PendingAclGuardCleanup {
    fn kind(&self) -> ProcessRecoveryKind {
        ProcessRecoveryKind::WindowsAcl
    }

    fn retry(&mut self) -> io::Result<bool> {
        let Some(guard) = self.guard.as_mut() else {
            return Ok(true);
        };
        guard.cleanup()?;
        self.guard.take();
        Ok(true)
    }
}

fn finish_acl_apply(mut guard: AclGuard, apply_result: io::Result<()>) -> io::Result<AclGuard> {
    let Err(apply_error) = apply_result else {
        return Ok(guard);
    };
    let mut failures = FinalizationFailures::new();
    failures.record_error(ProcessFinalizationStage::Spawn, apply_error);
    match guard.cleanup() {
        Ok(()) => Err(failures
            .finish(Ok(()), ())
            .expect_err("an ACL apply failure was recorded")),
        Err(cleanup_error) => {
            let recovery = register_process_recovery(PendingAclGuardCleanup { guard: Some(guard) });
            match retry_process_recovery(recovery.id()) {
                Ok(ProcessRecoveryState::Complete) => {
                    return Err(failures
                        .finish(Err(cleanup_error), ())
                        .expect_err("an ACL apply failure was recorded"));
                }
                Ok(ProcessRecoveryState::Pending) => {}
                Err(error) => failures.record_error(ProcessFinalizationStage::Recovery, error),
            }
            failures.record_error(
                ProcessFinalizationStage::Reconciliation,
                recovery_required_error(recovery),
            );
            Err(failures
                .finish(Err(cleanup_error), ())
                .expect_err("an ACL apply failure was recorded"))
        }
    }
}

impl Drop for AclGuard {
    fn drop(&mut self) {
        if let Err(error) = self.cleanup() {
            tracing::warn!(
                error_kind = ?error.kind(),
                os_error = ?error.raw_os_error(),
                "failed to restore Windows sandbox ACL during drop"
            );
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AclMode {
    Writable,
    Protected,
}

struct AclLease {
    saved: SavedDacl,
    mode: AclMode,
    holders: usize,
}

fn acquire_acl_lease(path: &Path, mode: AclMode, sid: &mut [u8]) -> io::Result<ProtectedObjectIdentity> {
    let mut opened = SavedDacl::open(path)?;
    let identity = opened.identity;
    let mut leases = acl_leases()
        .lock()
        .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?;
    if let Some(lease) = leases.get_mut(&identity) {
        opened.disarm();
        if lease.mode != mode {
            return Err(sandbox_io_error(SandboxError::UnrepresentableSandboxLayout));
        }
        if mode == AclMode::Writable {
            apply_acl_change(&mut lease.saved, mode, sid)?;
        }
        lease.holders = lease
            .holders
            .checked_add(1)
            .ok_or_else(|| sandbox_io_error(SandboxError::IdentityBudgetExceeded))?;
        return Ok(identity);
    }
    apply_acl_change(&mut opened, mode, sid)?;
    leases.insert(
        identity,
        AclLease {
            saved: opened,
            mode,
            holders: 1,
        },
    );
    Ok(identity)
}

fn release_acl_lease(identity: ProtectedObjectIdentity, sid: &mut [u8]) -> io::Result<()> {
    let mut leases = acl_leases()
        .lock()
        .map_err(|_| io::Error::other("Windows ACL lease registry is poisoned"))?;
    let Some((mode, holders)) = leases.get(&identity).map(|lease| (lease.mode, lease.holders)) else {
        return Ok(());
    };
    if holders == 1 {
        let lease = leases
            .get_mut(&identity)
            .ok_or_else(|| io::Error::other("Windows ACL lease disappeared during restore"))?;
        let restore_result = lease.saved.restore();
        finish_restore_acl(restore_result)?;
        leases.remove(&identity);
        return Ok(());
    }
    if mode == AclMode::Writable {
        let lease = leases
            .get_mut(&identity)
            .ok_or_else(|| io::Error::other("Windows ACL lease disappeared during revoke"))?;
        let revoke_result = apply_acl_entry(&mut lease.saved, 0, REVOKE_ACCESS, false, sid);
        finish_revoke_acl(revoke_result)?;
    }
    let lease = leases
        .get_mut(&identity)
        .ok_or_else(|| io::Error::other("Windows ACL lease disappeared during release"))?;
    lease.holders -= 1;
    Ok(())
}

fn apply_acl_change(saved: &mut SavedDacl, mode: AclMode, sid: &mut [u8]) -> io::Result<()> {
    match mode {
        AclMode::Writable => apply_acl_entry(
            saved,
            FILE_GENERIC_READ | FILE_GENERIC_WRITE | FILE_GENERIC_EXECUTE | DELETE | FILE_DELETE_CHILD,
            GRANT_ACCESS,
            false,
            sid,
        ),
        AclMode::Protected => apply_acl_entry(saved, 0, REVOKE_ACCESS, true, sid),
    }
}

fn apply_acl_entry(
    saved: &mut SavedDacl,
    permissions: u32,
    mode: i32,
    protect_dacl: bool,
    sid: &mut [u8],
) -> io::Result<()> {
    let inheritance = if saved.is_directory {
        OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE
    } else {
        0
    };
    let access = EXPLICIT_ACCESS_W {
        grfAccessPermissions: permissions,
        grfAccessMode: mode,
        grfInheritance: inheritance,
        Trustee: TRUSTEE_W {
            pMultipleTrustee: std::ptr::null_mut(),
            MultipleTrusteeOperation: 0,
            TrusteeForm: TRUSTEE_IS_SID,
            TrusteeType: TRUSTEE_IS_USER,
            ptstrName: sid.as_mut_ptr().cast(),
        },
    };
    let current = CurrentDacl::open(saved.handle())?;
    let mut updated: *mut ACL = std::ptr::null_mut();
    let result = unsafe { SetEntriesInAclW(1, &access, current.dacl, &mut updated) };
    if result != ERROR_SUCCESS {
        return Err(win32_error(result));
    }
    let result = unsafe {
        SetSecurityInfo(
            saved.handle(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION
                | if protect_dacl {
                    PROTECTED_DACL_SECURITY_INFORMATION
                } else {
                    0
                },
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            updated,
            std::ptr::null(),
        )
    };
    unsafe {
        LocalFree(updated.cast());
    }
    if result != ERROR_SUCCESS {
        return Err(win32_error(result));
    }
    Ok(())
}

fn acl_leases() -> &'static Mutex<HashMap<ProtectedObjectIdentity, AclLease>> {
    ACL_LEASES.get_or_init(|| Mutex::new(HashMap::new()))
}

struct SavedDacl {
    file: std::fs::File,
    identity: ProtectedObjectIdentity,
    is_directory: bool,
    descriptor: PSECURITY_DESCRIPTOR,
    dacl: *mut ACL,
    protected: bool,
    restored: bool,
}

// The descriptor allocation and its embedded DACL pointer move together and
// are accessed only while the ACL lease registry is locked.
unsafe impl Send for SavedDacl {}

struct CurrentDacl {
    descriptor: PSECURITY_DESCRIPTOR,
    dacl: *mut ACL,
}

impl CurrentDacl {
    fn open(handle: windows_sys::Win32::Foundation::HANDLE) -> io::Result<Self> {
        let mut dacl = std::ptr::null_mut();
        let mut descriptor = std::ptr::null_mut();
        let result = unsafe {
            GetSecurityInfo(
                handle,
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut dacl,
                std::ptr::null_mut(),
                &mut descriptor,
            )
        };
        if result != ERROR_SUCCESS {
            return Err(win32_error(result));
        }
        Ok(Self { descriptor, dacl })
    }
}

impl Drop for CurrentDacl {
    fn drop(&mut self) {
        unsafe {
            LocalFree(self.descriptor);
        }
    }
}

impl SavedDacl {
    fn open(path: &Path) -> io::Result<Self> {
        use std::os::windows::fs::OpenOptionsExt;
        use std::os::windows::io::AsRawHandle;

        let mut options = std::fs::OpenOptions::new();
        options
            .access_mode(READ_CONTROL | WRITE_DAC)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS);
        let file = options
            .open(path)
            .map_err(|_| sandbox_io_error(SandboxError::IdentityInspectionFailed))?;
        let metadata = file
            .metadata()
            .map_err(|_| sandbox_io_error(SandboxError::IdentityInspectionFailed))?;
        if std::os::windows::fs::MetadataExt::file_attributes(&metadata) & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(sandbox_io_error(SandboxError::ReparsePointExposed));
        }
        let identity = ProtectedObjectIdentity::from_file(&file)
            .map_err(|_| sandbox_io_error(SandboxError::IdentityInspectionFailed))?;
        let mut dacl = std::ptr::null_mut();
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
        if result != ERROR_SUCCESS {
            return Err(win32_error(result));
        }
        let mut control = 0_u16;
        let mut revision = 0_u32;
        if unsafe { GetSecurityDescriptorControl(descriptor, &mut control, &mut revision) } == 0 {
            unsafe {
                LocalFree(descriptor);
            }
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            file,
            identity,
            is_directory: metadata.is_dir(),
            descriptor,
            dacl,
            protected: control & SE_DACL_PROTECTED != 0,
            restored: false,
        })
    }

    fn handle(&self) -> windows_sys::Win32::Foundation::HANDLE {
        use std::os::windows::io::AsRawHandle;

        self.file.as_raw_handle()
    }

    fn restore(&mut self) -> io::Result<()> {
        let identity = ProtectedObjectIdentity::from_file(&self.file)
            .map_err(|error| io::Error::other(format!("retained ACL identity inspection failed: {error}")))?;
        if identity != self.identity {
            return Err(io::Error::other("retained ACL object identity changed"));
        }
        let security_information = DACL_SECURITY_INFORMATION
            | if self.protected {
                windows_sys::Win32::Security::PROTECTED_DACL_SECURITY_INFORMATION
            } else {
                windows_sys::Win32::Security::UNPROTECTED_DACL_SECURITY_INFORMATION
            };
        let result = unsafe {
            SetSecurityInfo(
                self.handle(),
                SE_FILE_OBJECT,
                security_information,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                self.dacl,
                std::ptr::null(),
            )
        };
        if result != ERROR_SUCCESS {
            return Err(win32_error(result));
        }
        self.restored = true;
        Ok(())
    }

    fn disarm(&mut self) {
        self.restored = true;
    }
}

impl Drop for SavedDacl {
    fn drop(&mut self) {
        if !self.restored
            && let Err(error) = self.restore()
        {
            tracing::warn!(
                error_kind = ?error.kind(),
                os_error = ?error.raw_os_error(),
                "failed to restore retained Windows sandbox ACL"
            );
        }
        unsafe {
            LocalFree(self.descriptor);
        }
    }
}

fn collect_tree(root: &Path) -> io::Result<Vec<PathBuf>> {
    use std::os::windows::fs::MetadataExt;
    use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;

    let mut paths = vec![root.to_path_buf()];
    let mut cursor = 0_usize;
    while cursor < paths.len() {
        if paths.len() > MAX_ACL_OBJECTS {
            return Err(sandbox_io_error(SandboxError::IdentityBudgetExceeded));
        }
        let path = &paths[cursor];
        let metadata =
            std::fs::symlink_metadata(path).map_err(|_| sandbox_io_error(SandboxError::IdentityInspectionFailed))?;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(sandbox_io_error(SandboxError::ReparsePointExposed));
        }
        if metadata.is_dir() {
            let entries =
                std::fs::read_dir(path).map_err(|_| sandbox_io_error(SandboxError::IdentityInspectionFailed))?;
            for entry in entries {
                paths.push(
                    entry
                        .map_err(|_| sandbox_io_error(SandboxError::IdentityInspectionFailed))?
                        .path(),
                );
            }
        }
        cursor += 1;
    }
    Ok(paths)
}

fn copy_sid(sid: PSID) -> io::Result<Vec<u8>> {
    let length = unsafe { GetLengthSid(sid) };
    if length == 0 {
        return Err(io::Error::last_os_error());
    }
    let mut result = vec![0_u8; length as usize];
    if unsafe { windows_sys::Win32::Security::CopySid(length, result.as_mut_ptr().cast(), sid) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(result)
}

fn wide_nul(value: &OsStr) -> io::Result<Vec<u16>> {
    let mut wide = value.encode_wide().collect::<Vec<_>>();
    if wide.contains(&0) {
        return Err(sandbox_io_error(SandboxError::PreparationFailed));
    }
    wide.push(0);
    Ok(wide)
}

fn win32_error(code: u32) -> io::Error {
    io::Error::from_raw_os_error(code as i32)
}

fn acl_cleanup_error(error: io::Error) -> io::Error {
    if error
        .get_ref()
        .and_then(|source| source.downcast_ref::<SandboxError>())
        .is_some_and(|error| matches!(error, SandboxError::CleanupFailed { .. }))
    {
        error
    } else {
        sandbox_io_error(SandboxError::cleanup_failed(error))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AclCleanupOperation {
    Restore,
    Revoke,
}

impl fmt::Display for AclCleanupOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Restore => formatter.write_str("restore"),
            Self::Revoke => formatter.write_str("revoke"),
        }
    }
}

#[derive(Debug)]
struct AclCleanupOperationError {
    operation: AclCleanupOperation,
    source: io::Error,
}

impl fmt::Display for AclCleanupOperationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Windows ACL {} failed: {}", self.operation, self.source)
    }
}

impl Error for AclCleanupOperationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

fn finish_restore_acl(result: io::Result<()>) -> io::Result<()> {
    finish_acl_cleanup_operation(AclCleanupOperation::Restore, result)
}

fn finish_revoke_acl(result: io::Result<()>) -> io::Result<()> {
    finish_acl_cleanup_operation(AclCleanupOperation::Revoke, result)
}

fn finish_acl_cleanup_operation(operation: AclCleanupOperation, result: io::Result<()>) -> io::Result<()> {
    result.map_err(|source| {
        let kind = source.kind();
        let contextual = io::Error::new(kind, AclCleanupOperationError { operation, source });
        acl_cleanup_error(contextual)
    })
}

#[cfg(test)]
#[path = "windows_acl_test.rs"]
mod windows_acl_test;
