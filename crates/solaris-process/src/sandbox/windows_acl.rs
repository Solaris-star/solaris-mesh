use std::collections::{HashMap, HashSet};
use std::ffi::{OsStr, OsString};
use std::io;
use std::os::windows::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use windows_sys::Win32::Foundation::{ERROR_SUCCESS, GENERIC_READ, HANDLE, LocalFree};
use windows_sys::Win32::Security::Authorization::{
    ConvertSidToStringSidW, EXPLICIT_ACCESS_W, GRANT_ACCESS, GetSecurityInfo, REVOKE_ACCESS, SE_FILE_OBJECT,
    SE_WINDOW_OBJECT, SetEntriesInAclW, SetSecurityInfo, TRUSTEE_IS_SID, TRUSTEE_IS_USER, TRUSTEE_W,
};
use windows_sys::Win32::Security::Isolation::{CreateAppContainerProfile, DeleteAppContainerProfile};
use windows_sys::Win32::Security::{
    ACE_HEADER, ACL, AddAce, CONTAINER_INHERIT_ACE, DACL_SECURITY_INFORMATION, EqualSid, FreeSid, GetAce, GetLengthSid,
    GetSecurityDescriptorControl, GetSecurityDescriptorDacl, InitializeAcl, InitializeSecurityDescriptor, IsValidSid,
    OBJECT_INHERIT_ACE, PROTECTED_DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID, SE_DACL_AUTO_INHERIT_REQ,
    SE_DACL_AUTO_INHERITED, SE_DACL_DEFAULTED, SE_DACL_PROTECTED, SECURITY_DESCRIPTOR, SetFileSecurityW,
    SetSecurityDescriptorControl, SetSecurityDescriptorDacl,
};
use windows_sys::Win32::Storage::FileSystem::{
    DELETE, FILE_ATTRIBUTE_REPARSE_POINT, FILE_DELETE_CHILD, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
    FILE_GENERIC_EXECUTE, FILE_GENERIC_READ, FILE_GENERIC_WRITE, FILE_NAME_NORMALIZED, FILE_SHARE_DELETE,
    FILE_SHARE_READ, FILE_SHARE_WRITE, GetFinalPathNameByHandleW, READ_CONTROL, VOLUME_NAME_DOS, WRITE_DAC,
};
use windows_sys::Win32::System::StationsAndDesktops::{GetProcessWindowStation, GetThreadDesktop};
use windows_sys::Win32::System::Threading::GetCurrentThreadId;

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
static USER_OBJECT_ACL_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

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

pub(super) struct UserObjectAclGuard {
    handles: Vec<HANDLE>,
    sid: Vec<u8>,
}

unsafe impl Send for UserObjectAclGuard {}

impl UserObjectAclGuard {
    pub(super) fn apply(sid: PSID) -> io::Result<Self> {
        let sid = copy_sid(sid)?;
        let window_station = unsafe { GetProcessWindowStation() };
        let desktop = unsafe { GetThreadDesktop(GetCurrentThreadId()) };
        if window_station.is_null() || desktop.is_null() {
            return Err(io::Error::last_os_error());
        }
        let mut guard = Self {
            handles: Vec::with_capacity(2),
            sid,
        };
        for handle in [window_station.cast(), desktop.cast()] {
            if let Err(error) = modify_user_object_acl(handle, GENERIC_READ, GRANT_ACCESS, &mut guard.sid) {
                let _ = guard.cleanup();
                return Err(error);
            }
            guard.handles.push(handle);
        }
        Ok(guard)
    }

    pub(super) fn cleanup(&mut self) -> io::Result<()> {
        let mut first_error = None;
        while let Some(handle) = self.handles.pop() {
            if let Err(error) = modify_user_object_acl(handle, 0, REVOKE_ACCESS, &mut self.sid) {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

impl Drop for UserObjectAclGuard {
    fn drop(&mut self) {
        if let Err(error) = self.cleanup() {
            tracing::warn!(
                error_kind = ?error.kind(),
                os_error = ?error.raw_os_error(),
                "failed to restore Windows sandbox window-station/desktop ACL"
            );
        }
    }
}

fn modify_user_object_acl(
    handle: HANDLE,
    permissions: u32,
    mode: windows_sys::Win32::Security::Authorization::ACCESS_MODE,
    sid: &mut [u8],
) -> io::Result<()> {
    let _lock = USER_OBJECT_ACL_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .map_err(|_| io::Error::other("Windows user-object ACL lock is poisoned"))?;
    let mut dacl: *mut ACL = std::ptr::null_mut();
    let mut descriptor: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
    let result = unsafe {
        GetSecurityInfo(
            handle,
            SE_WINDOW_OBJECT,
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
    let access = EXPLICIT_ACCESS_W {
        grfAccessPermissions: permissions,
        grfAccessMode: mode,
        grfInheritance: 0,
        Trustee: TRUSTEE_W {
            pMultipleTrustee: std::ptr::null_mut(),
            MultipleTrusteeOperation: 0,
            TrusteeForm: TRUSTEE_IS_SID,
            TrusteeType: TRUSTEE_IS_USER,
            ptstrName: sid.as_mut_ptr().cast(),
        },
    };
    let mut updated: *mut ACL = std::ptr::null_mut();
    let result = unsafe { SetEntriesInAclW(1, &access, dacl, &mut updated) };
    if result != ERROR_SUCCESS {
        unsafe {
            LocalFree(descriptor.cast());
        }
        return Err(win32_error(result));
    }
    let result = unsafe {
        SetSecurityInfo(
            handle,
            SE_WINDOW_OBJECT,
            DACL_SECURITY_INFORMATION,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            updated,
            std::ptr::null(),
        )
    };
    unsafe {
        LocalFree(updated.cast());
        LocalFree(descriptor.cast());
    }
    if result != ERROR_SUCCESS {
        return Err(win32_error(result));
    }
    Ok(())
}

pub(super) struct AclGuard {
    leased: Vec<ProtectedObjectIdentity>,
    sid: Vec<u8>,
    writable_roots: Vec<PathBuf>,
    protected_roots: Vec<PathBuf>,
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
            writable_roots: std::iter::once(workspace.to_path_buf())
                .chain(private_roots.iter().cloned())
                .collect(),
            protected_roots: protected_roots.to_vec(),
            #[cfg(feature = "sandbox-test-fixtures")]
            fail_cleanup_attempts: 0,
        };
        let apply_result = (|| {
            for root in protected_roots {
                for path in deepest_first(collect_tree(root)?) {
                    guard.acquire(path, AclMode::Protected)?;
                }
            }
            let workspace_paths = deepest_first(collect_tree(workspace)?);
            for path in workspace_paths {
                if protected_roots.iter().any(|protected| path.starts_with(protected)) {
                    continue;
                }
                guard.acquire(path, AclMode::Writable)?;
            }
            for root in private_roots {
                for path in deepest_first(collect_tree(root)?) {
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

    fn cleanup_unleased_writable_objects(&mut self, preexisting: &HashSet<ProtectedObjectIdentity>) -> io::Result<()> {
        let mut visited = HashSet::new();
        for root in self.writable_roots.clone() {
            if !root.exists() {
                continue;
            }
            let mut paths = collect_tree(&root)?;
            paths.reverse();
            for path in paths {
                if self.protected_roots.iter().any(|protected| path.starts_with(protected)) {
                    continue;
                }
                let mut current = SavedDacl::open(&path)?;
                let identity = current.identity;
                if preexisting.contains(&identity) || !visited.insert(identity) {
                    current.disarm();
                    continue;
                }
                let result = remove_sid_from_acl_exact(&current, &mut self.sid);
                current.disarm();
                result?;
            }
        }
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
        let mut leases = acl_leases()
            .lock()
            .map_err(|_| io::Error::other("Windows ACL lease registry is poisoned"))?;
        let mut first_error = None;
        let mut final_restore = Vec::new();
        let mut index = self.leased.len();
        while index > 0 {
            index -= 1;
            let identity = self.leased[index];
            let Some(lease) = leases.get_mut(&identity) else {
                self.leased.remove(index);
                continue;
            };
            if lease.holders == 1 {
                match finish_restore_acl(lease.saved.restore()) {
                    Ok(()) => final_restore.push(identity),
                    Err(error) => {
                        first_error.get_or_insert(error);
                    }
                }
                continue;
            }
            let release_result = if lease.mode == AclMode::Writable {
                finish_revoke_acl(apply_acl_entry(
                    &mut lease.saved,
                    0,
                    REVOKE_ACCESS,
                    false,
                    &mut self.sid,
                ))
            } else {
                Ok(())
            };
            match release_result {
                Ok(()) => {
                    lease.holders -= 1;
                    self.leased.remove(index);
                }
                Err(error) => {
                    first_error.get_or_insert_with(|| acl_cleanup_error(error));
                }
            }
        }

        if first_error.is_none() {
            let preexisting: HashSet<_> = leases.keys().copied().collect();
            if let Err(error) = self.cleanup_unleased_writable_objects(&preexisting) {
                first_error.get_or_insert_with(|| acl_cleanup_error(error));
            }
        }

        for identity in &final_restore {
            if let Some(lease) = leases.get_mut(identity)
                && let Err(error) = finish_restore_acl(lease.saved.restore())
            {
                first_error.get_or_insert(error);
            }
        }

        if let Some(error) = first_error {
            Err(error)
        } else {
            for identity in final_restore {
                leases.remove(&identity);
                self.leased.retain(|leased| *leased != identity);
            }
            Ok(())
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

fn remove_sid_from_acl_exact(saved: &SavedDacl, sid: &mut [u8]) -> io::Result<()> {
    const ACCESS_ALLOWED_ACE_TYPE: u8 = 0;
    const ACCESS_ALLOWED_SID_OFFSET: usize = 8;

    let current = CurrentDacl::open(saved.handle())?;
    if current.dacl.is_null() {
        return Ok(());
    }
    let acl = unsafe { &*current.dacl };
    let acl_size = usize::from(acl.AclSize);
    let words = acl_size.div_ceil(std::mem::size_of::<u32>());
    let mut storage = vec![0_u32; words.max(1)];
    let filtered = storage.as_mut_ptr().cast::<ACL>();
    if unsafe {
        InitializeAcl(
            filtered,
            u32::try_from(storage.len() * 4).unwrap_or(u32::MAX),
            u32::from(acl.AclRevision),
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }

    let mut removed = false;
    for index in 0..u32::from(acl.AceCount) {
        let mut ace = std::ptr::null_mut();
        if unsafe { GetAce(current.dacl, index, &mut ace) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let header = unsafe { &*ace.cast::<ACE_HEADER>() };
        let ace_size = usize::from(header.AceSize);
        let remove = if header.AceType == ACCESS_ALLOWED_ACE_TYPE && ace_size > ACCESS_ALLOWED_SID_OFFSET {
            let candidate = unsafe { ace.cast::<u8>().add(ACCESS_ALLOWED_SID_OFFSET).cast() };
            let sid_len = if unsafe { IsValidSid(candidate) } != 0 {
                usize::try_from(unsafe { GetLengthSid(candidate) }).unwrap_or(usize::MAX)
            } else {
                usize::MAX
            };
            sid_len <= ace_size - ACCESS_ALLOWED_SID_OFFSET
                && unsafe { EqualSid(candidate, sid.as_mut_ptr().cast()) } != 0
        } else {
            false
        };
        if remove {
            removed = true;
            continue;
        }
        if unsafe {
            AddAce(
                filtered,
                u32::from(acl.AclRevision),
                u32::MAX,
                ace,
                u32::from(header.AceSize),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
    }
    if removed {
        saved.write_dacl_exact(filtered)?;
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
    descriptor: Vec<u8>,
    control: u16,
    restored: bool,
}

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
        if control & windows_sys::Win32::Security::SE_SELF_RELATIVE == 0 {
            unsafe {
                LocalFree(descriptor.cast());
            }
            return Err(io::Error::other(
                "GetSecurityInfo returned a non-self-relative descriptor",
            ));
        }
        let descriptor_len =
            usize::try_from(unsafe { windows_sys::Win32::Security::GetSecurityDescriptorLength(descriptor) })
                .map_err(|_| io::Error::other("security descriptor is too large"))?;
        let descriptor_bytes = unsafe { std::slice::from_raw_parts(descriptor.cast::<u8>(), descriptor_len) }.to_vec();
        unsafe {
            LocalFree(descriptor.cast());
        }
        Ok(Self {
            file,
            identity,
            is_directory: metadata.is_dir(),
            descriptor: descriptor_bytes,
            control,
            restored: false,
        })
    }

    fn handle(&self) -> windows_sys::Win32::Foundation::HANDLE {
        use std::os::windows::io::AsRawHandle;

        self.file.as_raw_handle()
    }

    fn current_path(&self) -> io::Result<PathBuf> {
        let flags = FILE_NAME_NORMALIZED | VOLUME_NAME_DOS;
        let required = unsafe { GetFinalPathNameByHandleW(self.handle(), std::ptr::null_mut(), 0, flags) };
        if required == 0 {
            return Err(io::Error::last_os_error());
        }
        let capacity = usize::try_from(required)
            .map_err(|_| io::Error::other("retained ACL path is too large"))?
            .saturating_add(1);
        let mut buffer = vec![0_u16; capacity];
        let written = unsafe {
            GetFinalPathNameByHandleW(
                self.handle(),
                buffer.as_mut_ptr(),
                u32::try_from(buffer.len()).map_err(|_| io::Error::other("retained ACL path is too large"))?,
                flags,
            )
        };
        if written == 0
            || usize::try_from(written)
                .ok()
                .is_none_or(|written| written >= buffer.len())
        {
            return Err(io::Error::last_os_error());
        }
        Ok(PathBuf::from(OsString::from_wide(
            &buffer[..usize::try_from(written).expect("written path length was validated")],
        )))
    }

    fn write_dacl_exact(&self, dacl: *mut ACL) -> io::Result<()> {
        let mut descriptor = SECURITY_DESCRIPTOR::default();
        let descriptor_ptr = (&mut descriptor as *mut SECURITY_DESCRIPTOR).cast();
        if unsafe { InitializeSecurityDescriptor(descriptor_ptr, 1) } == 0 {
            return Err(io::Error::last_os_error());
        }
        if unsafe {
            SetSecurityDescriptorDacl(
                descriptor_ptr,
                1,
                dacl,
                i32::from(self.control & SE_DACL_DEFAULTED != 0),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        let inheritance_bits = SE_DACL_AUTO_INHERIT_REQ | SE_DACL_AUTO_INHERITED | SE_DACL_PROTECTED;
        if unsafe { SetSecurityDescriptorControl(descriptor_ptr, inheritance_bits, self.control & inheritance_bits) }
            == 0
        {
            return Err(io::Error::last_os_error());
        }
        let path = self.current_path()?;
        let restore_lock = open_restore_lock(&path)?;
        let restore_identity = ProtectedObjectIdentity::from_file(&restore_lock)
            .map_err(|error| io::Error::other(format!("exact DACL identity inspection failed: {error}")))?;
        if restore_identity != self.identity {
            return Err(io::Error::other(
                "retained DACL path identity changed before exact update",
            ));
        }
        let path = wide_nul(path.as_os_str())?;
        if unsafe { SetFileSecurityW(path.as_ptr(), dacl_security_information(self.control), descriptor_ptr) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn restore(&mut self) -> io::Result<()> {
        let identity = ProtectedObjectIdentity::from_file(&self.file)
            .map_err(|error| io::Error::other(format!("retained ACL identity inspection failed: {error}")))?;
        if identity != self.identity {
            return Err(io::Error::other("retained ACL object identity changed"));
        }
        let mut descriptor = self.descriptor.clone();
        if self.control & SE_DACL_AUTO_INHERITED != 0 {
            // SetFileSecurityW can clear SE_DACL_AUTO_INHERITED on an object
            // that was renamed while the sandbox held it open. Restore those
            // descriptors through the retained object handle so the inherited
            // control state is preserved on the original object identity.
            let mut dacl_present = 0;
            let mut dacl = std::ptr::null_mut();
            let mut dacl_defaulted = 0;
            if unsafe {
                GetSecurityDescriptorDacl(
                    descriptor.as_mut_ptr().cast(),
                    &mut dacl_present,
                    &mut dacl,
                    &mut dacl_defaulted,
                )
            } == 0
            {
                return Err(io::Error::last_os_error());
            }
            if dacl_present == 0 {
                return Err(io::Error::other(
                    "saved Windows sandbox security descriptor has no DACL",
                ));
            }
            let result = unsafe {
                SetSecurityInfo(
                    self.handle(),
                    SE_FILE_OBJECT,
                    dacl_security_information(self.control),
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    dacl,
                    std::ptr::null(),
                )
            };
            if result != ERROR_SUCCESS {
                return Err(win32_error(result));
            }
        } else {
            // Supplying an unprotected DACL through SetSecurityInfo causes
            // Windows to recompute inheritance and can add SE_DACL_AUTO_INHERITED
            // even when the original descriptor did not have it. Apply the
            // saved self-relative descriptor by the retained object's current
            // name instead, while a non-delete-sharing identity lock prevents
            // path replacement during the update.
            let path = self.current_path()?;
            let restore_lock = open_restore_lock(&path)?;
            let restore_identity = ProtectedObjectIdentity::from_file(&restore_lock)
                .map_err(|error| io::Error::other(format!("restore ACL identity inspection failed: {error}")))?;
            if restore_identity != self.identity {
                return Err(io::Error::other("retained ACL path identity changed before restore"));
            }
            let path = wide_nul(path.as_os_str())?;
            if unsafe {
                SetFileSecurityW(
                    path.as_ptr(),
                    dacl_security_information(self.control),
                    descriptor.as_mut_ptr().cast(),
                )
            } == 0
            {
                return Err(io::Error::last_os_error());
            }
        }
        self.restored = true;
        Ok(())
    }

    fn disarm(&mut self) {
        self.restored = true;
    }
}

fn dacl_security_information(control: u16) -> u32 {
    let mut information = DACL_SECURITY_INFORMATION;
    if control & SE_DACL_PROTECTED != 0 {
        information |= PROTECTED_DACL_SECURITY_INFORMATION;
    } else if control & SE_DACL_AUTO_INHERITED != 0 {
        information |= windows_sys::Win32::Security::UNPROTECTED_DACL_SECURITY_INFORMATION;
    }
    information
}

fn open_restore_lock(path: &Path) -> io::Result<std::fs::File> {
    use std::os::windows::fs::OpenOptionsExt;

    let mut options = std::fs::OpenOptions::new();
    options
        .access_mode(READ_CONTROL | WRITE_DAC)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS);
    options.open(path)
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
    }
}

fn deepest_first(mut paths: Vec<PathBuf>) -> Vec<PathBuf> {
    paths.sort_by(|left, right| {
        right
            .components()
            .count()
            .cmp(&left.components().count())
            .then_with(|| right.cmp(left))
    });
    paths
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AclCleanupOperation {
    Restore,
    Revoke,
}

#[derive(Debug)]
struct AclCleanupOperationError {
    operation: AclCleanupOperation,
    source: io::Error,
}

impl std::fmt::Display for AclCleanupOperationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let operation = match self.operation {
            AclCleanupOperation::Restore => "restore",
            AclCleanupOperation::Revoke => "revoke",
        };
        write!(formatter, "Windows sandbox ACL {operation} failed: {}", self.source)
    }
}

impl std::error::Error for AclCleanupOperationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
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
        acl_cleanup_error(io::Error::new(kind, AclCleanupOperationError { operation, source }))
    })
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

#[cfg(test)]
#[path = "windows_acl_test.rs"]
mod windows_acl_test;
