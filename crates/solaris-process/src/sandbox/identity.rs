use std::collections::{HashMap, HashSet};
use std::io;
#[cfg(any(target_os = "linux", target_os = "macos", windows))]
use std::path::{Path, PathBuf};

use super::{SandboxError, sandbox_io_error};
use crate::ProtectedObjectIdentity;

#[cfg(any(target_os = "linux", target_os = "macos", windows))]
const MAX_SCANNED_OBJECTS: usize = 262_144;

pub(super) type FileIdentity = ProtectedObjectIdentity;

pub(super) fn validate_workspace_object_links(
    protected: impl IntoIterator<Item = FileIdentity>,
    workspace: impl IntoIterator<Item = (FileIdentity, u64)>,
) -> io::Result<()> {
    let protected = protected.into_iter().collect::<HashSet<_>>();
    let mut workspace_links = HashMap::<FileIdentity, (u64, u64)>::new();
    for (identity, link_count) in workspace {
        if protected.contains(&identity) {
            return Err(sandbox_io_error(SandboxError::ProtectedObjectAlias));
        }
        if link_count <= 1 {
            continue;
        }
        let entry = workspace_links.entry(identity).or_insert((0, link_count));
        if entry.1 != link_count {
            return Err(sandbox_io_error(SandboxError::IdentityInspectionFailed));
        }
        entry.0 = entry
            .0
            .checked_add(1)
            .ok_or_else(|| sandbox_io_error(SandboxError::IdentityBudgetExceeded))?;
    }
    if workspace_links
        .values()
        .any(|(workspace_names, link_count)| workspace_names != link_count)
    {
        return Err(sandbox_io_error(SandboxError::ExternalHardlink));
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos", windows))]
pub(super) struct ProtectedIdentitySnapshot {
    identities: HashSet<FileIdentity>,
}

#[cfg(any(target_os = "linux", target_os = "macos", windows))]
impl ProtectedIdentitySnapshot {
    pub(super) fn capture(
        protected_roots: &[PathBuf],
        retained_identities: &[ProtectedObjectIdentity],
    ) -> io::Result<Self> {
        if retained_identities.len() > MAX_SCANNED_OBJECTS {
            return Err(sandbox_io_error(SandboxError::IdentityBudgetExceeded));
        }
        let mut identities = HashSet::new();
        identities
            .try_reserve(retained_identities.len())
            .map_err(|_| sandbox_io_error(SandboxError::IdentityBudgetExceeded))?;
        identities.extend(retained_identities.iter().copied());
        scan_roots(protected_roots, &[], ScanPurpose::Protected, |metadata| {
            if metadata.links > 1 {
                reserve_identity(&mut identities)?;
                identities.insert(metadata.identity);
            }
            Ok(())
        })?;
        Ok(Self { identities })
    }

    pub(super) fn verify_workspace(&mut self, protected_roots: &[PathBuf], workspace_root: &Path) -> io::Result<()> {
        scan_roots(protected_roots, &[], ScanPurpose::Protected, |metadata| {
            if metadata.links > 1 {
                reserve_identity(&mut self.identities)?;
                self.identities.insert(metadata.identity);
            }
            Ok(())
        })?;
        let mut workspace_objects = Vec::new();
        scan_roots(
            std::slice::from_ref(&workspace_root.to_path_buf()),
            protected_roots,
            ScanPurpose::Workspace,
            |metadata| {
                if workspace_objects.len() == workspace_objects.capacity() {
                    workspace_objects
                        .try_reserve(1024)
                        .map_err(|_| sandbox_io_error(SandboxError::IdentityBudgetExceeded))?;
                }
                workspace_objects.push((metadata.identity, metadata.links));
                Ok(())
            },
        )?;
        validate_workspace_object_links(self.identities.iter().copied(), workspace_objects)
    }

    #[cfg(target_os = "linux")]
    pub(super) fn verify_linux_workspace(
        &mut self,
        protected_roots: &[PathBuf],
        workspace_root: &Path,
        workspace_directory: std::fs::File,
    ) -> io::Result<()> {
        self.verify_retained_unix_workspace(protected_roots, workspace_root, workspace_directory)
    }

    #[cfg(target_os = "macos")]
    pub(super) fn verify_macos_workspace(
        &mut self,
        protected_roots: &[PathBuf],
        workspace_root: &Path,
        workspace_directory: std::fs::File,
    ) -> io::Result<()> {
        self.verify_retained_unix_workspace(protected_roots, workspace_root, workspace_directory)
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn verify_retained_unix_workspace(
        &mut self,
        protected_roots: &[PathBuf],
        workspace_root: &Path,
        workspace_directory: std::fs::File,
    ) -> io::Result<()> {
        scan_roots(protected_roots, &[], ScanPurpose::Protected, |metadata| {
            if metadata.links > 1 {
                reserve_identity(&mut self.identities)?;
                self.identities.insert(metadata.identity);
            }
            Ok(())
        })?;
        let excluded_roots = protected_roots
            .iter()
            .filter_map(|protected| protected.strip_prefix(workspace_root).ok())
            .map(Path::to_path_buf)
            .collect::<Vec<_>>();
        // The path is used only to translate protected destinations. The
        // workspace traversal itself starts exclusively from the retained fd.
        let mut workspace_objects = Vec::new();
        scan_retained_unix_directory(workspace_directory, &excluded_roots, |metadata| {
            if workspace_objects.len() == workspace_objects.capacity() {
                workspace_objects
                    .try_reserve(1024)
                    .map_err(|_| sandbox_io_error(SandboxError::IdentityBudgetExceeded))?;
            }
            workspace_objects.push((metadata.identity, metadata.links));
            Ok(())
        })?;
        validate_workspace_object_links(self.identities.iter().copied(), workspace_objects)
    }
}

#[cfg(any(target_os = "linux", target_os = "macos", windows))]
fn reserve_identity(identities: &mut HashSet<FileIdentity>) -> io::Result<()> {
    if identities.len() >= MAX_SCANNED_OBJECTS {
        return Err(sandbox_io_error(SandboxError::IdentityBudgetExceeded));
    }
    if identities.len() == identities.capacity() {
        identities
            .try_reserve(1024)
            .map_err(|_| sandbox_io_error(SandboxError::IdentityBudgetExceeded))?;
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos", windows))]
#[derive(Clone, Copy)]
enum ScanPurpose {
    Protected,
    Workspace,
}

#[cfg(any(target_os = "linux", target_os = "macos", windows))]
#[derive(Clone, Copy)]
struct OpenObjectMetadata {
    identity: FileIdentity,
    links: u64,
}

#[cfg(target_os = "linux")]
fn scan_roots(
    roots: &[PathBuf],
    excluded_roots: &[PathBuf],
    purpose: ScanPurpose,
    mut visit_file: impl FnMut(OpenObjectMetadata) -> io::Result<()>,
) -> io::Result<()> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt};

    let mut pending = Vec::new();
    pending
        .try_reserve(roots.len())
        .map_err(|_| sandbox_io_error(SandboxError::IdentityBudgetExceeded))?;
    pending.extend(roots.iter().cloned());
    let mut scanned = 0_usize;
    while let Some(path) = pending.pop() {
        if excluded_roots.iter().any(|excluded| path.starts_with(excluded)) {
            continue;
        }
        scanned = scanned
            .checked_add(1)
            .filter(|count| *count <= MAX_SCANNED_OBJECTS)
            .ok_or_else(|| sandbox_io_error(SandboxError::IdentityBudgetExceeded))?;
        let mut options = std::fs::OpenOptions::new();
        options
            .read(true)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_PATH);
        let file = match options.open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(_) => return Err(sandbox_io_error(SandboxError::IdentityInspectionFailed)),
        };
        let metadata = file
            .metadata()
            .map_err(|_| sandbox_io_error(SandboxError::IdentityInspectionFailed))?;
        let file_type = metadata.file_type();
        if file_type.is_symlink() {
            continue;
        }
        if file_type.is_socket() || file_type.is_fifo() {
            if matches!(purpose, ScanPurpose::Workspace) {
                return Err(sandbox_io_error(SandboxError::HostSocketExposed));
            }
            continue;
        }
        if metadata.is_file() {
            visit_file(OpenObjectMetadata {
                identity: ProtectedObjectIdentity::from_unix_parts(metadata.dev(), metadata.ino()),
                links: metadata.nlink(),
            })?;
            continue;
        }
        if !metadata.is_dir() {
            continue;
        }
        let entries = std::fs::read_dir(&path).map_err(|_| sandbox_io_error(SandboxError::IdentityInspectionFailed))?;
        for entry in entries {
            let entry = entry.map_err(|_| sandbox_io_error(SandboxError::IdentityInspectionFailed))?;
            if pending.len() >= MAX_SCANNED_OBJECTS {
                return Err(sandbox_io_error(SandboxError::IdentityBudgetExceeded));
            }
            pending
                .try_reserve(1)
                .map_err(|_| sandbox_io_error(SandboxError::IdentityBudgetExceeded))?;
            pending.push(entry.path());
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "identity_test.rs"]
mod identity_test;

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn scan_retained_unix_directory(
    root: std::fs::File,
    excluded_roots: &[PathBuf],
    mut visit_file: impl FnMut(OpenObjectMetadata) -> io::Result<()>,
) -> io::Result<()> {
    use cap_std::fs::{Dir, MetadataExt};
    use std::os::unix::fs::FileTypeExt;

    let mut pending = vec![(Dir::from_std_file(root), PathBuf::new())];
    let mut scanned = 0_usize;
    while let Some((directory, relative)) = pending.pop() {
        scanned = checked_scan_count(scanned)?;
        let entries = directory
            .entries()
            .map_err(|_| sandbox_io_error(SandboxError::IdentityInspectionFailed))?;
        for entry in entries {
            let entry = entry.map_err(|_| sandbox_io_error(SandboxError::IdentityInspectionFailed))?;
            scanned = checked_scan_count(scanned)?;
            let entry_relative = relative.join(entry.file_name());
            if excluded_roots
                .iter()
                .any(|excluded| entry_relative.starts_with(excluded))
            {
                continue;
            }
            let metadata = entry
                .metadata()
                .map_err(|_| sandbox_io_error(SandboxError::IdentityInspectionFailed))?;
            let file_type = metadata.file_type();
            if file_type.is_symlink() {
                continue;
            }
            if file_type.is_socket() || file_type.is_fifo() {
                return Err(sandbox_io_error(SandboxError::HostSocketExposed));
            }
            if metadata.is_file() {
                visit_file(OpenObjectMetadata {
                    identity: ProtectedObjectIdentity::from_unix_parts(metadata.dev(), metadata.ino()),
                    links: metadata.nlink(),
                })?;
                continue;
            }
            if !metadata.is_dir() {
                continue;
            }
            if pending.len() >= MAX_SCANNED_OBJECTS {
                return Err(sandbox_io_error(SandboxError::IdentityBudgetExceeded));
            }
            let child = entry
                .open_dir()
                .map_err(|_| sandbox_io_error(SandboxError::IdentityInspectionFailed))?;
            pending
                .try_reserve(1)
                .map_err(|_| sandbox_io_error(SandboxError::IdentityBudgetExceeded))?;
            pending.push((child, entry_relative));
        }
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn checked_scan_count(scanned: usize) -> io::Result<usize> {
    scanned
        .checked_add(1)
        .filter(|count| *count <= MAX_SCANNED_OBJECTS)
        .ok_or_else(|| sandbox_io_error(SandboxError::IdentityBudgetExceeded))
}

#[cfg(target_os = "macos")]
fn scan_roots(
    roots: &[PathBuf],
    excluded_roots: &[PathBuf],
    purpose: ScanPurpose,
    mut visit_file: impl FnMut(OpenObjectMetadata) -> io::Result<()>,
) -> io::Result<()> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt};

    let mut pending = Vec::new();
    pending
        .try_reserve(roots.len())
        .map_err(|_| sandbox_io_error(SandboxError::IdentityBudgetExceeded))?;
    pending.extend(roots.iter().cloned());
    let mut scanned = 0_usize;
    while let Some(path) = pending.pop() {
        if excluded_roots.iter().any(|excluded| path.starts_with(excluded)) {
            continue;
        }
        scanned = scanned
            .checked_add(1)
            .filter(|count| *count <= MAX_SCANNED_OBJECTS)
            .ok_or_else(|| sandbox_io_error(SandboxError::IdentityBudgetExceeded))?;
        let link_metadata = match std::fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(_) => return Err(sandbox_io_error(SandboxError::IdentityInspectionFailed)),
        };
        let link_type = link_metadata.file_type();
        if link_type.is_symlink() {
            continue;
        }
        if link_type.is_socket() || link_type.is_fifo() {
            if matches!(purpose, ScanPurpose::Workspace) {
                return Err(sandbox_io_error(SandboxError::HostSocketExposed));
            }
            continue;
        }
        let mut options = std::fs::OpenOptions::new();
        options
            .read(true)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
        let file = match options.open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(_) => return Err(sandbox_io_error(SandboxError::IdentityInspectionFailed)),
        };
        let metadata = file
            .metadata()
            .map_err(|_| sandbox_io_error(SandboxError::IdentityInspectionFailed))?;
        if metadata.dev() != link_metadata.dev() || metadata.ino() != link_metadata.ino() {
            return Err(sandbox_io_error(SandboxError::IdentityInspectionFailed));
        }
        if metadata.is_file() {
            visit_file(OpenObjectMetadata {
                identity: ProtectedObjectIdentity::from_unix_parts(metadata.dev(), metadata.ino()),
                links: metadata.nlink(),
            })?;
            continue;
        }
        if !metadata.is_dir() {
            continue;
        }
        let entries = std::fs::read_dir(&path).map_err(|_| sandbox_io_error(SandboxError::IdentityInspectionFailed))?;
        for entry in entries {
            let entry = entry.map_err(|_| sandbox_io_error(SandboxError::IdentityInspectionFailed))?;
            if pending.len() >= MAX_SCANNED_OBJECTS {
                return Err(sandbox_io_error(SandboxError::IdentityBudgetExceeded));
            }
            pending
                .try_reserve(1)
                .map_err(|_| sandbox_io_error(SandboxError::IdentityBudgetExceeded))?;
            pending.push(entry.path());
        }
    }
    Ok(())
}

#[cfg(windows)]
fn scan_roots(
    roots: &[PathBuf],
    excluded_roots: &[PathBuf],
    purpose: ScanPurpose,
    mut visit_file: impl FnMut(OpenObjectMetadata) -> io::Result<()>,
) -> io::Result<()> {
    use std::os::windows::fs::{MetadataExt, OpenOptionsExt};
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS,
        FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, GetFileInformationByHandle,
    };

    let mut pending = Vec::new();
    pending
        .try_reserve(roots.len())
        .map_err(|_| sandbox_io_error(SandboxError::IdentityBudgetExceeded))?;
    pending.extend(roots.iter().cloned());
    let mut scanned = 0_usize;
    while let Some(path) = pending.pop() {
        if excluded_roots.iter().any(|excluded| path.starts_with(excluded)) {
            continue;
        }
        scanned = scanned
            .checked_add(1)
            .filter(|count| *count <= MAX_SCANNED_OBJECTS)
            .ok_or_else(|| sandbox_io_error(SandboxError::IdentityBudgetExceeded))?;
        let mut options = std::fs::OpenOptions::new();
        options
            .read(true)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS);
        let file = match options.open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(_) => return Err(sandbox_io_error(SandboxError::IdentityInspectionFailed)),
        };
        let metadata = file
            .metadata()
            .map_err(|_| sandbox_io_error(SandboxError::IdentityInspectionFailed))?;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            let error = match purpose {
                ScanPurpose::Workspace => SandboxError::ReparsePointExposed,
                ScanPurpose::Protected => SandboxError::IdentityInspectionFailed,
            };
            return Err(sandbox_io_error(error));
        }
        if metadata.is_file() {
            use std::os::windows::io::AsRawHandle;

            let mut information = BY_HANDLE_FILE_INFORMATION::default();
            if unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut information) } == 0 {
                return Err(sandbox_io_error(SandboxError::IdentityInspectionFailed));
            }
            visit_file(OpenObjectMetadata {
                identity: ProtectedObjectIdentity::from_file(&file)
                    .map_err(|_| sandbox_io_error(SandboxError::IdentityInspectionFailed))?,
                links: u64::from(information.nNumberOfLinks),
            })?;
            continue;
        }
        if !metadata.is_dir() {
            continue;
        }
        let entries = std::fs::read_dir(&path).map_err(|_| sandbox_io_error(SandboxError::IdentityInspectionFailed))?;
        for entry in entries {
            let entry = entry.map_err(|_| sandbox_io_error(SandboxError::IdentityInspectionFailed))?;
            if pending.len() >= MAX_SCANNED_OBJECTS {
                return Err(sandbox_io_error(SandboxError::IdentityBudgetExceeded));
            }
            pending
                .try_reserve(1)
                .map_err(|_| sandbox_io_error(SandboxError::IdentityBudgetExceeded))?;
            pending.push(entry.path());
        }
    }
    Ok(())
}
