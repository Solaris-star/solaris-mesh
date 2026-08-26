use std::fmt;
use std::fs::File;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use crate::ProtectedObjectIdentity;

/// A retained operating-system authority for one configured workspace root.
///
/// The authority is captured when the permission boundary is installed. A
/// later process authorization can seal it into a launch capability without
/// trusting the workspace path again.
#[derive(Clone)]
pub struct WorkspaceRootAuthority {
    inner: Arc<WorkspaceRootAuthorityInner>,
}

/// An object-bound workspace root consumed by strict process launch policy.
///
/// Keeping this value alive also keeps the underlying directory handles alive.
/// On Windows those handles intentionally omit `FILE_SHARE_DELETE`, preventing
/// any component of the configured root from being renamed during launch and
/// for the lifetime of the managed child.
#[derive(Clone)]
pub struct WorkspaceRootLaunchCapability {
    inner: Arc<WorkspaceRootLaunchCapabilityInner>,
}

struct WorkspaceRootAuthorityInner {
    path: PathBuf,
    identity: ProtectedObjectIdentity,
    handles: PlatformHandles,
}

struct WorkspaceRootLaunchCapabilityInner {
    path: PathBuf,
    launch_path: PathBuf,
    identity: ProtectedObjectIdentity,
    handles: PlatformHandles,
}

#[cfg(unix)]
struct PlatformHandles {
    directory: File,
}

#[cfg(windows)]
struct PlatformHandles {
    // Retain every directory from the volume root through the workspace. A
    // single final-directory handle would not stop an ancestor replacement.
    _directories: Vec<File>,
}

#[cfg(not(any(unix, windows)))]
struct PlatformHandles;

impl WorkspaceRootAuthority {
    /// Capture a workspace directory as an operating-system object.
    pub fn capture(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = normalize_absolute(path.as_ref())?;
        let (identity, handles) = capture_platform(&path, false)?;
        #[cfg(windows)]
        let handles = {
            // Windows MoveFileEx can reject a directory rename even when an
            // identity-only handle was opened with FILE_SHARE_DELETE. The
            // long-lived authority therefore retains the FILE_ID_INFO value,
            // while the short-lived launch capability owns the no-delete-share
            // component guards required by the final launch boundary.
            drop(handles);
            PlatformHandles {
                _directories: Vec::new(),
            }
        };
        Ok(Self {
            inner: Arc::new(WorkspaceRootAuthorityInner {
                path,
                identity,
                handles,
            }),
        })
    }

    /// Verify that `path` still names the configured directory, then create a
    /// capability which keeps the original object alive through process launch.
    pub fn seal(&self, path: impl AsRef<Path>) -> io::Result<WorkspaceRootLaunchCapability> {
        let requested = normalize_absolute(path.as_ref())?;
        if requested != self.inner.path {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "workspace root does not match the captured authority",
            ));
        }
        let handles = seal_platform(&self.inner)?;
        let launch_path = platform_launch_path(&handles, &self.inner.path)?;
        Ok(WorkspaceRootLaunchCapability {
            inner: Arc::new(WorkspaceRootLaunchCapabilityInner {
                path: self.inner.path.clone(),
                launch_path,
                identity: self.inner.identity,
                handles,
            }),
        })
    }

    /// Recheck the configured path against the retained object identity.
    pub fn validates_current_path(&self) -> bool {
        current_platform_identity(&self.inner.path).is_ok_and(|identity| identity == self.inner.identity)
    }

    pub(crate) fn identity(&self) -> ProtectedObjectIdentity {
        self.inner.identity
    }
}

impl WorkspaceRootLaunchCapability {
    pub(crate) fn path(&self) -> &Path {
        &self.inner.path
    }

    pub(crate) fn identity(&self) -> ProtectedObjectIdentity {
        self.inner.identity
    }

    pub(crate) fn launch_path(&self) -> &Path {
        &self.inner.launch_path
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn duplicate_linux_directory(&self) -> io::Result<File> {
        self.inner.handles.directory.try_clone()
    }

    #[cfg(target_os = "macos")]
    pub(crate) fn duplicate_macos_directory(&self) -> io::Result<File> {
        self.inner.handles.directory.try_clone()
    }

    #[cfg(target_os = "macos")]
    pub(crate) fn verify_macos_path_identity(&self) -> io::Result<()> {
        let current = current_platform_identity(&self.inner.path)?;
        if current == self.inner.identity {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "workspace root identity changed before process launch",
            ))
        }
    }

    #[cfg(all(test, target_os = "linux"))]
    pub(crate) fn linux_test_placeholder(path: PathBuf) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let file = File::open(directory.path()).unwrap();
        let identity = ProtectedObjectIdentity::from_file(&file).unwrap();
        Self {
            inner: Arc::new(WorkspaceRootLaunchCapabilityInner {
                launch_path: path.clone(),
                path,
                identity,
                handles: PlatformHandles { directory: file },
            }),
        }
    }
}

impl fmt::Debug for WorkspaceRootAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkspaceRootAuthority")
            .field("path", &self.inner.path)
            .field("identity", &self.inner.identity)
            .field(
                "retained_directory_count",
                &self.inner.handles.retained_directory_count(),
            )
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for WorkspaceRootLaunchCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkspaceRootLaunchCapability")
            .field("path", &self.inner.path)
            .field("launch_path", &self.inner.launch_path)
            .field("identity", &self.inner.identity)
            .field(
                "retained_directory_count",
                &self.inner.handles.retained_directory_count(),
            )
            .finish_non_exhaustive()
    }
}

impl PartialEq for WorkspaceRootAuthority {
    fn eq(&self, other: &Self) -> bool {
        self.inner.path == other.inner.path && self.inner.identity == other.inner.identity
    }
}

impl Eq for WorkspaceRootAuthority {}

impl PartialEq for WorkspaceRootLaunchCapability {
    fn eq(&self, other: &Self) -> bool {
        self.inner.path == other.inner.path && self.inner.identity == other.inner.identity
    }
}

impl Eq for WorkspaceRootLaunchCapability {}

impl PlatformHandles {
    fn retained_directory_count(&self) -> usize {
        #[cfg(unix)]
        {
            1
        }
        #[cfg(windows)]
        {
            self._directories.len()
        }
        #[cfg(not(any(unix, windows)))]
        {
            0
        }
    }
}

#[cfg(unix)]
fn capture_platform(path: &Path, _guard_renames: bool) -> io::Result<(ProtectedObjectIdentity, PlatformHandles)> {
    use std::ffi::CString;
    use std::os::fd::FromRawFd;
    use std::os::unix::ffi::OsStrExt;

    let path_bytes = path.as_os_str().as_bytes();
    let path = CString::new(path_bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "workspace path contains NUL"))?;
    let descriptor = unsafe {
        libc::open(
            path.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if descriptor == -1 {
        return Err(io::Error::last_os_error());
    }
    let directory = unsafe { File::from_raw_fd(descriptor) };
    let metadata = directory.metadata()?;
    if !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "workspace root is not a directory",
        ));
    }
    let identity = ProtectedObjectIdentity::from_file(&directory)?;
    Ok((identity, PlatformHandles { directory }))
}

#[cfg(windows)]
fn capture_platform(path: &Path, guard_renames: bool) -> io::Result<(ProtectedObjectIdentity, PlatformHandles)> {
    let component_paths = windows_component_paths(path)?;
    let mut directories = Vec::with_capacity(component_paths.len());
    for component_path in component_paths {
        directories.push(open_windows_directory(&component_path, guard_renames)?);
    }
    let directory = directories
        .last()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "workspace root has no volume root"))?;
    let identity = ProtectedObjectIdentity::from_file(directory)?;
    Ok((
        identity,
        PlatformHandles {
            _directories: directories,
        },
    ))
}

#[cfg(not(any(unix, windows)))]
fn capture_platform(_path: &Path, _guard_renames: bool) -> io::Result<(ProtectedObjectIdentity, PlatformHandles)> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "workspace object authorities are unavailable on this platform",
    ))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn seal_platform(inner: &WorkspaceRootAuthorityInner) -> io::Result<PlatformHandles> {
    let (identity, handles) = capture_platform(&inner.path, true)?;
    if identity == inner.identity {
        Ok(handles)
    } else {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "workspace root identity changed before process launch",
        ))
    }
}

#[cfg(windows)]
fn seal_platform(inner: &WorkspaceRootAuthorityInner) -> io::Result<PlatformHandles> {
    let (identity, handles) = capture_platform(&inner.path, true)?;
    if identity == inner.identity {
        Ok(handles)
    } else {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "workspace root identity changed before process launch",
        ))
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
fn seal_platform(_inner: &WorkspaceRootAuthorityInner) -> io::Result<PlatformHandles> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "this sandbox backend cannot bind an opened workspace directory",
    ))
}

#[cfg(unix)]
fn current_platform_identity(path: &Path) -> io::Result<ProtectedObjectIdentity> {
    let (_, handles) = capture_platform(path, false)?;
    ProtectedObjectIdentity::from_file(&handles.directory)
}

#[cfg(windows)]
fn current_platform_identity(path: &Path) -> io::Result<ProtectedObjectIdentity> {
    let (_, handles) = capture_platform(path, false)?;
    ProtectedObjectIdentity::from_file(
        handles
            ._directories
            .last()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "workspace root has no directory handle"))?,
    )
}

#[cfg(not(any(unix, windows)))]
fn current_platform_identity(_path: &Path) -> io::Result<ProtectedObjectIdentity> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "workspace object identities are unavailable on this platform",
    ))
}

#[cfg(all(not(windows), not(target_os = "macos")))]
fn platform_launch_path(_handles: &PlatformHandles, path: &Path) -> io::Result<PathBuf> {
    Ok(path.to_path_buf())
}

#[cfg(target_os = "macos")]
fn platform_launch_path(handles: &PlatformHandles, _path: &Path) -> io::Result<PathBuf> {
    let path = crate::runner::final_path_from_open_file(&handles.directory)?;
    if path.is_absolute() {
        Ok(path)
    } else {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "workspace directory handle returned a non-absolute path",
        ))
    }
}

#[cfg(windows)]
fn platform_launch_path(handles: &PlatformHandles, _path: &Path) -> io::Result<PathBuf> {
    use std::os::windows::ffi::OsStringExt;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{FILE_NAME_NORMALIZED, GetFinalPathNameByHandleW, VOLUME_NAME_DOS};

    let directory = handles
        ._directories
        .last()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "workspace root has no directory handle"))?;
    let handle = directory.as_raw_handle();
    let required =
        unsafe { GetFinalPathNameByHandleW(handle, std::ptr::null_mut(), 0, FILE_NAME_NORMALIZED | VOLUME_NAME_DOS) };
    if required == 0 {
        return Err(io::Error::last_os_error());
    }
    let mut buffer = vec![0_u16; required as usize + 1];
    let written = unsafe {
        GetFinalPathNameByHandleW(
            handle,
            buffer.as_mut_ptr(),
            u32::try_from(buffer.len())
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "workspace path is too long"))?,
            FILE_NAME_NORMALIZED | VOLUME_NAME_DOS,
        )
    };
    if written == 0 || written as usize >= buffer.len() {
        return Err(io::Error::last_os_error());
    }
    buffer.truncate(written as usize);
    Ok(PathBuf::from(std::ffi::OsString::from_wide(&buffer)))
}

#[cfg(windows)]
fn windows_component_paths(path: &Path) -> io::Result<Vec<PathBuf>> {
    let mut current = PathBuf::new();
    let mut paths = Vec::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => current.push(prefix.as_os_str()),
            Component::RootDir => {
                current.push(component.as_os_str());
                paths.push(current.clone());
            }
            Component::Normal(value) => {
                current.push(value);
                paths.push(current.clone());
            }
            Component::CurDir | Component::ParentDir => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "workspace path is not normalized",
                ));
            }
        }
    }
    Ok(paths)
}

#[cfg(windows)]
fn open_windows_directory(path: &Path, guard_renames: bool) -> io::Result<File> {
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::fs::MetadataExt;
    use std::os::windows::io::FromRawHandle;
    use windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE;
    use windows_sys::Win32::Storage::FileSystem::{
        CreateFileW, FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
        FILE_LIST_DIRECTORY, FILE_READ_ATTRIBUTES, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING,
    };

    let mut wide_path = path.as_os_str().encode_wide().collect::<Vec<_>>();
    if wide_path.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "workspace path contains NUL",
        ));
    }
    wide_path.push(0);
    let share_mode = FILE_SHARE_READ | FILE_SHARE_WRITE | if guard_renames { 0 } else { FILE_SHARE_DELETE };
    let desired_access = if guard_renames {
        FILE_LIST_DIRECTORY | FILE_READ_ATTRIBUTES
    } else {
        0
    };
    let handle = unsafe {
        CreateFileW(
            wide_path.as_ptr(),
            desired_access,
            share_mode,
            std::ptr::null(),
            OPEN_EXISTING,
            FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS,
            std::ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    let directory = unsafe { File::from_raw_handle(handle) };
    let metadata = directory.metadata()?;
    if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "workspace path contains a Windows reparse point",
        ));
    }
    if !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "workspace path component is not a directory",
        ));
    }
    Ok(directory)
}

fn normalize_absolute(path: &Path) -> io::Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "workspace path escapes its filesystem root",
                    ));
                }
            }
            Component::Normal(value) => normalized.push(value),
        }
    }
    Ok(normalized)
}

#[cfg(test)]
#[path = "workspace_root_test.rs"]
mod workspace_root_test;
