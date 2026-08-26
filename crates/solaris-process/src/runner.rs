use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use tokio::process::Command;

use crate::command::PinnedCommand;
use crate::environment::configure_safe_process_environment;
use crate::executable::{ExecutableError, ExecutableIdentity, executable_path_identity};
use crate::launch_policy::ProcessLaunchPolicy;

pub(crate) const MAX_EXECUTABLE_BYTES: u64 = 64 * 1024 * 1024;
#[cfg(target_os = "linux")]
const MFD_EXEC_FLAG: libc::c_uint = 0x0010;

#[derive(Debug)]
pub struct PinnedExecutable {
    #[cfg(windows)]
    execution_path: PathBuf,
    identity: ExecutableIdentity,
    #[cfg(windows)]
    _file: File,
    #[cfg(unix)]
    snapshot: UnixExecutableSnapshot,
}

impl PinnedExecutable {
    /// Builds a command that executes the retained implementation.
    ///
    /// On Unix, only this command's child receives an executable snapshot fd
    /// without `FD_CLOEXEC`. The parent and unrelated children always retain
    /// close-on-exec descriptors.
    pub fn command(self) -> std::result::Result<PinnedCommand, ExecutableError> {
        #[cfg(windows)]
        {
            let mut command = Command::new(&self.execution_path);
            configure_safe_process_environment(&mut command);
            Ok(PinnedCommand {
                command,
                _executable: self,
                launch_policy: ProcessLaunchPolicy::Ambient,
                spawn_authorizer: None,
                stdin: None,
                stdout: None,
                stderr: None,
            })
        }
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;

            self.snapshot.verify(&self.identity)?;
            let reserved = self.snapshot.duplicate_cloexec(&self.identity)?;
            let descriptor = reserved.as_raw_fd();
            let execution_path = executable_fd_path(descriptor).map_err(|error| {
                ExecutableError::new(
                    "executable snapshot fd path unavailable",
                    self.identity.path_digest.clone(),
                    Some(error),
                )
            })?;
            let mut command = Command::new(execution_path);
            configure_safe_process_environment(&mut command);
            unsafe {
                command.pre_exec(move || {
                    if libc::fcntl(reserved.as_raw_fd(), libc::F_SETFD, 0) == -1 {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
            Ok(PinnedCommand {
                command,
                _executable: self,
                launch_policy: ProcessLaunchPolicy::Ambient,
                spawn_authorizer: None,
                stdin: None,
                stdout: None,
                stderr: None,
            })
        }
    }

    pub fn identity(&self) -> &ExecutableIdentity {
        &self.identity
    }

    #[cfg(windows)]
    pub(crate) fn windows_execution_path(&self) -> &Path {
        &self.execution_path
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn duplicate_linux_snapshot(&self) -> std::result::Result<File, ExecutableError> {
        self.duplicate_unix_snapshot()
    }

    #[cfg(target_os = "macos")]
    pub(crate) fn duplicate_macos_snapshot(&self) -> std::result::Result<File, ExecutableError> {
        self.duplicate_unix_snapshot()
    }

    #[cfg(unix)]
    pub(crate) fn duplicate_unix_snapshot(&self) -> std::result::Result<File, ExecutableError> {
        self.snapshot.verify(&self.identity)?;
        self.snapshot.duplicate_cloexec(&self.identity)
    }

    #[cfg(all(test, unix, not(target_os = "linux")))]
    fn snapshot_link_count(&self) -> u64 {
        use std::os::unix::fs::MetadataExt;

        self.snapshot.file.metadata().unwrap().nlink()
    }

    #[cfg(all(test, unix, not(target_os = "linux")))]
    fn snapshot_access_mode(&self) -> libc::c_int {
        use std::os::fd::AsRawFd;

        (unsafe { libc::fcntl(self.snapshot.file.as_raw_fd(), libc::F_GETFL) }) & libc::O_ACCMODE
    }

    #[cfg(all(test, target_os = "macos"))]
    fn snapshot_raw_fd(&self) -> std::os::fd::RawFd {
        use std::os::fd::AsRawFd;

        self.snapshot.file.as_raw_fd()
    }

    #[cfg(all(test, target_os = "macos"))]
    fn snapshot_macos_flags(&self) -> libc::c_uint {
        macos_file_flags(&self.snapshot.file).unwrap()
    }

    #[cfg(all(test, unix))]
    fn snapshot_fd_flags(&self) -> libc::c_int {
        use std::os::fd::AsRawFd;

        unsafe { libc::fcntl(self.snapshot.file.as_raw_fd(), libc::F_GETFD) }
    }

    #[cfg(all(test, target_os = "linux"))]
    fn snapshot_linux_seals(&self) -> libc::c_int {
        use std::os::fd::AsRawFd;

        unsafe { libc::fcntl(self.snapshot.file.as_raw_fd(), libc::F_GET_SEALS) }
    }

    #[cfg(all(test, unix))]
    fn snapshot_test_path(&self) -> PathBuf {
        use std::os::fd::AsRawFd;

        executable_fd_path(self.snapshot.file.as_raw_fd()).unwrap()
    }
}

pub fn inspect_executable(path: &Path) -> std::result::Result<ExecutableIdentity, ExecutableError> {
    let requested_digest = executable_path_identity(path);
    let mut file = open_executable(path, &requested_digest)?;
    identity_from_open_file(&mut file, &requested_digest).map(|(identity, _)| identity)
}

pub fn pin_executable(
    path: &Path,
    expected: &ExecutableIdentity,
) -> std::result::Result<PinnedExecutable, ExecutableError> {
    pin_executable_inner(path, expected, |_| {})
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PinPhase {
    BeforeOpen,
    AfterOpen,
}

fn pin_executable_inner(
    path: &Path,
    expected: &ExecutableIdentity,
    mut phase_hook: impl FnMut(PinPhase),
) -> std::result::Result<PinnedExecutable, ExecutableError> {
    let requested_digest = executable_path_identity(path);
    phase_hook(PinPhase::BeforeOpen);
    let mut file = open_executable(path, &requested_digest)?;
    phase_hook(PinPhase::AfterOpen);
    let (actual, bytes) = identity_from_open_file(&mut file, &requested_digest)?;
    if actual.path_digest != expected.path_digest {
        return Err(ExecutableError::new(
            "executable path identity changed before execution",
            actual.path_digest,
            None,
        ));
    }
    if actual.content_digest != expected.content_digest {
        return Err(ExecutableError::new(
            "executable implementation changed before execution",
            actual.path_digest,
            None,
        ));
    }

    #[cfg(windows)]
    drop(bytes);

    #[cfg(unix)]
    let snapshot = UnixExecutableSnapshot::create(&bytes, &actual.path_digest)?;
    Ok(PinnedExecutable {
        #[cfg(windows)]
        execution_path: actual.canonical_path.clone(),
        identity: actual,
        #[cfg(windows)]
        _file: file,
        #[cfg(unix)]
        snapshot,
    })
}

pub fn pin_executable_by_digest(
    path: &Path,
    expected_digest: &str,
) -> std::result::Result<PinnedExecutable, ExecutableError> {
    let mut expected = inspect_executable(path)?;
    expected.content_digest = expected_digest.to_owned();
    pin_executable(path, &expected)
}

fn open_executable(path: &Path, requested_digest: &str) -> std::result::Result<File, ExecutableError> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;

        options.share_mode(windows_sys::Win32::Storage::FileSystem::FILE_SHARE_READ);
        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_CLOEXEC | libc::O_NONBLOCK | libc::O_NOFOLLOW);
    }
    options
        .open(path)
        .map_err(|error| ExecutableError::new("executable pin open failed", requested_digest.to_owned(), Some(error)))
}

fn identity_from_open_file(
    file: &mut File,
    requested_digest: &str,
) -> std::result::Result<(ExecutableIdentity, Vec<u8>), ExecutableError> {
    identity_from_open_file_inner(file, requested_digest, || {})
}

fn identity_from_open_file_inner(
    file: &mut File,
    requested_digest: &str,
    before_read: impl FnOnce(),
) -> std::result::Result<(ExecutableIdentity, Vec<u8>), ExecutableError> {
    let metadata = file.metadata().map_err(|error| {
        ExecutableError::new(
            "executable handle metadata inspection failed",
            requested_digest.to_owned(),
            Some(error),
        )
    })?;
    if !metadata.is_file() {
        return Err(ExecutableError::new(
            "executable is not a regular file",
            requested_digest.to_owned(),
            None,
        ));
    }
    #[cfg(windows)]
    if metadata.file_type().is_symlink() {
        return Err(ExecutableError::new(
            "executable symbolic links are not allowed",
            requested_digest.to_owned(),
            None,
        ));
    }
    if metadata.len() > MAX_EXECUTABLE_BYTES {
        return Err(ExecutableError::new(
            "executable exceeds size limit",
            requested_digest.to_owned(),
            None,
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        if metadata.permissions().mode() & 0o111 == 0 {
            return Err(ExecutableError::new(
                "executable has no execute permission",
                requested_digest.to_owned(),
                None,
            ));
        }
    }
    let canonical_path = final_path_from_open_file(file).map_err(|error| {
        ExecutableError::new(
            "executable final path inspection failed",
            requested_digest.to_owned(),
            Some(error),
        )
    })?;
    if !canonical_path.is_absolute() {
        return Err(ExecutableError::new(
            "executable final path is not absolute",
            requested_digest.to_owned(),
            None,
        ));
    }
    let path_digest = executable_path_identity(&canonical_path);
    before_read();
    let mut bytes = Vec::with_capacity((metadata.len() as usize).min(64 * 1024));
    let mut content_hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| ExecutableError::new("executable pin read failed", path_digest.clone(), Some(error)))?;
        if read == 0 {
            break;
        }
        if (bytes.len() as u64).saturating_add(read as u64) > MAX_EXECUTABLE_BYTES {
            return Err(ExecutableError::new(
                "executable exceeds size limit while reading",
                path_digest,
                None,
            ));
        }
        content_hasher.update(&buffer[..read]);
        bytes.extend_from_slice(&buffer[..read]);
    }
    let final_metadata = file.metadata().map_err(|error| {
        ExecutableError::new(
            "executable handle metadata revalidation failed",
            path_digest.clone(),
            Some(error),
        )
    })?;
    if final_metadata.len() != metadata.len() || final_metadata.len() != bytes.len() as u64 {
        return Err(ExecutableError::new(
            "executable changed size while reading",
            path_digest,
            None,
        ));
    }
    let content_digest = format!("{:x}", content_hasher.finalize());
    Ok((
        ExecutableIdentity {
            canonical_path,
            path_digest,
            content_digest,
        },
        bytes,
    ))
}

#[cfg(target_os = "linux")]
fn final_path_from_open_file(file: &File) -> std::io::Result<PathBuf> {
    use std::os::fd::AsRawFd;

    std::fs::read_link(format!("/proc/self/fd/{}", file.as_raw_fd()))
}

#[cfg(target_vendor = "apple")]
pub(crate) fn final_path_from_open_file(file: &File) -> std::io::Result<PathBuf> {
    use std::ffi::CStr;
    use std::os::fd::AsRawFd;
    use std::os::unix::ffi::OsStrExt;

    let mut buffer = vec![0_u8; libc::PATH_MAX as usize];
    // SAFETY: `file` owns a valid descriptor for the duration of the call and
    // Darwin F_GETPATH requires a writable PATH_MAX-byte buffer. `buffer`
    // provides exactly that capacity and is not accessed concurrently.
    if unsafe {
        libc::fcntl(
            file.as_raw_fd(),
            libc::F_GETPATH,
            buffer.as_mut_ptr().cast::<libc::c_char>(),
        )
    } == -1
    {
        return Err(std::io::Error::last_os_error());
    }
    // Verify the kernel's NUL-termination contract before constructing CStr,
    // so malformed output cannot make a pointer walk beyond the buffer.
    let terminator = buffer
        .iter()
        .position(|byte| *byte == 0)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "F_GETPATH path is not NUL-terminated"))?;
    let bytes = CStr::from_bytes_with_nul(&buffer[..=terminator])
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "F_GETPATH path is invalid"))?
        .to_bytes();
    Ok(PathBuf::from(std::ffi::OsStr::from_bytes(bytes)))
}

#[cfg(all(unix, not(target_os = "linux"), not(target_vendor = "apple")))]
fn final_path_from_open_file(file: &File) -> std::io::Result<PathBuf> {
    use std::os::fd::AsRawFd;

    let descriptor = file.as_raw_fd();
    std::fs::read_link(format!("/proc/self/fd/{descriptor}"))
        .or_else(|_| std::fs::read_link(format!("/dev/fd/{descriptor}")))
}

#[cfg(windows)]
fn final_path_from_open_file(file: &File) -> std::io::Result<PathBuf> {
    use std::ffi::OsString;
    use std::os::windows::ffi::OsStringExt;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::GetFinalPathNameByHandleW;

    let mut buffer = vec![0_u16; 512];
    loop {
        let length = unsafe {
            GetFinalPathNameByHandleW(
                file.as_raw_handle() as windows_sys::Win32::Foundation::HANDLE,
                buffer.as_mut_ptr(),
                buffer.len() as u32,
                0,
            )
        };
        if length == 0 {
            return Err(std::io::Error::last_os_error());
        }
        if (length as usize) < buffer.len() {
            buffer.truncate(length as usize);
            return Ok(PathBuf::from(OsString::from_wide(&buffer)));
        }
        buffer.resize(length as usize + 1, 0);
    }
}

#[cfg(unix)]
fn executable_fd_path(descriptor: std::os::fd::RawFd) -> std::io::Result<PathBuf> {
    let proc_path = PathBuf::from(format!("/proc/self/fd/{descriptor}"));
    if proc_path.exists() {
        return Ok(proc_path);
    }
    let dev_path = PathBuf::from(format!("/dev/fd/{descriptor}"));
    if dev_path.exists() {
        return Ok(dev_path);
    }
    Err(std::io::Error::other("no executable fd path is available"))
}

#[cfg(unix)]
#[derive(Debug)]
struct UnixExecutableSnapshot {
    file: File,
    snapshot_digest: String,
}

#[cfg(unix)]
impl UnixExecutableSnapshot {
    #[cfg(target_os = "linux")]
    fn create(bytes: &[u8], identity_digest: &str) -> std::result::Result<Self, ExecutableError> {
        Self::create_sealed_memfd(bytes, identity_digest)
    }

    #[cfg(target_os = "macos")]
    fn create(bytes: &[u8], identity_digest: &str) -> std::result::Result<Self, ExecutableError> {
        Self::create_macos_immutable(bytes, identity_digest)
    }

    #[cfg(all(not(target_os = "linux"), not(target_os = "macos")))]
    fn create(bytes: &[u8], identity_digest: &str) -> std::result::Result<Self, ExecutableError> {
        Self::create_anonymous_readonly(bytes, identity_digest)
    }

    #[cfg(target_os = "linux")]
    fn create_sealed_memfd(bytes: &[u8], identity_digest: &str) -> std::result::Result<Self, ExecutableError> {
        use std::io::Write;
        use std::os::fd::FromRawFd;

        let base_flags = libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING;
        let mut descriptor = unsafe { libc::memfd_create(c"solaris-executable".as_ptr(), base_flags | MFD_EXEC_FLAG) };
        if descriptor == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::EINVAL) {
            descriptor = unsafe { libc::memfd_create(c"solaris-executable".as_ptr(), base_flags) };
        }
        if descriptor == -1 {
            return Err(ExecutableError::new(
                "executable snapshot creation failed",
                identity_digest.to_owned(),
                Some(std::io::Error::last_os_error()),
            ));
        }
        let mut file = unsafe { File::from_raw_fd(descriptor) };
        file.write_all(bytes)
            .and_then(|_| file.flush())
            .and_then(|_| file.sync_all())
            .map_err(|error| {
                ExecutableError::new(
                    "executable snapshot write failed",
                    identity_digest.to_owned(),
                    Some(error),
                )
            })?;
        if unsafe { libc::fchmod(descriptor, 0o500) } == -1 {
            return Err(ExecutableError::new(
                "executable snapshot permission failed",
                identity_digest.to_owned(),
                Some(std::io::Error::last_os_error()),
            ));
        }
        let seals = libc::F_SEAL_WRITE | libc::F_SEAL_GROW | libc::F_SEAL_SHRINK | libc::F_SEAL_SEAL;
        if unsafe { libc::fcntl(descriptor, libc::F_ADD_SEALS, seals) } == -1 {
            return Err(ExecutableError::new(
                "executable snapshot sealing failed",
                identity_digest.to_owned(),
                Some(std::io::Error::last_os_error()),
            ));
        }
        Ok(Self {
            file,
            snapshot_digest: format!("{:x}", Sha256::digest(bytes)),
        })
    }

    #[cfg(target_os = "macos")]
    fn create_macos_immutable(bytes: &[u8], identity_digest: &str) -> std::result::Result<Self, ExecutableError> {
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::PermissionsExt;

        let mut file = create_anonymous_snapshot(identity_digest)?;
        write_snapshot(&mut file, bytes, identity_digest)?;
        file.set_permissions(std::fs::Permissions::from_mode(0o500))
            .map_err(|error| {
                ExecutableError::new(
                    "executable snapshot permission failed",
                    identity_digest.to_owned(),
                    Some(error),
                )
            })?;
        let descriptor = file.as_raw_fd();
        if unsafe { libc::fchflags(descriptor, libc::UF_IMMUTABLE) } == -1 {
            return Err(ExecutableError::new(
                "executable snapshot immutable flag failed",
                identity_digest.to_owned(),
                Some(std::io::Error::last_os_error()),
            ));
        }
        if macos_file_flags(&file).map_err(|error| {
            ExecutableError::new(
                "executable snapshot immutable flag inspection failed",
                identity_digest.to_owned(),
                Some(error),
            )
        })? & libc::UF_IMMUTABLE
            == 0
        {
            return Err(ExecutableError::new(
                "executable snapshot is not immutable",
                identity_digest.to_owned(),
                None,
            ));
        }
        ensure_snapshot_fd_cloexec(descriptor, identity_digest)?;
        Ok(Self {
            file,
            snapshot_digest: format!("{:x}", Sha256::digest(bytes)),
        })
    }

    #[cfg(all(not(target_os = "linux"), not(target_os = "macos")))]
    fn create_anonymous_readonly(bytes: &[u8], identity_digest: &str) -> std::result::Result<Self, ExecutableError> {
        use std::os::unix::fs::PermissionsExt;

        let mut writer = create_anonymous_snapshot(identity_digest)?;
        write_snapshot(&mut writer, bytes, identity_digest)?;
        writer
            .set_permissions(std::fs::Permissions::from_mode(0o500))
            .map_err(|error| {
                ExecutableError::new(
                    "executable snapshot permission failed",
                    identity_digest.to_owned(),
                    Some(error),
                )
            })?;
        use std::os::fd::AsRawFd;

        let writer_path = executable_fd_path(writer.as_raw_fd()).map_err(|error| {
            ExecutableError::new(
                "anonymous executable snapshot fd path unavailable",
                identity_digest.to_owned(),
                Some(error),
            )
        })?;
        let file = File::open(&writer_path).map_err(|error| {
            ExecutableError::new(
                "executable snapshot reopen failed",
                identity_digest.to_owned(),
                Some(error),
            )
        })?;
        let descriptor = file.as_raw_fd();
        let access_mode = unsafe { libc::fcntl(descriptor, libc::F_GETFL) };
        if access_mode == -1 || access_mode & libc::O_ACCMODE != libc::O_RDONLY {
            return Err(ExecutableError::new(
                "executable snapshot reopen is not read-only",
                identity_digest.to_owned(),
                None,
            ));
        }
        ensure_snapshot_fd_cloexec(descriptor, identity_digest)?;
        drop(writer);
        Ok(Self {
            file,
            snapshot_digest: format!("{:x}", Sha256::digest(bytes)),
        })
    }

    fn verify(&self, identity: &ExecutableIdentity) -> std::result::Result<(), ExecutableError> {
        use std::os::unix::fs::PermissionsExt;

        let metadata = self.file.metadata().map_err(|error| {
            ExecutableError::new(
                "executable snapshot metadata verification failed",
                identity.path_digest.clone(),
                Some(error),
            )
        })?;
        if !metadata.is_file() || metadata.permissions().mode() & 0o777 != 0o500 {
            return Err(ExecutableError::new(
                "executable snapshot metadata changed before execution",
                identity.path_digest.clone(),
                None,
            ));
        }
        #[cfg(not(target_os = "linux"))]
        {
            use std::os::unix::fs::MetadataExt;

            if metadata.nlink() != 0 {
                return Err(ExecutableError::new(
                    "executable snapshot is not anonymous",
                    identity.path_digest.clone(),
                    None,
                ));
            }
            #[cfg(target_os = "macos")]
            if macos_file_flags(&self.file).map_err(|error| {
                ExecutableError::new(
                    "executable snapshot immutable flag verification failed",
                    identity.path_digest.clone(),
                    Some(error),
                )
            })? & libc::UF_IMMUTABLE
                == 0
            {
                return Err(ExecutableError::new(
                    "executable snapshot immutable flag changed before execution",
                    identity.path_digest.clone(),
                    None,
                ));
            }
            #[cfg(not(target_os = "macos"))]
            {
                use std::os::fd::AsRawFd;

                let flags = unsafe { libc::fcntl(self.file.as_raw_fd(), libc::F_GETFL) };
                if flags == -1 || flags & libc::O_ACCMODE != libc::O_RDONLY {
                    return Err(ExecutableError::new(
                        "executable snapshot is not read-only",
                        identity.path_digest.clone(),
                        None,
                    ));
                }
            }
        }
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::AsRawFd;

            let required = libc::F_SEAL_WRITE | libc::F_SEAL_GROW | libc::F_SEAL_SHRINK | libc::F_SEAL_SEAL;
            let actual = unsafe { libc::fcntl(self.file.as_raw_fd(), libc::F_GET_SEALS) };
            if actual == -1 || actual & required != required {
                return Err(ExecutableError::new(
                    "executable snapshot seals changed before execution",
                    identity.path_digest.clone(),
                    None,
                ));
            }
        }
        use std::os::fd::AsRawFd;

        let descriptor_flags = unsafe { libc::fcntl(self.file.as_raw_fd(), libc::F_GETFD) };
        if descriptor_flags == -1 || descriptor_flags & libc::FD_CLOEXEC == 0 {
            return Err(ExecutableError::new(
                "executable snapshot descriptor is inheritable",
                identity.path_digest.clone(),
                None,
            ));
        }
        let snapshot_digest = digest_open_unix_file(&self.file).map_err(|error| {
            ExecutableError::new(
                "executable snapshot verification failed",
                identity.path_digest.clone(),
                Some(error),
            )
        })?;
        if snapshot_digest != self.snapshot_digest || snapshot_digest != identity.content_digest {
            return Err(ExecutableError::new(
                "executable snapshot changed before execution",
                identity.path_digest.clone(),
                None,
            ));
        }
        Ok(())
    }

    fn duplicate_cloexec(&self, identity: &ExecutableIdentity) -> std::result::Result<File, ExecutableError> {
        use std::os::fd::{AsRawFd, FromRawFd};

        let descriptor = unsafe { libc::fcntl(self.file.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 3) };
        if descriptor == -1 {
            return Err(ExecutableError::new(
                "executable snapshot duplication failed",
                identity.path_digest.clone(),
                Some(std::io::Error::last_os_error()),
            ));
        }
        Ok(unsafe { File::from_raw_fd(descriptor) })
    }
}

#[cfg(all(unix, not(target_os = "linux")))]
fn create_anonymous_snapshot(identity_digest: &str) -> std::result::Result<File, ExecutableError> {
    tempfile::tempfile().map_err(|error| {
        ExecutableError::new(
            "anonymous executable snapshot creation failed",
            identity_digest.to_owned(),
            Some(error),
        )
    })
}

#[cfg(all(unix, not(target_os = "linux")))]
fn write_snapshot(file: &mut File, bytes: &[u8], identity_digest: &str) -> std::result::Result<(), ExecutableError> {
    use std::io::Write;

    file.write_all(bytes)
        .and_then(|_| file.flush())
        .and_then(|_| file.sync_all())
        .map_err(|error| {
            ExecutableError::new(
                "executable snapshot write failed",
                identity_digest.to_owned(),
                Some(error),
            )
        })
}

#[cfg(all(unix, not(target_os = "linux")))]
fn ensure_snapshot_fd_cloexec(
    descriptor: std::os::fd::RawFd,
    identity_digest: &str,
) -> std::result::Result<(), ExecutableError> {
    if unsafe { libc::fcntl(descriptor, libc::F_SETFD, libc::FD_CLOEXEC) } == -1 {
        return Err(ExecutableError::new(
            "executable snapshot close-on-exec setup failed",
            identity_digest.to_owned(),
            Some(std::io::Error::last_os_error()),
        ));
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn macos_file_flags(file: &File) -> std::io::Result<libc::c_uint> {
    use std::os::fd::AsRawFd;

    let mut status: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::fstat(file.as_raw_fd(), &mut status) } == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(status.st_flags)
}

#[cfg(unix)]
fn digest_open_unix_file(file: &File) -> std::io::Result<String> {
    use std::os::unix::fs::FileExt;

    let mut hasher = Sha256::new();
    let mut offset = 0_u64;
    let mut buffer = [0_u8; 8192];
    loop {
        let read = file.read_at(&mut buffer, offset)?;
        if read == 0 {
            return Ok(format!("{:x}", hasher.finalize()));
        }
        hasher.update(&buffer[..read]);
        offset = offset.saturating_add(read as u64);
    }
}

#[cfg(test)]
use crate::command_runner::CommandRunner;

#[cfg(test)]
#[path = "runner_test.rs"]
mod runner_test;
