use std::fs::File;
use std::io;
use std::time::SystemTime;

use solaris_process::ProtectedObjectIdentity;

/// Return the filesystem link count for an already-opened file handle.
///
/// Callers can use this to reject files whose other names cannot be proven to
/// remain inside the caller's trust boundary.
pub fn opened_file_link_count(file: &File) -> io::Result<u64> {
    opened_file_link_count_platform(file)
}

/// Stable identity for one filesystem object backed by a live OS handle.
///
/// Construction consumes an already-opened handle. The handle remains owned
/// by this value so Windows file identifiers cannot be reused while an
/// identity is retained.
#[derive(Debug)]
pub struct OpenedFileIdentity {
    handle: same_file::Handle,
    state_file: File,
    protected_object_identity: Option<ProtectedObjectIdentity>,
}

/// Metadata from an open filesystem object that changes when its contents or
/// security-relevant attributes change.
///
/// This deliberately includes the OS change timestamp rather than relying on
/// the user-settable modification timestamp. It is suitable for short-lived,
/// run-local evidence validation, not for persistence across machines.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenedFileState {
    length: u64,
    modified: SystemTime,
    change_marker: [u64; 4],
}

impl OpenedFileState {
    pub fn matches_observed_metadata(&self, length: u64, modified: SystemTime) -> bool {
        self.length == length && self.modified == modified
    }
}

impl OpenedFileIdentity {
    pub fn from_owned_file(file: File) -> io::Result<Self> {
        let state_file = file.try_clone()?;
        #[cfg(any(unix, windows))]
        let protected_object_identity = Some(ProtectedObjectIdentity::from_file(&file)?);
        #[cfg(not(any(unix, windows)))]
        let protected_object_identity = None;
        same_file::Handle::from_file(file).map(|handle| Self {
            handle,
            state_file,
            protected_object_identity,
        })
    }

    pub fn same_object(&self, other: &Self) -> bool {
        self.handle == other.handle
    }

    pub fn protected_object_identity(&self) -> Option<ProtectedObjectIdentity> {
        self.protected_object_identity
    }

    /// Inspect the retained handle without reopening the path.
    pub fn current_state(&self) -> io::Result<OpenedFileState> {
        let metadata = self.state_file.metadata()?;
        Ok(OpenedFileState {
            length: metadata.len(),
            modified: metadata.modified()?,
            change_marker: change_marker(&self.state_file, &metadata)?,
        })
    }
}

#[cfg(unix)]
fn opened_file_link_count_platform(file: &File) -> io::Result<u64> {
    use std::os::unix::fs::MetadataExt;

    file.metadata().map(|metadata| metadata.nlink())
}

#[cfg(windows)]
fn opened_file_link_count_platform(file: &File) -> io::Result<u64> {
    use std::os::windows::io::AsRawHandle;

    opened_windows_handle_link_count(file.as_raw_handle())
}

#[cfg(windows)]
fn opened_windows_handle_link_count(handle: std::os::windows::io::RawHandle) -> io::Result<u64> {
    use std::ffi::c_void;

    #[repr(C)]
    struct FileStandardInfo {
        allocation_size: i64,
        end_of_file: i64,
        number_of_links: u32,
        delete_pending: u8,
        directory: u8,
    }

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetFileInformationByHandleEx(
            file: *mut c_void,
            info_class: i32,
            information: *mut c_void,
            buffer_size: u32,
        ) -> i32;
    }

    const FILE_STANDARD_INFO_CLASS: i32 = 1;
    let mut information = FileStandardInfo {
        allocation_size: 0,
        end_of_file: 0,
        number_of_links: 0,
        delete_pending: 0,
        directory: 0,
    };
    let buffer_size = u32::try_from(std::mem::size_of::<FileStandardInfo>())
        .map_err(|_| io::Error::other("FILE_STANDARD_INFO size exceeds the Windows API limit"))?;
    // SAFETY: `information` is writable for `buffer_size` bytes. The API does
    // not retain either argument, and reports invalid handles as an error.
    let succeeded = unsafe {
        GetFileInformationByHandleEx(
            handle,
            FILE_STANDARD_INFO_CLASS,
            (&raw mut information).cast(),
            buffer_size,
        )
    };
    if succeeded == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(u64::from(information.number_of_links))
    }
}

#[cfg(not(any(unix, windows)))]
fn opened_file_link_count_platform(_: &File) -> io::Result<u64> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "filesystem link count is unavailable on this platform",
    ))
}

#[cfg(unix)]
fn change_marker(_file: &File, metadata: &std::fs::Metadata) -> io::Result<[u64; 4]> {
    use std::os::unix::fs::MetadataExt;

    Ok([
        metadata.ctime() as u64,
        metadata.ctime_nsec() as u64,
        u64::from(metadata.mode()),
        metadata.nlink(),
    ])
}

#[cfg(windows)]
fn change_marker(file: &File, _metadata: &std::fs::Metadata) -> io::Result<[u64; 4]> {
    use std::ffi::c_void;
    use std::os::windows::io::AsRawHandle;

    #[repr(C)]
    struct FileBasicInfo {
        creation_time: i64,
        last_access_time: i64,
        last_write_time: i64,
        change_time: i64,
        file_attributes: u32,
    }

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetFileInformationByHandleEx(
            file: *mut c_void,
            info_class: i32,
            information: *mut c_void,
            buffer_size: u32,
        ) -> i32;
    }

    const FILE_BASIC_INFO_CLASS: i32 = 0;
    let mut information = FileBasicInfo {
        creation_time: 0,
        last_access_time: 0,
        last_write_time: 0,
        change_time: 0,
        file_attributes: 0,
    };
    let buffer_size = u32::try_from(std::mem::size_of::<FileBasicInfo>())
        .map_err(|_| io::Error::other("FILE_BASIC_INFO size exceeds the Windows API limit"))?;
    let result = unsafe {
        GetFileInformationByHandleEx(
            file.as_raw_handle(),
            FILE_BASIC_INFO_CLASS,
            (&raw mut information).cast(),
            buffer_size,
        )
    };
    if result == 0 {
        return Err(io::Error::last_os_error());
    }
    if information.change_time == 0 {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "filesystem does not expose a reliable change timestamp",
        ));
    }
    Ok([
        information.change_time as u64,
        u64::from(information.file_attributes),
        0,
        0,
    ])
}

#[cfg(not(any(unix, windows)))]
fn change_marker(_file: &File, _metadata: &std::fs::Metadata) -> io::Result<[u64; 4]> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "reliable open-file state is unavailable on this platform",
    ))
}

#[cfg(test)]
#[path = "file_identity_test.rs"]
mod file_identity_test;
