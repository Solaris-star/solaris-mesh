use std::fs::File;
use std::io;

/// Stable identity for an already-opened filesystem object.
///
/// The representation is platform-neutral so an authorization layer can
/// retain identities without interpreting platform metadata. Identities are
/// meaningful only on the host that created them.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct ProtectedObjectIdentity {
    storage: u64,
    object: u128,
}

impl ProtectedObjectIdentity {
    pub fn from_file(file: &File) -> io::Result<Self> {
        object_identity_from_file(file)
    }

    #[cfg(unix)]
    pub(crate) const fn from_unix_parts(device: u64, inode: u64) -> Self {
        Self {
            storage: device,
            object: inode as u128,
        }
    }

    #[cfg(test)]
    pub(crate) const fn new(storage: u64, object: u64) -> Self {
        Self {
            storage,
            object: object as u128,
        }
    }

    #[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
    pub(crate) fn external_runner_value(self) -> String {
        format!("{:016x}:{:032x}", self.storage, self.object)
    }
}

#[cfg(unix)]
fn object_identity_from_file(file: &File) -> io::Result<ProtectedObjectIdentity> {
    use std::os::unix::fs::MetadataExt;

    let metadata = file.metadata()?;
    Ok(ProtectedObjectIdentity::from_unix_parts(metadata.dev(), metadata.ino()))
}

#[cfg(windows)]
fn object_identity_from_file(file: &File) -> io::Result<ProtectedObjectIdentity> {
    use std::os::windows::io::AsRawHandle;

    use windows_sys::Win32::Storage::FileSystem::{FILE_ID_INFO, FileIdInfo, GetFileInformationByHandleEx};

    let information_size = u32::try_from(std::mem::size_of::<FILE_ID_INFO>())
        .map_err(|_| io::Error::other("FILE_ID_INFO size exceeds the Windows API limit"))?;
    let mut information = FILE_ID_INFO::default();
    let result = unsafe {
        GetFileInformationByHandleEx(
            file.as_raw_handle(),
            FileIdInfo,
            (&raw mut information).cast(),
            information_size,
        )
    };
    if result == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(ProtectedObjectIdentity {
        storage: information.VolumeSerialNumber,
        object: u128::from_le_bytes(information.FileId.Identifier),
    })
}

#[cfg(not(any(unix, windows)))]
fn object_identity_from_file(_file: &File) -> io::Result<ProtectedObjectIdentity> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "filesystem object identity is unavailable on this platform",
    ))
}
