use std::env;
use std::ffi::{OsStr, OsString};
use std::fs::{File, Metadata};
use std::io::{self, ErrorKind, Read};
use std::mem;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use cap_std::ambient_authority;
use cap_std::fs::{Dir, Metadata as CapMetadata, OpenOptions, ReadDir};
use sha2::{Digest, Sha256};
use solaris_config::file_identity::OpenedFileIdentity;

use super::{SessionStoreError, io_error};

pub(super) struct StoreDirectory {
    path: PathBuf,
    anchor: Dir,
    steps: Vec<DirectoryStep>,
    directory: Dir,
    identity: Arc<OpenedFileIdentity>,
}

struct DirectoryStep {
    name: OsString,
    identity: Arc<OpenedFileIdentity>,
}

pub(super) struct StoreFileSlot {
    relative_path: PathBuf,
    identity: Arc<OpenedFileIdentity>,
}

pub(super) struct OpenedSource {
    pub(super) content: Vec<u8>,
}

impl StoreDirectory {
    pub(super) fn open(path: &Path) -> Result<Self, SessionStoreError> {
        let absolute = absolute_path(path)?;
        let (anchor_path, names) = split_absolute_directory_path(&absolute)?;
        let anchor = Dir::open_ambient_dir(&anchor_path, ambient_authority())
            .map_err(|source| io_error("open session store path anchor", source))?;
        let mut directory = anchor
            .try_clone()
            .map_err(|source| io_error("clone session store path anchor", source))?;
        let mut resolved_path = anchor_path;
        let mut steps = Vec::with_capacity(names.len());
        for name in names {
            let (child, identity) = open_or_create_directory_component(&directory, &name)?;
            resolved_path.push(&name);
            steps.push(DirectoryStep { name, identity });
            directory = child;
        }
        let identity = directory_identity(&directory, "identify session store root")?;
        Ok(Self {
            path: resolved_path,
            anchor,
            steps,
            directory,
            identity,
        })
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) fn verify(&self) -> Result<(), SessionStoreError> {
        let mut current = self
            .anchor
            .try_clone()
            .map_err(|source| io_error("clone session store path anchor", source))?;
        for step in &self.steps {
            let metadata = current
                .symlink_metadata(&step.name)
                .map_err(|source| io_error("verify session store path component", source))?;
            require_directory_metadata(&metadata, "verify session store path component")?;
            let child = current
                .open_dir(&step.name)
                .map_err(|source| io_error("reopen session store path component", source))?;
            let identity = directory_identity(&child, "reidentify session store path component")?;
            if !step.identity.same_object(&identity) {
                return Err(SessionStoreError::UnsafePath {
                    operation: "session store path component identity changed",
                });
            }
            current = child;
        }
        let identity = directory_identity(&current, "reidentify session store root")?;
        if !self.identity.same_object(&identity) {
            return Err(SessionStoreError::UnsafePath {
                operation: "session store root identity changed",
            });
        }
        Ok(())
    }

    pub(super) fn create_file_slot(&self, relative_path: &Path) -> Result<StoreFileSlot, SessionStoreError> {
        self.verify()?;
        validate_relative_path(relative_path)?;
        match self.directory.symlink_metadata(relative_path) {
            Ok(metadata) => require_regular_metadata(&metadata, "inspect session store database slot")?,
            Err(source) if source.kind() == ErrorKind::NotFound => {}
            Err(source) => return Err(io_error("inspect session store database slot", source)),
        }
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);
        let file = self
            .directory
            .open_with(relative_path, &options)
            .map_err(|source| io_error("open session store database slot", source))?;
        let opened = inspect_regular_file(file.into_std(), "inspect session store database")?;
        self.verify_relative_file(relative_path, &opened.identity, "verify session store database")?;
        self.verify()?;
        Ok(StoreFileSlot {
            relative_path: relative_path.to_path_buf(),
            identity: opened.identity,
        })
    }

    pub(super) fn verify_file_slot(&self, slot: &StoreFileSlot) -> Result<(), SessionStoreError> {
        self.verify()?;
        self.verify_relative_file(
            &slot.relative_path,
            &slot.identity,
            "verify session store database slot",
        )
    }

    pub(super) fn verify_file_slot_if_present(&self, slot: &StoreFileSlot) -> Result<(), SessionStoreError> {
        self.verify()?;
        match self.directory.symlink_metadata(&slot.relative_path) {
            Ok(_) => self.verify_file_slot(slot),
            Err(source) if source.kind() == ErrorKind::NotFound => Ok(()),
            Err(source) => Err(io_error("inspect optional session store file slot", source)),
        }
    }

    pub(super) fn list_root_names(&self) -> Result<Vec<OsString>, SessionStoreError> {
        self.verify()?;
        collect_names(
            self.directory
                .entries()
                .map_err(|source| io_error("list session store root", source))?,
            "read session store root entry",
        )
    }

    pub(super) fn list_directory_names(
        &self,
        relative_path: &Path,
    ) -> Result<Option<Vec<OsString>>, SessionStoreError> {
        self.verify()?;
        validate_relative_path(relative_path)?;
        let metadata = match self.directory.symlink_metadata(relative_path) {
            Ok(metadata) => metadata,
            Err(source) if source.kind() == ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(io_error("inspect legacy session directory", source)),
        };
        require_directory_metadata(&metadata, "inspect legacy session directory")?;
        let directory = self
            .directory
            .open_dir(relative_path)
            .map_err(|source| io_error("open legacy session directory", source))?;
        let identity = directory_identity(&directory, "identify legacy session directory")?;
        let current = self
            .directory
            .open_dir(relative_path)
            .map_err(|source| io_error("reopen legacy session directory", source))?;
        let current_identity = directory_identity(&current, "reidentify legacy session directory")?;
        if !identity.same_object(&current_identity) {
            return Err(SessionStoreError::UnsafePath {
                operation: "legacy session directory changed while opening",
            });
        }
        collect_names(
            directory
                .entries()
                .map_err(|source| io_error("list legacy session directory", source))?,
            "read legacy session directory entry",
        )
        .map(Some)
    }

    pub(super) fn relative_is_directory(&self, relative_path: &Path) -> Result<bool, SessionStoreError> {
        self.verify()?;
        validate_relative_path(relative_path)?;
        match self.directory.symlink_metadata(relative_path) {
            Ok(metadata) => {
                if metadata_is_redirected(&metadata) {
                    return Err(SessionStoreError::UnsafePath {
                        operation: "inspect legacy session child directory",
                    });
                }
                Ok(metadata.is_dir())
            }
            Err(source) if source.kind() == ErrorKind::NotFound => Ok(false),
            Err(source) => Err(io_error("inspect legacy session child directory", source)),
        }
    }

    pub(super) fn source_path_digest(&self, relative_path: &Path) -> Result<Vec<u8>, SessionStoreError> {
        self.verify()?;
        validate_relative_path(relative_path)?;
        Ok(Sha256::digest(canonical_path_bytes(&self.path.join(relative_path))).to_vec())
    }

    pub(super) fn display_path(&self, relative_path: &Path) -> Result<PathBuf, SessionStoreError> {
        validate_relative_path(relative_path)?;
        Ok(self.path.join(relative_path))
    }

    pub(super) fn read_source(
        &self,
        relative_path: &Path,
        max_bytes: u64,
    ) -> Result<Option<OpenedSource>, SessionStoreError> {
        self.verify()?;
        validate_relative_path(relative_path)?;
        match self.verify_components(relative_path)? {
            ComponentPresence::Missing => return Ok(None),
            ComponentPresence::Present => {}
        }
        let file = match self.directory.open(relative_path) {
            Ok(file) => file.into_std(),
            Err(source) if source.kind() == ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(io_error("open legacy session source", source)),
        };
        let mut opened = inspect_regular_file(file, "inspect legacy session source")?;
        let length = opened
            .file
            .metadata()
            .map_err(|source| io_error("measure legacy session source", source))?
            .len();
        if length > max_bytes {
            return Err(SessionStoreError::LegacySourceTooLarge { max_bytes });
        }
        let capacity = usize::try_from(length).map_err(|_| SessionStoreError::LegacySourceTooLarge { max_bytes })?;
        let mut content = Vec::new();
        content
            .try_reserve(capacity)
            .map_err(|_| SessionStoreError::LegacySourceTooLarge { max_bytes })?;
        opened
            .file
            .by_ref()
            .take(max_bytes.saturating_add(1))
            .read_to_end(&mut content)
            .map_err(|source| io_error("read legacy session source", source))?;
        if u64::try_from(content.len()).unwrap_or(u64::MAX) > max_bytes {
            return Err(SessionStoreError::LegacySourceTooLarge { max_bytes });
        }
        self.verify_relative_file(
            relative_path,
            &opened.identity,
            "verify legacy session source after read",
        )?;
        self.verify()?;
        Ok(Some(OpenedSource { content }))
    }

    fn verify_relative_file(
        &self,
        relative_path: &Path,
        expected: &OpenedFileIdentity,
        operation: &'static str,
    ) -> Result<(), SessionStoreError> {
        if matches!(self.verify_components(relative_path)?, ComponentPresence::Missing) {
            return Err(SessionStoreError::UnsafePath { operation });
        }
        let file = self
            .directory
            .open(relative_path)
            .map_err(|source| io_error(operation, source))?;
        let current = inspect_regular_file(file.into_std(), operation)?;
        if !expected.same_object(&current.identity) {
            return Err(SessionStoreError::UnsafePath { operation });
        }
        Ok(())
    }

    fn verify_components(&self, relative_path: &Path) -> Result<ComponentPresence, SessionStoreError> {
        let components = relative_path.components().collect::<Vec<_>>();
        let mut current = PathBuf::new();
        for (index, component) in components.iter().enumerate() {
            let Component::Normal(name) = component else {
                return Err(SessionStoreError::UnsafePath {
                    operation: "validate session store relative path",
                });
            };
            current.push(name);
            let is_final = index + 1 == components.len();
            let metadata = match self.directory.symlink_metadata(&current) {
                Ok(metadata) => metadata,
                Err(source) if source.kind() == ErrorKind::NotFound => return Ok(ComponentPresence::Missing),
                Err(source) => return Err(io_error("inspect session store relative path", source)),
            };
            if metadata_is_redirected(&metadata) {
                return Err(SessionStoreError::UnsafePath {
                    operation: "session store relative path is redirected",
                });
            }
            if is_final {
                if !metadata.is_file() {
                    return Err(SessionStoreError::UnsafePath {
                        operation: "session store relative path is not a file",
                    });
                }
            } else if !metadata.is_dir() {
                return Err(SessionStoreError::UnsafePath {
                    operation: "session store relative parent is not a directory",
                });
            }
        }
        Ok(ComponentPresence::Present)
    }
}

fn open_or_create_directory_component(
    parent: &Dir,
    name: &OsStr,
) -> Result<(Dir, Arc<OpenedFileIdentity>), SessionStoreError> {
    match parent.symlink_metadata(name) {
        Ok(metadata) => require_directory_metadata(&metadata, "inspect session store path component")?,
        Err(source) if source.kind() == ErrorKind::NotFound => match parent.create_dir(name) {
            Ok(()) => {}
            Err(source) if source.kind() == ErrorKind::AlreadyExists => {}
            Err(source) => return Err(io_error("create session store path component", source)),
        },
        Err(source) => return Err(io_error("inspect session store path component", source)),
    }
    let metadata = parent
        .symlink_metadata(name)
        .map_err(|source| io_error("inspect created session store path component", source))?;
    require_directory_metadata(&metadata, "inspect created session store path component")?;
    let directory = parent
        .open_dir(name)
        .map_err(|source| io_error("open session store path component", source))?;
    let identity = directory_identity(&directory, "identify session store path component")?;
    let metadata = parent
        .symlink_metadata(name)
        .map_err(|source| io_error("reinspect session store path component", source))?;
    require_directory_metadata(&metadata, "reinspect session store path component")?;
    let current = parent
        .open_dir(name)
        .map_err(|source| io_error("reopen session store path component", source))?;
    let current_identity = directory_identity(&current, "reidentify session store path component")?;
    if !identity.same_object(&current_identity) {
        return Err(SessionStoreError::UnsafePath {
            operation: "session store path component changed while opening",
        });
    }
    Ok((directory, identity))
}

struct InspectedFile {
    file: File,
    identity: Arc<OpenedFileIdentity>,
}

enum ComponentPresence {
    Present,
    Missing,
}

fn collect_names(entries: ReadDir, operation: &'static str) -> Result<Vec<OsString>, SessionStoreError> {
    let mut names = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| io_error(operation, source))?;
        names.push(entry.file_name());
    }
    names.sort();
    Ok(names)
}

fn inspect_regular_file(file: File, operation: &'static str) -> Result<InspectedFile, SessionStoreError> {
    let metadata = file.metadata().map_err(|source| io_error(operation, source))?;
    let links = link_count(&file)?;
    if !metadata.is_file() || std_metadata_is_redirected(&metadata) || links != 1 {
        return Err(SessionStoreError::UnsafePath { operation });
    }
    let identity_file = file.try_clone().map_err(|source| io_error(operation, source))?;
    let identity = OpenedFileIdentity::from_owned_file(identity_file)
        .map(Arc::new)
        .map_err(|source| io_error(operation, source))?;
    Ok(InspectedFile { file, identity })
}

fn require_directory_metadata(metadata: &CapMetadata, operation: &'static str) -> Result<(), SessionStoreError> {
    if !metadata.is_dir() || metadata_is_redirected(metadata) {
        return Err(SessionStoreError::UnsafePath { operation });
    }
    Ok(())
}

fn require_regular_metadata(metadata: &CapMetadata, operation: &'static str) -> Result<(), SessionStoreError> {
    if !metadata.is_file() || metadata_is_redirected(metadata) {
        return Err(SessionStoreError::UnsafePath { operation });
    }
    Ok(())
}

fn directory_identity(directory: &Dir, operation: &'static str) -> Result<Arc<OpenedFileIdentity>, SessionStoreError> {
    let file = directory
        .try_clone()
        .map_err(|source| io_error(operation, source))?
        .into_std_file();
    OpenedFileIdentity::from_owned_file(file)
        .map(Arc::new)
        .map_err(|source| io_error(operation, source))
}

fn validate_relative_path(path: &Path) -> Result<(), SessionStoreError> {
    if path.as_os_str().is_empty()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(SessionStoreError::UnsafePath {
            operation: "validate session store relative path",
        });
    }
    Ok(())
}

fn absolute_path(path: &Path) -> Result<PathBuf, SessionStoreError> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        env::current_dir()
            .map(|current| current.join(path))
            .map_err(|source| io_error("resolve session store root", source))
    }
}

fn split_absolute_directory_path(path: &Path) -> Result<(PathBuf, Vec<OsString>), SessionStoreError> {
    if !path.is_absolute() {
        return Err(SessionStoreError::UnsafePath {
            operation: "resolve session store root",
        });
    }
    let mut anchor = PathBuf::new();
    let mut names = Vec::new();
    let mut saw_root = false;
    for component in path.components() {
        match component {
            Component::Prefix(prefix) if anchor.as_os_str().is_empty() && !saw_root && names.is_empty() => {
                anchor.push(prefix.as_os_str());
            }
            Component::RootDir if !saw_root && names.is_empty() => {
                anchor.push(Path::new(std::path::MAIN_SEPARATOR_STR));
                saw_root = true;
            }
            Component::Normal(name) if saw_root => names.push(name.to_os_string()),
            Component::CurDir => {}
            Component::ParentDir | Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                return Err(SessionStoreError::UnsafePath {
                    operation: "resolve session store root",
                });
            }
        }
    }
    if !saw_root || names.is_empty() {
        return Err(SessionStoreError::UnsafePath {
            operation: "resolve session store root",
        });
    }
    Ok((anchor, names))
}

#[cfg(unix)]
fn metadata_is_redirected(metadata: &CapMetadata) -> bool {
    metadata.is_symlink()
}

#[cfg(windows)]
fn metadata_is_redirected(metadata: &CapMetadata) -> bool {
    use cap_std::fs::MetadataExt;
    use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;

    metadata.is_symlink() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

#[cfg(not(any(unix, windows)))]
fn metadata_is_redirected(metadata: &CapMetadata) -> bool {
    metadata.is_symlink()
}

#[cfg(unix)]
fn std_metadata_is_redirected(metadata: &Metadata) -> bool {
    metadata.file_type().is_symlink()
}

#[cfg(windows)]
fn std_metadata_is_redirected(metadata: &Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;

    metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

#[cfg(not(any(unix, windows)))]
fn std_metadata_is_redirected(metadata: &Metadata) -> bool {
    metadata.file_type().is_symlink()
}

#[cfg(unix)]
fn link_count(file: &File) -> Result<u64, SessionStoreError> {
    use std::os::unix::fs::MetadataExt;

    file.metadata()
        .map(|metadata| metadata.nlink())
        .map_err(|source| io_error("inspect session store file link count", source))
}

#[cfg(windows)]
fn link_count(file: &File) -> Result<u64, SessionStoreError> {
    use std::os::windows::io::AsRawHandle;

    use windows_sys::Win32::Storage::FileSystem::{BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle};

    // SAFETY: The output points to initialized writable storage and the file
    // owns a valid handle for the duration of the call.
    let mut information: BY_HANDLE_FILE_INFORMATION = unsafe { mem::zeroed() };
    // SAFETY: The raw handle remains valid because `file` is borrowed.
    let succeeded = unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut information) };
    if succeeded == 0 {
        return Err(io_error(
            "inspect session store file link count",
            io::Error::last_os_error(),
        ));
    }
    Ok(u64::from(information.nNumberOfLinks))
}

#[cfg(not(any(unix, windows)))]
fn link_count(_: &File) -> Result<u64, SessionStoreError> {
    Err(SessionStoreError::UnsafePath {
        operation: "session store file identity is unavailable",
    })
}

#[cfg(unix)]
fn canonical_path_bytes(path: &Path) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;

    path.as_os_str().as_bytes().to_vec()
}

#[cfg(windows)]
fn canonical_path_bytes(path: &Path) -> Vec<u8> {
    use std::os::windows::ffi::OsStrExt;

    path.as_os_str().encode_wide().flat_map(u16::to_le_bytes).collect()
}

#[cfg(not(any(unix, windows)))]
fn canonical_path_bytes(path: &Path) -> Vec<u8> {
    path.to_string_lossy().into_owned().into_bytes()
}
