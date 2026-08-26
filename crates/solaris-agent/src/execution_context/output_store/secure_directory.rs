use std::env;
use std::ffi::{OsStr, OsString};
use std::fs::{File, Metadata};
use std::io::{self, ErrorKind, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use cap_std::ambient_authority;
use cap_std::fs::{Dir, Metadata as CapMetadata, OpenOptions};
use solaris_config::file_identity::OpenedFileIdentity;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum WriteStep {
    TemporarySynced,
    Renamed,
    ParentSynced,
}

pub(super) struct SecureDirectory {
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

struct ValidatedChildFile {
    name: OsString,
    identity: Arc<OpenedFileIdentity>,
}

impl SecureDirectory {
    pub(super) fn open_or_create(path: &Path) -> io::Result<Self> {
        Self::open(path, true)
    }

    pub(super) fn open_existing(path: &Path) -> io::Result<Self> {
        Self::open(path, false)
    }

    pub(super) fn same_as_path(&self, path: &Path) -> io::Result<bool> {
        match Self::open_existing(path) {
            Ok(other) => Ok(self.identity.same_object(other.identity.as_ref())),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn open(path: &Path, create: bool) -> io::Result<Self> {
        let absolute = normalized_absolute_directory_path(path)?;
        let (anchor_path, names) = split_absolute_directory_path(&absolute)?;
        let anchor = Dir::open_ambient_dir(&anchor_path, ambient_authority())
            .map_err(|error| contextual(error, "open effect output path anchor"))?;
        let mut directory = anchor.try_clone()?;
        let mut directory_path = anchor_path;
        let mut steps = Vec::with_capacity(names.len());
        let component_count = names.len();
        let mut immediate_parent = None;
        for (index, name) in names.into_iter().enumerate() {
            if create && index + 1 == component_count {
                immediate_parent = Some((directory.try_clone()?, directory_path.clone()));
            }
            let (child, identity) = open_directory_component(&directory, &directory_path, &name, create)?;
            directory_path.push(&name);
            steps.push(DirectoryStep { name, identity });
            directory = child;
        }
        let identity = directory_identity(&directory)?;
        let secured = Self {
            path: absolute,
            anchor,
            steps,
            directory,
            identity,
        };
        secured.verify()?;
        if let Some((parent, path)) = immediate_parent {
            sync_directory(&parent, &path)?;
        }
        Ok(secured)
    }

    pub(super) fn write_atomically(
        &self,
        target_name: &str,
        bytes: &[u8],
        mut observer: impl FnMut(WriteStep),
    ) -> io::Result<()> {
        self.verify()?;
        if self.confirm_existing_durable(target_name, bytes, &mut observer)? {
            return Ok(());
        }

        let temporary_name = format!(".{target_name}.{}.tmp", Uuid::now_v7());
        let mut temporary = self.create_file(&temporary_name)?;
        let temporary_identity = file_identity(temporary.try_clone()?)?;
        if let Err(error) = temporary.write_all(bytes).and_then(|()| temporary.sync_all()) {
            let _ = self.directory.remove_file(&temporary_name);
            return Err(error);
        }
        observer(WriteStep::TemporarySynced);

        self.verify()?;
        match self.directory.rename(&temporary_name, &self.directory, target_name) {
            Ok(()) => {}
            Err(rename_error) => {
                let _ = self.directory.remove_file(&temporary_name);
                return self
                    .confirm_existing_durable(target_name, bytes, &mut observer)
                    .and_then(|confirmed| if confirmed { Ok(()) } else { Err(rename_error) });
            }
        }
        observer(WriteStep::Renamed);

        let target = self.open_regular_file(target_name, true)?;
        let target_identity = file_identity(target.try_clone()?)?;
        if !temporary_identity.same_object(&target_identity) {
            return Err(io::Error::other("effect output changed during atomic rename"));
        }
        target.sync_all()?;
        sync_directory(&self.directory, &self.path)?;
        observer(WriteStep::ParentSynced);
        let current = self.open_regular_file(target_name, false)?;
        let current_identity = file_identity(current)?;
        if !target_identity.same_object(current_identity.as_ref()) {
            return Err(io::Error::other("effect output changed after durable rename"));
        }
        self.verify()?;
        Ok(())
    }

    fn confirm_existing_durable(
        &self,
        target_name: &str,
        expected: &[u8],
        observer: &mut impl FnMut(WriteStep),
    ) -> io::Result<bool> {
        let expected_len =
            u64::try_from(expected.len()).map_err(|_| io::Error::other("protected blob is too large"))?;
        let Some(existing) = self.read_optional(target_name, Some(expected_len))? else {
            return Ok(false);
        };
        if existing != expected {
            return Err(io::Error::other("effect output reference collision"));
        }
        self.verify()?;
        let target = self.open_regular_file(target_name, true)?;
        let target_identity = file_identity(target.try_clone()?)?;
        target.sync_all()?;
        sync_directory(&self.directory, &self.path)?;
        observer(WriteStep::ParentSynced);
        let current = self.open_regular_file(target_name, false)?;
        let current_identity = file_identity(current)?;
        if !target_identity.same_object(current_identity.as_ref()) {
            return Err(io::Error::other("effect output changed while confirming durability"));
        }
        self.verify()?;
        Ok(true)
    }

    pub(super) fn read_file(&self, name: &str) -> io::Result<Vec<u8>> {
        self.read_optional(name, None)?
            .ok_or_else(|| io::Error::new(ErrorKind::NotFound, "effect output does not exist"))
    }

    pub(super) fn read_file_bounded(&self, name: &str, max_bytes: u64) -> io::Result<Vec<u8>> {
        self.read_optional(name, Some(max_bytes))?
            .ok_or_else(|| io::Error::new(ErrorKind::NotFound, "protected blob does not exist"))
    }

    pub(super) fn remove_child_directory(&self, name: &str) -> io::Result<()> {
        self.verify()?;
        let metadata = match self.directory.symlink_metadata(name) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(contextual(error, "inspect effect output Run directory")),
        };
        require_directory_metadata(&metadata)?;
        let child = self
            .directory
            .open_dir(name)
            .map_err(|error| contextual(error, "open effect output Run directory"))?;
        let child_identity = directory_identity(&child)?;
        self.verify_child_directory(name, child_identity.as_ref())?;
        let child_path = self.path.join(name);

        let mut files = Vec::new();
        for entry in child
            .entries()
            .map_err(|error| contextual(error, "list effect output Run directory"))?
        {
            let entry = entry.map_err(|error| contextual(error, "read effect output Run entry"))?;
            let entry_name = entry.file_name();
            validate_child_name(&entry_name)?;
            let metadata = child
                .symlink_metadata(&entry_name)
                .map_err(|error| contextual(error, "inspect effect output Run entry"))?;
            require_regular_metadata(&metadata)?;
            let file = open_regular_file_in(&child, &entry_name, false)?;
            files.push(ValidatedChildFile {
                name: entry_name,
                identity: file_identity(file)?,
            });
        }

        for file in &files {
            let current = open_regular_file_in(&child, &file.name, false)?;
            let current_identity = file_identity(current)?;
            if !file.identity.same_object(current_identity.as_ref()) {
                return Err(io::Error::other("effect output changed during Run deletion"));
            }
            child
                .remove_file(&file.name)
                .map_err(|error| contextual(error, "delete effect output blob"))?;
        }
        sync_directory(&child, &child_path)?;
        self.verify_child_directory(name, child_identity.as_ref())?;
        drop(files);
        drop(child);
        drop(child_identity);
        self.verify()?;
        self.directory
            .remove_dir(name)
            .map_err(|error| contextual(error, "delete effect output Run directory"))?;
        sync_directory(&self.directory, &self.path)?;
        self.verify()?;
        Ok(())
    }

    fn read_optional(&self, name: &str, max_bytes: Option<u64>) -> io::Result<Option<Vec<u8>>> {
        self.verify()?;
        match self.directory.symlink_metadata(name) {
            Ok(metadata) => require_regular_metadata(&metadata)?,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(contextual(error, "inspect effect output")),
        }
        let mut file = self.open_regular_file(name, false)?;
        let identity = file_identity(file.try_clone()?)?;
        if let Some(limit) = max_bytes
            && file.metadata()?.len() > limit
        {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "protected blob exceeds its size limit",
            ));
        }
        let mut bytes = Vec::new();
        match max_bytes {
            Some(limit) => {
                file.take(limit.saturating_add(1)).read_to_end(&mut bytes)?;
                if u64::try_from(bytes.len()).map_or(true, |length| length > limit) {
                    return Err(io::Error::new(
                        ErrorKind::InvalidData,
                        "protected blob exceeds its size limit",
                    ));
                }
            }
            None => {
                file.read_to_end(&mut bytes)?;
            }
        }
        let current = self.open_regular_file(name, false)?;
        let current_identity = file_identity(current)?;
        if !identity.same_object(&current_identity) {
            return Err(io::Error::other("effect output changed while reading"));
        }
        self.verify()?;
        Ok(Some(bytes))
    }

    fn create_file(&self, name: &str) -> io::Result<File> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        configure_nofollow(&mut options);
        #[cfg(unix)]
        {
            use cap_std::fs::OpenOptionsExt;

            options.mode(0o600);
        }
        let file = self.directory.open_with(name, &options)?.into_std();
        require_opened_regular_file(&file)?;
        Ok(file)
    }

    fn open_regular_file(&self, name: &str, write: bool) -> io::Result<File> {
        open_regular_file_in(&self.directory, name, write)
    }

    fn verify(&self) -> io::Result<()> {
        let mut current = self.anchor.try_clone()?;
        for step in &self.steps {
            let metadata = current
                .symlink_metadata(&step.name)
                .map_err(|error| contextual(error, "verify effect output path component"))?;
            require_directory_metadata(&metadata)?;
            let child = current
                .open_dir(&step.name)
                .map_err(|error| contextual(error, "reopen effect output path component"))?;
            let identity = directory_identity(&child)?;
            if !step.identity.same_object(&identity) {
                return Err(io::Error::other("effect output path component identity changed"));
            }
            current = child;
        }
        let identity = directory_identity(&current)?;
        if !self.identity.same_object(&identity) {
            return Err(io::Error::other("effect output root identity changed"));
        }
        Ok(())
    }

    fn verify_child_directory(&self, name: &str, expected: &OpenedFileIdentity) -> io::Result<()> {
        self.verify()?;
        let metadata = self
            .directory
            .symlink_metadata(name)
            .map_err(|error| contextual(error, "verify effect output Run directory"))?;
        require_directory_metadata(&metadata)?;
        let current = self
            .directory
            .open_dir(name)
            .map_err(|error| contextual(error, "reopen effect output Run directory"))?;
        if !expected.same_object(directory_identity(&current)?.as_ref()) {
            return Err(io::Error::other("effect output Run directory identity changed"));
        }
        Ok(())
    }
}

pub(super) fn paths_refer_to_same_directory(left: &Path, right: &Path) -> io::Result<bool> {
    match SecureDirectory::open_existing(left) {
        Ok(left) => left.same_as_path(right),
        Err(error) if error.kind() == ErrorKind::NotFound => compare_missing_directory_paths(left, right),
        Err(error) => Err(error),
    }
}

fn compare_missing_directory_paths(left: &Path, right: &Path) -> io::Result<bool> {
    let (left_ancestor, left_suffix) = nearest_existing_directory(left)?;
    let (right_ancestor, right_suffix) = nearest_existing_directory(right)?;
    Ok(left_ancestor.same_object(right_ancestor.as_ref())
        && left_suffix.len() == right_suffix.len()
        && left_suffix
            .iter()
            .zip(right_suffix.iter())
            .all(|(left, right)| directory_names_equal(left, right)))
}

fn nearest_existing_directory(path: &Path) -> io::Result<(Arc<OpenedFileIdentity>, Vec<OsString>)> {
    let normalized = normalized_absolute_directory_path(path)?;
    let (anchor, mut names) = split_absolute_directory_path(&normalized)?;
    let mut missing = Vec::new();
    loop {
        if names.is_empty() {
            let directory = Dir::open_ambient_dir(&anchor, ambient_authority())
                .map_err(|error| contextual(error, "open effect output path anchor"))?;
            missing.reverse();
            return Ok((directory_identity(&directory)?, missing));
        }
        let mut candidate = anchor.clone();
        candidate.extend(&names);
        match SecureDirectory::open_existing(&candidate) {
            Ok(directory) => {
                missing.reverse();
                return Ok((directory.identity, missing));
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {
                let Some(name) = names.pop() else {
                    return Err(io::Error::other("missing effect output path component"));
                };
                missing.push(name);
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(any(windows, target_os = "macos"))]
fn directory_names_equal(left: &OsStr, right: &OsStr) -> bool {
    left.to_string_lossy().to_lowercase() == right.to_string_lossy().to_lowercase()
}

#[cfg(not(any(windows, target_os = "macos")))]
fn directory_names_equal(left: &OsStr, right: &OsStr) -> bool {
    left == right
}

fn open_regular_file_in(directory: &Dir, name: impl AsRef<Path>, write: bool) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(!write).write(write);
    configure_nofollow(&mut options);
    let file = directory
        .open_with(name, &options)
        .map_err(|error| contextual(error, "open effect output"))?
        .into_std();
    require_opened_regular_file(&file)?;
    Ok(file)
}

fn validate_child_name(name: &OsStr) -> io::Result<()> {
    let mut components = Path::new(name).components();
    if matches!(
        (components.next(), components.next()),
        (Some(Component::Normal(_)), None)
    ) {
        Ok(())
    } else {
        Err(io::Error::other("invalid effect output Run entry name"))
    }
}

fn open_directory_component(
    parent: &Dir,
    parent_path: &Path,
    name: &OsStr,
    create: bool,
) -> io::Result<(Dir, Arc<OpenedFileIdentity>)> {
    let mut created = false;
    match parent.symlink_metadata(name) {
        Ok(metadata) => require_directory_metadata(&metadata)?,
        Err(error) if error.kind() == ErrorKind::NotFound && create => match parent.create_dir(name) {
            Ok(()) => created = true,
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
            Err(error) => return Err(contextual(error, "create effect output path component")),
        },
        Err(error) => return Err(contextual(error, "inspect effect output path component")),
    }
    let metadata = parent
        .symlink_metadata(name)
        .map_err(|error| contextual(error, "inspect created effect output path component"))?;
    require_directory_metadata(&metadata)?;
    let directory = parent
        .open_dir(name)
        .map_err(|error| contextual(error, "open effect output path component"))?;
    let identity = directory_identity(&directory)?;
    let current_metadata = parent
        .symlink_metadata(name)
        .map_err(|error| contextual(error, "reinspect effect output path component"))?;
    require_directory_metadata(&current_metadata)?;
    let current = parent
        .open_dir(name)
        .map_err(|error| contextual(error, "reopen effect output path component"))?;
    let current_identity = directory_identity(&current)?;
    if !identity.same_object(current_identity.as_ref()) {
        return Err(io::Error::other("effect output path component changed while opening"));
    }
    if created {
        sync_directory(parent, parent_path)?;
    }
    Ok((directory, identity))
}

fn require_directory_metadata(metadata: &CapMetadata) -> io::Result<()> {
    if !metadata.is_dir() || metadata_is_redirected(metadata) {
        return Err(io::Error::other("effect output path component is redirected"));
    }
    Ok(())
}

fn require_regular_metadata(metadata: &CapMetadata) -> io::Result<()> {
    if !metadata.is_file() || metadata_is_redirected(metadata) {
        return Err(io::Error::other("effect output is not a regular unredirected file"));
    }
    Ok(())
}

fn require_opened_regular_file(file: &File) -> io::Result<()> {
    let metadata = file.metadata()?;
    if !metadata.is_file() || std_metadata_is_redirected(&metadata) || link_count(file)? != 1 {
        return Err(io::Error::other("effect output is redirected or multiply linked"));
    }
    Ok(())
}

fn directory_identity(directory: &Dir) -> io::Result<Arc<OpenedFileIdentity>> {
    OpenedFileIdentity::from_owned_file(directory.try_clone()?.into_std_file()).map(Arc::new)
}

fn file_identity(file: File) -> io::Result<Arc<OpenedFileIdentity>> {
    OpenedFileIdentity::from_owned_file(file).map(Arc::new)
}

fn absolute_path(path: &Path) -> io::Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        env::current_dir().map(|current| current.join(path))
    }
}

fn normalized_absolute_directory_path(path: &Path) -> io::Result<PathBuf> {
    let absolute = absolute_path(path)?;
    let (mut normalized, names) = split_absolute_directory_path(&absolute)?;
    normalized.extend(names);
    Ok(normalized)
}

fn split_absolute_directory_path(path: &Path) -> io::Result<(PathBuf, Vec<OsString>)> {
    if !path.is_absolute() {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "effect output root is relative",
        ));
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
            _ => return Err(io::Error::new(ErrorKind::InvalidInput, "invalid effect output root")),
        }
    }
    if !saw_root || names.is_empty() {
        return Err(io::Error::new(ErrorKind::InvalidInput, "invalid effect output root"));
    }
    Ok((anchor, names))
}

fn contextual(error: io::Error, message: &'static str) -> io::Error {
    io::Error::new(error.kind(), message)
}

#[cfg(unix)]
fn configure_nofollow(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt;

    options.custom_flags(libc::O_NOFOLLOW);
}

#[cfg(windows)]
fn configure_nofollow(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;

    options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
}

#[cfg(not(any(unix, windows)))]
fn configure_nofollow(_: &mut OpenOptions) {}

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
fn link_count(file: &File) -> io::Result<u64> {
    use std::os::unix::fs::MetadataExt;

    file.metadata().map(|metadata| metadata.nlink())
}

#[cfg(windows)]
fn link_count(file: &File) -> io::Result<u64> {
    use std::mem;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle};

    let mut information: BY_HANDLE_FILE_INFORMATION = unsafe { mem::zeroed() };
    let succeeded = unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut information) };
    if succeeded == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(u64::from(information.nNumberOfLinks))
    }
}

#[cfg(not(any(unix, windows)))]
fn link_count(_: &File) -> io::Result<u64> {
    // These targets do not expose a portable link-count API. New blobs are
    // created with create_new and existing blobs are never rewritten.
    Ok(1)
}

#[cfg(not(windows))]
fn sync_directory(directory: &Dir, _: &Path) -> io::Result<()> {
    directory.try_clone()?.into_std_file().sync_all()
}

#[cfg(windows)]
fn sync_directory(directory: &Dir, path: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::io::{AsRawHandle, FromRawHandle};
    use std::ptr;
    use windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE;
    use windows_sys::Win32::Storage::FileSystem::{
        CreateFileW, FILE_APPEND_DATA, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
        FILE_FLAG_WRITE_THROUGH, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, FILE_WRITE_DATA,
        FlushFileBuffers, OPEN_EXISTING,
    };

    let mut encoded = path.as_os_str().encode_wide().collect::<Vec<_>>();
    if encoded.contains(&0) {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "effect output directory contains NUL",
        ));
    }
    encoded.push(0);
    let handle = unsafe {
        CreateFileW(
            encoded.as_ptr(),
            FILE_WRITE_DATA | FILE_APPEND_DATA,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            ptr::null(),
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_WRITE_THROUGH,
            ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        let error = io::Error::last_os_error();
        return Err(io::Error::new(
            error.kind(),
            format!("open effect output directory for durable sync: {error}"),
        ));
    }
    let reopened = unsafe { File::from_raw_handle(handle) };
    let metadata = reopened.metadata().map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("inspect effect output directory for durable sync: {error}"),
        )
    })?;
    if !metadata.is_dir() || std_metadata_is_redirected(&metadata) {
        return Err(io::Error::other("effect output directory is redirected while syncing"));
    }
    let expected = directory_identity(directory)?;
    let actual = OpenedFileIdentity::from_owned_file(reopened.try_clone()?)?;
    if !expected.same_object(&actual) {
        return Err(io::Error::other(
            "effect output directory identity changed while syncing",
        ));
    }
    let result = unsafe { FlushFileBuffers(reopened.as_raw_handle()) };
    if result == 0 {
        let error = io::Error::last_os_error();
        return Err(io::Error::new(
            error.kind(),
            format!("flush effect output directory for durable sync: {error}"),
        ));
    }
    Ok(())
}
