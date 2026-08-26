use super::*;

/// A workspace capability retained while runtime Skill directories are
/// discovered. Every discovered directory is opened relative to this handle.
pub(crate) struct SkillDiscoveryRoot {
    path: PathBuf,
    verification: Arc<WorkspaceVerification>,
}

/// An already-opened runtime Skill root together with the workspace-relative
/// identity chain that led to it.
pub(crate) struct DiscoveredSkillDirectory {
    path: PathBuf,
    verification: DiscoveredSkillVerification,
    directory: Dir,
    identity: Arc<OpenedFileIdentity>,
}

pub(super) struct DiscoveredSkillVerification {
    workspace: Arc<WorkspaceVerification>,
    lexical_skill_directory: LexicalDirectoryWitness,
    steps: Vec<BoundedDirectoryStep>,
}

struct WorkspaceVerification {
    canonical_anchor: Dir,
    canonical_steps: Vec<BoundedDirectoryStep>,
    canonical_directory: Dir,
    canonical_identity: Arc<OpenedFileIdentity>,
    lexical_workspace: LexicalDirectoryWitness,
}

struct LexicalDirectoryWitness {
    path: PathBuf,
    canonical_path: PathBuf,
    steps: Vec<LexicalDirectoryStep>,
}

struct LexicalDirectoryStep {
    path: PathBuf,
    identity: LexicalEntryIdentity,
}

#[cfg(unix)]
struct LexicalEntryIdentity {
    identity: Arc<OpenedFileIdentity>,
}

#[cfg(windows)]
struct LexicalEntryIdentity {
    identity: Arc<OpenedFileIdentity>,
}

#[cfg(not(any(unix, windows)))]
struct LexicalEntryIdentity;

impl SkillDiscoveryRoot {
    pub(crate) fn open(lexical_path: &Path, canonical_path: &Path) -> io::Result<Self> {
        let canonical_path = normalized_absolute_path(canonical_path)?;
        let (anchor_path, names) = split_absolute_directory_path(&canonical_path)?;
        let canonical_anchor = Dir::open_ambient_dir(&anchor_path, ambient_authority())?;
        let mut canonical_directory = canonical_anchor.try_clone()?;
        let mut canonical_steps = Vec::with_capacity(names.len());
        for name in names {
            require_directory_metadata(&canonical_directory.symlink_metadata(&name)?)?;
            let child = open_directory_in(&canonical_directory, &name)?;
            let identity = directory_identity(&child)?;
            let reopened = open_directory_in(&canonical_directory, &name)?;
            if !identity.same_object(directory_identity(&reopened)?.as_ref()) {
                return Err(io::Error::other("Skill workspace identity changed while opening"));
            }
            canonical_steps.push(BoundedDirectoryStep { name, identity });
            canonical_directory = child;
        }
        let canonical_identity = directory_identity(&canonical_directory)?;
        let lexical_workspace = LexicalDirectoryWitness::capture(lexical_path, &canonical_path)?;
        let verification = Arc::new(WorkspaceVerification {
            canonical_anchor,
            canonical_steps,
            canonical_directory,
            canonical_identity,
            lexical_workspace,
        });
        let root = Self {
            path: canonical_path,
            verification,
        };
        root.verify()?;
        Ok(root)
    }

    pub(crate) fn open_skill_directory(
        &self,
        lexical_path: &Path,
        canonical_path: &Path,
    ) -> io::Result<DiscoveredSkillDirectory> {
        let relative = canonical_path.strip_prefix(&self.path).map_err(|_| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "discovered Skill directory escaped its workspace capability",
            )
        })?;
        let names = safe_relative_names(relative)?;
        if names.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "discovered Skill directory is the workspace root",
            ));
        }

        self.verify()?;
        let lexical_skill_directory = LexicalDirectoryWitness::capture(lexical_path, canonical_path)?;
        let mut current = self.verification.canonical_directory.try_clone()?;
        let mut steps = Vec::with_capacity(names.len());
        for name in names {
            require_directory_metadata(&current.symlink_metadata(&name)?)?;
            let child = open_directory_in(&current, &name)?;
            let identity = directory_identity(&child)?;
            let reopened = open_directory_in(&current, &name)?;
            if !identity.same_object(directory_identity(&reopened)?.as_ref()) {
                return Err(io::Error::other(
                    "discovered Skill directory identity changed while opening",
                ));
            }
            steps.push(BoundedDirectoryStep { name, identity });
            current = child;
        }
        self.verify()?;
        verify_directory_steps(&self.verification.canonical_directory, &steps)?;
        let identity = directory_identity(&current)?;
        let discovered = DiscoveredSkillDirectory {
            path: canonical_path.to_path_buf(),
            verification: DiscoveredSkillVerification {
                workspace: Arc::clone(&self.verification),
                lexical_skill_directory,
                steps,
            },
            directory: current,
            identity,
        };
        discovered.verify()?;
        Ok(discovered)
    }

    fn verify(&self) -> io::Result<()> {
        self.verification.verify()
    }
}

impl DiscoveredSkillDirectory {
    pub(super) fn verify(&self) -> io::Result<()> {
        self.verification.verify()?;
        if self.verification.matches_root_identity(self.identity.as_ref())? {
            Ok(())
        } else {
            Err(io::Error::other("discovered Skill root identity changed"))
        }
    }

    pub(super) fn into_parts(self) -> (PathBuf, DiscoveredSkillVerification, Dir, Arc<OpenedFileIdentity>) {
        (self.path, self.verification, self.directory, self.identity)
    }
}

impl DiscoveredSkillVerification {
    pub(super) fn verify(&self) -> io::Result<()> {
        self.workspace.verify()?;
        self.lexical_skill_directory.verify()?;
        verify_directory_steps(&self.workspace.canonical_directory, &self.steps)
    }

    pub(super) fn matches_root_identity(&self, identity: &OpenedFileIdentity) -> io::Result<bool> {
        let last = self
            .steps
            .last()
            .ok_or_else(|| io::Error::other("discovered Skill identity chain is empty"))?;
        Ok(identity.same_object(last.identity.as_ref()))
    }
}

impl WorkspaceVerification {
    fn verify(&self) -> io::Result<()> {
        self.lexical_workspace.verify()?;
        verify_directory_steps(&self.canonical_anchor, &self.canonical_steps)?;
        let current = self
            .canonical_steps
            .last()
            .ok_or_else(|| io::Error::other("Skill workspace identity chain is empty"))?;
        if !self.canonical_identity.same_object(current.identity.as_ref()) {
            return Err(io::Error::other("Skill workspace identity changed"));
        }
        Ok(())
    }
}

impl LexicalDirectoryWitness {
    fn capture(path: &Path, canonical_path: &Path) -> io::Result<Self> {
        let path = normalized_absolute_path(path)?;
        let (anchor_path, names) = split_absolute_directory_path(&path)?;
        let mut current = anchor_path;
        let mut steps = Vec::with_capacity(names.len());
        for name in names {
            current.push(name);
            let metadata = std::fs::symlink_metadata(&current)?;
            require_lexical_directory_entry(&current, &metadata)?;
            steps.push(LexicalDirectoryStep {
                path: current.clone(),
                identity: lexical_entry_identity(&current, &metadata)?,
            });
        }
        let witness = Self {
            path,
            canonical_path: canonical_path.to_path_buf(),
            steps,
        };
        witness.verify()?;
        Ok(witness)
    }

    fn verify(&self) -> io::Result<()> {
        for step in &self.steps {
            let metadata = std::fs::symlink_metadata(&step.path)?;
            require_lexical_directory_entry(&step.path, &metadata)?;
            if !lexical_entry_matches(&step.identity, &step.path, &metadata)? {
                return Err(io::Error::other("lexical Skill workspace path identity changed"));
            }
        }
        if std::fs::canonicalize(&self.path)? != self.canonical_path {
            return Err(io::Error::other(
                "lexical Skill workspace now resolves to another directory",
            ));
        }
        Ok(())
    }
}

fn require_lexical_directory_entry(path: &Path, metadata: &std::fs::Metadata) -> io::Result<()> {
    if metadata.is_dir() {
        return Ok(());
    }
    if std_metadata_is_redirected(metadata) && std::fs::metadata(path)?.is_dir() {
        return Ok(());
    }
    Err(io::Error::new(
        io::ErrorKind::PermissionDenied,
        "lexical Skill workspace path contains a non-directory component",
    ))
}

fn split_absolute_directory_path(path: &Path) -> io::Result<(PathBuf, Vec<OsString>)> {
    if !path.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "dynamic Skill root is not absolute",
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
            Component::ParentDir | Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "dynamic Skill root is not normalized",
                ));
            }
        }
    }
    if !saw_root || names.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "dynamic Skill root has no directory component",
        ));
    }
    Ok((anchor, names))
}

#[cfg(unix)]
fn lexical_entry_identity(path: &Path, metadata: &std::fs::Metadata) -> io::Result<LexicalEntryIdentity> {
    open_unix_lexical_entry(path, metadata)
        .and_then(OpenedFileIdentity::from_owned_file)
        .map(Arc::new)
        .map(|identity| LexicalEntryIdentity { identity })
}

#[cfg(unix)]
fn lexical_entry_matches(
    expected: &LexicalEntryIdentity,
    path: &Path,
    metadata: &std::fs::Metadata,
) -> io::Result<bool> {
    let current = lexical_entry_identity(path, metadata)?;
    Ok(expected.identity.same_object(current.identity.as_ref()))
}

#[cfg(target_os = "linux")]
fn open_unix_lexical_entry(path: &Path, _: &std::fs::Metadata) -> io::Result<File> {
    use std::ffi::CString;
    use std::os::fd::FromRawFd;
    use std::os::unix::ffi::OsStrExt;

    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "lexical Skill path contains NUL"))?;
    let descriptor = unsafe { libc::open(path.as_ptr(), libc::O_PATH | libc::O_NOFOLLOW | libc::O_CLOEXEC) };
    if descriptor == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(descriptor) })
    }
}

#[cfg(target_os = "macos")]
fn open_unix_lexical_entry(path: &Path, metadata: &std::fs::Metadata) -> io::Result<File> {
    use std::ffi::CString;
    use std::os::fd::FromRawFd;
    use std::os::unix::ffi::OsStrExt;

    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "lexical Skill path contains NUL"))?;
    let flags = if std_metadata_is_redirected(metadata) {
        libc::O_SYMLINK | libc::O_CLOEXEC
    } else {
        libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC
    };
    let descriptor = unsafe { libc::open(path.as_ptr(), flags) };
    if descriptor == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(descriptor) })
    }
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
fn open_unix_lexical_entry(path: &Path, metadata: &std::fs::Metadata) -> io::Result<File> {
    use std::ffi::CString;
    use std::os::fd::FromRawFd;
    use std::os::unix::ffi::OsStrExt;

    if std_metadata_is_redirected(metadata) {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "live lexical Skill alias handles are unavailable on this platform",
        ));
    }
    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "lexical Skill path contains NUL"))?;
    let descriptor = unsafe {
        libc::open(
            path.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if descriptor == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(descriptor) })
    }
}

#[cfg(windows)]
fn lexical_entry_identity(path: &Path, _: &std::fs::Metadata) -> io::Result<LexicalEntryIdentity> {
    use std::os::windows::fs::OpenOptionsExt;

    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_SHARE_DELETE: u32 = 0x0000_0004;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;

    let mut options = std::fs::OpenOptions::new();
    options
        .access_mode(0)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS);
    let file = options.open(path)?;
    OpenedFileIdentity::from_owned_file(file)
        .map(Arc::new)
        .map(|identity| LexicalEntryIdentity { identity })
}

#[cfg(windows)]
fn lexical_entry_matches(
    expected: &LexicalEntryIdentity,
    path: &Path,
    metadata: &std::fs::Metadata,
) -> io::Result<bool> {
    let current = lexical_entry_identity(path, metadata)?;
    Ok(expected.identity.same_object(current.identity.as_ref()))
}

#[cfg(not(any(unix, windows)))]
fn lexical_entry_identity(_: &Path, _: &std::fs::Metadata) -> io::Result<LexicalEntryIdentity> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "lexical Skill path identities are unavailable on this platform",
    ))
}

#[cfg(not(any(unix, windows)))]
fn lexical_entry_matches(_: &LexicalEntryIdentity, _: &Path, _: &std::fs::Metadata) -> io::Result<bool> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "lexical Skill path identities are unavailable on this platform",
    ))
}
