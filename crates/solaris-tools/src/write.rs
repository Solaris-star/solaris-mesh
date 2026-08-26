use std::ffi::OsStr;
use std::io::{BufReader, ErrorKind, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use cap_std::ambient_authority;
use cap_std::fs::{Dir, OpenOptions};
use serde_json::{Value, json};

use solaris_config::file_identity::OpenedFileIdentity;
use solaris_protocol::events::ToolCategory;
use solaris_types::effect::{EffectClass, EffectDescriptor, EffectReplayPolicy, ResourceFootprint};
use solaris_types::permission::PermissionMode;
use solaris_types::tool::{JsonSchema, ToolResult};

use crate::file_cache::{FileStateCache, update_cache_after_verified_write};
use crate::{PreparedToolExecution, Tool, ToolExecutionContext};

#[path = "write_traversal.rs"]
mod write_traversal;

use write_traversal::collect_directory_paths;

pub(crate) const DEFAULT_SEARCH_MAX_ENTRIES: usize = 50_000;
const DEFAULT_SEARCH_MAX_DEPTH: usize = 64;
pub(crate) const SEARCH_CANDIDATE_UNREADABLE_ERROR_KIND: &str = "workspace_search_candidate_unreadable";

pub struct WriteTool {
    file_cache: Option<Arc<RwLock<FileStateCache>>>,
    file_access: WorkspaceFileAccess,
}

/// Decides which workspace paths recursive search tools may inspect.
///
/// The policy is consulted before a directory is entered or a file is opened.
/// Implementations may be dynamic; callers should not cache a decision.
pub trait WorkspaceSearchPolicy: Send + Sync {
    /// Refresh identities for protected state that may have appeared since
    /// the preceding tool operation. Implementations should keep this work
    /// bounded independently of workspace size.
    fn refresh(&self) -> Result<(), String> {
        Ok(())
    }

    fn allows_ambient_paths(&self) -> bool {
        false
    }

    fn scoped_to_permission_mode(&self, _mode: PermissionMode) -> Option<Arc<dyn WorkspaceSearchPolicy>> {
        None
    }

    fn allows_read(&self, path: &Path) -> bool;

    fn requires_opened_file_identity(&self) -> bool {
        false
    }

    fn allows_directory(&self, path: &Path) -> bool {
        self.allows_read(path)
    }

    fn allows_file(&self, path: &Path) -> bool {
        self.allows_read(path)
    }

    fn allows_opened_directory(&self, path: &Path, _identity: Arc<OpenedFileIdentity>) -> bool {
        self.allows_directory(path)
    }

    fn allows_file_slot(&self, path: &Path, _parent_identity: &OpenedFileIdentity, _file_name: &OsStr) -> bool {
        self.allows_file(path)
    }

    fn allows_opened_file(
        &self,
        path: &Path,
        _parent_identity: &OpenedFileIdentity,
        _file_name: &OsStr,
        _identity: Arc<OpenedFileIdentity>,
    ) -> bool {
        self.allows_file(path)
    }
}

#[derive(Default)]
struct AllowAllWorkspaceSearch;

impl WorkspaceSearchPolicy for AllowAllWorkspaceSearch {
    fn allows_read(&self, _path: &Path) -> bool {
        true
    }
}

#[derive(Clone)]
pub(crate) struct WorkspaceFileAccess {
    root_path: PathBuf,
    lexical_root_path: PathBuf,
    root: Option<Arc<Dir>>,
    initialization_error: Option<String>,
    search_policy: Arc<dyn WorkspaceSearchPolicy>,
    permission_mode: Option<PermissionMode>,
}

pub(crate) struct WorkspacePath {
    pub(crate) relative_path: PathBuf,
    pub(crate) modified: std::time::SystemTime,
    pub(crate) size: u64,
    pub(crate) identity: Option<Arc<OpenedFileIdentity>>,
}

pub(crate) struct WorkspaceReadSnapshot {
    pub(crate) bytes: Vec<u8>,
    pub(crate) identity: Arc<OpenedFileIdentity>,
}

pub(crate) struct WorkspaceOpenedReader {
    pub(crate) reader: BufReader<cap_std::fs::File>,
    pub(crate) modified: std::time::SystemTime,
    pub(crate) size: u64,
    pub(crate) identity: Arc<OpenedFileIdentity>,
}

#[derive(Clone, Copy)]
struct WorkspaceTraversalLimits {
    max_entries: usize,
    max_depth: usize,
}

struct OpenedDirectory {
    directory: Dir,
    identity: Arc<OpenedFileIdentity>,
    absolute_path: PathBuf,
}

struct OpenedFile {
    file: cap_std::fs::File,
    identity: Arc<OpenedFileIdentity>,
    metadata: cap_std::fs::Metadata,
}

static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

impl WorkspaceFileAccess {
    pub(crate) fn new(workspace_root: impl Into<PathBuf>) -> Self {
        Self::new_with_search_policy(workspace_root, Arc::new(AllowAllWorkspaceSearch))
    }

    pub(crate) fn new_with_search_policy(
        workspace_root: impl Into<PathBuf>,
        search_policy: Arc<dyn WorkspaceSearchPolicy>,
    ) -> Self {
        let requested = workspace_root.into();
        let lexical_root_path = absolute_lexical_path(&requested);
        let canonical = requested.canonicalize();
        match canonical {
            Ok(root_path) => match Dir::open_ambient_dir(&root_path, ambient_authority()) {
                Ok(root) => Self {
                    root_path,
                    lexical_root_path,
                    root: Some(Arc::new(root)),
                    initialization_error: None,
                    search_policy,
                    permission_mode: None,
                },
                Err(error) => Self {
                    root_path,
                    lexical_root_path,
                    root: None,
                    initialization_error: Some(format!("failed to open workspace root: {error}")),
                    search_policy,
                    permission_mode: None,
                },
            },
            Err(error) => Self {
                root_path: lexical_root_path.clone(),
                lexical_root_path,
                root: None,
                initialization_error: Some(format!("failed to canonicalize workspace root: {error}")),
                search_policy,
                permission_mode: None,
            },
        }
    }

    pub(crate) fn scoped_to_permission_mode(&self, mode: PermissionMode) -> Self {
        let search_policy = self
            .search_policy
            .scoped_to_permission_mode(mode)
            .unwrap_or_else(|| Arc::clone(&self.search_policy));
        Self {
            root_path: self.root_path.clone(),
            lexical_root_path: self.lexical_root_path.clone(),
            root: self.root.clone(),
            initialization_error: self.initialization_error.clone(),
            search_policy,
            permission_mode: Some(mode),
        }
    }

    fn root(&self) -> Result<&Dir, String> {
        self.root.as_deref().ok_or_else(|| {
            self.initialization_error
                .clone()
                .unwrap_or_else(|| "workspace root is unavailable".into())
        })
    }

    pub(crate) fn refresh_policy(&self) -> Result<(), String> {
        self.search_policy.refresh()
    }

    pub(crate) fn resolve_path(&self, requested: &Path) -> PathBuf {
        if requested.is_absolute() {
            requested.to_path_buf()
        } else {
            self.root_path.join(requested)
        }
    }

    pub(crate) fn relative_path(&self, requested: &Path) -> Result<PathBuf, String> {
        let absolute = normalize_absolute_path(&self.resolve_path(requested))?;
        if !absolute.starts_with(&self.lexical_root_path)
            && !absolute.starts_with(&self.root_path)
            && !self.ambient_paths_allowed()
        {
            return Err(format!("path {} is outside the workspace", requested.display()));
        }
        let resolved = resolve_existing_path(&absolute, requested)?;
        if let Ok(relative) = resolved.strip_prefix(&self.root_path) {
            return Ok(relative.to_path_buf());
        }
        if !self.ambient_paths_allowed() {
            return Err(format!("path {} is outside the workspace", requested.display()));
        }
        Ok(resolved)
    }

    pub(crate) fn open_reader(
        &self,
        requested: &Path,
        buffer_capacity: usize,
    ) -> Result<WorkspaceOpenedReader, String> {
        let relative = self.relative_path(requested)?;
        let opened = self.open_file(&relative)?;
        Ok(WorkspaceOpenedReader {
            reader: BufReader::with_capacity(buffer_capacity.max(1), opened.file),
            modified: opened
                .metadata
                .modified()
                .map(|value| value.into_std())
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH),
            size: opened.metadata.len(),
            identity: opened.identity,
        })
    }

    pub(crate) fn inspect_file_identity(
        &self,
        requested: &Path,
    ) -> Result<(Arc<OpenedFileIdentity>, std::time::SystemTime), String> {
        let relative = self.relative_path(requested)?;
        let opened = self.open_file(&relative)?;
        let modified = opened
            .metadata
            .modified()
            .map(|value| value.into_std())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        Ok((opened.identity, modified))
    }

    pub(crate) fn read_snapshot_limited(
        &self,
        requested: &Path,
        max_bytes: usize,
    ) -> Result<Option<WorkspaceReadSnapshot>, String> {
        let relative = self.relative_path(requested)?;
        let opened = self.open_file(&relative)?;
        if opened.metadata.len() > usize_to_u64_saturating(max_bytes) {
            return Ok(None);
        }
        let identity = opened.identity;
        let bytes =
            read_opened_file_limited(opened.file, max_bytes).map_err(|_| "failed to read workspace file".to_owned())?;
        Ok(bytes.map(|bytes| WorkspaceReadSnapshot { bytes, identity }))
    }

    pub(crate) fn collect_paths(&self, requested: &Path, limit: usize) -> Result<Vec<WorkspacePath>, String> {
        self.collect_paths_with_limits(
            requested,
            WorkspaceTraversalLimits {
                max_entries: limit,
                max_depth: DEFAULT_SEARCH_MAX_DEPTH,
            },
        )
    }

    fn collect_paths_with_limits(
        &self,
        requested: &Path,
        limits: WorkspaceTraversalLimits,
    ) -> Result<Vec<WorkspacePath>, String> {
        let relative = self.relative_path(requested)?;
        if let Ok(opened) = self.open_file(&relative) {
            return Ok(vec![WorkspacePath {
                relative_path: PathBuf::new(),
                modified: opened
                    .metadata
                    .modified()
                    .map(|value| value.into_std())
                    .unwrap_or(std::time::SystemTime::UNIX_EPOCH),
                size: opened.metadata.len(),
                identity: Some(opened.identity),
            }]);
        }
        let opened = self.open_directory(&relative, false)?;
        let mut files = Vec::new();
        collect_directory_paths(
            opened.directory,
            opened.identity,
            opened.absolute_path,
            limits,
            self.search_policy.as_ref(),
            &mut files,
        )?;
        Ok(files)
    }

    pub(crate) fn exists(&self, relative: &Path) -> Result<bool, String> {
        match self.open_file(relative) {
            Ok(_) => Ok(true),
            Err(error) if error.contains("not found") => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn write_atomic(&self, relative: &Path, content: &[u8]) -> Result<(), String> {
        self.write_atomic_if_unchanged(relative, content, None)
    }

    pub(crate) fn write_atomic_if_unchanged(
        &self,
        relative: &Path,
        content: &[u8],
        expected: Option<(&OpenedFileIdentity, &[u8])>,
    ) -> Result<(), String> {
        let opened_parent = self.open_directory(parent_path(relative), true)?;
        let file_name = relative
            .file_name()
            .ok_or_else(|| "workspace file path has no file name".to_owned())?;
        let absolute_path = opened_parent.absolute_path.join(file_name);
        let mut before = self.inspect_target(&opened_parent, file_name, &absolute_path)?;
        if !identity_matches(
            expected.map(|(identity, _)| identity),
            before.as_ref().map(|opened| &opened.identity),
        ) {
            return Err("workspace file changed before atomic write".to_owned());
        }
        if let Some((_, expected_content)) = expected
            && !opened_target_matches_content(before.as_mut(), expected_content)?
        {
            return Err("workspace file content changed before atomic write".to_owned());
        }
        let sequence = TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temp_name = format!(
            ".{}.tmp.{}.{}",
            file_name.to_string_lossy(),
            std::process::id(),
            sequence
        );
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        let mut file = opened_parent
            .directory
            .open_with(&temp_name, &options)
            .map_err(|error| format!("failed to create temporary workspace file: {error}"))?;
        if let Err(error) = file.write_all(content).and_then(|_| file.sync_all()) {
            let _ = opened_parent.directory.remove_file(&temp_name);
            return Err(format!("failed to write temporary workspace file: {error}"));
        }
        let temporary_identity = match file_identity(&file) {
            Ok(identity) => identity,
            Err(error) => {
                drop(file);
                let _ = opened_parent.directory.remove_file(&temp_name);
                return Err(error);
            }
        };
        drop(file);
        let mut current = self.inspect_target(&opened_parent, file_name, &absolute_path)?;
        if !optional_identity_matches(
            before.as_ref().map(|opened| &opened.identity),
            current.as_ref().map(|opened| &opened.identity),
        ) {
            let _ = opened_parent.directory.remove_file(&temp_name);
            return Err("workspace file changed during atomic write".to_owned());
        }
        if let Some((_, expected_content)) = expected {
            let content_matches = match opened_target_matches_content(current.as_mut(), expected_content) {
                Ok(matches) => matches,
                Err(error) => {
                    let _ = opened_parent.directory.remove_file(&temp_name);
                    return Err(error);
                }
            };
            if !content_matches {
                let _ = opened_parent.directory.remove_file(&temp_name);
                return Err("workspace file content changed during atomic write".to_owned());
            }
        }
        if let Err(error) = inspect_temporary_file(&opened_parent, OsStr::new(&temp_name), &temporary_identity) {
            let _ = opened_parent.directory.remove_file(&temp_name);
            return Err(error);
        }
        // `Dir::rename` is atomic, but neither cap-std nor the common Unix and
        // Windows replace APIs bind the operation to the source and destination
        // identities checked above. An adversary that can mutate this directory
        // can still replace either name after its check and before this rename.
        // Eliminating that final interval requires a platform-specific
        // handle-bound conditional replace primitive or preventing concurrent
        // directory mutation externally.
        if let Err(error) = opened_parent
            .directory
            .rename(&temp_name, &opened_parent.directory, file_name)
        {
            let _ = opened_parent.directory.remove_file(&temp_name);
            return Err(format!("failed to replace workspace file atomically: {error}"));
        }
        Ok(())
    }

    fn inspect_target(
        &self,
        parent: &OpenedDirectory,
        file_name: &OsStr,
        absolute_path: &Path,
    ) -> Result<Option<OpenedFile>, String> {
        if !self.search_policy.allows_file(absolute_path)
            || !self
                .search_policy
                .allows_file_slot(absolute_path, &parent.identity, file_name)
        {
            return Err("workspace file access denied by policy".to_owned());
        }
        match parent.directory.open(file_name) {
            Ok(file) => {
                let metadata = file
                    .metadata()
                    .map_err(|_| "failed to inspect opened workspace file".to_owned())?;
                if !metadata.is_file() {
                    return Err("workspace path is not a regular file".to_owned());
                }
                let identity = file_identity(&file)?;
                if !self.search_policy.allows_opened_file(
                    absolute_path,
                    &parent.identity,
                    file_name,
                    Arc::clone(&identity),
                ) {
                    return Err("workspace file access denied by policy".to_owned());
                }
                Ok(Some(OpenedFile {
                    file,
                    identity,
                    metadata,
                }))
            }
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(_) => Err("failed to open workspace file".to_owned()),
        }
    }

    fn open_file(&self, relative: &Path) -> Result<OpenedFile, String> {
        let file_name = relative
            .file_name()
            .ok_or_else(|| "workspace file path has no file name".to_owned())?;
        let parent = self.open_directory(parent_path(relative), false)?;
        let absolute_path = parent.absolute_path.join(file_name);
        if !self.search_policy.allows_file(&absolute_path)
            || !self
                .search_policy
                .allows_file_slot(&absolute_path, &parent.identity, file_name)
        {
            return Err("workspace file access denied by policy".to_owned());
        }
        let file = parent.directory.open(file_name).map_err(|error| {
            if error.kind() == ErrorKind::NotFound {
                "workspace file not found".to_owned()
            } else {
                "failed to open workspace file".to_owned()
            }
        })?;
        let metadata = file
            .metadata()
            .map_err(|_| "failed to inspect opened workspace file".to_owned())?;
        if !metadata.is_file() {
            return Err("workspace path is not a regular file".to_owned());
        }
        let identity = file_identity(&file)?;
        if !self
            .search_policy
            .allows_opened_file(&absolute_path, &parent.identity, file_name, Arc::clone(&identity))
        {
            return Err("workspace file access denied by policy".to_owned());
        }
        Ok(OpenedFile {
            file,
            identity,
            metadata,
        })
    }

    fn open_directory(&self, relative: &Path, create_missing: bool) -> Result<OpenedDirectory, String> {
        let (mut directory, mut absolute_path, relative) = self.open_access_root(relative)?;
        if !self.search_policy.allows_directory(&absolute_path) {
            return Err("workspace directory access denied by policy".to_owned());
        }
        let mut identity = directory_identity(&directory)?;
        if !self
            .search_policy
            .allows_opened_directory(&absolute_path, Arc::clone(&identity))
        {
            return Err("workspace directory access denied by policy".to_owned());
        }
        for component in relative.components() {
            let Component::Normal(name) = component else {
                return Err("workspace path contains an invalid component".to_owned());
            };
            absolute_path.push(name);
            if !self.search_policy.allows_directory(&absolute_path) {
                return Err("workspace directory access denied by policy".to_owned());
            }
            let child = match directory.open_dir(name) {
                Ok(child) => child,
                Err(error) if create_missing && error.kind() == ErrorKind::NotFound => {
                    directory
                        .create_dir(name)
                        .map_err(|_| "failed to create workspace directory".to_owned())?;
                    directory
                        .open_dir(name)
                        .map_err(|_| "failed to open created workspace directory".to_owned())?
                }
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    return Err("workspace directory not found".to_owned());
                }
                Err(_) => return Err("failed to open workspace directory".to_owned()),
            };
            let child_identity = directory_identity(&child)?;
            if !self
                .search_policy
                .allows_opened_directory(&absolute_path, Arc::clone(&child_identity))
            {
                return Err("workspace directory access denied by policy".to_owned());
            }
            directory = child;
            identity = child_identity;
        }
        Ok(OpenedDirectory {
            directory,
            identity,
            absolute_path,
        })
    }

    fn ambient_paths_allowed(&self) -> bool {
        if self.permission_mode.is_some_and(|mode| mode != PermissionMode::Bypass) {
            return false;
        }
        self.search_policy.allows_ambient_paths()
    }

    fn open_access_root(&self, path: &Path) -> Result<(Dir, PathBuf, PathBuf), String> {
        if path.is_absolute() {
            if !self.ambient_paths_allowed() {
                return Err("path is outside the workspace".to_owned());
            }
            let (anchor, relative) = split_absolute_path(path)?;
            let directory = Dir::open_ambient_dir(&anchor, ambient_authority())
                .map_err(|_| "failed to open filesystem root".to_owned())?;
            return Ok((directory, anchor, relative));
        }
        let directory = self
            .root()?
            .try_clone()
            .map_err(|_| "failed to clone workspace root handle".to_owned())?;
        Ok((directory, self.root_path.clone(), path.to_path_buf()))
    }
}

fn absolute_lexical_path(path: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|current| current.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
    };
    normalize_absolute_path(&absolute).unwrap_or(absolute)
}

fn normalize_absolute_path(path: &Path) -> Result<PathBuf, String> {
    if !path.is_absolute() {
        return Err("workspace path is not absolute".to_owned());
    }
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                normalized.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                if !matches!(normalized.components().next_back(), Some(Component::Normal(_))) {
                    return Err("workspace path contains an invalid component".to_owned());
                }
                normalized.pop();
            }
        }
    }
    Ok(normalized)
}

fn resolve_existing_path(absolute: &Path, requested: &Path) -> Result<PathBuf, String> {
    let mut existing = absolute;
    let mut missing = Vec::new();
    while !existing.exists() {
        let name = existing
            .file_name()
            .ok_or_else(|| format!("path {} cannot be resolved", requested.display()))?;
        missing.push(name.to_os_string());
        existing = existing
            .parent()
            .ok_or_else(|| format!("path {} cannot be resolved", requested.display()))?;
    }
    let mut resolved = existing
        .canonicalize()
        .map_err(|error| format!("failed to resolve path {}: {error}", requested.display()))?;
    for component in missing.into_iter().rev() {
        resolved.push(component);
    }
    Ok(resolved)
}

fn split_absolute_path(path: &Path) -> Result<(PathBuf, PathBuf), String> {
    let mut anchor = PathBuf::new();
    let mut relative = PathBuf::new();
    let mut reached_relative = false;
    for component in path.components() {
        match component {
            Component::Prefix(_) | Component::RootDir if !reached_relative => {
                anchor.push(component.as_os_str());
            }
            Component::Normal(_) => {
                reached_relative = true;
                relative.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir | Component::Prefix(_) | Component::RootDir => {
                return Err("workspace path contains an invalid component".to_owned());
            }
        }
    }
    if !anchor.is_absolute() {
        return Err("workspace path has no filesystem root".to_owned());
    }
    Ok((anchor, relative))
}

fn inspect_temporary_file(
    parent: &OpenedDirectory,
    temporary_name: &OsStr,
    expected: &OpenedFileIdentity,
) -> Result<(), String> {
    let file = parent
        .directory
        .open(temporary_name)
        .map_err(|_| "workspace temporary file changed before atomic write".to_owned())?;
    let metadata = file
        .metadata()
        .map_err(|_| "workspace temporary file changed before atomic write".to_owned())?;
    if !metadata.is_file() {
        return Err("workspace temporary file changed before atomic write".to_owned());
    }
    let current = file_identity(&file)?;
    if !expected.same_object(&current) {
        return Err("workspace temporary file changed before atomic write".to_owned());
    }
    Ok(())
}

fn read_opened_file_limited(file: cap_std::fs::File, max_bytes: usize) -> std::io::Result<Option<Vec<u8>>> {
    let limit = usize_to_u64_saturating(max_bytes);
    let read_limit = limit.saturating_add(1);
    let mut bytes = Vec::with_capacity(max_bytes.min(64 * 1024));
    file.take(read_limit).read_to_end(&mut bytes)?;
    Ok((bytes.len() <= max_bytes).then_some(bytes))
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

pub(crate) fn record_skipped_search_candidate() {
    tracing::debug!(
        error_kind = SEARCH_CANDIDATE_UNREADABLE_ERROR_KIND,
        "skipped unreadable workspace search candidate"
    );
}

fn file_identity(file: &cap_std::fs::File) -> Result<Arc<OpenedFileIdentity>, String> {
    let clone = file
        .try_clone()
        .map_err(|_| "failed to clone opened workspace file handle".to_owned())?;
    OpenedFileIdentity::from_owned_file(clone.into_std())
        .map(Arc::new)
        .map_err(|_| "failed to identify opened workspace file".to_owned())
}

fn directory_identity(directory: &Dir) -> Result<Arc<OpenedFileIdentity>, String> {
    let clone = directory
        .try_clone()
        .map_err(|_| "failed to clone opened workspace directory handle".to_owned())?;
    OpenedFileIdentity::from_owned_file(clone.into_std_file())
        .map(Arc::new)
        .map_err(|_| "failed to identify opened workspace directory".to_owned())
}

fn parent_path(path: &Path) -> &Path {
    path.parent().unwrap_or_else(|| Path::new(""))
}

fn identity_matches(expected: Option<&OpenedFileIdentity>, actual: Option<&Arc<OpenedFileIdentity>>) -> bool {
    match (expected, actual) {
        (Some(expected), Some(actual)) => expected.same_object(actual),
        (None, _) => true,
        (Some(_), None) => false,
    }
}

fn optional_identity_matches(
    before: Option<&Arc<OpenedFileIdentity>>,
    after: Option<&Arc<OpenedFileIdentity>>,
) -> bool {
    match (before, after) {
        (Some(before), Some(after)) => before.same_object(after),
        (None, None) => true,
        _ => false,
    }
}

fn opened_target_matches_content(opened: Option<&mut OpenedFile>, expected: &[u8]) -> Result<bool, String> {
    let Some(opened) = opened else {
        return Ok(false);
    };
    if opened.metadata.len() != usize_to_u64_saturating(expected.len()) {
        return Ok(false);
    }
    let mut offset = 0usize;
    let mut buffer = [0u8; 8192];
    while offset < expected.len() {
        let read = opened
            .file
            .read(&mut buffer)
            .map_err(|_| "failed to revalidate workspace file content".to_owned())?;
        if read == 0 || expected.get(offset..offset + read) != Some(&buffer[..read]) {
            return Ok(false);
        }
        offset += read;
    }
    let mut extra = [0u8; 1];
    opened
        .file
        .read(&mut extra)
        .map(|read| read == 0)
        .map_err(|_| "failed to revalidate workspace file content".to_owned())
}

impl WriteTool {
    /// Create a WriteTool with optional file state cache.
    ///
    /// When cache is `Some`, the tool updates the cache after each successful
    /// write so that subsequent Edit/Read calls see the latest content and mtime.
    ///
    /// No "must Read first" guard: Write is intended for creating new files
    /// or complete rewrites.
    ///
    /// Pass `None` to disable cache integration (legacy behavior).
    pub fn new(file_cache: Option<Arc<RwLock<FileStateCache>>>) -> Self {
        Self::new_with_workspace_root(
            file_cache,
            std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
        )
    }

    pub fn new_with_workspace_root(
        file_cache: Option<Arc<RwLock<FileStateCache>>>,
        workspace_root: impl Into<PathBuf>,
    ) -> Self {
        Self {
            file_cache,
            file_access: WorkspaceFileAccess::new(workspace_root),
        }
    }

    pub fn new_with_search_policy(
        file_cache: Option<Arc<RwLock<FileStateCache>>>,
        workspace_root: impl Into<PathBuf>,
        search_policy: Arc<dyn WorkspaceSearchPolicy>,
    ) -> Self {
        Self {
            file_cache,
            file_access: WorkspaceFileAccess::new_with_search_policy(workspace_root, search_policy),
        }
    }
}

#[async_trait]
impl Tool for WriteTool {
    fn name(&self) -> &str {
        "Write"
    }

    fn description(&self) -> &str {
        "Writes content to a file, creating parent directories if needed.\n\n\
         Usage:\n\
         - This tool overwrites the existing file completely (not append).\n\
         - If the file already exists, you must use Read first to see its current content.\n\
         - Prefer Edit over Write for modifying existing files — Edit only sends the diff.\n\
         - Use Write only for creating new files or complete rewrites."
    }

    fn input_schema(&self) -> JsonSchema {
        json!({
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "The absolute path to the file to write"
                },
                "content": {
                    "type": "string",
                    "description": "The content to write to the file"
                }
            },
            "required": ["file_path", "content"]
        })
    }

    fn is_concurrency_safe(&self, _input: &Value) -> bool {
        false
    }

    fn prepare_execution<'a>(
        &'a self,
        input: Value,
        context: ToolExecutionContext,
    ) -> Result<PreparedToolExecution<'a>, String> {
        let mode = context
            .permission_mode()
            .ok_or_else(|| "missing effective permission mode for file tool execution".to_owned())?;
        let tool = Self {
            file_cache: self.file_cache.clone(),
            file_access: self.file_access.scoped_to_permission_mode(mode),
        };
        Ok(PreparedToolExecution::new(
            None,
            Box::pin(async move { tool.execute(input).await }),
        ))
    }

    async fn execute(&self, input: Value) -> ToolResult {
        let Some(file_path) = input["file_path"].as_str() else {
            return ToolResult {
                content: "Missing required parameter: file_path".to_string(),
                is_error: true,
            };
        };
        let Some(content) = input["content"].as_str() else {
            return ToolResult {
                content: "Missing required parameter: content".to_string(),
                is_error: true,
            };
        };

        if let Err(error) = self.file_access.refresh_policy() {
            return ToolResult {
                content: format!("Failed to refresh workspace protection: {error}"),
                is_error: true,
            };
        }

        let path = Path::new(file_path);
        let relative = match self.file_access.relative_path(path) {
            Ok(relative) => relative,
            Err(error) => {
                return ToolResult {
                    content: format!("Failed to resolve file: {error}"),
                    is_error: true,
                };
            }
        };
        let existed = match self.file_access.exists(&relative) {
            Ok(existed) => existed,
            Err(error) => {
                return ToolResult {
                    content: format!("Failed to inspect file: {error}"),
                    is_error: true,
                };
            }
        };
        if let Err(error) = self.file_access.write_atomic(&relative, content.as_bytes()) {
            return ToolResult {
                content: format!("Failed to write file: {error}"),
                is_error: true,
            };
        }

        if let Some(cache_arc) = &self.file_cache {
            match self.file_access.inspect_file_identity(path) {
                Ok((identity, modified)) => {
                    update_cache_after_verified_write(cache_arc, path, content, identity, modified);
                }
                Err(_) => {
                    if let Ok(mut cache) = cache_arc.write() {
                        cache.remove(path);
                    }
                }
            }
        }

        let line_count = content.lines().count();
        let action = if existed { "Updated" } else { "Created" };
        ToolResult {
            content: format!("{} {} ({} lines)", action, file_path, line_count),
            is_error: false,
        }
    }

    fn describe_effect(&self, input: &Value) -> EffectDescriptor {
        let requested = input.get("file_path").and_then(Value::as_str).unwrap_or("unknown");
        let path = self.file_access.resolve_path(Path::new(requested));
        let path = path.to_string_lossy();
        EffectDescriptor {
            class: EffectClass::WorkspaceMutation,
            action: format!("Write {path}"),
            resources: ResourceFootprint {
                file_writes: vec![path.into_owned()],
                ..Default::default()
            },
            replay_policy: EffectReplayPolicy::ReconcileRequired,
        }
    }

    fn max_result_size(&self) -> usize {
        10_000
    }

    fn category(&self) -> ToolCategory {
        ToolCategory::Edit
    }

    fn describe(&self, input: &Value) -> String {
        let path = input.get("file_path").and_then(|v| v.as_str()).unwrap_or("unknown");
        format!("Write to {}", path)
    }
}

#[cfg(test)]
#[path = "write_test.rs"]
mod write_test;
