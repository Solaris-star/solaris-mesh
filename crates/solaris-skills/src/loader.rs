use std::collections::{HashMap, HashSet};
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{self, Read};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use cap_std::ambient_authority;
use cap_std::fs::{Dir, Metadata as CapMetadata, OpenOptions};
use futures::future::join_all;
use solaris_config::file_identity::{OpenedFileIdentity, opened_file_link_count};

use crate::bundled;
use crate::frontmatter::{parse_frontmatter, parse_skill_fields};
use crate::mcp::load_mcp_skills;
use crate::paths::{
    additional_skills_dirs, project_commands_dirs, project_skills_dirs, user_commands_dir, user_skills_dir,
};
use crate::types::{LoadedFrom, SkillMetadata, SkillSource};
use solaris_mcp::manager::McpManager;

#[path = "loader_capability.rs"]
mod loader_capability;
use loader_capability::DiscoveredSkillVerification;
pub(crate) use loader_capability::{DiscoveredSkillDirectory, SkillDiscoveryRoot};
#[path = "loader_manifest.rs"]
mod loader_manifest;
use loader_manifest::verify_opened_manifest_state;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A loaded skill paired with its canonical filesystem path for deduplication.
pub struct LoadedSkill {
    pub metadata: SkillMetadata,
    /// Canonicalized path used for dedup (symlinks resolved, `.`/`..` removed).
    pub resolved_path: PathBuf,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Load all skills from the filesystem and optionally from MCP servers.
///
/// Priority order (highest first): bundled → MCP → user → project → additional → legacy.
/// Deduplicates first by canonical path (symlinks resolved), then by name (first wins).
/// Bundled skills always take precedence over same-named MCP or filesystem skills.
///
/// If `bare` is true, only `add_dirs` are consulted (used for isolated
/// environments where the user/project directories should be ignored).
/// Bundled skills are included in bare mode as well.
///
/// Pass `mcp_manager: Some(&manager)` to include MCP-discovered skills.
pub async fn load_all_skills(
    cwd: &Path,
    add_dirs: &[PathBuf],
    bare: bool,
    mcp_manager: Option<&McpManager>,
) -> Vec<SkillMetadata> {
    // Resolve bundled skills with file extraction (async context).
    let bundled_loaded = prepare_bundled_loaded().await;

    let mut all: Vec<LoadedSkill> = Vec::new();

    if bare {
        // Bare mode: only load from explicit add_dirs
        let dirs = additional_skills_dirs(add_dirs);
        let futures: Vec<_> = dirs
            .iter()
            .map(|d| load_skills_from_dir(d, SkillSource::Project, LoadedFrom::Skills))
            .collect();
        for batch in join_all(futures).await {
            all.extend(batch);
        }
        // Bundled skills prepended so they win deduplication
        all.splice(0..0, bundled_loaded);
        return deduplicate_by_name(deduplicate(all));
    }

    // 1. User-level skills (highest priority)
    if let Some(dir) = user_skills_dir()
        && dir.is_dir()
    {
        all.extend(load_skills_from_dir(&dir, SkillSource::User, LoadedFrom::Skills).await);
    }

    // 2. Project-level skills (parallel across all dirs)
    let project_dirs = project_skills_dirs(cwd);
    let futures: Vec<_> = project_dirs
        .iter()
        .map(|d| load_skills_from_dir(d, SkillSource::Project, LoadedFrom::Skills))
        .collect();
    for batch in join_all(futures).await {
        all.extend(batch);
    }

    // 3. Additional dirs from --add-dir
    let add_skill_dirs = additional_skills_dirs(add_dirs);
    let futures: Vec<_> = add_skill_dirs
        .iter()
        .map(|d| load_skills_from_dir(d, SkillSource::Project, LoadedFrom::Skills))
        .collect();
    for batch in join_all(futures).await {
        all.extend(batch);
    }

    // 4. User-level legacy commands (lowest user priority)
    if let Some(dir) = user_commands_dir()
        && dir.is_dir()
    {
        all.extend(load_skills_from_commands_dir(&dir, SkillSource::User).await);
    }

    // 5. Project-level legacy commands (parallel)
    let cmd_dirs = project_commands_dirs(cwd);
    let futures: Vec<_> = cmd_dirs
        .iter()
        .map(|d| load_skills_from_commands_dir(d, SkillSource::Project))
        .collect();
    for batch in join_all(futures).await {
        all.extend(batch);
    }

    // MCP skills inserted after bundled (highest priority) but before filesystem
    // skills, so: bundled > MCP > user > project > additional > legacy.
    let mcp_loaded = match mcp_manager {
        Some(mgr) => load_mcp_skills(mgr).await,
        None => Vec::new(),
    };

    // Bundled skills first, then MCP, then filesystem
    all.splice(0..0, mcp_loaded);
    all.splice(0..0, bundled_loaded);

    // Path-based dedup first (handles symlinked duplicates), then name-based
    // dedup to enforce MCP vs. filesystem priority.
    deduplicate_by_name(deduplicate(all))
}

/// Call `bundled::prepare_bundled_skills()` and wrap results as `LoadedSkill`.
///
/// Each bundled skill is assigned a virtual path `<bundled:name>` for
/// deduplication purposes (these paths can never match real filesystem paths).
async fn prepare_bundled_loaded() -> Vec<LoadedSkill> {
    bundled::prepare_bundled_skills()
        .await
        .into_iter()
        .map(|meta| {
            let virtual_path = PathBuf::from(format!("<bundled:{}>", meta.name));
            LoadedSkill {
                metadata: meta,
                resolved_path: virtual_path,
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Internal: load from skills/ directory (directory-only format)
// ---------------------------------------------------------------------------

/// Load skills from a `skills/` directory.
///
/// Only the directory format is supported: each direct or nested subdirectory
/// that contains a `SKILL.md` file (case-sensitive) is loaded.
/// The skill name is derived from the relative path using colon separators.
pub(crate) async fn load_skills_from_dir(
    base_dir: &Path,
    source: SkillSource,
    loaded_from: LoadedFrom,
) -> Vec<LoadedSkill> {
    let mut results = Vec::new();
    collect_skill_md(base_dir, base_dir, source, loaded_from, &mut results).await;
    results
}

/// Load dynamically discovered project skills while rejecting filesystem
/// redirects and multiply-linked manifests below the discovered root.
pub(crate) async fn load_bounded_skills_from_dir(
    base_dir: &Path,
    source: SkillSource,
    loaded_from: LoadedFrom,
) -> Vec<LoadedSkill> {
    load_bounded_skills_from_dir_observed(base_dir, source, loaded_from, |_, _| Ok(())).await
}

pub(crate) async fn load_discovered_skills_from_dir(
    discovered: DiscoveredSkillDirectory,
    source: SkillSource,
    loaded_from: LoadedFrom,
) -> Vec<LoadedSkill> {
    load_bounded_skills_from_root(
        move || BoundedSkillRoot::from_discovered(discovered),
        source,
        loaded_from,
        |_, _| Ok(()),
    )
    .await
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BoundedLoadRacePoint {
    BeforeChildDirectoryTraversal,
    BeforeManifestOpen,
    AfterManifestOpen,
}

#[cfg(test)]
async fn load_bounded_skills_from_dir_with_hook<F>(
    base_dir: &Path,
    source: SkillSource,
    loaded_from: LoadedFrom,
    hook: F,
) -> Vec<LoadedSkill>
where
    F: FnMut(BoundedLoadRacePoint, &Path) -> io::Result<()> + Send + 'static,
{
    load_bounded_skills_from_dir_observed(base_dir, source, loaded_from, hook).await
}

async fn load_bounded_skills_from_dir_observed<F>(
    base_dir: &Path,
    source: SkillSource,
    loaded_from: LoadedFrom,
    hook: F,
) -> Vec<LoadedSkill>
where
    F: FnMut(BoundedLoadRacePoint, &Path) -> io::Result<()> + Send + 'static,
{
    let base_dir = base_dir.to_path_buf();
    load_bounded_skills_from_root(move || BoundedSkillRoot::open(&base_dir), source, loaded_from, hook).await
}

async fn load_bounded_skills_from_root<F, R>(
    root_factory: R,
    source: SkillSource,
    loaded_from: LoadedFrom,
    mut hook: F,
) -> Vec<LoadedSkill>
where
    F: FnMut(BoundedLoadRacePoint, &Path) -> io::Result<()> + Send + 'static,
    R: FnOnce() -> io::Result<BoundedSkillRoot> + Send + 'static,
{
    let inspected = tokio::task::spawn_blocking(move || {
        let root = root_factory()?;
        let mut results = Vec::new();
        collect_bounded_skill_md(
            &root,
            &root.directory,
            &[],
            Path::new(""),
            source,
            loaded_from,
            &mut hook,
            &mut results,
        )?;
        Ok::<_, io::Error>(results)
    })
    .await;

    match inspected {
        Ok(Ok(results)) => results,
        Ok(Err(error)) => {
            tracing::warn!(
                target: "solaris_skills",
                error = %error,
                "skipping dynamic Skill root after a bounded filesystem check failed"
            );
            Vec::new()
        }
        Err(error) => {
            tracing::warn!(
                target: "solaris_skills",
                error = %error,
                "skipping dynamic Skill root after its inspector stopped"
            );
            Vec::new()
        }
    }
}

/// Recursively scan `dir` for `SKILL.md` files.
// This is a recursive async function — we use a Box::pin to satisfy the compiler.
fn collect_skill_md<'a>(
    base_dir: &'a Path,
    dir: &'a Path,
    source: SkillSource,
    loaded_from: LoadedFrom,
    results: &'a mut Vec<LoadedSkill>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
    Box::pin(async move {
        let mut read_dir = match tokio::fs::read_dir(dir).await {
            Ok(rd) => rd,
            Err(_) => return,
        };

        while let Ok(Some(entry)) = read_dir.next_entry().await {
            let path = entry.path();
            let scan_dir = match tokio::fs::metadata(&path).await {
                // Trusted sources retain the historical behavior of following
                // directory symlinks.
                Ok(meta) if meta.is_dir() => path,
                _ => continue,
            };

            // Check for SKILL.md directly inside this subdirectory using an
            // exact case-sensitive name comparison (important on case-insensitive
            // filesystems like macOS APFS).
            if let Some(skill_file) = find_exact_file(&scan_dir, "SKILL.md").await {
                if let Some(skill) = load_skill_file(&skill_file, base_dir, &scan_dir, source, loaded_from).await {
                    results.push(skill);
                }
            } else {
                // Recurse into subdirectory (namespace nesting)
                collect_skill_md(base_dir, &scan_dir, source, loaded_from, results).await;
            }
        }
    })
}

struct BoundedSkillRoot {
    path: PathBuf,
    verification: BoundedSkillRootVerification,
    directory: Dir,
    identity: Arc<OpenedFileIdentity>,
}

enum BoundedSkillRootVerification {
    Ambient { parent: Dir, name: OsString },
    Discovered(DiscoveredSkillVerification),
}

#[derive(Clone)]
struct BoundedDirectoryStep {
    name: std::ffi::OsString,
    identity: Arc<OpenedFileIdentity>,
}

impl BoundedSkillRoot {
    fn open(path: &Path) -> io::Result<Self> {
        let path = normalized_absolute_path(path)?;
        let name = path
            .file_name()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "dynamic Skill root has no directory name"))?
            .to_os_string();
        validate_child_name(&name)?;
        let parent_path = path.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "dynamic Skill root has no parent directory",
            )
        })?;
        let parent = Dir::open_ambient_dir(parent_path, ambient_authority())?;
        require_directory_metadata(&parent.symlink_metadata(&name)?)?;
        let directory = open_directory_in(&parent, &name)?;
        let identity = directory_identity(&directory)?;
        let root = Self {
            path,
            verification: BoundedSkillRootVerification::Ambient { parent, name },
            directory,
            identity,
        };
        root.verify()?;
        Ok(root)
    }

    fn verify(&self) -> io::Result<()> {
        match &self.verification {
            BoundedSkillRootVerification::Ambient { parent, name } => {
                verify_named_directory(parent, name, self.identity.as_ref())?;
            }
            BoundedSkillRootVerification::Discovered(verification) => {
                verification.verify()?;
                if !verification.matches_root_identity(self.identity.as_ref())? {
                    return Err(io::Error::other("discovered Skill root identity changed"));
                }
            }
        }
        Ok(())
    }

    fn from_discovered(discovered: DiscoveredSkillDirectory) -> io::Result<Self> {
        discovered.verify()?;
        let (path, verification, directory, identity) = discovered.into_parts();
        let root = Self {
            path,
            verification: BoundedSkillRootVerification::Discovered(verification),
            directory,
            identity,
        };
        root.verify()?;
        Ok(root)
    }
}

#[allow(clippy::too_many_arguments)]
fn collect_bounded_skill_md<F>(
    root: &BoundedSkillRoot,
    directory: &Dir,
    steps: &[BoundedDirectoryStep],
    relative: &Path,
    source: SkillSource,
    loaded_from: LoadedFrom,
    hook: &mut F,
    results: &mut Vec<LoadedSkill>,
) -> io::Result<()>
where
    F: FnMut(BoundedLoadRacePoint, &Path) -> io::Result<()>,
{
    verify_bounded_directory(root, steps)?;
    for entry in directory.entries()? {
        let entry = entry?;
        let name = entry.file_name();
        validate_child_name(&name)?;
        let metadata = directory.symlink_metadata(&name)?;
        if metadata_is_redirected(&metadata) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "dynamic Skill traversal encountered a filesystem redirect",
            ));
        }
        if !metadata.is_dir() {
            continue;
        }

        let child_relative = relative.join(&name);
        hook(BoundedLoadRacePoint::BeforeChildDirectoryTraversal, &child_relative)?;
        let child = open_directory_in(directory, &name)?;
        let child_identity = directory_identity(&child)?;
        let reopened = open_directory_in(directory, &name)?;
        if !child_identity.same_object(directory_identity(&reopened)?.as_ref()) {
            return Err(io::Error::other(
                "dynamic Skill directory identity changed while opening",
            ));
        }

        let mut child_steps = Vec::with_capacity(steps.len().saturating_add(1));
        child_steps.extend_from_slice(steps);
        child_steps.push(BoundedDirectoryStep {
            name,
            identity: child_identity,
        });
        verify_bounded_directory(root, &child_steps)?;

        if has_exact_bounded_manifest(&child)? {
            let skill =
                load_bounded_skill_file(root, &child, &child_steps, &child_relative, source, loaded_from, hook)?;
            results.push(skill);
        } else {
            collect_bounded_skill_md(
                root,
                &child,
                &child_steps,
                &child_relative,
                source,
                loaded_from,
                hook,
                results,
            )?;
        }
    }
    verify_bounded_directory(root, steps)
}

fn verify_bounded_directory(root: &BoundedSkillRoot, steps: &[BoundedDirectoryStep]) -> io::Result<()> {
    root.verify()?;
    verify_directory_steps(&root.directory, steps)
}

fn has_exact_bounded_manifest(directory: &Dir) -> io::Result<bool> {
    for entry in directory.entries()? {
        let entry = entry?;
        let name = entry.file_name();
        validate_child_name(&name)?;
        if name == OsStr::new("SKILL.md") {
            return Ok(true);
        }
    }
    Ok(false)
}

// ---------------------------------------------------------------------------
// Internal: load from commands/ directory (legacy flat + directory format)
// ---------------------------------------------------------------------------

/// Load skills from a legacy `commands/` directory.
///
/// Supports two formats:
/// - Directory format: `<name>/SKILL.md` (takes precedence over flat `.md`)
/// - Flat format: `<name>.md` or `<subdir>/<name>.md`
async fn load_skills_from_commands_dir(base_dir: &Path, source: SkillSource) -> Vec<LoadedSkill> {
    let mut results = Vec::new();
    collect_commands(base_dir, base_dir, source, &mut results).await;
    results
}

fn collect_commands<'a>(
    base_dir: &'a Path,
    dir: &'a Path,
    source: SkillSource,
    results: &'a mut Vec<LoadedSkill>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
    Box::pin(async move {
        let mut read_dir = match tokio::fs::read_dir(dir).await {
            Ok(rd) => rd,
            Err(_) => return,
        };

        // Collect all entries first so we can check for directory/flat conflicts
        let mut entries = Vec::new();
        while let Ok(Some(entry)) = read_dir.next_entry().await {
            entries.push(entry);
        }

        // Track names that have a directory format (to skip their flat counterpart)
        let mut dir_names: HashSet<String> = HashSet::new();

        // First pass: handle directory format
        for entry in &entries {
            let path = entry.path();
            // Follow symlinks: use metadata() which resolves symlink targets.
            let is_dir = match tokio::fs::metadata(&path).await {
                Ok(meta) => meta.is_dir(),
                Err(_) => continue,
            };

            if is_dir {
                // Use exact case-sensitive lookup to avoid false positives on
                // case-insensitive filesystems (e.g., macOS APFS).
                if let Some(skill_file) = find_exact_file(&path, "SKILL.md").await {
                    // Directory format — load it
                    if let Some(skill) =
                        load_skill_file(&skill_file, base_dir, &path, source, LoadedFrom::CommandsDeprecated).await
                    {
                        let name = path
                            .file_name()
                            .map(|n| n.to_string_lossy().into_owned())
                            .unwrap_or_default();
                        dir_names.insert(name);
                        results.push(skill);
                    }
                } else {
                    // Recurse: this is a namespace subdirectory (e.g., db/migrate.md)
                    collect_commands(base_dir, &path, source, results).await;
                }
            }
        }

        // Second pass: handle flat .md files (skip if directory version exists)
        for entry in &entries {
            let path = entry.path();
            // Follow symlinks: use metadata() to check if this is a file (not a dir symlink).
            let is_file = match tokio::fs::metadata(&path).await {
                Ok(meta) => meta.is_file(),
                Err(_) => continue,
            };

            if is_file && path.extension().and_then(|e| e.to_str()) == Some("md") {
                let stem = path
                    .file_stem()
                    .map(|s| s.to_string_lossy().into_owned())
                    .unwrap_or_default();

                // Skip if a directory format was already loaded for this name
                if dir_names.contains(&stem) {
                    continue;
                }

                // The "skill directory" for flat files is their parent dir + stem
                let pseudo_dir = path.parent().unwrap_or(base_dir).join(&stem);
                if let Some(skill) =
                    load_skill_file(&path, base_dir, &pseudo_dir, source, LoadedFrom::CommandsDeprecated).await
                {
                    results.push(skill);
                }
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Internal: load a single skill file
// ---------------------------------------------------------------------------

/// Read, parse, and return a `LoadedSkill` for a single Markdown file.
/// Returns `None` if the file cannot be read.
async fn load_skill_file(
    file_path: &Path,
    base_dir: &Path,
    skill_dir: &Path,
    source: SkillSource,
    loaded_from: LoadedFrom,
) -> Option<LoadedSkill> {
    let content = tokio::fs::read_to_string(file_path).await.ok()?;
    let resolved_path = try_canonicalize(file_path).unwrap_or_else(|| file_path.to_owned());
    Some(build_loaded_skill(
        &content,
        resolved_path,
        base_dir,
        skill_dir,
        source,
        loaded_from,
    ))
}

fn build_loaded_skill(
    content: &str,
    resolved_path: PathBuf,
    base_dir: &Path,
    skill_dir: &Path,
    source: SkillSource,
    loaded_from: LoadedFrom,
) -> LoadedSkill {
    let parsed = parse_frontmatter(content);

    let resolved_name = build_namespace(base_dir, skill_dir);
    // skill_root is the directory containing SKILL.md (i.e., skill_dir itself),
    // used for ${SOLARIS_SKILL_DIR} variable substitution in skill content.
    let skill_root = Some(skill_dir.to_string_lossy().into_owned());

    let metadata = parse_skill_fields(
        &parsed.frontmatter,
        &parsed.content,
        &resolved_name,
        source,
        loaded_from,
        skill_root.as_deref(),
    );

    LoadedSkill {
        metadata,
        resolved_path,
    }
}

#[allow(clippy::too_many_arguments)]
fn load_bounded_skill_file<F>(
    root: &BoundedSkillRoot,
    directory: &Dir,
    steps: &[BoundedDirectoryStep],
    skill_relative: &Path,
    source: SkillSource,
    loaded_from: LoadedFrom,
    hook: &mut F,
) -> io::Result<LoadedSkill>
where
    F: FnMut(BoundedLoadRacePoint, &Path) -> io::Result<()>,
{
    let manifest_name = OsStr::new("SKILL.md");
    require_regular_metadata(&directory.symlink_metadata(manifest_name)?)?;
    verify_bounded_directory(root, steps)?;
    let manifest_relative = skill_relative.join(manifest_name);
    hook(BoundedLoadRacePoint::BeforeManifestOpen, &manifest_relative)?;

    let mut file = open_regular_file_in(directory, manifest_name)?;
    let identity = file_identity(&file)?;
    let state = identity.current_state()?;
    let reopened = open_regular_file_in(directory, manifest_name)?;
    let reopened_identity = file_identity(&reopened)?;
    if !identity.same_object(reopened_identity.as_ref()) {
        return Err(io::Error::other(
            "dynamic Skill manifest identity changed while opening",
        ));
    }
    verify_opened_manifest_state(&reopened, reopened_identity.as_ref(), &state)?;
    hook(BoundedLoadRacePoint::AfterManifestOpen, &manifest_relative)?;
    verify_bounded_directory(root, steps)?;
    verify_opened_manifest_state(&file, identity.as_ref(), &state)?;

    let mut content = String::new();
    file.read_to_string(&mut content)?;
    verify_opened_manifest_state(&file, identity.as_ref(), &state)?;
    let final_reopened = open_regular_file_in(directory, manifest_name)?;
    let final_identity = file_identity(&final_reopened)?;
    if !identity.same_object(final_identity.as_ref()) {
        return Err(io::Error::other(
            "dynamic Skill manifest identity changed after reading",
        ));
    }
    verify_opened_manifest_state(&final_reopened, final_identity.as_ref(), &state)?;
    verify_bounded_directory(root, steps)?;
    let skill_dir = root.path.join(skill_relative);
    Ok(build_loaded_skill(
        &content,
        root.path.join(manifest_relative),
        &root.path,
        &skill_dir,
        source,
        loaded_from,
    ))
}

fn open_directory_in(parent: &Dir, name: &OsStr) -> io::Result<Dir> {
    validate_child_name(name)?;
    let mut options = OpenOptions::new();
    options.read(true);
    configure_directory_nofollow(&mut options);
    let file = parent.open_with(name, &options)?.into_std();
    let metadata = file.metadata()?;
    if !metadata.is_dir() || std_metadata_is_redirected(&metadata) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "dynamic Skill path component is redirected",
        ));
    }
    Ok(Dir::from_std_file(file))
}

fn open_regular_file_in(parent: &Dir, name: &OsStr) -> io::Result<File> {
    validate_child_name(name)?;
    let mut options = OpenOptions::new();
    options.read(true);
    configure_nofollow(&mut options);
    let file = parent.open_with(name, &options)?.into_std();
    let metadata = file.metadata()?;
    if !metadata.is_file() || std_metadata_is_redirected(&metadata) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "dynamic Skill manifest is redirected",
        ));
    }
    if opened_file_link_count(&file)? != 1 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "dynamic Skill manifest has another filesystem name",
        ));
    }
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
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid dynamic Skill path component",
        ))
    }
}

fn safe_relative_names(path: &Path) -> io::Result<Vec<OsString>> {
    path.components()
        .map(|component| match component {
            Component::Normal(name) => Ok(name.to_os_string()),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "discovered Skill path contains an unsafe component",
            )),
        })
        .collect()
}

fn verify_named_directory(parent: &Dir, name: &OsStr, expected: &OpenedFileIdentity) -> io::Result<()> {
    require_directory_metadata(&parent.symlink_metadata(name)?)?;
    let current = open_directory_in(parent, name)?;
    if expected.same_object(directory_identity(&current)?.as_ref()) {
        Ok(())
    } else {
        Err(io::Error::other("dynamic Skill root identity changed"))
    }
}

fn verify_directory_steps(root: &Dir, steps: &[BoundedDirectoryStep]) -> io::Result<()> {
    let mut current = root.try_clone()?;
    for step in steps {
        require_directory_metadata(&current.symlink_metadata(&step.name)?)?;
        let child = open_directory_in(&current, &step.name)?;
        if !step.identity.same_object(directory_identity(&child)?.as_ref()) {
            return Err(io::Error::other("dynamic Skill path component identity changed"));
        }
        current = child;
    }
    Ok(())
}

fn normalized_absolute_path(path: &Path) -> io::Result<PathBuf> {
    if path.components().any(|component| component == Component::ParentDir) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "dynamic Skill root contains a parent component",
        ));
    }
    std::path::absolute(path)
}

fn directory_identity(directory: &Dir) -> io::Result<Arc<OpenedFileIdentity>> {
    OpenedFileIdentity::from_owned_file(directory.try_clone()?.into_std_file()).map(Arc::new)
}

fn file_identity(file: &File) -> io::Result<Arc<OpenedFileIdentity>> {
    OpenedFileIdentity::from_owned_file(file.try_clone()?).map(Arc::new)
}

fn require_directory_metadata(metadata: &CapMetadata) -> io::Result<()> {
    if metadata.is_dir() && !metadata_is_redirected(metadata) {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "dynamic Skill path component is redirected",
        ))
    }
}

fn require_regular_metadata(metadata: &CapMetadata) -> io::Result<()> {
    if metadata.is_file() && !metadata_is_redirected(metadata) {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "dynamic Skill manifest is redirected",
        ))
    }
}

#[cfg(unix)]
fn configure_nofollow(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt;

    options.custom_flags(libc::O_NOFOLLOW);
}

#[cfg(windows)]
fn configure_nofollow(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt;

    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
}

#[cfg(not(any(unix, windows)))]
fn configure_nofollow(_: &mut OpenOptions) {}

#[cfg(unix)]
fn configure_directory_nofollow(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt;

    options.custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY);
}

#[cfg(windows)]
fn configure_directory_nofollow(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt;

    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS);
}

#[cfg(not(any(unix, windows)))]
fn configure_directory_nofollow(_: &mut OpenOptions) {}

#[cfg(unix)]
fn metadata_is_redirected(metadata: &CapMetadata) -> bool {
    metadata.is_symlink()
}

#[cfg(windows)]
fn metadata_is_redirected(metadata: &CapMetadata) -> bool {
    use cap_std::fs::MetadataExt;

    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
    metadata.is_symlink() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

#[cfg(not(any(unix, windows)))]
fn metadata_is_redirected(metadata: &CapMetadata) -> bool {
    metadata.is_symlink()
}

#[cfg(unix)]
fn std_metadata_is_redirected(metadata: &std::fs::Metadata) -> bool {
    metadata.file_type().is_symlink()
}

#[cfg(windows)]
fn std_metadata_is_redirected(metadata: &std::fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;

    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
    metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

#[cfg(not(any(unix, windows)))]
fn std_metadata_is_redirected(metadata: &std::fs::Metadata) -> bool {
    metadata.file_type().is_symlink()
}

// ---------------------------------------------------------------------------
// Internal: namespace building
// ---------------------------------------------------------------------------

/// Build a colon-separated namespace from a directory hierarchy.
///
/// Examples:
/// - base=`<config_dir>/solaris/skills`, target=`<config_dir>/solaris/skills/db/migrate` → `"db:migrate"`
/// - base=`<config_dir>/solaris/skills`, target=`<config_dir>/solaris/skills/my-skill` → `"my-skill"`
pub(crate) fn build_namespace(base_dir: &Path, target_dir: &Path) -> String {
    match target_dir.strip_prefix(base_dir) {
        Ok(relative) => relative
            .components()
            .map(|c| c.as_os_str().to_string_lossy().into_owned())
            .collect::<Vec<_>>()
            .join(":"),
        Err(_) => target_dir
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default(),
    }
}

// ---------------------------------------------------------------------------
// Internal: deduplication
// ---------------------------------------------------------------------------

/// Deduplicate loaded skills by canonical path. First occurrence wins.
fn deduplicate(skills: Vec<LoadedSkill>) -> Vec<SkillMetadata> {
    let mut seen: HashSet<PathBuf> = HashSet::new();
    let mut result = Vec::new();

    for skill in skills {
        if seen.insert(skill.resolved_path) {
            result.push(skill.metadata);
        }
    }

    result
}

/// Deduplicate by skill name (case-sensitive). First occurrence wins.
///
/// Called after path-based dedup to enforce priority between bundled, MCP,
/// and filesystem skills that share the same name but have different paths.
fn deduplicate_by_name(skills: Vec<SkillMetadata>) -> Vec<SkillMetadata> {
    let mut seen: HashMap<String, ()> = HashMap::new();
    let mut result = Vec::new();

    for skill in skills {
        if seen.insert(skill.name.clone(), ()).is_none() {
            result.push(skill);
        }
    }

    result
}

// ---------------------------------------------------------------------------
// Internal: safe canonicalize
// ---------------------------------------------------------------------------

/// Canonicalize a path, returning `None` if the path does not exist.
/// Never panics.
pub(crate) fn try_canonicalize(path: &Path) -> Option<PathBuf> {
    std::fs::canonicalize(path).ok()
}

/// Find a file with an exact case-sensitive name inside `dir`.
///
/// On case-insensitive filesystems (e.g., macOS APFS), `Path::is_file()` may
/// return `true` for `SKILL.md` even when only `skill.md` exists.  This
/// function reads the directory entries and performs a byte-for-byte name
/// comparison to avoid false positives.
///
/// Returns `None` if no entry with that exact name exists or if the directory
/// cannot be read.
async fn find_exact_file(dir: &Path, name: &str) -> Option<PathBuf> {
    let mut rd = tokio::fs::read_dir(dir).await.ok()?;
    while let Ok(Some(entry)) = rd.next_entry().await {
        if entry.file_name().to_string_lossy() == name {
            let path = entry.path();
            let ft = entry.file_type().await.ok()?;
            if ft.is_file() {
                return Some(path);
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "loader_test.rs"]
mod loader_test;

#[cfg(test)]
#[path = "loader_supplemental_test.rs"]
mod loader_supplemental_test;
