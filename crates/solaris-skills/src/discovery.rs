use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};

use ignore::gitignore::{Gitignore, GitignoreBuilder};

use crate::loader::{
    DiscoveredSkillDirectory, LoadedSkill, SkillDiscoveryRoot, load_bounded_skills_from_dir,
    load_discovered_skills_from_dir,
};
use crate::types::{LoadedFrom, SkillMetadata, SkillSource};

// ---------------------------------------------------------------------------
// Public manager
// ---------------------------------------------------------------------------

/// Manages runtime discovery of `.solaris/skills/` directories found in
/// subdirectories when the LLM operates on files.
///
/// CWD-level skills are loaded at startup; this manager handles dynamically
/// discovered skills in directories nested below the CWD.
///
/// # Concurrency
///
/// Not designed for concurrent access — caller wraps in `Arc<Mutex<>>` if needed.
pub struct RuntimeDiscovery {
    /// Directories already checked (both hits and misses) — avoids repeated stat.
    checked_dirs: HashSet<PathBuf>,
    /// Skills loaded from dynamically discovered directories, keyed by skill name.
    dynamic_skills: HashMap<String, SkillMetadata>,
    /// Open directory capabilities waiting to be consumed by the loader.
    discovered_skill_directories: HashMap<PathBuf, DiscoveredSkillDirectory>,
    /// Paths originating from discovery may never fall back to an ambient
    /// reopen after their retained capability has been consumed.
    capability_required_skill_directories: HashSet<PathBuf>,
    #[cfg(test)]
    before_skill_directory_return: Option<DiscoveryRaceHook>,
}

#[cfg(test)]
type DiscoveryRaceHook = Box<dyn FnMut(&Path) -> std::io::Result<()> + Send>;

impl Default for RuntimeDiscovery {
    fn default() -> Self {
        Self::new()
    }
}

impl RuntimeDiscovery {
    /// Create a new, empty discovery manager.
    pub fn new() -> Self {
        Self {
            checked_dirs: HashSet::new(),
            dynamic_skills: HashMap::new(),
            discovered_skill_directories: HashMap::new(),
            capability_required_skill_directories: HashSet::new(),
            #[cfg(test)]
            before_skill_directory_return: None,
        }
    }

    #[cfg(test)]
    fn set_before_skill_directory_return_hook<F>(&mut self, hook: F)
    where
        F: FnMut(&Path) -> std::io::Result<()> + Send + 'static,
    {
        self.before_skill_directory_return = Some(Box::new(hook));
    }

    /// Discover `.solaris/skills/` directories by walking up from each file path to `cwd`.
    ///
    /// Only discovers directories **below** `cwd` (cwd-level skills are loaded at
    /// startup). Already-checked directories are skipped to avoid redundant stat
    /// calls on every Read/Write/Edit operation.
    ///
    /// Directories belonging to gitignored paths are silently skipped. Ignore
    /// files are parsed in-process so discovery never launches an executable
    /// selected from repository-controlled `PATH` state.
    ///
    /// Returns newly discovered skill directories sorted deepest-first so that
    /// skills closer to the file take precedence when names conflict.
    ///
    /// Aligns with TypeScript `discoverSkillDirsForPaths` L861-915.
    pub async fn discover_dirs_for_paths(&mut self, file_paths: &[&str], cwd: &str) -> Vec<PathBuf> {
        // Normalise cwd: strip trailing separator to avoid prefix-match false positives
        let resolved_cwd = cwd.trim_end_matches(std::path::MAIN_SEPARATOR);
        let cwd_with_sep = format!("{}{}", resolved_cwd, std::path::MAIN_SEPARATOR);
        let canonical_workspace = match tokio::fs::canonicalize(resolved_cwd).await {
            Ok(path) => path,
            Err(_) => return Vec::new(),
        };
        let discovery_root = match SkillDiscoveryRoot::open(Path::new(resolved_cwd), &canonical_workspace) {
            Ok(root) => root,
            Err(error) => {
                tracing::warn!(
                    target: "solaris_skills",
                    error = %error,
                    "skipping runtime Skill discovery because the workspace could not be safely opened"
                );
                return Vec::new();
            }
        };

        let mut new_dirs: Vec<PathBuf> = Vec::new();

        for &file_path in file_paths {
            let file = Path::new(file_path);
            let Some(parent) = file.parent() else {
                continue;
            };

            let mut current = parent.to_path_buf();

            // Walk up toward cwd but NOT including cwd itself
            // Use prefix+separator check to avoid /project-backup matching when cwd=/project
            loop {
                let current_str = current.to_string_lossy();
                if !current_str.starts_with(&*cwd_with_sep) {
                    break;
                }

                let skill_dir = current.join(".solaris").join("skills");

                if !self.checked_dirs.contains(&skill_dir) {
                    self.checked_dirs.insert(skill_dir.clone());

                    if tokio::fs::metadata(&skill_dir).await.is_ok() {
                        let Some(canonical_skill) = canonical_skill_directory(&skill_dir, &canonical_workspace).await
                        else {
                            tracing::warn!(
                                target: "solaris_skills",
                                path = %skill_dir.display(),
                                "skipping dynamic Skill directory whose canonical path escapes the workspace"
                            );
                            continue;
                        };
                        // Check if the containing directory (currentDir = skill_dir's
                        // grandparent) is gitignored. Aligns with TS L892 which passes
                        // `currentDir` (not skillDir) to isPathGitignored (C4).
                        let containing_dir = skill_dir
                            .parent() // .solaris/
                            .and_then(|p| p.parent()) // currentDir
                            .unwrap_or(&current);

                        if dynamic_skill_directory_is_ignored(
                            containing_dir,
                            &skill_dir,
                            &canonical_skill,
                            resolved_cwd,
                        )
                        .await
                        {
                            tracing::debug!(target: "solaris_skills", path = %skill_dir.display(), "skipping gitignored skills directory");
                        } else {
                            let discovered = match discovery_root
                                .open_skill_directory(&skill_dir, &canonical_skill.path)
                            {
                                Ok(discovered) => discovered,
                                Err(error) => {
                                    tracing::warn!(
                                        target: "solaris_skills",
                                        error = %error,
                                        "skipping runtime Skill directory that could not be opened from the workspace capability"
                                    );
                                    continue;
                                }
                            };
                            #[cfg(test)]
                            if let Some(hook) = self.before_skill_directory_return.as_mut()
                                && hook(&canonical_skill.path).is_err()
                            {
                                continue;
                            }
                            let path = canonical_skill.path;
                            self.discovered_skill_directories.insert(path.clone(), discovered);
                            self.capability_required_skill_directories.insert(path.clone());
                            new_dirs.push(path);
                        }
                    }
                }

                // Move to parent
                let parent_dir = match current.parent() {
                    Some(p) if p != current => p.to_path_buf(),
                    _ => break, // Reached filesystem root
                };
                current = parent_dir;
            }
        }

        // Sort deepest-first: more path components = deeper
        new_dirs.sort_by_key(|d| std::cmp::Reverse(d.components().count()));

        new_dirs
    }

    /// Load skills from newly discovered directories and merge into dynamic skills.
    ///
    /// Directories should be sorted deepest-first (as returned by
    /// `discover_dirs_for_paths`). Deeper directories take precedence: when two
    /// skills share a name, the one from the deeper directory wins.
    ///
    /// Only prompt-type skills are merged (skills with no `skill_type` or
    /// `skill_type == "prompt"`), aligning with TS `addSkillDirectories` L947 (C8).
    ///
    /// Returns the count of newly merged skills.
    pub async fn add_skill_directories(&mut self, dirs: &[PathBuf]) -> usize {
        if dirs.is_empty() {
            return 0;
        }

        // Load all directories in parallel-ish (sequential here for simplicity;
        // the dirs slice is typically small — one per recently-touched file).
        let mut loaded_batches: Vec<Vec<LoadedSkill>> = Vec::with_capacity(dirs.len());
        for dir in dirs {
            let batch = match self.discovered_skill_directories.remove(dir) {
                Some(discovered) => {
                    load_discovered_skills_from_dir(discovered, SkillSource::Project, LoadedFrom::Skills).await
                }
                None if self.capability_required_skill_directories.contains(dir) => {
                    tracing::warn!(
                        target: "solaris_skills",
                        "skipping runtime Skill directory because its discovery capability is no longer available"
                    );
                    Vec::new()
                }
                None => load_bounded_skills_from_dir(dir, SkillSource::Project, LoadedFrom::Skills).await,
            };
            loaded_batches.push(batch);
        }

        let previous_count = self.dynamic_skills.len();

        // Process in reverse order (shallowest first) so deeper entries override.
        // `dirs` is already deepest-first, so reversing gives shallowest-first.
        for batch in loaded_batches.iter().rev() {
            for loaded in batch {
                if is_prompt_type(&loaded.metadata) {
                    self.dynamic_skills
                        .insert(loaded.metadata.name.clone(), loaded.metadata.clone());
                }
            }
        }

        let new_count = self.dynamic_skills.len();
        // Net increase in unique skill names. Replacements of existing skills
        // (same name, deeper directory) are not counted — this is a rough
        // "newly visible" metric for logging, not a total-loaded count.
        let added = new_count.saturating_sub(previous_count);

        if added > 0 {
            tracing::info!(target: "solaris_skills", added, directories = dirs.len(), "dynamically discovered new skills");
        }

        added
    }

    /// Get all dynamically discovered skills.
    pub fn get_dynamic_skills(&self) -> Vec<&SkillMetadata> {
        self.dynamic_skills.values().collect()
    }

    /// Clear dynamic skills (e.g., when reloading the skill set).
    ///
    /// `checked_dirs` is preserved to avoid redundant stat calls for directories
    /// already known not to contain a `.solaris/skills/` subdirectory.
    pub fn clear_dynamic_skills(&mut self) {
        self.dynamic_skills.clear();
    }

    /// Clear the set of directories that have already been checked for
    /// `.solaris/skills/` subdirectories.
    ///
    /// Call this when a file-system watcher detects changes so that newly
    /// created directories (or directories that were previously absent) are
    /// re-examined on the next [`discover_dirs_for_paths`](Self::discover_dirs_for_paths) call.
    pub fn clear_checked_dirs(&mut self) {
        self.checked_dirs.clear();
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

const MAX_IGNORE_FILE_BYTES: u64 = 1024 * 1024;

struct CanonicalSkillLocation {
    path: PathBuf,
    project_root: PathBuf,
}

/// Check whether `path` is ignored without starting `git` or any other child
/// process. Outside a repository the check remains fail-open. Once a repository
/// is found, malformed or oversized ignore data fails closed so untrusted,
/// ignored Skill content is not loaded accidentally.
async fn is_path_gitignored(path: &Path, cwd: &str) -> bool {
    match evaluate_gitignore(path, Path::new(cwd)).await {
        Ok(ignored) => ignored,
        Err(error) => {
            tracing::warn!(
                target: "solaris_skills",
                error = %error,
                "could not safely evaluate gitignore rules; skipping dynamic Skill directory"
            );
            true
        }
    }
}

async fn canonical_skill_directory(skill_dir: &Path, cwd: &Path) -> Option<CanonicalSkillLocation> {
    let skill_dir = tokio::fs::canonicalize(skill_dir).await.ok()?;
    let metadata = tokio::fs::metadata(&skill_dir).await.ok()?;
    let project_root = skill_dir.parent()?.parent()?.to_path_buf();
    (metadata.is_dir() && skill_dir.starts_with(cwd) && project_root.starts_with(cwd)).then_some(
        CanonicalSkillLocation {
            path: skill_dir,
            project_root,
        },
    )
}

async fn dynamic_skill_directory_is_ignored(
    project_root: &Path,
    skill_dir: &Path,
    canonical: &CanonicalSkillLocation,
    cwd: &str,
) -> bool {
    let mut checked = HashSet::new();
    for path in [
        project_root.to_path_buf(),
        skill_dir.to_path_buf(),
        canonical.project_root.clone(),
        canonical.path.clone(),
    ] {
        if checked.insert(path.clone()) && is_path_gitignored(&path, cwd).await {
            return true;
        }
    }
    false
}

async fn evaluate_gitignore(path: &Path, cwd: &Path) -> std::io::Result<bool> {
    let path = path.to_path_buf();
    let cwd = cwd.to_path_buf();
    tokio::task::spawn_blocking(move || evaluate_gitignore_blocking(&path, &cwd))
        .await
        .map_err(|error| std::io::Error::other(format!("gitignore worker failed: {error}")))?
}

fn evaluate_gitignore_blocking(path: &Path, cwd: &Path) -> std::io::Result<bool> {
    let process_cwd = std::env::current_dir()?;
    let lexical_cwd = normalize_absolute_lexical_path(cwd, &process_cwd)?;
    let cwd = std::fs::canonicalize(&lexical_cwd)?;
    let lexical_candidate = normalize_absolute_lexical_path(path, &lexical_cwd)?;
    let canonical_candidate = std::fs::canonicalize(&lexical_candidate)?;
    if !canonical_candidate.starts_with(&cwd) {
        return Ok(false);
    }
    let Some(repository_root) = find_repository_root(&cwd)? else {
        return Ok(false);
    };
    if !canonical_candidate.starts_with(&repository_root) {
        return Ok(false);
    }
    let Some(lexical_repository_root) = find_repository_root(&lexical_cwd)? else {
        return Err(std::io::Error::other(
            "lexical workspace path did not resolve to its repository root",
        ));
    };
    if std::fs::canonicalize(&lexical_repository_root)? != repository_root {
        return Err(std::io::Error::other(
            "lexical and canonical workspace paths resolved to different repositories",
        ));
    }
    // Preserve the lexical alias while using the canonical root representation.
    // This matters on Windows, where canonical paths normally have the `\\?\`
    // prefix but caller-provided paths do not. Callers separately evaluate the
    // canonical locations to cover rules that target the destination.
    let candidate = repository_path_preserving_lexical_alias(
        &lexical_candidate,
        &lexical_cwd,
        &lexical_repository_root,
        &cwd,
        &repository_root,
    )?;

    let mut matchers = Vec::new();
    let mut global_builder = GitignoreBuilder::new(&repository_root);
    global_builder.case_insensitive(cfg!(windows)).map_err(ignore_error)?;
    let (global, global_error) = global_builder.build_global();
    if let Some(error) = global_error {
        return Err(ignore_error(error));
    }
    if !global.is_empty() {
        matchers.push(global);
    }
    let common_git_directory = resolve_common_git_directory(&repository_root)?;
    if let Some(matcher) = build_ignore_matcher(&common_git_directory.join("info").join("exclude"), &repository_root)? {
        matchers.push(matcher);
    }
    let candidate_is_directory = std::fs::metadata(&canonical_candidate)?.is_dir();
    let rule_limit = candidate.parent();
    let rule_directories = ancestors_between(&repository_root, rule_limit.unwrap_or(&repository_root));
    for directory in rule_directories {
        if let Some(matcher) = build_ignore_matcher(&directory.join(".gitignore"), &directory)? {
            matchers.push(matcher);
        }
    }

    let relative = candidate
        .strip_prefix(&repository_root)
        .map_err(|_| std::io::Error::other("gitignore candidate escaped repository root"))?;
    let components = relative.components().collect::<Vec<_>>();
    let mut prefix = repository_root.clone();
    for (index, component) in components.iter().enumerate() {
        prefix.push(component.as_os_str());
        let is_directory = index + 1 < components.len() || candidate_is_directory;
        let mut ignored = false;
        for matcher in &matchers {
            if !prefix.starts_with(matcher.path()) {
                continue;
            }
            let matched = matcher.matched(&prefix, is_directory);
            if matched.is_ignore() {
                ignored = true;
            } else if matched.is_whitelist() {
                ignored = false;
            }
        }
        // Git cannot re-include a child while one of its parent directories is
        // still ignored, so stop at the first ignored path prefix.
        if ignored {
            return Ok(true);
        }
    }
    Ok(false)
}

fn repository_path_preserving_lexical_alias(
    candidate: &Path,
    lexical_cwd: &Path,
    lexical_repository_root: &Path,
    canonical_cwd: &Path,
    repository_root: &Path,
) -> std::io::Result<PathBuf> {
    let mapped = if let Ok(workspace_relative) = candidate.strip_prefix(lexical_cwd) {
        validate_safe_relative_path(workspace_relative)?;
        let repository_relative = candidate.strip_prefix(lexical_repository_root).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "lexical gitignore candidate escaped repository root",
            )
        })?;
        validate_safe_relative_path(repository_relative)?;
        repository_root.join(repository_relative)
    } else {
        let workspace_relative = candidate.strip_prefix(canonical_cwd).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "gitignore candidate escaped the workspace root",
            )
        })?;
        validate_safe_relative_path(workspace_relative)?;
        canonical_cwd.join(workspace_relative)
    };

    mapped
        .strip_prefix(repository_root)
        .map_err(|_| std::io::Error::other("gitignore candidate escaped repository root"))?;
    Ok(mapped)
}

fn validate_safe_relative_path(path: &Path) -> std::io::Result<()> {
    if path
        .components()
        .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "gitignore candidate contains an unsafe relative component",
        ));
    }
    Ok(())
}

fn normalize_absolute_lexical_path(path: &Path, base: &Path) -> std::io::Result<PathBuf> {
    let input = if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in input.components() {
        match component {
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                normalized.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "path escapes its filesystem root",
                    ));
                }
            }
        }
    }
    if normalized.is_absolute() {
        Ok(normalized)
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "path could not be resolved to an absolute location",
        ))
    }
}

fn find_repository_root(cwd: &Path) -> std::io::Result<Option<PathBuf>> {
    let mut current = Some(cwd);
    while let Some(directory) = current {
        match std::fs::symlink_metadata(directory.join(".git")) {
            Ok(_) => return Ok(Some(directory.to_path_buf())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        current = directory.parent();
    }
    Ok(None)
}

fn ancestors_between(root: &Path, limit: &Path) -> Vec<PathBuf> {
    let mut directories = Vec::new();
    let mut current = Some(limit);
    while let Some(directory) = current {
        if !directory.starts_with(root) {
            break;
        }
        directories.push(directory.to_path_buf());
        if directory == root {
            break;
        }
        current = directory.parent();
    }
    directories.reverse();
    directories
}

fn resolve_common_git_directory(repository_root: &Path) -> std::io::Result<PathBuf> {
    let dot_git = repository_root.join(".git");
    let metadata = std::fs::symlink_metadata(&dot_git)?;
    let git_directory = if metadata.is_dir() {
        std::fs::canonicalize(&dot_git)?
    } else if metadata.is_file() {
        let pointer = read_small_utf8_file(&dot_git, 64 * 1024)?;
        let path = pointer
            .lines()
            .next()
            .and_then(|line| line.strip_prefix("gitdir:"))
            .map(str::trim)
            .filter(|path| !path.is_empty())
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid .git file"))?;
        let path = PathBuf::from(path);
        let path = if path.is_absolute() {
            path
        } else {
            repository_root.join(path)
        };
        std::fs::canonicalize(path)?
    } else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            ".git is neither a directory nor a regular file",
        ));
    };
    let common_pointer = git_directory.join("commondir");
    if !common_pointer.is_file() {
        return Ok(git_directory);
    }
    let common = read_small_utf8_file(&common_pointer, 64 * 1024)?;
    let common = common.trim();
    if common.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "empty Git commondir pointer",
        ));
    }
    let common = PathBuf::from(common);
    let common = if common.is_absolute() {
        common
    } else {
        git_directory.join(common)
    };
    std::fs::canonicalize(common)
}

fn build_ignore_matcher(path: &Path, base: &Path) -> std::io::Result<Option<Gitignore>> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if !metadata.file_type().is_file() || metadata.len() > MAX_IGNORE_FILE_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "unsafe gitignore file type or size",
        ));
    }
    let mut builder = GitignoreBuilder::new(base);
    builder.case_insensitive(cfg!(windows)).map_err(ignore_error)?;
    if let Some(error) = builder.add(path) {
        return Err(ignore_error(error));
    }
    builder.build().map(Some).map_err(ignore_error)
}

fn read_small_utf8_file(path: &Path, limit: u64) -> std::io::Result<String> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() || metadata.len() > limit {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "unsafe Git metadata file type or size",
        ));
    }
    std::fs::read_to_string(path)
}

fn ignore_error(error: ignore::Error) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
}

/// Returns `true` if the skill is a prompt-type skill (the default when no
/// `skill_type` is set) or explicitly typed as `"prompt"`.
///
/// Aligns with TypeScript `addSkillDirectories` L947: `skill.type === 'prompt'` (C8).
fn is_prompt_type(_skill: &SkillMetadata) -> bool {
    // SkillMetadata does not expose skill_type as a parsed field yet.
    // All skills loaded via load_skills_from_dir are treated as prompt type.
    // Update when SkillMetadata gains a skill_type field.
    true
}

#[cfg(test)]
#[path = "discovery_test.rs"]
mod discovery_test;
