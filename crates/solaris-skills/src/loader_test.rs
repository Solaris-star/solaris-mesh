use super::*;
use std::fs;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

fn write_skill(dir: &Path, rel_path: &str, content: &str) {
    let full = dir.join(rel_path);
    fs::create_dir_all(full.parent().unwrap()).unwrap();
    fs::write(full, content).unwrap();
}

// --- build_namespace ---

#[test]
fn test_build_namespace_simple() {
    let base = Path::new("/skills");
    let target = Path::new("/skills/my-skill");
    assert_eq!(build_namespace(base, target), "my-skill");
}

#[test]
fn test_build_namespace_nested() {
    let base = Path::new("/skills");
    let target = Path::new("/skills/db/migrate");
    assert_eq!(build_namespace(base, target), "db:migrate");
}

#[test]
fn test_build_namespace_three_levels() {
    let base = Path::new("/skills");
    let target = Path::new("/skills/a/b/c");
    assert_eq!(build_namespace(base, target), "a:b:c");
}

#[test]
fn test_build_namespace_same_dir() {
    let base = Path::new("/skills");
    // target == base → empty string
    let result = build_namespace(base, base);
    assert_eq!(result, "");
}

// --- try_canonicalize ---

#[test]
fn test_try_canonicalize_existing_path() {
    let tmp = TempDir::new().unwrap();
    let result = try_canonicalize(tmp.path());
    assert!(result.is_some());
}

#[test]
fn test_try_canonicalize_nonexistent_returns_none() {
    let result = try_canonicalize(Path::new("/nonexistent/path/xyz"));
    assert!(result.is_none());
}

// --- deduplicate ---

#[test]
fn test_deduplicate_removes_duplicates() {
    let tmp = TempDir::new().unwrap();
    let file = tmp.path().join("skill.md");
    fs::write(&file, "").unwrap();
    let canonical = std::fs::canonicalize(&file).unwrap();

    let fm = crate::types::FrontmatterData::default();
    let make_meta =
        || crate::frontmatter::parse_skill_fields(&fm, "", "test", SkillSource::User, LoadedFrom::Skills, None);

    let skills = vec![
        LoadedSkill {
            metadata: make_meta(),
            resolved_path: canonical.clone(),
        },
        LoadedSkill {
            metadata: make_meta(),
            resolved_path: canonical.clone(),
        },
    ];

    let result = deduplicate(skills);
    assert_eq!(result.len(), 1);
}

#[test]
fn test_deduplicate_different_paths_preserved() {
    let tmp = TempDir::new().unwrap();
    let file1 = tmp.path().join("skill1.md");
    let file2 = tmp.path().join("skill2.md");
    fs::write(&file1, "").unwrap();
    fs::write(&file2, "").unwrap();

    let fm = crate::types::FrontmatterData::default();
    let make_meta =
        || crate::frontmatter::parse_skill_fields(&fm, "", "test", SkillSource::User, LoadedFrom::Skills, None);

    let skills = vec![
        LoadedSkill {
            metadata: make_meta(),
            resolved_path: std::fs::canonicalize(&file1).unwrap(),
        },
        LoadedSkill {
            metadata: make_meta(),
            resolved_path: std::fs::canonicalize(&file2).unwrap(),
        },
    ];

    let result = deduplicate(skills);
    assert_eq!(result.len(), 2);
}

// --- load_skills_from_dir ---

#[tokio::test]
async fn test_load_skills_from_dir_basic() {
    let tmp = TempDir::new().unwrap();
    write_skill(
        tmp.path(),
        "my-skill/SKILL.md",
        "---\nname: my-skill\ndescription: A test skill\n---\n# Body\n",
    );

    let skills = load_skills_from_dir(tmp.path(), SkillSource::User, LoadedFrom::Skills).await;
    assert_eq!(skills.len(), 1);
    assert_eq!(skills[0].metadata.name, "my-skill");
}

#[cfg(any(unix, windows))]
#[tokio::test]
async fn trusted_loader_retains_directory_symlink_compatibility() {
    let trusted_root = TempDir::new().unwrap();
    let target_root = TempDir::new().unwrap();
    write_skill(
        target_root.path(),
        "linked/SKILL.md",
        "---\ndescription: trusted alias\n---\n",
    );
    let alias = trusted_root.path().join("linked");
    if !create_test_directory_symlink(&target_root.path().join("linked"), &alias) {
        return;
    }

    let skills = load_skills_from_dir(trusted_root.path(), SkillSource::User, LoadedFrom::Skills).await;
    assert_eq!(skills.len(), 1);
    assert_eq!(skills[0].metadata.name, "linked");
}

#[cfg(any(unix, windows))]
#[tokio::test]
async fn bounded_loader_rejects_parent_replacement_before_child_open() {
    let workspace = TempDir::new().unwrap();
    let outside = TempDir::new().unwrap();
    let skills = workspace.path().join("skills");
    let namespace = skills.join("namespace");
    write_skill(&namespace, "child/SKILL.md", "---\ndescription: internal\n---\n");
    write_skill(outside.path(), "child/SKILL.md", "---\ndescription: external\n---\n");

    let parked = skills.join("namespace-parked");
    let replacement = skills.join("namespace-replacement");
    create_race_directory_alias(outside.path(), &replacement).await;
    let outcome = Arc::new(Mutex::new(ReplacementOutcome::NotAttempted));
    let hook_outcome = Arc::clone(&outcome);
    let hook_namespace = namespace.clone();
    let hook_parked = parked.clone();
    let hook_replacement = replacement.clone();
    let loaded = load_bounded_skills_from_dir_with_hook(
        &skills,
        SkillSource::Project,
        LoadedFrom::Skills,
        move |point, relative| {
            if point == BoundedLoadRacePoint::BeforeChildDirectoryTraversal
                && relative == Path::new("namespace").join("child")
            {
                replace_directory_path(
                    &hook_namespace,
                    &hook_parked,
                    &hook_replacement,
                    Path::new("child").join("SKILL.md").as_path(),
                    &hook_outcome,
                )?;
            }
            Ok(())
        },
    )
    .await;

    assert!(
        loaded.is_empty(),
        "a replaced parent must invalidate the entire bounded load"
    );
    let observed = *outcome.lock().unwrap();
    assert_race_was_exercised(observed);
    restore_replaced_directory(&namespace, &parked, observed);
    remove_directory_alias_if_present(&replacement);
}

#[cfg(any(unix, windows))]
#[tokio::test]
async fn bounded_loader_rejects_parent_replacement_before_manifest_open() {
    let workspace = TempDir::new().unwrap();
    let outside = TempDir::new().unwrap();
    let skills = workspace.path().join("skills");
    let skill = skills.join("skill");
    write_skill(&skills, "skill/SKILL.md", "---\ndescription: internal\n---\n");
    write_skill(outside.path(), "SKILL.md", "---\ndescription: external\n---\n");

    let parked = skills.join("skill-parked");
    let replacement = skills.join("skill-replacement");
    create_race_directory_alias(outside.path(), &replacement).await;
    let outcome = Arc::new(Mutex::new(ReplacementOutcome::NotAttempted));
    let hook_outcome = Arc::clone(&outcome);
    let hook_skill = skill.clone();
    let hook_parked = parked.clone();
    let hook_replacement = replacement.clone();
    let loaded = load_bounded_skills_from_dir_with_hook(
        &skills,
        SkillSource::Project,
        LoadedFrom::Skills,
        move |point, relative| {
            if point == BoundedLoadRacePoint::BeforeManifestOpen && relative == Path::new("skill").join("SKILL.md") {
                replace_directory_path(
                    &hook_skill,
                    &hook_parked,
                    &hook_replacement,
                    Path::new("SKILL.md"),
                    &hook_outcome,
                )?;
            }
            Ok(())
        },
    )
    .await;

    assert!(
        loaded.is_empty(),
        "a replaced manifest parent must invalidate the bounded load"
    );
    let observed = *outcome.lock().unwrap();
    assert_race_was_exercised(observed);
    restore_replaced_directory(&skill, &parked, observed);
    remove_directory_alias_if_present(&replacement);
}

#[tokio::test]
async fn bounded_loader_rejects_a_hardlink_and_content_change_after_manifest_open() {
    let workspace = TempDir::new().unwrap();
    let outside = TempDir::new().unwrap();
    let skills = workspace.path().join("skills");
    let manifest = skills.join("skill").join("SKILL.md");
    write_skill(&skills, "skill/SKILL.md", "---\ndescription: internal\n---\n");
    let external_link = outside.path().join("SKILL.md");
    let raced = Arc::new(Mutex::new(false));
    let hook_raced = Arc::clone(&raced);
    let hook_manifest = manifest.clone();
    let hook_external_link = external_link.clone();

    let loaded = load_bounded_skills_from_dir_with_hook(
        &skills,
        SkillSource::Project,
        LoadedFrom::Skills,
        move |point, relative| {
            if point == BoundedLoadRacePoint::AfterManifestOpen && relative == Path::new("skill").join("SKILL.md") {
                fs::hard_link(&hook_manifest, &hook_external_link)?;
                fs::write(&hook_external_link, "---\ndescription: external\n---\n")?;
                *hook_raced.lock().unwrap() = true;
            }
            Ok(())
        },
    )
    .await;

    assert!(*raced.lock().unwrap(), "the post-open hardlink race must run");
    assert!(
        loaded.is_empty(),
        "a manifest changed through a new external hardlink must be rejected"
    );
}

#[tokio::test]
async fn test_load_skills_from_dir_nested_namespace() {
    let tmp = TempDir::new().unwrap();
    write_skill(tmp.path(), "db/migrate/SKILL.md", "---\ndescription: Migrate DB\n---\n");

    let skills = load_skills_from_dir(tmp.path(), SkillSource::User, LoadedFrom::Skills).await;
    assert_eq!(skills.len(), 1);
    assert_eq!(skills[0].metadata.name, "db:migrate");
}

#[tokio::test]
async fn test_load_skills_from_dir_case_sensitive_skill_md() {
    let tmp = TempDir::new().unwrap();
    // Only lowercase "skill.md" — should NOT be loaded
    write_skill(tmp.path(), "my-skill/skill.md", "---\n---\n# Body\n");

    let skills = load_skills_from_dir(tmp.path(), SkillSource::User, LoadedFrom::Skills).await;
    assert!(skills.is_empty(), "skill.md (lowercase) should not be loaded");
}

#[tokio::test]
async fn test_load_skills_from_dir_empty_dir() {
    let tmp = TempDir::new().unwrap();
    let skills = load_skills_from_dir(tmp.path(), SkillSource::User, LoadedFrom::Skills).await;
    assert!(skills.is_empty());
}

#[tokio::test]
async fn test_load_skills_from_dir_nonexistent_silently_skipped() {
    let skills = load_skills_from_dir(Path::new("/nonexistent/path"), SkillSource::User, LoadedFrom::Skills).await;
    assert!(skills.is_empty());
}

// --- load_skills_from_commands_dir ---

#[tokio::test]
async fn test_load_commands_directory_format() {
    let tmp = TempDir::new().unwrap();
    write_skill(tmp.path(), "my-cmd/SKILL.md", "---\ndescription: A command\n---\n");

    let skills = load_skills_from_commands_dir(tmp.path(), SkillSource::User).await;
    assert_eq!(skills.len(), 1);
    assert_eq!(skills[0].metadata.loaded_from, LoadedFrom::CommandsDeprecated);
}

#[tokio::test]
async fn test_load_commands_flat_format() {
    let tmp = TempDir::new().unwrap();
    write_skill(tmp.path(), "simple.md", "---\ndescription: Simple\n---\n");

    let skills = load_skills_from_commands_dir(tmp.path(), SkillSource::User).await;
    assert_eq!(skills.len(), 1);
    assert_eq!(skills[0].metadata.loaded_from, LoadedFrom::CommandsDeprecated);
}

#[tokio::test]
async fn test_load_commands_dir_format_takes_precedence_over_flat() {
    let tmp = TempDir::new().unwrap();
    // Both my-cmd/SKILL.md and my-cmd.md exist — directory format wins
    write_skill(
        tmp.path(),
        "my-cmd/SKILL.md",
        "---\ndescription: Directory version\n---\n",
    );
    write_skill(tmp.path(), "my-cmd.md", "---\ndescription: Flat version\n---\n");

    let skills = load_skills_from_commands_dir(tmp.path(), SkillSource::User).await;
    let descriptions: Vec<_> = skills.iter().map(|s| s.metadata.description.as_str()).collect();
    assert!(
        descriptions.contains(&"Directory version"),
        "directory format should be loaded"
    );
    assert!(
        !descriptions.contains(&"Flat version"),
        "flat format should be skipped when directory exists"
    );
}

#[tokio::test]
async fn test_load_commands_nested_flat() {
    let tmp = TempDir::new().unwrap();
    write_skill(tmp.path(), "db/migrate.md", "---\ndescription: DB migrate\n---\n");

    let skills = load_skills_from_commands_dir(tmp.path(), SkillSource::User).await;
    assert_eq!(skills.len(), 1);
    assert_eq!(skills[0].metadata.name, "db:migrate");
}

// --- load_all_skills ---

#[tokio::test]
async fn test_load_all_skills_bare_mode() {
    let tmp = TempDir::new().unwrap();
    // Create .solaris/skills/ under the add_dir
    let skills_dir = tmp.path().join(".solaris").join("skills");
    fs::create_dir_all(&skills_dir).unwrap();
    write_skill(&skills_dir, "my-skill/SKILL.md", "---\n---\n");

    let result = load_all_skills(Path::new("/nonexistent"), &[tmp.path().to_owned()], true, None).await;
    let filesystem_skills: Vec<_> = result
        .iter()
        .filter(|skill| skill.source != SkillSource::Bundled)
        .collect();
    assert_eq!(filesystem_skills.len(), 1);
    assert_eq!(filesystem_skills[0].name, "my-skill");
}

#[tokio::test]
async fn test_load_all_skills_deduplicates() {
    let tmp = TempDir::new().unwrap();
    let root = tmp.path();
    // Create git root
    fs::create_dir(root.join(".git")).unwrap();

    // Create same skill in project dir (will appear twice due to walk)
    let skills_dir = root.join(".solaris").join("skills");
    fs::create_dir_all(&skills_dir).unwrap();
    write_skill(&skills_dir, "my-skill/SKILL.md", "---\n---\n");

    let result = load_all_skills(root, &[], false, None).await;
    let names: Vec<_> = result.iter().map(|s| s.name.as_str()).collect();
    let count = names.iter().filter(|&&n| n == "my-skill").count();
    assert_eq!(count, 1, "skill should appear exactly once after dedup");
}

#[cfg(any(unix, windows))]
fn create_test_directory_symlink(target: &Path, link: &Path) -> bool {
    match create_directory_symlink(target, link) {
        Ok(()) => true,
        Err(error) => {
            #[cfg(windows)]
            if error.kind() == std::io::ErrorKind::PermissionDenied || error.raw_os_error() == Some(1314) {
                return false;
            }
            panic!("create directory symlink: {error}");
        }
    }
}

#[cfg(unix)]
fn create_directory_symlink(target: &Path, link: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(windows)]
fn create_directory_symlink(target: &Path, link: &Path) -> std::io::Result<()> {
    std::os::windows::fs::symlink_dir(target, link)
}

#[cfg(any(unix, windows))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReplacementOutcome {
    NotAttempted,
    Replaced,
    BlockedByRetainedHandle,
}

#[cfg(any(unix, windows))]
fn replace_directory_path(
    current: &Path,
    parked: &Path,
    replacement: &Path,
    external_manifest: &Path,
    outcome: &Mutex<ReplacementOutcome>,
) -> std::io::Result<()> {
    match fs::rename(current, parked) {
        Ok(()) => {
            fs::rename(replacement, current)?;
            let visible = fs::read_to_string(current.join(external_manifest))?;
            assert!(
                visible.contains("description: external"),
                "a path-based reader would have observed the external manifest"
            );
            *outcome.lock().unwrap() = ReplacementOutcome::Replaced;
            Ok(())
        }
        Err(error) if cfg!(windows) && error.kind() == std::io::ErrorKind::PermissionDenied => {
            *outcome.lock().unwrap() = ReplacementOutcome::BlockedByRetainedHandle;
            Err(std::io::Error::new(
                error.kind(),
                "the retained Windows directory handle blocked replacement",
            ))
        }
        Err(error) => Err(error),
    }
}

#[cfg(any(unix, windows))]
fn assert_race_was_exercised(outcome: ReplacementOutcome) {
    assert_eq!(outcome, ReplacementOutcome::Replaced);
}

#[cfg(any(unix, windows))]
fn restore_replaced_directory(current: &Path, parked: &Path, outcome: ReplacementOutcome) {
    if outcome == ReplacementOutcome::Replaced {
        remove_directory_alias_if_present(current);
        fs::rename(parked, current).unwrap();
    }
}

#[cfg(unix)]
fn remove_directory_alias_if_present(alias: &Path) {
    if alias.symlink_metadata().is_ok() {
        fs::remove_file(alias).unwrap();
    }
}

#[cfg(windows)]
fn remove_directory_alias_if_present(alias: &Path) {
    if alias.symlink_metadata().is_ok() {
        fs::remove_dir(alias).unwrap();
    }
}

#[cfg(unix)]
async fn create_race_directory_alias(target: &Path, alias: &Path) {
    std::os::unix::fs::symlink(target, alias).unwrap();
}

#[cfg(windows)]
async fn create_race_directory_alias(target: &Path, alias: &Path) {
    fn literal(path: &Path) -> String {
        format!("'{}'", path.to_string_lossy().replace('\'', "''"))
    }

    let shell = solaris_config::shell::resolve_shell(Some("powershell")).expect("PowerShell");
    let script = format!(
        "$ErrorActionPreference = 'Stop'; $item = New-Item -ItemType Junction -Path {} -Target {}; \
         if ($item.LinkType -ne 'Junction') {{ throw 'expected a junction' }}",
        literal(alias),
        literal(target),
    );
    let mut command = solaris_config::shell::shell_command_builder(&shell, &script, false);
    let output = command.output().await.expect("create junction");
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
}
