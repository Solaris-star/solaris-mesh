use super::*;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use tempfile::TempDir;

    #[cfg(windows)]
    use super::repository_path_preserving_lexical_alias;
    use super::{RuntimeDiscovery, canonical_skill_directory, evaluate_gitignore, is_path_gitignored, is_prompt_type};
    use crate::types::{ExecutionContext, LoadedFrom, SkillMetadata, SkillSource};

    fn make_skill(name: &str) -> SkillMetadata {
        SkillMetadata {
            name: name.to_string(),
            display_name: None,
            description: String::new(),
            has_user_specified_description: false,
            allowed_tools: vec![],
            argument_hint: None,
            argument_names: vec![],
            when_to_use: None,
            version: None,
            model: None,
            disable_model_invocation: false,
            user_invocable: true,
            execution_context: ExecutionContext::Inline,
            agent: None,
            effort: None,
            shell: None,
            paths: vec![],
            network: Default::default(),
            hooks_raw: None,
            source: SkillSource::Project,
            loaded_from: LoadedFrom::Skills,
            content: String::new(),
            content_length: 0,
            skill_root: None,
        }
    }

    // --- is_prompt_type ---

    #[test]
    fn is_prompt_type_always_returns_true() {
        let skill = make_skill("any-skill");
        assert!(is_prompt_type(&skill));
    }

    // --- is_path_gitignored ---

    #[test]
    fn discovery_source_never_launches_git_from_path() {
        let source = include_str!("discovery.rs");
        for forbidden in [
            "std::process",
            "tokio::process",
            "Command::new",
            "git_check_ignore_command",
            "shell_command_builder",
            "solaris_process",
        ] {
            assert!(!source.contains(forbidden), "discovery source contains {forbidden}");
        }
    }

    // Not gitignored in a non-git dir → fail open → returns false.
    #[tokio::test]
    async fn is_path_gitignored_returns_false_outside_git_repo() {
        let tmp = TempDir::new().unwrap();
        // No `git init` → not a git repo → git check-ignore fails → fail open
        let cwd = tmp.path().to_str().unwrap();
        let target = tmp.path().join("somefile.rs");
        fs::write(&target, "").unwrap();

        let result = is_path_gitignored(&target, cwd).await;
        assert!(!result);
    }

    // Gitignored path in a real git repo → returns true.
    #[tokio::test]
    async fn is_path_gitignored_returns_true_for_ignored_path() {
        let tmp = TempDir::new().unwrap();
        let cwd = tmp.path().to_str().unwrap();

        fs::create_dir_all(tmp.path().join(".git")).unwrap();

        fs::write(tmp.path().join(".gitignore"), "ignored_dir/\n").unwrap();
        let ignored = tmp.path().join("ignored_dir");
        fs::create_dir_all(&ignored).unwrap();

        let result = is_path_gitignored(&ignored, cwd).await;
        assert!(result, "ignored_dir/ should be detected as gitignored");
    }

    // Non-ignored path in a real git repo → returns false.
    #[tokio::test]
    async fn is_path_gitignored_returns_false_for_tracked_path() {
        let tmp = TempDir::new().unwrap();
        let cwd = tmp.path().to_str().unwrap();

        fs::create_dir_all(tmp.path().join(".git")).unwrap();

        // Empty .gitignore — nothing is ignored
        fs::write(tmp.path().join(".gitignore"), "").unwrap();
        let tracked = tmp.path().join("normal_dir");
        fs::create_dir_all(&tracked).unwrap();

        let result = is_path_gitignored(&tracked, cwd).await;
        assert!(!result);
    }

    // A missing candidate is an evaluation error and must fail closed.
    #[tokio::test]
    async fn is_path_gitignored_returns_true_for_nonexistent_path() {
        let tmp = TempDir::new().unwrap();
        let cwd = tmp.path().to_str().unwrap();
        let nonexistent = Path::new("/nonexistent/path/xyz");

        let result = is_path_gitignored(nonexistent, cwd).await;
        assert!(result);
    }

    #[tokio::test]
    async fn gitignore_matches_git_escape_anchor_doublestar_and_negation_rules() {
        let repository = TempDir::new().unwrap();
        fs::create_dir_all(repository.path().join(".git")).unwrap();
        fs::write(
            repository.path().join(".gitignore"),
            "\\#secret/\n\\!important/\n**/generated/\n/root-only/\nignored/*\n!ignored/keep/\ntrailing/   \n",
        )
        .unwrap();
        for path in [
            "#secret",
            "!important",
            "a/b/generated",
            "root-only",
            "ignored/keep",
            "nested/root-only",
            "trailing",
        ] {
            fs::create_dir_all(repository.path().join(path)).unwrap();
        }

        for ignored in ["#secret", "!important", "a/b/generated", "root-only", "trailing"] {
            assert!(
                evaluate_gitignore(&repository.path().join(ignored), repository.path())
                    .await
                    .unwrap(),
                "{ignored} should be ignored"
            );
        }
        for visible in ["ignored/keep", "nested/root-only"] {
            assert!(
                !evaluate_gitignore(&repository.path().join(visible), repository.path())
                    .await
                    .unwrap(),
                "{visible} should remain visible"
            );
        }
    }

    #[tokio::test]
    async fn nested_gitignore_has_higher_precedence() {
        let repository = TempDir::new().unwrap();
        fs::create_dir_all(repository.path().join(".git")).unwrap();
        fs::write(repository.path().join(".gitignore"), "*.tmp\n").unwrap();
        let nested = repository.path().join("nested");
        fs::create_dir_all(&nested).unwrap();
        fs::write(nested.join(".gitignore"), "!keep.tmp\n").unwrap();
        let kept = nested.join("keep.tmp");
        let ignored = nested.join("drop.tmp");
        fs::write(&kept, "").unwrap();
        fs::write(&ignored, "").unwrap();

        assert!(!evaluate_gitignore(&kept, repository.path()).await.unwrap());
        assert!(evaluate_gitignore(&ignored, repository.path()).await.unwrap());
    }

    #[tokio::test]
    async fn nested_gitignore_cannot_reinclude_an_ignored_ancestor() {
        let repository = TempDir::new().unwrap();
        fs::create_dir_all(repository.path().join(".git")).unwrap();
        fs::write(repository.path().join(".gitignore"), "blocked/\n").unwrap();
        let nested = repository.path().join("blocked").join("nested");
        let candidate = nested.join("module");
        fs::create_dir_all(&candidate).unwrap();
        fs::write(nested.join(".gitignore"), "!blocked/\n").unwrap();

        assert!(evaluate_gitignore(&candidate, repository.path()).await.unwrap());
    }

    #[tokio::test]
    async fn worktree_git_file_uses_common_info_exclude() {
        let directory = TempDir::new().unwrap();
        let repository = directory.path().join("worktree");
        let git_directory = directory.path().join("gitdir");
        let common_directory = directory.path().join("common");
        fs::create_dir_all(&repository).unwrap();
        fs::create_dir_all(&git_directory).unwrap();
        fs::create_dir_all(common_directory.join("info")).unwrap();
        fs::write(
            repository.join(".git"),
            format!("gitdir: {}\n", git_directory.display()),
        )
        .unwrap();
        fs::write(git_directory.join("commondir"), "../common\n").unwrap();
        fs::write(common_directory.join("info").join("exclude"), "excluded/\n").unwrap();
        let excluded = repository.join("excluded");
        fs::create_dir_all(&excluded).unwrap();

        assert!(evaluate_gitignore(&excluded, &repository).await.unwrap());
    }

    #[tokio::test]
    async fn canonical_skill_directory_rejects_workspace_escape() {
        let workspace = TempDir::new().unwrap();
        let outside = TempDir::new().unwrap();
        let outside_skills = outside.path().join("skills");
        fs::create_dir_all(&outside_skills).unwrap();

        assert!(
            canonical_skill_directory(&outside_skills, workspace.path())
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn discovery_checks_the_skill_directory_itself() {
        let repository = TempDir::new().unwrap();
        fs::create_dir_all(repository.path().join(".git")).unwrap();
        fs::write(repository.path().join(".gitignore"), "module/.solaris/skills/\n").unwrap();
        let module = repository.path().join("module");
        fs::create_dir_all(module.join(".solaris").join("skills")).unwrap();
        let source = module.join("source.rs");
        fs::write(&source, "").unwrap();

        let mut discovery = RuntimeDiscovery::new();
        let found = discovery
            .discover_dirs_for_paths(&[source.to_str().unwrap()], repository.path().to_str().unwrap())
            .await;

        assert!(found.is_empty());
    }

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn gitignore_checks_the_lexical_symlink_path() {
        let repository = TempDir::new().unwrap();
        fs::create_dir_all(repository.path().join(".git")).unwrap();
        fs::write(repository.path().join(".gitignore"), "module/.solaris/skills/\n").unwrap();
        let target_skills = repository.path().join("target-project").join(".solaris").join("skills");
        fs::create_dir_all(&target_skills).unwrap();
        let alias_parent = repository.path().join("module").join(".solaris");
        fs::create_dir_all(&alias_parent).unwrap();
        let alias = alias_parent.join("skills");
        if !create_test_directory_symlink(&target_skills, &alias) {
            return;
        }

        assert!(evaluate_gitignore(&alias, repository.path()).await.unwrap());
    }

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn gitignore_preserves_a_lexical_cwd_symlink_alias() {
        let repository = TempDir::new().unwrap();
        fs::create_dir_all(repository.path().join(".git")).unwrap();
        fs::write(
            repository.path().join(".gitignore"),
            "/workspace/module/.solaris/skills/\n",
        )
        .unwrap();
        let real_workspace = repository.path().join("real");
        let target_skills = real_workspace.join("module").join(".solaris").join("skills");
        let local_skill = target_skills.join("local");
        fs::create_dir_all(&local_skill).unwrap();
        fs::write(local_skill.join("SKILL.md"), "---\ndescription: local\n---\n").unwrap();
        let source = real_workspace.join("module").join("source.rs");
        fs::write(&source, "").unwrap();
        let workspace_alias = repository.path().join("workspace");
        if !create_test_directory_symlink(&real_workspace, &workspace_alias) {
            return;
        }
        let lexical_skills = workspace_alias.join("module").join(".solaris").join("skills");
        let lexical_source = workspace_alias.join("module").join("source.rs");

        assert!(evaluate_gitignore(&lexical_skills, &workspace_alias).await.unwrap());
        let mut discovery = RuntimeDiscovery::new();
        let found = discovery
            .discover_dirs_for_paths(&[lexical_source.to_str().unwrap()], workspace_alias.to_str().unwrap())
            .await;
        assert!(
            found.is_empty(),
            "runtime discovery must preserve gitignore rules written against a lexical workspace alias"
        );
    }

    #[cfg(windows)]
    #[test]
    fn lexical_windows_path_maps_into_the_canonical_root_representation() {
        let lexical_cwd = Path::new(r"C:\repository\workspace");
        let lexical_candidate = lexical_cwd.join("module").join(".solaris").join("skills");

        let ordinary = repository_path_preserving_lexical_alias(
            &lexical_candidate,
            lexical_cwd,
            Path::new(r"C:\repository"),
            Path::new(r"C:\repository\workspace"),
            Path::new(r"C:\repository"),
        )
        .unwrap();
        assert_eq!(ordinary, lexical_candidate);

        let extended = repository_path_preserving_lexical_alias(
            &lexical_candidate,
            lexical_cwd,
            Path::new(r"C:\repository"),
            Path::new(r"\\?\C:\repository\workspace"),
            Path::new(r"\\?\C:\repository"),
        )
        .unwrap();
        assert_eq!(
            extended,
            Path::new(r"\\?\C:\repository\workspace\module\.solaris\skills")
        );

        let aliased_cwd = repository_path_preserving_lexical_alias(
            &lexical_candidate,
            lexical_cwd,
            Path::new(r"C:\repository"),
            Path::new(r"\\?\C:\repository\real"),
            Path::new(r"\\?\C:\repository"),
        )
        .unwrap();
        assert_eq!(
            aliased_cwd,
            Path::new(r"\\?\C:\repository\workspace\module\.solaris\skills")
        );

        let canonical_target = Path::new(r"\\?\C:\repository\workspace\target\.solaris\skills");
        assert_eq!(
            repository_path_preserving_lexical_alias(
                canonical_target,
                lexical_cwd,
                Path::new(r"C:\repository"),
                Path::new(r"\\?\C:\repository\workspace"),
                Path::new(r"\\?\C:\repository"),
            )
            .unwrap(),
            canonical_target
        );

        assert!(
            repository_path_preserving_lexical_alias(
                Path::new(r"C:\repository\workspace\..\outside"),
                lexical_cwd,
                Path::new(r"C:\repository"),
                Path::new(r"\\?\C:\repository\workspace"),
                Path::new(r"\\?\C:\repository"),
            )
            .is_err()
        );
        assert!(
            repository_path_preserving_lexical_alias(
                Path::new(r"D:\outside"),
                lexical_cwd,
                Path::new(r"C:\repository"),
                Path::new(r"\\?\C:\repository\workspace"),
                Path::new(r"\\?\C:\repository"),
            )
            .is_err()
        );
        assert!(
            repository_path_preserving_lexical_alias(
                Path::new(r"\\?\C:\repository\workspace\..\outside"),
                lexical_cwd,
                Path::new(r"C:\repository"),
                Path::new(r"\\?\C:\repository\workspace"),
                Path::new(r"\\?\C:\repository"),
            )
            .is_err()
        );
    }

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn discovery_checks_the_canonical_skill_directory() {
        let repository = TempDir::new().unwrap();
        fs::create_dir_all(repository.path().join(".git")).unwrap();
        fs::write(
            repository.path().join(".gitignore"),
            "target-project/.solaris/skills/\n",
        )
        .unwrap();
        let target_skills = repository.path().join("target-project").join(".solaris").join("skills");
        fs::create_dir_all(&target_skills).unwrap();
        let module = repository.path().join("module");
        let alias_parent = module.join(".solaris");
        fs::create_dir_all(&alias_parent).unwrap();
        let alias = alias_parent.join("skills");
        if !create_test_directory_symlink(&target_skills, &alias) {
            return;
        }
        let source = module.join("source.rs");
        fs::write(&source, "").unwrap();

        let mut discovery = RuntimeDiscovery::new();
        let found = discovery
            .discover_dirs_for_paths(&[source.to_str().unwrap()], repository.path().to_str().unwrap())
            .await;

        assert!(found.is_empty());
    }

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn discovery_checks_the_canonical_skill_project_root() {
        let repository = TempDir::new().unwrap();
        fs::create_dir_all(repository.path().join(".git")).unwrap();
        fs::write(repository.path().join(".gitignore"), "ignored-target/\n").unwrap();
        let target_skills = repository.path().join("ignored-target").join(".solaris").join("skills");
        fs::create_dir_all(&target_skills).unwrap();
        let module = repository.path().join("module");
        let alias_parent = module.join(".solaris");
        fs::create_dir_all(&alias_parent).unwrap();
        let alias = alias_parent.join("skills");
        if !create_test_directory_symlink(&target_skills, &alias) {
            return;
        }
        let source = module.join("source.rs");
        fs::write(&source, "").unwrap();

        let mut discovery = RuntimeDiscovery::new();
        let found = discovery
            .discover_dirs_for_paths(&[source.to_str().unwrap()], repository.path().to_str().unwrap())
            .await;

        assert!(found.is_empty());
    }

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn dynamic_loading_rejects_a_nested_symlink_outside_its_skill_root() {
        let workspace = TempDir::new().unwrap();
        let outside = TempDir::new().unwrap();
        let skills = workspace.path().join(".solaris").join("skills");
        fs::create_dir_all(&skills).unwrap();
        let outside_skill = outside.path().join("external");
        fs::create_dir_all(&outside_skill).unwrap();
        fs::write(outside_skill.join("SKILL.md"), "---\ndescription: external\n---\n").unwrap();
        let nested_alias = skills.join("external");
        if !create_test_directory_symlink(&outside_skill, &nested_alias) {
            return;
        }

        let mut discovery = RuntimeDiscovery::new();
        assert_eq!(discovery.add_skill_directories(&[skills]).await, 0);
        assert!(discovery.get_dynamic_skills().is_empty());
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn dynamic_loading_rejects_a_nested_junction_outside_its_skill_root() {
        let workspace = TempDir::new().unwrap();
        let outside = TempDir::new().unwrap();
        let skills = workspace.path().join(".solaris").join("skills");
        fs::create_dir_all(&skills).unwrap();
        let outside_skill = outside.path().join("external");
        fs::create_dir_all(&outside_skill).unwrap();
        fs::write(outside_skill.join("SKILL.md"), "---\ndescription: external\n---\n").unwrap();
        create_windows_junction(&outside_skill, &skills.join("external")).await;

        let mut discovery = RuntimeDiscovery::new();
        assert_eq!(discovery.add_skill_directories(&[skills]).await, 0);
        assert!(discovery.get_dynamic_skills().is_empty());
    }

    #[tokio::test]
    async fn dynamic_loading_rejects_a_manifest_with_an_external_hardlink() {
        let workspace = TempDir::new().unwrap();
        let outside = TempDir::new().unwrap();
        let skills = workspace.path().join(".solaris").join("skills");
        let linked_skill = skills.join("linked");
        fs::create_dir_all(&linked_skill).unwrap();
        let outside_manifest = outside.path().join("SKILL.md");
        fs::write(&outside_manifest, "---\ndescription: external\n---\n").unwrap();
        fs::hard_link(&outside_manifest, linked_skill.join("SKILL.md")).unwrap();

        let mut discovery = RuntimeDiscovery::new();
        assert_eq!(discovery.add_skill_directories(&[skills]).await, 0);
        assert!(discovery.get_dynamic_skills().is_empty());
    }

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn discovery_capability_survives_an_ancestor_replacement_before_return() {
        use std::sync::{Arc, Mutex};

        let repository = TempDir::new().unwrap();
        let outside = TempDir::new().unwrap();
        fs::create_dir_all(repository.path().join(".git")).unwrap();
        let ancestor = repository.path().join("ancestor");
        let skills = ancestor.join("module").join(".solaris").join("skills");
        let local_skill = skills.join("local");
        fs::create_dir_all(&local_skill).unwrap();
        fs::write(local_skill.join("SKILL.md"), "---\ndescription: local\n---\n").unwrap();
        let source = ancestor.join("module").join("source.rs");
        fs::write(&source, "").unwrap();

        let outside_skills = outside.path().join("module").join(".solaris").join("skills");
        let external_skill = outside_skills.join("external");
        fs::create_dir_all(&external_skill).unwrap();
        fs::write(external_skill.join("SKILL.md"), "---\ndescription: external\n---\n").unwrap();
        let replacement = ancestor.join("module").join(".solaris").join("skills-replacement");
        create_race_directory_alias(&outside_skills, &replacement).await;
        let parked = ancestor.join("module").join(".solaris").join("skills-parked");
        let replaced = Arc::new(Mutex::new(false));
        let hook_replaced = Arc::clone(&replaced);
        let hook_skills = skills.clone();
        let hook_parked = parked.clone();
        let hook_replacement = replacement.clone();

        let mut discovery = RuntimeDiscovery::new();
        discovery.set_before_skill_directory_return_hook(move |_| {
            fs::rename(&hook_skills, &hook_parked)?;
            fs::rename(&hook_replacement, &hook_skills)?;
            *hook_replaced.lock().unwrap() = true;
            Ok(())
        });
        let found = discovery
            .discover_dirs_for_paths(&[source.to_str().unwrap()], repository.path().to_str().unwrap())
            .await;

        assert_eq!(found.len(), 1);
        assert!(*replaced.lock().unwrap());
        assert!(
            fs::read_to_string(found[0].join("external").join("SKILL.md"))
                .unwrap()
                .contains("description: external"),
            "a path reopen observes the external replacement"
        );
        assert_eq!(discovery.add_skill_directories(&found).await, 0);
        assert_eq!(discovery.add_skill_directories(&found).await, 0);
        assert!(discovery.get_dynamic_skills().is_empty());

        remove_directory_alias(&skills);
        fs::rename(&parked, &skills).unwrap();
    }

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn dynamic_loading_rejects_a_workspace_ancestor_redirect_after_discovery() {
        use std::sync::{Arc, Mutex};

        let container = TempDir::new().unwrap();
        let outside = TempDir::new().unwrap();
        let repository = container.path().join("repository");
        fs::create_dir_all(repository.join(".git")).unwrap();
        let real_workspace = repository.join("real-workspace");
        let workspace_alias = repository.join("workspace-alias");
        if !create_test_directory_symlink(&real_workspace, &workspace_alias) {
            return;
        }
        let skills = real_workspace.join("module").join(".solaris").join("skills");
        let local_skill = skills.join("local");
        fs::create_dir_all(&local_skill).unwrap();
        fs::write(local_skill.join("SKILL.md"), "---\ndescription: local\n---\n").unwrap();
        let source = workspace_alias.join("module").join("source.rs");
        fs::write(&source, "").unwrap();

        let external_workspace = outside.path().join("workspace");
        let external_skill = external_workspace
            .join("module")
            .join(".solaris")
            .join("skills")
            .join("external");
        fs::create_dir_all(&external_skill).unwrap();
        fs::write(external_skill.join("SKILL.md"), "---\ndescription: external\n---\n").unwrap();
        let replacement = repository.join("workspace-alias-replacement");
        create_race_directory_alias(&external_workspace, &replacement).await;
        let replaced = Arc::new(Mutex::new(false));
        let hook_replaced = Arc::clone(&replaced);
        let hook_alias = workspace_alias.clone();
        let hook_replacement = replacement.clone();

        let mut discovery = RuntimeDiscovery::new();
        discovery.set_before_skill_directory_return_hook(move |_| {
            remove_directory_alias_for_race(&hook_alias)?;
            fs::rename(&hook_replacement, &hook_alias)?;
            *hook_replaced.lock().unwrap() = true;
            Ok(())
        });
        let found = discovery
            .discover_dirs_for_paths(&[source.to_str().unwrap()], workspace_alias.to_str().unwrap())
            .await;

        assert_eq!(found.len(), 1);
        assert!(*replaced.lock().unwrap());
        assert_eq!(
            discovery.add_skill_directories(&found).await,
            0,
            "changing a workspace ancestor to a redirect must invalidate discovery"
        );
        assert!(discovery.get_dynamic_skills().is_empty());

        remove_directory_alias(&workspace_alias);
    }

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn dynamic_loading_rejects_a_repository_directory_replacement_after_discovery() {
        use std::sync::{Arc, Mutex};

        let container = TempDir::new().unwrap();
        let real_repository = container.path().join("real-repository");
        let repository_alias = container.path().join("repository-alias");
        let workspace = real_repository.join("workspace");
        let skills = workspace.join("module").join(".solaris").join("skills");
        let local_skill = skills.join("local");
        fs::create_dir_all(real_repository.join(".git")).unwrap();
        fs::create_dir_all(&local_skill).unwrap();
        fs::write(local_skill.join("SKILL.md"), "---\ndescription: local\n---\n").unwrap();
        if !create_test_directory_symlink(&real_repository, &repository_alias) {
            return;
        }
        let lexical_workspace = repository_alias.join("workspace");
        let source = lexical_workspace.join("module").join("source.rs");
        fs::write(&source, "").unwrap();

        let replacement = container.path().join("repository-replacement");
        let replacement_skill = replacement
            .join("workspace")
            .join("module")
            .join(".solaris")
            .join("skills")
            .join("external");
        fs::create_dir_all(replacement.join(".git")).unwrap();
        fs::create_dir_all(&replacement_skill).unwrap();
        fs::write(replacement_skill.join("SKILL.md"), "---\ndescription: external\n---\n").unwrap();
        let replaced = Arc::new(Mutex::new(false));
        let hook_replaced = Arc::clone(&replaced);
        let hook_repository = repository_alias.clone();
        let hook_replacement = replacement.clone();

        let mut discovery = RuntimeDiscovery::new();
        discovery.set_before_skill_directory_return_hook(move |_| {
            remove_directory_alias_for_race(&hook_repository)?;
            fs::rename(&hook_replacement, &hook_repository)?;
            *hook_replaced.lock().unwrap() = true;
            Ok(())
        });
        let found = discovery
            .discover_dirs_for_paths(&[source.to_str().unwrap()], lexical_workspace.to_str().unwrap())
            .await;

        assert_eq!(found.len(), 1);
        assert!(*replaced.lock().unwrap());
        assert_eq!(
            discovery.add_skill_directories(&found).await,
            0,
            "replacing the repository directory must invalidate discovery"
        );
        assert!(discovery.get_dynamic_skills().is_empty());

        fs::remove_dir_all(&repository_alias).unwrap();
    }

    #[cfg(any(unix, windows))]
    #[tokio::test]
    async fn dynamic_loading_rejects_a_nested_lexical_alias_replacement_after_discovery() {
        use std::sync::{Arc, Mutex};

        let workspace = TempDir::new().unwrap();
        let outside = TempDir::new().unwrap();
        fs::create_dir_all(workspace.path().join(".git")).unwrap();
        let real_module = workspace.path().join("real-module");
        let local_skill = real_module.join(".solaris").join("skills").join("local");
        fs::create_dir_all(&local_skill).unwrap();
        fs::write(local_skill.join("SKILL.md"), "---\ndescription: local\n---\n").unwrap();
        fs::write(real_module.join("source.rs"), "").unwrap();
        let module_alias = workspace.path().join("module-link");
        if !create_test_directory_symlink(&real_module, &module_alias) {
            return;
        }

        let outside_module = outside.path().join("module");
        let external_skill = outside_module.join(".solaris").join("skills").join("external");
        fs::create_dir_all(&external_skill).unwrap();
        fs::write(external_skill.join("SKILL.md"), "---\ndescription: external\n---\n").unwrap();
        fs::write(outside_module.join("source.rs"), "").unwrap();
        let replacement = workspace.path().join("module-link-replacement");
        create_race_directory_alias(&outside_module, &replacement).await;
        let replaced = Arc::new(Mutex::new(false));
        let hook_replaced = Arc::clone(&replaced);
        let hook_alias = module_alias.clone();
        let hook_replacement = replacement.clone();

        let mut discovery = RuntimeDiscovery::new();
        discovery.set_before_skill_directory_return_hook(move |_| {
            remove_directory_alias_for_race(&hook_alias)?;
            fs::rename(&hook_replacement, &hook_alias)?;
            *hook_replaced.lock().unwrap() = true;
            Ok(())
        });
        let lexical_source = module_alias.join("source.rs");
        let found = discovery
            .discover_dirs_for_paths(&[lexical_source.to_str().unwrap()], workspace.path().to_str().unwrap())
            .await;

        assert_eq!(found.len(), 1);
        assert!(*replaced.lock().unwrap());
        assert_eq!(
            discovery.add_skill_directories(&found).await,
            0,
            "changing a lexical path below the workspace must invalidate discovery"
        );
        assert!(discovery.get_dynamic_skills().is_empty());

        remove_directory_alias(&module_alias);
    }

    #[tokio::test]
    async fn clear_checked_dirs_never_restores_ambient_loading_for_a_discovered_path() {
        let workspace = TempDir::new().unwrap();
        fs::create_dir_all(workspace.path().join(".git")).unwrap();
        let module = workspace.path().join("module");
        let skills = module.join(".solaris").join("skills");
        let local_skill = skills.join("local");
        fs::create_dir_all(&local_skill).unwrap();
        fs::write(local_skill.join("SKILL.md"), "---\ndescription: local\n---\n").unwrap();
        let source = module.join("source.rs");
        fs::write(&source, "").unwrap();

        let mut discovery = RuntimeDiscovery::new();
        let found = discovery
            .discover_dirs_for_paths(&[source.to_str().unwrap()], workspace.path().to_str().unwrap())
            .await;
        assert_eq!(found.len(), 1);
        assert_eq!(discovery.add_skill_directories(&found).await, 1);
        discovery.clear_dynamic_skills();
        discovery.clear_checked_dirs();

        assert_eq!(
            discovery.add_skill_directories(&found).await,
            0,
            "a consumed discovery capability must not become an ambient path after cache clearing"
        );
        assert!(discovery.get_dynamic_skills().is_empty());

        let rediscovered = discovery
            .discover_dirs_for_paths(&[source.to_str().unwrap()], workspace.path().to_str().unwrap())
            .await;
        assert_eq!(rediscovered, found);
        assert_eq!(discovery.add_skill_directories(&rediscovered).await, 1);

        let mut legacy_direct_add = RuntimeDiscovery::new();
        assert_eq!(legacy_direct_add.add_skill_directories(&found).await, 1);
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

    #[cfg(windows)]
    async fn create_windows_junction(target: &Path, junction: &Path) {
        fn literal(path: &Path) -> String {
            format!("'{}'", path.to_string_lossy().replace('\'', "''"))
        }

        let shell = solaris_config::shell::resolve_shell(Some("powershell")).expect("PowerShell");
        let script = format!(
            "$ErrorActionPreference = 'Stop'; $item = New-Item -ItemType Junction -Path {} -Target {}; \
             if ($item.LinkType -ne 'Junction') {{ throw 'expected a junction' }}",
            literal(junction),
            literal(target),
        );
        let mut command = solaris_config::shell::shell_command_builder(&shell, &script, false);
        let output = command.output().await.expect("create junction");
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    }

    #[cfg(unix)]
    async fn create_race_directory_alias(target: &Path, alias: &Path) {
        std::os::unix::fs::symlink(target, alias).unwrap();
    }

    #[cfg(windows)]
    async fn create_race_directory_alias(target: &Path, alias: &Path) {
        create_windows_junction(target, alias).await;
    }

    #[cfg(unix)]
    fn remove_directory_alias(alias: &Path) {
        fs::remove_file(alias).unwrap();
    }

    #[cfg(windows)]
    fn remove_directory_alias(alias: &Path) {
        fs::remove_dir(alias).unwrap();
    }

    #[cfg(unix)]
    fn remove_directory_alias_for_race(alias: &Path) -> std::io::Result<()> {
        fs::remove_file(alias)
    }

    #[cfg(windows)]
    fn remove_directory_alias_for_race(alias: &Path) -> std::io::Result<()> {
        fs::remove_dir(alias)
    }
}

// ---------------------------------------------------------------------------
// Supplemental tests (tester role — covers test-plan.md cases)
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "discovery_supplemental_test.rs"]
mod discovery_supplemental_test;
