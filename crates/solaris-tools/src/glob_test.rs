use super::*;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fs;
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;

    use solaris_types::tool::ToolResult;

    #[cfg(windows)]
    use crate::test_support::create_windows_junction;
    use crate::write::WorkspaceSearchPolicy;

    struct RecordingDenyPolicy {
        denied_root: PathBuf,
        checked: Arc<Mutex<Vec<PathBuf>>>,
    }

    struct AllowAmbientPaths;

    impl WorkspaceSearchPolicy for AllowAmbientPaths {
        fn allows_ambient_paths(&self) -> bool {
            true
        }

        fn allows_read(&self, _path: &Path) -> bool {
            true
        }
    }

    impl WorkspaceSearchPolicy for RecordingDenyPolicy {
        fn allows_read(&self, path: &Path) -> bool {
            self.checked.lock().unwrap().push(path.to_path_buf());
            !path.starts_with(&self.denied_root)
        }
    }

    async fn run_glob(pattern: &str, path: &str) -> ToolResult {
        let tool = GlobTool::new(PathBuf::from(path));
        let input = json!({ "pattern": pattern, "path": path });
        tool.execute(input).await
    }

    #[tokio::test]
    async fn test_glob_matches_pattern() {
        let dir = tempdir().unwrap();
        let base = dir.path();

        fs::write(base.join("main.rs"), "fn main() {}").unwrap();
        fs::write(base.join("lib.rs"), "pub mod lib;").unwrap();
        fs::write(base.join("notes.txt"), "some notes").unwrap();
        fs::write(base.join("readme.md"), "# Readme").unwrap();

        let result = run_glob("*.rs", base.to_str().unwrap()).await;

        assert!(!result.is_error, "glob should succeed");
        let lines: Vec<&str> = result.content.lines().collect();
        assert_eq!(lines.len(), 2, "should match exactly 2 .rs files");
        for line in &lines {
            assert!(line.ends_with(".rs"), "each match should be a .rs file, got: {}", line);
        }
        assert!(!result.content.contains("notes.txt"), "should not include .txt files");
        assert!(!result.content.contains("readme.md"), "should not include .md files");
    }

    #[tokio::test]
    async fn test_glob_no_matches() {
        let dir = tempdir().unwrap();
        let base = dir.path();

        fs::write(base.join("main.rs"), "fn main() {}").unwrap();
        fs::write(base.join("lib.rs"), "pub mod lib;").unwrap();

        let result = run_glob("*.xyz", base.to_str().unwrap()).await;

        assert!(!result.is_error, "no-match glob should not be an error");
        assert_eq!(result.content, "No files matched the pattern");
    }

    #[tokio::test]
    async fn test_glob_with_limit() {
        let dir = tempdir().unwrap();
        let base = dir.path();

        for i in 0..5 {
            fs::write(base.join(format!("file_{}.txt", i)), format!("content {}", i)).unwrap();
        }

        let result = run_glob("*.txt", base.to_str().unwrap()).await;

        assert!(!result.is_error, "glob should succeed");
        let lines: Vec<&str> = result.content.lines().collect();
        assert_eq!(lines.len(), 5, "all 5 files should be returned");
    }

    #[tokio::test]
    async fn glob_sorts_all_controlled_matches_before_taking_the_newest_hundred() {
        let dir = tempdir().unwrap();
        let base = dir.path();
        for index in 0..101 {
            fs::write(base.join(format!("candidate-{index:03}.txt")), index.to_string()).unwrap();
        }
        let access = WorkspaceFileAccess::new(base);
        let candidates = access.collect_paths(base, 50_000).unwrap();
        assert_eq!(candidates.len(), 101);
        let newest = candidates[100].relative_path.clone();
        let newest_path = base.join(&newest);
        std::fs::OpenOptions::new()
            .write(true)
            .open(&newest_path)
            .unwrap()
            .set_modified(std::time::SystemTime::now() + std::time::Duration::from_secs(60))
            .unwrap();
        let tool = GlobTool::new(base.to_path_buf());

        let result = tool.execute(json!({"pattern": "*.txt", "path": "."})).await;

        assert!(!result.is_error, "{}", result.content);
        assert_eq!(result.content.lines().count(), 100);
        assert_eq!(result.content.lines().next(), Some(newest.to_string_lossy().as_ref()));
    }

    #[tokio::test]
    async fn test_glob_recursive() {
        let dir = tempdir().unwrap();
        let base = dir.path();

        // Create nested directory structure
        let sub_a = base.join("a");
        let sub_b = base.join("a").join("b");
        fs::create_dir_all(&sub_b).unwrap();

        fs::write(base.join("root.txt"), "root level").unwrap();
        fs::write(sub_a.join("mid.txt"), "middle level").unwrap();
        fs::write(sub_b.join("deep.txt"), "deep level").unwrap();
        // Non-matching file
        fs::write(sub_a.join("skip.rs"), "not a txt").unwrap();

        let result = run_glob("**/*.txt", base.to_str().unwrap()).await;

        assert!(!result.is_error, "recursive glob should succeed");
        let lines: Vec<&str> = result.content.lines().collect();
        assert_eq!(lines.len(), 3, "should find 3 .txt files across all levels");
        assert!(result.content.contains("root.txt"), "should include root-level file");
        assert!(result.content.contains("mid.txt"), "should include mid-level file");
        assert!(result.content.contains("deep.txt"), "should include deep-level file");
        assert!(!result.content.contains("skip.rs"), "should not include .rs files");
    }

    #[tokio::test]
    async fn glob_stops_before_entering_directories_beyond_the_depth_limit() {
        let dir = tempdir().unwrap();
        let mut deepest = dir.path().to_path_buf();
        for depth in 1..=65 {
            deepest.push("d");
            fs::create_dir(&deepest).unwrap();
            if depth == 64 {
                fs::write(deepest.join("at-depth-limit.txt"), "visible at boundary").unwrap();
            }
        }
        fs::write(deepest.join("too-deep.txt"), "hidden by traversal limit").unwrap();

        let result = run_glob("**/*.txt", dir.path().to_str().unwrap()).await;

        assert!(!result.is_error, "{}", result.content);
        assert_eq!(
            result.content,
            deepest
                .parent()
                .unwrap()
                .strip_prefix(dir.path())
                .unwrap()
                .join("at-depth-limit.txt")
                .display()
                .to_string()
        );
        assert!(!result.content.contains("too-deep.txt"));
    }

    #[tokio::test]
    async fn effect_and_execution_use_workspace_for_relative_path() {
        let tmp = tempdir().unwrap();
        fs::write(tmp.path().join("marker.txt"), "hello").unwrap();

        let tool = GlobTool::new(tmp.path().to_path_buf());
        let input = json!({"pattern": "marker.txt"});
        let effect = tool.describe_effect(&input);
        let result = tool.execute(input).await;
        assert_eq!(
            PathBuf::from(&effect.resources.file_reads[0]).canonicalize().unwrap(),
            tmp.path().canonicalize().unwrap()
        );
        assert!(!result.is_error, "unexpected error: {}", result.content);
        assert!(
            result.content.contains("marker.txt"),
            "should find marker.txt, got: {}",
            result.content
        );
    }

    #[tokio::test]
    async fn absolute_and_parent_glob_patterns_are_rejected() {
        let tmp = tempdir().unwrap();
        let tool = GlobTool::new(tmp.path().to_path_buf());

        let absolute = tool.execute(json!({"pattern": "/outside/**"})).await;
        assert!(absolute.is_error);
        let parent = tool.execute(json!({"pattern": "../**"})).await;
        assert!(parent.is_error);
    }

    #[tokio::test]
    async fn ambient_policy_globs_absolute_and_parent_relative_paths_outside_workspace() {
        let root = tempdir().unwrap();
        let workspace = root.path().join("workspace");
        let outside = root.path().join("outside");
        fs::create_dir(&workspace).unwrap();
        fs::create_dir(&outside).unwrap();
        fs::write(outside.join("marker.txt"), "outside").unwrap();

        let restricted = GlobTool::new(workspace.clone());
        assert!(
            restricted
                .execute(json!({"pattern": "*.txt", "path": outside.clone()}))
                .await
                .is_error
        );
        assert!(
            restricted
                .execute(json!({"pattern": "*.txt", "path": "../outside"}))
                .await
                .is_error
        );

        let ambient = GlobTool::new_with_search_policy(workspace, Arc::new(AllowAmbientPaths));
        let absolute = ambient.execute(json!({"pattern": "*.txt", "path": outside})).await;
        let parent_relative = ambient.execute(json!({"pattern": "*.txt", "path": "../outside"})).await;

        assert!(!absolute.is_error, "{}", absolute.content);
        assert!(absolute.content.contains("marker.txt"));
        assert!(!parent_relative.is_error, "{}", parent_relative.content);
        assert!(parent_relative.content.contains("marker.txt"));
    }

    #[tokio::test]
    async fn glob_does_not_follow_directory_links_outside_workspace() {
        let workspace = tempdir().unwrap();
        let outside = tempdir().unwrap();
        fs::write(outside.path().join("secret.txt"), "outside-secret").unwrap();
        let link = workspace.path().join("linked");
        #[cfg(unix)]
        std::os::unix::fs::symlink(outside.path(), &link).unwrap();
        #[cfg(windows)]
        create_windows_junction(outside.path(), &link).await;

        let result = run_glob("**/*.txt", workspace.path().to_str().unwrap()).await;
        assert!(!result.is_error, "{}", result.content);
        assert!(!result.content.contains("secret.txt"));
    }

    #[tokio::test]
    async fn glob_prunes_denied_directories_before_recursive_access() {
        let workspace = tempdir().unwrap();
        let protected = workspace.path().join(".runtime");
        fs::create_dir(&protected).unwrap();
        let secret = protected.join("secret.txt");
        fs::write(&secret, "never expose").unwrap();
        #[cfg(windows)]
        let _secret_lock = {
            use std::os::windows::fs::OpenOptionsExt;
            std::fs::OpenOptions::new()
                .read(true)
                .share_mode(0)
                .open(&secret)
                .unwrap()
        };
        fs::write(workspace.path().join("public.txt"), "visible").unwrap();
        let checked = Arc::new(Mutex::new(Vec::new()));
        let canonical_protected = protected.canonicalize().unwrap();
        let tool = GlobTool::new_with_search_policy(
            workspace.path().to_path_buf(),
            Arc::new(RecordingDenyPolicy {
                denied_root: canonical_protected.clone(),
                checked: Arc::clone(&checked),
            }),
        );

        let result = tool.execute(json!({"pattern": "**/*.txt", "path": "."})).await;

        assert!(!result.is_error, "{}", result.content);
        assert!(result.content.contains("public.txt"));
        assert!(!result.content.contains("secret.txt"));
        assert!(checked.lock().unwrap().iter().any(|path| path == &canonical_protected));
    }

    struct IdentityCheckingPolicy;

    struct FailingRefreshPolicy;

    impl WorkspaceSearchPolicy for IdentityCheckingPolicy {
        fn allows_read(&self, _path: &Path) -> bool {
            true
        }

        fn requires_opened_file_identity(&self) -> bool {
            true
        }
    }

    impl WorkspaceSearchPolicy for FailingRefreshPolicy {
        fn refresh(&self) -> Result<(), String> {
            Err("protected identity refresh unavailable".to_owned())
        }

        fn allows_read(&self, _path: &Path) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn glob_fails_closed_when_protection_refresh_fails() {
        let workspace = tempdir().unwrap();
        fs::write(workspace.path().join("public.txt"), "visible").unwrap();
        let tool = GlobTool::new_with_search_policy(workspace.path().to_path_buf(), Arc::new(FailingRefreshPolicy));

        let result = tool.execute(json!({"pattern": "*.txt", "path": "."})).await;

        assert!(result.is_error);
        assert_eq!(
            result.content,
            "Failed to refresh workspace protection: protected identity refresh unavailable"
        );
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn glob_skips_an_exclusively_locked_identity_checked_candidate() {
        use std::os::windows::fs::OpenOptionsExt;

        let workspace = tempdir().unwrap();
        let locked = workspace.path().join("locked.txt");
        fs::write(&locked, "unreadable").unwrap();
        fs::write(workspace.path().join("public.txt"), "visible").unwrap();
        let _exclusive = fs::OpenOptions::new().read(true).share_mode(0).open(&locked).unwrap();
        let tool = GlobTool::new_with_search_policy(workspace.path().to_path_buf(), Arc::new(IdentityCheckingPolicy));

        let result = tool.execute(json!({"pattern": "*.txt", "path": "."})).await;

        assert!(!result.is_error, "{}", result.content);
        assert_eq!(result.content, "public.txt");
        assert!(!result.content.contains("locked.txt"));
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn glob_lists_a_file_without_opening_its_contents() {
        use std::os::windows::fs::OpenOptionsExt;

        let workspace = tempdir().unwrap();
        let path = workspace.path().join("locked.txt");
        fs::write(&path, "content must not be opened").unwrap();
        let _exclusive = fs::OpenOptions::new().read(true).share_mode(0).open(&path).unwrap();
        let tool = GlobTool::new(workspace.path().to_path_buf());

        let result = tool.execute(json!({"pattern": "*.txt", "path": "."})).await;

        assert!(!result.is_error, "{}", result.content);
        assert_eq!(result.content, "locked.txt");
    }
}
