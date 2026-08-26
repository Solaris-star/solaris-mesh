use super::*;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::path::Path;
    use std::sync::{Arc, Mutex};

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

    #[tokio::test]
    async fn grep_tool_finds_pattern_in_own_source() {
        let tool = GrepTool::new(PathBuf::from(env!("CARGO_MANIFEST_DIR")));
        let input = json!({
            "pattern": "GrepTool",
            "path": env!("CARGO_MANIFEST_DIR")
        });
        let result = tool.execute(input).await;
        assert!(!result.is_error, "grep failed: {}", result.content);
        assert!(result.content.contains("GrepTool"));
    }

    #[tokio::test]
    async fn effect_and_execution_use_workspace_for_relative_path() {
        use std::fs;
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join("searchable.txt"), "unique_grep_marker_xyz").unwrap();

        let tool = GrepTool::new(tmp.path().to_path_buf());
        let input = json!({"pattern": "unique_grep_marker_xyz", "path": "."});
        let effect = tool.describe_effect(&input);
        let result = tool.execute(input).await;
        assert_eq!(
            PathBuf::from(&effect.resources.file_reads[0]).canonicalize().unwrap(),
            tmp.path().canonicalize().unwrap()
        );
        assert!(!result.is_error, "unexpected error: {}", result.content);
        assert!(
            result.content.contains("unique_grep_marker_xyz"),
            "should find pattern, got: {}",
            result.content
        );
    }

    #[test]
    fn grep_effect_does_not_claim_an_external_process() {
        let tool = GrepTool::new(PathBuf::from(env!("CARGO_MANIFEST_DIR")));
        let effect = tool.describe_effect(&json!({"pattern": "GrepTool", "path": "."}));
        assert_eq!(effect.class, EffectClass::ReadOnly);
        assert!(effect.resources.process_commands.is_empty());
        assert!(effect.resources.process_invocations.is_empty());
    }

    #[tokio::test]
    async fn grep_rejects_search_paths_outside_workspace() {
        let workspace = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("secret.txt"), "outside-secret").unwrap();
        let tool = GrepTool::new(workspace.path().to_path_buf());
        let result = tool
            .execute(json!({"pattern": "outside-secret", "path": outside.path()}))
            .await;
        assert!(result.is_error);
        assert!(!result.content.contains("outside-secret"));
    }

    #[tokio::test]
    async fn ambient_policy_greps_absolute_and_parent_relative_paths_outside_workspace() {
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("workspace");
        let outside = root.path().join("outside");
        std::fs::create_dir(&workspace).unwrap();
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(outside.join("marker.txt"), "unique-outside-marker").unwrap();

        let restricted = GrepTool::new(workspace.clone());
        assert!(
            restricted
                .execute(json!({"pattern": "unique-outside-marker", "path": outside.clone()}))
                .await
                .is_error
        );
        assert!(
            restricted
                .execute(json!({"pattern": "unique-outside-marker", "path": "../outside"}))
                .await
                .is_error
        );

        let ambient = GrepTool::new_with_search_policy(workspace, Arc::new(AllowAmbientPaths));
        let absolute = ambient
            .execute(json!({"pattern": "unique-outside-marker", "path": outside}))
            .await;
        let parent_relative = ambient
            .execute(json!({"pattern": "unique-outside-marker", "path": "../outside"}))
            .await;

        assert!(!absolute.is_error, "{}", absolute.content);
        assert!(absolute.content.contains("unique-outside-marker"));
        assert!(!parent_relative.is_error, "{}", parent_relative.content);
        assert!(parent_relative.content.contains("unique-outside-marker"));
    }

    #[tokio::test]
    async fn grep_prunes_denied_directories_before_recursive_access() {
        let workspace = tempfile::tempdir().unwrap();
        let protected = workspace.path().join(".runtime");
        std::fs::create_dir(&protected).unwrap();
        let secret = protected.join("secret.txt");
        std::fs::write(&secret, "unique-protected-marker").unwrap();
        #[cfg(windows)]
        let _secret_lock = {
            use std::os::windows::fs::OpenOptionsExt;
            std::fs::OpenOptions::new()
                .read(true)
                .share_mode(0)
                .open(&secret)
                .unwrap()
        };
        std::fs::write(workspace.path().join("public.txt"), "unique-public-marker").unwrap();
        let checked = Arc::new(Mutex::new(Vec::new()));
        let canonical_protected = protected.canonicalize().unwrap();
        let tool = GrepTool::new_with_search_policy(
            workspace.path().to_path_buf(),
            Arc::new(RecordingDenyPolicy {
                denied_root: canonical_protected.clone(),
                checked: Arc::clone(&checked),
            }),
        );

        let protected_result = tool
            .execute(json!({"pattern": "unique-protected-marker", "path": "."}))
            .await;
        let public_result = tool
            .execute(json!({"pattern": "unique-public-marker", "path": "."}))
            .await;

        assert!(!protected_result.is_error, "{}", protected_result.content);
        assert!(!protected_result.content.contains("unique-protected-marker"));
        assert!(public_result.content.contains("unique-public-marker"));
        assert!(checked.lock().unwrap().iter().any(|path| path == &canonical_protected));
    }

    struct IdentityCheckingPolicy;

    impl WorkspaceSearchPolicy for IdentityCheckingPolicy {
        fn allows_read(&self, _path: &Path) -> bool {
            true
        }

        fn requires_opened_file_identity(&self) -> bool {
            true
        }
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn grep_skips_an_exclusively_locked_identity_checked_candidate() {
        use std::os::windows::fs::OpenOptionsExt;

        let workspace = tempfile::tempdir().unwrap();
        let locked = workspace.path().join("locked.txt");
        std::fs::write(&locked, "secret-needle").unwrap();
        std::fs::write(workspace.path().join("public.txt"), "public-needle").unwrap();
        let _exclusive = std::fs::OpenOptions::new()
            .read(true)
            .share_mode(0)
            .open(&locked)
            .unwrap();
        let tool = GrepTool::new_with_search_policy(workspace.path().to_path_buf(), Arc::new(IdentityCheckingPolicy));

        let result = tool.execute(json!({"pattern": "needle", "path": "."})).await;

        assert!(!result.is_error, "{}", result.content);
        assert!(result.content.contains("public-needle"));
        assert!(!result.content.contains("secret-needle"));
        assert!(!result.content.contains("locked.txt"));
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn grep_skips_an_exclusively_locked_ordinary_candidate_during_content_read() {
        use std::os::windows::fs::OpenOptionsExt;

        let workspace = tempfile::tempdir().unwrap();
        let locked = workspace.path().join("locked.txt");
        std::fs::write(&locked, "secret-needle").unwrap();
        std::fs::write(workspace.path().join("public.txt"), "public-needle").unwrap();
        let _exclusive = std::fs::OpenOptions::new()
            .read(true)
            .share_mode(0)
            .open(&locked)
            .unwrap();
        let tool = GrepTool::new(workspace.path().to_path_buf());

        let result = tool.execute(json!({"pattern": "needle", "path": "."})).await;

        assert!(!result.is_error, "{}", result.content);
        assert!(result.content.contains("public-needle"));
        assert!(!result.content.contains("secret-needle"));
        assert!(!result.content.contains("locked.txt"));
    }

    #[tokio::test]
    async fn grep_skips_files_larger_than_the_per_file_byte_limit() {
        let workspace = tempfile::tempdir().unwrap();
        std::fs::write(workspace.path().join("large.txt"), "needle").unwrap();
        let tool = GrepTool::new_with_limits(workspace.path().to_path_buf(), 5, 100);

        let result = tool.execute(json!({"pattern": "needle", "path": "."})).await;

        assert!(!result.is_error, "{}", result.content);
        assert_eq!(result.content, "No matches found");
    }

    #[tokio::test]
    async fn grep_stops_reading_at_the_total_scan_byte_limit() {
        let workspace = tempfile::tempdir().unwrap();
        std::fs::write(workspace.path().join("large.txt"), "needle").unwrap();
        let tool = GrepTool::new_with_limits(workspace.path().to_path_buf(), 100, 5);

        let result = tool.execute(json!({"pattern": "needle", "path": "."})).await;

        assert!(!result.is_error, "{}", result.content);
        assert_eq!(result.content, "No matches found");
    }
}
