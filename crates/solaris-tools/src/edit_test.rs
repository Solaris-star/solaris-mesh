use super::*;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_cache::file_mtime_ms;
    use serde_json::json;
    use tempfile::tempdir;

    use crate::file_cache::update_cache_after_write;
    use solaris_config::file_cache::FileCacheConfig;

    fn make_cache() -> Arc<RwLock<FileStateCache>> {
        let config = FileCacheConfig {
            max_entries: 100,
            max_size_bytes: 25 * 1024 * 1024,
            enabled: true,
        };
        Arc::new(RwLock::new(FileStateCache::new(&config)))
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

    /// Simulate a Read by inserting a cache entry for the given file path.
    fn simulate_read(cache: &Arc<RwLock<FileStateCache>>, path: &Path) {
        let content = std::fs::read_to_string(path).unwrap_or_default();
        update_cache_after_write(cache, path, &content);
    }

    // -- Legacy tests (no cache) --

    #[tokio::test]
    async fn test_edit_replace_block() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.txt");
        std::fs::write(&file_path, "hello world").unwrap();

        let tool = EditTool::new_with_workspace_root(None, dir.path());
        let input = json!({
            "file_path": file_path.to_str().unwrap(),
            "old_string": "hello",
            "new_string": "goodbye"
        });

        let result = tool.execute(input).await;

        assert!(!result.is_error, "unexpected error: {}", result.content);
        let content = std::fs::read_to_string(&file_path).unwrap();
        assert_eq!(content, "goodbye world");
    }

    #[tokio::test]
    async fn test_edit_old_string_not_found() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.txt");
        std::fs::write(&file_path, "hello world").unwrap();

        let tool = EditTool::new_with_workspace_root(None, dir.path());
        let input = json!({
            "file_path": file_path.to_str().unwrap(),
            "old_string": "nonexistent",
            "new_string": "replacement"
        });

        let result = tool.execute(input).await;

        assert!(result.is_error);
        assert!(
            result.content.contains("not found"),
            "expected 'not found' in error message, got: {}",
            result.content
        );
    }

    #[tokio::test]
    async fn test_edit_preserves_surrounding() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.txt");
        std::fs::write(&file_path, "aaa\nbbb\nccc\n").unwrap();

        let tool = EditTool::new_with_workspace_root(None, dir.path());
        let input = json!({
            "file_path": file_path.to_str().unwrap(),
            "old_string": "bbb",
            "new_string": "XXX"
        });

        let result = tool.execute(input).await;

        assert!(!result.is_error, "unexpected error: {}", result.content);
        let content = std::fs::read_to_string(&file_path).unwrap();
        assert_eq!(content, "aaa\nXXX\nccc\n");
    }

    #[tokio::test]
    async fn test_edit_nonexistent_file() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("does_not_exist.txt");

        let tool = EditTool::new_with_workspace_root(None, dir.path());
        let input = json!({
            "file_path": file_path.to_str().unwrap(),
            "old_string": "anything",
            "new_string": "replacement"
        });

        let result = tool.execute(input).await;

        assert!(result.is_error);
        assert!(
            result.content.contains("Failed to read file"),
            "expected read failure message, got: {}",
            result.content
        );
    }

    // -- Cache guard tests --

    #[tokio::test]
    async fn edit_without_read_returns_error() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("unread.txt");
        std::fs::write(&file_path, "hello").unwrap();

        let cache = make_cache();
        let tool = EditTool::new_with_workspace_root(Some(cache), dir.path());

        let input = json!({
            "file_path": file_path.to_str().unwrap(),
            "old_string": "hello",
            "new_string": "bye"
        });

        let result = tool.execute(input).await;

        assert!(result.is_error);
        assert!(
            result.content.contains("must Read"),
            "expected 'must Read' in error: {}",
            result.content
        );
        // File must be unchanged.
        assert_eq!(std::fs::read_to_string(&file_path).unwrap(), "hello");
    }

    #[tokio::test]
    async fn edit_after_read_succeeds() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("read_then_edit.txt");
        std::fs::write(&file_path, "hello world").unwrap();

        let cache = make_cache();
        simulate_read(&cache, &file_path);

        let tool = EditTool::new_with_workspace_root(Some(cache), dir.path());
        let input = json!({
            "file_path": file_path.to_str().unwrap(),
            "old_string": "hello",
            "new_string": "goodbye"
        });

        let result = tool.execute(input).await;

        assert!(!result.is_error, "unexpected error: {}", result.content);
        assert_eq!(std::fs::read_to_string(&file_path).unwrap(), "goodbye world");
    }

    #[tokio::test]
    async fn edit_detects_external_modification() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("stale.txt");
        std::fs::write(&file_path, "original").unwrap();

        let cache = make_cache();
        simulate_read(&cache, &file_path);

        // External modification: change file after caching.
        std::thread::sleep(std::time::Duration::from_millis(50));
        std::fs::write(&file_path, "externally changed").unwrap();

        let tool = EditTool::new_with_workspace_root(Some(cache), dir.path());
        let input = json!({
            "file_path": file_path.to_str().unwrap(),
            "old_string": "original",
            "new_string": "new"
        });

        let result = tool.execute(input).await;

        assert!(result.is_error);
        assert!(
            result.content.contains("modified externally"),
            "expected staleness error: {}",
            result.content
        );
    }

    #[tokio::test]
    async fn partial_read_does_not_authorize_a_whole_file_edit() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("partial.txt");
        std::fs::write(&file_path, "first\nsecond\n").unwrap();
        let cache = make_cache();
        let read_tool = crate::read::ReadTool::new_with_workspace_root(Some(cache.clone()), dir.path());
        let edit_tool = EditTool::new_with_workspace_root(Some(cache), dir.path());
        let read = read_tool
            .execute(json!({
                "file_path": file_path,
                "offset": 0,
                "limit": 1
            }))
            .await;

        let edit = edit_tool
            .execute(json!({
                "file_path": file_path,
                "old_string": "second",
                "new_string": "changed"
            }))
            .await;

        assert!(!read.is_error, "{}", read.content);
        assert!(edit.is_error);
        assert!(edit.content.contains("Read the file again"), "{}", edit.content);
        assert_eq!(std::fs::read_to_string(&file_path).unwrap(), "first\nsecond\n");
    }

    #[tokio::test]
    async fn edit_then_edit_succeeds_via_cache_update() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("double_edit.txt");
        std::fs::write(&file_path, "aaa bbb ccc").unwrap();

        let cache = make_cache();
        simulate_read(&cache, &file_path);

        let tool = EditTool::new_with_workspace_root(Some(cache), dir.path());

        // First edit.
        let input1 = json!({
            "file_path": file_path.to_str().unwrap(),
            "old_string": "aaa",
            "new_string": "AAA"
        });
        let r1 = tool.execute(input1).await;
        assert!(!r1.is_error, "first edit failed: {}", r1.content);

        // Second edit should succeed because first edit updated the cache.
        let input2 = json!({
            "file_path": file_path.to_str().unwrap(),
            "old_string": "bbb",
            "new_string": "BBB"
        });
        let r2 = tool.execute(input2).await;
        assert!(!r2.is_error, "second edit failed: {}", r2.content);
        assert_eq!(std::fs::read_to_string(&file_path).unwrap(), "AAA BBB ccc");
    }

    #[tokio::test]
    async fn no_cache_edit_bypasses_guard() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("nocache.txt");
        std::fs::write(&file_path, "hello").unwrap();

        let tool = EditTool::new_with_workspace_root(None, dir.path());
        let input = json!({
            "file_path": file_path.to_str().unwrap(),
            "old_string": "hello",
            "new_string": "bye"
        });

        let result = tool.execute(input).await;
        assert!(!result.is_error, "expected success without cache: {}", result.content);
        assert_eq!(std::fs::read_to_string(&file_path).unwrap(), "bye");
    }

    #[tokio::test]
    async fn replace_all_updates_cache() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("replaceall.txt");
        std::fs::write(&file_path, "a-a-a").unwrap();

        let cache = make_cache();
        simulate_read(&cache, &file_path);

        let tool = EditTool::new_with_workspace_root(Some(cache.clone()), dir.path());
        let input = json!({
            "file_path": file_path.to_str().unwrap(),
            "old_string": "a",
            "new_string": "b",
            "replace_all": true
        });

        let result = tool.execute(input).await;
        assert!(!result.is_error, "replace_all failed: {}", result.content);

        // Verify cache was updated: mtime should match current disk mtime.
        let disk_mtime = file_mtime_ms(&file_path).unwrap();
        let mut c = cache.write().unwrap();
        let cached = c.get(&file_path).expect("file should be in cache");
        assert_eq!(cached.mtime_ms, disk_mtime);
    }

    #[tokio::test]
    async fn relative_edit_effect_and_execution_use_the_workspace_root() {
        let workspace = tempdir().unwrap();
        std::fs::create_dir(workspace.path().join("nested")).unwrap();
        std::fs::write(workspace.path().join("nested/file.txt"), "before").unwrap();
        let tool = EditTool::new_with_workspace_root(None, workspace.path());

        let effect = tool.describe_effect(&json!({"file_path": "nested/file.txt"}));
        let result = tool
            .execute(json!({
                "file_path": "nested/file.txt",
                "old_string": "before",
                "new_string": "after"
            }))
            .await;
        let expected = workspace
            .path()
            .canonicalize()
            .unwrap()
            .join("nested/file.txt")
            .display()
            .to_string();

        assert_eq!(effect.resources.file_reads, vec![expected.clone()]);
        assert_eq!(effect.resources.file_writes, vec![expected]);
        assert!(!result.is_error, "{}", result.content);
        assert_eq!(
            std::fs::read_to_string(workspace.path().join("nested/file.txt")).unwrap(),
            "after"
        );
    }

    #[tokio::test]
    async fn edit_rejects_a_file_above_the_byte_limit() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("too-large.txt");
        let mut content = b"needle".to_vec();
        content.resize(8 * 1024 * 1024 + 1, b'x');
        std::fs::write(&file_path, content).unwrap();
        let tool = EditTool::new_with_workspace_root(None, dir.path());

        let result = tool
            .execute(json!({
                "file_path": file_path,
                "old_string": "needle",
                "new_string": "replacement"
            }))
            .await;

        assert!(result.is_error);
        assert!(
            result.content.contains("exceeds the 8388608-byte edit limit"),
            "{}",
            result.content
        );
    }

    #[tokio::test]
    async fn edit_accepts_a_file_exactly_at_the_byte_limit() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("at-limit.txt");
        let mut content = b"needle".to_vec();
        content.resize(MAX_EDIT_FILE_BYTES, b'x');
        std::fs::write(&file_path, content).unwrap();
        let tool = EditTool::new_with_workspace_root(None, dir.path());

        let result = tool
            .execute(json!({
                "file_path": file_path,
                "old_string": "needle",
                "new_string": "thread"
            }))
            .await;

        assert!(!result.is_error, "{}", result.content);
        let edited = std::fs::read(&file_path).unwrap();
        assert_eq!(edited.len(), MAX_EDIT_FILE_BYTES);
        assert!(edited.starts_with(b"thread"));
    }

    #[tokio::test]
    async fn edit_rejects_replacement_output_above_the_byte_limit() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("expanding.txt");
        std::fs::write(&file_path, "x".repeat(4 * 1024 * 1024 + 1)).unwrap();
        let tool = EditTool::new_with_workspace_root(None, dir.path());

        let result = tool
            .execute(json!({
                "file_path": file_path,
                "old_string": "x",
                "new_string": "xx",
                "replace_all": true
            }))
            .await;

        assert!(result.is_error);
        assert_eq!(result.content, "Edited content exceeds the 8388608-byte edit limit");
    }

    #[tokio::test]
    async fn ambient_policy_edits_absolute_and_parent_relative_paths_outside_workspace() {
        let parent = tempdir().unwrap();
        let workspace = parent.path().join("workspace");
        let outside = parent.path().join("outside");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        let absolute_path = outside.join("absolute.txt");
        let relative_target = outside.join("relative.txt");
        let relative_path = Path::new("..").join("outside/relative.txt");
        std::fs::write(&absolute_path, "before-absolute").unwrap();
        std::fs::write(&relative_target, "before-relative").unwrap();
        let restricted = EditTool::new_with_workspace_root(None, &workspace);
        let ambient = EditTool::new_with_search_policy(None, &workspace, Arc::new(AllowAmbientPaths));

        assert!(
            restricted
                .execute(json!({
                    "file_path": &absolute_path,
                    "old_string": "before",
                    "new_string": "denied"
                }))
                .await
                .is_error
        );
        assert!(
            restricted
                .execute(json!({
                    "file_path": &relative_path,
                    "old_string": "before",
                    "new_string": "denied"
                }))
                .await
                .is_error
        );
        let absolute = ambient
            .execute(json!({
                "file_path": &absolute_path,
                "old_string": "before",
                "new_string": "after"
            }))
            .await;
        let parent_relative = ambient
            .execute(json!({
                "file_path": &relative_path,
                "old_string": "before",
                "new_string": "after"
            }))
            .await;

        assert!(!absolute.is_error, "{}", absolute.content);
        assert!(!parent_relative.is_error, "{}", parent_relative.content);
        assert_eq!(std::fs::read_to_string(&absolute_path).unwrap(), "after-absolute");
        assert_eq!(std::fs::read_to_string(&relative_target).unwrap(), "after-relative");
    }
}
