use super::*;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Barrier;
    use std::sync::atomic::AtomicUsize;
    use tempfile::tempdir;

    use crate::Tool;
    use crate::file_cache::file_mtime_ms;
    #[cfg(windows)]
    use crate::test_support::create_windows_junction;
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

    struct CountingDirectoryPolicy {
        checks: AtomicUsize,
    }

    impl WorkspaceSearchPolicy for CountingDirectoryPolicy {
        fn allows_read(&self, _path: &Path) -> bool {
            true
        }

        fn allows_directory(&self, _path: &Path) -> bool {
            self.checks.fetch_add(1, Ordering::SeqCst);
            true
        }

        fn allows_opened_directory(&self, _path: &Path, _identity: Arc<OpenedFileIdentity>) -> bool {
            true
        }
    }

    #[test]
    fn path_collection_counts_empty_directories_toward_the_entry_limit() {
        let workspace = tempdir().unwrap();
        for index in 0..20 {
            std::fs::create_dir(workspace.path().join(format!("empty-{index:02}"))).unwrap();
        }
        let policy = Arc::new(CountingDirectoryPolicy {
            checks: AtomicUsize::new(0),
        });
        let access = WorkspaceFileAccess::new_with_search_policy(workspace.path(), policy.clone());

        let paths = access
            .collect_paths_with_limits(
                Path::new("."),
                WorkspaceTraversalLimits {
                    max_entries: 5,
                    max_depth: 64,
                },
            )
            .unwrap();

        assert!(paths.is_empty());
        assert!(
            policy.checks.load(Ordering::SeqCst) <= 6,
            "the root plus at most five entries may be inspected"
        );
    }

    // -- Legacy tests (no cache) --

    #[tokio::test]
    async fn test_write_new_file() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("hello.txt");

        let input = json!({
            "file_path": file_path.to_str().unwrap(),
            "content": "hello world"
        });

        let tool = WriteTool::new_with_workspace_root(None, dir.path());
        let result = tool.execute(input).await;

        assert!(!result.is_error, "expected success, got: {}", result.content);
        assert!(file_path.exists(), "file should exist after write");
        assert_eq!(std::fs::read_to_string(&file_path).unwrap(), "hello world");
    }

    #[tokio::test]
    async fn test_write_creates_parent_dirs() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("subdir/nested/file.txt");

        let input = json!({
            "file_path": file_path.to_str().unwrap(),
            "content": "nested content"
        });

        let tool = WriteTool::new_with_workspace_root(None, dir.path());
        let result = tool.execute(input).await;

        assert!(!result.is_error, "expected success, got: {}", result.content);
        assert!(file_path.parent().unwrap().exists(), "parent dirs should be created");
        assert_eq!(std::fs::read_to_string(&file_path).unwrap(), "nested content");
    }

    #[tokio::test]
    async fn test_write_overwrite_existing() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("overwrite.txt");

        let tool = WriteTool::new_with_workspace_root(None, dir.path());

        let input1 = json!({
            "file_path": file_path.to_str().unwrap(),
            "content": "original"
        });
        let result1 = tool.execute(input1).await;
        assert!(!result1.is_error);
        assert!(result1.content.contains("Created"));

        let input2 = json!({
            "file_path": file_path.to_str().unwrap(),
            "content": "replaced"
        });
        let result2 = tool.execute(input2).await;
        assert!(!result2.is_error);
        assert!(result2.content.contains("Updated"));

        assert_eq!(std::fs::read_to_string(&file_path).unwrap(), "replaced");
    }

    #[tokio::test]
    async fn test_write_file_content_matches() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("exact.txt");

        let content = "line 1\nline 2\nline 3\n";
        let input = json!({
            "file_path": file_path.to_str().unwrap(),
            "content": content
        });

        let tool = WriteTool::new_with_workspace_root(None, dir.path());
        let result = tool.execute(input).await;

        assert!(!result.is_error, "expected success, got: {}", result.content);

        let read_back = std::fs::read_to_string(&file_path).unwrap();
        assert_eq!(
            read_back, content,
            "read-back content must exactly match written content"
        );
    }

    // -- Cache integration tests --

    #[tokio::test]
    async fn write_populates_cache() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("cached.txt");

        let cache = make_cache();
        let tool = WriteTool::new_with_workspace_root(Some(cache.clone()), dir.path());

        let input = json!({
            "file_path": file_path.to_str().unwrap(),
            "content": "cached content"
        });
        let result = tool.execute(input).await;
        assert!(!result.is_error, "write failed: {}", result.content);

        // Cache should have an entry with correct mtime.
        let disk_mtime = file_mtime_ms(&file_path).unwrap();
        let mut c = cache.write().unwrap();
        let cached = c.get(&file_path).expect("file should be in cache after write");
        assert_eq!(cached.mtime_ms, disk_mtime);
        assert!(cached.content.contains("cached content"));
    }

    #[tokio::test]
    async fn write_then_edit_succeeds() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("write_edit.txt");

        let cache = make_cache();
        let write_tool = WriteTool::new_with_workspace_root(Some(cache.clone()), dir.path());
        let edit_tool = crate::edit::EditTool::new_with_workspace_root(Some(cache), dir.path());

        // Write creates the file and populates cache.
        let write_input = json!({
            "file_path": file_path.to_str().unwrap(),
            "content": "hello world"
        });
        let wr = write_tool.execute(write_input).await;
        assert!(!wr.is_error, "write failed: {}", wr.content);

        // Edit should succeed without needing a separate Read.
        let edit_input = json!({
            "file_path": file_path.to_str().unwrap(),
            "old_string": "hello",
            "new_string": "goodbye"
        });
        let er = edit_tool.execute(edit_input).await;
        assert!(!er.is_error, "edit after write failed: {}", er.content);
        assert_eq!(std::fs::read_to_string(&file_path).unwrap(), "goodbye world");
    }

    #[tokio::test]
    async fn write_overwrite_updates_cache_mtime() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("overwrite_cache.txt");

        let cache = make_cache();
        let tool = WriteTool::new_with_workspace_root(Some(cache.clone()), dir.path());

        // First write.
        let input1 = json!({
            "file_path": file_path.to_str().unwrap(),
            "content": "v1"
        });
        tool.execute(input1).await;

        let mtime1 = {
            let mut c = cache.write().unwrap();
            c.get(&file_path).unwrap().mtime_ms
        };

        // Brief delay to ensure mtime changes.
        std::thread::sleep(std::time::Duration::from_millis(50));

        // Second write.
        let input2 = json!({
            "file_path": file_path.to_str().unwrap(),
            "content": "v2"
        });
        tool.execute(input2).await;

        let mtime2 = {
            let mut c = cache.write().unwrap();
            c.get(&file_path).unwrap().mtime_ms
        };

        assert!(mtime2 >= mtime1, "cache mtime should update after overwrite");
    }

    #[tokio::test]
    async fn workspace_handle_rejects_parent_replaced_with_external_link_after_validation() {
        let parent = tempdir().unwrap();
        let workspace = parent.path().join("workspace");
        let outside = parent.path().join("outside");
        let guarded_parent = workspace.join("guarded");
        std::fs::create_dir_all(&guarded_parent).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        let outside_file = outside.join("target.txt");
        std::fs::write(&outside_file, b"outside").unwrap();
        let access = WorkspaceFileAccess::new(&workspace);
        let relative = access.relative_path(&guarded_parent.join("target.txt")).unwrap();
        std::fs::rename(&guarded_parent, workspace.join("guarded-original")).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&outside, &guarded_parent).unwrap();
        #[cfg(windows)]
        create_windows_junction(&outside, &guarded_parent).await;

        let result = access.write_atomic(&relative, b"changed");

        assert!(result.is_err());
        assert_eq!(std::fs::read(&outside_file).unwrap(), b"outside");
    }

    struct SwapTargetPolicy {
        barrier: Arc<Barrier>,
        checks: AtomicU64,
    }

    impl WorkspaceSearchPolicy for SwapTargetPolicy {
        fn allows_read(&self, _path: &Path) -> bool {
            true
        }

        fn allows_file_slot(&self, _path: &Path, _parent_identity: &OpenedFileIdentity, _file_name: &OsStr) -> bool {
            if self.checks.fetch_add(1, Ordering::SeqCst) == 1 {
                self.barrier.wait();
                self.barrier.wait();
            }
            true
        }
    }

    #[test]
    fn atomic_write_rejects_target_replaced_after_the_first_opened_identity_check() {
        let workspace = tempdir().unwrap();
        let target = workspace.path().join("target.txt");
        let protected = workspace.path().join("protected.sqlite3");
        std::fs::write(&target, b"ordinary").unwrap();
        std::fs::write(&protected, b"protected").unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let access = WorkspaceFileAccess::new_with_search_policy(
            workspace.path(),
            Arc::new(SwapTargetPolicy {
                barrier: Arc::clone(&barrier),
                checks: AtomicU64::new(0),
            }),
        );
        let target_for_thread = target.clone();
        let protected_for_thread = protected.clone();
        let attacker = std::thread::spawn(move || {
            barrier.wait();
            std::fs::remove_file(&target_for_thread).unwrap();
            std::fs::hard_link(&protected_for_thread, &target_for_thread).unwrap();
            barrier.wait();
        });

        let result = access.write_atomic(Path::new("target.txt"), b"replacement");
        attacker.join().unwrap();

        assert!(result.is_err());
        assert_eq!(std::fs::read(&protected).unwrap(), b"protected");
    }

    #[test]
    fn atomic_edit_rejects_in_place_content_change_after_the_first_identity_check() {
        let workspace = tempdir().unwrap();
        let target = workspace.path().join("target.txt");
        std::fs::write(&target, b"original").unwrap();
        let snapshot = WorkspaceFileAccess::new(workspace.path())
            .read_snapshot_limited(Path::new("target.txt"), 1024)
            .unwrap()
            .unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let access = WorkspaceFileAccess::new_with_search_policy(
            workspace.path(),
            Arc::new(SwapTargetPolicy {
                barrier: Arc::clone(&barrier),
                checks: AtomicU64::new(0),
            }),
        );
        let target_for_thread = target.clone();
        let attacker = std::thread::spawn(move || {
            barrier.wait();
            std::fs::write(&target_for_thread, b"external").unwrap();
            barrier.wait();
        });

        let result = access.write_atomic_if_unchanged(
            Path::new("target.txt"),
            b"replacement",
            Some((&snapshot.identity, &snapshot.bytes)),
        );
        attacker.join().unwrap();

        assert!(result.is_err());
        assert_eq!(std::fs::read(&target).unwrap(), b"external");
    }

    #[test]
    fn atomic_write_rejects_temporary_name_replaced_before_rename() {
        let workspace = tempdir().unwrap();
        let target = workspace.path().join("target.txt");
        let protected = workspace.path().join("protected.sqlite3");
        std::fs::write(&target, b"ordinary").unwrap();
        std::fs::write(&protected, b"protected").unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let access = WorkspaceFileAccess::new_with_search_policy(
            workspace.path(),
            Arc::new(SwapTargetPolicy {
                barrier: Arc::clone(&barrier),
                checks: AtomicU64::new(0),
            }),
        );
        let workspace_path = workspace.path().to_path_buf();
        let protected_for_thread = protected.clone();
        let attacker = std::thread::spawn(move || {
            barrier.wait();
            let temporary = std::fs::read_dir(&workspace_path)
                .unwrap()
                .find_map(|entry| {
                    let entry = entry.ok()?;
                    entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with(".target.txt.tmp.")
                        .then(|| entry.path())
                })
                .expect("workspace temporary file");
            std::fs::remove_file(&temporary).unwrap();
            std::fs::hard_link(&protected_for_thread, temporary).unwrap();
            barrier.wait();
        });

        let result = access.write_atomic(Path::new("target.txt"), b"replacement");
        attacker.join().unwrap();

        assert!(result.is_err());
        assert_eq!(std::fs::read(&target).unwrap(), b"ordinary");
        assert_eq!(std::fs::read(&protected).unwrap(), b"protected");
        assert!(std::fs::read_dir(workspace.path()).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".target.txt.tmp.")
        }));
    }

    #[tokio::test]
    async fn relative_write_effect_and_execution_use_the_workspace_root() {
        let workspace = tempdir().unwrap();
        let tool = WriteTool::new_with_workspace_root(None, workspace.path());

        let effect = tool.describe_effect(&json!({"file_path": "nested/file.txt"}));
        let result = tool
            .execute(json!({"file_path": "nested/file.txt", "content": "workspace content"}))
            .await;

        assert_eq!(
            effect.resources.file_writes,
            vec![
                workspace
                    .path()
                    .canonicalize()
                    .unwrap()
                    .join("nested/file.txt")
                    .display()
                    .to_string()
            ]
        );
        assert_eq!(effect.class, EffectClass::WorkspaceMutation);
        assert_eq!(effect.replay_policy, EffectReplayPolicy::ReconcileRequired);
        assert!(!result.is_error, "{}", result.content);
        assert_eq!(
            std::fs::read_to_string(workspace.path().join("nested/file.txt")).unwrap(),
            "workspace content"
        );
    }

    #[tokio::test]
    async fn ambient_policy_writes_absolute_and_parent_relative_paths_outside_workspace() {
        let parent = tempdir().unwrap();
        let workspace = parent.path().join("workspace");
        let outside = parent.path().join("outside");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        let absolute_path = outside.join("absolute.txt");
        let relative_path = Path::new("..").join("outside/relative.txt");
        let restricted = WriteTool::new_with_workspace_root(None, &workspace);
        let ambient = WriteTool::new_with_search_policy(None, &workspace, Arc::new(AllowAmbientPaths));

        assert!(
            restricted
                .execute(json!({"file_path": &absolute_path, "content": "denied"}))
                .await
                .is_error
        );
        assert!(
            restricted
                .execute(json!({"file_path": &relative_path, "content": "denied"}))
                .await
                .is_error
        );
        let absolute = ambient
            .execute(json!({"file_path": &absolute_path, "content": "absolute-write"}))
            .await;
        let parent_relative = ambient
            .execute(json!({"file_path": &relative_path, "content": "relative-write"}))
            .await;

        assert!(!absolute.is_error, "{}", absolute.content);
        assert!(!parent_relative.is_error, "{}", parent_relative.content);
        assert_eq!(std::fs::read_to_string(&absolute_path).unwrap(), "absolute-write");
        assert_eq!(
            std::fs::read_to_string(outside.join("relative.txt")).unwrap(),
            "relative-write"
        );
    }
}
