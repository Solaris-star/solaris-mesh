use super::*;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::io::{BufReader, Read, Write, repeat};
    use tempfile::tempdir;

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

    // -- Basic read tests (no cache) --

    #[tokio::test]
    async fn test_read_file_full() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.txt");
        let mut file = std::fs::File::create(&file_path).unwrap();
        writeln!(file, "line one").unwrap();
        writeln!(file, "line two").unwrap();
        writeln!(file, "line three").unwrap();
        drop(file);

        let tool = ReadTool::new_with_workspace_root(None, dir.path());
        let input = json!({ "file_path": file_path.to_str().unwrap() });
        let result = tool.execute(input).await;

        assert!(!result.is_error);
        assert!(result.content.contains("1\tline one"));
        assert!(result.content.contains("2\tline two"));
        assert!(result.content.contains("3\tline three"));
    }

    #[tokio::test]
    async fn test_read_file_with_offset_and_limit() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("lines.txt");
        let mut file = std::fs::File::create(&file_path).unwrap();
        for i in 1..=10 {
            writeln!(file, "line {}", i).unwrap();
        }
        drop(file);

        let tool = ReadTool::new_with_workspace_root(None, dir.path());
        let input = json!({
            "file_path": file_path.to_str().unwrap(),
            "offset": 2,
            "limit": 3
        });
        let result = tool.execute(input).await;

        assert!(!result.is_error);
        let lines: Vec<&str> = result.content.lines().collect();
        assert_eq!(lines.len(), 3);
        assert!(lines[0].contains("3\tline 3"));
        assert!(lines[1].contains("4\tline 4"));
        assert!(lines[2].contains("5\tline 5"));
    }

    #[tokio::test]
    async fn read_rejects_a_line_range_that_overflows() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("overflow.txt");
        std::fs::write(&file_path, "one\ntwo\n").unwrap();
        let tool = ReadTool::new_with_workspace_root(None, dir.path());

        let result = tool
            .execute(json!({
                "file_path": file_path,
                "offset": u64::MAX,
                "limit": 2
            }))
            .await;

        assert!(result.is_error);
        assert_eq!(
            result.content,
            "Invalid read range: offset + limit exceeds the supported range"
        );
    }

    #[tokio::test]
    async fn full_read_rejects_content_above_the_byte_limit() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("too-large.txt");
        std::fs::write(&file_path, vec![b'a'; 8 * 1024 * 1024 + 1]).unwrap();
        let tool = ReadTool::new_with_workspace_root(None, dir.path());

        let result = tool.execute(json!({ "file_path": file_path })).await;

        assert!(result.is_error);
        assert_eq!(result.content, "Read exceeds the 8388608-byte output limit");
    }

    #[tokio::test]
    async fn full_read_accepts_output_exactly_at_the_byte_limit() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("at-limit.txt");
        let prefix_bytes = format!("{:>6}\t", 1).len();
        std::fs::write(&file_path, vec![b'a'; MAX_READ_OUTPUT_BYTES - prefix_bytes]).unwrap();
        let tool = ReadTool::new_with_workspace_root(None, dir.path());

        let result = tool.execute(json!({ "file_path": file_path })).await;

        assert!(!result.is_error, "{}", result.content);
        assert_eq!(result.content.len(), MAX_READ_OUTPUT_BYTES);
    }

    #[tokio::test]
    async fn partial_read_of_a_large_file_returns_only_requested_lines() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("large-window.txt");
        let mut file = std::fs::File::create(&file_path).unwrap();
        writeln!(file, "first").unwrap();
        file.write_all(&vec![b'x'; 8 * 1024 * 1024 + 1]).unwrap();
        writeln!(file).unwrap();
        writeln!(file, "third").unwrap();
        drop(file);
        let tool = ReadTool::new_with_workspace_root(None, dir.path());

        let result = tool
            .execute(json!({
                "file_path": file_path,
                "offset": 0,
                "limit": 1
            }))
            .await;

        assert!(!result.is_error, "{}", result.content);
        assert_eq!(result.content, "     1\tfirst");
    }

    #[test]
    fn streamed_read_stops_at_the_scan_byte_limit() {
        let source = repeat(b'x').take(u64::try_from(MAX_READ_SCAN_BYTES + 1).unwrap());
        let mut reader = BufReader::with_capacity(READ_BUFFER_BYTES, source);

        let result = read_numbered_lines(&mut reader, 1, Some(1));

        assert_eq!(result, Err(READ_SCAN_LIMIT_ERROR.to_owned()));
    }

    #[tokio::test]
    async fn streamed_read_preserves_utf8_across_buffer_boundaries_and_crlf_lines() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("utf8-boundary.txt");
        let mut content = vec![b'x'; READ_BUFFER_BYTES - 1];
        content.extend_from_slice("é\r\nnext\r".as_bytes());
        std::fs::write(&file_path, content).unwrap();
        let tool = ReadTool::new_with_workspace_root(None, dir.path());

        let result = tool.execute(json!({ "file_path": file_path })).await;

        assert!(!result.is_error, "{}", result.content);
        let lines = result.content.lines().collect::<Vec<_>>();
        assert!(lines[0].ends_with('é'));
        assert!(!lines[0].ends_with('\r'));
        assert_eq!(lines[1], "     2\tnext\r");
    }

    #[tokio::test]
    async fn test_read_nonexistent_file() {
        let tool = ReadTool::new(None);
        let input = json!({ "file_path": "/tmp/nonexistent_file_abc123.txt" });
        let result = tool.execute(input).await;

        assert!(result.is_error);
        assert!(result.content.contains("Failed to read file"));
    }

    #[tokio::test]
    async fn test_read_empty_file() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("empty.txt");
        std::fs::File::create(&file_path).unwrap();

        let tool = ReadTool::new_with_workspace_root(None, dir.path());
        let input = json!({ "file_path": file_path.to_str().unwrap() });
        let result = tool.execute(input).await;

        assert!(!result.is_error);
        assert!(result.content.is_empty());
    }

    #[tokio::test]
    async fn test_read_large_file_truncation() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("large.txt");
        let mut file = std::fs::File::create(&file_path).unwrap();
        for i in 1..=200 {
            writeln!(file, "line number {}", i).unwrap();
        }
        drop(file);

        let tool = ReadTool::new_with_workspace_root(None, dir.path());
        let input = json!({ "file_path": file_path.to_str().unwrap() });
        let result = tool.execute(input).await;

        assert!(!result.is_error);
        let lines: Vec<&str> = result.content.lines().collect();
        assert_eq!(lines.len(), 200);
        assert!(lines[0].contains("1\tline number 1"));
        assert!(lines[199].contains("200\tline number 200"));
    }

    // -- Dedup tests (with cache) --

    #[tokio::test]
    async fn dedup_returns_stub_on_unchanged_file() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("dedup.txt");
        std::fs::write(&file_path, "hello\n").unwrap();

        let cache = make_cache();
        let tool = ReadTool::new_with_workspace_root(Some(cache), dir.path());

        let input = json!({ "file_path": file_path.to_str().unwrap() });

        // First read: full content.
        let r1 = tool.execute(input.clone()).await;
        assert!(!r1.is_error);
        assert!(r1.content.contains("hello"));

        // Second read: dedup stub.
        let r2 = tool.execute(input).await;
        assert!(!r2.is_error);
        assert_eq!(r2.content, FILE_UNCHANGED_STUB);
    }

    #[tokio::test]
    async fn classified_read_marks_cache_hit_and_zero_limit_noop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("status.txt");
        std::fs::write(&path, "alpha\nbeta\n").unwrap();
        let cache = Arc::new(RwLock::new(FileStateCache::new(&FileCacheConfig::default())));
        let tool = ReadTool::new_with_workspace_root(Some(cache), dir.path());
        let input = json!({"file_path": path});

        let first = tool.execute_classified(input.clone()).await;
        let cached = tool.execute_classified(input).await;
        let noop = tool
            .execute_classified(json!({"file_path": dir.path().join("status.txt"), "limit": 0}))
            .await;

        assert_eq!(first.status, ToolResultStatus::Executed);
        assert_eq!(cached.status, ToolResultStatus::CacheHit);
        assert_eq!(noop.status, ToolResultStatus::Noop);
    }

    #[tokio::test]
    async fn dedup_returns_new_content_after_modification() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("modified.txt");
        std::fs::write(&file_path, "version1\n").unwrap();

        let cache = make_cache();
        let tool = ReadTool::new_with_workspace_root(Some(cache), dir.path());

        let input = json!({ "file_path": file_path.to_str().unwrap() });

        let r1 = tool.execute(input.clone()).await;
        assert!(r1.content.contains("version1"));

        // Modify the file — ensure mtime changes.
        std::thread::sleep(std::time::Duration::from_millis(50));
        std::fs::write(&file_path, "version2\n").unwrap();

        let r2 = tool.execute(input).await;
        assert!(!r2.is_error);
        assert!(r2.content.contains("version2"));
    }

    #[tokio::test]
    async fn dedup_different_offset_limit_returns_full() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("multi.txt");
        let mut file = std::fs::File::create(&file_path).unwrap();
        for i in 1..=20 {
            writeln!(file, "line {}", i).unwrap();
        }
        drop(file);

        let cache = make_cache();
        let tool = ReadTool::new_with_workspace_root(Some(cache), dir.path());

        let input1 = json!({
            "file_path": file_path.to_str().unwrap(),
            "offset": 0,
            "limit": 10
        });
        let r1 = tool.execute(input1).await;
        assert!(!r1.is_error);
        assert!(r1.content.contains("line 1"));

        // Different range: should return full content, not stub.
        let input2 = json!({
            "file_path": file_path.to_str().unwrap(),
            "offset": 10,
            "limit": 10
        });
        let r2 = tool.execute(input2).await;
        assert!(!r2.is_error);
        assert!(r2.content.contains("line 11"));
        assert!(!r2.content.contains(FILE_UNCHANGED_STUB));
    }

    #[tokio::test]
    async fn no_cache_always_returns_full_content() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("nocache.txt");
        std::fs::write(&file_path, "data\n").unwrap();

        let tool = ReadTool::new_with_workspace_root(None, dir.path());
        let input = json!({ "file_path": file_path.to_str().unwrap() });

        let r1 = tool.execute(input.clone()).await;
        assert!(r1.content.contains("data"));

        let r2 = tool.execute(input).await;
        assert!(r2.content.contains("data"));
        assert_ne!(r2.content, FILE_UNCHANGED_STUB);
    }

    #[tokio::test]
    async fn nonexistent_file_not_cached() {
        let cache = make_cache();
        let tool = ReadTool::new(Some(cache.clone()));

        let input = json!({ "file_path": "/tmp/nonexistent_xyz_789.txt" });
        let r = tool.execute(input).await;
        assert!(r.is_error);

        // Cache should be empty.
        let c = cache.read().unwrap();
        assert!(c.is_empty());
    }

    #[tokio::test]
    async fn dedup_empty_file() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("empty.txt");
        std::fs::File::create(&file_path).unwrap();

        let cache = make_cache();
        let tool = ReadTool::new_with_workspace_root(Some(cache), dir.path());

        let input = json!({ "file_path": file_path.to_str().unwrap() });

        let r1 = tool.execute(input.clone()).await;
        assert!(!r1.is_error);

        let r2 = tool.execute(input).await;
        assert!(!r2.is_error);
        assert_eq!(r2.content, FILE_UNCHANGED_STUB);
        assert!(r2.content.contains("force set to true"));
    }

    #[tokio::test]
    async fn force_returns_content_after_unchanged_result_was_compacted() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("compacted.txt");
        std::fs::write(&file_path, "needed again\n").unwrap();

        let cache = make_cache();
        let tool = ReadTool::new_with_workspace_root(Some(cache), dir.path());
        let input = json!({ "file_path": file_path.to_str().unwrap() });

        let first = tool.execute(input.clone()).await;
        assert!(first.content.contains("needed again"));

        let deduplicated = tool.execute(input.clone()).await;
        assert_eq!(deduplicated.content, FILE_UNCHANGED_STUB);

        let restored = tool
            .execute(json!({
                "file_path": file_path.to_str().unwrap(),
                "force": true
            }))
            .await;
        assert!(!restored.is_error);
        assert!(restored.content.contains("needed again"));
        assert_ne!(restored.content, FILE_UNCHANGED_STUB);
    }

    #[tokio::test]
    async fn compacted_result_invalidates_only_its_cached_read() {
        let dir = tempdir().unwrap();
        let first_path = dir.path().join("first.txt");
        let second_path = dir.path().join("second.txt");
        std::fs::write(&first_path, "first content\n").unwrap();
        std::fs::write(&second_path, "second content\n").unwrap();

        let cache = make_cache();
        let tool = ReadTool::new_with_workspace_root(Some(cache), dir.path());
        let first_input = json!({ "file_path": first_path.to_str().unwrap() });
        let second_input = json!({ "file_path": second_path.to_str().unwrap() });

        assert!(
            tool.execute(first_input.clone())
                .await
                .content
                .contains("first content")
        );
        assert!(
            tool.execute(second_input.clone())
                .await
                .content
                .contains("second content")
        );
        assert_eq!(tool.execute(first_input.clone()).await.content, FILE_UNCHANGED_STUB);
        assert_eq!(tool.execute(second_input.clone()).await.content, FILE_UNCHANGED_STUB);

        tool.on_result_compacted(&first_input);

        assert!(tool.execute(first_input).await.content.contains("first content"));
        assert_eq!(tool.execute(second_input).await.content, FILE_UNCHANGED_STUB);
    }

    #[tokio::test]
    async fn history_compaction_invalidates_all_cached_reads() {
        let dir = tempdir().unwrap();
        let first_path = dir.path().join("first.txt");
        let second_path = dir.path().join("second.txt");
        std::fs::write(&first_path, "first content\n").unwrap();
        std::fs::write(&second_path, "second content\n").unwrap();

        let cache = make_cache();
        let tool = ReadTool::new_with_workspace_root(Some(cache), dir.path());
        let first_input = json!({ "file_path": first_path.to_str().unwrap() });
        let second_input = json!({ "file_path": second_path.to_str().unwrap() });

        tool.execute(first_input.clone()).await;
        tool.execute(second_input.clone()).await;
        tool.on_history_compacted();

        assert!(tool.execute(first_input).await.content.contains("first content"));
        assert!(tool.execute(second_input).await.content.contains("second content"));
    }

    #[tokio::test]
    async fn workspace_read_rejects_files_outside_its_root() {
        let workspace = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let secret = outside.path().join("secret.txt");
        std::fs::write(&secret, "outside-secret").unwrap();
        let tool = ReadTool::new_with_workspace_root(None, workspace.path());

        let result = tool.execute(json!({"file_path": secret})).await;
        assert!(result.is_error);
        assert!(!result.content.contains("outside-secret"));
    }

    #[tokio::test]
    async fn ambient_policy_reads_absolute_and_parent_relative_paths_outside_workspace() {
        let parent = tempdir().unwrap();
        let workspace = parent.path().join("workspace");
        let outside = parent.path().join("outside");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        let secret = outside.join("secret.txt");
        std::fs::write(&secret, "outside-secret").unwrap();
        let relative = Path::new("..").join("outside/secret.txt");
        let restricted = ReadTool::new_with_workspace_root(None, &workspace);
        let ambient = ReadTool::new_with_search_policy(None, &workspace, Arc::new(AllowAmbientPaths));

        assert!(restricted.execute(json!({"file_path": &secret})).await.is_error);
        assert!(restricted.execute(json!({"file_path": &relative})).await.is_error);
        let absolute = ambient.execute(json!({"file_path": &secret})).await;
        let parent_relative = ambient.execute(json!({"file_path": &relative})).await;

        assert!(!absolute.is_error, "{}", absolute.content);
        assert!(absolute.content.contains("outside-secret"));
        assert!(!parent_relative.is_error, "{}", parent_relative.content);
        assert!(parent_relative.content.contains("outside-secret"));
    }

    #[tokio::test]
    async fn relative_read_effect_and_execution_use_the_workspace_root() {
        let workspace = tempdir().unwrap();
        std::fs::create_dir(workspace.path().join("nested")).unwrap();
        std::fs::write(workspace.path().join("nested/file.txt"), "workspace content").unwrap();
        let tool = ReadTool::new_with_workspace_root(None, workspace.path());

        let effect = tool.describe_effect(&json!({"file_path": "nested/file.txt"}));
        let result = tool.execute(json!({"file_path": "nested/file.txt"})).await;

        assert_eq!(
            effect.resources.file_reads,
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
        assert!(!result.is_error, "{}", result.content);
        assert!(result.content.contains("workspace content"));
    }
}
