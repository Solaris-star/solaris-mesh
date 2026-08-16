use super::*;

// ===========================================================================
// Unit tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::path::Path;

    // -- sanitize_path --------------------------------------------------------

    #[test]
    fn sanitize_simple_path() {
        assert_eq!(sanitize_path("/home/user/project"), "-home-user-project");
    }

    #[test]
    fn sanitize_preserves_alphanumeric() {
        assert_eq!(sanitize_path("abc123"), "abc123");
    }

    #[test]
    fn sanitize_replaces_special_chars() {
        assert_eq!(sanitize_path("a/b:c d"), "a-b-c-d");
    }

    #[test]
    fn sanitize_long_path_truncates_with_hash() {
        let long_path = "/".to_string() + &"a".repeat(300);
        let result = sanitize_path(&long_path);
        assert!(result.len() > MAX_SANITIZED_LENGTH); // truncated + hash
        assert!(result.len() < MAX_SANITIZED_LENGTH + 20); // hash isn't huge
        assert!(result.contains('-')); // has separator before hash
    }

    #[test]
    fn sanitize_two_long_paths_produce_different_results() {
        let path_a = "/".to_string() + &"a".repeat(300);
        let path_b = "/".to_string() + &"b".repeat(300);
        assert_ne!(sanitize_path(&path_a), sanitize_path(&path_b));
    }

    // -- contains_traversal ---------------------------------------------------

    #[test]
    fn traversal_detected() {
        assert!(contains_traversal("../foo"));
        assert!(contains_traversal("foo/../bar"));
        assert!(contains_traversal("/foo/.."));
        assert!(contains_traversal("foo\\..\\bar"));
    }

    #[test]
    fn traversal_not_detected_for_safe_paths() {
        assert!(!contains_traversal("/foo/bar"));
        assert!(!contains_traversal("foo.bar"));
        assert!(!contains_traversal("foo...bar"));
        assert!(!contains_traversal("/tmp/test.md"));
    }

    // -- validate_memory_path -------------------------------------------------

    #[test]
    fn validate_rejects_relative_path() {
        let err = validate_memory_path(Path::new("relative/path")).unwrap_err();
        assert!(matches!(err, MemoryError::PathValidation(_)));
        assert!(err.to_string().contains("absolute"));
    }

    #[cfg(unix)]
    #[test]
    fn validate_rejects_short_path() {
        let err = validate_memory_path(Path::new("/a")).unwrap_err();
        assert!(matches!(err, MemoryError::PathValidation(_)));
        assert!(err.to_string().contains("short"));
    }

    #[cfg(windows)]
    #[test]
    fn validate_rejects_short_path() {
        let err = validate_memory_path(Path::new("C:\\a")).unwrap_err();
        assert!(matches!(err, MemoryError::PathValidation(_)));
        assert!(err.to_string().contains("short"));
    }

    #[cfg(unix)]
    #[test]
    fn validate_rejects_traversal() {
        let err = validate_memory_path(Path::new("/tmp/../../../etc/passwd")).unwrap_err();
        assert!(matches!(err, MemoryError::PathValidation(_)));
        assert!(err.to_string().contains("traversal"));
    }

    #[cfg(windows)]
    #[test]
    fn validate_rejects_traversal() {
        let err = validate_memory_path(Path::new("C:\\tmp\\..\\..\\..\\etc\\passwd")).unwrap_err();
        assert!(matches!(err, MemoryError::PathValidation(_)));
        assert!(err.to_string().contains("traversal"));
    }

    #[cfg(unix)]
    #[test]
    fn validate_accepts_normal_absolute_path() {
        let result = validate_memory_path(Path::new("/tmp/memory/test.md"));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), PathBuf::from("/tmp/memory/test.md"));
    }

    #[cfg(windows)]
    #[test]
    fn validate_accepts_normal_absolute_path() {
        let result = validate_memory_path(Path::new("C:\\tmp\\memory\\test.md"));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), PathBuf::from("C:\\tmp\\memory\\test.md"));
    }

    // -- memory_entrypoint ----------------------------------------------------

    #[test]
    fn entrypoint_appends_memory_md() {
        let dir = Path::new("/base/memory");
        assert_eq!(memory_entrypoint(dir), PathBuf::from("/base/memory/MEMORY.md"));
    }

    // -- is_memory_path -------------------------------------------------------

    #[test]
    fn is_memory_path_inside() {
        // Use temp dir so paths actually exist for canonicalization
        let tmp = tempfile::tempdir().unwrap();
        let mem_dir = tmp.path().join("memory");
        fs::create_dir_all(&mem_dir).unwrap();
        let file = mem_dir.join("test.md");
        fs::write(&file, "").unwrap();

        assert!(is_memory_path(&file, &mem_dir));
    }

    #[test]
    fn is_memory_path_outside() {
        let tmp = tempfile::tempdir().unwrap();
        let mem_dir = tmp.path().join("memory");
        fs::create_dir_all(&mem_dir).unwrap();
        let outside = tmp.path().join("other.md");
        fs::write(&outside, "").unwrap();

        assert!(!is_memory_path(&outside, &mem_dir));
    }

    #[test]
    fn is_memory_path_nonexistent_returns_false() {
        // Non-existent paths with no common prefix
        assert!(!is_memory_path(
            Path::new("/nonexistent/a/b.md"),
            Path::new("/different/dir"),
        ));
    }

    #[test]
    fn is_memory_path_traversal_in_nonexistent_path_returns_false() {
        // Non-existent path with `..` must not bypass membership check
        // (regression test for review-1.3 ISSUE-1)
        assert!(!is_memory_path(
            Path::new("/base/memory/../../../etc/passwd"),
            Path::new("/base/memory"),
        ));
    }

    // -- ensure_memory_dir ----------------------------------------------------

    #[test]
    fn ensure_creates_nested_dirs() {
        let tmp = tempfile::tempdir().unwrap();
        let deep = tmp.path().join("a").join("b").join("c");
        assert!(!deep.exists());
        ensure_memory_dir(&deep).unwrap();
        assert!(deep.is_dir());
    }

    #[test]
    fn ensure_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("memory");
        ensure_memory_dir(&dir).unwrap();
        // Second call should not error
        ensure_memory_dir(&dir).unwrap();
        assert!(dir.is_dir());
    }

    // -- memory_base_dir (env override) ---------------------------------------

    #[test]
    #[serial(env)]
    fn base_dir_solaris_env_override() {
        let saved = save_memory_env();
        set_memory_env(Some("/custom/memory"), None);

        assert_eq!(memory_base_dir(), Some(PathBuf::from("/custom/memory")));

        restore_memory_env(saved);
    }

    #[test]
    #[serial(env)]
    fn base_dir_solaris_env_takes_priority_over_legacy_alias() {
        let saved = save_memory_env();
        set_memory_env(Some("/new/memory"), Some("/legacy/memory"));

        assert_eq!(memory_base_dir(), Some(PathBuf::from("/new/memory")));

        restore_memory_env(saved);
    }

    #[test]
    #[serial(env)]
    fn base_dir_legacy_env_remains_a_compatibility_alias() {
        let saved = save_memory_env();
        set_memory_env(None, Some("/legacy/memory"));

        assert_eq!(memory_base_dir(), Some(PathBuf::from("/legacy/memory")));

        restore_memory_env(saved);
    }

    #[test]
    #[serial(env)]
    fn base_dir_empty_env_falls_through() {
        let saved = save_memory_env();
        set_memory_env(Some(""), Some(""));

        let result = memory_base_dir();
        assert_ne!(result, Some(PathBuf::from("")));

        restore_memory_env(saved);
    }

    // -- auto_memory_dir ------------------------------------------------------

    #[test]
    #[serial(env)]
    fn auto_memory_dir_structure() {
        let saved = save_memory_env();
        set_memory_env(Some("/base"), None);

        let dir = auto_memory_dir(Path::new("/home/user/project")).unwrap();
        assert_eq!(dir, PathBuf::from("/base/projects/-home-user-project/memory"));

        restore_memory_env(saved);
    }

    fn save_memory_env() -> (Option<String>, Option<String>) {
        (
            std::env::var(MEMORY_DIR_ENV).ok(),
            std::env::var(LEGACY_MEMORY_DIR_ENV).ok(),
        )
    }

    fn set_memory_env(solaris: Option<&str>, legacy: Option<&str>) {
        // SAFETY: only called from #[serial(env)] tests.
        unsafe {
            match solaris {
                Some(value) => std::env::set_var(MEMORY_DIR_ENV, value),
                None => std::env::remove_var(MEMORY_DIR_ENV),
            }
            match legacy {
                Some(value) => std::env::set_var(LEGACY_MEMORY_DIR_ENV, value),
                None => std::env::remove_var(LEGACY_MEMORY_DIR_ENV),
            }
        }
    }

    fn restore_memory_env(saved: (Option<String>, Option<String>)) {
        set_memory_env(saved.0.as_deref(), saved.1.as_deref());
    }

    // -- normalize_lexical ----------------------------------------------------

    #[test]
    fn normalize_collapses_dot() {
        let input = Path::new("/foo/./bar/./baz");
        assert_eq!(normalize_lexical(input), PathBuf::from("/foo/bar/baz"));
    }

    #[test]
    fn normalize_preserves_absolute() {
        let input = Path::new("/foo/bar");
        assert_eq!(normalize_lexical(input), PathBuf::from("/foo/bar"));
    }
}
