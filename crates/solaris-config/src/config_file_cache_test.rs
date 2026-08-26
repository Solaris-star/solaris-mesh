    // -------------------------------------------------------------------------
    // Phase 5.5: FileCacheConfig in ConfigFile / merge
    // -------------------------------------------------------------------------

    #[test]
    fn tc_5_5_04_file_cache_toml_deserialization() {
        let toml_str = r#"
[file_cache]
max_entries = 50
max_size_bytes = 10485760
enabled = false
"#;
        let config: ConfigFile = toml::from_str(toml_str).unwrap();
        assert_eq!(config.file_cache.max_entries, 50);
        assert_eq!(config.file_cache.max_size_bytes, 10_485_760);
        assert!(!config.file_cache.enabled);
    }

    #[test]
    fn tc_5_5_02_file_cache_defaults_when_absent() {
        let config: ConfigFile = toml::from_str("").unwrap();
        assert_eq!(config.file_cache.max_entries, 100);
        assert_eq!(config.file_cache.max_size_bytes, 25 * 1024 * 1024);
        assert!(config.file_cache.enabled);
    }

    #[test]
    fn tc_5_5_01_file_cache_custom_capacity_propagates() {
        let toml_str = r#"
[file_cache]
max_entries = 50
"#;
        let config: ConfigFile = toml::from_str(toml_str).unwrap();
        assert_eq!(config.file_cache.max_entries, 50);
        // Other fields keep defaults.
        assert_eq!(config.file_cache.max_size_bytes, 25 * 1024 * 1024);
        assert!(config.file_cache.enabled);
    }

    #[test]
    fn tc_5_5_03_file_cache_disabled_propagates() {
        let toml_str = r#"
[file_cache]
enabled = false
"#;
        let config: ConfigFile = toml::from_str(toml_str).unwrap();
        assert!(!config.file_cache.enabled);
    }

    #[test]
    fn merge_file_cache_project_overrides_global() {
        let global = ConfigFile {
            file_cache: FileCacheConfig {
                max_entries: 200,
                max_size_bytes: 50 * 1024 * 1024,
                enabled: true,
            },
            ..Default::default()
        };
        let project = ConfigFile {
            file_cache: FileCacheConfig {
                max_entries: 50,
                ..Default::default()
            },
            ..Default::default()
        };

        let merged = merge_config_files(global, project);
        assert_eq!(
            merged.file_cache.max_entries, 50,
            "project non-default max_entries should override global"
        );
    }

    #[test]
    fn merge_file_cache_global_preserved_when_project_default() {
        let global = ConfigFile {
            file_cache: FileCacheConfig {
                max_entries: 200,
                max_size_bytes: 50 * 1024 * 1024,
                enabled: true,
            },
            ..Default::default()
        };
        let project = ConfigFile::default();

        let merged = merge_config_files(global, project);
        assert_eq!(
            merged.file_cache.max_entries, 200,
            "global should be preserved when project is all-default"
        );
        assert_eq!(merged.file_cache.max_size_bytes, 50 * 1024 * 1024);
    }

    #[test]
    fn merge_file_cache_project_max_size_bytes_overrides_global() {
        // R-5.5-01: project changes only max_size_bytes (enabled=true, max_entries=default).
        let global = ConfigFile {
            file_cache: FileCacheConfig {
                max_entries: 100,
                max_size_bytes: 50 * 1024 * 1024,
                enabled: true,
            },
            ..Default::default()
        };
        let project = ConfigFile {
            file_cache: FileCacheConfig {
                max_entries: 100,                 // default
                max_size_bytes: 10 * 1024 * 1024, // non-default
                enabled: true,                    // default
            },
            ..Default::default()
        };

        let merged = merge_config_files(global, project);
        assert_eq!(
            merged.file_cache.max_size_bytes,
            10 * 1024 * 1024,
            "project max_size_bytes should override global"
        );
    }

    #[test]
    fn merge_file_cache_disabled_overrides_global() {
        let global = ConfigFile {
            file_cache: FileCacheConfig {
                enabled: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let project = ConfigFile {
            file_cache: FileCacheConfig {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        };

        let merged = merge_config_files(global, project);
        assert!(
            !merged.file_cache.enabled,
            "project enabled=false should override global"
        );
    }
