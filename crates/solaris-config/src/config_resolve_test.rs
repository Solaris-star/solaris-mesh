    #[test]
    fn test_resolve_with_project_dir_loads_project_config() {
        let tmp = tempfile::tempdir().unwrap();
        let project_toml = tmp.path().join(".solaris.toml");
        std::fs::write(
            &project_toml,
            r#"
[default]
max_tokens = 1234
"#,
        )
        .unwrap();

        let base_cli_args = CliArgs {
            provider: Some("anthropic".into()),
            api_key: Some("test-key".into()),
            base_url: None,
            model: None,
            max_tokens: None,
            thinking: None,
            thinking_budget: None,
            max_turns: None,
            max_tool_call_malformed_turns: None,
            max_tool_call_failure_turns: None,
            system_prompt: None,
            profile: None,
            auto_approve: false,
            project_dir: Some(tmp.path().to_path_buf()),
        };

        let config = Config::resolve(&base_cli_args).unwrap();
        assert_eq!(config.max_tokens, Some(1234));
        assert_eq!(config.max_tool_call_malformed_turns, None);
        assert_eq!(config.max_tool_call_failure_turns, None);

        std::fs::write(
            &project_toml,
            r#"
[default]
max_tokens = 1234
max_tool_call_malformed_turns = 2
max_tool_call_failure_turns = 4
"#,
        )
        .unwrap();

        let config = Config::resolve(&base_cli_args).unwrap();
        assert_eq!(config.max_tool_call_malformed_turns, Some(2));
        assert_eq!(config.max_tool_call_failure_turns, Some(4));

        let cli_args = CliArgs {
            max_tool_call_malformed_turns: Some(0),
            max_tool_call_failure_turns: Some(0),
            ..base_cli_args
        };

        let config = Config::resolve(&cli_args).unwrap();
        assert_eq!(config.max_tool_call_malformed_turns, Some(0));
        assert_eq!(config.max_tool_call_failure_turns, Some(0));
    }

    #[test]
    fn test_config_resolve_loads_flat_provider_compat_after_domain_split() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join(".solaris.toml"),
            r#"
[default]
provider = "openai"
model = "test-model"

[providers.openai]
api_key = "test-key"
base_url = "https://example.test/v1"

[providers.openai.compat]
max_tokens_field = "max_completion_tokens"
api_path = "/chat/completions"
merge_assistant_messages = false
clean_orphan_tool_calls = false
clean_orphan_tool_results = false
dedup_tool_results = true
sanitize_malformed_tool_calls = false
strip_patterns = ["__REASONING__"]
auto_tool_id = false
supports_thinking = true
supports_effort = true
effort_levels = ["low", "medium"]
"#,
        )
        .unwrap();

        let cli = CliArgs {
            provider: None,
            api_key: None,
            base_url: None,
            model: None,
            max_tokens: None,
            thinking: None,
            thinking_budget: None,
            max_turns: None,
            max_tool_call_malformed_turns: None,
            max_tool_call_failure_turns: None,
            system_prompt: None,
            profile: None,
            auto_approve: false,
            project_dir: Some(tmp.path().to_path_buf()),
        };

        let config = Config::resolve(&cli).unwrap();

        assert_eq!(config.compat.max_tokens_field(), "max_completion_tokens");
        assert_eq!(config.compat.api_path(), "/chat/completions");
        assert!(!config.compat.merge_assistant_messages());
        assert!(!config.compat.clean_orphan_tool_calls());
        assert!(!config.compat.clean_orphan_tool_results());
        assert!(config.compat.dedup_tool_results());
        assert!(!config.compat.sanitize_malformed_tool_calls());
        assert!(!config.compat.auto_tool_id());
        assert!(config.compat.supports_thinking());
        assert!(config.compat.supports_effort());
        assert_eq!(config.compat.effort_levels(), &["low", "medium"]);
        assert_eq!(
            config.compat.messages.strip_patterns,
            Some(vec!["__REASONING__".to_string()])
        );
    }

    #[test]
    fn test_config_resolve_cli_thinking_records_request_without_enabling_capability() {
        let tmp = tempfile::tempdir().unwrap();
        let cli = CliArgs {
            provider: Some("openai".into()),
            api_key: Some("test-key".into()),
            base_url: None,
            model: None,
            max_tokens: None,
            thinking: Some("enabled".into()),
            thinking_budget: Some(16_000),
            max_turns: None,
            max_tool_call_malformed_turns: None,
            max_tool_call_failure_turns: None,
            system_prompt: None,
            profile: None,
            auto_approve: false,
            project_dir: Some(tmp.path().to_path_buf()),
        };

        let config = Config::resolve(&cli).unwrap();

        assert!(!config.compat.supports_thinking());
        assert!(matches!(
            config.thinking,
            Some(ThinkingConfig::Enabled { budget_tokens: 16_000 })
        ));
    }

    #[test]
    fn test_config_resolve_cli_thinking_disabled_does_not_enable_capability() {
        let tmp = tempfile::tempdir().unwrap();
        let cli = CliArgs {
            provider: Some("openai".into()),
            api_key: Some("test-key".into()),
            base_url: None,
            model: None,
            max_tokens: None,
            thinking: Some("disabled".into()),
            thinking_budget: None,
            max_turns: None,
            max_tool_call_malformed_turns: None,
            max_tool_call_failure_turns: None,
            system_prompt: None,
            profile: None,
            auto_approve: false,
            project_dir: Some(tmp.path().to_path_buf()),
        };

        let config = Config::resolve(&cli).unwrap();

        assert!(!config.compat.supports_thinking());
        assert!(matches!(config.thinking, Some(ThinkingConfig::Disabled)));
    }

    #[test]
    fn test_config_resolve_cli_thinking_preserves_explicit_unsupported_capability() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join(".solaris.toml"),
            r#"
[providers.openai.compat]
supports_thinking = false
"#,
        )
        .unwrap();
        let cli = CliArgs {
            provider: Some("openai".into()),
            api_key: Some("test-key".into()),
            base_url: None,
            model: None,
            max_tokens: None,
            thinking: Some("enabled".into()),
            thinking_budget: Some(16_000),
            max_turns: None,
            max_tool_call_malformed_turns: None,
            max_tool_call_failure_turns: None,
            system_prompt: None,
            profile: None,
            auto_approve: false,
            project_dir: Some(tmp.path().to_path_buf()),
        };

        let config = Config::resolve(&cli).unwrap();

        assert!(!config.compat.supports_thinking());
        assert!(matches!(
            config.thinking,
            Some(ThinkingConfig::Enabled { budget_tokens: 16_000 })
        ));
    }

    #[test]
    fn test_config_resolve_cli_thinking_budget_alone_does_not_enable_thinking() {
        let tmp = tempfile::tempdir().unwrap();
        let cli = CliArgs {
            provider: Some("openai".into()),
            api_key: Some("test-key".into()),
            base_url: None,
            model: None,
            max_tokens: None,
            thinking: None,
            thinking_budget: Some(12_000),
            max_turns: None,
            max_tool_call_malformed_turns: None,
            max_tool_call_failure_turns: None,
            system_prompt: None,
            profile: None,
            auto_approve: false,
            project_dir: Some(tmp.path().to_path_buf()),
        };

        let config = Config::resolve(&cli).unwrap();

        assert!(!config.compat.supports_thinking());
        assert!(config.thinking.is_none());
    }

    #[test]
    fn test_config_resolve_rejects_invalid_cli_thinking() {
        let tmp = tempfile::tempdir().unwrap();
        let cli = CliArgs {
            provider: Some("openai".into()),
            api_key: Some("test-key".into()),
            base_url: None,
            model: None,
            max_tokens: None,
            thinking: Some("auto".into()),
            thinking_budget: None,
            max_turns: None,
            max_tool_call_malformed_turns: None,
            max_tool_call_failure_turns: None,
            system_prompt: None,
            profile: None,
            auto_approve: false,
            project_dir: Some(tmp.path().to_path_buf()),
        };

        let err = Config::resolve(&cli).unwrap_err().to_string();

        assert!(err.contains("Invalid --thinking value"));
    }

    #[test]
    fn test_config_resolve_normalizes_official_openai_root_base_url() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join(".solaris.toml"),
            r#"
[providers.openai]
base_url = "https://api.openai.com"
"#,
        )
        .unwrap();
        let cli = CliArgs {
            provider: Some("openai".into()),
            api_key: Some("test-key".into()),
            base_url: None,
            model: None,
            max_tokens: None,
            thinking: None,
            thinking_budget: None,
            max_turns: None,
            max_tool_call_malformed_turns: None,
            max_tool_call_failure_turns: None,
            system_prompt: None,
            profile: None,
            auto_approve: false,
            project_dir: Some(tmp.path().to_path_buf()),
        };

        let config = Config::resolve(&cli).unwrap();

        assert_eq!(config.base_url, "https://api.openai.com/v1");
    }

    #[test]
    fn test_config_resolve_does_not_normalize_openai_compatible_base_url() {
        let tmp = tempfile::tempdir().unwrap();
        let cli = CliArgs {
            provider: Some("openai".into()),
            api_key: Some("test-key".into()),
            base_url: Some("https://api.deepseek.com".into()),
            model: None,
            max_tokens: None,
            thinking: None,
            thinking_budget: None,
            max_turns: None,
            max_tool_call_malformed_turns: None,
            max_tool_call_failure_turns: None,
            system_prompt: None,
            profile: None,
            auto_approve: false,
            project_dir: Some(tmp.path().to_path_buf()),
        };

        let config = Config::resolve(&cli).unwrap();

        assert_eq!(config.base_url, "https://api.deepseek.com");
    }

    #[test]
    fn test_config_resolve_loads_flat_provider_max_tool_count_limits() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join(".solaris.toml"),
            r#"
[default]
provider = "gemini"
model = "test-model"

[providers.gemini]
provider = "openai"
api_key = "test-key"
base_url = "https://example.test/v1"

[providers.gemini.compat]
max_tool_count = 512
max_request_body_bytes = 1048576
"#,
        )
        .unwrap();

        let cli = CliArgs {
            provider: None,
            api_key: None,
            base_url: None,
            model: None,
            max_tokens: None,
            thinking: None,
            thinking_budget: None,
            max_turns: None,
            max_tool_call_malformed_turns: None,
            max_tool_call_failure_turns: None,
            system_prompt: None,
            profile: None,
            auto_approve: false,
            project_dir: Some(tmp.path().to_path_buf()),
        };

        let config = Config::resolve(&cli).unwrap();

        assert_eq!(config.compat.max_tool_count(), Some(512));
        assert_eq!(config.compat.max_request_body_bytes(), Some(1_048_576));
    }

    #[test]
    fn test_openai_field_controls_alias_and_profile_override_flattened_compat() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join(".solaris.toml"),
            r#"
[default]
provider = "nim"
model = "alias-model"

[providers.openai]
api_key = "builtin-key"
base_url = "https://api.openai.test/v1"

[providers.openai.compat]
include_stream_options = true
emit_tools = true
supports_effort = true

[providers.nim]
provider = "openai"
api_key = "alias-key"
base_url = "https://nim.example.test/v1"

[providers.nim.compat]
include_stream_options = false
emit_tools = false
supports_effort = false

[profiles.restore-openai-fields]
provider = "nim"

[profiles.restore-openai-fields.compat]
include_stream_options = true
emit_tools = true
supports_effort = true
"#,
        )
        .unwrap();

        let base_cli = CliArgs {
            provider: None,
            api_key: None,
            base_url: None,
            model: None,
            max_tokens: None,
            thinking: None,
            thinking_budget: None,
            max_turns: None,
            max_tool_call_malformed_turns: None,
            max_tool_call_failure_turns: None,
            system_prompt: None,
            profile: None,
            auto_approve: false,
            project_dir: Some(tmp.path().to_path_buf()),
        };

        let alias_config = Config::resolve(&base_cli).unwrap();
        assert_eq!(alias_config.provider, ProviderType::OpenAI);
        assert_eq!(alias_config.provider_label, "nim");
        assert!(!alias_config.compat.include_stream_options());
        assert!(!alias_config.compat.emit_tools());
        assert!(!alias_config.compat.supports_effort());

        let profile_config = Config::resolve(&CliArgs {
            profile: Some("restore-openai-fields".to_string()),
            ..base_cli
        })
        .unwrap();
        assert_eq!(profile_config.provider, ProviderType::OpenAI);
        assert_eq!(profile_config.provider_label, "nim");
        assert!(profile_config.compat.include_stream_options());
        assert!(profile_config.compat.emit_tools());
        assert!(profile_config.compat.supports_effort());
    }

    #[test]
    fn test_config_resolve_tool_wire_shape_override_from_provider_compat() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join(".solaris.toml"),
            r#"
[default]
provider = "openai"
model = "test-model"

[providers.openai]
api_key = "test-key"
base_url = "https://example.test/v1"

[providers.openai.compat]
tool_wire_shape = "anthropic_input_schema"
"#,
        )
        .unwrap();

        let cli = CliArgs {
            provider: None,
            api_key: None,
            base_url: None,
            model: None,
            max_tokens: None,
            thinking: None,
            thinking_budget: None,
            max_turns: None,
            max_tool_call_malformed_turns: None,
            max_tool_call_failure_turns: None,
            system_prompt: None,
            profile: None,
            auto_approve: false,
            project_dir: Some(tmp.path().to_path_buf()),
        };

        let config = Config::resolve(&cli).unwrap();

        assert_eq!(config.compat.tool_wire_shape(), ToolWireShape::AnthropicInputSchema);
    }

    #[test]
    fn test_resolve_zero_max_turns_disables_turn_limit() {
        let tmp = tempfile::tempdir().unwrap();
        let cli_args = CliArgs {
            provider: Some("anthropic".into()),
            api_key: Some("test-key".into()),
            base_url: None,
            model: None,
            max_tokens: None,
            thinking: None,
            thinking_budget: None,
            max_turns: Some(0),
            max_tool_call_malformed_turns: None,
            max_tool_call_failure_turns: None,
            system_prompt: None,
            profile: None,
            auto_approve: false,
            project_dir: Some(tmp.path().to_path_buf()),
        };

        let config = Config::resolve(&cli_args).unwrap();
        assert_eq!(config.max_turns, None);
    }

    #[test]
    fn test_resolve_without_project_dir_uses_cwd() {
        let cli_args = CliArgs {
            provider: Some("anthropic".into()),
            api_key: Some("test-key".into()),
            base_url: None,
            model: None,
            max_tokens: None,
            thinking: None,
            thinking_budget: None,
            max_turns: None,
            max_tool_call_malformed_turns: None,
            max_tool_call_failure_turns: None,
            system_prompt: None,
            profile: None,
            auto_approve: false,
            project_dir: None,
        };

        let config = Config::resolve(&cli_args);
        assert!(config.is_ok());
    }
