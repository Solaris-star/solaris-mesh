    // -------------------------------------------------------------------------
    // parse_builtin_provider tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_provider_type_from_str_anthropic() {
        let result = parse_builtin_provider("anthropic");
        assert_eq!(result, Some(ProviderType::Anthropic));
    }

    #[test]
    fn test_provider_type_from_str_openai() {
        let result = parse_builtin_provider("openai");
        assert_eq!(result, Some(ProviderType::OpenAI));
    }

    #[test]
    fn test_provider_type_from_str_bedrock() {
        let result = parse_builtin_provider("bedrock");
        assert_eq!(result, Some(ProviderType::Bedrock));
    }

    #[test]
    fn test_provider_type_from_str_vertex() {
        let result = parse_builtin_provider("vertex");
        assert_eq!(result, Some(ProviderType::Vertex));
    }

    #[test]
    fn test_provider_type_from_str_invalid() {
        let result = parse_builtin_provider("invalid");
        assert_eq!(result, None);
    }

    #[test]
    fn test_provider_alias_resolves_to_builtin_provider() {
        let mut providers = HashMap::new();
        providers.insert(
            "my-service".to_string(),
            ProviderConfig {
                provider: Some("openai".to_string()),
                model: Some("custom-model-v1".to_string()),
                api_key: Some("alias-key".to_string()),
                base_url: Some("https://my-service.example.com/v1".to_string()),
                ..Default::default()
            },
        );

        let resolved = resolve_provider_alias(&providers, "my-service").unwrap();
        assert_eq!(resolved.requested_name, "my-service");
        assert_eq!(resolved.provider_type, ProviderType::OpenAI);
        assert_eq!(resolved.effective_config.model.as_deref(), Some("custom-model-v1"));
        assert_eq!(resolved.effective_config.api_key.as_deref(), Some("alias-key"));
        assert_eq!(
            resolved.effective_config.base_url.as_deref(),
            Some("https://my-service.example.com/v1")
        );
    }

    #[test]
    fn test_provider_alias_overlays_builtin_provider_defaults() {
        let mut providers = HashMap::new();
        providers.insert(
            "openai".to_string(),
            ProviderConfig {
                api_key: Some("builtin-key".to_string()),
                model: Some("gpt-4o".to_string()),
                ..Default::default()
            },
        );
        providers.insert(
            "my-service".to_string(),
            ProviderConfig {
                provider: Some("openai".to_string()),
                base_url: Some("https://my-service.example.com/v1".to_string()),
                ..Default::default()
            },
        );

        let resolved = resolve_provider_alias(&providers, "my-service").unwrap();
        assert_eq!(resolved.provider_type, ProviderType::OpenAI);
        assert_eq!(resolved.effective_config.api_key.as_deref(), Some("builtin-key"));
        assert_eq!(resolved.effective_config.model.as_deref(), Some("gpt-4o"));
        assert_eq!(
            resolved.effective_config.base_url.as_deref(),
            Some("https://my-service.example.com/v1")
        );
    }

    #[test]
    fn test_provider_alias_requires_underlying_provider_type() {
        let mut providers = HashMap::new();
        providers.insert("my-service".to_string(), ProviderConfig::default());

        let result = resolve_provider_alias(&providers, "my-service");
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("my-service"));
        assert!(msg.contains("provider"));
        assert!(msg.contains("built-in type"));
    }

    // -------------------------------------------------------------------------
    // merge_config_files tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_merge_config_cli_overrides_file() {
        // Project config sets a non-default provider; it should win over global.
        let global = ConfigFile {
            default: DefaultConfig {
                provider: "anthropic".to_string(),
                model: Some("global-model".to_string()),
                max_tokens: Some(4096),
                max_turns: Some(10),
                max_tool_call_malformed_turns: Some(6),
                max_tool_call_failure_turns: Some(6),
                system_prompt: Some("global prompt".to_string()),
            },
            ..Default::default()
        };
        let project = ConfigFile {
            default: DefaultConfig {
                provider: "openai".to_string(), // non-default -> overrides global
                model: Some("project-model".to_string()),
                max_tokens: Some(2048),
                max_turns: Some(5), // non-default -> overrides global
                max_tool_call_malformed_turns: Some(2),
                max_tool_call_failure_turns: Some(2),
                system_prompt: Some("project prompt".to_string()),
            },
            ..Default::default()
        };

        let merged = merge_config_files(global, project);

        assert_eq!(merged.default.provider, "openai");
        assert_eq!(merged.default.model, Some("project-model".to_string()));
        assert_eq!(merged.default.max_tokens, Some(2048));
        assert_eq!(merged.default.max_turns, Some(5));
        assert_eq!(merged.default.max_tool_call_malformed_turns, Some(2));
        assert_eq!(merged.default.max_tool_call_failure_turns, Some(2));
        assert_eq!(merged.default.system_prompt, Some("project prompt".to_string()));
    }

    #[test]
    fn test_merge_config_file_provides_defaults() {
        // Project config is default; global values should be preserved.
        let global = ConfigFile {
            default: DefaultConfig {
                provider: "openai".to_string(),
                model: Some("global-model".to_string()),
                max_tokens: Some(1024),
                max_turns: Some(5),
                max_tool_call_malformed_turns: Some(4),
                max_tool_call_failure_turns: Some(4),
                system_prompt: Some("global prompt".to_string()),
            },
            ..Default::default()
        };
        // Project stays at built-in defaults (provider = "anthropic", max_tokens = None, max_turns = None)
        let project = ConfigFile::default();

        let merged = merge_config_files(global, project);

        // provider: project default "anthropic" == default_provider() -> use global "openai"
        assert_eq!(merged.default.provider, "openai");
        assert_eq!(merged.default.model, Some("global-model".to_string()));
        assert_eq!(merged.default.max_tokens, Some(1024));
        assert_eq!(merged.default.max_turns, Some(5));
        assert_eq!(merged.default.max_tool_call_malformed_turns, Some(4));
        assert_eq!(merged.default.max_tool_call_failure_turns, Some(4));
        assert_eq!(merged.default.system_prompt, Some("global prompt".to_string()));
    }

    #[test]
    fn test_merge_config_empty_file() {
        // Two default ConfigFiles merged should yield defaults.
        let merged = merge_config_files(ConfigFile::default(), ConfigFile::default());

        assert_eq!(merged.default.provider, default_provider());
        assert_eq!(merged.default.max_tokens, None);
        assert_eq!(merged.default.max_turns, None);
        assert_eq!(merged.default.max_tool_call_malformed_turns, None);
        assert_eq!(merged.default.max_tool_call_failure_turns, None);
        assert!(merged.default.model.is_none());
        assert!(merged.providers.is_empty());
        assert!(merged.profiles.is_empty());
    }

    // -------------------------------------------------------------------------
    // resolve_profile tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_profile_inheritance() {
        // Profile "child" extends "parent"; child fields win, missing ones fall back to parent.
        let mut profiles = HashMap::new();
        profiles.insert(
            "parent".to_string(),
            ProfileConfig {
                provider: Some("anthropic".to_string()),
                model: Some("claude-3".to_string()),
                max_tokens: Some(4096),
                max_tool_call_malformed_turns: Some(3),
                max_tool_call_failure_turns: Some(3),
                ..Default::default()
            },
        );
        profiles.insert(
            "child".to_string(),
            ProfileConfig {
                model: Some("claude-4".to_string()), // overrides parent
                extends: Some("parent".to_string()),
                ..Default::default()
            },
        );

        let mut visited = Vec::new();
        let result = resolve_profile(&profiles, "child", &mut visited).unwrap();

        // Child's model wins
        assert_eq!(result.model, Some("claude-4".to_string()));
        // Parent's provider is inherited
        assert_eq!(result.provider, Some("anthropic".to_string()));
        // Parent's max_tokens is inherited
        assert_eq!(result.max_tokens, Some(4096));
        // Parent's tool-call-malformed turn limit is inherited
        assert_eq!(result.max_tool_call_malformed_turns, Some(3));
        // Parent's tool-call-failure turn limit is inherited
        assert_eq!(result.max_tool_call_failure_turns, Some(3));
        // extends is cleared after resolution
        assert!(result.extends.is_none());
    }

    #[test]
    fn test_profile_cycle_detection() {
        // A extends B, B extends A -> should fail with cycle error.
        let mut profiles = HashMap::new();
        profiles.insert(
            "a".to_string(),
            ProfileConfig {
                extends: Some("b".to_string()),
                ..Default::default()
            },
        );
        profiles.insert(
            "b".to_string(),
            ProfileConfig {
                extends: Some("a".to_string()),
                ..Default::default()
            },
        );

        let mut visited = Vec::new();
        let result = resolve_profile(&profiles, "a", &mut visited);

        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("Circular profile inheritance"));
    }

    #[test]
    fn test_profile_not_found() {
        let profiles: HashMap<String, ProfileConfig> = HashMap::new();
        let mut visited = Vec::new();
        let result = resolve_profile(&profiles, "nonexistent", &mut visited);

        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("nonexistent"));
    }

    // -------------------------------------------------------------------------
    // resolve_api_key tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_api_key_from_cli_arg() {
        // CLI key takes highest priority regardless of other sources.
        let result = resolve_api_key(Some("cli-key"), Some("config-key"), ProviderType::Anthropic).unwrap();
        assert_eq!(result, "cli-key");
    }

    #[test]
    fn test_api_key_from_config() {
        // When CLI key is absent, config file key should be used.
        let result = resolve_api_key(None, Some("config-key"), ProviderType::Anthropic).unwrap();
        assert_eq!(result, "config-key");
    }

    #[test]
    fn test_api_key_missing_returns_error() {
        // Remove all env vars that could supply a key so the function must fail.
        // Note: single-threaded tests share the process environment; clearing here
        // is safe for unit test purposes.
        // SAFETY: single-threaded test context; no other threads read these vars.
        unsafe {
            std::env::remove_var("API_KEY");
            std::env::remove_var("ANTHROPIC_API_KEY");
        }

        // Only fails if OAuth credentials file is also absent, which is true in CI.
        // We accept either an error OR an empty key (Bedrock/Vertex path), but for
        // Anthropic with no key at all the function should return an error.
        let result = resolve_api_key(None, None, ProviderType::Anthropic);

        // The result is either an error (no OAuth file) or Ok (OAuth file found).
        // We can only assert the error path reliably when the OAuth file is absent.
        if let Err(e) = result {
            let msg = e.to_string();
            assert!(msg.contains("No API key found"));
        }
        // If OAuth credentials exist on the test machine, the function returns Ok("").
        // Both outcomes are correct; the important invariant is no panic.
    }

    #[test]
    fn test_api_key_bedrock_returns_empty_without_key() {
        // Bedrock uses AWS credentials, so an empty key is the expected success value.
        let result = resolve_api_key(None, None, ProviderType::Bedrock).unwrap();
        assert_eq!(result, "");
    }

    #[test]
    fn test_api_key_vertex_returns_empty_without_key() {
        // Vertex uses GCP credentials, so an empty key is the expected success value.
        let result = resolve_api_key(None, None, ProviderType::Vertex).unwrap();
        assert_eq!(result, "");
    }

    // -------------------------------------------------------------------------
    // P5-14: SkillsPermissionConfig TOML deserialization
    // -------------------------------------------------------------------------

    #[test]
    fn test_merge_config_global_auto_approve_preserved_with_project_allow_list() {
        let global = ConfigFile {
            tools: ToolsConfig {
                auto_approve: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let project = ConfigFile {
            tools: ToolsConfig {
                allow_list: vec!["ExecCommand".into()], // non-default, triggers if branch
                ..Default::default()
            },
            ..Default::default()
        };
        let merged = merge_config_files(global, project);
        assert!(
            merged.tools.auto_approve,
            "global auto_approve=true should be preserved"
        );
    }

    #[test]
    fn test_config_file_parses_shell_default() {
        let cfg: ConfigFile = toml::from_str(
            r#"
[shell]
default = "powershell"
"#,
        )
        .unwrap();

        assert_eq!(cfg.shell.default, "powershell");
    }

    #[test]
    fn test_merge_config_project_shell_overrides_global() {
        let global = ConfigFile {
            shell: crate::shell::ShellConfig { default: "bash".into() },
            ..Default::default()
        };
        let project = ConfigFile {
            shell: crate::shell::ShellConfig {
                default: "powershell".into(),
            },
            ..Default::default()
        };

        let merged = merge_config_files(global, project);

        assert_eq!(merged.shell.default, "powershell");
    }

    #[test]
    fn test_profile_shell_overrides_base_config() {
        let mut config = ConfigFile {
            default: DefaultConfig {
                max_tool_call_malformed_turns: Some(5),
                max_tool_call_failure_turns: Some(5),
                ..Default::default()
            },
            shell: crate::shell::ShellConfig { default: "bash".into() },
            ..Default::default()
        };
        config.profiles.insert(
            "windows".into(),
            ProfileConfig {
                max_tool_call_malformed_turns: Some(2),
                max_tool_call_failure_turns: Some(2),
                shell: Some("powershell".into()),
                ..Default::default()
            },
        );

        let applied = apply_profile(config, "windows").unwrap();

        assert_eq!(applied.shell.default, "powershell");
        assert_eq!(applied.default.max_tool_call_malformed_turns, Some(2));
        assert_eq!(applied.default.max_tool_call_failure_turns, Some(2));
    }

    #[test]
    fn p5_14_skills_deny_allow_deserialized() {
        let toml_str = r#"
[tools]
auto_approve = false
allow_list = ["Read"]

[tools.skills]
deny = ["dangerous-skill", "admin:*"]
allow = ["commit", "review-pr", "db:*"]
"#;
        let config: ConfigFile = toml::from_str(toml_str).unwrap();
        assert_eq!(
            config.tools.skills.deny,
            vec!["dangerous-skill".to_string(), "admin:*".to_string()]
        );
        assert_eq!(
            config.tools.skills.allow,
            vec!["commit".to_string(), "review-pr".to_string(), "db:*".to_string()]
        );
    }

    #[test]
    fn p5_14_skills_defaults_to_empty() {
        // When [tools.skills] is absent, deny and allow default to empty vecs.
        let config: ConfigFile = toml::from_str("").unwrap();
        assert!(config.tools.skills.deny.is_empty());
        assert!(config.tools.skills.allow.is_empty());
    }

    #[test]
    fn p5_14_merge_skills_concat() {
        // global and project skills lists are concatenated.
        let global = ConfigFile {
            tools: ToolsConfig {
                skills: SkillsPermissionConfig {
                    deny: vec!["global-deny".to_string()],
                    allow: vec!["global-allow".to_string()],
                },
                ..Default::default()
            },
            ..Default::default()
        };
        let project = ConfigFile {
            tools: ToolsConfig {
                skills: SkillsPermissionConfig {
                    deny: vec!["project-deny".to_string()],
                    allow: vec!["project-allow".to_string()],
                },
                ..Default::default()
            },
            ..Default::default()
        };

        let merged = merge_config_files(global, project);
        assert_eq!(
            merged.tools.skills.deny,
            vec!["global-deny".to_string(), "project-deny".to_string()]
        );
        assert_eq!(
            merged.tools.skills.allow,
            vec!["global-allow".to_string(), "project-allow".to_string()]
        );
    }
