// -------------------------------------------------------------------------
// ConfigFile TOML deserialization tests
// -------------------------------------------------------------------------

#[test]
fn test_config_file_deserialize_minimal() {
    // An empty TOML string should deserialize to all defaults without error.
    let config: ConfigFile = toml::from_str("").unwrap();

    assert_eq!(config.default.provider, "anthropic");
    assert_eq!(config.default.max_tokens, None);
    assert_eq!(config.default.max_turns, None);
    assert_eq!(config.default.max_tool_call_malformed_turns, None);
    assert_eq!(config.default.max_tool_call_failure_turns, None);
    assert!(config.default.model.is_none());
    assert!(config.providers.is_empty());
    assert!(config.profiles.is_empty());
}

#[test]
fn resolve_max_turns_defaults_to_unlimited_and_preserves_explicit_limits() {
    assert_eq!(resolve_max_turns(None), None);
    assert_eq!(resolve_max_turns(Some(0)), None);
    assert_eq!(resolve_max_turns(Some(7)), Some(7));
}

#[test]
fn mcp_server_config_deserializes_startup_timeout_ms() {
    let toml_str = r#"
[mcp.servers.slow-tools]
transport = "stdio"
command = "node"
args = ["server.js"]
startup_timeout_ms = 45000
"#;

    let config: ConfigFile = toml::from_str(toml_str).unwrap();
    let server = config.mcp.servers.get("slow-tools").unwrap();

    assert_eq!(server.startup_timeout_ms, Some(45_000));
}

#[test]
fn test_config_file_deserialize_with_providers() {
    let toml_str = r#"
[default]
provider = "openai"
model = "gpt-4o"
max_tokens = 4096

[providers.openai]
api_key = "sk-test-key"
base_url = "https://api.openai.com/v1"

[providers.anthropic]
api_key = "sk-ant-test"
prompt_caching = false
"#;
    let config: ConfigFile = toml::from_str(toml_str).unwrap();

    assert_eq!(config.default.provider, "openai");
    assert_eq!(config.default.model, Some("gpt-4o".to_string()));
    assert_eq!(config.default.max_tokens, Some(4096));

    let openai = config.providers.get("openai").unwrap();
    assert_eq!(openai.api_key.as_deref(), Some("sk-test-key"));
    assert_eq!(openai.base_url.as_deref(), Some("https://api.openai.com/v1"));

    let anthropic = config.providers.get("anthropic").unwrap();
    assert_eq!(anthropic.api_key.as_deref(), Some("sk-ant-test"));
    assert_eq!(anthropic.prompt_caching, Some(false));
}

#[test]
fn provider_contract_exposes_four_prices_and_cache_accounting() {
    let compat: ProviderCompat = toml::from_str(
        r#"
cache_token_accounting = "separate_from_input"
input_cost_per_million = 2.0
cache_read_cost_per_million = 0.2
cache_write_cost_per_million = 2.5
output_cost_per_million = 8.0
"#,
    )
    .unwrap();
    let config = Config {
        provider_label: "anthropic".to_owned(),
        provider: ProviderType::Anthropic,
        api_key: "test".to_owned(),
        base_url: "https://example.invalid".to_owned(),
        model: "test-model".to_owned(),
        max_tokens: None,
        max_turns: None,
        max_tool_call_malformed_turns: None,
        max_tool_call_failure_turns: None,
        system_prompt: None,
        thinking: None,
        prompt_caching: true,
        compat,
        tools: ToolsConfig::default(),
        session: SessionConfig::default(),
        memory: Default::default(),
        compact: CompactConfig::default(),
        plan: PlanConfig::default(),
        shell: ShellConfig::default(),
        file_cache: FileCacheConfig::default(),
        hooks: HooksConfig::default(),
        bedrock: None,
        vertex: None,
        mcp: McpConfig::default(),
        logging: LoggingConfig::default(),
        multi_agent: MultiAgentConfig::default(),
    };

    let signals = config.provider_contract().signals;
    assert_eq!(
        signals.cache_token_accounting,
        Some(CacheTokenAccounting::SeparateFromInput)
    );
    assert_eq!(signals.input_cost_per_million, Some(2.0));
    assert_eq!(signals.cache_read_cost_per_million, Some(0.2));
    assert_eq!(signals.cache_write_cost_per_million, Some(2.5));
    assert_eq!(signals.output_cost_per_million, Some(8.0));
}

#[test]
fn built_in_protocol_defaults_define_cache_accounting() {
    let anthropic = ProviderCompat::anthropic_defaults();
    let openai = ProviderCompat::openai_defaults();

    assert_eq!(
        anthropic.transport.cache_token_accounting,
        Some(CacheTokenAccounting::SeparateFromInput)
    );
    assert_eq!(
        openai.transport.cache_token_accounting,
        Some(CacheTokenAccounting::IncludedInInput)
    );
}

#[test]
fn test_config_file_deserialize_custom_provider_alias() {
    let toml_str = r#"
[default]
provider = "my-service"

[providers.my-service]
provider = "openai"
model = "custom-model-v1"
api_key = "alias-key"
base_url = "https://my-service.example.com/api/openai"
"#;
    let config: ConfigFile = toml::from_str(toml_str).unwrap();

    assert_eq!(config.default.provider, "my-service");
    let alias = config.providers.get("my-service").unwrap();
    assert_eq!(alias.provider.as_deref(), Some("openai"));
    assert_eq!(alias.model.as_deref(), Some("custom-model-v1"));
    assert_eq!(alias.api_key.as_deref(), Some("alias-key"));
    assert_eq!(
        alias.base_url.as_deref(),
        Some("https://my-service.example.com/api/openai")
    );
}

// -------------------------------------------------------------------------
// merge_provider_configs tests
// -------------------------------------------------------------------------

#[test]
fn test_merge_provider_configs_overlay_overrides_base() {
    let base = ProviderConfig {
        api_key: Some("base-key".to_string()),
        base_url: Some("https://base.example.com".to_string()),
        model: Some("base-model".to_string()),
        ..Default::default()
    };
    let overlay = ProviderConfig {
        api_key: Some("overlay-key".to_string()),
        model: Some("overlay-model".to_string()),
        ..Default::default()
    };

    let merged = merge_provider_configs(base, overlay);
    assert_eq!(merged.api_key.as_deref(), Some("overlay-key"));
    assert_eq!(merged.model.as_deref(), Some("overlay-model"));
    // base_url not in overlay -> preserved from base
    assert_eq!(merged.base_url.as_deref(), Some("https://base.example.com"));
}

#[test]
fn test_merge_provider_configs_overlay_none_preserves_base() {
    let base = ProviderConfig {
        api_key: Some("base-key".to_string()),
        base_url: Some("https://base.example.com".to_string()),
        model: Some("base-model".to_string()),
        prompt_caching: Some(true),
        provider: Some("openai".to_string()),
        ..Default::default()
    };
    let overlay = ProviderConfig::default();

    let merged = merge_provider_configs(base, overlay);
    assert_eq!(merged.api_key.as_deref(), Some("base-key"));
    assert_eq!(merged.base_url.as_deref(), Some("https://base.example.com"));
    assert_eq!(merged.model.as_deref(), Some("base-model"));
    assert_eq!(merged.prompt_caching, Some(true));
    assert_eq!(merged.provider.as_deref(), Some("openai"));
}

#[test]
fn test_merge_provider_configs_compat_merges_both() {
    let base = ProviderConfig {
        compat: Some(ProviderCompat {
            messages: MessageCompat {
                merge_assistant_messages: Some(true),
                ..Default::default()
            },
            tools: ToolCompat {
                clean_orphan_tool_calls: Some(true),
                ..Default::default()
            },
            ..Default::default()
        }),
        ..Default::default()
    };
    let overlay = ProviderConfig {
        compat: Some(ProviderCompat {
            messages: MessageCompat {
                merge_assistant_messages: Some(false), // override base
                dedup_tool_results: Some(true),        // new field
                ..Default::default()
            },
            ..Default::default()
        }),
        ..Default::default()
    };

    let merged = merge_provider_configs(base, overlay);
    let compat = merged.compat.unwrap();
    // overlay wins
    assert_eq!(compat.messages.merge_assistant_messages, Some(false));
    // base preserved
    assert_eq!(compat.tools.clean_orphan_tool_calls, Some(true));
    // overlay adds new
    assert_eq!(compat.messages.dedup_tool_results, Some(true));
}

#[test]
fn test_merge_provider_configs_compat_merges_across_domains() {
    let base = ProviderConfig {
        compat: Some(ProviderCompat {
            transport: TransportCompat {
                max_tokens_field: Some("max_tokens".to_string()),
                cache_token_accounting: Some(CacheTokenAccounting::IncludedInInput),
                input_cost_per_million: Some(2.0),
                ..Default::default()
            },
            messages: MessageCompat {
                merge_assistant_messages: Some(true),
                clean_orphan_tool_results: Some(true),
                ..Default::default()
            },
            tools: ToolCompat {
                auto_tool_id: Some(true),
                ..Default::default()
            },
            reasoning: ReasoningCompat {
                supports_effort: Some(true),
                effort_levels: Some(vec!["low".to_string(), "high".to_string()]),
                ..Default::default()
            },
            ..Default::default()
        }),
        ..Default::default()
    };
    let overlay = ProviderConfig {
        compat: Some(ProviderCompat {
            transport: TransportCompat {
                api_path: Some("/chat/completions".to_string()),
                cache_read_cost_per_million: Some(0.2),
                output_cost_per_million: Some(8.0),
                ..Default::default()
            },
            messages: MessageCompat {
                merge_assistant_messages: Some(false),
                ..Default::default()
            },
            schema: SchemaCompat {
                sanitize_schema: Some(true),
            },
            ..Default::default()
        }),
        ..Default::default()
    };

    let merged = merge_provider_configs(base, overlay);
    let compat = merged.compat.unwrap();

    assert_eq!(compat.transport.max_tokens_field.as_deref(), Some("max_tokens"));
    assert_eq!(compat.transport.api_path.as_deref(), Some("/chat/completions"));
    assert_eq!(
        compat.transport.cache_token_accounting,
        Some(CacheTokenAccounting::IncludedInInput)
    );
    assert_eq!(compat.transport.input_cost_per_million, Some(2.0));
    assert_eq!(compat.transport.cache_read_cost_per_million, Some(0.2));
    assert_eq!(compat.transport.output_cost_per_million, Some(8.0));
    assert_eq!(compat.messages.merge_assistant_messages, Some(false));
    assert_eq!(compat.messages.clean_orphan_tool_results, Some(true));
    assert_eq!(compat.tools.auto_tool_id, Some(true));
    assert_eq!(compat.schema.sanitize_schema, Some(true));
    assert_eq!(compat.reasoning.supports_effort, Some(true));
    assert_eq!(
        compat.reasoning.effort_levels,
        Some(vec!["low".to_string(), "high".to_string()])
    );
}

#[test]
fn test_merge_provider_configs_both_empty() {
    let merged = merge_provider_configs(ProviderConfig::default(), ProviderConfig::default());
    assert!(merged.api_key.is_none());
    assert!(merged.base_url.is_none());
    assert!(merged.model.is_none());
    assert!(merged.provider.is_none());
    assert!(merged.prompt_caching.is_none());
    assert!(merged.compat.is_none());
}

// -------------------------------------------------------------------------
// resolve_provider_alias: builtin name path tests
// -------------------------------------------------------------------------

#[test]
fn test_resolve_builtin_provider_with_config() {
    let mut providers = HashMap::new();
    providers.insert(
        "openai".to_string(),
        ProviderConfig {
            api_key: Some("openai-key".to_string()),
            base_url: Some("https://custom-openai.example.com".to_string()),
            ..Default::default()
        },
    );

    let resolved = resolve_provider_alias(&providers, "openai").unwrap();
    assert_eq!(resolved.requested_name, "openai");
    assert_eq!(resolved.provider_type, ProviderType::OpenAI);
    assert_eq!(resolved.effective_config.api_key.as_deref(), Some("openai-key"));
    assert_eq!(
        resolved.effective_config.base_url.as_deref(),
        Some("https://custom-openai.example.com")
    );
}

#[test]
fn test_resolve_builtin_provider_without_config_entry() {
    let providers = HashMap::new();

    let resolved = resolve_provider_alias(&providers, "anthropic").unwrap();
    assert_eq!(resolved.requested_name, "anthropic");
    assert_eq!(resolved.provider_type, ProviderType::Anthropic);
    // No config entry -> all fields default to None
    assert!(resolved.effective_config.api_key.is_none());
    assert!(resolved.effective_config.base_url.is_none());
    assert!(resolved.effective_config.model.is_none());
}

// -------------------------------------------------------------------------
// resolve_provider_alias: error path tests
// -------------------------------------------------------------------------

#[test]
fn test_resolve_alias_maps_to_invalid_builtin_type() {
    let mut providers = HashMap::new();
    providers.insert(
        "my-db".to_string(),
        ProviderConfig {
            provider: Some("mysql".to_string()),
            ..Default::default()
        },
    );

    let result = resolve_provider_alias(&providers, "my-db");
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("my-db"));
    assert!(msg.contains("mysql"));
    assert!(msg.contains("not a built-in provider"));
}

#[test]
fn test_resolve_alias_not_found_in_providers() {
    let providers = HashMap::new();

    let result = resolve_provider_alias(&providers, "nonexistent");
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("nonexistent"));
    assert!(msg.contains("built-in provider"));
    assert!(msg.contains("[providers.nonexistent]"));
}

// -------------------------------------------------------------------------
// provider_label (requested_name) tests
// -------------------------------------------------------------------------

#[test]
fn test_provider_label_is_alias_name_not_underlying_type() {
    let mut providers = HashMap::new();
    providers.insert(
        "my-service".to_string(),
        ProviderConfig {
            provider: Some("openai".to_string()),
            api_key: Some("key".to_string()),
            ..Default::default()
        },
    );

    let resolved = resolve_provider_alias(&providers, "my-service").unwrap();
    // provider_label should be the alias name, not "openai"
    assert_eq!(resolved.requested_name, "my-service");
    assert_eq!(resolved.provider_type, ProviderType::OpenAI);
}

#[test]
fn test_provider_label_is_builtin_name_for_builtin() {
    let providers = HashMap::new();

    for (name, expected_type) in [
        ("anthropic", ProviderType::Anthropic),
        ("openai", ProviderType::OpenAI),
        ("bedrock", ProviderType::Bedrock),
        ("vertex", ProviderType::Vertex),
    ] {
        let resolved = resolve_provider_alias(&providers, name).unwrap();
        assert_eq!(resolved.requested_name, name);
        assert_eq!(resolved.provider_type, expected_type);
    }
}

// -------------------------------------------------------------------------
// model priority: alias model in resolution chain
// -------------------------------------------------------------------------

#[test]
fn test_alias_model_available_in_effective_config() {
    // Verifies that alias.model is carried through effective_config,
    // which feeds into the priority chain: CLI > alias.model > default.model > hardcoded
    let mut providers = HashMap::new();
    providers.insert(
        "my-service".to_string(),
        ProviderConfig {
            provider: Some("openai".to_string()),
            model: Some("alias-model-v1".to_string()),
            ..Default::default()
        },
    );

    let resolved = resolve_provider_alias(&providers, "my-service").unwrap();
    assert_eq!(resolved.effective_config.model.as_deref(), Some("alias-model-v1"));
}

#[test]
fn test_alias_model_inherits_from_underlying_provider() {
    // When alias has no model but underlying provider does,
    // the alias should inherit it via merge_provider_configs
    let mut providers = HashMap::new();
    providers.insert(
        "openai".to_string(),
        ProviderConfig {
            model: Some("gpt-4o".to_string()),
            ..Default::default()
        },
    );
    providers.insert(
        "my-service".to_string(),
        ProviderConfig {
            provider: Some("openai".to_string()),
            base_url: Some("https://my-service.example.com".to_string()),
            // no model -> should inherit from openai
            ..Default::default()
        },
    );

    let resolved = resolve_provider_alias(&providers, "my-service").unwrap();
    assert_eq!(resolved.effective_config.model.as_deref(), Some("gpt-4o"));
}

#[test]
fn test_alias_model_overrides_underlying_provider_model() {
    // When both alias and underlying provider define model,
    // alias model should win
    let mut providers = HashMap::new();
    providers.insert(
        "openai".to_string(),
        ProviderConfig {
            model: Some("gpt-4o".to_string()),
            ..Default::default()
        },
    );
    providers.insert(
        "my-service".to_string(),
        ProviderConfig {
            provider: Some("openai".to_string()),
            model: Some("custom-model-v2".to_string()),
            ..Default::default()
        },
    );

    let resolved = resolve_provider_alias(&providers, "my-service").unwrap();
    assert_eq!(resolved.effective_config.model.as_deref(), Some("custom-model-v2"));
}
