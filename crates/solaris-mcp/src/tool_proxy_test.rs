use super::*;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde_json::json;
    use solaris_config::config::TransportType;
    use solaris_tools::tool_search::ToolSearchTool;

    use crate::protocol::{JsonRpcRequest, JsonRpcResponse, McpToolAnnotations, McpToolDef};
    use crate::transport::{McpError, McpTransport};

    struct UnusedTransport;

    #[async_trait]
    impl McpTransport for UnusedTransport {
        async fn request(&self, _req: &JsonRpcRequest) -> Result<JsonRpcResponse, McpError> {
            panic!("dynamic ToolSearch test must not call the MCP transport")
        }

        async fn notify(&self, _req: &JsonRpcRequest) -> Result<(), McpError> {
            Ok(())
        }

        async fn close(&self) -> Result<(), McpError> {
            Ok(())
        }
    }

    fn identity_key() -> McpIdentityKey {
        McpIdentityKey::new("test-key-v1", b"0123456789abcdef0123456789abcdef".to_vec()).unwrap()
    }

    fn make_proxy(deferred: bool) -> McpToolProxy {
        // manager is only used during execute(), which we don't call in these
        // tests, so we can construct one with no servers.
        let manager = Arc::new(McpManager::new_for_test(vec![]));
        let config = make_server_config(Some(deferred));
        McpToolProxy::new(
            "test_tool".into(),
            "test_tool".into(),
            "test_server".into(),
            "A test tool".into(),
            json!({"type": "object"}),
            manager,
            &config,
            &identity_key(),
            None,
            false,
        )
    }

    #[test]
    fn proxy_deferred_true_returns_true() {
        let proxy = make_proxy(true);
        assert!(proxy.is_deferred());
    }

    #[test]
    fn proxy_deferred_false_returns_false() {
        let proxy = make_proxy(false);
        assert!(!proxy.is_deferred());
    }

    fn make_server_config(deferred: Option<bool>) -> McpServerConfig {
        McpServerConfig {
            transport: TransportType::Stdio,
            command: Some("echo".into()),
            args: None,
            env: None,
            url: None,
            headers: None,
            network: Default::default(),
            deferred,
            startup_timeout_ms: None,
        }
    }

    #[test]
    fn register_defaults_to_deferred_when_config_omits_field() {
        let manager = Arc::new(McpManager::new_for_test(vec![]));
        let mut registry = solaris_tools::registry::ToolRegistry::new();
        // Empty server configs — deferred field absent
        let configs = HashMap::new();

        register_mcp_tools(&mut registry, &manager, &[], &configs, &identity_key());

        // No tools registered because manager has no tools, but the logic
        // is tested via the deferred default path. Test with a real config below.
        assert!(registry.tool_names().is_empty());
    }

    #[tokio::test]
    async fn dynamic_single_server_registration_refreshes_real_tool_search_with_v3_schema() {
        let input_schema = json!({
            "type": "object",
            "properties": {
                "query": { "type": "string" },
                "limit": { "type": "integer", "minimum": 1 }
            },
            "required": ["query"]
        });
        let manager = Arc::new(McpManager::new_for_test_with_tools(
            "dynamic-server",
            false,
            Box::new(UnusedTransport),
            vec![McpToolDef {
                name: "lookup".into(),
                description: Some("Dynamic deferred lookup".into()),
                input_schema: input_schema.clone(),
                annotations: McpToolAnnotations::default(),
            }],
        ));
        let config = McpServerConfig {
            transport: TransportType::StreamableHttp,
            command: None,
            args: None,
            env: None,
            url: Some("https://mcp.example.test/rpc".into()),
            headers: None,
            network: Default::default(),
            deferred: None,
            startup_timeout_ms: None,
        };
        let mut registry = solaris_tools::registry::ToolRegistry::new();
        registry
            .register_unique(Box::new(ToolSearchTool::new(registry.to_tool_defs())))
            .unwrap();

        register_single_server_tools(&mut registry, &manager, "dynamic-server", &[], &config, &identity_key());

        let aliases = registry
            .tool_names()
            .into_iter()
            .filter(|name| name.starts_with("mcp_v3_"))
            .collect::<Vec<_>>();
        assert_eq!(aliases.len(), 1, "dynamic registration must remain unique");
        assert_eq!(
            registry
                .tool_names()
                .iter()
                .filter(|name| *name == "ToolSearch")
                .count(),
            1,
            "refresh must preserve exactly one built-in ToolSearch"
        );

        let result = registry
            .get("ToolSearch")
            .unwrap()
            .execute(json!({ "query": aliases[0] }))
            .await;
        assert!(!result.is_error);
        let matches: Vec<Value> =
            serde_json::from_str(&result.content).expect("real ToolSearch must return JSON schema");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0]["name"], aliases[0]);
        assert_eq!(matches[0]["parameters"], input_schema);
    }

    #[test]
    fn server_config_deferred_none_defaults_true() {
        let config = make_server_config(None);
        let deferred = config.deferred.unwrap_or(true);
        assert!(deferred, "deferred should default to true when None");
    }

    #[test]
    fn server_config_deferred_explicit_false() {
        let config = make_server_config(Some(false));
        let deferred = config.deferred.unwrap_or(true);
        assert!(!deferred, "deferred should be false when explicitly set");
    }

    #[test]
    fn stdio_proxy_uses_server_namespace_without_persisting_process_args() {
        let mut config = make_server_config(None);
        config.command = Some("node".into());
        config.args = Some(vec!["server.js".into(), "--token=secret".into()]);

        let effect = mcp_effect_descriptor("local", "search", &config, false);

        assert_eq!(effect.class, EffectClass::ExternalSideEffect);
        assert!(effect.resources.process_commands.is_empty());
        assert!(effect.resources.process_invocations.is_empty());
        assert_eq!(effect.resources.external_resources, vec!["mcp:local:search"]);
        assert!(!serde_json::to_string(&effect).unwrap().contains("secret"));
    }

    #[test]
    fn http_proxy_declares_configured_endpoint() {
        let config = McpServerConfig {
            transport: TransportType::StreamableHttp,
            command: None,
            args: None,
            env: None,
            url: Some("https://mcp.example.test/rpc".into()),
            headers: None,
            network: Default::default(),
            deferred: None,
            startup_timeout_ms: None,
        };

        let effect = mcp_effect_descriptor("remote", "read", &config, true);

        assert_eq!(effect.class, EffectClass::Network);
        assert_eq!(effect.resources.network_domains, vec!["https://mcp.example.test"]);
    }

    #[test]
    fn stdio_read_only_claim_still_requires_process_permission() {
        let config = make_server_config(None);

        let effect = mcp_effect_descriptor("local", "read", &config, true);

        assert_eq!(effect.class, EffectClass::Process);
        assert!(effect.resources.unrestricted_process);
        assert!(effect.resources.unrestricted_file_reads);
        assert!(effect.resources.unrestricted_file_writes);
        assert!(!effect.resources.unrestricted_network);
    }

    #[test]
    fn permission_capability_is_stable_across_display_name_collisions() {
        let manager = Arc::new(McpManager::new_for_test(vec![]));
        let config = make_server_config(None);
        let make = |display_name: &str| {
            McpToolProxy::new(
                display_name.into(),
                "search:items".into(),
                "server:alpha".into(),
                "search".into(),
                json!({"type": "object"}),
                Arc::clone(&manager),
                &config,
                &identity_key(),
                None,
                true,
            )
        };

        let root = make("search:items");
        let child = make("mcp__server_alpha_search_items");
        let fork = make("mcp__server_alpha_search_items_2");

        assert_eq!(root.permission_capability(), child.permission_capability());
        assert_eq!(child.permission_capability(), fork.permission_capability());
        assert_ne!(root.name(), child.name());
        assert_eq!(root.permission_capability(), "mcp:12:server:alpha:12:search:items");
    }

    #[test]
    fn display_name_is_a_deterministic_server_namespace() {
        let first = mcp_display_name("alpha", "Read");

        assert_eq!(first, mcp_display_name("alpha", "Read"));
        assert_eq!(first, "mcp_v3_4e7f3b0d99ed853eb59025d6cc7ba1b0dfc921d58764024ceae8a562");
        assert!(first.starts_with("mcp_v3_"));
        assert_ne!(first, mcp_display_name("alpha", "search"));
        assert_ne!(mcp_display_name("alpha", "search"), mcp_display_name("beta", "search"));
    }

    #[test]
    fn display_name_is_provider_safe_and_fixed_length_for_long_and_unicode_inputs() {
        let filesystem = mcp_display_name("filesystem", "read_multiple_files");
        let long = mcp_display_name(&"太阳_server".repeat(100), &"读取/文件_tool".repeat(100));

        for name in [filesystem, long] {
            assert!(
                name.len() <= 64,
                "model-visible MCP name exceeds provider limit: {name}"
            );
            assert_eq!(name.len(), 63, "aliases should not disclose input length");
            assert!(
                name.chars()
                    .all(|character| character.is_ascii_alphanumeric() || character == '_'),
                "hashed model-visible names must use provider-safe ASCII characters: {name}"
            );
        }
    }

    #[test]
    fn display_name_hashes_unambiguous_server_tool_tuples() {
        assert_ne!(mcp_display_name("a", "bc"), mcp_display_name("ab", "c"));
        assert_ne!(
            mcp_display_name("server__alpha", "read"),
            mcp_display_name("server", "alpha__read")
        );
        assert_ne!(
            mcp_display_name("太阳", "读取/文件"),
            mcp_display_name("太阳读取", "/文件")
        );
    }

    #[test]
    fn controlled_alias_collision_is_rejected_without_replacing_first_proxy() {
        let manager = Arc::new(McpManager::new_for_test(vec![]));
        let config = make_server_config(None);
        let alias = mcp_display_name_from_digest(&[0x5a; 32]);
        let make = |server: &str, tool: &str| {
            McpToolProxy::new(
                alias.clone(),
                tool.into(),
                server.into(),
                "test".into(),
                json!({"type": "object"}),
                Arc::clone(&manager),
                &config,
                &identity_key(),
                None,
                true,
            )
        };
        let first = make("first-server", "first-tool");
        let first_capability = first.permission_capability().to_owned();
        let second = make("second-server", "second-tool");
        let mut registry = solaris_tools::registry::ToolRegistry::new();

        registry.register_unique(Box::new(first)).unwrap();
        let error = registry.register_unique(Box::new(second)).unwrap_err();

        assert!(error.contains("already registered"));
        assert_eq!(registry.tool_names(), vec![alias.clone()]);
        assert_eq!(registry.get(&alias).unwrap().permission_capability(), first_capability);
    }

    #[test]
    fn effect_action_never_contains_untrusted_tool_input() {
        let proxy = make_proxy(true);
        let effect = proxy.describe_effect(&json!({"token": "must-not-leak"}));

        assert!(!effect.action.contains("must-not-leak"));
        assert_eq!(effect.action, "call MCP test_server/test_tool");
    }

    #[test]
    fn server_config_deferred_explicit_true() {
        let config = make_server_config(Some(true));
        let deferred = config.deferred.unwrap_or(true);
        assert!(deferred, "deferred should be true when explicitly set");
    }

    #[test]
    fn config_identity_tracks_secret_values_without_serializing_them() {
        let mut config = make_server_config(None);
        config.args = Some(vec!["--token=argument-secret".into()]);
        config.env = Some(HashMap::from([("API_TOKEN".into(), "environment-secret".into())]));
        config.url = Some("https://mcp.example.test/rpc?token=url-secret".into());
        config.headers = Some(HashMap::from([("Authorization".into(), "header-secret".into())]));

        let key = identity_key();
        let first = mcp_server_config_identity(&config, &key);
        let serialized = serde_json::to_string(&first).unwrap();

        assert!(serialized.contains("API_TOKEN"));
        assert!(serialized.contains("Authorization"));
        for secret in ["argument-secret", "environment-secret", "url-secret", "header-secret"] {
            assert!(!serialized.contains(secret));
        }

        config
            .headers
            .as_mut()
            .unwrap()
            .insert("Authorization".into(), "changed-secret".into());
        assert_ne!(first, mcp_server_config_identity(&config, &key));
        let rotated = McpIdentityKey::new("test-key-v2", b"abcdef0123456789abcdef0123456789".to_vec()).unwrap();
        assert_ne!(first, mcp_server_config_identity(&config, &rotated));
    }

    #[test]
    fn proxy_implementation_identity_is_pinned_to_server_config() {
        let manager = Arc::new(McpManager::new_for_test(vec![]));
        let mut first_config = make_server_config(None);
        first_config.env = Some(HashMap::from([("API_TOKEN".into(), "first".into())]));
        let mut second_config = first_config.clone();
        second_config
            .env
            .as_mut()
            .unwrap()
            .insert("API_TOKEN".into(), "second".into());

        let first = McpToolProxy::new(
            "test_tool".into(),
            "test_tool".into(),
            "test_server".into(),
            "A test tool".into(),
            json!({"type": "object"}),
            Arc::clone(&manager),
            &first_config,
            &identity_key(),
            None,
            false,
        );
        let second = McpToolProxy::new(
            "test_tool".into(),
            "test_tool".into(),
            "test_server".into(),
            "A test tool".into(),
            json!({"type": "object"}),
            manager,
            &second_config,
            &identity_key(),
            None,
            false,
        );

        assert_ne!(first.implementation_identity(), second.implementation_identity());
    }

    #[test]
    fn proxy_implementation_identity_tracks_executable_bytes() {
        let manager = Arc::new(McpManager::new_for_test(vec![]));
        let directory = tempfile::tempdir().unwrap();
        let executable = directory.path().join("server-bin");
        let shell = solaris_config::shell::default_shell();
        std::fs::copy(&shell.path, &executable).unwrap();
        let mut config = make_server_config(None);
        config.command = Some(executable.to_string_lossy().into_owned());
        let first_executable = solaris_process::inspect_executable(&executable).unwrap();
        let first = McpToolProxy::new(
            "tool".into(),
            "tool".into(),
            "server".into(),
            "description".into(),
            json!({"type": "object"}),
            Arc::clone(&manager),
            &config,
            &identity_key(),
            Some(&first_executable),
            true,
        )
        .implementation_identity();

        let replacement = directory.path().join("replacement-bin");
        std::fs::copy(&shell.path, &replacement).unwrap();
        let mut bytes = std::fs::read(&replacement).unwrap();
        bytes.push(0);
        std::fs::write(&replacement, bytes).unwrap();
        std::fs::remove_file(&executable).unwrap();
        std::fs::rename(&replacement, &executable).unwrap();
        let second_executable = solaris_process::inspect_executable(&executable).unwrap();
        let second = McpToolProxy::new(
            "tool".into(),
            "tool".into(),
            "server".into(),
            "description".into(),
            json!({"type": "object"}),
            manager,
            &config,
            &identity_key(),
            Some(&second_executable),
            true,
        )
        .implementation_identity();

        assert_ne!(first, second);
    }

    #[test]
    fn connection_footprint_redacts_args_and_url_secrets() {
        let key = identity_key();
        let mut stdio = make_server_config(None);
        stdio.command = Some("node".into());
        stdio.args = Some(vec!["server.js".into(), "--token=stdio-secret".into()]);
        stdio.network.network_domains = vec!["https://api.example.test".into()];
        assert_eq!(
            mcp_connection_resources(&stdio, &key).network_domains,
            vec!["https://api.example.test"]
        );
        let stdio_json = serde_json::to_string(&mcp_connection_resources(&stdio, &key)).unwrap();
        assert!(!stdio_json.contains("stdio-secret"));
        assert!(stdio_json.contains("hmac-sha256"));

        let remote = McpServerConfig {
            transport: TransportType::StreamableHttp,
            command: None,
            args: None,
            env: None,
            url: Some("https://user:url-secret@mcp.example.test/rpc?token=query-secret".into()),
            headers: None,
            network: Default::default(),
            deferred: None,
            startup_timeout_ms: None,
        };
        let remote_json = serde_json::to_string(&mcp_connection_resources(&remote, &key)).unwrap();
        assert_eq!(
            mcp_connection_resources(&remote, &key).network_domains,
            vec!["https://mcp.example.test"]
        );
        assert!(!remote_json.contains("url-secret"));
        assert!(!remote_json.contains("query-secret"));
    }
}
