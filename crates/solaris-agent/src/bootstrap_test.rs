use super::*;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use solaris_config::config::{BedrockConfig, CliArgs, McpServerConfig, ProviderType, TransportType, VertexConfig};
    use solaris_providers::error::ProviderError;
    use solaris_providers::provider::LlmProvider;
    use solaris_types::effect::DurabilityClass;
    use solaris_types::effect::EffectRequest;
    use solaris_types::identity::{EffectId, OperationId};
    use solaris_types::llm::{LlmEvent, LlmRequest};
    use solaris_types::message::ContentBlock;
    use solaris_types::message::{Message, Role};
    use solaris_types::permission::PermissionCeiling;
    use solaris_types::plugin::{
        ImplementationIdentity, PluginCapabilities, PluginCommandToolDefinition, PluginCompatibility, PluginDefinition,
        PluginResources, PluginSource, ResolvedPluginIdentity,
    };

    use crate::compact::auto::autocompact_with_effect;
    use crate::compact::state::CompactState;
    use crate::confirm::ToolConfirmer;
    use crate::orchestration::execute_tool_calls_with_policy;
    use crate::output::OutputSink;
    use crate::output::null_sink::NullSink;
    use crate::runtime_ledger::{
        InMemoryRuntimeLedger, JsonlRuntimeLedger, LEDGER_SCHEMA_VERSION, LedgerRecord, RuntimeLedger,
    };

    use super::*;

    fn mcp_identity_key() -> McpIdentityKey {
        McpIdentityKey::new("test-key-v1", b"0123456789abcdef0123456789abcdef".to_vec()).unwrap()
    }

    #[derive(Default)]
    struct RecordingBootstrapOutput {
        errors: Mutex<Vec<String>>,
    }

    impl OutputSink for RecordingBootstrapOutput {
        fn emit_text_delta(&self, _: &str, _: &str) {}
        fn emit_thinking(&self, _: &str, _: &str) {}
        fn emit_tool_call(&self, _: &str, _: &str, _: &str) {}
        fn emit_tool_result(&self, _: &str, _: &str, _: bool, _: &str) {}
        fn emit_stream_start(&self, _: &str) {}
        fn emit_stream_end(&self, _: &str, _: usize, _: u64, _: u64, _: u64, _: u64) {}

        fn emit_error(&self, message: &str) {
            self.errors.lock().unwrap().push(message.to_owned());
        }

        fn emit_info(&self, _: &str) {}
    }

    struct FailingProvider;

    #[async_trait]
    impl LlmProvider for FailingProvider {
        async fn stream(&self, _request: &LlmRequest) -> Result<tokio::sync::mpsc::Receiver<LlmEvent>, ProviderError> {
            Err(ProviderError::Connection("provider unavailable".into()))
        }
    }

    struct OutcomeFailingLedger {
        next_sequence: AtomicU64,
        output_directory: tempfile::TempDir,
    }

    impl Default for OutcomeFailingLedger {
        fn default() -> Self {
            Self {
                next_sequence: AtomicU64::new(0),
                output_directory: tempfile::tempdir().unwrap(),
            }
        }
    }

    impl RuntimeLedger for OutcomeFailingLedger {
        fn logical_append_capability(&self) -> crate::runtime_ledger::LogicalAppendCapability {
            crate::runtime_ledger::LogicalAppendCapability::Unsupported
        }

        fn acquire_workflow_mutation_lease(
            &self,
            _run_id: &RunId,
            _owner_id: &str,
            _now_unix_ms: i64,
        ) -> std::io::Result<crate::runtime_ledger::WorkflowMutationLease> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "workflow mutation leases are unsupported",
            ))
        }

        fn renew_workflow_mutation_lease(
            &self,
            _lease: &crate::runtime_ledger::WorkflowMutationLease,
            _now_unix_ms: i64,
        ) -> std::io::Result<crate::runtime_ledger::WorkflowMutationLease> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "workflow mutation leases are unsupported",
            ))
        }

        fn commit_workflow_restore(
            &self,
            _lease: &crate::runtime_ledger::WorkflowMutationLease,
            _expected_sequence: u64,
            _now_unix_ms: i64,
        ) -> std::io::Result<crate::runtime_ledger::WorkflowRestoreCommit> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "workflow mutation leases are unsupported",
            ))
        }

        fn release_workflow_mutation_lease(
            &self,
            _lease: &crate::runtime_ledger::WorkflowMutationLease,
        ) -> std::io::Result<()> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "workflow mutation leases are unsupported",
            ))
        }

        fn append(
            &self,
            run_id: &RunId,
            durability: DurabilityClass,
            record_type: &str,
            payload: serde_json::Value,
        ) -> std::io::Result<LedgerRecord> {
            if record_type == "effect_outcome" {
                return Err(std::io::Error::other("outcome ledger unavailable"));
            }
            Ok(LedgerRecord {
                schema_version: LEDGER_SCHEMA_VERSION,
                seq: self.next_sequence.fetch_add(1, Ordering::SeqCst) + 1,
                run_id: run_id.clone(),
                timestamp_unix_ms: chrono::Utc::now().timestamp_millis(),
                durability,
                record_type: record_type.into(),
                payload,
            })
        }

        fn append_under_workflow_lease(
            &self,
            _lease: &crate::runtime_ledger::WorkflowMutationLease,
            _now_unix_ms: i64,
            _durability: DurabilityClass,
            _record_type: &str,
            _payload: serde_json::Value,
        ) -> std::io::Result<LedgerRecord> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "workflow mutation leases are unsupported",
            ))
        }

        fn compare_and_append(
            &self,
            _run_id: &RunId,
            _durability: DurabilityClass,
            record_type: &str,
            _identity_fields: &[&str],
            _payload: serde_json::Value,
        ) -> std::io::Result<LedgerRecord> {
            if record_type == "effect_outcome" {
                return Err(std::io::Error::other("outcome ledger unavailable"));
            }
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "logical append is unsupported",
            ))
        }

        fn compare_and_append_under_workflow_lease(
            &self,
            _lease: &crate::runtime_ledger::WorkflowMutationLease,
            _now_unix_ms: i64,
            _durability: DurabilityClass,
            _record_type: &str,
            _identity_fields: &[&str],
            _payload: serde_json::Value,
        ) -> std::io::Result<LedgerRecord> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "workflow mutation leases are unsupported",
            ))
        }

        fn run_ids(&self) -> std::io::Result<Vec<RunId>> {
            Ok(Vec::new())
        }

        fn records_for_run(&self, _run_id: &RunId) -> std::io::Result<Vec<LedgerRecord>> {
            Ok(Vec::new())
        }

        fn effect_output_root(&self) -> Option<std::path::PathBuf> {
            Some(self.output_directory.path().join("effect-outcomes"))
        }
    }

    #[tokio::test]
    async fn persistent_runtime_defaults_to_sqlite_and_imports_legacy_jsonl() {
        let workspace = tempfile::tempdir().unwrap();
        let runtime_directory = workspace.path().join(".solaris").join("runtime");
        let legacy_path = runtime_directory.join("ledger.jsonl");
        let legacy_run = RunId::from("legacy-bootstrap-run");
        {
            let legacy = JsonlRuntimeLedger::open(&legacy_path).unwrap();
            legacy
                .append(
                    &legacy_run,
                    DurabilityClass::SyncCritical,
                    "legacy-record",
                    serde_json::json!({"value": 1}),
                )
                .unwrap();
        }
        let mut config = Config::resolve(&CliArgs {
            provider: Some("anthropic".into()),
            api_key: Some("test".into()),
            base_url: Some("https://provider.example.test".into()),
            model: Some("test-model".into()),
            max_tokens: None,
            thinking: None,
            thinking_budget: None,
            max_turns: None,
            max_tool_call_malformed_turns: None,
            max_tool_call_failure_turns: None,
            system_prompt: None,
            profile: None,
            auto_approve: true,
            project_dir: Some(workspace.path().to_path_buf()),
        })
        .unwrap();
        config.session.directory = workspace
            .path()
            .join(".solaris")
            .join("sessions")
            .to_string_lossy()
            .into_owned();
        let mut bootstrap = AgentBootstrap::new(config, workspace.path().to_string_lossy(), Arc::new(NullSink));

        bootstrap.initialize_persistent_runtime(workspace.path()).unwrap();

        let sqlite_path = runtime_directory.join("ledger.sqlite3");
        assert!(sqlite_path.is_file());
        let protected_paths = bootstrap.permission_context.protected_path_fingerprint_material();
        let canonical_state = workspace.path().join(".solaris").canonicalize().unwrap();
        assert!(
            protected_paths
                .iter()
                .any(|path| path == &format!("root:{}", canonical_state.display()))
        );
        for suffix in ["ledger.sqlite3", "ledger.sqlite3-wal", "ledger.sqlite3-shm"] {
            assert!(protected_paths.iter().any(|path| path.ends_with(suffix)), "{suffix}");
        }
        let records = bootstrap
            .collaboration_runtime
            .ledger()
            .records_for_run(&legacy_run)
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].record_type, "legacy-record");

        std::fs::write(runtime_directory.join("secret.txt"), "runtime-search-secret").unwrap();
        std::fs::write(workspace.path().join("public.txt"), "public").unwrap();
        bootstrap.permission_context.set_mode(PermissionMode::Auto);
        let registry = bootstrap.build_builtin_registry(workspace.path());
        let glob = registry.get("Glob").unwrap();
        let read = registry.get("Read").unwrap();
        let ledger_alias = workspace.path().join("ledger-hard-link.sqlite3");
        std::fs::hard_link(&sqlite_path, &ledger_alias).unwrap();
        let auto = glob
            .execute(serde_json::json!({"pattern": "**/*.txt", "path": "."}))
            .await;
        assert!(auto.content.contains("public.txt"));
        assert!(!auto.content.contains("secret.txt"));
        assert!(
            read.execute(serde_json::json!({"file_path": ledger_alias}))
                .await
                .is_error
        );
        bootstrap.permission_context.set_mode(PermissionMode::Bypass);
        let bypass = glob
            .execute(serde_json::json!({"pattern": "**/*.txt", "path": "."}))
            .await;
        assert!(bypass.content.contains("secret.txt"));
        assert!(
            !read
                .execute(serde_json::json!({"file_path": ledger_alias}))
                .await
                .is_error
        );
    }

    #[test]
    fn persistent_runtime_protects_the_configured_state_root_and_changes_the_permission_fingerprint() {
        let workspace = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let mut config = Config::resolve(&CliArgs {
            provider: Some("anthropic".into()),
            api_key: Some("test".into()),
            base_url: Some("https://provider.example.test".into()),
            model: Some("test-model".into()),
            max_tokens: None,
            thinking: None,
            thinking_budget: None,
            max_turns: None,
            max_tool_call_malformed_turns: None,
            max_tool_call_failure_turns: None,
            system_prompt: None,
            profile: None,
            auto_approve: false,
            project_dir: Some(workspace.path().to_path_buf()),
        })
        .unwrap();
        config.session.directory = state.path().join("sessions").to_string_lossy().into_owned();
        let mut bootstrap = AgentBootstrap::new(config, workspace.path().to_string_lossy(), Arc::new(NullSink));

        bootstrap.initialize_persistent_runtime(workspace.path()).unwrap();

        let state_root = state.path().canonicalize().unwrap();
        let protected_paths = bootstrap.permission_context.protected_path_fingerprint_material();
        assert!(
            protected_paths
                .iter()
                .any(|path| path == &format!("root:{}", state_root.display()))
        );
        assert!(
            protected_paths
                .iter()
                .all(|path| !path.contains(&workspace.path().join(".solaris").to_string_lossy().into_owned()))
        );

        let registry = ToolRegistry::new();
        let protected_environment = build_environment_snapshot_with_plugins(
            &bootstrap.config,
            &registry,
            &bootstrap.permission_context,
            Vec::new(),
        );
        let unprotected = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
        unprotected.set_boundary(bootstrap.permission_context.boundary());
        let unprotected_environment =
            build_environment_snapshot_with_plugins(&bootstrap.config, &registry, &unprotected, Vec::new());
        assert_ne!(
            protected_environment.permission_fingerprint,
            unprotected_environment.permission_fingerprint
        );
    }

    #[tokio::test]
    async fn context_free_auto_tool_execution_requires_approval_outside_boundary() {
        let workspace = tempfile::tempdir().unwrap();
        let mut registry = ToolRegistry::new();
        registry.register(Box::new(ExecCommandTool::new(workspace.path().to_path_buf())));
        let calls = vec![ContentBlock::ToolUse {
            id: "outside-process".into(),
            name: "ExecCommand".into(),
            input: serde_json::json!({"command": "whoami"}),
            extra: None,
        }];
        let mut confirmer = ToolConfirmer::new(false, Vec::new());
        confirmer.set_interactive(false);
        let confirmer = Arc::new(Mutex::new(confirmer));

        let outcome = execute_tool_calls_with_policy(
            &registry,
            &calls,
            &confirmer,
            PermissionMode::Auto,
            PermissionCeiling::unrestricted(),
            None,
            solaris_compact::CompactLevel::default(),
            false,
        )
        .await
        .expect("permission denial is returned as a tool result");

        assert!(matches!(
            outcome.results.as_slice(),
            [ContentBlock::ToolResult {
                is_error: true,
                content,
                ..
            }] if content == "Tool execution denied by user"
        ));
    }

    #[tokio::test]
    async fn provider_failure_reports_terminal_outcome_persistence_failure() {
        let workspace = tempfile::tempdir().unwrap();
        let mut config = Config::resolve(&CliArgs {
            provider: Some("anthropic".into()),
            api_key: Some("test".into()),
            base_url: Some("https://provider.example.test".into()),
            model: Some("test-model".into()),
            max_tokens: None,
            thinking: None,
            thinking_budget: None,
            max_turns: Some(1),
            max_tool_call_malformed_turns: None,
            max_tool_call_failure_turns: None,
            system_prompt: None,
            profile: None,
            auto_approve: true,
            project_dir: None,
        })
        .unwrap();
        config.session.enabled = false;
        let provider_effect = provider_effect_descriptor_for_config(&config, workspace.path());
        let permissions = PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted());
        permissions.set_boundary(ExecutionBoundary::workspace(workspace.path().to_string_lossy()));
        permissions.allow_configured_effect_for("config:provider", "ProviderRequest", &provider_effect);
        let context = EffectExecutionContext::new(
            RunId::from("provider-outcome-failure"),
            AgentId::from("root"),
            Arc::new(OutcomeFailingLedger::default()),
            permissions,
            solaris_types::runtime::OperationEnvironmentSnapshot::default(),
        );
        let mut engine = AgentEngine::new_with_provider(
            Arc::new(FailingProvider),
            config,
            ToolRegistry::new(),
            Arc::new(NullSink),
            workspace.path().to_path_buf(),
        );
        engine.set_execution_context(context);

        let error = engine.run("hello", "msg").await.unwrap_err().to_string();

        assert!(error.contains("provider unavailable"));
        assert!(error.contains("provider outcome persistence failed"));
        assert!(error.contains("outcome ledger unavailable"));
    }

    #[tokio::test]
    async fn autocompact_failure_reports_terminal_outcome_persistence_failure() {
        let workspace = tempfile::tempdir().unwrap();
        let provider_effect = provider_effect_descriptor("https://provider.example.test");
        let permissions = PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted());
        permissions.set_boundary(ExecutionBoundary::workspace(workspace.path().to_string_lossy()));
        permissions.allow_configured_effect_for("config:provider", "AutoCompact", &provider_effect);
        let context = EffectExecutionContext::new(
            RunId::from("compact-outcome-failure"),
            AgentId::from("root"),
            Arc::new(OutcomeFailingLedger::default()),
            permissions,
            solaris_types::runtime::OperationEnvironmentSnapshot::default(),
        );
        let mut state = CompactState::new();

        let error = autocompact_with_effect(
            &FailingProvider,
            &[Message::new(
                Role::User,
                vec![ContentBlock::Text { text: "hello".into() }],
            )],
            "test-model",
            &solaris_config::compact::CompactConfig::default(),
            &mut state,
            &context,
            &provider_effect,
        )
        .await
        .unwrap_err()
        .to_string();

        assert!(error.contains("provider unavailable"));
        assert!(error.contains("outcome persistence failed"));
        assert!(error.contains("outcome ledger unavailable"));
    }

    #[test]
    fn activated_plugin_grants_are_scoped_and_revoke_survives_missing_executable() {
        let root = tempfile::tempdir().unwrap();
        let executable = root.path().join("plugin-runner");
        std::fs::write(&executable, b"plugin runner").unwrap();
        let source = PluginSource::Local {
            path: root.path().to_string_lossy().into_owned(),
        };
        let definition = PluginCommandToolDefinition {
            name: "PluginTool:test".into(),
            description: "test plugin tool".into(),
            input_schema: serde_json::json!({"type": "object"}),
            command: "plugin-runner".into(),
            args: vec!["--configured".into()],
            effect: EffectDescriptor {
                class: EffectClass::Process,
                action: "execute test plugin".into(),
                resources: ResourceFootprint::default(),
                replay_policy: EffectReplayPolicy::Never,
            },
            concurrency_safe: false,
            max_result_size: 1024,
            timeout_ms: 1_000,
        };
        let second_definition = PluginCommandToolDefinition {
            name: "PluginTool:second".into(),
            description: "second plugin tool".into(),
            args: vec!["--second".into()],
            ..definition.clone()
        };
        let mut plugin = ResolvedPluginDefinition {
            definition: PluginDefinition {
                id: "test".into(),
                version: "1.0.0".into(),
                source: source.clone(),
                materialized_path: None,
                capabilities: PluginCapabilities {
                    tools: vec![definition.name.clone(), second_definition.name.clone()],
                    ..Default::default()
                },
                compatibility: PluginCompatibility::default(),
                requested_paths: Vec::new(),
                requires_services: Vec::new(),
                resources: PluginResources::default(),
                command_tools: vec![definition.clone(), second_definition.clone()],
                command_contributions: Vec::new(),
            },
            identity: ResolvedPluginIdentity {
                plugin_id: "test".into(),
                source,
                implementation: ImplementationIdentity {
                    implementation_id: "plugin:test".into(),
                    version: Some("1.0.0".into()),
                    digest: Some("definition-digest".into()),
                },
            },
            authority_root: Some(root.path().to_string_lossy().into_owned()),
        };
        let context = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
        context.set_boundary(ExecutionBoundary::workspace("workspace"));
        authorize_activated_plugin(&context, &plugin).unwrap();
        let plugin_descriptor = PluginCommandTool::from_resolved(&plugin, definition)
            .unwrap()
            .execution_boundary_descriptor();
        let second_descriptor = PluginCommandTool::from_resolved(&plugin, second_definition)
            .unwrap()
            .execution_boundary_descriptor();
        let request = |capability: &str, descriptor: EffectDescriptor| EffectRequest {
            effect_id: EffectId::new(format!("effect:{capability}")),
            operation_id: OperationId::new(format!("operation:{capability}")),
            capability: capability.into(),
            descriptor,
            effective_input: serde_json::Value::Null,
            input_digest: None,
        };

        assert_eq!(
            context
                .evaluate_effect(
                    &RunId::from("run"),
                    "PluginTool:test",
                    &request("PluginTool:test", plugin_descriptor.clone()),
                )
                .decision,
            PermissionDecision::Allow
        );
        assert_eq!(
            context
                .evaluate_effect(
                    &RunId::from("run"),
                    "PluginTool:second",
                    &request("PluginTool:second", second_descriptor.clone()),
                )
                .decision,
            PermissionDecision::Allow
        );
        assert_eq!(
            context
                .evaluate_effect(
                    &RunId::from("run"),
                    "ExecCommand",
                    &request("ExecCommand", plugin_descriptor.clone()),
                )
                .decision,
            PermissionDecision::Ask
        );

        for (capability, class, resources) in [
            (
                "Network",
                EffectClass::Network,
                ResourceFootprint {
                    network_domains: vec!["https://plugin.example.test".into()],
                    ..Default::default()
                },
            ),
            (
                "Read",
                EffectClass::ReadOnly,
                ResourceFootprint {
                    file_reads: vec!["outside/secret.txt".into()],
                    ..Default::default()
                },
            ),
            (
                "Write",
                EffectClass::WorkspaceMutation,
                ResourceFootprint {
                    file_writes: vec!["outside/changed.txt".into()],
                    ..Default::default()
                },
            ),
        ] {
            let descriptor = EffectDescriptor {
                class,
                action: "unrelated effect".into(),
                resources,
                replay_policy: EffectReplayPolicy::Never,
            };
            assert_eq!(
                context
                    .evaluate_effect(&RunId::from("run"), capability, &request(capability, descriptor))
                    .decision,
                PermissionDecision::Ask,
                "activated plugin grant must not authorize {capability}"
            );
        }

        context.set_mode(PermissionMode::Auto);
        assert_eq!(
            context
                .evaluate_effect(
                    &RunId::from("run"),
                    "PluginTool:test",
                    &request("PluginTool:test", plugin_descriptor.clone()),
                )
                .decision,
            PermissionDecision::Allow
        );

        plugin.definition.command_tools.clear();
        std::fs::remove_file(&executable).unwrap();
        revoke_activated_plugin(&context, &plugin).unwrap();

        assert!(context.rules().iter().all(|rule| !matches!(
            rule.capability.as_deref(),
            Some("PluginTool:test" | "PluginTool:second")
        )));
        assert_eq!(
            context
                .evaluate_effect(
                    &RunId::from("run"),
                    "PluginTool:test",
                    &request("PluginTool:test", plugin_descriptor),
                )
                .decision,
            PermissionDecision::Ask
        );
        assert_eq!(
            context
                .evaluate_effect(
                    &RunId::from("run"),
                    "PluginTool:second",
                    &request("PluginTool:second", second_descriptor),
                )
                .decision,
            PermissionDecision::Ask
        );
    }

    #[test]
    fn installing_execution_context_shares_permissions_with_engine_and_external_clones() {
        let workspace = tempfile::tempdir().unwrap();
        let config = Config::resolve(&CliArgs {
            provider: Some("anthropic".into()),
            api_key: Some("test".into()),
            base_url: Some("https://provider.example.test".into()),
            model: Some("test-model".into()),
            max_tokens: None,
            thinking: None,
            thinking_budget: None,
            max_turns: None,
            max_tool_call_malformed_turns: None,
            max_tool_call_failure_turns: None,
            system_prompt: None,
            profile: None,
            auto_approve: true,
            project_dir: None,
        })
        .unwrap();
        let mut engine = AgentEngine::new(
            config,
            ToolRegistry::new(),
            Arc::new(NullSink),
            workspace.path().to_path_buf(),
        );
        let replacement = PermissionContext::new(PermissionMode::Plan, PermissionCeiling::plan());
        let context = EffectExecutionContext::new(
            RunId::from("setter-run"),
            AgentId::from("setter-agent"),
            Arc::new(InMemoryRuntimeLedger::default()),
            replacement,
            solaris_types::runtime::OperationEnvironmentSnapshot::default(),
        );
        let external_clone = context.clone();

        engine.set_execution_context(context);

        assert_eq!(engine.permission_context().mode(), PermissionMode::Plan);
        assert_eq!(
            engine
                .execution_context()
                .expect("public constructor installs an execution context")
                .permissions()
                .mode(),
            PermissionMode::Plan
        );
        assert_eq!(external_clone.permissions().mode(), PermissionMode::Plan);

        let configured = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
        configured.set_boundary(ExecutionBoundary::workspace(
            workspace.path().to_string_lossy().into_owned(),
        ));
        let requests = [
            (
                "ProviderRequest",
                EffectDescriptor {
                    class: EffectClass::Network,
                    action: "request configured provider".into(),
                    resources: ResourceFootprint {
                        network_domains: vec!["https://provider.example.test".into()],
                        ..Default::default()
                    },
                    replay_policy: EffectReplayPolicy::Never,
                },
            ),
            (
                "HookCommand:test",
                EffectDescriptor {
                    class: EffectClass::Process,
                    action: "run configured hook".into(),
                    resources: ResourceFootprint {
                        process_commands: vec!["configured-hook".into()],
                        ..Default::default()
                    },
                    replay_policy: EffectReplayPolicy::Never,
                },
            ),
            (
                "Write",
                EffectDescriptor {
                    class: EffectClass::WorkspaceMutation,
                    action: "write configured file".into(),
                    resources: ResourceFootprint {
                        file_writes: vec![
                            workspace
                                .path()
                                .parent()
                                .expect("temporary directory has parent")
                                .join("configured-outside.txt")
                                .to_string_lossy()
                                .into_owned(),
                        ],
                        ..Default::default()
                    },
                    replay_policy: EffectReplayPolicy::Never,
                },
            ),
        ]
        .map(|(capability, descriptor)| {
            configured.allow_configured_effect_for(
                format!("test:{}", capability.to_ascii_lowercase()),
                capability,
                &descriptor,
            );
            configured.add_rule(PermissionRule {
                capability: Some(capability.into()),
                action: None,
                effect_class: Some(descriptor.class),
                resource_prefixes: Vec::new(),
                decision: PermissionDecision::Allow,
            });
            EffectRequest {
                effect_id: EffectId::new(format!("effect-{capability}")),
                operation_id: OperationId::new(format!("operation-{capability}")),
                capability: capability.into(),
                descriptor,
                input_digest: None,
                effective_input: serde_json::Value::Null,
            }
        });

        engine.set_permission_context(configured);

        assert_eq!(engine.permission_context().mode(), PermissionMode::Auto);
        assert_eq!(
            engine
                .execution_context()
                .expect("execution context")
                .permissions()
                .mode(),
            PermissionMode::Auto
        );
        assert_eq!(external_clone.permissions().mode(), PermissionMode::Auto);
        for request in &requests {
            assert_eq!(
                engine
                    .permission_context()
                    .evaluate_effect(&RunId::from("setter-run"), &request.capability, request)
                    .decision,
                PermissionDecision::Allow
            );
            assert_eq!(
                engine
                    .execution_context()
                    .expect("execution context")
                    .evaluate(request)
                    .decision,
                PermissionDecision::Allow
            );
            assert_eq!(external_clone.evaluate(request).decision, PermissionDecision::Allow);
        }

        let revoked = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
        revoked.set_boundary(ExecutionBoundary::workspace(
            workspace.path().to_string_lossy().into_owned(),
        ));
        engine.set_permission_context(revoked);
        for request in &requests {
            assert_eq!(external_clone.evaluate(request).decision, PermissionDecision::Ask);
        }
    }

    #[test]
    fn vertex_provider_effect_declares_auth_file_and_token_endpoints() {
        let workspace = tempfile::tempdir().unwrap();
        let credential = workspace.path().join("vertex-key.json");
        std::fs::write(&credential, "{}").unwrap();
        let mut config = Config::resolve(&CliArgs {
            provider: Some("vertex".into()),
            api_key: None,
            base_url: None,
            model: Some("claude-sonnet-4@20250514".into()),
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
        })
        .unwrap();
        config.provider = ProviderType::Vertex;
        config.vertex = Some(VertexConfig {
            project_id: Some("project".into()),
            region: Some("europe-west1".into()),
            credentials_file: Some(credential.to_string_lossy().into_owned()),
            service_account_json: None,
        });

        let descriptor = provider_effect_descriptor_for_config(&config, workspace.path());
        assert!(
            descriptor
                .resources
                .file_reads
                .contains(&credential.canonicalize().unwrap().to_string_lossy().into_owned())
        );
        assert!(
            descriptor
                .resources
                .network_domains
                .contains(&"oauth2.googleapis.com".to_owned())
        );
        assert!(
            descriptor
                .resources
                .network_domains
                .contains(&"europe-west1-aiplatform.googleapis.com".to_owned())
        );
    }

    #[test]
    fn provider_credential_paths_are_pinned_to_workspace_before_provider_creation() {
        let workspace = tempfile::tempdir().unwrap();
        let vertex_file = workspace.path().join("vertex.json");
        let bedrock_file = workspace.path().join("credentials");
        std::fs::write(&vertex_file, "{}").unwrap();
        std::fs::write(&bedrock_file, "[default]").unwrap();

        let config_for = |provider: &str, model: &str| {
            Config::resolve(&CliArgs {
                provider: Some(provider.into()),
                api_key: None,
                base_url: None,
                model: Some(model.into()),
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
            })
            .unwrap()
        };

        let mut vertex = config_for("vertex", "claude-sonnet-4@20250514");
        vertex.provider = ProviderType::Vertex;
        vertex.vertex = Some(VertexConfig {
            credentials_file: Some("vertex.json".into()),
            ..Default::default()
        });
        pin_provider_credential_paths(&mut vertex, workspace.path());
        assert_eq!(
            vertex
                .vertex
                .as_ref()
                .and_then(|value| value.credentials_file.as_deref()),
            Some(vertex_file.canonicalize().unwrap().to_string_lossy().as_ref())
        );

        let mut bedrock = config_for("bedrock", "anthropic.claude-sonnet-4-20250514-v1:0");
        bedrock.provider = ProviderType::Bedrock;
        bedrock.bedrock = Some(BedrockConfig {
            profile: Some("default".into()),
            credentials_file: Some("credentials".into()),
            ..Default::default()
        });
        pin_provider_credential_paths(&mut bedrock, workspace.path());
        assert_eq!(
            bedrock
                .bedrock
                .as_ref()
                .and_then(|value| value.credentials_file.as_deref()),
            Some(bedrock_file.canonicalize().unwrap().to_string_lossy().as_ref())
        );
    }

    #[test]
    fn stdio_mcp_connection_requires_reconciliation_but_remote_transport_can_replay() {
        let mut config = McpServerConfig {
            transport: TransportType::Stdio,
            command: Some("server".into()),
            args: None,
            env: None,
            url: None,
            headers: None,
            network: Default::default(),
            deferred: None,
            startup_timeout_ms: None,
        };
        assert_eq!(
            mcp_connection_effect_descriptor("test", &config, &mcp_identity_key()).replay_policy,
            EffectReplayPolicy::ReconcileRequired
        );
        config.transport = TransportType::StreamableHttp;
        config.command = None;
        config.url = Some("https://example.test/mcp".into());
        assert_eq!(
            mcp_connection_effect_descriptor("test", &config, &mcp_identity_key()).replay_policy,
            EffectReplayPolicy::ReplaySafe
        );
    }

    #[test]
    fn runtime_env_only_propagates_resource_limits_and_preserves_server_env() {
        let mut config = Config::resolve(&CliArgs {
            provider: Some("anthropic".to_string()),
            api_key: Some("sk-test".to_string()),
            base_url: None,
            model: Some("claude-sonnet-4-20250514".to_string()),
            max_tokens: Some(4096),
            thinking: None,
            thinking_budget: None,
            max_turns: None,
            max_tool_call_malformed_turns: None,
            max_tool_call_failure_turns: None,
            system_prompt: None,
            profile: None,
            auto_approve: false,
            project_dir: None,
        })
        .unwrap();
        config.mcp.servers.insert(
            "stdio".to_string(),
            McpServerConfig {
                transport: TransportType::Stdio,
                command: Some("server".to_string()),
                args: None,
                env: Some(HashMap::from([
                    ("OVERRIDE".to_string(), "server".to_string()),
                    ("SERVER_ONLY".to_string(), "1".to_string()),
                    ("MCP_API_KEY".to_string(), "server-secret".to_string()),
                ])),
                url: None,
                headers: None,
                network: Default::default(),
                deferred: None,
                startup_timeout_ms: None,
            },
        );

        let output: Arc<dyn OutputSink> = Arc::new(NullSink);
        let bootstrap = AgentBootstrap::new(config, "/tmp", output).runtime_env(vec![
            ("OVERRIDE".to_string(), "runtime".to_string()),
            ("RUNTIME_ONLY".to_string(), "1".to_string()),
            ("SOLARIS_MAX_RUN_TOKENS".to_string(), "32000".to_string()),
            ("SOLARIS_MAX_RUN_COST".to_string(), "secret-in-safe-key".to_string()),
        ]);

        assert_eq!(
            bootstrap.runtime_env,
            vec![("SOLARIS_MAX_RUN_TOKENS".to_string(), "32000".to_string())]
        );

        let servers = bootstrap.mcp_servers_with_runtime_env();
        let env = servers
            .get("stdio")
            .and_then(|server| server.env.as_ref())
            .expect("stdio server env should exist");

        assert_eq!(env.get("OVERRIDE").map(String::as_str), Some("server"));
        assert_eq!(env.get("SERVER_ONLY").map(String::as_str), Some("1"));
        assert_eq!(env.get("MCP_API_KEY").map(String::as_str), Some("server-secret"));
        assert_eq!(env.get("SOLARIS_MAX_RUN_TOKENS").map(String::as_str), Some("32000"));
        assert!(!env.contains_key("RUNTIME_ONLY"));
        assert!(!env.contains_key("SOLARIS_MAX_RUN_COST"));
    }

    include!("bootstrap_mcp_pin_test.rs");
    include!("bootstrap_mcp_runtime_test.rs");
    include!("bootstrap_legacy_task_restore_test.rs");
}
