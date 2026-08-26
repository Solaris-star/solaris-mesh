use super::{
    AgentEngine, AgentError, CacheBreakDetector, CompactLevel, CompactState, ProviderCompat,
    runtime_configuration_state,
};
use crate::tool_call::{merge_tool_results, tool_call_malformed_fingerprint};

// ---------------------------------------------------------------------------
// set_config tests — apply_config_update()
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests_set_config {
    use std::sync::{Arc, Mutex};

    use solaris_config::compat::ReasoningCompat;
    use solaris_config::hooks::{HookDef, HookEngine, HooksConfig};
    use solaris_providers::error::ProviderError;
    use solaris_providers::provider::LlmProvider;
    use solaris_tools::registry::ToolRegistry;
    use solaris_types::config::{ConfigField, ConfigFieldStatus};
    use solaris_types::llm::{LlmEvent, LlmRequest};
    use solaris_types::permission::PermissionMode;
    use solaris_types::run_preset::Intensity;
    use solaris_types::workflow::MultiAgentPolicy;

    use super::{CompactLevel, ProviderCompat};
    use crate::confirm::{ConfirmResult, ToolConfirmer};
    use crate::output::OutputSink;
    use crate::turn::TurnKind;

    struct NullOutput;
    impl OutputSink for NullOutput {
        fn emit_text_delta(&self, _: &str, _: &str) {}
        fn emit_thinking(&self, _: &str, _: &str) {}
        fn emit_tool_call(&self, _: &str, _: &str, _: &str) {}
        fn emit_tool_result(&self, _: &str, _: &str, _: bool, _: &str) {}
        fn emit_stream_start(&self, _: &str) {}
        fn emit_stream_end(&self, _: &str, _: usize, _: u64, _: u64, _: u64, _: u64) {}
        fn emit_error(&self, _: &str) {}
        fn emit_info(&self, _: &str) {}
    }

    #[derive(Default)]
    struct StatusOutput(Mutex<Vec<solaris_types::tool::ToolResultStatus>>);

    impl OutputSink for StatusOutput {
        fn emit_text_delta(&self, _: &str, _: &str) {}
        fn emit_thinking(&self, _: &str, _: &str) {}
        fn emit_tool_call(&self, _: &str, _: &str, _: &str) {}
        fn emit_tool_result(&self, _: &str, _: &str, _: bool, _: &str) {}
        fn emit_tool_result_with_status(
            &self,
            _: &str,
            _: &str,
            status: solaris_types::tool::ToolResultStatus,
            _: &str,
        ) {
            self.0.lock().unwrap().push(status);
        }
        fn emit_stream_start(&self, _: &str) {}
        fn emit_stream_end(&self, _: &str, _: usize, _: u64, _: u64, _: u64, _: u64) {}
        fn emit_error(&self, _: &str) {}
        fn emit_info(&self, _: &str) {}
    }

    struct NullProvider;
    #[async_trait::async_trait]
    impl LlmProvider for NullProvider {
        async fn stream(&self, _: &LlmRequest) -> Result<tokio::sync::mpsc::Receiver<LlmEvent>, ProviderError> {
            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            Ok(rx)
        }
    }

    fn make_engine(model: &str) -> super::AgentEngine {
        super::AgentEngine {
            provider: Arc::new(NullProvider),
            provider_label: "test-provider".to_owned(),
            provider_effect: crate::bootstrap::provider_effect_descriptor("https://provider.test"),
            model: model.to_string(),
            max_tokens: Some(4096),
            thinking: None,
            compat: ProviderCompat::anthropic_defaults(),
            system_prompt: String::new(),
            reasoning_effort: None,
            runtime_configuration: super::runtime_configuration_state(
                "test-provider",
                model,
                solaris_types::permission::PermissionMode::Bypass,
                &None,
                CompactLevel::default(),
            ),
            multi_agent_policy: std::sync::Arc::new(std::sync::RwLock::new(
                solaris_types::workflow::MultiAgentPolicy::default(),
            )),
            resources: crate::resource_manager::ResourceManager::new(Default::default()),
            messages: vec![],
            total_usage: Default::default(),
            msg_id: String::new(),
            max_turns_per_run: Some(10),
            max_tool_call_malformed_turns: 3,
            max_tool_call_failure_turns: 3,
            tools: ToolRegistry::new(),
            confirmer: Arc::new(Mutex::new(ToolConfirmer::new(true, vec![]))),
            permission_context: crate::permission_engine::PermissionContext::from_auto_approve(true),
            execution_context: None,
            allow_list: vec![],
            hooks: None,
            session_manager: None,
            current_session: None,
            output: Arc::new(NullOutput),
            approval_manager: None,
            protocol_writer: None,
            compact_config: solaris_config::compact::CompactConfig::default(),
            compact_state: super::CompactState::new(),
            compact_level: CompactLevel::default(),
            toon_enabled: false,
            plan_state: Default::default(),
            plan_active_flag: None,
            plan_mode_disable_handler: None,
            cache_detector: super::CacheBreakDetector::new(),
            commands: crate::commands::default_registry(),
        }
    }

    #[test]
    fn session_runtime_state_restores_effort_allow_list_plan_and_dynamic_hooks() {
        let dynamic_hooks = HooksConfig {
            post_tool_use: vec![HookDef {
                name: "verify".to_owned(),
                tool_match: vec!["Read".to_owned()],
                file_match: Vec::new(),
                command: "verify-output".to_owned(),
                timeout_ms: 9_000,
                network: Default::default(),
            }],
            ..Default::default()
        };
        let mut original = make_engine("model");
        original.reasoning_effort = Some("high".to_owned());
        original.allow_list = vec!["Read".to_owned()];
        original.plan_state = crate::plan::state::PlanState {
            is_active: true,
            pre_plan_allow_list: vec!["Write".to_owned()],
        };
        original.hooks = Some(HookEngine::new(dynamic_hooks.clone(), std::env::temp_dir()));
        let saved = original.session_runtime_state();

        let mut restored = make_engine("model");
        restored.confirmer = Arc::new(Mutex::new(ToolConfirmer::new(false, Vec::new())));
        restored.hooks = Some(HookEngine::new(HooksConfig::default(), std::env::temp_dir()));
        let plan_active = Arc::new(std::sync::atomic::AtomicBool::new(false));
        restored.set_plan_active_flag(Arc::clone(&plan_active));
        restored.restore_session_runtime_state(Some(&saved));

        assert_eq!(restored.reasoning_effort.as_deref(), Some("high"));
        assert_eq!(restored.allow_list, ["Read"]);
        assert!(restored.plan_state.is_active);
        assert_eq!(restored.plan_state.pre_plan_allow_list, ["Write"]);
        assert!(plan_active.load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(restored.hooks.as_ref().unwrap().config_snapshot(), dynamic_hooks);
        let configuration = restored.runtime_configuration_view().snapshot();
        assert_eq!(configuration.effective_effort.as_deref(), Some("high"));
        assert_eq!(configuration.permission, PermissionMode::Plan);
        let mut confirmer = restored.confirmer.lock().unwrap();
        confirmer.set_interactive(false);
        assert_eq!(confirmer.check("Read", ""), ConfirmResult::ApprovedAlways);
        assert_eq!(confirmer.check("Write", ""), ConfirmResult::Denied);
    }

    #[test]
    fn session_runtime_state_keeps_the_durable_memory_snapshot_reference() {
        let reference = crate::session::SessionMemorySnapshot {
            format_version: 1,
            digest_sha256: "a".repeat(64),
            encoded_bytes: 123,
            captured_at_ms: 456,
        };
        let mut engine = make_engine("model");
        engine.current_session = Some(crate::session::Session {
            id: "memory-session".to_owned(),
            run_id: Some("memory-run".to_owned()),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            provider: "test-provider".to_owned(),
            model: "model".to_owned(),
            cwd: "workspace".to_owned(),
            total_usage: Default::default(),
            messages: Vec::new(),
            runtime_state: Some(crate::session::SessionRuntimeState {
                memory_snapshot: Some(reference.clone()),
                ..Default::default()
            }),
        });

        assert_eq!(engine.session_runtime_state().memory_snapshot, Some(reference));
    }

    #[test]
    fn tool_result_emission_preserves_explicit_status() {
        let output = Arc::new(StatusOutput::default());
        let mut engine = make_engine("test-model");
        engine.output = output.clone();
        let calls = vec![solaris_types::message::ContentBlock::ToolUse {
            id: "cached-call".into(),
            name: "Read".into(),
            input: serde_json::json!({}),
            extra: None,
        }];
        let results = vec![solaris_types::message::ContentBlock::ToolResult {
            tool_use_id: "cached-call".into(),
            content: "cached value".into(),
            is_error: false,
        }];

        engine.emit_tool_results(
            &calls,
            &results,
            &[solaris_types::tool::ToolResultStatus::CacheHit],
            &std::collections::BTreeMap::new(),
        );

        assert_eq!(
            *output.0.lock().unwrap(),
            [solaris_types::tool::ToolResultStatus::CacheHit]
        );
    }

    fn make_engine_with_compat(model: &str, compat: ProviderCompat) -> super::AgentEngine {
        let mut engine = make_engine(model);
        engine.compat = compat;
        engine
    }

    #[derive(Clone, Default)]
    struct InfoLogBuffer(Arc<Mutex<String>>);

    struct InfoSubscriber(InfoLogBuffer);

    struct InfoVisitor<'buffer>(&'buffer InfoLogBuffer);

    impl tracing::field::Visit for InfoVisitor<'_> {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            use std::fmt::Write;

            let _ = write!(self.0.0.lock().unwrap(), "{}={value:?} ", field.name());
        }
    }

    impl tracing::Subscriber for InfoSubscriber {
        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            *metadata.level() <= tracing::Level::INFO
        }

        fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }

        fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

        fn event(&self, event: &tracing::Event<'_>) {
            event.record(&mut InfoVisitor(&self.0));
        }

        fn enter(&self, _span: &tracing::span::Id) {}

        fn exit(&self, _span: &tracing::span::Id) {}
    }

    struct SecretStopHookExecutor(&'static str);

    #[async_trait::async_trait]
    impl solaris_config::hooks::HookExecutor for SecretStopHookExecutor {
        async fn execute(
            &self,
            _invocation: solaris_config::hooks::HookInvocation,
        ) -> Result<solaris_config::hooks::HookExecutionResult, solaris_config::hooks::HookError> {
            Ok(solaris_config::hooks::HookExecutionResult {
                success: true,
                output: self.0.to_owned(),
            })
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stop_hook_logs_only_length_and_digest() {
        use solaris_config::hooks::{HookDef, HookEngine, HooksConfig};

        const SECRET: &str = "stop-hook-secret-must-not-reach-info-log";
        let mut hooks = HookEngine::new(
            HooksConfig {
                stop: vec![HookDef {
                    name: "secret-stop".to_owned(),
                    tool_match: Vec::new(),
                    file_match: Vec::new(),
                    command: "ignored by test executor".to_owned(),
                    timeout_ms: 1_000,
                    network: Default::default(),
                }],
                ..Default::default()
            },
            std::path::PathBuf::from("."),
        );
        hooks.set_executor(Arc::new(SecretStopHookExecutor(SECRET)));
        let mut engine = make_engine("model");
        engine.hooks = Some(hooks);
        let buffer = InfoLogBuffer::default();
        let _guard = tracing::subscriber::set_default(InfoSubscriber(buffer.clone()));

        engine.run_stop_hooks().await;

        let logs = buffer.0.lock().unwrap().clone();
        assert!(logs.contains("stop hook output"), "{logs}");
        assert!(logs.contains("hook_output_status"), "{logs}");
        assert!(logs.contains("hook_output_bytes"), "{logs}");
        assert!(logs.contains("hook_output_digest"), "{logs}");
        assert!(!logs.contains(SECRET), "{logs}");
    }

    #[test]
    fn replacing_permission_context_updates_effect_execution_context() {
        use solaris_types::identity::{AgentId, RunId};
        use solaris_types::permission::{PermissionCeiling, PermissionMode};
        use solaris_types::runtime::OperationEnvironmentSnapshot;

        use crate::execution_context::EffectExecutionContext;
        use crate::permission_engine::PermissionContext;
        use crate::runtime_ledger::InMemoryRuntimeLedger;

        let mut engine = make_engine("model");
        engine.set_execution_context(EffectExecutionContext::new(
            RunId::from("run"),
            AgentId::from("agent"),
            Arc::new(InMemoryRuntimeLedger::default()),
            PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
            OperationEnvironmentSnapshot::default(),
        ));

        engine.set_permission_context(PermissionContext::new(PermissionMode::Plan, PermissionCeiling::plan()));

        assert_eq!(
            engine.execution_context().unwrap().permissions().mode(),
            PermissionMode::Plan
        );
    }

    #[test]
    fn required_workflow_turn_is_available_to_the_next_model_request() {
        use solaris_types::identity::RunId;
        use solaris_types::message::{ContentBlock, Role};

        let mut engine = make_engine("model");
        assert!(
            engine
                .commit_workflow_turn(
                    "plan the change",
                    &RunId::from("root:workflow:turn-1"),
                    "standard-plan",
                    "1",
                    &serde_json::json!({"plan": ["edit", "test"]}),
                )
                .unwrap()
        );
        assert!(
            !engine
                .commit_workflow_turn(
                    "plan the change",
                    &RunId::from("root:workflow:turn-1"),
                    "standard-plan",
                    "1",
                    &serde_json::json!({"plan": ["edit", "test"]}),
                )
                .unwrap()
        );

        assert_eq!(engine.messages.len(), 2);
        assert_eq!(engine.messages[0].role, Role::User);
        assert_eq!(engine.messages[1].role, Role::Assistant);
        assert!(matches!(
            engine.messages[0].content.as_slice(),
            [ContentBlock::Text { text }] if text == "plan the change"
        ));
        assert_eq!(
            engine.messages[1].provider_metadata["solaris.workflow"]["workflow_id"],
            "standard-plan"
        );
    }

    fn status(outcome: &solaris_types::config::ConfigUpdateOutcome, field: ConfigField) -> ConfigFieldStatus {
        outcome
            .results
            .iter()
            .find(|result| result.field == field)
            .map(|result| result.status)
            .unwrap()
    }

    #[test]
    fn set_config_rejects_empty_request() {
        let mut engine = make_engine("current");
        let outcome = engine.apply_config_update(None, None, None, None, None);

        assert!(!outcome.applied);
        assert!(!outcome.changed);
        assert!(outcome.results.is_empty());
        assert_eq!(engine.model, "current");
    }

    #[test]
    fn typed_multi_agent_policy_update_changes_runtime_and_spawn_gate_atomically() {
        let mut engine = make_engine("model");

        let outcome = engine.apply_config_update_with_multi_agent(
            None,
            None,
            None,
            None,
            None,
            Some(solaris_types::workflow::MultiAgentPolicy::Disabled),
        );

        assert!(outcome.applied);
        assert!(outcome.changed);
        assert_eq!(
            engine.runtime_configuration_view().snapshot().multi_agent_policy,
            solaris_types::workflow::MultiAgentPolicy::Disabled
        );
        assert_eq!(
            *engine
                .multi_agent_policy
                .read()
                .unwrap_or_else(|error| error.into_inner()),
            solaris_types::workflow::MultiAgentPolicy::Disabled
        );
    }

    #[test]
    fn typed_max_active_agents_updates_the_shared_admission_limit() {
        let mut engine = make_engine("model");
        let shared_resources = crate::resource_manager::ResourceManager::new(Default::default());
        engine.set_resource_manager(std::sync::Arc::clone(&shared_resources));

        let outcome = engine.apply_runtime_config_update(solaris_types::config::RuntimeConfigUpdate {
            max_active_agents: Some(4),
            ..Default::default()
        });

        assert!(outcome.applied);
        assert!(outcome.changed);
        assert_eq!(
            status(&outcome, ConfigField::MaxActiveAgents),
            ConfigFieldStatus::Applied
        );
        assert_eq!(engine.resources.budget().max_active_agents, Some(4));
        assert_eq!(engine.resources.effective_agent_limit(), 4);
        assert_eq!(shared_resources.budget().max_active_agents, Some(4));
        assert_eq!(shared_resources.effective_agent_limit(), 4);
        let configuration = engine.runtime_configuration_view().snapshot();
        assert_eq!(configuration.max_active_agents, Some(4));
        assert_eq!(configuration.effective_max_active_agents, 4);
    }

    #[test]
    fn invalid_max_active_agents_rejects_the_entire_config_update() {
        let mut engine = make_engine("old-model");
        let before = engine.runtime_configuration_view().snapshot();

        let outcome = engine.apply_runtime_config_update(solaris_types::config::RuntimeConfigUpdate {
            model: Some("new-model".to_owned()),
            max_active_agents: Some(0),
            ..Default::default()
        });

        assert!(!outcome.applied);
        assert!(!outcome.changed);
        assert_eq!(status(&outcome, ConfigField::Model), ConfigFieldStatus::Rejected);
        assert_eq!(
            status(&outcome, ConfigField::MaxActiveAgents),
            ConfigFieldStatus::Rejected
        );
        assert_eq!(engine.model, "old-model");
        assert_eq!(engine.resources.budget().max_active_agents, None);
        assert_eq!(engine.runtime_configuration_view().snapshot(), before);
    }

    #[test]
    fn provider_request_reflects_dynamic_on_demand_and_proactive_spawn_policy() {
        let mut engine = make_engine("model");

        let on_demand = engine.build_request(TurnKind::Normal);
        assert!(on_demand.system.contains("Multi-agent policy: OnDemand"));
        assert!(on_demand.system.contains("user explicitly requests sub-agents"));
        assert!(
            on_demand
                .system
                .contains("independent parallel work is genuinely necessary")
        );
        assert!(!on_demand.system.contains("proactively divide"));

        let outcome = engine.apply_config_update_with_multi_agent(
            None,
            None,
            None,
            None,
            None,
            Some(MultiAgentPolicy::Proactive),
        );
        assert!(outcome.applied);
        let proactive = engine.build_request(TurnKind::Normal);
        assert!(proactive.system.contains("Multi-agent policy: Proactive"));
        assert!(
            proactive
                .system
                .contains("proactively divide suitable independent work")
        );
        assert!(!proactive.system.contains("user explicitly requests sub-agents"));

        let outcome =
            engine.apply_config_update_with_multi_agent(None, None, None, None, None, Some(MultiAgentPolicy::Disabled));
        assert!(outcome.applied);
        let disabled = engine.build_request(TurnKind::Normal);
        assert!(disabled.system.contains("Multi-agent policy: Disabled"));
        assert!(disabled.system.contains("every Spawn request is rejected"));
    }

    #[test]
    fn set_config_rejects_empty_model() {
        let mut engine = make_engine("current");
        let outcome = engine.apply_config_update(Some("  ".into()), None, None, None, None);

        assert!(!outcome.applied);
        assert_eq!(status(&outcome, ConfigField::Model), ConfigFieldStatus::Rejected);
        assert_eq!(engine.model, "current");
    }

    #[test]
    fn set_config_is_atomic_when_effort_is_unsupported() {
        let mut engine = make_engine("old-model");
        let outcome = engine.apply_config_update(
            Some("new-model".into()),
            None,
            None,
            Some("high".into()),
            Some("full".into()),
        );

        assert!(!outcome.applied);
        assert!(!outcome.changed);
        assert_eq!(status(&outcome, ConfigField::Effort), ConfigFieldStatus::Unsupported);
        assert_eq!(status(&outcome, ConfigField::Model), ConfigFieldStatus::Rejected);
        assert_eq!(status(&outcome, ConfigField::Compaction), ConfigFieldStatus::Rejected);
        assert_eq!(engine.model, "old-model");
        assert_eq!(engine.compact_level, CompactLevel::Safe);
        let configuration = engine.runtime_configuration_view().snapshot();
        assert_eq!(configuration.model, "old-model");
        assert!(configuration.effective_effort.is_none());
    }

    #[test]
    fn set_config_rejects_unsupported_thinking() {
        let mut engine = make_engine_with_compat("model", ProviderCompat::openai_defaults());
        let outcome = engine.apply_config_update(None, Some("enabled".into()), None, None, None);

        assert!(!outcome.applied);
        assert_eq!(status(&outcome, ConfigField::Thinking), ConfigFieldStatus::Unsupported);
        assert!(engine.thinking.is_none());
    }

    #[test]
    fn set_config_rejects_invalid_thinking_and_compaction() {
        let mut engine = make_engine("model");
        let outcome = engine.apply_config_update(None, Some("sometimes".into()), None, None, Some("aggressive".into()));

        assert!(!outcome.applied);
        assert_eq!(status(&outcome, ConfigField::Thinking), ConfigFieldStatus::Rejected);
        assert_eq!(status(&outcome, ConfigField::Compaction), ConfigFieldStatus::Rejected);
        assert!(engine.thinking.is_none());
        assert_eq!(engine.compact_level, CompactLevel::Safe);
    }

    #[test]
    fn set_config_updates_budget_when_thinking_is_already_enabled() {
        let mut engine = make_engine("model");
        engine.thinking = Some(solaris_types::llm::ThinkingConfig::Enabled { budget_tokens: 5_000 });
        let outcome = engine.apply_config_update(None, None, Some(20_000), None, None);

        assert!(outcome.applied);
        assert!(outcome.changed);
        assert_eq!(
            status(&outcome, ConfigField::ThinkingBudget),
            ConfigFieldStatus::Applied
        );
        assert!(matches!(
            engine.thinking,
            Some(solaris_types::llm::ThinkingConfig::Enabled { budget_tokens: 20_000 })
        ));
    }

    #[test]
    fn set_config_rejects_budget_when_thinking_is_not_enabled() {
        let mut engine = make_engine("model");
        let outcome = engine.apply_config_update(None, None, Some(20_000), None, None);

        assert!(!outcome.applied);
        assert!(!outcome.changed);
        assert_eq!(
            status(&outcome, ConfigField::ThinkingBudget),
            ConfigFieldStatus::Rejected
        );
        assert!(engine.thinking.is_none());
    }

    #[test]
    fn set_config_rejects_budget_with_disabled_thinking() {
        let mut engine = make_engine("model");
        let outcome = engine.apply_config_update(None, Some("disabled".into()), Some(20_000), None, None);

        assert!(!outcome.applied);
        assert_eq!(
            status(&outcome, ConfigField::ThinkingBudget),
            ConfigFieldStatus::Rejected
        );
        assert_eq!(status(&outcome, ConfigField::Thinking), ConfigFieldStatus::Rejected);
        assert!(engine.thinking.is_none());
    }

    #[test]
    fn set_config_rejects_zero_thinking_budget() {
        let mut engine = make_engine("model");
        let outcome = engine.apply_config_update(None, Some("enabled".into()), Some(0), None, None);

        assert!(!outcome.applied);
        assert_eq!(
            status(&outcome, ConfigField::ThinkingBudget),
            ConfigFieldStatus::Rejected
        );
        assert_eq!(status(&outcome, ConfigField::Thinking), ConfigFieldStatus::Rejected);
        assert!(engine.thinking.is_none());
    }

    #[test]
    fn set_config_rejects_unknown_effort_level() {
        let mut engine = make_engine_with_compat("model", ProviderCompat::openai_defaults());
        let outcome = engine.apply_config_update(None, None, None, Some("ultra".into()), None);

        assert!(!outcome.applied);
        assert_eq!(status(&outcome, ConfigField::Effort), ConfigFieldStatus::Rejected);
        assert!(engine.reasoning_effort.is_none());
    }

    #[test]
    fn set_config_applies_all_valid_fields_and_reports_change() {
        let compat = ProviderCompat {
            reasoning: ReasoningCompat {
                supports_thinking: Some(true),
                supports_effort: Some(true),
                effort_levels: Some(vec!["low".into(), "high".into()]),
            },
            ..Default::default()
        };
        let mut engine = make_engine_with_compat("old-model", compat);
        let outcome = engine.apply_config_update(
            Some("new-model".into()),
            Some("enabled".into()),
            Some(12_000),
            Some("high".into()),
            Some("full".into()),
        );

        assert!(outcome.applied);
        assert!(outcome.changed);
        assert_eq!(outcome.results.len(), 5);
        assert!(
            outcome
                .results
                .iter()
                .all(|result| result.status == ConfigFieldStatus::Applied)
        );
        assert_eq!(engine.model, "new-model");
        assert_eq!(engine.reasoning_effort.as_deref(), Some("high"));
        let configuration = engine.runtime_configuration_view().snapshot();
        assert_eq!(configuration.model, "new-model");
        assert_eq!(configuration.effective_effort.as_deref(), Some("high"));
        assert_eq!(engine.compact_level, CompactLevel::Full);
        assert!(matches!(
            engine.thinking,
            Some(solaris_types::llm::ThinkingConfig::Enabled { budget_tokens: 12_000 })
        ));
    }

    #[test]
    fn set_config_valid_noop_is_applied_without_change() {
        let mut engine = make_engine("same-model");
        let outcome = engine.apply_config_update(Some("same-model".into()), None, None, None, None);

        assert!(outcome.applied);
        assert!(!outcome.changed);
        assert_eq!(status(&outcome, ConfigField::Model), ConfigFieldStatus::Applied);
    }

    #[test]
    fn set_config_can_clear_effort_even_if_provider_does_not_support_setting_it() {
        let mut engine = make_engine("model");
        engine.reasoning_effort = Some("high".into());
        let outcome = engine.apply_config_update(None, None, None, Some(String::new()), None);

        assert!(outcome.applied);
        assert!(outcome.changed);
        assert!(engine.reasoning_effort.is_none());
        assert!(
            engine
                .runtime_configuration_view()
                .snapshot()
                .effective_effort
                .is_none()
        );
    }

    #[test]
    fn selected_extra_reports_high_as_the_effective_provider_effort() {
        let compat = ProviderCompat {
            reasoning: ReasoningCompat {
                supports_effort: Some(true),
                effort_levels: Some(vec!["low".into(), "medium".into(), "high".into()]),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut engine = make_engine_with_compat("model", compat);

        engine.apply_intensity(Intensity::Extra);

        let configuration = engine.runtime_configuration_view().snapshot();
        assert_eq!(configuration.selected_intensity, Intensity::Extra);
        assert_eq!(configuration.effective_effort.as_deref(), Some("high"));
    }

    #[test]
    fn selected_ultracode_reports_the_highest_advertised_effort() {
        let compat = ProviderCompat {
            reasoning: ReasoningCompat {
                supports_effort: Some(true),
                effort_levels: Some(vec!["low".into(), "custom-strong".into()]),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut engine = make_engine_with_compat("model", compat);

        engine.apply_intensity(Intensity::Ultracode);

        let configuration = engine.runtime_configuration_view().snapshot();
        assert_eq!(configuration.selected_intensity, Intensity::Ultracode);
        assert_eq!(configuration.effective_effort.as_deref(), Some("custom-strong"));
    }

    #[test]
    fn provider_without_effort_keeps_selected_intensity_and_reports_no_effective_effort() {
        let mut engine = make_engine_with_compat("model", ProviderCompat::default());

        engine.apply_intensity(Intensity::Extra);

        let configuration = engine.runtime_configuration_view().snapshot();
        assert_eq!(configuration.selected_intensity, Intensity::Extra);
        assert!(configuration.effective_effort.is_none());
    }

    #[test]
    fn permission_changes_are_visible_in_the_shared_configuration() {
        let mut engine = make_engine("model");

        engine.set_permission_mode(PermissionMode::Plan);

        assert_eq!(
            engine.runtime_configuration_view().snapshot().permission,
            PermissionMode::Plan
        );
    }

    #[test]
    fn shared_permission_handle_changes_are_visible_without_manual_refresh() {
        let engine = make_engine("model");
        let view = engine.runtime_configuration_view();

        engine.permission_context().set_mode(PermissionMode::Auto);

        assert_eq!(view.snapshot().permission, PermissionMode::Auto);
    }

    #[test]
    fn execution_context_permission_changes_are_visible_without_manual_refresh() {
        use solaris_types::identity::{AgentId, RunId};
        use solaris_types::permission::PermissionCeiling;
        use solaris_types::runtime::OperationEnvironmentSnapshot;

        use crate::execution_context::EffectExecutionContext;
        use crate::permission_engine::PermissionContext;
        use crate::runtime_ledger::InMemoryRuntimeLedger;

        let mut engine = make_engine("model");
        engine.set_execution_context(EffectExecutionContext::new(
            RunId::from("run"),
            AgentId::from("agent"),
            Arc::new(InMemoryRuntimeLedger::default()),
            PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
            OperationEnvironmentSnapshot::default(),
        ));
        let view = engine.runtime_configuration_view();

        engine
            .execution_context()
            .unwrap()
            .permissions()
            .set_mode(PermissionMode::Auto);

        assert_eq!(view.snapshot().permission, PermissionMode::Auto);
    }

    #[test]
    fn shared_plan_flag_has_priority_over_mutable_permission_mode() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let mut engine = make_engine("model");
        let plan_active = Arc::new(AtomicBool::new(false));
        let view = engine.runtime_configuration_view();
        engine.set_plan_active_flag(Arc::clone(&plan_active));

        engine.permission_context().set_mode(PermissionMode::Bypass);
        plan_active.store(true, Ordering::Release);
        assert_eq!(view.snapshot().permission, PermissionMode::Plan);

        plan_active.store(false, Ordering::Release);
        assert_eq!(view.snapshot().permission, PermissionMode::Bypass);
    }

    #[test]
    fn set_config_snapshot_serializes_actual_thinking_budget_and_compaction() {
        let mut engine = make_engine("model");
        let outcome = engine.apply_config_update(None, Some("enabled".into()), Some(12_000), None, Some("full".into()));
        assert!(outcome.applied);

        let reconnect_view = engine.runtime_configuration_view();
        let snapshot = serde_json::to_value(reconnect_view.snapshot()).unwrap();
        assert_eq!(snapshot["thinking"]["type"], "enabled");
        assert_eq!(snapshot["thinking"]["budget_tokens"], 12_000);
        assert_eq!(snapshot["thinking_budget"], 12_000);
        assert_eq!(snapshot["compaction"], "full");
    }
}

#[cfg(test)]
#[path = "engine_provider_effect_test.rs"]
mod provider_effect_boundary_tests;

// ---------------------------------------------------------------------------
// Phase 6 tests — apply_context_modifiers()
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests_phase6 {
    use std::sync::{Arc, Mutex};

    use solaris_providers::error::ProviderError;
    use solaris_providers::provider::LlmProvider;
    use solaris_tools::registry::ToolRegistry;
    use solaris_types::llm::{LlmEvent, LlmRequest};
    use solaris_types::skill_types::{ContextModifier, EffortLevel};

    use super::{CompactLevel, ProviderCompat};
    use crate::confirm::ToolConfirmer;
    use crate::output::OutputSink;

    struct NullOutput;
    impl OutputSink for NullOutput {
        fn emit_text_delta(&self, _: &str, _: &str) {}
        fn emit_thinking(&self, _: &str, _: &str) {}
        fn emit_tool_call(&self, _: &str, _: &str, _: &str) {}
        fn emit_tool_result(&self, _: &str, _: &str, _: bool, _: &str) {}
        fn emit_stream_start(&self, _: &str) {}
        fn emit_stream_end(&self, _: &str, _: usize, _: u64, _: u64, _: u64, _: u64) {}
        fn emit_error(&self, _: &str) {}
        fn emit_info(&self, _: &str) {}
    }

    struct NullProvider;
    #[async_trait::async_trait]
    impl LlmProvider for NullProvider {
        async fn stream(&self, _: &LlmRequest) -> Result<tokio::sync::mpsc::Receiver<LlmEvent>, ProviderError> {
            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            Ok(rx)
        }
    }

    fn make_engine(model: &str, allow_list: Vec<String>) -> super::AgentEngine {
        super::AgentEngine {
            provider: Arc::new(NullProvider),
            provider_label: "test-provider".to_owned(),
            provider_effect: crate::bootstrap::provider_effect_descriptor("https://provider.test"),
            model: model.to_string(),
            max_tokens: Some(4096),
            thinking: None,
            compat: ProviderCompat::anthropic_defaults(),
            system_prompt: String::new(),
            reasoning_effort: None,
            runtime_configuration: super::runtime_configuration_state(
                "test-provider",
                model,
                solaris_types::permission::PermissionMode::Bypass,
                &None,
                CompactLevel::default(),
            ),
            multi_agent_policy: std::sync::Arc::new(std::sync::RwLock::new(
                solaris_types::workflow::MultiAgentPolicy::default(),
            )),
            resources: crate::resource_manager::ResourceManager::new(Default::default()),
            messages: vec![],
            total_usage: Default::default(),
            msg_id: String::new(),
            max_turns_per_run: Some(10),
            max_tool_call_malformed_turns: 3,
            max_tool_call_failure_turns: 3,
            tools: ToolRegistry::new(),
            confirmer: Arc::new(Mutex::new(ToolConfirmer::new(true, allow_list.clone()))),
            permission_context: crate::permission_engine::PermissionContext::from_auto_approve(true),
            execution_context: None,
            allow_list,
            hooks: None,
            session_manager: None,
            current_session: None,
            output: Arc::new(NullOutput),
            approval_manager: None,
            protocol_writer: None,
            compact_config: solaris_config::compact::CompactConfig::default(),
            compact_state: super::CompactState::new(),
            compact_level: CompactLevel::default(),
            toon_enabled: false,
            plan_state: Default::default(),
            plan_active_flag: None,
            plan_mode_disable_handler: None,
            cache_detector: super::CacheBreakDetector::new(),
            commands: crate::commands::default_registry(),
        }
    }

    #[test]
    fn tc_6_21_model_override_applied() {
        let mut engine = make_engine("original-model", vec![]);
        engine.thinking = Some(solaris_types::llm::ThinkingConfig::Enabled { budget_tokens: 8_000 });
        engine.compact_level = CompactLevel::Full;
        engine.refresh_runtime_configuration();
        let modifiers = vec![Some(ContextModifier {
            model: Some("override-model".to_string()),
            ..Default::default()
        })];
        engine.apply_context_modifiers(&modifiers);
        assert_eq!(engine.model, "override-model");
        let snapshot = engine.runtime_configuration_view().snapshot();
        assert_eq!(snapshot.model, "override-model");
        assert_eq!(snapshot.thinking_budget, Some(8_000));
        assert_eq!(snapshot.compaction, CompactLevel::Full);
    }

    #[test]
    fn tc_6_22_effort_override_applied() {
        let mut engine = make_engine("m", vec![]);
        let modifiers = vec![Some(ContextModifier {
            effort: Some(EffortLevel::High),
            ..Default::default()
        })];
        engine.apply_context_modifiers(&modifiers);
        assert_eq!(engine.reasoning_effort.as_deref(), Some("high"));
        assert_eq!(
            engine
                .runtime_configuration_view()
                .snapshot()
                .effective_effort
                .as_deref(),
            Some("high")
        );
    }

    #[test]
    fn tc_6_22b_effort_all_variants() {
        for (level, expected) in [
            (EffortLevel::Low, "low"),
            (EffortLevel::Medium, "medium"),
            (EffortLevel::High, "high"),
            (EffortLevel::Max, "max"),
        ] {
            let mut engine = make_engine("m", vec![]);
            engine.apply_context_modifiers(&[Some(ContextModifier {
                effort: Some(level),
                ..Default::default()
            })]);
            assert_eq!(
                engine.reasoning_effort.as_deref(),
                Some(expected),
                "EffortLevel::{level:?} should map to {expected:?}"
            );
        }
    }

    #[test]
    fn tc_6_23_allowed_tools_no_duplicates() {
        let mut engine = make_engine("m", vec!["ExecCommand".to_string()]);
        let modifiers = vec![Some(ContextModifier {
            allowed_tools: vec!["ExecCommand".to_string(), "Read".to_string()],
            ..Default::default()
        })];
        engine.apply_context_modifiers(&modifiers);
        let bash_count = engine.allow_list.iter().filter(|t| t.as_str() == "ExecCommand").count();
        assert_eq!(bash_count, 1, "ExecCommand should appear exactly once");
        assert!(engine.allow_list.contains(&"Read".to_string()));
    }

    #[test]
    fn tc_6_24_none_modifiers_skipped() {
        let mut engine = make_engine("original", vec![]);
        engine.apply_context_modifiers(&[None, None]);
        assert_eq!(engine.model, "original");
        assert!(engine.reasoning_effort.is_none());
    }

    #[test]
    fn tc_6_25_empty_modifiers_no_change() {
        let mut engine = make_engine("current-model", vec![]);
        engine.apply_context_modifiers(&[]);
        assert_eq!(engine.model, "current-model");
        assert!(engine.allow_list.is_empty());
    }

    #[test]
    fn tc_6_26_none_model_does_not_overwrite() {
        let mut engine = make_engine("current-model", vec![]);
        engine.apply_context_modifiers(&[Some(ContextModifier {
            allowed_tools: vec!["ExecCommand".to_string()],
            ..Default::default()
        })]);
        assert_eq!(engine.model, "current-model");
        assert!(engine.allow_list.contains(&"ExecCommand".to_string()));
    }

    #[test]
    fn tc_6_27_multiple_modifiers_stacked() {
        let mut engine = make_engine("initial", vec![]);
        let modifiers = vec![
            Some(ContextModifier {
                model: Some("model-a".to_string()),
                allowed_tools: vec!["ExecCommand".to_string()],
                ..Default::default()
            }),
            Some(ContextModifier {
                model: Some("model-b".to_string()),
                allowed_tools: vec!["Read".to_string()],
                ..Default::default()
            }),
        ];
        engine.apply_context_modifiers(&modifiers);
        assert_eq!(engine.model, "model-b", "last model wins");
        assert!(engine.allow_list.contains(&"ExecCommand".to_string()));
        assert!(engine.allow_list.contains(&"Read".to_string()));
    }

    #[test]
    fn tc_6_28_modifier_applied_after_tool_execution_not_during() {
        let mut engine = make_engine("original", vec![]);
        let model_before = engine.model.clone();
        let modifiers = vec![Some(ContextModifier {
            model: Some("new-model".to_string()),
            ..Default::default()
        })];
        assert_eq!(engine.model, model_before);
        engine.apply_context_modifiers(&modifiers);
        assert_eq!(engine.model, "new-model");
        assert_eq!(model_before, "original");
    }
}

// ---------------------------------------------------------------------------
// Phase 2 tests — run_compaction()
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "engine_compact_unit_test.rs"]
mod tests_compact;

#[cfg(test)]
#[path = "engine_plan_mode_test.rs"]
mod tests_plan_mode;

#[cfg(test)]
#[path = "engine_command_test.rs"]
mod tests_handle_command;

#[cfg(test)]
#[path = "engine_loop_helper_test.rs"]
mod tests_loop_helpers;
