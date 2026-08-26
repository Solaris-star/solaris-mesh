use super::*;

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use async_trait::async_trait;
    use solaris_providers::ProviderError;
    use solaris_providers::provider::LlmProvider;
    use solaris_types::compact::CompactTrigger;
    use solaris_types::identity::RunId;

    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};

    struct BlockingProvider;

    struct CountingProvider(Arc<AtomicUsize>);

    #[async_trait]
    impl LlmProvider for BlockingProvider {
        async fn stream(&self, _: &LlmRequest) -> Result<mpsc::Receiver<LlmEvent>, ProviderError> {
            std::future::pending().await
        }
    }

    #[async_trait]
    impl LlmProvider for CountingProvider {
        async fn stream(&self, _: &LlmRequest) -> Result<mpsc::Receiver<LlmEvent>, ProviderError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(ProviderError::Connection("must not execute".to_owned()))
        }
    }

    fn default_config() -> CompactConfig {
        CompactConfig::default()
    }

    // ── should_autocompact (TC-2.4-01..03, TC-2.4-14) ──────────────────

    #[test]
    fn above_threshold_triggers() {
        // threshold = 200k - 20k - 13k = 167k
        let config = default_config();
        assert!(should_autocompact(170_000, &config));
    }

    #[test]
    fn below_threshold_does_not_trigger() {
        let config = default_config();
        assert!(!should_autocompact(160_000, &config));
    }

    #[test]
    fn at_exact_threshold_triggers() {
        let config = default_config();
        assert!(should_autocompact(167_000, &config));
    }

    #[test]
    fn disabled_config_never_triggers() {
        let config = CompactConfig {
            enabled: false,
            ..default_config()
        };
        assert!(!should_autocompact(999_999, &config));
    }

    #[test]
    fn custom_config_threshold() {
        let config = CompactConfig {
            context_window: 100_000,
            output_reserve: 10_000,
            autocompact_buffer: 5_000,
            ..default_config()
        };
        // threshold = 100k - 10k - 5k = 85k
        assert!(!should_autocompact(80_000, &config));
        assert!(should_autocompact(85_000, &config));
        assert!(should_autocompact(90_000, &config));
    }

    #[test]
    fn zero_tokens_does_not_trigger() {
        let config = default_config();
        assert!(!should_autocompact(0, &config));
    }

    #[test]
    fn threshold_pct_overrides_default_calculation() {
        let config = CompactConfig {
            context_window: 200_000,
            autocompact_threshold_pct: Some(50),
            ..default_config()
        };
        // threshold = 200k * 50 / 100 = 100k
        assert!(!should_autocompact(99_999, &config));
        assert!(should_autocompact(100_000, &config));
        assert!(should_autocompact(150_000, &config));
    }

    #[test]
    fn threshold_pct_zero_triggers_immediately() {
        let config = CompactConfig {
            autocompact_threshold_pct: Some(0),
            ..default_config()
        };
        // threshold = 0, any non-negative triggers
        assert!(should_autocompact(0, &config));
        assert!(should_autocompact(1, &config));
    }

    #[test]
    fn threshold_pct_100_never_triggers() {
        let config = CompactConfig {
            context_window: 200_000,
            autocompact_threshold_pct: Some(100),
            ..default_config()
        };
        // threshold = 200k, provider never reports 200k input_tokens
        assert!(!should_autocompact(199_999, &config));
        assert!(should_autocompact(200_000, &config));
    }

    #[test]
    fn threshold_pct_none_uses_default_logic() {
        let config = CompactConfig {
            autocompact_threshold_pct: None,
            ..default_config()
        };
        // Same as default: threshold = 200k - 20k - 13k = 167k
        assert!(!should_autocompact(166_999, &config));
        assert!(should_autocompact(167_000, &config));
    }

    // ── truncate_for_retry ──────────────────────────────────────────────

    #[test]
    fn truncate_drops_20_percent() {
        let msgs: Vec<Message> = (0..10)
            .map(|i| {
                let role = if i % 2 == 0 { Role::User } else { Role::Assistant };
                Message::new(
                    role,
                    vec![ContentBlock::Text {
                        text: format!("msg-{i}"),
                    }],
                )
            })
            .collect();

        let result = truncate_for_retry(&msgs).unwrap();
        // Drop 20% of 10 = 2 messages, remaining 8
        assert_eq!(result.len(), 8);
    }

    #[test]
    fn truncate_ensures_user_first() {
        let msgs: Vec<Message> = (0..5)
            .map(|i| {
                Message::new(
                    Role::Assistant,
                    vec![ContentBlock::Text {
                        text: format!("msg-{i}"),
                    }],
                )
            })
            .collect();

        let result = truncate_for_retry(&msgs).unwrap();
        assert_eq!(result[0].role, Role::User);
    }

    #[test]
    fn truncate_too_few_returns_none() {
        let msgs = vec![Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "only one".to_string(),
            }],
        )];
        assert!(truncate_for_retry(&msgs).is_none());
    }

    #[test]
    fn truncate_empty_returns_none() {
        assert!(truncate_for_retry(&[]).is_none());
    }

    #[test]
    fn truncate_preserves_user_first_without_placeholder() {
        // First remaining message is already User — no placeholder needed
        let msgs: Vec<Message> = (0..10)
            .map(|i| {
                let role = if i % 2 == 0 { Role::User } else { Role::Assistant };
                Message::new(
                    role,
                    vec![ContentBlock::Text {
                        text: format!("msg-{i}"),
                    }],
                )
            })
            .collect();

        let result = truncate_for_retry(&msgs).unwrap();
        // msgs[2] (User) should be first; no placeholder prepended
        assert_eq!(result.len(), 8);
        match &result[0].content[0] {
            ContentBlock::Text { text } => assert_eq!(text, "msg-2"),
            _ => panic!("expected Text"),
        }
    }

    // ── boundary detection / extraction ─────────────────────────────────

    #[test]
    fn detect_boundary_message() {
        let metadata = CompactMetadata {
            trigger: CompactTrigger::Auto,
            pre_compact_tokens: 150_000,
            messages_summarized: 42,
        };
        let text = format!("{BOUNDARY_PREFIX}\n{}", serde_json::to_string(&metadata).unwrap());
        let msg = Message::new(Role::User, vec![ContentBlock::Text { text }]);
        assert!(is_compact_boundary(&msg));
    }

    #[test]
    fn non_boundary_message() {
        let msg = Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "hello".to_string(),
            }],
        );
        assert!(!is_compact_boundary(&msg));
    }

    #[test]
    fn extract_metadata_from_boundary() {
        let metadata = CompactMetadata {
            trigger: CompactTrigger::Auto,
            pre_compact_tokens: 150_000,
            messages_summarized: 42,
        };
        let text = format!("{BOUNDARY_PREFIX}\n{}", serde_json::to_string(&metadata).unwrap());
        let msg = Message::new(Role::User, vec![ContentBlock::Text { text }]);
        let extracted = extract_compact_metadata(&msg).unwrap();
        assert_eq!(extracted, metadata);
    }

    #[test]
    fn extract_metadata_from_non_boundary_returns_none() {
        let msg = Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "not a boundary".to_string(),
            }],
        );
        assert!(extract_compact_metadata(&msg).is_none());
    }

    #[tokio::test]
    async fn changed_model_cannot_bypass_unknown_autocompact_effect() {
        let provider_effect = crate::bootstrap::provider_effect_descriptor("");
        let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
        permissions.allow_configured_effect_for("config:provider", "AutoCompact", &provider_effect);
        permissions.replace_rules(vec![PermissionRule {
            capability: Some("AutoCompact".into()),
            action: None,
            effect_class: Some(solaris_types::effect::EffectClass::Network),
            resource_prefixes: Vec::new(),
            decision: PermissionDecision::Allow,
        }]);
        let ledger = Arc::new(InMemoryRuntimeLedger::default());
        let run_id = RunId::from("autocompact-unknown-run");
        let context = EffectExecutionContext::new(
            run_id.clone(),
            AgentId::from("root"),
            ledger.clone(),
            permissions,
            OperationEnvironmentSnapshot::default(),
        );
        let messages = vec![Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "summarize me".to_owned(),
            }],
        )];
        let config = CompactConfig::default();
        let first_context = context.clone();
        let first_messages = messages.clone();
        let first_config = config.clone();
        let first_effect = provider_effect.clone();
        let running = tokio::spawn(async move {
            let mut state = CompactState::new();
            autocompact_with_effect(
                &BlockingProvider,
                &first_messages,
                "model",
                &first_config,
                &mut state,
                &first_context,
                &first_effect,
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if ledger
                    .records_for_run(&run_id)
                    .unwrap()
                    .iter()
                    .any(|record| record.record_type == "effect_intent")
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        running.abort();
        let _ = running.await;

        let calls = Arc::new(AtomicUsize::new(0));
        let mut resumed_state = CompactState::new();
        let error = autocompact_with_effect(
            &CountingProvider(calls.clone()),
            &messages,
            "changed-model",
            &config,
            &mut resumed_state,
            &context,
            &provider_effect,
        )
        .await
        .unwrap_err();

        assert!(
            matches!(error, CompactError::ReconciliationRequired(_)),
            "unexpected error: {error:?}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}
