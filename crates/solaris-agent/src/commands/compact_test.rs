use super::*;

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use solaris_providers::{LlmProvider, ProviderError};
    use solaris_types::llm::{LlmEvent, LlmRequest};
    use solaris_types::message::{ContentBlock, Message, Role};

    use super::*;
    use crate::commands::{CommandContext, CommandRegistry};
    use crate::compact::state::CompactState;
    use crate::output::OutputSink;
    use crate::output::null_sink::NullSink;

    struct NullProvider;
    #[async_trait::async_trait]
    impl LlmProvider for NullProvider {
        async fn stream(&self, _: &LlmRequest) -> Result<tokio::sync::mpsc::Receiver<LlmEvent>, ProviderError> {
            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            Ok(rx)
        }
    }

    struct SensitiveFailureProvider;
    #[async_trait::async_trait]
    impl LlmProvider for SensitiveFailureProvider {
        async fn stream(&self, _: &LlmRequest) -> Result<tokio::sync::mpsc::Receiver<LlmEvent>, ProviderError> {
            Err(ProviderError::Api {
                status: 500,
                message: "sensitive-provider-response".to_owned(),
            })
        }
    }

    #[test]
    fn reconciliation_failure_has_a_distinct_diagnostic_kind() {
        assert_eq!(
            compact_error_kind(&auto::CompactError::ReconciliationRequired("unknown".to_owned())),
            "reconciliation_required"
        );
        assert_eq!(
            compact_error_kind(&auto::CompactError::Effect("denied".to_owned())),
            "effect"
        );
    }

    #[derive(Default)]
    struct RecordingOutput {
        errors: Mutex<Vec<String>>,
    }

    impl OutputSink for RecordingOutput {
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

    #[tokio::test]
    async fn compact_already_compact_guard() {
        let provider: Arc<dyn LlmProvider> = Arc::new(NullProvider);
        let registry = CommandRegistry::new();
        let output = NullSink;
        let mut messages = vec![Message::new(Role::User, vec![ContentBlock::Text { text: "hi".into() }])];
        let mut state = CompactState::new();
        let config = solaris_config::compact::CompactConfig::default();

        let mut ctx = CommandContext {
            messages: &mut messages,
            compact_state: &mut state,
            compact_config: &config,
            provider,
            model: "test-model",
            output: &output,
            registry: &registry,
        };

        let cmd = CompactCommand;
        let result = cmd.execute(&mut ctx, "").await.unwrap();
        assert_eq!(result, CommandResult::Continue);
        assert_eq!(ctx.messages.len(), 1);
    }

    #[tokio::test]
    async fn compact_resets_circuit_breaker() {
        let provider: Arc<dyn LlmProvider> = Arc::new(NullProvider);
        let registry = CommandRegistry::new();
        let output = NullSink;
        let mut messages: Vec<Message> = (0..10)
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
        let mut state = CompactState::new();
        state.consecutive_failures = 5;
        let config = solaris_config::compact::CompactConfig::default();

        let mut ctx = CommandContext {
            messages: &mut messages,
            compact_state: &mut state,
            compact_config: &config,
            provider,
            model: "test-model",
            output: &output,
            registry: &registry,
        };

        let cmd = CompactCommand;
        let _ = cmd.execute(&mut ctx, "").await;
        // Circuit breaker was reset to 0 before the call, then failure increments it
        assert!(ctx.compact_state.consecutive_failures <= 1);
    }

    #[tokio::test]
    async fn compact_failure_is_error_without_provider_text_or_direct_event() {
        let provider: Arc<dyn LlmProvider> = Arc::new(SensitiveFailureProvider);
        let registry = CommandRegistry::new();
        let output = RecordingOutput::default();
        let mut messages: Vec<Message> = (0..4)
            .map(|index| {
                Message::new(
                    if index % 2 == 0 { Role::User } else { Role::Assistant },
                    vec![ContentBlock::Text {
                        text: format!("message-{index}"),
                    }],
                )
            })
            .collect();
        let mut state = CompactState::new();
        let config = solaris_config::compact::CompactConfig::default();
        let mut ctx = CommandContext {
            messages: &mut messages,
            compact_state: &mut state,
            compact_config: &config,
            provider,
            model: "test-model",
            output: &output,
            registry: &registry,
        };

        let error = CompactCommand
            .execute(&mut ctx, "")
            .await
            .expect_err("failed manual compaction must fail the command");

        assert_eq!(error.to_string(), "Context compaction failed");
        assert!(!error.to_string().contains("sensitive-provider-response"));
        assert!(output.errors.lock().unwrap().is_empty());
    }
}
