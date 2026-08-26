use std::sync::Arc;

use solaris_providers::{LlmProvider, ProviderError};
use solaris_types::llm::{LlmEvent, LlmRequest};

use super::*;
use crate::commands::{CommandContext, CommandRegistry};
use crate::compact::state::CompactState;
use crate::output::null_sink::NullSink;

struct NullProvider;

#[async_trait::async_trait]
impl LlmProvider for NullProvider {
    async fn stream(&self, _: &LlmRequest) -> Result<tokio::sync::mpsc::Receiver<LlmEvent>, ProviderError> {
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        Ok(rx)
    }
}

fn context<'a>(
    messages: &'a mut Vec<solaris_types::message::Message>,
    state: &'a mut CompactState,
    config: &'a solaris_config::compact::CompactConfig,
    provider: Arc<dyn LlmProvider>,
    output: &'a NullSink,
    registry: &'a CommandRegistry,
) -> CommandContext<'a> {
    CommandContext {
        messages,
        compact_state: state,
        compact_config: config,
        provider,
        model: "model-a",
        output,
        registry,
    }
}

#[tokio::test]
async fn model_command_returns_a_typed_update() {
    let provider: Arc<dyn LlmProvider> = Arc::new(NullProvider);
    let registry = CommandRegistry::new();
    let output = NullSink;
    let mut messages = Vec::new();
    let mut state = CompactState::new();
    let config = solaris_config::compact::CompactConfig::default();
    let mut ctx = context(&mut messages, &mut state, &config, provider, &output, &registry);

    let result = ModelCommand.execute(&mut ctx, "model-b").await.unwrap();

    assert_eq!(result, CommandResult::SetModel("model-b".to_owned()));
}

#[tokio::test]
async fn model_command_rejects_multiple_arguments() {
    let provider: Arc<dyn LlmProvider> = Arc::new(NullProvider);
    let registry = CommandRegistry::new();
    let output = NullSink;
    let mut messages = Vec::new();
    let mut state = CompactState::new();
    let config = solaris_config::compact::CompactConfig::default();
    let mut ctx = context(&mut messages, &mut state, &config, provider, &output, &registry);

    let error = ModelCommand.execute(&mut ctx, "model-b extra").await.unwrap_err();

    assert!(error.to_string().contains("single non-empty identifier"));
}
