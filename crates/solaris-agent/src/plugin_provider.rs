use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use solaris_providers::{LlmProvider, ProviderError};
use solaris_types::llm::{LlmEvent, LlmRequest};
use solaris_types::plugin::{
    PluginContributionKind, PluginProviderCommandRequest, PluginProviderCommandResponse, PluginProviderEvent,
};
use tokio::sync::mpsc;

use crate::plugin_tool::PluginContributionDispatcher;

const MAX_PROVIDER_EVENTS: usize = 4_096;

#[async_trait]
trait PluginProviderInvoker: Send + Sync {
    async fn invoke(&self, provider_name: &str, input: Value) -> Result<Value, String>;
}

#[async_trait]
impl PluginProviderInvoker for PluginContributionDispatcher {
    async fn invoke(&self, provider_name: &str, input: Value) -> Result<Value, String> {
        self.invoke(PluginContributionKind::Provider, provider_name, input)
            .await
    }
}

/// Adapts an activated `provider-command-v1` plugin contribution to the
/// provider-neutral runtime interface.
pub(crate) struct PluginLlmProvider {
    name: String,
    invoker: Arc<dyn PluginProviderInvoker>,
}

impl PluginLlmProvider {
    pub(crate) fn new(name: impl Into<String>, dispatcher: Arc<PluginContributionDispatcher>) -> Result<Self, String> {
        let name = name.into();
        if !dispatcher.supports_provider_command_v1(&name) {
            return Err(format!(
                "plugin provider {name} does not declare {}",
                PluginProviderCommandRequest::PROTOCOL
            ));
        }
        Ok(Self {
            name,
            invoker: dispatcher,
        })
    }

    #[cfg(test)]
    fn with_invoker(name: impl Into<String>, invoker: Arc<dyn PluginProviderInvoker>) -> Self {
        Self {
            name: name.into(),
            invoker,
        }
    }
}

#[async_trait]
impl LlmProvider for PluginLlmProvider {
    async fn stream(&self, request: &LlmRequest) -> Result<mpsc::Receiver<LlmEvent>, ProviderError> {
        let input = serde_json::to_value(PluginProviderCommandRequest::new(request.clone()))
            .map_err(|_| ProviderError::Parse("plugin provider request serialization failed".to_owned()))?;
        let value = self
            .invoker
            .invoke(&self.name, input)
            .await
            .map_err(|_| ProviderError::Connection(format!("plugin provider {} invocation failed", self.name)))?;
        let response: PluginProviderCommandResponse = serde_json::from_value(value)
            .map_err(|_| ProviderError::Parse("plugin provider returned an invalid response envelope".to_owned()))?;
        let events = validate_and_convert(response.events)?;
        let (tx, rx) = mpsc::channel(events.len().max(1));
        for event in events {
            tx.send(event)
                .await
                .map_err(|_| ProviderError::Connection("plugin provider response channel closed".to_owned()))?;
        }
        Ok(rx)
    }
}

fn validate_and_convert(events: Vec<PluginProviderEvent>) -> Result<Vec<LlmEvent>, ProviderError> {
    if events.is_empty() || events.len() > MAX_PROVIDER_EVENTS {
        return Err(ProviderError::Parse(
            "plugin provider returned an invalid event count".to_owned(),
        ));
    }
    let last = events.len() - 1;
    let mut terminal_seen = false;
    let mut converted = Vec::with_capacity(events.len());
    for (index, event) in events.into_iter().enumerate() {
        if terminal_seen {
            return Err(ProviderError::Parse(
                "plugin provider returned events after a terminal event".to_owned(),
            ));
        }
        let event = match event {
            PluginProviderEvent::TextDelta { text } => LlmEvent::TextDelta(text),
            PluginProviderEvent::ToolUse { id, name, input, extra } => {
                if id.is_empty() || name.is_empty() {
                    return Err(ProviderError::Parse(
                        "plugin provider returned an invalid tool call".to_owned(),
                    ));
                }
                LlmEvent::ToolUse { id, name, input, extra }
            }
            PluginProviderEvent::ThinkingDelta { text } => LlmEvent::ThinkingDelta(text),
            PluginProviderEvent::ThinkingSignature { signature } => LlmEvent::ThinkingSignature(signature),
            PluginProviderEvent::ProviderMetadata { namespace, value } => {
                if namespace.is_empty() {
                    return Err(ProviderError::Parse(
                        "plugin provider returned an empty metadata namespace".to_owned(),
                    ));
                }
                LlmEvent::ProviderMetadata { namespace, value }
            }
            PluginProviderEvent::Done { stop_reason, usage } => {
                terminal_seen = true;
                LlmEvent::Done { stop_reason, usage }
            }
            PluginProviderEvent::Error { message: _ } => {
                terminal_seen = true;
                LlmEvent::Error("plugin provider reported an error".to_owned())
            }
        };
        if terminal_seen && index != last {
            return Err(ProviderError::Parse(
                "plugin provider terminal event was not last".to_owned(),
            ));
        }
        converted.push(event);
    }
    if !terminal_seen {
        return Err(ProviderError::Parse(
            "plugin provider response has no terminal event".to_owned(),
        ));
    }
    Ok(converted)
}

#[cfg(test)]
#[path = "plugin_provider_test.rs"]
mod plugin_provider_test;
