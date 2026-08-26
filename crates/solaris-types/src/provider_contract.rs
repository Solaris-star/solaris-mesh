use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::message::TokenUsage;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProtocolId(pub String);

impl ProtocolId {
    pub fn openai_responses() -> Self {
        Self("openai-responses".to_owned())
    }

    pub fn openai_chat() -> Self {
        Self("openai-chat".to_owned())
    }

    pub fn anthropic_messages() -> Self {
        Self("anthropic-messages".to_owned())
    }

    pub fn cache_token_accounting(&self) -> Option<CacheTokenAccounting> {
        match self.0.as_str() {
            "openai-chat" | "openai-responses" => Some(CacheTokenAccounting::IncludedInInput),
            "anthropic-messages" => Some(CacheTokenAccounting::SeparateFromInput),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheTokenAccounting {
    IncludedInInput,
    SeparateFromInput,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CategorizedTokenUsage {
    pub uncached_input_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_write_tokens: u64,
    pub output_tokens: u64,
}

impl CategorizedTokenUsage {
    pub fn total_tokens(self) -> u64 {
        self.uncached_input_tokens
            .saturating_add(self.cache_read_tokens)
            .saturating_add(self.cache_write_tokens)
            .saturating_add(self.output_tokens)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReasoningCapabilities {
    pub supported: bool,
    #[serde(default)]
    pub effort_levels: Vec<String>,
    #[serde(default)]
    pub requires_round_trip_metadata: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ProviderSignals {
    pub concurrency_hint: Option<usize>,
    pub requests_per_minute: Option<u64>,
    pub tokens_per_minute: Option<u64>,
    pub cache_token_accounting: Option<CacheTokenAccounting>,
    pub input_cost_per_million: Option<f64>,
    pub cache_read_cost_per_million: Option<f64>,
    pub cache_write_cost_per_million: Option<f64>,
    pub output_cost_per_million: Option<f64>,
}

impl ProviderSignals {
    pub fn merge(base: Self, overlay: Self) -> Self {
        Self {
            concurrency_hint: overlay.concurrency_hint.or(base.concurrency_hint),
            requests_per_minute: overlay.requests_per_minute.or(base.requests_per_minute),
            tokens_per_minute: overlay.tokens_per_minute.or(base.tokens_per_minute),
            cache_token_accounting: overlay.cache_token_accounting.or(base.cache_token_accounting),
            input_cost_per_million: overlay.input_cost_per_million.or(base.input_cost_per_million),
            cache_read_cost_per_million: overlay.cache_read_cost_per_million.or(base.cache_read_cost_per_million),
            cache_write_cost_per_million: overlay
                .cache_write_cost_per_million
                .or(base.cache_write_cost_per_million),
            output_cost_per_million: overlay.output_cost_per_million.or(base.output_cost_per_million),
        }
    }

    pub fn categorize_usage(&self, usage: &TokenUsage) -> CategorizedTokenUsage {
        let cached_input = usage.cache_read_tokens.saturating_add(usage.cache_creation_tokens);
        let uncached_input_tokens = match self
            .cache_token_accounting
            .unwrap_or(CacheTokenAccounting::IncludedInInput)
        {
            CacheTokenAccounting::IncludedInInput => usage.input_tokens.saturating_sub(cached_input),
            CacheTokenAccounting::SeparateFromInput => usage.input_tokens,
        };
        CategorizedTokenUsage {
            uncached_input_tokens,
            cache_read_tokens: usage.cache_read_tokens,
            cache_write_tokens: usage.cache_creation_tokens,
            output_tokens: usage.output_tokens,
        }
    }

    /// Calculate the cost for one usage record.
    ///
    /// A missing rate makes the result unknown when the corresponding token
    /// category is non-zero. Reporting an exact zero in that case would make
    /// custom providers look free even though their price is simply absent.
    pub fn usage_cost(&self, usage: &TokenUsage) -> Option<f64> {
        let categorized = self.categorize_usage(usage);
        let input = priced_tokens(categorized.uncached_input_tokens, self.input_cost_per_million)?;
        let cache_read = priced_tokens(categorized.cache_read_tokens, self.cache_read_cost_per_million)?;
        let cache_write = priced_tokens(categorized.cache_write_tokens, self.cache_write_cost_per_million)?;
        let output = priced_tokens(categorized.output_tokens, self.output_cost_per_million)?;
        Some((input + cache_read + cache_write + output) / 1_000_000.0)
    }
}

fn priced_tokens(tokens: u64, rate_per_million: Option<f64>) -> Option<f64> {
    if tokens == 0 {
        Some(0.0)
    } else {
        rate_per_million.map(|rate| tokens as f64 * rate)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderContract {
    pub protocol: ProtocolId,
    #[serde(default)]
    pub reasoning: ReasoningCapabilities,
    #[serde(default)]
    pub tool_calling: bool,
    #[serde(default)]
    pub streaming: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_window: Option<u64>,
    #[serde(default)]
    pub cache_stable_append_tail: bool,
    #[serde(default)]
    pub metadata_namespaces: Vec<String>,
    #[serde(default)]
    pub signals: ProviderSignals,
}

pub type ProviderNativeMetadata = BTreeMap<String, Value>;

#[cfg(test)]
#[path = "provider_contract_test.rs"]
mod provider_contract_test;
