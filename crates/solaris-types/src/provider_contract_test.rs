use crate::message::TokenUsage;

use super::{CacheTokenAccounting, ProtocolId, ProviderSignals};

#[test]
fn protocol_declares_cache_token_accounting_without_provider_name_checks() {
    assert_eq!(
        ProtocolId::openai_chat().cache_token_accounting(),
        Some(CacheTokenAccounting::IncludedInInput)
    );
    assert_eq!(
        ProtocolId::openai_responses().cache_token_accounting(),
        Some(CacheTokenAccounting::IncludedInInput)
    );
    assert_eq!(
        ProtocolId::anthropic_messages().cache_token_accounting(),
        Some(CacheTokenAccounting::SeparateFromInput)
    );
    assert_eq!(ProtocolId("custom".to_owned()).cache_token_accounting(), None);
}

#[test]
fn included_cache_tokens_are_subtracted_from_uncached_input() {
    let signals = ProviderSignals {
        cache_token_accounting: Some(CacheTokenAccounting::IncludedInInput),
        ..Default::default()
    };
    let usage = signals.categorize_usage(&TokenUsage {
        input_tokens: 1_000,
        output_tokens: 50,
        cache_creation_tokens: 100,
        cache_read_tokens: 600,
    });

    assert_eq!(usage.uncached_input_tokens, 300);
    assert_eq!(usage.cache_read_tokens, 600);
    assert_eq!(usage.cache_write_tokens, 100);
    assert_eq!(usage.output_tokens, 50);
    assert_eq!(usage.total_tokens(), 1_050);
}

#[test]
fn separate_cache_tokens_do_not_reduce_uncached_input() {
    let signals = ProviderSignals {
        cache_token_accounting: Some(CacheTokenAccounting::SeparateFromInput),
        ..Default::default()
    };
    let usage = signals.categorize_usage(&TokenUsage {
        input_tokens: 300,
        output_tokens: 50,
        cache_creation_tokens: 100,
        cache_read_tokens: 600,
    });

    assert_eq!(usage.uncached_input_tokens, 300);
    assert_eq!(usage.total_tokens(), 1_050);
}

#[test]
fn cost_uses_four_independent_rates_without_double_charging_cached_input() {
    let signals = ProviderSignals {
        cache_token_accounting: Some(CacheTokenAccounting::IncludedInInput),
        input_cost_per_million: Some(2.0),
        cache_read_cost_per_million: Some(0.2),
        cache_write_cost_per_million: Some(2.5),
        output_cost_per_million: Some(8.0),
        ..Default::default()
    };
    let usage = TokenUsage {
        input_tokens: 1_000_000,
        output_tokens: 100_000,
        cache_creation_tokens: 100_000,
        cache_read_tokens: 600_000,
    };

    let expected = 300_000.0 * 2.0 / 1_000_000.0
        + 600_000.0 * 0.2 / 1_000_000.0
        + 100_000.0 * 2.5 / 1_000_000.0
        + 100_000.0 * 8.0 / 1_000_000.0;
    assert!((signals.usage_cost(&usage).unwrap() - expected).abs() < 1e-12);
}

#[test]
fn missing_rate_makes_non_zero_usage_cost_unknown() {
    let signals = ProviderSignals {
        cache_token_accounting: Some(CacheTokenAccounting::SeparateFromInput),
        input_cost_per_million: Some(2.0),
        output_cost_per_million: Some(3.0),
        ..Default::default()
    };
    let usage = TokenUsage {
        input_tokens: 20,
        output_tokens: 5,
        cache_creation_tokens: 7,
        cache_read_tokens: 8,
    };

    assert_eq!(signals.usage_cost(&usage), None);
}

#[test]
fn missing_rate_for_an_empty_category_does_not_hide_known_cost() {
    let signals = ProviderSignals {
        input_cost_per_million: Some(2.0),
        output_cost_per_million: Some(3.0),
        ..Default::default()
    };
    let usage = TokenUsage {
        input_tokens: 20,
        output_tokens: 5,
        ..Default::default()
    };

    assert_eq!(signals.usage_cost(&usage), Some(0.000_055));
}

#[test]
fn merge_preserves_base_values_and_applies_only_present_overlay_values() {
    let merged = ProviderSignals::merge(
        ProviderSignals {
            requests_per_minute: Some(10),
            input_cost_per_million: Some(2.0),
            cache_read_cost_per_million: Some(0.2),
            ..Default::default()
        },
        ProviderSignals {
            requests_per_minute: Some(20),
            output_cost_per_million: Some(8.0),
            ..Default::default()
        },
    );

    assert_eq!(merged.requests_per_minute, Some(20));
    assert_eq!(merged.input_cost_per_million, Some(2.0));
    assert_eq!(merged.cache_read_cost_per_million, Some(0.2));
    assert_eq!(merged.output_cost_per_million, Some(8.0));
}
