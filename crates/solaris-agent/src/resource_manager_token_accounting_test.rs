use std::sync::Arc;

use solaris_types::identity::RunId;
use solaris_types::message::TokenUsage;
use solaris_types::provider_contract::{CacheTokenAccounting, ProviderSignals};
use solaris_types::resource::ResourceBudget;

use super::ResourceManager;
use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};

fn categorized_signals(accounting: CacheTokenAccounting) -> ProviderSignals {
    ProviderSignals {
        cache_token_accounting: Some(accounting),
        input_cost_per_million: Some(2.0),
        cache_read_cost_per_million: Some(0.5),
        cache_write_cost_per_million: Some(1.0),
        output_cost_per_million: Some(4.0),
        ..Default::default()
    }
}

fn usage(input_tokens: u64) -> TokenUsage {
    TokenUsage {
        input_tokens,
        cache_read_tokens: 600,
        cache_creation_tokens: 100,
        output_tokens: 50,
    }
}

#[test]
fn included_cache_tokens_are_not_counted_twice() {
    let manager = ResourceManager::new(ResourceBudget::default());
    manager.set_provider_signals(categorized_signals(CacheTokenAccounting::IncludedInInput));

    assert!(manager.record_model_usage(&usage(1_000)));

    let recorded = manager.usage();
    assert_eq!(recorded.tokens, 1_050);
    assert_eq!(recorded.input_tokens, 1_000);
    assert_eq!(recorded.uncached_input_tokens, 300);
    assert_eq!(recorded.cache_read_tokens, 600);
    assert_eq!(recorded.cache_creation_tokens, 100);
    assert_eq!(recorded.output_tokens, 50);
    assert!((recorded.cost - 0.001_2).abs() < 1e-12);
}

#[test]
fn separately_reported_cache_tokens_are_included_in_totals_and_cost() {
    let manager = ResourceManager::new(ResourceBudget::default());
    manager.set_provider_signals(categorized_signals(CacheTokenAccounting::SeparateFromInput));

    assert!(manager.record_model_usage(&usage(300)));

    let recorded = manager.usage();
    assert_eq!(recorded.tokens, 1_050);
    assert_eq!(recorded.input_tokens, 300);
    assert_eq!(recorded.uncached_input_tokens, 300);
    assert_eq!(recorded.cache_read_tokens, 600);
    assert_eq!(recorded.cache_creation_tokens, 100);
    assert_eq!(recorded.output_tokens, 50);
    assert!((recorded.cost - 0.001_2).abs() < 1e-12);
}

#[test]
fn categorized_usage_survives_resource_checkpoint_restore() {
    let ledger: Arc<dyn RuntimeLedger> = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("categorized-token-restore");
    let first = ResourceManager::new(ResourceBudget::default());
    first.set_provider_signals(categorized_signals(CacheTokenAccounting::SeparateFromInput));
    first.attach_ledger(run_id.clone(), Arc::clone(&ledger)).unwrap();
    assert!(first.record_model_usage_once("usage-1", &usage(300), true));

    let restored = ResourceManager::new(ResourceBudget::default());
    restored.attach_ledger(run_id, ledger).unwrap();

    assert_eq!(restored.usage(), first.usage());
    assert_eq!(restored.usage().uncached_input_tokens, 300);
    assert_eq!(restored.usage().tokens, 1_050);
}

#[test]
fn missing_price_marks_cost_unknown_instead_of_reporting_zero() {
    let manager = ResourceManager::new(ResourceBudget::default());
    manager.set_provider_signals(ProviderSignals {
        cache_token_accounting: Some(CacheTokenAccounting::SeparateFromInput),
        input_cost_per_million: Some(2.0),
        output_cost_per_million: Some(4.0),
        ..Default::default()
    });

    assert!(manager.record_model_usage(&usage(300)));

    let recorded = manager.usage();
    assert!(!recorded.cost_known);
    assert_eq!(recorded.cost, 0.0);
}

#[test]
fn configured_cost_budget_fails_closed_when_provider_price_is_incomplete() {
    let manager = ResourceManager::new(ResourceBudget {
        max_cost: Some(10.0),
        ..Default::default()
    });
    manager.set_provider_signals(ProviderSignals {
        cache_token_accounting: Some(CacheTokenAccounting::SeparateFromInput),
        input_cost_per_million: Some(2.0),
        output_cost_per_million: Some(4.0),
        ..Default::default()
    });

    assert!(!manager.record_model_usage(&usage(300)));
    assert_eq!(
        manager.hard_runtime_budget_reason().as_deref(),
        Some("run cost is unknown because provider pricing is incomplete")
    );
}

#[test]
fn unknown_cost_survives_resource_checkpoint_restore() {
    let ledger: Arc<dyn RuntimeLedger> = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("unknown-cost-restore");
    let first = ResourceManager::new(ResourceBudget::default());
    first.set_provider_signals(ProviderSignals {
        cache_token_accounting: Some(CacheTokenAccounting::SeparateFromInput),
        input_cost_per_million: Some(2.0),
        output_cost_per_million: Some(4.0),
        ..Default::default()
    });
    first.attach_ledger(run_id.clone(), Arc::clone(&ledger)).unwrap();
    assert!(first.record_model_usage_once("usage-unknown", &usage(300), true));

    let restored = ResourceManager::new(ResourceBudget::default());
    restored.attach_ledger(run_id, ledger).unwrap();

    assert!(!restored.usage().cost_known);
    assert_eq!(restored.usage().cost, 0.0);
}
