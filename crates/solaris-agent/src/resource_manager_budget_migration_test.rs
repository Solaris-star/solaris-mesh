use std::sync::Arc;

use solaris_types::effect::DurabilityClass;
use solaris_types::resource::ResourceBudget;

use super::*;
use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};

#[test]
fn legacy_checkpoint_missing_spawn_safety_fields_restores_safe_defaults() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("legacy-resource-budget-defaults");
    let mut budget = serde_json::to_value(ResourceBudget {
        max_spawn_depth: Some(99),
        max_total_descendants_per_run: Some(999),
        ..Default::default()
    })
    .unwrap();
    let budget = budget.as_object_mut().unwrap();
    budget.remove("max_spawn_depth");
    budget.remove("max_total_descendants_per_run");
    ledger
        .append(
            &run_id,
            DurabilityClass::SyncCritical,
            "resource_usage_checkpoint",
            serde_json::json!({
                "budget": budget,
                "usage": ResourceUsage::default(),
            }),
        )
        .unwrap();

    let restored = ResourceManager::new(ResourceBudget {
        max_spawn_depth: Some(8),
        max_total_descendants_per_run: Some(256),
        ..Default::default()
    });
    restored
        .attach_ledger(run_id, Arc::clone(&ledger) as Arc<dyn RuntimeLedger>)
        .unwrap();

    assert_eq!(restored.budget().max_spawn_depth, Some(8));
    assert_eq!(restored.budget().max_total_descendants_per_run, Some(256));
}

#[test]
fn legacy_checkpoint_preserves_explicit_spawn_safety_fields() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("legacy-resource-budget-explicit");
    ledger
        .append(
            &run_id,
            DurabilityClass::SyncCritical,
            "resource_usage_checkpoint",
            serde_json::json!({
                "budget": ResourceBudget {
                    max_spawn_depth: Some(3),
                    max_total_descendants_per_run: Some(17),
                    ..Default::default()
                },
                "usage": ResourceUsage::default(),
            }),
        )
        .unwrap();

    let restored = ResourceManager::new(ResourceBudget {
        max_spawn_depth: Some(8),
        max_total_descendants_per_run: Some(256),
        ..Default::default()
    });
    restored
        .attach_ledger(run_id, Arc::clone(&ledger) as Arc<dyn RuntimeLedger>)
        .unwrap();

    assert_eq!(restored.budget().max_spawn_depth, Some(3));
    assert_eq!(restored.budget().max_total_descendants_per_run, Some(17));
}

#[test]
fn legacy_budget_delta_cannot_remove_migrated_spawn_safety_defaults() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("legacy-resource-budget-delta");
    ledger
        .append(
            &run_id,
            DurabilityClass::SyncCritical,
            "resource_usage_checkpoint",
            serde_json::json!({
                "budget": ResourceBudget {
                    max_spawn_depth: Some(8),
                    max_total_descendants_per_run: Some(256),
                    ..Default::default()
                },
                "usage": ResourceUsage::default(),
            }),
        )
        .unwrap();
    let mut delta_budget = serde_json::to_value(ResourceBudget {
        max_turns: Some(4),
        ..Default::default()
    })
    .unwrap();
    let delta_budget = delta_budget.as_object_mut().unwrap();
    delta_budget.remove("max_spawn_depth");
    delta_budget.remove("max_total_descendants_per_run");
    ledger
        .append(
            &run_id,
            DurabilityClass::SyncCritical,
            "resource_usage_delta",
            serde_json::json!({
                "schema_version": 1,
                "budget": delta_budget,
            }),
        )
        .unwrap();

    let restored = ResourceManager::new(ResourceBudget::default());
    restored
        .attach_ledger(run_id, Arc::clone(&ledger) as Arc<dyn RuntimeLedger>)
        .unwrap();

    assert_eq!(restored.budget().max_spawn_depth, Some(8));
    assert_eq!(restored.budget().max_total_descendants_per_run, Some(256));
    assert_eq!(restored.budget().max_turns, Some(4));
}

#[test]
fn current_resource_checkpoint_declares_its_schema_version() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("versioned-resource-checkpoint");
    let manager = ResourceManager::new(ResourceBudget::default());
    manager
        .attach_ledger(run_id.clone(), Arc::clone(&ledger) as Arc<dyn RuntimeLedger>)
        .unwrap();

    let records = ledger.records_for_run(&run_id).unwrap();
    assert_eq!(records[0].record_type, "resource_usage_checkpoint");
    assert_eq!(records[0].payload["schema_version"], 1);
}
