use std::sync::Arc;

use solaris_types::effect::DurabilityClass;
use solaris_types::identity::RunId;
use solaris_types::message::TokenUsage;
use solaris_types::resource::ResourceBudget;
use solaris_types::tool::{ToolCallStat, ToolResultStatus};

use super::*;

fn stat(scope: &str, name: &str, input: serde_json::Value, status: ToolResultStatus) -> ToolCallStat {
    ToolCallStat::new(scope, name, &input, status)
}

#[test]
fn durable_resource_updates_use_deltas_and_restore_without_reapplying_usage() {
    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};

    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("resource-delta-recovery");
    let manager = ResourceManager::new(ResourceBudget::default());
    manager
        .attach_ledger(run_id.clone(), Arc::clone(&ledger) as Arc<dyn RuntimeLedger>)
        .unwrap();

    let usage = TokenUsage {
        input_tokens: 11,
        output_tokens: 7,
        ..Default::default()
    };
    assert!(manager.record_model_usage_once("effect-a", &usage, true));
    assert!(manager.record_model_usage_once("effect-b", &usage, true));

    let records = ledger.records_for_run(&run_id).unwrap();
    assert_eq!(records[0].record_type, "resource_usage_checkpoint");
    assert_eq!(records[1].record_type, "resource_usage_delta");
    assert_eq!(records[2].record_type, "resource_usage_delta");
    assert!(records[1].payload.get("applied_usage_effects").is_none());
    assert_eq!(
        records[1].payload["applied_usage_effects_added"],
        serde_json::json!(["effect-a"])
    );
    assert_eq!(
        records[2].payload["applied_usage_effects_added"],
        serde_json::json!(["effect-b"])
    );

    let restored = ResourceManager::new(ResourceBudget::default());
    restored
        .attach_ledger(run_id, Arc::clone(&ledger) as Arc<dyn RuntimeLedger>)
        .unwrap();
    assert_eq!(restored.usage().turns, 2);
    assert_eq!(restored.usage().tokens, 36);
    assert!(restored.record_model_usage_once("effect-a", &usage, true));
    assert_eq!(restored.usage().turns, 2);
    assert_eq!(restored.usage().tokens, 36);
}

#[test]
fn tool_call_statistics_resume_without_double_counting_a_replayed_round() {
    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};

    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("tool-call-statistics-recovery");
    let first = ResourceManager::new(ResourceBudget::default());
    first
        .attach_ledger(run_id.clone(), Arc::clone(&ledger) as Arc<dyn RuntimeLedger>)
        .unwrap();

    let scope = "task:scope-a|env:env-a";
    let first_round = [
        stat(
            scope,
            "read",
            serde_json::json!({"path": "a"}),
            ToolResultStatus::Executed,
        ),
        stat(
            scope,
            "read",
            serde_json::json!({"path": "b"}),
            ToolResultStatus::CacheHit,
        ),
        stat(scope, "noop", serde_json::json!({}), ToolResultStatus::Noop),
        stat(
            scope,
            "write",
            serde_json::json!({"path": "c"}),
            ToolResultStatus::Denied,
        ),
        stat(scope, "bash", serde_json::json!({"cmd": "x"}), ToolResultStatus::Failed),
    ];
    first.record_tool_calls_once_checked("round-a", &first_round).unwrap();
    first.record_tool_calls_once_checked("round-a", &first_round).unwrap();
    let usage = first.usage();
    assert_eq!(usage.tool_calls, 5);
    assert_eq!(usage.useful_tool_calls, 2);
    assert_eq!(usage.useful_call_rate, Some(0.4));
    assert_eq!(usage.duplicate_tool_calls, 0);
    assert_eq!(usage.duplicate_call_rate, Some(0.0));
    assert_eq!(usage.seen_tool_call_fingerprints.len(), 5);
    let records = ledger.records_for_run(&run_id).unwrap();
    let statistics_delta = records
        .iter()
        .rev()
        .find(|record| record.record_type == "resource_usage_delta")
        .unwrap();
    assert_eq!(statistics_delta.payload["usage"]["tool_calls"], 5);
    assert_eq!(statistics_delta.payload["usage"]["useful_tool_calls"], 2);
    assert_eq!(statistics_delta.payload["usage"]["useful_call_rate"], 0.4);
    assert_eq!(statistics_delta.payload["usage"]["duplicate_tool_calls"], 0);
    assert_eq!(statistics_delta.payload["usage"]["duplicate_call_rate"], 0.0);

    let restored = ResourceManager::new(ResourceBudget::default());
    restored
        .attach_ledger(run_id, Arc::clone(&ledger) as Arc<dyn RuntimeLedger>)
        .unwrap();
    restored
        .record_tool_calls_once_checked("round-a", &first_round)
        .unwrap();
    let second_round = [
        stat(
            scope,
            "grep",
            serde_json::json!({"pattern": "y"}),
            ToolResultStatus::Timeout,
        ),
        stat(
            scope,
            "glob",
            serde_json::json!({"pattern": "z"}),
            ToolResultStatus::Executed,
        ),
    ];
    restored
        .record_tool_calls_once_checked("round-b", &second_round)
        .unwrap();

    let usage = restored.usage();
    assert_eq!(usage.tool_calls, 7);
    assert_eq!(usage.useful_tool_calls, 3);
    assert!((usage.useful_call_rate.unwrap() - 3.0 / 7.0).abs() < f64::EPSILON);
    assert_eq!(usage.duplicate_tool_calls, 0);
    assert_eq!(usage.duplicate_call_rate, Some(0.0));
    let snapshot = serde_json::to_value(usage).unwrap();
    assert_eq!(snapshot["tool_calls"], 7);
    assert_eq!(snapshot["useful_tool_calls"], 3);
    assert!(snapshot["useful_call_rate"].as_f64().is_some());
    assert_eq!(snapshot["duplicate_tool_calls"], 0);
    assert!(snapshot["duplicate_call_rate"].as_f64().is_some());
}

#[test]
fn duplicate_tool_call_fingerprints_survive_checkpoint_restore() {
    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};

    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("duplicate-tool-call-recovery");
    let first = ResourceManager::new(ResourceBudget::default());
    first
        .attach_ledger(run_id.clone(), Arc::clone(&ledger) as Arc<dyn RuntimeLedger>)
        .unwrap();
    let statuses = [ToolResultStatus::Executed, ToolResultStatus::Failed];
    let fingerprints = ["same".to_owned(), "failed".to_owned()];
    first
        .record_tool_call_fingerprints_once_checked("round-a", &statuses, &fingerprints)
        .unwrap();
    first
        .record_tool_call_fingerprints_once_checked("round-b", &statuses, &fingerprints)
        .unwrap();
    assert_eq!(first.usage().duplicate_tool_calls, 2);
    assert_eq!(first.usage().duplicate_call_rate, Some(0.5));

    let restored = ResourceManager::new(ResourceBudget::default());
    restored
        .attach_ledger(run_id, Arc::clone(&ledger) as Arc<dyn RuntimeLedger>)
        .unwrap();
    restored
        .record_tool_call_fingerprints_once_checked("round-c", &[ToolResultStatus::CacheHit], &["same".to_owned()])
        .unwrap();
    let usage = restored.usage();
    assert_eq!(usage.tool_calls, 5);
    assert_eq!(usage.duplicate_tool_calls, 3);
    assert_eq!(usage.duplicate_call_rate, Some(0.6));
}

#[test]
fn resource_deltas_are_compacted_by_periodic_full_snapshots() {
    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};

    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("resource-periodic-snapshot");
    let manager = ResourceManager::new(ResourceBudget::default());
    manager
        .attach_ledger(run_id.clone(), Arc::clone(&ledger) as Arc<dyn RuntimeLedger>)
        .unwrap();

    for _ in 0..persistence::RESOURCE_SNAPSHOT_INTERVAL {
        assert!(manager.record_tokens_and_cost(1, 0.0));
    }

    let records = ledger.records_for_run(&run_id).unwrap();
    assert_eq!(records.len(), persistence::RESOURCE_SNAPSHOT_INTERVAL as usize + 1);
    assert_eq!(records.first().unwrap().record_type, "resource_usage_checkpoint");
    assert_eq!(records.last().unwrap().record_type, "resource_usage_checkpoint");
    assert_eq!(
        records
            .iter()
            .filter(|record| record.record_type == "resource_usage_delta")
            .count(),
        persistence::RESOURCE_SNAPSHOT_INTERVAL as usize - 1
    );
}

#[test]
fn malformed_resource_delta_fails_recovery_without_writing_a_replacement_snapshot() {
    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};

    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("resource-malformed-delta");
    let manager = ResourceManager::new(ResourceBudget::default());
    manager
        .attach_ledger(run_id.clone(), Arc::clone(&ledger) as Arc<dyn RuntimeLedger>)
        .unwrap();
    ledger
        .append(
            &run_id,
            DurabilityClass::SyncCritical,
            "resource_usage_delta",
            serde_json::json!({"schema_version": 999}),
        )
        .unwrap();
    let before = ledger.records_for_run(&run_id).unwrap().len();

    let restored = ResourceManager::new(ResourceBudget::default());
    let error = restored
        .attach_ledger(run_id.clone(), Arc::clone(&ledger) as Arc<dyn RuntimeLedger>)
        .unwrap_err();

    assert!(error.contains("unsupported resource delta schema version 999"));
    assert_eq!(ledger.records_for_run(&run_id).unwrap().len(), before);
    assert_eq!(restored.usage(), ResourceUsage::default());
}
