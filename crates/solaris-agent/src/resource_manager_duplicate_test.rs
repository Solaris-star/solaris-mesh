use std::sync::Arc;

use solaris_types::identity::RunId;
use solaris_types::resource::ResourceBudget;
use solaris_types::tool::{ToolCallStat, ToolResultStatus};

use super::*;

fn stat(scope: &str, name: &str, input: serde_json::Value, status: ToolResultStatus) -> ToolCallStat {
    ToolCallStat::new(scope, name, &input, status)
}

fn manager() -> Arc<ResourceManager> {
    ResourceManager::new(ResourceBudget::default())
}

#[test]
fn first_occurrence_is_never_a_duplicate() {
    let manager = manager();
    let scope = "task:t|env:e";
    manager
        .record_tool_calls_once_checked(
            "round-1",
            &[stat(
                scope,
                "read",
                serde_json::json!({"path": "/a"}),
                ToolResultStatus::Executed,
            )],
        )
        .unwrap();

    let usage = manager.usage();
    assert_eq!(usage.tool_calls, 1);
    assert_eq!(usage.duplicate_tool_calls, 0);
    assert_eq!(usage.duplicate_call_rate, Some(0.0));
    assert_eq!(usage.seen_tool_call_fingerprints.len(), 1);
}

#[test]
fn identical_input_is_a_duplicate() {
    let manager = manager();
    let scope = "task:t|env:e";
    manager
        .record_tool_calls_once_checked(
            "round-1",
            &[stat(
                scope,
                "read",
                serde_json::json!({"path": "/a"}),
                ToolResultStatus::Executed,
            )],
        )
        .unwrap();
    manager
        .record_tool_calls_once_checked(
            "round-2",
            &[stat(
                scope,
                "read",
                serde_json::json!({"path": "/a"}),
                ToolResultStatus::Executed,
            )],
        )
        .unwrap();

    let usage = manager.usage();
    assert_eq!(usage.tool_calls, 2);
    assert_eq!(usage.duplicate_tool_calls, 1);
    assert_eq!(usage.duplicate_call_rate, Some(0.5));
}

#[test]
fn object_key_order_variant_is_a_duplicate() {
    let manager = manager();
    let scope = "task:t|env:e";
    manager
        .record_tool_calls_once_checked(
            "round-1",
            &[stat(
                scope,
                "read",
                serde_json::json!({"path": "/a", "limit": 10}),
                ToolResultStatus::Executed,
            )],
        )
        .unwrap();
    manager
        .record_tool_calls_once_checked(
            "round-2",
            &[stat(
                scope,
                "read",
                serde_json::json!({"limit": 10, "path": "/a"}),
                ToolResultStatus::CacheHit,
            )],
        )
        .unwrap();

    let usage = manager.usage();
    assert_eq!(usage.tool_calls, 2);
    assert_eq!(usage.duplicate_tool_calls, 1);
    assert_eq!(usage.duplicate_call_rate, Some(0.5));
}

#[test]
fn different_input_is_not_a_duplicate() {
    let manager = manager();
    let scope = "task:t|env:e";
    manager
        .record_tool_calls_once_checked(
            "round-1",
            &[stat(
                scope,
                "read",
                serde_json::json!({"path": "/a"}),
                ToolResultStatus::Executed,
            )],
        )
        .unwrap();
    manager
        .record_tool_calls_once_checked(
            "round-2",
            &[stat(
                scope,
                "read",
                serde_json::json!({"path": "/b"}),
                ToolResultStatus::Executed,
            )],
        )
        .unwrap();

    let usage = manager.usage();
    assert_eq!(usage.tool_calls, 2);
    assert_eq!(usage.duplicate_tool_calls, 0);
    assert_eq!(usage.duplicate_call_rate, Some(0.0));
}

#[test]
fn different_tool_name_is_not_a_duplicate() {
    let manager = manager();
    let scope = "task:t|env:e";
    let input = serde_json::json!({"path": "/a"});
    manager
        .record_tool_calls_once_checked(
            "round-1",
            &[stat(scope, "read", input.clone(), ToolResultStatus::Executed)],
        )
        .unwrap();
    manager
        .record_tool_calls_once_checked("round-2", &[stat(scope, "write", input, ToolResultStatus::Executed)])
        .unwrap();

    let usage = manager.usage();
    assert_eq!(usage.tool_calls, 2);
    assert_eq!(usage.duplicate_tool_calls, 0);
    assert_eq!(usage.duplicate_call_rate, Some(0.0));
}

#[test]
fn cross_task_scope_is_not_a_duplicate() {
    let manager = manager();
    manager
        .record_tool_calls_once_checked(
            "round-1",
            &[stat(
                "task:a|env:e",
                "read",
                serde_json::json!({"path": "/a"}),
                ToolResultStatus::Executed,
            )],
        )
        .unwrap();
    manager
        .record_tool_calls_once_checked(
            "round-2",
            &[stat(
                "task:b|env:e",
                "read",
                serde_json::json!({"path": "/a"}),
                ToolResultStatus::Executed,
            )],
        )
        .unwrap();

    let usage = manager.usage();
    assert_eq!(usage.tool_calls, 2);
    assert_eq!(usage.duplicate_tool_calls, 0);
    assert_eq!(usage.duplicate_call_rate, Some(0.0));
}

#[test]
fn cross_environment_scope_is_not_a_duplicate() {
    let manager = manager();
    manager
        .record_tool_calls_once_checked(
            "round-1",
            &[stat(
                "task:t|env:e1",
                "read",
                serde_json::json!({"path": "/a"}),
                ToolResultStatus::Executed,
            )],
        )
        .unwrap();
    manager
        .record_tool_calls_once_checked(
            "round-2",
            &[stat(
                "task:t|env:e2",
                "read",
                serde_json::json!({"path": "/a"}),
                ToolResultStatus::Executed,
            )],
        )
        .unwrap();

    let usage = manager.usage();
    assert_eq!(usage.tool_calls, 2);
    assert_eq!(usage.duplicate_tool_calls, 0);
    assert_eq!(usage.duplicate_call_rate, Some(0.0));
}

#[test]
fn cache_hit_repeat_counts_as_duplicate() {
    let manager = manager();
    let scope = "task:t|env:e";
    manager
        .record_tool_calls_once_checked(
            "round-1",
            &[stat(
                scope,
                "read",
                serde_json::json!({"path": "/a"}),
                ToolResultStatus::Executed,
            )],
        )
        .unwrap();
    manager
        .record_tool_calls_once_checked(
            "round-2",
            &[stat(
                scope,
                "read",
                serde_json::json!({"path": "/a"}),
                ToolResultStatus::CacheHit,
            )],
        )
        .unwrap();

    let usage = manager.usage();
    assert_eq!(usage.tool_calls, 2);
    assert_eq!(usage.useful_tool_calls, 2);
    assert_eq!(usage.duplicate_tool_calls, 1);
    assert_eq!(usage.duplicate_call_rate, Some(0.5));
}

#[test]
fn failed_repeat_counts_as_duplicate() {
    let manager = manager();
    let scope = "task:t|env:e";
    manager
        .record_tool_calls_once_checked(
            "round-1",
            &[stat(
                scope,
                "bash",
                serde_json::json!({"cmd": "x"}),
                ToolResultStatus::Failed,
            )],
        )
        .unwrap();
    manager
        .record_tool_calls_once_checked(
            "round-2",
            &[stat(
                scope,
                "bash",
                serde_json::json!({"cmd": "x"}),
                ToolResultStatus::Failed,
            )],
        )
        .unwrap();

    let usage = manager.usage();
    assert_eq!(usage.tool_calls, 2);
    assert_eq!(usage.useful_tool_calls, 0);
    assert_eq!(usage.duplicate_tool_calls, 1);
    assert_eq!(usage.duplicate_call_rate, Some(0.5));
}

#[test]
fn denied_and_aborted_repeats_count_as_duplicates() {
    let manager = manager();
    let scope = "task:t|env:e";
    manager
        .record_tool_calls_once_checked(
            "round-1",
            &[
                stat(
                    scope,
                    "write",
                    serde_json::json!({"path": "/c"}),
                    ToolResultStatus::Denied,
                ),
                stat(
                    scope,
                    "bash",
                    serde_json::json!({"cmd": "y"}),
                    ToolResultStatus::Aborted,
                ),
            ],
        )
        .unwrap();
    manager
        .record_tool_calls_once_checked(
            "round-2",
            &[
                stat(
                    scope,
                    "write",
                    serde_json::json!({"path": "/c"}),
                    ToolResultStatus::Denied,
                ),
                stat(
                    scope,
                    "bash",
                    serde_json::json!({"cmd": "y"}),
                    ToolResultStatus::OutcomeUnknown,
                ),
            ],
        )
        .unwrap();

    let usage = manager.usage();
    assert_eq!(usage.tool_calls, 4);
    assert_eq!(usage.duplicate_tool_calls, 2);
    assert_eq!(usage.duplicate_call_rate, Some(0.5));
}

#[test]
fn duplicate_within_a_single_round_counts() {
    let manager = manager();
    let scope = "task:t|env:e";
    manager
        .record_tool_calls_once_checked(
            "round-1",
            &[
                stat(
                    scope,
                    "read",
                    serde_json::json!({"path": "/a"}),
                    ToolResultStatus::Executed,
                ),
                stat(
                    scope,
                    "read",
                    serde_json::json!({"path": "/a"}),
                    ToolResultStatus::Executed,
                ),
                stat(
                    scope,
                    "read",
                    serde_json::json!({"path": "/b"}),
                    ToolResultStatus::Executed,
                ),
            ],
        )
        .unwrap();

    let usage = manager.usage();
    assert_eq!(usage.tool_calls, 3);
    assert_eq!(usage.duplicate_tool_calls, 1);
    assert!((usage.duplicate_call_rate.unwrap() - 1.0 / 3.0).abs() < f64::EPSILON);
}

#[test]
fn replayed_round_is_idempotent_for_duplicates() {
    let manager = manager();
    let scope = "task:t|env:e";
    let round = [
        stat(
            scope,
            "read",
            serde_json::json!({"path": "/a"}),
            ToolResultStatus::Executed,
        ),
        stat(
            scope,
            "read",
            serde_json::json!({"path": "/a"}),
            ToolResultStatus::Executed,
        ),
    ];
    manager.record_tool_calls_once_checked("round-1", &round).unwrap();
    // Replaying the same round call ID must not re-apply the statistics.
    manager.record_tool_calls_once_checked("round-1", &round).unwrap();

    let usage = manager.usage();
    assert_eq!(usage.tool_calls, 2);
    assert_eq!(usage.duplicate_tool_calls, 1);
    assert_eq!(usage.duplicate_call_rate, Some(0.5));
    assert_eq!(usage.seen_tool_call_fingerprints.len(), 1);
}

#[test]
fn duplicate_detection_survives_checkpoint_restore() {
    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};

    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("duplicate-restore");
    let scope = "task:t|env:e";
    let first = ResourceManager::new(ResourceBudget::default());
    first
        .attach_ledger(run_id.clone(), Arc::clone(&ledger) as Arc<dyn RuntimeLedger>)
        .unwrap();
    first
        .record_tool_calls_once_checked(
            "round-1",
            &[stat(
                scope,
                "read",
                serde_json::json!({"path": "/a"}),
                ToolResultStatus::Executed,
            )],
        )
        .unwrap();

    let restored = ResourceManager::new(ResourceBudget::default());
    restored
        .attach_ledger(run_id, Arc::clone(&ledger) as Arc<dyn RuntimeLedger>)
        .unwrap();
    // The fingerprint was persisted, so the same call after restore is a
    // duplicate even though it is the first call observed by this manager.
    restored
        .record_tool_calls_once_checked(
            "round-2",
            &[stat(
                scope,
                "read",
                serde_json::json!({"path": "/a"}),
                ToolResultStatus::Executed,
            )],
        )
        .unwrap();

    let usage = restored.usage();
    assert_eq!(usage.tool_calls, 2);
    assert_eq!(usage.duplicate_tool_calls, 1);
    assert_eq!(usage.duplicate_call_rate, Some(0.5));
}

#[test]
fn duplicate_detection_survives_delta_restore() {
    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};

    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("duplicate-delta-restore");
    let scope = "task:t|env:e";
    let first = ResourceManager::new(ResourceBudget::default());
    first
        .attach_ledger(run_id.clone(), Arc::clone(&ledger) as Arc<dyn RuntimeLedger>)
        .unwrap();
    first
        .record_tool_calls_once_checked(
            "round-1",
            &[stat(
                scope,
                "read",
                serde_json::json!({"path": "/a"}),
                ToolResultStatus::Executed,
            )],
        )
        .unwrap();
    first
        .record_tool_calls_once_checked(
            "round-2",
            &[stat(
                scope,
                "read",
                serde_json::json!({"path": "/b"}),
                ToolResultStatus::Executed,
            )],
        )
        .unwrap();

    let restored = ResourceManager::new(ResourceBudget::default());
    restored
        .attach_ledger(run_id, Arc::clone(&ledger) as Arc<dyn RuntimeLedger>)
        .unwrap();
    restored
        .record_tool_calls_once_checked(
            "round-3",
            &[stat(
                scope,
                "read",
                serde_json::json!({"path": "/a"}),
                ToolResultStatus::CacheHit,
            )],
        )
        .unwrap();

    let usage = restored.usage();
    assert_eq!(usage.tool_calls, 3);
    assert_eq!(usage.duplicate_tool_calls, 1);
    assert!((usage.duplicate_call_rate.unwrap() - 1.0 / 3.0).abs() < f64::EPSILON);
}

#[test]
fn usage_effect_is_idempotent_across_restore() {
    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};

    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("duplicate-idempotent-restore");
    let scope = "task:t|env:e";
    let round = [
        stat(
            scope,
            "read",
            serde_json::json!({"path": "/a"}),
            ToolResultStatus::Executed,
        ),
        stat(
            scope,
            "read",
            serde_json::json!({"path": "/a"}),
            ToolResultStatus::Executed,
        ),
    ];
    let first = ResourceManager::new(ResourceBudget::default());
    first
        .attach_ledger(run_id.clone(), Arc::clone(&ledger) as Arc<dyn RuntimeLedger>)
        .unwrap();
    first.record_tool_calls_once_checked("round-1", &round).unwrap();

    let restored = ResourceManager::new(ResourceBudget::default());
    restored
        .attach_ledger(run_id, Arc::clone(&ledger) as Arc<dyn RuntimeLedger>)
        .unwrap();
    // Replaying the already-applied round after restore must be a no-op.
    restored.record_tool_calls_once_checked("round-1", &round).unwrap();

    let usage = restored.usage();
    assert_eq!(usage.tool_calls, 2);
    assert_eq!(usage.duplicate_tool_calls, 1);
    assert_eq!(usage.duplicate_call_rate, Some(0.5));
}

#[test]
fn duplicate_fields_persist_in_checkpoint_and_delta() {
    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};

    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("duplicate-persist");
    let scope = "task:t|env:e";
    let manager = ResourceManager::new(ResourceBudget::default());
    manager
        .attach_ledger(run_id.clone(), Arc::clone(&ledger) as Arc<dyn RuntimeLedger>)
        .unwrap();
    manager
        .record_tool_calls_once_checked(
            "round-1",
            &[
                stat(
                    scope,
                    "read",
                    serde_json::json!({"path": "/a"}),
                    ToolResultStatus::Executed,
                ),
                stat(
                    scope,
                    "read",
                    serde_json::json!({"path": "/a"}),
                    ToolResultStatus::Executed,
                ),
            ],
        )
        .unwrap();

    let records = ledger.records_for_run(&run_id).unwrap();
    let delta = records
        .iter()
        .rev()
        .find(|record| record.record_type == "resource_usage_delta")
        .unwrap();
    assert_eq!(delta.payload["usage"]["tool_calls"], 2);
    assert_eq!(delta.payload["usage"]["duplicate_tool_calls"], 1);
    assert_eq!(delta.payload["usage"]["duplicate_call_rate"], 0.5);
    assert!(
        delta.payload["usage"]["seen_tool_call_fingerprints"]
            .as_array()
            .is_some()
    );
}
