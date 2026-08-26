use std::path::Path;
use std::sync::{Arc, Barrier};
use std::thread;

use solaris_types::identity::{AgentId, RunId};
use solaris_types::permission::{PermissionCeiling, PermissionMode};
use solaris_types::runtime::OperationEnvironmentSnapshot;

use super::*;
use crate::permission_engine::PermissionContext;
use crate::runtime_ledger::{InMemoryRuntimeLedger, JsonlRuntimeLedger, SqliteRuntimeLedger};

fn context() -> EffectExecutionContext {
    EffectExecutionContext::new(
        RunId::new("run-plan"),
        AgentId::new("agent-plan"),
        Arc::new(InMemoryRuntimeLedger::default()),
        PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    )
}

fn sqlite_context(path: &Path, run_id: &str) -> EffectExecutionContext {
    EffectExecutionContext::new(
        RunId::new(run_id),
        AgentId::new("agent-plan"),
        Arc::new(SqliteRuntimeLedger::open(path).unwrap()),
        PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    )
}

#[test]
fn plan_artifact_is_durable_and_revisioned() {
    let context = context();
    let first = context.record_plan_artifact("msg-1", "# First").unwrap();
    let second = context.record_plan_artifact("msg-2", "# Second").unwrap();
    let stored = context.plan_artifacts_for_run(context.run_id()).unwrap();

    assert_eq!(first.revision, 1);
    assert_eq!(second.revision, 2);
    assert_eq!(stored, vec![first, second]);
}

#[test]
fn retrying_the_same_submitted_plan_reuses_its_artifact() {
    let context = context();

    let first = context.record_plan_artifact("msg-retry", "# Stable plan").unwrap();
    let recovered = context.record_plan_artifact("msg-retry", "# Stable plan").unwrap();
    let stored = context.plan_artifacts_for_run(context.run_id()).unwrap();

    assert_eq!(recovered, first);
    assert_eq!(stored, vec![first]);
}

#[test]
fn jsonl_keeps_sequential_idempotency_and_revision_semantics() {
    let directory = tempfile::tempdir().unwrap();
    let context = EffectExecutionContext::new(
        RunId::new("run-plan-jsonl"),
        AgentId::new("agent-plan"),
        Arc::new(JsonlRuntimeLedger::open(directory.path().join("runtime.jsonl")).unwrap()),
        PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );

    let first = context.record_plan_artifact("msg-jsonl", "# First").unwrap();
    let retried = context.record_plan_artifact("msg-jsonl", "# First").unwrap();
    let revised = context.record_plan_artifact("msg-jsonl", "# Revised").unwrap();
    let stored = context.plan_artifacts_for_run(context.run_id()).unwrap();

    assert_eq!(retried, first);
    assert_eq!(revised.revision, 2);
    assert_eq!(stored, vec![first, revised]);
}

#[test]
fn independent_sqlite_connections_store_one_identical_submission() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("runtime.sqlite3");
    let first_context = sqlite_context(&path, "run-plan-concurrent-same");
    let second_context = sqlite_context(&path, "run-plan-concurrent-same");
    let barrier = Arc::new(Barrier::new(2));

    let first_barrier = Arc::clone(&barrier);
    let first = thread::spawn(move || {
        first_barrier.wait();
        first_context.record_plan_artifact("msg-same", "# Same")
    });
    let second = thread::spawn(move || {
        barrier.wait();
        second_context.record_plan_artifact("msg-same", "# Same")
    });

    let first = first.join().unwrap().unwrap();
    let second = second.join().unwrap().unwrap();
    let reader = sqlite_context(&path, "run-plan-concurrent-same");
    let stored = reader.plan_artifacts_for_run(reader.run_id()).unwrap();

    assert_eq!(first, second);
    assert_eq!(first.revision, 1);
    assert_eq!(stored, vec![first]);
}

#[test]
fn independent_sqlite_connections_allocate_unique_monotonic_revisions() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("runtime.sqlite3");
    let first_context = sqlite_context(&path, "run-plan-concurrent-different");
    let second_context = sqlite_context(&path, "run-plan-concurrent-different");
    let barrier = Arc::new(Barrier::new(2));

    let first_barrier = Arc::clone(&barrier);
    let first = thread::spawn(move || {
        first_barrier.wait();
        first_context.record_plan_artifact("msg-revision", "# First contender")
    });
    let second = thread::spawn(move || {
        barrier.wait();
        second_context.record_plan_artifact("msg-revision", "# Second contender")
    });

    let first = first.join().unwrap().unwrap();
    let second = second.join().unwrap().unwrap();
    let reader = sqlite_context(&path, "run-plan-concurrent-different");
    let stored = reader.plan_artifacts_for_run(reader.run_id()).unwrap();
    let revisions: Vec<_> = stored.iter().map(|artifact| artifact.revision).collect();

    assert_ne!(first.digest, second.digest);
    assert_eq!(revisions, vec![1, 2]);
    assert_eq!(stored.len(), 2);
    assert_eq!(stored[0].id, stored[1].id);
}
