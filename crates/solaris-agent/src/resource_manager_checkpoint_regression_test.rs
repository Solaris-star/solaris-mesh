use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use solaris_types::effect::DurabilityClass;
use solaris_types::identity::RunId;
use solaris_types::message::TokenUsage;
use solaris_types::provider_contract::ProviderSignals;
use solaris_types::resource::ResourceBudget;

use crate::collaboration_runtime::CollaborationRuntime;
use crate::resource_policy::ResourcePolicy;
use crate::runtime_ledger::{InMemoryRuntimeLedger, LedgerRecord, RuntimeLedger};
use crate::scheduler::Scheduler;

use super::*;

#[derive(Default)]
struct MutationObservingLedger {
    inner: InMemoryRuntimeLedger,
    runtime: Mutex<Option<Weak<CollaborationRuntime<()>>>>,
    read_held_run_line: AtomicBool,
    append_held_run_line: AtomicBool,
}

impl MutationObservingLedger {
    fn run_line_is_held(&self, run_id: &RunId) -> bool {
        let runtime = self
            .runtime
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .and_then(Weak::upgrade)
            .expect("collaboration runtime");
        let line = runtime.mutation_coordinator().line_for(run_id);
        let held = matches!(line.try_lock(), Err(std::sync::TryLockError::WouldBlock));
        held
    }
}

impl RuntimeLedger for MutationObservingLedger {
    crate::runtime_ledger::forward_compare_and_append!();

    fn append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        payload: serde_json::Value,
    ) -> std::io::Result<LedgerRecord> {
        if record_type == "resource_usage_checkpoint" {
            self.append_held_run_line
                .store(self.run_line_is_held(run_id), Ordering::SeqCst);
        }
        self.inner.append(run_id, durability, record_type, payload)
    }

    fn run_ids(&self) -> std::io::Result<Vec<RunId>> {
        self.inner.run_ids()
    }

    fn records_for_run(&self, run_id: &RunId) -> std::io::Result<Vec<LedgerRecord>> {
        self.read_held_run_line
            .store(self.run_line_is_held(run_id), Ordering::SeqCst);
        self.inner.records_for_run(run_id)
    }
}

#[derive(Default)]
struct SwitchableAttachLedger {
    inner: InMemoryRuntimeLedger,
    fail_snapshots: AtomicBool,
}

impl RuntimeLedger for SwitchableAttachLedger {
    crate::runtime_ledger::forward_compare_and_append!();

    fn append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        payload: serde_json::Value,
    ) -> std::io::Result<LedgerRecord> {
        if record_type == "resource_usage_checkpoint" && self.fail_snapshots.load(Ordering::SeqCst) {
            return Err(std::io::Error::other("injected attach snapshot failure"));
        }
        self.inner.append(run_id, durability, record_type, payload)
    }

    fn run_ids(&self) -> std::io::Result<Vec<RunId>> {
        self.inner.run_ids()
    }

    fn records_for_run(&self, run_id: &RunId) -> std::io::Result<Vec<LedgerRecord>> {
        self.inner.records_for_run(run_id)
    }
}

#[test]
fn first_runtime_attach_holds_the_new_run_mutation_line_for_recovery_and_snapshot() {
    let ledger = Arc::new(MutationObservingLedger::default());
    let runtime = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(1)),
        Arc::clone(&ledger) as Arc<dyn RuntimeLedger>,
    ));
    *ledger.runtime.lock().unwrap_or_else(|error| error.into_inner()) = Some(Arc::downgrade(&runtime));

    ResourceManager::new(ResourceBudget::default())
        .attach_runtime(RunId::from("first-attach-mutation-line"), runtime)
        .unwrap();

    assert!(ledger.read_held_run_line.load(Ordering::SeqCst));
    assert!(ledger.append_held_run_line.load(Ordering::SeqCst));
}

#[test]
fn periodic_snapshots_do_not_repeat_the_full_applied_usage_history() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("linear-resource-history");
    let manager = ResourceManager::new(ResourceBudget::default());
    manager
        .attach_ledger(run_id.clone(), Arc::clone(&ledger) as Arc<dyn RuntimeLedger>)
        .unwrap();
    let usage = TokenUsage {
        input_tokens: 1,
        ..Default::default()
    };

    for index in 0..persistence::RESOURCE_SNAPSHOT_INTERVAL * 4 {
        assert!(manager.record_model_usage_once(&format!("effect-{index:04}"), &usage, false));
    }

    let records = ledger.records_for_run(&run_id).unwrap();
    let checkpoint_sizes = records
        .iter()
        .filter(|record| record.record_type == "resource_usage_checkpoint")
        .map(|record| {
            assert!(
                record.payload.get("applied_usage_effects").is_none(),
                "full effect history must not be copied into periodic snapshots"
            );
            record.payload.to_string().len()
        })
        .collect::<Vec<_>>();
    let smallest = *checkpoint_sizes.iter().min().unwrap();
    let largest = *checkpoint_sizes.iter().max().unwrap();
    assert!(largest.saturating_sub(smallest) < 256);

    let restored = ResourceManager::new(ResourceBudget::default());
    restored
        .attach_ledger(run_id, Arc::clone(&ledger) as Arc<dyn RuntimeLedger>)
        .unwrap();
    let before = restored.usage();
    assert!(restored.record_model_usage_once("effect-0000", &usage, false));
    assert_eq!(restored.usage(), before);
}

#[test]
fn cold_resume_restores_delta_count_and_still_emits_periodic_snapshots() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("cold-resume-resource-snapshot-interval");

    for _ in 0..72 {
        let manager = ResourceManager::new(ResourceBudget::default());
        manager
            .attach_ledger(run_id.clone(), Arc::clone(&ledger) as Arc<dyn RuntimeLedger>)
            .unwrap();
        assert!(manager.record_tokens_and_cost(1, 0.0));
    }

    let records = ledger.records_for_run(&run_id).unwrap();
    assert_eq!(
        records.len(),
        73,
        "each state change must add exactly one resource record"
    );
    let checkpoint_indexes = records
        .iter()
        .enumerate()
        .filter_map(|(index, record)| (record.record_type == "resource_usage_checkpoint").then_some(index))
        .collect::<Vec<_>>();
    assert_eq!(
        checkpoint_indexes,
        vec![0, persistence::RESOURCE_SNAPSHOT_INTERVAL as usize]
    );
    assert!(
        records.len() - 1 - checkpoint_indexes.last().copied().unwrap()
            < persistence::RESOURCE_SNAPSHOT_INTERVAL as usize,
        "cold resume must not allow an unbounded delta tail"
    );
}

#[test]
fn changing_an_unrelated_budget_field_preserves_the_wall_time_deadline() {
    let now = Arc::new(AtomicI64::new(1_000));
    let clock: Arc<dyn Fn() -> i64 + Send + Sync> = {
        let now = Arc::clone(&now);
        Arc::new(move || now.load(Ordering::SeqCst))
    };
    let manager = ResourceManager::new_with_clock(
        ResourceBudget {
            max_wall_time_ms: Some(500),
            ..Default::default()
        },
        clock,
    );

    now.store(1_400, Ordering::SeqCst);
    manager
        .set_budget(ResourceBudget {
            max_wall_time_ms: Some(500),
            max_turns: Some(10),
            ..Default::default()
        })
        .unwrap();

    now.store(1_499, Ordering::SeqCst);
    assert!(manager.hard_runtime_budget_reason().is_none());
    now.store(1_500, Ordering::SeqCst);
    assert!(manager.hard_runtime_budget_reason().unwrap().contains("wall-time"));
}

#[test]
fn failed_attach_snapshot_restores_all_manager_state_and_leaves_it_detached() {
    let failing = Arc::new(SwitchableAttachLedger::default());
    let run_id = RunId::from("attach-rollback");
    failing.fail_snapshots.store(true, Ordering::SeqCst);

    let manager = ResourceManager::new(ResourceBudget {
        max_turns: Some(99),
        ..Default::default()
    });
    manager.set_provider_signals(ProviderSignals {
        requests_per_minute: Some(12),
        ..Default::default()
    });
    assert!(manager.record_tokens_and_cost(3, 0.0));
    let before = manager.state_snapshot();

    let error = manager
        .attach_ledger(run_id, Arc::clone(&failing) as Arc<dyn RuntimeLedger>)
        .unwrap_err();
    assert!(error.contains("injected attach snapshot failure"));
    assert!(manager.state_snapshot() == before);
    assert!(
        manager
            .persistence
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .is_none()
    );
    assert!(
        manager
            .persistence_error
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_none()
    );
    assert_eq!(
        *manager
            .resource_deltas_since_snapshot
            .lock()
            .unwrap_or_else(|error| error.into_inner()),
        0
    );

    manager
        .attach_ledger(
            RunId::from("attach-after-rollback"),
            Arc::new(InMemoryRuntimeLedger::default()),
        )
        .unwrap();
    assert!(manager.record_tokens_and_cost(1, 0.0));
}

#[test]
fn legacy_checkpoints_accept_missing_provider_rate_and_missing_rate_fields() {
    for (suffix, provider_rate) in [
        ("missing-parent", None),
        ("missing-children", Some(serde_json::json!({"started_at_unix_ms": 0}))),
    ] {
        let ledger = Arc::new(InMemoryRuntimeLedger::default());
        let run_id = RunId::from(format!("legacy-rate-{suffix}"));
        let mut payload = serde_json::json!({
            "budget": ResourceBudget::default(),
            "provider_signals": ProviderSignals::default(),
            "usage": ResourceUsage {
                turns: 4,
                tokens: 9,
                ..Default::default()
            },
            "deadline_unix_ms": null
        });
        if let Some(provider_rate) = provider_rate {
            payload["provider_rate"] = provider_rate;
        }
        ledger
            .append(
                &run_id,
                DurabilityClass::SyncCritical,
                "resource_usage_checkpoint",
                payload,
            )
            .unwrap();

        let restored = ResourceManager::new(ResourceBudget::default());
        restored
            .attach_ledger(run_id, Arc::clone(&ledger) as Arc<dyn RuntimeLedger>)
            .unwrap();
        assert_eq!(restored.usage().turns, 4);
        assert_eq!(restored.usage().tokens, 9);
    }
}
