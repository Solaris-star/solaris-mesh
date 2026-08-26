use solaris_types::effect::DurabilityClass;
use solaris_types::message::TokenUsage;
use solaris_types::provider_contract::ProviderSignals;
use solaris_types::resource::ResourceBudget;

use super::*;

#[derive(Default)]
struct FailingCheckpointLedger {
    inner: crate::runtime_ledger::InMemoryRuntimeLedger,
    fail: std::sync::atomic::AtomicBool,
}

impl crate::runtime_ledger::RuntimeLedger for FailingCheckpointLedger {
    crate::runtime_ledger::forward_compare_and_append!();

    fn append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        payload: serde_json::Value,
    ) -> std::io::Result<crate::runtime_ledger::LedgerRecord> {
        if matches!(record_type, "resource_usage_checkpoint" | "resource_usage_delta")
            && self.fail.load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(std::io::Error::other("injected checkpoint failure"));
        }
        self.inner.append(run_id, durability, record_type, payload)
    }

    fn run_ids(&self) -> std::io::Result<Vec<RunId>> {
        self.inner.run_ids()
    }

    fn records_for_run(&self, run_id: &RunId) -> std::io::Result<Vec<crate::runtime_ledger::LedgerRecord>> {
        self.inner.records_for_run(run_id)
    }
}

struct TransactionObservingLedger {
    inner: crate::runtime_ledger::InMemoryRuntimeLedger,
    manager: std::sync::Weak<ResourceManager>,
    recovery_held_transaction: std::sync::atomic::AtomicBool,
}

impl crate::runtime_ledger::RuntimeLedger for TransactionObservingLedger {
    crate::runtime_ledger::forward_compare_and_append!();

    fn append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        payload: serde_json::Value,
    ) -> std::io::Result<crate::runtime_ledger::LedgerRecord> {
        self.inner.append(run_id, durability, record_type, payload)
    }

    fn run_ids(&self) -> std::io::Result<Vec<RunId>> {
        self.inner.run_ids()
    }

    fn records_for_run(&self, run_id: &RunId) -> std::io::Result<Vec<crate::runtime_ledger::LedgerRecord>> {
        let manager = self.manager.upgrade().expect("resource manager");
        let transaction_is_held = matches!(
            manager.checkpoint_transaction.try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        );
        self.recovery_held_transaction
            .store(transaction_is_held, std::sync::atomic::Ordering::SeqCst);
        self.inner.records_for_run(run_id)
    }
}

#[test]
fn model_usage_preserves_the_provider_token_breakdown() {
    let manager = ResourceManager::new(ResourceBudget::default());
    let usage = TokenUsage {
        input_tokens: 100,
        output_tokens: 20,
        cache_creation_tokens: 7,
        cache_read_tokens: 60,
    };

    assert!(manager.record_model_usage(&usage));
    let recorded = manager.usage();
    assert_eq!(recorded.tokens, 120);
    assert_eq!(recorded.uncached_input_tokens, 33);
    assert_eq!(recorded.input_tokens, 100);
    assert_eq!(recorded.output_tokens, 20);
    assert_eq!(recorded.cache_creation_tokens, 7);
    assert_eq!(recorded.cache_read_tokens, 60);
}

#[test]
fn default_agent_budget_uses_bounded_system_parallelism() {
    for (system_parallelism, expected) in [(1, 2), (2, 2), (8, 8), (32, 8)] {
        let manager = ResourceManager::new_with_parallelism_hint(ResourceBudget::default(), system_parallelism);
        assert_eq!(manager.effective_agent_limit(), expected);
    }
}

#[test]
fn explicit_agent_budget_is_clamped_to_supported_range() {
    for (configured, expected) in [(0, 1), (1, 1), (64, 64), (65, 64), (500, 64)] {
        let manager = ResourceManager::new(ResourceBudget {
            max_active_agents: Some(configured),
            ..Default::default()
        });
        assert_eq!(manager.effective_agent_limit(), expected);
    }
}

#[test]
fn provider_concurrency_signal_is_only_a_scheduling_hint() {
    let manager = ResourceManager::new(ResourceBudget {
        max_active_agents: Some(500),
        ..Default::default()
    });
    manager.set_provider_signals(ProviderSignals {
        concurrency_hint: Some(32),
        ..Default::default()
    });
    assert_eq!(manager.effective_agent_limit(), 64);
    assert!(manager.scheduling_parallelism_hint() <= 32);
}

#[test]
fn missing_agent_budget_enforces_the_bounded_default_limit() {
    let manager = ResourceManager::new_with_parallelism_hint(ResourceBudget::default(), 32);
    let permits: Vec<_> = (0..8)
        .map(|_| manager.try_acquire_agent(1).expect("bounded default agent permit"))
        .collect();
    assert!(manager.try_acquire_agent(1).is_none());
    assert_eq!(manager.usage().active_agents, 8);
    drop(permits);
}

#[test]
fn permit_releases_active_count_on_drop() {
    let manager = ResourceManager::new(ResourceBudget {
        max_active_agents: Some(1),
        ..Default::default()
    });
    let permit = manager.try_acquire_agent(1).expect("first permit");
    assert!(manager.try_acquire_agent(1).is_none());
    drop(permit);
    assert!(manager.try_acquire_agent(1).is_some());
}

#[test]
fn attach_recovery_uses_the_resource_state_transaction() {
    use crate::runtime_ledger::RuntimeLedger;

    let manager = ResourceManager::new(ResourceBudget::default());
    let ledger = Arc::new(TransactionObservingLedger {
        inner: crate::runtime_ledger::InMemoryRuntimeLedger::default(),
        manager: Arc::downgrade(&manager),
        recovery_held_transaction: std::sync::atomic::AtomicBool::new(false),
    });

    manager
        .attach_ledger(
            RunId::from("attach-resource-transaction"),
            Arc::clone(&ledger) as Arc<dyn RuntimeLedger>,
        )
        .unwrap();

    assert!(
        ledger
            .recovery_held_transaction
            .load(std::sync::atomic::Ordering::SeqCst)
    );
}

#[test]
fn transient_permit_changes_do_not_write_resource_records_but_durable_agent_usage_survives_restart() {
    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};

    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("transient-resource-checkpoints");
    let manager = ResourceManager::new(ResourceBudget::default());
    manager
        .attach_ledger(run_id.clone(), Arc::clone(&ledger) as Arc<dyn RuntimeLedger>)
        .unwrap();

    let resource_record_count = || ledger.records_for_run(&run_id).unwrap().len();
    assert_eq!(resource_record_count(), 1);

    let reattached = manager
        .try_acquire_reattached_agent(1)
        .expect("reattached agent permit");
    let effect = manager.try_acquire_effect().expect("effect permit");
    assert_eq!(manager.usage().active_agents, 1);
    assert_eq!(manager.usage().concurrent_effects, 1);
    assert_eq!(resource_record_count(), 1);

    drop(reattached);
    drop(effect);
    assert_eq!(manager.usage().active_agents, 0);
    assert_eq!(manager.usage().concurrent_effects, 0);
    assert_eq!(resource_record_count(), 1);

    let new_agent = manager.try_acquire_agent(1).expect("new agent permit");
    assert_eq!(resource_record_count(), 2);
    let records = ledger.records_for_run(&run_id).unwrap();
    let durable_agent_delta = records.last().unwrap();
    assert_eq!(durable_agent_delta.record_type, "resource_usage_delta");
    assert_eq!(durable_agent_delta.payload["usage"]["active_agents"], 1);
    assert_eq!(durable_agent_delta.payload["usage"]["concurrent_effects"], 0);
    assert_eq!(durable_agent_delta.payload["usage"]["total_descendants"], 1);

    drop(new_agent);
    assert_eq!(manager.usage().active_agents, 0);
    assert_eq!(resource_record_count(), 2);

    let restored = ResourceManager::new(ResourceBudget::default());
    restored
        .attach_ledger(run_id.clone(), Arc::clone(&ledger) as Arc<dyn RuntimeLedger>)
        .unwrap();
    assert_eq!(restored.usage().active_agents, 0);
    assert_eq!(restored.usage().concurrent_effects, 0);
    assert_eq!(restored.usage().total_descendants, 1);
    assert_eq!(resource_record_count(), 2);
    assert_eq!(
        ledger.records_for_run(&run_id).unwrap().last().unwrap().record_type,
        "resource_usage_delta"
    );
}

#[test]
fn failed_agent_checkpoint_returns_without_releasing_an_existing_agent() {
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    use crate::runtime_ledger::RuntimeLedger;

    const CHILD_ENV: &str = "SOLARIS_FAILED_AGENT_CHECKPOINT_CHILD";
    const CHILD_MARKER_ENV: &str = "SOLARIS_FAILED_AGENT_CHECKPOINT_MARKER";
    const TEST_NAME: &str =
        "resource_manager::resource_manager_test::failed_agent_checkpoint_returns_without_releasing_an_existing_agent";

    if std::env::var_os(CHILD_ENV).is_some() {
        std::fs::write(
            std::env::var_os(CHILD_MARKER_ENV).expect("child marker path"),
            b"entered",
        )
        .unwrap();
        let ledger = Arc::new(FailingCheckpointLedger::default());
        let manager = ResourceManager::new(ResourceBudget::default());
        manager
            .attach_ledger(
                RunId::from("failed-agent-checkpoint"),
                Arc::clone(&ledger) as Arc<dyn RuntimeLedger>,
            )
            .unwrap();
        let first = manager.try_acquire_agent(1).expect("first agent permit");
        ledger.fail.store(true, std::sync::atomic::Ordering::SeqCst);

        assert!(manager.try_acquire_agent(1).is_none());
        assert_eq!(manager.usage().active_agents, 1);
        assert_eq!(manager.usage().total_descendants, 1);
        drop(first);
        return;
    }

    let child_temp = tempfile::tempdir().unwrap();
    let child_marker = child_temp.path().join("entered");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(TEST_NAME)
        .arg("--nocapture")
        .env(CHILD_ENV, "1")
        .env(CHILD_MARKER_ENV, &child_marker)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "checkpoint failure child test failed: {status}");
            assert!(child_marker.is_file(), "checkpoint failure child test did not run");
            break;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("agent admission did not return after checkpoint failure");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[tokio::test]
async fn persistence_failure_wakes_agent_and_effect_waiters() {
    use std::future::Future;
    use std::task::Poll;
    use std::time::Duration;

    use crate::runtime_ledger::RuntimeLedger;

    let ledger = Arc::new(FailingCheckpointLedger::default());
    let manager = ResourceManager::new(ResourceBudget {
        max_active_agents: Some(1),
        max_concurrent_effects: Some(1),
        ..Default::default()
    });
    manager.set_provider_signals(ProviderSignals {
        requests_per_minute: Some(1),
        tokens_per_minute: Some(100),
        ..Default::default()
    });
    manager
        .attach_ledger(
            RunId::from("persistence-error-wakes-agent"),
            Arc::clone(&ledger) as Arc<dyn RuntimeLedger>,
        )
        .unwrap();
    let first_agent = manager.try_acquire_agent(1).expect("first agent permit");
    let first_effect = manager.try_acquire_effect().expect("first effect permit");

    let (agent_ready_tx, agent_ready_rx) = tokio::sync::oneshot::channel();
    let mut agent_waiter = tokio::spawn({
        let manager = Arc::clone(&manager);
        async move {
            let mut acquiring = Box::pin(manager.acquire_agent(1));
            std::future::poll_fn(|context| match acquiring.as_mut().poll(context) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(_) => panic!("agent acquisition did not wait"),
            })
            .await;
            agent_ready_tx.send(()).unwrap();
            acquiring.await
        }
    });
    let (effect_ready_tx, effect_ready_rx) = tokio::sync::oneshot::channel();
    let mut effect_waiter = tokio::spawn({
        let manager = Arc::clone(&manager);
        async move {
            let mut acquiring = Box::pin(manager.acquire_effect());
            std::future::poll_fn(|context| match acquiring.as_mut().poll(context) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(_) => panic!("effect acquisition did not wait"),
            })
            .await;
            effect_ready_tx.send(()).unwrap();
            acquiring.await
        }
    });
    agent_ready_rx.await.unwrap();
    effect_ready_rx.await.unwrap();

    ledger.fail.store(true, std::sync::atomic::Ordering::SeqCst);
    let error = match manager.acquire_provider_request(40) {
        Ok(_) => panic!("provider request must fail when checkpoint persistence fails"),
        Err(error) => error,
    };
    assert!(error.contains("persistence failed"));

    let agent_result = match tokio::time::timeout(Duration::from_secs(1), &mut agent_waiter).await {
        Ok(result) => result.unwrap(),
        Err(error) => {
            agent_waiter.abort();
            effect_waiter.abort();
            let _ = agent_waiter.await;
            let _ = effect_waiter.await;
            panic!("waiting agent did not return after persistence failure: {error}");
        }
    };
    let effect_result = match tokio::time::timeout(Duration::from_secs(1), &mut effect_waiter).await {
        Ok(result) => result.unwrap(),
        Err(error) => {
            effect_waiter.abort();
            let _ = effect_waiter.await;
            panic!("waiting effect did not return after persistence failure: {error}");
        }
    };
    match agent_result {
        Err(error) => assert!(error.contains("persistence failed")),
        Ok(_) => panic!("waiting agent must not be admitted"),
    }
    match effect_result {
        Err(error) => assert!(error.contains("persistence failed")),
        Ok(_) => panic!("waiting effect must not be admitted"),
    }
    assert_eq!(manager.usage().active_agents, 1);
    assert_eq!(manager.usage().concurrent_effects, 1);
    drop(first_effect);
    drop(first_agent);
}

#[tokio::test]
async fn waiting_agent_acquires_after_release() {
    let manager = ResourceManager::new(ResourceBudget {
        max_active_agents: Some(1),
        ..Default::default()
    });
    let first = manager.try_acquire_agent(1).unwrap();
    let waiting = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.acquire_agent(1).await.unwrap() })
    };
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    assert!(!waiting.is_finished());
    drop(first);
    let second = tokio::time::timeout(std::time::Duration::from_millis(250), waiting)
        .await
        .expect("waiting agent should wake")
        .unwrap();
    drop(second);
    assert_eq!(manager.usage().active_agents, 0);
}

#[tokio::test]
async fn effect_limit_waits_and_releases() {
    let manager = ResourceManager::new(ResourceBudget {
        max_concurrent_effects: Some(1),
        ..Default::default()
    });
    let first = manager.try_acquire_effect().unwrap();
    let waiting = {
        let manager = manager.clone();
        tokio::spawn(async move { manager.acquire_effect().await.unwrap() })
    };
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    assert!(!waiting.is_finished());
    drop(first);
    let second = tokio::time::timeout(std::time::Duration::from_millis(250), waiting)
        .await
        .expect("waiting effect should wake")
        .unwrap();
    drop(second);
    assert_eq!(manager.usage().concurrent_effects, 0);
}

#[tokio::test(start_paused = true)]
async fn wall_time_deadline_wakes_all_admission_waiters() {
    use std::future::Future;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::task::Poll;
    use std::time::Duration;

    let now = Arc::new(AtomicI64::new(1_000));
    let clock: Arc<dyn Fn() -> i64 + Send + Sync> = {
        let now = Arc::clone(&now);
        Arc::new(move || now.load(Ordering::SeqCst))
    };
    let manager = ResourceManager::new_with_clock(
        ResourceBudget {
            max_active_agents: Some(1),
            max_concurrent_effects: Some(1),
            max_wall_time_ms: Some(100),
            ..Default::default()
        },
        clock,
    );
    let first_agent = manager.try_acquire_agent(1).expect("first agent permit");
    let first_effect = manager.try_acquire_effect().expect("first effect permit");

    let (agent_ready_tx, agent_ready_rx) = tokio::sync::oneshot::channel();
    let agent_waiter = tokio::spawn({
        let manager = Arc::clone(&manager);
        async move {
            let mut acquiring = Box::pin(manager.acquire_agent(1));
            std::future::poll_fn(|context| match acquiring.as_mut().poll(context) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(_) => panic!("agent acquisition did not wait"),
            })
            .await;
            agent_ready_tx.send(()).unwrap();
            acquiring.await.map(drop)
        }
    });
    let (reattached_ready_tx, reattached_ready_rx) = tokio::sync::oneshot::channel();
    let reattached_waiter = tokio::spawn({
        let manager = Arc::clone(&manager);
        async move {
            let mut acquiring = Box::pin(manager.acquire_reattached_agent(1));
            std::future::poll_fn(|context| match acquiring.as_mut().poll(context) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(_) => panic!("reattached agent acquisition did not wait"),
            })
            .await;
            reattached_ready_tx.send(()).unwrap();
            acquiring.await.map(drop)
        }
    });
    let (effect_ready_tx, effect_ready_rx) = tokio::sync::oneshot::channel();
    let effect_waiter = tokio::spawn({
        let manager = Arc::clone(&manager);
        async move {
            let mut acquiring = Box::pin(manager.acquire_effect());
            std::future::poll_fn(|context| match acquiring.as_mut().poll(context) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(_) => panic!("effect acquisition did not wait"),
            })
            .await;
            effect_ready_tx.send(()).unwrap();
            acquiring.await.map(drop)
        }
    });
    agent_ready_rx.await.unwrap();
    reattached_ready_rx.await.unwrap();
    effect_ready_rx.await.unwrap();

    now.store(1_100, Ordering::SeqCst);
    tokio::time::advance(Duration::from_millis(100)).await;
    tokio::task::yield_now().await;

    if !agent_waiter.is_finished() || !reattached_waiter.is_finished() || !effect_waiter.is_finished() {
        agent_waiter.abort();
        reattached_waiter.abort();
        effect_waiter.abort();
        let _ = agent_waiter.await;
        let _ = reattached_waiter.await;
        let _ = effect_waiter.await;
        panic!("wall-time deadline did not wake every admission waiter");
    }

    for result in [
        agent_waiter.await.unwrap(),
        reattached_waiter.await.unwrap(),
        effect_waiter.await.unwrap(),
    ] {
        match result {
            Err(error) => assert!(error.contains("wall-time"), "unexpected error: {error}"),
            Ok(()) => panic!("expired wall-time budget must not admit a waiter"),
        }
    }
    assert_eq!(manager.usage().active_agents, 1);
    assert_eq!(manager.usage().concurrent_effects, 1);
    drop(first_effect);
    drop(first_agent);
}

#[test]
fn turn_token_and_cost_budgets_allow_limit_then_block_next_operation() {
    let manager = ResourceManager::new(ResourceBudget {
        max_turns: Some(1),
        max_tokens: Some(300),
        max_cost: Some(0.0003),
        ..Default::default()
    });
    manager.set_provider_signals(ProviderSignals {
        input_cost_per_million: Some(1.0),
        output_cost_per_million: Some(1.0),
        ..Default::default()
    });
    assert!(manager.record_model_usage(&TokenUsage {
        input_tokens: 200,
        output_tokens: 100,
        ..Default::default()
    }));
    let usage = manager.usage();
    assert_eq!(usage.turns, 1);
    assert_eq!(usage.tokens, 300);
    assert!((usage.cost - 0.0003).abs() < 1e-12);
    assert!(manager.hard_runtime_budget_reason().is_some());
}

#[tokio::test]
async fn wall_time_budget_blocks_future_operations() {
    let manager = ResourceManager::new(ResourceBudget {
        max_wall_time_ms: Some(5),
        ..Default::default()
    });
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    assert!(manager.hard_runtime_budget_reason().unwrap().contains("wall-time"));
    match manager.acquire_effect().await {
        Ok(_) => panic!("wall-time budget should reject future effects"),
        Err(error) => assert!(error.contains("wall-time")),
    }
}

#[test]
fn changing_wall_time_budget_recomputes_or_clears_the_deadline_immediately() {
    use std::sync::atomic::{AtomicI64, Ordering};

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

    now.store(1_200, Ordering::SeqCst);
    manager
        .set_budget(ResourceBudget {
            max_wall_time_ms: Some(100),
            ..Default::default()
        })
        .unwrap();
    now.store(1_299, Ordering::SeqCst);
    assert!(manager.hard_runtime_budget_reason().is_none());
    now.store(1_300, Ordering::SeqCst);
    assert!(manager.hard_runtime_budget_reason().unwrap().contains("wall-time"));

    manager
        .set_budget(ResourceBudget {
            max_wall_time_ms: Some(250),
            ..Default::default()
        })
        .unwrap();
    now.store(1_549, Ordering::SeqCst);
    assert!(manager.hard_runtime_budget_reason().is_none());
    now.store(1_550, Ordering::SeqCst);
    assert!(manager.hard_runtime_budget_reason().unwrap().contains("wall-time"));

    manager.set_budget(ResourceBudget::default()).unwrap();
    assert!(manager.hard_runtime_budget_reason().is_none());
}

#[test]
fn descendant_budget_is_run_wide_even_after_permit_release() {
    let manager = ResourceManager::new(ResourceBudget {
        max_total_descendants_per_run: Some(1),
        ..Default::default()
    });
    let first = manager.try_acquire_agent(1).unwrap();
    drop(first);
    assert!(manager.try_acquire_agent(1).is_none());
}

#[test]
fn cumulative_budget_and_deadline_survive_manager_restart() {
    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};
    use std::sync::atomic::{AtomicI64, Ordering};

    let now = Arc::new(AtomicI64::new(1_000));
    let clock: Arc<dyn Fn() -> i64 + Send + Sync> = {
        let now = Arc::clone(&now);
        Arc::new(move || now.load(Ordering::SeqCst))
    };
    let budget = ResourceBudget {
        max_total_descendants_per_run: Some(1),
        max_turns: Some(2),
        max_wall_time_ms: Some(500),
        ..Default::default()
    };
    let ledger: Arc<dyn RuntimeLedger> = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("resource-run");
    let manager = ResourceManager::new_with_clock(budget.clone(), Arc::clone(&clock));
    manager.attach_ledger(run_id.clone(), Arc::clone(&ledger)).unwrap();
    drop(manager.try_acquire_agent(1).unwrap());
    assert!(manager.record_model_usage(&TokenUsage::default()));

    now.store(1_200, Ordering::SeqCst);
    let restored = ResourceManager::new_with_clock(budget, clock);
    restored.attach_ledger(run_id, ledger).unwrap();
    assert_eq!(restored.usage().total_descendants, 1);
    assert_eq!(restored.usage().turns, 1);
    assert!(restored.try_acquire_agent(1).is_none());
    now.store(1_501, Ordering::SeqCst);
    let reason = restored.hard_runtime_budget_reason().unwrap();
    assert!(reason.contains("wall-time"), "unexpected budget reason: {reason}");
}

#[test]
fn restart_restores_original_budget_and_provider_signals_instead_of_relaxed_config() {
    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};

    let ledger: Arc<dyn RuntimeLedger> = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("resource-policy-restore");
    let strict_budget = ResourceBudget {
        max_total_descendants_per_run: Some(1),
        ..Default::default()
    };
    let strict_signals = ProviderSignals {
        requests_per_minute: Some(1),
        tokens_per_minute: Some(10),
        ..Default::default()
    };
    let first = ResourceManager::new(strict_budget.clone());
    first.set_provider_signals(strict_signals.clone());
    first.attach_ledger(run_id.clone(), Arc::clone(&ledger)).unwrap();

    let restored = ResourceManager::new(ResourceBudget::default());
    restored.attach_ledger(run_id, ledger).unwrap();

    assert_eq!(restored.budget(), strict_budget);
    assert_eq!(restored.provider_signals(), strict_signals);
}

#[test]
fn provider_rpm_and_tpm_signals_gate_admission_and_reset_after_a_minute() {
    use std::sync::atomic::{AtomicI64, Ordering};

    let now = Arc::new(AtomicI64::new(10_000));
    let clock: Arc<dyn Fn() -> i64 + Send + Sync> = {
        let now = Arc::clone(&now);
        Arc::new(move || now.load(Ordering::SeqCst))
    };
    let manager = ResourceManager::new_with_clock(ResourceBudget::default(), clock);
    manager.set_provider_signals(ProviderSignals {
        requests_per_minute: Some(2),
        tokens_per_minute: Some(100),
        ..Default::default()
    });

    manager.acquire_provider_request(60).unwrap().commit(60).unwrap();
    assert!(
        manager
            .acquire_provider_request(50)
            .err()
            .unwrap()
            .contains("token rate")
    );
    manager.acquire_provider_request(40).unwrap().commit(40).unwrap();
    assert!(
        manager
            .acquire_provider_request(0)
            .err()
            .unwrap()
            .contains("request rate")
    );

    now.store(70_001, Ordering::SeqCst);
    manager.acquire_provider_request(100).unwrap().commit(100).unwrap();
}

#[test]
fn unstarted_provider_reservation_does_not_consume_rpm() {
    let manager = ResourceManager::new(ResourceBudget::default());
    manager.set_provider_signals(ProviderSignals {
        requests_per_minute: Some(1),
        tokens_per_minute: Some(100),
        ..Default::default()
    });

    let reservation = manager.acquire_provider_request(40).unwrap();
    drop(reservation);

    let mut started = manager.acquire_provider_request(40).unwrap();
    started.mark_started().unwrap();
    assert!(manager.acquire_provider_request(1).is_err());
    drop(started);
    assert!(manager.acquire_provider_request(1).is_err());
}

#[test]
fn provider_reservation_is_readmitted_when_its_window_expires() {
    use std::sync::atomic::{AtomicI64, Ordering};

    let now = Arc::new(AtomicI64::new(10_000));
    let clock: Arc<dyn Fn() -> i64 + Send + Sync> = {
        let now = Arc::clone(&now);
        Arc::new(move || now.load(Ordering::SeqCst))
    };
    let manager = ResourceManager::new_with_clock(ResourceBudget::default(), clock);
    manager.set_provider_signals(ProviderSignals {
        requests_per_minute: Some(1),
        tokens_per_minute: Some(100),
        ..Default::default()
    });

    let mut old_reservation = manager.acquire_provider_request(40).unwrap();
    now.store(70_001, Ordering::SeqCst);
    old_reservation.mark_started().unwrap();
    assert!(manager.acquire_provider_request(1).is_err());
}

#[test]
fn expired_reservation_cannot_consume_another_permits_new_window_slot() {
    use std::sync::atomic::{AtomicI64, Ordering};

    let now = Arc::new(AtomicI64::new(10_000));
    let clock: Arc<dyn Fn() -> i64 + Send + Sync> = {
        let now = Arc::clone(&now);
        Arc::new(move || now.load(Ordering::SeqCst))
    };
    let manager = ResourceManager::new_with_clock(ResourceBudget::default(), clock);
    manager.set_provider_signals(ProviderSignals {
        requests_per_minute: Some(1),
        tokens_per_minute: Some(100),
        ..Default::default()
    });

    let mut old_reservation = manager.acquire_provider_request(60).unwrap();
    now.store(70_001, Ordering::SeqCst);
    let mut new_reservation = manager.acquire_provider_request(30).unwrap();
    assert!(old_reservation.mark_started().is_err());
    drop(old_reservation);
    new_reservation.mark_started().unwrap();
    assert!(manager.acquire_provider_request(1).is_err());
}

#[test]
fn provider_rate_window_survives_restart_and_expires_after_a_minute() {
    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};
    use std::sync::atomic::{AtomicI64, Ordering};

    let now = Arc::new(AtomicI64::new(10_000));
    let clock: Arc<dyn Fn() -> i64 + Send + Sync> = {
        let now = Arc::clone(&now);
        Arc::new(move || now.load(Ordering::SeqCst))
    };
    let ledger: Arc<dyn RuntimeLedger> = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("provider-rate-restart");
    let first = ResourceManager::new_with_clock(ResourceBudget::default(), Arc::clone(&clock));
    first.set_provider_signals(ProviderSignals {
        requests_per_minute: Some(1),
        tokens_per_minute: Some(100),
        ..Default::default()
    });
    first.attach_ledger(run_id.clone(), Arc::clone(&ledger)).unwrap();
    let mut reserved = first.acquire_provider_request(80).unwrap();
    reserved.mark_started().unwrap();
    std::mem::forget(reserved);

    let restored = ResourceManager::new_with_clock(ResourceBudget::default(), Arc::clone(&clock));
    restored.set_provider_signals(ProviderSignals {
        requests_per_minute: Some(1),
        tokens_per_minute: Some(100),
        ..Default::default()
    });
    restored.attach_ledger(run_id.clone(), Arc::clone(&ledger)).unwrap();
    assert!(
        restored
            .acquire_provider_request(1)
            .err()
            .unwrap()
            .contains("request rate")
    );

    now.store(70_001, Ordering::SeqCst);
    let expired = ResourceManager::new_with_clock(ResourceBudget::default(), clock);
    expired.set_provider_signals(ProviderSignals {
        requests_per_minute: Some(1),
        tokens_per_minute: Some(100),
        ..Default::default()
    });
    expired.attach_ledger(run_id, ledger).unwrap();
    expired.acquire_provider_request(100).unwrap().commit(100).unwrap();
}

#[test]
fn unstarted_provider_reservation_is_cleared_when_restored() {
    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};

    let ledger: Arc<dyn RuntimeLedger> = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("provider-unstarted-restart");
    let first = ResourceManager::new(ResourceBudget::default());
    first.set_provider_signals(ProviderSignals {
        requests_per_minute: Some(1),
        tokens_per_minute: Some(100),
        ..Default::default()
    });
    first.attach_ledger(run_id.clone(), Arc::clone(&ledger)).unwrap();
    let reservation = first.acquire_provider_request(80).unwrap();
    std::mem::forget(reservation);

    let restored = ResourceManager::new(ResourceBudget::default());
    restored.attach_ledger(run_id, ledger).unwrap();
    restored.acquire_provider_request(100).unwrap().commit(100).unwrap();
}

#[test]
fn provider_and_compaction_usage_are_applied_once_across_restart() {
    use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};

    let ledger: Arc<dyn RuntimeLedger> = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("usage-exactly-once");
    let first = ResourceManager::new(ResourceBudget::default());
    first.set_provider_signals(ProviderSignals {
        input_cost_per_million: Some(2.0),
        cache_read_cost_per_million: Some(2.0),
        cache_write_cost_per_million: Some(2.0),
        output_cost_per_million: Some(3.0),
        ..Default::default()
    });
    first.attach_ledger(run_id.clone(), Arc::clone(&ledger)).unwrap();
    let provider_usage = TokenUsage {
        input_tokens: 20,
        output_tokens: 5,
        cache_creation_tokens: 7,
        cache_read_tokens: 8,
    };
    let compaction_usage = TokenUsage {
        input_tokens: 10,
        output_tokens: 2,
        cache_creation_tokens: 4,
        cache_read_tokens: 6,
    };

    assert!(first.record_model_usage_once("provider-effect", &provider_usage, true));
    assert!(first.record_model_usage_once("provider-effect", &provider_usage, true));
    assert!(first.record_model_usage_once("compact-effect", &compaction_usage, false));
    let first_usage = first.usage();
    assert_eq!(first_usage.turns, 1);
    assert_eq!(first_usage.tokens, 37);
    assert_eq!(first_usage.uncached_input_tokens, 5);
    assert_eq!(first_usage.input_tokens, 30);
    assert_eq!(first_usage.output_tokens, 7);
    assert_eq!(first_usage.cache_creation_tokens, 11);
    assert_eq!(first_usage.cache_read_tokens, 14);
    assert!((first_usage.cost - 0.000_081).abs() < 1e-12);

    let restored = ResourceManager::new(ResourceBudget::default());
    restored.attach_ledger(run_id, ledger).unwrap();
    assert!(restored.record_model_usage_once("provider-effect", &provider_usage, true));
    assert!(restored.record_model_usage_once("compact-effect", &compaction_usage, false));
    let restored_usage = restored.usage();
    assert_eq!(restored_usage.turns, 1);
    assert_eq!(restored_usage.tokens, 37);
    assert_eq!(restored_usage.uncached_input_tokens, 5);
    assert_eq!(restored_usage.input_tokens, 30);
    assert_eq!(restored_usage.output_tokens, 7);
    assert_eq!(restored_usage.cache_creation_tokens, 11);
    assert_eq!(restored_usage.cache_read_tokens, 14);
    assert!((restored_usage.cost - 0.000_081).abs() < 1e-12);
}

#[test]
fn persisted_resource_changes_are_published_as_live_events() {
    use crate::collaboration_runtime::CollaborationRuntime;
    use crate::resource_policy::ResourcePolicy;
    use crate::scheduler::Scheduler;

    let runtime = Arc::new(CollaborationRuntime::new(Scheduler::new(ResourcePolicy::new(1))));
    let mut receiver = runtime.subscribe_live_events();
    let manager = ResourceManager::new(ResourceBudget::default());
    manager
        .attach_runtime(RunId::from("resource-live-events"), runtime)
        .unwrap();
    manager.record_model_usage_once(
        "provider-effect",
        &TokenUsage {
            input_tokens: 7,
            output_tokens: 3,
            ..Default::default()
        },
        true,
    );

    let events = std::iter::from_fn(|| receiver.try_recv().ok()).collect::<Vec<_>>();
    let event = events.last().expect("resource persistence live event");
    assert_eq!(event.kind, "resource_usage_delta");
    assert_eq!(event.journal_sequence, Some(2));
    assert_eq!(event.payload["usage"]["tokens"], 10);
    assert_eq!(event.payload["usage"]["turns"], 1);
}

#[test]
fn checkpoint_failure_rolls_back_and_rejects_current_resource_admission() {
    use crate::runtime_ledger::RuntimeLedger;

    let ledger = Arc::new(FailingCheckpointLedger::default());
    let run_id = RunId::from("resource-fail-closed");
    let manager = ResourceManager::new(ResourceBudget::default());
    manager.set_provider_signals(ProviderSignals {
        requests_per_minute: Some(1),
        tokens_per_minute: Some(100),
        ..Default::default()
    });
    manager
        .attach_ledger(run_id.clone(), Arc::clone(&ledger) as Arc<dyn RuntimeLedger>)
        .unwrap();
    ledger.fail.store(true, std::sync::atomic::Ordering::SeqCst);

    let error = match manager.acquire_provider_request(40) {
        Ok(_) => panic!("provider request must fail when checkpoint persistence fails"),
        Err(error) => error,
    };
    assert!(error.contains("persistence failed"));
    assert!(manager.try_acquire_agent(1).is_none());
    assert!(!manager.record_model_usage_once(
        "failed-usage",
        &TokenUsage {
            input_tokens: 10,
            output_tokens: 5,
            ..Default::default()
        },
        true,
    ));
    assert_eq!(manager.usage(), ResourceUsage::default());

    ledger.fail.store(false, std::sync::atomic::Ordering::SeqCst);
    let restored = ResourceManager::new(ResourceBudget::default());
    restored.set_provider_signals(ProviderSignals {
        requests_per_minute: Some(1),
        tokens_per_minute: Some(100),
        ..Default::default()
    });
    restored
        .attach_ledger(run_id, ledger as Arc<dyn RuntimeLedger>)
        .unwrap();
    assert_eq!(restored.usage(), ResourceUsage::default());
    restored.acquire_provider_request(100).unwrap().commit(100).unwrap();
}

#[test]
fn budget_persistence_failure_is_reported_and_rolls_back_the_limit() {
    use crate::runtime_ledger::RuntimeLedger;

    let ledger = Arc::new(FailingCheckpointLedger::default());
    let manager = ResourceManager::new(ResourceBudget::default());
    manager
        .attach_ledger(
            RunId::from("budget-update-failure"),
            Arc::clone(&ledger) as Arc<dyn RuntimeLedger>,
        )
        .unwrap();
    ledger.fail.store(true, std::sync::atomic::Ordering::SeqCst);

    let error = manager
        .set_budget(ResourceBudget {
            max_active_agents: Some(4),
            ..Default::default()
        })
        .unwrap_err();

    assert!(error.contains("persistence failed"));
    assert_eq!(manager.budget().max_active_agents, None);
}
