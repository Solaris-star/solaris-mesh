use std::sync::Arc;
use std::time::Duration;

use solaris_types::resource::ResourceBudget;

use super::*;

#[tokio::test]
async fn agent_release_between_failed_check_and_wait_registration_is_not_lost() {
    let manager = ResourceManager::new(ResourceBudget {
        max_active_agents: Some(1),
        ..Default::default()
    });
    let first = manager.try_acquire_agent(1).unwrap();
    let (reached, resume) = manager.install_wait_transition_hook();
    let mut waiting = tokio::spawn({
        let manager = Arc::clone(&manager);
        async move { manager.acquire_agent(1).await }
    });

    tokio::time::timeout(Duration::from_secs(1), reached.wait())
        .await
        .expect("agent waiter must reach the failed-check transition");
    drop(first);
    resume.wait().await;

    let second = tokio::time::timeout(Duration::from_millis(250), &mut waiting)
        .await
        .expect("agent release notification must not be lost")
        .unwrap()
        .unwrap();
    drop(second);
}

#[tokio::test]
async fn reattached_release_between_failed_check_and_wait_registration_is_not_lost() {
    let manager = ResourceManager::new(ResourceBudget {
        max_active_agents: Some(1),
        ..Default::default()
    });
    let first = manager.try_acquire_agent(1).unwrap();
    let (reached, resume) = manager.install_wait_transition_hook();
    let mut waiting = tokio::spawn({
        let manager = Arc::clone(&manager);
        async move { manager.acquire_reattached_agent(1).await }
    });

    tokio::time::timeout(Duration::from_secs(1), reached.wait())
        .await
        .expect("reattached waiter must reach the failed-check transition");
    drop(first);
    resume.wait().await;

    let second = tokio::time::timeout(Duration::from_millis(250), &mut waiting)
        .await
        .expect("reattached release notification must not be lost")
        .unwrap()
        .unwrap();
    drop(second);
}

#[tokio::test]
async fn effect_release_between_failed_check_and_wait_registration_is_not_lost() {
    let manager = ResourceManager::new(ResourceBudget {
        max_concurrent_effects: Some(1),
        ..Default::default()
    });
    let first = manager.try_acquire_effect().unwrap();
    let (reached, resume) = manager.install_wait_transition_hook();
    let mut waiting = tokio::spawn({
        let manager = Arc::clone(&manager);
        async move { manager.acquire_effect().await }
    });

    tokio::time::timeout(Duration::from_secs(1), reached.wait())
        .await
        .expect("effect waiter must reach the failed-check transition");
    drop(first);
    resume.wait().await;

    let second = tokio::time::timeout(Duration::from_millis(250), &mut waiting)
        .await
        .expect("effect release notification must not be lost")
        .unwrap()
        .unwrap();
    drop(second);
}

#[test]
fn reattached_agent_ignores_descendant_budget_without_incrementing_it() {
    let manager = ResourceManager::new(ResourceBudget {
        max_active_agents: Some(2),
        max_total_descendants_per_run: Some(1),
        ..Default::default()
    });
    drop(manager.try_acquire_agent(1).unwrap());
    assert_eq!(manager.usage().total_descendants, 1);

    let reattached = manager
        .try_acquire_reattached_agent(1)
        .expect("existing Agent may reattach after the descendant budget is exhausted");
    assert!(manager.try_acquire_agent(1).is_none());
    assert_eq!(manager.usage().total_descendants, 1);
    drop(reattached);
    assert_eq!(manager.usage().total_descendants, 1);
}

#[test]
fn reattached_agent_still_checks_depth_active_and_runtime_budgets() {
    let depth_limited = ResourceManager::new(ResourceBudget {
        max_spawn_depth: Some(0),
        ..Default::default()
    });
    assert!(depth_limited.try_acquire_reattached_agent(1).is_none());

    let active_limited = ResourceManager::new(ResourceBudget {
        max_active_agents: Some(1),
        ..Default::default()
    });
    let active = active_limited.try_acquire_agent(1).unwrap();
    assert!(active_limited.try_acquire_reattached_agent(1).is_none());
    drop(active);

    let runtime_limited = ResourceManager::new(ResourceBudget {
        max_turns: Some(0),
        ..Default::default()
    });
    assert!(runtime_limited.try_acquire_reattached_agent(0).is_none());
}

#[tokio::test]
async fn descendant_counter_overflow_is_rejected_without_mutating_usage() {
    let manager = ResourceManager::new(ResourceBudget::default());
    manager
        .usage
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .total_descendants = usize::MAX;

    assert!(manager.try_acquire_agent(1).is_none());
    assert_eq!(manager.usage().total_descendants, usize::MAX);
    let error = match manager.acquire_agent(1).await {
        Ok(_) => panic!("descendant counter overflow must reject admission"),
        Err(error) => error,
    };
    assert!(error.contains("descendant counter"), "{error}");
    assert_eq!(manager.usage().total_descendants, usize::MAX);
}
