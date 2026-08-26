use std::collections::HashSet;
use std::future::Future;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use solaris_types::identity::RunId;
use solaris_types::message::TokenUsage;
use solaris_types::provider_contract::ProviderSignals;
use solaris_types::resource::{MAX_ACTIVE_AGENTS, MIN_ACTIVE_AGENTS, ResourceBudget};
use solaris_types::tool::{ToolResultStatus, useful_call_rate};
#[cfg(test)]
use tokio::sync::Barrier;
use tokio::sync::Notify;

use crate::collaboration_runtime::CollaborationRuntime;
use crate::runtime_ledger::RuntimeLedger;

const DEFAULT_MAX_ACTIVE_AGENTS: usize = 8;
const MIN_DEFAULT_ACTIVE_AGENTS: usize = 2;

#[path = "resource_manager/persistence.rs"]
mod persistence;
#[path = "resource_manager/usage.rs"]
mod usage;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ResourceUsage {
    pub active_agents: usize,
    pub concurrent_effects: usize,
    pub total_descendants: usize,
    pub turns: usize,
    pub tokens: u64,
    #[serde(default)]
    pub uncached_input_tokens: u64,
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
    #[serde(default)]
    pub cache_creation_tokens: u64,
    #[serde(default)]
    pub cache_read_tokens: u64,
    /// Number of terminal model-visible tool calls observed for this Run.
    #[serde(default)]
    pub tool_calls: u64,
    /// Tool calls that executed or returned a valid cached result.
    #[serde(default)]
    pub useful_tool_calls: u64,
    /// Calls whose canonical tool identity was already observed in the same scope.
    #[serde(default)]
    pub duplicate_tool_calls: u64,
    /// Canonical tool-call identities retained for checkpoint and cold restore.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub seen_tool_call_fingerprints: Vec<String>,
    /// `useful_tool_calls / tool_calls`, or `None` before the first call.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub useful_call_rate: Option<f64>,
    /// `duplicate_tool_calls / tool_calls`, or `None` before the first call.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duplicate_call_rate: Option<f64>,
    pub cost: f64,
    /// Whether `cost` represents all recorded provider usage.
    ///
    /// Legacy checkpoints omitted this field and are treated as known because
    /// they were written only after the runtime had already chosen a numeric
    /// cost representation.
    #[serde(default = "default_cost_known")]
    pub cost_known: bool,
}

const fn default_cost_known() -> bool {
    true
}

#[derive(Clone)]
struct ResourcePersistence {
    run_id: RunId,
    ledger: Arc<dyn RuntimeLedger>,
    runtime: Option<Arc<CollaborationRuntime<()>>>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
struct ProviderRateWindow {
    started_at_unix_ms: i64,
    requests: u64,
    reserved_requests: u64,
    tokens: u64,
    reserved_tokens: u64,
    unstarted_reserved_tokens: u64,
}

#[cfg(test)]
#[derive(Clone)]
struct WaitTransitionHook {
    reached: Arc<Barrier>,
    resume: Arc<Barrier>,
}

pub struct ResourceManager {
    budget: RwLock<ResourceBudget>,
    provider_signals: RwLock<ProviderSignals>,
    usage: Mutex<ResourceUsage>,
    system_parallelism_hint: usize,
    started_at: Instant,
    deadline_unix_ms: RwLock<Option<i64>>,
    persistence: RwLock<Option<ResourcePersistence>>,
    persistence_error: Mutex<Option<String>>,
    checkpoint_transaction: Mutex<()>,
    resource_deltas_since_snapshot: Mutex<u64>,
    provider_rate: Mutex<ProviderRateWindow>,
    applied_usage_effects: Mutex<HashSet<String>>,
    clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    changed: Notify,
    #[cfg(test)]
    wait_transition_hook: Mutex<Option<WaitTransitionHook>>,
}

impl ResourceManager {
    pub fn new(budget: ResourceBudget) -> Arc<Self> {
        Self::new_with_clock(budget, Arc::new(|| chrono::Utc::now().timestamp_millis()))
    }

    fn new_with_clock(budget: ResourceBudget, clock: Arc<dyn Fn() -> i64 + Send + Sync>) -> Arc<Self> {
        let system_parallelism_hint = std::thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or(1);
        Self::new_with_clock_and_parallelism_hint(budget, clock, system_parallelism_hint)
    }

    #[cfg(test)]
    pub(crate) fn new_with_test_clock(budget: ResourceBudget, clock: Arc<dyn Fn() -> i64 + Send + Sync>) -> Arc<Self> {
        Self::new_with_clock(budget, clock)
    }

    #[cfg(test)]
    fn new_with_parallelism_hint(budget: ResourceBudget, system_parallelism_hint: usize) -> Arc<Self> {
        Self::new_with_clock_and_parallelism_hint(
            budget,
            Arc::new(|| chrono::Utc::now().timestamp_millis()),
            system_parallelism_hint,
        )
    }

    fn new_with_clock_and_parallelism_hint(
        budget: ResourceBudget,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
        system_parallelism_hint: usize,
    ) -> Arc<Self> {
        let deadline_unix_ms = budget
            .max_wall_time_ms
            .map(|limit| clock().saturating_add(i64::try_from(limit).unwrap_or(i64::MAX)));
        Arc::new(Self {
            budget: RwLock::new(budget),
            provider_signals: RwLock::new(ProviderSignals::default()),
            usage: Mutex::new(ResourceUsage::default()),
            system_parallelism_hint: system_parallelism_hint.max(1),
            started_at: Instant::now(),
            deadline_unix_ms: RwLock::new(deadline_unix_ms),
            persistence: RwLock::new(None),
            persistence_error: Mutex::new(None),
            checkpoint_transaction: Mutex::new(()),
            resource_deltas_since_snapshot: Mutex::new(0),
            provider_rate: Mutex::new(ProviderRateWindow::default()),
            applied_usage_effects: Mutex::new(HashSet::new()),
            clock,
            changed: Notify::new(),
            #[cfg(test)]
            wait_transition_hook: Mutex::new(None),
        })
    }

    #[cfg(test)]
    fn state_snapshot(&self) -> persistence::ResourceStateSnapshot {
        persistence::state_snapshot(self)
    }

    #[cfg(test)]
    fn install_wait_transition_hook(&self) -> (Arc<Barrier>, Arc<Barrier>) {
        let hook = WaitTransitionHook {
            reached: Arc::new(Barrier::new(2)),
            resume: Arc::new(Barrier::new(2)),
        };
        *self
            .wait_transition_hook
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(hook.clone());
        (hook.reached, hook.resume)
    }

    #[cfg(test)]
    async fn pause_at_wait_transition(&self) {
        let hook = self
            .wait_transition_hook
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(hook) = hook {
            hook.reached.wait().await;
            hook.resume.wait().await;
        }
    }

    pub(crate) fn attach_ledger(&self, run_id: RunId, ledger: Arc<dyn RuntimeLedger>) -> Result<(), String> {
        persistence::attach(self, run_id, ledger, None)
    }

    pub(crate) fn attach_runtime(&self, run_id: RunId, runtime: Arc<CollaborationRuntime<()>>) -> Result<(), String> {
        persistence::attach(self, run_id, runtime.ledger(), Some(runtime))
    }

    pub(crate) fn set_budget(&self, budget: ResourceBudget) -> Result<(), String> {
        self.with_state_update(|| {
            let previous_wall_time = self
                .budget
                .read()
                .unwrap_or_else(|error| error.into_inner())
                .max_wall_time_ms;
            if previous_wall_time != budget.max_wall_time_ms {
                let deadline = budget
                    .max_wall_time_ms
                    .map(|limit| (self.clock)().saturating_add(i64::try_from(limit).unwrap_or(i64::MAX)));
                *self.deadline_unix_ms.write().unwrap_or_else(|error| error.into_inner()) = deadline;
            }
            *self.budget.write().unwrap_or_else(|error| error.into_inner()) = budget;
            ((), true)
        })?;
        self.changed.notify_waiters();
        Ok(())
    }

    pub fn budget(&self) -> ResourceBudget {
        self.budget.read().unwrap_or_else(|error| error.into_inner()).clone()
    }

    pub fn set_provider_signals(&self, signals: ProviderSignals) {
        let _ = self.with_state_update(|| {
            *self.provider_signals.write().unwrap_or_else(|error| error.into_inner()) = signals;
            ((), true)
        });
        self.changed.notify_waiters();
    }

    pub fn provider_signals(&self) -> ProviderSignals {
        self.provider_signals
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    pub fn effective_agent_limit(&self) -> usize {
        let budget = self.budget.read().unwrap_or_else(|error| error.into_inner());
        budget.max_active_agents.map_or_else(
            || {
                self.system_parallelism_hint
                    .clamp(MIN_DEFAULT_ACTIVE_AGENTS, DEFAULT_MAX_ACTIVE_AGENTS)
            },
            |limit| limit.clamp(MIN_ACTIVE_AGENTS, MAX_ACTIVE_AGENTS),
        )
    }

    /// A scheduling hint only. It never denies creation of a logical Agent.
    pub fn scheduling_parallelism_hint(&self) -> usize {
        let provider = self.provider_signals.read().unwrap_or_else(|error| error.into_inner());
        provider
            .concurrency_hint
            .map_or(self.system_parallelism_hint.max(1), |hint| {
                self.system_parallelism_hint.max(1).min(hint.max(1))
            })
    }

    pub fn try_acquire_agent(self: &Arc<Self>, depth: usize) -> Option<AgentResourcePermit> {
        let admitted = self
            .with_state_update(|| {
                if self.hard_agent_block_reason(depth).is_some() {
                    return (false, false);
                }
                let mut usage = self.usage.lock().unwrap_or_else(|error| error.into_inner());
                if usage.active_agents >= self.effective_agent_limit() {
                    return (false, false);
                }
                let Some(total_descendants) = usage.total_descendants.checked_add(1) else {
                    return (false, false);
                };
                usage.active_agents += 1;
                usage.total_descendants = total_descendants;
                (true, true)
            })
            .unwrap_or(false);
        admitted.then(|| AgentResourcePermit {
            manager: Arc::clone(self),
            released: false,
        })
    }

    pub async fn acquire_agent(self: &Arc<Self>, depth: usize) -> Result<AgentResourcePermit, String> {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            let _ = notified.as_mut().enable();
            if let Some(permit) = self.try_acquire_agent(depth) {
                return Ok(permit);
            }
            if let Some(reason) = self.hard_agent_block_reason(depth) {
                return Err(reason);
            }
            #[cfg(test)]
            self.pause_at_wait_transition().await;
            self.wait_for_change_or_deadline(notified).await;
        }
    }

    pub fn try_acquire_reattached_agent(self: &Arc<Self>, depth: usize) -> Option<AgentResourcePermit> {
        self.with_transient_state_update(|| {
            if self.hard_reattached_agent_block_reason(depth).is_some() {
                return None;
            }
            let mut usage = self.usage.lock().unwrap_or_else(|error| error.into_inner());
            if usage.active_agents >= self.effective_agent_limit() {
                return None;
            }
            usage.active_agents += 1;
            Some(AgentResourcePermit {
                manager: Arc::clone(self),
                released: false,
            })
        })
    }

    pub async fn acquire_reattached_agent(self: &Arc<Self>, depth: usize) -> Result<AgentResourcePermit, String> {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            let _ = notified.as_mut().enable();
            if let Some(permit) = self.try_acquire_reattached_agent(depth) {
                return Ok(permit);
            }
            if let Some(reason) = self.hard_reattached_agent_block_reason(depth) {
                return Err(reason);
            }
            #[cfg(test)]
            self.pause_at_wait_transition().await;
            self.wait_for_change_or_deadline(notified).await;
        }
    }

    pub fn try_acquire_effect(self: &Arc<Self>) -> Option<EffectResourcePermit> {
        self.with_transient_state_update(|| {
            if self.hard_runtime_budget_reason().is_some() {
                return None;
            }
            let max_effects = self
                .budget
                .read()
                .unwrap_or_else(|error| error.into_inner())
                .max_concurrent_effects;
            let mut usage = self.usage.lock().unwrap_or_else(|error| error.into_inner());
            if max_effects.is_some_and(|limit| usage.concurrent_effects >= limit.max(1)) {
                return None;
            }
            usage.concurrent_effects += 1;
            Some(EffectResourcePermit {
                manager: Arc::clone(self),
                released: false,
            })
        })
    }

    pub async fn acquire_effect(self: &Arc<Self>) -> Result<EffectResourcePermit, String> {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            let _ = notified.as_mut().enable();
            if let Some(permit) = self.try_acquire_effect() {
                return Ok(permit);
            }
            if let Some(reason) = self.hard_runtime_budget_reason() {
                return Err(reason);
            }
            #[cfg(test)]
            self.pause_at_wait_transition().await;
            self.wait_for_change_or_deadline(notified).await;
        }
    }

    async fn wait_for_change_or_deadline(&self, notified: impl Future<Output = ()>) {
        let deadline = *self.deadline_unix_ms.read().unwrap_or_else(|error| error.into_inner());
        let Some(deadline) = deadline else {
            notified.await;
            return;
        };
        let remaining_ms = deadline.saturating_sub((self.clock)()).max(0) as u64;
        // Bound each timer so an extreme configured deadline cannot overflow the
        // async runtime's monotonic clock. Long waits simply re-check once a day.
        let wait = Duration::from_millis(remaining_ms.min(86_400_000));
        tokio::select! {
            _ = notified => {}
            _ = tokio::time::sleep(wait) => {}
        }
    }

    pub fn acquire_provider_request(self: &Arc<Self>, estimated_tokens: u64) -> Result<ProviderRatePermit, String> {
        let window_started_at_unix_ms = self
            .with_state_update(|| {
                let signals = self.provider_signals();
                let now = (self.clock)();
                let mut window = self.provider_rate.lock().unwrap_or_else(|error| error.into_inner());
                let mut changed = false;
                if window.started_at_unix_ms == 0 || now.saturating_sub(window.started_at_unix_ms) >= 60_000 {
                    *window = ProviderRateWindow {
                        started_at_unix_ms: now,
                        ..Default::default()
                    };
                    changed = true;
                }
                if signals
                    .requests_per_minute
                    .is_some_and(|limit| window.requests.saturating_add(window.reserved_requests) >= limit)
                {
                    return (
                        Err(format!(
                            "provider request rate exhausted ({}/{})",
                            window.requests.saturating_add(window.reserved_requests),
                            signals.requests_per_minute.unwrap()
                        )),
                        changed,
                    );
                }
                if signals
                    .tokens_per_minute
                    .is_some_and(|limit| window.tokens.saturating_add(estimated_tokens) > limit)
                {
                    return (
                        Err(format!(
                            "provider token rate exhausted ({}/{})",
                            window.tokens,
                            signals.tokens_per_minute.unwrap()
                        )),
                        changed,
                    );
                }
                window.reserved_requests = window.reserved_requests.saturating_add(1);
                window.tokens = window.tokens.saturating_add(estimated_tokens);
                window.reserved_tokens = window.reserved_tokens.saturating_add(estimated_tokens);
                window.unstarted_reserved_tokens = window.unstarted_reserved_tokens.saturating_add(estimated_tokens);
                (Ok(window.started_at_unix_ms), true)
            })
            .and_then(|result| result)?;
        Ok(ProviderRatePermit {
            manager: Arc::clone(self),
            reserved_tokens: estimated_tokens,
            window_started_at_unix_ms,
            state: ProviderRatePermitState::Reserved,
        })
    }

    fn mark_provider_request_started(
        &self,
        reserved_tokens: u64,
        reserved_window_started_at_unix_ms: i64,
    ) -> Result<i64, String> {
        self.with_state_update(|| {
            let signals = self.provider_signals();
            let now = (self.clock)();
            let mut window = self.provider_rate.lock().unwrap_or_else(|error| error.into_inner());
            if window.started_at_unix_ms == 0 || now.saturating_sub(window.started_at_unix_ms) >= 60_000 {
                *window = ProviderRateWindow {
                    started_at_unix_ms: now,
                    ..Default::default()
                };
            }
            if window.started_at_unix_ms != reserved_window_started_at_unix_ms {
                if signals
                    .requests_per_minute
                    .is_some_and(|limit| window.requests.saturating_add(window.reserved_requests) >= limit)
                {
                    return (
                        Err("provider request rate exhausted while starting reservation".to_owned()),
                        true,
                    );
                }
                if signals
                    .tokens_per_minute
                    .is_some_and(|limit| window.tokens.saturating_add(reserved_tokens) > limit)
                {
                    return (
                        Err("provider token rate exhausted while starting reservation".to_owned()),
                        true,
                    );
                }
                window.requests = window.requests.saturating_add(1);
                window.tokens = window.tokens.saturating_add(reserved_tokens);
                window.reserved_tokens = window.reserved_tokens.saturating_add(reserved_tokens);
                return (Ok(window.started_at_unix_ms), true);
            }
            if window.reserved_requests == 0 || window.unstarted_reserved_tokens < reserved_tokens {
                return (
                    Err("provider request reservation is no longer active".to_owned()),
                    false,
                );
            }
            window.reserved_requests -= 1;
            window.requests = window.requests.saturating_add(1);
            window.unstarted_reserved_tokens -= reserved_tokens;
            (Ok(window.started_at_unix_ms), true)
        })
        .and_then(|result| result)
    }

    fn rollback_provider_request_start(
        &self,
        reserved_tokens: u64,
        window_started_at_unix_ms: i64,
    ) -> Result<(), String> {
        self.with_state_update(|| {
            let mut window = self.provider_rate.lock().unwrap_or_else(|error| error.into_inner());
            if window.started_at_unix_ms != window_started_at_unix_ms {
                return (Ok(()), false);
            }
            if window.requests == 0 {
                return (Err("provider request start is no longer active".to_owned()), false);
            }
            window.requests -= 1;
            window.reserved_requests = window.reserved_requests.saturating_add(1);
            window.unstarted_reserved_tokens = window.unstarted_reserved_tokens.saturating_add(reserved_tokens);
            (Ok(()), true)
        })
        .and_then(|result| result)
    }

    fn commit_provider_tokens(
        &self,
        reserved_tokens: u64,
        actual_tokens: u64,
        window_started_at_unix_ms: i64,
    ) -> Result<(), String> {
        self.with_state_update(|| {
            let mut window = self.provider_rate.lock().unwrap_or_else(|error| error.into_inner());
            if window.started_at_unix_ms != window_started_at_unix_ms {
                return ((), false);
            }
            window.tokens = window
                .tokens
                .saturating_sub(reserved_tokens)
                .saturating_add(actual_tokens);
            window.reserved_tokens = window.reserved_tokens.saturating_sub(reserved_tokens);
            ((), true)
        })
    }

    fn release_provider_tokens(&self, reserved_tokens: u64, window_started_at_unix_ms: i64) {
        let _ = self.with_state_update(|| {
            let mut window = self.provider_rate.lock().unwrap_or_else(|error| error.into_inner());
            if window.started_at_unix_ms != window_started_at_unix_ms {
                return ((), false);
            }
            window.tokens = window.tokens.saturating_sub(reserved_tokens);
            window.reserved_tokens = window.reserved_tokens.saturating_sub(reserved_tokens);
            ((), true)
        });
    }

    fn release_provider_reservation(&self, reserved_tokens: u64, window_started_at_unix_ms: i64) {
        let _ = self.with_state_update(|| {
            let mut window = self.provider_rate.lock().unwrap_or_else(|error| error.into_inner());
            if window.started_at_unix_ms != window_started_at_unix_ms {
                return ((), false);
            }
            window.reserved_requests = window.reserved_requests.saturating_sub(1);
            window.tokens = window.tokens.saturating_sub(reserved_tokens);
            window.reserved_tokens = window.reserved_tokens.saturating_sub(reserved_tokens);
            window.unstarted_reserved_tokens = window.unstarted_reserved_tokens.saturating_sub(reserved_tokens);
            ((), true)
        });
    }

    pub fn record_model_usage(&self, usage: &TokenUsage) -> bool {
        self.record_model_usage_checked(usage).unwrap_or(false)
    }

    pub fn record_model_usage_once(&self, effect_id: &str, usage: &TokenUsage, count_turn: bool) -> bool {
        self.record_model_usage_once_checked(effect_id, usage, count_turn)
            .unwrap_or(false)
    }

    pub(crate) fn record_model_usage_checked(&self, usage: &TokenUsage) -> Result<bool, String> {
        self.with_state_update(|| (self.apply_model_usage_state(usage, true), true))
    }

    pub(crate) fn record_model_usage_once_checked(
        &self,
        effect_id: &str,
        usage: &TokenUsage,
        count_turn: bool,
    ) -> Result<bool, String> {
        self.with_state_update(|| {
            let mut applied = self
                .applied_usage_effects
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if applied.contains(effect_id) {
                return (true, false);
            }
            applied.insert(effect_id.to_owned());
            drop(applied);
            (self.apply_model_usage_state(usage, count_turn), true)
        })
    }

    /// Records one terminal tool round exactly once for this Run.
    ///
    /// The stable round call ID shares the same persisted applied-ID set used
    /// by provider usage, with a namespace prefix to prevent collisions.
    #[cfg(test)]
    pub(crate) fn record_tool_calls_once_checked(
        &self,
        round_call_id: &str,
        statuses: &[ToolResultStatus],
    ) -> Result<(), String> {
        let fingerprints = statuses
            .iter()
            .enumerate()
            .map(|(index, _)| format!("legacy:{round_call_id}:{index}"))
            .collect::<Vec<_>>();
        self.record_tool_call_fingerprints_once_checked(round_call_id, statuses, &fingerprints)
    }

    pub(crate) fn record_tool_call_fingerprints_once_checked(
        &self,
        round_call_id: &str,
        statuses: &[ToolResultStatus],
        fingerprints: &[String],
    ) -> Result<(), String> {
        if statuses.len() != fingerprints.len() {
            return Err("tool-call statuses and fingerprints have different lengths".to_owned());
        }
        let applied_id = format!("tool-round:{round_call_id}");
        self.with_state_update(|| {
            let mut applied = self
                .applied_usage_effects
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if applied.contains(&applied_id) {
                return ((), false);
            }
            applied.insert(applied_id);
            drop(applied);

            let round_calls = u64::try_from(statuses.len()).unwrap_or(u64::MAX);
            let round_useful =
                u64::try_from(statuses.iter().filter(|status| status.is_useful_call()).count()).unwrap_or(u64::MAX);
            let round_rate = useful_call_rate(statuses);
            let mut usage = self.usage.lock().unwrap_or_else(|error| error.into_inner());
            let mut seen = usage
                .seen_tool_call_fingerprints
                .iter()
                .cloned()
                .collect::<HashSet<_>>();
            let round_duplicates = fingerprints
                .iter()
                .filter(|fingerprint| !seen.insert((*fingerprint).clone()))
                .count();
            usage.tool_calls = usage.tool_calls.saturating_add(round_calls);
            usage.useful_tool_calls = usage.useful_tool_calls.saturating_add(round_useful);
            usage.duplicate_tool_calls = usage
                .duplicate_tool_calls
                .saturating_add(u64::try_from(round_duplicates).unwrap_or(u64::MAX));
            usage.seen_tool_call_fingerprints = seen.into_iter().collect();
            usage.seen_tool_call_fingerprints.sort();
            usage.refresh_useful_call_rate();
            usage.refresh_duplicate_call_rate();
            tracing::debug!(
                round_calls,
                round_useful,
                round_useful_call_rate = ?round_rate,
                total_calls = usage.tool_calls,
                useful_calls = usage.useful_tool_calls,
                useful_call_rate = ?usage.useful_call_rate,
                round_duplicates,
                duplicate_calls = usage.duplicate_tool_calls,
                duplicate_call_rate = ?usage.duplicate_call_rate,
                "recorded terminal tool-call statistics"
            );
            drop(usage);
            self.changed.notify_waiters();
            ((), true)
        })
    }

    fn apply_model_usage_state(&self, usage: &TokenUsage, count_turn: bool) -> bool {
        let signals = self.provider_signals();
        let categorized = signals.categorize_usage(usage);
        let budget = self.budget();
        let mut current = self.usage.lock().unwrap_or_else(|error| error.into_inner());
        current.tokens = current.tokens.saturating_add(categorized.total_tokens());
        current.uncached_input_tokens = current
            .uncached_input_tokens
            .saturating_add(categorized.uncached_input_tokens);
        current.input_tokens = current.input_tokens.saturating_add(usage.input_tokens);
        current.output_tokens = current.output_tokens.saturating_add(usage.output_tokens);
        current.cache_creation_tokens = current
            .cache_creation_tokens
            .saturating_add(usage.cache_creation_tokens);
        current.cache_read_tokens = current.cache_read_tokens.saturating_add(usage.cache_read_tokens);
        match signals.usage_cost(usage) {
            Some(cost) => current.cost += cost,
            None => current.cost_known = false,
        }
        if count_turn {
            current.turns = current.turns.saturating_add(1);
        }
        let within = budget.max_turns.is_none_or(|limit| current.turns <= limit)
            && budget.max_tokens.is_none_or(|limit| current.tokens <= limit)
            && budget
                .max_cost
                .is_none_or(|limit| current.cost_known && current.cost <= limit + cost_epsilon(limit));
        self.changed.notify_waiters();
        within
    }

    pub fn record_tokens_and_cost(&self, tokens: u64, cost: f64) -> bool {
        let within = self
            .with_state_update(|| {
                let budget = self.budget();
                let mut usage = self.usage.lock().unwrap_or_else(|error| error.into_inner());
                usage.tokens = usage.tokens.saturating_add(tokens);
                usage.cost += cost;
                let within = budget.max_tokens.is_none_or(|limit| usage.tokens <= limit)
                    && budget
                        .max_cost
                        .is_none_or(|limit| usage.cost <= limit + cost_epsilon(limit));
                (within, true)
            })
            .unwrap_or(false);
        self.changed.notify_waiters();
        within
    }

    pub fn usage(&self) -> ResourceUsage {
        self.usage.lock().unwrap_or_else(|error| error.into_inner()).clone()
    }

    pub fn elapsed_ms(&self) -> u64 {
        self.started_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
    }

    /// Remaining time until the durable wall-time deadline.
    pub(crate) fn remaining_wall_time_ms(&self) -> Option<u64> {
        self.deadline_unix_ms
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .map(|deadline| deadline.saturating_sub((self.clock)()).max(0) as u64)
    }

    pub fn hard_runtime_budget_reason(&self) -> Option<String> {
        if let Some(error) = self
            .persistence_error
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
        {
            return Some(format!("resource checkpoint persistence failed: {error}"));
        }
        let budget = self.budget();
        let usage = self.usage();
        if budget.max_turns.is_some_and(|limit| usage.turns >= limit) {
            return Some(format!(
                "run turn budget exhausted ({}/{})",
                usage.turns,
                budget.max_turns.unwrap()
            ));
        }
        if let Some(deadline) = *self.deadline_unix_ms.read().unwrap_or_else(|error| error.into_inner())
            && (self.clock)() >= deadline
        {
            return Some(format!("run wall-time budget exhausted at unix deadline {deadline}"));
        }
        if budget.max_tokens.is_some_and(|limit| usage.tokens >= limit) {
            return Some(format!(
                "run token budget exhausted ({}/{})",
                usage.tokens,
                budget.max_tokens.unwrap()
            ));
        }
        if budget.max_cost.is_some() && !usage.cost_known {
            return Some("run cost is unknown because provider pricing is incomplete".to_owned());
        }
        if budget.max_cost.is_some_and(|limit| usage.cost >= limit) {
            return Some(format!(
                "run cost budget exhausted ({:.6}/{:.6})",
                usage.cost,
                budget.max_cost.unwrap()
            ));
        }
        None
    }

    fn hard_agent_block_reason(&self, depth: usize) -> Option<String> {
        if let Some(reason) = self.spawn_depth_block_reason(depth) {
            return Some(reason);
        }
        let budget = self.budget();
        let usage = self.usage();
        if budget
            .max_total_descendants_per_run
            .is_some_and(|limit| usage.total_descendants >= limit)
        {
            return Some(format!(
                "run descendant budget exhausted ({}/{})",
                usage.total_descendants,
                budget.max_total_descendants_per_run.unwrap()
            ));
        }
        if usage.total_descendants == usize::MAX {
            return Some("run descendant counter exhausted at usize::MAX".to_owned());
        }
        self.hard_runtime_budget_reason()
    }

    fn hard_reattached_agent_block_reason(&self, depth: usize) -> Option<String> {
        self.spawn_depth_block_reason(depth)
            .or_else(|| self.hard_runtime_budget_reason())
    }

    fn spawn_depth_block_reason(&self, depth: usize) -> Option<String> {
        let budget = self.budget();
        budget
            .max_spawn_depth
            .filter(|limit| depth > *limit)
            .map(|limit| format!("spawn depth {depth} exceeds configured safety budget {limit}"))
    }

    fn release_agent(&self) {
        self.with_transient_state_update(|| {
            let mut usage = self.usage.lock().unwrap_or_else(|error| error.into_inner());
            usage.active_agents = usage.active_agents.saturating_sub(1);
        });
        self.changed.notify_waiters();
    }

    fn release_effect(&self) {
        self.with_transient_state_update(|| {
            let mut usage = self.usage.lock().unwrap_or_else(|error| error.into_inner());
            usage.concurrent_effects = usage.concurrent_effects.saturating_sub(1);
        });
        self.changed.notify_waiters();
    }

    fn with_transient_state_update<R>(&self, update: impl FnOnce() -> R) -> R {
        self.with_state_transaction(|_| update())
    }

    fn with_state_update<R>(&self, update: impl FnOnce() -> (R, bool)) -> Result<R, String> {
        self.with_state_transaction(|persistence| {
            let before = persistence::state_snapshot(self);
            let (result, changed) = update();
            if changed {
                let after = persistence::state_snapshot(self);
                if let Err(error) = persistence::persist_state_change_locked(self, persistence, &before, &after) {
                    persistence::restore_state(self, before);
                    self.record_persistence_error(error.clone());
                    return Err(format!("resource checkpoint persistence failed: {error}"));
                }
            }
            Ok(result)
        })
    }

    fn record_persistence_error(&self, error: String) {
        let should_notify = {
            let mut current = self
                .persistence_error
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if current.is_some() {
                false
            } else {
                *current = Some(error);
                true
            }
        };
        if should_notify {
            self.changed.notify_waiters();
        }
    }

    fn with_state_transaction<R>(&self, transaction: impl FnOnce(Option<&ResourcePersistence>) -> R) -> R {
        let persistence = self
            .persistence
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let mutation_line = persistence.as_ref().and_then(|persistence| {
            persistence
                .runtime
                .as_ref()
                .map(|runtime| runtime.mutation_coordinator().line_for(&persistence.run_id))
        });
        let _mutation_guard = mutation_line
            .as_ref()
            .map(|line| line.lock().unwrap_or_else(|error| error.into_inner()));
        let _checkpoint_guard = self
            .checkpoint_transaction
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        transaction(persistence.as_ref())
    }
}

pub struct AgentResourcePermit {
    manager: Arc<ResourceManager>,
    released: bool,
}

impl AgentResourcePermit {
    pub fn release(mut self) {
        if !self.released {
            self.manager.release_agent();
            self.released = true;
        }
    }
}

impl Drop for AgentResourcePermit {
    fn drop(&mut self) {
        if !self.released {
            self.manager.release_agent();
            self.released = true;
        }
    }
}

pub struct EffectResourcePermit {
    manager: Arc<ResourceManager>,
    released: bool,
}

pub struct ProviderRatePermit {
    manager: Arc<ResourceManager>,
    reserved_tokens: u64,
    window_started_at_unix_ms: i64,
    state: ProviderRatePermitState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderRatePermitState {
    Reserved,
    Started,
    Completed,
}

impl ProviderRatePermit {
    pub fn mark_started(&mut self) -> Result<(), String> {
        if self.state == ProviderRatePermitState::Reserved {
            self.window_started_at_unix_ms = self
                .manager
                .mark_provider_request_started(self.reserved_tokens, self.window_started_at_unix_ms)?;
            self.state = ProviderRatePermitState::Started;
        }
        Ok(())
    }

    pub(crate) fn rollback_start(&mut self) -> Result<(), String> {
        if self.state == ProviderRatePermitState::Started {
            self.manager
                .rollback_provider_request_start(self.reserved_tokens, self.window_started_at_unix_ms)?;
            self.state = ProviderRatePermitState::Reserved;
        }
        Ok(())
    }

    pub fn commit(mut self, actual_tokens: u64) -> Result<(), String> {
        self.mark_started()?;
        self.manager
            .commit_provider_tokens(self.reserved_tokens, actual_tokens, self.window_started_at_unix_ms)?;
        self.state = ProviderRatePermitState::Completed;
        Ok(())
    }
}

impl Drop for ProviderRatePermit {
    fn drop(&mut self) {
        match self.state {
            ProviderRatePermitState::Reserved => self
                .manager
                .release_provider_reservation(self.reserved_tokens, self.window_started_at_unix_ms),
            ProviderRatePermitState::Started => self
                .manager
                .release_provider_tokens(self.reserved_tokens, self.window_started_at_unix_ms),
            ProviderRatePermitState::Completed => return,
        }
        self.state = ProviderRatePermitState::Completed;
    }
}

impl EffectResourcePermit {
    pub fn release(mut self) {
        if !self.released {
            self.manager.release_effect();
            self.released = true;
        }
    }
}

impl Drop for EffectResourcePermit {
    fn drop(&mut self) {
        if !self.released {
            self.manager.release_effect();
            self.released = true;
        }
    }
}

fn cost_epsilon(limit: f64) -> f64 {
    1e-12_f64.max(limit.abs() * 1e-12)
}

#[cfg(test)]
#[path = "resource_manager_persistence_test.rs"]
mod resource_manager_persistence_test;

#[cfg(test)]
#[path = "resource_manager_admission_regression_test.rs"]
mod resource_manager_admission_regression_test;
#[cfg(test)]
#[path = "resource_manager_budget_migration_test.rs"]
mod resource_manager_budget_migration_test;
#[cfg(test)]
#[path = "resource_manager_checkpoint_regression_test.rs"]
mod resource_manager_checkpoint_regression_test;
#[cfg(test)]
#[path = "resource_manager_test.rs"]
mod resource_manager_test;
#[cfg(test)]
#[path = "resource_manager_token_accounting_test.rs"]
mod resource_manager_token_accounting_test;
