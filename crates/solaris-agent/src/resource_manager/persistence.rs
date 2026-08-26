use serde::{Deserialize, Serialize};
use serde_json::json;
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::RunId;

use crate::collaboration_runtime::CollaborationRuntime;
use crate::runtime_ledger::LedgerRecord;
use crate::runtime_ledger::RuntimeLedger;

use super::{ProviderRateWindow, ResourceManager, ResourcePersistence, ResourceUsage};

pub(super) const RESOURCE_SNAPSHOT_INTERVAL: u64 = 64;
const RESOURCE_CHECKPOINT_SCHEMA_VERSION: u32 = 1;
const RESOURCE_DELTA_SCHEMA_VERSION: u32 = 1;
const LEGACY_DEFAULT_MAX_SPAWN_DEPTH: usize = 8;
const LEGACY_DEFAULT_MAX_TOTAL_DESCENDANTS_PER_RUN: usize = 256;

#[derive(Clone, PartialEq)]
pub(super) struct ResourceStateSnapshot {
    budget: solaris_types::resource::ResourceBudget,
    provider_signals: solaris_types::provider_contract::ProviderSignals,
    usage: ResourceUsage,
    deadline_unix_ms: Option<i64>,
    provider_rate: ProviderRateWindow,
    applied_usage_effects: std::collections::HashSet<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct OptionalDeadline {
    value: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct ResourceStateDelta {
    schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    budget: Option<solaris_types::resource::ResourceBudget>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    provider_signals: Option<solaris_types::provider_contract::ProviderSignals>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    usage: Option<ResourceUsage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    deadline_unix_ms: Option<OptionalDeadline>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    provider_rate: Option<ProviderRateWindow>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    applied_usage_effects_added: Vec<String>,
}

pub(super) struct RestoredResourceState {
    pub(super) has_durable_state: bool,
    pub(super) deltas_since_snapshot: u64,
}

pub(super) fn attach(
    manager: &ResourceManager,
    run_id: RunId,
    ledger: std::sync::Arc<dyn RuntimeLedger>,
    runtime: Option<std::sync::Arc<CollaborationRuntime<()>>>,
) -> Result<(), String> {
    let persistence = ResourcePersistence {
        run_id,
        ledger,
        runtime,
    };
    let mutation_line = persistence
        .runtime
        .as_ref()
        .map(|runtime| runtime.mutation_coordinator().line_for(&persistence.run_id));
    let _mutation_guard = mutation_line
        .as_ref()
        .map(|line| line.lock().unwrap_or_else(|error| error.into_inner()));
    let _checkpoint_guard = manager
        .checkpoint_transaction
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let previous_state = state_snapshot(manager);
    let previous_persistence = manager
        .persistence
        .read()
        .unwrap_or_else(|error| error.into_inner())
        .clone();
    let previous_persistence_error = manager
        .persistence_error
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clone();
    let previous_delta_count = *manager
        .resource_deltas_since_snapshot
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let result = (|| {
        let records = persistence
            .ledger
            .records_for_run(&persistence.run_id)
            .map_err(|error| error.to_string())?;
        let restored = restore_latest_state(manager, &records)?;
        *manager
            .resource_deltas_since_snapshot
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = restored.deltas_since_snapshot;
        *manager.persistence.write().unwrap_or_else(|error| error.into_inner()) = Some(persistence.clone());
        if !restored.has_durable_state || restored.deltas_since_snapshot >= RESOURCE_SNAPSHOT_INTERVAL {
            persist_snapshot_locked(manager, Some(&persistence))
                .map_err(|error| format!("resource checkpoint persistence failed: {error}"))?;
        }
        Ok(())
    })();
    if result.is_err() {
        restore_state(manager, previous_state);
        *manager.persistence.write().unwrap_or_else(|error| error.into_inner()) = previous_persistence;
        *manager
            .persistence_error
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = previous_persistence_error;
        *manager
            .resource_deltas_since_snapshot
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = previous_delta_count;
    }
    result
}

pub(super) fn state_snapshot(manager: &ResourceManager) -> ResourceStateSnapshot {
    ResourceStateSnapshot {
        budget: manager.budget(),
        provider_signals: manager.provider_signals(),
        usage: manager.usage(),
        deadline_unix_ms: *manager
            .deadline_unix_ms
            .read()
            .unwrap_or_else(|error| error.into_inner()),
        provider_rate: manager
            .provider_rate
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone(),
        applied_usage_effects: manager
            .applied_usage_effects
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone(),
    }
}

pub(super) fn restore_state(manager: &ResourceManager, snapshot: ResourceStateSnapshot) {
    *manager.budget.write().unwrap_or_else(|error| error.into_inner()) = snapshot.budget;
    *manager
        .provider_signals
        .write()
        .unwrap_or_else(|error| error.into_inner()) = snapshot.provider_signals;
    *manager.usage.lock().unwrap_or_else(|error| error.into_inner()) = snapshot.usage;
    *manager
        .deadline_unix_ms
        .write()
        .unwrap_or_else(|error| error.into_inner()) = snapshot.deadline_unix_ms;
    *manager.provider_rate.lock().unwrap_or_else(|error| error.into_inner()) = snapshot.provider_rate;
    *manager
        .applied_usage_effects
        .lock()
        .unwrap_or_else(|error| error.into_inner()) = snapshot.applied_usage_effects;
}

impl ResourceStateDelta {
    fn between(before: &ResourceStateSnapshot, after: &ResourceStateSnapshot) -> Option<Self> {
        if !before.applied_usage_effects.is_subset(&after.applied_usage_effects) {
            return None;
        }
        let mut added: Vec<_> = after
            .applied_usage_effects
            .difference(&before.applied_usage_effects)
            .cloned()
            .collect();
        added.sort();
        Some(Self {
            schema_version: RESOURCE_DELTA_SCHEMA_VERSION,
            budget: (before.budget != after.budget).then(|| after.budget.clone()),
            provider_signals: (before.provider_signals != after.provider_signals)
                .then(|| after.provider_signals.clone()),
            usage: (before.usage != after.usage).then(|| after.usage.clone()),
            deadline_unix_ms: (before.deadline_unix_ms != after.deadline_unix_ms).then_some(OptionalDeadline {
                value: after.deadline_unix_ms,
            }),
            provider_rate: (before.provider_rate != after.provider_rate).then(|| after.provider_rate.clone()),
            applied_usage_effects_added: added,
        })
    }

    fn apply(self, state: &mut ResourceStateSnapshot) -> Result<(), String> {
        if self.schema_version != RESOURCE_DELTA_SCHEMA_VERSION {
            return Err(format!(
                "unsupported resource delta schema version {}",
                self.schema_version
            ));
        }
        if let Some(value) = self.budget {
            state.budget = value;
        }
        if let Some(value) = self.provider_signals {
            state.provider_signals = value;
        }
        if let Some(value) = self.usage {
            state.usage = value;
        }
        if let Some(value) = self.deadline_unix_ms {
            state.deadline_unix_ms = value.value;
        }
        if let Some(value) = self.provider_rate {
            state.provider_rate = value;
        }
        state.applied_usage_effects.extend(self.applied_usage_effects_added);
        Ok(())
    }
}

pub(super) fn restore_latest_state(
    manager: &ResourceManager,
    records: &[LedgerRecord],
) -> Result<RestoredResourceState, String> {
    let Some(checkpoint_index) = records
        .iter()
        .rposition(|record| record.record_type == "resource_usage_checkpoint")
    else {
        if records
            .iter()
            .any(|record| record.record_type == "resource_usage_delta")
        {
            return Err("resource delta exists without a preceding checkpoint".to_owned());
        }
        return Ok(RestoredResourceState {
            has_durable_state: false,
            deltas_since_snapshot: 0,
        });
    };

    let mut state = checkpoint_state(manager, &records[checkpoint_index])?;
    let mut deltas_since_snapshot = 0_u64;
    for record in &records[checkpoint_index + 1..] {
        if record.record_type != "resource_usage_delta" {
            continue;
        }
        let delta = resource_state_delta_from_record(record.payload.clone())
            .map_err(|error| format!("invalid resource usage delta: {error}"))?;
        delta.apply(&mut state)?;
        deltas_since_snapshot = deltas_since_snapshot.saturating_add(1);
    }
    restore_applied_usage_history(records, &mut state);
    normalize_recovered_state(manager, &mut state);
    restore_state(manager, state);
    Ok(RestoredResourceState {
        has_durable_state: true,
        deltas_since_snapshot,
    })
}

fn checkpoint_state(manager: &ResourceManager, record: &LedgerRecord) -> Result<ResourceStateSnapshot, String> {
    let schema_version = checkpoint_schema_version(record)?;
    let budget = record
        .payload
        .get("budget")
        .cloned()
        .map(|value| resource_budget_from_checkpoint(value, schema_version))
        .transpose()
        .map_err(|error| format!("invalid resource budget checkpoint: {error}"))?
        .unwrap_or_else(|| manager.budget());
    let provider_signals = record
        .payload
        .get("provider_signals")
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(|error| format!("invalid provider signal checkpoint: {error}"))?
        .unwrap_or_else(|| manager.provider_signals());
    let usage = serde_json::from_value(record.payload.get("usage").cloned().unwrap_or_default())
        .map_err(|error| format!("invalid resource usage checkpoint: {error}"))?;
    let deadline_unix_ms = record
        .payload
        .get("deadline_unix_ms")
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(|error| format!("invalid resource deadline checkpoint: {error}"))?
        .unwrap_or_else(|| {
            *manager
                .deadline_unix_ms
                .read()
                .unwrap_or_else(|error| error.into_inner())
        });
    let provider_rate = record
        .payload
        .get("provider_rate")
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(|error| format!("invalid provider rate checkpoint: {error}"))?
        .unwrap_or_default();
    let applied_usage_effects = record
        .payload
        .get("applied_usage_effects")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .map(str::to_owned)
        .collect();
    Ok(ResourceStateSnapshot {
        budget,
        provider_signals,
        usage,
        deadline_unix_ms,
        provider_rate,
        applied_usage_effects,
    })
}

fn checkpoint_schema_version(record: &LedgerRecord) -> Result<u32, String> {
    let Some(value) = record.payload.get("schema_version") else {
        return Ok(0);
    };
    let version = value
        .as_u64()
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| "resource checkpoint schema_version must be a u32".to_owned())?;
    if version > RESOURCE_CHECKPOINT_SCHEMA_VERSION {
        return Err(format!("unsupported resource checkpoint schema version {version}"));
    }
    Ok(version)
}

fn resource_budget_from_checkpoint(
    mut value: serde_json::Value,
    schema_version: u32,
) -> Result<solaris_types::resource::ResourceBudget, serde_json::Error> {
    if schema_version == 0 {
        migrate_legacy_resource_budget_fields(&mut value);
    }
    serde_json::from_value(value)
}

fn resource_state_delta_from_record(mut value: serde_json::Value) -> Result<ResourceStateDelta, serde_json::Error> {
    if let Some(budget) = value.get_mut("budget") {
        migrate_legacy_resource_budget_fields(budget);
    }
    serde_json::from_value(value)
}

fn migrate_legacy_resource_budget_fields(value: &mut serde_json::Value) {
    let Some(budget) = value.as_object_mut() else {
        return;
    };
    budget
        .entry("max_spawn_depth")
        .or_insert_with(|| json!(LEGACY_DEFAULT_MAX_SPAWN_DEPTH));
    budget
        .entry("max_total_descendants_per_run")
        .or_insert_with(|| json!(LEGACY_DEFAULT_MAX_TOTAL_DESCENDANTS_PER_RUN));
}

fn restore_applied_usage_history(records: &[LedgerRecord], state: &mut ResourceStateSnapshot) {
    for record in records {
        let field = match record.record_type.as_str() {
            "resource_usage_checkpoint" => "applied_usage_effects",
            "resource_usage_delta" => "applied_usage_effects_added",
            _ => continue,
        };
        state.applied_usage_effects.extend(
            record
                .payload
                .get(field)
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_owned),
        );
    }
}

fn normalize_recovered_state(manager: &ResourceManager, state: &mut ResourceStateSnapshot) {
    state.usage.active_agents = 0;
    state.usage.concurrent_effects = 0;
    state.usage.refresh_useful_call_rate();
    if state.provider_rate.started_at_unix_ms != 0
        && (manager.clock)().saturating_sub(state.provider_rate.started_at_unix_ms) >= 60_000
    {
        state.provider_rate = ProviderRateWindow::default();
        return;
    }
    state.provider_rate.tokens = state
        .provider_rate
        .tokens
        .saturating_sub(state.provider_rate.unstarted_reserved_tokens);
    state.provider_rate.reserved_tokens = state
        .provider_rate
        .reserved_tokens
        .saturating_sub(state.provider_rate.unstarted_reserved_tokens);
    state.provider_rate.reserved_requests = 0;
    state.provider_rate.unstarted_reserved_tokens = 0;
}

pub(super) fn persist_state_change_locked(
    manager: &ResourceManager,
    persistence: Option<&ResourcePersistence>,
    before: &ResourceStateSnapshot,
    after: &ResourceStateSnapshot,
) -> Result<(), String> {
    let Some(persistence) = persistence else {
        return Ok(());
    };
    if before == after {
        return Ok(());
    }

    let delta = ResourceStateDelta::between(before, after);
    let mut delta_count = manager
        .resource_deltas_since_snapshot
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let next_count = delta_count.saturating_add(1);
    match delta {
        Some(delta) if next_count < RESOURCE_SNAPSHOT_INTERVAL => {
            append_record(
                persistence,
                "resource_usage_delta",
                serde_json::to_value(delta).map_err(|error| error.to_string())?,
            )?;
            *delta_count = next_count;
        }
        _ => {
            append_snapshot(manager, persistence)?;
            *delta_count = 0;
        }
    }
    Ok(())
}

pub(super) fn persist_snapshot_locked(
    manager: &ResourceManager,
    persistence: Option<&ResourcePersistence>,
) -> Result<(), String> {
    let Some(persistence) = persistence else {
        return Ok(());
    };
    append_snapshot(manager, persistence)?;
    *manager
        .resource_deltas_since_snapshot
        .lock()
        .unwrap_or_else(|error| error.into_inner()) = 0;
    Ok(())
}

fn append_snapshot(manager: &ResourceManager, persistence: &ResourcePersistence) -> Result<(), String> {
    let usage = manager.usage();
    let deadline = *manager
        .deadline_unix_ms
        .read()
        .unwrap_or_else(|error| error.into_inner());
    let provider_rate = manager
        .provider_rate
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clone();
    let payload = json!({
        "schema_version": RESOURCE_CHECKPOINT_SCHEMA_VERSION,
        "budget": manager.budget(),
        "usage": usage,
        "effective_agent_limit": manager.effective_agent_limit(),
        "provider_signals": manager.provider_signals(),
        "elapsed_ms": manager.elapsed_ms(),
        "deadline_unix_ms": deadline,
        "provider_rate": provider_rate,
    });
    append_record(persistence, "resource_usage_checkpoint", payload)
}

fn append_record(
    persistence: &ResourcePersistence,
    record_type: &str,
    payload: serde_json::Value,
) -> Result<(), String> {
    persistence
        .ledger
        .append(
            &persistence.run_id,
            DurabilityClass::SyncCritical,
            record_type,
            payload.clone(),
        )
        .map(|record| {
            if let Some(runtime) = persistence.runtime.as_ref() {
                runtime.emit_durable_event(&record, None, record_type, payload);
            }
        })
        .map_err(|error| error.to_string())
}
