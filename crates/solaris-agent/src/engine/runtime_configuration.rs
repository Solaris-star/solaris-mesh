use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use solaris_compact::CompactLevel;
use solaris_protocol::events::RuntimeConfiguration;
use solaris_types::llm::ThinkingConfig;
use solaris_types::permission::PermissionMode;
use solaris_types::run_preset::Intensity;
use solaris_types::workflow::MultiAgentPolicy;

use crate::permission_engine::PermissionContext;

/// Read-only access to the configuration currently used for new model turns.
///
/// Hosts receive this view so snapshots and change events read the same state
/// that the engine updates after a configuration change is applied.
#[derive(Clone)]
pub struct RuntimeConfigurationView {
    state: Arc<RwLock<RuntimeConfigurationState>>,
    permissions: PermissionContext,
}

impl RuntimeConfigurationView {
    pub(super) fn new(state: Arc<RwLock<RuntimeConfigurationState>>, permissions: PermissionContext) -> Self {
        Self { state, permissions }
    }

    pub fn snapshot(&self) -> RuntimeConfiguration {
        let (mut configuration, tracked_plan_active, shared_plan_active) = {
            let state = self.state.read().unwrap_or_else(|error| error.into_inner());
            (
                state.configuration.clone(),
                state.plan_active,
                state.shared_plan_active.clone(),
            )
        };
        let plan_active = shared_plan_active
            .as_ref()
            .map(|flag| flag.load(Ordering::Acquire))
            .unwrap_or(tracked_plan_active);
        configuration.permission = if plan_active {
            PermissionMode::Plan
        } else {
            self.permissions.mode()
        };
        configuration
    }
}

pub(super) struct RuntimeConfigurationState {
    pub(super) configuration: RuntimeConfiguration,
    pub(super) plan_active: bool,
    pub(super) shared_plan_active: Option<Arc<AtomicBool>>,
}

pub(super) fn runtime_configuration_state(
    provider: &str,
    model: &str,
    permission: PermissionMode,
    thinking: &Option<ThinkingConfig>,
    compaction: CompactLevel,
) -> Arc<RwLock<RuntimeConfigurationState>> {
    Arc::new(RwLock::new(RuntimeConfigurationState {
        configuration: RuntimeConfiguration {
            provider: provider.to_owned(),
            model: model.to_owned(),
            permission,
            selected_intensity: Intensity::default(),
            multi_agent_policy: MultiAgentPolicy::default(),
            max_active_agents: None,
            effective_max_active_agents: 1,
            effective_effort: None,
            thinking: thinking.clone(),
            thinking_budget: thinking_budget(thinking),
            compaction,
        },
        plan_active: false,
        shared_plan_active: None,
    }))
}

pub(super) fn thinking_budget(thinking: &Option<ThinkingConfig>) -> Option<u32> {
    match thinking {
        Some(ThinkingConfig::Enabled { budget_tokens }) => Some(*budget_tokens),
        Some(ThinkingConfig::Disabled) | None => None,
    }
}
