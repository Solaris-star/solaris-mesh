use std::sync::atomic::Ordering;

use solaris_types::skill_types::{ContextModifier, PlanModeTransition, effort_to_string};

use super::AgentEngine;
use crate::error::AgentError;

impl AgentEngine {
    pub(super) fn persist_plan_artifacts(&self, modifiers: &[Option<ContextModifier>]) -> Result<(), AgentError> {
        let plans = modifiers.iter().flatten().filter_map(|modifier| {
            let PlanModeTransition::Exit {
                plan_content: Some(plan),
            } = modifier.plan_mode_transition.as_ref()?
            else {
                return None;
            };
            Some(plan.as_str())
        });
        let plans = plans.collect::<Vec<_>>();
        if plans.len() > 1 {
            return Err(AgentError::ApiError(
                "multiple plan artifacts were submitted in one tool round".to_owned(),
            ));
        }
        if let Some(plan) = plans.first() {
            let context = self.execution_context.as_ref().ok_or_else(|| {
                AgentError::ApiError("effect execution context is required to persist a plan artifact".to_owned())
            })?;
            context
                .record_plan_artifact(&self.msg_id, plan)
                .map_err(|error| AgentError::ApiError(format!("plan artifact persistence failed: {error}")))?;
        }
        Ok(())
    }

    /// Apply context modifiers collected from tool executions.
    pub(super) fn apply_context_modifiers(&mut self, modifiers: &[Option<ContextModifier>]) {
        for modifier in modifiers.iter().flatten() {
            if let Some(ref model) = modifier.model {
                self.model = model.clone();
            }
            if let Some(effort) = modifier.effort {
                self.reasoning_effort = Some(effort_to_string(effort));
            }
            for tool_name in &modifier.allowed_tools {
                if !self.allow_list.contains(tool_name) {
                    self.allow_list.push(tool_name.clone());
                }
                self.confirmer.lock().unwrap().add_to_allow_list(tool_name);
            }

            if let Some(ref transition) = modifier.plan_mode_transition {
                match transition {
                    PlanModeTransition::Enter => {
                        self.disable_mcp_for_plan();
                        self.plan_state.pre_plan_allow_list = self.allow_list.clone();
                        self.plan_state.is_active = true;
                        if let Some(ref flag) = self.plan_active_flag {
                            flag.store(true, Ordering::Release);
                        }
                    }
                    PlanModeTransition::Exit { .. } => {
                        self.plan_state.is_active = false;
                        self.allow_list = self.plan_state.pre_plan_allow_list.clone();
                        if let Some(ref flag) = self.plan_active_flag {
                            flag.store(false, Ordering::Release);
                        }
                    }
                }
            }
        }
        self.refresh_runtime_configuration();
    }
}
