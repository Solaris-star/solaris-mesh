use anyhow::{Context, Result, bail};
use chrono::Utc;
use solaris_config::hooks::HooksConfig;

use super::AgentEngine;
use crate::error::AgentError;
use crate::plan::state::PlanState;
use crate::session::SessionRuntimeState;

impl AgentEngine {
    pub(crate) fn install_session_manager(&mut self, manager: Option<crate::session::SessionManager>) {
        if let Some(manager) = manager {
            self.session_manager = Some(manager);
        }
    }

    /// Create state and acquire its owner lease in one SQLite transaction.
    pub fn init_session(&mut self, provider_name: &str, cwd: &str, session_id: Option<&str>) -> Result<()> {
        let Some(manager) = &self.session_manager else {
            return Ok(());
        };
        let run_id = self
            .execution_context
            .as_ref()
            .map(|context| context.run_id().to_string())
            .context("execution context is required before session creation")?;
        let session = manager
            .create_active_session(provider_name, &self.model, cwd, session_id, &run_id)
            .map_err(anyhow::Error::new)?;
        let fence = manager.active_fence(&session.id).map_err(anyhow::Error::new)?;
        if let Some(context) = self.execution_context.as_ref() {
            context.replace_session_fence(fence);
        }
        tracing::info!(
            target: "solaris_agent",
            session_id = %session.id,
            run_id = %run_id,
            provider = %provider_name,
            model = %self.model,
            "session started"
        );
        self.current_session = Some(session);
        Ok(())
    }

    /// Acquire a durable lease and refresh the in-memory resume snapshot.
    pub(crate) fn activate_resumed_session(&mut self) -> Result<()> {
        let Some(session_id) = self.current_session.as_ref().map(|session| session.id.clone()) else {
            return Ok(());
        };
        let Some(manager) = &self.session_manager else {
            return Ok(());
        };
        if let Some(active_session_id) = manager.active_session_id().map_err(anyhow::Error::new)? {
            if active_session_id != session_id {
                bail!("session manager already owns active session '{active_session_id}'");
            }
            manager.ensure_active_session(&session_id).map_err(anyhow::Error::new)?;
            return Ok(());
        }

        let mut session = manager.load_active_session(&session_id).map_err(anyhow::Error::new)?;
        let expected_run_id = self
            .execution_context
            .as_ref()
            .map(|context| context.run_id().to_string())
            .context("execution context is required before session resume")?;
        if let Some(stored_run_id) = session.run_id.as_deref() {
            if stored_run_id != expected_run_id {
                if let Err(release_error) = manager.release_active_session() {
                    bail!(
                        "session '{}' belongs to run '{}' instead of '{}'; failed to release rejected lease: {}",
                        session.id,
                        stored_run_id,
                        expected_run_id,
                        release_error
                    );
                }
                bail!(
                    "session '{}' belongs to run '{}' instead of '{}'",
                    session.id,
                    stored_run_id,
                    expected_run_id
                );
            }
        } else {
            session.run_id = Some(expected_run_id.clone());
        }
        let fence = manager.active_fence(&session_id).map_err(anyhow::Error::new)?;
        if let Some(context) = self.execution_context.as_ref() {
            context.replace_session_fence(fence);
        }
        session.provider.clone_from(&self.provider_label);
        session.model.clone_from(&self.model);
        manager.save_active_session(&session).map_err(anyhow::Error::new)?;
        self.messages.clone_from(&session.messages);
        self.total_usage.clone_from(&session.total_usage);
        let runtime_state = session.runtime_state.clone();
        self.current_session = Some(session);
        self.restore_session_runtime_state(runtime_state.as_ref());
        tracing::info!(
            target: "solaris_agent",
            session_id,
            run_id = %expected_run_id,
            "session resumed with active lease"
        );
        Ok(())
    }

    pub(super) fn ensure_session_lease(&self) -> Result<(), AgentError> {
        let (Some(manager), Some(session)) = (&self.session_manager, &self.current_session) else {
            return Ok(());
        };
        manager
            .ensure_active_session(&session.id)
            .map_err(|error| AgentError::ApiError(error.to_string()))
    }

    pub(super) fn save_session(&mut self) -> Result<(), AgentError> {
        self.persist_session_state().map_err(AgentError::ApiError)
    }

    pub(super) fn persist_session_state(&mut self) -> std::result::Result<(), String> {
        let runtime_state = self.session_runtime_state();
        if let (Some(manager), Some(session)) = (&self.session_manager, &mut self.current_session) {
            manager
                .ensure_active_session(&session.id)
                .map_err(|error| error.to_string())?;
            session.messages.clone_from(&self.messages);
            session.provider.clone_from(&self.provider_label);
            session.model.clone_from(&self.model);
            session.total_usage.clone_from(&self.total_usage);
            session.runtime_state = Some(runtime_state);
            session.updated_at = Utc::now();
            manager
                .save_active_session(session)
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }

    pub(super) fn session_runtime_state(&self) -> SessionRuntimeState {
        SessionRuntimeState {
            reasoning_effort: self.reasoning_effort.clone(),
            allow_list: self.allow_list.clone(),
            plan_active: self.plan_state.is_active,
            pre_plan_allow_list: self.plan_state.pre_plan_allow_list.clone(),
            hooks: self
                .hooks
                .as_ref()
                .map_or_else(HooksConfig::default, |hooks| hooks.config_snapshot()),
            memory_snapshot: self
                .current_session
                .as_ref()
                .and_then(|session| session.runtime_state.as_ref())
                .and_then(|state| state.memory_snapshot.clone()),
        }
    }

    pub(super) fn restore_session_runtime_state(&mut self, runtime_state: Option<&SessionRuntimeState>) {
        let Some(runtime_state) = runtime_state else {
            return;
        };
        self.reasoning_effort.clone_from(&runtime_state.reasoning_effort);
        self.allow_list.clone_from(&runtime_state.allow_list);
        self.plan_state = PlanState {
            is_active: runtime_state.plan_active,
            pre_plan_allow_list: runtime_state.pre_plan_allow_list.clone(),
        };
        if let Some(hooks) = &mut self.hooks {
            hooks.replace_config(runtime_state.hooks.clone());
        }
        if let Ok(mut confirmer) = self.confirmer.lock() {
            confirmer.replace_allow_list(&runtime_state.allow_list);
        }
        if let Some(flag) = &self.plan_active_flag {
            flag.store(runtime_state.plan_active, std::sync::atomic::Ordering::Release);
        }
        self.refresh_runtime_configuration();
    }

    pub(super) fn release_session_lease(&self) -> Result<()> {
        if let Some(manager) = &self.session_manager {
            manager.release_active_session().map_err(anyhow::Error::new)?;
        }
        Ok(())
    }

    /// Release a cold-resumable child session without running terminal stop hooks.
    pub(crate) fn release_session_for_cold_resume(&self) -> Result<()> {
        self.release_session_lease()
    }
}
