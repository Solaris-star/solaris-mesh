use std::path::PathBuf;

use anyhow::Result;

use crate::session::{ActiveSessionFence, SessionManager};

use super::AgentBootstrap;

impl AgentBootstrap {
    pub(super) fn acquire_resumed_session_lease(&mut self) -> Result<Option<ActiveSessionFence>> {
        let Some(requested) = self.resume_session.as_ref() else {
            return Ok(None);
        };
        if !self.config.session.enabled {
            return Ok(None);
        }
        let manager = SessionManager::new(
            PathBuf::from(&self.config.session.directory),
            self.config.session.max_sessions,
        );
        let session_id = requested.id.clone();
        let persisted = manager.load_active_session(&session_id).map_err(anyhow::Error::new)?;
        let expected_run_id = self.run_id.to_string();
        if let Some(stored_run_id) = persisted.run_id.as_deref()
            && stored_run_id != expected_run_id
        {
            let mismatch = format!(
                "session '{}' belongs to run '{}' instead of '{}'",
                persisted.id, stored_run_id, expected_run_id
            );
            if let Err(release_error) = manager.release_active_session() {
                anyhow::bail!("{mismatch}; failed to release rejected session lease: {release_error}");
            }
            anyhow::bail!(mismatch);
        }
        let fence = manager.active_fence(&session_id).map_err(anyhow::Error::new)?;
        self.resume_session = Some(persisted);
        self.session_manager = Some(manager);
        Ok(Some(fence))
    }
}
