use std::sync::{Arc, RwLock};

use crate::session::ActiveSessionFence;

use super::EffectExecutionContext;

pub(crate) type SessionFenceState = Arc<RwLock<Vec<ActiveSessionFence>>>;

impl EffectExecutionContext {
    pub(crate) fn with_session_fence(self, fence: ActiveSessionFence) -> Self {
        self.set_session_fence(fence);
        self
    }

    pub(crate) fn set_session_fence(&self, fence: ActiveSessionFence) {
        self.session_fences
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .push(fence);
    }

    pub(crate) fn replace_session_fence(&self, fence: ActiveSessionFence) {
        let mut fences = self.session_fences.write().unwrap_or_else(|error| error.into_inner());
        fences.clear();
        fences.push(fence);
    }

    pub(crate) fn share_session_fence_with(mut self, context: &Self) -> Self {
        self.session_fences = context.session_fence_state();
        self
    }

    pub(crate) fn with_session_fence_state(mut self, state: SessionFenceState) -> Self {
        self.session_fences = state;
        self
    }

    pub(crate) fn session_fence_state(&self) -> SessionFenceState {
        Arc::clone(&self.session_fences)
    }

    pub(crate) fn ensure_session_fence(&self) -> std::io::Result<()> {
        let fences = self
            .session_fences
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        for fence in fences {
            fence.ensure_current().map_err(|error| {
                std::io::Error::other(format!("session lease fence rejected durable effect: {error}"))
            })?;
        }
        Ok(())
    }
}
