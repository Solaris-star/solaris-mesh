use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};

use tokio::sync::oneshot;

use crate::commands::{ApprovalScope, SessionMode};

/// Result of a tool approval request
pub enum ToolApprovalResult {
    Approved { scope: ApprovalScope },
    Denied { reason: String },
}

struct PendingApproval {
    generation: u64,
    tx: oneshot::Sender<ToolApprovalResult>,
}

struct ApprovalState {
    pending: Mutex<HashMap<String, PendingApproval>>,
    auto_approved: Mutex<HashSet<String>>,
    session_mode: Mutex<SessionMode>,
    next_generation: AtomicU64,
}

pub struct PendingApprovalHandle {
    call_id: String,
    generation: u64,
    receiver: oneshot::Receiver<ToolApprovalResult>,
    state: Weak<ApprovalState>,
}

impl Future for PendingApprovalHandle {
    type Output = Result<ToolApprovalResult, oneshot::error::RecvError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.receiver).poll(context)
    }
}

impl Drop for PendingApprovalHandle {
    fn drop(&mut self) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        if let Ok(mut pending) = state.pending.lock()
            && pending
                .get(&self.call_id)
                .is_some_and(|entry| entry.generation == self.generation)
        {
            pending.remove(&self.call_id);
        }
    }
}

/// Manages pending tool approval requests using oneshot channels.
///
/// Approval scope is returned to the Mesh permission layer. A client approval with
/// `ApprovalScope::Always` becomes a resource-scoped CapabilityLease rather than
/// mutating a whole-tool allow-list.
///
/// Also holds the current user-facing permission mode. Only Bypass itself
/// globally suppresses interactive approval; Plan/Auto are effect-aware upstream.
pub struct ToolApprovalManager {
    state: Arc<ApprovalState>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalResolution {
    Applied,
    NotFound,
}

impl ApprovalResolution {
    pub fn was_applied(self) -> bool {
        matches!(self, Self::Applied)
    }
}

impl ToolApprovalManager {
    pub fn new() -> Self {
        Self {
            state: Arc::new(ApprovalState {
                pending: Mutex::new(HashMap::new()),
                auto_approved: Mutex::new(HashSet::new()),
                session_mode: Mutex::new(SessionMode::Auto),
                next_generation: AtomicU64::new(1),
            }),
        }
    }

    pub fn request_approval(&self, call_id: &str, _approval_key: &str) -> PendingApprovalHandle {
        let (tx, rx) = oneshot::channel();
        let generation = self.state.next_generation.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut pending) = self.state.pending.lock() {
            pending.insert(call_id.to_string(), PendingApproval { generation, tx });
        }
        PendingApprovalHandle {
            call_id: call_id.to_owned(),
            generation,
            receiver: rx,
            state: Arc::downgrade(&self.state),
        }
    }

    pub fn approve(&self, call_id: &str, scope: ApprovalScope) -> ApprovalResolution {
        let pending = self
            .state
            .pending
            .lock()
            .ok()
            .and_then(|mut pending| pending.remove(call_id));

        if let Some(pending) = pending {
            if pending.tx.send(ToolApprovalResult::Approved { scope }).is_ok() {
                ApprovalResolution::Applied
            } else {
                ApprovalResolution::NotFound
            }
        } else {
            ApprovalResolution::NotFound
        }
    }

    pub fn resolve(&self, call_id: &str, result: ToolApprovalResult) -> ApprovalResolution {
        if let Some(pending) = self
            .state
            .pending
            .lock()
            .ok()
            .and_then(|mut pending| pending.remove(call_id))
        {
            if pending.tx.send(result).is_ok() {
                ApprovalResolution::Applied
            } else {
                ApprovalResolution::NotFound
            }
        } else {
            ApprovalResolution::NotFound
        }
    }

    pub fn is_auto_approved(&self, approval_key: &str) -> bool {
        // Check session mode first
        let mode_approved = self
            .state
            .session_mode
            .lock()
            .map(|mode| match *mode {
                SessionMode::Bypass => true,
                SessionMode::Plan | SessionMode::Auto => false,
            })
            .unwrap_or(false);

        if mode_approved {
            return true;
        }

        // Fall back to per-capability legacy "always" approvals
        self.state
            .auto_approved
            .lock()
            .map(|auto| auto.contains(approval_key))
            .unwrap_or(false)
    }

    /// Set the user-facing permission mode. Takes effect immediately.
    pub fn set_mode(&self, mode: SessionMode) {
        if let Ok(mut current) = self.state.session_mode.lock() {
            *current = mode;
        }
    }

    pub fn permission_mode(&self) -> SessionMode {
        self.state.session_mode.lock().map(|mode| *mode).unwrap_or_default()
    }
    /// Return the current session mode as a string for capability reporting.
    pub fn current_mode(&self) -> String {
        self.state
            .session_mode
            .lock()
            .map(|mode| match *mode {
                SessionMode::Plan => "plan",
                SessionMode::Auto => "auto",
                SessionMode::Bypass => "bypass",
            })
            .unwrap_or("auto")
            .to_string()
    }

    pub fn drop_pending(&self, call_id: &str) {
        if let Ok(mut pending) = self.state.pending.lock() {
            pending.remove(call_id);
        }
    }

    pub fn pending_count(&self) -> usize {
        self.state
            .pending
            .lock()
            .map(|pending| pending.len())
            .unwrap_or_default()
    }

    pub fn add_auto_approve(&self, approval_key: &str) {
        if let Ok(mut auto) = self.state.auto_approved.lock() {
            auto.insert(approval_key.to_string());
        }
    }
}

impl Default for ToolApprovalManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[path = "approval_test.rs"]
mod approval_test;
