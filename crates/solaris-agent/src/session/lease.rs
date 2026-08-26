use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use thiserror::Error;

use super::Session;
use super::store::{
    DEFAULT_HEARTBEAT_SECONDS, DurableTaskPhase, SessionLease, SessionStore, SessionStoreError, StoredDurableTask,
};

#[derive(Debug, Error)]
pub(crate) enum ActiveSessionError {
    #[error("session '{session_id}' already has an active lease in this manager")]
    AlreadyActive { session_id: String },
    #[error("session '{session_id}' lease was lost: {reason}")]
    LeaseLost { session_id: String, reason: String },
    #[error("session lease state lock was poisoned")]
    LockPoisoned,
    #[error("failed to generate a unique session ID after bounded retries")]
    SessionIdExhausted,
    #[error("failed to start session heartbeat: {0}")]
    HeartbeatStart(#[source] std::io::Error),
    #[error("failed to persist the durable session Memory snapshot: {0}")]
    MemorySnapshotPersistence(#[source] std::io::Error),
    #[error("session '{session_id}' already has a different durable Memory snapshot")]
    MemorySnapshotAlreadySet { session_id: String },
    #[error("session manager already has a different pending durable Memory snapshot")]
    InitialMemorySnapshotAlreadySet,
    #[error("session heartbeat thread terminated unexpectedly")]
    HeartbeatJoin,
    #[error(transparent)]
    Store(#[from] SessionStoreError),
}

impl ActiveSessionError {
    #[cfg(test)]
    pub(crate) fn is_lease_lost(&self) -> bool {
        matches!(self, Self::LeaseLost { .. })
    }
}

#[derive(Debug)]
struct LeaseState {
    lease: SessionLease,
    lost_reason: Option<String>,
    released: bool,
}

#[derive(Clone)]
pub(crate) struct ActiveSessionFence {
    store: SessionStore,
    state: Arc<Mutex<LeaseState>>,
}

impl ActiveSessionFence {
    pub(crate) fn ensure_current(&self) -> Result<(), ActiveSessionError> {
        let mut state = self.state.lock().map_err(|_| ActiveSessionError::LockPoisoned)?;
        require_trusted(&state)?;
        if let Err(error) = self.store.heartbeat(&state.lease) {
            return Err(mark_lost(&mut state, error));
        }
        Ok(())
    }

    pub(crate) fn begin_or_resume_task(
        &self,
        task_key: &str,
        input_digest: &[u8],
    ) -> Result<StoredDurableTask, ActiveSessionError> {
        let mut state = self.state.lock().map_err(|_| ActiveSessionError::LockPoisoned)?;
        require_trusted(&state)?;
        match self.store.begin_or_resume_task(&state.lease, task_key, input_digest) {
            Ok(task) => Ok(task),
            Err(error) => Err(mark_lost_if_fence_error(&mut state, error)),
        }
    }

    pub(crate) fn load_task(&self, task_key: &str) -> Result<StoredDurableTask, ActiveSessionError> {
        let mut state = self.state.lock().map_err(|_| ActiveSessionError::LockPoisoned)?;
        require_trusted(&state)?;
        match self.store.load_task(&state.lease, task_key) {
            Ok(Some(task)) => Ok(task),
            Ok(None) => Err(ActiveSessionError::Store(SessionStoreError::TaskNotFound {
                session_id: state.lease.session_id.clone(),
                task_key: task_key.to_owned(),
            })),
            Err(error) => Err(mark_lost_if_fence_error(&mut state, error)),
        }
    }

    pub(crate) fn transition_task(
        &self,
        task_key: &str,
        phase: DurableTaskPhase,
        call_id: Option<&str>,
        terminal_result_json: Option<&[u8]>,
    ) -> Result<StoredDurableTask, ActiveSessionError> {
        let mut state = self.state.lock().map_err(|_| ActiveSessionError::LockPoisoned)?;
        require_trusted(&state)?;
        let current = match self.store.load_task(&state.lease, task_key) {
            Ok(Some(task)) => task,
            Ok(None) => {
                return Err(ActiveSessionError::Store(SessionStoreError::TaskNotFound {
                    session_id: state.lease.session_id.clone(),
                    task_key: task_key.to_owned(),
                }));
            }
            Err(error) => return Err(mark_lost_if_fence_error(&mut state, error)),
        };
        match self.store.transition_task(
            &state.lease,
            task_key,
            current.task_revision,
            phase,
            call_id,
            terminal_result_json,
        ) {
            Ok(task) => Ok(task),
            Err(error) => Err(mark_lost_if_fence_error(&mut state, error)),
        }
    }
}

pub(super) struct ActiveSessionLease {
    store: SessionStore,
    state: Arc<Mutex<LeaseState>>,
    stop: Option<Sender<()>>,
    heartbeat: Option<JoinHandle<()>>,
}

impl ActiveSessionLease {
    pub(super) fn start(store: SessionStore, lease: SessionLease) -> Result<Self, ActiveSessionError> {
        let state = Arc::new(Mutex::new(LeaseState {
            lease,
            lost_reason: None,
            released: false,
        }));
        let (stop, receiver) = mpsc::channel();
        let thread_store = store.clone();
        let thread_state = Arc::clone(&state);
        let heartbeat = thread::Builder::new()
            .name("solaris-session-heartbeat".to_owned())
            .spawn(move || {
                loop {
                    match receiver.recv_timeout(Duration::from_secs(DEFAULT_HEARTBEAT_SECONDS as u64)) {
                        Ok(()) | Err(RecvTimeoutError::Disconnected) => break,
                        Err(RecvTimeoutError::Timeout) => {}
                    }
                    let Ok(mut state) = thread_state.lock() else {
                        break;
                    };
                    if state.released || state.lost_reason.is_some() {
                        break;
                    }
                    if let Err(error) = thread_store.heartbeat(&state.lease) {
                        state.lost_reason = Some(error.to_string());
                        tracing::warn!(
                            target: "solaris_agent",
                            session_id = %state.lease.session_id,
                            error = %error,
                            "session heartbeat failed; lease is no longer trusted"
                        );
                        break;
                    }
                }
            })
            .map_err(ActiveSessionError::HeartbeatStart)?;
        Ok(Self {
            store,
            state,
            stop: Some(stop),
            heartbeat: Some(heartbeat),
        })
    }

    pub(super) fn session_id(&self) -> Result<String, ActiveSessionError> {
        let state = self.state.lock().map_err(|_| ActiveSessionError::LockPoisoned)?;
        Ok(state.lease.session_id.clone())
    }

    pub(super) fn ensure_current(&self) -> Result<(), ActiveSessionError> {
        self.fence().ensure_current()
    }

    pub(super) fn fence(&self) -> ActiveSessionFence {
        ActiveSessionFence {
            store: self.store.clone(),
            state: Arc::clone(&self.state),
        }
    }

    pub(super) fn save(&self, session: &Session) -> Result<(), ActiveSessionError> {
        let mut state = self.state.lock().map_err(|_| ActiveSessionError::LockPoisoned)?;
        require_trusted(&state)?;
        if let Err(error) = self.store.save(&mut state.lease, session) {
            return Err(mark_lost(&mut state, error));
        }
        Ok(())
    }

    pub(super) fn checkpoint_task_user(
        &self,
        session: &Session,
        task_key: &str,
    ) -> Result<StoredDurableTask, ActiveSessionError> {
        let mut state = self.state.lock().map_err(|_| ActiveSessionError::LockPoisoned)?;
        require_trusted(&state)?;
        match self.store.checkpoint_task_user(&mut state.lease, session, task_key) {
            Ok(task) => Ok(task),
            Err(error) => Err(mark_lost_if_fence_error(&mut state, error)),
        }
    }

    pub(super) fn release(&mut self) -> Result<(), ActiveSessionError> {
        self.stop_heartbeat()?;
        let mut state = self.state.lock().map_err(|_| ActiveSessionError::LockPoisoned)?;
        if state.released {
            return Ok(());
        }
        require_trusted(&state)?;
        if let Err(error) = self.store.release(&state.lease) {
            return Err(mark_lost(&mut state, error));
        }
        state.released = true;
        Ok(())
    }

    fn stop_heartbeat(&mut self) -> Result<(), ActiveSessionError> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(heartbeat) = self.heartbeat.take()
            && heartbeat.join().is_err()
        {
            return Err(ActiveSessionError::HeartbeatJoin);
        }
        Ok(())
    }
}

impl Drop for ActiveSessionLease {
    fn drop(&mut self) {
        if let Err(error) = self.stop_heartbeat() {
            tracing::error!(
                target: "solaris_agent",
                error = %error,
                "failed to stop session heartbeat during drop"
            );
        }
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if state.released || state.lost_reason.is_some() {
            return;
        }
        match self.store.release(&state.lease) {
            Ok(()) => state.released = true,
            Err(error) => {
                state.lost_reason = Some(error.to_string());
                tracing::warn!(
                    target: "solaris_agent",
                    session_id = %state.lease.session_id,
                    error = %error,
                    "session lease release during drop failed"
                );
            }
        }
    }
}

fn require_trusted(state: &LeaseState) -> Result<(), ActiveSessionError> {
    if let Some(reason) = &state.lost_reason {
        return Err(ActiveSessionError::LeaseLost {
            session_id: state.lease.session_id.clone(),
            reason: reason.clone(),
        });
    }
    if state.released {
        return Err(ActiveSessionError::LeaseLost {
            session_id: state.lease.session_id.clone(),
            reason: "lease was released".to_owned(),
        });
    }
    Ok(())
}

fn mark_lost(state: &mut LeaseState, error: SessionStoreError) -> ActiveSessionError {
    let reason = error.to_string();
    state.lost_reason = Some(reason.clone());
    ActiveSessionError::LeaseLost {
        session_id: state.lease.session_id.clone(),
        reason,
    }
}

fn mark_lost_if_fence_error(state: &mut LeaseState, error: SessionStoreError) -> ActiveSessionError {
    if matches!(
        error,
        SessionStoreError::StaleLease { .. } | SessionStoreError::RevisionConflict { .. }
    ) {
        return mark_lost(state, error);
    }
    ActiveSessionError::Store(error)
}
