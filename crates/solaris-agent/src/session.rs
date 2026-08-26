use std::path::PathBuf;
use std::sync::Mutex;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use solaris_config::hooks::HooksConfig;
use solaris_types::message::{ContentBlock, Message, Role, TokenUsage};

use crate::memory_runtime::PreparedSessionMemorySnapshot;
use crate::runtime_ledger::RuntimeLedger;

#[path = "session/host_outbox.rs"]
mod host_outbox;
#[path = "session/lease.rs"]
mod lease;
pub(crate) mod store;

pub use host_outbox::{HostOutbox, HostOutboxAckOutcome, HostOutboxDelivery, HostOutboxError};
use lease::ActiveSessionLease;
pub(crate) use lease::{ActiveSessionError, ActiveSessionFence};
pub(crate) use store::{DurableTaskPhase, StoredDurableTask};
use store::{SessionGcReport, SessionStore, SessionStoreError};

/// Engine state that changes at runtime and must survive a process restart.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRuntimeState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    #[serde(default)]
    pub allow_list: Vec<String>,
    #[serde(default)]
    pub plan_active: bool,
    #[serde(default)]
    pub pre_plan_allow_list: Vec<String>,
    #[serde(default)]
    pub hooks: HooksConfig,
    /// Immutable long-term Memory view captured when this durable session was
    /// first created. The referenced bytes live in protected Runtime state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_snapshot: Option<SessionMemorySnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionMemorySnapshot {
    pub format_version: u32,
    pub digest_sha256: String,
    pub encoded_bytes: u64,
    pub captured_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub provider: String,
    #[serde(default)]
    pub model: String,
    pub cwd: String,
    pub total_usage: TokenUsage,
    pub messages: Vec<Message>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_state: Option<SessionRuntimeState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionIndex {
    pub sessions: Vec<SessionMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionMeta {
    pub id: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub model: String,
    /// First user message, truncated to 80 chars
    pub summary: String,
    pub message_count: usize,
}

pub struct SessionManager {
    directory: PathBuf,
    max_sessions: usize,
    owner_id: String,
    store: Mutex<Option<SessionStore>>,
    active: Mutex<Option<ActiveSessionLease>>,
    initial_memory_snapshot: Mutex<Option<PreparedSessionMemorySnapshot>>,
}

impl SessionManager {
    pub fn new(directory: PathBuf, max_sessions: usize) -> Self {
        Self {
            directory,
            max_sessions,
            owner_id: format!("session-owner-{}", Uuid::now_v7()),
            store: Mutex::new(None),
            active: Mutex::new(None),
            initial_memory_snapshot: Mutex::new(None),
        }
    }

    pub(crate) fn set_initial_memory_snapshot(
        &self,
        snapshot: PreparedSessionMemorySnapshot,
    ) -> Result<(), ActiveSessionError> {
        let mut slot = self
            .initial_memory_snapshot
            .lock()
            .map_err(|_| ActiveSessionError::LockPoisoned)?;
        if let Some(existing) = slot.as_ref()
            && existing.reference() != snapshot.reference()
        {
            return Err(ActiveSessionError::InitialMemorySnapshotAlreadySet);
        }
        *slot = Some(snapshot);
        Ok(())
    }

    /// Installs a set-once Memory snapshot on an already leased session.
    ///
    /// The reference is saved through the owner/epoch/revision CAS before the
    /// protected blob is written. A crash between the two operations therefore
    /// leaves a durable missing reference that fails closed on restart instead
    /// of recapturing newer Memory contents.
    pub(crate) fn install_active_memory_snapshot(
        &self,
        session: &mut Session,
        snapshot: &PreparedSessionMemorySnapshot,
    ) -> Result<(), ActiveSessionError> {
        self.install_active_memory_snapshot_with_observer(session, snapshot, || Ok(()))
    }

    fn install_active_memory_snapshot_with_observer(
        &self,
        session: &mut Session,
        snapshot: &PreparedSessionMemorySnapshot,
        observer: impl FnOnce() -> std::io::Result<()>,
    ) -> Result<(), ActiveSessionError> {
        let existing = session
            .runtime_state
            .as_ref()
            .and_then(|state| state.memory_snapshot.as_ref());
        match existing {
            Some(existing) if existing != snapshot.reference() => {
                return Err(ActiveSessionError::MemorySnapshotAlreadySet {
                    session_id: session.id.clone(),
                });
            }
            Some(_) => {}
            None => {
                let mut updated = session.clone();
                updated
                    .runtime_state
                    .get_or_insert_with(Default::default)
                    .memory_snapshot = Some(snapshot.reference().clone());
                self.save_active_session(&updated)?;
                *session = updated;
            }
        }
        snapshot
            .persist_with_observer(observer)
            .map_err(ActiveSessionError::MemorySnapshotPersistence)
    }

    #[cfg(test)]
    pub(crate) fn install_active_memory_snapshot_with_test_observer(
        &self,
        session: &mut Session,
        snapshot: &PreparedSessionMemorySnapshot,
        observer: impl FnOnce() -> std::io::Result<()>,
    ) -> Result<(), ActiveSessionError> {
        self.install_active_memory_snapshot_with_observer(session, snapshot, observer)
    }

    /// Create a persisted session without retaining an active lease.
    ///
    /// Agent execution uses `create_active_session` instead so creation and
    /// lease acquisition remain one transaction.
    pub fn create(&self, provider: &str, model: &str, cwd: &str, session_id: Option<&str>) -> anyhow::Result<Session> {
        let id = if let Some(custom_id) = session_id {
            custom_id.to_string()
        } else {
            self.generate_unique_id()?
        };
        let now = Utc::now();
        let session = Session {
            id,
            run_id: Some(generate_run_id()),
            created_at: now,
            updated_at: now,
            provider: provider.to_string(),
            model: model.to_string(),
            cwd: cwd.to_string(),
            total_usage: TokenUsage::default(),
            messages: Vec::new(),
            runtime_state: self.initial_runtime_state().map_err(anyhow::Error::new)?,
        };
        let store = self.store()?;
        let owner_id = self.compatibility_owner_id();
        let lease = store.create_active(&session, &owner_id)?;
        if let Some(snapshot) = self.initial_memory_snapshot().map_err(anyhow::Error::new)?
            && let Err(error) = snapshot.persist()
        {
            let release_result = store.release(&lease);
            if let Err(release_error) = release_result {
                anyhow::bail!(
                    "failed to persist durable session Memory snapshot: {error}; failed to release session lease: {release_error}"
                );
            }
            return Err(anyhow::Error::new(error).context("failed to persist durable session Memory snapshot"));
        }
        store.release(&lease)?;
        store.enforce_retention(self.max_sessions)?;
        Ok(session)
    }

    /// Save session state through an active CAS lease.
    ///
    /// This compatibility entry point acquires a short-lived lease when the
    /// manager does not already own the session. Engines use
    /// `save_active_session` and retain their lease across the run.
    pub fn save(&self, session: &Session) -> anyhow::Result<()> {
        if let Some(active_session_id) = self.active_session_id().map_err(anyhow::Error::new)? {
            if active_session_id != session.id {
                anyhow::bail!(
                    "session manager owns active session '{}' and cannot save '{}'",
                    active_session_id,
                    session.id
                );
            }
            return self.save_active_session(session).map_err(anyhow::Error::new);
        }

        let store = self.store()?;
        let owner_id = self.compatibility_owner_id();
        if let Some(stored) = store.load(&session.id)? {
            let active = store.load_active(&session.id, &owner_id)?;
            let mut lease = active.lease;
            let mut persisted = session.clone();
            if persisted.run_id.is_none() {
                persisted.run_id = stored.session.run_id;
            }
            let save_result = store.save(&mut lease, &persisted);
            let release_result = store.release(&lease);
            match (save_result, release_result) {
                (Ok(()), Ok(())) => {}
                (Err(save_error), Ok(())) => return Err(save_error.into()),
                (Ok(()), Err(release_error)) => return Err(release_error.into()),
                (Err(save_error), Err(release_error)) => {
                    anyhow::bail!(
                        "session save failed: {save_error}; failed to release compatibility lease: {release_error}"
                    );
                }
            }
            store.enforce_retention(self.max_sessions)?;
            return Ok(());
        }

        let mut persisted = session.clone();
        if persisted.run_id.as_deref().is_none_or(str::is_empty) {
            persisted.run_id = Some(generate_run_id());
        }
        let lease = store.create_active(&persisted, &owner_id)?;
        store.release(&lease)?;
        store.enforce_retention(self.max_sessions)?;
        Ok(())
    }

    /// Load a session by ID (or "latest")
    pub fn load(&self, id_or_latest: &str) -> anyhow::Result<Session> {
        if id_or_latest == "latest" {
            let sessions = self.list()?;
            let latest = sessions.last().ok_or_else(|| anyhow::anyhow!("No sessions found"))?;
            return self.load(&latest.id);
        }

        self.load_if_exists(id_or_latest)?
            .ok_or_else(|| anyhow::anyhow!("Session '{}' not found", id_or_latest))
    }

    /// Load a session by exact ID, returning `None` when it has not been created yet.
    pub fn load_if_exists(&self, session_id: &str) -> anyhow::Result<Option<Session>> {
        Ok(self.store()?.load(session_id)?.map(|stored| stored.session))
    }

    pub fn open_host_outbox(&self, session_id: &str, run_id: &str) -> Result<HostOutbox, HostOutboxError> {
        HostOutbox::open(self.store().map_err(HostOutboxError::from_store)?, session_id, run_id)
    }

    pub(crate) fn run_due_gc(
        &self,
        ledger: &dyn RuntimeLedger,
        max_jobs: usize,
    ) -> Result<SessionGcReport, SessionStoreError> {
        self.store()?.run_due_gc(ledger, max_jobs)
    }

    /// List all sessions
    pub fn list(&self) -> anyhow::Result<Vec<SessionMeta>> {
        let store = self.store()?;
        store.enforce_retention(self.max_sessions)?;
        Ok(store
            .list()?
            .into_iter()
            .map(|stored| meta_from_session(&stored.session))
            .collect())
    }

    /// Update the session index (public, called from engine after save).
    ///
    /// The new file layout does not maintain a global index as source of truth.
    /// Keep this method as a compatibility shim for existing callers/tests.
    pub fn update_index_for(&self, session: &Session) -> anyhow::Result<()> {
        self.save(session)
    }

    fn generate_unique_id(&self) -> Result<String, ActiveSessionError> {
        let store = self.store()?;
        for _ in 0..10 {
            let id = generate_session_id();
            if store.load(&id)?.is_none() {
                return Ok(id);
            }
        }
        Err(ActiveSessionError::SessionIdExhausted)
    }

    pub(crate) fn create_active_session(
        &self,
        provider: &str,
        model: &str,
        cwd: &str,
        session_id: Option<&str>,
        run_id: &str,
    ) -> Result<Session, ActiveSessionError> {
        self.create_active_session_with_memory_observer(provider, model, cwd, session_id, run_id, || Ok(()))
    }

    fn create_active_session_with_memory_observer(
        &self,
        provider: &str,
        model: &str,
        cwd: &str,
        session_id: Option<&str>,
        run_id: &str,
        observer: impl FnOnce() -> std::io::Result<()>,
    ) -> Result<Session, ActiveSessionError> {
        let mut slot = self.active.lock().map_err(|_| ActiveSessionError::LockPoisoned)?;
        if let Some(active) = slot.as_ref() {
            return Err(ActiveSessionError::AlreadyActive {
                session_id: active.session_id()?,
            });
        }
        let id = match session_id {
            Some(session_id) => session_id.to_owned(),
            None => self.generate_unique_id()?,
        };
        let now = Utc::now();
        let session = Session {
            id,
            run_id: Some(run_id.to_owned()),
            created_at: now,
            updated_at: now,
            provider: provider.to_owned(),
            model: model.to_owned(),
            cwd: cwd.to_owned(),
            total_usage: TokenUsage::default(),
            messages: Vec::new(),
            runtime_state: self.initial_runtime_state()?,
        };
        let store = self.store()?;
        let lease = store.create_active(&session, &self.owner_id)?;
        if let Some(snapshot) = self.initial_memory_snapshot()?
            && let Err(error) = snapshot.persist_with_observer(observer)
        {
            if let Err(release_error) = store.release(&lease) {
                return Err(ActiveSessionError::LeaseLost {
                    session_id: session.id.clone(),
                    reason: format!(
                        "Memory snapshot persistence failed: {error}; lease release failed: {release_error}"
                    ),
                });
            }
            return Err(ActiveSessionError::MemorySnapshotPersistence(error));
        }
        let release_lease = lease.clone();
        let active = match ActiveSessionLease::start(store.clone(), lease) {
            Ok(active) => active,
            Err(error) => {
                if let Err(release_error) = store.release(&release_lease) {
                    tracing::error!(
                        target: "solaris_agent",
                        session_id = %release_lease.session_id,
                        error = %release_error,
                        "failed to release session lease after heartbeat startup failure"
                    );
                }
                return Err(error);
            }
        };
        if let Err(error) = store.enforce_retention(self.max_sessions) {
            drop(active);
            return Err(error.into());
        }
        *slot = Some(active);
        Ok(session)
    }

    #[cfg(test)]
    pub(crate) fn create_active_session_with_memory_test_observer(
        &self,
        provider: &str,
        model: &str,
        cwd: &str,
        session_id: Option<&str>,
        run_id: &str,
        observer: impl FnOnce() -> std::io::Result<()>,
    ) -> Result<Session, ActiveSessionError> {
        self.create_active_session_with_memory_observer(provider, model, cwd, session_id, run_id, observer)
    }

    pub(crate) fn load_active_session(&self, session_id: &str) -> Result<Session, ActiveSessionError> {
        let mut slot = self.active.lock().map_err(|_| ActiveSessionError::LockPoisoned)?;
        if let Some(active) = slot.as_ref() {
            return Err(ActiveSessionError::AlreadyActive {
                session_id: active.session_id()?,
            });
        }
        let store = self.store()?;
        let active = store.load_active(session_id, &self.owner_id)?;
        let session = active.session;
        let release_lease = active.lease.clone();
        let active_lease = match ActiveSessionLease::start(store.clone(), active.lease) {
            Ok(active) => active,
            Err(error) => {
                if let Err(release_error) = store.release(&release_lease) {
                    tracing::error!(
                        target: "solaris_agent",
                        session_id = %release_lease.session_id,
                        error = %release_error,
                        "failed to release resumed session lease after heartbeat startup failure"
                    );
                }
                return Err(error);
            }
        };
        *slot = Some(active_lease);
        Ok(session)
    }

    pub(crate) fn ensure_active_session(&self, session_id: &str) -> Result<(), ActiveSessionError> {
        let active = self.active.lock().map_err(|_| ActiveSessionError::LockPoisoned)?;
        let Some(active) = active.as_ref() else {
            return Err(ActiveSessionError::LeaseLost {
                session_id: session_id.to_owned(),
                reason: "this process does not own an active lease".to_owned(),
            });
        };
        let active_session_id = active.session_id()?;
        if active_session_id != session_id {
            return Err(ActiveSessionError::LeaseLost {
                session_id: session_id.to_owned(),
                reason: format!("this process owns session '{active_session_id}' instead"),
            });
        }
        active.ensure_current()
    }

    pub(crate) fn active_fence(&self, session_id: &str) -> Result<ActiveSessionFence, ActiveSessionError> {
        let active = self.active.lock().map_err(|_| ActiveSessionError::LockPoisoned)?;
        let Some(active) = active.as_ref() else {
            return Err(ActiveSessionError::LeaseLost {
                session_id: session_id.to_owned(),
                reason: "this process does not own an active lease".to_owned(),
            });
        };
        let active_session_id = active.session_id()?;
        if active_session_id != session_id {
            return Err(ActiveSessionError::LeaseLost {
                session_id: session_id.to_owned(),
                reason: format!("this process owns session '{active_session_id}' instead"),
            });
        }
        Ok(active.fence())
    }

    pub(crate) fn active_task_fence(&self, session_id: &str) -> Result<ActiveSessionFence, ActiveSessionError> {
        self.active_fence(session_id)
    }

    pub(crate) fn save_active_session(&self, session: &Session) -> Result<(), ActiveSessionError> {
        let active = self.active.lock().map_err(|_| ActiveSessionError::LockPoisoned)?;
        let Some(active) = active.as_ref() else {
            return Err(ActiveSessionError::LeaseLost {
                session_id: session.id.clone(),
                reason: "this process does not own an active lease".to_owned(),
            });
        };
        active.save(session)
    }

    pub(crate) fn checkpoint_active_task_user(
        &self,
        session: &Session,
        task_key: &str,
    ) -> Result<StoredDurableTask, ActiveSessionError> {
        let active = self.active.lock().map_err(|_| ActiveSessionError::LockPoisoned)?;
        let Some(active) = active.as_ref() else {
            return Err(ActiveSessionError::LeaseLost {
                session_id: session.id.clone(),
                reason: "this process does not own an active lease".to_owned(),
            });
        };
        active.checkpoint_task_user(session, task_key)
    }

    pub(crate) fn release_active_session(&self) -> Result<(), ActiveSessionError> {
        let active = self.active.lock().map_err(|_| ActiveSessionError::LockPoisoned)?.take();
        if let Some(mut active) = active {
            active.release()?;
        }
        Ok(())
    }

    pub(crate) fn active_session_id(&self) -> Result<Option<String>, ActiveSessionError> {
        let active = self.active.lock().map_err(|_| ActiveSessionError::LockPoisoned)?;
        active.as_ref().map(ActiveSessionLease::session_id).transpose()
    }

    fn store(&self) -> Result<SessionStore, SessionStoreError> {
        let mut store = self.store.lock().map_err(|_| SessionStoreError::Io {
            operation: "lock session store",
            source: std::io::Error::other("session store lock poisoned"),
        })?;
        if let Some(store) = store.as_ref() {
            return Ok(store.clone());
        }
        let opened = SessionStore::open(&self.directory)?;
        *store = Some(opened.clone());
        Ok(opened)
    }

    fn compatibility_owner_id(&self) -> String {
        format!("{}:compat:{}", self.owner_id, Uuid::now_v7())
    }

    fn initial_runtime_state(&self) -> Result<Option<SessionRuntimeState>, ActiveSessionError> {
        let snapshot = self.initial_memory_snapshot()?;
        Ok(snapshot.map(|memory_snapshot| SessionRuntimeState {
            memory_snapshot: Some(memory_snapshot.reference().clone()),
            ..SessionRuntimeState::default()
        }))
    }

    fn initial_memory_snapshot(&self) -> Result<Option<PreparedSessionMemorySnapshot>, ActiveSessionError> {
        self.initial_memory_snapshot
            .lock()
            .map_err(|_| ActiveSessionError::LockPoisoned)
            .map(|snapshot| snapshot.clone())
    }
}

fn meta_from_session(session: &Session) -> SessionMeta {
    let summary = session
        .messages
        .iter()
        .find(|m| m.role == Role::User)
        .and_then(|m| {
            m.content.iter().find_map(|c| {
                if let ContentBlock::Text { text } = c {
                    Some(truncate_str(text, 80))
                } else {
                    None
                }
            })
        })
        .unwrap_or_default();

    SessionMeta {
        id: session.id.clone(),
        created_at: session.created_at,
        updated_at: session.updated_at,
        model: session.model.clone(),
        summary,
        message_count: session.messages.len(),
    }
}

fn generate_session_id() -> String {
    Uuid::now_v7().to_string()
}

fn generate_run_id() -> String {
    format!("run-{}", Uuid::now_v7())
}

fn truncate_str(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let truncated: String = s.chars().take(max - 3).collect();
        format!("{}...", truncated)
    }
}

#[cfg(test)]
#[path = "session_test.rs"]
mod session_test;

#[cfg(test)]
#[path = "session_lifecycle_test.rs"]
mod session_lifecycle_test;
