use chrono::Utc;
use rusqlite::{OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};

use super::{
    Session, SessionLease, SessionStore, SessionStoreError, begin_immediate, counter_from_sql, db_error,
    encode_session, lease_expiry, optional_nonempty_run_id, query_revision, query_run_id, require_current_lease,
    require_not_tombstoned, sync_session_run_reference, validate_owner,
};

const TASK_INPUT_DIGEST_BYTES: usize = 32;
const MAX_TASK_KEY_BYTES: usize = 160;
const MAX_CALL_ID_BYTES: usize = 512;
const MAX_TERMINAL_RESULT_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DurableTaskPhase {
    Created,
    UserCheckpointed,
    AwaitingProvider,
    ProviderInFlight,
    ProviderCompleted,
    ToolsInFlight,
    ToolsCompleted,
    Completed,
    OutcomeUnknown,
    Aborted,
}

impl DurableTaskPhase {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::UserCheckpointed => "user_checkpointed",
            Self::AwaitingProvider => "awaiting_provider",
            Self::ProviderInFlight => "provider_in_flight",
            Self::ProviderCompleted => "provider_completed",
            Self::ToolsInFlight => "tools_in_flight",
            Self::ToolsCompleted => "tools_completed",
            Self::Completed => "completed",
            Self::OutcomeUnknown => "outcome_unknown",
            Self::Aborted => "aborted",
        }
    }

    fn parse(value: &str) -> Result<Self, SessionStoreError> {
        match value {
            "created" => Ok(Self::Created),
            "user_checkpointed" => Ok(Self::UserCheckpointed),
            "awaiting_provider" => Ok(Self::AwaitingProvider),
            "provider_in_flight" => Ok(Self::ProviderInFlight),
            "provider_completed" => Ok(Self::ProviderCompleted),
            "tools_in_flight" => Ok(Self::ToolsInFlight),
            "tools_completed" => Ok(Self::ToolsCompleted),
            "completed" => Ok(Self::Completed),
            "outcome_unknown" => Ok(Self::OutcomeUnknown),
            "aborted" => Ok(Self::Aborted),
            _ => Err(SessionStoreError::TaskStateCorrupt),
        }
    }

    fn can_transition_to(self, next: Self) -> bool {
        if self == next {
            return true;
        }
        if matches!(self, Self::Completed | Self::OutcomeUnknown | Self::Aborted) {
            return false;
        }
        matches!(
            next,
            Self::UserCheckpointed
                | Self::AwaitingProvider
                | Self::ProviderInFlight
                | Self::ProviderCompleted
                | Self::ToolsInFlight
                | Self::ToolsCompleted
                | Self::Completed
                | Self::OutcomeUnknown
                | Self::Aborted
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredDurableTask {
    pub(crate) task_key: String,
    pub(crate) input_digest: Vec<u8>,
    pub(crate) phase: DurableTaskPhase,
    pub(crate) call_id: Option<String>,
    pub(crate) session_revision: i64,
    pub(crate) task_revision: i64,
    pub(crate) terminal_result_json: Option<Vec<u8>>,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub(crate) enum TaskTransitionFault {
    BeforeCommit,
    AfterCommit,
}

impl SessionStore {
    #[cfg(test)]
    pub(crate) fn transition_task_with_fault(
        &self,
        lease: &SessionLease,
        task_key: &str,
        expected_task_revision: i64,
        phase: DurableTaskPhase,
        fault: TaskTransitionFault,
    ) -> Result<StoredDurableTask, SessionStoreError> {
        if matches!(fault, TaskTransitionFault::BeforeCommit) {
            return Err(SessionStoreError::Io {
                operation: "injected durable task failure before commit",
                source: std::io::Error::other("injected failure"),
            });
        }
        let _ = self.transition_task(
            lease,
            task_key,
            expected_task_revision,
            phase,
            Some("fault-call-v1"),
            None,
        )?;
        Err(SessionStoreError::Io {
            operation: "injected durable task failure after commit",
            source: std::io::Error::other("injected failure"),
        })
    }

    #[cfg(test)]
    pub(crate) fn checkpoint_task_user_with_fault(
        &self,
        lease: &mut SessionLease,
        session: &Session,
        task_key: &str,
        fault: TaskTransitionFault,
    ) -> Result<StoredDurableTask, SessionStoreError> {
        if matches!(fault, TaskTransitionFault::BeforeCommit) {
            return Err(SessionStoreError::Io {
                operation: "injected user checkpoint failure before commit",
                source: std::io::Error::other("injected failure"),
            });
        }
        let _ = self.checkpoint_task_user(lease, session, task_key)?;
        Err(SessionStoreError::Io {
            operation: "injected user checkpoint failure after commit",
            source: std::io::Error::other("injected failure"),
        })
    }

    pub(crate) fn load_task(
        &self,
        lease: &SessionLease,
        task_key: &str,
    ) -> Result<Option<StoredDurableTask>, SessionStoreError> {
        let mut connection = self.open_connection()?;
        let storage = connection.storage_guard();
        let transaction = begin_immediate(&mut connection, "begin durable task load")?;
        let now_ms = Utc::now().timestamp_millis();
        require_current_lease(&transaction, lease, now_ms)?;
        let task = query_task(&transaction, &lease.session_id, task_key)?;
        storage.verify()?;
        transaction
            .commit()
            .map_err(|source| db_error("commit durable task load", source))?;
        connection.verify_storage_slots()?;
        Ok(task)
    }

    pub(crate) fn begin_or_resume_task(
        &self,
        lease: &SessionLease,
        task_key: &str,
        input_digest: &[u8],
    ) -> Result<StoredDurableTask, SessionStoreError> {
        validate_task_identity(task_key, input_digest)?;
        let mut connection = self.open_connection()?;
        let storage = connection.storage_guard();
        let transaction = begin_immediate(&mut connection, "begin durable task")?;
        let now_ms = Utc::now().timestamp_millis();
        require_current_lease(&transaction, lease, now_ms)?;
        let session_revision = query_revision(&transaction, &lease.session_id)?;
        if session_revision != lease.revision {
            return Err(SessionStoreError::RevisionConflict {
                session_id: lease.session_id.clone(),
                expected: lease.revision,
                actual: session_revision,
            });
        }
        if let Some(task) = query_task(&transaction, &lease.session_id, task_key)? {
            if task.input_digest != input_digest {
                return Err(SessionStoreError::TaskInputConflict {
                    session_id: lease.session_id.clone(),
                    task_key: task_key.to_owned(),
                });
            }
            storage.verify()?;
            transaction
                .commit()
                .map_err(|source| db_error("commit durable task resume", source))?;
            connection.verify_storage_slots()?;
            return Ok(task);
        }
        transaction
            .execute(
                "INSERT INTO durable_agent_tasks
                    (session_id, task_key, input_digest, phase, call_id, session_revision,
                     task_revision, terminal_result_json, updated_at_ms)
                 VALUES (?1, ?2, ?3, 'created', NULL, ?4, 0, NULL, ?5)",
                params![&lease.session_id, task_key, input_digest, session_revision, now_ms],
            )
            .map_err(|source| db_error("insert durable task", source))?;
        storage.verify()?;
        transaction
            .commit()
            .map_err(|source| db_error("commit durable task creation", source))?;
        connection.verify_storage_slots()?;
        Ok(StoredDurableTask {
            task_key: task_key.to_owned(),
            input_digest: input_digest.to_vec(),
            phase: DurableTaskPhase::Created,
            call_id: None,
            session_revision,
            task_revision: 0,
            terminal_result_json: None,
        })
    }

    pub(crate) fn checkpoint_task_user(
        &self,
        lease: &mut SessionLease,
        session: &Session,
        task_key: &str,
    ) -> Result<StoredDurableTask, SessionStoreError> {
        if lease.session_id != session.id {
            return Err(SessionStoreError::SessionIdMismatch {
                lease_session_id: lease.session_id.clone(),
                state_session_id: session.id.clone(),
            });
        }
        validate_owner(&lease.owner_id)?;
        validate_transition_input(task_key, None, None)?;
        let encoded = encode_session(session, "encode durable task user checkpoint")?;
        let mut connection = self.open_connection()?;
        let storage = connection.storage_guard();
        let transaction = begin_immediate(&mut connection, "begin durable task user checkpoint")?;
        let now_ms = Utc::now().timestamp_millis();
        let expires_at_ms = lease_expiry(now_ms);
        require_not_tombstoned(&transaction, &lease.session_id)?;
        let actual_revision = query_revision(&transaction, &lease.session_id)?;
        require_current_lease(&transaction, lease, now_ms)?;
        if actual_revision != lease.revision {
            return Err(SessionStoreError::RevisionConflict {
                session_id: lease.session_id.clone(),
                expected: lease.revision,
                actual: actual_revision,
            });
        }
        let stored_run_id = query_run_id(&transaction, &lease.session_id)?;
        let requested_run_id = optional_nonempty_run_id(session)?;
        if stored_run_id
            .as_deref()
            .is_some_and(|stored| Some(stored) != requested_run_id)
        {
            return Err(SessionStoreError::RunIdConflict {
                session_id: lease.session_id.clone(),
            });
        }
        let current =
            query_task(&transaction, &lease.session_id, task_key)?.ok_or_else(|| SessionStoreError::TaskNotFound {
                session_id: lease.session_id.clone(),
                task_key: task_key.to_owned(),
            })?;
        if current.phase != DurableTaskPhase::Created || current.call_id.is_some() {
            return Err(SessionStoreError::InvalidTaskTransition {
                from: current.phase.as_str(),
                to: DurableTaskPhase::UserCheckpointed.as_str(),
            });
        }
        let next_session_revision =
            lease
                .revision
                .checked_add(1)
                .ok_or_else(|| SessionStoreError::RevisionExhausted {
                    session_id: lease.session_id.clone(),
                })?;
        let next_task_revision = current
            .task_revision
            .checked_add(1)
            .ok_or(SessionStoreError::TaskRevisionExhausted)?;
        let changed_session = transaction
            .execute(
                "UPDATE sessions
                 SET state_json = ?2, revision = ?3, run_id = ?4, updated_at_ms = ?5
                 WHERE session_id = ?1 AND revision = ?6",
                params![
                    &lease.session_id,
                    encoded,
                    next_session_revision,
                    session.run_id.as_deref(),
                    session.updated_at.timestamp_millis(),
                    lease.revision,
                ],
            )
            .map_err(|source| db_error("save durable task user checkpoint", source))?;
        if changed_session != 1 {
            return Err(SessionStoreError::RevisionConflict {
                session_id: lease.session_id.clone(),
                expected: lease.revision,
                actual: query_revision(&transaction, &lease.session_id)?,
            });
        }
        let changed_task = transaction
            .execute(
                "UPDATE durable_agent_tasks
                 SET phase = 'user_checkpointed', session_revision = ?4,
                     task_revision = ?5, updated_at_ms = ?6
                 WHERE session_id = ?1 AND task_key = ?2 AND task_revision = ?3
                   AND phase = 'created' AND call_id IS NULL",
                params![
                    &lease.session_id,
                    task_key,
                    current.task_revision,
                    next_session_revision,
                    next_task_revision,
                    now_ms,
                ],
            )
            .map_err(|source| db_error("record durable task user checkpoint", source))?;
        if changed_task != 1 {
            return Err(SessionStoreError::TaskRevisionConflict {
                task_key: task_key.to_owned(),
                expected: current.task_revision,
                actual: query_task(&transaction, &lease.session_id, task_key)?
                    .map_or(current.task_revision, |task| task.task_revision),
            });
        }
        transaction
            .execute(
                "UPDATE session_leases
                 SET heartbeat_at_ms = ?4, expires_at_ms = ?5
                 WHERE session_id = ?1 AND owner_id = ?2 AND epoch = ?3",
                params![&lease.session_id, &lease.owner_id, lease.epoch, now_ms, expires_at_ms],
            )
            .map_err(|source| db_error("refresh lease after durable task user checkpoint", source))?;
        sync_session_run_reference(&transaction, &lease.session_id, requested_run_id, now_ms)?;
        storage.verify()?;
        transaction
            .commit()
            .map_err(|source| db_error("commit durable task user checkpoint", source))?;
        connection.verify_storage_slots()?;
        lease.revision = next_session_revision;
        Ok(StoredDurableTask {
            task_key: task_key.to_owned(),
            input_digest: current.input_digest,
            phase: DurableTaskPhase::UserCheckpointed,
            call_id: None,
            session_revision: next_session_revision,
            task_revision: next_task_revision,
            terminal_result_json: None,
        })
    }

    pub(crate) fn transition_task(
        &self,
        lease: &SessionLease,
        task_key: &str,
        expected_task_revision: i64,
        phase: DurableTaskPhase,
        call_id: Option<&str>,
        terminal_result_json: Option<&[u8]>,
    ) -> Result<StoredDurableTask, SessionStoreError> {
        validate_transition_input(task_key, call_id, terminal_result_json)?;
        let mut connection = self.open_connection()?;
        let storage = connection.storage_guard();
        let transaction = begin_immediate(&mut connection, "begin durable task transition")?;
        let now_ms = Utc::now().timestamp_millis();
        require_current_lease(&transaction, lease, now_ms)?;
        let session_revision = query_revision(&transaction, &lease.session_id)?;
        if session_revision != lease.revision {
            return Err(SessionStoreError::RevisionConflict {
                session_id: lease.session_id.clone(),
                expected: lease.revision,
                actual: session_revision,
            });
        }
        let current =
            query_task(&transaction, &lease.session_id, task_key)?.ok_or_else(|| SessionStoreError::TaskNotFound {
                session_id: lease.session_id.clone(),
                task_key: task_key.to_owned(),
            })?;
        if current.task_revision != expected_task_revision {
            return Err(SessionStoreError::TaskRevisionConflict {
                task_key: task_key.to_owned(),
                expected: expected_task_revision,
                actual: current.task_revision,
            });
        }
        if current.phase == phase
            && current.call_id.as_deref() == call_id
            && current.terminal_result_json.as_deref() == terminal_result_json
        {
            storage.verify()?;
            transaction
                .commit()
                .map_err(|source| db_error("commit idempotent durable task transition", source))?;
            connection.verify_storage_slots()?;
            return Ok(current);
        }
        if !current.phase.can_transition_to(phase) {
            return Err(SessionStoreError::InvalidTaskTransition {
                from: current.phase.as_str(),
                to: phase.as_str(),
            });
        }
        if phase == DurableTaskPhase::Completed && terminal_result_json.is_none() {
            return Err(SessionStoreError::InvalidTaskInput);
        }
        if phase != DurableTaskPhase::Completed && terminal_result_json.is_some() {
            return Err(SessionStoreError::InvalidTaskInput);
        }
        let next_revision = expected_task_revision
            .checked_add(1)
            .ok_or(SessionStoreError::TaskRevisionExhausted)?;
        let changed = transaction
            .execute(
                "UPDATE durable_agent_tasks
                 SET phase = ?4, call_id = ?5, session_revision = ?6, task_revision = ?7,
                     terminal_result_json = ?8, updated_at_ms = ?9
                 WHERE session_id = ?1 AND task_key = ?2 AND task_revision = ?3",
                params![
                    &lease.session_id,
                    task_key,
                    expected_task_revision,
                    phase.as_str(),
                    call_id,
                    session_revision,
                    next_revision,
                    terminal_result_json,
                    now_ms,
                ],
            )
            .map_err(|source| db_error("transition durable task", source))?;
        if changed != 1 {
            return Err(SessionStoreError::TaskRevisionConflict {
                task_key: task_key.to_owned(),
                expected: expected_task_revision,
                actual: query_task(&transaction, &lease.session_id, task_key)?
                    .map_or(expected_task_revision, |task| task.task_revision),
            });
        }
        storage.verify()?;
        transaction
            .commit()
            .map_err(|source| db_error("commit durable task transition", source))?;
        connection.verify_storage_slots()?;
        Ok(StoredDurableTask {
            task_key: task_key.to_owned(),
            input_digest: current.input_digest,
            phase,
            call_id: call_id.map(str::to_owned),
            session_revision,
            task_revision: next_revision,
            terminal_result_json: terminal_result_json.map(<[u8]>::to_vec),
        })
    }
}

fn query_task(
    transaction: &Transaction<'_>,
    session_id: &str,
    task_key: &str,
) -> Result<Option<StoredDurableTask>, SessionStoreError> {
    transaction
        .query_row(
            "SELECT input_digest, phase, call_id, session_revision, task_revision, terminal_result_json
             FROM durable_agent_tasks WHERE session_id = ?1 AND task_key = ?2",
            params![session_id, task_key],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, Option<Vec<u8>>>(5)?,
                ))
            },
        )
        .optional()
        .map_err(|source| db_error("query durable task", source))?
        .map(
            |(input_digest, phase, call_id, session_revision, task_revision, terminal_result_json)| {
                Ok(StoredDurableTask {
                    task_key: task_key.to_owned(),
                    input_digest,
                    phase: DurableTaskPhase::parse(&phase)?,
                    call_id,
                    session_revision: counter_from_sql(session_revision)?,
                    task_revision: counter_from_sql(task_revision)?,
                    terminal_result_json,
                })
            },
        )
        .transpose()
}

fn validate_task_identity(task_key: &str, input_digest: &[u8]) -> Result<(), SessionStoreError> {
    if task_key.is_empty()
        || task_key.len() > MAX_TASK_KEY_BYTES
        || task_key.contains('\0')
        || input_digest.len() != TASK_INPUT_DIGEST_BYTES
    {
        return Err(SessionStoreError::InvalidTaskInput);
    }
    Ok(())
}

fn validate_transition_input(
    task_key: &str,
    call_id: Option<&str>,
    terminal_result_json: Option<&[u8]>,
) -> Result<(), SessionStoreError> {
    if task_key.is_empty() || task_key.len() > MAX_TASK_KEY_BYTES || task_key.contains('\0') {
        return Err(SessionStoreError::InvalidTaskInput);
    }
    if call_id.is_some_and(|value| value.is_empty() || value.len() > MAX_CALL_ID_BYTES || value.contains('\0')) {
        return Err(SessionStoreError::InvalidTaskInput);
    }
    if terminal_result_json.is_some_and(|value| value.len() > MAX_TERMINAL_RESULT_BYTES) {
        return Err(SessionStoreError::InvalidTaskInput);
    }
    Ok(())
}
