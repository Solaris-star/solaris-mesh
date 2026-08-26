use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, Transaction, params};

use super::{DEFAULT_LEASE_SECONDS, SessionStore, SessionStoreError, begin_immediate, counter_from_sql, db_error};

#[path = "store_conversation_support.rs"]
mod store_conversation_support;
use store_conversation_support::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConversationIdentity {
    pub(crate) schema_version: u8,
    pub(crate) run_id: String,
    pub(crate) parent_agent_id: String,
    pub(crate) conversation_id: String,
    pub(crate) agent_id: String,
    pub(crate) session_id: String,
    pub(crate) task_id: String,
    pub(crate) open_operation_id: String,
    pub(crate) spec_digest: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConversationState {
    Opening,
    Open,
    Closing,
    Closed,
}

impl ConversationState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Opening => "opening",
            Self::Open => "open",
            Self::Closing => "closing",
            Self::Closed => "closed",
        }
    }

    fn parse(value: &str) -> Result<Self, SessionStoreError> {
        match value {
            "opening" => Ok(Self::Opening),
            "open" => Ok(Self::Open),
            "closing" => Ok(Self::Closing),
            "closed" => Ok(Self::Closed),
            _ => Err(SessionStoreError::ConversationStateCorrupt),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct StoredConversation {
    pub(crate) identity: ConversationIdentity,
    pub(crate) state: ConversationState,
    pub(crate) revision: i64,
    pub(crate) opening_owner: Option<String>,
    pub(crate) opening_epoch: i64,
    pub(crate) opening_expires_at_ms: Option<i64>,
    pub(crate) handle_json: Option<Vec<u8>>,
    pub(crate) next_sequence: u64,
    pub(crate) next_to_run: u64,
    pub(crate) active_sequence: Option<u64>,
    pub(crate) terminal_state: Option<String>,
    pub(crate) failure_class: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) enum ConversationOpenClaim {
    Owned { revision: i64, epoch: i64 },
    Existing(StoredConversation),
    Busy(#[allow(dead_code)] StoredConversation),
}

#[derive(Debug, Clone)]
pub(crate) enum ConversationCloseClaim {
    Started(StoredConversation),
    Existing(StoredConversation),
    AlreadyClosed(StoredConversation),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConversationTurnIdentity {
    pub(crate) turn_id: String,
    pub(crate) operation_id: String,
    pub(crate) message_id: String,
    pub(crate) input_digest: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConversationTurnState {
    Queued,
    Admitted,
    IntentCommitted,
    Completed,
    Failed,
    Cancelled,
    OutcomeUnknown,
    ReconciliationRequired,
}

impl ConversationTurnState {
    pub(crate) fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::OutcomeUnknown | Self::ReconciliationRequired
        )
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Admitted => "admitted",
            Self::IntentCommitted => "intent_committed",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::OutcomeUnknown => "outcome_unknown",
            Self::ReconciliationRequired => "reconciliation_required",
        }
    }

    fn parse(value: &str) -> Result<Self, SessionStoreError> {
        match value {
            "queued" => Ok(Self::Queued),
            "admitted" => Ok(Self::Admitted),
            "intent_committed" => Ok(Self::IntentCommitted),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            "outcome_unknown" => Ok(Self::OutcomeUnknown),
            "reconciliation_required" => Ok(Self::ReconciliationRequired),
            _ => Err(SessionStoreError::ConversationStateCorrupt),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct StoredConversationTurn {
    pub(crate) identity: ConversationTurnIdentity,
    pub(crate) sequence: u64,
    pub(crate) state: ConversationTurnState,
    pub(crate) revision: i64,
    pub(crate) claim_owner: Option<String>,
    pub(crate) claim_expires_at_ms: Option<i64>,
    pub(crate) outcome_json: Option<Vec<u8>>,
    pub(crate) failure_class: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) enum ConversationTurnEnqueue {
    Inserted,
    Existing(StoredConversationTurn),
    Blocked { failure_class: String },
}

#[derive(Debug, Clone)]
pub(crate) enum ConversationTurnClaim {
    Claimed(StoredConversationTurn),
    Pending(#[allow(dead_code)] StoredConversationTurn),
    Blocked { failure_class: String },
    IntentCommitted(StoredConversationTurn),
    Terminal(StoredConversationTurn),
    ConversationNotOpen(#[allow(dead_code)] ConversationState),
}

pub(crate) struct ConversationTurnCompletion {
    pub(crate) state: ConversationTurnState,
    pub(crate) outcome_json: Vec<u8>,
    pub(crate) failure_class: Option<String>,
    pub(crate) block_following_turns: bool,
}

impl SessionStore {
    #[cfg(test)]
    pub(crate) fn claim_conversation_open(
        &self,
        identity: &ConversationIdentity,
        owner_id: &str,
    ) -> Result<ConversationOpenClaim, SessionStoreError> {
        self.claim_conversation_open_with_duration(identity, owner_id, DEFAULT_LEASE_SECONDS * 1_000)
    }

    pub(crate) fn claim_conversation_open_with_duration(
        &self,
        identity: &ConversationIdentity,
        owner_id: &str,
        lease_duration_ms: i64,
    ) -> Result<ConversationOpenClaim, SessionStoreError> {
        self.claim_conversation_open_with_duration_at(
            identity,
            owner_id,
            lease_duration_ms,
            Utc::now().timestamp_millis(),
        )
    }

    fn claim_conversation_open_with_duration_at(
        &self,
        identity: &ConversationIdentity,
        owner_id: &str,
        lease_duration_ms: i64,
        now_ms: i64,
    ) -> Result<ConversationOpenClaim, SessionStoreError> {
        validate_identity(identity)?;
        validate_nonempty(owner_id)?;
        validate_lease_duration(lease_duration_ms)?;
        let mut connection = self.open_connection()?;
        let storage = connection.storage_guard();
        let transaction = begin_immediate(&mut connection, "begin Agent conversation open claim")?;
        if let Some(stored) = query_conversation(&transaction, identity)? {
            require_identity(&stored, identity)?;
            let maximum_expiry_ms = opening_lease_expiry(now_ms, lease_duration_ms)?;
            let claim = if stored.state != ConversationState::Opening {
                ConversationOpenClaim::Existing(stored)
            } else if stored.opening_owner.as_deref() == Some(owner_id) {
                renew_opening_lease(
                    &transaction,
                    identity,
                    owner_id,
                    stored.opening_epoch,
                    stored.revision,
                    now_ms,
                    lease_duration_ms,
                )?;
                ConversationOpenClaim::Owned {
                    revision: stored.revision,
                    epoch: stored.opening_epoch,
                }
            } else if stored
                .opening_expires_at_ms
                .is_some_and(|expires| expires <= now_ms || expires > maximum_expiry_ms)
            {
                let next_revision = next_revision(stored.revision)?;
                let next_epoch = next_epoch(stored.opening_epoch)?;
                let expires_at_ms = opening_lease_expiry(now_ms, lease_duration_ms)?;
                let changed = transaction
                    .execute(
                        "UPDATE agent_conversations
                         SET revision = ?4, opening_owner = ?5, opening_epoch = ?6,
                             opening_expires_at_ms = ?7, updated_at_ms = ?8
                         WHERE run_id = ?1 AND parent_agent_id = ?2 AND conversation_id = ?3
                           AND state = 'opening' AND revision = ?9
                           AND opening_epoch = ?10 AND opening_owner = ?11
                           AND opening_expires_at_ms = ?12",
                        params![
                            &identity.run_id,
                            &identity.parent_agent_id,
                            &identity.conversation_id,
                            next_revision,
                            owner_id,
                            next_epoch,
                            expires_at_ms,
                            now_ms,
                            stored.revision,
                            stored.opening_epoch,
                            stored
                                .opening_owner
                                .as_deref()
                                .ok_or(SessionStoreError::ConversationStateCorrupt)?,
                            stored
                                .opening_expires_at_ms
                                .ok_or(SessionStoreError::ConversationStateCorrupt)?,
                        ],
                    )
                    .map_err(|source| db_error("take over expired Agent conversation opening lease", source))?;
                require_changed(changed, stored.revision, &transaction, identity)?;
                ConversationOpenClaim::Owned {
                    revision: next_revision,
                    epoch: next_epoch,
                }
            } else {
                ConversationOpenClaim::Busy(stored)
            };
            storage.verify()?;
            transaction
                .commit()
                .map_err(|source| db_error("commit existing Agent conversation open claim", source))?;
            connection.verify_storage_slots()?;
            return Ok(claim);
        }
        require_unique_conversation_binding(&transaction, identity)?;
        transaction
            .execute(
                "INSERT INTO agent_conversations (
                    run_id, parent_agent_id, conversation_id, schema_version, state, revision,
                    opening_owner, agent_id, session_id, task_id, open_operation_id, spec_digest,
                    handle_json, next_sequence, next_to_run, active_sequence, terminal_state,
                    failure_class, updated_at_ms, opening_epoch, opening_expires_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, 'opening', 0, ?5, ?6, ?7, ?8, ?9, ?10,
                           NULL, 0, 0, NULL, NULL, NULL, ?11, 1, ?12)",
                params![
                    &identity.run_id,
                    &identity.parent_agent_id,
                    &identity.conversation_id,
                    i64::from(identity.schema_version),
                    owner_id,
                    &identity.agent_id,
                    &identity.session_id,
                    &identity.task_id,
                    &identity.open_operation_id,
                    &identity.spec_digest,
                    now_ms,
                    opening_lease_expiry(now_ms, lease_duration_ms)?,
                ],
            )
            .map_err(|source| db_error("insert Agent conversation open claim", source))?;
        storage.verify()?;
        transaction
            .commit()
            .map_err(|source| db_error("commit Agent conversation open claim", source))?;
        connection.verify_storage_slots()?;
        Ok(ConversationOpenClaim::Owned { revision: 0, epoch: 1 })
    }

    pub(crate) fn finalize_conversation_open(
        &self,
        identity: &ConversationIdentity,
        owner_id: &str,
        expected_epoch: i64,
        expected_revision: i64,
        handle_json: &[u8],
    ) -> Result<StoredConversation, SessionStoreError> {
        self.finish_open(
            identity,
            Some(owner_id),
            Some(expected_epoch),
            Some(expected_revision),
            handle_json,
        )
    }

    pub(crate) fn recover_conversation_open(
        &self,
        identity: &ConversationIdentity,
        handle_json: &[u8],
    ) -> Result<StoredConversation, SessionStoreError> {
        self.finish_open(identity, None, None, None, handle_json)
    }

    fn finish_open(
        &self,
        identity: &ConversationIdentity,
        owner_id: Option<&str>,
        expected_epoch: Option<i64>,
        expected_revision: Option<i64>,
        handle_json: &[u8],
    ) -> Result<StoredConversation, SessionStoreError> {
        validate_identity(identity)?;
        validate_handle(handle_json)?;
        if let Some(owner_id) = owner_id {
            validate_nonempty(owner_id)?;
        }
        let mut connection = self.open_connection()?;
        let storage = connection.storage_guard();
        let transaction = begin_immediate(&mut connection, "begin Agent conversation open finalize")?;
        let stored = require_conversation(&transaction, identity)?;
        require_identity(&stored, identity)?;
        match stored.state {
            ConversationState::Open => {
                require_handle(&stored, handle_json)?;
                storage.verify()?;
                transaction
                    .commit()
                    .map_err(|source| db_error("commit idempotent Agent conversation open finalize", source))?;
                connection.verify_storage_slots()?;
                return Ok(stored);
            }
            ConversationState::Opening => {}
            state => return Err(state_conflict(state)),
        }
        if owner_id.is_some_and(|owner| stored.opening_owner.as_deref() != Some(owner)) {
            return Err(SessionStoreError::ConversationRevisionConflict {
                expected: expected_revision.unwrap_or(stored.revision),
                actual: stored.revision,
            });
        }
        if expected_epoch.is_some_and(|epoch| epoch != stored.opening_epoch) {
            return Err(SessionStoreError::ConversationRevisionConflict {
                expected: expected_revision.unwrap_or(stored.revision),
                actual: stored.revision,
            });
        }
        if expected_revision.is_some_and(|revision| revision != stored.revision) {
            return Err(SessionStoreError::ConversationRevisionConflict {
                expected: expected_revision.unwrap_or_default(),
                actual: stored.revision,
            });
        }
        let next_revision = next_revision(stored.revision)?;
        let changed = if let Some(owner_id) = owner_id {
            transaction.execute(
                "UPDATE agent_conversations
                 SET state = 'open', revision = ?4, opening_owner = NULL,
                     opening_expires_at_ms = NULL, handle_json = ?5, updated_at_ms = ?6
                 WHERE run_id = ?1 AND parent_agent_id = ?2 AND conversation_id = ?3
                   AND state = 'opening' AND revision = ?7
                   AND opening_owner = ?8 AND opening_epoch = ?9",
                params![
                    &identity.run_id,
                    &identity.parent_agent_id,
                    &identity.conversation_id,
                    next_revision,
                    handle_json,
                    Utc::now().timestamp_millis(),
                    stored.revision,
                    owner_id,
                    expected_epoch.ok_or(SessionStoreError::InvalidConversationInput)?,
                ],
            )
        } else {
            transaction.execute(
                "UPDATE agent_conversations
                 SET state = 'open', revision = ?4, opening_owner = NULL,
                     opening_expires_at_ms = NULL, handle_json = ?5, updated_at_ms = ?6
                 WHERE run_id = ?1 AND parent_agent_id = ?2 AND conversation_id = ?3
                   AND state = 'opening' AND revision = ?7",
                params![
                    &identity.run_id,
                    &identity.parent_agent_id,
                    &identity.conversation_id,
                    next_revision,
                    handle_json,
                    Utc::now().timestamp_millis(),
                    stored.revision,
                ],
            )
        }
        .map_err(|source| db_error("finalize Agent conversation open", source))?;
        require_changed(changed, stored.revision, &transaction, identity)?;
        let finalized = require_conversation(&transaction, identity)?;
        storage.verify()?;
        transaction
            .commit()
            .map_err(|source| db_error("commit Agent conversation open finalize", source))?;
        connection.verify_storage_slots()?;
        Ok(finalized)
    }

    pub(crate) fn abandon_conversation_open(
        &self,
        identity: &ConversationIdentity,
        owner_id: &str,
        expected_epoch: i64,
        expected_revision: i64,
    ) -> Result<bool, SessionStoreError> {
        validate_identity(identity)?;
        validate_nonempty(owner_id)?;
        let mut connection = self.open_connection()?;
        let storage = connection.storage_guard();
        let transaction = begin_immediate(&mut connection, "begin Agent conversation open abandon")?;
        let changed = transaction
            .execute(
                "DELETE FROM agent_conversations
                 WHERE run_id = ?1 AND parent_agent_id = ?2 AND conversation_id = ?3
                   AND state = 'opening' AND opening_owner = ?4 AND revision = ?5
                   AND opening_epoch = ?6 AND handle_json IS NULL",
                params![
                    &identity.run_id,
                    &identity.parent_agent_id,
                    &identity.conversation_id,
                    owner_id,
                    expected_revision,
                    expected_epoch,
                ],
            )
            .map_err(|source| db_error("abandon Agent conversation open", source))?;
        storage.verify()?;
        transaction
            .commit()
            .map_err(|source| db_error("commit Agent conversation open abandon", source))?;
        connection.verify_storage_slots()?;
        Ok(changed == 1)
    }

    pub(crate) fn begin_conversation_close(
        &self,
        identity: &ConversationIdentity,
        handle_json: &[u8],
    ) -> Result<ConversationCloseClaim, SessionStoreError> {
        validate_identity(identity)?;
        validate_handle(handle_json)?;
        let mut connection = self.open_connection()?;
        let storage = connection.storage_guard();
        let transaction = begin_immediate(&mut connection, "begin Agent conversation close")?;
        let stored = require_conversation(&transaction, identity)?;
        require_identity(&stored, identity)?;
        require_handle(&stored, handle_json)?;
        let claim = match stored.state {
            ConversationState::Open => {
                let next_revision = next_revision(stored.revision)?;
                let changed = transaction
                    .execute(
                        "UPDATE agent_conversations
                         SET state = 'closing', revision = ?4, updated_at_ms = ?5
                         WHERE run_id = ?1 AND parent_agent_id = ?2 AND conversation_id = ?3
                           AND state = 'open' AND revision = ?6",
                        params![
                            &identity.run_id,
                            &identity.parent_agent_id,
                            &identity.conversation_id,
                            next_revision,
                            Utc::now().timestamp_millis(),
                            stored.revision,
                        ],
                    )
                    .map_err(|source| db_error("claim Agent conversation close", source))?;
                require_changed(changed, stored.revision, &transaction, identity)?;
                ConversationCloseClaim::Started(require_conversation(&transaction, identity)?)
            }
            ConversationState::Closing => ConversationCloseClaim::Existing(stored),
            ConversationState::Closed => ConversationCloseClaim::AlreadyClosed(stored),
            ConversationState::Opening => return Err(state_conflict(ConversationState::Opening)),
        };
        storage.verify()?;
        transaction
            .commit()
            .map_err(|source| db_error("commit Agent conversation close claim", source))?;
        connection.verify_storage_slots()?;
        Ok(claim)
    }

    pub(crate) fn finish_conversation_close(
        &self,
        identity: &ConversationIdentity,
        handle_json: &[u8],
        terminal_state: &str,
        failure_class: Option<&str>,
    ) -> Result<StoredConversation, SessionStoreError> {
        validate_identity(identity)?;
        validate_handle(handle_json)?;
        validate_nonempty(terminal_state)?;
        let mut connection = self.open_connection()?;
        let storage = connection.storage_guard();
        let transaction = begin_immediate(&mut connection, "begin Agent conversation close finalize")?;
        let stored = require_conversation(&transaction, identity)?;
        require_identity(&stored, identity)?;
        require_handle(&stored, handle_json)?;
        if stored.state == ConversationState::Closed {
            if stored.terminal_state.as_deref() != Some(terminal_state)
                || stored.failure_class.as_deref() != failure_class
            {
                return Err(SessionStoreError::ConversationIdentityConflict);
            }
            storage.verify()?;
            transaction
                .commit()
                .map_err(|source| db_error("commit idempotent Agent conversation close finalize", source))?;
            connection.verify_storage_slots()?;
            return Ok(stored);
        }
        if stored.state != ConversationState::Closing || stored.active_sequence.is_some() {
            return Err(state_conflict(stored.state));
        }
        let next_revision = next_revision(stored.revision)?;
        let changed = transaction
            .execute(
                "UPDATE agent_conversations
                 SET state = 'closed', revision = ?4, terminal_state = ?5,
                     failure_class = COALESCE(failure_class, ?6), updated_at_ms = ?7
                 WHERE run_id = ?1 AND parent_agent_id = ?2 AND conversation_id = ?3
                   AND state = 'closing' AND revision = ?8 AND active_sequence IS NULL",
                params![
                    &identity.run_id,
                    &identity.parent_agent_id,
                    &identity.conversation_id,
                    next_revision,
                    terminal_state,
                    failure_class,
                    Utc::now().timestamp_millis(),
                    stored.revision,
                ],
            )
            .map_err(|source| db_error("finalize Agent conversation close", source))?;
        require_changed(changed, stored.revision, &transaction, identity)?;
        let finalized = require_conversation(&transaction, identity)?;
        storage.verify()?;
        transaction
            .commit()
            .map_err(|source| db_error("commit Agent conversation close finalize", source))?;
        connection.verify_storage_slots()?;
        Ok(finalized)
    }

    pub(crate) fn enqueue_conversation_turn(
        &self,
        identity: &ConversationIdentity,
        handle_json: &[u8],
        turn: &ConversationTurnIdentity,
    ) -> Result<ConversationTurnEnqueue, SessionStoreError> {
        validate_identity(identity)?;
        validate_handle(handle_json)?;
        validate_turn_identity(turn)?;
        let mut connection = self.open_connection()?;
        let storage = connection.storage_guard();
        let transaction = begin_immediate(&mut connection, "begin Agent conversation turn enqueue")?;
        let stored = require_conversation(&transaction, identity)?;
        require_identity(&stored, identity)?;
        require_handle(&stored, handle_json)?;
        if let Some(existing) = query_turn_by_id(&transaction, identity, &turn.turn_id)? {
            require_turn_identity(&existing, turn)?;
            storage.verify()?;
            transaction
                .commit()
                .map_err(|source| db_error("commit existing Agent conversation turn enqueue", source))?;
            connection.verify_storage_slots()?;
            return Ok(ConversationTurnEnqueue::Existing(existing));
        }
        if stored.state != ConversationState::Open {
            return Err(state_conflict(stored.state));
        }
        if let Some(failure_class) = stored.failure_class {
            storage.verify()?;
            transaction
                .commit()
                .map_err(|source| db_error("commit blocked Agent conversation turn enqueue", source))?;
            connection.verify_storage_slots()?;
            return Ok(ConversationTurnEnqueue::Blocked { failure_class });
        }
        let sequence = stored.next_sequence;
        let following_sequence = sequence
            .checked_add(1)
            .ok_or(SessionStoreError::ConversationStateCorrupt)?;
        let next_conversation_revision = next_revision(stored.revision)?;
        let now_ms = Utc::now().timestamp_millis();
        let changed = transaction
            .execute(
                "UPDATE agent_conversations
                 SET next_sequence = ?4, revision = ?5, updated_at_ms = ?6
                 WHERE run_id = ?1 AND parent_agent_id = ?2 AND conversation_id = ?3
                   AND state = 'open' AND revision = ?7 AND failure_class IS NULL",
                params![
                    &identity.run_id,
                    &identity.parent_agent_id,
                    &identity.conversation_id,
                    u64_to_sql(following_sequence)?,
                    next_conversation_revision,
                    now_ms,
                    stored.revision,
                ],
            )
            .map_err(|source| db_error("reserve Agent conversation turn sequence", source))?;
        require_changed(changed, stored.revision, &transaction, identity)?;
        transaction
            .execute(
                "INSERT INTO agent_conversation_turns (
                    run_id, parent_agent_id, conversation_id, sequence, turn_id, operation_id,
                    message_id, input_digest, state, revision, claim_owner, claim_expires_at_ms,
                    outcome_json, failure_class, updated_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'queued', 0,
                           NULL, NULL, NULL, NULL, ?9)",
                params![
                    &identity.run_id,
                    &identity.parent_agent_id,
                    &identity.conversation_id,
                    u64_to_sql(sequence)?,
                    &turn.turn_id,
                    &turn.operation_id,
                    &turn.message_id,
                    &turn.input_digest,
                    now_ms,
                ],
            )
            .map_err(|source| db_error("insert Agent conversation turn", source))?;
        let inserted = require_turn_by_sequence(&transaction, identity, sequence)?;
        storage.verify()?;
        transaction
            .commit()
            .map_err(|source| db_error("commit Agent conversation turn enqueue", source))?;
        connection.verify_storage_slots()?;
        let _ = inserted;
        Ok(ConversationTurnEnqueue::Inserted)
    }

    pub(crate) fn claim_conversation_turn(
        &self,
        identity: &ConversationIdentity,
        handle_json: &[u8],
        turn: &ConversationTurnIdentity,
        owner_id: &str,
    ) -> Result<ConversationTurnClaim, SessionStoreError> {
        self.claim_conversation_turn_at(
            identity,
            handle_json,
            turn,
            owner_id,
            Utc::now().timestamp_millis(),
            DEFAULT_LEASE_SECONDS * 1_000,
        )
    }

    pub(crate) fn commit_conversation_turn_intent(
        &self,
        identity: &ConversationIdentity,
        handle_json: &[u8],
        turn: &ConversationTurnIdentity,
        sequence: u64,
        owner_id: &str,
        expected_revision: i64,
    ) -> Result<StoredConversationTurn, SessionStoreError> {
        validate_identity(identity)?;
        validate_handle(handle_json)?;
        validate_turn_identity(turn)?;
        validate_nonempty(owner_id)?;
        let mut connection = self.open_connection()?;
        let storage = connection.storage_guard();
        let transaction = begin_immediate(&mut connection, "begin Agent conversation turn intent")?;
        let conversation = require_conversation(&transaction, identity)?;
        require_identity(&conversation, identity)?;
        require_handle(&conversation, handle_json)?;
        if conversation.state != ConversationState::Open || conversation.active_sequence != Some(sequence) {
            return Err(state_conflict(conversation.state));
        }
        let stored = require_turn_by_sequence(&transaction, identity, sequence)?;
        require_turn_identity(&stored, turn)?;
        if stored.state == ConversationTurnState::IntentCommitted && stored.claim_owner.as_deref() == Some(owner_id) {
            storage.verify()?;
            transaction
                .commit()
                .map_err(|source| db_error("commit idempotent Agent conversation turn intent", source))?;
            connection.verify_storage_slots()?;
            return Ok(stored);
        }
        if stored.state != ConversationTurnState::Admitted || stored.claim_owner.as_deref() != Some(owner_id) {
            return Err(SessionStoreError::ConversationStateConflict {
                state: stored.state.as_str().to_owned(),
            });
        }
        if stored.revision != expected_revision {
            return Err(SessionStoreError::ConversationRevisionConflict {
                expected: expected_revision,
                actual: stored.revision,
            });
        }
        let next_turn_revision = next_revision(stored.revision)?;
        let changed = transaction
            .execute(
                "UPDATE agent_conversation_turns
                 SET state = 'intent_committed', revision = ?5, updated_at_ms = ?6
                 WHERE run_id = ?1 AND parent_agent_id = ?2 AND conversation_id = ?3
                   AND sequence = ?4 AND state = 'admitted' AND revision = ?7 AND claim_owner = ?8",
                params![
                    &identity.run_id,
                    &identity.parent_agent_id,
                    &identity.conversation_id,
                    u64_to_sql(sequence)?,
                    next_turn_revision,
                    Utc::now().timestamp_millis(),
                    stored.revision,
                    owner_id,
                ],
            )
            .map_err(|source| db_error("commit Agent conversation turn intent", source))?;
        if changed != 1 {
            let actual = require_turn_by_sequence(&transaction, identity, sequence)?.revision;
            return Err(SessionStoreError::ConversationRevisionConflict {
                expected: stored.revision,
                actual,
            });
        }
        let committed = require_turn_by_sequence(&transaction, identity, sequence)?;
        storage.verify()?;
        transaction
            .commit()
            .map_err(|source| db_error("commit Agent conversation turn intent transaction", source))?;
        connection.verify_storage_slots()?;
        Ok(committed)
    }

    pub(crate) fn finalize_conversation_turn(
        &self,
        identity: &ConversationIdentity,
        handle_json: &[u8],
        turn: &ConversationTurnIdentity,
        sequence: u64,
        completion: ConversationTurnCompletion,
    ) -> Result<StoredConversationTurn, SessionStoreError> {
        validate_identity(identity)?;
        validate_handle(handle_json)?;
        validate_turn_identity(turn)?;
        if !completion.state.is_terminal() || completion.outcome_json.is_empty() {
            return Err(SessionStoreError::InvalidConversationInput);
        }
        let mut connection = self.open_connection()?;
        let storage = connection.storage_guard();
        let transaction = begin_immediate(&mut connection, "begin Agent conversation turn finalize")?;
        let conversation = require_conversation(&transaction, identity)?;
        require_identity(&conversation, identity)?;
        require_handle(&conversation, handle_json)?;
        let stored = require_turn_by_sequence(&transaction, identity, sequence)?;
        require_turn_identity(&stored, turn)?;
        if stored.state.is_terminal() {
            if stored.state != completion.state
                || stored.outcome_json.as_deref() != Some(completion.outcome_json.as_slice())
                || stored.failure_class != completion.failure_class
            {
                return Err(SessionStoreError::ConversationIdentityConflict);
            }
            storage.verify()?;
            transaction
                .commit()
                .map_err(|source| db_error("commit idempotent Agent conversation turn finalize", source))?;
            connection.verify_storage_slots()?;
            return Ok(stored);
        }
        if stored.sequence != conversation.next_to_run {
            return Err(SessionStoreError::ConversationStateConflict {
                state: stored.state.as_str().to_owned(),
            });
        }
        if conversation.state == ConversationState::Open && stored.state != ConversationTurnState::IntentCommitted {
            return Err(SessionStoreError::ConversationStateConflict {
                state: stored.state.as_str().to_owned(),
            });
        }
        if !matches!(conversation.state, ConversationState::Open | ConversationState::Closing) {
            return Err(state_conflict(conversation.state));
        }
        if conversation.active_sequence.is_some_and(|active| active != sequence) {
            return Err(SessionStoreError::ConversationStateCorrupt);
        }
        let next_turn_revision = next_revision(stored.revision)?;
        let now_ms = Utc::now().timestamp_millis();
        let changed = transaction
            .execute(
                "UPDATE agent_conversation_turns
                 SET state = ?5, revision = ?6, claim_owner = NULL, claim_expires_at_ms = NULL,
                     outcome_json = ?7, failure_class = ?8, updated_at_ms = ?9
                 WHERE run_id = ?1 AND parent_agent_id = ?2 AND conversation_id = ?3
                   AND sequence = ?4 AND revision = ?10
                   AND state IN ('queued', 'admitted', 'intent_committed')",
                params![
                    &identity.run_id,
                    &identity.parent_agent_id,
                    &identity.conversation_id,
                    u64_to_sql(sequence)?,
                    completion.state.as_str(),
                    next_turn_revision,
                    &completion.outcome_json,
                    completion.failure_class.as_deref(),
                    now_ms,
                    stored.revision,
                ],
            )
            .map_err(|source| db_error("finalize Agent conversation turn", source))?;
        if changed != 1 {
            return Err(SessionStoreError::ConversationRevisionConflict {
                expected: stored.revision,
                actual: require_turn_by_sequence(&transaction, identity, sequence)?.revision,
            });
        }
        let next_to_run = sequence
            .checked_add(1)
            .ok_or(SessionStoreError::ConversationStateCorrupt)?;
        let next_conversation_revision = next_revision(conversation.revision)?;
        let blocking_failure = completion
            .block_following_turns
            .then_some(completion.failure_class.as_deref().unwrap_or("unknown"));
        let changed = transaction
            .execute(
                "UPDATE agent_conversations
                 SET next_to_run = ?4, active_sequence = NULL, revision = ?5,
                     failure_class = COALESCE(failure_class, ?6), updated_at_ms = ?7
                 WHERE run_id = ?1 AND parent_agent_id = ?2 AND conversation_id = ?3
                   AND revision = ?8 AND next_to_run = ?9
                   AND (active_sequence IS NULL OR active_sequence = ?9)",
                params![
                    &identity.run_id,
                    &identity.parent_agent_id,
                    &identity.conversation_id,
                    u64_to_sql(next_to_run)?,
                    next_conversation_revision,
                    blocking_failure,
                    now_ms,
                    conversation.revision,
                    u64_to_sql(sequence)?,
                ],
            )
            .map_err(|source| db_error("advance Agent conversation turn queue", source))?;
        require_changed(changed, conversation.revision, &transaction, identity)?;
        let finalized = require_turn_by_sequence(&transaction, identity, sequence)?;
        storage.verify()?;
        transaction
            .commit()
            .map_err(|source| db_error("commit Agent conversation turn finalize", source))?;
        connection.verify_storage_slots()?;
        Ok(finalized)
    }

    pub(crate) fn load_conversation_turn(
        &self,
        identity: &ConversationIdentity,
        turn_id: &str,
    ) -> Result<Option<StoredConversationTurn>, SessionStoreError> {
        validate_identity(identity)?;
        validate_nonempty(turn_id)?;
        let connection = self.open_connection()?;
        let conversation = require_conversation(&connection, identity)?;
        require_identity(&conversation, identity)?;
        let stored = query_turn_by_id(&connection, identity, turn_id)?;
        connection.verify_storage_slots()?;
        Ok(stored)
    }

    pub(crate) fn list_conversation_turns(
        &self,
        identity: &ConversationIdentity,
    ) -> Result<Vec<StoredConversationTurn>, SessionStoreError> {
        validate_identity(identity)?;
        let connection = self.open_connection()?;
        let conversation = require_conversation(&connection, identity)?;
        require_identity(&conversation, identity)?;
        let mut statement = connection
            .prepare(
                "SELECT turn_id, operation_id, message_id, input_digest, sequence, state,
                        revision, claim_owner, claim_expires_at_ms, outcome_json, failure_class
                 FROM agent_conversation_turns
                 WHERE run_id = ?1 AND parent_agent_id = ?2 AND conversation_id = ?3
                 ORDER BY sequence",
            )
            .map_err(|source| db_error("prepare Agent conversation turn list", source))?;
        let rows = statement
            .query_map(
                params![&identity.run_id, &identity.parent_agent_id, &identity.conversation_id],
                read_turn_row,
            )
            .map_err(|source| db_error("query Agent conversation turn list", source))?;
        let mut turns = Vec::new();
        for row in rows {
            turns.push(parse_turn_row(
                row.map_err(|source| db_error("read Agent conversation turn list", source))?,
            )?);
        }
        drop(statement);
        connection.verify_storage_slots()?;
        Ok(turns)
    }
}

#[cfg(test)]
#[path = "store_conversation_test.rs"]
mod store_conversation_test;
