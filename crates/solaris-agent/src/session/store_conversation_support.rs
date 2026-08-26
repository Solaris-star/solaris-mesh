use super::*;

pub(super) enum TurnClaimInspection {
    Return(ConversationTurnClaim),
    Write {
        conversation: Box<StoredConversation>,
        stored: StoredConversationTurn,
    },
}

pub(super) fn inspect_turn_claim(
    connection: &Connection,
    identity: &ConversationIdentity,
    handle_json: &[u8],
    turn: &ConversationTurnIdentity,
    owner_id: &str,
    now_ms: i64,
    lease_duration_ms: i64,
) -> Result<TurnClaimInspection, SessionStoreError> {
    let conversation = require_conversation(connection, identity)?;
    require_identity(&conversation, identity)?;
    require_handle(&conversation, handle_json)?;
    let stored =
        query_turn_by_id(connection, identity, &turn.turn_id)?.ok_or(SessionStoreError::ConversationTurnNotFound)?;
    require_turn_identity(&stored, turn)?;
    if stored.state.is_terminal() {
        return Ok(TurnClaimInspection::Return(ConversationTurnClaim::Terminal(stored)));
    }
    if stored.state == ConversationTurnState::IntentCommitted {
        return Ok(TurnClaimInspection::Return(ConversationTurnClaim::IntentCommitted(
            stored,
        )));
    }
    if let Some(failure_class) = conversation.failure_class.clone() {
        return Ok(TurnClaimInspection::Return(ConversationTurnClaim::Blocked {
            failure_class,
        }));
    }
    if conversation.state != ConversationState::Open {
        return Ok(TurnClaimInspection::Return(ConversationTurnClaim::ConversationNotOpen(
            conversation.state,
        )));
    }
    let maximum_expiry_ms = opening_lease_expiry(now_ms, lease_duration_ms)?;
    match stored.state {
        ConversationTurnState::Admitted
            if stored.claim_owner.as_deref() != Some(owner_id)
                && stored
                    .claim_expires_at_ms
                    .is_some_and(|expires| expires > now_ms && expires <= maximum_expiry_ms) =>
        {
            Ok(TurnClaimInspection::Return(ConversationTurnClaim::Pending(stored)))
        }
        ConversationTurnState::Queued
            if stored.sequence != conversation.next_to_run || conversation.active_sequence.is_some() =>
        {
            Ok(TurnClaimInspection::Return(ConversationTurnClaim::Pending(stored)))
        }
        ConversationTurnState::Queued | ConversationTurnState::Admitted => Ok(TurnClaimInspection::Write {
            conversation: Box::new(conversation),
            stored,
        }),
        _ => Err(SessionStoreError::ConversationStateCorrupt),
    }
}

impl SessionStore {
    pub(super) fn claim_conversation_turn_at(
        &self,
        identity: &ConversationIdentity,
        handle_json: &[u8],
        turn: &ConversationTurnIdentity,
        owner_id: &str,
        now_ms: i64,
        lease_duration_ms: i64,
    ) -> Result<ConversationTurnClaim, SessionStoreError> {
        validate_identity(identity)?;
        validate_handle(handle_json)?;
        validate_turn_identity(turn)?;
        validate_nonempty(owner_id)?;
        validate_lease_duration(lease_duration_ms)?;
        let connection = self.open_connection()?;
        let read = inspect_turn_claim(
            &connection,
            identity,
            handle_json,
            turn,
            owner_id,
            now_ms,
            lease_duration_ms,
        )?;
        connection.verify_storage_slots()?;
        if let TurnClaimInspection::Return(result) = read {
            return Ok(result);
        }
        drop(connection);
        let mut connection = self.open_connection()?;
        let storage = connection.storage_guard();
        let transaction = begin_immediate(&mut connection, "begin Agent conversation turn claim")?;
        let (conversation, stored) = match inspect_turn_claim(
            &transaction,
            identity,
            handle_json,
            turn,
            owner_id,
            now_ms,
            lease_duration_ms,
        )? {
            TurnClaimInspection::Write { conversation, stored } => (*conversation, stored),
            TurnClaimInspection::Return(result) => {
                storage.verify()?;
                transaction
                    .commit()
                    .map_err(|source| db_error("commit read-only Agent conversation turn claim", source))?;
                connection.verify_storage_slots()?;
                return Ok(result);
            }
        };
        let result = match stored.state {
            ConversationTurnState::Admitted => {
                let claimed = update_turn_claim(&transaction, identity, &stored, owner_id, now_ms, lease_duration_ms)?;
                ConversationTurnClaim::Claimed(claimed)
            }
            ConversationTurnState::Queued => {
                let next_conversation_revision = next_revision(conversation.revision)?;
                let changed = transaction
                    .execute(
                        "UPDATE agent_conversations
                             SET active_sequence = ?4, revision = ?5, updated_at_ms = ?6
                             WHERE run_id = ?1 AND parent_agent_id = ?2 AND conversation_id = ?3
                               AND state = 'open' AND revision = ?7 AND active_sequence IS NULL
                               AND next_to_run = ?4",
                        params![
                            &identity.run_id,
                            &identity.parent_agent_id,
                            &identity.conversation_id,
                            u64_to_sql(stored.sequence)?,
                            next_conversation_revision,
                            Utc::now().timestamp_millis(),
                            conversation.revision,
                        ],
                    )
                    .map_err(|source| db_error("admit Agent conversation turn", source))?;
                require_changed(changed, conversation.revision, &transaction, identity)?;
                let claimed = update_turn_claim(&transaction, identity, &stored, owner_id, now_ms, lease_duration_ms)?;
                ConversationTurnClaim::Claimed(claimed)
            }
            _ => return Err(SessionStoreError::ConversationStateCorrupt),
        };
        storage.verify()?;
        transaction
            .commit()
            .map_err(|source| db_error("commit Agent conversation turn claim", source))?;
        connection.verify_storage_slots()?;
        Ok(result)
    }

    #[cfg(test)]
    pub(crate) fn heartbeat_conversation_open(
        &self,
        identity: &ConversationIdentity,
        owner_id: &str,
        expected_epoch: i64,
        expected_revision: i64,
    ) -> Result<bool, SessionStoreError> {
        self.heartbeat_conversation_open_with_duration(
            identity,
            owner_id,
            expected_epoch,
            expected_revision,
            DEFAULT_LEASE_SECONDS * 1_000,
        )
    }

    pub(crate) fn heartbeat_conversation_open_with_duration(
        &self,
        identity: &ConversationIdentity,
        owner_id: &str,
        expected_epoch: i64,
        expected_revision: i64,
        lease_duration_ms: i64,
    ) -> Result<bool, SessionStoreError> {
        validate_identity(identity)?;
        validate_nonempty(owner_id)?;
        validate_lease_duration(lease_duration_ms)?;
        let mut connection = self.open_connection()?;
        let storage = connection.storage_guard();
        let transaction = begin_immediate(&mut connection, "begin Agent conversation opening heartbeat")?;
        let changed = renew_opening_lease(
            &transaction,
            identity,
            owner_id,
            expected_epoch,
            expected_revision,
            Utc::now().timestamp_millis(),
            lease_duration_ms,
        )?;
        storage.verify()?;
        transaction
            .commit()
            .map_err(|source| db_error("commit Agent conversation opening heartbeat", source))?;
        connection.verify_storage_slots()?;
        Ok(changed)
    }

    #[allow(dead_code)]
    pub(crate) fn load_conversation(
        &self,
        identity: &ConversationIdentity,
    ) -> Result<Option<StoredConversation>, SessionStoreError> {
        validate_identity(identity)?;
        let connection = self.open_connection()?;
        let stored = query_conversation(&connection, identity)?;
        if let Some(stored) = stored.as_ref() {
            require_identity(stored, identity)?;
        }
        connection.verify_storage_slots()?;
        Ok(stored)
    }

    #[allow(dead_code)]
    pub(crate) fn require_conversation_handle(
        &self,
        identity: &ConversationIdentity,
        handle_json: &[u8],
    ) -> Result<StoredConversation, SessionStoreError> {
        validate_handle(handle_json)?;
        let connection = self.open_connection()?;
        let stored = require_conversation(&connection, identity)?;
        require_identity(&stored, identity)?;
        require_handle(&stored, handle_json)?;
        connection.verify_storage_slots()?;
        Ok(stored)
    }

    pub(crate) fn conversation_requires_failed_close(
        &self,
        identity: &ConversationIdentity,
        handle_json: &[u8],
    ) -> Result<bool, SessionStoreError> {
        validate_identity(identity)?;
        validate_handle(handle_json)?;
        let connection = self.open_connection()?;
        let conversation = require_conversation(&connection, identity)?;
        require_identity(&conversation, identity)?;
        require_handle(&conversation, handle_json)?;
        let requires_failed: bool = connection
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM agent_conversation_turns
                    WHERE run_id = ?1 AND parent_agent_id = ?2 AND conversation_id = ?3
                      AND state IN ('failed', 'outcome_unknown', 'reconciliation_required')
                 )",
                params![&identity.run_id, &identity.parent_agent_id, &identity.conversation_id],
                |row| row.get(0),
            )
            .map_err(|source| db_error("inspect Agent conversation terminal turn states", source))?;
        connection.verify_storage_slots()?;
        Ok(requires_failed)
    }
}

pub(super) fn opening_lease_expiry(now_ms: i64, lease_duration_ms: i64) -> Result<i64, SessionStoreError> {
    now_ms
        .checked_add(lease_duration_ms)
        .ok_or(SessionStoreError::ConversationStateCorrupt)
}

pub(super) fn validate_lease_duration(lease_duration_ms: i64) -> Result<(), SessionStoreError> {
    if lease_duration_ms <= 0 {
        return Err(SessionStoreError::InvalidConversationInput);
    }
    Ok(())
}

pub(super) fn renew_opening_lease(
    transaction: &Transaction<'_>,
    identity: &ConversationIdentity,
    owner_id: &str,
    expected_epoch: i64,
    expected_revision: i64,
    now_ms: i64,
    lease_duration_ms: i64,
) -> Result<bool, SessionStoreError> {
    let expires_at_ms = opening_lease_expiry(now_ms, lease_duration_ms)?;
    let changed = transaction
        .execute(
            "UPDATE agent_conversations
             SET opening_expires_at_ms = ?6, updated_at_ms = ?5
             WHERE run_id = ?1 AND parent_agent_id = ?2 AND conversation_id = ?3
               AND state = 'opening' AND opening_owner = ?4
               AND opening_epoch = ?7 AND revision = ?8",
            params![
                &identity.run_id,
                &identity.parent_agent_id,
                &identity.conversation_id,
                owner_id,
                now_ms,
                expires_at_ms,
                expected_epoch,
                expected_revision,
            ],
        )
        .map_err(|source| db_error("renew Agent conversation opening lease", source))?;
    Ok(changed == 1)
}

pub(super) fn update_turn_claim(
    transaction: &Transaction<'_>,
    identity: &ConversationIdentity,
    stored: &StoredConversationTurn,
    owner_id: &str,
    now_ms: i64,
    lease_duration_ms: i64,
) -> Result<StoredConversationTurn, SessionStoreError> {
    let next_turn_revision = next_revision(stored.revision)?;
    let expires_at_ms = now_ms
        .checked_add(lease_duration_ms)
        .ok_or(SessionStoreError::ConversationStateCorrupt)?;
    let changed = transaction
        .execute(
            "UPDATE agent_conversation_turns
             SET state = 'admitted', revision = ?5, claim_owner = ?6,
                 claim_expires_at_ms = ?7, updated_at_ms = ?8
             WHERE run_id = ?1 AND parent_agent_id = ?2 AND conversation_id = ?3
               AND sequence = ?4 AND revision = ?9 AND state IN ('queued', 'admitted')",
            params![
                &identity.run_id,
                &identity.parent_agent_id,
                &identity.conversation_id,
                u64_to_sql(stored.sequence)?,
                next_turn_revision,
                owner_id,
                expires_at_ms,
                now_ms,
                stored.revision,
            ],
        )
        .map_err(|source| db_error("claim Agent conversation turn", source))?;
    if changed != 1 {
        return Err(SessionStoreError::ConversationRevisionConflict {
            expected: stored.revision,
            actual: require_turn_by_sequence(transaction, identity, stored.sequence)?.revision,
        });
    }
    require_turn_by_sequence(transaction, identity, stored.sequence)
}

pub(super) fn query_conversation(
    connection: &Connection,
    identity: &ConversationIdentity,
) -> Result<Option<StoredConversation>, SessionStoreError> {
    connection
        .query_row(
            "SELECT schema_version, run_id, parent_agent_id, conversation_id, agent_id,
                    session_id, task_id, open_operation_id, spec_digest, state, revision,
                    opening_owner, opening_epoch, opening_expires_at_ms, handle_json,
                    next_sequence, next_to_run, active_sequence, terminal_state, failure_class
             FROM agent_conversations
             WHERE run_id = ?1 AND parent_agent_id = ?2 AND conversation_id = ?3",
            params![&identity.run_id, &identity.parent_agent_id, &identity.conversation_id],
            |row| {
                Ok(ConversationRow {
                    schema_version: row.get(0)?,
                    run_id: row.get(1)?,
                    parent_agent_id: row.get(2)?,
                    conversation_id: row.get(3)?,
                    agent_id: row.get(4)?,
                    session_id: row.get(5)?,
                    task_id: row.get(6)?,
                    open_operation_id: row.get(7)?,
                    spec_digest: row.get(8)?,
                    state: row.get(9)?,
                    revision: row.get(10)?,
                    opening_owner: row.get(11)?,
                    opening_epoch: row.get(12)?,
                    opening_expires_at_ms: row.get(13)?,
                    handle_json: row.get(14)?,
                    next_sequence: row.get(15)?,
                    next_to_run: row.get(16)?,
                    active_sequence: row.get(17)?,
                    terminal_state: row.get(18)?,
                    failure_class: row.get(19)?,
                })
            },
        )
        .optional()
        .map_err(|source| db_error("load Agent conversation", source))?
        .map(parse_conversation_row)
        .transpose()
}

pub(super) fn require_conversation(
    connection: &Connection,
    identity: &ConversationIdentity,
) -> Result<StoredConversation, SessionStoreError> {
    query_conversation(connection, identity)?.ok_or(SessionStoreError::ConversationNotFound)
}

pub(super) fn query_turn_by_id(
    connection: &Connection,
    identity: &ConversationIdentity,
    turn_id: &str,
) -> Result<Option<StoredConversationTurn>, SessionStoreError> {
    connection
        .query_row(
            "SELECT turn_id, operation_id, message_id, input_digest, sequence, state,
                    revision, claim_owner, claim_expires_at_ms, outcome_json, failure_class
             FROM agent_conversation_turns
             WHERE run_id = ?1 AND parent_agent_id = ?2 AND conversation_id = ?3 AND turn_id = ?4",
            params![
                &identity.run_id,
                &identity.parent_agent_id,
                &identity.conversation_id,
                turn_id
            ],
            read_turn_row,
        )
        .optional()
        .map_err(|source| db_error("load Agent conversation turn", source))?
        .map(parse_turn_row)
        .transpose()
}

pub(super) fn require_turn_by_sequence(
    connection: &Connection,
    identity: &ConversationIdentity,
    sequence: u64,
) -> Result<StoredConversationTurn, SessionStoreError> {
    connection
        .query_row(
            "SELECT turn_id, operation_id, message_id, input_digest, sequence, state,
                    revision, claim_owner, claim_expires_at_ms, outcome_json, failure_class
             FROM agent_conversation_turns
             WHERE run_id = ?1 AND parent_agent_id = ?2 AND conversation_id = ?3 AND sequence = ?4",
            params![
                &identity.run_id,
                &identity.parent_agent_id,
                &identity.conversation_id,
                u64_to_sql(sequence)?,
            ],
            read_turn_row,
        )
        .optional()
        .map_err(|source| db_error("load Agent conversation turn by sequence", source))?
        .map(parse_turn_row)
        .transpose()?
        .ok_or(SessionStoreError::ConversationTurnNotFound)
}

pub(super) fn require_unique_conversation_binding(
    transaction: &Transaction<'_>,
    identity: &ConversationIdentity,
) -> Result<(), SessionStoreError> {
    let conflict: bool = transaction
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM agent_conversations
                WHERE agent_id = ?1 OR session_id = ?2
             )",
            params![&identity.agent_id, &identity.session_id],
            |row| row.get(0),
        )
        .map_err(|source| db_error("check Agent conversation identity uniqueness", source))?;
    if conflict {
        return Err(SessionStoreError::ConversationIdentityConflict);
    }
    Ok(())
}

pub(super) fn require_identity(
    stored: &StoredConversation,
    requested: &ConversationIdentity,
) -> Result<(), SessionStoreError> {
    if &stored.identity != requested {
        return Err(SessionStoreError::ConversationIdentityConflict);
    }
    Ok(())
}

pub(super) fn require_handle(stored: &StoredConversation, requested: &[u8]) -> Result<(), SessionStoreError> {
    if stored.handle_json.as_deref() != Some(requested) {
        return Err(SessionStoreError::ConversationHandleMismatch);
    }
    Ok(())
}

pub(super) fn require_turn_identity(
    stored: &StoredConversationTurn,
    requested: &ConversationTurnIdentity,
) -> Result<(), SessionStoreError> {
    if &stored.identity != requested {
        return Err(SessionStoreError::ConversationTurnInputConflict);
    }
    Ok(())
}

pub(super) fn require_changed(
    changed: usize,
    expected_revision: i64,
    transaction: &Transaction<'_>,
    identity: &ConversationIdentity,
) -> Result<(), SessionStoreError> {
    if changed == 1 {
        return Ok(());
    }
    let actual = require_conversation(transaction, identity)?.revision;
    Err(SessionStoreError::ConversationRevisionConflict {
        expected: expected_revision,
        actual,
    })
}

pub(super) fn validate_identity(identity: &ConversationIdentity) -> Result<(), SessionStoreError> {
    if identity.schema_version == 0 {
        return Err(SessionStoreError::InvalidConversationInput);
    }
    for value in [
        &identity.run_id,
        &identity.parent_agent_id,
        &identity.conversation_id,
        &identity.agent_id,
        &identity.session_id,
        &identity.task_id,
        &identity.open_operation_id,
        &identity.spec_digest,
    ] {
        validate_nonempty(value)?;
    }
    Ok(())
}

pub(super) fn validate_turn_identity(identity: &ConversationTurnIdentity) -> Result<(), SessionStoreError> {
    for value in [
        &identity.turn_id,
        &identity.operation_id,
        &identity.message_id,
        &identity.input_digest,
    ] {
        validate_nonempty(value)?;
    }
    Ok(())
}

pub(super) fn validate_handle(handle_json: &[u8]) -> Result<(), SessionStoreError> {
    if handle_json.is_empty() {
        return Err(SessionStoreError::InvalidConversationInput);
    }
    Ok(())
}

pub(super) fn validate_nonempty(value: &str) -> Result<(), SessionStoreError> {
    if value.trim().is_empty() {
        return Err(SessionStoreError::InvalidConversationInput);
    }
    Ok(())
}

pub(super) fn next_revision(revision: i64) -> Result<i64, SessionStoreError> {
    revision
        .checked_add(1)
        .ok_or(SessionStoreError::ConversationRevisionExhausted)
}

pub(super) fn next_epoch(epoch: i64) -> Result<i64, SessionStoreError> {
    epoch
        .checked_add(1)
        .ok_or(SessionStoreError::ConversationEpochExhausted)
}

pub(super) fn sql_to_u64(value: i64) -> Result<u64, SessionStoreError> {
    u64::try_from(value).map_err(|_| SessionStoreError::ConversationStateCorrupt)
}

pub(super) fn u64_to_sql(value: u64) -> Result<i64, SessionStoreError> {
    i64::try_from(value).map_err(|_| SessionStoreError::ConversationStateCorrupt)
}

pub(super) fn state_conflict(state: ConversationState) -> SessionStoreError {
    SessionStoreError::ConversationStateConflict {
        state: state.as_str().to_owned(),
    }
}

pub(super) fn read_turn_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TurnRow> {
    Ok(TurnRow {
        turn_id: row.get(0)?,
        operation_id: row.get(1)?,
        message_id: row.get(2)?,
        input_digest: row.get(3)?,
        sequence: row.get(4)?,
        state: row.get(5)?,
        revision: row.get(6)?,
        claim_owner: row.get(7)?,
        claim_expires_at_ms: row.get(8)?,
        outcome_json: row.get(9)?,
        failure_class: row.get(10)?,
    })
}

pub(super) fn parse_turn_row(row: TurnRow) -> Result<StoredConversationTurn, SessionStoreError> {
    Ok(StoredConversationTurn {
        identity: ConversationTurnIdentity {
            turn_id: row.turn_id,
            operation_id: row.operation_id,
            message_id: row.message_id,
            input_digest: row.input_digest,
        },
        sequence: sql_to_u64(row.sequence)?,
        state: ConversationTurnState::parse(&row.state)?,
        revision: counter_from_sql(row.revision)?,
        claim_owner: row.claim_owner,
        claim_expires_at_ms: row.claim_expires_at_ms,
        outcome_json: row.outcome_json,
        failure_class: row.failure_class,
    })
}

fn parse_conversation_row(row: ConversationRow) -> Result<StoredConversation, SessionStoreError> {
    let schema_version = u8::try_from(row.schema_version).map_err(|_| SessionStoreError::ConversationStateCorrupt)?;
    let opening_epoch = counter_from_sql(row.opening_epoch)?;
    if opening_epoch == 0
        || (row.state == "opening"
            && (row.opening_owner.is_none() || row.opening_expires_at_ms.is_none() || row.handle_json.is_some()))
        || (row.state != "opening"
            && (row.opening_owner.is_some() || row.opening_expires_at_ms.is_some() || row.handle_json.is_none()))
    {
        return Err(SessionStoreError::ConversationStateCorrupt);
    }
    Ok(StoredConversation {
        identity: ConversationIdentity {
            schema_version,
            run_id: row.run_id,
            parent_agent_id: row.parent_agent_id,
            conversation_id: row.conversation_id,
            agent_id: row.agent_id,
            session_id: row.session_id,
            task_id: row.task_id,
            open_operation_id: row.open_operation_id,
            spec_digest: row.spec_digest,
        },
        state: ConversationState::parse(&row.state)?,
        revision: counter_from_sql(row.revision)?,
        opening_owner: row.opening_owner,
        opening_epoch,
        opening_expires_at_ms: row.opening_expires_at_ms,
        handle_json: row.handle_json,
        next_sequence: sql_to_u64(row.next_sequence)?,
        next_to_run: sql_to_u64(row.next_to_run)?,
        active_sequence: row.active_sequence.map(sql_to_u64).transpose()?,
        terminal_state: row.terminal_state,
        failure_class: row.failure_class,
    })
}

struct ConversationRow {
    schema_version: i64,
    run_id: String,
    parent_agent_id: String,
    conversation_id: String,
    agent_id: String,
    session_id: String,
    task_id: String,
    open_operation_id: String,
    spec_digest: String,
    state: String,
    revision: i64,
    opening_owner: Option<String>,
    opening_epoch: i64,
    opening_expires_at_ms: Option<i64>,
    handle_json: Option<Vec<u8>>,
    next_sequence: i64,
    next_to_run: i64,
    active_sequence: Option<i64>,
    terminal_state: Option<String>,
    failure_class: Option<String>,
}

pub(super) struct TurnRow {
    turn_id: String,
    operation_id: String,
    message_id: String,
    input_digest: String,
    sequence: i64,
    state: String,
    revision: i64,
    claim_owner: Option<String>,
    claim_expires_at_ms: Option<i64>,
    outcome_json: Option<Vec<u8>>,
    failure_class: Option<String>,
}
