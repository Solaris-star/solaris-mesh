use super::*;

pub(super) async fn blocking_agent<T, F>(operation: F) -> Result<T, AgentConversationError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, AgentConversationError> + Send + 'static,
{
    tokio::task::spawn_blocking(operation).await.map_err(|error| {
        AgentConversationError::reconciliation_required(format!("Agent conversation blocking worker failed: {error}"))
    })?
}

#[derive(Clone)]
pub(super) struct AsyncConversationStore(SessionStore);

impl AsyncConversationStore {
    pub(super) async fn open(directory: String) -> Result<Self, AgentConversationError> {
        blocking_agent(move || SessionStore::open(directory).map(Self).map_err(store_error)).await
    }

    pub(super) async fn claim_open(
        &self,
        identity: &ConversationIdentity,
        owner_id: &str,
        lease_duration_ms: i64,
    ) -> Result<ConversationOpenClaim, AgentConversationError> {
        let store = self.0.clone();
        let identity = identity.clone();
        let owner_id = owner_id.to_owned();
        blocking_agent(move || {
            store
                .claim_conversation_open_with_duration(&identity, &owner_id, lease_duration_ms)
                .map_err(store_error)
        })
        .await
    }

    pub(super) async fn recover_open(
        &self,
        identity: &ConversationIdentity,
        handle_json: Vec<u8>,
    ) -> Result<(), AgentConversationError> {
        let store = self.0.clone();
        let identity = identity.clone();
        blocking_agent(move || {
            store
                .recover_conversation_open(&identity, &handle_json)
                .map(|_| ())
                .map_err(store_error)
        })
        .await
    }

    pub(super) async fn heartbeat_open(
        &self,
        identity: &ConversationIdentity,
        owner_id: &str,
        epoch: i64,
        revision: i64,
        lease_duration_ms: i64,
    ) -> Result<bool, AgentConversationError> {
        let store = self.0.clone();
        let identity = identity.clone();
        let owner_id = owner_id.to_owned();
        blocking_agent(move || {
            store
                .heartbeat_conversation_open_with_duration(&identity, &owner_id, epoch, revision, lease_duration_ms)
                .map_err(store_error)
        })
        .await
    }

    pub(super) async fn finalize_open(
        &self,
        identity: &ConversationIdentity,
        owner_id: &str,
        epoch: i64,
        revision: i64,
        handle_json: Vec<u8>,
    ) -> Result<(), AgentConversationError> {
        let store = self.0.clone();
        let identity = identity.clone();
        let owner_id = owner_id.to_owned();
        blocking_agent(move || {
            store
                .finalize_conversation_open(&identity, &owner_id, epoch, revision, &handle_json)
                .map(|_| ())
                .map_err(store_error)
        })
        .await
    }

    pub(super) async fn abandon_open(
        &self,
        identity: &ConversationIdentity,
        owner_id: &str,
        epoch: i64,
        revision: i64,
    ) -> Result<bool, AgentConversationError> {
        let store = self.0.clone();
        let identity = identity.clone();
        let owner_id = owner_id.to_owned();
        blocking_agent(move || {
            store
                .abandon_conversation_open(&identity, &owner_id, epoch, revision)
                .map_err(store_error)
        })
        .await
    }

    pub(super) async fn enqueue_turn(
        &self,
        identity: &ConversationIdentity,
        handle_json: Vec<u8>,
        turn: &ConversationTurnIdentity,
    ) -> Result<ConversationTurnEnqueue, AgentConversationError> {
        let store = self.0.clone();
        let identity = identity.clone();
        let turn = turn.clone();
        blocking_agent(move || {
            store
                .enqueue_conversation_turn(&identity, &handle_json, &turn)
                .map_err(enqueue_store_error)
        })
        .await
    }

    pub(super) async fn load_turn(
        &self,
        identity: &ConversationIdentity,
        turn_id: &str,
    ) -> Result<Option<crate::session::store::StoredConversationTurn>, AgentConversationError> {
        let store = self.0.clone();
        let identity = identity.clone();
        let turn_id = turn_id.to_owned();
        blocking_agent(move || store.load_conversation_turn(&identity, &turn_id).map_err(store_error)).await
    }

    pub(super) async fn claim_turn(
        &self,
        identity: &ConversationIdentity,
        handle_json: Vec<u8>,
        turn: &ConversationTurnIdentity,
        owner_id: &str,
    ) -> Result<ConversationTurnClaim, AgentConversationError> {
        let store = self.0.clone();
        let identity = identity.clone();
        let turn = turn.clone();
        let owner_id = owner_id.to_owned();
        blocking_agent(move || {
            store
                .claim_conversation_turn(&identity, &handle_json, &turn, &owner_id)
                .map_err(store_error)
        })
        .await
    }

    pub(super) async fn commit_turn_intent(
        &self,
        identity: &ConversationIdentity,
        handle_json: Vec<u8>,
        turn: &ConversationTurnIdentity,
        sequence: u64,
        owner_id: &str,
        revision: i64,
    ) -> Result<(), AgentConversationError> {
        let store = self.0.clone();
        let identity = identity.clone();
        let turn = turn.clone();
        let owner_id = owner_id.to_owned();
        blocking_agent(move || {
            store
                .commit_conversation_turn_intent(&identity, &handle_json, &turn, sequence, &owner_id, revision)
                .map(|_| ())
                .map_err(store_error)
        })
        .await
    }

    pub(super) async fn finalize_turn(
        &self,
        identity: &ConversationIdentity,
        handle_json: Vec<u8>,
        turn: &ConversationTurnIdentity,
        sequence: u64,
        completion: ConversationTurnCompletion,
    ) -> Result<(), AgentConversationError> {
        let store = self.0.clone();
        let identity = identity.clone();
        let turn = turn.clone();
        blocking_agent(move || {
            store
                .finalize_conversation_turn(&identity, &handle_json, &turn, sequence, completion)
                .map(|_| ())
                .map_err(store_error)
        })
        .await
    }

    pub(super) async fn begin_close(
        &self,
        identity: &ConversationIdentity,
        handle_json: Vec<u8>,
    ) -> Result<ConversationCloseClaim, AgentConversationError> {
        let store = self.0.clone();
        let identity = identity.clone();
        blocking_agent(move || {
            store
                .begin_conversation_close(&identity, &handle_json)
                .map_err(close_store_error)
        })
        .await
    }

    pub(super) async fn finish_close(
        &self,
        identity: &ConversationIdentity,
        handle_json: Vec<u8>,
        terminal_state: &str,
        failure_class: Option<&str>,
    ) -> Result<(), AgentConversationError> {
        let store = self.0.clone();
        let identity = identity.clone();
        let terminal_state = terminal_state.to_owned();
        let failure_class = failure_class.map(str::to_owned);
        blocking_agent(move || {
            store
                .finish_conversation_close(&identity, &handle_json, &terminal_state, failure_class.as_deref())
                .map(|_| ())
                .map_err(store_error)
        })
        .await
    }

    pub(super) async fn list_turns(
        &self,
        identity: &ConversationIdentity,
    ) -> Result<Vec<crate::session::store::StoredConversationTurn>, AgentConversationError> {
        let store = self.0.clone();
        let identity = identity.clone();
        blocking_agent(move || store.list_conversation_turns(&identity).map_err(store_error)).await
    }

    pub(super) async fn requires_failed_close(
        &self,
        identity: &ConversationIdentity,
        handle_json: Vec<u8>,
    ) -> Result<bool, AgentConversationError> {
        let store = self.0.clone();
        let identity = identity.clone();
        blocking_agent(move || {
            store
                .conversation_requires_failed_close(&identity, &handle_json)
                .map_err(store_error)
        })
        .await
    }
}
