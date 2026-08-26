use chrono::Utc;
use rusqlite::Connection;
use std::sync::{Arc, Barrier};
use tempfile::TempDir;

use solaris_types::message::TokenUsage;

use super::*;
use crate::session::Session;

fn fixture() -> (TempDir, SessionStore, ConversationIdentity, Vec<u8>) {
    let directory = TempDir::new().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    let now = Utc::now();
    let session = Session {
        id: "session-a".to_owned(),
        run_id: Some("run-a".to_owned()),
        created_at: now,
        updated_at: now,
        provider: "test".to_owned(),
        model: "test".to_owned(),
        cwd: directory.path().to_string_lossy().into_owned(),
        total_usage: TokenUsage::default(),
        messages: Vec::new(),
        runtime_state: None,
    };
    let lease = store.create_active(&session, "session-owner").unwrap();
    store.release(&lease).unwrap();
    let identity = ConversationIdentity {
        schema_version: 1,
        run_id: "run-a".to_owned(),
        parent_agent_id: "parent-a".to_owned(),
        conversation_id: "conversation-a".to_owned(),
        agent_id: "agent-a".to_owned(),
        session_id: "session-a".to_owned(),
        task_id: "task-a".to_owned(),
        open_operation_id: "open-a".to_owned(),
        spec_digest: "digest-a".to_owned(),
    };
    (directory, store, identity, br#"{"handle":"a"}"#.to_vec())
}

fn turn(turn_id: &str) -> ConversationTurnIdentity {
    ConversationTurnIdentity {
        turn_id: turn_id.to_owned(),
        operation_id: format!("operation-{turn_id}"),
        message_id: format!("message-{turn_id}"),
        input_digest: format!("digest-{turn_id}"),
    }
}

fn open_conversation(store: &SessionStore, identity: &ConversationIdentity, handle: &[u8]) -> StoredConversation {
    let ConversationOpenClaim::Owned { revision, epoch } = store.claim_conversation_open(identity, "owner-a").unwrap()
    else {
        panic!("expected owned opening claim");
    };
    store
        .finalize_conversation_open(identity, "owner-a", epoch, revision, handle)
        .unwrap()
}

#[test]
fn open_claim_is_durable_exact_and_owned_by_one_writer() {
    let (_directory, store, identity, handle) = fixture();
    let ConversationOpenClaim::Owned { revision, epoch } = store.claim_conversation_open(&identity, "owner-a").unwrap()
    else {
        panic!("expected owned opening claim");
    };
    assert_eq!(revision, 0);
    let ConversationOpenClaim::Busy(busy) = store.claim_conversation_open(&identity, "owner-b").unwrap() else {
        panic!("expected a busy opening claim");
    };
    assert_eq!(busy.opening_owner.as_deref(), Some("owner-a"));

    let opened = store
        .finalize_conversation_open(&identity, "owner-a", epoch, revision, &handle)
        .unwrap();
    assert_eq!(opened.state, ConversationState::Open);
    assert_eq!(opened.handle_json.as_deref(), Some(handle.as_slice()));
    let ConversationOpenClaim::Existing(existing) = store.claim_conversation_open(&identity, "owner-b").unwrap() else {
        panic!("expected the durable open result");
    };
    assert_eq!(existing.revision, opened.revision);

    let mut conflicting = identity.clone();
    conflicting.task_id = "different-task".to_owned();
    assert!(matches!(
        store.claim_conversation_open(&conflicting, "owner-b"),
        Err(SessionStoreError::ConversationIdentityConflict)
    ));
    assert!(matches!(
        store.require_conversation_handle(&identity, b"different-handle"),
        Err(SessionStoreError::ConversationHandleMismatch)
    ));
}

#[test]
fn different_conversations_may_reuse_the_callers_open_operation() {
    let (directory, store, identity, handle) = fixture();
    open_conversation(&store, &identity, &handle);
    let mut shared_child = identity.clone();
    shared_child.conversation_id = "conversation-shared-child".to_owned();
    assert!(matches!(
        store.claim_conversation_open(&shared_child, "owner-shared"),
        Err(SessionStoreError::ConversationIdentityConflict)
    ));
    let now = Utc::now();
    let second_session = Session {
        id: "session-b".to_owned(),
        run_id: Some("run-a".to_owned()),
        created_at: now,
        updated_at: now,
        provider: "test".to_owned(),
        model: "test".to_owned(),
        cwd: directory.path().to_string_lossy().into_owned(),
        total_usage: TokenUsage::default(),
        messages: Vec::new(),
        runtime_state: None,
    };
    let lease = store.create_active(&second_session, "session-owner-b").unwrap();
    store.release(&lease).unwrap();
    let mut second = identity.clone();
    second.conversation_id = "conversation-b".to_owned();
    second.agent_id = "agent-b".to_owned();
    second.session_id = "session-b".to_owned();
    second.task_id = "task-b".to_owned();
    second.spec_digest = "digest-b".to_owned();
    let ConversationOpenClaim::Owned { .. } = store.claim_conversation_open(&second, "owner-b").unwrap() else {
        panic!("the caller operation is not a cross-conversation uniqueness key");
    };
}

#[test]
fn only_the_opening_owner_can_abandon_an_unpublished_claim() {
    let (_directory, store, identity, _handle) = fixture();
    let ConversationOpenClaim::Owned { revision, epoch } = store.claim_conversation_open(&identity, "owner-a").unwrap()
    else {
        panic!("expected owned opening claim");
    };
    assert!(
        !store
            .abandon_conversation_open(&identity, "owner-b", epoch, revision)
            .unwrap()
    );
    assert!(store.load_conversation(&identity).unwrap().is_some());
    assert!(
        store
            .abandon_conversation_open(&identity, "owner-a", epoch, revision)
            .unwrap()
    );
    assert!(store.load_conversation(&identity).unwrap().is_none());
}

#[test]
fn expired_opening_claim_increments_epoch_and_invalidates_every_old_owner_operation() {
    let (directory, store, identity, handle) = fixture();
    let ConversationOpenClaim::Owned {
        revision: old_revision,
        epoch: old_epoch,
    } = store.claim_conversation_open(&identity, "owner-a").unwrap()
    else {
        panic!("expected the first opening lease");
    };
    Connection::open(store.database_path())
        .unwrap()
        .execute(
            "UPDATE agent_conversations SET opening_expires_at_ms = 0
             WHERE run_id = ?1 AND parent_agent_id = ?2 AND conversation_id = ?3",
            rusqlite::params![&identity.run_id, &identity.parent_agent_id, &identity.conversation_id],
        )
        .unwrap();
    let restarted = SessionStore::open(directory.path()).unwrap();
    let ConversationOpenClaim::Owned {
        revision: new_revision,
        epoch: new_epoch,
    } = restarted.claim_conversation_open(&identity, "owner-b").unwrap()
    else {
        panic!("an expired opening lease must be taken over");
    };

    assert_eq!(new_revision, old_revision + 1);
    assert_eq!(new_epoch, old_epoch + 1);
    assert!(
        !store
            .heartbeat_conversation_open(&identity, "owner-a", old_epoch, old_revision)
            .unwrap()
    );
    assert!(
        !store
            .abandon_conversation_open(&identity, "owner-a", old_epoch, old_revision)
            .unwrap()
    );
    assert!(matches!(
        store.finalize_conversation_open(&identity, "owner-a", old_epoch, old_revision, &handle),
        Err(SessionStoreError::ConversationRevisionConflict { .. })
    ));
    assert!(
        restarted
            .heartbeat_conversation_open(&identity, "owner-b", new_epoch, new_revision)
            .unwrap()
    );
    restarted
        .finalize_conversation_open(&identity, "owner-b", new_epoch, new_revision, &handle)
        .unwrap();
}

#[test]
fn wall_clock_rollback_safely_takes_over_an_impossibly_future_opening_lease() {
    let (_directory, store, identity, handle) = fixture();
    let lease_ms = DEFAULT_LEASE_SECONDS * 1_000;
    let ConversationOpenClaim::Owned {
        revision: old_revision,
        epoch: old_epoch,
    } = store
        .claim_conversation_open_with_duration_at(&identity, "owner-a", lease_ms, 10_000)
        .unwrap()
    else {
        panic!("expected the first opening lease");
    };

    let ConversationOpenClaim::Owned {
        revision: new_revision,
        epoch: new_epoch,
    } = store
        .claim_conversation_open_with_duration_at(&identity, "owner-b", lease_ms, 5_000)
        .unwrap()
    else {
        panic!("clock rollback must not extend an opening lease beyond its maximum duration");
    };
    assert_eq!(new_revision, old_revision + 1);
    assert_eq!(new_epoch, old_epoch + 1);
    assert!(
        !store
            .heartbeat_conversation_open(&identity, "owner-a", old_epoch, old_revision)
            .unwrap()
    );
    assert!(matches!(
        store.finalize_conversation_open(&identity, "owner-a", old_epoch, old_revision, &handle),
        Err(SessionStoreError::ConversationRevisionConflict { .. })
    ));
    store
        .finalize_conversation_open(&identity, "owner-b", new_epoch, new_revision, &handle)
        .unwrap();
}

#[test]
fn concurrent_store_instances_grant_one_opening_owner() {
    let (_directory, store, identity, _handle) = fixture();
    let barrier = Arc::new(Barrier::new(3));
    let mut workers = Vec::new();
    for owner in ["owner-a", "owner-b"] {
        let store = store.clone();
        let identity = identity.clone();
        let barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            barrier.wait();
            store.claim_conversation_open(&identity, owner).unwrap()
        }));
    }
    barrier.wait();
    let claims = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        claims
            .iter()
            .filter(|claim| matches!(claim, ConversationOpenClaim::Owned { .. }))
            .count(),
        1
    );
    assert_eq!(
        claims
            .iter()
            .filter(|claim| matches!(claim, ConversationOpenClaim::Busy(_)))
            .count(),
        1
    );
}

#[test]
fn turns_are_admitted_in_durable_fifo_order_and_intent_is_not_stolen() {
    let (_directory, store, identity, handle) = fixture();
    open_conversation(&store, &identity, &handle);
    let first = turn("first");
    let second = turn("second");
    let ConversationTurnEnqueue::Inserted = store.enqueue_conversation_turn(&identity, &handle, &first).unwrap() else {
        panic!("expected first turn insert");
    };
    let ConversationTurnEnqueue::Inserted = store.enqueue_conversation_turn(&identity, &handle, &second).unwrap()
    else {
        panic!("expected second turn insert");
    };
    let rows = store.list_conversation_turns(&identity).unwrap();
    assert_eq!((rows[0].sequence, rows[1].sequence), (0, 1));
    assert!(matches!(
        store
            .claim_conversation_turn(&identity, &handle, &second, "worker-b")
            .unwrap(),
        ConversationTurnClaim::Pending(_)
    ));
    let ConversationTurnClaim::Claimed(admitted) = store
        .claim_conversation_turn(&identity, &handle, &first, "worker-a")
        .unwrap()
    else {
        panic!("expected the FIFO head claim");
    };
    assert!(matches!(
        store
            .claim_conversation_turn(&identity, &handle, &first, "worker-b")
            .unwrap(),
        ConversationTurnClaim::Pending(_)
    ));
    store
        .commit_conversation_turn_intent(
            &identity,
            &handle,
            &first,
            admitted.sequence,
            "worker-a",
            admitted.revision,
        )
        .unwrap();
    assert!(matches!(
        store
            .claim_conversation_turn(&identity, &handle, &first, "worker-b")
            .unwrap(),
        ConversationTurnClaim::IntentCommitted(_)
    ));
    store
        .finalize_conversation_turn(
            &identity,
            &handle,
            &first,
            0,
            ConversationTurnCompletion {
                state: ConversationTurnState::Completed,
                outcome_json: br#"{"status":"completed"}"#.to_vec(),
                failure_class: None,
                block_following_turns: false,
            },
        )
        .unwrap();
    assert!(matches!(
        store
            .claim_conversation_turn(&identity, &handle, &second, "worker-b")
            .unwrap(),
        ConversationTurnClaim::Claimed(_)
    ));
}

#[test]
fn duplicate_turn_requires_the_exact_operation_message_and_input() {
    let (_directory, store, identity, handle) = fixture();
    open_conversation(&store, &identity, &handle);
    let original = turn("same");
    store.enqueue_conversation_turn(&identity, &handle, &original).unwrap();
    assert!(matches!(
        store.enqueue_conversation_turn(&identity, &handle, &original).unwrap(),
        ConversationTurnEnqueue::Existing(_)
    ));
    let mut changed = original.clone();
    changed.input_digest = "different".to_owned();
    assert!(matches!(
        store.enqueue_conversation_turn(&identity, &handle, &changed),
        Err(SessionStoreError::ConversationTurnInputConflict)
    ));
}

#[test]
fn turn_claim_clock_rollback_fences_the_old_owner_and_rejects_its_finalize() {
    let (_directory, store, identity, handle) = fixture();
    open_conversation(&store, &identity, &handle);
    let claimed_turn = turn("clock-rollback");
    store
        .enqueue_conversation_turn(&identity, &handle, &claimed_turn)
        .unwrap();
    let lease_ms = DEFAULT_LEASE_SECONDS * 1_000;
    let ConversationTurnClaim::Claimed(old_claim) = store
        .claim_conversation_turn_at(&identity, &handle, &claimed_turn, "worker-a", 10_000, lease_ms)
        .unwrap()
    else {
        panic!("expected the initial turn claim");
    };

    let ConversationTurnClaim::Claimed(new_claim) = store
        .claim_conversation_turn_at(&identity, &handle, &claimed_turn, "worker-b", 5_000, lease_ms)
        .unwrap()
    else {
        panic!("an impossibly future turn lease must be safely taken over");
    };
    assert_eq!(new_claim.revision, old_claim.revision + 1);
    assert!(matches!(
        store.commit_conversation_turn_intent(
            &identity,
            &handle,
            &claimed_turn,
            old_claim.sequence,
            "worker-a",
            old_claim.revision,
        ),
        Err(SessionStoreError::ConversationStateConflict { .. })
            | Err(SessionStoreError::ConversationRevisionConflict { .. })
    ));
    assert!(matches!(
        store.finalize_conversation_turn(
            &identity,
            &handle,
            &claimed_turn,
            old_claim.sequence,
            ConversationTurnCompletion {
                state: ConversationTurnState::Completed,
                outcome_json: br#"{"status":"completed"}"#.to_vec(),
                failure_class: None,
                block_following_turns: false,
            },
        ),
        Err(SessionStoreError::ConversationStateConflict { .. })
    ));
    store
        .commit_conversation_turn_intent(
            &identity,
            &handle,
            &claimed_turn,
            new_claim.sequence,
            "worker-b",
            new_claim.revision,
        )
        .unwrap();
}

#[test]
fn closing_blocks_admission_and_requires_active_turn_resolution() {
    let (_directory, store, identity, handle) = fixture();
    open_conversation(&store, &identity, &handle);
    let queued = turn("queued");
    store.enqueue_conversation_turn(&identity, &handle, &queued).unwrap();
    assert!(matches!(
        store.begin_conversation_close(&identity, &handle).unwrap(),
        ConversationCloseClaim::Started(_)
    ));
    assert!(matches!(
        store
            .claim_conversation_turn(&identity, &handle, &queued, "worker")
            .unwrap(),
        ConversationTurnClaim::ConversationNotOpen(ConversationState::Closing)
    ));
    store
        .finalize_conversation_turn(
            &identity,
            &handle,
            &queued,
            0,
            ConversationTurnCompletion {
                state: ConversationTurnState::Cancelled,
                outcome_json: br#"{"status":"cancelled"}"#.to_vec(),
                failure_class: None,
                block_following_turns: false,
            },
        )
        .unwrap();
    let closed = store
        .finish_conversation_close(&identity, &handle, "completed", None)
        .unwrap();
    assert_eq!(closed.state, ConversationState::Closed);
    assert!(matches!(
        store.begin_conversation_close(&identity, &handle).unwrap(),
        ConversationCloseClaim::AlreadyClosed(_)
    ));
}

#[test]
fn an_unknown_outcome_blocks_new_turns() {
    let (_directory, store, identity, handle) = fixture();
    open_conversation(&store, &identity, &handle);
    let first = turn("first");
    store.enqueue_conversation_turn(&identity, &handle, &first).unwrap();
    let ConversationTurnClaim::Claimed(admitted) = store
        .claim_conversation_turn(&identity, &handle, &first, "worker")
        .unwrap()
    else {
        panic!("expected turn claim");
    };
    store
        .commit_conversation_turn_intent(&identity, &handle, &first, 0, "worker", admitted.revision)
        .unwrap();
    store
        .finalize_conversation_turn(
            &identity,
            &handle,
            &first,
            0,
            ConversationTurnCompletion {
                state: ConversationTurnState::OutcomeUnknown,
                outcome_json: br#"{"status":"outcome_unknown"}"#.to_vec(),
                failure_class: Some("outcome_unknown".to_owned()),
                block_following_turns: true,
            },
        )
        .unwrap();
    assert!(matches!(
        store
            .enqueue_conversation_turn(&identity, &handle, &turn("later"))
            .unwrap(),
        ConversationTurnEnqueue::Blocked { .. }
    ));
}
