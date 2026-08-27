use chrono::Utc;
use solaris_types::message::{ContentBlock, Message, Role, TokenUsage};
use tempfile::tempdir;

use super::*;

#[test]
fn begin_or_resume_task_is_idempotent_and_rejects_changed_input() {
    let directory = tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    let session = sample_session("session-task");
    let lease = store.create_active(&session, "owner-task").unwrap();

    let created = store.begin_or_resume_task(&lease, "task-key-v1:abc", &[1; 32]).unwrap();
    assert_eq!(created.phase, DurableTaskPhase::Created);
    assert_eq!(created.task_revision, 0);
    assert_eq!(created.session_revision, 0);

    let resumed = store.begin_or_resume_task(&lease, "task-key-v1:abc", &[1; 32]).unwrap();
    assert_eq!(resumed, created);

    let error = store
        .begin_or_resume_task(&lease, "task-key-v1:abc", &[2; 32])
        .unwrap_err();
    assert!(matches!(error, SessionStoreError::TaskInputConflict { .. }));
}

#[test]
fn task_transition_is_fenced_and_records_session_checkpoint_revision() {
    let directory = tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    let mut session = sample_session("session-transition");
    let mut lease = store.create_active(&session, "owner-transition").unwrap();
    let task = store
        .begin_or_resume_task(&lease, "task-key-v1:transition", &[3; 32])
        .unwrap();

    session.model = "saved-before-tools-completed".to_owned();
    store.save(&mut lease, &session).unwrap();
    let completed = store
        .transition_task(
            &lease,
            &task.task_key,
            task.task_revision,
            DurableTaskPhase::ToolsCompleted,
            Some("tool-call-v1:stable"),
            None,
        )
        .unwrap();
    assert_eq!(completed.session_revision, lease.revision);
    assert_eq!(completed.task_revision, 1);

    let stale = store.transition_task(
        &lease,
        &task.task_key,
        task.task_revision,
        DurableTaskPhase::Completed,
        None,
        Some(br#"{"text":"done"}"#),
    );
    assert!(matches!(stale, Err(SessionStoreError::TaskRevisionConflict { .. })));
}

#[test]
fn task_transition_faults_distinguish_commit_then_error_from_pure_failure() {
    let directory = tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    let session = sample_session("session-fault");
    let lease = store.create_active(&session, "owner-fault").unwrap();
    let task = store
        .begin_or_resume_task(&lease, "task-key-v1:fault", &[4; 32])
        .unwrap();

    let before = store.transition_task_with_fault(
        &lease,
        &task.task_key,
        task.task_revision,
        DurableTaskPhase::ProviderInFlight,
        TaskTransitionFault::BeforeCommit,
    );
    assert!(before.is_err());
    let unchanged = store.load_task(&lease, &task.task_key).unwrap().unwrap();
    assert_eq!(unchanged.phase, DurableTaskPhase::Created);
    assert_eq!(unchanged.task_revision, 0);

    let after = store.transition_task_with_fault(
        &lease,
        &task.task_key,
        task.task_revision,
        DurableTaskPhase::ProviderInFlight,
        TaskTransitionFault::AfterCommit,
    );
    assert!(after.is_err());
    let committed = store.load_task(&lease, &task.task_key).unwrap().unwrap();
    assert_eq!(committed.phase, DurableTaskPhase::ProviderInFlight);
    assert_eq!(committed.task_revision, 1);
}

#[test]
fn user_checkpoint_atomically_saves_session_and_distinguishes_commit_then_error() {
    let directory = tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    let mut session = sample_session("session-user-checkpoint");
    let mut lease = store.create_active(&session, "owner-user-checkpoint").unwrap();
    let task = store
        .begin_or_resume_task(&lease, "task-key-v1:user-checkpoint", &[5; 32])
        .unwrap();
    session.messages.push(Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: "checkpoint exactly once".to_owned(),
        }],
    ));

    let before =
        store.checkpoint_task_user_with_fault(&mut lease, &session, &task.task_key, TaskTransitionFault::BeforeCommit);
    assert!(before.is_err());
    assert!(
        store
            .load("session-user-checkpoint")
            .unwrap()
            .unwrap()
            .session
            .messages
            .is_empty()
    );
    let created = store.load_task(&lease, &task.task_key).unwrap().unwrap();
    assert_eq!(created.phase, DurableTaskPhase::Created);

    let after =
        store.checkpoint_task_user_with_fault(&mut lease, &session, &task.task_key, TaskTransitionFault::AfterCommit);
    assert!(after.is_err());
    let saved = store.load("session-user-checkpoint").unwrap().unwrap();
    assert_eq!(saved.session.messages.len(), 1);
    let checkpointed = store.load_task(&lease, &task.task_key).unwrap().unwrap();
    assert_eq!(checkpointed.phase, DurableTaskPhase::UserCheckpointed);
    assert_eq!(checkpointed.session_revision, lease.revision);
    assert_eq!(checkpointed.task_revision, 1);
}

#[test]
fn side_effect_unknown_is_a_terminal_durable_task_phase() {
    let directory = tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    let session = sample_session("session-side-effect-unknown");
    let lease = store.create_active(&session, "owner-side-effect-unknown").unwrap();
    let task = store
        .begin_or_resume_task(&lease, "task-key-v1:side-effect-unknown", &[9; 32])
        .unwrap();

    let unknown = store
        .transition_task(
            &lease,
            &task.task_key,
            task.task_revision,
            DurableTaskPhase::SideEffectUnknown,
            Some("tool-call-v3:unknown"),
            None,
        )
        .unwrap();
    assert_eq!(unknown.phase, DurableTaskPhase::SideEffectUnknown);
    assert_eq!(unknown.call_id.as_deref(), Some("tool-call-v3:unknown"));
    assert_eq!(
        store.load_task(&lease, &task.task_key).unwrap().unwrap().phase,
        DurableTaskPhase::SideEffectUnknown
    );
    assert!(matches!(
        store.transition_task(
            &lease,
            &task.task_key,
            unknown.task_revision,
            DurableTaskPhase::Completed,
            None,
            Some(br#"{"text":"unsafe retry"}"#),
        ),
        Err(SessionStoreError::InvalidTaskTransition { .. })
    ));
}

fn sample_session(id: &str) -> Session {
    let now = Utc::now();
    Session {
        id: id.to_owned(),
        run_id: Some(format!("run-{id}")),
        created_at: now,
        updated_at: now,
        provider: "test".to_owned(),
        model: "model".to_owned(),
        cwd: "/workspace".to_owned(),
        total_usage: TokenUsage::default(),
        messages: Vec::new(),
        runtime_state: None,
    }
}
