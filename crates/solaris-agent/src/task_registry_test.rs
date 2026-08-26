use solaris_types::identity::{AgentId, OperationId, RunId, TaskId};
use solaris_types::runtime::{TaskFailureClass, TaskRecord, TaskState};

use super::{TaskCasMutation, TaskRegistry};

fn queued_task(revision: u64) -> TaskRecord {
    TaskRecord {
        run_id: RunId::from("run"),
        task_id: TaskId::from("task"),
        revision,
        task_key: Some("stable-task".to_owned()),
        team_id: None,
        workflow_id: None,
        node_id: None,
        role: Some("worker".to_owned()),
        depends_on: Vec::new(),
        content: None,
        expected_write_scope: Vec::new(),
        owner_agent_id: None,
        state: TaskState::Queued,
        outcome_ref: None,
        failure_class: None,
    }
}

fn operation(value: &str) -> OperationId {
    OperationId::from(value)
}

#[test]
fn stale_assign_writer_cannot_overwrite_owner() {
    let registry = TaskRegistry::default();
    registry.upsert(queued_task(0));
    let first = TaskCasMutation::Assign {
        owner: AgentId::from("first"),
    };
    let second = TaskCasMutation::Assign {
        owner: AgentId::from("second"),
    };

    assert_eq!(
        registry
            .cas_durable(
                &TaskId::from("task"),
                0,
                &operation("first"),
                "first-digest",
                &first,
                |_| Ok(())
            )
            .unwrap()
            .revision,
        1
    );
    let error = registry
        .cas_durable(
            &TaskId::from("task"),
            0,
            &operation("second"),
            "second-digest",
            &second,
            |_| panic!("stale writer persisted"),
        )
        .unwrap_err();

    assert!(error.to_string().contains("stale task revision"));
    assert_eq!(
        registry.get(&TaskId::from("task")).unwrap().owner_agent_id,
        Some(AgentId::from("first"))
    );
}

#[test]
fn exact_task_cas_replay_is_idempotent() {
    let registry = TaskRegistry::default();
    registry.upsert(queued_task(0));
    let mutation = TaskCasMutation::Assign {
        owner: AgentId::from("owner"),
    };
    registry
        .cas_durable(
            &TaskId::from("task"),
            0,
            &operation("assign"),
            "digest",
            &mutation,
            |_| Ok(()),
        )
        .unwrap();

    let replay = registry
        .cas_durable(
            &TaskId::from("task"),
            0,
            &operation("assign"),
            "digest",
            &mutation,
            |_| panic!("replay persisted twice"),
        )
        .unwrap();

    assert_eq!(replay.revision, 1);
}

#[test]
fn historical_operation_replay_returns_its_original_result_after_later_cas() {
    let registry = TaskRegistry::default();
    registry.upsert(queued_task(0));
    let owner = AgentId::from("owner");
    let assigned = registry
        .cas_durable(
            &TaskId::from("task"),
            0,
            &operation("assign-history"),
            "assign-history",
            &TaskCasMutation::Assign { owner: owner.clone() },
            |_| Ok(()),
        )
        .unwrap();
    registry
        .cas_durable(
            &TaskId::from("task"),
            1,
            &operation("running-history"),
            "running-history",
            &TaskCasMutation::MarkRunning { owner: owner.clone() },
            |_| Ok(()),
        )
        .unwrap();

    let replay = registry
        .cas_durable(
            &TaskId::from("task"),
            0,
            &operation("assign-history"),
            "assign-history",
            &TaskCasMutation::Assign { owner },
            |_| panic!("historical replay persisted twice"),
        )
        .unwrap();

    assert_eq!(replay, assigned);
    assert_eq!(registry.get(&TaskId::from("task")).unwrap().state, TaskState::Running);
}

#[test]
fn historical_operation_replay_survives_registry_restore() {
    let source = TaskRegistry::default();
    source.upsert(queued_task(0));
    let owner = AgentId::from("owner");
    let assigned = source
        .cas_durable(
            &TaskId::from("task"),
            0,
            &operation("restore-assign-history"),
            "restore-assign-history",
            &TaskCasMutation::Assign { owner: owner.clone() },
            |_| Ok(()),
        )
        .unwrap();
    let running = source
        .cas_durable(
            &TaskId::from("task"),
            1,
            &operation("restore-running-history"),
            "restore-running-history",
            &TaskCasMutation::MarkRunning { owner: owner.clone() },
            |_| Ok(()),
        )
        .unwrap();

    let restored = TaskRegistry::default();
    restored.upsert(queued_task(0));
    restored
        .restore_cas(
            assigned.clone(),
            operation("restore-assign-history"),
            "restore-assign-history".to_owned(),
            0,
            1,
        )
        .unwrap();
    restored
        .restore_cas(
            running,
            operation("restore-running-history"),
            "restore-running-history".to_owned(),
            1,
            2,
        )
        .unwrap();

    let replay = restored
        .cas_durable(
            &TaskId::from("task"),
            0,
            &operation("restore-assign-history"),
            "restore-assign-history",
            &TaskCasMutation::Assign { owner },
            |_| panic!("restored historical replay persisted twice"),
        )
        .unwrap();
    assert_eq!(replay, assigned);
    assert_eq!(restored.get(&TaskId::from("task")).unwrap().revision, 2);
}

#[test]
fn task_state_machine_rejects_illegal_transitions_and_records_outcome() {
    let registry = TaskRegistry::default();
    registry.upsert(queued_task(0));
    let owner = AgentId::from("owner");
    assert!(
        registry
            .cas_durable(
                &TaskId::from("task"),
                0,
                &operation("illegal-running"),
                "illegal-running",
                &TaskCasMutation::MarkRunning { owner: owner.clone() },
                |_| Ok(())
            )
            .is_err()
    );
    registry
        .cas_durable(
            &TaskId::from("task"),
            0,
            &operation("assign"),
            "assign",
            &TaskCasMutation::Assign { owner: owner.clone() },
            |_| Ok(()),
        )
        .unwrap();
    registry
        .cas_durable(
            &TaskId::from("task"),
            1,
            &operation("running"),
            "running",
            &TaskCasMutation::MarkRunning { owner: owner.clone() },
            |_| Ok(()),
        )
        .unwrap();
    let settled = registry
        .cas_durable(
            &TaskId::from("task"),
            2,
            &operation("settle"),
            "settle",
            &TaskCasMutation::Settle {
                owner,
                state: TaskState::Failed,
                outcome_ref: Some("mesh://outcome/1".to_owned()),
                failure_class: Some(TaskFailureClass::OutcomeUnknown),
            },
            |_| Ok(()),
        )
        .unwrap();
    assert_eq!(settled.revision, 3);
    assert_eq!(settled.outcome_ref.as_deref(), Some("mesh://outcome/1"));
    assert_eq!(settled.failure_class, Some(TaskFailureClass::OutcomeUnknown));
}

#[test]
fn revision_overflow_is_rejected_before_persistence() {
    let registry = TaskRegistry::default();
    registry.upsert(queued_task(u64::MAX));
    let error = registry
        .cas_durable(
            &TaskId::from("task"),
            u64::MAX,
            &operation("overflow"),
            "overflow",
            &TaskCasMutation::Assign {
                owner: AgentId::from("owner"),
            },
            |_| panic!("overflow persisted"),
        )
        .unwrap_err();
    assert_eq!(error.to_string(), "task revision overflow");
}

#[test]
fn same_owner_and_content_with_a_different_operation_is_stale_not_replay() {
    let registry = TaskRegistry::default();
    registry.upsert(queued_task(0));
    let mutation = TaskCasMutation::Assign {
        owner: AgentId::from("owner"),
    };
    registry
        .cas_durable(
            &TaskId::from("task"),
            0,
            &operation("writer-a"),
            "same-content",
            &mutation,
            |_| Ok(()),
        )
        .unwrap();

    let error = registry
        .cas_durable(
            &TaskId::from("task"),
            0,
            &operation("writer-b"),
            "same-content",
            &mutation,
            |_| panic!("different operation persisted"),
        )
        .unwrap_err();

    assert!(error.to_string().contains("stale task revision"));
}

#[test]
fn illegal_workflow_transition_never_persists_or_changes_projection() {
    let registry = TaskRegistry::default();
    let mut task = queued_task(0);
    task.workflow_id = Some("workflow".to_owned());
    registry.upsert(task);
    registry
        .cas_durable(
            &TaskId::from("task"),
            0,
            &operation("complete"),
            "complete",
            &TaskCasMutation::WorkflowState {
                state: TaskState::Completed,
                clear_owner: false,
                outcome_ref: Some("mesh://workflow-output".to_owned()),
                failure_class: None,
            },
            |_| Ok(()),
        )
        .unwrap();

    let error = registry
        .cas_durable(
            &TaskId::from("task"),
            1,
            &operation("reopen"),
            "reopen",
            &TaskCasMutation::WorkflowState {
                state: TaskState::Failed,
                clear_owner: false,
                outcome_ref: None,
                failure_class: None,
            },
            |_| panic!("illegal transition reached persistence"),
        )
        .unwrap_err();

    assert!(error.to_string().contains("illegal workflow task transition"));
    let unchanged = registry.get(&TaskId::from("task")).unwrap();
    assert_eq!(unchanged.revision, 1);
    assert_eq!(unchanged.state, TaskState::Completed);
    assert_eq!(unchanged.outcome_ref.as_deref(), Some("mesh://workflow-output"));
}

#[test]
fn workflow_transition_table_accepts_only_declared_edges() {
    let owner = AgentId::from("owner");
    let allowed = [
        (TaskState::Queued, TaskState::Completed, false),
        (TaskState::Queued, TaskState::Failed, false),
        (TaskState::Queued, TaskState::Skipped, false),
        (TaskState::Queued, TaskState::Cancelled, false),
        (TaskState::Assigned, TaskState::Completed, false),
        (TaskState::Assigned, TaskState::Failed, false),
        (TaskState::Assigned, TaskState::Cancelled, false),
        (TaskState::Running, TaskState::Completed, false),
        (TaskState::Running, TaskState::Failed, false),
        (TaskState::Running, TaskState::Cancelled, false),
        (TaskState::Failed, TaskState::Queued, true),
    ];
    for (from, to, clear_owner) in allowed {
        let registry = TaskRegistry::default();
        let mut task = queued_task(0);
        task.workflow_id = Some("workflow".to_owned());
        task.state = from;
        task.owner_agent_id = (!matches!(from, TaskState::Queued)).then(|| owner.clone());
        registry.upsert(task);

        let updated = registry
            .cas_durable(
                &TaskId::from("task"),
                0,
                &operation(&format!("allowed-{from:?}-{to:?}")),
                "allowed",
                &TaskCasMutation::WorkflowState {
                    state: to,
                    clear_owner,
                    outcome_ref: None,
                    failure_class: None,
                },
                |_| Ok(()),
            )
            .unwrap_or_else(|error| panic!("{from:?} -> {to:?} must be allowed: {error}"));
        assert_eq!(updated.state, to);
        assert_eq!(updated.revision, 1);
        if clear_owner {
            assert!(updated.owner_agent_id.is_none());
        }
    }

    let denied = [
        (TaskState::Queued, TaskState::Running, false),
        (TaskState::Assigned, TaskState::Skipped, false),
        (TaskState::Running, TaskState::Skipped, false),
        (TaskState::Failed, TaskState::Completed, false),
        (TaskState::Failed, TaskState::Queued, false),
        (TaskState::Completed, TaskState::Queued, true),
        (TaskState::Cancelled, TaskState::Queued, true),
        (TaskState::Skipped, TaskState::Queued, true),
    ];
    for (from, to, clear_owner) in denied {
        let registry = TaskRegistry::default();
        let mut task = queued_task(0);
        task.workflow_id = Some("workflow".to_owned());
        task.state = from;
        task.owner_agent_id = (!matches!(from, TaskState::Queued)).then(|| owner.clone());
        registry.upsert(task.clone());

        let error = registry
            .cas_durable(
                &TaskId::from("task"),
                0,
                &operation(&format!("denied-{from:?}-{to:?}")),
                "denied",
                &TaskCasMutation::WorkflowState {
                    state: to,
                    clear_owner,
                    outcome_ref: None,
                    failure_class: None,
                },
                |_| panic!("denied transition reached persistence"),
            )
            .unwrap_err();
        assert!(error.to_string().contains("illegal workflow task transition"));
        assert_eq!(registry.get(&TaskId::from("task")), Some(task));
    }
}

#[test]
fn supervisor_retry_is_limited_to_direct_retryable_failures() {
    let owner = AgentId::from("owner");
    let mut task = queued_task(0);
    task.state = TaskState::Failed;
    task.owner_agent_id = Some(owner.clone());
    task.failure_class = Some(TaskFailureClass::Retryable);
    let registry = TaskRegistry::default();
    registry.upsert(task);

    let updated = registry
        .cas_durable(
            &TaskId::from("task"),
            0,
            &operation("supervisor-retry"),
            "retry",
            &TaskCasMutation::SupervisorRetry {
                reason: Some("transient".to_owned()),
            },
            |_| Ok(()),
        )
        .unwrap();
    assert_eq!(updated.state, TaskState::Queued);
    assert!(updated.owner_agent_id.is_none());
    assert_eq!(updated.outcome_ref.as_deref(), Some("transient"));
    assert!(updated.failure_class.is_none());

    let mut workflow_task = queued_task(0);
    workflow_task.workflow_id = Some("workflow".to_owned());
    workflow_task.state = TaskState::Failed;
    workflow_task.owner_agent_id = Some(owner);
    workflow_task.failure_class = Some(TaskFailureClass::Retryable);
    let workflow_registry = TaskRegistry::default();
    workflow_registry.upsert(workflow_task);
    let error = workflow_registry
        .cas_durable(
            &TaskId::from("task"),
            0,
            &operation("workflow-supervisor-retry"),
            "retry",
            &TaskCasMutation::SupervisorRetry { reason: None },
            |_| panic!("workflow Supervisor retry must not use direct mutation"),
        )
        .unwrap_err();
    assert!(error.to_string().contains("direct failed task"));
}
