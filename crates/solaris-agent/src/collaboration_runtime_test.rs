use super::*;
use crate::permission_engine::PermissionContext;
use crate::relationship_store::AgentRelationshipStore;
use crate::resource_policy::ResourcePolicy;
use crate::runtime_ledger::{InMemoryRuntimeLedger, LedgerRecord, RuntimeLedger};
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::{OperationId, TaskId};
use solaris_types::permission::{PermissionCeiling, PermissionMode};
use solaris_types::runtime::AgentLifecycleState;
use solaris_types::spawner::{AgentOutcomeStatus, SubAgentResult};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Default)]
struct ToggleLedger {
    inner: InMemoryRuntimeLedger,
    fail: AtomicBool,
}

impl ToggleLedger {
    fn set_fail(&self, fail: bool) {
        self.fail.store(fail, Ordering::SeqCst);
    }
}

impl RuntimeLedger for ToggleLedger {
    crate::runtime_ledger::forward_compare_and_append!();

    fn append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        payload: serde_json::Value,
    ) -> std::io::Result<LedgerRecord> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(std::io::Error::other("injected ledger failure"));
        }
        self.inner.append(run_id, durability, record_type, payload)
    }

    fn run_ids(&self) -> std::io::Result<Vec<RunId>> {
        self.inner.run_ids()
    }

    fn records_for_run(&self, run_id: &RunId) -> std::io::Result<Vec<LedgerRecord>> {
        self.inner.records_for_run(run_id)
    }
}

#[test]
fn exposes_scheduler_lifecycle() {
    let scheduler = Scheduler::new(ResourcePolicy::new(1));
    let mut runtime = CollaborationRuntime::new(scheduler);
    runtime.enqueue(ScheduledTask {
        id: "task-1".into(),
        payload: 42,
    });
    assert_eq!(runtime.queued_len(), 1);
    let task = runtime.acquire_next().expect("task should be scheduled");
    assert_eq!(task.payload, 42);
    runtime.release();
    assert_eq!(runtime.active(), 0);
}

#[test]
fn legacy_inline_agent_outcome_remains_readable() {
    let runtime: CollaborationRuntime<()> = CollaborationRuntime::new(Scheduler::new(ResourcePolicy::new(2)));
    let run_id = RunId::from("legacy-inline-outcome-run");
    let parent = AgentId::from("legacy-inline-outcome-parent");
    runtime.agents().upsert(AgentRecord {
        run_id: run_id.clone(),
        agent_id: parent.clone(),
        team_id: None,
        parent_agent_id: None,
        state: AgentLifecycleState::Active,
    });
    let operation_id = OperationId::from("legacy-inline-outcome-operation");
    let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    let reservation = runtime
        .reserve_spawn(
            run_id.clone(),
            parent,
            operation_id.clone(),
            &permissions,
            PermissionCeiling::unrestricted(),
        )
        .unwrap();
    runtime.commit_spawn(&reservation).unwrap();
    let result = SubAgentResult {
        name: "legacy-worker".into(),
        agent_id: Some(reservation.child_agent_id.clone()),
        task_id: None,
        status: AgentOutcomeStatus::Completed,
        failure_class: None,
        output: Some(serde_json::json!({"text":"legacy result"})),
        text: "legacy result".into(),
        usage: Default::default(),
        turns: 1,
        is_error: false,
    };
    runtime
        .ledger()
        .append(
            &run_id,
            DurabilityClass::SyncCritical,
            "agent_outcome",
            serde_json::json!({
                "spawn_operation_id": operation_id,
                "child_agent_id": reservation.child_agent_id,
                "result": result,
            }),
        )
        .unwrap();

    let recovered = runtime.agent_outcome(&reservation).unwrap().unwrap();

    assert_eq!(
        serde_json::to_value(recovered).unwrap(),
        serde_json::to_value(result).unwrap()
    );
}

#[test]
fn supervisor_isolation_cannot_be_bypassed_by_omitting_team_id() {
    let runtime: CollaborationRuntime<()> = CollaborationRuntime::new(Scheduler::new(ResourcePolicy::new(4)));
    let run = RunId::from("supervisor-run");
    let root = AgentId::from("supervisor-root");
    let worker_a = AgentId::from("worker-a");
    let worker_b = AgentId::from("worker-b");
    for agent_id in [&root, &worker_a, &worker_b] {
        runtime.agents().upsert(AgentRecord {
            run_id: run.clone(),
            agent_id: agent_id.clone(),
            team_id: None,
            parent_agent_id: (agent_id != &root).then(|| root.clone()),
            state: AgentLifecycleState::Active,
        });
    }
    let team = TeamId::from("supervisor-team");
    runtime
        .create_collaboration_team(
            run.clone(),
            team.clone(),
            "supervisor",
            CollaborationStrategy::Supervisor,
            Some(root.clone()),
        )
        .unwrap();
    for agent_id in [&root, &worker_a, &worker_b] {
        runtime.join_team(&run, &team, agent_id.clone()).unwrap();
    }

    assert!(
        runtime
            .send_message(run.clone(), None, worker_a.clone(), worker_b.clone(), "peer", json!({}),)
            .is_err()
    );
    runtime
        .send_message(run.clone(), None, worker_a, root.clone(), "result", json!({"ok": true}))
        .unwrap();
    let before_root = runtime.messages().inbox(&root).len();
    let before_worker_b = runtime.messages().inbox(&worker_b).len();
    assert!(
        runtime
            .broadcast_message(
                run.clone(),
                team.clone(),
                AgentId::from("worker-a"),
                "forbidden",
                json!({}),
            )
            .is_err()
    );
    assert_eq!(runtime.messages().inbox(&root).len(), before_root);
    assert_eq!(runtime.messages().inbox(&worker_b).len(), before_worker_b);
    let worker_task = TaskRecord {
        run_id: run.clone(),
        task_id: CollaborationRuntime::<()>::scoped_team_task_id(&team, "worker-created"),
        revision: 0,
        task_key: None,
        team_id: Some(team.clone()),
        workflow_id: None,
        node_id: None,
        role: Some("worker".into()),
        depends_on: Vec::new(),
        content: None,
        expected_write_scope: Vec::new(),
        owner_agent_id: Some(worker_b.clone()),
        state: TaskState::Queued,
        outcome_ref: None,
        failure_class: None,
    };
    assert!(
        runtime
            .create_collaboration_task(&run, &team, &AgentId::from("worker-a"), 256, worker_task)
            .is_err()
    );
    let broadcast = runtime
        .broadcast_message(run, team, root, "assignment", json!({"task": "review"}))
        .unwrap();
    assert_eq!(broadcast.len(), 2);
}

#[test]
fn only_supervisor_coordinator_can_cancel_a_queued_workflow_task_and_replay_is_stable() {
    let runtime: CollaborationRuntime<()> = CollaborationRuntime::new(Scheduler::new(ResourcePolicy::new(2)));
    let run = RunId::from("supervisor-cancel-run");
    let coordinator = AgentId::from("supervisor-cancel-coordinator");
    let worker = AgentId::from("supervisor-cancel-worker");
    for agent_id in [&coordinator, &worker] {
        runtime.agents().upsert(AgentRecord {
            run_id: run.clone(),
            agent_id: agent_id.clone(),
            team_id: None,
            parent_agent_id: None,
            state: AgentLifecycleState::Active,
        });
    }
    let team = TeamId::from("supervisor-cancel-team");
    runtime
        .create_collaboration_team(
            run.clone(),
            team.clone(),
            "supervisor",
            CollaborationStrategy::Supervisor,
            Some(coordinator.clone()),
        )
        .unwrap();
    runtime.join_team(&run, &team, coordinator.clone()).unwrap();
    runtime.join_team(&run, &team, worker.clone()).unwrap();
    let task_id = TaskId::from("supervisor-cancel-task");
    runtime
        .create_collaboration_task(
            &run,
            &team,
            &coordinator,
            256,
            TaskRecord {
                run_id: run.clone(),
                task_id: task_id.clone(),
                revision: 0,
                task_key: Some("queued".into()),
                team_id: Some(team.clone()),
                workflow_id: Some("workflow:test".into()),
                node_id: None,
                role: Some("worker".into()),
                depends_on: Vec::new(),
                content: None,
                expected_write_scope: Vec::new(),
                owner_agent_id: None,
                state: TaskState::Queued,
                outcome_ref: None,
                failure_class: None,
            },
        )
        .unwrap();
    let operation = OperationId::from("supervisor-cancel-operation");

    assert!(
        runtime
            .cancel_supervisor_workflow_task(&run, &task_id, &worker, 0, &operation)
            .is_err()
    );
    assert_eq!(
        runtime
            .cancel_supervisor_workflow_task(&run, &task_id, &coordinator, 0, &operation)
            .unwrap(),
        1
    );
    assert_eq!(
        runtime
            .cancel_supervisor_workflow_task(&run, &task_id, &coordinator, 0, &operation)
            .unwrap(),
        1
    );
    assert_eq!(runtime.tasks().get(&task_id).unwrap().state, TaskState::Cancelled);

    for (label, mark_running) in [("assigned", false), ("running", true)] {
        let task_id = TaskId::new(format!("supervisor-cancel-{label}"));
        runtime
            .create_collaboration_task(
                &run,
                &team,
                &coordinator,
                256,
                TaskRecord {
                    run_id: run.clone(),
                    task_id: task_id.clone(),
                    revision: 0,
                    task_key: Some(label.into()),
                    team_id: Some(team.clone()),
                    workflow_id: Some("workflow:test".into()),
                    node_id: None,
                    role: Some("worker".into()),
                    depends_on: Vec::new(),
                    content: None,
                    expected_write_scope: Vec::new(),
                    owner_agent_id: Some(worker.clone()),
                    state: TaskState::Queued,
                    outcome_ref: None,
                    failure_class: None,
                },
            )
            .unwrap();
        let revision = if mark_running {
            runtime
                .mark_task_running(
                    &run,
                    &task_id,
                    &worker,
                    0,
                    &OperationId::new(format!("supervisor-mark-{label}")),
                )
                .unwrap()
        } else {
            0
        };
        runtime
            .cancel_supervisor_workflow_task(
                &run,
                &task_id,
                &coordinator,
                revision,
                &OperationId::new(format!("supervisor-cancel-{label}")),
            )
            .unwrap();
        assert_eq!(runtime.tasks().get(&task_id).unwrap().state, TaskState::Cancelled);
    }
}

#[test]
fn spawn_reservation_is_idempotent_after_commit() {
    let runtime: CollaborationRuntime<()> = CollaborationRuntime::new(Scheduler::new(ResourcePolicy::new(2)));
    let permissions = PermissionContext::new(PermissionMode::Plan, PermissionCeiling::plan());
    let first = runtime
        .reserve_spawn(
            RunId::from("run"),
            AgentId::from("root"),
            OperationId::from("spawn-1"),
            &permissions,
            PermissionCeiling::unrestricted(),
        )
        .unwrap();
    assert!(!first.permission_ceiling.workspace_mutation);
    runtime.commit_spawn(&first).unwrap();
    let second = runtime
        .reserve_spawn(
            RunId::from("run"),
            AgentId::from("root"),
            OperationId::from("spawn-1"),
            &permissions,
            PermissionCeiling::unrestricted(),
        )
        .unwrap();
    assert!(second.reattached);
    assert_eq!(first.child_agent_id, second.child_agent_id);
}

#[test]
fn spawn_terminal_commands_are_idempotent_and_never_reactivate_cancelled_agent() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let runtime: CollaborationRuntime<()> =
        CollaborationRuntime::with_ledger(Scheduler::new(ResourcePolicy::new(2)), ledger.clone());
    let run = RunId::from("terminal-run");
    let root = AgentId::from("root");
    runtime.agents().upsert(AgentRecord {
        run_id: run.clone(),
        agent_id: root.clone(),
        team_id: None,
        parent_agent_id: None,
        state: AgentLifecycleState::Active,
    });
    let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    let reservation = runtime
        .reserve_spawn(
            run.clone(),
            root,
            OperationId::from("terminal-spawn"),
            &permissions,
            PermissionCeiling::unrestricted(),
        )
        .unwrap();

    runtime.commit_spawn(&reservation).unwrap();
    runtime.commit_spawn(&reservation).unwrap();
    assert_eq!(
        ledger
            .records_for_run(&run)
            .unwrap()
            .iter()
            .filter(|record| record.record_type == "agent_spawn_committed")
            .count(),
        1
    );

    runtime.cancel_spawn(&reservation, "test cancel").unwrap();
    runtime.cancel_spawn(&reservation, "duplicate cancel").unwrap();
    runtime.abort_spawn(&reservation, "abort after commit").unwrap();
    assert_eq!(
        runtime.agents().get(&reservation.child_agent_id).unwrap().state,
        AgentLifecycleState::Cancelled
    );
    assert_eq!(
        ledger
            .records_for_run(&run)
            .unwrap()
            .iter()
            .filter(|record| record.record_type == "agent_spawn_cancelled")
            .count(),
        1
    );
    assert!(runtime.commit_spawn(&reservation).is_err());
}

#[test]
fn failed_reattach_preserves_agent_relationship_and_team_across_restore() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run = RunId::from("reattach-run");
    let root = AgentId::from("root");
    let team = TeamId::from("reattach-team");
    let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    let runtime: CollaborationRuntime<()> =
        CollaborationRuntime::with_ledger(Scheduler::new(ResourcePolicy::new(2)), ledger.clone());
    runtime.agents().upsert(AgentRecord {
        run_id: run.clone(),
        agent_id: root.clone(),
        team_id: None,
        parent_agent_id: None,
        state: AgentLifecycleState::Active,
    });
    let first = runtime
        .reserve_spawn(
            run.clone(),
            root.clone(),
            OperationId::from("stable-spawn"),
            &permissions,
            PermissionCeiling::unrestricted(),
        )
        .unwrap();
    runtime.commit_spawn(&first).unwrap();
    runtime
        .create_collaboration_team(
            run.clone(),
            team.clone(),
            "reattach",
            CollaborationStrategy::Team,
            Some(root.clone()),
        )
        .unwrap();
    runtime.join_team(&run, &team, root.clone()).unwrap();
    runtime.join_team(&run, &team, first.child_agent_id.clone()).unwrap();
    let reattached = runtime
        .reserve_spawn(
            run.clone(),
            root.clone(),
            OperationId::from("stable-spawn"),
            &permissions,
            PermissionCeiling::unrestricted(),
        )
        .unwrap();
    assert!(reattached.reattached);
    runtime.abort_spawn(&reattached, "injected reattach failure").unwrap();
    let mut immediate = runtime.projection();

    let restored: CollaborationRuntime<()> =
        CollaborationRuntime::with_ledger(Scheduler::new(ResourcePolicy::new(2)), ledger);
    restored.agents().upsert(AgentRecord {
        run_id: run.clone(),
        agent_id: root.clone(),
        team_id: None,
        parent_agent_id: None,
        state: AgentLifecycleState::Active,
    });
    restored.restore_projection(&run).unwrap();
    let mut replayed = restored.projection();

    immediate
        .agents
        .sort_by(|left, right| left.agent_id.cmp(&right.agent_id));
    replayed
        .agents
        .sort_by(|left, right| left.agent_id.cmp(&right.agent_id));

    assert_eq!(immediate.agents, replayed.agents);
    assert_eq!(immediate.relationships, replayed.relationships);
    assert_eq!(immediate.teams, replayed.teams);
    assert!(
        restored
            .reserve_spawn(
                run,
                root,
                OperationId::from("stable-spawn"),
                &permissions,
                PermissionCeiling::unrestricted(),
            )
            .unwrap()
            .reattached
    );
}

#[test]
fn failed_critical_writes_never_publish_collaboration_state() {
    let ledger = Arc::new(ToggleLedger::default());
    let runtime: CollaborationRuntime<()> =
        CollaborationRuntime::with_ledger(Scheduler::new(ResourcePolicy::new(2)), ledger.clone());
    let run = RunId::from("failure-run");
    let root = AgentId::from("root");
    runtime.agents().upsert(AgentRecord {
        run_id: run.clone(),
        agent_id: root.clone(),
        team_id: None,
        parent_agent_id: None,
        state: AgentLifecycleState::Active,
    });
    ledger.set_fail(true);
    let failed_team = TeamId::from("failed-team");
    assert!(runtime.create_team(run.clone(), failed_team.clone(), "failed").is_err());
    assert!(runtime.teams().get(&failed_team).is_none());

    let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    assert!(
        runtime
            .reserve_spawn(
                run.clone(),
                root.clone(),
                OperationId::from("failed-reserve"),
                &permissions,
                PermissionCeiling::unrestricted(),
            )
            .is_err()
    );
    assert_eq!(runtime.agents().snapshot().len(), 1);
    assert!(
        runtime
            .set_agent_state(&run, &root, AgentLifecycleState::Failed)
            .is_err()
    );
    assert_eq!(runtime.agents().get(&root).unwrap().state, AgentLifecycleState::Active);

    ledger.set_fail(false);
    let team = TeamId::from("team");
    runtime.create_team(run.clone(), team.clone(), "team").unwrap();
    let reservation = runtime
        .reserve_spawn(
            run.clone(),
            root.clone(),
            OperationId::from("spawn"),
            &permissions,
            PermissionCeiling::unrestricted(),
        )
        .unwrap();
    ledger.set_fail(true);
    assert!(runtime.join_team(&run, &team, root.clone()).is_err());
    assert!(!runtime.teams().get(&team).unwrap().members.contains(&root));
    assert!(runtime.commit_spawn(&reservation).is_err());
    assert!(runtime.relationships().child_for_key(&reservation.key).is_none());
    assert_eq!(
        runtime.agents().get(&reservation.child_agent_id).unwrap().state,
        AgentLifecycleState::Reserved
    );
    assert!(runtime.abort_spawn(&reservation, "failure").is_err());
    assert!(runtime.agents().get(&reservation.child_agent_id).is_some());
}

#[test]
fn restore_rejects_invalid_team_message_limits() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run = RunId::from("invalid-message-limit-restore");
    let team = TeamRecord {
        run_id: run.clone(),
        team_id: TeamId::from("invalid-message-limit-team"),
        name: "invalid limits".into(),
        strategy: CollaborationStrategy::Team,
        coordinator: Some(AgentId::from("coordinator")),
        direct_peer_messaging: true,
        max_pending_messages: 0,
        max_message_bytes: CollaborationRuntimeConfig::DEFAULT_MAX_MESSAGE_BYTES,
        members: std::collections::HashSet::new(),
    };
    ledger
        .append(
            &run,
            DurabilityClass::SyncCritical,
            "team_created",
            serde_json::to_value(team).unwrap(),
        )
        .unwrap();
    let runtime: CollaborationRuntime<()> =
        CollaborationRuntime::with_ledger(Scheduler::new(ResourcePolicy::new(2)), ledger);

    let error = runtime
        .restore_projection(&run)
        .expect_err("invalid restored message limits must fail closed");

    assert!(error.to_string().contains("max_pending_messages"));
    assert!(runtime.teams().snapshot().is_empty());
}

#[test]
fn task_handoff_validates_run_and_owner_and_keeps_owner_on_ledger_failure() {
    let ledger = Arc::new(ToggleLedger::default());
    let runtime: CollaborationRuntime<()> =
        CollaborationRuntime::with_ledger(Scheduler::new(ResourcePolicy::new(2)), ledger.clone());
    let run = RunId::from("handoff-run");
    let other_run = RunId::from("other-run");
    let from = AgentId::from("from");
    let to = AgentId::from("to");
    for agent_id in [&from, &to] {
        runtime.agents().upsert(AgentRecord {
            run_id: run.clone(),
            agent_id: agent_id.clone(),
            team_id: None,
            parent_agent_id: None,
            state: AgentLifecycleState::Active,
        });
    }
    let task_id = TaskId::from("task");
    runtime.tasks().upsert(TaskRecord {
        run_id: run.clone(),
        task_id: task_id.clone(),
        revision: 0,
        task_key: None,
        team_id: None,
        workflow_id: None,
        node_id: None,
        role: None,
        depends_on: Vec::new(),
        content: None,
        expected_write_scope: Vec::new(),
        owner_agent_id: Some(from.clone()),
        state: solaris_types::runtime::TaskState::Assigned,
        outcome_ref: None,
        failure_class: None,
    });

    assert!(
        runtime
            .handoff_task(
                &other_run,
                &task_id,
                &from,
                to.clone(),
                0,
                &OperationId::from("handoff-wrong-run"),
            )
            .is_err()
    );
    assert_eq!(
        runtime.tasks().get(&task_id).unwrap().owner_agent_id,
        Some(from.clone())
    );
    ledger.set_fail(true);
    assert!(
        runtime
            .handoff_task(
                &run,
                &task_id,
                &from,
                to.clone(),
                0,
                &OperationId::from("handoff-ledger-failure"),
            )
            .is_err()
    );
    assert_eq!(
        runtime.tasks().get(&task_id).unwrap().owner_agent_id,
        Some(from.clone())
    );
    ledger.set_fail(false);
    runtime
        .handoff_task(
            &run,
            &task_id,
            &from,
            to.clone(),
            0,
            &OperationId::from("handoff-success"),
        )
        .unwrap();
    assert_eq!(runtime.tasks().get(&task_id).unwrap().owner_agent_id, Some(to));
    assert!(
        runtime
            .handoff_task(
                &run,
                &task_id,
                &from,
                AgentId::from("missing"),
                1,
                &OperationId::from("handoff-missing"),
            )
            .is_err()
    );
}

#[test]
fn team_state_requires_membership_and_projects_durably() {
    let runtime: CollaborationRuntime<()> = CollaborationRuntime::new(Scheduler::new(ResourcePolicy::new(2)));
    let run = RunId::from("team-state-run");
    let team = TeamId::from("team-state");
    let root = AgentId::from("root");
    let outsider = AgentId::from("outsider");
    for agent_id in [root.clone(), outsider.clone()] {
        runtime.agents().upsert(solaris_types::runtime::AgentRecord {
            run_id: run.clone(),
            agent_id,
            team_id: None,
            parent_agent_id: None,
            state: solaris_types::runtime::AgentLifecycleState::Active,
        });
    }
    assert!(runtime.create_team(run.clone(), team.clone(), "state").unwrap());
    assert!(runtime.join_team(&run, &team, root.clone()).unwrap());

    let fact = runtime
        .set_team_fact(&run, &team, &root, "answer", json!({"value": 42}))
        .unwrap();
    assert_eq!(fact.key, "answer");
    assert!(
        runtime
            .set_team_fact(&run, &team, &outsider, "bad", json!(true))
            .is_err()
    );

    let artifact = runtime
        .register_artifact(
            &run,
            Some(team.clone()),
            &root,
            "mesh://artifact/report",
            "report",
            json!({"format":"json"}),
        )
        .unwrap();
    assert_eq!(artifact.kind, "report");
    assert!(
        runtime
            .register_artifact(
                &run,
                Some(team.clone()),
                &outsider,
                "mesh://artifact/forbidden",
                "report",
                json!({}),
            )
            .is_err()
    );

    let projection = runtime.projection();
    assert_eq!(projection.facts.len(), 1);
    assert_eq!(projection.artifacts.len(), 1);
}

#[test]
fn jsonl_restart_restores_collaboration_projection() {
    use crate::runtime_ledger::{JsonlRuntimeLedger, RuntimeLedger};
    use std::sync::Arc;

    let path = std::env::temp_dir().join(format!(
        "solaris-collaboration-restart-{}-{}.jsonl",
        std::process::id(),
        uuid::Uuid::now_v7()
    ));
    let run = RunId::from("restart-run");
    let root = AgentId::from("root");
    let team = TeamId::from("restart-team");
    let operation = OperationId::from("spawn-restart");
    let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    let child_id;
    let task_id = CollaborationRuntime::<()>::scoped_team_task_id(&team, "durable-task");

    {
        let ledger: Arc<dyn RuntimeLedger> = Arc::new(JsonlRuntimeLedger::open(&path).unwrap());
        let runtime: CollaborationRuntime<()> =
            CollaborationRuntime::with_ledger(Scheduler::new(ResourcePolicy::new(4)), ledger);
        runtime.agents().upsert(AgentRecord {
            run_id: run.clone(),
            agent_id: root.clone(),
            team_id: None,
            parent_agent_id: None,
            state: AgentLifecycleState::Active,
        });
        let reservation = runtime
            .reserve_spawn(
                run.clone(),
                root.clone(),
                operation.clone(),
                &permissions,
                PermissionCeiling::unrestricted(),
            )
            .unwrap();
        child_id = reservation.child_agent_id.clone();
        runtime.commit_spawn(&reservation).unwrap();
        runtime
            .set_agent_state(&run, &child_id, AgentLifecycleState::Completed)
            .unwrap();
        runtime.create_team(run.clone(), team.clone(), "restart").unwrap();
        runtime.join_team(&run, &team, root.clone()).unwrap();
        runtime.join_team(&run, &team, child_id.clone()).unwrap();
        runtime.set_team_fact(&run, &team, &root, "answer", json!(42)).unwrap();
        runtime
            .register_artifact(
                &run,
                Some(team.clone()),
                &root,
                "mesh://restart/report",
                "report",
                json!({"durable":true}),
            )
            .unwrap();
        // JSONL is legacy/migration-only and no longer advertises atomic Run
        // task admission. Write one historical task record through the legacy
        // single-record path so restart compatibility remains covered without
        // claiming that new JSONL collaboration admission is safe.
        runtime
            .register_runtime_task(
                &run,
                TaskRecord {
                    run_id: run.clone(),
                    task_id: task_id.clone(),
                    revision: 0,
                    task_key: Some("durable-task".to_owned()),
                    team_id: Some(team.clone()),
                    workflow_id: None,
                    node_id: None,
                    role: Some("worker".into()),
                    depends_on: Vec::new(),
                    content: None,
                    expected_write_scope: Vec::new(),
                    owner_agent_id: Some(root.clone()),
                    state: TaskState::Assigned,
                    outcome_ref: None,
                    failure_class: None,
                },
            )
            .unwrap();
        runtime
            .handoff_task(
                &run,
                &task_id,
                &root,
                child_id.clone(),
                0,
                &OperationId::from("restart-handoff"),
            )
            .unwrap();
        runtime
            .settle_collaboration_task(
                &run,
                &task_id,
                &child_id,
                1,
                &OperationId::from("restart-settle"),
                TaskSettlement {
                    state: TaskState::Completed,
                    outcome_ref: None,
                    failure_class: None,
                },
            )
            .unwrap();
        runtime
            .send_message(
                run.clone(),
                Some(team.clone()),
                root.clone(),
                child_id.clone(),
                "handoff",
                json!({"hello":"child"}),
            )
            .unwrap();
    }

    {
        let ledger: Arc<dyn RuntimeLedger> = Arc::new(JsonlRuntimeLedger::open(&path).unwrap());
        let runtime: CollaborationRuntime<()> =
            CollaborationRuntime::with_ledger(Scheduler::new(ResourcePolicy::new(4)), ledger);
        runtime.agents().upsert(AgentRecord {
            run_id: run.clone(),
            agent_id: root.clone(),
            team_id: None,
            parent_agent_id: None,
            state: AgentLifecycleState::Active,
        });
        runtime.restore_projection(&run).unwrap();
        assert_eq!(
            runtime.agents().get(&child_id).unwrap().state,
            AgentLifecycleState::Completed
        );
        assert_eq!(runtime.team_facts(&team).len(), 1);
        assert_eq!(runtime.artifacts(Some(&team), None).len(), 1);
        assert_eq!(runtime.messages().inbox(&child_id).len(), 1);
        let restored_task = runtime.tasks().get(&task_id).expect("durable Team task restored");
        assert_eq!(restored_task.team_id, Some(team.clone()));
        assert_eq!(restored_task.owner_agent_id, Some(child_id.clone()));
        assert_eq!(restored_task.state, TaskState::Completed);
        let restored_team = runtime.teams().get(&team).unwrap();
        assert!(restored_team.members.contains(&root));
        assert!(restored_team.members.contains(&child_id));

        let reattached = runtime
            .reserve_spawn(
                run.clone(),
                root.clone(),
                operation.clone(),
                &permissions,
                PermissionCeiling::unrestricted(),
            )
            .unwrap();
        assert!(reattached.reattached);
        assert_eq!(reattached.child_agent_id, child_id);
    }

    let _ = std::fs::remove_file(path);
}
