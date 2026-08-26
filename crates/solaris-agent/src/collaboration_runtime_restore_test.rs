use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier, mpsc};
use std::time::Duration;

use serde_json::{Value, json};
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::{AgentId, OperationId, RunId, TaskId};
use solaris_types::runtime::{AgentLifecycleState, AgentRecord, TaskFailureClass, TaskRecord, TaskState};

use crate::execution_context::stable_digest_value;
use crate::resource_policy::ResourcePolicy;
use crate::runtime_ledger::{InMemoryRuntimeLedger, LedgerRecord, RuntimeLedger};

use super::{CollaborationRuntime, Scheduler};

struct RestoreBarrierLedger {
    inner: InMemoryRuntimeLedger,
    block_next_read: AtomicBool,
    snapshot_ready: Arc<Barrier>,
    release_snapshot: Arc<Barrier>,
}

impl RestoreBarrierLedger {
    fn new() -> Self {
        Self {
            inner: InMemoryRuntimeLedger::default(),
            block_next_read: AtomicBool::new(false),
            snapshot_ready: Arc::new(Barrier::new(2)),
            release_snapshot: Arc::new(Barrier::new(2)),
        }
    }

    fn arm_read_barrier(&self) {
        self.block_next_read.store(true, Ordering::SeqCst);
    }
}

impl RuntimeLedger for RestoreBarrierLedger {
    crate::runtime_ledger::forward_compare_and_append!();

    fn append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        payload: Value,
    ) -> std::io::Result<LedgerRecord> {
        self.inner.append(run_id, durability, record_type, payload)
    }

    fn run_ids(&self) -> std::io::Result<Vec<RunId>> {
        self.inner.run_ids()
    }

    fn records_for_run(&self, run_id: &RunId) -> std::io::Result<Vec<LedgerRecord>> {
        let records = self.inner.records_for_run(run_id)?;
        if self.block_next_read.swap(false, Ordering::SeqCst) {
            self.snapshot_ready.wait();
            self.release_snapshot.wait();
        }
        Ok(records)
    }
}

fn queued_workflow_task(run_id: &RunId, workflow_id: &str, node_id: &str) -> TaskRecord {
    TaskRecord {
        run_id: run_id.clone(),
        task_id: TaskId::new(format!("workflow:{run_id}:{node_id}")),
        revision: 0,
        task_key: Some(format!("workflow:{workflow_id}:{node_id}")),
        team_id: None,
        workflow_id: Some(workflow_id.to_owned()),
        node_id: Some(node_id.to_owned()),
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

struct TaskCasAppend<'a> {
    record_run_id: &'a RunId,
    outer_task_id: &'a TaskId,
    operation_id: &'a OperationId,
    transition: &'a str,
    mutation: Value,
    expected_revision: u64,
    task: &'a TaskRecord,
}

fn append_task_cas(ledger: &dyn RuntimeLedger, append: TaskCasAppend<'_>) {
    let mutation_digest = stable_digest_value(&append.mutation);
    ledger
        .append(
            append.record_run_id,
            DurabilityClass::SyncCritical,
            "task_cas",
            json!({
                "task_id": append.outer_task_id,
                "operation_id": append.operation_id,
                "transition": append.transition,
                "mutation": append.mutation,
                "mutation_digest": mutation_digest,
                "expected_revision": append.expected_revision,
                "new_revision": append.task.revision,
                "task": append.task,
            }),
        )
        .unwrap();
}

fn append_new_wire_task_cas(
    ledger: &dyn RuntimeLedger,
    run_id: &RunId,
    operation_id: &OperationId,
    transition: &str,
    mutation: Value,
    expected_task: &TaskRecord,
    task: &TaskRecord,
) {
    let mutation_digest = stable_digest_value(&mutation);
    ledger
        .append(
            run_id,
            DurabilityClass::SyncCritical,
            "task_cas",
            json!({
                "task_id": task.task_id,
                "operation_id": operation_id,
                "transition": transition,
                "mutation": mutation,
                "mutation_digest": mutation_digest,
                "expected_revision": expected_task.revision,
                "new_revision": task.revision,
                "expected_task": expected_task,
                "task": task,
            }),
        )
        .unwrap();
}

#[test]
fn direct_supervisor_retry_cas_restores_queued_projection() {
    let run_id = RunId::from("restore-direct-supervisor-retry");
    let task_id = TaskId::from("collaboration:restore-direct-supervisor-retry:worker");
    let owner = AgentId::from("restore-direct-supervisor-owner");
    let failed = TaskRecord {
        run_id: run_id.clone(),
        task_id: task_id.clone(),
        revision: 0,
        task_key: Some("collaboration:worker".to_owned()),
        team_id: None,
        workflow_id: None,
        node_id: None,
        role: Some("worker".to_owned()),
        depends_on: Vec::new(),
        content: None,
        expected_write_scope: Vec::new(),
        owner_agent_id: Some(owner),
        state: TaskState::Failed,
        outcome_ref: Some("agent-outcome:attempt-0".to_owned()),
        failure_class: Some(TaskFailureClass::Retryable),
    };
    let mut queued = failed.clone();
    queued.revision = 1;
    queued.owner_agent_id = None;
    queued.state = TaskState::Queued;
    queued.outcome_ref = Some("transient".to_owned());
    queued.failure_class = None;

    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    ledger
        .append(
            &run_id,
            DurabilityClass::SyncCritical,
            "task_created",
            serde_json::to_value(&failed).unwrap(),
        )
        .unwrap();
    append_new_wire_task_cas(
        ledger.as_ref(),
        &run_id,
        &OperationId::from("direct-supervisor-retry-cas"),
        "supervisor_retry",
        json!({"supervisor_retry": {"reason": "transient"}}),
        &failed,
        &queued,
    );

    let runtime = CollaborationRuntime::with_ledger(Scheduler::<()>::new(ResourcePolicy::new(2)), ledger);
    runtime.restore_projection(&run_id).unwrap();
    assert_eq!(runtime.tasks().get(&task_id), Some(queued));
}

#[test]
fn restore_projection_serializes_snapshot_application_with_terminal_agent_write() {
    let ledger = Arc::new(RestoreBarrierLedger::new());
    let run_id = RunId::from("restore-lock-root");
    let parent = AgentId::from("restore-lock-parent");
    let child = AgentId::from("restore-lock-child");
    ledger
        .append(
            &run_id,
            DurabilityClass::SyncCritical,
            "agent_spawn_intent",
            json!({"parent_agent_id": parent, "child_agent_id": child}),
        )
        .unwrap();
    let runtime = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::<()>::new(ResourcePolicy::new(2)),
        Arc::clone(&ledger) as Arc<dyn RuntimeLedger>,
    ));
    runtime.agents().upsert(AgentRecord {
        run_id: run_id.clone(),
        agent_id: child.clone(),
        team_id: None,
        parent_agent_id: Some(parent),
        state: AgentLifecycleState::Reserved,
    });
    ledger.arm_read_barrier();

    let restoring = {
        let runtime = Arc::clone(&runtime);
        let run_id = run_id.clone();
        std::thread::spawn(move || runtime.restore_projection(&run_id))
    };
    ledger.snapshot_ready.wait();
    let (finished_tx, finished_rx) = mpsc::channel();
    let terminal_write = {
        let runtime = Arc::clone(&runtime);
        let run_id = run_id.clone();
        let child = child.clone();
        std::thread::spawn(move || {
            let result = runtime.set_agent_state(&run_id, &child, AgentLifecycleState::Completed);
            finished_tx.send(()).unwrap();
            result
        })
    };
    assert!(finished_rx.recv_timeout(Duration::from_millis(100)).is_err());
    ledger.release_snapshot.wait();
    restoring.join().unwrap().unwrap();
    terminal_write.join().unwrap().unwrap();

    assert_eq!(
        runtime.agents().get(&child).unwrap().state,
        AgentLifecycleState::Completed
    );
}

#[test]
fn task_cas_restore_rejects_tampered_identity_owner_transition_and_lineage() {
    struct Case {
        name: &'static str,
        record_run_id: RunId,
        outer_task_id: TaskId,
        transition: &'static str,
        mutation: Value,
        task: TaskRecord,
    }

    let workflow_run = RunId::from("restore-validation-root:workflow:one");
    let base = queued_workflow_task(&workflow_run, "restore-validation", "work");
    let owner = AgentId::from("restore-validation-owner");
    let mut assigned = base.clone();
    assigned.revision = 1;
    assigned.owner_agent_id = Some(owner.clone());
    assigned.state = TaskState::Assigned;
    let mut wrong_owner = assigned.clone();
    wrong_owner.owner_agent_id = Some(AgentId::from("tampered-owner"));
    let mut wrong_workflow = assigned.clone();
    wrong_workflow.workflow_id = Some("tampered-workflow".to_owned());
    let mut wrong_revision = assigned.clone();
    wrong_revision.revision = 2;
    let cases = vec![
        Case {
            name: "foreign lineage",
            record_run_id: RunId::from("foreign-root"),
            outer_task_id: assigned.task_id.clone(),
            transition: "assign",
            mutation: json!({"assign": {"owner": owner}}),
            task: assigned.clone(),
        },
        Case {
            name: "outer task id mismatch",
            record_run_id: RunId::from("restore-validation-root"),
            outer_task_id: TaskId::from("different-task"),
            transition: "assign",
            mutation: json!({"assign": {"owner": owner}}),
            task: assigned.clone(),
        },
        Case {
            name: "owner mismatch",
            record_run_id: RunId::from("restore-validation-root"),
            outer_task_id: assigned.task_id.clone(),
            transition: "assign",
            mutation: json!({"assign": {"owner": owner}}),
            task: wrong_owner,
        },
        Case {
            name: "transition mismatch",
            record_run_id: RunId::from("restore-validation-root"),
            outer_task_id: assigned.task_id.clone(),
            transition: "mark_running",
            mutation: json!({"assign": {"owner": owner}}),
            task: assigned.clone(),
        },
        Case {
            name: "revision mismatch",
            record_run_id: RunId::from("restore-validation-root"),
            outer_task_id: assigned.task_id.clone(),
            transition: "assign",
            mutation: json!({"assign": {"owner": owner}}),
            task: wrong_revision,
        },
        Case {
            name: "workflow identity mismatch",
            record_run_id: RunId::from("restore-validation-root"),
            outer_task_id: assigned.task_id.clone(),
            transition: "assign",
            mutation: json!({"assign": {"owner": owner}}),
            task: wrong_workflow,
        },
    ];

    for case in cases {
        let ledger = Arc::new(InMemoryRuntimeLedger::default());
        let runtime = CollaborationRuntime::with_ledger(
            Scheduler::<()>::new(ResourcePolicy::new(1)),
            Arc::clone(&ledger) as Arc<dyn RuntimeLedger>,
        );
        runtime.tasks().upsert(base.clone());
        let operation_id = OperationId::new(format!("restore-validation-{}", case.name));
        append_task_cas(
            ledger.as_ref(),
            TaskCasAppend {
                record_run_id: &case.record_run_id,
                outer_task_id: &case.outer_task_id,
                operation_id: &operation_id,
                transition: case.transition,
                mutation: case.mutation,
                expected_revision: 0,
                task: &case.task,
            },
        );

        assert!(
            runtime.restore_projection(&case.record_run_id).is_err(),
            "{} must be rejected",
            case.name
        );
        assert_eq!(runtime.tasks().get(&base.task_id), Some(base.clone()), "{}", case.name);
    }
}

#[test]
fn new_wire_task_cas_rejects_non_transition_field_tampering() {
    struct Case {
        name: &'static str,
        transition: &'static str,
        mutation: Value,
        expected: TaskRecord,
        tampered: TaskRecord,
    }

    let workflow_run = RunId::from("new-wire-validation-root:workflow:one");
    let root_run = RunId::from("new-wire-validation-root");
    let base = queued_workflow_task(&workflow_run, "new-wire-validation", "work");
    let owner_a = AgentId::from("new-wire-owner-a");
    let owner_b = AgentId::from("new-wire-owner-b");
    let mut assigned = base.clone();
    assigned.revision = 1;
    assigned.state = TaskState::Assigned;
    assigned.owner_agent_id = Some(owner_a.clone());
    let mut running = assigned.clone();
    running.revision = 2;
    running.state = TaskState::Running;
    let mut handed_off = assigned.clone();
    handed_off.revision = 2;
    handed_off.owner_agent_id = Some(owner_b.clone());

    let mut tampered_assign = assigned.clone();
    tampered_assign.outcome_ref = Some("tampered-assign-outcome".to_owned());
    let mut tampered_running = running;
    tampered_running.failure_class = Some(TaskFailureClass::NonRetryable);
    let mut tampered_handoff = handed_off;
    tampered_handoff.outcome_ref = Some("tampered-handoff-outcome".to_owned());
    let cases = [
        Case {
            name: "assign outcome",
            transition: "assign",
            mutation: json!({"assign": {"owner": owner_a}}),
            expected: base,
            tampered: tampered_assign,
        },
        Case {
            name: "mark_running failure class",
            transition: "mark_running",
            mutation: json!({"mark_running": {"owner": owner_a}}),
            expected: assigned.clone(),
            tampered: tampered_running,
        },
        Case {
            name: "handoff outcome",
            transition: "handoff",
            mutation: json!({"handoff": {"from": owner_a, "to": owner_b}}),
            expected: assigned,
            tampered: tampered_handoff,
        },
    ];

    for case in cases {
        let ledger = Arc::new(InMemoryRuntimeLedger::default());
        append_new_wire_task_cas(
            ledger.as_ref(),
            &root_run,
            &OperationId::new(format!("new-wire-{}", case.name)),
            case.transition,
            case.mutation,
            &case.expected,
            &case.tampered,
        );
        let runtime = CollaborationRuntime::with_ledger(
            Scheduler::<()>::new(ResourcePolicy::new(1)),
            Arc::clone(&ledger) as Arc<dyn RuntimeLedger>,
        );
        runtime.tasks().upsert(case.expected.clone());

        assert!(
            runtime.restore_projection(&root_run).is_err(),
            "{} must be rejected",
            case.name
        );
        assert_eq!(
            runtime.tasks().get(&case.expected.task_id),
            Some(case.expected),
            "{}",
            case.name
        );
    }
}

#[test]
fn new_wire_task_cas_requires_expected_task_and_mutation_as_a_pair() {
    let workflow_run = RunId::from("paired-wire-validation-root:workflow:one");
    let root_run = RunId::from("paired-wire-validation-root");
    let expected = queued_workflow_task(&workflow_run, "paired-wire-validation", "work");
    let owner = AgentId::from("paired-wire-owner");
    let mutation = json!({"assign": {"owner": owner}});
    let mutation_digest = stable_digest_value(&mutation);
    let mut tampered = expected.clone();
    tampered.revision = 1;
    tampered.state = TaskState::Assigned;
    tampered.owner_agent_id = Some(owner);
    tampered.outcome_ref = Some("tampered-outcome".to_owned());

    let mut accepted = Vec::new();
    for (name, include_expected_task, include_mutation) in [
        ("expected_task without mutation", true, false),
        ("mutation without expected_task", false, true),
    ] {
        let ledger = Arc::new(InMemoryRuntimeLedger::default());
        let mut payload = json!({
            "task_id": tampered.task_id,
            "operation_id": OperationId::new(format!("paired-wire-{name}")),
            "transition": "assign",
            "mutation": mutation,
            "mutation_digest": mutation_digest,
            "expected_revision": expected.revision,
            "new_revision": tampered.revision,
            "expected_task": expected,
            "task": tampered,
        });
        if !include_expected_task {
            payload.as_object_mut().unwrap().remove("expected_task");
        }
        if !include_mutation {
            payload.as_object_mut().unwrap().remove("mutation");
        }
        ledger
            .append(&root_run, DurabilityClass::SyncCritical, "task_cas", payload)
            .unwrap();
        let runtime = CollaborationRuntime::with_ledger(
            Scheduler::<()>::new(ResourcePolicy::new(1)),
            Arc::clone(&ledger) as Arc<dyn RuntimeLedger>,
        );
        runtime.tasks().upsert(expected.clone());

        if runtime.restore_projection(&root_run).is_ok() {
            accepted.push(name);
        } else {
            assert_eq!(runtime.tasks().get(&expected.task_id), Some(expected.clone()));
        }
    }
    assert!(
        accepted.is_empty(),
        "partially new wire records were accepted: {accepted:?}"
    );
}

#[test]
fn task_cas_restore_rejects_reused_operation_and_accepts_explicit_workflow_lineage() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let workflow_run = RunId::from("restore-operation-root:workflow:one");
    let root_run = RunId::from("restore-operation-root");
    let base = queued_workflow_task(&workflow_run, "restore-operation", "work");
    let owner = AgentId::from("restore-operation-owner");
    let operation_id = OperationId::from("restore-operation-assign");
    let mut assigned = base.clone();
    assigned.revision = 1;
    assigned.owner_agent_id = Some(owner.clone());
    assigned.state = TaskState::Assigned;
    append_new_wire_task_cas(
        ledger.as_ref(),
        &root_run,
        &operation_id,
        "assign",
        json!({"assign": {"owner": owner}}),
        &base,
        &assigned,
    );
    let mut running = assigned.clone();
    running.revision = 2;
    running.state = TaskState::Running;
    append_new_wire_task_cas(
        ledger.as_ref(),
        &root_run,
        &operation_id,
        "mark_running",
        json!({"mark_running": {"owner": owner}}),
        &assigned,
        &running,
    );
    let runtime = CollaborationRuntime::with_ledger(
        Scheduler::<()>::new(ResourcePolicy::new(1)),
        Arc::clone(&ledger) as Arc<dyn RuntimeLedger>,
    );
    runtime.tasks().upsert(base);

    let error = runtime.restore_projection(&root_run).unwrap_err();
    assert!(error.to_string().contains("operation"), "{error}");
    assert_eq!(runtime.tasks().get(&assigned.task_id), Some(assigned));
}

#[test]
fn legacy_task_cas_without_mutation_payload_restores_with_validated_digest() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let workflow_run = RunId::from("legacy-cas-root:workflow:one");
    let root_run = RunId::from("legacy-cas-root");
    let base = queued_workflow_task(&workflow_run, "legacy-cas", "work");
    let owner = AgentId::from("legacy-cas-owner");
    let mutation = json!({"assign": {"owner": owner}});
    let mut assigned = base.clone();
    assigned.revision = 1;
    assigned.owner_agent_id = Some(owner);
    assigned.state = TaskState::Assigned;
    ledger
        .append(
            &root_run,
            DurabilityClass::SyncCritical,
            "task_cas",
            json!({
                "task_id": assigned.task_id,
                "operation_id": "legacy-cas-assign",
                "transition": "assign",
                "mutation_digest": stable_digest_value(&mutation),
                "expected_revision": 0,
                "new_revision": 1,
                "task": assigned,
            }),
        )
        .unwrap();
    let runtime = CollaborationRuntime::with_ledger(
        Scheduler::<()>::new(ResourcePolicy::new(1)),
        Arc::clone(&ledger) as Arc<dyn RuntimeLedger>,
    );
    runtime.tasks().upsert(base);

    runtime.restore_projection(&root_run).unwrap();
    let restored = runtime
        .tasks()
        .get(&TaskId::from("workflow:legacy-cas-root:workflow:one:work"))
        .unwrap();
    assert_eq!(restored.revision, 1);
    assert_eq!(restored.state, TaskState::Assigned);
    assert_eq!(restored.owner_agent_id, Some(AgentId::from("legacy-cas-owner")));
}

#[test]
fn legacy_assignment_rejects_foreign_lineage_and_owner_overwrite() {
    let base_run = RunId::from("legacy-assign-root");
    let task_id = TaskId::from("legacy-assign-task");
    let mut base = queued_workflow_task(&RunId::from("legacy-assign-root:workflow:one"), "legacy-assign", "work");
    base.task_id = task_id.clone();
    base.workflow_id = None;
    base.node_id = None;
    base.task_key = Some("legacy-assign-task".to_owned());
    let foreign_ledger = Arc::new(InMemoryRuntimeLedger::default());
    foreign_ledger
        .append(
            &RunId::from("foreign-legacy-root"),
            DurabilityClass::SyncCritical,
            "task_assigned",
            json!({"task_id": task_id, "agent_id": "foreign-owner"}),
        )
        .unwrap();
    let foreign = CollaborationRuntime::with_ledger(
        Scheduler::<()>::new(ResourcePolicy::new(1)),
        Arc::clone(&foreign_ledger) as Arc<dyn RuntimeLedger>,
    );
    foreign.tasks().upsert(base.clone());
    assert!(foreign.restore_projection(&RunId::from("foreign-legacy-root")).is_err());
    assert_eq!(foreign.tasks().get(&base.task_id), Some(base.clone()));

    let overwrite_ledger = Arc::new(InMemoryRuntimeLedger::default());
    overwrite_ledger
        .append(
            &base_run,
            DurabilityClass::SyncCritical,
            "task_assigned",
            json!({"task_id": base.task_id, "agent_id": "replacement-owner"}),
        )
        .unwrap();
    let overwrite = CollaborationRuntime::with_ledger(
        Scheduler::<()>::new(ResourcePolicy::new(1)),
        Arc::clone(&overwrite_ledger) as Arc<dyn RuntimeLedger>,
    );
    let mut assigned = base;
    assigned.state = TaskState::Assigned;
    assigned.owner_agent_id = Some(AgentId::from("existing-owner"));
    overwrite.tasks().upsert(assigned.clone());
    assert!(overwrite.restore_projection(&base_run).is_err());
    assert_eq!(overwrite.tasks().get(&assigned.task_id), Some(assigned));
}
