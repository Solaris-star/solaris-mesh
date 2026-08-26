use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};

use solaris_types::effect::DurabilityClass;
use solaris_types::identity::{AgentId, OperationId, RunId, TaskId};
use solaris_types::runtime::{AgentLifecycleState, AgentRecord, TaskFailureClass, TaskRecord, TaskState};

use crate::collaboration_runtime::TaskSettlement;
use crate::resource_policy::ResourcePolicy;
use crate::runtime_ledger::{InMemoryRuntimeLedger, LedgerRecord, RuntimeLedger};

use super::{CollaborationRuntime, Scheduler};

#[derive(Default)]
struct FailAfterTaskCasAppendOnce {
    inner: InMemoryRuntimeLedger,
    failed: AtomicBool,
}

impl RuntimeLedger for FailAfterTaskCasAppendOnce {
    crate::runtime_ledger::forward_compare_and_append!();

    fn append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        payload: serde_json::Value,
    ) -> std::io::Result<LedgerRecord> {
        let record = self.inner.append(run_id, durability, record_type, payload)?;
        if record_type == "task_cas" && !self.failed.swap(true, Ordering::SeqCst) {
            return Err(std::io::Error::other("injected crash after task CAS append"));
        }
        Ok(record)
    }

    fn run_ids(&self) -> std::io::Result<Vec<RunId>> {
        self.inner.run_ids()
    }

    fn records_for_run(&self, run_id: &RunId) -> std::io::Result<Vec<LedgerRecord>> {
        self.inner.records_for_run(run_id)
    }
}

fn task(run_id: &RunId, task_id: &TaskId) -> TaskRecord {
    TaskRecord {
        run_id: run_id.clone(),
        task_id: task_id.clone(),
        revision: 0,
        task_key: Some("stable-task".to_owned()),
        team_id: None,
        workflow_id: None,
        node_id: None,
        role: Some("worker".to_owned()),
        depends_on: Vec::new(),
        content: Some(serde_json::json!({"request": "inspect"})),
        expected_write_scope: vec!["src/**".to_owned()],
        owner_agent_id: None,
        state: TaskState::Queued,
        outcome_ref: None,
        failure_class: None,
    }
}

fn add_agent(runtime: &CollaborationRuntime<()>, run_id: &RunId, agent_id: &AgentId) {
    runtime.agents().upsert(AgentRecord {
        run_id: run_id.clone(),
        agent_id: agent_id.clone(),
        team_id: None,
        parent_agent_id: None,
        state: AgentLifecycleState::Active,
    });
}

#[test]
fn run_task_admission_is_durable_and_idempotent_after_restore() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("task-admission-restore");
    let first: CollaborationRuntime<()> = CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(2)),
        Arc::clone(&ledger) as Arc<dyn RuntimeLedger>,
    );
    let task_a = task(&run_id, &TaskId::from("task-a"));
    let task_b = task(&run_id, &TaskId::from("task-b"));
    assert_eq!(
        first
            .register_runtime_tasks_admitted(&run_id, vec![task_a.clone(), task_b.clone()], 2)
            .unwrap(),
        vec![true, true]
    );
    assert_eq!(
        first
            .register_runtime_tasks_admitted(&run_id, vec![task_a.clone()], 2)
            .unwrap(),
        vec![false]
    );

    let restored: CollaborationRuntime<()> = CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(2)),
        Arc::clone(&ledger) as Arc<dyn RuntimeLedger>,
    );
    assert_eq!(
        restored
            .register_runtime_tasks_admitted(&run_id, vec![task_b], 2)
            .unwrap(),
        vec![false]
    );
    let error = restored
        .register_runtime_tasks_admitted(&run_id, vec![task(&run_id, &TaskId::from("task-c"))], 2)
        .unwrap_err();
    assert!(error.to_string().contains("at most 2"));
}

#[test]
fn concurrent_run_task_admission_never_exceeds_the_durable_limit() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let runtime: Arc<CollaborationRuntime<()>> = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(2)),
        Arc::clone(&ledger) as Arc<dyn RuntimeLedger>,
    ));
    let run_id = RunId::from("task-admission-race");
    let barrier = Arc::new(Barrier::new(3));
    let threads = ["task-a", "task-b"].map(|id| {
        let runtime = Arc::clone(&runtime);
        let run_id = run_id.clone();
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            barrier.wait();
            runtime.register_runtime_tasks_admitted(&run_id, vec![task(&run_id, &TaskId::from(id))], 1)
        })
    });
    barrier.wait();
    let successes = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .filter(Result::is_ok)
        .count();
    assert_eq!(successes, 1);
    assert_eq!(
        ledger
            .records_for_run(&run_id)
            .unwrap()
            .iter()
            .filter(|record| record.record_type == "task_created")
            .count(),
        1
    );
}

#[test]
fn durable_task_cas_recovers_after_append_before_projection_and_replay_is_single_revision() {
    let ledger = Arc::new(FailAfterTaskCasAppendOnce::default());
    let run_id = RunId::from("cas-crash-run");
    let task_id = TaskId::from("cas-crash-task");
    let owner = AgentId::from("owner");
    let operation_id = OperationId::from("crash-assign");
    let runtime: CollaborationRuntime<()> =
        CollaborationRuntime::with_ledger(Scheduler::new(ResourcePolicy::new(2)), ledger.clone());
    add_agent(&runtime, &run_id, &owner);
    runtime.register_runtime_task(&run_id, task(&run_id, &task_id)).unwrap();

    let error = runtime
        .assign_task_owner(&run_id, &task_id, &owner, 0, &operation_id)
        .expect_err("injected crash must leave the in-memory task unchanged");
    assert!(error.to_string().contains("injected crash"));
    let unchanged = runtime.tasks().get(&task_id).unwrap();
    assert_eq!(unchanged.revision, 0);
    assert_eq!(unchanged.state, TaskState::Queued);

    assert_eq!(
        runtime
            .assign_task_owner(&run_id, &task_id, &owner, 0, &operation_id)
            .unwrap(),
        1
    );
    assert_eq!(runtime.tasks().get(&task_id).unwrap().revision, 1);
    assert_eq!(
        ledger
            .records_for_run(&run_id)
            .unwrap()
            .iter()
            .filter(|record| record.record_type == "task_cas")
            .count(),
        1
    );

    let restored: CollaborationRuntime<()> =
        CollaborationRuntime::with_ledger(Scheduler::new(ResourcePolicy::new(2)), ledger);
    add_agent(&restored, &run_id, &owner);
    restored.restore_projection(&run_id).unwrap();
    let recovered = restored.tasks().get(&task_id).unwrap();
    assert_eq!(recovered.revision, 1);
    assert_eq!(recovered.owner_agent_id, Some(owner));
    assert_eq!(recovered.state, TaskState::Assigned);
}

#[test]
fn concurrent_stale_writers_publish_exactly_one_owner_and_one_record() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let runtime = Arc::new(CollaborationRuntime::with_ledger(
        Scheduler::new(ResourcePolicy::new(2)),
        ledger.clone(),
    ));
    let run_id = RunId::from("cas-race-run");
    let task_id = TaskId::from("cas-race-task");
    let first = AgentId::from("first");
    let second = AgentId::from("second");
    add_agent(&runtime, &run_id, &first);
    add_agent(&runtime, &run_id, &second);
    runtime.register_runtime_task(&run_id, task(&run_id, &task_id)).unwrap();
    let barrier = Arc::new(Barrier::new(3));

    let writers: Vec<_> = [first.clone(), second.clone()]
        .into_iter()
        .map(|owner| {
            let runtime = Arc::clone(&runtime);
            let run_id = run_id.clone();
            let task_id = task_id.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                let operation_id = OperationId::new(format!("assign-{owner}"));
                runtime.assign_task_owner(&run_id, &task_id, &owner, 0, &operation_id)
            })
        })
        .collect();
    barrier.wait();
    let results: Vec<_> = writers.into_iter().map(|writer| writer.join().unwrap()).collect();

    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
    let stored = runtime.tasks().get(&task_id).unwrap();
    assert_eq!(stored.revision, 1);
    assert!(stored.owner_agent_id == Some(first) || stored.owner_agent_id == Some(second));
    assert_eq!(
        ledger
            .records_for_run(&run_id)
            .unwrap()
            .iter()
            .filter(|record| record.record_type == "task_cas")
            .count(),
        1
    );
}

#[test]
fn running_and_unknown_outcome_settlement_restore_all_cas_metadata() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("cas-outcome-run");
    let task_id = TaskId::from("cas-outcome-task");
    let owner = AgentId::from("owner");
    let runtime: CollaborationRuntime<()> =
        CollaborationRuntime::with_ledger(Scheduler::new(ResourcePolicy::new(2)), ledger.clone());
    add_agent(&runtime, &run_id, &owner);
    runtime.register_runtime_task(&run_id, task(&run_id, &task_id)).unwrap();
    runtime
        .assign_task_owner(&run_id, &task_id, &owner, 0, &OperationId::from("outcome-assign"))
        .unwrap();
    runtime
        .mark_task_running(&run_id, &task_id, &owner, 1, &OperationId::from("outcome-running"))
        .unwrap();
    let settle_operation = OperationId::from("outcome-settle");
    let settle = || {
        runtime.settle_collaboration_task(
            &run_id,
            &task_id,
            &owner,
            2,
            &settle_operation,
            TaskSettlement {
                state: TaskState::Failed,
                outcome_ref: Some("mesh://outcome/unknown".to_owned()),
                failure_class: Some(TaskFailureClass::OutcomeUnknown),
            },
        )
    };

    assert_eq!(settle().unwrap(), 3);
    assert_eq!(settle().unwrap(), 3);
    assert!(
        runtime
            .settle_collaboration_task(
                &run_id,
                &task_id,
                &owner,
                2,
                &OperationId::from("different-settle"),
                TaskSettlement {
                    state: TaskState::Failed,
                    outcome_ref: Some("mesh://outcome/unknown".to_owned()),
                    failure_class: Some(TaskFailureClass::OutcomeUnknown),
                },
            )
            .unwrap_err()
            .to_string()
            .contains("stale task revision")
    );
    assert_eq!(
        ledger
            .records_for_run(&run_id)
            .unwrap()
            .iter()
            .filter(|record| record.record_type == "task_cas")
            .count(),
        3
    );

    let restored: CollaborationRuntime<()> =
        CollaborationRuntime::with_ledger(Scheduler::new(ResourcePolicy::new(2)), ledger);
    add_agent(&restored, &run_id, &owner);
    restored.restore_projection(&run_id).unwrap();
    let task = restored.tasks().get(&task_id).unwrap();
    assert_eq!(task.revision, 3);
    assert_eq!(task.state, TaskState::Failed);
    assert_eq!(task.owner_agent_id, Some(owner));
    assert_eq!(task.outcome_ref.as_deref(), Some("mesh://outcome/unknown"));
    assert_eq!(task.failure_class, Some(TaskFailureClass::OutcomeUnknown));
}

#[test]
fn durable_historical_operation_replay_precedes_current_revision_before_and_after_restart() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("cas-history-run");
    let task_id = TaskId::from("cas-history-task");
    let owner = AgentId::from("owner");
    let runtime: CollaborationRuntime<()> =
        CollaborationRuntime::with_ledger(Scheduler::new(ResourcePolicy::new(2)), ledger.clone());
    add_agent(&runtime, &run_id, &owner);
    runtime.register_runtime_task(&run_id, task(&run_id, &task_id)).unwrap();
    let assign = OperationId::from("history-assign");
    runtime
        .assign_task_owner(&run_id, &task_id, &owner, 0, &assign)
        .unwrap();
    runtime
        .mark_task_running(&run_id, &task_id, &owner, 1, &OperationId::from("history-running"))
        .unwrap();
    assert_eq!(
        runtime
            .assign_task_owner(&run_id, &task_id, &owner, 0, &assign)
            .unwrap(),
        1
    );
    assert_eq!(
        ledger
            .records_for_run(&run_id)
            .unwrap()
            .iter()
            .filter(|record| record.record_type == "task_cas")
            .count(),
        2
    );

    let restored: CollaborationRuntime<()> =
        CollaborationRuntime::with_ledger(Scheduler::new(ResourcePolicy::new(2)), ledger.clone());
    add_agent(&restored, &run_id, &owner);
    restored.restore_projection(&run_id).unwrap();
    assert_eq!(
        restored
            .assign_task_owner(&run_id, &task_id, &owner, 0, &assign)
            .unwrap(),
        1
    );
    assert_eq!(restored.tasks().get(&task_id).unwrap().revision, 2);
    assert_eq!(
        ledger
            .records_for_run(&run_id)
            .unwrap()
            .iter()
            .filter(|record| record.record_type == "task_cas")
            .count(),
        2
    );
}
