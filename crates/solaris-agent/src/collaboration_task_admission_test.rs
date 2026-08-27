use super::tasks::TaskAdmissionError;
use super::{CollaborationRuntime, TaskSettlement};
use crate::resource_policy::ResourcePolicy;
use crate::runtime_ledger::{InMemoryRuntimeLedger, LedgerRecord, RuntimeLedger, SqliteRuntimeLedger};
use crate::scheduler::Scheduler;
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::{AgentId, OperationId, RunId, TaskId};
use solaris_types::runtime::{AgentLifecycleState, AgentRecord, TaskRecord, TaskState};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

fn collaboration_task(run_id: &RunId, id: &str) -> TaskRecord {
    TaskRecord {
        run_id: run_id.clone(),
        task_id: TaskId::new(format!("collaboration:{run_id}:{id}")),
        revision: 0,
        task_key: Some(format!("collaboration:{id}")),
        team_id: None,
        workflow_id: None,
        node_id: None,
        role: Some(id.to_owned()),
        depends_on: Vec::new(),
        content: None,
        expected_write_scope: Vec::new(),
        owner_agent_id: None,
        state: TaskState::Queued,
        outcome_ref: None,
        failure_class: None,
    }
}

fn runtime_with_ledger(ledger: Arc<dyn RuntimeLedger>) -> CollaborationRuntime<()> {
    CollaborationRuntime::with_ledger(Scheduler::new(ResourcePolicy::new(4)), ledger)
}

fn durable_task_created_count(ledger: &dyn RuntimeLedger, run_id: &RunId) -> usize {
    ledger
        .records_for_run(run_id)
        .unwrap()
        .iter()
        .filter(|record| record.record_type == "task_created")
        .count()
}

#[test]
fn admission_accepts_a_batch_within_the_limit() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let runtime = runtime_with_ledger(ledger.clone());
    let run = RunId::from("admission-basic");

    runtime
        .admit_collaboration_tasks(
            &run,
            3,
            vec![collaboration_task(&run, "a"), collaboration_task(&run, "b")],
        )
        .unwrap();

    assert_eq!(durable_task_created_count(ledger.as_ref(), &run), 2);
    assert!(runtime.tasks().get(&collaboration_task(&run, "a").task_id).is_some());
    assert!(runtime.tasks().get(&collaboration_task(&run, "b").task_id).is_some());
}

#[test]
fn admission_rejects_a_batch_that_exceeds_the_limit() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let runtime = runtime_with_ledger(ledger.clone());
    let run = RunId::from("admission-reject");

    let error = runtime
        .admit_collaboration_tasks(
            &run,
            2,
            vec![
                collaboration_task(&run, "a"),
                collaboration_task(&run, "b"),
                collaboration_task(&run, "c"),
            ],
        )
        .expect_err("batch over the limit must be rejected");

    assert!(matches!(error, TaskAdmissionError::OverLimit { limit: 2 }));
    assert_eq!(durable_task_created_count(ledger.as_ref(), &run), 0);
    assert!(runtime.tasks().snapshot().is_empty());
}

#[test]
fn sequential_spawns_accumulate_against_the_run_quota() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let runtime = runtime_with_ledger(ledger.clone());
    let run = RunId::from("admission-sequential");

    runtime
        .admit_collaboration_tasks(&run, 3, vec![collaboration_task(&run, "first")])
        .unwrap();
    runtime
        .admit_collaboration_tasks(&run, 3, vec![collaboration_task(&run, "second")])
        .unwrap();
    runtime
        .admit_collaboration_tasks(&run, 3, vec![collaboration_task(&run, "third")])
        .unwrap();

    let error = runtime
        .admit_collaboration_tasks(&run, 3, vec![collaboration_task(&run, "fourth")])
        .expect_err("the fourth task must exceed the quota of three");
    assert!(matches!(error, TaskAdmissionError::OverLimit { limit: 3 }));
    assert_eq!(durable_task_created_count(ledger.as_ref(), &run), 3);
}

#[test]
fn idempotent_replay_does_not_double_count_the_quota() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let runtime = runtime_with_ledger(ledger.clone());
    let run = RunId::from("admission-replay");
    let batch = vec![collaboration_task(&run, "a"), collaboration_task(&run, "b")];

    runtime.admit_collaboration_tasks(&run, 2, batch.clone()).unwrap();
    // Replaying the identical batch must succeed and not consume extra quota.
    runtime.admit_collaboration_tasks(&run, 2, batch.clone()).unwrap();

    assert_eq!(durable_task_created_count(ledger.as_ref(), &run), 2);
    // A genuinely new task is still blocked because the quota is full.
    let error = runtime
        .admit_collaboration_tasks(&run, 2, vec![collaboration_task(&run, "c")])
        .expect_err("quota already consumed by the replayed batch");
    assert!(matches!(error, TaskAdmissionError::OverLimit { limit: 2 }));
}

#[test]
fn replay_after_partial_admission_heals_without_double_counting() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run = RunId::from("admission-partial");
    let batch = vec![
        collaboration_task(&run, "a"),
        collaboration_task(&run, "b"),
        collaboration_task(&run, "c"),
    ];

    // Simulate a crash that persisted only the first two tasks of the batch.
    {
        let runtime = runtime_with_ledger(ledger.clone());
        for task in &batch[..2] {
            runtime.register_runtime_task(&run, task.clone()).unwrap();
        }
    }
    assert_eq!(durable_task_created_count(ledger.as_ref(), &run), 2);

    // A fresh runtime replays the whole batch: the two durable tasks are
    // recognized, and only the missing third consumes quota.
    let runtime = runtime_with_ledger(ledger.clone());
    runtime.restore_projection(&run).unwrap();
    runtime.admit_collaboration_tasks(&run, 3, batch.clone()).unwrap();
    assert_eq!(durable_task_created_count(ledger.as_ref(), &run), 3);

    // Replaying again is a no-op that does not grow the ledger.
    runtime.admit_collaboration_tasks(&run, 3, batch).unwrap();
    assert_eq!(durable_task_created_count(ledger.as_ref(), &run), 3);
}

#[test]
fn cold_restore_rebuilds_the_quota_before_new_admission() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run = RunId::from("admission-restore");

    {
        let runtime = runtime_with_ledger(ledger.clone());
        runtime
            .admit_collaboration_tasks(
                &run,
                2,
                vec![collaboration_task(&run, "a"), collaboration_task(&run, "b")],
            )
            .unwrap();
    }

    // A brand-new runtime sharing the ledger must see the durable tasks after
    // restore and refuse to exceed the quota.
    let restored = runtime_with_ledger(ledger.clone());
    restored.restore_projection(&run).unwrap();
    assert_eq!(restored.tasks().snapshot().len(), 2);

    let error = restored
        .admit_collaboration_tasks(&run, 2, vec![collaboration_task(&run, "c")])
        .expect_err("restored quota must already be full");
    assert!(matches!(error, TaskAdmissionError::OverLimit { limit: 2 }));

    // Replaying the original batch still succeeds idempotently.
    restored
        .admit_collaboration_tasks(
            &run,
            2,
            vec![collaboration_task(&run, "a"), collaboration_task(&run, "b")],
        )
        .unwrap();
    assert_eq!(durable_task_created_count(ledger.as_ref(), &run), 2);
}

#[test]
fn concurrent_admissions_never_exceed_the_limit() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let runtime = Arc::new(runtime_with_ledger(ledger.clone()));
    let run = RunId::from("admission-concurrent");
    let limit = 5usize;
    let contenders = 16usize;

    let accepted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    std::thread::scope(|scope| {
        for index in 0..contenders {
            let runtime = Arc::clone(&runtime);
            let run = run.clone();
            let accepted = Arc::clone(&accepted);
            scope.spawn(move || {
                let task = collaboration_task(&run, &format!("task-{index}"));
                if runtime.admit_collaboration_tasks(&run, limit, vec![task]).is_ok() {
                    accepted.fetch_add(1, Ordering::SeqCst);
                }
            });
        }
    });

    assert_eq!(accepted.load(Ordering::SeqCst), limit);
    assert_eq!(durable_task_created_count(ledger.as_ref(), &run), limit);
    assert_eq!(runtime.tasks().snapshot().len(), limit);
}

#[test]
fn boundary_limits_one_thirty_two_and_two_fifty_six() {
    for limit in [1usize, 32, 256] {
        let ledger = Arc::new(InMemoryRuntimeLedger::default());
        let runtime = runtime_with_ledger(ledger.clone());
        let run = RunId::from(format!("admission-limit-{limit}"));

        let batch: Vec<TaskRecord> = (0..limit).map(|i| collaboration_task(&run, &format!("t{i}"))).collect();
        runtime.admit_collaboration_tasks(&run, limit, batch).unwrap();
        assert_eq!(durable_task_created_count(ledger.as_ref(), &run), limit);

        let error = runtime
            .admit_collaboration_tasks(&run, limit, vec![collaboration_task(&run, "overflow")])
            .expect_err("one task past the limit must be rejected");
        assert!(matches!(error, TaskAdmissionError::OverLimit { limit: l } if l == limit));
    }
}

#[test]
fn reviewer_task_counts_toward_the_quota() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let runtime = runtime_with_ledger(ledger.clone());
    let run = RunId::from("admission-reviewer");

    // One primary plus the automatic independent reviewer fills a limit of two.
    runtime
        .admit_collaboration_tasks(
            &run,
            2,
            vec![
                collaboration_task(&run, "primary"),
                collaboration_task(&run, "__independent_reviewer"),
            ],
        )
        .unwrap();
    let error = runtime
        .admit_collaboration_tasks(&run, 2, vec![collaboration_task(&run, "extra")])
        .expect_err("the reviewer already consumed the second slot");
    assert!(matches!(error, TaskAdmissionError::OverLimit { limit: 2 }));

    // With a limit of one, a primary plus reviewer batch is rejected outright.
    let run2 = RunId::from("admission-reviewer-tight");
    let error = runtime
        .admit_collaboration_tasks(
            &run2,
            1,
            vec![
                collaboration_task(&run2, "primary"),
                collaboration_task(&run2, "__independent_reviewer"),
            ],
        )
        .expect_err("primary plus reviewer exceeds a limit of one");
    assert!(matches!(error, TaskAdmissionError::OverLimit { limit: 1 }));
    assert_eq!(durable_task_created_count(ledger.as_ref(), &run2), 0);
}

#[test]
fn workflow_tasks_consume_the_run_collaboration_quota() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let runtime = runtime_with_ledger(ledger.clone());
    let run = RunId::from("admission-scoped");
    let root = AgentId::from("admission-scoped-root");
    runtime.agents().upsert(AgentRecord {
        run_id: run.clone(),
        agent_id: root.clone(),
        team_id: None,
        parent_agent_id: None,
        state: AgentLifecycleState::Active,
    });

    // Workflow tasks share the same historical Run quota.
    let workflow_task = TaskRecord {
        run_id: run.clone(),
        task_id: TaskId::new(format!("workflow:{run}:node")),
        revision: 0,
        task_key: Some("workflow:wf:node".to_owned()),
        team_id: None,
        workflow_id: Some("wf".to_owned()),
        node_id: Some("node".to_owned()),
        role: Some("worker".to_owned()),
        depends_on: Vec::new(),
        content: None,
        expected_write_scope: Vec::new(),
        owner_agent_id: Some(root.clone()),
        state: TaskState::Assigned,
        outcome_ref: None,
        failure_class: None,
    };
    runtime.register_runtime_task(&run, workflow_task).unwrap();

    let error = runtime
        .admit_collaboration_tasks(&run, 1, vec![collaboration_task(&run, "only")])
        .expect_err("a Workflow task already occupies the Run quota");
    assert!(matches!(error, TaskAdmissionError::OverLimit { limit: 1 }));
    assert_eq!(durable_task_created_count(ledger.as_ref(), &run), 1);
}

#[test]
fn duplicate_task_in_a_batch_is_rejected() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let runtime = runtime_with_ledger(ledger.clone());
    let run = RunId::from("admission-duplicate");

    let error = runtime
        .admit_collaboration_tasks(
            &run,
            4,
            vec![collaboration_task(&run, "same"), collaboration_task(&run, "same")],
        )
        .expect_err("a batch cannot contain the same task twice");
    assert!(matches!(error, TaskAdmissionError::Runtime(_)));
    assert_eq!(durable_task_created_count(ledger.as_ref(), &run), 0);
}

#[test]
fn conflicting_metadata_for_a_durable_task_is_rejected() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let runtime = runtime_with_ledger(ledger.clone());
    let run = RunId::from("admission-conflict");

    runtime
        .admit_collaboration_tasks(&run, 2, vec![collaboration_task(&run, "a")])
        .unwrap();

    let mut conflicting = collaboration_task(&run, "a");
    conflicting.role = Some("different-role".to_owned());
    let error = runtime
        .admit_collaboration_tasks(&run, 2, vec![conflicting])
        .expect_err("replaying a task with different metadata must fail");
    assert!(matches!(error, TaskAdmissionError::Runtime(_)));
    assert_eq!(durable_task_created_count(ledger.as_ref(), &run), 1);
}

#[derive(Default)]
struct FailAfterLedger {
    inner: InMemoryRuntimeLedger,
    fail: AtomicBool,
}

impl RuntimeLedger for FailAfterLedger {
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
fn unsupported_ledger_admission_fails_closed() {
    let ledger = Arc::new(FailAfterLedger::default());
    let runtime = runtime_with_ledger(ledger.clone());
    let run = RunId::from("admission-crash");

    // Fail on the second append so only the first task is persisted.
    ledger.fail.store(true, Ordering::SeqCst);
    let result = runtime.admit_collaboration_tasks(
        &run,
        3,
        vec![collaboration_task(&run, "a"), collaboration_task(&run, "b")],
    );
    assert!(result.is_err());
    // The quota check passed but persistence failed; nothing durable leaked
    // beyond what the ledger actually accepted (here, zero because the first
    // append already saw the failure flag).
    assert_eq!(durable_task_created_count(ledger.as_ref(), &run), 0);

    ledger.fail.store(false, Ordering::SeqCst);
    let unsupported = runtime
        .admit_collaboration_tasks(
            &run,
            3,
            vec![collaboration_task(&run, "a"), collaboration_task(&run, "b")],
        )
        .expect_err("a ledger without atomic admission must fail closed");
    assert!(matches!(unsupported, TaskAdmissionError::Runtime(_)));
    assert_eq!(durable_task_created_count(ledger.as_ref(), &run), 0);
}

#[test]
fn admission_after_settlement_still_counts_the_task() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let runtime = runtime_with_ledger(ledger.clone());
    let run = RunId::from("admission-settled");
    let root = AgentId::from("admission-settled-root");
    runtime.agents().upsert(AgentRecord {
        run_id: run.clone(),
        agent_id: root.clone(),
        team_id: None,
        parent_agent_id: None,
        state: AgentLifecycleState::Active,
    });

    let task = collaboration_task(&run, "done");
    let task_id = task.task_id.clone();
    runtime.admit_collaboration_tasks(&run, 1, vec![task]).unwrap();

    // Drive the task to a terminal state through the public CAS path.
    runtime
        .assign_task_owner(&run, &task_id, &root, 0, &OperationId::from("assign"))
        .unwrap();
    runtime
        .settle_collaboration_task(
            &run,
            &task_id,
            &root,
            1,
            &OperationId::from("settle"),
            TaskSettlement {
                state: TaskState::Completed,
                outcome_ref: None,
                failure_class: None,
            },
        )
        .unwrap();
    assert_eq!(runtime.tasks().get(&task_id).unwrap().state, TaskState::Completed);

    // A completed task still occupies its durable quota slot.
    let error = runtime
        .admit_collaboration_tasks(&run, 1, vec![collaboration_task(&run, "other")])
        .expect_err("a settled task still occupies its quota slot");
    assert!(matches!(error, TaskAdmissionError::OverLimit { limit: 1 }));
}

#[test]
fn two_sqlite_runtimes_cannot_race_past_the_run_limit() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("runtime.sqlite3");
    let first_ledger = Arc::new(SqliteRuntimeLedger::open(&database).unwrap());
    let second_ledger = Arc::new(SqliteRuntimeLedger::open(&database).unwrap());
    let first = runtime_with_ledger(first_ledger.clone());
    let second = runtime_with_ledger(second_ledger.clone());
    let run = RunId::from("sqlite-cross-runtime-admission");
    let barrier = Arc::new(std::sync::Barrier::new(3));

    let workers = [(first, "a"), (second, "b")].map(|(runtime, id)| {
        let run = run.clone();
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            barrier.wait();
            runtime.admit_collaboration_tasks(&run, 1, vec![collaboration_task(&run, id)])
        })
    });
    barrier.wait();
    let results = workers.map(|worker| worker.join().unwrap());

    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
    assert_eq!(durable_task_created_count(first_ledger.as_ref(), &run), 1);
    assert_eq!(durable_task_created_count(second_ledger.as_ref(), &run), 1);
}
