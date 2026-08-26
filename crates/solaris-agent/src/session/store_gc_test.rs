use std::env;
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use chrono::Utc;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::RunId;
use solaris_types::message::TokenUsage;

use super::*;
use crate::execution_context::{EffectOutputStore, stable_digest_bytes};
use crate::runtime_ledger::{LedgerRecord, RuntimeLedger, SqliteRuntimeLedger};
use crate::session::{Session, store::SessionStore};

const GC_FIXTURE_DIRECTORY: &str = "SOLARIS_SESSION_GC_FIXTURE_DIRECTORY";
const GC_FIXTURE_READY: &str = "SOLARIS_SESSION_GC_FIXTURE_READY";
const GC_FIXTURE_START: &str = "SOLARIS_SESSION_GC_FIXTURE_START";
const GC_FIXTURE_RESULT: &str = "SOLARIS_SESSION_GC_FIXTURE_RESULT";
const GC_CHILD_TEST: &str = "session::store::store_gc::store_gc_test::multiprocess_session_gc_child";
const GC_PROCESS_TIMEOUT: Duration = Duration::from_secs(45);

struct BlockingDeleteLedger {
    inner: SqliteRuntimeLedger,
    delete_started: mpsc::Sender<()>,
    resume_delete: Mutex<mpsc::Receiver<()>>,
    blocked_once: AtomicBool,
}

impl RuntimeLedger for BlockingDeleteLedger {
    crate::runtime_ledger::forward_compare_and_append!();

    fn append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        payload: Value,
    ) -> io::Result<LedgerRecord> {
        self.inner.append(run_id, durability, record_type, payload)
    }

    fn run_ids(&self) -> io::Result<Vec<RunId>> {
        self.inner.run_ids()
    }

    fn records_for_run(&self, run_id: &RunId) -> io::Result<Vec<LedgerRecord>> {
        self.inner.records_for_run(run_id)
    }

    fn delete_run_exact(&self, run_id: &RunId) -> io::Result<usize> {
        if !self.blocked_once.swap(true, Ordering::SeqCst) {
            self.delete_started
                .send(())
                .map_err(|_| io::Error::other("GC delete-start observer was dropped"))?;
            self.resume_delete
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .recv()
                .map_err(|_| io::Error::other("GC delete resume signal was dropped"))?;
        }
        self.inner.delete_run_exact(run_id)
    }

    fn effect_output_root(&self) -> Option<PathBuf> {
        self.inner.effect_output_root()
    }

    fn protected_state_paths(&self) -> Vec<PathBuf> {
        self.inner.protected_state_paths()
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct GcProcessResult {
    selected_jobs: usize,
    passes: usize,
    examined: usize,
    completed: usize,
    deferred: usize,
    failed: usize,
    ledger_records_deleted: usize,
    blob_runs_processed: usize,
    remaining_jobs: usize,
}

impl GcProcessResult {
    fn add_report(&mut self, report: SessionGcReport) {
        self.passes = self.passes.saturating_add(1);
        self.examined = self.examined.saturating_add(report.examined);
        self.completed = self.completed.saturating_add(report.completed);
        self.deferred = self.deferred.saturating_add(report.deferred);
        self.failed = self.failed.saturating_add(report.failed);
        self.ledger_records_deleted = self
            .ledger_records_deleted
            .saturating_add(report.ledger_records_deleted);
        self.blob_runs_processed = self.blob_runs_processed.saturating_add(report.blob_runs_processed);
    }
}

fn session(id: &str, run_id: &str) -> Session {
    let now = Utc::now();
    Session {
        id: id.to_owned(),
        run_id: Some(run_id.to_owned()),
        created_at: now,
        updated_at: now,
        provider: "provider".to_owned(),
        model: "model".to_owned(),
        cwd: "workspace".to_owned(),
        total_usage: TokenUsage::default(),
        messages: Vec::new(),
        runtime_state: None,
    }
}

fn create_released(store: &SessionStore, session_id: &str, run_id: &str) {
    let lease = store
        .create_active(&session(session_id, run_id), &format!("owner-{session_id}"))
        .unwrap();
    store.release(&lease).unwrap();
}

fn set_session_order(store: &SessionStore, session_id: &str, order: i64) {
    Connection::open(store.database_path())
        .unwrap()
        .execute(
            "UPDATE sessions SET created_at_ms = ?2, updated_at_ms = ?2 WHERE session_id = ?1",
            params![session_id, order],
        )
        .unwrap();
}

fn mark_jobs_due(store: &SessionStore) {
    Connection::open(store.database_path())
        .unwrap()
        .execute("UPDATE session_gc_jobs SET eligible_at_ms = 0", [])
        .unwrap();
}

fn phase(store: &SessionStore, session_id: &str) -> String {
    Connection::open(store.database_path())
        .unwrap()
        .query_row(
            "SELECT phase FROM session_gc_jobs WHERE session_id = ?1",
            params![session_id],
            |row| row.get(0),
        )
        .unwrap()
}

fn last_error(store: &SessionStore, session_id: &str) -> Option<String> {
    Connection::open(store.database_path())
        .unwrap()
        .query_row(
            "SELECT last_error FROM session_gc_jobs WHERE session_id = ?1",
            params![session_id],
            |row| row.get(0),
        )
        .unwrap()
}

fn sqlite_ledger(root: &std::path::Path) -> SqliteRuntimeLedger {
    let runtime = root.join("runtime");
    fs::create_dir(&runtime).unwrap();
    SqliteRuntimeLedger::open(runtime.join("ledger.sqlite3")).unwrap()
}

fn append(ledger: &dyn RuntimeLedger, run_id: &str) {
    ledger
        .append(
            &RunId::from(run_id),
            DurabilityClass::SyncCritical,
            "gc-test",
            json!({"run_id": run_id}),
        )
        .unwrap();
}

fn write_blob(ledger: &dyn RuntimeLedger, run_id: &str, content: &str) -> std::path::PathBuf {
    EffectOutputStore::for_run_with_ledger(&RunId::from(run_id), ledger)
        .write_named("gc-test", content)
        .unwrap();
    ledger
        .effect_output_root()
        .unwrap()
        .join(stable_digest_bytes(run_id.as_bytes()))
}

fn plan_oldest(store: &SessionStore, visible_limit: usize) {
    store.enforce_retention(visible_limit).unwrap();
    mark_jobs_due(store);
}

#[test]
fn gc_saves_exact_tree_then_deletes_ledger_blobs_and_session_metadata() {
    let directory = tempfile::tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    create_released(&store, "old", "run-old");
    create_released(&store, "new", "run-new");
    set_session_order(&store, "old", 1);
    set_session_order(&store, "new", 2);
    plan_oldest(&store, 1);

    let ledger = sqlite_ledger(directory.path());
    append(&ledger, "run-old");
    append(&ledger, "run-old:workflow:child");
    append(&ledger, "run-oldish");
    let root_blob = write_blob(&ledger, "run-old", "root-output");
    let child_blob = write_blob(&ledger, "run-old:workflow:child", "child-output");
    let similar_blob = write_blob(&ledger, "run-oldish", "similar-output");

    let report = store.run_due_gc_at(&ledger, 10, Utc::now()).unwrap();

    assert_eq!(report.completed, 1);
    assert_eq!(report.failed, 0);
    assert_eq!(report.ledger_records_deleted, 2);
    assert_eq!(report.blob_runs_processed, 2);
    assert!(ledger.records_for_run(&RunId::from("run-old")).unwrap().is_empty());
    assert!(
        ledger
            .records_for_run(&RunId::from("run-old:workflow:child"))
            .unwrap()
            .is_empty()
    );
    assert_eq!(ledger.records_for_run(&RunId::from("run-oldish")).unwrap().len(), 1);
    assert!(!root_blob.exists());
    assert!(!child_blob.exists());
    assert!(similar_blob.is_dir());
    assert!(store.load("old").unwrap().is_none());
    assert!(store.load("new").unwrap().is_some());
    assert_eq!(phase(&store, "old"), "complete");

    let connection = Connection::open(store.database_path()).unwrap();
    let tombstone: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM session_tombstones WHERE session_id = 'old'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let claims: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM session_gc_run_claims WHERE session_id = 'old'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(tombstone, 1);
    assert_eq!(claims, 0);
}

#[test]
fn slow_ledger_deletion_does_not_block_another_session_heartbeat_or_save() {
    let directory = tempfile::tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    create_released(&store, "old", "slow-delete-run");
    let mut live_session = session("live", "live-run");
    let live_lease = store.create_active(&live_session, "live-owner").unwrap();
    set_session_order(&store, "old", 1);
    set_session_order(&store, "live", 2);
    plan_oldest(&store, 1);

    let inner = sqlite_ledger(directory.path());
    append(&inner, "slow-delete-run");
    let (delete_started, delete_started_rx) = mpsc::channel();
    let (resume_delete, resume_delete_rx) = mpsc::channel();
    let ledger = Arc::new(BlockingDeleteLedger {
        inner,
        delete_started,
        resume_delete: Mutex::new(resume_delete_rx),
        blocked_once: AtomicBool::new(false),
    });
    let gc_store = store.clone();
    let gc_ledger = Arc::clone(&ledger);
    let gc_thread = thread::spawn(move || gc_store.run_due_gc_at(gc_ledger.as_ref(), 10, Utc::now()));
    delete_started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("GC never reached the blocking Ledger deletion");

    let operation_store = store.clone();
    let (operation_finished, operation_finished_rx) = mpsc::channel();
    let operation_thread = thread::spawn(move || {
        let mut live_lease = live_lease;
        let result = operation_store
            .heartbeat(&live_lease)
            .and_then(|()| {
                live_session.model = "saved-during-slow-gc".to_owned();
                live_session.updated_at = Utc::now();
                operation_store.save(&mut live_lease, &live_session)
            })
            .map_err(|error| error.to_string());
        operation_finished.send(result).unwrap();
        live_lease
    });

    let finished_while_delete_blocked = operation_finished_rx.recv_timeout(Duration::from_secs(2));
    resume_delete.send(()).unwrap();
    let report = gc_thread.join().unwrap().unwrap();
    let live_lease = operation_thread.join().unwrap();

    assert!(
        finished_while_delete_blocked.is_ok(),
        "Session DB write stayed blocked while the external Ledger deletion was paused"
    );
    finished_while_delete_blocked.unwrap().unwrap();
    assert_eq!(report.completed, 1);
    assert_eq!(
        store.load("live").unwrap().unwrap().session.model,
        "saved-during-slow-gc"
    );
    store.release(&live_lease).unwrap();
}

#[test]
fn crash_after_ledger_deletion_restarts_from_committed_exact_tree_without_retrying_effects() {
    let directory = tempfile::tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    create_released(&store, "old", "crash-ledger");
    create_released(&store, "new", "new-run");
    set_session_order(&store, "old", 1);
    set_session_order(&store, "new", 2);
    plan_oldest(&store, 1);
    let ledger = sqlite_ledger(directory.path());
    append(&ledger, "crash-ledger");
    let blob = write_blob(&ledger, "crash-ledger", "retained-until-retry");
    let injected = AtomicBool::new(false);

    let first = store
        .run_due_gc_at_with_observer(&ledger, 10, Utc::now(), &mut |step, _| {
            if step == GcStep::LedgerDeleted && !injected.swap(true, Ordering::SeqCst) {
                return Err(io::Error::other("injected crash after Ledger deletion"));
            }
            Ok(())
        })
        .unwrap();

    assert_eq!(first.failed, 1);
    assert_eq!(phase(&store, "old"), "planned");
    assert!(last_error(&store, "old").unwrap().contains("injected crash"));
    assert!(ledger.records_for_run(&RunId::from("crash-ledger")).unwrap().is_empty());
    assert!(blob.is_dir());
    assert!(
        store.load("old").unwrap().is_none(),
        "tombstoned sessions stay invisible"
    );

    let second = store.run_due_gc_at(&ledger, 10, Utc::now()).unwrap();
    assert_eq!(second.completed, 1);
    assert_eq!(second.failed, 0);
    assert_eq!(second.ledger_records_deleted, 0);
    assert!(!blob.exists());
    assert_eq!(phase(&store, "old"), "complete");
    assert!(last_error(&store, "old").is_none());
}

#[test]
fn crash_after_blob_deletion_restarts_idempotently_and_finishes_metadata() {
    let directory = tempfile::tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    create_released(&store, "old", "crash-blob");
    create_released(&store, "new", "new-run");
    set_session_order(&store, "old", 1);
    set_session_order(&store, "new", 2);
    plan_oldest(&store, 1);
    let ledger = sqlite_ledger(directory.path());
    append(&ledger, "crash-blob");
    let blob = write_blob(&ledger, "crash-blob", "deleted-before-retry");
    let injected = AtomicBool::new(false);

    let first = store
        .run_due_gc_at_with_observer(&ledger, 10, Utc::now(), &mut |step, _| {
            if step == GcStep::BlobsDeleted && !injected.swap(true, Ordering::SeqCst) {
                return Err(io::Error::other("injected crash after blob deletion"));
            }
            Ok(())
        })
        .unwrap();

    assert_eq!(first.failed, 1);
    assert_eq!(phase(&store, "old"), "ledger_deleted");
    assert!(ledger.records_for_run(&RunId::from("crash-blob")).unwrap().is_empty());
    assert!(!blob.exists());

    let second = store.run_due_gc_at(&ledger, 10, Utc::now()).unwrap();
    assert_eq!(second.completed, 1);
    assert_eq!(second.failed, 0);
    assert_eq!(phase(&store, "old"), "complete");
}

#[test]
fn safety_is_rechecked_after_blob_deletion_before_phase_checkpoint() {
    let directory = tempfile::tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    create_released(&store, "old", "post-check-run");
    create_released(&store, "new", "new-run");
    set_session_order(&store, "old", 1);
    set_session_order(&store, "new", 2);
    plan_oldest(&store, 1);
    let ledger = sqlite_ledger(directory.path());
    append(&ledger, "post-check-run");
    let blob = write_blob(&ledger, "post-check-run", "deleted-before-post-check");
    let database_path = store.database_path().to_owned();
    let injected = AtomicBool::new(false);

    let first = store
        .run_due_gc_at_with_observer(&ledger, 10, Utc::now(), &mut |step, _| {
            if step == GcStep::BlobsDeleted && !injected.swap(true, Ordering::SeqCst) {
                Connection::open(&database_path)
                    .unwrap()
                    .execute(
                        "INSERT INTO host_outbox
                            (delivery_id, session_id, msg_id, run_epoch, sequence, digest, payload,
                             created_at_ms, acknowledged_at_ms)
                         VALUES ('post-check-delivery', 'old', 'msg', 1, 1, zeroblob(32), x'7b7d', 1, NULL)",
                        [],
                    )
                    .unwrap();
            }
            Ok(())
        })
        .unwrap();

    assert_eq!(first.deferred, 1);
    assert_eq!(first.failed, 0);
    assert_eq!(phase(&store, "old"), "ledger_deleted");
    assert!(!blob.exists());
    let connection = Connection::open(store.database_path()).unwrap();
    let retained_session: i64 = connection
        .query_row("SELECT COUNT(*) FROM sessions WHERE session_id = 'old'", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(retained_session, 1);
    connection
        .execute(
            "UPDATE host_outbox SET acknowledged_at_ms = 2 WHERE delivery_id = 'post-check-delivery'",
            [],
        )
        .unwrap();

    let second = store.run_due_gc_at(&ledger, 10, Utc::now()).unwrap();
    assert_eq!(second.completed, 1);
    assert_eq!(phase(&store, "old"), "complete");
}

#[test]
fn pending_host_delivery_or_active_lease_defers_every_destructive_stage() {
    let directory = tempfile::tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    create_released(&store, "old", "deferred-run");
    create_released(&store, "new", "new-run");
    set_session_order(&store, "old", 1);
    set_session_order(&store, "new", 2);
    plan_oldest(&store, 1);
    let ledger = sqlite_ledger(directory.path());
    append(&ledger, "deferred-run");
    let connection = Connection::open(store.database_path()).unwrap();
    connection
        .execute(
            "INSERT INTO host_outbox
                (delivery_id, session_id, msg_id, run_epoch, sequence, digest, payload,
                 created_at_ms, acknowledged_at_ms)
             VALUES ('delivery', 'old', 'msg', 1, 1, zeroblob(32), x'7b7d', 1, NULL)",
            [],
        )
        .unwrap();

    let pending = store.run_due_gc_at(&ledger, 10, Utc::now()).unwrap();
    assert_eq!(pending.deferred, 1);
    assert_eq!(ledger.records_for_run(&RunId::from("deferred-run")).unwrap().len(), 1);
    connection
        .execute(
            "UPDATE host_outbox SET acknowledged_at_ms = 2 WHERE delivery_id = 'delivery'",
            [],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE session_leases
             SET owner_id = 'late-owner', heartbeat_at_ms = ?2, expires_at_ms = ?3
             WHERE session_id = ?1",
            params!["old", Utc::now().timestamp_millis(), i64::MAX],
        )
        .unwrap();

    let active = store.run_due_gc_at(&ledger, 10, Utc::now()).unwrap();
    assert_eq!(active.deferred, 1);
    assert_eq!(ledger.records_for_run(&RunId::from("deferred-run")).unwrap().len(), 1);
    connection
        .execute(
            "UPDATE session_leases
             SET owner_id = NULL, heartbeat_at_ms = NULL, expires_at_ms = NULL
             WHERE session_id = 'old'",
            [],
        )
        .unwrap();

    let completed = store.run_due_gc_at(&ledger, 10, Utc::now()).unwrap();
    assert_eq!(completed.completed, 1);
    assert!(ledger.records_for_run(&RunId::from("deferred-run")).unwrap().is_empty());
}

#[test]
fn non_owner_shared_session_releases_its_reference_and_last_owner_deletes_run() {
    let directory = tempfile::tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    create_released(&store, "a-session", "shared-run");
    create_released(&store, "z-session", "shared-run");
    set_session_order(&store, "a-session", 1);
    set_session_order(&store, "z-session", 2);
    plan_oldest(&store, 1);
    let ledger = sqlite_ledger(directory.path());
    append(&ledger, "shared-run");
    let blob = write_blob(&ledger, "shared-run", "shared");

    let first = store.run_due_gc_at(&ledger, 10, Utc::now()).unwrap();
    assert_eq!(first.completed, 1);
    assert_eq!(ledger.records_for_run(&RunId::from("shared-run")).unwrap().len(), 1);
    assert!(blob.is_dir());
    assert!(store.load("z-session").unwrap().is_some());

    store.enforce_retention(0).unwrap();
    mark_jobs_due(&store);
    let second = store.run_due_gc_at(&ledger, 10, Utc::now()).unwrap();
    assert_eq!(second.completed, 1);
    assert!(ledger.records_for_run(&RunId::from("shared-run")).unwrap().is_empty());
    assert!(!blob.exists());
}

#[test]
fn designated_shared_owner_waits_until_non_owner_job_releases_reference() {
    let directory = tempfile::tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    create_released(&store, "z-session", "shared-run");
    create_released(&store, "a-session", "shared-run");
    set_session_order(&store, "z-session", 1);
    set_session_order(&store, "a-session", 2);
    plan_oldest(&store, 1);
    let ledger = sqlite_ledger(directory.path());
    append(&ledger, "shared-run");

    let owner_waits = store.run_due_gc_at(&ledger, 10, Utc::now()).unwrap();
    assert_eq!(owner_waits.deferred, 1);
    assert_eq!(ledger.records_for_run(&RunId::from("shared-run")).unwrap().len(), 1);

    store.enforce_retention(0).unwrap();
    mark_jobs_due(&store);
    let release_other = store.run_due_gc_at(&ledger, 10, Utc::now()).unwrap();
    assert_eq!(release_other.completed, 1);
    assert!(release_other.deferred >= 1);
    assert_eq!(ledger.records_for_run(&RunId::from("shared-run")).unwrap().len(), 1);

    let owner_finishes = store.run_due_gc_at(&ledger, 10, Utc::now()).unwrap();
    assert_eq!(owner_finishes.completed, 1);
    assert!(ledger.records_for_run(&RunId::from("shared-run")).unwrap().is_empty());
}

#[test]
fn real_processes_collect_the_same_due_jobs_without_corruption_or_duplicate_deletion() {
    let directory = tempfile::tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    create_released(&store, "a-shared", "shared-run");
    create_released(&store, "z-shared", "shared-run");
    create_released(&store, "solo", "solo-run");
    set_session_order(&store, "a-shared", 1);
    set_session_order(&store, "z-shared", 2);
    set_session_order(&store, "solo", 3);
    plan_oldest(&store, 0);

    let ledger = sqlite_ledger(directory.path());
    append(&ledger, "shared-run");
    append(&ledger, "solo-run");
    append(&ledger, "solo-run:workflow:child");
    append(&ledger, "solo-runish");
    let shared_blob = write_blob(&ledger, "shared-run", "shared-output");
    let solo_blob = write_blob(&ledger, "solo-run", "solo-output");
    let child_blob = write_blob(&ledger, "solo-run:workflow:child", "child-output");
    let similar_blob = write_blob(&ledger, "solo-runish", "similar-output");

    let start = directory.path().join("gc-start");
    let ready_a = directory.path().join("gc-a.ready");
    let ready_b = directory.path().join("gc-b.ready");
    let result_a = directory.path().join("gc-a.json");
    let result_b = directory.path().join("gc-b.json");
    let children = vec![
        spawn_gc_fixture(directory.path(), &ready_a, &start, &result_a),
        spawn_gc_fixture(directory.path(), &ready_b, &start, &result_b),
    ];

    wait_for_fixture_files(&[&ready_a, &ready_b]);
    assert_eq!(fs::read_to_string(&ready_a).unwrap(), "3");
    assert_eq!(fs::read_to_string(&ready_b).unwrap(), "3");
    fs::write(&start, b"go").unwrap();
    wait_for_gc_children(children);

    let results = [read_gc_result(&result_a), read_gc_result(&result_b)];
    assert!(results.iter().all(|result| result.selected_jobs == 3));
    assert!(results.iter().all(|result| result.remaining_jobs == 0));
    assert_eq!(results.iter().map(|result| result.completed).sum::<usize>(), 3);
    assert_eq!(results.iter().map(|result| result.failed).sum::<usize>(), 0);
    assert_eq!(
        results
            .iter()
            .map(|result| result.ledger_records_deleted)
            .sum::<usize>(),
        3,
        "each durable Ledger record must be deleted once"
    );
    assert_eq!(
        results.iter().map(|result| result.blob_runs_processed).sum::<usize>(),
        3,
        "each exact Run output directory must be processed once"
    );

    assert!(ledger.records_for_run(&RunId::from("shared-run")).unwrap().is_empty());
    assert!(ledger.records_for_run(&RunId::from("solo-run")).unwrap().is_empty());
    assert!(
        ledger
            .records_for_run(&RunId::from("solo-run:workflow:child"))
            .unwrap()
            .is_empty()
    );
    assert_eq!(ledger.records_for_run(&RunId::from("solo-runish")).unwrap().len(), 1);
    assert!(!shared_blob.exists());
    assert!(!solo_blob.exists());
    assert!(!child_blob.exists());
    assert!(similar_blob.is_dir());

    let connection = Connection::open(store.database_path()).unwrap();
    let counts: (i64, i64, i64, i64, i64, i64) = connection
        .query_row(
            "SELECT
                (SELECT COUNT(*) FROM session_gc_jobs WHERE phase = 'complete'),
                (SELECT COUNT(DISTINCT job_id) FROM session_gc_jobs),
                (SELECT COUNT(*) FROM session_tombstones),
                (SELECT COUNT(*) FROM sessions),
                (SELECT COUNT(*) FROM session_run_references),
                (SELECT COUNT(*) FROM session_gc_run_claims)",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(counts, (3, 3, 3, 0, 0, 0));

    let non_owner_tree = gc_run_tree(&connection, "a-shared");
    assert_eq!(non_owner_tree["released_shared_roots"], json!(["shared-run"]));
    assert_eq!(non_owner_tree["exact_run_ids"], json!([]));
    let owner_tree = gc_run_tree(&connection, "z-shared");
    assert_eq!(owner_tree["exact_run_ids"], json!(["shared-run"]));

    assert_sqlite_integrity(store.database_path());
    assert_sqlite_integrity(&directory.path().join("runtime").join("ledger.sqlite3"));
}

#[test]
#[ignore = "launched as an isolated OS process by the session GC concurrency test"]
fn multiprocess_session_gc_child() {
    let Some(directory) = env::var_os(GC_FIXTURE_DIRECTORY).map(PathBuf::from) else {
        return;
    };
    let ready = PathBuf::from(env::var_os(GC_FIXTURE_READY).unwrap());
    let start = PathBuf::from(env::var_os(GC_FIXTURE_START).unwrap());
    let result = PathBuf::from(env::var_os(GC_FIXTURE_RESULT).unwrap());
    let store = SessionStore::open(&directory).unwrap();
    let ledger = SqliteRuntimeLedger::open(directory.join("runtime").join("ledger.sqlite3")).unwrap();
    let selected_jobs = store.due_gc_job_ids(Utc::now().timestamp_millis(), 64).unwrap().len();
    fs::write(&ready, selected_jobs.to_string()).unwrap();
    eprintln!("session-gc-fixture stage=ready selected_jobs={selected_jobs}");
    wait_for_fixture_files(&[&start]);
    eprintln!("session-gc-fixture stage=released selected_jobs={selected_jobs}");

    let mut process_result = GcProcessResult {
        selected_jobs,
        ..GcProcessResult::default()
    };
    loop {
        let report = store.run_due_gc_at(&ledger, 64, Utc::now()).unwrap();
        process_result.add_report(report);
        process_result.remaining_jobs = incomplete_gc_jobs(&store);
        eprintln!(
            "session-gc-fixture stage=pass passes={} remaining_jobs={} completed={} deferred={} failed={}",
            process_result.passes,
            process_result.remaining_jobs,
            process_result.completed,
            process_result.deferred,
            process_result.failed
        );
        if process_result.remaining_jobs == 0 {
            break;
        }
        thread::yield_now();
    }
    eprintln!("session-gc-fixture stage=settled passes={}", process_result.passes);
    fs::write(&result, serde_json::to_vec(&process_result).unwrap()).unwrap();
}

#[test]
fn incomplete_gc_claim_rejects_reuse_of_same_ancestor_or_descendant_run() {
    let directory = tempfile::tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    create_released(&store, "old", "claimed:child");
    create_released(&store, "new", "new-run");
    set_session_order(&store, "old", 1);
    set_session_order(&store, "new", 2);
    plan_oldest(&store, 1);

    for (session_id, run_id) in [
        ("same", "claimed:child"),
        ("ancestor", "claimed"),
        ("descendant", "claimed:child:workflow"),
    ] {
        let error = store
            .create_active(&session(session_id, run_id), &format!("owner-{session_id}"))
            .unwrap_err();
        assert!(matches!(
            error,
            SessionStoreError::RunGcClaimed {
                requested_run_id,
                ..
            } if requested_run_id == run_id
        ));
    }

    let unrelated = store
        .create_active(&session("unrelated", "claimed-other"), "owner-unrelated")
        .unwrap();
    store.release(&unrelated).unwrap();
}

#[test]
fn zero_job_budget_is_a_read_only_noop() {
    let directory = tempfile::tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    let ledger = sqlite_ledger(directory.path());

    let report = store.run_due_gc_at(&ledger, 0, Utc::now()).unwrap();

    assert_eq!(report, SessionGcReport::default());
}

fn spawn_gc_fixture(directory: &Path, ready: &Path, start: &Path, result: &Path) -> Child {
    let mut command = Command::new(env::current_exe().unwrap());
    command
        .arg(GC_CHILD_TEST)
        .arg("--exact")
        .arg("--ignored")
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(GC_FIXTURE_DIRECTORY, directory)
        .env(GC_FIXTURE_READY, ready)
        .env(GC_FIXTURE_START, start)
        .env(GC_FIXTURE_RESULT, result)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    command.spawn().unwrap()
}

fn wait_for_fixture_files(paths: &[&Path]) {
    let deadline = Instant::now() + GC_PROCESS_TIMEOUT;
    while paths.iter().any(|path| !path.is_file()) {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for session GC process fixture files"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_gc_children(children: Vec<Child>) {
    for child in children {
        let (status, stderr) = wait_for_gc_child(child);
        assert!(
            status.success(),
            "session GC fixture exited with {status}; stderr: {stderr}"
        );
    }
}

fn wait_for_gc_child(mut child: Child) -> (ExitStatus, String) {
    let deadline = Instant::now() + GC_PROCESS_TIMEOUT;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return (status, read_gc_child_stderr(&mut child));
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let status = child.wait().unwrap();
            let stderr = read_gc_child_stderr(&mut child);
            panic!("session GC fixture timed out and was terminated with {status}; stderr: {stderr}");
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn read_gc_child_stderr(child: &mut Child) -> String {
    let mut stderr = Vec::new();
    if let Some(mut stream) = child.stderr.take() {
        stream.read_to_end(&mut stderr).unwrap();
    }
    String::from_utf8_lossy(&stderr).into_owned()
}

fn read_gc_result(path: &Path) -> GcProcessResult {
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
}

fn incomplete_gc_jobs(store: &SessionStore) -> usize {
    let count: i64 = Connection::open(store.database_path())
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM session_gc_jobs WHERE phase != 'complete'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    usize::try_from(count).unwrap()
}

fn gc_run_tree(connection: &Connection, session_id: &str) -> serde_json::Value {
    let encoded: Vec<u8> = connection
        .query_row(
            "SELECT exact_run_tree_json FROM session_gc_jobs WHERE session_id = ?1",
            params![session_id],
            |row| row.get(0),
        )
        .unwrap();
    serde_json::from_slice(&encoded).unwrap()
}

fn assert_sqlite_integrity(path: &Path) {
    let connection = Connection::open(path).unwrap();
    let integrity: String = connection
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .unwrap();
    assert_eq!(integrity, "ok", "SQLite integrity check failed for {}", path.display());
    let mut statement = connection.prepare("PRAGMA foreign_key_check").unwrap();
    let mut rows = statement.query([]).unwrap();
    assert!(
        rows.next().unwrap().is_none(),
        "SQLite foreign key check failed for {}",
        path.display()
    );
}
