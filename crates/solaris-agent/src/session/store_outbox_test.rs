use std::env;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use solaris_types::message::TokenUsage;
use tempfile::tempdir;

use super::super::*;
use super::*;

const FIXTURE_MODE: &str = "SOLARIS_OUTBOX_FIXTURE_MODE";
const FIXTURE_DIRECTORY: &str = "SOLARIS_OUTBOX_FIXTURE_DIRECTORY";
const FIXTURE_SESSION_ID: &str = "SOLARIS_OUTBOX_FIXTURE_SESSION_ID";
const FIXTURE_RUN_ID: &str = "SOLARIS_OUTBOX_FIXTURE_RUN_ID";
const FIXTURE_EPOCH: &str = "SOLARIS_OUTBOX_FIXTURE_EPOCH";
const FIXTURE_DELIVERY_ID: &str = "SOLARIS_OUTBOX_FIXTURE_DELIVERY_ID";

#[test]
fn restart_adopts_pending_delivery_and_rejects_stale_or_forged_acknowledgements() {
    let directory = tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    let start = timestamp(1_800_000_000_000);
    let session = sample_session("outbox-restart");
    let first_lease = store.create_active_at(&session, "first-owner", start).unwrap();
    let first = store
        .prepare_host_outbox_at(&session.id, session.run_id.as_deref().unwrap(), start)
        .unwrap();
    assert_eq!(first.run_epoch, first_lease.epoch);
    let original = store
        .enqueue_host_delivery_at(&first, "message-1", br#"{"type":"text_delta","text":"hello"}"#, start)
        .unwrap();

    let restart_time = start + ChronoDuration::seconds(DEFAULT_LEASE_SECONDS + 1);
    let second_lease = store
        .load_active_at(&session.id, "second-owner", restart_time)
        .unwrap()
        .lease;
    let second = store
        .prepare_host_outbox_at(&session.id, session.run_id.as_deref().unwrap(), restart_time)
        .unwrap();
    let pending = store.pending_host_deliveries_at(&second, restart_time).unwrap();

    assert_eq!(second.run_epoch, second_lease.epoch);
    assert_ne!(second.run_epoch, first.run_epoch);
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].delivery_id, original.delivery_id);
    assert_eq!(pending[0].digest, original.digest);
    assert_eq!(pending[0].payload, original.payload);
    assert!(matches!(
        store.acknowledge_host_delivery_at(
            &second,
            first.run_epoch,
            &original.delivery_id,
            &original.digest,
            restart_time,
        ),
        Err(SessionStoreError::HostOutboxStaleEpoch)
    ));
    assert!(matches!(
        store.acknowledge_host_delivery_at(
            &first,
            second.run_epoch,
            &original.delivery_id,
            &original.digest,
            restart_time,
        ),
        Err(SessionStoreError::HostOutboxStaleEpoch)
    ));
    assert!(matches!(
        store.acknowledge_host_delivery_at(
            &second,
            second.run_epoch,
            "forged-delivery",
            &original.digest,
            restart_time,
        ),
        Err(SessionStoreError::HostOutboxDeliveryNotFound)
    ));
    assert!(matches!(
        store.acknowledge_host_delivery_at(
            &second,
            second.run_epoch,
            &original.delivery_id,
            &format!("sha256:{}", "0".repeat(64)),
            restart_time,
        ),
        Err(SessionStoreError::HostOutboxDigestMismatch)
    ));
    assert_eq!(
        store
            .acknowledge_host_delivery_at(
                &second,
                second.run_epoch,
                &original.delivery_id,
                &original.digest,
                restart_time,
            )
            .unwrap(),
        StoredHostAckOutcome::Acknowledged
    );
    assert_eq!(
        store
            .acknowledge_host_delivery_at(
                &second,
                second.run_epoch,
                &original.delivery_id,
                &original.digest,
                restart_time,
            )
            .unwrap(),
        StoredHostAckOutcome::AlreadyAcknowledged
    );
    assert!(
        store
            .pending_host_deliveries_at(&second, restart_time)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn outbox_digest_uses_canonical_event_bytes_for_enqueue_replay_and_ack() {
    let directory = tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    let session = sample_session("outbox-canonical-digest");
    store.create_active(&session, "owner").unwrap();
    let outbox = store
        .prepare_host_outbox(&session.id, session.run_id.as_deref().unwrap())
        .unwrap();
    let first = store
        .enqueue_host_delivery(
            &outbox,
            "message",
            br#"{"type":"info","payload":{"label":"1","count":1},"items":[{"z":2,"a":true}]}"#,
        )
        .unwrap();
    let reordered = store
        .enqueue_host_delivery(
            &outbox,
            "message",
            br#"{"items":[{"a":true,"z":2}],"payload":{"count":1,"label":"1"},"type":"info"}"#,
        )
        .unwrap();
    let changed = store
        .enqueue_host_delivery(
            &outbox,
            "message",
            br#"{"items":[{"a":true,"z":3}],"payload":{"count":1,"label":"1"},"type":"info"}"#,
        )
        .unwrap();

    assert_eq!(first.digest, reordered.digest);
    assert_ne!(first.digest, changed.digest);
    let pending = store.pending_host_deliveries(&outbox).unwrap();
    assert_eq!(pending[0].digest, first.digest);
    assert_eq!(pending[1].digest, reordered.digest);
    assert_eq!(
        store
            .acknowledge_host_delivery(&outbox, outbox.run_epoch, &first.delivery_id, &first.digest)
            .unwrap(),
        StoredHostAckOutcome::Acknowledged
    );
}

#[test]
fn acknowledgement_and_session_save_commit_under_one_sqlite_writer_order() {
    let directory = tempdir().unwrap();
    let store = Arc::new(SessionStore::open(directory.path()).unwrap());
    let session = sample_session("outbox-concurrent");
    let lease = store.create_active(&session, "owner").unwrap();
    let outbox = store
        .prepare_host_outbox(&session.id, session.run_id.as_deref().unwrap())
        .unwrap();
    let delivery = store
        .enqueue_host_delivery(&outbox, "message", br#"{"type":"stream_end"}"#)
        .unwrap();
    let barrier = Arc::new(Barrier::new(2));

    let ack_thread = {
        let store = Arc::clone(&store);
        let outbox = outbox.clone();
        let delivery = delivery.clone();
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || {
            barrier.wait();
            store.acknowledge_host_delivery(&outbox, outbox.run_epoch, &delivery.delivery_id, &delivery.digest)
        })
    };
    let save_thread = {
        let store = Arc::clone(&store);
        let mut lease = lease.clone();
        let mut session = session.clone();
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || {
            barrier.wait();
            session.model = "saved-concurrently".to_owned();
            session.updated_at = Utc::now();
            store.save(&mut lease, &session)
        })
    };

    assert_eq!(ack_thread.join().unwrap().unwrap(), StoredHostAckOutcome::Acknowledged);
    save_thread.join().unwrap().unwrap();
    assert_eq!(
        store.load(&session.id).unwrap().unwrap().session.model,
        "saved-concurrently"
    );
}

#[test]
fn process_exit_during_enqueue_or_acknowledgement_keeps_transactions_atomic() {
    let directory = tempdir().unwrap();
    let store = SessionStore::open(directory.path()).unwrap();
    let session = sample_session("outbox-crash");
    store.create_active(&session, "owner").unwrap();
    let outbox = store
        .prepare_host_outbox(&session.id, session.run_id.as_deref().unwrap())
        .unwrap();

    let enqueue_status = spawn_crash_fixture("enqueue", directory.path(), &outbox, None);
    assert_eq!(enqueue_status, 92);
    assert!(store.pending_host_deliveries(&outbox).unwrap().is_empty());

    let delivery = store
        .enqueue_host_delivery(&outbox, "committed-message", br#"{"type":"info"}"#)
        .unwrap();
    let ack_status = spawn_crash_fixture("ack", directory.path(), &outbox, Some(&delivery.delivery_id));
    assert_eq!(ack_status, 93);
    let pending = store.pending_host_deliveries(&outbox).unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].delivery_id, delivery.delivery_id);
}

#[test]
fn outbox_crash_fixture() {
    let Ok(mode) = env::var(FIXTURE_MODE) else {
        return;
    };
    let store = SessionStore::open(PathBuf::from(env::var_os(FIXTURE_DIRECTORY).unwrap())).unwrap();
    let context = StoredHostOutbox {
        session_id: env::var(FIXTURE_SESSION_ID).unwrap(),
        run_id: env::var(FIXTURE_RUN_ID).unwrap(),
        run_epoch: env::var(FIXTURE_EPOCH).unwrap().parse().unwrap(),
    };
    match mode.as_str() {
        "enqueue" => store.test_exit_before_host_enqueue_commit(&context, 92),
        "ack" => store.test_exit_before_host_ack_commit(&context, &env::var(FIXTURE_DELIVERY_ID).unwrap(), 93),
        _ => panic!("unknown outbox fixture mode"),
    }
}

fn spawn_crash_fixture(mode: &str, directory: &Path, context: &StoredHostOutbox, delivery_id: Option<&str>) -> i32 {
    let mut command = Command::new(env::current_exe().unwrap());
    command
        .arg("session::store::store_outbox::store_outbox_test::outbox_crash_fixture")
        .arg("--exact")
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(FIXTURE_MODE, mode)
        .env(FIXTURE_DIRECTORY, directory)
        .env(FIXTURE_SESSION_ID, &context.session_id)
        .env(FIXTURE_RUN_ID, &context.run_id)
        .env(FIXTURE_EPOCH, context.run_epoch.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(delivery_id) = delivery_id {
        command.env(FIXTURE_DELIVERY_ID, delivery_id);
    }
    let mut child = command.spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status.code().unwrap_or(-1);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("outbox crash fixture timed out");
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn sample_session(id: &str) -> Session {
    let now = timestamp(1_800_000_000_000);
    Session {
        id: id.to_owned(),
        run_id: Some(format!("run-{id}")),
        created_at: now,
        updated_at: now,
        provider: "test-provider".to_owned(),
        model: "test-model".to_owned(),
        cwd: "/workspace".to_owned(),
        total_usage: TokenUsage::default(),
        messages: Vec::new(),
        runtime_state: None,
    }
}

fn timestamp(milliseconds: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_millis_opt(milliseconds).single().unwrap()
}
