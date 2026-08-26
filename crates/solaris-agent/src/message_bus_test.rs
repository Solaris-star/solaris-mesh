use serde_json::json;
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::{AgentId, RunId};
use std::sync::atomic::{AtomicBool, Ordering};

use super::*;
use crate::resource_policy::ResourcePolicy;
use crate::scheduler::Scheduler;

struct FailAfterDurableAppendOnce {
    inner: InMemoryRuntimeLedger,
    record_type: &'static str,
    failed: AtomicBool,
}

impl FailAfterDurableAppendOnce {
    fn new(record_type: &'static str) -> Self {
        Self {
            inner: InMemoryRuntimeLedger::default(),
            record_type,
            failed: AtomicBool::new(false),
        }
    }
}

impl RuntimeLedger for FailAfterDurableAppendOnce {
    crate::runtime_ledger::forward_compare_and_append!();

    fn append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        payload: serde_json::Value,
    ) -> std::io::Result<LedgerRecord> {
        let record = self.inner.append(run_id, durability, record_type, payload)?;
        if record_type == self.record_type && !self.failed.swap(true, Ordering::SeqCst) {
            return Err(std::io::Error::other("injected failure after durable message append"));
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

fn registered_bus(ledger: Arc<dyn RuntimeLedger>, run_id: &RunId, agents: &[AgentId]) -> MessageBus {
    let registry = Arc::new(crate::agent_registry::AgentRegistry::default());
    for agent_id in agents {
        registry.upsert(solaris_types::runtime::AgentRecord {
            run_id: run_id.clone(),
            agent_id: agent_id.clone(),
            team_id: None,
            parent_agent_id: None,
            state: solaris_types::runtime::AgentLifecycleState::Active,
        });
    }
    MessageBus::new(
        ledger,
        registry,
        Arc::new(crate::team_registry::TeamRegistry::default()),
    )
}

fn registered_team_bus(
    ledger: Arc<dyn RuntimeLedger>,
    run_id: &RunId,
    agents: &[AgentId],
    max_pending_messages: u32,
    max_message_bytes: u32,
) -> (MessageBus, TeamId) {
    let registry = Arc::new(crate::agent_registry::AgentRegistry::default());
    let team_id = TeamId::from(format!("limits-{run_id}"));
    for agent_id in agents {
        registry.upsert(solaris_types::runtime::AgentRecord {
            run_id: run_id.clone(),
            agent_id: agent_id.clone(),
            team_id: Some(team_id.clone()),
            parent_agent_id: None,
            state: solaris_types::runtime::AgentLifecycleState::Active,
        });
    }
    let teams = Arc::new(crate::team_registry::TeamRegistry::default());
    teams.create(crate::team_registry::TeamRecord {
        run_id: run_id.clone(),
        team_id: team_id.clone(),
        name: "limits".into(),
        strategy: solaris_types::workflow::CollaborationStrategy::Team,
        coordinator: agents.first().cloned(),
        direct_peer_messaging: true,
        max_pending_messages,
        max_message_bytes,
        members: agents.iter().cloned().collect(),
    });
    (MessageBus::new(ledger, registry, teams), team_id)
}

#[test]
fn legacy_team_record_uses_message_limit_defaults() {
    let team: crate::team_registry::TeamRecord = serde_json::from_value(json!({
        "run_id": "legacy-run",
        "team_id": "legacy-team",
        "name": "legacy",
        "strategy": "team"
    }))
    .unwrap();

    assert_eq!(
        team.max_pending_messages,
        solaris_types::workflow::CollaborationRuntimeConfig::DEFAULT_MAX_PENDING_MESSAGES
    );
    assert_eq!(
        team.max_message_bytes,
        solaris_types::workflow::CollaborationRuntimeConfig::DEFAULT_MAX_MESSAGE_BYTES
    );
}

#[test]
fn team_message_bytes_accept_exact_limit_and_reject_one_byte_less_without_mutation() {
    let body = json!({"text": "é"});
    let body_bytes = serde_json::to_vec(&body).unwrap().len() as u32;
    let run_id = RunId::from("message-byte-limit");
    let sender = AgentId::from("sender");
    let target = AgentId::from("target");

    let exact_ledger = Arc::new(InMemoryRuntimeLedger::default());
    let (exact_bus, exact_team) =
        registered_team_bus(exact_ledger, &run_id, &[sender.clone(), target.clone()], 2, body_bytes);
    exact_bus
        .send(
            run_id.clone(),
            Some(exact_team),
            sender.clone(),
            target.clone(),
            "exact",
            body.clone(),
        )
        .unwrap();

    let rejected_ledger = Arc::new(InMemoryRuntimeLedger::default());
    let (rejected_bus, rejected_team) = registered_team_bus(
        rejected_ledger.clone(),
        &run_id,
        &[sender.clone(), target.clone()],
        2,
        body_bytes - 1,
    );
    assert!(
        rejected_bus
            .send(
                run_id.clone(),
                Some(rejected_team),
                sender,
                target.clone(),
                "too-large",
                body,
            )
            .unwrap_err()
            .to_string()
            .contains("max_message_bytes")
    );
    assert!(rejected_bus.inbox(&target).is_empty());
    assert!(rejected_ledger.records_for_run(&run_id).unwrap().is_empty());
}

#[test]
fn acknowledgement_releases_pending_capacity_and_full_inbox_allows_idempotent_replay() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("pending-limit");
    let sender = AgentId::from("sender");
    let target = AgentId::from("target");
    let (bus, team_id) = registered_team_bus(ledger.clone(), &run_id, &[sender.clone(), target.clone()], 1, 1_024);
    let send_stable = || {
        bus.send_with_id_within_mutation(
            "stable-full-inbox".into(),
            run_id.clone(),
            Some(team_id.clone()),
            sender.clone(),
            target.clone(),
            "request",
            json!({"value": 1}),
        )
    };
    let (first, first_record) = send_stable().unwrap();
    assert!(first_record.is_some());
    assert!(send_stable().unwrap().1.is_none());
    assert!(
        bus.send(
            run_id.clone(),
            Some(team_id.clone()),
            sender.clone(),
            target.clone(),
            "overflow",
            json!({}),
        )
        .unwrap_err()
        .to_string()
        .contains("max_pending_messages")
    );
    assert!(bus.acknowledge(&run_id, &target, &first.message_id).unwrap());
    bus.send(run_id, Some(team_id), sender, target.clone(), "after-ack", json!({}))
        .unwrap();
    assert_eq!(bus.inbox(&target).len(), 1);
}

#[test]
fn broadcast_rejects_atomically_when_any_recipient_inbox_is_full() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("broadcast-pending-limit");
    let sender = AgentId::from("sender");
    let full = AgentId::from("full");
    let empty = AgentId::from("empty");
    let (bus, team_id) = registered_team_bus(
        ledger.clone(),
        &run_id,
        &[sender.clone(), full.clone(), empty.clone()],
        1,
        1_024,
    );
    bus.send(
        run_id.clone(),
        Some(team_id.clone()),
        sender.clone(),
        full.clone(),
        "prefill",
        json!({}),
    )
    .unwrap();
    let records_before = ledger.records_for_run(&run_id).unwrap().len();

    assert!(
        bus.broadcast_within_mutation(
            run_id.clone(),
            team_id,
            sender,
            vec![full.clone(), empty.clone()],
            "broadcast".into(),
            json!({"value": 1}),
        )
        .unwrap_err()
        .to_string()
        .contains("max_pending_messages")
    );
    assert_eq!(ledger.records_for_run(&run_id).unwrap().len(), records_before);
    assert_eq!(bus.inbox(&full).len(), 1);
    assert!(bus.inbox(&empty).is_empty());
}

#[test]
fn messages_are_delivered_to_target_inbox() {
    let run_id = RunId::from("run");
    let target = AgentId::from("b");
    let bus = registered_bus(
        Arc::new(InMemoryRuntimeLedger::default()),
        &run_id,
        &[AgentId::from("a"), target.clone()],
    );
    bus.send(
        run_id.clone(),
        None,
        AgentId::from("a"),
        target.clone(),
        "request",
        json!({"question": "review"}),
    )
    .unwrap();
    assert_eq!(bus.inbox(&target).len(), 1);
    assert_eq!(bus.drain(&run_id, &target).unwrap().len(), 1);
    assert!(bus.inbox(&target).is_empty());
}

#[test]
fn stable_message_id_is_idempotent_and_rejects_content_collisions() {
    let run_id = RunId::from("stable-message-run");
    let sender = AgentId::from("sender");
    let target = AgentId::from("target");
    let bus = registered_bus(
        Arc::new(InMemoryRuntimeLedger::default()),
        &run_id,
        &[sender.clone(), target.clone()],
    );
    let send = |body| {
        bus.send_with_id_within_mutation(
            "workflow-worker-result:one".to_owned(),
            run_id.clone(),
            None,
            sender.clone(),
            target.clone(),
            "worker_result",
            body,
        )
    };

    let (_, first_record) = send(json!({"value": 1})).unwrap();
    let (_, second_record) = send(json!({"value": 1})).unwrap();

    assert!(first_record.is_some());
    assert!(second_record.is_none());
    assert_eq!(bus.inbox(&target).len(), 1);
    assert!(send(json!({"value": 2})).is_err());
}

#[test]
fn stable_broadcast_replay_succeeds_when_inboxes_are_full_and_tampering_is_rejected() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("stable-broadcast-run");
    let sender = AgentId::from("sender");
    let first = AgentId::from("first");
    let second = AgentId::from("second");
    let recipients = vec![first.clone(), second.clone()];
    let (bus, team_id) = registered_team_bus(
        ledger.clone(),
        &run_id,
        &[sender.clone(), first.clone(), second.clone()],
        1,
        1_024,
    );
    let broadcast = |recipients: Vec<AgentId>, body| {
        bus.broadcast_with_id_within_mutation(
            "broadcast-effect:one".to_owned(),
            run_id.clone(),
            team_id.clone(),
            sender.clone(),
            recipients,
            "assignment".to_owned(),
            body,
        )
    };

    let (initial, initial_record) = broadcast(recipients.clone(), json!({"task": 1})).unwrap();
    let (replayed, replay_record) = broadcast(recipients.clone(), json!({"task": 1})).unwrap();

    assert_eq!(initial, replayed);
    assert!(initial_record.is_some());
    assert!(replay_record.is_none());
    assert_eq!(bus.inbox(&first).len(), 1);
    assert_eq!(bus.inbox(&second).len(), 1);
    assert!(broadcast(recipients.clone(), json!({"task": 2})).is_err());
    assert!(broadcast(vec![first], json!({"task": 1})).is_err());
    assert_eq!(
        ledger
            .records_for_run(&run_id)
            .unwrap()
            .iter()
            .filter(|record| record.record_type == "messages_broadcast")
            .count(),
        1
    );
}

#[test]
fn durable_send_and_broadcast_replay_repair_missing_projection_in_same_process() {
    let send_ledger = Arc::new(FailAfterDurableAppendOnce::new("message_delivered"));
    let send_run = RunId::from("send-projection-repair-same-process");
    let sender = AgentId::from("sender");
    let target = AgentId::from("target");
    let send_bus = registered_bus(send_ledger.clone(), &send_run, &[sender.clone(), target.clone()]);
    let send = || {
        send_bus.send_with_id_within_mutation(
            "stable-send-after-append".into(),
            send_run.clone(),
            None,
            sender.clone(),
            target.clone(),
            "result",
            json!({"value": 1}),
        )
    };

    assert!(send().is_err());
    assert!(send_bus.inbox(&target).is_empty());
    let (message, replay_record) = send().unwrap();
    assert_eq!(message.message_id, "stable-send-after-append");
    assert!(replay_record.is_none());
    assert_eq!(send_bus.inbox(&target), vec![message]);
    assert!(send().unwrap().1.is_none());
    assert_eq!(send_bus.inbox(&target).len(), 1);

    let broadcast_ledger = Arc::new(FailAfterDurableAppendOnce::new("messages_broadcast"));
    let broadcast_run = RunId::from("broadcast-projection-repair-same-process");
    let first = AgentId::from("first");
    let second = AgentId::from("second");
    let (broadcast_bus, team_id) = registered_team_bus(
        broadcast_ledger.clone(),
        &broadcast_run,
        &[sender.clone(), first.clone(), second.clone()],
        1,
        1_024,
    );
    let broadcast = || {
        broadcast_bus.broadcast_with_id_within_mutation(
            "stable-broadcast-after-append".into(),
            broadcast_run.clone(),
            team_id.clone(),
            sender.clone(),
            vec![first.clone(), second.clone()],
            "result".into(),
            json!({"value": 1}),
        )
    };

    assert!(broadcast().is_err());
    assert!(broadcast_bus.inbox(&first).is_empty());
    assert!(broadcast_bus.inbox(&second).is_empty());
    let (messages, replay_record) = broadcast().unwrap();
    assert!(replay_record.is_none());
    assert_eq!(broadcast_bus.inbox(&first), vec![messages[0].clone()]);
    assert_eq!(broadcast_bus.inbox(&second), vec![messages[1].clone()]);
    assert!(broadcast().unwrap().1.is_none());
    assert_eq!(broadcast_bus.inbox(&first).len(), 1);
    assert_eq!(broadcast_bus.inbox(&second).len(), 1);
}

#[test]
fn durable_send_and_broadcast_replay_repair_missing_projection_after_restart() {
    let send_ledger = Arc::new(FailAfterDurableAppendOnce::new("message_delivered"));
    let send_run = RunId::from("send-projection-repair-restart");
    let sender = AgentId::from("sender");
    let target = AgentId::from("target");
    let first_bus = registered_bus(send_ledger.clone(), &send_run, &[sender.clone(), target.clone()]);
    assert!(
        first_bus
            .send_with_id_within_mutation(
                "stable-send-restart".into(),
                send_run.clone(),
                None,
                sender.clone(),
                target.clone(),
                "result",
                json!({"value": 1}),
            )
            .is_err()
    );
    drop(first_bus);
    let restarted_bus = registered_bus(send_ledger.clone(), &send_run, &[sender.clone(), target.clone()]);
    let (message, replay_record) = restarted_bus
        .send_with_id_within_mutation(
            "stable-send-restart".into(),
            send_run.clone(),
            None,
            sender.clone(),
            target.clone(),
            "result",
            json!({"value": 1}),
        )
        .unwrap();
    assert!(replay_record.is_none());
    assert_eq!(restarted_bus.inbox(&target), vec![message]);

    let broadcast_ledger = Arc::new(FailAfterDurableAppendOnce::new("messages_broadcast"));
    let broadcast_run = RunId::from("broadcast-projection-repair-restart");
    let first = AgentId::from("first");
    let second = AgentId::from("second");
    let (first_bus, team_id) = registered_team_bus(
        broadcast_ledger.clone(),
        &broadcast_run,
        &[sender.clone(), first.clone(), second.clone()],
        1,
        1_024,
    );
    assert!(
        first_bus
            .broadcast_with_id_within_mutation(
                "stable-broadcast-restart".into(),
                broadcast_run.clone(),
                team_id.clone(),
                sender.clone(),
                vec![first.clone(), second.clone()],
                "result".into(),
                json!({"value": 1}),
            )
            .is_err()
    );
    drop(first_bus);
    let (restarted_bus, restarted_team_id) = registered_team_bus(
        broadcast_ledger,
        &broadcast_run,
        &[sender.clone(), first.clone(), second.clone()],
        1,
        1_024,
    );
    let (messages, replay_record) = restarted_bus
        .broadcast_with_id_within_mutation(
            "stable-broadcast-restart".into(),
            broadcast_run,
            restarted_team_id,
            sender,
            vec![first.clone(), second.clone()],
            "result".into(),
            json!({"value": 1}),
        )
        .unwrap();
    assert!(replay_record.is_none());
    assert_eq!(restarted_bus.inbox(&first), vec![messages[0].clone()]);
    assert_eq!(restarted_bus.inbox(&second), vec![messages[1].clone()]);
}

#[test]
fn stable_send_replay_respects_later_acknowledgement_and_dequeue_records() {
    let acknowledged_ledger = Arc::new(InMemoryRuntimeLedger::default());
    let acknowledged_run = RunId::from("stable-send-ack-replay");
    let sender = AgentId::from("sender");
    let target = AgentId::from("target");
    let bus = registered_bus(
        acknowledged_ledger.clone(),
        &acknowledged_run,
        &[sender.clone(), target.clone()],
    );
    let (acknowledged, _) = bus
        .send_with_id_within_mutation(
            "stable-send-ack".into(),
            acknowledged_run.clone(),
            None,
            sender.clone(),
            target.clone(),
            "result",
            json!({"value": 1}),
        )
        .unwrap();
    assert!(
        bus.acknowledge(&acknowledged_run, &target, &acknowledged.message_id)
            .unwrap()
    );
    drop(bus);
    let restarted = registered_bus(
        acknowledged_ledger,
        &acknowledged_run,
        &[sender.clone(), target.clone()],
    );
    restarted
        .send_with_id_within_mutation(
            "stable-send-ack".into(),
            acknowledged_run,
            None,
            sender.clone(),
            target.clone(),
            "result",
            json!({"value": 1}),
        )
        .unwrap();
    assert!(restarted.inbox(&target).is_empty());
    assert_eq!(restarted.snapshot().len(), 1);
    assert!(restarted.snapshot()[0].acknowledged_at_unix_ms.is_some());

    let dequeued_ledger = Arc::new(InMemoryRuntimeLedger::default());
    let dequeued_run = RunId::from("stable-send-dequeue-replay");
    let bus = registered_bus(
        dequeued_ledger.clone(),
        &dequeued_run,
        &[sender.clone(), target.clone()],
    );
    bus.send_with_id_within_mutation(
        "stable-send-dequeue".into(),
        dequeued_run.clone(),
        None,
        sender.clone(),
        target.clone(),
        "result",
        json!({"value": 1}),
    )
    .unwrap();
    assert_eq!(bus.drain(&dequeued_run, &target).unwrap().len(), 1);
    drop(bus);
    let restarted = registered_bus(dequeued_ledger, &dequeued_run, &[sender.clone(), target.clone()]);
    restarted
        .send_with_id_within_mutation(
            "stable-send-dequeue".into(),
            dequeued_run,
            None,
            sender,
            target.clone(),
            "result",
            json!({"value": 1}),
        )
        .unwrap();
    assert!(restarted.inbox(&target).is_empty());
    assert!(restarted.snapshot().is_empty());
}

#[test]
fn restore_message_is_idempotent_by_message_id() {
    let run_id = RunId::from("restore-message-idempotent");
    let sender = AgentId::from("sender");
    let target = AgentId::from("target");
    let bus = registered_bus(
        Arc::new(InMemoryRuntimeLedger::default()),
        &run_id,
        &[sender.clone(), target.clone()],
    );
    let message = AgentMessage {
        message_id: "restore-once".into(),
        run_id,
        team_id: None,
        from: sender,
        to: target,
        kind: "result".into(),
        body: json!({"value": 1}),
        created_at_unix_ms: 1,
        acknowledged_at_unix_ms: None,
    };

    bus.restore_message(message.clone());
    bus.restore_message(message.clone());

    assert_eq!(bus.snapshot(), vec![message]);
}

#[test]
fn legacy_broadcast_array_remains_restorable() {
    let run_id = RunId::from("legacy-broadcast-run");
    let sender = AgentId::from("sender");
    let target = AgentId::from("target");
    let bus = registered_bus(
        Arc::new(InMemoryRuntimeLedger::default()),
        &run_id,
        &[sender.clone(), target.clone()],
    );
    let message = AgentMessage {
        message_id: "legacy-message".to_owned(),
        run_id,
        team_id: None,
        from: sender,
        to: target.clone(),
        kind: "legacy".to_owned(),
        body: json!({"value": 1}),
        created_at_unix_ms: 1,
        acknowledged_at_unix_ms: None,
    };

    bus.restore_broadcast(json!([message])).unwrap();

    assert_eq!(bus.inbox(&target).len(), 1);
}

#[test]
fn drained_messages_stay_removed_after_ledger_restore() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("restore-run");
    let target = AgentId::from("target");
    let bus = registered_bus(ledger.clone(), &run_id, &[AgentId::from("sender"), target.clone()]);
    bus.send(
        run_id.clone(),
        None,
        AgentId::from("sender"),
        target.clone(),
        "request",
        json!({}),
    )
    .unwrap();
    assert_eq!(bus.drain(&run_id, &target).unwrap().len(), 1);

    let restored: crate::collaboration_runtime::CollaborationRuntime<()> =
        crate::collaboration_runtime::CollaborationRuntime::with_ledger(Scheduler::new(ResourcePolicy::new(1)), ledger);
    restored.restore_projection(&run_id).unwrap();
    for agent_id in [AgentId::from("sender"), target.clone()] {
        restored.agents().upsert(solaris_types::runtime::AgentRecord {
            run_id: run_id.clone(),
            agent_id,
            team_id: None,
            parent_agent_id: None,
            state: solaris_types::runtime::AgentLifecycleState::Active,
        });
    }
    assert!(restored.messages().inbox(&target).is_empty());
}

#[test]
fn concurrent_drain_delivers_each_message_once() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("concurrent-drain");
    let target = AgentId::from("target");
    let bus = Arc::new(registered_bus(
        ledger.clone(),
        &run_id,
        &[AgentId::from("sender"), target.clone()],
    ));
    bus.send(
        run_id.clone(),
        None,
        AgentId::from("sender"),
        target.clone(),
        "request",
        json!({"value": 1}),
    )
    .unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let mut workers = Vec::new();
    for _ in 0..2 {
        let bus = Arc::clone(&bus);
        let run_id = run_id.clone();
        let target = target.clone();
        let barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            barrier.wait();
            bus.drain(&run_id, &target).unwrap()
        }));
    }
    barrier.wait();
    let delivered: Vec<_> = workers.into_iter().flat_map(|worker| worker.join().unwrap()).collect();
    assert_eq!(delivered.len(), 1);
    assert_eq!(
        ledger
            .records_for_run(&run_id)
            .unwrap()
            .iter()
            .filter(|record| record.record_type == "messages_dequeued")
            .count(),
        1
    );
    assert!(bus.inbox(&target).is_empty());
}

#[test]
fn concurrent_claims_deliver_each_message_to_only_one_operation() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("concurrent-claim");
    let target = AgentId::from("target");
    let bus = Arc::new(registered_bus(
        ledger,
        &run_id,
        &[AgentId::from("sender"), target.clone()],
    ));
    bus.send(
        run_id.clone(),
        None,
        AgentId::from("sender"),
        target.clone(),
        "request",
        json!({"value": 1}),
    )
    .unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let mut workers = Vec::new();
    for claim_id in ["claim-a", "claim-b"] {
        let bus = Arc::clone(&bus);
        let run_id = run_id.clone();
        let target = target.clone();
        let barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            barrier.wait();
            bus.claim(&run_id, &target, claim_id).unwrap()
        }));
    }
    barrier.wait();
    let claimed: Vec<_> = workers.into_iter().map(|worker| worker.join().unwrap()).collect();
    assert_eq!(claimed.iter().map(Vec::len).sum::<usize>(), 1);
}

#[test]
fn acknowledged_messages_are_not_claimed_by_a_new_operation() {
    let run_id = RunId::from("claim-ack");
    let target = AgentId::from("target");
    let bus = registered_bus(
        Arc::new(InMemoryRuntimeLedger::default()),
        &run_id,
        &[AgentId::from("sender"), target.clone()],
    );
    let message = bus
        .send(
            run_id.clone(),
            None,
            AgentId::from("sender"),
            target.clone(),
            "request",
            json!({}),
        )
        .unwrap();
    assert_eq!(bus.claim(&run_id, &target, "first").unwrap().len(), 1);
    assert!(bus.acknowledge(&run_id, &target, &message.message_id).unwrap());
    assert!(bus.claim(&run_id, &target, "first").unwrap().is_empty());
    assert!(bus.claim(&run_id, &target, "second").unwrap().is_empty());
    assert!(bus.inbox(&target).is_empty());
}

#[test]
fn durable_claim_is_reused_by_the_same_operation_after_restore() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("claim-restore");
    let target = AgentId::from("target");
    let bus = registered_bus(ledger.clone(), &run_id, &[AgentId::from("sender"), target.clone()]);
    bus.send(
        run_id.clone(),
        None,
        AgentId::from("sender"),
        target.clone(),
        "request",
        json!({"value": 1}),
    )
    .unwrap();
    let first = bus.claim(&run_id, &target, "stable-read").unwrap();
    assert_eq!(first.len(), 1);

    let restored: crate::collaboration_runtime::CollaborationRuntime<()> =
        crate::collaboration_runtime::CollaborationRuntime::with_ledger(Scheduler::new(ResourcePolicy::new(1)), ledger);
    restored.restore_projection(&run_id).unwrap();
    for agent_id in [AgentId::from("sender"), target.clone()] {
        restored.agents().upsert(solaris_types::runtime::AgentRecord {
            run_id: run_id.clone(),
            agent_id,
            team_id: None,
            parent_agent_id: None,
            state: solaris_types::runtime::AgentLifecycleState::Active,
        });
    }
    assert_eq!(
        restored.messages().claim(&run_id, &target, "stable-read").unwrap(),
        first
    );
    assert!(
        restored
            .messages()
            .claim(&run_id, &target, "other-read")
            .unwrap()
            .is_empty()
    );
}

#[test]
fn message_acknowledgement_is_target_scoped_and_durable() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("run");
    let target = AgentId::from("b");
    let bus = registered_bus(
        ledger.clone(),
        &run_id,
        &[AgentId::from("a"), target.clone(), AgentId::from("c")],
    );
    let message = bus
        .send(
            run_id.clone(),
            None,
            AgentId::from("a"),
            target.clone(),
            "request",
            json!({"question": "review"}),
        )
        .unwrap();

    assert!(
        !bus.acknowledge(&run_id, &AgentId::from("c"), &message.message_id)
            .unwrap()
    );
    assert!(bus.acknowledge(&run_id, &target, &message.message_id).unwrap());
    assert!(bus.inbox(&target).is_empty());
    let records = ledger.records_for_run(&run_id).unwrap();
    assert!(records.iter().any(|record| {
        record.record_type == "message_acknowledged" && record.durability == DurabilityClass::SyncCritical
    }));
}

#[test]
fn cross_run_send_and_acknowledgement_are_rejected_without_ledger_changes() {
    let ledger: Arc<dyn RuntimeLedger> = Arc::new(InMemoryRuntimeLedger::default());
    let run_a = RunId::from("run-a");
    let run_b = RunId::from("run-b");
    let sender = AgentId::from("sender");
    let receiver = AgentId::from("receiver");
    let bus = registered_bus(Arc::clone(&ledger), &run_a, &[sender.clone(), receiver.clone()]);
    assert!(
        bus.send(
            run_b.clone(),
            None,
            sender.clone(),
            receiver.clone(),
            "request",
            json!({}),
        )
        .is_err()
    );
    assert!(ledger.records_for_run(&run_b).unwrap().is_empty());

    let message = bus
        .send(run_a.clone(), None, sender, receiver.clone(), "request", json!({}))
        .unwrap();
    assert!(bus.acknowledge(&run_b, &receiver, &message.message_id).is_err());
    assert!(ledger.records_for_run(&run_b).unwrap().is_empty());
    assert!(bus.inbox(&receiver)[0].acknowledged_at_unix_ms.is_none());
}
