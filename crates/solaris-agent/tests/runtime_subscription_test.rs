use solaris_agent::collaboration_runtime::CollaborationRuntime;
use solaris_agent::resource_policy::ResourcePolicy;
use solaris_agent::scheduler::Scheduler;
use solaris_types::identity::RunId;

#[test]
fn runtime_subscription_starts_with_snapshot_and_receives_next_sequence() {
    let runtime: CollaborationRuntime<()> = CollaborationRuntime::new(Scheduler::new(ResourcePolicy::new(1)));
    let run_id = RunId::from("run-1");
    let first = runtime.emit_live_event(run_id.clone(), None, "first", serde_json::json!({"value": 1}));

    let mut subscription = runtime
        .subscribe_with_snapshot(&run_id)
        .expect("subscribe with snapshot");
    assert_eq!(subscription.snapshot.live_sequence, first.sequence);
    assert!(subscription.snapshot.timestamp_unix_ms > 0);

    let second = runtime.emit_live_event(run_id, None, "second", serde_json::json!({"value": 2}));
    let received = subscription.receiver.try_recv().expect("next live event");
    assert_eq!(received.sequence, second.sequence);
    assert_eq!(received.kind, "second");
    assert!(received.timestamp_unix_ms > 0);
}
