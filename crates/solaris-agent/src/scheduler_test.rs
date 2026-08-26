use super::*;

#[test]
fn fifo_order() {
    let mut scheduler = Scheduler::new(ResourcePolicy::new(2));
    scheduler.enqueue(ScheduledTask {
        id: "a".into(),
        payload: 1,
    });
    scheduler.enqueue(ScheduledTask {
        id: "b".into(),
        payload: 2,
    });
    assert_eq!(scheduler.acquire_next().unwrap().id, "a");
    assert_eq!(scheduler.acquire_next().unwrap().id, "b");
}

#[test]
fn capacity_and_release() {
    let mut scheduler = Scheduler::new(ResourcePolicy::new(1));
    scheduler.enqueue(ScheduledTask {
        id: "a".into(),
        payload: (),
    });
    scheduler.enqueue(ScheduledTask {
        id: "b".into(),
        payload: (),
    });
    assert!(scheduler.acquire_next().is_some());
    assert!(scheduler.acquire_next().is_none());
    scheduler.release();
    assert!(scheduler.acquire_next().is_some());
}

#[test]
fn empty_queue_does_not_leak_active() {
    let mut scheduler: Scheduler<()> = Scheduler::new(ResourcePolicy::new(3));
    assert!(scheduler.acquire_next().is_none());
    assert_eq!(scheduler.active(), 0);
}
