//! FIFO task scheduler for Solaris Mesh agent execution.

use std::collections::VecDeque;

use crate::resource_policy::ResourcePolicy;

#[derive(Debug)]
pub struct ScheduledTask<T> {
    pub id: String,
    pub payload: T,
}

pub struct Scheduler<T> {
    queue: VecDeque<ScheduledTask<T>>,
    policy: ResourcePolicy,
}

impl<T> Scheduler<T> {
    pub fn new(policy: ResourcePolicy) -> Self {
        Self {
            queue: VecDeque::new(),
            policy,
        }
    }

    pub fn enqueue(&mut self, task: ScheduledTask<T>) {
        self.queue.push_back(task);
    }

    pub fn queued_len(&self) -> usize {
        self.queue.len()
    }

    pub fn active(&self) -> usize {
        self.policy.active()
    }

    pub fn available_slots(&self) -> usize {
        self.policy.available_slots()
    }

    pub fn acquire_next(&mut self) -> Option<ScheduledTask<T>> {
        if self.queue.is_empty() || !self.policy.try_acquire() {
            return None;
        }
        self.queue.pop_front()
    }

    pub fn release(&mut self) {
        self.policy.release();
    }
}

#[cfg(test)]
mod tests {
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
}
