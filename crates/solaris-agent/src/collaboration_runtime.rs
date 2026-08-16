//! Collaboration runtime entry point for Solaris Mesh orchestration.

use crate::scheduler::{ScheduledTask, Scheduler};

pub struct CollaborationRuntime<T> {
    scheduler: Scheduler<T>,
}

impl<T> CollaborationRuntime<T> {
    pub fn new(scheduler: Scheduler<T>) -> Self {
        Self { scheduler }
    }

    pub fn enqueue(&mut self, task: ScheduledTask<T>) {
        self.scheduler.enqueue(task);
    }

    pub fn acquire_next(&mut self) -> Option<ScheduledTask<T>> {
        self.scheduler.acquire_next()
    }

    pub fn release(&mut self) {
        self.scheduler.release();
    }

    pub fn queued_len(&self) -> usize {
        self.scheduler.queued_len()
    }

    pub fn active(&self) -> usize {
        self.scheduler.active()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resource_policy::ResourcePolicy;

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
        assert_eq!(runtime.active(), 1);

        runtime.release();
        assert_eq!(runtime.active(), 0);
    }
}
