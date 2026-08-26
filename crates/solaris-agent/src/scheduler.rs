//! FIFO task scheduler for Solaris Mesh agent execution.

use std::collections::VecDeque;
use std::sync::Arc;

use crate::resource_manager::{AgentResourcePermit, ResourceManager};
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

/// Resource-budget-aware scheduler used by multi-agent runtime paths.
pub struct MeshScheduler<T> {
    queue: VecDeque<ScheduledTask<T>>,
}

pub struct LeasedTask<T> {
    pub task: ScheduledTask<T>,
    permit: AgentResourcePermit,
}

impl<T> LeasedTask<T> {
    pub fn into_parts(self) -> (ScheduledTask<T>, AgentResourcePermit) {
        (self.task, self.permit)
    }
}

impl<T> Default for MeshScheduler<T> {
    fn default() -> Self {
        Self { queue: VecDeque::new() }
    }
}

impl<T> MeshScheduler<T> {
    pub fn enqueue(&mut self, task: ScheduledTask<T>) {
        self.queue.push_back(task);
    }

    pub fn queued_len(&self) -> usize {
        self.queue.len()
    }

    pub fn acquire_next(&mut self, resources: &Arc<ResourceManager>, spawn_depth: usize) -> Option<LeasedTask<T>> {
        if self.queue.is_empty() {
            return None;
        }
        let permit = resources.try_acquire_agent(spawn_depth)?;
        let task = self.queue.pop_front()?;
        Some(LeasedTask { task, permit })
    }
}

#[cfg(test)]
#[path = "scheduler_test.rs"]
mod scheduler_test;
