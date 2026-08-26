//! Dynamic resource policy for Mesh agent scheduling.

#[derive(Clone, Debug)]
pub struct ResourcePolicy {
    max_active: usize,
    active: usize,
}

impl ResourcePolicy {
    pub fn new(max_active: usize) -> Self {
        Self {
            max_active: max_active.max(1),
            active: 0,
        }
    }

    pub fn from_system(configured_max: Option<usize>) -> Self {
        let system_capacity = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .max(1);
        // Agent work is primarily remote-I/O bound; CPU parallelism is only a
        // default hint, never a hard ceiling on an explicit configured budget.
        let max_active = configured_max.unwrap_or(system_capacity).max(1);
        Self::new(max_active)
    }

    pub fn max_active(&self) -> usize {
        self.max_active
    }

    pub fn active(&self) -> usize {
        self.active
    }

    pub fn available_slots(&self) -> usize {
        self.max_active.saturating_sub(self.active)
    }

    pub fn try_acquire(&mut self) -> bool {
        if self.available_slots() > 0 {
            self.active += 1;
            true
        } else {
            false
        }
    }

    pub fn release(&mut self) {
        self.active = self.active.saturating_sub(1);
    }
}

#[cfg(test)]
#[path = "resource_policy_test.rs"]
mod resource_policy_test;
