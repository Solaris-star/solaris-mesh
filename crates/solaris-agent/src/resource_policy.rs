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
        let max_active = configured_max.unwrap_or(system_capacity).min(system_capacity).max(1);
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
mod tests {
    use super::*;

    #[test]
    fn configured_limit_is_respected() {
        let p = ResourcePolicy::from_system(Some(2));
        assert!(p.max_active() <= 2);
        assert!(p.max_active() >= 1);
    }

    #[test]
    fn tracks_load_and_release() {
        let mut p = ResourcePolicy::new(3);
        assert_eq!(p.available_slots(), 3);
        assert!(p.try_acquire());
        assert_eq!(p.active(), 1);
        p.release();
        assert_eq!(p.active(), 0);
    }

    #[test]
    fn new_policy_is_not_fixed_at_five() {
        let mut p = ResourcePolicy::new(10);
        for _ in 0..10 {
            assert!(p.try_acquire());
        }
        assert!(!p.try_acquire());
    }
}
