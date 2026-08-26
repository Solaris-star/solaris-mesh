use serde::{Deserialize, Serialize};

/// Raw memory settings as written in config files.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct MemoryConfigFile {
    pub enabled: Option<bool>,
    pub review: Option<bool>,
}

/// Resolved memory settings used by the runtime.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MemoryConfig {
    pub enabled: bool,
    pub review: bool,
}

impl MemoryConfigFile {
    pub(crate) fn merge(global: Self, project: Self) -> Self {
        Self {
            enabled: project.enabled.or(global.enabled),
            review: project.review.or(global.review),
        }
    }

    pub(crate) fn resolve(self) -> MemoryConfig {
        MemoryConfig {
            enabled: self.enabled.unwrap_or(false),
            review: self.review.unwrap_or(false),
        }
    }
}

#[cfg(test)]
#[path = "memory_config_test.rs"]
mod memory_config_test;
