use serde::{Deserialize, Serialize};

pub const MIN_ACTIVE_AGENTS: usize = 1;
pub const MAX_ACTIVE_AGENTS: usize = 64;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ResourceBudget {
    pub max_active_agents: Option<usize>,
    pub max_concurrent_effects: Option<usize>,
    pub max_spawn_depth: Option<usize>,
    pub max_total_descendants_per_run: Option<usize>,
    pub max_turns: Option<usize>,
    pub max_tokens: Option<u64>,
    pub max_wall_time_ms: Option<u64>,
    pub max_cost: Option<f64>,
    pub max_process_output_bytes: Option<usize>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ResourceLease {
    pub active_agents: usize,
    pub concurrent_effects: usize,
    pub token_budget: Option<u64>,
    pub wall_time_ms: Option<u64>,
    pub cost_budget: Option<f64>,
}
