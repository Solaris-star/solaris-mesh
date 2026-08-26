use super::ResourceUsage;

impl Default for ResourceUsage {
    fn default() -> Self {
        Self {
            active_agents: 0,
            concurrent_effects: 0,
            total_descendants: 0,
            turns: 0,
            tokens: 0,
            uncached_input_tokens: 0,
            input_tokens: 0,
            output_tokens: 0,
            cache_creation_tokens: 0,
            cache_read_tokens: 0,
            tool_calls: 0,
            useful_tool_calls: 0,
            duplicate_tool_calls: 0,
            seen_tool_call_fingerprints: Vec::new(),
            useful_call_rate: None,
            duplicate_call_rate: None,
            cost: 0.0,
            cost_known: true,
        }
    }
}

impl ResourceUsage {
    pub(super) fn refresh_useful_call_rate(&mut self) {
        self.useful_call_rate = (self.tool_calls != 0).then(|| self.useful_tool_calls as f64 / self.tool_calls as f64);
    }

    pub(super) fn refresh_duplicate_call_rate(&mut self) {
        self.duplicate_call_rate =
            (self.tool_calls != 0).then(|| self.duplicate_tool_calls as f64 / self.tool_calls as f64);
    }
}
