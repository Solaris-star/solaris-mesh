use std::collections::HashSet;
use std::sync::Arc;

use solaris_protocol::events::ToolCategory;
use solaris_tools::tool_search::ToolSearchTool;

use super::AgentEngine;

pub(super) type PlanModeDisableHandler = Arc<dyn Fn() + Send + Sync>;

impl AgentEngine {
    pub(crate) fn set_plan_mode_disable_handler(&mut self, handler: PlanModeDisableHandler) {
        self.plan_mode_disable_handler = Some(handler);
    }

    pub(super) fn disable_mcp_for_plan(&mut self) {
        if let Some(handler) = &self.plan_mode_disable_handler {
            handler();
        }
        self.tools.remove_category(ToolCategory::Mcp);
        self.tools.remove_names(&HashSet::from(["ToolSearch".to_owned()]));
        let snapshot = self.tools.to_tool_defs();
        if let Err(error) = self.tools.register_unique(Box::new(ToolSearchTool::new(snapshot))) {
            tracing::warn!(target: "solaris_agent", %error, "failed to refresh ToolSearch for plan mode");
        }
    }
}
