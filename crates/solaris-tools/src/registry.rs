use solaris_protocol::events::ToolCategory;
use solaris_types::tool::ToolDef;

use crate::Tool;

pub struct ToolRegistry {
    tools: Vec<Box<dyn Tool>>,
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self::new()
    }
}
impl ToolRegistry {
    pub fn new() -> Self {
        Self { tools: Vec::new() }
    }

    pub fn register(&mut self, tool: Box<dyn Tool>) {
        self.tools.push(tool);
    }

    /// Register a tool only when its model-visible name is unused.
    pub fn register_unique(&mut self, tool: Box<dyn Tool>) -> Result<(), String> {
        let name = tool.name().to_owned();
        if self.get(&name).is_some() {
            return Err(format!("tool name '{name}' is already registered"));
        }
        self.tools.push(tool);
        Ok(())
    }

    /// Find a tool by name
    pub fn get(&self, name: &str) -> Option<&dyn Tool> {
        self.tools.iter().find(|t| t.name() == name).map(|t| t.as_ref())
    }

    /// Get all registered tool names
    pub fn tool_names(&self) -> Vec<String> {
        self.tools.iter().map(|t| t.name().to_string()).collect()
    }

    /// Retain only capabilities explicitly selected by an inherited/role scope.
    pub fn retain_names(&mut self, allowed: &std::collections::HashSet<String>) {
        self.tools.retain(|tool| allowed.contains(tool.name()));
    }

    /// Remove tools by exact name. Returns the number of removed registrations.
    pub fn remove_names(&mut self, names: &std::collections::HashSet<String>) -> usize {
        let before = self.tools.len();
        self.tools.retain(|tool| !names.contains(tool.name()));
        before.saturating_sub(self.tools.len())
    }

    /// Remove every tool in one protocol category.
    pub fn remove_category(&mut self, category: ToolCategory) -> usize {
        let before = self.tools.len();
        self.tools.retain(|tool| tool.category() != category);
        before.saturating_sub(self.tools.len())
    }

    /// Notify the matching tool that one of its model-visible results was compacted.
    pub fn notify_result_compacted(&self, name: &str, input: &serde_json::Value) {
        if let Some(tool) = self.get(name) {
            tool.on_result_compacted(input);
        }
    }

    /// Notify all registered tools that detailed conversation history was compacted.
    pub fn notify_history_compacted(&self) {
        for tool in &self.tools {
            tool.on_history_compacted();
        }
    }

    /// Generate API tool definitions for all registered tools
    pub fn to_tool_defs(&self) -> Vec<ToolDef> {
        self.tools
            .iter()
            .map(|t| ToolDef {
                name: t.name().to_string(),
                description: t.description().to_string(),
                input_schema: t.input_schema(),
                deferred: t.is_deferred(),
            })
            .collect()
    }

    /// Generate API tool definitions for tools matching a predicate.
    ///
    /// Used by plan mode to restrict the tool set sent to the LLM.
    pub fn to_tool_defs_filtered<F>(&self, filter: F) -> Vec<ToolDef>
    where
        F: Fn(&dyn Tool) -> bool,
    {
        self.tools
            .iter()
            .filter(|t| filter(t.as_ref()))
            .map(|t| ToolDef {
                name: t.name().to_string(),
                description: t.description().to_string(),
                input_schema: t.input_schema(),
                deferred: t.is_deferred(),
            })
            .collect()
    }
}

#[cfg(test)]
#[path = "registry_test.rs"]
mod registry_test;
