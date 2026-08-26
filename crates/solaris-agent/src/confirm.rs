use std::collections::HashSet;
use std::io::{self, BufRead, Write};

pub struct ToolConfirmer {
    auto_approve: bool,
    interactive: bool,
    allow_list: HashSet<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfirmResult {
    ApprovedOnce,
    ApprovedAlways,
    Denied,
    Quit,
}

impl ToolConfirmer {
    pub fn new(auto_approve: bool, allow_list: Vec<String>) -> Self {
        Self {
            auto_approve,
            interactive: true,
            allow_list: allow_list.into_iter().collect(),
        }
    }

    /// Returns whether auto-approve is enabled
    pub fn is_auto_approve(&self) -> bool {
        self.auto_approve
    }

    /// Configure whether undecided approvals may prompt stdin.
    /// Background/sub-agent runtimes set this to false so they fail closed
    /// instead of blocking on an unavailable terminal.
    pub fn set_interactive(&mut self, interactive: bool) {
        self.interactive = interactive;
    }

    pub fn is_interactive(&self) -> bool {
        self.interactive
    }
    /// Add a tool name to the allow list at runtime.
    /// Used by skill context modifiers to grant auto-approval for specified tools.
    pub fn add_to_allow_list(&mut self, name: &str) {
        self.allow_list.insert(name.to_string());
    }

    pub(crate) fn replace_allow_list(&mut self, allow_list: &[String]) {
        self.allow_list = allow_list.iter().cloned().collect();
    }

    /// Check if the tool needs confirmation. Returns the user's decision.
    pub fn check(&mut self, tool_name: &str, tool_input_display: &str) -> ConfirmResult {
        if self.auto_approve || self.allow_list.contains(tool_name) {
            return ConfirmResult::ApprovedAlways;
        }

        if !self.interactive {
            return ConfirmResult::Denied;
        }

        eprint!(
            "\n[tool] {}({})\nAllow? [y]es / [n]o / [a]lways / [q]uit > ",
            tool_name, tool_input_display
        );
        io::stderr().flush().unwrap();

        let mut input = String::new();
        if io::stdin().lock().read_line(&mut input).is_err() {
            return ConfirmResult::Denied;
        }

        match input.trim().to_lowercase().as_str() {
            "y" | "yes" | "" => ConfirmResult::ApprovedOnce,
            "a" | "always" => ConfirmResult::ApprovedAlways,
            "q" | "quit" => ConfirmResult::Quit,
            _ => ConfirmResult::Denied,
        }
    }
}

#[cfg(test)]
#[path = "confirm_test.rs"]
mod confirm_test;
