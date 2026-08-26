use super::approval::host_safe_tool_info;
use super::*;
use solaris_protocol::events::ToolCategory;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_ledger::RuntimeLedger;
    use serde_json::json;
    use solaris_config::hooks::{HookDef, HooksConfig};
    use solaris_types::permission::ExecutionBoundary;

    include!("orchestration_effect_security_test.rs");
    include!("orchestration_capability_test.rs");
    include!("orchestration_tool_execution_test.rs");
    include!("orchestration_auto_exec_test.rs");
    include!("orchestration_hook_process_policy_test.rs");
    include!("orchestration_status_test.rs");
    include!("orchestration_hook_recovery_test.rs");
}
