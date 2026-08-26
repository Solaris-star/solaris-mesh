use super::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_mode_does_not_bypass_any_capability() {
        let mgr = ToolApprovalManager::new();
        assert!(!mgr.is_auto_approved("Read"));
        assert!(!mgr.is_auto_approved("Write"));
        assert!(!mgr.is_auto_approved("ExecCommand"));
        assert_eq!(mgr.current_mode(), "auto");
    }

    #[test]
    fn plan_mode_does_not_bypass_approval_keys() {
        let mgr = ToolApprovalManager::new();
        mgr.set_mode(SessionMode::Plan);
        assert!(!mgr.is_auto_approved("Read"));
        assert!(!mgr.is_auto_approved("Write"));
        assert!(!mgr.is_auto_approved("ExecCommand"));
        assert_eq!(mgr.current_mode(), "plan");
    }

    #[test]
    fn bypass_mode_skips_interactive_approval_for_all_keys() {
        let mgr = ToolApprovalManager::new();
        mgr.set_mode(SessionMode::Bypass);
        assert!(mgr.is_auto_approved("Read"));
        assert!(mgr.is_auto_approved("Write"));
        assert!(mgr.is_auto_approved("ExecCommand"));
        assert!(mgr.is_auto_approved("mcp:tool"));
        assert_eq!(mgr.current_mode(), "bypass");
    }

    #[test]
    fn switching_mode_changes_only_interactive_bypass_behavior() {
        let mgr = ToolApprovalManager::new();
        assert!(!mgr.is_auto_approved("Edit"));
        mgr.set_mode(SessionMode::Plan);
        assert!(!mgr.is_auto_approved("Edit"));
        mgr.set_mode(SessionMode::Bypass);
        assert!(mgr.is_auto_approved("Edit"));
        mgr.set_mode(SessionMode::Auto);
        assert!(!mgr.is_auto_approved("Edit"));
    }

    #[test]
    fn user_always_approval_persists_across_mode_changes() {
        let mgr = ToolApprovalManager::new();
        mgr.add_auto_approve("ExecCommand");
        assert!(mgr.is_auto_approved("ExecCommand"));
        mgr.set_mode(SessionMode::Plan);
        assert!(mgr.is_auto_approved("ExecCommand"));
        assert!(!mgr.is_auto_approved("Read"));
        mgr.set_mode(SessionMode::Auto);
        assert!(mgr.is_auto_approved("ExecCommand"));
    }
}
