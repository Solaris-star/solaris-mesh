use solaris_types::permission::PermissionMode;

use super::PermissionContext;

#[test]
fn auto_approve_does_not_grant_bypass_permission() {
    assert_eq!(PermissionContext::from_auto_approve(true).mode(), PermissionMode::Auto);
    assert_eq!(PermissionContext::from_auto_approve(false).mode(), PermissionMode::Auto);
}
