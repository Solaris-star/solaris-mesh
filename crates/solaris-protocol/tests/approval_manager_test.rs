use rstest::rstest;
use solaris_protocol::commands::ApprovalScope;
use solaris_protocol::{ApprovalResolution, ToolApprovalManager, ToolApprovalResult};

#[rstest]
#[case(ApprovalScope::Once, "ExecCommand")]
#[case(ApprovalScope::Always, "Edit")]
#[tokio::test]
async fn approve_resolves_request_without_mutating_legacy_allow_list(
    #[case] scope: ApprovalScope,
    #[case] approval_key: &str,
) {
    let manager = ToolApprovalManager::new();
    let rx = manager.request_approval("call-1", approval_key);

    assert_eq!(manager.approve("call-1", scope), ApprovalResolution::Applied);
    assert_eq!(manager.approve("call-1", scope), ApprovalResolution::NotFound);

    let result = rx.await.expect("approval result should arrive");
    assert!(matches!(result, ToolApprovalResult::Approved { scope: returned } if returned == scope));
    assert!(!manager.is_auto_approved(approval_key));
}

#[test]
fn dropping_receiver_removes_only_its_pending_generation() {
    let manager = ToolApprovalManager::new();
    let first = manager.request_approval("call-1", "Read");
    let second = manager.request_approval("call-1", "Read");
    drop(first);
    assert_eq!(manager.pending_count(), 1);

    drop(second);
    assert_eq!(manager.pending_count(), 0);
    assert_eq!(
        manager.approve("call-1", ApprovalScope::Once),
        ApprovalResolution::NotFound
    );
}

#[tokio::test]
async fn resolve_preserves_denial_reason() {
    let manager = ToolApprovalManager::new();
    let rx = manager.request_approval("call-2", "ExecCommand");

    assert_eq!(
        manager.resolve(
            "call-2",
            ToolApprovalResult::Denied {
                reason: "policy violation".to_string(),
            },
        ),
        ApprovalResolution::Applied
    );
    assert_eq!(
        manager.resolve("call-2", ToolApprovalResult::Denied { reason: String::new() }),
        ApprovalResolution::NotFound
    );

    let result = rx.await.expect("denial result should arrive");
    assert!(matches!(
        result,
        ToolApprovalResult::Denied { reason } if reason == "policy violation"
    ));
    assert!(!manager.is_auto_approved("ExecCommand"));
}
