use super::*;
use solaris_types::sandbox::{SandboxBackend, SandboxEnforcement, SandboxReason, SandboxReport};
use solaris_types::tool::ToolResultMetadata;
use solaris_types::tool::ToolResultStatus;

#[test]
fn child_tool_result_payload_keeps_aborted_and_unknown_statuses() {
    let aborted = tool_result_payload("call-1", "ExecCommand", ToolResultStatus::Aborted, "cancelled");
    let unknown = tool_result_payload("call-2", "Write", ToolResultStatus::OutcomeUnknown, "reconcile");

    assert_eq!(aborted["status"], "aborted");
    assert_eq!(aborted["is_error"], true);
    assert_eq!(unknown["status"], "outcome_unknown");
    assert_eq!(unknown["is_error"], true);
}

#[test]
fn child_tool_result_payload_keeps_typed_sandbox_metadata() {
    let metadata = ToolResultMetadata::sandbox_report(SandboxReport::new(
        SandboxEnforcement::Unavailable,
        SandboxBackend::WindowsAppContainer,
        SandboxReason::NetworkProxyUnavailable,
    ));

    let payload = tool_result_payload_with_metadata(
        "call-3",
        "ExecCommand",
        ToolResultStatus::Denied,
        "denied",
        Some(&metadata),
    );

    assert_eq!(payload["status"], "denied");
    assert_eq!(
        payload["metadata"]["sandbox_report"]["backend"],
        "windows_app_container"
    );
    assert_eq!(payload["metadata"]["sandbox_report"]["enforcement"], "unavailable");
    assert_eq!(
        payload["metadata"]["sandbox_report"]["reason"],
        "network_proxy_unavailable"
    );
}
