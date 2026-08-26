use super::*;

#[test]
fn network_proxy_unavailable_report_has_a_stable_wire_shape() {
    let report = SandboxReport::new(
        SandboxEnforcement::Unavailable,
        SandboxBackend::WindowsAppContainer,
        SandboxReason::NetworkProxyUnavailable,
    );

    let value = serde_json::to_value(report).unwrap();

    assert_eq!(
        value,
        serde_json::json!({
            "enforcement": "unavailable",
            "backend": "windows_app_container",
            "reason": "network_proxy_unavailable"
        })
    );
    assert_eq!(serde_json::from_value::<SandboxReport>(value).unwrap(), report);
}
