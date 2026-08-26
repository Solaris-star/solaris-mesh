use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};

use solaris_agent::bootstrap::{allow_mcp_connection_for, revoke_mcp_connection_for};
use solaris_agent::permission_engine::PermissionContext;
use solaris_config::config::{McpServerConfig, TransportType};
use solaris_mcp::identity::McpIdentityKey;
use solaris_protocol::commands::{DeliveryAcknowledgement, ProtocolCommand};
use solaris_protocol::events::ProtocolEvent;
use solaris_protocol::writer::{DeliveryAckOutcome, ProtocolEmitter};
use solaris_types::permission::{PermissionCeiling, PermissionMode, ProcessNetworkConfig};

use super::{
    consume_delivery_acknowledgement, ensure_dynamic_mcp_allowed, ensure_mcp_name_available, to_mcp_server_config,
};

#[derive(Default)]
struct AckRecorder {
    acknowledgements: AtomicUsize,
}

impl ProtocolEmitter for AckRecorder {
    fn emit(&self, _event: &ProtocolEvent) -> io::Result<()> {
        Ok(())
    }

    fn acknowledge_delivery(&self, _acknowledgement: &DeliveryAcknowledgement) -> io::Result<DeliveryAckOutcome> {
        self.acknowledgements.fetch_add(1, Ordering::SeqCst);
        Ok(DeliveryAckOutcome::Acknowledged)
    }
}

#[test]
fn delivery_acknowledgement_is_consumed_without_ending_the_pre_message_phase() {
    let writer = AckRecorder::default();
    let command = ProtocolCommand::AcknowledgeDelivery(DeliveryAcknowledgement {
        session_id: "session".to_owned(),
        run_epoch: 2,
        delivery_id: "delivery".to_owned(),
        digest: format!("sha256:{}", "a".repeat(64)),
    });

    assert!(consume_delivery_acknowledgement(&writer, command).is_none());
    assert_eq!(writer.acknowledgements.load(Ordering::SeqCst), 1);
    assert!(matches!(
        consume_delivery_acknowledgement(&writer, ProtocolCommand::HostContextReady),
        Some(ProtocolCommand::HostContextReady)
    ));
}

#[test]
fn dynamic_mcp_defaults_to_deferred_schema_loading() {
    let config = to_mcp_server_config(
        "streamable-http",
        None,
        None,
        None,
        Some("https://mcp.example.test/rpc".into()),
        None,
        Default::default(),
    )
    .unwrap();

    assert_eq!(config.deferred, None);
}

#[test]
fn dynamic_stdio_mcp_preserves_approved_network_destinations() {
    let config = to_mcp_server_config(
        "stdio",
        Some("node".into()),
        None,
        None,
        None,
        None,
        ProcessNetworkConfig {
            network_domains: vec!["api.example.test:443".into()],
        },
    )
    .unwrap();

    assert_eq!(config.network.network_domains, ["api.example.test:443"]);
}

#[test]
fn plan_rejects_dynamic_mcp_before_configuration_processing() {
    let error = ensure_dynamic_mcp_allowed(PermissionMode::Plan).unwrap_err();

    assert!(error.contains("plan mode"));
    assert!(ensure_dynamic_mcp_allowed(PermissionMode::Auto).is_ok());
    assert!(ensure_dynamic_mcp_allowed(PermissionMode::Bypass).is_ok());
}

#[test]
fn duplicate_mcp_name_is_rejected_before_dynamic_grant_changes() {
    let permissions = PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted());
    let key = McpIdentityKey::new("test-key", b"0123456789abcdef0123456789abcdef".to_vec()).unwrap();
    let config = McpServerConfig {
        transport: TransportType::StreamableHttp,
        command: None,
        args: None,
        env: None,
        url: Some("https://static.example.test/mcp".into()),
        headers: None,
        network: Default::default(),
        deferred: Some(false),
        startup_timeout_ms: None,
    };
    allow_mcp_connection_for(&permissions, "config:mcp:duplicate", "duplicate", &config, &key);

    let error = ensure_mcp_name_available(&permissions, "duplicate").unwrap_err();

    assert!(error.contains("already registered"));
    assert!(permissions.has_configured_effect_source("config:mcp:duplicate"));
    assert!(!permissions.has_configured_effect_source("host:mcp:duplicate"));

    revoke_mcp_connection_for(&permissions, "config:mcp:duplicate");
    allow_mcp_connection_for(&permissions, "host:mcp:duplicate", "duplicate", &config, &key);
    assert!(ensure_mcp_name_available(&permissions, "duplicate").is_err());
    assert!(permissions.has_configured_effect_source("host:mcp:duplicate"));
}
