use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;
use solaris_config::config::{McpServerConfig, TransportType};
use solaris_mcp::identity::McpIdentityKey;
use solaris_mcp::manager::McpManager;
use solaris_mcp::protocol::{JsonRpcRequest, JsonRpcResponse, McpToolAnnotations, McpToolDef};
use solaris_mcp::transport::{McpError, McpTransport};
use solaris_memory::service::MemoryService;
use solaris_tools::registry::ToolRegistry;

use super::{ChildCapabilityBlueprint, register_tool_search_for_deferred_tools};
use crate::memory_runtime::MemoryRuntime;

struct UnusedTransport;

#[async_trait]
impl McpTransport for UnusedTransport {
    async fn request(&self, _req: &JsonRpcRequest) -> Result<JsonRpcResponse, McpError> {
        panic!("child registry construction must not call the MCP transport")
    }

    async fn notify(&self, _req: &JsonRpcRequest) -> Result<(), McpError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), McpError> {
        Ok(())
    }
}

fn identity_key() -> Arc<McpIdentityKey> {
    Arc::new(McpIdentityKey::new("test", b"0123456789abcdef0123456789abcdef".to_vec()).unwrap())
}

#[test]
fn taking_mcp_sources_prevents_future_children_from_inheriting_them() {
    let first = Arc::new(McpManager::new());
    let second = Arc::new(McpManager::new());
    let blueprint = ChildCapabilityBlueprint::new(
        Arc::new(Vec::new()),
        Vec::new(),
        Vec::new(),
        true,
        Some(Arc::clone(&first)),
        HashMap::new(),
        Some(identity_key()),
        Vec::new(),
    );
    blueprint.add_mcp_source(Arc::clone(&second), HashMap::new());

    let removed = blueprint.take_mcp_sources();

    assert_eq!(removed.len(), 2);
    assert!(removed.iter().any(|manager| Arc::ptr_eq(manager, &first)));
    assert!(removed.iter().any(|manager| Arc::ptr_eq(manager, &second)));
    assert_eq!(blueprint.mcp_source_count(), 0);
}

#[tokio::test]
async fn deferred_child_mcp_proxy_registers_tool_search_until_plan_removes_sources() {
    let manager = Arc::new(McpManager::new_for_test_with_tools(
        "deferred-server",
        false,
        Box::new(UnusedTransport),
        vec![McpToolDef {
            name: "lookup".into(),
            description: Some("Deferred fixture schema".into()),
            input_schema: json!({
                "type": "object",
                "properties": { "query": { "type": "string" } },
                "required": ["query"]
            }),
            annotations: McpToolAnnotations::default(),
        }],
    ));
    let config = McpServerConfig {
        transport: TransportType::StreamableHttp,
        command: None,
        args: None,
        env: None,
        url: Some("https://mcp.example.test/rpc".into()),
        headers: None,
        network: Default::default(),
        deferred: None,
        startup_timeout_ms: None,
    };
    let blueprint = ChildCapabilityBlueprint::new(
        Arc::new(Vec::new()),
        Vec::new(),
        Vec::new(),
        true,
        Some(manager),
        HashMap::from([("deferred-server".into(), config)]),
        Some(identity_key()),
        Vec::new(),
    );
    let mut registry = ToolRegistry::new();

    blueprint.register_mcp_tools(&mut registry);
    register_tool_search_for_deferred_tools(&mut registry);

    let deferred_name = registry
        .to_tool_defs()
        .into_iter()
        .find(|tool| tool.deferred)
        .expect("child must inherit the deferred MCP proxy")
        .name;
    assert!(deferred_name.starts_with("mcp_v3_"));
    let search_result = registry
        .get("ToolSearch")
        .expect("deferred child proxy requires ToolSearch")
        .execute(json!({"query": "Deferred fixture schema"}))
        .await;
    assert!(!search_result.is_error);
    assert!(search_result.content.contains(&deferred_name));
    assert!(search_result.content.contains("required"));

    blueprint.take_mcp_sources();
    let mut plan_registry = ToolRegistry::new();
    blueprint.register_mcp_tools(&mut plan_registry);
    register_tool_search_for_deferred_tools(&mut plan_registry);
    assert!(plan_registry.tool_names().is_empty());
}

#[tokio::test]
async fn child_memory_capability_can_only_submit_proposals() {
    let temp = tempfile::tempdir().unwrap();
    let service = Arc::new(MemoryService::open(temp.path().join("memory.sqlite3")).unwrap());
    let runtime = Arc::new(MemoryRuntime::from_service(
        Arc::clone(&service),
        temp.path().to_path_buf(),
        false,
    ));
    let blueprint = ChildCapabilityBlueprint::new(
        Arc::new(Vec::new()),
        Vec::new(),
        Vec::new(),
        true,
        None,
        HashMap::new(),
        None,
        Vec::new(),
    )
    .with_memory_runtime(Some(runtime));
    let mut registry = ToolRegistry::new();
    blueprint.register_memory_tool(&mut registry);

    let result = registry
        .get("Memory")
        .unwrap()
        .execute(json!({
            "operation": "create",
            "scope": "MEMORY",
            "type": "project",
            "name": "child-note",
            "content": "review this later"
        }))
        .await;

    assert!(!result.is_error);
    assert!(service.list().unwrap().is_empty());
    assert_eq!(service.pending_proposals().unwrap().len(), 1);
}
