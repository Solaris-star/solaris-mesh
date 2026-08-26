use std::collections::HashMap;
#[cfg(test)]
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(test)]
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use futures::stream::{FuturesUnordered, StreamExt};
use serde_json::json;
use solaris_process::{ExecutableIdentity, PinnedCommand};
#[cfg(test)]
use tokio::sync::Semaphore;
use tokio::sync::{Mutex, RwLock, watch};
use tokio::time::Instant;

use super::config::{McpServerConfig, TransportType};
use super::protocol::{
    ClientCapabilities, ClientInfo, InitializeParams, InitializeResult, JsonRpcRequest, McpResource, McpToolDef,
    McpToolResult, ResourcesListResult, ResourcesReadResult, ToolsListResult,
};
use super::transport::sse::SseTransport;
use super::transport::stdio::StdioTransport;
use super::transport::streamable_http::StreamableHttpTransport;
use super::transport::{McpError, McpTransport, validate_response_id};

const DEFAULT_STARTUP_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_SHUTDOWN_TIMEOUT_MS: u64 = 5_000;
const REQUEST_ID_EXHAUSTED_MESSAGE: &str = "MCP request id space is exhausted";
const MANAGER_DISABLED_MESSAGE: &str = "MCP manager is disabled";

/// A connected MCP server with its discovered tools and capabilities
struct McpServer {
    #[allow(dead_code)]
    name: String,
    transport: Box<dyn McpTransport>,
    tools: Vec<McpToolDef>,
    /// Whether the server declared resources capability in its initialize response
    supports_resources: bool,
    executable_identity: Option<ExecutableIdentity>,
    request_gate: Mutex<()>,
}

/// Manages connections to multiple MCP servers
pub struct McpManager {
    servers: HashMap<String, McpServer>,
    disabled: AtomicBool,
    disable_signal: watch::Sender<bool>,
    shutdown_complete: Mutex<bool>,
    request_lifecycle: RwLock<()>,
    #[cfg(test)]
    before_transport_pause: StdMutex<Option<Arc<RequestPause>>>,
    /// Monotonically increasing request ID counter for all JSON-RPC calls
    next_id: AtomicU64,
}

#[cfg(test)]
struct RequestPause {
    reached: Semaphore,
    resume: Semaphore,
}

/// Initialized server awaiting insertion into its owning manager.
pub struct PendingMcpServer {
    name: String,
    server: McpServer,
    tool_names: Vec<String>,
}

/// Proof that an MCP connection entered the Host's permission and effect
/// pipeline before a transport is opened. The manager owns the guard for the
/// whole connection attempt, so cancellation can record a terminal outcome.
pub trait McpConnectionGuard: Send {
    fn complete(&mut self, result: &Result<Vec<String>, McpError>) -> Result<(), McpError>;
}

impl Default for McpManager {
    fn default() -> Self {
        Self::new()
    }
}

impl McpManager {
    pub fn new() -> Self {
        Self {
            servers: HashMap::new(),
            disabled: AtomicBool::new(false),
            disable_signal: watch::channel(false).0,
            shutdown_complete: Mutex::new(false),
            request_lifecycle: RwLock::new(()),
            #[cfg(test)]
            before_transport_pause: StdMutex::new(None),
            next_id: AtomicU64::new(10),
        }
    }

    #[cfg(test)]
    async fn connect_all_with_connector<F, Fut>(
        configs: &HashMap<String, McpServerConfig>,
        connector: F,
    ) -> Result<Self, McpError>
    where
        F: Fn(String, McpServerConfig) -> Fut,
        Fut: Future<Output = Result<McpServer, McpError>>,
    {
        let mut servers = HashMap::new();
        let mut pending = FuturesUnordered::new();

        for (name, config) in configs {
            let name = name.clone();
            let config = config.clone();
            let connect = connector(name.clone(), config.clone());
            pending.push(async move {
                let result = Self::with_startup_timeout(&name, &config, connect).await;
                (name, result)
            });
        }

        while let Some((name, result)) = pending.next().await {
            match result {
                Ok(server) => {
                    tracing::info!(target: "solaris_mcp", server = %name, tools = server.tools.len(), resources = server.supports_resources, "mcp server connected");
                    servers.insert(name, server);
                }
                Err(e) => {
                    // Non-fatal: continue with other servers
                    tracing::warn!(target: "solaris_mcp", server = %name, error = %e, "mcp server connection failed");
                }
            }
        }

        Ok(Self {
            servers,
            disabled: AtomicBool::new(false),
            disable_signal: watch::channel(false).0,
            shutdown_complete: Mutex::new(false),
            request_lifecycle: RwLock::new(()),
            #[cfg(test)]
            before_transport_pause: StdMutex::new(None),
            next_id: AtomicU64::new(10),
        })
    }

    fn startup_timeout(config: &McpServerConfig) -> Duration {
        Duration::from_millis(config.startup_timeout_ms.unwrap_or(DEFAULT_STARTUP_TIMEOUT_MS))
    }

    async fn with_startup_timeout<Fut>(
        name: &str,
        config: &McpServerConfig,
        connect: Fut,
    ) -> Result<McpServer, McpError>
    where
        Fut: Future<Output = Result<McpServer, McpError>>,
    {
        let timeout = Self::startup_timeout(config);
        match tokio::time::timeout(timeout, connect).await {
            Ok(result) => result,
            Err(_) => Err(McpError::Transport(format!(
                "MCP server '{name}' startup timed out after {}ms; set startup_timeout_ms to increase it",
                timeout.as_millis()
            ))),
        }
    }

    /// Connect a single server after an effect guard has been created.
    /// There is deliberately no context-free transport entry point.
    pub async fn connect_one_authorized<G>(
        &mut self,
        name: String,
        config: &McpServerConfig,
        guard: G,
    ) -> Result<Vec<String>, McpError>
    where
        G: McpConnectionGuard,
    {
        self.ensure_enabled()?;
        let pending = Self::connect_one_authorized_inner(name, config, None, guard).await?;
        Ok(self.commit_pending(pending))
    }

    /// Connect a stdio server with a command prepared by the effect boundary.
    pub async fn connect_one_authorized_with_stdio_command<G>(
        &mut self,
        name: String,
        config: &McpServerConfig,
        command: PinnedCommand,
        guard: G,
    ) -> Result<Vec<String>, McpError>
    where
        G: McpConnectionGuard,
    {
        self.ensure_enabled()?;
        let pending = Self::connect_one_authorized_inner(name, config, Some(command), guard).await?;
        Ok(self.commit_pending(pending))
    }

    pub async fn connect_pending_authorized<G>(
        name: String,
        config: &McpServerConfig,
        guard: G,
    ) -> Result<PendingMcpServer, McpError>
    where
        G: McpConnectionGuard,
    {
        Self::connect_one_authorized_inner(name, config, None, guard).await
    }

    pub async fn connect_pending_authorized_with_stdio_command<G>(
        name: String,
        config: &McpServerConfig,
        command: PinnedCommand,
        guard: G,
    ) -> Result<PendingMcpServer, McpError>
    where
        G: McpConnectionGuard,
    {
        Self::connect_one_authorized_inner(name, config, Some(command), guard).await
    }

    async fn connect_one_authorized_inner<G>(
        name: String,
        config: &McpServerConfig,
        prepared_stdio_command: Option<PinnedCommand>,
        mut guard: G,
    ) -> Result<PendingMcpServer, McpError>
    where
        G: McpConnectionGuard,
    {
        let pending = async {
            if prepared_stdio_command.is_some() && config.transport != TransportType::Stdio {
                return Err(McpError::InitFailed(
                    "a prepared stdio command requires stdio transport".into(),
                ));
            }
            let server = Self::with_startup_timeout(
                &name,
                config,
                Self::connect_server(&name, config, prepared_stdio_command),
            )
            .await?;
            let tool_names: Vec<String> = server.tools.iter().map(|tool| tool.name.clone()).collect();
            Ok((server, tool_names))
        }
        .await;
        match pending {
            Ok((server, tool_names)) => {
                let completion = Ok(tool_names.clone());
                if let Err(error) = guard.complete(&completion) {
                    let _ = server.transport.close().await;
                    return Err(error);
                }
                tracing::info!(target: "solaris_mcp", server = %name, tools = server.tools.len(), resources = server.supports_resources, "mcp server connected");
                Ok(PendingMcpServer {
                    name,
                    server,
                    tool_names,
                })
            }
            Err(error) => {
                let completion = Err(McpError::InitFailed("MCP connection failed".into()));
                guard.complete(&completion)?;
                Err(error)
            }
        }
    }

    pub fn commit_pending(&mut self, pending: PendingMcpServer) -> Vec<String> {
        let PendingMcpServer {
            name,
            server,
            tool_names,
        } = pending;
        self.servers.insert(name, server);
        tool_names
    }

    /// Connect to a single MCP server: create transport, initialize, discover tools
    async fn connect_server(
        name: &str,
        config: &McpServerConfig,
        prepared_stdio_command: Option<PinnedCommand>,
    ) -> Result<McpServer, McpError> {
        let empty_map = HashMap::new();

        // 1. Create transport
        let transport: Box<dyn McpTransport> = match config.transport {
            TransportType::Stdio => {
                config
                    .command
                    .as_deref()
                    .ok_or_else(|| McpError::InitFailed("stdio transport requires 'command'".into()))?;
                let args = config.args.as_deref().unwrap_or(&[]);
                let env = config.env.as_ref().unwrap_or(&empty_map);
                let command = prepared_stdio_command.ok_or_else(|| {
                    McpError::InitFailed("stdio transport requires a pinned executable command".into())
                })?;
                let executable_identity = command.executable_identity().clone();
                let transport = Box::new(StdioTransport::spawn(command, args, env).await?);
                return Self::initialize_server(name, transport, Some(executable_identity)).await;
            }
            TransportType::Sse => {
                let url = config
                    .url
                    .as_deref()
                    .ok_or_else(|| McpError::InitFailed("SSE transport requires 'url'".into()))?;
                let headers = config.headers.as_ref().unwrap_or(&empty_map);
                Box::new(SseTransport::connect(url, headers).await?)
            }
            TransportType::StreamableHttp => {
                let url = config
                    .url
                    .as_deref()
                    .ok_or_else(|| McpError::InitFailed("streamable-http transport requires 'url'".into()))?;
                let headers = config.headers.as_ref().unwrap_or(&empty_map);
                Box::new(StreamableHttpTransport::connect(url, headers).await?)
            }
        };

        Self::initialize_server(name, transport, None).await
    }

    async fn initialize_server(
        name: &str,
        transport: Box<dyn McpTransport>,
        executable_identity: Option<ExecutableIdentity>,
    ) -> Result<McpServer, McpError> {
        // Initialize handshake
        let init_params = InitializeParams {
            protocol_version: "2025-03-26".to_string(),
            capabilities: ClientCapabilities { tools: Some(json!({})) },
            client_info: ClientInfo {
                name: "solaris".to_string(),
                version: "0.3.0".to_string(),
            },
        };

        let init_req = JsonRpcRequest::new(
            1,
            "initialize",
            Some(
                serde_json::to_value(&init_params)
                    .map_err(|e| McpError::InitFailed(format!("Failed to serialize init params: {}", e)))?,
            ),
        );

        let init_response = transport.request(&init_req).await?;
        let init_result: InitializeResult = serde_json::from_value(
            init_response
                .result
                .ok_or_else(|| McpError::InitFailed("No result in initialize response".into()))?,
        )
        .map_err(|_| McpError::InitFailed("Failed to parse initialize response".into()))?;

        // Check whether server declared resources capability
        let supports_resources = init_result
            .capabilities
            .get("resources")
            .map(|v| !v.is_null())
            .unwrap_or(false);

        // 3. Send initialized notification
        let initialized_notification = JsonRpcRequest::notification("notifications/initialized", None);
        transport.notify(&initialized_notification).await?;

        // 4. List tools
        let list_req = JsonRpcRequest::new(2, "tools/list", None);
        let list_response = transport.request(&list_req).await?;
        let tools_result: ToolsListResult = serde_json::from_value(
            list_response
                .result
                .ok_or_else(|| McpError::InitFailed("No result in tools/list response".into()))?,
        )
        .map_err(|_| McpError::InitFailed("Failed to parse tools/list response".into()))?;

        Ok(McpServer {
            name: name.to_string(),
            transport,
            tools: tools_result.tools,
            supports_resources,
            executable_identity,
            request_gate: Mutex::new(()),
        })
    }

    /// Get all discovered tools with their server names
    pub fn all_tools(&self) -> Vec<(&str, &McpToolDef)> {
        if self.is_disabled() {
            return Vec::new();
        }
        let mut result = Vec::new();
        for (server_name, server) in &self.servers {
            for tool in &server.tools {
                result.push((server_name.as_str(), tool));
            }
        }
        result
    }

    /// Check if a tool name exists across any server
    pub fn has_tool_name(&self, name: &str) -> bool {
        self.servers.values().any(|s| s.tools.iter().any(|t| t.name == name))
    }

    /// Count how many servers have a tool with the given name
    pub fn tool_name_count(&self, name: &str) -> usize {
        self.servers
            .values()
            .filter(|s| s.tools.iter().any(|t| t.name == name))
            .count()
    }

    /// Execute a tool on a specific server
    pub async fn call_tool(
        &self,
        server_name: &str,
        tool_name: &str,
        arguments: serde_json::Value,
    ) -> Result<String, McpError> {
        let response = self
            .request_server(
                server_name,
                "tools/call",
                Some(json!({
                    "name": tool_name,
                    "arguments": arguments
                })),
            )
            .await?;

        let result_value = response
            .result
            .ok_or_else(|| McpError::Transport("No result in tool call response".into()))?;

        // Parse result and concatenate text content
        let tool_result: McpToolResult = serde_json::from_value(result_value)
            .map_err(|_| McpError::Transport("Failed to parse tool response".into()))?;

        let mut text_parts = Vec::new();
        for content in &tool_result.content {
            match content {
                super::protocol::McpContent::Text { text } => text_parts.push(text.clone()),
                super::protocol::McpContent::Image { mime_type, .. } => {
                    text_parts.push(format!("[image: {}]", mime_type));
                }
                super::protocol::McpContent::Resource { .. } => {
                    text_parts.push("[resource]".to_string());
                }
            }
        }

        Ok(text_parts.join("\n"))
    }

    async fn request_server(
        &self,
        server_name: &str,
        method: &str,
        params: Option<serde_json::Value>,
    ) -> Result<super::protocol::JsonRpcResponse, McpError> {
        self.ensure_enabled()?;
        let server = self
            .servers
            .get(server_name)
            .ok_or_else(|| McpError::ServerNotFound(server_name.to_string()))?;
        let _request_guard = server.request_gate.lock().await;
        let _lifecycle_guard = self.request_lifecycle.read().await;
        self.ensure_enabled()?;
        let mut disable_signal = self.disable_signal.subscribe();
        #[cfg(test)]
        self.pause_before_transport_request().await;
        self.ensure_enabled()?;
        let request_id = self.allocate_request_id()?;
        let request = JsonRpcRequest::new(request_id, method, params);
        let response = tokio::select! {
            biased;
            _ = wait_for_disable(&mut disable_signal) => {
                return Err(McpError::Transport(MANAGER_DISABLED_MESSAGE.into()));
            }
            response = server.transport.request(&request) => response?,
        };
        validate_response_id(request_id, &response)?;
        Ok(response)
    }

    fn allocate_request_id(&self) -> Result<u64, McpError> {
        self.next_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| current.checked_add(1))
            .map_err(|_| McpError::Transport(REQUEST_ID_EXHAUSTED_MESSAGE.into()))
    }

    fn ensure_enabled(&self) -> Result<(), McpError> {
        if self.is_disabled() {
            return Err(McpError::Transport(MANAGER_DISABLED_MESSAGE.into()));
        }
        Ok(())
    }

    /// Begin permanently rejecting new requests without waiting for admitted
    /// requests. Call [`Self::shutdown`] when a completion boundary and
    /// transport closure are required.
    pub fn begin_disable(&self) {
        self.disabled.store(true, Ordering::Release);
        self.disable_signal.send_replace(true);
    }

    /// Permanently reject new requests and wait for every admitted request to
    /// leave the transport boundary.
    #[cfg(test)]
    async fn disable(&self) {
        self.begin_disable();
        let _ = self
            .drain_requests(Duration::from_millis(DEFAULT_SHUTDOWN_TIMEOUT_MS))
            .await;
    }

    #[cfg(test)]
    async fn drain_requests(&self, timeout: Duration) -> bool {
        self.drain_requests_until(Instant::now() + timeout).await
    }

    async fn drain_requests_until(&self, deadline: Instant) -> bool {
        match tokio::time::timeout_at(deadline, self.request_lifecycle.write()).await {
            Ok(_drained) => true,
            Err(_) => {
                tracing::warn!(target: "solaris_mcp", "timed out draining mcp requests before shutdown deadline");
                false
            }
        }
    }

    fn is_disabled(&self) -> bool {
        self.disabled.load(Ordering::Acquire)
    }

    #[cfg(test)]
    async fn pause_before_transport_request(&self) {
        let pause = self.before_transport_pause.lock().unwrap().take();
        if let Some(pause) = pause {
            pause.reached.add_permits(1);
            pause.resume.acquire().await.unwrap().forget();
        }
    }

    #[cfg(test)]
    fn pause_next_request_before_transport(&self) -> Arc<RequestPause> {
        let pause = Arc::new(RequestPause {
            reached: Semaphore::new(0),
            resume: Semaphore::new(0),
        });
        *self.before_transport_pause.lock().unwrap() = Some(Arc::clone(&pause));
        pause
    }

    /// Get names of all connected servers.
    pub fn server_names(&self) -> Vec<String> {
        self.servers.keys().cloned().collect()
    }

    pub(crate) fn server_executable_identity(&self, server_name: &str) -> Option<&ExecutableIdentity> {
        self.servers
            .get(server_name)
            .and_then(|server| server.executable_identity.as_ref())
    }

    /// Check if a connected server declared the resources capability.
    pub fn server_supports_resources(&self, server_name: &str) -> bool {
        if self.is_disabled() {
            return false;
        }
        self.servers
            .get(server_name)
            .map(|s| s.supports_resources)
            .unwrap_or(false)
    }

    /// List all resources from a server.
    pub async fn list_resources(&self, server_name: &str) -> Result<Vec<McpResource>, McpError> {
        let response = self.request_server(server_name, "resources/list", None).await?;

        let result_value = response
            .result
            .ok_or_else(|| McpError::Transport("No result in resources/list response".into()))?;

        let list_result: ResourcesListResult = serde_json::from_value(result_value)
            .map_err(|_| McpError::Transport("Failed to parse resources/list response".into()))?;

        Ok(list_result.resources)
    }

    /// Read a single resource by URI from a server. Returns the text content.
    pub async fn read_resource(&self, server_name: &str, uri: &str) -> Result<String, McpError> {
        let response = self
            .request_server(server_name, "resources/read", Some(json!({ "uri": uri })))
            .await?;

        let result_value = response
            .result
            .ok_or_else(|| McpError::Transport("No result in resources/read response".into()))?;

        let read_result: ResourcesReadResult = serde_json::from_value(result_value)
            .map_err(|_| McpError::Transport("Failed to parse resources/read response".into()))?;

        // Return the first text content found
        read_result
            .contents
            .into_iter()
            .find_map(|c| c.text)
            .ok_or_else(|| McpError::Transport("No text content in resource response".into()))
    }

    /// Gracefully shutdown all servers
    pub async fn shutdown(&self) {
        let _ = self
            .shutdown_with_timeout(Duration::from_millis(DEFAULT_SHUTDOWN_TIMEOUT_MS))
            .await;
    }

    async fn shutdown_with_timeout(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        self.begin_disable();
        let mut shutdown_complete = match tokio::time::timeout_at(deadline, self.shutdown_complete.lock()).await {
            Ok(guard) => guard,
            Err(_) => {
                tracing::warn!(target: "solaris_mcp", "timed out waiting for concurrent mcp shutdown");
                return false;
            }
        };
        if *shutdown_complete {
            return true;
        }

        let mut closes = FuturesUnordered::new();
        for (name, server) in &self.servers {
            closes.push(async move { (name, server.transport.close().await) });
        }
        let mut closes_completed = true;
        while !closes.is_empty() {
            match tokio::time::timeout_at(deadline, closes.next()).await {
                Ok(Some((_name, Ok(())))) => {}
                Ok(Some((name, Err(error)))) => {
                    tracing::warn!(target: "solaris_mcp", server = %name, %error, "error closing mcp server");
                }
                Ok(None) => break,
                Err(_) => {
                    closes_completed = false;
                    tracing::warn!(target: "solaris_mcp", pending_servers = closes.len(), "timed out closing mcp servers before shutdown deadline");
                    break;
                }
            }
        }
        let drained = self.drain_requests_until(deadline).await;
        let completed = closes_completed && drained;
        if completed {
            *shutdown_complete = true;
        }
        completed
    }

    /// Test-only constructor: build a manager from pre-configured servers.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn new_for_test(entries: Vec<(&str, bool, Box<dyn super::transport::McpTransport>)>) -> Self {
        let mut servers = HashMap::new();
        for (name, supports_resources, transport) in entries {
            servers.insert(
                name.to_string(),
                McpServer {
                    name: name.to_string(),
                    transport,
                    tools: Vec::new(),
                    supports_resources,
                    executable_identity: None,
                    request_gate: Mutex::new(()),
                },
            );
        }
        Self {
            servers,
            disabled: AtomicBool::new(false),
            disable_signal: watch::channel(false).0,
            shutdown_complete: Mutex::new(false),
            request_lifecycle: RwLock::new(()),
            #[cfg(test)]
            before_transport_pause: StdMutex::new(None),
            next_id: AtomicU64::new(10),
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn new_for_test_with_tools(
        name: &str,
        supports_resources: bool,
        transport: Box<dyn super::transport::McpTransport>,
        tools: Vec<McpToolDef>,
    ) -> Self {
        let mut manager = Self::new_for_test(vec![(name, supports_resources, transport)]);
        manager.servers.get_mut(name).expect("test server was inserted").tools = tools;
        manager
    }
}

async fn wait_for_disable(signal: &mut watch::Receiver<bool>) {
    loop {
        if *signal.borrow_and_update() {
            return;
        }
        if signal.changed().await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
#[path = "manager_test.rs"]
mod manager_test;

#[cfg(test)]
#[path = "manager_shutdown_test.rs"]
mod manager_shutdown_test;
