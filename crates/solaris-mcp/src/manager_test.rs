use super::*;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::JsonRpcResponse;
    use async_trait::async_trait;
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::{Barrier, Semaphore};

    // -----------------------------------------------------------------------
    // MockTransport: returns pre-configured JSON-RPC responses
    // -----------------------------------------------------------------------

    struct MockTransport {
        /// Responses returned in order for each request call
        responses: Mutex<Vec<serde_json::Value>>,
    }

    impl MockTransport {
        fn new(responses: Vec<serde_json::Value>) -> Self {
            Self {
                responses: Mutex::new(responses),
            }
        }
    }

    #[async_trait]
    impl McpTransport for MockTransport {
        async fn request(&self, req: &JsonRpcRequest) -> Result<JsonRpcResponse, McpError> {
            let mut guard = self.responses.lock().unwrap();
            let value = if guard.is_empty() { json!(null) } else { guard.remove(0) };
            Ok(JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id: req.id,
                result: Some(value),
                error: None,
            })
        }

        async fn notify(&self, _req: &JsonRpcRequest) -> Result<(), McpError> {
            Ok(())
        }

        async fn close(&self) -> Result<(), McpError> {
            Ok(())
        }
    }

    struct ErrorTransport;

    #[async_trait]
    impl McpTransport for ErrorTransport {
        async fn request(&self, _req: &JsonRpcRequest) -> Result<JsonRpcResponse, McpError> {
            Err(McpError::Transport("mock transport error".into()))
        }

        async fn notify(&self, _req: &JsonRpcRequest) -> Result<(), McpError> {
            Ok(())
        }

        async fn close(&self) -> Result<(), McpError> {
            Ok(())
        }
    }

    struct FixedResponseIdTransport {
        response_id: Option<u64>,
    }

    #[async_trait]
    impl McpTransport for FixedResponseIdTransport {
        async fn request(&self, _req: &JsonRpcRequest) -> Result<JsonRpcResponse, McpError> {
            Ok(JsonRpcResponse {
                jsonrpc: "2.0".to_owned(),
                id: self.response_id,
                result: Some(json!({ "resources": [] })),
                error: None,
            })
        }

        async fn notify(&self, _req: &JsonRpcRequest) -> Result<(), McpError> {
            Ok(())
        }

        async fn close(&self) -> Result<(), McpError> {
            Ok(())
        }
    }

    struct ControlledTransport {
        state: Arc<ControlledTransportState>,
    }

    struct ControlledTransportState {
        active: AtomicUsize,
        maximum_active: AtomicUsize,
        close_calls: AtomicUsize,
        ids: Mutex<Vec<u64>>,
        methods: Mutex<Vec<String>>,
        entered: Semaphore,
        release: Semaphore,
    }

    impl ControlledTransportState {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                active: AtomicUsize::new(0),
                maximum_active: AtomicUsize::new(0),
                close_calls: AtomicUsize::new(0),
                ids: Mutex::new(Vec::new()),
                methods: Mutex::new(Vec::new()),
                entered: Semaphore::new(0),
                release: Semaphore::new(0),
            })
        }

        async fn wait_for_entry(&self) {
            self.entered.acquire().await.unwrap().forget();
        }

        fn release_one(&self) {
            self.release.add_permits(1);
        }
    }

    #[async_trait]
    impl McpTransport for ControlledTransport {
        async fn request(&self, req: &JsonRpcRequest) -> Result<JsonRpcResponse, McpError> {
            let id = req.id.expect("manager requests must carry an id");
            let active = self.state.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.state.maximum_active.fetch_max(active, Ordering::SeqCst);
            self.state.ids.lock().unwrap().push(id);
            self.state.methods.lock().unwrap().push(req.method.clone());
            self.state.entered.add_permits(1);
            self.state.release.acquire().await.unwrap().forget();
            self.state.active.fetch_sub(1, Ordering::SeqCst);

            let result = match req.method.as_str() {
                "tools/call" => {
                    let name = req
                        .params
                        .as_ref()
                        .and_then(|params| params.get("name"))
                        .and_then(serde_json::Value::as_str)
                        .unwrap();
                    json!({ "content": [{ "type": "text", "text": format!("tool:{name}:{id}") }] })
                }
                "resources/list" => json!({ "resources": [{ "uri": format!("resource:{id}") }] }),
                "resources/read" => {
                    let uri = req
                        .params
                        .as_ref()
                        .and_then(|params| params.get("uri"))
                        .and_then(serde_json::Value::as_str)
                        .unwrap();
                    json!({ "contents": [{ "uri": uri, "text": format!("read:{uri}:{id}") }] })
                }
                method => panic!("unexpected method: {method}"),
            };

            Ok(JsonRpcResponse {
                jsonrpc: "2.0".to_owned(),
                id: Some(id),
                result: Some(result),
                error: None,
            })
        }

        async fn notify(&self, _req: &JsonRpcRequest) -> Result<(), McpError> {
            Ok(())
        }

        async fn close(&self) -> Result<(), McpError> {
            self.state.close_calls.fetch_add(1, Ordering::SeqCst);
            self.state.release.add_permits(1);
            Ok(())
        }
    }

    struct RecordingGuard(Arc<Mutex<Option<bool>>>);

    impl McpConnectionGuard for RecordingGuard {
        fn complete(&mut self, result: &Result<Vec<String>, McpError>) -> Result<(), McpError> {
            *self.0.lock().unwrap() = Some(result.is_err());
            Ok(())
        }
    }

    struct FailingGuard;

    impl McpConnectionGuard for FailingGuard {
        fn complete(&mut self, _result: &Result<Vec<String>, McpError>) -> Result<(), McpError> {
            Err(McpError::InitFailed("ledger completion failed".into()))
        }
    }

    fn prepared_stdio_fixture() -> (
        McpServerConfig,
        solaris_process::PinnedCommand,
        solaris_process::ExecutableIdentity,
    ) {
        use solaris_config::shell::ShellKind;

        let shell = solaris_config::shell::default_shell();
        let script = match shell.kind {
            ShellKind::PowerShell => {
                r#"$null = [Console]::In.ReadLine(); [Console]::Out.WriteLine('{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-03-26","capabilities":{},"serverInfo":null}}'); $null = [Console]::In.ReadLine(); $null = [Console]::In.ReadLine(); [Console]::Out.WriteLine('{"jsonrpc":"2.0","id":2,"result":{"tools":[]}}')"#
            }
            ShellKind::Cmd => {
                r#"set /p _=& echo {"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-03-26","capabilities":{},"serverInfo":null}}& set /p _=& set /p _=& echo {"jsonrpc":"2.0","id":2,"result":{"tools":[]}}"#
            }
            ShellKind::Bash | ShellKind::Zsh | ShellKind::Sh => {
                r#"read -r _; printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-03-26","capabilities":{},"serverInfo":null}}'; read -r _; read -r _; printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"tools":[]}}'"#
            }
        };
        let identity = solaris_process::inspect_executable(&shell.path).unwrap();
        let command = solaris_process::pin_executable(&shell.path, &identity)
            .unwrap()
            .command()
            .unwrap();
        let env = ["SYSTEMROOT", "WINDIR"]
            .into_iter()
            .filter_map(|key| std::env::var(key).ok().map(|value| (key.to_owned(), value)))
            .collect();
        (
            McpServerConfig {
                transport: TransportType::Stdio,
                command: Some(shell.path.to_string_lossy().into_owned()),
                args: Some(shell.derive_exec_args(script, false)),
                env: Some(env),
                url: None,
                headers: None,
                network: Default::default(),
                deferred: None,
                startup_timeout_ms: Some(5_000),
            },
            command,
            identity,
        )
    }

    #[tokio::test]
    async fn raw_authorized_entry_rejects_stdio_and_completes_guard() {
        let mut manager = McpManager::new();
        let (config, command, _) = prepared_stdio_fixture();
        drop(command);
        let completion = Arc::new(Mutex::new(None));

        let error = manager
            .connect_one_authorized("raw-stdio".to_owned(), &config, RecordingGuard(Arc::clone(&completion)))
            .await
            .unwrap_err();

        assert!(matches!(error, McpError::InitFailed(_)));
        assert_eq!(*completion.lock().unwrap(), Some(true));
    }

    #[tokio::test]
    async fn prepared_stdio_entry_connects_with_a_pinned_command() {
        let mut manager = McpManager::new();
        let (config, command, _) = prepared_stdio_fixture();
        let expected_identity = command.executable_identity().clone();
        let completion = Arc::new(Mutex::new(None));

        let tools = manager
            .connect_one_authorized_with_stdio_command(
                "prepared-stdio".to_owned(),
                &config,
                command,
                RecordingGuard(Arc::clone(&completion)),
            )
            .await
            .unwrap();

        assert!(tools.is_empty());
        assert_eq!(*completion.lock().unwrap(), Some(false));
        assert_eq!(manager.server_names(), vec!["prepared-stdio"]);
        assert_eq!(
            manager.server_executable_identity("prepared-stdio"),
            Some(&expected_identity)
        );
    }

    #[tokio::test]
    async fn failed_effect_completion_does_not_commit_pending_stdio_server() {
        let mut manager = McpManager::new();
        let (config, command, _) = prepared_stdio_fixture();

        let error = manager
            .connect_one_authorized_with_stdio_command("pending-stdio".to_owned(), &config, command, FailingGuard)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("ledger completion failed"));
        assert!(manager.server_names().is_empty());
    }

    #[tokio::test]
    async fn stdio_startup_timeout_kills_background_descendant() {
        use solaris_config::shell::ShellKind;

        let directory = tempfile::tempdir().unwrap();
        let marker = directory.path().join("startup-descendant-must-not-survive");
        let shell = solaris_config::shell::default_shell();
        let script = match shell.kind {
            ShellKind::PowerShell => format!(
                "$p = Start-Process -FilePath (Get-Process -Id $PID).Path -ArgumentList '-NoProfile', '-Command', \"Start-Sleep -Seconds 2; Set-Content -LiteralPath '{}' -Value survived\" -PassThru -WindowStyle Hidden; Start-Sleep -Seconds 30",
                marker.to_string_lossy().replace('\'', "''")
            ),
            ShellKind::Cmd => format!(
                "start /b cmd /c \"ping -n 3 127.0.0.1 >nul & echo survived>\\\"{}\\\"\" & ping -n 31 127.0.0.1 >nul",
                marker.display()
            ),
            ShellKind::Bash | ShellKind::Zsh | ShellKind::Sh => format!(
                "(sleep 2; printf survived > '{}') & sleep 30",
                marker.to_string_lossy().replace('\'', "'\\''")
            ),
        };
        let identity = solaris_process::inspect_executable(&shell.path).unwrap();
        let command = solaris_process::pin_executable(&shell.path, &identity)
            .unwrap()
            .command()
            .unwrap();
        let config = McpServerConfig {
            transport: TransportType::Stdio,
            command: Some(shell.path.to_string_lossy().into_owned()),
            args: Some(shell.derive_exec_args(&script, false)),
            env: Some(
                ["PATH", "PATHEXT", "SYSTEMROOT", "WINDIR", "COMSPEC"]
                    .into_iter()
                    .filter_map(|key| std::env::var(key).ok().map(|value| (key.to_owned(), value)))
                    .collect(),
            ),
            url: None,
            headers: None,
            network: Default::default(),
            deferred: None,
            startup_timeout_ms: Some(250),
        };
        let completion = Arc::new(Mutex::new(None));
        let mut manager = McpManager::new();

        let error = manager
            .connect_one_authorized_with_stdio_command(
                "timeout-stdio".to_owned(),
                &config,
                command,
                RecordingGuard(Arc::clone(&completion)),
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("startup timed out"));
        assert!(manager.server_names().is_empty());
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(!marker.exists(), "MCP startup descendant survived transport drop");
    }

    // -----------------------------------------------------------------------
    // Test helpers: build McpManager with pre-configured servers
    // -----------------------------------------------------------------------

    fn make_manager_with_servers(entries: Vec<(&str, bool, Box<dyn McpTransport>)>) -> McpManager {
        McpManager::new_for_test(entries)
    }

    fn delayed_config(delay_ms: u64, startup_timeout_ms: Option<u64>) -> McpServerConfig {
        McpServerConfig {
            transport: TransportType::Stdio,
            command: None,
            args: Some(vec![delay_ms.to_string()]),
            env: None,
            url: None,
            headers: None,
            network: Default::default(),
            deferred: None,
            startup_timeout_ms,
        }
    }

    fn successful_test_server(name: &str) -> McpServer {
        McpServer {
            name: name.to_string(),
            transport: Box::new(MockTransport::new(vec![])),
            tools: vec![],
            supports_resources: false,
            executable_identity: None,
            request_gate: tokio::sync::Mutex::new(()),
        }
    }

    async fn delayed_test_connect(name: String, config: McpServerConfig) -> Result<McpServer, McpError> {
        let delay_ms = config
            .args
            .as_ref()
            .and_then(|args| args.first())
            .and_then(|arg| arg.parse::<u64>().ok())
            .unwrap_or(0);
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        Ok(successful_test_server(&name))
    }

    #[tokio::test]
    async fn connect_all_attempts_servers_concurrently() {
        let configs = HashMap::from([
            ("slow-a".to_string(), delayed_config(0, None)),
            ("slow-b".to_string(), delayed_config(0, None)),
            ("slow-c".to_string(), delayed_config(0, None)),
        ]);
        let all_connectors_started = Arc::new(Barrier::new(4));
        let connector_barrier = Arc::clone(&all_connectors_started);

        let manager_task = tokio::spawn(async move {
            McpManager::connect_all_with_connector(&configs, move |name, _config| {
                let connector_barrier = Arc::clone(&connector_barrier);
                async move {
                    connector_barrier.wait().await;
                    Ok(successful_test_server(&name))
                }
            })
            .await
            .unwrap()
        });

        tokio::time::timeout(Duration::from_millis(100), all_connectors_started.wait())
            .await
            .expect("connect_all should start every connector before awaiting the first result");
        let manager = manager_task.await.unwrap();

        assert_eq!(manager.server_names().len(), 3);
    }

    #[tokio::test]
    async fn connect_all_applies_per_server_startup_timeout() {
        let configs = HashMap::from([
            ("fast".to_string(), delayed_config(10, None)),
            ("too-slow".to_string(), delayed_config(200, Some(20))),
        ]);

        let started_at = tokio::time::Instant::now();
        let manager = McpManager::connect_all_with_connector(&configs, delayed_test_connect)
            .await
            .unwrap();
        let elapsed = started_at.elapsed();

        assert_eq!(manager.server_names(), vec!["fast".to_string()]);
        assert!(
            elapsed < Duration::from_millis(150),
            "timed out server should not block connect_all; elapsed={elapsed:?}"
        );
    }

    // -----------------------------------------------------------------------
    // TC-2.x: server_supports_resources [黑盒 + 白盒]
    // -----------------------------------------------------------------------

    #[test]
    fn tc_2_1_server_supports_resources_true() {
        // [黑盒] TC-2.1: server with resources capability returns true
        let manager = make_manager_with_servers(vec![("test-server", true, Box::new(MockTransport::new(vec![])))]);

        assert!(manager.server_supports_resources("test-server"));
    }

    #[test]
    fn tc_2_2_server_supports_resources_false() {
        // [黑盒] TC-2.2: server without resources capability returns false
        let manager = make_manager_with_servers(vec![(
            "no-resources-server",
            false,
            Box::new(MockTransport::new(vec![])),
        )]);

        assert!(!manager.server_supports_resources("no-resources-server"));
    }

    #[test]
    fn tc_2_3_server_supports_resources_unknown_server() {
        // [黑盒] TC-2.3: unknown server name returns false (not error)
        let manager = make_manager_with_servers(vec![]);

        assert!(!manager.server_supports_resources("unknown-server"));
    }

    #[test]
    fn tc_2_wb_supports_resources_from_capabilities_null_value() {
        // [白盒] capabilities.get("resources") = null → supports_resources = false
        // This is tested via the parsed field; we verify via make_manager helper
        let manager = make_manager_with_servers(vec![(
            "server",
            false, // null resources → false per impl: !v.is_null() = false
            Box::new(MockTransport::new(vec![])),
        )]);

        assert!(!manager.server_supports_resources("server"));
    }

    // -----------------------------------------------------------------------
    // TC-2.10/2.11: server_names [黑盒]
    // -----------------------------------------------------------------------

    #[test]
    fn tc_2_10_server_names_returns_all() {
        // [黑盒] TC-2.10: server_names returns all connected server names
        let manager = make_manager_with_servers(vec![
            ("server-a", false, Box::new(MockTransport::new(vec![]))),
            ("server-b", true, Box::new(MockTransport::new(vec![]))),
        ]);

        let mut names = manager.server_names();
        names.sort();
        assert_eq!(names, vec!["server-a", "server-b"]);
    }

    #[test]
    fn tc_2_11_server_names_empty_manager() {
        // [黑盒] TC-2.11: no connected servers → empty vec
        let manager = make_manager_with_servers(vec![]);

        assert!(manager.server_names().is_empty());
    }

    #[test]
    fn tc_2_wb_server_names_returns_owned_strings() {
        // [白盒] Decision 1: server_names() returns Vec<String> not Vec<&str>
        let manager = make_manager_with_servers(vec![("my-server", false, Box::new(MockTransport::new(vec![])))]);

        let names: Vec<String> = manager.server_names();
        assert_eq!(names, vec!["my-server"]);
    }

    // -----------------------------------------------------------------------
    // TC-2.4/2.5: list_resources [黑盒]
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn tc_2_4_list_resources_normal() {
        // [黑盒] TC-2.4: list_resources returns resources from server
        let resources_response = json!({
            "resources": [
                {"uri": "skill://skill-a"},
                {"uri": "skill://skill-b", "name": "Skill B"}
            ]
        });

        let manager = make_manager_with_servers(vec![(
            "test-server",
            true,
            Box::new(MockTransport::new(vec![resources_response])),
        )]);

        let result = manager.list_resources("test-server").await.unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].uri, "skill://skill-a");
        assert_eq!(result[1].uri, "skill://skill-b");
    }

    #[tokio::test]
    async fn tc_2_5_list_resources_empty() {
        // [黑盒] TC-2.5: list_resources returns empty list when server has no resources
        let resources_response = json!({"resources": []});

        let manager = make_manager_with_servers(vec![(
            "test-server",
            true,
            Box::new(MockTransport::new(vec![resources_response])),
        )]);

        let result = manager.list_resources("test-server").await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn tc_2_6_list_resources_server_not_found() {
        // [黑盒] TC-2.6: list_resources returns error when server does not exist
        let manager = make_manager_with_servers(vec![]);

        let result = manager.list_resources("nonexistent").await;
        assert!(result.is_err());
        match result.unwrap_err() {
            McpError::ServerNotFound(name) => assert_eq!(name, "nonexistent"),
            e => panic!("expected ServerNotFound, got {:?}", e),
        }
    }

    // -----------------------------------------------------------------------
    // TC-2.7/2.8/2.9: read_resource [黑盒 + 白盒]
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn tc_2_7_read_resource_returns_text() {
        // [黑盒] TC-2.7: read_resource returns text content
        let read_response = json!({
            "contents": [{"uri": "skill://my-skill", "mimeType": "text/plain", "text": "---\ndescription: A skill\n---\n# My Skill\n"}]
        });

        let manager = make_manager_with_servers(vec![(
            "test-server",
            true,
            Box::new(MockTransport::new(vec![read_response])),
        )]);

        let result = manager.read_resource("test-server", "skill://my-skill").await.unwrap();
        assert!(result.contains("description: A skill"));
    }

    #[tokio::test]
    async fn tc_2_8_read_resource_transport_error() {
        // [黑盒] TC-2.8: read_resource returns error when server returns transport error
        let manager = make_manager_with_servers(vec![("test-server", true, Box::new(ErrorTransport))]);

        let result = manager.read_resource("test-server", "skill://nonexistent").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn tc_2_9_read_resource_server_not_found() {
        // [黑盒] TC-2.9: read_resource returns error when server does not exist
        let manager = make_manager_with_servers(vec![]);

        let result = manager.read_resource("nonexistent", "skill://my-skill").await;
        assert!(result.is_err());
        match result.unwrap_err() {
            McpError::ServerNotFound(name) => assert_eq!(name, "nonexistent"),
            e => panic!("expected ServerNotFound, got {:?}", e),
        }
    }

    #[tokio::test]
    async fn tc_2_wb_read_resource_no_text_content_returns_error() {
        // [白盒] Decision 3: find_map returns None when all contents have text=None → error
        let read_response = json!({
            "contents": [{"uri": "skill://binary", "mimeType": "application/octet-stream"}]
        });

        let manager = make_manager_with_servers(vec![(
            "test-server",
            true,
            Box::new(MockTransport::new(vec![read_response])),
        )]);

        let result = manager.read_resource("test-server", "skill://binary").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn tc_2_wb_read_resource_find_map_first_text() {
        // [白盒] Decision 3: find_map returns first content with non-None text
        let read_response = json!({
            "contents": [
                {"uri": "skill://x"},
                {"uri": "skill://x", "text": "actual content"}
            ]
        });

        let manager = make_manager_with_servers(vec![(
            "test-server",
            true,
            Box::new(MockTransport::new(vec![read_response])),
        )]);

        let result = manager.read_resource("test-server", "skill://x").await.unwrap();
        assert_eq!(result, "actual content");
    }

    #[tokio::test]
    async fn concurrent_tool_calls_use_distinct_ids_and_do_not_cross_results() {
        let state = ControlledTransportState::new();
        let manager = Arc::new(make_manager_with_servers(vec![(
            "shared",
            true,
            Box::new(ControlledTransport {
                state: Arc::clone(&state),
            }),
        )]));
        let start = Arc::new(Barrier::new(3));

        let first_manager = Arc::clone(&manager);
        let first_start = Arc::clone(&start);
        let first = tokio::spawn(async move {
            first_start.wait().await;
            first_manager
                .call_tool("shared", "first", json!({ "marker": "first" }))
                .await
        });
        let second_manager = Arc::clone(&manager);
        let second_start = Arc::clone(&start);
        let second = tokio::spawn(async move {
            second_start.wait().await;
            second_manager
                .call_tool("shared", "second", json!({ "marker": "second" }))
                .await
        });

        start.wait().await;
        state.wait_for_entry().await;
        assert_eq!(state.active.load(Ordering::SeqCst), 1);
        state.release_one();
        state.wait_for_entry().await;
        assert_eq!(state.active.load(Ordering::SeqCst), 1);
        state.release_one();

        let first_result = first.await.unwrap().unwrap();
        let second_result = second.await.unwrap().unwrap();
        assert!(first_result.starts_with("tool:first:"));
        assert!(second_result.starts_with("tool:second:"));
        assert_eq!(state.maximum_active.load(Ordering::SeqCst), 1);

        let ids = state.ids.lock().unwrap();
        assert_eq!(ids.len(), 2);
        assert_ne!(ids[0], ids[1]);
    }

    #[tokio::test]
    async fn tool_list_and_read_requests_share_the_same_server_gate() {
        let state = ControlledTransportState::new();
        let manager = Arc::new(make_manager_with_servers(vec![(
            "shared",
            true,
            Box::new(ControlledTransport {
                state: Arc::clone(&state),
            }),
        )]));
        let start = Arc::new(Barrier::new(4));

        let tool_manager = Arc::clone(&manager);
        let tool_start = Arc::clone(&start);
        let tool = tokio::spawn(async move {
            tool_start.wait().await;
            tool_manager.call_tool("shared", "mixed", json!({})).await
        });
        let list_manager = Arc::clone(&manager);
        let list_start = Arc::clone(&start);
        let list = tokio::spawn(async move {
            list_start.wait().await;
            list_manager.list_resources("shared").await
        });
        let read_manager = Arc::clone(&manager);
        let read_start = Arc::clone(&start);
        let read = tokio::spawn(async move {
            read_start.wait().await;
            read_manager.read_resource("shared", "resource://mixed").await
        });

        start.wait().await;
        for _ in 0..3 {
            state.wait_for_entry().await;
            assert_eq!(state.active.load(Ordering::SeqCst), 1);
            state.release_one();
        }

        assert!(tool.await.unwrap().unwrap().starts_with("tool:mixed:"));
        assert_eq!(list.await.unwrap().unwrap().len(), 1);
        assert!(read.await.unwrap().unwrap().starts_with("read:resource://mixed:"));
        assert_eq!(state.maximum_active.load(Ordering::SeqCst), 1);

        let mut methods = state.methods.lock().unwrap().clone();
        methods.sort();
        assert_eq!(methods, ["resources/list", "resources/read", "tools/call"]);
        let ids = state.ids.lock().unwrap();
        assert_eq!(ids.len(), 3);
        assert!(
            ids.iter()
                .all(|id| ids.iter().filter(|candidate| *candidate == id).count() == 1)
        );
    }

    #[tokio::test]
    async fn disabling_manager_rejects_waiters_before_id_allocation_or_transport_send() {
        let state = ControlledTransportState::new();
        let manager = Arc::new(make_manager_with_servers(vec![(
            "shared",
            true,
            Box::new(ControlledTransport {
                state: Arc::clone(&state),
            }),
        )]));

        let first_manager = Arc::clone(&manager);
        let first = tokio::spawn(async move { first_manager.list_resources("shared").await });
        state.wait_for_entry().await;

        let second_manager = Arc::clone(&manager);
        let second = tokio::spawn(async move { second_manager.list_resources("shared").await });
        tokio::task::yield_now().await;
        manager.begin_disable();

        let immediate_error = manager.list_resources("shared").await.unwrap_err();
        assert_eq!(immediate_error.to_string(), "Transport error: MCP manager is disabled");
        state.release_one();

        let first_error = first.await.unwrap().unwrap_err();
        assert_eq!(first_error.to_string(), "Transport error: MCP manager is disabled");
        let waiting_error = second.await.unwrap().unwrap_err();
        assert_eq!(waiting_error.to_string(), "Transport error: MCP manager is disabled");
        assert_eq!(state.ids.lock().unwrap().len(), 1);
        assert_eq!(state.methods.lock().unwrap().as_slice(), ["resources/list"]);
    }

    #[tokio::test]
    async fn disable_drain_prevents_transport_entry_after_the_second_check() {
        let state = ControlledTransportState::new();
        let manager = Arc::new(make_manager_with_servers(vec![(
            "shared",
            true,
            Box::new(ControlledTransport {
                state: Arc::clone(&state),
            }),
        )]));
        let pause = manager.pause_next_request_before_transport();

        let request_manager = Arc::clone(&manager);
        let request = tokio::spawn(async move { request_manager.list_resources("shared").await });
        pause.reached.acquire().await.unwrap().forget();
        assert!(state.ids.lock().unwrap().is_empty());

        let disable_complete = Arc::new(AtomicBool::new(false));
        let disabling_manager = Arc::clone(&manager);
        let recorded_completion = Arc::clone(&disable_complete);
        let mut disabling = tokio::spawn(async move {
            disabling_manager.disable().await;
            recorded_completion.store(true, Ordering::SeqCst);
        });
        while !manager.is_disabled() {
            tokio::task::yield_now().await;
        }

        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut disabling)
                .await
                .is_err(),
            "disable completed while a request still held the lifecycle admission"
        );

        pause.resume.add_permits(1);
        let request_error = request.await.unwrap().unwrap_err();
        assert_eq!(request_error.to_string(), "Transport error: MCP manager is disabled");
        disabling.await.unwrap();
        assert!(disable_complete.load(Ordering::SeqCst));
        assert!(state.ids.lock().unwrap().is_empty());

        let ids_after_disable = state.ids.lock().unwrap().len();
        assert_eq!(
            manager.list_resources("shared").await.unwrap_err().to_string(),
            "Transport error: MCP manager is disabled"
        );
        assert_eq!(state.ids.lock().unwrap().len(), ids_after_disable);
    }

    #[tokio::test]
    async fn shutdown_disables_new_requests_and_closes_an_in_flight_transport() {
        let state = ControlledTransportState::new();
        let manager = Arc::new(make_manager_with_servers(vec![(
            "shared",
            true,
            Box::new(ControlledTransport {
                state: Arc::clone(&state),
            }),
        )]));
        let request_manager = Arc::clone(&manager);
        let request = tokio::spawn(async move { request_manager.list_resources("shared").await });
        state.wait_for_entry().await;

        tokio::time::timeout(Duration::from_secs(1), manager.shutdown())
            .await
            .expect("shutdown must cancel or finish an in-flight request");

        assert!(manager.is_disabled());
        assert_eq!(state.close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            request.await.unwrap().unwrap_err().to_string(),
            "Transport error: MCP manager is disabled"
        );
        assert_eq!(
            manager.list_resources("shared").await.unwrap_err().to_string(),
            "Transport error: MCP manager is disabled"
        );
    }

    #[tokio::test]
    async fn manager_rejects_missing_or_mismatched_response_ids() {
        for response_id in [None, Some(999)] {
            let manager = make_manager_with_servers(vec![(
                "wrong-id",
                true,
                Box::new(FixedResponseIdTransport { response_id }),
            )]);

            let error = manager.list_resources("wrong-id").await.unwrap_err();
            assert_eq!(
                error.to_string(),
                "Transport error: MCP response id did not match request"
            );
        }
    }

    #[tokio::test]
    async fn request_id_exhaustion_does_not_wrap_to_zero() {
        let manager = make_manager_with_servers(vec![(
            "boundary",
            true,
            Box::new(MockTransport::new(vec![json!({ "resources": [] })])),
        )]);
        manager.next_id.store(u64::MAX - 1, Ordering::Relaxed);

        assert!(manager.list_resources("boundary").await.unwrap().is_empty());
        let error = manager.list_resources("boundary").await.unwrap_err();

        assert_eq!(error.to_string(), "Transport error: MCP request id space is exhausted");
        assert_eq!(manager.next_id.load(Ordering::Relaxed), u64::MAX);
    }

    #[test]
    fn tc_2_wb_next_id_starts_at_10() {
        // [白盒] Decision 4: AtomicU64 counter starts at 10 to avoid conflict with connect_server IDs 1/2
        let manager = make_manager_with_servers(vec![]);
        // next_id is private — we verify by doing two fetch_adds and checking values are 10 and 11
        let id1 = manager.next_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let id2 = manager.next_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(id1, 10, "first ID should be 10");
        assert_eq!(id2, 11, "second ID should be 11");
    }
}
