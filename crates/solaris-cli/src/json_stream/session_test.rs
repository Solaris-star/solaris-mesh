use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use solaris_agent::output::OutputSink;
use solaris_agent::output::protocol_sink::ProtocolSink;
use solaris_agent::session::SessionManager;
use solaris_mcp::manager::McpManager;
use solaris_mcp::protocol::{JsonRpcRequest, JsonRpcResponse};
use solaris_mcp::transport::{McpError, McpTransport};
use solaris_protocol::events::ProtocolEvent;
use solaris_protocol::writer::{ProtocolEmitter, ProtocolWriter};
use solaris_types::message::{ContentBlock, Message, Role};
use tokio::sync::Semaphore;

use super::{
    DurableHostEmitter, check_protocol_output_health, resolve_effective_resume, shutdown_mcp_managers_with_timeout,
};

struct FailingProtocolEmitter;

impl ProtocolEmitter for FailingProtocolEmitter {
    fn emit(&self, _event: &ProtocolEvent) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "injected Host output failure",
        ))
    }
}

struct NeverClosingState {
    closes: AtomicUsize,
    entered: Semaphore,
}

impl NeverClosingState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            closes: AtomicUsize::new(0),
            entered: Semaphore::new(0),
        })
    }
}

struct NeverClosingTransport {
    state: Arc<NeverClosingState>,
}

#[async_trait]
impl McpTransport for NeverClosingTransport {
    async fn request(&self, _req: &JsonRpcRequest) -> Result<JsonRpcResponse, McpError> {
        panic!("shutdown deadline test does not issue requests")
    }

    async fn notify(&self, _req: &JsonRpcRequest) -> Result<(), McpError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), McpError> {
        self.state.closes.fetch_add(1, Ordering::SeqCst);
        self.state.entered.add_permits(1);
        std::future::pending().await
    }
}

#[test]
fn exact_host_session_id_creates_once_then_resumes_preserved_history() {
    let directory = tempfile::tempdir().unwrap();
    let manager = SessionManager::new(directory.path().to_path_buf(), 10);
    assert_eq!(
        resolve_effective_resume(&manager, None, Some("studio-session")).unwrap(),
        None
    );

    let mut session = manager
        .create("test", "model", "workspace", Some("studio-session"))
        .unwrap();
    session.messages.push(Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: "preserved".into(),
        }],
    ));
    manager.save(&session).unwrap();

    assert_eq!(
        resolve_effective_resume(&manager, None, Some("studio-session")).unwrap(),
        Some("studio-session".into())
    );
    let restored = manager.load("studio-session").unwrap();
    assert_eq!(restored.messages.len(), 1);
    assert!(matches!(
        &restored.messages[0].content[0],
        ContentBlock::Text { text } if text == "preserved"
    ));
}

#[test]
fn main_loop_health_check_observes_output_sink_failure() {
    let sink = ProtocolSink::new(Arc::new(FailingProtocolEmitter));
    let durable = DurableHostEmitter::new(Arc::new(ProtocolWriter::new()));
    sink.emit_info("cannot be delivered");

    let error = check_protocol_output_health(&sink, &durable).unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
    assert_eq!(error.to_string(), "injected Host output failure");
}

#[tokio::test(start_paused = true)]
async fn cli_mcp_shutdown_uses_one_deadline_for_static_and_dynamic_managers() {
    let states = [
        NeverClosingState::new(),
        NeverClosingState::new(),
        NeverClosingState::new(),
    ];
    let managers = states
        .iter()
        .enumerate()
        .map(|(index, state)| {
            Arc::new(McpManager::new_for_test(vec![(
                if index == 0 { "static" } else { "dynamic" },
                false,
                Box::new(NeverClosingTransport {
                    state: Arc::clone(state),
                }),
            )]))
        })
        .collect::<Vec<_>>();
    let started = tokio::time::Instant::now();
    let shutdown =
        tokio::spawn(async move { shutdown_mcp_managers_with_timeout(&managers, Duration::from_millis(100)).await });

    tokio::task::yield_now().await;
    assert!(
        states.iter().all(|state| state.entered.available_permits() == 1),
        "all managers must begin closing before the shared deadline"
    );

    tokio::time::advance(Duration::from_millis(100)).await;
    tokio::task::yield_now().await;
    assert!(shutdown.is_finished(), "Stop must have one fixed MCP shutdown bound");
    assert!(!shutdown.await.unwrap());
    assert_eq!(tokio::time::Instant::now() - started, Duration::from_millis(100));
    assert!(states.iter().all(|state| state.closes.load(Ordering::SeqCst) == 1));
}
