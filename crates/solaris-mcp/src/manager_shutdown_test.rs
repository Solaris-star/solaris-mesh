use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;
use tokio::sync::Semaphore;

use super::*;
use crate::protocol::JsonRpcResponse;

struct RequestState {
    active: AtomicUsize,
    requests: AtomicUsize,
    closes: AtomicUsize,
    entered: Semaphore,
}

impl RequestState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            active: AtomicUsize::new(0),
            requests: AtomicUsize::new(0),
            closes: AtomicUsize::new(0),
            entered: Semaphore::new(0),
        })
    }
}

struct ActiveRequest<'a>(&'a AtomicUsize);

impl Drop for ActiveRequest<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

struct NeverReturningTransport {
    state: Arc<RequestState>,
}

#[async_trait]
impl McpTransport for NeverReturningTransport {
    async fn request(&self, _req: &JsonRpcRequest) -> Result<JsonRpcResponse, McpError> {
        self.state.requests.fetch_add(1, Ordering::SeqCst);
        self.state.active.fetch_add(1, Ordering::SeqCst);
        let _active = ActiveRequest(&self.state.active);
        self.state.entered.add_permits(1);
        std::future::pending().await
    }

    async fn notify(&self, _req: &JsonRpcRequest) -> Result<(), McpError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), McpError> {
        self.state.closes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct CountingTransport {
    state: Arc<RequestState>,
}

#[async_trait]
impl McpTransport for CountingTransport {
    async fn request(&self, req: &JsonRpcRequest) -> Result<JsonRpcResponse, McpError> {
        self.state.requests.fetch_add(1, Ordering::SeqCst);
        Ok(JsonRpcResponse {
            jsonrpc: "2.0".into(),
            id: req.id,
            result: Some(json!({ "resources": [] })),
            error: None,
        })
    }

    async fn notify(&self, _req: &JsonRpcRequest) -> Result<(), McpError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), McpError> {
        self.state.closes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct CloseState {
    closes: AtomicUsize,
    entered: Semaphore,
    resume: Semaphore,
}

impl CloseState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            closes: AtomicUsize::new(0),
            entered: Semaphore::new(0),
            resume: Semaphore::new(0),
        })
    }
}

struct ControlledCloseTransport {
    state: Arc<CloseState>,
}

#[async_trait]
impl McpTransport for ControlledCloseTransport {
    async fn request(&self, req: &JsonRpcRequest) -> Result<JsonRpcResponse, McpError> {
        Ok(JsonRpcResponse {
            jsonrpc: "2.0".into(),
            id: req.id,
            result: Some(json!({ "resources": [] })),
            error: None,
        })
    }

    async fn notify(&self, _req: &JsonRpcRequest) -> Result<(), McpError> {
        Ok(())
    }

    async fn close(&self) -> Result<(), McpError> {
        self.state.closes.fetch_add(1, Ordering::SeqCst);
        self.state.entered.add_permits(1);
        self.state.resume.acquire().await.unwrap().forget();
        Ok(())
    }
}

#[tokio::test]
async fn shutdown_cancels_never_returning_request_when_transport_close_is_noop() {
    let state = RequestState::new();
    let manager = Arc::new(McpManager::new_for_test(vec![(
        "http-like",
        true,
        Box::new(NeverReturningTransport {
            state: Arc::clone(&state),
        }),
    )]));
    let request_manager = Arc::clone(&manager);
    let request = tokio::spawn(async move { request_manager.list_resources("http-like").await });
    state.entered.acquire().await.unwrap().forget();

    tokio::time::timeout(Duration::from_millis(250), manager.shutdown())
        .await
        .expect("shutdown must explicitly cancel a request that transport close cannot stop");
    let request_error = tokio::time::timeout(Duration::from_millis(250), request)
        .await
        .expect("cancelled request must leave the manager lifecycle")
        .unwrap()
        .unwrap_err();

    assert_eq!(request_error.to_string(), "Transport error: MCP manager is disabled");
    assert_eq!(state.active.load(Ordering::SeqCst), 0);
    assert_eq!(state.closes.load(Ordering::SeqCst), 1);
    assert_eq!(state.requests.load(Ordering::SeqCst), 1);
    assert!(manager.is_disabled());

    tokio::time::timeout(Duration::from_millis(250), manager.shutdown())
        .await
        .expect("completed shutdown must remain idempotent");
    assert_eq!(
        manager.list_resources("http-like").await.unwrap_err().to_string(),
        "Transport error: MCP manager is disabled"
    );
    assert_eq!(state.requests.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn timed_out_shutdown_drain_can_be_retried_without_late_transport_send() {
    let state = RequestState::new();
    let manager = Arc::new(McpManager::new_for_test(vec![(
        "paused",
        true,
        Box::new(CountingTransport {
            state: Arc::clone(&state),
        }),
    )]));
    let pause = manager.pause_next_request_before_transport();
    let request_manager = Arc::clone(&manager);
    let request = tokio::spawn(async move { request_manager.list_resources("paused").await });
    pause.reached.acquire().await.unwrap().forget();

    assert!(!manager.shutdown_with_timeout(Duration::from_millis(20)).await);
    assert!(manager.is_disabled());
    assert_eq!(state.requests.load(Ordering::SeqCst), 0);

    pause.resume.add_permits(1);
    let error = request.await.unwrap().unwrap_err();
    assert_eq!(error.to_string(), "Transport error: MCP manager is disabled");
    assert_eq!(state.requests.load(Ordering::SeqCst), 0);

    assert!(manager.shutdown_with_timeout(Duration::from_millis(250)).await);
    assert_eq!(state.closes.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn shutdown_uses_one_deadline_for_all_transport_closes() {
    let states = [CloseState::new(), CloseState::new(), CloseState::new()];
    let manager = Arc::new(McpManager::new_for_test(vec![
        (
            "first",
            false,
            Box::new(ControlledCloseTransport {
                state: Arc::clone(&states[0]),
            }),
        ),
        (
            "second",
            false,
            Box::new(ControlledCloseTransport {
                state: Arc::clone(&states[1]),
            }),
        ),
        (
            "third",
            false,
            Box::new(ControlledCloseTransport {
                state: Arc::clone(&states[2]),
            }),
        ),
    ]));
    let started = tokio::time::Instant::now();
    let shutdown_manager = Arc::clone(&manager);
    let shutdown =
        tokio::spawn(async move { shutdown_manager.shutdown_with_timeout(Duration::from_millis(100)).await });

    tokio::task::yield_now().await;
    assert!(
        states.iter().all(|state| state.entered.available_permits() == 1),
        "all transport closes must start concurrently"
    );

    tokio::time::advance(Duration::from_millis(100)).await;
    tokio::task::yield_now().await;
    assert!(shutdown.is_finished(), "one total deadline must bound every close");
    assert!(!shutdown.await.unwrap());
    assert_eq!(tokio::time::Instant::now() - started, Duration::from_millis(100));
    assert!(states.iter().all(|state| state.closes.load(Ordering::SeqCst) == 1));
}

#[tokio::test(start_paused = true)]
async fn transport_close_and_request_drain_share_the_same_deadline() {
    let close_state = CloseState::new();
    let manager = Arc::new(McpManager::new_for_test(vec![(
        "controlled",
        true,
        Box::new(ControlledCloseTransport {
            state: Arc::clone(&close_state),
        }),
    )]));
    let pause = manager.pause_next_request_before_transport();
    let request_manager = Arc::clone(&manager);
    let request = tokio::spawn(async move { request_manager.list_resources("controlled").await });
    pause.reached.acquire().await.unwrap().forget();

    let started = tokio::time::Instant::now();
    let shutdown_manager = Arc::clone(&manager);
    let shutdown =
        tokio::spawn(async move { shutdown_manager.shutdown_with_timeout(Duration::from_millis(100)).await });
    close_state.entered.acquire().await.unwrap().forget();

    tokio::time::advance(Duration::from_millis(60)).await;
    close_state.resume.add_permits(1);
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(40)).await;
    tokio::task::yield_now().await;

    assert!(
        shutdown.is_finished(),
        "request drain must use the remainder of the original deadline"
    );
    assert!(!shutdown.await.unwrap());
    assert_eq!(tokio::time::Instant::now() - started, Duration::from_millis(100));

    pause.resume.add_permits(1);
    let error = request.await.unwrap().unwrap_err();
    assert_eq!(error.to_string(), "Transport error: MCP manager is disabled");
}
