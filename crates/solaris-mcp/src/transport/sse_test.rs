use super::*;

#[tokio::test]
async fn invalid_header_error_does_not_include_the_secret_value() {
    let secret = "sse-secret\n";
    let headers = HashMap::from([("Authorization".to_owned(), secret.to_owned())]);

    let error = match SseTransport::connect("https://example.test/sse", &headers).await {
        Ok(_) => panic!("invalid header must be rejected"),
        Err(error) => error,
    };

    assert!(error.to_string().contains("Authorization"));
    assert!(!error.to_string().contains("sse-secret"));
}

#[test]
fn relative_sse_endpoint_stays_on_the_approved_origin() {
    let origin = reqwest::Url::parse("https://example.test/sse").unwrap();
    assert_eq!(
        resolve_same_origin_endpoint(&origin, "/messages?id=1").unwrap(),
        "https://example.test/messages?id=1"
    );
}

#[test]
fn absolute_cross_origin_sse_endpoint_is_rejected() {
    let origin = reqwest::Url::parse("https://example.test/sse").unwrap();
    let error = resolve_same_origin_endpoint(&origin, "https://attacker.test/messages").unwrap_err();
    assert!(error.to_string().contains("crosses the approved origin"));
}

#[tokio::test]
async fn duplicate_pending_id_is_rejected_without_replacing_the_first_sender() {
    let mut pending = PendingState::default();
    let (first_sender, first_receiver) = oneshot::channel();
    register_pending_response(&mut pending, 42, first_sender).unwrap();
    let (duplicate_sender, duplicate_receiver) = oneshot::channel();

    let error = register_pending_response(&mut pending, 42, duplicate_sender).unwrap_err();

    assert_eq!(
        error.to_string(),
        "Transport error: MCP request id is already in flight"
    );
    let response = JsonRpcResponse {
        jsonrpc: "2.0".to_owned(),
        id: Some(42),
        result: Some(serde_json::json!({})),
        error: None,
    };
    pending.responses.remove(&42).unwrap().send(Ok(response)).unwrap();
    assert_eq!(first_receiver.await.unwrap().unwrap().id, Some(42));
    assert!(duplicate_receiver.await.is_err());
}

#[tokio::test]
async fn mismatched_response_id_fails_the_waiting_request() {
    let pending = Arc::new(Mutex::new(PendingState::default()));
    let (sender, receiver) = oneshot::channel();
    register_pending_response(&mut pending.lock().unwrap(), 42, sender).unwrap();

    dispatch_response(
        &pending,
        JsonRpcResponse {
            jsonrpc: "2.0".to_owned(),
            id: Some(99),
            result: Some(serde_json::json!({})),
            error: None,
        },
    );

    let error = receiver.await.unwrap().unwrap_err();
    assert_eq!(
        error.to_string(),
        "Transport error: MCP response id did not match request"
    );
}

#[tokio::test]
async fn late_response_for_a_cancelled_request_does_not_fail_the_current_request() {
    let pending = Arc::new(Mutex::new(PendingState::default()));
    let (cancelled_sender, cancelled_receiver) = oneshot::channel();
    register_pending_response(&mut pending.lock().unwrap(), 41, cancelled_sender).unwrap();
    let cancelled_registration = PendingRegistration {
        pending: Arc::clone(&pending),
        request_id: 41,
    };
    drop(cancelled_receiver);
    drop(cancelled_registration);

    let (current_sender, current_receiver) = oneshot::channel();
    register_pending_response(&mut pending.lock().unwrap(), 42, current_sender).unwrap();
    dispatch_response(
        &pending,
        JsonRpcResponse {
            jsonrpc: "2.0".to_owned(),
            id: Some(41),
            result: Some(serde_json::json!({})),
            error: None,
        },
    );
    dispatch_response(
        &pending,
        JsonRpcResponse {
            jsonrpc: "2.0".to_owned(),
            id: Some(42),
            result: Some(serde_json::json!({})),
            error: None,
        },
    );

    assert_eq!(current_receiver.await.unwrap().unwrap().id, Some(42));
}

#[tokio::test]
async fn keep_alive_comment_does_not_fail_the_waiting_request() {
    let pending = Arc::new(Mutex::new(PendingState::default()));
    let (sender, receiver) = oneshot::channel();
    register_pending_response(&mut pending.lock().unwrap(), 42, sender).unwrap();

    dispatch_sse_event(&pending, "", "");
    dispatch_sse_event(&pending, "message", r#"{"jsonrpc":"2.0","id":42,"result":{}}"#);

    assert_eq!(receiver.await.unwrap().unwrap().id, Some(42));
}

#[tokio::test]
async fn duplicate_success_response_does_not_fail_a_new_request() {
    let pending = Arc::new(Mutex::new(PendingState::default()));
    let (completed_sender, completed_receiver) = oneshot::channel();
    register_pending_response(&mut pending.lock().unwrap(), 41, completed_sender).unwrap();
    dispatch_response(
        &pending,
        JsonRpcResponse {
            jsonrpc: "2.0".to_owned(),
            id: Some(41),
            result: Some(serde_json::json!({})),
            error: None,
        },
    );
    assert_eq!(completed_receiver.await.unwrap().unwrap().id, Some(41));

    let (current_sender, current_receiver) = oneshot::channel();
    register_pending_response(&mut pending.lock().unwrap(), 42, current_sender).unwrap();
    dispatch_response(
        &pending,
        JsonRpcResponse {
            jsonrpc: "2.0".to_owned(),
            id: Some(41),
            result: Some(serde_json::json!({})),
            error: None,
        },
    );
    dispatch_response(
        &pending,
        JsonRpcResponse {
            jsonrpc: "2.0".to_owned(),
            id: Some(42),
            result: Some(serde_json::json!({})),
            error: None,
        },
    );

    assert_eq!(current_receiver.await.unwrap().unwrap().id, Some(42));
}
