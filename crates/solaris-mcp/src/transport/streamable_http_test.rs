use super::*;

#[tokio::test]
async fn invalid_header_error_does_not_include_the_secret_value() {
    let secret = "streamable-secret\n";
    let headers = HashMap::from([("Authorization".to_owned(), secret.to_owned())]);

    let error = match StreamableHttpTransport::connect("https://example.test/mcp", &headers).await {
        Ok(_) => panic!("invalid header must be rejected"),
        Err(error) => error,
    };

    assert!(error.to_string().contains("Authorization"));
    assert!(!error.to_string().contains("streamable-secret"));
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Servers built on the MCP Python SDK / `fastmcp` use `sse-starlette`,
    /// whose default SSE line separator is CRLF (`\r\n`). The event terminator
    /// is therefore `\r\n\r\n`, which does not contain `\n\n`. The parser must
    /// still recover the JSON-RPC response, otherwise such servers connect but
    /// expose no tools to the model.
    #[test]
    fn extracts_jsonrpc_from_crlf_delimited_sse() {
        let body = "event: message\r\ndata: {\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"tools\":[]}}\r\n\r\n";
        let resp = extract_jsonrpc_from_sse_buffer(body).expect("should parse CRLF SSE");
        assert_eq!(resp.id, Some(2));
        assert!(resp.result.is_some());
    }

    /// Node `@modelcontextprotocol/sdk` servers emit LF-delimited SSE.
    #[test]
    fn extracts_jsonrpc_from_lf_delimited_sse() {
        let body = "event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}\n\n";
        let resp = extract_jsonrpc_from_sse_buffer(body).expect("should parse LF SSE");
        assert_eq!(resp.id, Some(1));
    }

    /// Data split across multiple `data:` lines must be reassembled.
    #[test]
    fn reassembles_multiline_data() {
        let body = "data: {\"jsonrpc\":\"2.0\",\r\ndata: \"id\":7,\"result\":{}}\r\n\r\n";
        let resp = extract_jsonrpc_from_sse_buffer(body).expect("should join data lines");
        assert_eq!(resp.id, Some(7));
    }

    /// Notifications (no `id`) and comment/ping lines must be skipped.
    #[test]
    fn returns_none_without_complete_response() {
        let body = ": keep-alive\r\n\r\nevent: message\r\ndata: not-json\r\n\r\n";
        assert!(extract_jsonrpc_from_sse_buffer(body).is_none());
    }

    #[test]
    fn skips_server_notification_before_response() {
        let body = concat!(
            "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{}}\n\n",
            "data: {\"jsonrpc\":\"2.0\",\"id\":8,\"result\":{}}\n\n"
        );

        let response = extract_jsonrpc_from_sse_buffer(body).unwrap();

        assert_eq!(response.id, Some(8));
    }

    #[tokio::test]
    async fn streamable_http_does_not_follow_cross_origin_redirects() {
        let destination = MockServer::start().await;
        let source = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/rpc"))
            .respond_with(ResponseTemplate::new(302).insert_header("Location", destination.uri()))
            .mount(&source)
            .await;
        let transport = StreamableHttpTransport::connect(&format!("{}/rpc", source.uri()), &HashMap::new())
            .await
            .unwrap();

        let request = JsonRpcRequest::new(1, "tools/list", None);
        assert!(transport.request(&request).await.is_err());
        assert!(destination.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn duplicate_in_flight_request_id_is_rejected_and_released_on_drop() {
        let transport = StreamableHttpTransport::connect("https://example.test/mcp", &HashMap::new())
            .await
            .unwrap();
        let first = transport.register_request_id(17).unwrap();

        let error = transport.register_request_id(17).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Transport error: MCP request id is already in flight"
        );

        drop(first);
        assert!(transport.register_request_id(17).is_ok());
    }
}
