use std::collections::{HashMap, HashSet};
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderValue};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use super::{
    DUPLICATE_REQUEST_ID_MESSAGE, McpError, McpTransport, SERVER_ERROR_MESSAGE, redirect_safe_client, request_id,
    validate_response_id,
};
use crate::protocol::{JsonRpcRequest, JsonRpcResponse, parse_jsonrpc_response_message};

/// Streamable HTTP transport: uses HTTP POST for both requests and responses
/// Supports optional SSE streaming for server responses
pub struct StreamableHttpTransport {
    client: reqwest::Client,
    url: String,
    headers: HeaderMap,
    session_id: Mutex<Option<String>>,
    in_flight: StdMutex<HashSet<u64>>,
    next_id: AtomicU64,
}

#[derive(Debug)]
struct InFlightRequestId<'a> {
    in_flight: &'a StdMutex<HashSet<u64>>,
    request_id: u64,
}

impl Drop for InFlightRequestId<'_> {
    fn drop(&mut self) {
        if let Ok(mut in_flight) = self.in_flight.lock() {
            in_flight.remove(&self.request_id);
        }
    }
}

impl StreamableHttpTransport {
    /// Create a new Streamable HTTP transport
    pub async fn connect(url: &str, headers: &HashMap<String, String>) -> Result<Self, McpError> {
        let mut header_map = HeaderMap::new();
        for (k, v) in headers {
            let name = reqwest::header::HeaderName::from_bytes(k.as_bytes())
                .map_err(|e| McpError::Transport(format!("Invalid header name '{}': {}", k, e)))?;
            let value = HeaderValue::from_str(v)
                .map_err(|e| McpError::Transport(format!("Invalid value for header '{}': {}", k, e)))?;
            header_map.insert(name, value);
        }

        Ok(Self {
            client: redirect_safe_client(url)?,
            url: url.to_string(),
            headers: header_map,
            session_id: Mutex::new(None),
            in_flight: StdMutex::new(HashSet::new()),
            next_id: AtomicU64::new(1),
        })
    }

    /// Allocate an ID for callers using the transport without `McpManager`.
    pub fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    fn register_request_id(&self, request_id: u64) -> Result<InFlightRequestId<'_>, McpError> {
        let mut in_flight = self
            .in_flight
            .lock()
            .map_err(|_| McpError::Transport("MCP request state is unavailable".into()))?;
        if !in_flight.insert(request_id) {
            return Err(McpError::Transport(DUPLICATE_REQUEST_ID_MESSAGE.into()));
        }
        Ok(InFlightRequestId {
            in_flight: &self.in_flight,
            request_id,
        })
    }

    /// Build request with session ID header if available
    async fn build_request(&self, body: &str) -> reqwest::RequestBuilder {
        let mut req = self
            .client
            .post(&self.url)
            .headers(self.headers.clone())
            .header("Content-Type", "application/json")
            .header("Accept", "application/json, text/event-stream");

        if let Some(sid) = self.session_id.lock().await.as_ref() {
            req = req.header("Mcp-Session-Id", sid.as_str());
        }

        req.body(body.to_string())
    }

    /// Parse response based on content type
    async fn parse_response(&self, response: reqwest::Response) -> Result<JsonRpcResponse, McpError> {
        // Capture session ID from response headers
        if let Some(sid) = response.headers().get("mcp-session-id")
            && let Ok(sid_str) = sid.to_str()
        {
            *self.session_id.lock().await = Some(sid_str.to_string());
        }

        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        if content_type.contains("text/event-stream") {
            // SSE response: parse events to find the JSON-RPC response
            self.parse_sse_response(response).await
        } else {
            // Direct JSON response
            let text = response
                .text()
                .await
                .map_err(|e| McpError::Transport(format!("Read response body failed: {}", e.without_url())))?;
            match parse_jsonrpc_response_message(text.as_bytes()) {
                Ok(Some(response)) => Ok(response),
                Ok(None) => Err(McpError::Transport(
                    "HTTP request returned a JSON-RPC notification instead of a response".into(),
                )),
                Err(_) => {
                    let mut hasher = Sha256::new();
                    hasher.update(b"solaris.mcp/invalid-http-response/v1\0");
                    hasher.update(text.as_bytes());
                    Err(McpError::Transport(format!(
                        "Parse JSON response failed; bytes={}; digest=sha256:{:x}",
                        text.len(),
                        hasher.finalize()
                    )))
                }
            }
        }
    }

    /// Parse an SSE stream response to extract JSON-RPC response
    async fn parse_sse_response(&self, response: reqwest::Response) -> Result<JsonRpcResponse, McpError> {
        use futures::StreamExt;

        let mut stream = response.bytes_stream();
        let mut buffer = String::new();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| McpError::Transport(format!("SSE read error: {}", e.without_url())))?;
            buffer.push_str(&String::from_utf8_lossy(&chunk));

            if let Some(rpc_response) = extract_jsonrpc_from_sse_buffer(&buffer) {
                return Ok(rpc_response);
            }
        }

        Err(McpError::Transport("SSE stream ended without JSON-RPC response".into()))
    }
}

/// Extract the first complete JSON-RPC response from an accumulated SSE buffer.
///
/// Tolerant of both LF (`\n`) and CRLF (`\r\n`) line endings: Node
/// `@modelcontextprotocol/sdk` servers emit LF, while Python `fastmcp` / MCP
/// SDK servers (via `sse-starlette`, whose default separator is CRLF) emit
/// `\r\n\r\n` event terminators. Returns `None` until the buffer holds a
/// complete event carrying a parseable JSON-RPC response.
fn extract_jsonrpc_from_sse_buffer(buffer: &str) -> Option<JsonRpcResponse> {
    // Normalize CRLF to LF so blank-line event boundaries are detectable.
    let normalized = buffer.replace('\r', "");

    for event_block in normalized.split("\n\n") {
        let mut data_lines = Vec::new();
        for line in event_block.lines() {
            if let Some(value) = line.strip_prefix("data:") {
                data_lines.push(value.trim());
            }
        }

        let data = data_lines.join("\n");
        if !data.is_empty()
            && let Ok(Some(rpc_response)) = parse_jsonrpc_response_message(data.as_bytes())
        {
            return Some(rpc_response);
        }
    }

    None
}

#[async_trait]
impl McpTransport for StreamableHttpTransport {
    async fn request(&self, req: &JsonRpcRequest) -> Result<JsonRpcResponse, McpError> {
        let expected_id = request_id(req)?;
        let _request_id = self.register_request_id(expected_id)?;
        let body =
            serde_json::to_string(req).map_err(|e| McpError::Transport(format!("JSON serialize error: {}", e)))?;

        let http_req = self.build_request(&body).await;
        let response = http_req
            .send()
            .await
            .map_err(|e| McpError::Transport(format!("HTTP request failed: {}", e.without_url())))?;

        if !response.status().is_success() {
            return Err(McpError::Transport(format!(
                "HTTP request returned status: {}",
                response.status()
            )));
        }

        let rpc_response = self.parse_response(response).await?;
        validate_response_id(expected_id, &rpc_response)?;

        if let Some(err) = &rpc_response.error {
            return Err(McpError::JsonRpc {
                code: err.code,
                message: SERVER_ERROR_MESSAGE.into(),
            });
        }

        Ok(rpc_response)
    }

    async fn notify(&self, req: &JsonRpcRequest) -> Result<(), McpError> {
        let body =
            serde_json::to_string(req).map_err(|e| McpError::Transport(format!("JSON serialize error: {}", e)))?;

        let http_req = self.build_request(&body).await;
        http_req
            .send()
            .await
            .map_err(|e| McpError::Transport(format!("Notification request failed: {}", e.without_url())))?;

        Ok(())
    }

    async fn close(&self) -> Result<(), McpError> {
        // No persistent connection to close for HTTP
        Ok(())
    }
}

#[cfg(test)]
#[path = "streamable_http_test.rs"]
mod streamable_http_test;
