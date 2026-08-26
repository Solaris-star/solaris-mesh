use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderValue};
use tokio::sync::oneshot;

use super::{
    DUPLICATE_REQUEST_ID_MESSAGE, McpError, McpTransport, RESPONSE_ID_MISMATCH_MESSAGE, SERVER_ERROR_MESSAGE,
    redirect_safe_client, request_id, validate_response_id,
};
use crate::protocol::{JsonRpcRequest, JsonRpcResponse, parse_jsonrpc_response_message};

type PendingResponse = Result<JsonRpcResponse, McpError>;
type PendingSender = oneshot::Sender<PendingResponse>;
type PendingResponses = Arc<Mutex<PendingState>>;

const MAX_RETIRED_REQUEST_IDS: usize = 1_024;

#[derive(Default)]
struct PendingState {
    responses: HashMap<u64, PendingSender>,
    retired: VecDeque<u64>,
}

impl PendingState {
    fn retire(&mut self, request_id: u64) {
        if self.retired.contains(&request_id) {
            return;
        }
        self.retired.push_back(request_id);
        if self.retired.len() > MAX_RETIRED_REQUEST_IDS {
            self.retired.pop_front();
        }
    }

    fn drain_and_retire(&mut self) -> Vec<(u64, PendingSender)> {
        let responses: Vec<_> = self.responses.drain().collect();
        for (request_id, _) in &responses {
            self.retire(*request_id);
        }
        responses
    }
}

fn register_pending_response(
    pending: &mut PendingState,
    request_id: u64,
    sender: PendingSender,
) -> Result<(), McpError> {
    if pending.retired.contains(&request_id) {
        return Err(McpError::Transport(DUPLICATE_REQUEST_ID_MESSAGE.into()));
    }
    match pending.responses.entry(request_id) {
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(sender);
            Ok(())
        }
        std::collections::hash_map::Entry::Occupied(_) => Err(McpError::Transport(DUPLICATE_REQUEST_ID_MESSAGE.into())),
    }
}

fn reject_pending_responses(pending: &PendingResponses, message: &'static str) {
    let responses = {
        let Ok(mut pending) = pending.lock() else {
            return;
        };
        pending.drain_and_retire()
    };
    for (_, sender) in responses {
        let _ = sender.send(Err(McpError::Transport(message.into())));
    }
}

fn dispatch_response(pending: &PendingResponses, response: JsonRpcResponse) {
    enum Dispatch {
        Deliver(PendingSender),
        Ignore,
        Reject(Vec<(u64, PendingSender)>),
    }

    let dispatch = {
        let Ok(mut pending) = pending.lock() else {
            return;
        };
        match response.id {
            Some(id) => match pending.responses.remove(&id) {
                Some(sender) => {
                    pending.retire(id);
                    Dispatch::Deliver(sender)
                }
                None if pending.retired.contains(&id) || pending.responses.is_empty() => Dispatch::Ignore,
                None => Dispatch::Reject(pending.drain_and_retire()),
            },
            None if pending.responses.is_empty() => Dispatch::Ignore,
            None => Dispatch::Reject(pending.drain_and_retire()),
        }
    };

    match dispatch {
        Dispatch::Deliver(sender) => {
            let _ = sender.send(Ok(response));
        }
        Dispatch::Ignore => {}
        Dispatch::Reject(responses) => {
            for (_, sender) in responses {
                let _ = sender.send(Err(McpError::Transport(RESPONSE_ID_MISMATCH_MESSAGE.into())));
            }
        }
    }
}

fn dispatch_sse_event(pending: &PendingResponses, event_type: &str, event_data: &str) {
    if (event_type != "message" && !event_type.is_empty()) || event_data.is_empty() {
        return;
    }
    match parse_jsonrpc_response_message(event_data.as_bytes()) {
        Ok(Some(response)) => dispatch_response(pending, response),
        Ok(None) => {}
        Err(_) => reject_pending_responses(pending, "MCP SSE response was invalid"),
    }
}

struct PendingRegistration {
    pending: PendingResponses,
    request_id: u64,
}

impl Drop for PendingRegistration {
    fn drop(&mut self) {
        if let Ok(mut pending) = self.pending.lock()
            && pending.responses.remove(&self.request_id).is_some()
        {
            pending.retire(self.request_id);
        }
    }
}

/// SSE transport: connects to an SSE endpoint for server→client events,
/// sends requests via POST to the endpoint URL received from the SSE stream
pub struct SseTransport {
    client: reqwest::Client,
    /// The POST endpoint URL (received from the SSE stream's "endpoint" event)
    post_url: String,
    headers: HeaderMap,
    /// Pending request-response channels, keyed by JSON-RPC id
    pending: PendingResponses,
    next_id: AtomicU64,
    /// Handle to the background SSE listener task
    _listener: tokio::task::JoinHandle<()>,
}

impl SseTransport {
    /// Connect to an SSE MCP server
    pub async fn connect(url: &str, headers: &HashMap<String, String>) -> Result<Self, McpError> {
        let mut header_map = HeaderMap::new();
        for (k, v) in headers {
            let name = reqwest::header::HeaderName::from_bytes(k.as_bytes())
                .map_err(|e| McpError::Transport(format!("Invalid header name '{}': {}", k, e)))?;
            let value = HeaderValue::from_str(v)
                .map_err(|e| McpError::Transport(format!("Invalid value for header '{}': {}", k, e)))?;
            header_map.insert(name, value);
        }

        let client = redirect_safe_client(url)?;
        let origin_url =
            reqwest::Url::parse(url).map_err(|error| McpError::Transport(format!("Invalid SSE URL: {error}")))?;

        // GET the SSE endpoint to establish the event stream
        let response = client
            .get(url)
            .headers(header_map.clone())
            .header("Accept", "text/event-stream")
            .send()
            .await
            .map_err(|e| McpError::Transport(format!("SSE connection failed: {}", e.without_url())))?;

        if !response.status().is_success() {
            return Err(McpError::Transport(format!(
                "SSE connection returned status: {}",
                response.status()
            )));
        }

        let pending: PendingResponses = Arc::new(Mutex::new(PendingState::default()));

        // Parse the SSE stream to find the endpoint URL
        // The server sends an "endpoint" event with the POST URL
        let mut bytes_stream = response.bytes_stream();
        let mut buffer = String::new();
        let mut post_url: Option<String> = None;

        use futures::StreamExt;
        // Read initial events to get the endpoint URL
        while let Some(chunk) = bytes_stream.next().await {
            let chunk = chunk.map_err(|e| McpError::Transport(format!("SSE read error: {}", e.without_url())))?;
            // Normalize CRLF to LF: Python `fastmcp` / MCP SDK servers emit
            // `\r\n` separators (via `sse-starlette`), so event boundaries are
            // `\r\n\r\n` and would not match a `\n\n` search otherwise.
            buffer.push_str(&String::from_utf8_lossy(&chunk).replace('\r', ""));

            // Parse SSE events from buffer
            while let Some(event_end) = buffer.find("\n\n") {
                let event_block = buffer[..event_end].to_string();
                buffer = buffer[event_end + 2..].to_string();

                let (event_type, event_data) = parse_sse_event(&event_block);

                if event_type == "endpoint" {
                    post_url = Some(resolve_same_origin_endpoint(&origin_url, &event_data)?);
                    break;
                }
            }

            if post_url.is_some() {
                break;
            }
        }

        let post_url = post_url.ok_or_else(|| McpError::Transport("No endpoint event received from SSE".into()))?;

        // Spawn background task to listen for SSE responses
        let pending_clone = pending.clone();
        let listener = tokio::spawn(async move {
            let mut buf = buffer; // carry over remaining buffer
            while let Some(chunk) = bytes_stream.next().await {
                let Ok(chunk) = chunk else { break };
                buf.push_str(&String::from_utf8_lossy(&chunk).replace('\r', ""));

                while let Some(event_end) = buf.find("\n\n") {
                    let event_block = buf[..event_end].to_string();
                    buf = buf[event_end + 2..].to_string();

                    let (event_type, event_data) = parse_sse_event(&event_block);

                    dispatch_sse_event(&pending_clone, &event_type, &event_data);
                }
            }
            reject_pending_responses(&pending_clone, "MCP SSE stream closed");
        });

        Ok(Self {
            client,
            post_url,
            headers: header_map,
            pending,
            next_id: AtomicU64::new(1),
            _listener: listener,
        })
    }

    /// Allocate an ID for callers using the transport without `McpManager`.
    pub fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }
}

#[async_trait]
impl McpTransport for SseTransport {
    async fn request(&self, req: &JsonRpcRequest) -> Result<JsonRpcResponse, McpError> {
        let req_id = request_id(req)?;

        // Set up response channel before sending
        let (tx, rx) = oneshot::channel::<PendingResponse>();
        {
            let mut pending = self
                .pending
                .lock()
                .map_err(|_| McpError::Transport("MCP pending request state is unavailable".into()))?;
            register_pending_response(&mut pending, req_id, tx)?;
        }
        let _registration = PendingRegistration {
            pending: Arc::clone(&self.pending),
            request_id: req_id,
        };

        // POST the request
        let body =
            serde_json::to_string(req).map_err(|e| McpError::Transport(format!("JSON serialize error: {}", e)))?;

        let response = self
            .client
            .post(&self.post_url)
            .headers(self.headers.clone())
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await
            .map_err(|e| McpError::Transport(format!("POST request failed: {}", e.without_url())))?;

        if !response.status().is_success() {
            return Err(McpError::Transport(format!(
                "POST returned status: {}",
                response.status()
            )));
        }

        // Wait for response from SSE stream
        let rpc_response = rx
            .await
            .map_err(|_| McpError::Transport("Response channel closed unexpectedly".into()))??;
        validate_response_id(req_id, &rpc_response)?;

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

        self.client
            .post(&self.post_url)
            .headers(self.headers.clone())
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await
            .map_err(|e| McpError::Transport(format!("Notification POST failed: {}", e.without_url())))?;

        Ok(())
    }

    async fn close(&self) -> Result<(), McpError> {
        self._listener.abort();
        reject_pending_responses(&self.pending, "MCP SSE transport closed");
        Ok(())
    }
}

/// Parse a single SSE event block into (event_type, data)
fn parse_sse_event(block: &str) -> (String, String) {
    let mut event_type = String::new();
    let mut data_lines = Vec::new();

    for line in block.lines() {
        if let Some(value) = line.strip_prefix("event:") {
            event_type = value.trim().to_string();
        } else if let Some(value) = line.strip_prefix("data:") {
            data_lines.push(value.trim().to_string());
        }
    }

    (event_type, data_lines.join("\n"))
}

fn resolve_same_origin_endpoint(origin: &reqwest::Url, endpoint: &str) -> Result<String, McpError> {
    let resolved = origin
        .join(endpoint)
        .map_err(|error| McpError::Transport(format!("Invalid SSE endpoint URL: {error}")))?;
    let same_origin = resolved.scheme() == origin.scheme()
        && resolved.host_str() == origin.host_str()
        && resolved.port_or_known_default() == origin.port_or_known_default();
    if !same_origin {
        return Err(McpError::Transport("SSE endpoint crosses the approved origin".into()));
    }
    Ok(resolved.to_string())
}

#[cfg(test)]
#[path = "sse_test.rs"]
mod sse_test;
