mod client;
pub mod sse;
pub mod stdio;
pub mod streamable_http;
mod types;

pub use types::{McpError, McpTransport};

use crate::protocol::{JsonRpcRequest, JsonRpcResponse};

use client::redirect_safe_client;

pub(crate) const DUPLICATE_REQUEST_ID_MESSAGE: &str = "MCP request id is already in flight";
const MISSING_REQUEST_ID_MESSAGE: &str = "MCP request id is missing";
pub(crate) const RESPONSE_ID_MISMATCH_MESSAGE: &str = "MCP response id did not match request";
pub(crate) const SERVER_ERROR_MESSAGE: &str = "MCP server returned an error";

pub(crate) fn request_id(request: &JsonRpcRequest) -> Result<u64, McpError> {
    request
        .id
        .ok_or_else(|| McpError::Transport(MISSING_REQUEST_ID_MESSAGE.into()))
}

pub(crate) fn validate_response_id(expected: u64, response: &JsonRpcResponse) -> Result<(), McpError> {
    if response.id == Some(expected) {
        Ok(())
    } else {
        Err(McpError::Transport(RESPONSE_ID_MISMATCH_MESSAGE.into()))
    }
}
