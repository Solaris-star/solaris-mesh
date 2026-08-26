use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use solaris_process::{ManagedChild, PinnedCommand};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::process::{ChildStderr, ChildStdin, ChildStdout};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use super::{McpError, McpTransport, SERVER_ERROR_MESSAGE, request_id, validate_response_id};
use crate::protocol::{JsonRpcRequest, JsonRpcResponse, parse_jsonrpc_response_message};

pub(crate) const MAX_STDIO_FRAME_BYTES: usize = 1024 * 1024;
const MAX_STDIO_STDERR_DIGEST_BYTES: usize = 64 * 1024;
const STDIO_STREAM_UNAVAILABLE_MESSAGE: &str = "MCP stdio request stream is unavailable";

#[derive(Default)]
struct StdioRequestState {
    poisoned: bool,
}

/// Stdio transport: communicates with MCP server via child process stdin/stdout
pub struct StdioTransport {
    stdin: Mutex<BufWriter<ChildStdin>>,
    stdout: Mutex<BufReader<ChildStdout>>,
    child: Mutex<ManagedChild>,
    stderr_task: Mutex<Option<JoinHandle<()>>>,
    request_state: Mutex<StdioRequestState>,
    next_id: AtomicU64,
}

impl StdioTransport {
    /// Spawn a child process and return the transport
    pub(crate) async fn spawn(
        mut cmd: PinnedCommand,
        args: &[String],
        env: &HashMap<String, String>,
    ) -> Result<Self, McpError> {
        cmd.kill_on_drop(true)
            .args(args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .envs(env);

        let mut child = cmd
            .spawn()
            .map_err(|e| McpError::Transport(format!("Failed to spawn MCP stdio server: {e}")))?;

        let stdin = child
            .take_stdin()
            .ok_or_else(|| McpError::Transport("Failed to capture child stdin".into()))?;
        let stdout = child
            .take_stdout()
            .ok_or_else(|| McpError::Transport("Failed to capture child stdout".into()))?;
        let stderr = child
            .take_stderr()
            .ok_or_else(|| McpError::Transport("Failed to capture child stderr".into()))?;
        let stderr_task = tokio::spawn(drain_stdio_stderr(stderr));

        Ok(Self {
            stdin: Mutex::new(BufWriter::new(stdin)),
            stdout: Mutex::new(BufReader::new(stdout)),
            child: Mutex::new(child),
            stderr_task: Mutex::new(Some(stderr_task)),
            request_state: Mutex::new(StdioRequestState::default()),
            next_id: AtomicU64::new(1),
        })
    }

    /// Allocate an ID for callers using the transport without `McpManager`.
    pub fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Send a JSON-RPC message (request or notification) via stdin
    async fn send(&self, req: &JsonRpcRequest) -> Result<(), McpError> {
        let json =
            serde_json::to_string(req).map_err(|e| McpError::Transport(format!("JSON serialize error: {}", e)))?;

        let mut stdin = self.stdin.lock().await;
        stdin
            .write_all(json.as_bytes())
            .await
            .map_err(|e| McpError::Transport(format!("Write to stdin failed: {}", e)))?;
        stdin
            .write_all(b"\n")
            .await
            .map_err(|e| McpError::Transport(format!("Write newline failed: {}", e)))?;
        stdin
            .flush()
            .await
            .map_err(|e| McpError::Transport(format!("Flush stdin failed: {}", e)))?;
        Ok(())
    }

    /// Read a single JSON-RPC response from stdout
    async fn read_response(&self) -> Result<JsonRpcResponse, McpError> {
        let mut stdout = self.stdout.lock().await;
        let mut frame = Vec::new();

        // Read lines until we get a non-empty one (skip blank lines)
        loop {
            frame.clear();
            loop {
                let available = stdout
                    .fill_buf()
                    .await
                    .map_err(|e| McpError::Transport(format!("Read from stdout failed: {e}")))?;
                if available.is_empty() {
                    return Err(McpError::Transport("Child process stdout closed".into()));
                }
                let newline = available.iter().position(|byte| *byte == b'\n');
                let consumed = newline.map_or(available.len(), |index| index + 1);
                let payload_len = newline.unwrap_or(consumed);
                if frame.len().saturating_add(payload_len) > MAX_STDIO_FRAME_BYTES {
                    drop(stdout);
                    self.child.lock().await.kill().await.map_err(|error| {
                        McpError::Transport(format!("Failed to terminate oversized MCP frame: {error}"))
                    })?;
                    return Err(McpError::Transport(format!(
                        "MCP stdio frame exceeded {MAX_STDIO_FRAME_BYTES} bytes"
                    )));
                }
                frame.extend_from_slice(&available[..payload_len]);
                stdout.consume(consumed);
                if newline.is_some() {
                    break;
                }
            }

            let trimmed = frame.strip_suffix(b"\r").unwrap_or(&frame);
            if !trimmed.is_empty() {
                match parse_jsonrpc_response_message(trimmed) {
                    Ok(Some(response)) => return Ok(response),
                    Ok(None) => continue,
                    Err(_) => {
                        let mut hasher = Sha256::new();
                        hasher.update(b"solaris.mcp/invalid-stdio-frame/v1\0");
                        hasher.update(trimmed);
                        return Err(McpError::Transport(format!(
                            "Failed to parse JSON-RPC response; bytes={}; digest=sha256:{:x}",
                            trimmed.len(),
                            hasher.finalize()
                        )));
                    }
                }
            }
        }
    }
}

async fn drain_stdio_stderr(mut stderr: ChildStderr) {
    let mut hasher = Sha256::new();
    hasher.update(b"solaris.mcp/stdio-stderr/v1\0");
    let mut total_bytes = 0_u64;
    let mut hashed_bytes = 0_usize;
    let mut error_kind = None;
    let mut buffer = [0_u8; 8192];
    loop {
        match stderr.read(&mut buffer).await {
            Ok(0) => break,
            Ok(read) => {
                total_bytes = total_bytes.saturating_add(read as u64);
                let digest_bytes = read.min(MAX_STDIO_STDERR_DIGEST_BYTES.saturating_sub(hashed_bytes));
                hasher.update(&buffer[..digest_bytes]);
                hashed_bytes += digest_bytes;
            }
            Err(_) => {
                error_kind = Some("read");
                break;
            }
        }
    }
    let truncated = total_bytes > hashed_bytes as u64;
    let digest = format!("sha256:{:x}", hasher.finalize());
    if let Some(error_kind) = error_kind {
        tracing::warn!(
            target: "solaris_mcp",
            stderr_error_kind = error_kind,
            stderr_bytes = total_bytes,
            stderr_digest = %digest,
            stderr_digest_truncated = truncated,
            "MCP stdio stderr drain failed"
        );
    } else if total_bytes > 0 {
        tracing::debug!(
            target: "solaris_mcp",
            stderr_error_kind = "none",
            stderr_bytes = total_bytes,
            stderr_digest = %digest,
            stderr_digest_truncated = truncated,
            "MCP stdio stderr captured"
        );
    }
}

#[async_trait]
impl McpTransport for StdioTransport {
    async fn request(&self, req: &JsonRpcRequest) -> Result<JsonRpcResponse, McpError> {
        let expected_id = request_id(req)?;
        let mut request_state = self.request_state.lock().await;
        if request_state.poisoned {
            return Err(McpError::Transport(STDIO_STREAM_UNAVAILABLE_MESSAGE.into()));
        }
        request_state.poisoned = true;
        self.send(req).await?;
        let response = self.read_response().await?;
        validate_response_id(expected_id, &response)?;
        request_state.poisoned = false;

        // Check for JSON-RPC error in response
        if let Some(err) = &response.error {
            return Err(McpError::JsonRpc {
                code: err.code,
                message: SERVER_ERROR_MESSAGE.into(),
            });
        }

        Ok(response)
    }

    async fn notify(&self, req: &JsonRpcRequest) -> Result<(), McpError> {
        let mut request_state = self.request_state.lock().await;
        if request_state.poisoned {
            return Err(McpError::Transport(STDIO_STREAM_UNAVAILABLE_MESSAGE.into()));
        }
        request_state.poisoned = true;
        self.send(req).await?;
        request_state.poisoned = false;
        Ok(())
    }

    async fn close(&self) -> Result<(), McpError> {
        // Drop stdin to signal EOF, then wait for child
        let mut child = self.child.lock().await;
        // kill the child process gracefully
        let _ = child.shutdown().await;
        drop(child);
        if let Some(mut stderr_task) = self.stderr_task.lock().await.take()
            && tokio::time::timeout(std::time::Duration::from_secs(1), &mut stderr_task)
                .await
                .is_err()
        {
            stderr_task.abort();
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "stdio_test.rs"]
mod stdio_test;
