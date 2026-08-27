use std::path::{Path, PathBuf};
use std::time::Duration;

use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::schema::v1::{
    CancelNotification, CloseSessionRequest, ContentBlock, Implementation, InitializeRequest, LoadSessionRequest,
    NewSessionRequest, PromptRequest, SetSessionConfigOptionRequest, TextContent,
};
use serde_json::{Value, json};
use tempfile::tempdir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

type StdoutLines = Lines<BufReader<ChildStdout>>;

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("solaris-cli must remain inside the workspace crates directory")
        .to_path_buf()
}

fn manifest_acp_args() -> Vec<String> {
    let manifest: Value = serde_json::from_slice(
        &std::fs::read(repository_root().join("solaris-extension.json")).expect("extension manifest must exist"),
    )
    .expect("extension manifest must be valid JSON");
    let adapter = &manifest["contributes"]["acpAdapters"][0];
    assert_eq!(adapter["cliCommand"], "solaris");
    adapter["acpArgs"]
        .as_array()
        .expect("manifest ACP args")
        .iter()
        .map(|value| value.as_str().expect("ACP arg is a string").to_owned())
        .collect()
}

async fn send_request(stdin: &mut ChildStdin, id: u64, method: &str, params: Value) {
    let request = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
    stdin
        .write_all(format!("{}\n", serde_json::to_string(&request).expect("request serializes")).as_bytes())
        .await
        .expect("ACP stdin accepts request");
    stdin.flush().await.expect("ACP request flushes");
}

async fn send_notification(stdin: &mut ChildStdin, method: &str, params: Value) {
    let notification = json!({"jsonrpc": "2.0", "method": method, "params": params});
    stdin
        .write_all(
            format!(
                "{}\n",
                serde_json::to_string(&notification).expect("notification serializes")
            )
            .as_bytes(),
        )
        .await
        .expect("ACP stdin accepts notification");
    stdin.flush().await.expect("ACP notification flushes");
}

async fn read_response(lines: &mut StdoutLines, id: u64) -> Value {
    loop {
        let line = tokio::time::timeout(Duration::from_secs(10), lines.next_line())
            .await
            .expect("ACP response did not arrive in time")
            .expect("ACP stdout read failed")
            .expect("ACP server closed stdout before response");
        let value: Value = serde_json::from_str(&line).expect("ACP response is JSON");
        if value.get("id").and_then(Value::as_u64) == Some(id) {
            return value;
        }
    }
}

fn assert_success(response: &Value, id: u64) {
    assert_eq!(response.get("id").and_then(Value::as_u64), Some(id), "{response}");
    assert!(response.get("result").is_some(), "ACP request failed: {response}");
    assert!(
        response.get("error").is_none(),
        "ACP request returned an error: {response}"
    );
}

async fn stop_child(mut child: Child) {
    let status = tokio::time::timeout(Duration::from_secs(10), child.wait())
        .await
        .expect("ACP process did not exit after stdin closed")
        .expect("ACP process wait failed");
    assert!(status.success(), "ACP process exited unsuccessfully: {status}");
}

#[tokio::test]
async fn acp_stdio_lifecycle_runs_without_a_real_provider() {
    let workspace = tempdir().expect("temporary ACP workspace");
    std::fs::write(
        workspace.path().join(".solaris.toml"),
        "[session]\ndirectory = \"sessions\"\n",
    )
    .expect("write isolated ACP config");

    let mut command = Command::new(env!("CARGO_BIN_EXE_solaris"));
    command
        .arg("--provider")
        .arg("anthropic")
        .arg("--api-key")
        .arg("test-key")
        .arg("--base-url")
        .arg("http://127.0.0.1:9/v1")
        .arg("--model")
        .arg("test-model")
        .arg("--project-dir")
        .arg(workspace.path())
        .arg("--max-turns")
        .arg("1")
        .args(manifest_acp_args());
    let mut child = command
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn Solaris ACP process");
    let mut stdin = child.stdin.take().expect("ACP stdin");
    let stdout = child.stdout.take().expect("ACP stdout");
    let mut lines = BufReader::new(stdout).lines();

    let initialize =
        InitializeRequest::new(ProtocolVersion::V1).client_info(Implementation::new("solaris-acp-test", "0.1"));
    send_request(&mut stdin, 1, "initialize", serde_json::to_value(initialize).unwrap()).await;
    let response = read_response(&mut lines, 1).await;
    assert_success(&response, 1);
    assert_eq!(response["result"]["protocolVersion"], 1);
    assert_eq!(response["result"]["agentInfo"]["name"], "solaris-mesh");

    let new_session = NewSessionRequest::new(workspace.path());
    send_request(&mut stdin, 2, "session/new", serde_json::to_value(new_session).unwrap()).await;
    let response = read_response(&mut lines, 2).await;
    assert_success(&response, 2);
    let session_id = response["result"]["sessionId"]
        .as_str()
        .expect("session/new returns a session id")
        .to_owned();
    assert!(response["result"]["configOptions"].is_array());

    let config_update = SetSessionConfigOptionRequest::new(session_id.clone(), "multi_agent_policy", "proactive");
    send_request(
        &mut stdin,
        3,
        "session/set_config_option",
        serde_json::to_value(config_update).unwrap(),
    )
    .await;
    let response = read_response(&mut lines, 3).await;
    assert_success(&response, 3);
    assert!(response["result"]["configOptions"].as_array().is_some_and(|options| {
        options
            .iter()
            .any(|option| option["id"] == "multi_agent_policy" && option["currentValue"] == "proactive")
    }));

    // Cancel is sent before prompt starts. The server must consume this
    // durable cancellation rather than contacting the configured provider.
    send_notification(
        &mut stdin,
        "session/cancel",
        serde_json::to_value(CancelNotification::new(session_id.clone())).unwrap(),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let prompt = PromptRequest::new(
        session_id.clone(),
        vec![ContentBlock::Text(TextContent::new("cancel before provider"))],
    );
    send_request(&mut stdin, 4, "session/prompt", serde_json::to_value(prompt).unwrap()).await;
    let response = read_response(&mut lines, 4).await;
    assert_success(&response, 4);
    assert_eq!(response["result"]["stopReason"], "cancelled");

    send_request(
        &mut stdin,
        5,
        "session/close",
        serde_json::to_value(CloseSessionRequest::new(session_id.clone())).unwrap(),
    )
    .await;
    assert_success(&read_response(&mut lines, 5).await, 5);

    let load_session = LoadSessionRequest::new(session_id.clone(), Path::new(workspace.path()));
    send_request(
        &mut stdin,
        6,
        "session/load",
        serde_json::to_value(load_session).unwrap(),
    )
    .await;
    let response = read_response(&mut lines, 6).await;
    assert_success(&response, 6);

    send_request(
        &mut stdin,
        7,
        "session/close",
        serde_json::to_value(CloseSessionRequest::new(session_id)).unwrap(),
    )
    .await;
    assert_success(&read_response(&mut lines, 7).await, 7);

    drop(stdin);
    drop(lines);
    stop_child(child).await;
}
