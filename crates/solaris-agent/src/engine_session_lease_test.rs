use std::env;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use rusqlite::Connection;
use solaris_config::config::{CliArgs, Config};
use solaris_providers::error::ProviderError;
use solaris_providers::provider::LlmProvider;
use solaris_tools::registry::ToolRegistry;
use solaris_types::llm::{LlmEvent, LlmRequest};
use solaris_types::message::{StopReason, TokenUsage};
use tempfile::tempdir;

use super::AgentEngine;
use crate::output::null_sink::NullSink;
use crate::session::SessionManager;

const FIXTURE_MODE: &str = "SOLARIS_ENGINE_LEASE_FIXTURE_MODE";
const FIXTURE_WORKSPACE: &str = "SOLARIS_ENGINE_LEASE_FIXTURE_WORKSPACE";
const FIXTURE_SESSIONS: &str = "SOLARIS_ENGINE_LEASE_FIXTURE_SESSIONS";
const FIXTURE_SESSION_ID: &str = "SOLARIS_ENGINE_LEASE_FIXTURE_SESSION_ID";
const FIXTURE_START: &str = "SOLARIS_ENGINE_LEASE_FIXTURE_START";
const FIXTURE_FINISH: &str = "SOLARIS_ENGINE_LEASE_FIXTURE_FINISH";
const FIXTURE_RESULT: &str = "SOLARIS_ENGINE_LEASE_FIXTURE_RESULT";
const FIXTURE_PROVIDER_CALLED: &str = "SOLARIS_ENGINE_LEASE_FIXTURE_PROVIDER_CALLED";

struct MarkerProvider {
    marker: PathBuf,
}

#[async_trait]
impl LlmProvider for MarkerProvider {
    async fn stream(&self, _: &LlmRequest) -> Result<tokio::sync::mpsc::Receiver<LlmEvent>, ProviderError> {
        fs::write(&self.marker, b"called").unwrap();
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        sender.send(LlmEvent::TextDelta("done".to_owned())).await.unwrap();
        sender
            .send(LlmEvent::Done {
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::default(),
            })
            .await
            .unwrap();
        Ok(receiver)
    }
}

#[test]
fn real_processes_allow_only_one_agent_to_execute_a_session() {
    let workspace = tempdir().unwrap();
    let sessions = workspace.path().join("session-state");
    let manager = SessionManager::new(sessions.clone(), 20);
    manager
        .create(
            "openai",
            "model",
            &workspace.path().to_string_lossy(),
            Some("shared-agent"),
        )
        .unwrap();
    let start = workspace.path().join("start");
    let finish = workspace.path().join("finish");
    let mut children = Vec::new();
    let mut results = Vec::new();
    let mut provider_markers = Vec::new();
    for index in 0..3 {
        let result = workspace.path().join(format!("result-{index}"));
        let provider_marker = workspace.path().join(format!("provider-{index}"));
        children.push(spawn_fixture(
            workspace.path(),
            &sessions,
            &start,
            &finish,
            &result,
            &provider_marker,
        ));
        results.push(result);
        provider_markers.push(provider_marker);
    }
    fs::write(&start, b"go").unwrap();
    wait_for_results(&results, &mut children);

    let outcomes: Vec<_> = results.iter().map(|path| fs::read_to_string(path).unwrap()).collect();
    fs::write(&finish, b"done").unwrap();
    wait_for_children(children);

    assert_eq!(
        outcomes.iter().filter(|outcome| outcome.as_str() == "executed").count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| outcome.starts_with("blocked:"))
            .count(),
        2
    );
    assert_eq!(provider_markers.iter().filter(|path| path.is_file()).count(), 1);
}

#[tokio::test]
async fn lease_takeover_blocks_provider_side_effect() {
    let workspace = tempdir().unwrap();
    let sessions = workspace.path().join("session-state");
    let provider_marker = workspace.path().join("provider-called");
    let provider = Arc::new(MarkerProvider {
        marker: provider_marker.clone(),
    });
    let mut engine = AgentEngine::new_with_provider(
        provider,
        config_for(workspace.path(), &sessions),
        ToolRegistry::new(),
        Arc::new(NullSink),
        workspace.path().to_path_buf(),
    );
    engine
        .init_session("openai", &workspace.path().to_string_lossy(), Some("fenced-engine"))
        .unwrap();
    let connection = Connection::open(sessions.join("session.sqlite3")).unwrap();
    connection
        .execute(
            "UPDATE session_leases SET heartbeat_at_ms = 0, expires_at_ms = 0
             WHERE session_id = 'fenced-engine'",
            [],
        )
        .unwrap();
    drop(connection);
    let replacement = SessionManager::new(sessions, 20);
    replacement.load_active_session("fenced-engine").unwrap();

    let error = engine.run("must not execute", "fenced-run").await.unwrap_err();

    assert!(error.to_string().contains("lease"));
    assert!(!provider_marker.exists());
    replacement.release_active_session().unwrap();
}

#[test]
fn engine_lease_process_fixture() {
    if env::var_os(FIXTURE_MODE).is_none() {
        return;
    }
    let workspace = PathBuf::from(env::var_os(FIXTURE_WORKSPACE).unwrap());
    let sessions = PathBuf::from(env::var_os(FIXTURE_SESSIONS).unwrap());
    let session_id = env::var(FIXTURE_SESSION_ID).unwrap();
    let start = PathBuf::from(env::var_os(FIXTURE_START).unwrap());
    let finish = PathBuf::from(env::var_os(FIXTURE_FINISH).unwrap());
    let result = PathBuf::from(env::var_os(FIXTURE_RESULT).unwrap());
    let provider_marker = PathBuf::from(env::var_os(FIXTURE_PROVIDER_CALLED).unwrap());
    wait_for_path(&start, Duration::from_secs(10));
    let session = SessionManager::new(sessions.clone(), 20).load(&session_id).unwrap();
    let provider = Arc::new(MarkerProvider {
        marker: provider_marker,
    });
    let mut engine = AgentEngine::resume_with_provider(
        provider,
        config_for(&workspace, &sessions),
        ToolRegistry::new(),
        Arc::new(NullSink),
        session,
        workspace,
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    match runtime.block_on(engine.run("execute once", "lease-process")) {
        Ok(_) => {
            fs::write(&result, b"executed").unwrap();
            wait_for_path(&finish, Duration::from_secs(15));
        }
        Err(error) => fs::write(&result, format!("blocked:{error}")).unwrap(),
    }
}

fn config_for(workspace: &Path, sessions: &Path) -> Config {
    let mut config = Config::resolve(&CliArgs {
        provider: Some("openai".to_owned()),
        api_key: Some("test-key".to_owned()),
        base_url: Some("https://provider.example.test/v1".to_owned()),
        model: Some("model".to_owned()),
        max_tokens: None,
        thinking: None,
        thinking_budget: None,
        max_turns: None,
        max_tool_call_malformed_turns: None,
        max_tool_call_failure_turns: None,
        system_prompt: None,
        profile: None,
        auto_approve: true,
        project_dir: Some(workspace.to_path_buf()),
    })
    .unwrap();
    config.session.enabled = true;
    config.session.directory = sessions.to_string_lossy().into_owned();
    config
}

fn spawn_fixture(
    workspace: &Path,
    sessions: &Path,
    start: &Path,
    finish: &Path,
    result: &Path,
    provider_marker: &Path,
) -> Child {
    Command::new(env::current_exe().unwrap())
        .arg("--exact")
        .arg("engine::engine_session_lease_test::engine_lease_process_fixture")
        .arg("--nocapture")
        .env(FIXTURE_MODE, "1")
        .env(FIXTURE_WORKSPACE, workspace)
        .env(FIXTURE_SESSIONS, sessions)
        .env(FIXTURE_SESSION_ID, "shared-agent")
        .env(FIXTURE_START, start)
        .env(FIXTURE_FINISH, finish)
        .env(FIXTURE_RESULT, result)
        .env(FIXTURE_PROVIDER_CALLED, provider_marker)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

fn wait_for_results(paths: &[PathBuf], children: &mut [Child]) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while paths.iter().any(|path| !path.is_file()) {
        let early_exit = children.iter_mut().enumerate().find_map(|(index, child)| {
            (!paths[index].is_file())
                .then(|| child.try_wait().unwrap().map(|status| (index, status)))
                .flatten()
        });
        if let Some((failed_index, failed_status)) = early_exit {
            let diagnostics = terminate_and_collect(children);
            panic!(
                "fixture {failed_index} exited before writing a result with {failed_status}; {}",
                diagnostics.join("; ")
            );
        }
        if Instant::now() >= deadline {
            let diagnostics = terminate_and_collect(children);
            panic!("timed out waiting for process results; {}", diagnostics.join("; "));
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn terminate_and_collect(children: &mut [Child]) -> Vec<String> {
    children
        .iter_mut()
        .enumerate()
        .map(|(index, child)| {
            if child.try_wait().unwrap().is_none() {
                child.kill().unwrap();
                let _ = child.wait().unwrap();
            }
            format!("fixture {index}: {}", read_child_stderr(child))
        })
        .collect()
}

fn wait_for_path(path: &Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while !path.is_file() {
        assert!(Instant::now() < deadline, "timed out waiting for fixture flag");
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_children(children: Vec<Child>) {
    let failures: Vec<_> = children.into_iter().filter_map(wait_for_child).collect();
    if !failures.is_empty() {
        panic!("fixture failures: {}", failures.join("; "));
    }
}

fn wait_for_child(mut child: Child) -> Option<String> {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            let stderr = read_child_stderr(&mut child);
            return (!status.success()).then(|| format!("fixture exited with {status}; stderr: {stderr}"));
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            let status = child.wait().unwrap();
            let stderr = read_child_stderr(&mut child);
            return Some(format!(
                "fixture timed out and was terminated with {status}; stderr: {stderr}"
            ));
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn read_child_stderr(child: &mut Child) -> String {
    let mut stderr = Vec::new();
    if let Some(mut stream) = child.stderr.take() {
        stream.read_to_end(&mut stderr).unwrap();
    }
    String::from_utf8_lossy(&stderr).into_owned()
}
