use std::sync::Arc;

use solaris_config::config::{CliArgs, Config};
use tempfile::tempdir;

use super::*;
use crate::output::null_sink::NullSink;

#[tokio::test]
async fn resume_claims_session_before_initializing_runtime_state() {
    let workspace = tempdir().unwrap();
    let sessions = workspace.path().join(".solaris").join("sessions");
    let holder = SessionManager::new(sessions.clone(), 20);
    let session = holder
        .create(
            "provider",
            "model",
            &workspace.path().to_string_lossy(),
            Some("early-fence"),
        )
        .unwrap();
    holder.load_active_session("early-fence").unwrap();

    let invalid_ledger = workspace.path().join(".solaris").join("runtime").join("ledger.sqlite3");
    std::fs::create_dir_all(&invalid_ledger).unwrap();
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
        project_dir: Some(workspace.path().to_path_buf()),
    })
    .unwrap();
    config.session.enabled = true;
    config.session.directory = sessions.to_string_lossy().into_owned();

    let result = AgentBootstrap::new(config, workspace.path().to_string_lossy(), Arc::new(NullSink))
        .resume(session)
        .build()
        .await;
    let error = match result {
        Ok(_) => panic!("concurrent resume unexpectedly succeeded"),
        Err(error) => error,
    };

    assert!(error.to_string().contains("active under owner"), "{error:#}");
    holder.release_active_session().unwrap();
}
