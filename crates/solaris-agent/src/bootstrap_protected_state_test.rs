use std::sync::Arc;

use serde_json::json;
use solaris_config::config::{CliArgs, Config};
use solaris_types::permission::PermissionMode;

use super::*;
use crate::output::null_sink::NullSink;

fn bootstrap_for(workspace: &Path) -> AgentBootstrap {
    let mut config = Config::resolve(&CliArgs {
        provider: Some("anthropic".into()),
        api_key: Some("test".into()),
        base_url: Some("https://provider.example.test".into()),
        model: Some("test-model".into()),
        max_tokens: None,
        thinking: None,
        thinking_budget: None,
        max_turns: None,
        max_tool_call_malformed_turns: None,
        max_tool_call_failure_turns: None,
        system_prompt: None,
        profile: None,
        auto_approve: false,
        project_dir: Some(workspace.to_path_buf()),
    })
    .unwrap();
    config.session.directory = workspace
        .join(".solaris")
        .join("sessions")
        .to_string_lossy()
        .into_owned();
    AgentBootstrap::new(config, workspace.to_string_lossy(), Arc::new(NullSink))
}

#[tokio::test]
async fn persistent_state_root_is_hidden_from_plan_and_auto_but_available_in_bypass() {
    let workspace = tempfile::tempdir().unwrap();
    let state_root = workspace.path().join(".solaris");
    let session_root = state_root.join("sessions");
    std::fs::create_dir_all(&session_root).unwrap();
    let read_path = session_root.join("read.txt");
    let edit_path = session_root.join("edit.txt");
    let write_path = session_root.join("write.txt");
    std::fs::write(&read_path, "protected-session-marker").unwrap();
    std::fs::write(&edit_path, "before-edit").unwrap();
    let mut bootstrap = bootstrap_for(workspace.path());
    bootstrap.initialize_persistent_runtime(workspace.path()).unwrap();
    let output_test_root = tempfile::tempdir_in(effect_output_state_root()).unwrap();
    let output_read_path = output_test_root.path().join("outcome-read.txt");
    let output_edit_path = output_test_root.path().join("outcome-edit.txt");
    let output_write_path = output_test_root.path().join("outcome-write.txt");
    std::fs::write(&output_read_path, "protected-outcome-marker").unwrap();
    std::fs::write(&output_edit_path, "before-outcome-edit").unwrap();
    let protected_paths = bootstrap.permission_context.protected_path_fingerprint_material();
    assert!(
        protected_paths
            .iter()
            .any(|path| { path == &format!("root:{}", effect_output_state_root().canonicalize().unwrap().display()) })
    );
    let registry = bootstrap.build_builtin_registry(workspace.path());
    let read = registry.get("Read").unwrap();
    let write = registry.get("Write").unwrap();
    let edit = registry.get("Edit").unwrap();
    let glob = registry.get("Glob").unwrap();
    let grep = registry.get("Grep").unwrap();

    for mode in [PermissionMode::Auto, PermissionMode::Plan] {
        bootstrap.permission_context.set_mode(mode);
        assert!(read.execute(json!({"file_path": read_path})).await.is_error);
        assert!(read.execute(json!({"file_path": output_read_path})).await.is_error);
        assert!(
            write
                .execute(json!({"file_path": write_path, "content": "blocked"}))
                .await
                .is_error
        );
        assert!(
            write
                .execute(json!({"file_path": output_write_path, "content": "blocked"}))
                .await
                .is_error
        );
        assert!(
            edit.execute(json!({
                "file_path": edit_path,
                "old_string": "before-edit",
                "new_string": "blocked-edit"
            }))
            .await
            .is_error
        );
        assert!(
            edit.execute(json!({
                "file_path": output_edit_path,
                "old_string": "before-outcome-edit",
                "new_string": "blocked-outcome-edit"
            }))
            .await
            .is_error
        );
        let glob_result = glob.execute(json!({"pattern": "**/*.txt", "path": "."})).await;
        assert!(!glob_result.content.contains("read.txt"));
        let grep_result = grep
            .execute(json!({"pattern": "protected-session-marker", "path": "."}))
            .await;
        assert!(!grep_result.content.contains("protected-session-marker"));
        let output_glob = glob
            .execute(json!({"pattern": "**/*.txt", "path": output_test_root.path()}))
            .await;
        assert!(!output_glob.content.contains("outcome-read.txt"));
        let output_grep = grep
            .execute(json!({
                "pattern": "protected-outcome-marker",
                "path": output_test_root.path()
            }))
            .await;
        assert!(!output_grep.content.contains("protected-outcome-marker"));
    }

    bootstrap.permission_context.set_mode(PermissionMode::Bypass);
    assert!(!read.execute(json!({"file_path": read_path})).await.is_error);
    assert!(!read.execute(json!({"file_path": edit_path})).await.is_error);
    assert!(!read.execute(json!({"file_path": output_read_path})).await.is_error);
    assert!(!read.execute(json!({"file_path": output_edit_path})).await.is_error);
    assert!(
        !write
            .execute(json!({"file_path": write_path, "content": "bypass-write"}))
            .await
            .is_error
    );
    assert!(
        !write
            .execute(json!({
                "file_path": output_write_path,
                "content": "bypass-outcome-write"
            }))
            .await
            .is_error
    );
    assert!(
        !edit
            .execute(json!({
                "file_path": edit_path,
                "old_string": "before-edit",
                "new_string": "bypass-edit"
            }))
            .await
            .is_error
    );
    assert!(
        !edit
            .execute(json!({
                "file_path": output_edit_path,
                "old_string": "before-outcome-edit",
                "new_string": "bypass-outcome-edit"
            }))
            .await
            .is_error
    );
    assert!(
        glob.execute(json!({"pattern": "**/*.txt", "path": "."}))
            .await
            .content
            .contains("read.txt")
    );
    assert!(
        grep.execute(json!({"pattern": "protected-session-marker", "path": "."}))
            .await
            .content
            .contains("read.txt")
    );
    assert!(
        glob.execute(json!({"pattern": "**/*.txt", "path": output_test_root.path()}))
            .await
            .content
            .contains("outcome-read.txt")
    );
    assert!(
        grep.execute(json!({
            "pattern": "protected-outcome-marker",
            "path": output_test_root.path()
        }))
        .await
        .content
        .contains("outcome-read.txt")
    );
}

#[tokio::test]
async fn custom_session_parent_is_the_protected_state_root() {
    let workspace = tempfile::tempdir().unwrap();
    let external_state = tempfile::tempdir().unwrap();
    let session_root = external_state.path().join("custom-state").join("sessions");
    let state_file = session_root.join("custom-session.txt");
    std::fs::create_dir_all(&session_root).unwrap();
    std::fs::write(&state_file, "custom-state-marker").unwrap();
    let mut bootstrap = bootstrap_for(workspace.path());
    bootstrap.config.session.directory = session_root.to_string_lossy().into_owned();
    bootstrap.initialize_persistent_runtime(workspace.path()).unwrap();
    let expected_root = session_root.parent().unwrap().canonicalize().unwrap();
    assert!(
        bootstrap
            .permission_context
            .protected_path_fingerprint_material()
            .contains(&format!("root:{}", expected_root.display()))
    );
    let registry = bootstrap.build_builtin_registry(workspace.path());
    let read = registry.get("Read").unwrap();

    for mode in [PermissionMode::Auto, PermissionMode::Plan] {
        bootstrap.permission_context.set_mode(mode);
        assert!(read.execute(json!({"file_path": state_file})).await.is_error);
    }
    bootstrap.permission_context.set_mode(PermissionMode::Bypass);
    assert!(!read.execute(json!({"file_path": state_file})).await.is_error);
}
