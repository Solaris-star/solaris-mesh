use std::path::PathBuf;
use std::sync::Arc;

use serde_json::json;

use solaris_skills::permissions::SkillPermissionChecker;
use solaris_skills::types::{ExecutionContext, LoadedFrom, SkillMetadata, SkillSource};
use solaris_tools::Tool;

use super::{SkillTool, inspect_skill_shell};

fn base_skill(name: &str, source: SkillSource, hooks_raw: Option<serde_json::Value>) -> SkillMetadata {
    SkillMetadata {
        name: name.to_string(),
        display_name: None,
        description: format!("desc of {name}"),
        has_user_specified_description: true,
        allowed_tools: vec![],
        argument_hint: None,
        argument_names: vec![],
        when_to_use: None,
        version: None,
        model: None,
        disable_model_invocation: false,
        user_invocable: true,
        execution_context: ExecutionContext::Inline,
        agent: None,
        effort: None,
        shell: None,
        paths: vec![],
        network: Default::default(),
        hooks_raw,
        source,
        loaded_from: LoadedFrom::Skills,
        content: "body".to_string(),
        content_length: 4,
        skill_root: None,
    }
}

fn tool_with(skills: Vec<SkillMetadata>) -> SkillTool {
    SkillTool::new(
        Arc::new(skills),
        PathBuf::from("/tmp"),
        SkillPermissionChecker::new(vec![], vec![], false),
    )
}

fn valid_hooks_json() -> serde_json::Value {
    json!({
        "PreToolUse": [{"hooks": [{"type": "command", "command": "echo pre"}]}]
    })
}

// TC-11.40: skill with valid hooks_raw returns Some(HooksConfig)
#[test]
fn tc_11_40_skill_with_hooks_returns_some() {
    let skill = base_skill("my-skill", SkillSource::User, Some(valid_hooks_json()));
    let tool = tool_with(vec![skill]);
    let result = tool.skill_hooks_for(&json!({"skill": "my-skill"}));
    assert!(result.is_some(), "TC-11.40: skill with valid hooks must return Some");
    let config = result.unwrap();
    assert!(
        !config.pre_tool_use.is_empty(),
        "TC-11.40: pre_tool_use must be non-empty"
    );
}

// TC-11.41: skill without hooks_raw returns None
#[test]
fn tc_11_41_skill_without_hooks_returns_none() {
    let skill = base_skill("no-hooks", SkillSource::User, None);
    let tool = tool_with(vec![skill]);
    let result = tool.skill_hooks_for(&json!({"skill": "no-hooks"}));
    assert!(result.is_none(), "TC-11.41: skill without hooks must return None");
}

// TC-11.42: nonexistent skill name returns None
#[test]
fn tc_11_42_nonexistent_skill_returns_none() {
    let tool = tool_with(vec![]);
    let result = tool.skill_hooks_for(&json!({"skill": "nonexistent"}));
    assert!(result.is_none(), "TC-11.42: nonexistent skill must return None");
}

// TC-11.43: input missing skill field returns None
#[test]
fn tc_11_43_missing_skill_field_returns_none() {
    let skill = base_skill("my-skill", SkillSource::User, Some(valid_hooks_json()));
    let tool = tool_with(vec![skill]);
    assert!(
        tool.skill_hooks_for(&json!({})).is_none(),
        "TC-11.43: no skill field → None"
    );
    assert!(
        tool.skill_hooks_for(&json!({"foo": "bar"})).is_none(),
        "TC-11.43: wrong field → None"
    );
}

// TC-11.44: MCP source skill with hooks_raw returns None
#[test]
fn tc_11_44_mcp_source_returns_none() {
    let skill = base_skill("mcp-skill", SkillSource::Mcp, Some(valid_hooks_json()));
    let tool = tool_with(vec![skill]);
    let result = tool.skill_hooks_for(&json!({"skill": "mcp-skill"}));
    assert!(result.is_none(), "TC-11.44: MCP source must return None");
}

// TC-11.45: invalid hooks_raw (array, not object) returns None without panic
#[test]
fn tc_11_45_invalid_hooks_raw_returns_none() {
    let skill = base_skill("bad-hooks", SkillSource::User, Some(json!([1, 2, 3])));
    let tool = tool_with(vec![skill]);
    let result = tool.skill_hooks_for(&json!({"skill": "bad-hooks"}));
    assert!(result.is_none(), "TC-11.45: invalid hooks_raw (array) must return None");
}

#[test]
fn skill_shell_inspection_rejects_oversized_executable() {
    let directory = tempfile::tempdir().unwrap();
    let shell = solaris_config::shell::default_shell();
    let executable = directory
        .path()
        .join(shell.path.file_name().expect("default shell should have a file name"));
    std::fs::copy(&shell.path, &executable).unwrap();
    let file = std::fs::OpenOptions::new().write(true).open(&executable).unwrap();
    file.set_len(64 * 1024 * 1024 + 1).unwrap();
    drop(file);

    let error = inspect_skill_shell(&executable).unwrap_err();

    assert!(error.contains("executable exceeds size limit"), "{error}");
    assert!(!error.contains(executable.to_string_lossy().as_ref()));
}

#[cfg(unix)]
#[test]
fn skill_shell_inspection_rejects_file_without_execute_bit() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().unwrap();
    let executable = directory.path().join("non-executable-skill-shell");
    std::fs::copy(solaris_config::shell::default_shell().path, &executable).unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o600)).unwrap();

    let error = inspect_skill_shell(&executable).unwrap_err();

    assert!(error.contains("executable permission denied"), "{error}");
    assert!(!error.contains(executable.to_string_lossy().as_ref()));
}

#[test]
fn skill_shell_inspection_does_not_expose_secret_path() {
    let directory = tempfile::tempdir().unwrap();
    let secret = "mesh-secret-shell-name-7429";
    let executable = directory.path().join(secret);

    let error = inspect_skill_shell(&executable).unwrap_err();

    assert!(!error.contains(secret));
    assert!(error.contains("sha256:"), "{error}");
}
