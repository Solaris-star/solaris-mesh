use std::path::PathBuf;
use std::sync::Arc;

use serde_json::json;

use solaris_skills::permissions::SkillPermissionChecker;
use solaris_skills::types::{ExecutionContext, LoadedFrom, SkillMetadata, SkillSource};

use super::SkillTool;
use solaris_tools::Tool;

fn make_skill(name: &str, content: &str) -> SkillMetadata {
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
        hooks_raw: None,
        source: SkillSource::User,
        loaded_from: LoadedFrom::Skills,
        content: content.to_string(),
        content_length: content.len(),
        skill_root: None,
    }
}

// P5-11: SkillTool returns error for a denied skill.
#[tokio::test]
async fn p5_11_denied_skill_returns_error() {
    let checker = SkillPermissionChecker::new(vec!["dangerous".to_string()], vec![], false);
    let tool = SkillTool::new(
        Arc::new(vec![make_skill("dangerous", "rm -rf /")]),
        PathBuf::from("/tmp"),
        checker,
    );
    let result = tool.execute(json!({"skill": "dangerous"})).await;
    assert!(result.is_error);
    assert!(result.content.contains("denied"), "content: {}", result.content);
}

// P5-12: SkillTool returns informative message for a skill that needs approval.
#[tokio::test]
async fn p5_12_ask_skill_returns_approval_prompt() {
    let checker = SkillPermissionChecker::new(vec![], vec![], false);
    let mut skill = make_skill("hooked", "body");
    skill.hooks_raw = Some(serde_json::json!({ "pre": "echo hi" }));
    let tool = SkillTool::new(Arc::new(vec![skill]), PathBuf::from("/tmp"), checker);
    let result = tool.execute(json!({"skill": "hooked"})).await;
    assert!(result.is_error);
    assert!(
        result.content.contains("approval") || result.content.contains("approve"),
        "content should mention approval: {}",
        result.content
    );
}
