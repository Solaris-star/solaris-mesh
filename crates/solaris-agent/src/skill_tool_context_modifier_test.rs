use std::path::PathBuf;
use std::sync::Arc;

use serde_json::json;

use solaris_skills::permissions::SkillPermissionChecker;
use solaris_skills::types::{EffortLevel, ExecutionContext, LoadedFrom, SkillMetadata, SkillSource};
use solaris_tools::Tool;

use super::SkillTool;

fn base_skill(name: &str) -> SkillMetadata {
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

// TC-6.14: skill name not in registry → None
#[test]
fn tc_6_14_skill_not_found_returns_none() {
    let tool = tool_with(vec![base_skill("commit")]);
    assert!(tool.context_modifier_for(&json!({"skill": "nonexistent"})).is_none());
}

// TC-6.15: input missing skill field → None
#[test]
fn tc_6_15_missing_skill_field_returns_none() {
    let tool = tool_with(vec![base_skill("commit")]);
    assert!(tool.context_modifier_for(&json!({})).is_none());
}

// TC-6.16: skill exists but no override fields → None
#[test]
fn tc_6_16_skill_no_override_returns_none() {
    let tool = tool_with(vec![base_skill("no-override")]);
    assert!(tool.context_modifier_for(&json!({"skill": "no-override"})).is_none());
}

// TC-6.17: skill has model override → Some with correct model
#[test]
fn tc_6_17_skill_with_model_returns_some() {
    let mut skill = base_skill("model-skill");
    skill.model = Some("test-model".to_string());
    let tool = tool_with(vec![skill]);

    let modifier = tool.context_modifier_for(&json!({"skill": "model-skill"}));
    assert!(modifier.is_some());
    let m = modifier.unwrap();
    assert_eq!(m.model.as_deref(), Some("test-model"));
    assert!(m.effort.is_none());
    assert!(m.allowed_tools.is_empty());
}

// TC-6.18: skill has effort override → Some with correct effort
#[test]
fn tc_6_18_skill_with_effort_returns_some() {
    let mut skill = base_skill("effort-skill");
    skill.effort = Some(EffortLevel::High);
    let tool = tool_with(vec![skill]);

    let modifier = tool.context_modifier_for(&json!({"skill": "effort-skill"}));
    assert!(modifier.is_some());
    let m = modifier.unwrap();
    assert_eq!(m.effort, Some(EffortLevel::High));
    assert!(m.model.is_none());
}

// TC-6.19: skill has allowed_tools override → Some with correct tools
#[test]
fn tc_6_19_skill_with_allowed_tools_returns_some() {
    let mut skill = base_skill("tools-skill");
    skill.allowed_tools = vec!["ExecCommand".to_string(), "Read".to_string()];
    let tool = tool_with(vec![skill]);

    let modifier = tool.context_modifier_for(&json!({"skill": "tools-skill"}));
    assert!(modifier.is_some());
    let m = modifier.unwrap();
    assert_eq!(m.allowed_tools, vec!["ExecCommand", "Read"]);
}

// TC-6.19b: leading slash is stripped before lookup
#[test]
fn tc_6_19b_leading_slash_stripped_in_context_modifier_for() {
    let mut skill = base_skill("slash-skill");
    skill.model = Some("m".to_string());
    let tool = tool_with(vec![skill]);

    // /slash-skill should resolve to slash-skill
    let modifier = tool.context_modifier_for(&json!({"skill": "/slash-skill"}));
    assert!(modifier.is_some());
}

// TC-6.20: with_session_id() stores session_id; new() defaults to None
#[test]
fn tc_6_20_session_id_stored_correctly() {
    let skills = Arc::new(vec![]);

    // new() → session_id is None
    let tool_no_session = SkillTool::new(
        skills.clone(),
        PathBuf::from("/tmp"),
        SkillPermissionChecker::new(vec![], vec![], false),
    );
    assert!(tool_no_session.session_id.is_none());

    // with_session_id() → session_id is set
    let tool_with_session = SkillTool::with_session_id(
        skills,
        PathBuf::from("/tmp"),
        SkillPermissionChecker::new(vec![], vec![], false),
        Some("sess-abc".to_string()),
    );
    assert_eq!(tool_with_session.session_id.as_deref(), Some("sess-abc"));
}

// TC-6.20b: with_session_id(None) stores None
#[test]
fn tc_6_20b_session_id_none_when_not_provided() {
    let tool = SkillTool::with_session_id(
        Arc::new(vec![]),
        PathBuf::from("/tmp"),
        SkillPermissionChecker::new(vec![], vec![], false),
        None,
    );
    assert!(tool.session_id.is_none());
}

// TC-6.17b: context_modifier_for() is independent of execute() — pure lookup, no side effects
#[test]
fn tc_6_17b_context_modifier_for_does_not_mutate_tool() {
    let mut skill = base_skill("pure-skill");
    skill.model = Some("model-x".to_string());
    let tool = tool_with(vec![skill]);

    // Call twice — result must be identical (no state mutation)
    let m1 = tool.context_modifier_for(&json!({"skill": "pure-skill"}));
    let m2 = tool.context_modifier_for(&json!({"skill": "pure-skill"}));
    assert_eq!(m1.unwrap().model, m2.unwrap().model);
}
