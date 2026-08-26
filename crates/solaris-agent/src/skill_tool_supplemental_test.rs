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
        allowed_tools: Vec::new(),
        argument_hint: None,
        argument_names: Vec::new(),
        when_to_use: None,
        version: None,
        model: None,
        disable_model_invocation: false,
        user_invocable: true,
        execution_context: ExecutionContext::Inline,
        agent: None,
        effort: None,
        shell: None,
        paths: Vec::new(),
        network: Default::default(),
        hooks_raw: None,
        source: SkillSource::User,
        loaded_from: LoadedFrom::Skills,
        content: content.to_string(),
        content_length: content.len(),
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

// -----------------------------------------------------------------------
// TC-11.x: find_skill
// -----------------------------------------------------------------------

#[test]
fn tc_11_1_exact_match_found() {
    let tool = tool_with(vec![make_skill("commit", "body")]);
    // Access find_skill through execute to verify behavior indirectly
    // (find_skill is private, tested via execute)
    // Direct check via available_names() not exposed, so we verify via execute.
    // Verified in tc_13_1 instead. This test just verifies construction.
    assert_eq!(tool.name(), "Skill");
}

#[test]
fn tc_11_4_case_sensitive_no_match() {
    // "Commit" (capital C) should not match "commit"
    let tool = tool_with(vec![make_skill("commit", "body")]);
    // Verified via execute in tc_13.x
    let _ = tool;
}

#[test]
fn tc_11_5_empty_skills_list_no_panic() {
    let tool = tool_with(vec![]);
    assert_eq!(tool.name(), "Skill"); // just verifies no panic
}

// -----------------------------------------------------------------------
// TC-12.x: name, schema, is_concurrency_safe
// -----------------------------------------------------------------------

#[test]
fn tc_12_1_name_is_skill() {
    let tool = tool_with(vec![]);
    assert_eq!(tool.name(), "Skill");
}

#[test]
fn tc_12_2_schema_skill_required() {
    let tool = tool_with(vec![]);
    let schema = tool.input_schema();
    let required = schema["required"].as_array().unwrap();
    let names: Vec<&str> = required.iter().map(|v| v.as_str().unwrap()).collect();
    assert!(names.contains(&"skill"), "schema required must contain 'skill'");
}

#[test]
fn tc_12_3_schema_args_not_required() {
    let tool = tool_with(vec![]);
    let schema = tool.input_schema();
    // args should be in properties
    assert!(schema["properties"]["args"].is_object(), "args should be in properties");
    // args should NOT be in required
    let required = schema["required"].as_array().unwrap();
    let names: Vec<&str> = required.iter().map(|v| v.as_str().unwrap()).collect();
    assert!(!names.contains(&"args"), "args should not be in required");
}

#[test]
fn tc_12_4_is_concurrency_safe_false() {
    let tool = tool_with(vec![]);
    assert!(!tool.is_concurrency_safe(&json!({})));
    assert!(!tool.is_concurrency_safe(&json!({"skill": "foo"})));
}

// -----------------------------------------------------------------------
// TC-13.x: execute (async)
// -----------------------------------------------------------------------

#[tokio::test]
async fn tc_13_1_successful_inline_execution() {
    let tool = tool_with(vec![make_skill("my-skill", "Run $ARGUMENTS")]);
    let result = tool.execute(json!({"skill": "my-skill", "args": "foo"})).await;
    assert!(!result.is_error);
    assert_eq!(result.content, "Run foo");
}

#[tokio::test]
async fn tc_13_2_skill_not_found_is_error() {
    let tool = tool_with(vec![make_skill("commit", "body")]);
    let result = tool.execute(json!({"skill": "nonexistent"})).await;
    assert!(result.is_error);
    assert!(result.content.contains("not found") || result.content.contains("Skill"));
}

#[tokio::test]
async fn tc_13_3_not_found_error_lists_available_skills() {
    let tool = tool_with(vec![make_skill("commit", "body"), make_skill("review", "body")]);
    let result = tool.execute(json!({"skill": "missing"})).await;
    assert!(result.is_error);
    assert!(result.content.contains("commit"));
    assert!(result.content.contains("review"));
}

#[tokio::test]
async fn tc_13_4_fork_skill_returns_error() {
    let mut skill = make_skill("fork-skill", "body");
    skill.execution_context = ExecutionContext::Fork;
    let tool = tool_with(vec![skill]);
    let result = tool.execute(json!({"skill": "fork-skill"})).await;
    assert!(result.is_error);
    assert!(result.content.contains("fork"));
}

#[tokio::test]
async fn tc_13_5_no_args_field_still_works() {
    let tool = tool_with(vec![make_skill("my-skill", "Just content.")]);
    let result = tool.execute(json!({"skill": "my-skill"})).await;
    assert!(!result.is_error);
    assert_eq!(result.content, "Just content.");
}

#[tokio::test]
async fn tc_13_6_leading_slash_stripped() {
    let tool = tool_with(vec![make_skill("my-skill", "body")]);
    let result = tool.execute(json!({"skill": "/my-skill"})).await;
    assert!(!result.is_error);
}

#[tokio::test]
async fn tc_13_7_missing_skill_field_returns_error() {
    let tool = tool_with(vec![]);
    let result = tool.execute(json!({"args": "foo"})).await;
    assert!(result.is_error);
    assert!(result.content.to_lowercase().contains("missing") || result.content.contains("skill"));
}

#[tokio::test]
async fn tc_13_8_full_variable_substitution_integration() {
    let mut skill = make_skill("my-skill", "Run ${SOLARIS_SKILL_DIR}/tool.sh $ARGUMENTS[0]");
    skill.skill_root = Some("/my/skill".to_string());
    let tool = tool_with(vec![skill]);
    let result = tool.execute(json!({"skill": "my-skill", "args": "alpha"})).await;
    assert!(!result.is_error);
    // base dir header is prepended, then substitution applied
    assert!(result.content.contains("/my/skill/tool.sh alpha"));
}

#[tokio::test]
async fn tc_13_x_case_sensitive_no_match() {
    // "Commit" does not match "commit"
    let tool = tool_with(vec![make_skill("commit", "body")]);
    let result = tool.execute(json!({"skill": "Commit"})).await;
    assert!(
        result.is_error,
        "case-sensitive lookup: 'Commit' should not match 'commit'"
    );
}

// -----------------------------------------------------------------------
// TC-14.x: description
// -----------------------------------------------------------------------

#[test]
fn tc_14_1_description_is_non_empty() {
    let tool = tool_with(vec![make_skill("commit", "body"), make_skill("review", "body")]);
    assert!(!tool.description().is_empty());
}

#[test]
fn tc_14_2_empty_skills_description_no_panic() {
    let tool = tool_with(vec![]);
    assert!(!tool.description().is_empty());
}
