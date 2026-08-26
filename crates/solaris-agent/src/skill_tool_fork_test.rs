use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::json;

use crate::spawner::{ForkOverrides, Spawner, SubAgentConfig, SubAgentResult};
use solaris_skills::permissions::SkillPermissionChecker;
use solaris_skills::types::{EffortLevel, ExecutionContext, LoadedFrom, SkillMetadata, SkillSource};
use solaris_tools::Tool;
use solaris_types::message::TokenUsage;

use super::SkillTool;

// ---------------------------------------------------------------------------
// MockSpawner — returns preset result, captures args
// ---------------------------------------------------------------------------

struct MockSpawner {
    is_error: bool,
    text: String,
    captured_config: Mutex<Option<SubAgentConfig>>,
    captured_overrides: Mutex<Option<ForkOverrides>>,
}

impl MockSpawner {
    fn success(text: &str) -> Arc<Self> {
        Arc::new(Self {
            is_error: false,
            text: text.to_string(),
            captured_config: Mutex::new(None),
            captured_overrides: Mutex::new(None),
        })
    }

    #[allow(dead_code)]
    fn error(text: &str) -> Arc<Self> {
        Arc::new(Self {
            is_error: true,
            text: text.to_string(),
            captured_config: Mutex::new(None),
            captured_overrides: Mutex::new(None),
        })
    }

    #[allow(dead_code)]
    fn take_config(&self) -> SubAgentConfig {
        self.captured_config
            .lock()
            .unwrap()
            .take()
            .expect("spawn_fork was not called")
    }

    #[allow(dead_code)]
    fn take_overrides(&self) -> ForkOverrides {
        self.captured_overrides
            .lock()
            .unwrap()
            .take()
            .expect("spawn_fork was not called")
    }
}

#[async_trait]
impl Spawner for MockSpawner {
    async fn spawn_fork(&self, config: SubAgentConfig, overrides: ForkOverrides) -> SubAgentResult {
        *self.captured_config.lock().unwrap() = Some(config.clone());
        *self.captured_overrides.lock().unwrap() = Some(overrides.clone());
        SubAgentResult {
            name: config.name.clone(),
            agent_id: None,
            task_id: None,
            status: if self.is_error {
                solaris_types::spawner::AgentOutcomeStatus::Failed
            } else {
                solaris_types::spawner::AgentOutcomeStatus::Completed
            },
            output: Some(serde_json::json!({"text": self.text.clone()})),
            text: self.text.clone(),
            usage: TokenUsage::default(),
            turns: 1,
            failure_class: self
                .is_error
                .then_some(solaris_types::runtime::TaskFailureClass::NonRetryable),
            is_error: self.is_error,
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_fork_skill(name: &str, content: &str) -> SkillMetadata {
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
        execution_context: ExecutionContext::Fork,
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

fn make_inline_skill(name: &str, content: &str) -> SkillMetadata {
    SkillMetadata {
        execution_context: ExecutionContext::Inline,
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

fn tool_with_spawner(skills: Vec<SkillMetadata>, spawner: Option<Arc<dyn Spawner>>) -> SkillTool {
    SkillTool::with_spawner(
        Arc::new(skills),
        PathBuf::from("/tmp"),
        SkillPermissionChecker::new(vec![], vec![], false),
        None,
        spawner,
    )
}

fn tool_no_spawner(skills: Vec<SkillMetadata>) -> SkillTool {
    tool_with_spawner(skills, None)
}

// ---------------------------------------------------------------------------
// TC-7.20: inline skill takes inline path — spawner NOT called
// ---------------------------------------------------------------------------
#[tokio::test]
async fn tc_7_20_inline_skill_takes_inline_path() {
    let spawner = MockSpawner::success("should not be called");
    let tool = tool_with_spawner(
        vec![make_inline_skill("inline-skill", "inline content")],
        Some(spawner.clone() as Arc<dyn Spawner>),
    );
    let result = tool.execute(json!({"skill": "inline-skill"})).await;
    assert!(!result.is_error, "inline skill should succeed: {}", result.content);
    assert_eq!(result.content, "inline content");
    // spawn_fork should NOT have been called
    assert!(
        spawner.captured_config.lock().unwrap().is_none(),
        "spawner should not have been called for inline skill"
    );
}

// TC-7.21: fork skill takes fork path — spawner IS called
#[tokio::test]
async fn tc_7_21_fork_skill_takes_fork_path() {
    let spawner = MockSpawner::success("fork result");
    let tool = tool_with_spawner(
        vec![make_fork_skill("fork-skill", "fork content")],
        Some(spawner.clone() as Arc<dyn Spawner>),
    );
    let result = tool.execute(json!({"skill": "fork-skill"})).await;
    assert!(!result.is_error, "fork skill should succeed: {}", result.content);
    assert_eq!(result.content, "fork result");
    // spawn_fork should have been called exactly once
    assert!(
        spawner.captured_config.lock().unwrap().is_some(),
        "spawner should have been called for fork skill"
    );
}

// TC-7.12: no spawner — fork skill returns clear error message
#[tokio::test]
async fn tc_7_12_fork_skill_no_spawner_returns_error() {
    let tool = tool_no_spawner(vec![make_fork_skill("needs-spawner", "content")]);
    let result = tool.execute(json!({"skill": "needs-spawner"})).await;
    assert!(result.is_error, "should be error without spawner");
    assert!(
        result.content.contains("fork execution context"),
        "error message should mention 'fork execution context': {}",
        result.content
    );
}

// TC-7.23: context_modifier_for() returns None for fork skill
#[test]
fn tc_7_23_context_modifier_for_fork_returns_none() {
    // Fork skill with model/effort overrides — still returns None
    let mut skill = make_fork_skill("fork-with-model", "content");
    skill.model = Some("claude-opus-4-6".to_string());
    skill.effort = Some(EffortLevel::High);
    skill.allowed_tools = vec!["ExecCommand".to_string()];
    let tool = tool_no_spawner(vec![skill]);
    let modifier = tool.context_modifier_for(&json!({"skill": "fork-with-model"}));
    assert!(
        modifier.is_none(),
        "fork skill should return None from context_modifier_for"
    );
}

// TC-7.22: context_modifier_for() returns Some for inline skill with overrides
#[test]
fn tc_7_22_context_modifier_for_inline_returns_some() {
    let mut skill = make_inline_skill("inline-with-model", "content");
    skill.model = Some("my-model".to_string());
    let tool = tool_no_spawner(vec![skill]);
    let modifier = tool.context_modifier_for(&json!({"skill": "inline-with-model"}));
    assert!(
        modifier.is_some(),
        "inline skill with model override should return Some"
    );
    assert_eq!(modifier.unwrap().model.as_deref(), Some("my-model"));
}

// TC-7.24: fork skill no spawner — returns error without panic
#[tokio::test]
async fn tc_7_24_fork_no_spawner_no_panic() {
    let tool = tool_no_spawner(vec![make_fork_skill("no-spawn", "content")]);
    // Should not panic, must return Err
    let result = tool.execute(json!({"skill": "no-spawn"})).await;
    assert!(result.is_error);
    assert!(!result.content.is_empty());
}

// TC-7.30: fork skill — permission allow — proceeds to fork execution
#[tokio::test]
async fn tc_7_30_fork_skill_permission_allow_proceeds() {
    let spawner = MockSpawner::success("fork ok");
    let tool = SkillTool::with_spawner(
        Arc::new(vec![make_fork_skill("fork-allowed", "content")]),
        PathBuf::from("/tmp"),
        // deny_list empty, allow_list empty = allow all
        SkillPermissionChecker::new(vec![], vec![], false),
        None,
        Some(spawner as Arc<dyn Spawner>),
    );
    let result = tool.execute(json!({"skill": "fork-allowed"})).await;
    assert!(
        !result.is_error,
        "allowed fork skill should succeed: {}",
        result.content
    );
    assert_eq!(result.content, "fork ok");
}

// TC-7.31: fork skill — permission deny — blocked before fork execution
#[tokio::test]
async fn tc_7_31_fork_skill_permission_deny_blocked() {
    let spawner = MockSpawner::success("should not reach here");
    let tool = SkillTool::with_spawner(
        Arc::new(vec![make_fork_skill("fork-denied", "content")]),
        PathBuf::from("/tmp"),
        // deny "fork-denied"
        SkillPermissionChecker::new(vec!["fork-denied".to_string()], vec![], false),
        None,
        Some(spawner.clone() as Arc<dyn Spawner>),
    );
    let result = tool.execute(json!({"skill": "fork-denied"})).await;
    assert!(result.is_error, "denied fork skill should return error");
    assert!(
        result.content.contains("denied"),
        "error should mention 'denied': {}",
        result.content
    );
    // spawner should NOT have been called since permission check happens first
    assert!(
        spawner.captured_config.lock().unwrap().is_none(),
        "spawner should not be called when skill is denied"
    );
}

// with_spawner() constructor stores spawner correctly
#[test]
fn tc_7_with_spawner_constructor() {
    let spawner: Arc<dyn Spawner> = MockSpawner::success("ok");
    let tool = SkillTool::with_spawner(
        Arc::new(vec![]),
        PathBuf::from("/tmp"),
        SkillPermissionChecker::new(vec![], vec![], false),
        Some("sess-1".to_string()),
        Some(spawner),
    );
    // Verify session_id was also stored
    assert_eq!(tool.session_id.as_deref(), Some("sess-1"));
    // Verify spawner is Some
    assert!(tool.spawner.is_some());
}

// new() constructor leaves spawner as None
#[test]
fn tc_7_new_constructor_spawner_is_none() {
    let tool = SkillTool::new(
        Arc::new(vec![]),
        PathBuf::from("/tmp"),
        SkillPermissionChecker::new(vec![], vec![], false),
    );
    assert!(tool.spawner.is_none());
}
