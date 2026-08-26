use super::*;

use solaris_compact::CompactLevel;
use solaris_skills::permissions::SkillPermissionChecker;
use solaris_skills::types::{ExecutionContext, LoadedFrom, SkillSource};
use solaris_tools::registry::ToolRegistry;
use solaris_types::identity::{AgentId, RunId};
use solaris_types::message::ContentBlock;
use solaris_types::permission::{
    ExecutionBoundary, PermissionCeiling, PermissionDecision, PermissionMode, PermissionRule,
};
use solaris_types::runtime::OperationEnvironmentSnapshot;

use crate::confirm::ToolConfirmer;
use crate::orchestration::execute_tool_calls_with_policy_context;
use crate::permission_engine::PermissionContext;
use crate::runtime_ledger::InMemoryRuntimeLedger;

fn policy_skill(content: String) -> SkillMetadata {
    let content_length = content.len();
    SkillMetadata {
        name: "process-policy".into(),
        display_name: None,
        description: "process policy test".into(),
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
        content,
        content_length,
        skill_root: None,
    }
}

fn marker_command(marker: &Path) -> String {
    #[cfg(windows)]
    {
        format!(
            "Set-Content -LiteralPath '{}' -Value launched",
            marker.to_string_lossy().replace('\'', "''")
        )
    }
    #[cfg(not(windows))]
    {
        format!(
            "printf launched > '{}'",
            marker.to_string_lossy().replace('\'', "'\\''")
        )
    }
}

async fn execute_process_policy_case(mode: PermissionMode) -> (ContentBlock, bool) {
    let workspace = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let marker = outside.path().join("skill-process-marker.txt");
    let permissions = PermissionContext::new(mode, PermissionCeiling::unrestricted());
    permissions.set_boundary(ExecutionBoundary::workspace(
        workspace.path().to_string_lossy().into_owned(),
    ));
    permissions.register_protected_paths(state.path(), Vec::new()).unwrap();
    let context = EffectExecutionContext::new(
        RunId::from(format!("skill-process-policy-{mode:?}")),
        AgentId::from("root"),
        Arc::new(InMemoryRuntimeLedger::default()),
        permissions.clone(),
        OperationEnvironmentSnapshot::default(),
    );
    let tool = SkillTool::new(
        Arc::new(vec![policy_skill(format!("!`{}`", marker_command(&marker)))]),
        workspace.path().to_path_buf(),
        SkillPermissionChecker::new(Vec::new(), Vec::new(), false),
    )
    .with_shell_executor(Arc::new(EffectSkillShellExecutor::new(context.clone())));
    let input = serde_json::json!({"skill": "process-policy"});
    let descriptor = tool.describe_effect(&input);
    permissions.allow_configured_effect_for("test:skill", "Skill", &descriptor);
    permissions.set_generated_rules(
        "test:skill",
        vec![PermissionRule {
            capability: Some("Skill".into()),
            action: None,
            effect_class: Some(EffectClass::Process),
            resource_prefixes: Vec::new(),
            decision: PermissionDecision::Allow,
        }],
    );
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(tool));
    let confirmer = Arc::new(Mutex::new(ToolConfirmer::new(true, Vec::new())));
    let outcome = execute_tool_calls_with_policy_context(
        &registry,
        &[ContentBlock::ToolUse {
            id: "skill-process-policy-call".into(),
            name: "Skill".into(),
            input,
            extra: None,
        }],
        &confirmer,
        mode,
        PermissionCeiling::unrestricted(),
        &context,
        None,
        CompactLevel::Off,
        false,
    )
    .await
    .unwrap();
    (outcome.results.into_iter().next().unwrap(), marker.exists())
}

#[tokio::test]
async fn auto_skill_shell_cannot_start_with_ambient_access() {
    let (result, marker_exists) = execute_process_policy_case(PermissionMode::Auto).await;
    let ContentBlock::ToolResult { is_error, .. } = result else {
        panic!("Skill must return a tool result")
    };

    assert!(is_error);
    assert!(!marker_exists);
}

#[tokio::test]
async fn bypass_skill_shell_keeps_ambient_access() {
    let (result, marker_exists) = execute_process_policy_case(PermissionMode::Bypass).await;
    let ContentBlock::ToolResult { is_error, content, .. } = result else {
        panic!("Skill must return a tool result")
    };

    assert!(!is_error, "{content}");
    assert!(marker_exists);
}

#[tokio::test]
async fn plan_skill_shell_is_rejected_before_spawn() {
    let (result, marker_exists) = execute_process_policy_case(PermissionMode::Plan).await;
    let ContentBlock::ToolResult { is_error, .. } = result else {
        panic!("Skill must return a tool result")
    };

    assert!(is_error);
    assert!(!marker_exists);
}

#[tokio::test]
async fn direct_skill_shell_execution_fails_closed_without_prepared_authorization() {
    let workspace = tempfile::tempdir().unwrap();
    let marker = workspace.path().join("direct-skill-marker.txt");
    let permissions = PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted());
    let context = EffectExecutionContext::new(
        RunId::from("direct-skill-process-policy"),
        AgentId::from("root"),
        Arc::new(InMemoryRuntimeLedger::default()),
        permissions,
        OperationEnvironmentSnapshot::default(),
    );
    let tool = SkillTool::new(
        Arc::new(vec![policy_skill(format!("!`{}`", marker_command(&marker)))]),
        workspace.path().to_path_buf(),
        SkillPermissionChecker::new(Vec::new(), Vec::new(), false),
    )
    .with_shell_executor(Arc::new(EffectSkillShellExecutor::new(context)));

    let result = tool.execute(serde_json::json!({"skill": "process-policy"})).await;

    assert!(result.is_error);
    assert!(result.content.contains("approved prepared effect"));
    assert!(!marker.exists());
}

#[test]
fn prepared_skill_shell_rejects_ambient_policy_without_spawn_authorization() {
    let workspace = tempfile::tempdir().unwrap();
    let tool = SkillTool::new(
        Arc::new(vec![policy_skill("!`echo prepared`".to_owned())]),
        workspace.path().to_path_buf(),
        SkillPermissionChecker::new(Vec::new(), Vec::new(), false),
    );
    let input = serde_json::json!({"skill": "process-policy"});
    let context = tool
        .prepare_effect("prepared-skill", &input)
        .unwrap()
        .into_parts()
        .1
        .with_process_launch_policy(ProcessLaunchPolicy::Ambient);

    let error = match tool.prepare_execution(input, context) {
        Ok(_) => panic!("Skill process preparation must require final spawn authorization"),
        Err(error) => error,
    };

    assert!(error.contains("approved process spawn authorization"));
}
