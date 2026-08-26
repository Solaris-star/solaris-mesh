// Tests included by context_test.rs.
// --- build_system_prompt Phase 9 tests ---

use solaris_skills::types::{ExecutionContext, LoadedFrom, SkillMetadata, SkillSource};

fn make_test_skill(name: &str, description: &str, bundled: bool, hidden: bool) -> SkillMetadata {
    SkillMetadata {
        name: name.to_string(),
        display_name: None,
        description: description.to_string(),
        has_user_specified_description: false,
        allowed_tools: vec![],
        argument_hint: None,
        argument_names: vec![],
        when_to_use: None,
        version: None,
        model: None,
        disable_model_invocation: hidden,
        user_invocable: true,
        execution_context: ExecutionContext::Inline,
        agent: None,
        effort: None,
        shell: None,
        paths: vec![],
        network: Default::default(),
        hooks_raw: None,
        source: if bundled {
            SkillSource::Bundled
        } else {
            SkillSource::User
        },
        loaded_from: if bundled {
            LoadedFrom::Bundled
        } else {
            LoadedFrom::Skills
        },
        content: String::new(),
        content_length: 0,
        skill_root: None,
    }
}

#[test]
fn test_build_system_prompt_no_skills_no_reminder() {
    let result = build_system_prompt(
        &mut SystemPromptCache::new(),
        None,
        "/tmp",
        "test-model",
        &[],
        None,
        None,
        false,
        false,
    );
    assert!(
        !result.contains("The following skills are available"),
        "empty skills should not inject skill reminder"
    );
}

#[test]
fn test_build_system_prompt_with_skills_injects_reminder() {
    let skills = vec![
        make_test_skill("skill-one", "Does one", false, false),
        make_test_skill("skill-two", "Does two", false, false),
    ];
    let result = build_system_prompt(
        &mut SystemPromptCache::new(),
        None,
        "/tmp",
        "test-model",
        &skills,
        None,
        None,
        false,
        false,
    );
    assert!(
        result.contains("<system-reminder>"),
        "result should contain <system-reminder>"
    );
    assert!(
        result.contains("The following skills are available for use with the Skill tool:"),
        "result should contain skills header"
    );
    assert!(
        result.contains("</system-reminder>"),
        "result should close <system-reminder>"
    );
    assert!(result.contains("skill-one"), "result should list skill-one");
    assert!(result.contains("skill-two"), "result should list skill-two");
}

#[test]
fn test_build_system_prompt_hidden_skill_filtered() {
    let skills = vec![
        make_test_skill("visible-skill", "Visible", false, false),
        make_test_skill("hidden-skill", "Hidden", false, true),
    ];
    let result = build_system_prompt(
        &mut SystemPromptCache::new(),
        None,
        "/tmp",
        "test-model",
        &skills,
        None,
        None,
        false,
        false,
    );
    assert!(result.contains("visible-skill"), "visible skill should appear");
    assert!(!result.contains("hidden-skill"), "hidden skill should be filtered out");
}

#[test]
fn test_build_system_prompt_all_hidden_no_reminder() {
    let skills = vec![
        make_test_skill("hidden-a", "Hidden A", false, true),
        make_test_skill("hidden-b", "Hidden B", false, true),
    ];
    let result = build_system_prompt(
        &mut SystemPromptCache::new(),
        None,
        "/tmp",
        "test-model",
        &skills,
        None,
        None,
        false,
        false,
    );
    assert!(
        !result.contains("The following skills are available"),
        "all-hidden skills should not inject reminder"
    );
}

#[test]
fn test_build_system_prompt_custom_prompt_and_skills() {
    let skills = vec![make_test_skill("my-skill", "My desc", false, false)];
    let result = build_system_prompt(
        &mut SystemPromptCache::new(),
        Some("Custom instructions here"),
        "/tmp",
        "test-model",
        &skills,
        None,
        None,
        false,
        false,
    );
    assert!(
        result.contains("Custom instructions here"),
        "custom prompt should appear"
    );
    assert!(
        result.contains("The following skills are available for use with the Skill tool:"),
        "skills reminder should also appear"
    );
}

#[test]
fn test_build_system_prompt_skills_reminder_after_custom_prompt() {
    let skills = vec![make_test_skill("my-skill", "My desc", false, false)];
    let result = build_system_prompt(
        &mut SystemPromptCache::new(),
        Some("Custom text"),
        "/tmp",
        "test-model",
        &skills,
        None,
        None,
        false,
        false,
    );
    let custom_pos = result.find("Custom text").unwrap();
    let reminder_pos = result.rfind("<system-reminder>").unwrap();
    assert!(
        reminder_pos > custom_pos,
        "skills reminder should appear after custom prompt"
    );
}

#[test]
fn test_build_system_prompt_small_budget_triggers_minimal_mode() {
    // context_window_tokens = 50 → budget = 2 chars, triggers minimal mode for non-bundled
    let skill = make_test_skill("nb-skill", &"x".repeat(100), false, false);
    let result = build_system_prompt(
        &mut SystemPromptCache::new(),
        None,
        "/tmp",
        "test-model",
        &[skill],
        Some(50),
        None,
        false,
        false,
    );
    // Minimal mode: skill appears as name only, no ': '
    assert!(
        result.contains("- nb-skill"),
        "skill name should appear in minimal mode"
    );
    assert!(
        !result.contains("- nb-skill: "),
        "non-bundled should not have description in minimal mode"
    );
}

#[test]
fn test_build_system_prompt_cwd_in_prompt() {
    let result = build_system_prompt(
        &mut SystemPromptCache::new(),
        None,
        "/workspace/my-project",
        "test-model",
        &[],
        None,
        None,
        false,
        false,
    );
    assert!(
        result.contains("/workspace/my-project"),
        "cwd should appear in the system prompt"
    );
}

#[test]
fn test_build_system_prompt_includes_shell_info() {
    let shell = solaris_config::shell::ResolvedShell::new(
        solaris_config::shell::ShellKind::PowerShell,
        std::path::PathBuf::from(r"C:\Program Files\PowerShell\7\pwsh.exe"),
    );
    let result = build_system_prompt_with_shell(
        &mut SystemPromptCache::new(),
        None,
        "/tmp/project",
        "claude-test",
        &shell,
        &[],
        None,
        None,
        false,
        false,
    );

    assert!(result.contains("Operating system:"));
    assert!(
        result.contains(&format!("Architecture: {}", std::env::consts::ARCH)),
        "system prompt should include current CPU architecture"
    );
    assert!(result.contains("Default shell: powershell"));
    assert!(result.contains(r"Shell path: C:\Program Files\PowerShell\7\pwsh.exe"));
    assert!(result.contains("Shell syntax: powershell"));
}

#[test]
fn test_build_system_prompt_loads_agents_md_not_claude_md() {
    let tmp = tempfile::TempDir::new().unwrap();
    let cwd = tmp.path();

    // Create both AGENTS.md and CLAUDE.md
    std::fs::write(cwd.join("AGENTS.md"), "AGENTS_CONTENT_HERE").unwrap();
    std::fs::write(cwd.join("CLAUDE.md"), "CLAUDE_CONTENT_HERE").unwrap();

    let result = build_system_prompt(
        &mut SystemPromptCache::new(),
        None,
        &cwd.to_string_lossy(),
        "test-model",
        &[],
        None,
        None,
        false,
        false,
    );

    assert!(result.contains("AGENTS_CONTENT_HERE"), "should load AGENTS.md content");
    assert!(
        !result.contains("CLAUDE_CONTENT_HERE"),
        "should NOT load CLAUDE.md content"
    );
    assert!(
        result.contains("(project instructions)"),
        "header should indicate project instructions"
    );
    assert!(result.contains("AGENTS.md"), "header should contain AGENTS.md filename");
}

#[test]
fn test_build_system_prompt_no_agents_md_no_injection() {
    let tmp = tempfile::TempDir::new().unwrap();
    let cwd = tmp.path();

    // Only CLAUDE.md exists, no AGENTS.md
    std::fs::write(cwd.join("CLAUDE.md"), "SHOULD_NOT_APPEAR").unwrap();

    let result = build_system_prompt(
        &mut SystemPromptCache::new(),
        None,
        &cwd.to_string_lossy(),
        "test-model",
        &[],
        None,
        None,
        false,
        false,
    );

    assert!(!result.contains("SHOULD_NOT_APPEAR"), "CLAUDE.md should be ignored");
    assert!(
        !result.contains("(project instructions)"),
        "no project instructions should be injected"
    );
}
