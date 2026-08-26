// Tests included by context_test.rs.
// --- Tool usage guidance tests (task 4.3) ---

#[test]
fn tool_guidance_section_exists() {
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
        result.contains("# Using your tools"),
        "system prompt should contain the tool guidance heading"
    );
}

#[test]
fn tool_guidance_contains_bash_prohibition_list() {
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
    assert!(result.contains("Glob"), "should mention Glob as find/ls replacement");
    assert!(result.contains("Grep"), "should mention Grep as grep/rg replacement");
    assert!(
        result.contains("Read"),
        "should mention Read as cat/head/tail replacement"
    );
    assert!(result.contains("Edit"), "should mention Edit as sed/awk replacement");
    assert!(
        result.contains("Write"),
        "should mention Write as echo/heredoc replacement"
    );
}

#[test]
fn tool_guidance_contains_parallel_call_rules() {
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
    assert!(result.contains("parallel"), "should contain parallel call guidance");
    assert!(
        result.contains("sequentially"),
        "should explain when to run sequentially"
    );
}

#[test]
fn tool_guidance_contains_edit_over_write_preference() {
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
        result.contains("Prefer Edit over Write"),
        "should contain Edit-over-Write preference"
    );
}

#[test]
fn tool_guidance_chooses_rewrite_for_large_structural_changes() {
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
        result.contains("one complete Write"),
        "large structural changes should prefer one coherent rewrite"
    );
}

#[test]
fn tool_guidance_avoids_redundant_reads_and_stops_after_verification() {
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
        result.contains("Do not re-read an unchanged file"),
        "unchanged files should not be read repeatedly"
    );
    assert!(
        result.contains("finish the task"),
        "successful verification should terminate the task"
    );
    assert!(
        result.contains("Treat .solaris as internal runtime metadata"),
        "runtime metadata should not be inspected as task evidence"
    );
    assert!(
        result.contains("Do not search outside the workspace for hidden tests"),
        "the agent should not seek hidden benchmark answers"
    );
    assert!(
        result.contains("call the required tool immediately"),
        "the agent should act once it has enough evidence"
    );
}

#[test]
fn tool_guidance_contains_read_before_edit_rule() {
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
        result.contains("Read a file before editing"),
        "should contain Read-before-Edit rule"
    );
    assert!(
        result.contains("Read it again immediately before a later Edit"),
        "a mutation must invalidate an earlier Read for later edits"
    );
    assert!(
        result.contains("Do not re-read files merely because an ExecCommand succeeded"),
        "a successful command must not trigger broad post-verification reads"
    );
}

#[test]
fn tool_guidance_after_intro_before_custom_prompt() {
    let result = build_system_prompt(
        &mut SystemPromptCache::new(),
        Some("CUSTOM_MARKER_43"),
        "/tmp",
        "test-model",
        &[],
        None,
        None,
        false,
        false,
    );
    let intro_pos = result.find("Working directory").unwrap();
    let guidance_pos = result.find("# Using your tools").unwrap();
    let custom_pos = result.find("CUSTOM_MARKER_43").unwrap();
    assert!(guidance_pos > intro_pos, "tool guidance should appear after intro");
    assert!(
        guidance_pos < custom_pos,
        "tool guidance should appear before custom prompt"
    );
}

#[test]
fn tool_guidance_before_skills_reminder() {
    let skills = vec![make_test_skill("guide-test-skill", "A skill", false, false)];
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
    let guidance_pos = result.find("# Using your tools").unwrap();
    let skills_pos = result.find("guide-test-skill").unwrap();
    assert!(
        guidance_pos < skills_pos,
        "tool guidance should appear before skills reminder"
    );
}

#[test]
fn tool_guidance_present_in_plan_mode() {
    let result = build_system_prompt(
        &mut SystemPromptCache::new(),
        None,
        "/tmp",
        "test-model",
        &[],
        None,
        None,
        true,
        false,
    );
    assert!(
        result.contains("# Using your tools"),
        "tool guidance should be present in plan mode"
    );
}

#[test]
fn tool_guidance_contains_deferred_instruction() {
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
        result.contains("deferred"),
        "tool guidance should mention deferred tools"
    );
    assert!(result.contains("ToolSearch"), "tool guidance should mention ToolSearch");
}

#[test]
fn tool_guidance_before_memory() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mem_dir = tmp.path().join("memory");
    std::fs::create_dir_all(&mem_dir).unwrap();
    std::fs::write(mem_dir.join("MEMORY.md"), "- [X](x.md) \u{2014} test\n").unwrap();

    let result = build_system_prompt(
        &mut SystemPromptCache::new(),
        None,
        "/tmp",
        "test-model",
        &[],
        None,
        Some(&mem_dir),
        false,
        false,
    );
    let guidance_pos = result.find("# Using your tools").unwrap();
    let memory_pos = result.find("auto memory").unwrap();
    assert!(
        guidance_pos < memory_pos,
        "tool guidance should appear before memory section"
    );
}
