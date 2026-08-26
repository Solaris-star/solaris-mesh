// Tests included by context_test.rs.
// --- Memory integration tests ---

#[test]
fn memory_none_dir_no_injection() {
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
        !result.contains("auto memory"),
        "no memory content when memory_dir is None"
    );
}

#[test]
fn memory_with_dir_injects_prompt() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mem_dir = tmp.path().join("memory");
    std::fs::create_dir_all(&mem_dir).unwrap();
    std::fs::write(
        mem_dir.join("MEMORY.md"),
        "- [Role](user_role.md) \u{2014} senior engineer\n",
    )
    .unwrap();

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

    assert!(
        result.contains("auto memory"),
        "should contain memory system display name"
    );
    assert!(
        result.contains("Memory types:"),
        "should contain compact memory type summary"
    );
    assert!(result.contains("user_role.md"), "should contain MEMORY.md content");
}

#[test]
fn memory_nonexistent_dir_graceful_degradation() {
    let result = build_system_prompt(
        &mut SystemPromptCache::new(),
        None,
        "/tmp",
        "test-model",
        &[],
        None,
        Some(Path::new("/nonexistent/memory/dir")),
        false,
        false,
    );

    // Should not panic and should show empty state
    assert!(
        result.contains("currently empty"),
        "nonexistent memory dir should show empty state"
    );
}

#[test]
fn memory_empty_dir_shows_empty_state() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mem_dir = tmp.path().join("memory");
    std::fs::create_dir_all(&mem_dir).unwrap();
    // No MEMORY.md

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

    assert!(
        result.contains("currently empty"),
        "empty memory dir should show empty state"
    );
}

#[test]
fn memory_appears_after_agents_md_before_skills() {
    let tmp = tempfile::TempDir::new().unwrap();
    let cwd = tmp.path();

    // Create AGENTS.md
    std::fs::write(cwd.join("AGENTS.md"), "PROJECT_RULES_HERE").unwrap();

    // Create memory dir with content
    let mem_dir = tmp.path().join("memory");
    std::fs::create_dir_all(&mem_dir).unwrap();
    std::fs::write(mem_dir.join("MEMORY.md"), "- [A](a.md) \u{2014} test\n").unwrap();

    let skills = vec![make_test_skill("test-skill", "A skill", false, false)];

    let result = build_system_prompt(
        &mut SystemPromptCache::new(),
        None,
        &cwd.to_string_lossy(),
        "test-model",
        &skills,
        None,
        Some(&mem_dir),
        false,
        false,
    );

    let agents_pos = result.find("PROJECT_RULES_HERE").unwrap();
    let memory_pos = result.find("auto memory").unwrap();
    let skills_pos = result.find("test-skill").unwrap();

    assert!(agents_pos < memory_pos, "AGENTS.md should appear before memory");
    assert!(memory_pos < skills_pos, "memory should appear before skills");
}

#[test]
fn memory_no_bb_brand_in_prompt() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mem_dir = tmp.path().join("memory");
    std::fs::create_dir_all(&mem_dir).unwrap();
    std::fs::write(mem_dir.join("MEMORY.md"), "- [Test](test.md) \u{2014} entry\n").unwrap();

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

    assert!(!result.contains("~/.claude"), "should not contain bb brand path");
    assert!(!result.contains("CLAUDE.md"), "should not reference CLAUDE.md");
}
