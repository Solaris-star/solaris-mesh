// Tests included by context_test.rs.
// --- TOON format injection tests ---

#[test]
fn toon_enabled_injects_format_instructions() {
    let result = build_system_prompt(
        &mut SystemPromptCache::new(),
        None,
        "/tmp",
        "test-model",
        &[],
        None,
        None,
        false,
        true,
    );
    assert!(
        result.contains("TOON"),
        "toon_enabled should inject TOON format instructions"
    );
    assert!(
        result.contains("Token-Oriented Object Notation"),
        "should contain full TOON description"
    );
}

#[test]
fn toon_disabled_no_format_instructions() {
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
        !result.contains("TOON"),
        "toon_disabled should not inject TOON format instructions"
    );
}
