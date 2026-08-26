// Tests included by context_test.rs.
// --- SystemPromptCache tests ---

#[test]
fn cache_new_is_empty() {
    let cache = SystemPromptCache::new();
    assert!(cache.joined.is_none());
    assert!(cache.sections.is_empty());
}

#[test]
fn cache_stores_and_retrieves_section() {
    let mut cache = SystemPromptCache::new();
    cache.sections.insert("intro", "Hello world".to_string());
    assert_eq!(cache.sections.get("intro").unwrap(), "Hello world");
}

#[test]
fn cache_invalidate_removes_section_and_joined() {
    let mut cache = SystemPromptCache::new();
    cache.sections.insert("intro", "Hello".to_string());
    cache.sections.insert("memory", "Memory content".to_string());
    cache.joined = Some("Hello\n\nMemory content".to_string());

    cache.invalidate("memory");

    assert!(!cache.sections.contains_key("memory"));
    assert!(cache.joined.is_none());
    // Other sections preserved
    assert_eq!(cache.sections.get("intro").unwrap(), "Hello");
}

#[test]
fn cache_invalidate_all_clears_everything() {
    let mut cache = SystemPromptCache::new();
    cache.sections.insert("intro", "Hello".to_string());
    cache.sections.insert("memory", "Mem".to_string());
    cache.joined = Some("joined".to_string());

    cache.invalidate_all();

    assert!(cache.sections.is_empty());
    assert!(cache.joined.is_none());
}

#[test]
fn cache_invalidate_nonexistent_key_is_noop() {
    let mut cache = SystemPromptCache::new();
    cache.sections.insert("intro", "Hello".to_string());
    cache.joined = Some("joined".to_string());

    cache.invalidate("nonexistent");

    // joined is still invalidated (conservative behavior)
    assert!(cache.joined.is_none());
    assert_eq!(cache.sections.get("intro").unwrap(), "Hello");
}

// --- Cache integration tests ---

#[test]
fn build_system_prompt_uses_cache_on_second_call() {
    let mut cache = SystemPromptCache::new();
    let first = build_system_prompt(&mut cache, None, "/tmp", "test-model", &[], None, None, false, false);
    assert!(cache.joined.is_some());

    let second = build_system_prompt(&mut cache, None, "/tmp", "test-model", &[], None, None, false, false);
    assert_eq!(first, second);
}

#[test]
fn build_system_prompt_plan_mode_change_rebuilds() {
    let mut cache = SystemPromptCache::new();
    let without_plan = build_system_prompt(&mut cache, None, "/tmp", "test-model", &[], None, None, false, false);
    let with_plan = build_system_prompt(&mut cache, None, "/tmp", "test-model", &[], None, None, true, false);
    assert_ne!(without_plan, with_plan);
}
