// ---------------------------------------------------------------------------
// WB-8: prompt — format_skills_within_budget edge cases [白盒]
// ---------------------------------------------------------------------------

#[test]
fn wb_8a_empty_skills_returns_empty_string() {
    let result = format_skills_within_budget(&[], None);
    assert_eq!(result, "");
}

#[test]
fn wb_8b_single_skill_within_budget() {
    let mut skill = make_skill("my-skill", "body");
    skill.description = "A short description".to_string();
    let result = format_skills_within_budget(&[skill], None);
    assert!(result.contains("my-skill"));
    assert!(result.contains("A short description"));
}

#[test]
fn wb_8c_all_bundled_no_non_bundled() {
    // When only bundled skills exist, they're all returned even under budget pressure
    let mut b1 = make_skill("bundled-a", "body");
    b1.source = SkillSource::Bundled;
    b1.description = "Bundled A".to_string();
    let mut b2 = make_skill("bundled-b", "body");
    b2.source = SkillSource::Bundled;
    b2.description = "Bundled B".to_string();

    let result = format_skills_within_budget(&[b1, b2], Some(1)); // tiny budget
    assert!(result.contains("bundled-a"), "bundled A should be present");
    assert!(result.contains("bundled-b"), "bundled B should be present");
}
