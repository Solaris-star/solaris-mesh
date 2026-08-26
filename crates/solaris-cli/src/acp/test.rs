use super::*;

#[test]
fn prompt_text_joins_text_blocks() {
    let blocks = vec![
        ContentBlock::Text(TextContent::new("first")),
        ContentBlock::Text(TextContent::new("second")),
    ];

    assert_eq!(prompt_text(&blocks).expect("text blocks are accepted"), "first\nsecond");
}

#[test]
fn prompt_text_rejects_empty_input_at_the_boundary() {
    let blocks = Vec::new();
    assert!(
        prompt_text(&blocks)
            .expect("empty content is syntactically valid")
            .is_empty()
    );
}

#[test]
fn policy_parser_accepts_documented_values_only() {
    assert_eq!(
        parse_policy("on-demand").expect("alias is accepted"),
        MultiAgentPolicy::OnDemand
    );
    assert_eq!(
        parse_policy("PROACTIVE").expect("case is ignored"),
        MultiAgentPolicy::Proactive
    );
    assert!(parse_policy("ambient").is_err());
}

#[test]
fn strategy_parser_keeps_single_fixed_and_others_configured() {
    assert!(matches!(
        parse_strategy("auto").expect("auto"),
        CollaborationSelection::Auto
    ));
    assert!(matches!(
        parse_strategy("single").expect("single"),
        CollaborationSelection::Fixed(CollaborationStrategy::Single)
    ));
    assert!(matches!(
        parse_strategy("independent-reviewer").expect("reviewer alias"),
        CollaborationSelection::Configured(config) if config.strategy == CollaborationStrategy::IndependentReviewer
    ));
    assert!(parse_strategy("unknown").is_err());
}

#[test]
fn selection_name_is_stable_for_host_config_options() {
    assert_eq!(selection_name(&CollaborationSelection::Auto), "auto");
    assert_eq!(
        selection_name(&CollaborationSelection::Fixed(CollaborationStrategy::Fanout)),
        "fanout"
    );
    assert_eq!(selection_name(&CollaborationSelection::Inherit), "auto");
}
