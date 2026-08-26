use solaris_types::sandbox::{SandboxBackend, SandboxEnforcement, SandboxReason, SandboxReport};
use solaris_types::skill_types::{ContextModifier, EffortLevel, PlanModeTransition};
use solaris_types::tool::ToolResultMetadata;

use super::*;

#[test]
fn round_trip_preserves_modifier_and_original_status() {
    let outcome = DurableToolOutcome::new(
        "submitted".to_owned(),
        false,
        ToolResultStatus::Executed,
        Some(ContextModifier {
            model: Some("model-b".to_owned()),
            effort: Some(EffortLevel::High),
            allowed_tools: vec!["Read".to_owned()],
            plan_mode_transition: Some(PlanModeTransition::Exit {
                plan_content: Some("# Plan".to_owned()),
            }),
        }),
        Some(ToolResultMetadata::sandbox_report(SandboxReport::new(
            SandboxEnforcement::Partial,
            SandboxBackend::ExternalRunner,
            SandboxReason::ExternalRunnerCapabilityInsufficient,
        ))),
    );

    let decoded = DurableToolOutcome::decode(outcome.encode().unwrap(), false).unwrap();

    assert_eq!(decoded, outcome);
    assert_eq!(decoded.replay_status(), ToolResultStatus::CacheHit);
}

#[test]
fn legacy_plain_and_json_outputs_remain_visible_as_written() {
    for content in ["plain output", r#"{"user":"json"}"#] {
        let decoded = DurableToolOutcome::decode(content.to_owned(), false).unwrap();
        assert_eq!(decoded.content, content);
        assert_eq!(decoded.modifier, None);
        assert_eq!(decoded.metadata, None);
        assert!(decoded.is_legacy());
    }
}

#[test]
fn tagged_corruption_is_not_treated_as_legacy_output() {
    let error = DurableToolOutcome::decode(
        r#"{"schema":"solaris/durable-tool-outcome/v1","content":"x"}"#.to_owned(),
        false,
    )
    .unwrap_err();

    assert!(error.contains("invalid"));
}
