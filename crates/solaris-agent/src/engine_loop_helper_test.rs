use serde_json::json;
use solaris_types::message::{ContentBlock, StopReason, TokenUsage};

use super::{AgentError, merge_tool_results, tool_call_malformed_fingerprint};
use crate::stream::StreamOutcome;
use crate::tool_call::{
    DEFAULT_MAX_TOOL_CALL_FAILURE, ToolCallFailureFingerprint, ToolCallMalformedReason, tool_call_failure_fingerprint,
};
use crate::turn::{FinalizationReason, TurnGuardAction, TurnGuards, TurnKind, TurnOutcome};

fn tool_use(id: &str, name: &str) -> ContentBlock {
    tool_use_with_input(id, name, json!({}))
}

fn tool_use_with_input(id: &str, name: &str, input: serde_json::Value) -> ContentBlock {
    ContentBlock::ToolUse {
        id: id.to_string(),
        name: name.to_string(),
        input,
        extra: None,
    }
}

fn failed_exec_fingerprint() -> Option<ToolCallFailureFingerprint> {
    tool_call_failure_fingerprint(&[tool_use("call", "ExecCommand")])
}

fn executed_result(id: &str) -> ContentBlock {
    ContentBlock::ToolResult {
        tool_use_id: id.to_string(),
        content: format!("ok:{id}"),
        is_error: false,
    }
}

/// Mixed malformed + executable calls must re-interleave so each result
/// lands at its originating call's index, with executed results consumed
/// in order for the non-malformed slots.
#[test]
fn merge_interleaves_malformed_and_executed_in_call_order() {
    let calls = vec![tool_use("bad", ""), tool_use("ok1", "Read"), tool_use("ok2", "Glob")];
    let reasons = vec![Some(ToolCallMalformedReason::EmptyFunctionName), None, None];
    let executed = vec![executed_result("ok1"), executed_result("ok2")];
    let modifiers = vec![None, None];

    let (results, mods) = merge_tool_results(&calls, &reasons, executed, modifiers);

    assert_eq!(results.len(), 3);
    // Slot 0: synthetic malformed error result for "bad".
    assert!(matches!(
        &results[0],
        ContentBlock::ToolResult { tool_use_id, is_error: true, .. } if tool_use_id == "bad"
    ));
    // Slots 1,2: executed results, consumed in order.
    assert!(matches!(
        &results[1],
        ContentBlock::ToolResult { tool_use_id, is_error: false, .. } if tool_use_id == "ok1"
    ));
    assert!(matches!(
        &results[2],
        ContentBlock::ToolResult { tool_use_id, is_error: false, .. } if tool_use_id == "ok2"
    ));
    assert_eq!(mods.len(), 3);
}

#[test]
fn merge_all_malformed_needs_no_executed_results() {
    let calls = vec![tool_use("bad1", ""), tool_use("bad2", "")];
    let reasons = vec![
        Some(ToolCallMalformedReason::EmptyFunctionName),
        Some(ToolCallMalformedReason::EmptyFunctionName),
    ];

    let (results, mods) = merge_tool_results(&calls, &reasons, Vec::new(), Vec::new());

    assert_eq!(results.len(), 2);
    assert!(
        results
            .iter()
            .all(|r| matches!(r, ContentBlock::ToolResult { is_error: true, .. }))
    );
    assert!(mods.iter().all(Option::is_none));
}

#[test]
fn turn_budget_reached_respects_limit_and_none() {
    let mut guards = TurnGuards::new(Some(2), 3, DEFAULT_MAX_TOOL_CALL_FAILURE);
    assert_eq!(guards.turn_budget_reached(), None);
    guards.record_counted_turn();
    guards.record_counted_turn();
    assert_eq!(guards.turn_budget_reached(), Some(2));

    // No limit configured 鈫?never reached.
    let mut unlimited = TurnGuards::new(None, 3, DEFAULT_MAX_TOOL_CALL_FAILURE);
    for _ in 0..1_000 {
        unlimited.record_counted_turn();
    }
    assert_eq!(unlimited.turn_budget_reached(), None);
}

#[test]
fn after_tool_round_trips_consecutive_tool_call_failure_breaker() {
    let mut guards = TurnGuards::new(Some(100), 3, DEFAULT_MAX_TOOL_CALL_FAILURE);
    // First N-1 tool-call-failure rounds: no stop yet.
    for _ in 0..DEFAULT_MAX_TOOL_CALL_FAILURE - 1 {
        assert!(matches!(
            guards.after_tool_round(None, failed_exec_fingerprint()),
            TurnGuardAction::Continue
        ));
    }
    // The Nth consecutive tool-call-failure round trips the breaker.
    assert!(matches!(
        guards.after_tool_round(None, failed_exec_fingerprint()),
        TurnGuardAction::Stop(AgentError::ToolCallFailures { .. })
    ));
}

#[test]
fn after_tool_round_resets_tool_call_failure_streak_on_success() {
    let mut guards = TurnGuards::new(Some(100), 3, DEFAULT_MAX_TOOL_CALL_FAILURE);
    assert!(matches!(
        guards.after_tool_round(None, failed_exec_fingerprint()),
        TurnGuardAction::Continue
    ));
    // A non-error tool round resets the streak.
    assert!(matches!(guards.after_tool_round(None, None), TurnGuardAction::Continue));
    assert_eq!(guards.tool_call_failure_count(), 0);
    // So a single subsequent tool-call-failure round must not trip the breaker.
    assert!(matches!(
        guards.after_tool_round(None, failed_exec_fingerprint()),
        TurnGuardAction::Continue
    ));
}

#[test]
fn after_tool_round_does_not_trip_failure_breaker_for_different_tool_inputs() {
    let mut guards = TurnGuards::new(Some(100), 3, DEFAULT_MAX_TOOL_CALL_FAILURE);

    for index in 0..DEFAULT_MAX_TOOL_CALL_FAILURE {
        let fingerprint = tool_call_failure_fingerprint(&[tool_use_with_input(
            &format!("call-{index}"),
            "ExecCommand",
            json!({ "cmd": format!("command-{index}") }),
        )]);

        assert!(matches!(
            guards.after_tool_round(None, fingerprint),
            TurnGuardAction::Continue
        ));
        assert_eq!(guards.tool_call_failure_count(), 1);
    }
}

#[test]
fn after_tool_round_requests_finalize_when_budget_is_exhausted() {
    let mut guards = TurnGuards::new(Some(1), 3, DEFAULT_MAX_TOOL_CALL_FAILURE);
    guards.record_counted_turn();
    assert!(matches!(guards.after_tool_round(None, None), TurnGuardAction::Finalize));
}

#[test]
fn after_tool_round_stop_breaker_takes_priority_over_finalize() {
    let mut guards = TurnGuards::new(Some(1), 1, DEFAULT_MAX_TOOL_CALL_FAILURE);
    guards.record_counted_turn();

    let calls = vec![tool_use("bad", "")];
    let reasons = vec![Some(ToolCallMalformedReason::EmptyFunctionName)];
    let fingerprint = tool_call_malformed_fingerprint(&calls, &reasons);

    assert!(matches!(
        guards.after_tool_round(fingerprint, None),
        TurnGuardAction::Stop(AgentError::ToolCallMalformed { count: 1, limit: 1 })
    ));
}

#[test]
fn turn_kind_finalization_has_control_prompt_and_disables_tools() {
    assert!(TurnKind::Normal.control_prompt().is_none());
    assert!(!TurnKind::Normal.disable_tools());

    let kind = TurnKind::Finalization(FinalizationReason::TurnBudget);
    assert!(kind.disable_tools());
    assert!(
        kind.control_prompt()
            .expect("finalization must have a control prompt")
            .contains("Do not call any more tools")
    );
}

#[test]
fn turn_kind_max_tokens_prompt_names_truncation() {
    let kind = TurnKind::MaxTokensContinuation;
    let prompt = kind
        .control_prompt()
        .expect("max token continuation must have a prompt");

    assert!(!kind.disable_tools());
    assert!(prompt.contains("previous response was cut off"));
    assert!(prompt.contains("Use tools when needed"));
}

#[test]
fn post_mutation_verification_keeps_tools_and_requires_semantic_evidence() {
    let kind = TurnKind::PostMutationVerification;
    let prompt = kind
        .control_prompt()
        .expect("post-mutation verification must have a prompt");

    assert!(!kind.disable_tools());
    assert!(prompt.contains("every explicit requirement"));
    assert!(prompt.contains("not only syntax"));
    assert!(prompt.contains("Do not claim a check"));
    assert!(
        prompt.contains("Do not re-read files or repeat successful commands already visible"),
        "completion review must reuse evidence already present in the conversation"
    );
}

#[test]
fn turn_kind_empty_final_prompt_requests_visible_answer() {
    let prompt = TurnKind::Finalization(FinalizationReason::EmptyFinal)
        .control_prompt()
        .expect("empty final nudge must have a prompt");

    assert!(prompt.contains("visible answer text"));
    assert!(prompt.contains("Do not send reasoning only"));
}

fn stream_outcome(assistant_text: &str, stop_reason: StopReason, tool_calls: Vec<ContentBlock>) -> StreamOutcome {
    StreamOutcome {
        assistant_text: assistant_text.to_string(),
        thinking_text: String::new(),
        thinking_signature: None,
        provider_metadata: Default::default(),
        tool_calls,
        stop_reason,
        usage: TokenUsage::default(),
    }
}

#[test]
fn turn_outcome_classifies_tool_round_before_final_text() {
    let outcome = stream_outcome(
        "I will inspect this.",
        StopReason::EndTurn,
        vec![tool_use("call-1", "Read")],
    );

    assert!(matches!(TurnOutcome::from_stream(outcome), TurnOutcome::ToolRound(_)));
}

#[test]
fn turn_outcome_classifies_visible_end_turn_as_final() {
    let outcome = stream_outcome("Done", StopReason::EndTurn, Vec::new());

    assert!(matches!(TurnOutcome::from_stream(outcome), TurnOutcome::Final(_)));
}

#[test]
fn turn_outcome_classifies_max_tokens_as_truncated_even_with_text() {
    let outcome = stream_outcome("I will now write the file", StopReason::MaxTokens, Vec::new());

    assert!(matches!(TurnOutcome::from_stream(outcome), TurnOutcome::Truncated(_)));
}

#[test]
fn turn_outcome_classifies_empty_end_turn_as_empty_final() {
    let outcome = stream_outcome("   ", StopReason::EndTurn, Vec::new());

    assert!(matches!(TurnOutcome::from_stream(outcome), TurnOutcome::EmptyFinal(_)));
}
