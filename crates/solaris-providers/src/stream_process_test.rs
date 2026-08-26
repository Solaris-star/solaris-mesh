use super::*;
use crate::framing::{Frame, FrameKind};

#[test]
fn missing_close_notify_recovers_only_a_complete_openai_response() {
    let parser = OpenAiParser { auto_tool_id: false };
    let mut state = parser.new_state();
    let finish_frame = Frame {
        event: None,
        data: r#"{"choices":[{"delta":{"content":"complete"},"finish_reason":"stop"}]}"#.to_owned(),
        kind: FrameKind::Data,
    };
    let _ = parser.parse_frame(&finish_frame, &mut state);

    let events = recover_openai_terminal_after_stream_error(
        "request or response body error: peer closed connection without sending TLS close_notify",
        &parser,
        &mut state,
    )
    .expect("a complete response may recover from the exact rustls shutdown error");

    assert!(matches!(events.as_slice(), [LlmEvent::Done { .. }]));
}

#[test]
fn missing_close_notify_does_not_recover_an_incomplete_openai_response() {
    let parser = OpenAiParser { auto_tool_id: false };
    let mut state = parser.new_state();

    assert!(
        recover_openai_terminal_after_stream_error(
            "peer closed connection without sending TLS close_notify",
            &parser,
            &mut state,
        )
        .is_none()
    );
}

#[test]
fn unrelated_stream_errors_remain_failures_even_after_a_terminal_frame() {
    let parser = OpenAiParser { auto_tool_id: false };
    let mut state = parser.new_state();
    let finish_frame = Frame {
        event: None,
        data: r#"{"choices":[{"delta":{},"finish_reason":"stop"}]}"#.to_owned(),
        kind: FrameKind::Data,
    };
    let _ = parser.parse_frame(&finish_frame, &mut state);

    assert!(recover_openai_terminal_after_stream_error("connection reset by peer", &parser, &mut state).is_none());
}
