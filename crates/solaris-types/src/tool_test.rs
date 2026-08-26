use super::*;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // --- ToolDef construction and field validation ---

    #[test]
    fn test_tool_def_construction_fields() {
        // arrange
        let schema = json!({
            "type": "object",
            "properties": {
                "cmd": { "type": "string" }
            },
            "required": ["cmd"]
        });
        // act
        let tool = ToolDef {
            name: "bash".to_string(),
            description: "Run a shell command".to_string(),
            input_schema: schema.clone(),
            deferred: false,
        };
        // assert
        assert_eq!(tool.name, "bash");
        assert_eq!(tool.description, "Run a shell command");
        assert_eq!(tool.input_schema, schema);
    }

    #[test]
    fn test_tool_def_empty_schema_is_valid() {
        // arrange + act
        let tool = ToolDef {
            name: "noop".to_string(),
            description: "Does nothing".to_string(),
            input_schema: json!({}),
            deferred: false,
        };
        // assert
        assert_eq!(tool.input_schema, json!({}));
    }

    // --- ToolResult success scenario ---

    #[test]
    fn test_tool_result_success_is_error_false() {
        // arrange + act
        let result = ToolResult {
            content: "command output".to_string(),
            is_error: false,
        };
        // assert
        assert_eq!(result.content, "command output");
        assert!(!result.is_error);
    }

    // --- ToolResult error scenario ---

    #[test]
    fn test_tool_result_error_is_error_true() {
        // arrange + act
        let result = ToolResult {
            content: "permission denied".to_string(),
            is_error: true,
        };
        // assert
        assert_eq!(result.content, "permission denied");
        assert!(result.is_error);
    }

    #[test]
    fn test_tool_result_error_empty_content() {
        // arrange + act – errors may carry an empty content string
        let result = ToolResult {
            content: String::new(),
            is_error: true,
        };
        // assert
        assert!(result.content.is_empty());
        assert!(result.is_error);
    }

    #[test]
    fn tool_result_status_serializes_all_terminal_outcomes() {
        let cases = [
            (ToolResultStatus::Executed, "executed"),
            (ToolResultStatus::CacheHit, "cache_hit"),
            (ToolResultStatus::Noop, "noop"),
            (ToolResultStatus::Denied, "denied"),
            (ToolResultStatus::Failed, "failed"),
            (ToolResultStatus::Aborted, "aborted"),
            (ToolResultStatus::Timeout, "timeout"),
            (ToolResultStatus::OutcomeUnknown, "outcome_unknown"),
        ];

        for (status, expected) in cases {
            assert_eq!(serde_json::to_value(status).unwrap(), json!(expected));
        }
    }

    #[test]
    fn tool_result_status_reads_legacy_success_and_error() {
        let success: ToolResultStatus = serde_json::from_value(json!("success")).unwrap();
        let error: ToolResultStatus = serde_json::from_value(json!("error")).unwrap();

        assert_eq!(success, ToolResultStatus::Executed);
        assert_eq!(error, ToolResultStatus::Failed);
    }

    #[test]
    fn classified_tool_result_status_is_authoritative() {
        let result = ClassifiedToolResult::new("cached", ToolResultStatus::CacheHit);

        assert_eq!(result.status, ToolResultStatus::CacheHit);
        assert!(!result.is_error);
        assert_eq!(
            serde_json::to_value(&result).unwrap(),
            json!({"content": "cached", "is_error": false, "status": "cache_hit"})
        );
    }

    #[test]
    fn classified_tool_result_reads_legacy_binary_shape() {
        let success: ClassifiedToolResult =
            serde_json::from_value(json!({"content": "ok", "is_error": false})).unwrap();
        let error: ClassifiedToolResult = serde_json::from_value(json!({"content": "bad", "is_error": true})).unwrap();

        assert_eq!(success.status, ToolResultStatus::Executed);
        assert_eq!(error.status, ToolResultStatus::Failed);
    }

    #[test]
    fn classified_tool_result_preserves_typed_sandbox_metadata() {
        let report = crate::sandbox::SandboxReport::new(
            crate::sandbox::SandboxEnforcement::Partial,
            crate::sandbox::SandboxBackend::ExternalRunner,
            crate::sandbox::SandboxReason::ExternalRunnerCapabilityInsufficient,
        );
        let result = ClassifiedToolResult::new("strict sandbox unavailable", ToolResultStatus::Denied)
            .with_metadata(ToolResultMetadata::sandbox_report(report));

        let value = serde_json::to_value(&result).unwrap();
        assert_eq!(value["status"], "denied");
        assert_eq!(value["metadata"]["sandbox_report"]["backend"], "external_runner");
        assert_eq!(value["metadata"]["sandbox_report"]["enforcement"], "partial");
        assert_eq!(
            value["metadata"]["sandbox_report"]["reason"],
            "external_runner_capability_insufficient"
        );
        assert_eq!(serde_json::from_value::<ClassifiedToolResult>(value).unwrap(), result);
    }

    #[test]
    fn legacy_tool_result_serialization_includes_an_explicit_status() {
        let success = serde_json::to_value(ToolResult {
            content: "ok".into(),
            is_error: false,
        })
        .unwrap();
        let error = serde_json::to_value(ToolResult {
            content: "bad".into(),
            is_error: true,
        })
        .unwrap();

        assert_eq!(success["status"], "executed");
        assert_eq!(error["status"], "failed");
    }

    #[test]
    fn useful_call_rate_counts_executed_and_cache_hit_calls() {
        let statuses = [
            ToolResultStatus::Executed,
            ToolResultStatus::CacheHit,
            ToolResultStatus::Noop,
            ToolResultStatus::Failed,
        ];

        assert_eq!(useful_call_rate(&statuses), Some(0.5));
        assert_eq!(useful_call_rate(&[]), None);
    }

    // --- tool_call_fingerprint: object key order is irrelevant ---

    #[test]
    fn tool_call_fingerprint_ignores_object_key_order() {
        let scope = "task:t|env:e";
        let first = json!({"path": "/a", "limit": 10});
        let reordered = json!({"limit": 10, "path": "/a"});

        assert_eq!(
            tool_call_fingerprint(scope, "read", &first),
            tool_call_fingerprint(scope, "read", &reordered)
        );
    }

    #[test]
    fn tool_call_fingerprint_ignores_nested_object_key_order() {
        let scope = "task:t|env:e";
        let first = json!({"opts": {"a": 1, "b": 2}, "name": "x"});
        let reordered = json!({"name": "x", "opts": {"b": 2, "a": 1}});

        assert_eq!(
            tool_call_fingerprint(scope, "bash", &first),
            tool_call_fingerprint(scope, "bash", &reordered)
        );
    }

    #[test]
    fn tool_call_fingerprint_distinguishes_different_inputs() {
        let scope = "task:t|env:e";
        let first = json!({"path": "/a"});
        let second = json!({"path": "/b"});

        assert_ne!(
            tool_call_fingerprint(scope, "read", &first),
            tool_call_fingerprint(scope, "read", &second)
        );
    }

    #[test]
    fn tool_call_fingerprint_distinguishes_tool_names() {
        let scope = "task:t|env:e";
        let input = json!({"path": "/a"});

        assert_ne!(
            tool_call_fingerprint(scope, "read", &input),
            tool_call_fingerprint(scope, "write", &input)
        );
    }

    #[test]
    fn tool_call_fingerprint_distinguishes_scopes() {
        let input = json!({"path": "/a"});

        assert_ne!(
            tool_call_fingerprint("task:a|env:e", "read", &input),
            tool_call_fingerprint("task:b|env:e", "read", &input)
        );
        assert_ne!(
            tool_call_fingerprint("task:a|env:e1", "read", &input),
            tool_call_fingerprint("task:a|env:e2", "read", &input)
        );
    }

    #[test]
    fn tool_call_fingerprint_array_order_matters() {
        let scope = "task:t|env:e";
        let first = json!({"args": ["a", "b"]});
        let reordered = json!({"args": ["b", "a"]});

        assert_ne!(
            tool_call_fingerprint(scope, "bash", &first),
            tool_call_fingerprint(scope, "bash", &reordered)
        );
    }

    // --- duplicate_call_rate ---

    #[test]
    fn duplicate_call_rate_empty_sample_is_none() {
        assert_eq!(duplicate_call_rate(&[]), None);
    }

    #[test]
    fn duplicate_call_rate_no_repeats_is_zero() {
        let stats = [
            ToolCallStat::new("s", "read", &json!({"path": "/a"}), ToolResultStatus::Executed),
            ToolCallStat::new("s", "read", &json!({"path": "/b"}), ToolResultStatus::Executed),
        ];
        assert_eq!(duplicate_call_rate(&stats), Some(0.0));
    }

    #[test]
    fn duplicate_call_rate_counts_repeated_fingerprints() {
        let stats = [
            ToolCallStat::new("s", "read", &json!({"path": "/a"}), ToolResultStatus::Executed),
            ToolCallStat::new("s", "read", &json!({"path": "/a"}), ToolResultStatus::Executed),
            ToolCallStat::new("s", "read", &json!({"path": "/b"}), ToolResultStatus::Executed),
        ];
        assert_eq!(duplicate_call_rate(&stats), Some(1.0 / 3.0));
    }

    #[test]
    fn duplicate_call_rate_counts_all_terminal_statuses() {
        // A repeated fingerprint is a duplicate regardless of the terminal
        // status of either occurrence.
        let stats = [
            ToolCallStat::new("s", "bash", &json!({"cmd": "x"}), ToolResultStatus::Failed),
            ToolCallStat::new("s", "bash", &json!({"cmd": "x"}), ToolResultStatus::Denied),
            ToolCallStat::new("s", "bash", &json!({"cmd": "x"}), ToolResultStatus::CacheHit),
            ToolCallStat::new("s", "bash", &json!({"cmd": "x"}), ToolResultStatus::Aborted),
            ToolCallStat::new("s", "bash", &json!({"cmd": "x"}), ToolResultStatus::OutcomeUnknown),
        ];
        assert_eq!(duplicate_call_rate(&stats), Some(4.0 / 5.0));
    }

    #[test]
    fn duplicate_call_rate_key_order_variant_is_a_duplicate() {
        let stats = [
            ToolCallStat::new(
                "s",
                "read",
                &json!({"path": "/a", "limit": 1}),
                ToolResultStatus::Executed,
            ),
            ToolCallStat::new(
                "s",
                "read",
                &json!({"limit": 1, "path": "/a"}),
                ToolResultStatus::CacheHit,
            ),
        ];
        assert_eq!(duplicate_call_rate(&stats), Some(0.5));
    }

    #[test]
    fn duplicate_call_rate_cross_scope_is_not_a_duplicate() {
        let stats = [
            ToolCallStat::new(
                "task:a|env:e",
                "read",
                &json!({"path": "/a"}),
                ToolResultStatus::Executed,
            ),
            ToolCallStat::new(
                "task:b|env:e",
                "read",
                &json!({"path": "/a"}),
                ToolResultStatus::Executed,
            ),
        ];
        assert_eq!(duplicate_call_rate(&stats), Some(0.0));
    }

    #[test]
    fn test_tool_def_deferred_defaults_to_false() {
        let tool = ToolDef {
            name: "test".to_string(),
            description: "desc".to_string(),
            input_schema: json!({}),
            deferred: false,
        };
        assert!(!tool.deferred);
    }

    #[test]
    fn test_tool_def_deferred_true() {
        let tool = ToolDef {
            name: "spawn".to_string(),
            description: "desc".to_string(),
            input_schema: json!({}),
            deferred: true,
        };
        assert!(tool.deferred);
    }

    // --- truncate_deferred_description tests ---

    #[test]
    fn truncate_short_description_unchanged() {
        let desc = "Search for issues in Sentry.";
        assert_eq!(truncate_deferred_description(desc), desc);
    }

    #[test]
    fn truncate_at_blank_line() {
        let desc = "First paragraph here.\n\nSecond paragraph with details.";
        assert_eq!(truncate_deferred_description(desc), "First paragraph here.…");
    }

    #[test]
    fn truncate_at_200_chars_before_blank_line() {
        let desc = format!("{}. More text after.", "A".repeat(200));
        let result = truncate_deferred_description(&desc);
        assert!(result.len() <= 200 + '…'.len_utf8());
        assert!(result.ends_with('…'));
    }

    #[test]
    fn truncate_blank_line_before_200_chars() {
        let desc = "Short first paragraph.\n\nLong second paragraph that goes on and on.";
        let result = truncate_deferred_description(desc);
        assert_eq!(result, "Short first paragraph.…");
    }

    #[test]
    fn truncate_empty_string() {
        assert_eq!(truncate_deferred_description(""), "");
    }

    #[test]
    fn truncate_exactly_200_chars() {
        let desc = "X".repeat(200);
        assert_eq!(truncate_deferred_description(&desc), desc);
    }

    #[test]
    fn truncate_201_chars() {
        let desc = "X".repeat(201);
        let result = truncate_deferred_description(&desc);
        assert!(result.ends_with('…'));
        // 200 X's + ellipsis
        assert_eq!(result.len(), 200 + '…'.len_utf8());
    }

    #[test]
    fn truncate_multibyte_chars_safe() {
        // 100 two-byte chars = 200 bytes, but only 100 char positions
        let desc: String = "é".repeat(150);
        let result = truncate_deferred_description(&desc);
        // Should not panic and should be valid UTF-8
        assert!(result.ends_with('…'));
        // Should be at most 200 chars (counting code points)
        let char_count = result.chars().count();
        assert!(char_count <= 201); // 200 chars + ellipsis
    }
}
