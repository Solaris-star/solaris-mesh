use super::*;

#[cfg(test)]
mod tests {
    use super::*;

    struct DefaultCapabilityTool;

    #[async_trait]
    impl Tool for DefaultCapabilityTool {
        fn name(&self) -> &str {
            "DisplayName"
        }

        fn description(&self) -> &str {
            "test tool"
        }

        fn input_schema(&self) -> JsonSchema {
            serde_json::json!({"type": "object"})
        }

        fn is_concurrency_safe(&self, _input: &Value) -> bool {
            true
        }

        async fn execute(&self, _input: Value) -> ToolResult {
            ToolResult {
                content: "ok".into(),
                is_error: false,
            }
        }

        fn category(&self) -> ToolCategory {
            ToolCategory::Info
        }
    }

    #[test]
    fn permission_capability_defaults_to_registered_name() {
        let tool = DefaultCapabilityTool;
        assert_eq!(tool.permission_capability(), tool.name());
    }

    #[tokio::test]
    async fn default_classified_execution_maps_legacy_binary_result() {
        let tool = DefaultCapabilityTool;

        let result = tool.execute_classified(serde_json::json!({})).await;

        assert_eq!(result.status, solaris_types::tool::ToolResultStatus::Executed);
        assert!(!result.is_error);
    }

    #[tokio::test]
    async fn prepared_execution_preserves_an_explicit_status() {
        let execution = PreparedToolExecution::new_classified(
            None,
            Box::pin(async {
                solaris_types::tool::ClassifiedToolResult::new(
                    "nothing changed",
                    solaris_types::tool::ToolResultStatus::Noop,
                )
            }),
        );

        let result = execution.execute_classified().await;

        assert_eq!(result.status, solaris_types::tool::ToolResultStatus::Noop);
        assert!(!result.is_error);
    }

    #[test]
    fn truncate_utf8_ascii_within_limit() {
        assert_eq!(truncate_utf8("hello", 80), "hello");
    }

    #[test]
    fn truncate_utf8_ascii_at_boundary() {
        assert_eq!(truncate_utf8("abcde", 3), "abc");
    }

    #[test]
    fn truncate_utf8_multibyte_snaps_back() {
        // '些' is 3 bytes (E4 BA 9B) starting at index 79 would span 79..82
        let s = "# 用 script 模拟 TTY 交互来添加 DeepSeek 提供商\n# 首先看看有哪些";
        let result = truncate_utf8(s, 80);
        assert!(result.len() <= 80);
        assert!(result.is_char_boundary(result.len()));
    }

    #[test]
    fn truncate_utf8_empty() {
        assert_eq!(truncate_utf8("", 80), "");
    }

    #[test]
    fn truncate_utf8_zero_limit() {
        assert_eq!(truncate_utf8("hello", 0), "");
    }

    #[test]
    fn truncate_utf8_emoji() {
        // 🦀 is 4 bytes
        let s = "aaa🦀bbb";
        assert_eq!(truncate_utf8(s, 4), "aaa");
        assert_eq!(truncate_utf8(s, 7), "aaa🦀");
    }
}
