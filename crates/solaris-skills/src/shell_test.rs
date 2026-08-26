use super::*;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    // Note: these are the implementer's tests; supplemental tests below.
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    struct CountingExecutor(AtomicUsize);

    #[async_trait::async_trait]
    impl SkillShellExecutor for CountingExecutor {
        async fn execute(&self, _command: &str, _cwd: &std::path::Path) -> Result<String, ShellExecutionError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(String::new())
        }
    }

    // Helper: run execute_shell_commands with LoadedFrom::Skills
    async fn run(content: &str) -> Result<String, ShellExecutionError> {
        let tmp = std::env::temp_dir();
        execute_shell_commands(content, LoadedFrom::Skills, &tmp).await
    }

    // -----------------------------------------------------------------------
    // format_output
    // -----------------------------------------------------------------------

    #[test]
    fn test_format_output_both() {
        let s = format_output("out", "err");
        assert_eq!(s, "out\n[stderr]\nerr");
    }

    #[test]
    fn test_format_output_stdout_only() {
        assert_eq!(format_output("out", ""), "out");
    }

    #[test]
    fn test_format_output_stderr_only() {
        assert_eq!(format_output("", "err"), "[stderr]\nerr");
    }

    #[test]
    fn test_format_output_empty() {
        assert_eq!(format_output("", ""), "");
    }

    // -----------------------------------------------------------------------
    // extract_shell_matches
    // -----------------------------------------------------------------------

    #[test]
    fn test_extract_block_match() {
        let content = "Before\n```!\necho hello\n```\nAfter";
        let matches = extract_shell_matches(content);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].command, "echo hello");
        assert!(matches[0].full_match.starts_with("```!"));
    }

    #[test]
    fn test_extract_inline_line_start() {
        let content = "!`pwd`";
        let matches = extract_shell_matches(content);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].command, "pwd");
    }

    #[test]
    fn test_extract_inline_whitespace_preceded() {
        let content = "The dir is !`pwd` and user is !`whoami`";
        let matches = extract_shell_matches(content);
        assert_eq!(matches.len(), 2);
        let cmds: Vec<&str> = matches.iter().map(|m| m.command.as_str()).collect();
        assert!(cmds.contains(&"pwd"));
        assert!(cmds.contains(&"whoami"));
    }

    #[test]
    fn test_extract_no_matches() {
        let content = "No shell commands here.";
        assert!(extract_shell_matches(content).is_empty());
    }

    #[test]
    fn test_extract_block_and_inline() {
        let content = "!`echo inline`\n```!\necho block\n```";
        let matches = extract_shell_matches(content);
        assert_eq!(matches.len(), 2);
    }

    // -----------------------------------------------------------------------
    // MCP skill blocked
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_mcp_skill_returns_unchanged() {
        let content = "!`pwd`";
        let tmp = std::env::temp_dir();
        let result = execute_shell_commands(content, LoadedFrom::Mcp, &tmp).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), content);
    }

    // -----------------------------------------------------------------------
    // Block execution
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_block_execution() {
        let content = "Output:\n```!\necho hello\n```\nDone.";
        let result = run(content).await.unwrap();
        assert!(result.contains("hello"));
        assert!(!result.contains("```!"));
    }

    #[tokio::test]
    async fn test_inline_execution_line_start() {
        let content = "!`echo world`";
        let result = run(content).await.unwrap();
        assert!(result.contains("world"));
    }

    #[tokio::test]
    async fn test_inline_execution_whitespace_preceded() {
        let content = "Dir: !`echo /tmp`";
        let result = run(content).await.unwrap();
        assert!(result.contains("/tmp"));
        // Leading space preserved
        assert!(result.contains("Dir: "));
    }

    #[tokio::test]
    async fn test_no_shell_commands_unchanged() {
        let content = "No commands here.";
        let result = run(content).await.unwrap();
        assert_eq!(result, content);
    }

    #[tokio::test]
    async fn test_empty_output_replaced_with_empty_string() {
        // `cd .` exits 0 with no output on all platforms
        let content = "before !`cd .` after";
        let result = run(content).await.unwrap();
        assert_eq!(result, "before  after");
    }

    #[tokio::test]
    async fn test_multiple_inline_commands_are_rejected() {
        let content = "A: !`echo aaa` B: !`echo bbb`";
        let executor = CountingExecutor(AtomicUsize::new(0));
        let error = execute_shell_commands_with(content, LoadedFrom::Skills, &std::env::temp_dir(), Some(&executor))
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ShellExecutionError::TooManyCommands { count: 2, limit: 1 }
        ));
        assert_eq!(executor.0.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_stderr_formatted() {
        // Write to stderr only — cross-platform redirection
        let content = if cfg!(windows) {
            "!`echo err 1>&2`"
        } else {
            "!`echo err >&2`"
        };
        let result = run(content).await.unwrap();
        assert!(result.contains("[stderr]"));
        assert!(result.contains("err"));
    }
}

// ---------------------------------------------------------------------------
// Supplemental tests (tester role — split to keep file under 800 lines)
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "shell_supplemental_test.rs"]
mod shell_supplemental_test;
