use regex::Regex;
use std::sync::OnceLock;

use async_trait::async_trait;
use futures::future::join_all;
#[cfg(test)]
use solaris_process::{CommandRunner, DEFAULT_TIMEOUT};

use crate::types::LoadedFrom;

const MAX_EMBEDDED_SHELL_COMMANDS: usize = 1;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Parse and execute shell commands embedded in skill content.
///
/// Block pattern:  ```!\n<commands>\n```
/// Inline pattern: !`<command>` (preceded by start-of-line or whitespace)
///
/// Content containing more than one matched command is rejected.
/// MCP skills are silently skipped (content returned unchanged).
/// Command output replaces the original pattern in content.
pub async fn execute_shell_commands(
    content: &str,
    loaded_from: LoadedFrom,
    cwd: &std::path::Path,
) -> Result<String, ShellExecutionError> {
    execute_shell_commands_with(content, loaded_from, cwd, None).await
}

#[async_trait]
pub trait SkillShellExecutor: Send + Sync {
    async fn execute(&self, command: &str, cwd: &std::path::Path) -> Result<String, ShellExecutionError>;
}

pub async fn execute_shell_commands_with(
    content: &str,
    loaded_from: LoadedFrom,
    cwd: &std::path::Path,
    executor: Option<&dyn SkillShellExecutor>,
) -> Result<String, ShellExecutionError> {
    let matches = extract_shell_matches(content);
    if matches.len() > MAX_EMBEDDED_SHELL_COMMANDS {
        return Err(ShellExecutionError::TooManyCommands {
            count: matches.len(),
            limit: MAX_EMBEDDED_SHELL_COMMANDS,
        });
    }
    if loaded_from == LoadedFrom::Mcp {
        return Ok(content.to_owned());
    }

    if matches.is_empty() {
        return Ok(content.to_owned());
    }
    let futures: Vec<_> = matches
        .iter()
        .map(|matched| async {
            if let Some(executor) = executor {
                executor.execute(&matched.command, cwd).await
            } else {
                #[cfg(test)]
                {
                    execute_command(&matched.command, cwd).await
                }
                #[cfg(not(test))]
                {
                    Err(ShellExecutionError::EffectPipelineRequired)
                }
            }
        })
        .collect();
    let outputs: Vec<Result<String, ShellExecutionError>> = join_all(futures).await;
    let mut pairs: Vec<(usize, usize, String)> = Vec::with_capacity(matches.len());
    for (matched, result) in matches.iter().zip(outputs) {
        let output = result.map_err(|error| ShellExecutionError::CommandFailed {
            pattern: matched.full_match.clone(),
            output: error.to_string(),
        })?;
        pairs.push((matched.start, matched.end, output));
    }
    pairs.sort_by_key(|pair| std::cmp::Reverse(pair.0));
    let mut result = content.to_owned();
    for (start, end, output) in pairs {
        result.replace_range(start..end, &output);
    }
    Ok(result)
}

pub fn contains_shell_commands(content: &str) -> bool {
    !extract_shell_matches(content).is_empty()
}

pub fn shell_commands(content: &str) -> Vec<String> {
    extract_shell_matches(content)
        .into_iter()
        .map(|matched| matched.command)
        .collect()
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors that can occur during shell command execution.
#[derive(Debug, thiserror::Error)]
pub enum ShellExecutionError {
    #[error("Skill contains {count} embedded shell commands; the limit is {limit}")]
    TooManyCommands { count: usize, limit: usize },

    #[error("Shell command failed for pattern \"{pattern}\": {output}")]
    CommandFailed { pattern: String, output: String },

    #[error("Shell execution blocked for MCP skill")]
    McpBlocked,

    #[error("Embedded Skill shell commands are disabled unless an EffectRequest executor is provided")]
    EffectPipelineRequired,
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

/// A matched shell command with its byte range in the original content.
struct ShellMatch {
    /// Complete text to be replaced (full_match bytes in content[start..end])
    full_match: String,
    /// The command to execute
    command: String,
    /// Byte offset of `full_match` start in content
    start: usize,
    /// Byte offset one past the end of `full_match` in content
    end: usize,
}

// ---------------------------------------------------------------------------
// Regex helpers
// ---------------------------------------------------------------------------

/// Block regex: ```!\n<body>\n```
fn block_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?s)```!\s*\n([\s\S]*?)\n?```").expect("invalid block regex"))
}

/// Inline regex — two patterns needed because `regex` crate has no lookbehind:
///   1. Line-start: ^!`...`   (multiline mode)
///   2. Preceded by whitespace: ([ \t])!`...`
fn inline_line_start_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?m)^(!`([^`]+)`)").expect("invalid inline line-start regex"))
}

fn inline_whitespace_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"([ \t])(!`([^`]+)`)").expect("invalid inline whitespace regex"))
}

// ---------------------------------------------------------------------------
// extract_shell_matches
// ---------------------------------------------------------------------------

/// Extract all shell command matches from content, ordered by start position.
fn extract_shell_matches(content: &str) -> Vec<ShellMatch> {
    let mut matches: Vec<ShellMatch> = Vec::new();

    // Block matches: entire ```!...``` block is replaced
    for cap in block_regex().captures_iter(content) {
        let full = cap.get(0).unwrap();
        let command = cap.get(1).map_or("", |m| m.as_str()).trim().to_owned();
        matches.push(ShellMatch {
            full_match: full.as_str().to_owned(),
            command,
            start: full.start(),
            end: full.end(),
        });
    }

    // Track byte ranges already covered by block matches to avoid overlap
    let block_ranges: Vec<(usize, usize)> = matches.iter().map(|m| (m.start, m.end)).collect();

    let overlaps_block = |s: usize, e: usize| -> bool { block_ranges.iter().any(|(bs, be)| s < *be && e > *bs) };

    // Inline line-start: group(1) = full !`cmd`, group(2) = cmd
    for cap in inline_line_start_regex().captures_iter(content) {
        let full = cap.get(1).unwrap();
        let command = cap.get(2).unwrap().as_str().to_owned();
        if !overlaps_block(full.start(), full.end()) {
            matches.push(ShellMatch {
                full_match: full.as_str().to_owned(),
                command,
                start: full.start(),
                end: full.end(),
            });
        }
    }

    // Inline whitespace-preceded: group(1) = leading whitespace char,
    // group(2) = full !`cmd`, group(3) = cmd
    // We replace only the !`cmd` part (group 2), keeping the leading space intact.
    for cap in inline_whitespace_regex().captures_iter(content) {
        let full_match_group = cap.get(2).unwrap();
        let command = cap.get(3).unwrap().as_str().to_owned();
        if !overlaps_block(full_match_group.start(), full_match_group.end()) {
            matches.push(ShellMatch {
                full_match: full_match_group.as_str().to_owned(),
                command,
                start: full_match_group.start(),
                end: full_match_group.end(),
            });
        }
    }

    // Sort by start ascending (will be reversed before replacement)
    matches.sort_by_key(|m| m.start);

    // Deduplicate overlapping matches (keep first by start)
    let mut deduped: Vec<ShellMatch> = Vec::new();
    let mut last_end: usize = 0;
    for m in matches {
        if m.start >= last_end {
            last_end = m.end;
            deduped.push(m);
        }
    }

    deduped
}

#[cfg(test)]
fn format_output(stdout: &str, stderr: &str) -> String {
    match (stdout.is_empty(), stderr.is_empty()) {
        (false, false) => format!("{stdout}\n[stderr]\n{stderr}"),
        (false, true) => stdout.to_owned(),
        (true, false) => format!("[stderr]\n{stderr}"),
        (true, true) => String::new(),
    }
}

#[cfg(test)]
async fn execute_command(command: &str, cwd: &std::path::Path) -> Result<String, ShellExecutionError> {
    let shell = solaris_config::shell::default_shell();
    let mut command_builder = solaris_config::shell::shell_command_builder(&shell, command, false);
    command_builder.current_dir(cwd);
    let result =
        CommandRunner::new(command_builder)
            .run()
            .await
            .map_err(|error| ShellExecutionError::CommandFailed {
                pattern: command.to_owned(),
                output: error.to_string(),
            })?;
    let stdout = String::from_utf8_lossy(&result.stdout);
    let stderr = String::from_utf8_lossy(&result.stderr);
    let formatted = format_output(stdout.trim_end(), stderr.trim_end());
    if result.timed_out {
        return Err(ShellExecutionError::CommandFailed {
            pattern: command.to_owned(),
            output: format!("timed out after {}ms\n{formatted}", DEFAULT_TIMEOUT.as_millis()),
        });
    }
    if result.exit_code != Some(0) && stdout.is_empty() && stderr.is_empty() {
        return Err(ShellExecutionError::CommandFailed {
            pattern: command.to_owned(),
            output: format!("exit code {}", result.exit_code.unwrap_or(-1)),
        });
    }
    Ok(formatted)
}

#[cfg(test)]
#[path = "shell_test.rs"]
mod shell_test;
