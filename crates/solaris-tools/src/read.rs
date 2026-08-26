use std::io::BufRead;
use std::path::Path;
use std::sync::{Arc, RwLock};

#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use serde_json::{Value, json};

use solaris_protocol::events::ToolCategory;
use solaris_types::effect::{EffectDescriptor, EffectReplayPolicy, ResourceFootprint};
use solaris_types::file_state::FileState;
use solaris_types::tool::{JsonSchema, ToolResult, ToolResultStatus};

use crate::file_cache::{FileStateCache, content_digest};
use crate::read_only_evidence::{EvidenceValidation, ReadOnlyEvidenceIndex};
use crate::write::{WorkspaceFileAccess, WorkspaceSearchPolicy};
use crate::{PreparedToolExecution, Tool, ToolExecutionContext};

/// Stub returned when a file has not changed since the model last read it.
/// Saves tokens by avoiding re-sending identical content.
const FILE_UNCHANGED_STUB: &str = "File unchanged since last read. The content from the earlier Read \
     tool_result in this conversation is still current — refer to that \
     instead of re-reading. If compaction cleared that result and you need \
     the content again, retry this exact Read with force set to true.";

const READ_BUFFER_BYTES: usize = 8 * 1024;
const MAX_READ_OUTPUT_BYTES: usize = 8 * 1024 * 1024;
const MAX_READ_SCAN_BYTES: usize = 64 * 1024 * 1024;
const READ_RANGE_OVERFLOW_ERROR: &str = "Invalid read range: offset + limit exceeds the supported range";
const READ_OUTPUT_LIMIT_ERROR: &str = "Read exceeds the 8388608-byte output limit";
const READ_SCAN_LIMIT_ERROR: &str = "Read exceeds the 67108864-byte scan limit";

pub struct ReadTool {
    file_cache: Option<Arc<RwLock<FileStateCache>>>,
    file_access: WorkspaceFileAccess,
    read_only_evidence_index: Option<Arc<ReadOnlyEvidenceIndex>>,
    #[cfg(test)]
    body_read_counter: Option<Arc<AtomicUsize>>,
}

impl ReadTool {
    /// Create a ReadTool restricted to the current workspace directory.
    ///
    /// Pass `None` to disable caching (all reads return full content).
    pub fn new(file_cache: Option<Arc<RwLock<FileStateCache>>>) -> Self {
        let workspace_root = std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."));
        Self::new_with_workspace_root(file_cache, workspace_root)
    }

    pub fn new_with_workspace_root(
        file_cache: Option<Arc<RwLock<FileStateCache>>>,
        workspace_root: impl Into<std::path::PathBuf>,
    ) -> Self {
        Self {
            file_cache,
            file_access: WorkspaceFileAccess::new(workspace_root),
            read_only_evidence_index: None,
            #[cfg(test)]
            body_read_counter: None,
        }
    }

    pub fn new_with_search_policy(
        file_cache: Option<Arc<RwLock<FileStateCache>>>,
        workspace_root: impl Into<std::path::PathBuf>,
        search_policy: Arc<dyn WorkspaceSearchPolicy>,
    ) -> Self {
        Self {
            file_cache,
            file_access: WorkspaceFileAccess::new_with_search_policy(workspace_root, search_policy),
            read_only_evidence_index: None,
            #[cfg(test)]
            body_read_counter: None,
        }
    }

    pub fn with_read_only_evidence_index(mut self, index: Arc<ReadOnlyEvidenceIndex>) -> Self {
        self.read_only_evidence_index = Some(index);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_body_read_counter(mut self, counter: Arc<AtomicUsize>) -> Self {
        self.body_read_counter = Some(counter);
        self
    }

    fn shared_evidence_validation(&self, input: &Value) -> Option<EvidenceValidation> {
        if input.get("force").and_then(Value::as_bool) == Some(true)
            || input.get("limit").and_then(Value::as_u64) == Some(0)
        {
            return None;
        }
        let file_path = input.get("file_path").and_then(Value::as_str)?;
        self.file_access.refresh_policy().ok()?;
        let (identity, _) = self.file_access.inspect_file_identity(Path::new(file_path)).ok()?;
        EvidenceValidation::from_opened_object(file_path, identity)
    }

    fn record_body_read(&self) {
        #[cfg(test)]
        if let Some(counter) = &self.body_read_counter {
            counter.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn execute_observed(&self, input: &Value, use_legacy_cache: bool) -> (ToolResult, Option<EvidenceValidation>) {
        let Some(file_path) = input["file_path"].as_str() else {
            return (
                ToolResult {
                    content: "Missing required parameter: file_path".to_string(),
                    is_error: true,
                },
                None,
            );
        };

        let raw_offset = input["offset"].as_u64();
        let raw_limit = input["limit"].as_u64();
        if raw_offset
            .zip(raw_limit)
            .is_some_and(|(offset, limit)| offset.checked_add(limit).is_none())
        {
            return (read_error(READ_RANGE_OVERFLOW_ERROR), None);
        }
        let offset = match raw_offset.map(usize::try_from).transpose() {
            Ok(offset) => offset,
            Err(_) => {
                return (
                    read_error("Invalid read range: offset exceeds the supported range"),
                    None,
                );
            }
        };
        let limit = match raw_limit.map(usize::try_from).transpose() {
            Ok(limit) => limit,
            Err(_) => {
                return (
                    read_error("Invalid read range: limit exceeds the supported range"),
                    None,
                );
            }
        };
        if offset
            .zip(limit)
            .is_some_and(|(offset, limit)| offset.checked_add(limit).is_none())
        {
            return (read_error(READ_RANGE_OVERFLOW_ERROR), None);
        }
        let force = input["force"].as_bool().unwrap_or(false);

        if let Err(error) = self.file_access.refresh_policy() {
            return (
                read_error(format!("Failed to refresh workspace protection: {error}")),
                None,
            );
        }
        let mut opened = match self.file_access.open_reader(Path::new(file_path), READ_BUFFER_BYTES) {
            Ok(value) => value,
            Err(error) => {
                return (read_error(format!("Failed to read file {file_path}: {error}")), None);
            }
        };
        let evidence_state_before = opened.identity.current_state().ok();
        let mtime_ms = opened
            .modified
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|duration| u64::try_from(duration.as_millis()).ok());
        self.record_body_read();
        let binary = match opened.reader.fill_buf() {
            Ok(bytes) => bytes.iter().take(READ_BUFFER_BYTES).any(|&byte| byte == 0),
            Err(_) => {
                return (
                    read_error(format!(
                        "Failed to read file {file_path}: failed to inspect file contents"
                    )),
                    None,
                );
            }
        };
        if binary {
            return (
                ToolResult {
                    content: format!("(binary file, {} bytes)", opened.size),
                    is_error: false,
                },
                None,
            );
        }

        let effective_offset = offset.unwrap_or(0);
        let result_content = match read_numbered_lines(&mut opened.reader, effective_offset, limit) {
            Ok(content) => content,
            Err(error) => return (read_error(error), None),
        };
        let digest = content_digest(result_content.as_bytes());
        let validation = evidence_state_before
            .filter(|before| opened.identity.current_state().ok().as_ref() == Some(before))
            .and_then(|_| EvidenceValidation::from_opened_object(file_path, Arc::clone(&opened.identity)));

        if use_legacy_cache
            && !force
            && let Some(cache_arc) = &self.file_cache
            && let Ok(mut cache) = cache_arc.write()
            && cache.matches_opened(Path::new(file_path), &opened.identity, &digest, offset, limit) == Some(true)
        {
            return (
                ToolResult {
                    content: FILE_UNCHANGED_STUB.to_string(),
                    is_error: false,
                },
                validation,
            );
        }

        if let Some(cache_arc) = &self.file_cache
            && let (Ok(mut cache), Some(mtime)) = (cache_arc.write(), mtime_ms)
        {
            cache.insert_opened(
                file_path.into(),
                FileState {
                    content: result_content.clone(),
                    mtime_ms: mtime,
                    offset,
                    limit,
                },
                Arc::clone(&opened.identity),
                digest,
            );
        }

        (
            ToolResult {
                content: result_content,
                is_error: false,
            },
            validation,
        )
    }
}

#[async_trait]
impl Tool for ReadTool {
    fn name(&self) -> &str {
        "Read"
    }

    fn description(&self) -> &str {
        "Reads a file from the local filesystem. Returns content with line numbers.\n\n\
         Usage:\n\
         - The file_path parameter must be an absolute path, not a relative path.\n\
         - By default, it reads the entire file. Use offset and limit for partial reads on large files.\n\
         - Selected output is limited to 8 MiB and line scanning is limited to 64 MiB.\n\
         - Results are returned with line numbers (1-based) followed by a tab and the line content.\n\
         - Binary files return \"(binary file, N bytes)\" instead of content.\n\
         - This tool can only read files, not directories. To list a directory, use ExecCommand with ls."
    }

    fn input_schema(&self) -> JsonSchema {
        json!({
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "The absolute path to the file to read"
                },
                "offset": {
                    "type": "integer",
                    "description": "Line number to start reading from (0-based)"
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of lines to read"
                },
                "force": {
                    "type": "boolean",
                    "description": "Return the file content even when an unchanged cached read exists (default false). Use after compaction cleared the earlier tool result."
                }
            },
            "required": ["file_path"]
        })
    }

    fn is_concurrency_safe(&self, _input: &Value) -> bool {
        true
    }

    fn prepare_execution<'a>(
        &'a self,
        input: Value,
        context: ToolExecutionContext,
    ) -> Result<PreparedToolExecution<'a>, String> {
        let mode = context
            .permission_mode()
            .ok_or_else(|| "missing effective permission mode for file tool execution".to_owned())?;
        let evidence_scope = context.read_only_evidence_scope().cloned();
        let tool = Self {
            file_cache: self.file_cache.clone(),
            file_access: self.file_access.scoped_to_permission_mode(mode),
            read_only_evidence_index: self.read_only_evidence_index.clone(),
            #[cfg(test)]
            body_read_counter: self.body_read_counter.clone(),
        };
        Ok(PreparedToolExecution::new_classified(
            None,
            Box::pin(async move {
                if let (Some(index), Some(scope), Some(validation)) = (
                    &tool.read_only_evidence_index,
                    evidence_scope.as_ref(),
                    tool.shared_evidence_validation(&input),
                ) && let Some(hit) = index.lookup(tool.name(), &input, mode, scope, &validation)
                {
                    return hit;
                }
                let (result, validation) = tool.execute_observed(&input, evidence_scope.is_none());
                let status = tool.classify_result(&input, &result);
                let classified = result.classified(status);
                if input.get("force").and_then(Value::as_bool) == Some(true) {
                    return classified;
                }
                match (&tool.read_only_evidence_index, evidence_scope.as_ref(), validation) {
                    (Some(index), Some(scope), Some(validation)) => {
                        index.insert(tool.name(), &input, mode, scope, validation, classified)
                    }
                    _ => classified,
                }
            }),
        ))
    }

    async fn execute(&self, input: Value) -> ToolResult {
        self.execute_observed(&input, true).0
    }

    fn classify_result(&self, input: &Value, result: &ToolResult) -> ToolResultStatus {
        if result.is_error {
            ToolResultStatus::Failed
        } else if result.content == FILE_UNCHANGED_STUB {
            ToolResultStatus::CacheHit
        } else if input.get("limit").and_then(Value::as_u64) == Some(0) {
            ToolResultStatus::Noop
        } else {
            ToolResultStatus::Executed
        }
    }

    fn on_result_compacted(&self, input: &Value) {
        let Some(file_path) = input["file_path"].as_str() else {
            return;
        };
        if let Some(cache) = &self.file_cache
            && let Ok(mut cache) = cache.write()
        {
            cache.remove(Path::new(file_path));
        }
    }

    fn on_history_compacted(&self) {
        if let Some(cache) = &self.file_cache
            && let Ok(mut cache) = cache.write()
        {
            cache.clear();
        }
    }

    fn describe_effect(&self, input: &Value) -> EffectDescriptor {
        let requested = input.get("file_path").and_then(Value::as_str).unwrap_or("unknown");
        let path = self.file_access.resolve_path(Path::new(requested));
        let path = path.to_string_lossy();
        EffectDescriptor {
            class: solaris_types::effect::EffectClass::ReadOnly,
            action: format!("Read {path}"),
            resources: ResourceFootprint {
                file_reads: vec![path.into_owned()],
                ..Default::default()
            },
            replay_policy: EffectReplayPolicy::ReplaySafe,
        }
    }

    fn max_result_size(&self) -> usize {
        100_000
    }

    fn category(&self) -> ToolCategory {
        ToolCategory::Info
    }

    fn describe(&self, input: &Value) -> String {
        let path = input.get("file_path").and_then(|v| v.as_str()).unwrap_or("unknown");
        format!("Read {}", path)
    }
}

fn read_error(message: impl Into<String>) -> ToolResult {
    ToolResult {
        content: message.into(),
        is_error: true,
    }
}

fn read_numbered_lines(reader: &mut impl BufRead, offset: usize, limit: Option<usize>) -> Result<String, String> {
    if limit == Some(0) {
        return Ok(String::new());
    }
    let mut result = String::new();
    let mut line_bytes = Vec::new();
    let mut current_line = 0usize;
    let mut selected_lines = 0usize;
    let mut scanned_bytes = 0usize;
    let mut line_has_content = false;

    loop {
        let buffer = reader
            .fill_buf()
            .map_err(|_| "Failed to read file contents".to_owned())?;
        if buffer.is_empty() {
            if line_has_content {
                append_selected_line(
                    &mut result,
                    &mut line_bytes,
                    current_line,
                    offset,
                    limit,
                    &mut selected_lines,
                    false,
                )?;
            }
            return Ok(result);
        }

        let newline = buffer.iter().position(|&byte| byte == b'\n');
        let segment_end = newline.unwrap_or(buffer.len());
        let consumed = segment_end.saturating_add(usize::from(newline.is_some()));
        scanned_bytes = scanned_bytes
            .checked_add(consumed)
            .ok_or_else(|| READ_SCAN_LIMIT_ERROR.to_owned())?;
        if scanned_bytes > MAX_READ_SCAN_BYTES {
            return Err(READ_SCAN_LIMIT_ERROR.to_owned());
        }
        line_has_content |= segment_end > 0;
        if line_is_selected(current_line, offset, limit, selected_lines) {
            let new_len = line_bytes
                .len()
                .checked_add(segment_end)
                .ok_or_else(|| READ_OUTPUT_LIMIT_ERROR.to_owned())?;
            if new_len > MAX_READ_OUTPUT_BYTES {
                return Err(READ_OUTPUT_LIMIT_ERROR.to_owned());
            }
            line_bytes.extend_from_slice(&buffer[..segment_end]);
        }
        reader.consume(consumed);

        if newline.is_some() {
            append_selected_line(
                &mut result,
                &mut line_bytes,
                current_line,
                offset,
                limit,
                &mut selected_lines,
                true,
            )?;
            if limit.is_some_and(|limit| selected_lines >= limit) {
                return Ok(result);
            }
            current_line = current_line
                .checked_add(1)
                .ok_or_else(|| READ_RANGE_OVERFLOW_ERROR.to_owned())?;
            line_has_content = false;
        }
    }
}

fn line_is_selected(current_line: usize, offset: usize, limit: Option<usize>, selected_lines: usize) -> bool {
    current_line >= offset && limit.is_none_or(|limit| selected_lines < limit)
}

fn append_selected_line(
    result: &mut String,
    line_bytes: &mut Vec<u8>,
    current_line: usize,
    offset: usize,
    limit: Option<usize>,
    selected_lines: &mut usize,
    strip_trailing_carriage_return: bool,
) -> Result<(), String> {
    if !line_is_selected(current_line, offset, limit, *selected_lines) {
        line_bytes.clear();
        return Ok(());
    }
    if strip_trailing_carriage_return && line_bytes.last() == Some(&b'\r') {
        line_bytes.pop();
    }
    let line_number = current_line
        .checked_add(1)
        .ok_or_else(|| READ_RANGE_OVERFLOW_ERROR.to_owned())?;
    let text = String::from_utf8_lossy(line_bytes);
    let prefix = format!("{line_number:>6}\t");
    let separator_bytes = usize::from(!result.is_empty());
    let new_len = result
        .len()
        .checked_add(separator_bytes)
        .and_then(|length| length.checked_add(prefix.len()))
        .and_then(|length| length.checked_add(text.len()))
        .ok_or_else(|| READ_OUTPUT_LIMIT_ERROR.to_owned())?;
    if new_len > MAX_READ_OUTPUT_BYTES {
        return Err(READ_OUTPUT_LIMIT_ERROR.to_owned());
    }
    if !result.is_empty() {
        result.push('\n');
    }
    result.push_str(&prefix);
    result.push_str(&text);
    *selected_lines = selected_lines
        .checked_add(1)
        .ok_or_else(|| READ_RANGE_OVERFLOW_ERROR.to_owned())?;
    line_bytes.clear();
    Ok(())
}

#[cfg(test)]
#[path = "read_test.rs"]
mod read_test;
