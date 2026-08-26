use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use serde_json::{Value, json};

use solaris_protocol::events::ToolCategory;
use solaris_types::effect::{EffectClass, EffectDescriptor, EffectReplayPolicy, ResourceFootprint};
use solaris_types::permission::PermissionMode;
use solaris_types::tool::{ClassifiedToolResult, JsonSchema, ToolResult, ToolResultStatus};

use crate::read_only_evidence::{EvidenceValidation, ReadOnlyEvidenceIndex, evidence_digest};
use crate::tool::ReadOnlyEvidenceScope;
use crate::write::{
    DEFAULT_SEARCH_MAX_ENTRIES, WorkspaceFileAccess, WorkspaceSearchPolicy, record_skipped_search_candidate,
};
use crate::{PreparedToolExecution, Tool, ToolExecutionContext};

const MAX_FILE_BYTES: usize = 8 * 1024 * 1024;
const MAX_SCAN_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy)]
struct GrepLimits {
    max_file_bytes: usize,
    max_scan_bytes: usize,
}

pub struct GrepTool {
    file_access: WorkspaceFileAccess,
    limits: GrepLimits,
    read_only_evidence_index: Option<Arc<ReadOnlyEvidenceIndex>>,
    body_scan_counter: Option<Arc<AtomicUsize>>,
    regex_scan_counter: Option<Arc<AtomicUsize>>,
}

impl GrepTool {
    pub fn new(cwd: PathBuf) -> Self {
        Self {
            file_access: WorkspaceFileAccess::new(cwd),
            limits: GrepLimits::default(),
            read_only_evidence_index: None,
            body_scan_counter: None,
            regex_scan_counter: None,
        }
    }

    pub fn new_with_search_policy(cwd: PathBuf, search_policy: Arc<dyn WorkspaceSearchPolicy>) -> Self {
        Self {
            file_access: WorkspaceFileAccess::new_with_search_policy(cwd, search_policy),
            limits: GrepLimits::default(),
            read_only_evidence_index: None,
            body_scan_counter: None,
            regex_scan_counter: None,
        }
    }

    #[cfg(test)]
    fn new_with_limits(cwd: PathBuf, max_file_bytes: usize, max_scan_bytes: usize) -> Self {
        Self {
            file_access: WorkspaceFileAccess::new(cwd),
            limits: GrepLimits {
                max_file_bytes,
                max_scan_bytes,
            },
            read_only_evidence_index: None,
            body_scan_counter: None,
            regex_scan_counter: None,
        }
    }

    pub fn with_read_only_evidence_index(mut self, index: Arc<ReadOnlyEvidenceIndex>) -> Self {
        self.read_only_evidence_index = Some(index);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_body_scan_counter(mut self, counter: Arc<AtomicUsize>) -> Self {
        self.body_scan_counter = Some(counter);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_regex_scan_counter(mut self, counter: Arc<AtomicUsize>) -> Self {
        self.regex_scan_counter = Some(counter);
        self
    }

    async fn execute_observed(
        &self,
        input: &Value,
        evidence_scope: Option<(PermissionMode, ReadOnlyEvidenceScope)>,
    ) -> ClassifiedToolResult {
        let Some(pattern) = input["pattern"].as_str() else {
            return grep_error("Missing required parameter: pattern");
        };
        let raw_path = input["path"].as_str().unwrap_or(".");
        if let Err(error) = self.file_access.refresh_policy() {
            return grep_error(format!("Failed to refresh workspace protection: {error}"));
        }
        let path = self.file_access.resolve_path(Path::new(raw_path));
        tracing::debug!(resolved_path = %path.display(), pattern = %pattern, "GrepTool searching");

        let pattern = pattern.to_owned();
        let glob_pattern = input["glob"].as_str().map(str::to_owned);
        let case_insensitive = input["case_insensitive"].as_bool().unwrap_or(false);
        let file_access = self.file_access.clone();
        let limits = self.limits;
        let evidence_reuse =
            self.read_only_evidence_index
                .as_ref()
                .zip(evidence_scope)
                .map(|(index, (mode, scope))| GrepEvidenceReuse {
                    index: Arc::clone(index),
                    input: input.clone(),
                    mode,
                    scope,
                });
        let body_scan_counter = self.body_scan_counter.clone();
        let regex_scan_counter = self.regex_scan_counter.clone();
        match tokio::task::spawn_blocking(move || {
            search_files(GrepSearchRequest {
                pattern: &pattern,
                file_access: &file_access,
                path: &path,
                glob_pattern: glob_pattern.as_deref(),
                case_insensitive,
                limits,
                evidence_reuse: evidence_reuse.as_ref(),
                body_scan_counter: body_scan_counter.as_deref(),
                regex_scan_counter: regex_scan_counter.as_deref(),
            })
        })
        .await
        {
            Ok(result) => result,
            Err(error) => grep_error(format!("grep worker failed: {error}")),
        }
    }
}

struct GrepEvidenceReuse {
    index: Arc<ReadOnlyEvidenceIndex>,
    input: Value,
    mode: PermissionMode,
    scope: ReadOnlyEvidenceScope,
}

fn grep_error(message: impl Into<String>) -> ClassifiedToolResult {
    ClassifiedToolResult::new(message, ToolResultStatus::Failed)
}

impl Default for GrepLimits {
    fn default() -> Self {
        Self {
            max_file_bytes: MAX_FILE_BYTES,
            max_scan_bytes: MAX_SCAN_BYTES,
        }
    }
}

#[async_trait]
impl Tool for GrepTool {
    fn name(&self) -> &str {
        "Grep"
    }

    fn description(&self) -> &str {
        "Searches file contents using regex patterns.\n\n\
         IMPORTANT: ALWAYS use this Grep tool for content search. \
         NEVER run grep or rg as a ExecCommand command.\n\n\
         - Supports full regex syntax (e.g., \"log.*Error\", \"fn\\\\s+\\\\w+\").\n\
         - Use the glob parameter to filter by file pattern (e.g., \"*.rs\").\n\
         - Output is truncated to 250 lines.\n\
         - Set case_insensitive to true for case-insensitive search."
    }

    fn input_schema(&self) -> JsonSchema {
        json!({
            "type": "object",
            "properties": {
                "pattern": {
                    "type": "string",
                    "description": "The regex pattern to search for"
                },
                "path": {
                    "type": "string",
                    "description": "Directory to search in (default: cwd)"
                },
                "glob": {
                    "type": "string",
                    "description": "File filter pattern, e.g. \"*.rs\""
                },
                "case_insensitive": {
                    "type": "boolean",
                    "description": "Case insensitive search"
                }
            },
            "required": ["pattern"]
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
            file_access: self.file_access.scoped_to_permission_mode(mode),
            limits: self.limits,
            read_only_evidence_index: self.read_only_evidence_index.clone(),
            body_scan_counter: self.body_scan_counter.clone(),
            regex_scan_counter: self.regex_scan_counter.clone(),
        };
        Ok(PreparedToolExecution::new_classified(
            None,
            Box::pin(async move {
                tool.execute_observed(&input, evidence_scope.map(|scope| (mode, scope)))
                    .await
            }),
        ))
    }

    async fn execute(&self, input: Value) -> ToolResult {
        self.execute_observed(&input, None).await.into_legacy()
    }

    fn describe_effect(&self, input: &Value) -> EffectDescriptor {
        let raw_path = input.get("path").and_then(Value::as_str).unwrap_or(".");
        let path = self
            .file_access
            .resolve_path(Path::new(raw_path))
            .to_string_lossy()
            .into_owned();
        EffectDescriptor {
            class: EffectClass::ReadOnly,
            action: format!("Search file contents in {path}"),
            resources: ResourceFootprint {
                file_reads: vec![path],
                ..Default::default()
            },
            replay_policy: EffectReplayPolicy::ReplaySafe,
        }
    }

    fn max_result_size(&self) -> usize {
        20_000
    }

    fn category(&self) -> ToolCategory {
        ToolCategory::Info
    }

    fn describe(&self, input: &Value) -> String {
        let pattern = input.get("pattern").and_then(|v| v.as_str()).unwrap_or("");
        let raw_path = input.get("path").and_then(|v| v.as_str()).unwrap_or(".");
        format!("Grep '{}' in {}", pattern, raw_path)
    }
}

struct GrepSearchRequest<'a> {
    pattern: &'a str,
    file_access: &'a WorkspaceFileAccess,
    path: &'a Path,
    glob_pattern: Option<&'a str>,
    case_insensitive: bool,
    limits: GrepLimits,
    evidence_reuse: Option<&'a GrepEvidenceReuse>,
    body_scan_counter: Option<&'a AtomicUsize>,
    regex_scan_counter: Option<&'a AtomicUsize>,
}

fn search_files(request: GrepSearchRequest<'_>) -> ClassifiedToolResult {
    let GrepSearchRequest {
        pattern,
        file_access,
        path,
        glob_pattern,
        case_insensitive,
        limits,
        evidence_reuse,
        body_scan_counter,
        regex_scan_counter,
    } = request;
    let regex = match regex::RegexBuilder::new(pattern)
        .case_insensitive(case_insensitive)
        .build()
    {
        Ok(regex) => regex,
        Err(error) => {
            return grep_error(format!("Invalid regex pattern: {error}"));
        }
    };
    let glob = match glob_pattern.map(glob::Pattern::new).transpose() {
        Ok(glob) => glob,
        Err(error) => {
            return grep_error(format!("Invalid glob pattern: {error}"));
        }
    };
    let files = match file_access.collect_paths(path, DEFAULT_SEARCH_MAX_ENTRIES) {
        Ok(files) => files,
        Err(error) => {
            return grep_error(error);
        }
    };
    let files: Vec<_> = files
        .into_iter()
        .filter(|file| {
            glob.as_ref().is_none_or(|pattern| {
                pattern.matches_path(&file.relative_path)
                    || file
                        .relative_path
                        .file_name()
                        .is_some_and(|name| pattern.matches_path(Path::new(name)))
            })
        })
        .collect();
    let traversal_parts: Vec<_> = files
        .iter()
        .map(|file| file.relative_path.to_string_lossy().as_bytes().to_vec())
        .collect();
    let traversal_digest = evidence_digest(traversal_parts.iter());
    let metadata_consistent = files.iter().all(|file| {
        file.identity.as_deref().is_some_and(|identity| {
            identity
                .current_state()
                .is_ok_and(|state| state.matches_observed_metadata(file.size, file.modified))
        })
    });
    let pre_validation = metadata_consistent
        .then(|| {
            EvidenceValidation::from_opened_objects(
                files.iter().filter_map(|file| {
                    Some((
                        file.relative_path.to_string_lossy().into_owned(),
                        Arc::clone(file.identity.as_ref()?),
                    ))
                }),
                traversal_digest.clone(),
            )
        })
        .flatten();
    if let (Some(reuse), Some(validation)) = (evidence_reuse, pre_validation.as_ref())
        && let Some(hit) = reuse
            .index
            .lookup("Grep", &reuse.input, reuse.mode, &reuse.scope, validation)
    {
        return hit;
    }

    let file_count = files.len();
    let mut matches = Vec::new();
    let mut scanned_bytes = 0usize;
    let mut cacheable = pre_validation.is_some();
    let mut final_objects = Vec::with_capacity(file_count);
    for file in files {
        let candidate = path.join(&file.relative_path);
        let label = file.relative_path.to_string_lossy().into_owned();
        let original_identity = file.identity;
        let Ok(file_size) = usize::try_from(file.size) else {
            if let Some(identity) = original_identity {
                final_objects.push((label, identity));
            } else {
                cacheable = false;
            }
            continue;
        };
        let remaining = limits.max_scan_bytes.saturating_sub(scanned_bytes);
        if matches.len() >= 250 || file_size > limits.max_file_bytes || file_size > remaining {
            if let Some(identity) = original_identity {
                final_objects.push((label, identity));
            } else {
                cacheable = false;
            }
            continue;
        }
        if let Some(counter) = body_scan_counter {
            counter.fetch_add(1, Ordering::SeqCst);
        }
        let snapshot = match file_access.read_snapshot_limited(&candidate, limits.max_file_bytes.min(remaining)) {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => {
                cacheable = false;
                continue;
            }
            Err(_) => {
                cacheable = false;
                record_skipped_search_candidate();
                continue;
            }
        };
        if original_identity
            .as_ref()
            .is_some_and(|identity| !identity.same_object(&snapshot.identity))
        {
            cacheable = false;
        }
        let bytes = snapshot.bytes;
        scanned_bytes = scanned_bytes.saturating_add(bytes.len());
        final_objects.push((label, snapshot.identity));
        let Ok(content) = std::str::from_utf8(&bytes) else {
            continue;
        };
        if let Some(counter) = regex_scan_counter {
            counter.fetch_add(1, Ordering::SeqCst);
        }
        for (index, line) in content.lines().enumerate() {
            if regex.is_match(line) {
                matches.push(format!("{}:{}:{}", candidate.display(), index + 1, line));
                if matches.len() >= 250 {
                    break;
                }
            }
        }
    }
    let result = ToolResult {
        content: if matches.is_empty() {
            "No matches found".to_owned()
        } else {
            matches.join("\n")
        },
        is_error: false,
    };
    let post_validation = (cacheable && final_objects.len() == file_count)
        .then(|| {
            EvidenceValidation::from_opened_objects(
                final_objects
                    .iter()
                    .map(|(label, identity)| (label.clone(), Arc::clone(identity))),
                traversal_digest,
            )
        })
        .flatten();
    let validation = pre_validation.filter(|before| post_validation.as_ref() == Some(before));
    let classified = result.classified(ToolResultStatus::Executed);
    match (evidence_reuse, validation) {
        (Some(reuse), Some(validation)) => {
            reuse
                .index
                .insert("Grep", &reuse.input, reuse.mode, &reuse.scope, validation, classified)
        }
        _ => classified,
    }
}

#[cfg(test)]
#[path = "grep_test.rs"]
mod grep_test;
