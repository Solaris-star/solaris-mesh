use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};

use solaris_protocol::events::ToolCategory;
use solaris_types::effect::{EffectClass, EffectDescriptor, EffectReplayPolicy, ResourceFootprint};
use solaris_types::permission::PermissionMode;
use solaris_types::tool::{ClassifiedToolResult, JsonSchema, ToolResult, ToolResultStatus};

use crate::read_only_evidence::{EvidenceValidation, ReadOnlyEvidenceIndex, evidence_digest};
use crate::tool::ReadOnlyEvidenceScope;
use crate::write::{DEFAULT_SEARCH_MAX_ENTRIES, WorkspaceFileAccess, WorkspaceSearchPolicy};
use crate::{PreparedToolExecution, Tool, ToolExecutionContext};

const MAX_RESULTS: usize = 100;

pub struct GlobTool {
    file_access: WorkspaceFileAccess,
    read_only_evidence_index: Option<Arc<ReadOnlyEvidenceIndex>>,
}

impl GlobTool {
    pub fn new(cwd: PathBuf) -> Self {
        Self {
            file_access: WorkspaceFileAccess::new(cwd),
            read_only_evidence_index: None,
        }
    }

    pub fn new_with_search_policy(cwd: PathBuf, search_policy: Arc<dyn WorkspaceSearchPolicy>) -> Self {
        Self {
            file_access: WorkspaceFileAccess::new_with_search_policy(cwd, search_policy),
            read_only_evidence_index: None,
        }
    }

    pub fn with_read_only_evidence_index(mut self, index: Arc<ReadOnlyEvidenceIndex>) -> Self {
        self.read_only_evidence_index = Some(index);
        self
    }

    fn execute_observed(
        &self,
        input: &Value,
        evidence_scope: Option<(PermissionMode, &ReadOnlyEvidenceScope)>,
    ) -> ClassifiedToolResult {
        let Some(pattern) = input["pattern"].as_str() else {
            return glob_error("Missing required parameter: pattern").classified(ToolResultStatus::Failed);
        };
        let root = input["path"].as_str().unwrap_or(".");
        if let Err(error) = self.file_access.refresh_policy() {
            return glob_error(format!("Failed to refresh workspace protection: {error}"))
                .classified(ToolResultStatus::Failed);
        }
        let root_path = self.file_access.resolve_path(Path::new(root));
        tracing::debug!(resolved_root = %root_path.display(), pattern = %pattern, "GlobTool scanning");

        let pattern_path = Path::new(pattern);
        if pattern_path.is_absolute()
            || pattern_path.components().any(|component| {
                matches!(
                    component,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
        {
            return glob_error("Glob pattern must be relative to the requested workspace root")
                .classified(ToolResultStatus::Failed);
        }
        let matcher = match glob::Pattern::new(pattern) {
            Ok(pattern) => pattern,
            Err(error) => {
                return glob_error(format!("Invalid glob pattern: {error}")).classified(ToolResultStatus::Failed);
            }
        };
        let candidates = match self.file_access.collect_paths(&root_path, DEFAULT_SEARCH_MAX_ENTRIES) {
            Ok(files) => files,
            Err(error) => return glob_error(error).classified(ToolResultStatus::Failed),
        };
        let mut matches: Vec<_> = candidates
            .into_iter()
            .filter(|file| matcher.matches_path(&file.relative_path))
            .collect();
        let mut traversal_parts = Vec::with_capacity(matches.len());
        let mut evidence_objects = Vec::with_capacity(matches.len());
        let mut identities_complete = true;
        for file in &matches {
            let label = file.relative_path.to_string_lossy().into_owned();
            traversal_parts.push(label.as_bytes().to_vec());
            if let Some(identity) = file.identity.as_ref() {
                identities_complete &= identity
                    .current_state()
                    .is_ok_and(|state| state.matches_observed_metadata(file.size, file.modified));
                evidence_objects.push((label, Arc::clone(identity)));
            } else {
                identities_complete = false;
            }
        }
        traversal_parts.sort();
        let validation = identities_complete
            .then(|| EvidenceValidation::from_opened_objects(evidence_objects, evidence_digest(traversal_parts.iter())))
            .flatten();
        if let (Some(index), Some(validation), Some((mode, scope))) =
            (&self.read_only_evidence_index, validation.as_ref(), evidence_scope)
            && let Some(hit) = index.lookup(self.name(), input, mode, scope, validation)
        {
            return hit;
        }

        matches.sort_by(|left, right| {
            right.modified.cmp(&left.modified).then_with(|| {
                left.relative_path
                    .to_string_lossy()
                    .cmp(&right.relative_path.to_string_lossy())
            })
        });
        matches.truncate(MAX_RESULTS);
        let result = if matches.is_empty() {
            ToolResult {
                content: "No files matched the pattern".to_owned(),
                is_error: false,
            }
        } else {
            ToolResult {
                content: matches
                    .into_iter()
                    .map(|file| file.relative_path.display().to_string())
                    .collect::<Vec<_>>()
                    .join("\n"),
                is_error: false,
            }
        };
        let classified = result.classified(ToolResultStatus::Executed);
        match (&self.read_only_evidence_index, validation, evidence_scope) {
            (Some(index), Some(validation), Some((mode, scope))) => {
                index.insert(self.name(), input, mode, scope, validation, classified)
            }
            _ => classified,
        }
    }
}

#[async_trait]
impl Tool for GlobTool {
    fn name(&self) -> &str {
        "Glob"
    }

    fn description(&self) -> &str {
        "Fast file pattern matching tool that works with any codebase size.\n\n\
         - Supports glob patterns like \"**/*.rs\" or \"src/**/*.ts\".\n\
         - Returns matching file paths sorted by modification time (newest first).\n\
         - Returns at most 100 results. Only returns files, not directories.\n\
         - The path parameter defaults to the current working directory.\n\
         - Use this tool when you need to find files by name or extension patterns."
    }

    fn input_schema(&self) -> JsonSchema {
        json!({
            "type": "object",
            "properties": {
                "pattern": {
                    "type": "string",
                    "description": "Glob pattern, e.g. \"**/*.rs\""
                },
                "path": {
                    "type": "string",
                    "description": "Root directory (default: cwd)"
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
            read_only_evidence_index: self.read_only_evidence_index.clone(),
        };
        Ok(PreparedToolExecution::new_classified(
            None,
            Box::pin(async move { tool.execute_observed(&input, evidence_scope.as_ref().map(|scope| (mode, scope))) }),
        ))
    }

    async fn execute(&self, input: Value) -> ToolResult {
        self.execute_observed(&input, None).into_legacy()
    }

    fn describe_effect(&self, input: &Value) -> EffectDescriptor {
        let raw_path = input.get("path").and_then(Value::as_str).unwrap_or(".");
        let root = self
            .file_access
            .resolve_path(Path::new(raw_path))
            .to_string_lossy()
            .into_owned();
        EffectDescriptor {
            class: EffectClass::ReadOnly,
            action: format!("Search files in {root}"),
            resources: ResourceFootprint {
                file_reads: vec![root],
                ..Default::default()
            },
            replay_policy: EffectReplayPolicy::ReplaySafe,
        }
    }

    fn category(&self) -> ToolCategory {
        ToolCategory::Info
    }

    fn describe(&self, input: &Value) -> String {
        let pattern = input.get("pattern").and_then(|v| v.as_str()).unwrap_or("*");
        format!("Search for {}", pattern)
    }
}

fn glob_error(message: impl Into<String>) -> ToolResult {
    ToolResult {
        content: message.into(),
        is_error: true,
    }
}

#[cfg(test)]
#[path = "glob_test.rs"]
mod glob_test;
