use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use serde_json::{Value, json};

use solaris_protocol::events::ToolCategory;
use solaris_types::effect::{EffectClass, EffectDescriptor, EffectReplayPolicy, ResourceFootprint};
use solaris_types::tool::{JsonSchema, ToolResult};

use crate::file_cache::{FileStateCache, content_digest, numbered_content, update_cache_after_verified_write};
use crate::write::{WorkspaceFileAccess, WorkspaceSearchPolicy};
use crate::{PreparedToolExecution, Tool, ToolExecutionContext};

const MAX_EDIT_FILE_BYTES: usize = 8 * 1024 * 1024;

pub struct EditTool {
    file_cache: Option<Arc<RwLock<FileStateCache>>>,
    file_access: WorkspaceFileAccess,
}

impl EditTool {
    /// Create an EditTool with optional file state cache.
    ///
    /// When cache is `Some`, the tool enforces:
    /// - "Must Read first" guard (file must be in cache before editing)
    /// - Staleness detection (disk mtime must match cached mtime)
    /// - Post-write cache update (mtime + content refreshed after edit)
    ///
    /// Pass `None` to disable all cache-related guards (legacy behavior).
    pub fn new(file_cache: Option<Arc<RwLock<FileStateCache>>>) -> Self {
        Self::new_with_workspace_root(
            file_cache,
            std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
        )
    }

    pub fn new_with_workspace_root(
        file_cache: Option<Arc<RwLock<FileStateCache>>>,
        workspace_root: impl Into<PathBuf>,
    ) -> Self {
        Self {
            file_cache,
            file_access: WorkspaceFileAccess::new(workspace_root),
        }
    }

    pub fn new_with_search_policy(
        file_cache: Option<Arc<RwLock<FileStateCache>>>,
        workspace_root: impl Into<PathBuf>,
        search_policy: Arc<dyn WorkspaceSearchPolicy>,
    ) -> Self {
        Self {
            file_cache,
            file_access: WorkspaceFileAccess::new_with_search_policy(workspace_root, search_policy),
        }
    }
}

#[async_trait]
impl Tool for EditTool {
    fn name(&self) -> &str {
        "Edit"
    }

    fn description(&self) -> &str {
        "Performs exact string replacements in files.\n\n\
         Usage:\n\
         - You must use the Read tool first before editing a file.\n\
         - The old_string must be unique in the file. If multiple matches exist, \
         the edit will fail. Provide more surrounding context to make it unique, \
         or use replace_all to change every occurrence.\n\
         - Use replace_all for renaming variables or replacing all instances of a string.\n\
         - Source and resulting file content are each limited to 8 MiB.\n\
         - Prefer Edit over Write for modifying existing files — Edit only sends the diff.\n\
         - When matching text from Read output, preserve the exact indentation (tabs/spaces)."
    }

    fn input_schema(&self) -> JsonSchema {
        json!({
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "The absolute path to the file to modify"
                },
                "old_string": {
                    "type": "string",
                    "description": "The text to replace"
                },
                "new_string": {
                    "type": "string",
                    "description": "The replacement text"
                },
                "replace_all": {
                    "type": "boolean",
                    "description": "Replace all occurrences (default false)"
                }
            },
            "required": ["file_path", "old_string", "new_string"]
        })
    }

    fn is_concurrency_safe(&self, _input: &Value) -> bool {
        false
    }

    fn prepare_execution<'a>(
        &'a self,
        input: Value,
        context: ToolExecutionContext,
    ) -> Result<PreparedToolExecution<'a>, String> {
        let mode = context
            .permission_mode()
            .ok_or_else(|| "missing effective permission mode for file tool execution".to_owned())?;
        let tool = Self {
            file_cache: self.file_cache.clone(),
            file_access: self.file_access.scoped_to_permission_mode(mode),
        };
        Ok(PreparedToolExecution::new(
            None,
            Box::pin(async move { tool.execute(input).await }),
        ))
    }

    async fn execute(&self, input: Value) -> ToolResult {
        let Some(file_path) = input["file_path"].as_str() else {
            return ToolResult {
                content: "Missing required parameter: file_path".to_string(),
                is_error: true,
            };
        };
        let Some(old_string) = input["old_string"].as_str() else {
            return ToolResult {
                content: "Missing required parameter: old_string".to_string(),
                is_error: true,
            };
        };
        let Some(new_string) = input["new_string"].as_str() else {
            return ToolResult {
                content: "Missing required parameter: new_string".to_string(),
                is_error: true,
            };
        };
        if old_string.is_empty() {
            return ToolResult {
                content: "old_string must not be empty".to_owned(),
                is_error: true,
            };
        }
        let replace_all = input["replace_all"].as_bool().unwrap_or(false);

        if let Err(error) = self.file_access.refresh_policy() {
            return ToolResult {
                content: format!("Failed to refresh workspace protection: {error}"),
                is_error: true,
            };
        }

        let path = Path::new(file_path);
        let relative = match self.file_access.relative_path(path) {
            Ok(relative) => relative,
            Err(error) => {
                return ToolResult {
                    content: format!("Failed to resolve file: {error}"),
                    is_error: true,
                };
            }
        };

        let snapshot = match self.file_access.read_snapshot_limited(&relative, MAX_EDIT_FILE_BYTES) {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => {
                return ToolResult {
                    content: format!(
                        "Failed to read file {}: file exceeds the 8388608-byte edit limit",
                        file_path
                    ),
                    is_error: true,
                };
            }
            Err(e) => {
                return ToolResult {
                    content: format!("Failed to read file {}: {}", file_path, e),
                    is_error: true,
                };
            }
        };
        let content = match String::from_utf8(snapshot.bytes) {
            Ok(content) => content,
            Err(_) => {
                return ToolResult {
                    content: format!("Failed to read file {}: file is not UTF-8", file_path),
                    is_error: true,
                };
            }
        };
        if let Some(cache_arc) = &self.file_cache {
            let digest = content_digest(numbered_content(&content).as_bytes());
            let cache_match = match cache_arc.write() {
                Ok(mut cache) => cache.matches_opened(path, &snapshot.identity, &digest, None, None),
                Err(_) => {
                    return ToolResult {
                        content: "Failed to verify the prior Read state".to_owned(),
                        is_error: true,
                    };
                }
            };
            match cache_match {
                None => {
                    return ToolResult {
                        content: format!(
                            "You must Read {} before editing. Use the Read tool first \
                             so the file content is loaded into context.",
                            file_path
                        ),
                        is_error: true,
                    };
                }
                Some(false) => {
                    return ToolResult {
                        content: format!(
                            "File {} has been modified externally since last read. \
                             Read the file again to see the current content before editing.",
                            file_path
                        ),
                        is_error: true,
                    };
                }
                Some(true) => {}
            }
        }

        let match_count = content.matches(old_string).count();

        if match_count == 0 {
            return ToolResult {
                content: "old_string not found in file".to_string(),
                is_error: true,
            };
        }

        if match_count > 1 && !replace_all {
            return ToolResult {
                content: format!(
                    "Multiple matches found ({}). Use replace_all or provide more context.",
                    match_count
                ),
                is_error: true,
            };
        }

        let replacement_count = if replace_all { match_count } else { 1 };
        let Some(new_content_len) =
            replacement_output_len(content.len(), old_string.len(), new_string.len(), replacement_count)
        else {
            return ToolResult {
                content: "Edited content size exceeds the supported range".to_owned(),
                is_error: true,
            };
        };
        if new_content_len > MAX_EDIT_FILE_BYTES {
            return ToolResult {
                content: "Edited content exceeds the 8388608-byte edit limit".to_owned(),
                is_error: true,
            };
        }

        let new_content = if replace_all {
            content.replace(old_string, new_string)
        } else {
            content.replacen(old_string, new_string, 1)
        };

        if let Err(e) = self.file_access.write_atomic_if_unchanged(
            &relative,
            new_content.as_bytes(),
            Some((&snapshot.identity, content.as_bytes())),
        ) {
            return ToolResult {
                content: format!("Failed to write file: {}", e),
                is_error: true,
            };
        }

        // Bind the post-write cache entry to the object opened after the
        // atomic replacement. If inspection fails, discard the stale entry.
        if let Some(cache_arc) = &self.file_cache {
            match self.file_access.inspect_file_identity(path) {
                Ok((identity, modified)) => {
                    update_cache_after_verified_write(cache_arc, path, &new_content, identity, modified);
                }
                Err(_) => {
                    if let Ok(mut cache) = cache_arc.write() {
                        cache.remove(path);
                    }
                }
            }
        }

        ToolResult {
            content: format!("Edited {}: replaced {} occurrence(s)", file_path, match_count),
            is_error: false,
        }
    }

    fn describe_effect(&self, input: &Value) -> EffectDescriptor {
        let requested = input.get("file_path").and_then(Value::as_str).unwrap_or("unknown");
        let path = self.file_access.resolve_path(Path::new(requested));
        let path = path.to_string_lossy();
        EffectDescriptor {
            class: EffectClass::WorkspaceMutation,
            action: format!("Edit {path}"),
            resources: ResourceFootprint {
                file_reads: vec![path.to_string()],
                file_writes: vec![path.into_owned()],
                ..Default::default()
            },
            replay_policy: EffectReplayPolicy::ReconcileRequired,
        }
    }

    fn max_result_size(&self) -> usize {
        10_000
    }

    fn category(&self) -> ToolCategory {
        ToolCategory::Edit
    }

    fn describe(&self, input: &Value) -> String {
        let path = input.get("file_path").and_then(|v| v.as_str()).unwrap_or("unknown");
        format!("Edit {}", path)
    }
}

fn replacement_output_len(
    content_len: usize,
    old_len: usize,
    new_len: usize,
    replacement_count: usize,
) -> Option<usize> {
    let removed = old_len.checked_mul(replacement_count)?;
    let added = new_len.checked_mul(replacement_count)?;
    content_len.checked_sub(removed)?.checked_add(added)
}

#[cfg(test)]
#[path = "edit_test.rs"]
mod edit_test;
