use std::path::Path;
use std::sync::{Arc, RwLock};

use solaris_config::config::Config;
use solaris_tools::edit::EditTool;
use solaris_tools::exec_command::ExecCommandTool;
use solaris_tools::file_cache::FileStateCache;
use solaris_tools::glob::GlobTool;
use solaris_tools::grep::GrepTool;
use solaris_tools::read::ReadTool;
use solaris_tools::read_only_evidence::ReadOnlyEvidenceIndex;
use solaris_tools::registry::ToolRegistry;
use solaris_tools::write::WriteTool;

use crate::permission_engine::PermissionContext;

pub(super) fn build_builtin_registry(
    config: &Config,
    permissions: &PermissionContext,
    workspace_path: &Path,
    runtime_env: &[(String, String)],
    read_only_evidence_index: Arc<ReadOnlyEvidenceIndex>,
) -> ToolRegistry {
    let file_cache = config
        .file_cache
        .enabled
        .then(|| Arc::new(RwLock::new(FileStateCache::new(&config.file_cache))));
    let search_policy = permissions.workspace_search_policy();
    let mut registry = ToolRegistry::new();

    registry.register(Box::new(
        ReadTool::new_with_search_policy(file_cache.clone(), workspace_path, Arc::clone(&search_policy))
            .with_read_only_evidence_index(Arc::clone(&read_only_evidence_index)),
    ));
    registry.register(Box::new(WriteTool::new_with_search_policy(
        file_cache.clone(),
        workspace_path,
        Arc::clone(&search_policy),
    )));
    registry.register(Box::new(EditTool::new_with_search_policy(
        file_cache,
        workspace_path,
        Arc::clone(&search_policy),
    )));
    registry.register(Box::new(ExecCommandTool::new_with_env(
        workspace_path.to_path_buf(),
        runtime_env.to_vec(),
    )));
    registry.register(Box::new(
        GrepTool::new_with_search_policy(workspace_path.to_path_buf(), Arc::clone(&search_policy))
            .with_read_only_evidence_index(Arc::clone(&read_only_evidence_index)),
    ));
    registry.register(Box::new(
        GlobTool::new_with_search_policy(workspace_path.to_path_buf(), search_policy)
            .with_read_only_evidence_index(read_only_evidence_index),
    ));
    registry
}
