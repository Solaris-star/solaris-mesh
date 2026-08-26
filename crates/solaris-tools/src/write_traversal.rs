use std::path::PathBuf;
use std::sync::Arc;

use cap_std::fs::{Dir, ReadDir};
use solaris_config::file_identity::OpenedFileIdentity;

use super::{
    WorkspacePath, WorkspaceSearchPolicy, WorkspaceTraversalLimits, directory_identity, file_identity,
    record_skipped_search_candidate,
};

struct PendingDirectory {
    entries: ReadDir,
    identity: Arc<OpenedFileIdentity>,
    relative: PathBuf,
    depth: usize,
}

pub(super) fn collect_directory_paths(
    directory: Dir,
    identity: Arc<OpenedFileIdentity>,
    search_root: PathBuf,
    limits: WorkspaceTraversalLimits,
    search_policy: &dyn WorkspaceSearchPolicy,
    files: &mut Vec<WorkspacePath>,
) -> Result<(), String> {
    let entries = directory
        .entries()
        .map_err(|_| "failed to enumerate workspace search root".to_owned())?;
    let mut stack = vec![PendingDirectory {
        entries,
        identity,
        relative: PathBuf::new(),
        depth: 0,
    }];
    let mut visited_entries = 0usize;

    // Keep one iterator frame per nesting level. Pushing every discovered
    // sibling directory would hold up to `max_entries` handles at once.
    while let Some(pending) = stack.last_mut() {
        let Some(entry) = pending.entries.next() else {
            stack.pop();
            continue;
        };
        if visited_entries >= limits.max_entries {
            return Ok(());
        }
        visited_entries += 1;
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => {
                record_skipped_search_candidate();
                continue;
            }
        };
        let entry_depth = pending.depth.saturating_add(1);
        let file_name = entry.file_name();
        let entry_relative = pending.relative.join(&file_name);
        let absolute_path = search_root.join(&entry_relative);
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(_) => {
                record_skipped_search_candidate();
                continue;
            }
        };
        if file_type.is_symlink() {
            continue;
        }
        if file_type.is_dir() {
            if entry_depth > limits.max_depth {
                continue;
            }
            if !search_policy.allows_directory(&absolute_path) {
                continue;
            }
            let child = match entry.open_dir() {
                Ok(child) => child,
                Err(_) => {
                    record_skipped_search_candidate();
                    continue;
                }
            };
            let child_identity = match directory_identity(&child) {
                Ok(identity) => identity,
                Err(_) => {
                    record_skipped_search_candidate();
                    continue;
                }
            };
            if !search_policy.allows_opened_directory(&absolute_path, Arc::clone(&child_identity)) {
                continue;
            }
            let entries = match child.entries() {
                Ok(entries) => entries,
                Err(_) => {
                    record_skipped_search_candidate();
                    continue;
                }
            };
            stack.push(PendingDirectory {
                entries,
                identity: child_identity,
                relative: entry_relative,
                depth: entry_depth,
            });
            continue;
        }
        if !file_type.is_file() || !search_policy.allows_file(&absolute_path) {
            continue;
        }
        if !search_policy.requires_opened_file_identity() {
            let metadata = match entry.metadata() {
                Ok(metadata) => metadata,
                Err(_) => {
                    record_skipped_search_candidate();
                    continue;
                }
            };
            let identity = entry.open().ok().and_then(|file| file_identity(&file).ok());
            files.push(WorkspacePath {
                relative_path: entry_relative,
                modified: metadata
                    .modified()
                    .map(|value| value.into_std())
                    .unwrap_or(std::time::SystemTime::UNIX_EPOCH),
                size: metadata.len(),
                identity,
            });
            continue;
        }
        if !search_policy.allows_file_slot(&absolute_path, &pending.identity, &file_name) {
            continue;
        }
        let file = match entry.open() {
            Ok(file) => file,
            Err(_) => {
                record_skipped_search_candidate();
                continue;
            }
        };
        let identity = match file_identity(&file) {
            Ok(identity) => identity,
            Err(_) => {
                record_skipped_search_candidate();
                continue;
            }
        };
        if !search_policy.allows_opened_file(&absolute_path, &pending.identity, &file_name, Arc::clone(&identity)) {
            continue;
        }
        let metadata = match file.metadata() {
            Ok(metadata) if metadata.is_file() => metadata,
            Ok(_) | Err(_) => {
                record_skipped_search_candidate();
                continue;
            }
        };
        files.push(WorkspacePath {
            relative_path: entry_relative,
            modified: metadata
                .modified()
                .map(|value| value.into_std())
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH),
            size: metadata.len(),
            identity: Some(identity),
        });
    }
    Ok(())
}
