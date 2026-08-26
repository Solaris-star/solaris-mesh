use std::fs::{FileTimes, OpenOptions};
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::json;

use solaris_types::permission::PermissionMode;
use solaris_types::tool::ToolResultStatus;

use crate::edit::EditTool;
use crate::glob::GlobTool;
use crate::grep::GrepTool;
use crate::read::ReadTool;
use crate::read_only_evidence::ReadOnlyEvidenceIndex;
use crate::write::WriteTool;
use crate::{Tool, ToolExecutionContext};

fn execution_context(effect_id: &str, permission_digest: &str, environment_digest: &str) -> ToolExecutionContext {
    ToolExecutionContext::new(effect_id)
        .with_permission_mode(PermissionMode::Auto)
        .with_read_only_evidence_scope(permission_digest, environment_digest)
}

#[tokio::test]
async fn root_and_child_reuse_read_evidence_without_second_body_read() {
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("shared.txt");
    std::fs::write(&path, "shared evidence\n").unwrap();
    let evidence = Arc::new(ReadOnlyEvidenceIndex::default());
    let body_reads = Arc::new(AtomicUsize::new(0));
    let root = ReadTool::new_with_workspace_root(None, workspace.path())
        .with_read_only_evidence_index(Arc::clone(&evidence))
        .with_body_read_counter(Arc::clone(&body_reads));
    let child = ReadTool::new_with_workspace_root(None, workspace.path())
        .with_read_only_evidence_index(evidence)
        .with_body_read_counter(Arc::clone(&body_reads));
    let input = json!({"file_path": path});
    let equivalent_input = json!({"file_path": path, "offset": 0, "force": false});

    let root_result = root
        .prepare_execution(
            input.clone(),
            execution_context("root", "permission-a", "environment-a"),
        )
        .unwrap()
        .execute_classified()
        .await;
    let child_result = child
        .prepare_execution(
            equivalent_input,
            execution_context("child", "permission-a", "environment-a"),
        )
        .unwrap()
        .execute_classified()
        .await;

    assert_eq!(root_result.status, ToolResultStatus::Executed);
    assert_eq!(child_result.status, ToolResultStatus::CacheHit);
    assert_eq!(child_result.content, root_result.content);
    assert!(child_result.content.contains("shared evidence"));
    assert_eq!(body_reads.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn permission_or_environment_change_prevents_reuse() {
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("scope.txt");
    std::fs::write(&path, "scoped\n").unwrap();
    let evidence = Arc::new(ReadOnlyEvidenceIndex::default());
    let tool = ReadTool::new_with_workspace_root(None, workspace.path()).with_read_only_evidence_index(evidence);
    let input = json!({"file_path": path});

    let first = tool
        .prepare_execution(input.clone(), execution_context("one", "permission-a", "environment-a"))
        .unwrap()
        .execute_classified()
        .await;
    let permission_changed = tool
        .prepare_execution(input.clone(), execution_context("two", "permission-b", "environment-a"))
        .unwrap()
        .execute_classified()
        .await;
    let environment_changed = tool
        .prepare_execution(
            input.clone(),
            execution_context("three", "permission-b", "environment-b"),
        )
        .unwrap()
        .execute_classified()
        .await;
    let same_scope = tool
        .prepare_execution(input, execution_context("four", "permission-b", "environment-b"))
        .unwrap()
        .execute_classified()
        .await;

    assert_eq!(first.status, ToolResultStatus::Executed);
    assert_eq!(permission_changed.status, ToolResultStatus::Executed);
    assert_eq!(environment_changed.status, ToolResultStatus::Executed);
    assert_eq!(same_scope.status, ToolResultStatus::CacheHit);
}

#[tokio::test]
async fn oversized_evidence_is_not_retained() {
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("oversized.txt");
    std::fs::write(&path, "too large for the evidence budget\n").unwrap();
    let evidence = Arc::new(ReadOnlyEvidenceIndex::with_limits(4, 4));
    let tool = ReadTool::new_with_workspace_root(None, workspace.path()).with_read_only_evidence_index(evidence);
    let input = json!({"file_path": path});

    let first = tool
        .prepare_execution(input.clone(), execution_context("one", "permission", "environment"))
        .unwrap()
        .execute_classified()
        .await;
    let second = tool
        .prepare_execution(input, execution_context("two", "permission", "environment"))
        .unwrap()
        .execute_classified()
        .await;

    assert_eq!(first.status, ToolResultStatus::Executed);
    assert_eq!(second.status, ToolResultStatus::Executed);
}

#[tokio::test]
async fn in_place_file_change_invalidates_read_evidence_without_mtime_reliance() {
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("mutable.txt");
    std::fs::write(&path, "before\n").unwrap();
    let evidence = Arc::new(ReadOnlyEvidenceIndex::default());
    let tool = ReadTool::new_with_workspace_root(None, workspace.path()).with_read_only_evidence_index(evidence);
    let input = json!({"file_path": path});
    let original_modified = std::fs::metadata(&path).unwrap().modified().unwrap();

    let first = tool
        .prepare_execution(input.clone(), execution_context("one", "permission", "environment"))
        .unwrap()
        .execute_classified()
        .await;
    let mut file = OpenOptions::new().write(true).truncate(true).open(&path).unwrap();
    file.write_all(b"after!\n").unwrap();
    file.sync_all().unwrap();
    file.set_times(FileTimes::new().set_modified(original_modified))
        .unwrap();
    drop(file);
    assert_eq!(std::fs::metadata(&path).unwrap().modified().unwrap(), original_modified);
    let changed = tool
        .prepare_execution(input, execution_context("two", "permission", "environment"))
        .unwrap()
        .execute_classified()
        .await;

    assert_eq!(first.status, ToolResultStatus::Executed);
    assert_eq!(changed.status, ToolResultStatus::Executed);
    assert!(changed.content.contains("after!"));
}

#[tokio::test]
async fn force_read_bypasses_shared_evidence() {
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("force.txt");
    std::fs::write(&path, "always read\n").unwrap();
    let body_reads = Arc::new(AtomicUsize::new(0));
    let tool = ReadTool::new_with_workspace_root(None, workspace.path())
        .with_read_only_evidence_index(Arc::new(ReadOnlyEvidenceIndex::default()))
        .with_body_read_counter(Arc::clone(&body_reads));

    let first = tool
        .prepare_execution(
            json!({"file_path": path}),
            execution_context("one", "permission", "environment"),
        )
        .unwrap()
        .execute_classified()
        .await;
    let forced = tool
        .prepare_execution(
            json!({"file_path": path, "force": true}),
            execution_context("two", "permission", "environment"),
        )
        .unwrap()
        .execute_classified()
        .await;

    assert_eq!(first.status, ToolResultStatus::Executed);
    assert_eq!(forced.status, ToolResultStatus::Executed);
    assert_eq!(body_reads.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn write_and_edit_invalidate_previously_shared_read_evidence() {
    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("tool-mutated.txt");
    std::fs::write(&path, "before\n").unwrap();
    let evidence = Arc::new(ReadOnlyEvidenceIndex::default());
    let read =
        ReadTool::new_with_workspace_root(None, workspace.path()).with_read_only_evidence_index(Arc::clone(&evidence));
    let write = WriteTool::new_with_workspace_root(None, workspace.path());
    let edit = EditTool::new_with_workspace_root(None, workspace.path());
    let input = json!({"file_path": path});

    let first = read
        .prepare_execution(
            input.clone(),
            execution_context("read-one", "permission", "environment"),
        )
        .unwrap()
        .execute_classified()
        .await;
    let written = write.execute(json!({"file_path": path, "content": "written\n"})).await;
    let after_write = read
        .prepare_execution(
            input.clone(),
            execution_context("read-two", "permission", "environment"),
        )
        .unwrap()
        .execute_classified()
        .await;
    let edited = edit
        .execute(json!({
            "file_path": path,
            "old_string": "written",
            "new_string": "edited"
        }))
        .await;
    let after_edit = read
        .prepare_execution(input, execution_context("read-three", "permission", "environment"))
        .unwrap()
        .execute_classified()
        .await;

    assert_eq!(first.status, ToolResultStatus::Executed);
    assert!(!written.is_error, "{}", written.content);
    assert_eq!(after_write.status, ToolResultStatus::Executed);
    assert!(after_write.content.contains("written"));
    assert!(!edited.is_error, "{}", edited.content);
    assert_eq!(after_edit.status, ToolResultStatus::Executed);
    assert!(after_edit.content.contains("edited"));
}

#[tokio::test]
async fn glob_detects_changes_and_grep_hit_skips_body_and_regex_scans() {
    let workspace = tempfile::tempdir().unwrap();
    let alpha = workspace.path().join("alpha.rs");
    std::fs::write(&alpha, "fn alpha() {}\n").unwrap();
    let alpha_modified = std::fs::metadata(&alpha).unwrap().modified().unwrap();
    let evidence = Arc::new(ReadOnlyEvidenceIndex::default());
    let root_glob = GlobTool::new(workspace.path().to_path_buf()).with_read_only_evidence_index(Arc::clone(&evidence));
    let child_glob = GlobTool::new(workspace.path().to_path_buf()).with_read_only_evidence_index(Arc::clone(&evidence));
    let body_scans = Arc::new(AtomicUsize::new(0));
    let regex_scans = Arc::new(AtomicUsize::new(0));
    let root_grep = GrepTool::new(workspace.path().to_path_buf())
        .with_read_only_evidence_index(Arc::clone(&evidence))
        .with_body_scan_counter(Arc::clone(&body_scans))
        .with_regex_scan_counter(Arc::clone(&regex_scans));
    let child_grep = GrepTool::new(workspace.path().to_path_buf())
        .with_read_only_evidence_index(Arc::clone(&evidence))
        .with_body_scan_counter(Arc::clone(&body_scans))
        .with_regex_scan_counter(Arc::clone(&regex_scans));
    let glob_input = json!({"pattern": "*.rs"});
    let equivalent_glob_input = json!({"pattern": "*.rs", "path": "."});
    let grep_input = json!({"pattern": "alpha", "glob": "*.rs"});
    let equivalent_grep_input = json!({"pattern": "alpha", "glob": "*.rs", "path": ".", "case_insensitive": false});

    let first_glob = root_glob
        .prepare_execution(
            equivalent_glob_input,
            execution_context("glob-one", "permission", "environment"),
        )
        .unwrap()
        .execute_classified()
        .await;
    let cached_glob = child_glob
        .prepare_execution(
            glob_input.clone(),
            execution_context("glob-two", "permission", "environment"),
        )
        .unwrap()
        .execute_classified()
        .await;
    let beta = workspace.path().join("beta.rs");
    std::fs::write(&beta, "fn beta() {}\n").unwrap();
    let changed_glob = child_glob
        .prepare_execution(
            glob_input.clone(),
            execution_context("glob-three", "permission", "environment"),
        )
        .unwrap()
        .execute_classified()
        .await;
    std::fs::remove_file(beta).unwrap();
    let deleted_glob = child_glob
        .prepare_execution(glob_input, execution_context("glob-four", "permission", "environment"))
        .unwrap()
        .execute_classified()
        .await;

    let first_grep = root_grep
        .prepare_execution(
            equivalent_grep_input,
            execution_context("grep-one", "permission", "environment"),
        )
        .unwrap()
        .execute_classified()
        .await;
    let scans_after_first_grep = body_scans.load(Ordering::SeqCst);
    let regex_scans_after_first_grep = regex_scans.load(Ordering::SeqCst);
    assert!(scans_after_first_grep > 0);
    assert!(regex_scans_after_first_grep > 0);
    let cached_grep = child_grep
        .prepare_execution(
            grep_input.clone(),
            execution_context("grep-two", "permission", "environment"),
        )
        .unwrap()
        .execute_classified()
        .await;
    assert_eq!(body_scans.load(Ordering::SeqCst), scans_after_first_grep);
    assert_eq!(regex_scans.load(Ordering::SeqCst), regex_scans_after_first_grep);
    let mut file = OpenOptions::new().write(true).truncate(true).open(&alpha).unwrap();
    file.write_all(b"fn omega() {}\n").unwrap();
    file.sync_all().unwrap();
    file.set_times(FileTimes::new().set_modified(alpha_modified)).unwrap();
    drop(file);
    assert_eq!(std::fs::metadata(&alpha).unwrap().modified().unwrap(), alpha_modified);
    let changed_grep = child_grep
        .prepare_execution(grep_input, execution_context("grep-three", "permission", "environment"))
        .unwrap()
        .execute_classified()
        .await;

    assert_eq!(first_glob.status, ToolResultStatus::Executed);
    assert_eq!(cached_glob.status, ToolResultStatus::CacheHit);
    assert_eq!(changed_glob.status, ToolResultStatus::Executed);
    assert!(changed_glob.content.contains("beta.rs"));
    assert_eq!(deleted_glob.status, ToolResultStatus::Executed);
    assert!(!deleted_glob.content.contains("beta.rs"));
    assert_eq!(first_grep.status, ToolResultStatus::Executed);
    assert_eq!(cached_grep.status, ToolResultStatus::CacheHit);
    assert!(body_scans.load(Ordering::SeqCst) > scans_after_first_grep);
    assert!(regex_scans.load(Ordering::SeqCst) > regex_scans_after_first_grep);
    assert_eq!(changed_grep.status, ToolResultStatus::Executed);
    assert!(!changed_grep.content.contains("alpha.rs:1:fn alpha"));
}
