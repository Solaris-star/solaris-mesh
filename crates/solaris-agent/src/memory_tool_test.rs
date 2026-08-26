use std::sync::Arc;

use serde_json::{Value, json};
use tempfile::tempdir;

use solaris_memory::service::MemoryService;
use solaris_tools::Tool;
use solaris_types::effect::EffectClass;

use super::MemoryTool;
use crate::memory_runtime::MemoryRuntime;

fn runtime(review_enabled: bool) -> (tempfile::TempDir, Arc<MemoryRuntime>) {
    let temp = tempdir().unwrap();
    let database = temp.path().join("memory.sqlite3");
    let service = Arc::new(MemoryService::open(&database).unwrap());
    let runtime = Arc::new(MemoryRuntime::from_service(
        service,
        temp.path().to_path_buf(),
        review_enabled,
    ));
    (temp, runtime)
}

#[tokio::test]
async fn root_writes_apply_but_the_session_snapshot_stays_frozen() {
    let (_temp, runtime) = runtime(false);
    let tool = MemoryTool::root(Arc::clone(&runtime));
    let result = tool
        .execute(json!({
            "operation": "create",
            "scope": "MEMORY",
            "type": "project",
            "name": "release",
            "description": "release note",
            "content": "ship after tests"
        }))
        .await;

    assert!(!result.is_error);
    assert_eq!(runtime.service().list().unwrap().len(), 1);
    let search = tool.execute(json!({ "operation": "search", "query": "ship" })).await;
    assert_eq!(
        serde_json::from_str::<Value>(&search.content).unwrap()["records"],
        json!([])
    );
}

#[tokio::test]
async fn child_writes_are_proposals_and_children_cannot_review() {
    let (_temp, runtime) = runtime(false);
    let tool = MemoryTool::child(Arc::clone(&runtime));
    let proposed = tool
        .execute(json!({
            "operation": "create",
            "scope": "USER",
            "type": "feedback",
            "name": "style",
            "content": "prefer concise output"
        }))
        .await;

    assert!(!proposed.is_error);
    assert!(runtime.service().list().unwrap().is_empty());
    let proposal_id = serde_json::from_str::<Value>(&proposed.content).unwrap()["proposal"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let review = tool
        .execute(json!({
            "operation": "review",
            "proposal_id": proposal_id,
            "decision": "approve"
        }))
        .await;
    assert!(review.is_error);
    assert_eq!(runtime.service().pending_proposals().unwrap().len(), 1);
}

#[tokio::test]
async fn review_mode_routes_root_writes_through_proposals() {
    let (_temp, runtime) = runtime(true);
    let tool = MemoryTool::root(Arc::clone(&runtime));
    let result = tool
        .execute(json!({
            "operation": "create",
            "scope": "MEMORY",
            "type": "reference",
            "name": "docs",
            "content": "read the manual"
        }))
        .await;

    assert!(!result.is_error);
    assert!(runtime.service().list().unwrap().is_empty());
    assert_eq!(runtime.service().pending_proposals().unwrap().len(), 1);
}

#[test]
fn memory_reads_and_writes_have_distinct_effect_classes() {
    let (_temp, runtime) = runtime(false);
    let tool = MemoryTool::root(runtime);

    assert_eq!(
        tool.describe_effect(&json!({ "operation": "search", "query": "release" }))
            .class,
        EffectClass::ReadOnly
    );
    assert_eq!(
        tool.describe_effect(&json!({
            "operation": "delete",
            "id": "01900000-0000-7000-8000-000000000000",
            "expected_version": 1
        }))
        .class,
        EffectClass::ExternalSideEffect
    );

    let child = MemoryTool::child(Arc::new(tool.runtime.as_ref().clone()));
    assert_eq!(
        child
            .describe_effect(&json!({
                "operation": "create",
                "scope": "MEMORY",
                "type": "project",
                "name": "proposal",
                "content": "review later"
            }))
            .class,
        EffectClass::MeshStateMutation
    );
}
