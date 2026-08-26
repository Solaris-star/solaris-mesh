use super::*;

#[test]
fn protected_runtime_paths_are_denied_in_plan_and_auto_but_allowed_in_bypass() {
    let directory = tempfile::tempdir().expect("tempdir");
    let workspace = directory.path().join("workspace");
    let runtime = workspace.join(".custom-state").join("runtime");
    std::fs::create_dir_all(&runtime).expect("runtime");
    let ledger = runtime.join("ledger.sqlite3");
    std::fs::write(&ledger, b"state").expect("ledger");
    let context = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    context.set_boundary(ExecutionBoundary::workspace(workspace.to_string_lossy()));
    context
        .register_protected_paths(runtime.clone(), vec![ledger.clone()])
        .unwrap();
    let path = ledger.to_string_lossy().into_owned();
    let requests = [
        (
            "Read",
            effect_request(
                "Read",
                EffectClass::ReadOnly,
                ResourceFootprint {
                    file_reads: vec![path.clone()],
                    ..Default::default()
                },
            ),
        ),
        (
            "Write",
            effect_request(
                "Write",
                EffectClass::WorkspaceMutation,
                ResourceFootprint {
                    file_writes: vec![path.clone()],
                    ..Default::default()
                },
            ),
        ),
        (
            "Edit",
            effect_request(
                "Edit",
                EffectClass::WorkspaceMutation,
                ResourceFootprint {
                    file_reads: vec![path.clone()],
                    file_writes: vec![path],
                    ..Default::default()
                },
            ),
        ),
    ];

    context.set_mode(PermissionMode::Auto);
    for (capability, request) in &requests {
        let evaluation = context.evaluate_effect(&RunId::new("run"), capability, request);
        assert_eq!(evaluation.decision, PermissionDecision::Deny, "{capability}");
        assert!(evaluation.reason.contains("protected runtime state"));
    }
    context.set_mode(PermissionMode::Plan);
    let plan_read = context.evaluate_effect(&RunId::new("run"), "Read", &requests[0].1);
    assert_eq!(plan_read.decision, PermissionDecision::Deny);
    assert!(plan_read.reason.contains("protected runtime state"));
    context.set_mode(PermissionMode::Bypass);
    for (capability, request) in &requests {
        assert_eq!(
            context
                .evaluate_effect(&RunId::new("run"), capability, request)
                .decision,
            PermissionDecision::Allow,
            "{capability}"
        );
    }
}

#[test]
fn protected_runtime_deny_cannot_be_overridden_by_rules_configured_effects_or_leases() {
    let directory = tempfile::tempdir().expect("tempdir");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir_all(&runtime).expect("runtime");
    let ledger = runtime.join("ledger.sqlite3");
    std::fs::write(&ledger, b"state").expect("ledger");
    let context = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    context.set_boundary(ExecutionBoundary::default());
    context.register_protected_paths(runtime, vec![ledger.clone()]).unwrap();
    context.add_rule(PermissionRule {
        capability: Some("Read".into()),
        action: None,
        effect_class: Some(EffectClass::ReadOnly),
        resource_prefixes: Vec::new(),
        decision: PermissionDecision::Allow,
    });
    let request = effect_request(
        "Read",
        EffectClass::ReadOnly,
        ResourceFootprint {
            file_reads: vec![ledger.to_string_lossy().into_owned()],
            ..Default::default()
        },
    );
    context.allow_configured_effect_for("test:read", "Read", &request.descriptor);
    context.issue_lease(CapabilityLease {
        lease_id: "protected-read".into(),
        capability: "Read".into(),
        action: Some("test".into()),
        scope: LeaseScope::Effect {
            effect_id: request.effect_id.clone(),
        },
        grants: AdditionalPermissions {
            unrestricted_file_reads: true,
            file_reads: vec![ledger.to_string_lossy().into_owned()],
            ..Default::default()
        },
        expires_at_unix_ms: None,
        max_uses: None,
    });

    let evaluation = context.evaluate_effect(&RunId::new("run"), "Read", &request);
    assert_eq!(evaluation.decision, PermissionDecision::Deny);
    assert!(!evaluation.matched_lease);
}

#[test]
fn child_and_replacement_contexts_retain_dynamic_protected_paths() {
    let directory = tempfile::tempdir().expect("tempdir");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir_all(&runtime).expect("runtime");
    let ledger = runtime.join("ledger.sqlite3");
    std::fs::write(&ledger, b"state").expect("ledger");
    let parent = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    parent.set_boundary(ExecutionBoundary::unrestricted());
    let child = parent.narrowed(PermissionCeiling::unrestricted());
    parent.register_protected_paths(runtime, vec![ledger.clone()]).unwrap();
    let request = effect_request(
        "Read",
        EffectClass::ReadOnly,
        ResourceFootprint {
            file_reads: vec![ledger.to_string_lossy().into_owned()],
            ..Default::default()
        },
    );

    assert_eq!(
        child.evaluate_effect(&RunId::new("child"), "Read", &request).decision,
        PermissionDecision::Deny
    );

    let replacement = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    replacement.set_boundary(ExecutionBoundary::unrestricted());
    let replacement_runtime = directory.path().join("replacement-runtime");
    std::fs::create_dir_all(&replacement_runtime).expect("replacement runtime");
    let replacement_ledger = replacement_runtime.join("ledger.sqlite3");
    std::fs::write(&replacement_ledger, b"replacement state").expect("replacement ledger");
    replacement
        .register_protected_paths(replacement_runtime, vec![replacement_ledger.clone()])
        .unwrap();
    parent.replace_with(&replacement);
    assert_eq!(
        parent.evaluate_effect(&RunId::new("run"), "Read", &request).decision,
        PermissionDecision::Deny
    );
    let replacement_request = effect_request(
        "Read",
        EffectClass::ReadOnly,
        ResourceFootprint {
            file_reads: vec![replacement_ledger.to_string_lossy().into_owned()],
            ..Default::default()
        },
    );
    assert_eq!(
        parent
            .evaluate_effect(&RunId::new("run"), "Read", &replacement_request)
            .decision,
        PermissionDecision::Deny
    );
}

#[test]
fn protected_paths_resolve_parent_aliases_and_missing_state_files() {
    let directory = tempfile::tempdir().expect("tempdir");
    let runtime = directory.path().join("runtime");
    let sidecars = directory.path().join("state-files");
    std::fs::create_dir_all(&runtime).expect("runtime");
    std::fs::create_dir_all(&sidecars).expect("sidecars");
    let ledger = runtime.join("ledger.sqlite3");
    std::fs::write(&ledger, b"state").expect("ledger");
    let missing_wal = sidecars.join("ledger.sqlite3-wal");
    let context = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    context.set_boundary(ExecutionBoundary::unrestricted());
    context
        .register_protected_paths(runtime.clone(), vec![missing_wal.clone()])
        .unwrap();

    for target in [runtime.join("child").join("..").join("ledger.sqlite3"), missing_wal] {
        let request = effect_request(
            "Read",
            EffectClass::ReadOnly,
            ResourceFootprint {
                file_reads: vec![target.to_string_lossy().into_owned()],
                ..Default::default()
            },
        );
        assert_eq!(
            context.evaluate_effect(&RunId::new("run"), "Read", &request).decision,
            PermissionDecision::Deny,
            "{}",
            target.display()
        );
    }
}

#[test]
fn protected_paths_resolve_relative_workspace_aliases() {
    let current = std::env::current_dir().expect("current dir");
    let directory = tempfile::tempdir_in(&current).expect("tempdir");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir_all(&runtime).expect("runtime");
    let ledger = runtime.join("ledger.sqlite3");
    std::fs::write(&ledger, b"state").expect("ledger");
    let relative = ledger.strip_prefix(&current).expect("relative ledger").to_path_buf();
    let context = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    context.set_boundary(ExecutionBoundary::unrestricted());
    context.register_protected_paths(runtime, vec![ledger]).unwrap();
    let request = effect_request(
        "Read",
        EffectClass::ReadOnly,
        ResourceFootprint {
            file_reads: vec![relative.to_string_lossy().into_owned()],
            ..Default::default()
        },
    );

    assert_eq!(
        context.evaluate_effect(&RunId::new("run"), "Read", &request).decision,
        PermissionDecision::Deny
    );
}

#[cfg(unix)]
#[test]
fn protected_runtime_paths_resolve_directory_symlink_aliases() {
    use std::os::unix::fs::symlink;

    let directory = tempfile::tempdir().expect("tempdir");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir_all(&runtime).expect("runtime");
    std::fs::write(runtime.join("ledger.sqlite3"), b"state").expect("ledger");
    let alias = directory.path().join("runtime-alias");
    symlink(&runtime, &alias).expect("symlink");
    let context = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    context.set_boundary(ExecutionBoundary::unrestricted());
    context.register_protected_paths(runtime, Vec::new()).unwrap();
    let request = effect_request(
        "Read",
        EffectClass::ReadOnly,
        ResourceFootprint {
            file_reads: vec![alias.join("ledger.sqlite3").to_string_lossy().into_owned()],
            ..Default::default()
        },
    );

    assert_eq!(
        context.evaluate_effect(&RunId::new("run"), "Read", &request).decision,
        PermissionDecision::Deny
    );
}

#[cfg(windows)]
#[test]
fn protected_runtime_paths_resolve_directory_junction_aliases() {
    let directory = tempfile::tempdir().expect("tempdir");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir_all(&runtime).expect("runtime");
    std::fs::write(runtime.join("ledger.sqlite3"), b"state").expect("ledger");
    let alias = directory.path().join("runtime-alias");
    create_windows_junction(&runtime, &alias);
    let context = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    context.set_boundary(ExecutionBoundary::unrestricted());
    context.register_protected_paths(runtime, Vec::new()).unwrap();
    let request = effect_request(
        "Read",
        EffectClass::ReadOnly,
        ResourceFootprint {
            file_reads: vec![alias.join("ledger.sqlite3").to_string_lossy().into_owned()],
            ..Default::default()
        },
    );

    assert_eq!(
        context.evaluate_effect(&RunId::new("run"), "Read", &request).decision,
        PermissionDecision::Deny
    );
}

#[tokio::test]
async fn opened_identity_blocks_runtime_hard_links_for_all_file_tools_and_bypass_skips_only_that_check() {
    use serde_json::json;
    use solaris_tools::Tool;
    use solaris_tools::edit::EditTool;
    use solaris_tools::glob::GlobTool;
    use solaris_tools::grep::GrepTool;
    use solaris_tools::read::ReadTool;
    use solaris_tools::write::WriteTool;

    let directory = tempfile::tempdir().expect("tempdir");
    let workspace = directory.path().join("workspace");
    let runtime = workspace.join(".solaris/runtime");
    std::fs::create_dir_all(&runtime).expect("runtime");
    let database = runtime.join("ledger.sqlite3");
    let wal = runtime.join("ledger.sqlite3-wal");
    let shm = runtime.join("ledger.sqlite3-shm");
    let protected_files = [&database, &wal, &shm];
    let aliases = [
        workspace.join("ledger-alias.sqlite3"),
        workspace.join("ledger-alias.sqlite3-wal"),
        workspace.join("ledger-alias.sqlite3-shm"),
    ];
    for (index, (protected, alias)) in protected_files.iter().zip(&aliases).enumerate() {
        std::fs::write(protected, format!("protected-marker-{index}")).expect("protected runtime state");
        std::fs::hard_link(protected, alias).expect("hard link");
    }
    let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    permissions.set_boundary(ExecutionBoundary::workspace(workspace.to_string_lossy()));
    permissions
        .register_protected_paths(&runtime, vec![database.clone(), wal.clone(), shm.clone()])
        .expect("protected paths");
    let policy = permissions.workspace_search_policy();

    let read = ReadTool::new_with_search_policy(None, &workspace, Arc::clone(&policy));
    let write = WriteTool::new_with_search_policy(None, &workspace, Arc::clone(&policy));
    let edit = EditTool::new_with_search_policy(None, &workspace, Arc::clone(&policy));
    let grep = GrepTool::new_with_search_policy(workspace.clone(), Arc::clone(&policy));
    let glob = GlobTool::new_with_search_policy(workspace.clone(), policy);

    for alias in &aliases {
        assert!(read.execute(json!({"file_path": alias})).await.is_error);
        assert!(
            write
                .execute(json!({"file_path": alias, "content": "corrupt"}))
                .await
                .is_error
        );
        assert!(
            edit.execute(json!({
                "file_path": alias,
                "old_string": "protected",
                "new_string": "corrupt"
            }))
            .await
            .is_error
        );
    }
    let grep_result = grep
        .execute(json!({"path": workspace, "pattern": "protected-marker"}))
        .await;
    assert!(!grep_result.content.contains("protected-marker"));
    let glob_result = glob
        .execute(json!({"path": workspace, "pattern": "ledger-alias*"}))
        .await;
    assert!(!glob_result.content.contains("ledger-alias"));
    for (index, protected) in protected_files.iter().enumerate() {
        assert_eq!(
            std::fs::read_to_string(protected).expect("protected state intact"),
            format!("protected-marker-{index}")
        );
    }

    permissions.set_mode(PermissionMode::Bypass);
    let bypass = ReadTool::new_with_search_policy(None, &workspace, permissions.workspace_search_policy());
    let result = bypass.execute(json!({"file_path": &aliases[0]})).await;
    assert!(!result.is_error, "{}", result.content);
}

#[tokio::test]
async fn newly_created_sidecar_identity_is_refreshed_once_before_the_next_tool_operation() {
    use serde_json::json;
    use solaris_tools::Tool;
    use solaris_tools::read::ReadTool;

    let directory = tempfile::tempdir().expect("tempdir");
    let workspace = directory.path().join("workspace");
    let runtime = workspace.join(".solaris/runtime");
    std::fs::create_dir_all(&runtime).expect("runtime");
    let database = runtime.join("ledger.sqlite3");
    let wal = runtime.join("ledger.sqlite3-wal");
    let alias = workspace.join("wal-alias");
    std::fs::write(&database, b"database").expect("database");
    let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    permissions
        .register_protected_paths(&runtime, vec![database, wal.clone()])
        .expect("protected paths");
    std::fs::write(&wal, b"new-sidecar").expect("sidecar");
    std::fs::hard_link(&wal, &alias).expect("sidecar hard link");
    let read = ReadTool::new_with_search_policy(None, &workspace, permissions.workspace_search_policy());

    let result = read.execute(json!({"file_path": alias})).await;

    assert!(result.is_error);
    assert!(!result.content.contains("new-sidecar"));
}

#[tokio::test]
async fn identity_retention_exhaustion_fails_closed_for_auto_but_not_bypass() {
    use serde_json::json;
    use solaris_tools::Tool;
    use solaris_tools::read::ReadTool;

    let directory = tempfile::tempdir().expect("tempdir");
    let workspace = directory.path().join("workspace");
    let runtime = workspace.join(".solaris/runtime");
    let ordinary = workspace.join("ordinary.txt");
    std::fs::create_dir_all(&runtime).expect("runtime");
    std::fs::write(&ordinary, b"ordinary-content").expect("ordinary file");
    let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    permissions.set_boundary(ExecutionBoundary::workspace(workspace.to_string_lossy()));
    let mut capacity_error = None;
    for index in 0..=64 {
        let state = runtime.join(format!("state-{index}"));
        std::fs::write(&state, b"state").expect("state file");
        if let Err(error) = permissions.register_protected_paths(&runtime, vec![state]) {
            capacity_error = Some(error);
            break;
        }
    }
    assert!(
        capacity_error
            .as_deref()
            .is_some_and(|error| error.contains("retained identity capacity"))
    );
    let auto = ReadTool::new_with_search_policy(None, &workspace, permissions.workspace_search_policy());

    let auto_result = auto.execute(json!({"file_path": &ordinary})).await;

    assert!(auto_result.is_error);
    assert!(!auto_result.content.contains("ordinary-content"));
    permissions.set_mode(PermissionMode::Bypass);
    let bypass = ReadTool::new_with_search_policy(None, &workspace, permissions.workspace_search_policy());
    let bypass_result = bypass.execute(json!({"file_path": ordinary})).await;
    assert!(!bypass_result.is_error, "{}", bypass_result.content);
    assert!(bypass_result.content.contains("ordinary-content"));
}

#[cfg(not(windows))]
#[tokio::test]
async fn opened_directory_identity_blocks_runtime_root_after_it_is_moved() {
    use serde_json::json;
    use solaris_tools::Tool;
    use solaris_tools::glob::GlobTool;
    use solaris_tools::write::WriteTool;

    let directory = tempfile::tempdir().expect("tempdir");
    let workspace = directory.path().join("workspace");
    let runtime = workspace.join(".solaris/runtime");
    let moved = workspace.join("moved-runtime");
    std::fs::create_dir_all(&runtime).expect("runtime");
    std::fs::write(runtime.join("ledger.sqlite3"), b"state").expect("database");
    let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    permissions.set_boundary(ExecutionBoundary::workspace(workspace.to_string_lossy()));
    permissions
        .register_protected_paths(&runtime, vec![runtime.join("ledger.sqlite3")])
        .expect("protected paths");
    std::fs::rename(&runtime, &moved).expect("move runtime root");
    let policy = permissions.workspace_search_policy();
    let write = WriteTool::new_with_search_policy(None, &workspace, Arc::clone(&policy));
    let glob = GlobTool::new_with_search_policy(workspace.clone(), policy);

    let write_result = write
        .execute(json!({"file_path": moved.join("new-state"), "content": "state"}))
        .await;
    let glob_result = glob.execute(json!({"path": &moved, "pattern": "**/*"})).await;

    assert!(write_result.is_error);
    assert!(glob_result.is_error || !glob_result.content.contains("ledger.sqlite3"));
    assert!(!moved.join("new-state").exists());
}

#[cfg(not(windows))]
#[tokio::test]
async fn reregistered_runtime_path_protects_the_recreated_state_file_identity() {
    use serde_json::json;
    use solaris_tools::Tool;
    use solaris_tools::read::ReadTool;

    let directory = tempfile::tempdir().expect("tempdir");
    let workspace = directory.path().join("workspace");
    let runtime = workspace.join(".solaris/runtime");
    let moved = workspace.join("moved-runtime");
    let ledger = runtime.join("ledger.sqlite3");
    let alias = workspace.join("recreated-ledger-alias");
    std::fs::create_dir_all(&runtime).expect("runtime");
    std::fs::write(&ledger, b"old-state").expect("old database");
    let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    permissions.set_boundary(ExecutionBoundary::workspace(workspace.to_string_lossy()));
    permissions
        .register_protected_paths(&runtime, vec![ledger.clone()])
        .expect("initial protected paths");

    std::fs::rename(&runtime, &moved).expect("move runtime root");
    std::fs::create_dir_all(&runtime).expect("recreate runtime");
    std::fs::write(&ledger, b"new-state").expect("new database");
    std::fs::hard_link(&ledger, &alias).expect("new database hard link");
    permissions
        .register_protected_paths(&runtime, vec![ledger])
        .expect("refresh protected paths at the same lexical path");
    let read = ReadTool::new_with_search_policy(None, &workspace, permissions.workspace_search_policy());

    let new_result = read.execute(json!({"file_path": alias})).await;
    let old_result = read.execute(json!({"file_path": moved.join("ledger.sqlite3")})).await;

    assert!(new_result.is_error);
    assert!(!new_result.content.contains("new-state"));
    assert!(old_result.is_error);
    assert!(!old_result.content.contains("old-state"));
}

#[cfg(windows)]
#[test]
fn retained_directory_identity_prevents_runtime_root_move_on_windows() {
    let directory = tempfile::tempdir().expect("tempdir");
    let workspace = directory.path().join("workspace");
    let runtime = workspace.join(".solaris/runtime");
    let moved = workspace.join("moved-runtime");
    std::fs::create_dir_all(&runtime).expect("runtime");
    std::fs::write(runtime.join("ledger.sqlite3"), b"state").expect("database");
    let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    permissions
        .register_protected_paths(&runtime, vec![runtime.join("ledger.sqlite3")])
        .expect("protected paths");

    let error = std::fs::rename(&runtime, &moved).expect_err("retained handle must prevent rename");

    assert_ne!(error.kind(), std::io::ErrorKind::NotFound);
    assert!(runtime.is_dir());
}
