use super::*;
use solaris_types::effect::{EffectClass, EffectDescriptor, EffectReplayPolicy, EffectRequest, ResourceFootprint};
use solaris_types::identity::EffectId;
use solaris_types::permission::PermissionMode;

use crate::permission_engine::PermissionContext;

#[cfg(windows)]
fn create_directory_link(target: &Path, link: &Path) {
    fn literal(path: &Path) -> String {
        format!("'{}'", path.to_string_lossy().replace('\'', "''"))
    }
    let shell = solaris_config::shell::resolve_shell(Some("powershell")).unwrap();
    let script = format!(
        "$ErrorActionPreference='Stop'; New-Item -ItemType Junction -Path {} -Target {} | Out-Null",
        literal(link),
        literal(target)
    );
    let mut command = solaris_config::shell::shell_command_builder(&shell, &script, false);
    let status = tokio_test::block_on(command.status()).unwrap();
    assert!(status.success());
}

#[cfg(unix)]
fn create_directory_link(target: &Path, link: &Path) {
    std::os::unix::fs::symlink(target, link).unwrap();
}

#[test]
fn role_budget_never_increases_the_provider_request_token_limit() {
    assert_eq!(sub_agent_request_max_tokens(Some(4_096), 128_000), 4_096);
    assert_eq!(sub_agent_request_max_tokens(Some(256_000), 128_000), 128_000);
    assert_eq!(sub_agent_request_max_tokens(None, 128_000), 128_000);
}

#[test]
fn child_boundary_intersects_workspace_roots_without_mutating_parent() {
    let directory = tempfile::tempdir().unwrap();
    let workspace = directory.path().join("workspace");
    std::fs::create_dir_all(workspace.join("src")).unwrap();
    let parent = ExecutionBoundary::workspace(workspace.to_string_lossy());
    let requested = ExecutionBoundary {
        unrestricted_file_reads: true,
        writable_roots: vec!["src".into()],
        unrestricted_process: true,
        unrestricted_external_side_effects: true,
        unrestricted_network: true,
        ..ExecutionBoundary::default()
    };

    let child = intersect_execution_boundary(&parent, &requested, &workspace).unwrap();

    assert_eq!(parent, ExecutionBoundary::workspace(workspace.to_string_lossy()));
    assert_eq!(child.readable_roots, parent.readable_roots);
    assert_eq!(child.writable_roots, [workspace.join("src").to_string_lossy()]);
    assert!(!child.unrestricted_file_writes);
    assert!(!child.unrestricted_process);
}

#[test]
fn child_boundary_rejects_parent_components_globs_and_outside_absolute_roots() {
    let directory = tempfile::tempdir().unwrap();
    let workspace = directory.path().join("workspace");
    let outside = directory.path().join("outside");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    let parent = ExecutionBoundary::workspace(workspace.to_string_lossy());

    for root in [
        "../outside".to_owned(),
        "src/**/*.rs".to_owned(),
        outside.to_string_lossy().into_owned(),
    ] {
        let requested = ExecutionBoundary {
            writable_roots: vec![root.clone()],
            ..ExecutionBoundary::default()
        };
        assert!(
            intersect_execution_boundary(&parent, &requested, &workspace).is_err(),
            "scope {root} must fail closed"
        );
    }
}

#[test]
fn empty_child_write_scope_denies_writes_even_under_an_unrestricted_parent() {
    let directory = tempfile::tempdir().unwrap();
    let requested = ExecutionBoundary {
        unrestricted_file_reads: true,
        unrestricted_process: true,
        unrestricted_external_side_effects: true,
        unrestricted_network: true,
        ..ExecutionBoundary::default()
    };

    let child = intersect_execution_boundary(&ExecutionBoundary::unrestricted(), &requested, directory.path()).unwrap();

    assert!(child.writable_roots.is_empty());
    assert!(!child.unrestricted_file_writes);
}

#[test]
fn child_boundary_rejects_a_link_that_escapes_the_parent_workspace() {
    let directory = tempfile::tempdir().unwrap();
    let workspace = directory.path().join("workspace");
    let outside = directory.path().join("outside");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    create_directory_link(&outside, &workspace.join("link"));
    let requested = ExecutionBoundary {
        writable_roots: vec!["link/child".into()],
        ..ExecutionBoundary::default()
    };

    assert!(
        intersect_execution_boundary(
            &ExecutionBoundary::workspace(workspace.to_string_lossy()),
            &requested,
            &workspace,
        )
        .is_err()
    );
}

#[test]
fn intersected_child_boundary_allows_only_the_supervisor_write_scope() {
    let directory = tempfile::tempdir().unwrap();
    let workspace = directory.path().join("workspace");
    std::fs::create_dir_all(workspace.join("allowed")).unwrap();
    std::fs::create_dir_all(workspace.join("denied")).unwrap();
    let requested = ExecutionBoundary {
        unrestricted_file_reads: true,
        writable_roots: vec!["allowed".into()],
        unrestricted_process: true,
        unrestricted_external_side_effects: true,
        unrestricted_network: true,
        ..ExecutionBoundary::default()
    };
    let boundary = intersect_execution_boundary(
        &ExecutionBoundary::workspace(workspace.to_string_lossy()),
        &requested,
        &workspace,
    )
    .unwrap();
    let context = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    context.set_boundary(boundary);
    let request = |path: PathBuf| EffectRequest {
        effect_id: EffectId::from("scope-test"),
        operation_id: OperationId::from("scope-test"),
        capability: "Write".into(),
        descriptor: EffectDescriptor {
            class: EffectClass::WorkspaceMutation,
            action: "write file".into(),
            resources: ResourceFootprint {
                file_writes: vec![path.to_string_lossy().into_owned()],
                ..ResourceFootprint::default()
            },
            replay_policy: EffectReplayPolicy::ReplaySafe,
        },
        effective_input: json!({}),
        input_digest: None,
    };

    assert!(context.current_boundary_allows_request(&request(workspace.join("allowed/file.txt"))));
    assert!(!context.current_boundary_allows_request(&request(workspace.join("denied/file.txt"))));
}

#[test]
fn narrowed_boundary_is_independent_from_its_parent() {
    let directory = tempfile::tempdir().unwrap();
    let parent_root = directory.path().join("parent");
    let child_root = parent_root.join("child");
    std::fs::create_dir_all(&child_root).unwrap();
    let parent_boundary = ExecutionBoundary::workspace(parent_root.to_string_lossy());
    let child_boundary = ExecutionBoundary::workspace(child_root.to_string_lossy());
    let parent = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    parent.set_boundary(parent_boundary.clone());
    let child = parent.narrowed_with_boundary(PermissionCeiling::plan(), child_boundary.clone());

    assert_eq!(parent.boundary(), parent_boundary.clone());
    assert_eq!(child.boundary(), child_boundary);
    assert_eq!(child.ceiling(), PermissionCeiling::plan());

    child.set_boundary(ExecutionBoundary::default());
    assert_eq!(parent.boundary(), parent_boundary);
}

#[test]
fn replacing_a_parent_workspace_link_invalidates_the_child_boundary() {
    let directory = tempfile::tempdir().unwrap();
    let first_workspace = directory.path().join("workspace-a");
    let replacement_workspace = directory.path().join("workspace-b");
    let workspace_link = directory.path().join("workspace-link");
    for workspace in [&first_workspace, &replacement_workspace] {
        std::fs::create_dir_all(workspace.join("allowed")).unwrap();
    }
    create_directory_link(&first_workspace, &workspace_link);
    let parent = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    parent.set_boundary(ExecutionBoundary::workspace(workspace_link.to_string_lossy()));
    let child = parent.narrowed_with_boundary(
        PermissionCeiling::unrestricted(),
        ExecutionBoundary::workspace(workspace_link.join("allowed").to_string_lossy()),
    );
    let request = EffectRequest {
        effect_id: EffectId::from("link-swap"),
        operation_id: OperationId::from("link-swap"),
        capability: "Write".into(),
        descriptor: EffectDescriptor {
            class: EffectClass::WorkspaceMutation,
            action: "write file".into(),
            resources: ResourceFootprint {
                file_writes: vec![workspace_link.join("allowed/file.txt").to_string_lossy().into_owned()],
                ..ResourceFootprint::default()
            },
            replay_policy: EffectReplayPolicy::ReplaySafe,
        },
        effective_input: json!({}),
        input_digest: None,
    };
    assert!(child.current_boundary_allows_request(&request));

    #[cfg(windows)]
    std::fs::remove_dir(&workspace_link).unwrap();
    #[cfg(unix)]
    std::fs::remove_file(&workspace_link).unwrap();
    create_directory_link(&replacement_workspace, &workspace_link);

    assert!(!child.current_boundary_allows_request(&request));
    let process = EffectRequest {
        effect_id: EffectId::from("link-swap-process"),
        operation_id: OperationId::from("link-swap-process"),
        capability: "ExecCommand".into(),
        descriptor: EffectDescriptor {
            class: EffectClass::Process,
            action: "spawn process".into(),
            resources: ResourceFootprint {
                process_commands: vec!["test-command".into()],
                ..ResourceFootprint::default()
            },
            replay_policy: EffectReplayPolicy::ReconcileRequired,
        },
        effective_input: json!({}),
        input_digest: None,
    };
    let final_evaluation = child.evaluate_effect_for_final_process_spawn(
        &RunId::from("link-swap-run"),
        "ExecCommand",
        &process,
        PermissionMode::Auto,
        PermissionCeiling::unrestricted(),
    );
    assert_eq!(final_evaluation.decision, PermissionDecision::Deny);
    assert_eq!(final_evaluation.reason, "execution boundary root identity changed");
    assert_eq!(
        parent.boundary(),
        ExecutionBoundary::workspace(workspace_link.to_string_lossy())
    );
}
