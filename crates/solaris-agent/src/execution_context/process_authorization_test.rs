use super::*;

use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

#[cfg(any(target_os = "linux", target_os = "macos"))]
use solaris_process::SandboxEnforcement;
use solaris_process::{
    CommandRunner, ProcessRecoveryKind, ProcessRecoveryState, SandboxError, platform_sandbox_report,
    process_outcome_unknown, process_outcome_unknown_error_for_test, process_recovery_required,
    process_recovery_required_error_for_test, retry_process_recovery,
};
use solaris_types::effect::{EffectClass, EffectDescriptor, EffectReplayPolicy, ProcessInvocation, ResourceFootprint};
use solaris_types::identity::{AgentId, EffectId, OperationId, RunId};
use solaris_types::permission::{ExecutionBoundary, PermissionDecision, PermissionRule};

use crate::execution_context::{EffectOutcomeGuard, EffectRecoveryDecision};
use crate::permission_engine::{PermissionContext, PermissionReviewer};
use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};

fn marker_command(marker: &Path) -> String {
    #[cfg(windows)]
    {
        format!(
            "Set-Content -LiteralPath '{}' -Value launched",
            marker.to_string_lossy().replace('\'', "''")
        )
    }
    #[cfg(not(windows))]
    {
        format!(
            "printf launched > '{}'",
            marker.to_string_lossy().replace('\'', "'\\''")
        )
    }
}

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
    assert!(tokio_test::block_on(command.status()).unwrap().success());
}

#[cfg(unix)]
fn create_directory_link(target: &Path, link: &Path) {
    std::os::unix::fs::symlink(target, link).unwrap();
}

fn process_request() -> EffectRequest {
    EffectRequest {
        effect_id: EffectId::new("process-spawn-authorization-effect"),
        operation_id: OperationId::new("process-spawn-authorization-operation"),
        capability: "ProcessSpawnAuthorizationTest".to_owned(),
        descriptor: EffectDescriptor {
            class: EffectClass::Process,
            action: "spawn marker command".to_owned(),
            resources: ResourceFootprint {
                process_commands: vec!["marker-command".to_owned()],
                process_invocations: vec![ProcessInvocation {
                    executable: "test-shell".to_owned(),
                    argv: vec!["marker-command".to_owned()],
                }],
                ..Default::default()
            },
            replay_policy: EffectReplayPolicy::ReconcileRequired,
        },
        effective_input: serde_json::Value::Null,
        input_digest: None,
    }
}

fn allow_process_rule() -> PermissionRule {
    PermissionRule {
        capability: Some("ProcessSpawnAuthorizationTest".to_owned()),
        action: None,
        effect_class: Some(EffectClass::Process),
        resource_prefixes: Vec::new(),
        decision: PermissionDecision::Allow,
    }
}

fn deny_process_rule() -> PermissionRule {
    PermissionRule {
        decision: PermissionDecision::Deny,
        ..allow_process_rule()
    }
}

fn workspace_process_boundary(workspace: &Path, request: &EffectRequest) -> ExecutionBoundary {
    let mut boundary = ExecutionBoundary::workspace(workspace.to_string_lossy());
    boundary.process_command_prefixes = request.descriptor.resources.process_commands.clone();
    boundary.process_invocations = request.descriptor.resources.process_invocations.clone();
    boundary.network_domains = request.descriptor.resources.network_domains.clone();
    boundary
}

struct AllowReviewer {
    calls: Arc<AtomicUsize>,
}

impl PermissionReviewer for AllowReviewer {
    fn review(&self, _request: &EffectRequest) -> PermissionDecision {
        self.calls.fetch_add(1, Ordering::SeqCst);
        PermissionDecision::Allow
    }
}

struct MutatingReviewer {
    permissions: PermissionContext,
    calls: Arc<AtomicUsize>,
}

impl PermissionReviewer for MutatingReviewer {
    fn review(&self, _request: &EffectRequest) -> PermissionDecision {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.permissions.set_mode(PermissionMode::Plan);
        PermissionDecision::Deny
    }
}

fn process_context(mode: PermissionMode) -> (EffectExecutionContext, PermissionContext, EffectRequest) {
    let (context, permissions, request, _) = process_context_with_ledger(mode);
    (context, permissions, request)
}

fn process_context_with_ledger(
    mode: PermissionMode,
) -> (
    EffectExecutionContext,
    PermissionContext,
    EffectRequest,
    Arc<InMemoryRuntimeLedger>,
) {
    let permissions = PermissionContext::new(mode, PermissionCeiling::unrestricted());
    permissions.set_boundary(ExecutionBoundary::unrestricted());
    permissions.add_rule(allow_process_rule());
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let context = EffectExecutionContext::new(
        RunId::from("process-spawn-authorization-run"),
        AgentId::from("process-spawn-authorization-agent"),
        ledger.clone(),
        permissions.clone(),
        OperationEnvironmentSnapshot::default(),
    );
    (context, permissions, process_request(), ledger)
}

fn spawn_authorization(
    context: &EffectExecutionContext,
    request: EffectRequest,
    approved_mode: PermissionMode,
    workspace: &Path,
) -> ProcessSpawnAuthorization {
    if approved_mode == PermissionMode::Auto {
        context
            .permissions()
            .set_boundary(workspace_process_boundary(workspace, &request));
    }
    context
        .process_spawn_authorization(
            request,
            context.environment(),
            approved_mode,
            PermissionCeiling::unrestricted(),
            Some(workspace.to_path_buf()),
            false,
            None,
        )
        .unwrap()
}

async fn run_marker(marker: &Path, authorization: ProcessSpawnAuthorization) -> std::io::Result<()> {
    let shell = solaris_config::shell::default_shell();
    let command = solaris_config::shell::shell_command_builder(&shell, &marker_command(marker), false);
    CommandRunner::new(command)
        .spawn_authorizer(authorization)
        .run()
        .await
        .map(|_| ())
}

fn spawn_marker_child(marker: PathBuf) -> io::Result<ManagedChild> {
    let shell = solaris_config::shell::default_shell();
    let executable_error = |error: solaris_process::ExecutableError| io::Error::other(error.to_string());
    let identity = solaris_process::inspect_executable(&shell.path).map_err(executable_error)?;
    let pinned = solaris_process::pin_executable(&shell.path, &identity).map_err(executable_error)?;
    let mut command = pinned.command().map_err(executable_error)?;
    command.args(shell.derive_exec_args(&marker_command(&marker), false));
    command.spawn()
}

#[tokio::test]
async fn explicit_deny_added_after_approval_rejects_before_process_spawn() {
    let workspace = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let marker = outside.path().join("explicit-deny-marker.txt");
    let (context, permissions, request) = process_context(PermissionMode::Bypass);
    let authorization = spawn_authorization(&context, request, PermissionMode::Bypass, workspace.path());

    permissions.add_rule(deny_process_rule());
    let error = run_marker(&marker, authorization).await.unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert!(!marker.exists());
}

#[tokio::test]
async fn plan_mode_set_after_approval_rejects_before_process_spawn() {
    let workspace = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let marker = outside.path().join("plan-mode-marker.txt");
    let (context, permissions, request) = process_context(PermissionMode::Bypass);
    let authorization = spawn_authorization(&context, request, PermissionMode::Bypass, workspace.path());

    permissions.set_mode(PermissionMode::Plan);
    let error = run_marker(&marker, authorization).await.unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert!(!marker.exists());
}

#[test]
fn retained_process_cleanup_is_recorded_as_outcome_unknown_and_not_retryable_failure() {
    let _recovery_test_guard = solaris_process::isolate_process_recoveries_for_test();
    let workspace = tempfile::tempdir().unwrap();
    let (context, _permissions, request) = process_context(PermissionMode::Bypass);
    context.record_effect_intent(&request).unwrap();
    let authorization = EffectProcessSpawnAuthorizer {
        context: context.clone(),
        request: request.clone(),
        approved_environment: context.environment(),
        approved_mode: PermissionMode::Bypass,
        approved_ceiling: PermissionCeiling::unrestricted(),
        workspace_root: Some(workspace.path().to_path_buf()),
        approved_by_lease: false,
        required_plugin_implementation: None,
        spawn_consumed: Mutex::new(false),
    };
    let process_error = process_recovery_required_error_for_test(ProcessRecoveryKind::StartedChild);
    let expected_recovery = process_recovery_required(&process_error).unwrap();

    let error = match authorization.authorize_and_spawn(Box::new(move |_| Err(process_error))) {
        Ok(_) => panic!("a retained process cleanup must fail the spawn"),
        Err(error) => error,
    };

    assert_eq!(process_recovery_required(&error), Some(expected_recovery));
    assert!(!context.has_unresolved_effect_intent().unwrap());
    assert_eq!(
        context
            .unresolved_other_effect_for_capability(&request.capability, "different-effect")
            .unwrap(),
        Some(request.effect_id.to_string())
    );
    assert_eq!(
        retry_process_recovery(expected_recovery.id()).unwrap(),
        ProcessRecoveryState::Complete
    );
}

#[test]
fn released_process_launch_error_is_recorded_as_outcome_unknown_without_cleanup_recovery() {
    let workspace = tempfile::tempdir().unwrap();
    let (context, _permissions, request) = process_context(PermissionMode::Bypass);
    context.record_effect_intent(&request).unwrap();
    let authorization = EffectProcessSpawnAuthorizer {
        context: context.clone(),
        request: request.clone(),
        approved_environment: context.environment(),
        approved_mode: PermissionMode::Bypass,
        approved_ceiling: PermissionCeiling::unrestricted(),
        workspace_root: Some(workspace.path().to_path_buf()),
        approved_by_lease: false,
        required_plugin_implementation: None,
        spawn_consumed: Mutex::new(false),
    };
    let process_error = process_outcome_unknown_error_for_test();

    let error = match authorization.authorize_and_spawn(Box::new(move |_| Err(process_error))) {
        Ok(_) => panic!("an unknown released process launch must fail the spawn"),
        Err(error) => error,
    };

    assert!(process_outcome_unknown(&error));
    assert!(process_recovery_required(&error).is_none());
    assert!(!context.has_unresolved_effect_intent().unwrap());
    assert!(matches!(
        context.recover_effect(&request).unwrap(),
        EffectRecoveryDecision::Reconcile { reason } if reason.contains("outcome is unknown")
    ));
}

#[test]
fn insufficient_sandbox_enforcement_remains_typed_at_the_agent_boundary() {
    let workspace = tempfile::tempdir().unwrap();
    let (context, _permissions, request) = process_context(PermissionMode::Bypass);
    let authorization = EffectProcessSpawnAuthorizer {
        context: context.clone(),
        request,
        approved_environment: context.environment(),
        approved_mode: PermissionMode::Bypass,
        approved_ceiling: PermissionCeiling::unrestricted(),
        workspace_root: Some(workspace.path().to_path_buf()),
        approved_by_lease: false,
        required_plugin_implementation: None,
        spawn_consumed: Mutex::new(false),
    };
    let report = platform_sandbox_report();

    let error = match authorization.authorize_and_spawn(Box::new(move |_| {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            SandboxError::InsufficientEnforcement { report },
        ))
    })) {
        Ok(_) => panic!("insufficient sandbox enforcement must reject process spawn"),
        Err(error) => error,
    };

    assert_eq!(error.kind(), io::ErrorKind::Unsupported);
    assert_eq!(
        error.get_ref().and_then(|source| source.downcast_ref::<SandboxError>()),
        Some(&SandboxError::InsufficientEnforcement { report })
    );
}

#[test]
fn plugin_skill_hook_and_stdio_mcp_guards_cannot_overwrite_process_outcome_unknown() {
    let _recovery_test_guard = solaris_process::isolate_process_recoveries_for_test();
    for adapter in ["Plugin", "Skill", "Hook", "stdio MCP"] {
        let workspace = tempfile::tempdir().unwrap();
        let (context, _permissions, request, ledger) = process_context_with_ledger(PermissionMode::Bypass);
        context.record_effect_intent(&request).unwrap();
        let authorization = EffectProcessSpawnAuthorizer {
            context: context.clone(),
            request: request.clone(),
            approved_environment: context.environment(),
            approved_mode: PermissionMode::Bypass,
            approved_ceiling: PermissionCeiling::unrestricted(),
            workspace_root: Some(workspace.path().to_path_buf()),
            approved_by_lease: false,
            required_plugin_implementation: None,
            spawn_consumed: Mutex::new(false),
        };
        let process_error = process_recovery_required_error_for_test(ProcessRecoveryKind::StartedChild);
        let expected_recovery = process_recovery_required(&process_error).unwrap();
        let error = match authorization.authorize_and_spawn(Box::new(move |_| Err(process_error))) {
            Ok(_) => panic!("{adapter} recovery fixture must fail process spawn"),
            Err(error) => error,
        };
        assert_eq!(process_recovery_required(&error), Some(expected_recovery));

        let mut guard = EffectOutcomeGuard::new(
            context.clone(),
            request.clone(),
            format!("{adapter} execution was cancelled"),
        );
        assert!(
            guard
                .complete(true, &format!("{adapter} reported a normal failure"))
                .is_err(),
            "{adapter} must surface reconciliation instead of replacing outcome_unknown"
        );
        let outcomes = ledger
            .records_for_run(&RunId::from("process-spawn-authorization-run"))
            .unwrap()
            .into_iter()
            .filter(|record| record.record_type == "effect_outcome")
            .collect::<Vec<_>>();
        assert_eq!(outcomes.len(), 1, "{adapter} must keep one canonical terminal outcome");
        assert_eq!(outcomes[0].payload["status"], "outcome_unknown");
        assert_eq!(
            outcomes[0].payload["process_recovery"]["id"],
            expected_recovery.id().get()
        );
        assert!(matches!(
            context.recover_effect(&request).unwrap(),
            EffectRecoveryDecision::Reconcile { reason } if reason.contains("outcome is unknown")
        ));
        assert_eq!(
            retry_process_recovery(expected_recovery.id()).unwrap(),
            ProcessRecoveryState::Complete
        );
    }
}

#[test]
fn workspace_link_replacement_rejects_before_process_spawn() {
    let directory = tempfile::tempdir().unwrap();
    let first_workspace = directory.path().join("workspace-a");
    let replacement_workspace = directory.path().join("workspace-b");
    let workspace_link = directory.path().join("workspace-link");
    std::fs::create_dir_all(&first_workspace).unwrap();
    std::fs::create_dir_all(&replacement_workspace).unwrap();
    create_directory_link(&first_workspace, &workspace_link);
    let (context, permissions, request) = process_context(PermissionMode::Auto);
    permissions.set_boundary(ExecutionBoundary::workspace(workspace_link.to_string_lossy()));
    let authorization = EffectProcessSpawnAuthorizer {
        context: context.clone(),
        request,
        approved_environment: context.environment(),
        approved_mode: PermissionMode::Auto,
        approved_ceiling: PermissionCeiling::unrestricted(),
        workspace_root: Some(workspace_link.clone()),
        approved_by_lease: false,
        required_plugin_implementation: None,
        spawn_consumed: Mutex::new(false),
    };

    #[cfg(windows)]
    std::fs::remove_dir(&workspace_link).unwrap();
    #[cfg(unix)]
    std::fs::remove_file(&workspace_link).unwrap();
    create_directory_link(&replacement_workspace, &workspace_link);
    let spawn_calls = Arc::new(AtomicUsize::new(0));
    let counted_spawn_calls = Arc::clone(&spawn_calls);

    let error = match authorization.authorize_and_spawn(Box::new(move |_| {
        counted_spawn_calls.fetch_add(1, Ordering::SeqCst);
        Err(io::Error::other("invalid boundary spawn closure must not run"))
    })) {
        Ok(_) => panic!("replaced workspace identity must reject process spawn"),
        Err(error) => error,
    };

    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(spawn_calls.load(Ordering::SeqCst), 0);
}

#[cfg(windows)]
#[test]
fn workspace_cannot_be_renamed_after_final_authorization_until_the_capability_is_released() {
    let parent = tempfile::tempdir().unwrap();
    let workspace = parent.path().join("workspace");
    let renamed = parent.path().join("renamed-workspace");
    std::fs::create_dir(&workspace).unwrap();
    let (context, permissions, request) = process_context(PermissionMode::Auto);
    permissions.set_boundary(workspace_process_boundary(&workspace, &request));
    let authorizer = EffectProcessSpawnAuthorizer {
        context: context.clone(),
        request,
        approved_environment: context.environment(),
        approved_mode: PermissionMode::Auto,
        approved_ceiling: PermissionCeiling::unrestricted(),
        workspace_root: Some(workspace.clone()),
        approved_by_lease: false,
        required_plugin_implementation: None,
        spawn_consumed: Mutex::new(false),
    };

    let workspace_for_spawn = workspace.clone();
    let renamed_for_spawn = renamed.clone();
    let error = match authorizer.authorize_and_spawn(Box::new(move |_| {
        let rename_error = std::fs::rename(workspace_for_spawn, renamed_for_spawn).unwrap_err();
        assert_eq!(
            rename_error.raw_os_error(),
            Some(32),
            "expected ERROR_SHARING_VIOLATION"
        );
        Err(io::Error::other("test stopped after the final authorization boundary"))
    })) {
        Ok(_) => panic!("test closure must stop before operating-system spawn"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), io::ErrorKind::Other);

    permissions.set_boundary(ExecutionBoundary::unrestricted());
    std::fs::rename(&workspace, &renamed).unwrap();
}

#[cfg(target_os = "macos")]
#[test]
fn path_only_seatbelt_rejects_after_final_authorization_without_starting_the_target() {
    let workspace = tempfile::tempdir().unwrap();
    let (context, permissions, request) = process_context(PermissionMode::Auto);
    permissions.set_boundary(workspace_process_boundary(workspace.path(), &request));
    let authorizer = EffectProcessSpawnAuthorizer {
        context: context.clone(),
        request,
        approved_environment: context.environment(),
        approved_mode: PermissionMode::Auto,
        approved_ceiling: PermissionCeiling::unrestricted(),
        workspace_root: Some(workspace.path().to_path_buf()),
        approved_by_lease: false,
        required_plugin_implementation: None,
        spawn_consumed: Mutex::new(false),
    };
    let spawn_calls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&spawn_calls);

    let error = match authorizer.authorize_and_spawn(Box::new(move |_| {
        counted.fetch_add(1, Ordering::SeqCst);
        Err(io::Error::other("path-only target must not start"))
    })) {
        Ok(_) => panic!("path-only target must not start"),
        Err(error) => error,
    };

    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(spawn_calls.load(Ordering::SeqCst), 0);
    assert_ne!(
        solaris_process::platform_sandbox_report().enforcement(),
        SandboxEnforcement::Full
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn linux_final_authorization_binds_the_opened_workspace_after_path_replacement() {
    if solaris_process::platform_sandbox_report().enforcement() != SandboxEnforcement::Full {
        return;
    }
    let parent = tempfile::tempdir().unwrap();
    let workspace = parent.path().join("workspace");
    let original = parent.path().join("original-workspace");
    std::fs::create_dir(&workspace).unwrap();
    let (context, permissions, request) = process_context(PermissionMode::Auto);
    permissions.set_boundary(workspace_process_boundary(&workspace, &request));
    let authorizer = EffectProcessSpawnAuthorizer {
        context: context.clone(),
        request,
        approved_environment: context.environment(),
        approved_mode: PermissionMode::Auto,
        approved_ceiling: PermissionCeiling::unrestricted(),
        workspace_root: Some(workspace.clone()),
        approved_by_lease: false,
        required_plugin_implementation: None,
        spawn_consumed: Mutex::new(false),
    };
    let shell = solaris_config::shell::default_shell();
    let executable = solaris_process::inspect_executable(&shell.path).unwrap();
    let pinned = solaris_process::pin_executable(&shell.path, &executable).unwrap();
    let workspace_for_spawn = workspace.clone();
    let original_for_spawn = original.clone();

    let mut child = authorizer
        .authorize_and_spawn(Box::new(move |policy| {
            std::fs::rename(&workspace_for_spawn, &original_for_spawn)?;
            std::fs::create_dir(&workspace_for_spawn)?;
            let mut command = pinned.command().map_err(|error| io::Error::other(error.to_string()))?;
            command
                .args(shell.derive_exec_args("printf original > identity-marker", false))
                .current_dir(&workspace_for_spawn)
                .launch_policy(policy);
            command.spawn()
        }))
        .unwrap();
    assert!(child.wait().await.unwrap().success());
    assert_eq!(std::fs::read(original.join("identity-marker")).unwrap(), b"original");
    assert!(!workspace.join("identity-marker").exists());
}

#[tokio::test]
async fn approved_auto_remains_sandboxed_after_current_mode_becomes_bypass() {
    let workspace = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let marker = outside.path().join("stricter-mode-marker.txt");
    let (context, permissions, request) = process_context(PermissionMode::Auto);
    let authorization = spawn_authorization(&context, request, PermissionMode::Auto, workspace.path());

    permissions.set_mode(PermissionMode::Bypass);
    let result = run_marker(&marker, authorization).await;

    if cfg!(windows) {
        assert!(result.is_err());
    }
    #[cfg(target_os = "macos")]
    assert!(result.is_err(), "path-only Seatbelt must fail closed: {result:?}");
    assert!(!marker.exists());
}

#[test]
fn bypass_approval_does_not_cover_a_later_auto_review_requirement() {
    let workspace = tempfile::tempdir().unwrap();
    let (context, permissions, request) = process_context(PermissionMode::Bypass);
    permissions.replace_rules(Vec::new());
    assert_eq!(context.evaluate(&request).decision, PermissionDecision::Allow);
    let authorizer = EffectProcessSpawnAuthorizer {
        context: context.clone(),
        request,
        approved_environment: context.environment(),
        approved_mode: PermissionMode::Bypass,
        approved_ceiling: PermissionCeiling::unrestricted(),
        workspace_root: Some(workspace.path().to_path_buf()),
        approved_by_lease: false,
        required_plugin_implementation: None,
        spawn_consumed: Mutex::new(false),
    };
    permissions.set_mode(PermissionMode::Auto);
    let final_review_calls = Arc::new(AtomicUsize::new(0));
    permissions.set_reviewer(Arc::new(AllowReviewer {
        calls: Arc::clone(&final_review_calls),
    }));
    let spawn_calls = Arc::new(AtomicUsize::new(0));
    let counted_spawn_calls = Arc::clone(&spawn_calls);

    let error = match authorizer.authorize_and_spawn(Box::new(move |_| {
        counted_spawn_calls.fetch_add(1, Ordering::SeqCst);
        Err(io::Error::other("denied spawn closure must not run"))
    })) {
        Ok(_) => panic!("stricter Auto mode must reject a Bypass-only approval"),
        Err(error) => error,
    };

    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(spawn_calls.load(Ordering::SeqCst), 0);
    assert_eq!(final_review_calls.load(Ordering::SeqCst), 0);
}

#[test]
fn explicit_deny_mutation_waits_for_the_guarded_spawn_call() {
    let workspace = tempfile::tempdir().unwrap();
    let (context, permissions, request) = process_context(PermissionMode::Bypass);
    let authorizer = EffectProcessSpawnAuthorizer {
        context: context.clone(),
        request: request.clone(),
        approved_environment: context.environment(),
        approved_mode: PermissionMode::Bypass,
        approved_ceiling: PermissionCeiling::unrestricted(),
        workspace_root: Some(workspace.path().to_path_buf()),
        approved_by_lease: false,
        required_plugin_implementation: None,
        spawn_consumed: Mutex::new(false),
    };
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (mutation_started_tx, mutation_started_rx) = mpsc::channel();
    let (mutation_completed_tx, mutation_completed_rx) = mpsc::channel();

    let authorization = std::thread::spawn(move || {
        authorizer.authorize_and_spawn(Box::new(move |_| {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Err(io::Error::other("test stopped before operating-system spawn"))
        }))
    });
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let mutating_permissions = permissions.clone();
    let mutation = std::thread::spawn(move || {
        mutation_started_tx.send(()).unwrap();
        mutating_permissions.add_rule(deny_process_rule());
        mutation_completed_tx.send(()).unwrap();
    });
    mutation_started_rx.recv_timeout(Duration::from_secs(2)).unwrap();

    assert!(mutation_completed_rx.recv_timeout(Duration::from_millis(100)).is_err());
    release_tx.send(()).unwrap();
    assert!(authorization.join().unwrap().is_err());
    mutation_completed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    mutation.join().unwrap();
    assert_eq!(context.evaluate(&request).decision, PermissionDecision::Deny);
}

#[test]
fn failed_spawn_attempt_consumes_authorization_and_cannot_retry() {
    let workspace = tempfile::tempdir().unwrap();
    let (context, permissions, request) = process_context(PermissionMode::Auto);
    permissions.set_boundary(workspace_process_boundary(workspace.path(), &request));
    permissions.replace_rules(Vec::new());
    context.issue_approval_lease(&request, false).unwrap();
    let evaluation = context.evaluate(&request);
    assert_eq!(evaluation.decision, PermissionDecision::Allow);
    assert!(evaluation.matched_lease);
    let authorizer = EffectProcessSpawnAuthorizer {
        context: context.clone(),
        request: request.clone(),
        approved_environment: context.environment(),
        approved_mode: PermissionMode::Auto,
        approved_ceiling: PermissionCeiling::unrestricted(),
        workspace_root: Some(workspace.path().to_path_buf()),
        approved_by_lease: true,
        required_plugin_implementation: None,
        spawn_consumed: Mutex::new(false),
    };
    let spawn_attempts = Arc::new(AtomicUsize::new(0));

    let attempts = Arc::clone(&spawn_attempts);
    let error = match authorizer.authorize_and_spawn(Box::new(move |_| {
        attempts.fetch_add(1, Ordering::SeqCst);
        Err(io::Error::other("test stopped before operating-system spawn"))
    })) {
        Ok(_) => panic!("test spawn closure must return an error"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert!(!context.evaluate(&request).matched_lease);
    assert_eq!(spawn_attempts.load(Ordering::SeqCst), 1);

    let attempts = Arc::clone(&spawn_attempts);
    let error = match authorizer.authorize_and_spawn(Box::new(move |_| {
        attempts.fetch_add(1, Ordering::SeqCst);
        Err(io::Error::other("consumed spawn closure must not run"))
    })) {
        Ok(_) => panic!("consumed authorization must reject process spawn"),
        Err(error) => error,
    };

    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(spawn_attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn consumed_authorization_rejects_second_spawn_after_boundary_tightening() {
    let workspace = tempfile::tempdir().unwrap();
    let markers = tempfile::tempdir().unwrap();
    let first_marker = markers.path().join("first-boundary-command.txt");
    let second_marker = markers.path().join("second-boundary-command.txt");
    let (context, permissions, request) = process_context(PermissionMode::Auto);
    permissions.set_boundary(workspace_process_boundary(workspace.path(), &request));
    permissions.replace_rules(Vec::new());
    context.issue_approval_lease(&request, false).unwrap();
    let authorizer = EffectProcessSpawnAuthorizer {
        context: context.clone(),
        request: request.clone(),
        approved_environment: context.environment(),
        approved_mode: PermissionMode::Auto,
        approved_ceiling: PermissionCeiling::unrestricted(),
        workspace_root: Some(workspace.path().to_path_buf()),
        approved_by_lease: true,
        required_plugin_implementation: None,
        spawn_consumed: Mutex::new(false),
    };

    let mut first = authorizer
        .authorize_and_spawn(Box::new(move |_| spawn_marker_child(first_marker)))
        .unwrap();
    assert!(first.wait().await.unwrap().success());

    permissions.set_boundary(workspace_process_boundary(workspace.path(), &request));
    let error = match authorizer.authorize_and_spawn(Box::new(move |_| spawn_marker_child(second_marker.clone()))) {
        Ok(_) => panic!("tightened boundary must reject the second process"),
        Err(error) => error,
    };

    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert!(!markers.path().join("second-boundary-command.txt").exists());
}

#[tokio::test]
async fn consumed_authorization_rejects_second_spawn_after_grant_revocation() {
    let workspace = tempfile::tempdir().unwrap();
    let markers = tempfile::tempdir().unwrap();
    let first_marker = markers.path().join("first-configured-command.txt");
    let second_marker = markers.path().join("second-configured-command.txt");
    let (context, permissions, request) = process_context(PermissionMode::Auto);
    permissions.replace_rules(Vec::new());
    permissions.set_boundary(ExecutionBoundary::workspace(
        workspace.path().to_string_lossy().into_owned(),
    ));
    permissions.allow_configured_effect_for("test:process", request.capability.clone(), &request.descriptor);
    context.issue_approval_lease(&request, false).unwrap();
    let authorizer = EffectProcessSpawnAuthorizer {
        context: context.clone(),
        request: request.clone(),
        approved_environment: context.environment(),
        approved_mode: PermissionMode::Auto,
        approved_ceiling: PermissionCeiling::unrestricted(),
        workspace_root: Some(workspace.path().to_path_buf()),
        approved_by_lease: true,
        required_plugin_implementation: None,
        spawn_consumed: Mutex::new(false),
    };

    let mut first = authorizer
        .authorize_and_spawn(Box::new(move |_| spawn_marker_child(first_marker)))
        .unwrap();
    assert!(first.wait().await.unwrap().success());

    permissions.revoke_configured_effect_from("test:process");
    let error = match authorizer.authorize_and_spawn(Box::new(move |_| spawn_marker_child(second_marker.clone()))) {
        Ok(_) => panic!("revoked configured grant must reject the second process"),
        Err(error) => error,
    };

    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert!(!markers.path().join("second-configured-command.txt").exists());
}

#[test]
fn final_spawn_authorization_does_not_reenter_permission_reviewer() {
    const CHILD_ENV: &str = "SOLARIS_PROCESS_AUTHORIZATION_REVIEWER_CHILD";
    const CHILD_MARKER_ENV: &str = "SOLARIS_PROCESS_AUTHORIZATION_REVIEWER_MARKER";
    const TEST_NAME: &str = "execution_context::process_authorization::process_authorization_test::final_spawn_authorization_does_not_reenter_permission_reviewer";

    if std::env::var_os(CHILD_ENV).is_some() {
        std::fs::write(
            std::env::var_os(CHILD_MARKER_ENV).expect("child marker path"),
            b"entered",
        )
        .unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let (context, permissions, request) = process_context(PermissionMode::Auto);
        permissions.set_boundary(workspace_process_boundary(workspace.path(), &request));
        permissions.replace_rules(Vec::new());
        let initial_review_calls = Arc::new(AtomicUsize::new(0));
        permissions.set_reviewer(Arc::new(AllowReviewer {
            calls: Arc::clone(&initial_review_calls),
        }));
        let evaluation = context.evaluate(&request);
        assert_eq!(evaluation.decision, PermissionDecision::Allow);
        assert_eq!(initial_review_calls.load(Ordering::SeqCst), 1);
        let authorizer = EffectProcessSpawnAuthorizer {
            context: context.clone(),
            request,
            approved_environment: context.environment(),
            approved_mode: PermissionMode::Auto,
            approved_ceiling: PermissionCeiling::unrestricted(),
            workspace_root: Some(workspace.path().to_path_buf()),
            approved_by_lease: false,
            required_plugin_implementation: None,
            spawn_consumed: Mutex::new(false),
        };
        let final_review_calls = Arc::new(AtomicUsize::new(0));
        permissions.set_reviewer(Arc::new(MutatingReviewer {
            permissions: permissions.clone(),
            calls: Arc::clone(&final_review_calls),
        }));
        let spawn_calls = Arc::new(AtomicUsize::new(0));
        let counted_spawn_calls = Arc::clone(&spawn_calls);

        let error = match authorizer.authorize_and_spawn(Box::new(move |_| {
            counted_spawn_calls.fetch_add(1, Ordering::SeqCst);
            Err(io::Error::other("test stopped before operating-system spawn"))
        })) {
            Ok(_) => panic!("test spawn closure must return an error"),
            Err(error) => error,
        };

        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(spawn_calls.load(Ordering::SeqCst), 1);
        assert_eq!(final_review_calls.load(Ordering::SeqCst), 0);
        assert_eq!(permissions.mode(), PermissionMode::Auto);
        return;
    }

    let child_temp = tempfile::tempdir().unwrap();
    let child_marker = child_temp.path().join("entered");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(TEST_NAME)
        .arg("--nocapture")
        .env(CHILD_ENV, "1")
        .env(CHILD_MARKER_ENV, &child_marker)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "reviewer re-entry child test failed: {status}");
            assert!(child_marker.is_file(), "reviewer re-entry child test did not run");
            break;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("final spawn authorization deadlocked in PermissionReviewer::review");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn auto_spawn_policy_uses_only_exact_domains_from_the_revalidated_request() {
    let workspace = tempfile::tempdir().unwrap();
    let (context, _permissions, mut request) = process_context(PermissionMode::Auto);
    request.descriptor.resources.network_domains = vec!["https://API.Example.Test/v1".to_owned()];
    context
        .permissions()
        .set_boundary(workspace_process_boundary(workspace.path(), &request));
    let expected_policy =
        ProcessLaunchPolicy::workspace_sandbox_with_network(workspace.path(), [], [], ["https://api.example.test"])
            .unwrap();
    let authorizer = EffectProcessSpawnAuthorizer {
        context: context.clone(),
        request,
        approved_environment: context.environment(),
        approved_mode: PermissionMode::Auto,
        approved_ceiling: PermissionCeiling::unrestricted(),
        workspace_root: Some(workspace.path().to_path_buf()),
        approved_by_lease: false,
        required_plugin_implementation: None,
        spawn_consumed: Mutex::new(false),
    };

    let error = match authorizer.authorize_and_spawn(Box::new(move |policy| {
        assert_eq!(policy, expected_policy);
        Err(io::Error::other("test stopped before operating-system spawn"))
    })) {
        Ok(_) => panic!("test spawn closure must return an error"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), io::ErrorKind::Other);
}

#[test]
fn auto_spawn_rejects_unrestricted_or_ambiguous_network_before_spawn() {
    let workspace = tempfile::tempdir().unwrap();
    for configure in [
        |resources: &mut ResourceFootprint| resources.unrestricted_network = true,
        |resources: &mut ResourceFootprint| resources.network_domains = vec!["*.example.test".to_owned()],
    ] {
        let (context, _permissions, mut request) = process_context(PermissionMode::Auto);
        configure(&mut request.descriptor.resources);
        let authorizer = EffectProcessSpawnAuthorizer {
            context: context.clone(),
            request,
            approved_environment: context.environment(),
            approved_mode: PermissionMode::Auto,
            approved_ceiling: PermissionCeiling::unrestricted(),
            workspace_root: Some(workspace.path().to_path_buf()),
            approved_by_lease: false,
            required_plugin_implementation: None,
            spawn_consumed: Mutex::new(false),
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&calls);
        let error = match authorizer.authorize_and_spawn(Box::new(move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
            Err(io::Error::other("spawn must not run"))
        })) {
            Ok(_) => panic!("denied spawn closure must not run"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}
