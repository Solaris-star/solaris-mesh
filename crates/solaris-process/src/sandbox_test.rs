#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
use super::platform_sandbox_report;
use super::{
    FileIdentity, PreparedSandbox, SandboxBackend, SandboxEnforcement, SandboxError, SandboxReason, SandboxReport,
    cache_confirmed_full, ensure_protected_roots_are_disjoint, require_full_enforcement, validate_layout,
    validate_workspace_object_links,
};
use crate::ProcessLaunchPolicy;
use crate::sandbox_report::linux_sandbox_capability_report;
#[cfg(windows)]
use crate::sandbox_report_from_error;

fn sandbox_error(error: &std::io::Error) -> Option<&SandboxError> {
    error.get_ref()?.downcast_ref::<SandboxError>()
}

#[test]
fn protected_descendants_are_reduced_to_the_outermost_workspace_path() {
    let workspace = tempfile::tempdir().unwrap();
    let protected = workspace.path().join(".solaris");
    let nested = protected.join("runtime/session.sqlite3");
    std::fs::create_dir_all(nested.parent().unwrap()).unwrap();
    std::fs::write(&nested, b"state").unwrap();

    let layout = validate_layout(workspace.path(), &[nested, protected.clone()]).unwrap();

    assert_eq!(layout.protected_roots, vec![protected.canonicalize().unwrap()]);
}

#[test]
fn protected_path_nested_in_any_allowed_root_is_rejected() {
    let allowed = tempfile::tempdir().unwrap();
    let protected = allowed.path().join("protected");

    let error = ensure_protected_roots_are_disjoint(&[allowed.path().to_path_buf()], std::slice::from_ref(&protected))
        .unwrap_err();

    assert_eq!(sandbox_error(&error), Some(&SandboxError::UnrepresentableSandboxLayout));
}

#[cfg(target_os = "linux")]
#[test]
fn protected_path_within_read_only_system_allow_root_is_rejected() {
    let workspace = tempfile::tempdir().unwrap();
    let policy = ProcessLaunchPolicy::workspace_sandbox(workspace.path(), [std::path::PathBuf::from("/usr")]);

    let error = match PreparedSandbox::prepare(&policy) {
        Ok(_) => panic!("a protected read-only system root must not remain readable"),
        Err(error) => error,
    };

    assert_eq!(sandbox_error(&error), Some(&SandboxError::UnrepresentableSandboxLayout));
}

#[test]
fn equivalent_workspace_path_is_rejected() {
    let workspace = tempfile::tempdir().unwrap();
    let equivalent = workspace.path().join("child").join("..");
    let policy = ProcessLaunchPolicy::workspace_sandbox(workspace.path(), [equivalent]);

    let error = match PreparedSandbox::prepare(&policy) {
        Ok(_) => panic!("equivalent protected state must be rejected"),
        Err(error) => error,
    };

    assert_eq!(sandbox_error(&error), Some(&SandboxError::UnrepresentableSandboxLayout));
}

#[cfg(unix)]
#[test]
fn symlink_alias_into_workspace_is_rejected() {
    use std::os::unix::fs::symlink;

    let workspace = tempfile::tempdir().unwrap();
    let aliases = tempfile::tempdir().unwrap();
    let alias = aliases.path().join("workspace-alias");
    symlink(workspace.path(), &alias).unwrap();
    let policy = ProcessLaunchPolicy::workspace_sandbox(workspace.path(), [alias]);

    let error = match PreparedSandbox::prepare(&policy) {
        Ok(_) => panic!("symlinked protected state must be rejected"),
        Err(error) => error,
    };

    assert_eq!(sandbox_error(&error), Some(&SandboxError::UnrepresentableSandboxLayout));
}

#[cfg(windows)]
#[test]
fn junction_alias_into_workspace_is_rejected() {
    use std::os::windows::fs::symlink_dir;

    let workspace = tempfile::tempdir().unwrap();
    let aliases = tempfile::tempdir().unwrap();
    let alias = aliases.path().join("workspace-alias");
    if symlink_dir(workspace.path(), &alias).is_err() {
        return;
    }
    let policy = ProcessLaunchPolicy::workspace_sandbox(workspace.path(), [alias]);

    let error = match PreparedSandbox::prepare(&policy) {
        Ok(_) => panic!("linked protected state must be rejected"),
        Err(error) => error,
    };

    assert_eq!(sandbox_error(&error), Some(&SandboxError::UnrepresentableSandboxLayout));
}

#[test]
fn error_messages_do_not_include_paths() {
    let sentinel = "super-secret-sandbox-path";
    let workspace = tempfile::tempdir().unwrap();
    let protected = workspace.path().join(sentinel);
    let policy = ProcessLaunchPolicy::workspace_sandbox(workspace.path(), [protected]);

    let error = PreparedSandbox::prepare(&policy).err().unwrap();

    assert!(!error.to_string().contains(sentinel));
}

#[test]
fn strict_workspace_launch_accepts_only_full_enforcement() {
    assert!(SandboxEnforcement::Full.satisfies_strict_auto());
    assert!(!SandboxEnforcement::Partial.satisfies_strict_auto());
    assert!(!SandboxEnforcement::Unavailable.satisfies_strict_auto());
}

#[test]
fn partial_enforcement_is_rejected_with_its_structured_report() {
    let report = SandboxReport::new(
        SandboxEnforcement::Partial,
        SandboxBackend::ExternalRunner,
        SandboxReason::SetupFailed,
    );

    let error = require_full_enforcement(report).unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    assert_eq!(
        sandbox_error(&error),
        Some(&SandboxError::InsufficientEnforcement { report })
    );
}

#[test]
fn unavailable_enforcement_is_rejected_with_its_structured_report() {
    let report = SandboxReport::new(
        SandboxEnforcement::Unavailable,
        SandboxBackend::None,
        SandboxReason::BubblewrapUnavailable,
    );

    let error = require_full_enforcement(report).unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
    assert_eq!(
        sandbox_error(&error),
        Some(&SandboxError::InsufficientEnforcement { report })
    );
}

#[test]
fn missing_bwrap_has_a_structured_unavailable_report() {
    let report = linux_sandbox_capability_report(false, true, true);

    assert_eq!(report.enforcement(), SandboxEnforcement::Unavailable);
    assert_eq!(report.backend(), SandboxBackend::None);
    assert_eq!(report.reason(), SandboxReason::BubblewrapUnavailable);
}

#[test]
fn missing_helper_has_a_structured_unavailable_report() {
    let report = linux_sandbox_capability_report(true, false, true);

    assert_eq!(report.enforcement(), SandboxEnforcement::Unavailable);
    assert_eq!(report.backend(), SandboxBackend::None);
    assert_eq!(report.reason(), SandboxReason::HelperUnavailable);
}

#[test]
fn old_or_disabled_landlock_has_a_structured_unavailable_report() {
    let report = linux_sandbox_capability_report(true, true, false);

    assert_eq!(report.enforcement(), SandboxEnforcement::Unavailable);
    assert_eq!(report.backend(), SandboxBackend::None);
    assert_eq!(report.reason(), SandboxReason::LandlockUnavailable);
}

#[test]
fn same_filesystem_layout_without_an_object_alias_is_allowed() {
    validate_workspace_object_links(
        [FileIdentity::new(7, 10), FileIdentity::new(7, 11)],
        [(FileIdentity::new(7, 12), 1), (FileIdentity::new(7, 13), 1)],
    )
    .unwrap();
}

#[test]
fn workspace_alias_of_a_protected_object_is_rejected() {
    let error = validate_workspace_object_links(
        [FileIdentity::new(7, 10), FileIdentity::new(8, 20)],
        [(FileIdentity::new(7, 30), 1), (FileIdentity::new(8, 20), 2)],
    )
    .unwrap_err();

    assert_eq!(sandbox_error(&error), Some(&SandboxError::ProtectedObjectAlias));
}

#[test]
fn workspace_hardlink_with_an_external_name_is_rejected() {
    let identity = FileIdentity::new(7, 20);

    let error = validate_workspace_object_links([], [(identity, 2)]).unwrap_err();

    assert_eq!(sandbox_error(&error), Some(&SandboxError::ExternalHardlink));
}

#[test]
fn hardlinks_are_allowed_when_every_name_is_inside_the_workspace() {
    let identity = FileIdentity::new(7, 20);

    validate_workspace_object_links([], [(identity, 2), (identity, 2)]).unwrap();
}

#[test]
fn only_a_confirmed_full_probe_is_cached() {
    let cache = std::sync::OnceLock::new();
    let unavailable = SandboxReport::new(
        SandboxEnforcement::Unavailable,
        SandboxBackend::LinuxBubblewrapLandlock,
        SandboxReason::StartConfirmationFailed,
    );
    let full = SandboxReport::new(
        SandboxEnforcement::Full,
        SandboxBackend::LinuxBubblewrapLandlock,
        SandboxReason::Enforced,
    );

    assert_eq!(cache_confirmed_full(&cache, || unavailable), unavailable);
    assert!(cache.get().is_none());
    assert_eq!(cache_confirmed_full(&cache, || full), full);
    assert_eq!(cache.get(), Some(&full));
    assert_eq!(cache_confirmed_full(&cache, || panic!("cached probe reran")), full);
}

#[cfg(windows)]
#[test]
fn windows_approved_domain_fails_closed_with_a_typed_request_report() {
    let workspace = tempfile::tempdir().unwrap();
    let policy =
        ProcessLaunchPolicy::workspace_sandbox_with_network(workspace.path(), [], [], ["https://api.example.test"])
            .unwrap();

    let error = match PreparedSandbox::prepare_for_executable(&policy, true) {
        Ok(_) => panic!("unverified Windows proxy runner must not start"),
        Err(error) => error,
    };
    let report = SandboxReport::new(
        SandboxEnforcement::Unavailable,
        SandboxBackend::WindowsAppContainer,
        SandboxReason::NetworkProxyUnavailable,
    );
    assert_eq!(
        sandbox_error(&error),
        Some(&SandboxError::NetworkProxyUnavailable { report })
    );
    assert_eq!(sandbox_report_from_error(&error), Some(report));
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
#[test]
fn platform_report_does_not_claim_full_without_a_runner() {
    let report = platform_sandbox_report();

    assert_eq!(report.enforcement(), SandboxEnforcement::Unavailable);
    assert_eq!(report.backend(), SandboxBackend::None);
    assert_eq!(report.reason(), SandboxReason::PlatformRunnerUnavailable);
}
