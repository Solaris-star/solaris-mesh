use std::time::Duration;

use clap::Parser;
use solaris_types::permission::PermissionMode;

use crate::cli::Cli;

use super::{
    finish_process_recovery, process_recovery_event, process_recovery_terminal_message, resolve_permission_mode,
};

fn cli(arguments: &[&str], json_stream: bool) -> Cli {
    let mut values = vec!["solaris"];
    if json_stream {
        values.push("--json-stream");
    }
    values.extend_from_slice(arguments);
    Cli::parse_from(values)
}

#[test]
fn approval_convenience_keeps_auto_for_every_cli_host() {
    for json_stream in [false, true] {
        assert_eq!(
            resolve_permission_mode(&cli(&[], json_stream), true).unwrap(),
            PermissionMode::Auto
        );
        assert_eq!(
            resolve_permission_mode(&cli(&["--auto-approve"], json_stream), false).unwrap(),
            PermissionMode::Auto
        );
    }
}

#[test]
fn only_explicit_permission_bypass_selects_bypass_for_every_cli_host() {
    for json_stream in [false, true] {
        assert_eq!(
            resolve_permission_mode(&cli(&["--permission", "bypass"], json_stream), true).unwrap(),
            PermissionMode::Bypass
        );
        assert_eq!(
            resolve_permission_mode(&cli(&["--permission", "auto"], json_stream), true).unwrap(),
            PermissionMode::Auto
        );
        assert_eq!(
            resolve_permission_mode(&cli(&["--permission", "plan"], json_stream), true).unwrap(),
            PermissionMode::Plan
        );
    }
}

#[tokio::test]
async fn json_host_recovery_event_keeps_typed_pending_ids_for_reconciliation() {
    use solaris_process::{
        ProcessRecoveryKind, ProcessRecoveryLifecycle, ProcessRecoveryState, isolate_process_recoveries_for_test,
        pending_process_recoveries, pending_process_recovery_error_for_test, process_recovery_required,
        retry_process_recovery,
    };
    use solaris_protocol::events::ProtocolEvent;

    let _recovery_test_guard = isolate_process_recoveries_for_test();
    let marker = pending_process_recovery_error_for_test(ProcessRecoveryKind::StartedChild, 10);
    let expected = process_recovery_required(&marker).unwrap();
    let mut lifecycle = ProcessRecoveryLifecycle::start(Duration::from_secs(60));
    let error = lifecycle.shutdown(Duration::ZERO).await.unwrap_err();

    let ProtocolEvent::Error { error: info, .. } = process_recovery_event(&error) else {
        panic!("process recovery must be reported as a Host error event");
    };
    let report: serde_json::Value = serde_json::from_str(&info.message).unwrap();
    assert_eq!(info.code, "process_reconciliation_required");
    assert!(!info.retryable);
    assert_eq!(report["schema"], "solaris/process-recovery-report/v1");
    let entry = report["recoveries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["id"] == expected.id().get())
        .unwrap();
    assert_eq!(entry["kind"], "started_child");
    assert_eq!(entry["ref"], expected.reference());
    assert!(pending_process_recoveries().contains(&expected));

    while retry_process_recovery(expected.id()).unwrap() == ProcessRecoveryState::Pending {}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_and_json_hosts_continue_after_reporting_until_recovery_is_empty() {
    use std::sync::{Arc, Mutex};

    use solaris_process::{
        ProcessRecoveryKind, ProcessRecoveryLifecycle, blocking_process_recovery_error_for_test,
        isolate_process_recoveries_for_test, pending_process_recoveries, process_recovery_required,
    };
    use solaris_protocol::events::ProtocolEvent;

    let _recovery_test_guard = isolate_process_recoveries_for_test();
    for json_stream in [false, true] {
        let marker =
            blocking_process_recovery_error_for_test(ProcessRecoveryKind::StartedChild, Duration::from_millis(150));
        let expected = process_recovery_required(&marker).unwrap();
        let reports = Arc::new(Mutex::new(Vec::new()));
        let reports_for_callback = Arc::clone(&reports);
        let mut lifecycle = ProcessRecoveryLifecycle::start(Duration::from_secs(60));

        finish_process_recovery(&mut lifecycle, Duration::from_millis(25), false, |error| {
            assert!(pending_process_recoveries().contains(&expected));
            let message = if json_stream {
                let ProtocolEvent::Error { error, .. } = process_recovery_event(error) else {
                    panic!("JSON Host recovery report must be an error event");
                };
                error.message
            } else {
                process_recovery_terminal_message(error)
            };
            reports_for_callback.lock().unwrap().push(message);
        })
        .await
        .unwrap();

        let reports = reports.lock().unwrap();
        assert!(!reports.is_empty(), "Host must report before continuing recovery");
        assert!(reports.iter().any(|report| report.contains(&expected.reference())));
        assert!(!pending_process_recoveries().contains(&expected));
    }
}

#[tokio::test]
async fn explicit_force_exit_reports_and_leaves_recovery_queryable() {
    use solaris_process::{
        ProcessRecoveryKind, ProcessRecoveryLifecycle, ProcessRecoveryState, isolate_process_recoveries_for_test,
        pending_process_recoveries, pending_process_recovery_error_for_test, process_recovery_required,
        retry_process_recovery,
    };

    let _recovery_test_guard = isolate_process_recoveries_for_test();
    let marker = pending_process_recovery_error_for_test(ProcessRecoveryKind::StartedChild, 10);
    let expected = process_recovery_required(&marker).unwrap();
    let mut lifecycle = ProcessRecoveryLifecycle::start(Duration::from_secs(60));
    let mut reported = false;

    let error = finish_process_recovery(&mut lifecycle, Duration::ZERO, true, |error| {
        reported = true;
        assert!(error.pending().contains(&expected));
        assert!(pending_process_recoveries().contains(&expected));
    })
    .await
    .unwrap_err();

    assert!(reported);
    assert!(error.pending().contains(&expected));
    assert!(pending_process_recoveries().contains(&expected));
    while retry_process_recovery(expected.id()).unwrap() == ProcessRecoveryState::Pending {}
}
