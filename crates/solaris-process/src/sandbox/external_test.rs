use std::ffi::OsString;
use std::path::PathBuf;

use super::{
    ExpectedResponse, FilesystemPolicy, NetworkEndpoint, NetworkPolicy, ProbeResponse, ProtocolEnforcement,
    RequestBody, ResponseRejection, StartResponse, TargetIntent, encode_request, parse_runner_config, prove_arguments,
    required_capabilities, run_arguments, validate_probe_response, validate_start_response,
};
use crate::{SandboxBackend, SandboxEnforcement, SandboxReason};

#[test]
fn full_external_runner_contract_requires_descendants_drained_before_exit() {
    let capabilities = required_capabilities(false);

    assert!(capabilities.contains(&"descendant-drain-before-runner-exit-v1".to_owned()));
    assert!(!capabilities.contains(&"descendant-containment".to_owned()));
}

const RUNNER_SHA256: &str = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const LAUNCH_TOKEN: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn request(has_network: bool) -> super::RequestEnvelope {
    let process_intent = TargetIntent {
        transport: "inherited-readonly-fd-v1".to_owned(),
        inherited_path: "/proc/self/fd/9".to_owned(),
        approved_canonical_path: "/opt/tool".to_owned(),
        content_sha256: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
        arguments: vec!["--mode".to_owned(), "safe".to_owned()],
        working_directory: "/workspace".to_owned(),
        environment_sha256: "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd".to_owned(),
        environment_entry_count: 3,
    };
    encode_request(RequestBody {
        request_id: "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee".to_owned(),
        process_intent_sha256: String::new(),
        process_intent,
        filesystem: FilesystemPolicy {
            workspace_root: "/workspace".to_owned(),
            runtime_state_roots: vec!["/runtime".to_owned()],
            protected_object_identities: vec!["0000000000000001:00000000000000000000000000000002".to_owned()],
        },
        network: NetworkPolicy {
            direct_access: false,
            approved_endpoints: has_network
                .then(|| NetworkEndpoint {
                    host: "api.example.test".to_owned(),
                    port: 443,
                })
                .into_iter()
                .collect(),
        },
        required_capabilities: required_capabilities(has_network),
    })
    .unwrap()
    .0
}

fn probe_response(
    request: &super::RequestEnvelope,
    enforcement: ProtocolEnforcement,
    capabilities: Vec<String>,
) -> Vec<u8> {
    serde_json::to_vec(&ProbeResponse {
        protocol: super::PROTOCOL_NAME.to_owned(),
        version: super::PROTOCOL_VERSION,
        request_id: request.body.request_id.clone(),
        request_sha256: request.request_sha256.clone(),
        process_intent_sha256: request.body.process_intent_sha256.clone(),
        runner_binary_sha256: RUNNER_SHA256.to_owned(),
        enforcement,
        capabilities,
        launch_token: LAUNCH_TOKEN.to_owned(),
    })
    .unwrap()
}

fn expected(request: &super::RequestEnvelope) -> ExpectedResponse<'_> {
    ExpectedResponse {
        request,
        runner_binary_sha256: RUNNER_SHA256,
    }
}

#[test]
fn request_digest_binds_intent_workspace_runtime_and_network_policy() {
    let base = request(false);
    let changed_network = request(true);
    let mut changed_workspace = base.body.clone();
    changed_workspace.filesystem.workspace_root = "/different-workspace".to_owned();
    let changed_workspace = encode_request(changed_workspace).unwrap().0;
    let mut changed_runtime = base.body.clone();
    changed_runtime.filesystem.runtime_state_roots = vec!["/different-runtime".to_owned()];
    let changed_runtime = encode_request(changed_runtime).unwrap().0;
    let mut changed_intent = base.body.clone();
    changed_intent.process_intent.arguments.push("other".to_owned());
    let changed_intent = encode_request(changed_intent).unwrap().0;

    assert_ne!(base.request_sha256, changed_network.request_sha256);
    assert_ne!(base.request_sha256, changed_workspace.request_sha256);
    assert_ne!(base.request_sha256, changed_runtime.request_sha256);
    assert_ne!(base.request_sha256, changed_intent.request_sha256);
}

#[test]
fn v1_full_probe_is_partial_even_with_exact_self_reported_capabilities() {
    let request = request(true);
    let response = probe_response(
        &request,
        ProtocolEnforcement::Full,
        request.body.required_capabilities.clone(),
    );

    assert_eq!(
        validate_probe_response(&response, &expected(&request)),
        Err(ResponseRejection::Partial)
    );

    let mut wrong_digest: serde_json::Value = serde_json::from_slice(&response).unwrap();
    wrong_digest["request_sha256"] =
        serde_json::json!("sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
    assert_eq!(
        validate_probe_response(&serde_json::to_vec(&wrong_digest).unwrap(), &expected(&request)),
        Err(ResponseRejection::Invalid)
    );

    let mut wrong_runner: serde_json::Value = serde_json::from_slice(&response).unwrap();
    wrong_runner["runner_binary_sha256"] =
        serde_json::json!("sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
    assert_eq!(
        validate_probe_response(&serde_json::to_vec(&wrong_runner).unwrap(), &expected(&request)),
        Err(ResponseRejection::Invalid)
    );

    let mut wrong_version: serde_json::Value = serde_json::from_slice(&response).unwrap();
    wrong_version["version"] = serde_json::json!(super::PROTOCOL_VERSION + 1);
    assert_eq!(
        validate_probe_response(&serde_json::to_vec(&wrong_version).unwrap(), &expected(&request)),
        Err(ResponseRejection::Invalid)
    );

    let mut malformed_token: serde_json::Value = serde_json::from_slice(&response).unwrap();
    malformed_token["launch_token"] = serde_json::json!("not-a-token");
    assert_eq!(
        validate_probe_response(&serde_json::to_vec(&malformed_token).unwrap(), &expected(&request)),
        Err(ResponseRejection::Invalid)
    );
}

#[test]
fn missing_duplicate_or_extra_capabilities_are_partial() {
    let request = request(true);
    let mut missing = request.body.required_capabilities.clone();
    missing.pop();
    let mut duplicate = request.body.required_capabilities.clone();
    duplicate.push(duplicate[0].clone());
    let mut extra = request.body.required_capabilities.clone();
    extra.push("unrequested-capability".to_owned());

    for capabilities in [missing, duplicate, extra] {
        let response = probe_response(&request, ProtocolEnforcement::Full, capabilities);
        assert_eq!(
            validate_probe_response(&response, &expected(&request)),
            Err(ResponseRejection::Partial)
        );
    }
}

#[test]
fn malformed_unknown_or_oversized_probe_responses_are_rejected() {
    let request = request(false);
    let response = probe_response(
        &request,
        ProtocolEnforcement::Full,
        request.body.required_capabilities.clone(),
    );
    assert_eq!(
        validate_probe_response(b"not-json", &expected(&request)),
        Err(ResponseRejection::Invalid)
    );

    let mut unknown_field: serde_json::Value = serde_json::from_slice(&response).unwrap();
    unknown_field["unexpected"] = serde_json::json!(true);
    assert_eq!(
        validate_probe_response(&serde_json::to_vec(&unknown_field).unwrap(), &expected(&request)),
        Err(ResponseRejection::Invalid)
    );

    let oversized = vec![b' '; super::MAX_PROTOCOL_BYTES + 1];
    assert_eq!(
        validate_probe_response(&oversized, &expected(&request)),
        Err(ResponseRejection::Invalid)
    );
}

#[test]
fn partial_and_unavailable_probe_reports_never_become_full() {
    let request = request(false);
    for (enforcement, rejection, expected_enforcement) in [
        (
            ProtocolEnforcement::Partial,
            ResponseRejection::Partial,
            SandboxEnforcement::Partial,
        ),
        (
            ProtocolEnforcement::Unavailable,
            ResponseRejection::Unavailable,
            SandboxEnforcement::Unavailable,
        ),
    ] {
        let response = probe_response(&request, enforcement, request.body.required_capabilities.clone());
        assert_eq!(validate_probe_response(&response, &expected(&request)), Err(rejection));
        let report = rejection.report();
        assert_eq!(report.enforcement(), expected_enforcement);
        assert_eq!(report.backend(), SandboxBackend::ExternalRunner);
        assert_eq!(report.reason(), SandboxReason::ExternalRunnerCapabilityInsufficient);
    }
}

#[test]
fn v1_full_start_response_is_partial_even_when_runner_claims_target_started() {
    let request = request(false);
    let response = StartResponse {
        protocol: super::PROTOCOL_NAME.to_owned(),
        version: super::PROTOCOL_VERSION,
        request_id: request.body.request_id.clone(),
        request_sha256: request.request_sha256.clone(),
        process_intent_sha256: request.body.process_intent_sha256.clone(),
        runner_binary_sha256: RUNNER_SHA256.to_owned(),
        enforcement: ProtocolEnforcement::Full,
        capabilities: request.body.required_capabilities.clone(),
        launch_token: LAUNCH_TOKEN.to_owned(),
        target_started: true,
    };
    let bytes = serde_json::to_vec(&response).unwrap();
    assert_eq!(
        validate_start_response(&bytes, &expected(&request), LAUNCH_TOKEN),
        Err(ResponseRejection::Partial)
    );
    assert_eq!(
        validate_start_response(
            &bytes,
            &expected(&request),
            "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
        ),
        Err(ResponseRejection::Invalid)
    );

    let mut missing_capability: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    missing_capability["capabilities"].as_array_mut().unwrap().pop();
    assert_eq!(
        validate_start_response(
            &serde_json::to_vec(&missing_capability).unwrap(),
            &expected(&request),
            LAUNCH_TOKEN,
        ),
        Err(ResponseRejection::Partial)
    );

    let mut wrong_version: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    wrong_version["version"] = serde_json::json!(super::PROTOCOL_VERSION + 1);
    assert_eq!(
        validate_start_response(
            &serde_json::to_vec(&wrong_version).unwrap(),
            &expected(&request),
            LAUNCH_TOKEN,
        ),
        Err(ResponseRejection::Invalid)
    );
}

#[test]
fn external_runner_configuration_requires_an_absolute_path_and_exact_digest() {
    let absolute = std::env::current_dir().unwrap().join("trusted-runner");
    let digest = OsString::from("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

    let configured = parse_runner_config(Some(absolute.clone().into_os_string()), Some(digest.clone()))
        .unwrap()
        .unwrap();
    assert_eq!(configured.path, absolute);
    assert_eq!(configured.binary_sha256, RUNNER_SHA256);
    assert!(parse_runner_config(None, None).unwrap().is_none());
    assert!(parse_runner_config(Some(PathBuf::from("relative").into_os_string()), Some(digest.clone())).is_err());
    assert!(parse_runner_config(Some(absolute.into_os_string()), None).is_err());
    assert!(
        parse_runner_config(
            Some(PathBuf::from("/runner").into_os_string()),
            Some(OsString::from("BAD"))
        )
        .is_err()
    );
}

#[test]
fn runner_invocation_uses_fixed_arguments_without_a_shell() {
    let request_path = PathBuf::from("/tmp/request with spaces;and-metacharacters.json");
    let response_path = PathBuf::from("/tmp/response with spaces.json");

    assert_eq!(
        prove_arguments(request_path.clone()),
        vec![
            OsString::from("solaris-auto-v1"),
            OsString::from("prove"),
            OsString::from("--request"),
            request_path.clone().into_os_string(),
        ]
    );
    assert_eq!(
        run_arguments(request_path, LAUNCH_TOKEN, response_path.clone()),
        vec![
            OsString::from("solaris-auto-v1"),
            OsString::from("run"),
            OsString::from("--request"),
            OsString::from("/tmp/request with spaces;and-metacharacters.json"),
            OsString::from("--launch-token"),
            OsString::from(LAUNCH_TOKEN),
            OsString::from("--start-response"),
            response_path.into_os_string(),
        ]
    );
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
#[test]
fn self_reported_v1_full_cannot_hide_a_setsid_descendant() {
    const ROLE: &str = "SOLARIS_EXTERNAL_V1_COUNTEREXAMPLE_ROLE";
    if std::env::var(ROLE).as_deref() == Ok("runner") {
        let executable = std::env::current_exe().unwrap();
        let marker = std::env::var_os("SOLARIS_EXTERNAL_V1_COUNTEREXAMPLE_MARKER").unwrap();
        #[allow(clippy::zombie_processes)]
        let _descendant = std::process::Command::new(executable)
            .args([
                "--exact",
                "sandbox::external::external_test::self_reported_v1_full_cannot_hide_a_setsid_descendant",
                "--nocapture",
            ])
            .env(ROLE, "descendant")
            .env("SOLARIS_EXTERNAL_V1_COUNTEREXAMPLE_MARKER", marker)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        return;
    }
    if std::env::var(ROLE).as_deref() == Ok("descendant") {
        assert_ne!(unsafe { libc::setsid() }, -1);
        std::thread::sleep(std::time::Duration::from_millis(300));
        std::fs::write(
            std::env::var_os("SOLARIS_EXTERNAL_V1_COUNTEREXAMPLE_MARKER").unwrap(),
            b"escaped",
        )
        .unwrap();
        return;
    }

    let directory = tempfile::tempdir().unwrap();
    let marker = directory.path().join("delayed-marker");
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "sandbox::external::external_test::self_reported_v1_full_cannot_hide_a_setsid_descendant",
            "--nocapture",
        ])
        .env(ROLE, "runner")
        .env("SOLARIS_EXTERNAL_V1_COUNTEREXAMPLE_MARKER", &marker)
        .status()
        .unwrap();
    assert!(status.success());
    assert!(!marker.exists());
    std::thread::sleep(std::time::Duration::from_millis(500));
    assert_eq!(std::fs::read(&marker).unwrap(), b"escaped");

    let request = request(false);
    let response = probe_response(
        &request,
        ProtocolEnforcement::Full,
        request.body.required_capabilities.clone(),
    );
    assert_eq!(
        validate_probe_response(&response, &expected(&request)),
        Err(ResponseRejection::Partial)
    );
}
