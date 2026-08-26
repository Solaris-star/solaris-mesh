use std::collections::BTreeSet;
use std::ffi::OsString;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{SandboxBackend, SandboxEnforcement, SandboxReason, SandboxReport};

const PROTOCOL_NAME: &str = "solaris-auto-external-runner";
const PROTOCOL_VERSION: u16 = 1;
const MAX_PROTOCOL_BYTES: usize = 256 * 1024;
const BASE_CAPABILITIES: &[&str] = &[
    "control-channel-isolated",
    "descendant-drain-before-runner-exit-v1",
    "direct-network-blocked",
    "environment-digest-verified",
    "private-temporary-directory",
    "process-intent-verified",
    "protected-object-aliases-blocked",
    "runtime-state-hidden",
    "single-use-launch-token",
    "target-content-digest-verified",
    "workspace-write-isolated",
];
const NETWORK_CAPABILITIES: &[&str] = &["approved-domain-host-proxy", "dns-and-destination-ip-revalidated"];

#[derive(Clone, Debug, PartialEq, Eq)]
struct ExternalRunnerConfig {
    path: PathBuf,
    binary_sha256: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConfigError {
    Incomplete,
    RelativePath,
    InvalidDigest,
}

fn parse_runner_config(
    path: Option<OsString>,
    binary_sha256: Option<OsString>,
) -> Result<Option<ExternalRunnerConfig>, ConfigError> {
    let (path, binary_sha256) = match (path, binary_sha256) {
        (None, None) => return Ok(None),
        (Some(path), Some(binary_sha256)) => (path, binary_sha256),
        (Some(_), None) | (None, Some(_)) => return Err(ConfigError::Incomplete),
    };
    let path = PathBuf::from(path);
    if !path.is_absolute() {
        return Err(ConfigError::RelativePath);
    }
    let binary_sha256 = binary_sha256.into_string().map_err(|_| ConfigError::InvalidDigest)?;
    let digest = binary_sha256.strip_prefix("sha256:").unwrap_or(&binary_sha256);
    if !valid_hex_digest(digest) {
        return Err(ConfigError::InvalidDigest);
    }
    Ok(Some(ExternalRunnerConfig {
        path,
        binary_sha256: format!("sha256:{digest}"),
    }))
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct TargetIntent {
    transport: String,
    inherited_path: String,
    approved_canonical_path: String,
    content_sha256: String,
    arguments: Vec<String>,
    working_directory: String,
    environment_sha256: String,
    environment_entry_count: usize,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct FilesystemPolicy {
    workspace_root: String,
    runtime_state_roots: Vec<String>,
    protected_object_identities: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct NetworkEndpoint {
    host: String,
    port: u16,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct NetworkPolicy {
    direct_access: bool,
    approved_endpoints: Vec<NetworkEndpoint>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct RequestBody {
    request_id: String,
    process_intent_sha256: String,
    process_intent: TargetIntent,
    filesystem: FilesystemPolicy,
    network: NetworkPolicy,
    required_capabilities: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct RequestEnvelope {
    protocol: String,
    version: u16,
    request_sha256: String,
    body: RequestBody,
}

fn encode_request(mut body: RequestBody) -> Result<(RequestEnvelope, Vec<u8>), serde_json::Error> {
    body.process_intent_sha256 = tagged_sha256(&serde_json::to_vec(&body.process_intent)?);
    body.required_capabilities.sort();
    let body_bytes = serde_json::to_vec(&body)?;
    let envelope = RequestEnvelope {
        protocol: PROTOCOL_NAME.to_owned(),
        version: PROTOCOL_VERSION,
        request_sha256: tagged_sha256(&body_bytes),
        body,
    };
    let bytes = serde_json::to_vec(&envelope)?;
    Ok((envelope, bytes))
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum ProtocolEnforcement {
    Full,
    Partial,
    Unavailable,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ProbeResponse {
    protocol: String,
    version: u16,
    request_id: String,
    request_sha256: String,
    process_intent_sha256: String,
    runner_binary_sha256: String,
    enforcement: ProtocolEnforcement,
    capabilities: Vec<String>,
    launch_token: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StartResponse {
    protocol: String,
    version: u16,
    request_id: String,
    request_sha256: String,
    process_intent_sha256: String,
    runner_binary_sha256: String,
    enforcement: ProtocolEnforcement,
    capabilities: Vec<String>,
    launch_token: String,
    target_started: bool,
}

#[derive(Clone, Debug)]
struct ExpectedResponse<'a> {
    request: &'a RequestEnvelope,
    runner_binary_sha256: &'a str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ResponseRejection {
    Invalid,
    Partial,
    Unavailable,
}

impl ResponseRejection {
    const fn report(self) -> SandboxReport {
        match self {
            Self::Invalid => SandboxReport::new(
                SandboxEnforcement::Unavailable,
                SandboxBackend::ExternalRunner,
                SandboxReason::ExternalRunnerProtocolRejected,
            ),
            Self::Partial => SandboxReport::new(
                SandboxEnforcement::Partial,
                SandboxBackend::ExternalRunner,
                SandboxReason::ExternalRunnerCapabilityInsufficient,
            ),
            Self::Unavailable => SandboxReport::new(
                SandboxEnforcement::Unavailable,
                SandboxBackend::ExternalRunner,
                SandboxReason::ExternalRunnerCapabilityInsufficient,
            ),
        }
    }
}

fn validate_probe_response(bytes: &[u8], expected: &ExpectedResponse<'_>) -> Result<String, ResponseRejection> {
    if bytes.len() > MAX_PROTOCOL_BYTES {
        return Err(ResponseRejection::Invalid);
    }
    let response = serde_json::from_slice::<ProbeResponse>(bytes).map_err(|_| ResponseRejection::Invalid)?;
    validate_common_response(
        &response.protocol,
        response.version,
        &response.request_id,
        &response.request_sha256,
        &response.process_intent_sha256,
        &response.runner_binary_sha256,
        expected,
    )?;
    match response.enforcement {
        ProtocolEnforcement::Partial => return Err(ResponseRejection::Partial),
        ProtocolEnforcement::Unavailable => return Err(ResponseRejection::Unavailable),
        ProtocolEnforcement::Full => {}
    }
    validate_capabilities(&response.capabilities, expected)?;
    if !valid_hex_digest(&response.launch_token) {
        return Err(ResponseRejection::Invalid);
    }
    require_host_descendant_proof(response.version)?;
    Ok(response.launch_token)
}

fn validate_start_response(
    bytes: &[u8],
    expected: &ExpectedResponse<'_>,
    launch_token: &str,
) -> Result<(), ResponseRejection> {
    if bytes.len() > MAX_PROTOCOL_BYTES {
        return Err(ResponseRejection::Invalid);
    }
    let response = serde_json::from_slice::<StartResponse>(bytes).map_err(|_| ResponseRejection::Invalid)?;
    validate_common_response(
        &response.protocol,
        response.version,
        &response.request_id,
        &response.request_sha256,
        &response.process_intent_sha256,
        &response.runner_binary_sha256,
        expected,
    )?;
    match response.enforcement {
        ProtocolEnforcement::Partial => return Err(ResponseRejection::Partial),
        ProtocolEnforcement::Unavailable => return Err(ResponseRejection::Unavailable),
        ProtocolEnforcement::Full => {}
    }
    validate_capabilities(&response.capabilities, expected)?;
    if response.launch_token != launch_token || !response.target_started {
        return Err(ResponseRejection::Invalid);
    }
    require_host_descendant_proof(response.version)?;
    Ok(())
}

fn require_host_descendant_proof(version: u16) -> Result<(), ResponseRejection> {
    // Protocol v1 carries only a runner assertion. A compromised or mistaken
    // runner can call setsid(2), exit, and leave a descendant behind. Until a
    // later protocol binds a Host-verifiable OS/container/VM containment
    // object, Solaris must not publish Full enforcement from this assertion.
    match version {
        PROTOCOL_VERSION => Err(ResponseRejection::Partial),
        _ => Err(ResponseRejection::Invalid),
    }
}

fn validate_capabilities(capabilities: &[String], expected: &ExpectedResponse<'_>) -> Result<(), ResponseRejection> {
    let actual = capabilities.iter().cloned().collect::<BTreeSet<_>>();
    let required = expected
        .request
        .body
        .required_capabilities
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    if actual.len() != capabilities.len() || actual != required {
        return Err(ResponseRejection::Partial);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn validate_common_response(
    protocol: &str,
    version: u16,
    request_id: &str,
    request_sha256: &str,
    process_intent_sha256: &str,
    runner_binary_sha256: &str,
    expected: &ExpectedResponse<'_>,
) -> Result<(), ResponseRejection> {
    let request = expected.request;
    if protocol != PROTOCOL_NAME
        || version != PROTOCOL_VERSION
        || request_id != request.body.request_id
        || request_sha256 != request.request_sha256
        || process_intent_sha256 != request.body.process_intent_sha256
        || runner_binary_sha256 != expected.runner_binary_sha256
    {
        return Err(ResponseRejection::Invalid);
    }
    Ok(())
}

fn required_capabilities(has_approved_network: bool) -> Vec<String> {
    BASE_CAPABILITIES
        .iter()
        .chain(
            has_approved_network
                .then_some(NETWORK_CAPABILITIES)
                .into_iter()
                .flatten(),
        )
        .map(|capability| (*capability).to_owned())
        .collect()
}

fn prove_arguments(request_path: PathBuf) -> Vec<OsString> {
    vec![
        OsString::from("solaris-auto-v1"),
        OsString::from("prove"),
        OsString::from("--request"),
        request_path.into_os_string(),
    ]
}

fn run_arguments(request_path: PathBuf, launch_token: &str, start_response_path: PathBuf) -> Vec<OsString> {
    vec![
        OsString::from("solaris-auto-v1"),
        OsString::from("run"),
        OsString::from("--request"),
        request_path.into_os_string(),
        OsString::from("--launch-token"),
        OsString::from(launch_token),
        OsString::from("--start-response"),
        start_response_path.into_os_string(),
    ]
}

fn tagged_sha256(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn valid_hex_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
mod platform {
    use std::ffi::{OsStr, OsString};
    use std::fs::{File, OpenOptions};
    use std::io::{self, Read, Write};
    use std::os::fd::RawFd;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    use std::os::unix::process::CommandExt;
    use std::path::{Path, PathBuf};
    use std::process::Stdio;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use sha2::{Digest, Sha256};
    use tokio::process::{Child, Command};

    use super::{
        ExpectedResponse, ExternalRunnerConfig, FilesystemPolicy, NetworkEndpoint, NetworkPolicy, RequestBody,
        RequestEnvelope, ResponseRejection, TargetIntent, encode_request, parse_runner_config, prove_arguments,
        required_capabilities, run_arguments, tagged_sha256, validate_probe_response, validate_start_response,
    };
    use crate::containment::ChildContainment;
    use crate::environment::configure_safe_process_environment;
    use crate::executable::ExecutableIdentity;
    use crate::network_proxy::NetworkProxyPolicy;
    use crate::runner::{PinnedExecutable, inspect_executable, pin_executable};
    use crate::sandbox::layout::{ResolvedSandboxLayout, resolve_path};
    use crate::sandbox::{
        SandboxBackend, SandboxCommandDisposition, SandboxEnforcement, SandboxReason, SandboxReport, SandboxRunner,
        insufficient_enforcement_error,
    };

    const RUNNER_PATH_ENV: &str = "SOLARIS_AUTO_EXTERNAL_RUNNER";
    const RUNNER_SHA256_ENV: &str = "SOLARIS_AUTO_EXTERNAL_RUNNER_SHA256";
    const PROBE_TIMEOUT: Duration = Duration::from_secs(5);
    const START_TIMEOUT: Duration = Duration::from_secs(5);
    const POLL_INTERVAL: Duration = Duration::from_millis(10);
    static REQUEST_SEQUENCE: AtomicU64 = AtomicU64::new(1);

    pub(super) struct ExternalSandbox {
        layout: ResolvedSandboxLayout,
        target_identity: ExecutableIdentity,
        runner_config: ExternalRunnerConfig,
        runner_identity: ExecutableIdentity,
        report: SandboxReport,
        request: Option<RequestEnvelope>,
        request_bytes: Option<Vec<u8>>,
        launch_token: Option<String>,
        control_directory: Option<tempfile::TempDir>,
        start_response_path: Option<PathBuf>,
        _target_command: Option<Command>,
        _runner_executable: Option<PinnedExecutable>,
    }

    impl ExternalSandbox {
        pub(super) fn prepare(
            layout: ResolvedSandboxLayout,
            network_policy: NetworkProxyPolicy,
            target_identity: ExecutableIdentity,
        ) -> io::Result<Self> {
            let runner_config = configured_runner()?;
            let runner_identity = inspect_executable(&runner_config.path)
                .map_err(|_| unavailable_error(SandboxReason::ExternalRunnerUnavailable))?;
            if format!("sha256:{}", runner_identity.content_digest()) != runner_config.binary_sha256 {
                return Err(unavailable_error(SandboxReason::ExternalRunnerUnavailable));
            }
            let mut sandbox = Self {
                layout,
                target_identity,
                runner_config,
                runner_identity,
                report: SandboxReport::new(
                    SandboxEnforcement::Unavailable,
                    SandboxBackend::ExternalRunner,
                    SandboxReason::ExternalRunnerCapabilityInsufficient,
                ),
                request: None,
                request_bytes: None,
                launch_token: None,
                control_directory: None,
                start_response_path: None,
                _target_command: None,
                _runner_executable: None,
            };
            sandbox.prepare_policy(network_policy)?;
            Ok(sandbox)
        }

        fn prepare_policy(&mut self, network_policy: NetworkProxyPolicy) -> io::Result<()> {
            let runtime_state_roots = utf8_paths(&self.layout.protected_roots)?;
            let mut protected_object_identities = self
                .layout
                .protected_object_identities
                .iter()
                .map(|identity| identity.external_runner_value())
                .collect::<Vec<_>>();
            protected_object_identities.sort();
            let approved_endpoints = network_policy
                .external_runner_endpoints()
                .map(|(host, port)| NetworkEndpoint {
                    host: host.to_owned(),
                    port,
                })
                .collect::<Vec<_>>();
            let request_id = request_id();
            let control_directory = private_control_directory()?;
            let start_response_path = control_directory.path().join("started.json");
            self.control_directory = Some(control_directory);
            self.start_response_path = Some(start_response_path);
            self.request = Some(RequestEnvelope {
                protocol: String::new(),
                version: 0,
                request_sha256: String::new(),
                body: RequestBody {
                    request_id,
                    process_intent_sha256: String::new(),
                    process_intent: TargetIntent {
                        transport: "inherited-readonly-fd-v1".to_owned(),
                        inherited_path: String::new(),
                        approved_canonical_path: utf8_path(self.target_identity.canonical_path())?,
                        content_sha256: format!("sha256:{}", self.target_identity.content_digest()),
                        arguments: Vec::new(),
                        working_directory: String::new(),
                        environment_sha256: String::new(),
                        environment_entry_count: 0,
                    },
                    filesystem: FilesystemPolicy {
                        workspace_root: utf8_path(&self.layout.workspace_root)?,
                        runtime_state_roots,
                        protected_object_identities,
                    },
                    network: NetworkPolicy {
                        direct_access: false,
                        approved_endpoints,
                    },
                    required_capabilities: Vec::new(),
                },
            });
            Ok(())
        }

        fn configure_inner(&mut self, command: &mut Command) -> io::Result<()> {
            if self._target_command.is_some() {
                return Err(unavailable_error(SandboxReason::ExternalRunnerProtocolRejected));
            }
            let target_fd = inherited_target_fd(Path::new(command.as_std().get_program()))?;
            let target_path = utf8_path(Path::new(command.as_std().get_program()))?;
            let target_arguments = command
                .as_std()
                .get_args()
                .map(utf8_os_string)
                .collect::<io::Result<Vec<_>>>()?;
            let current_directory = command
                .as_std()
                .get_current_dir()
                .map(resolve_path)
                .transpose()?
                .unwrap_or_else(|| self.layout.workspace_root.clone());
            if !current_directory.starts_with(&self.layout.workspace_root)
                || self
                    .layout
                    .protected_roots
                    .iter()
                    .any(|protected| current_directory.starts_with(protected))
            {
                return Err(unavailable_error(SandboxReason::ExternalRunnerProtocolRejected));
            }
            let environment = explicit_environment(command)?;
            let environment_digest = digest_environment(&environment)?;
            let mut request = self
                .request
                .take()
                .ok_or_else(|| unavailable_error(SandboxReason::ExternalRunnerProtocolRejected))?;
            request.body.process_intent.inherited_path = target_path;
            request.body.process_intent.arguments = target_arguments;
            request.body.process_intent.working_directory = utf8_path(&current_directory)?;
            request.body.process_intent.environment_sha256 = environment_digest;
            request.body.process_intent.environment_entry_count = environment.len();
            request.body.required_capabilities =
                required_capabilities(!request.body.network.approved_endpoints.is_empty());
            let (request, request_bytes) = encode_request(request.body)
                .map_err(|_| unavailable_error(SandboxReason::ExternalRunnerProtocolRejected))?;
            if request_bytes.len() > super::MAX_PROTOCOL_BYTES {
                return Err(unavailable_error(SandboxReason::ExternalRunnerProtocolRejected));
            }
            let control_directory = self
                .control_directory
                .as_ref()
                .ok_or_else(|| unavailable_error(SandboxReason::ExternalRunnerProtocolRejected))?;
            let request_path = control_directory.path().join("request.json");
            write_request(&request_path, &request_bytes)?;
            let response = self.run_probe(&request_path)?;
            let validation = validate_probe_response(
                &response,
                &ExpectedResponse {
                    request: &request,
                    runner_binary_sha256: &self.runner_config.binary_sha256,
                },
            );
            let launch_token = match validation {
                Ok(launch_token) => launch_token,
                Err(rejection) => return Err(self.reject(rejection)),
            };
            let runner = pin_executable(&self.runner_config.path, &self.runner_identity)
                .map_err(|_| unavailable_error(SandboxReason::ExternalRunnerUnavailable))?;
            let mut wrapper = runner
                .command()
                .map_err(|_| unavailable_error(SandboxReason::ExternalRunnerUnavailable))?;
            wrapper
                .command
                .env_clear()
                .envs(environment.iter().map(|(key, value)| (key, value)));
            wrapper.command.args(run_arguments(
                request_path,
                &launch_token,
                self.start_response_path
                    .clone()
                    .ok_or_else(|| unavailable_error(SandboxReason::ExternalRunnerProtocolRejected))?,
            ));
            wrapper.command.current_dir(&self.layout.workspace_root);
            unsafe {
                wrapper.command.pre_exec(move || clear_cloexec(target_fd));
            }
            self._runner_executable = Some(wrapper._executable);
            self._target_command = Some(std::mem::replace(command, wrapper.command));
            self.request = Some(request);
            self.request_bytes = Some(request_bytes);
            self.launch_token = Some(launch_token);
            // Protocol v1 can never reach this point today because it has no
            // Host-verifiable descendant proof. Keep the stored report
            // non-Full as a second guard if validation changes accidentally.
            self.report = SandboxReport::new(
                SandboxEnforcement::Partial,
                SandboxBackend::ExternalRunner,
                SandboxReason::ExternalRunnerCapabilityInsufficient,
            );
            Ok(())
        }

        fn run_probe(&self, request_path: &Path) -> io::Result<Vec<u8>> {
            let runner = pin_executable(&self.runner_config.path, &self.runner_identity)
                .map_err(|_| unavailable_error(SandboxReason::ExternalRunnerUnavailable))?;
            let mut command = runner
                .command()
                .map_err(|_| unavailable_error(SandboxReason::ExternalRunnerUnavailable))?;
            configure_safe_process_environment(&mut command.command);
            command
                .command
                .args(prove_arguments(request_path.to_path_buf()))
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .current_dir(
                    self.control_directory
                        .as_ref()
                        .ok_or_else(|| unavailable_error(SandboxReason::ExternalRunnerProtocolRejected))?
                        .path(),
                );
            ChildContainment::configure_process_group(&mut command.command);
            run_bounded(&mut command.command, PROBE_TIMEOUT)
        }

        fn verify_request(&mut self) -> io::Result<()> {
            let request_bytes = self
                .request_bytes
                .as_ref()
                .ok_or_else(|| unavailable_error(SandboxReason::ExternalRunnerProtocolRejected))?;
            let request_path = self
                .control_directory
                .as_ref()
                .map(|directory| directory.path().join("request.json"))
                .ok_or_else(|| unavailable_error(SandboxReason::ExternalRunnerProtocolRejected))?;
            if std::fs::read(request_path).ok().as_deref() != Some(request_bytes.as_slice()) {
                return Err(self.reject(ResponseRejection::Invalid));
            }
            if self._target_command.is_none() || self._runner_executable.is_none() || self.launch_token.is_none() {
                return Err(self.reject(ResponseRejection::Invalid));
            }
            Ok(())
        }

        fn wait_for_start(&mut self, child: &mut Child) -> io::Result<()> {
            let deadline = Instant::now() + START_TIMEOUT;
            loop {
                let response_path = self
                    .start_response_path
                    .as_ref()
                    .ok_or_else(|| unavailable_error(SandboxReason::StartConfirmationFailed))?;
                match read_limited(response_path) {
                    Ok(Some(bytes)) => {
                        let request = self
                            .request
                            .as_ref()
                            .ok_or_else(|| unavailable_error(SandboxReason::StartConfirmationFailed))?;
                        let token = self
                            .launch_token
                            .as_deref()
                            .ok_or_else(|| unavailable_error(SandboxReason::StartConfirmationFailed))?;
                        let validation = validate_start_response(
                            &bytes,
                            &ExpectedResponse {
                                request,
                                runner_binary_sha256: &self.runner_config.binary_sha256,
                            },
                            token,
                        );
                        return match validation {
                            Ok(()) => Ok(()),
                            Err(rejection) => Err(self.reject(rejection)),
                        };
                    }
                    Ok(None) => {}
                    Err(_) => return Err(self.reject(ResponseRejection::Invalid)),
                }
                match child.try_wait() {
                    Ok(Some(_)) | Err(_) => return Err(self.fail_start()),
                    Ok(None) => {}
                }
                if Instant::now() >= deadline {
                    return Err(self.fail_start());
                }
                std::thread::sleep(POLL_INTERVAL);
            }
        }

        fn reject(&mut self, rejection: ResponseRejection) -> io::Error {
            self.report = rejection.report();
            insufficient_enforcement_error(self.report)
        }

        fn fail_start(&mut self) -> io::Error {
            self.report = SandboxReport::new(
                SandboxEnforcement::Unavailable,
                SandboxBackend::ExternalRunner,
                SandboxReason::StartConfirmationFailed,
            );
            insufficient_enforcement_error(self.report)
        }
    }

    impl SandboxRunner for ExternalSandbox {
        fn report(&self) -> SandboxReport {
            self.report
        }

        fn configure(&mut self, command: &mut Command) -> io::Result<SandboxCommandDisposition> {
            self.configure_inner(command)?;
            Ok(SandboxCommandDisposition::Replaced)
        }

        fn verify_before_spawn(&mut self) -> io::Result<()> {
            self.verify_request()
        }

        fn confirm_started(&mut self, child: &mut Child) -> io::Result<()> {
            self.wait_for_start(child)
        }

        fn guardian_requires_verified_process_tree_drain(&self) -> bool {
            // Protocol v1 is rejected before spawn, so no guardian can derive
            // a verified drain promise from the runner's assertion.
            false
        }
    }

    fn configured_runner() -> io::Result<ExternalRunnerConfig> {
        parse_runner_config(std::env::var_os(RUNNER_PATH_ENV), std::env::var_os(RUNNER_SHA256_ENV))
            .ok()
            .flatten()
            .ok_or_else(|| unavailable_error(SandboxReason::ExternalRunnerUnavailable))
    }

    fn unavailable_error(reason: SandboxReason) -> io::Error {
        insufficient_enforcement_error(SandboxReport::new(
            SandboxEnforcement::Unavailable,
            SandboxBackend::ExternalRunner,
            reason,
        ))
    }

    fn request_id() -> String {
        let sequence = REQUEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        tagged_sha256(format!("{}:{now}:{sequence}", std::process::id()).as_bytes())
    }

    fn private_control_directory() -> io::Result<tempfile::TempDir> {
        let directory = tempfile::Builder::new()
            .prefix("solaris-external-runner-")
            .tempdir()
            .map_err(|_| unavailable_error(SandboxReason::ExternalRunnerProtocolRejected))?;
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))
            .map_err(|_| unavailable_error(SandboxReason::ExternalRunnerProtocolRejected))?;
        Ok(directory)
    }

    fn write_request(path: &Path, bytes: &[u8]) -> io::Result<()> {
        let mut file = OpenOptions::new().write(true).create_new(true).mode(0o600).open(path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        file.set_permissions(std::fs::Permissions::from_mode(0o400))?;
        Ok(())
    }

    fn run_bounded(command: &mut Command, timeout: Duration) -> io::Result<Vec<u8>> {
        let mut child = command.as_std_mut().spawn()?;
        let child_id = child.id();
        let mut stdout = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("external runner stdout unavailable"))?;
        let reader = std::thread::spawn(move || {
            let mut bytes = Vec::new();
            stdout
                .by_ref()
                .take((super::MAX_PROTOCOL_BYTES + 1) as u64)
                .read_to_end(&mut bytes)?;
            Ok::<_, io::Error>(bytes)
        });
        let deadline = Instant::now() + timeout;
        let status = loop {
            if let Some(status) = child.try_wait()? {
                break status;
            }
            if Instant::now() >= deadline {
                terminate_probe_group(child_id);
                let _ = child.kill();
                let _ = child.wait();
                let _ = reader.join();
                return Err(unavailable_error(SandboxReason::ExternalRunnerProtocolRejected));
            }
            std::thread::sleep(POLL_INTERVAL);
        };
        terminate_probe_group(child_id);
        let bytes = reader
            .join()
            .map_err(|_| unavailable_error(SandboxReason::ExternalRunnerProtocolRejected))??;
        if !status.success() || bytes.len() > super::MAX_PROTOCOL_BYTES {
            return Err(unavailable_error(SandboxReason::ExternalRunnerProtocolRejected));
        }
        Ok(bytes)
    }

    fn terminate_probe_group(child_id: u32) {
        let Ok(child_id) = i32::try_from(child_id) else {
            return;
        };
        if child_id > 1 {
            let _ = unsafe { libc::kill(-child_id, libc::SIGKILL) };
        }
    }

    fn inherited_target_fd(program: &Path) -> io::Result<RawFd> {
        let parent = program.parent().and_then(Path::to_str);
        if !matches!(parent, Some("/proc/self/fd" | "/dev/fd")) {
            return Err(unavailable_error(SandboxReason::ExternalRunnerProtocolRejected));
        }
        let descriptor = program
            .file_name()
            .and_then(OsStr::to_str)
            .and_then(|value| value.parse::<RawFd>().ok())
            .filter(|descriptor| *descriptor >= 3)
            .ok_or_else(|| unavailable_error(SandboxReason::ExternalRunnerProtocolRejected))?;
        let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
        if flags == -1 || flags & libc::FD_CLOEXEC == 0 {
            return Err(unavailable_error(SandboxReason::ExternalRunnerProtocolRejected));
        }
        Ok(descriptor)
    }

    fn clear_cloexec(descriptor: RawFd) -> io::Result<()> {
        let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
        if flags == -1 || unsafe { libc::fcntl(descriptor, libc::F_SETFD, flags & !libc::FD_CLOEXEC) } == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn explicit_environment(command: &Command) -> io::Result<Vec<(OsString, OsString)>> {
        command
            .as_std()
            .get_envs()
            .map(|(key, value)| {
                let value = value.ok_or_else(|| unavailable_error(SandboxReason::ExternalRunnerProtocolRejected))?;
                Ok((key.to_os_string(), value.to_os_string()))
            })
            .collect()
    }

    fn digest_environment(environment: &[(OsString, OsString)]) -> io::Result<String> {
        let mut rows = environment
            .iter()
            .map(|(key, value)| Ok((utf8_os_string(key)?, utf8_os_string(value)?)))
            .collect::<io::Result<Vec<_>>>()?;
        rows.sort();
        let mut hasher = Sha256::new();
        hasher.update(b"solaris.external-runner/environment/v1\0");
        for (key, value) in rows {
            hasher.update((key.len() as u64).to_be_bytes());
            hasher.update(key.as_bytes());
            hasher.update((value.len() as u64).to_be_bytes());
            hasher.update(value.as_bytes());
        }
        Ok(format!("sha256:{:x}", hasher.finalize()))
    }

    fn utf8_paths(paths: &[PathBuf]) -> io::Result<Vec<String>> {
        paths.iter().map(|path| utf8_path(path)).collect()
    }

    fn utf8_path(path: &Path) -> io::Result<String> {
        path.to_str()
            .map(str::to_owned)
            .ok_or_else(|| unavailable_error(SandboxReason::ExternalRunnerProtocolRejected))
    }

    fn utf8_os_string(value: &OsStr) -> io::Result<String> {
        value
            .to_str()
            .map(str::to_owned)
            .ok_or_else(|| unavailable_error(SandboxReason::ExternalRunnerProtocolRejected))
    }

    fn read_limited(path: &Path) -> io::Result<Option<Vec<u8>>> {
        let mut file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let mut bytes = Vec::new();
        file.by_ref()
            .take((super::MAX_PROTOCOL_BYTES + 1) as u64)
            .read_to_end(&mut bytes)?;
        if bytes.len() > super::MAX_PROTOCOL_BYTES {
            return Err(io::Error::other("external runner start response exceeded its limit"));
        }
        match serde_json::from_slice::<super::StartResponse>(&bytes) {
            Ok(_) => Ok(Some(bytes)),
            Err(error) if error.is_eof() => Ok(None),
            Err(error) => Err(io::Error::new(io::ErrorKind::InvalidData, error)),
        }
    }
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
pub(super) use platform::ExternalSandbox;

#[cfg(test)]
#[path = "external_test.rs"]
mod external_test;
