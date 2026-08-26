use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::identity::{EffectId, OperationId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectClass {
    ReadOnly,
    AgentLifecycle,
    MeshStateMutation,
    WorkspaceMutation,
    Process,
    Network,
    ExternalSideEffect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectReplayPolicy {
    Never,
    Idempotent,
    ReplaySafe,
    ReconcileRequired,
    Compensatable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurabilityClass {
    SyncCritical,
    AsyncDurable,
    Ephemeral,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceFootprint {
    #[serde(default)]
    pub unrestricted_file_reads: bool,
    #[serde(default)]
    pub unrestricted_file_writes: bool,
    #[serde(default)]
    pub unrestricted_network: bool,
    #[serde(default)]
    pub unrestricted_process: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub file_reads: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub file_writes: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub network_domains: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub process_commands: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub process_invocations: Vec<ProcessInvocation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub external_resources: Vec<String>,
    /// Solaris Mesh-owned runtime resources. These remain inside the Kernel
    /// boundary and are governed by the AgentLifecycle/MeshState ceiling.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mesh_resources: Vec<String>,
}

impl ResourceFootprint {
    /// Mark a process whose transitive file, network, and child-process access
    /// cannot be proven from its argv. Host approval must cover this complete
    /// footprint instead of treating cwd or the first executable as a sandbox.
    pub fn declare_uncontained_process_access(&mut self) {
        self.unrestricted_file_reads = true;
        self.unrestricted_file_writes = true;
        self.unrestricted_network = true;
        self.unrestricted_process = true;
    }

    /// Marks the transitive file and child-process footprint of a process that
    /// will run in the strict Auto sandbox. Network access remains denied
    /// unless exact destinations are separately present in `network_domains`.
    pub fn declare_sandboxed_process_access(&mut self) {
        self.unrestricted_file_reads = true;
        self.unrestricted_file_writes = true;
        self.unrestricted_network = false;
        self.unrestricted_process = true;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProcessInvocation {
    pub executable: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub argv: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectDescriptor {
    pub class: EffectClass,
    pub action: String,
    #[serde(default)]
    pub resources: ResourceFootprint,
    pub replay_policy: EffectReplayPolicy,
}

impl EffectDescriptor {
    pub fn read_only(action: impl Into<String>) -> Self {
        Self {
            class: EffectClass::ReadOnly,
            action: action.into(),
            resources: ResourceFootprint::default(),
            replay_policy: EffectReplayPolicy::ReplaySafe,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectRequest {
    pub effect_id: EffectId,
    pub operation_id: OperationId,
    pub capability: String,
    pub descriptor: EffectDescriptor,
    pub effective_input: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_digest: Option<String>,
}

/// Secret-safe representation of an effect descriptor for durable audit and
/// Host-visible records. It contains only fixed categories, counts, and
/// one-way identities; execution must continue to use `EffectDescriptor`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectAuditProjection {
    #[serde(default)]
    pub version: String,
    pub class: EffectClass,
    pub action: EffectActionAudit,
    pub resources: Vec<EffectResourceAudit>,
    pub executables: Vec<ExecutableAuditIdentity>,
    pub replay_policy: EffectReplayPolicy,
    pub descriptor_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectActionAudit {
    pub summary: String,
    pub digest: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectResourceKind {
    FileRead,
    FileWrite,
    NetworkDomain,
    ProcessCommand,
    ProcessInvocation,
    ExternalResource,
    MeshResource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectResourceAudit {
    pub kind: EffectResourceKind,
    pub count: usize,
    pub unrestricted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutableAuditIdentity {
    pub executable_digest: String,
    pub argv_count: usize,
    pub invocation_digest: String,
}

impl EffectAuditProjection {
    pub const VERSION: &'static str = "solaris.effect-audit/v1";

    pub fn from_descriptor(descriptor: &EffectDescriptor) -> Self {
        let canonical_resources = canonical_audit_resources(&descriptor.resources);
        let resources = audit_resources(&canonical_resources);
        let executables = canonical_resources
            .process_invocations
            .iter()
            .map(|invocation| ExecutableAuditIdentity {
                executable_digest: sha256_text("executable", &invocation.executable),
                argv_count: invocation.argv.len(),
                invocation_digest: sha256_process_invocation(invocation),
            })
            .collect::<Vec<_>>();
        let action = EffectActionAudit {
            summary: effect_action_summary(descriptor.class).to_owned(),
            digest: sha256_text("action", &descriptor.action),
        };
        let descriptor_digest = sha256_projection_identity(
            descriptor.class,
            &action,
            &resources,
            &executables,
            descriptor.replay_policy,
        );
        Self {
            version: Self::VERSION.to_owned(),
            class: descriptor.class,
            action,
            resources,
            executables,
            replay_policy: descriptor.replay_policy,
            descriptor_digest,
        }
    }

    pub fn has_supported_version(&self) -> bool {
        self.version == Self::VERSION
    }

    pub fn is_secret_safe(&self) -> bool {
        self.has_supported_version()
            && self.action.summary == effect_action_summary(self.class)
            && is_sha256_identity(&self.action.digest)
            && is_sha256_identity(&self.descriptor_digest)
            && resources_are_canonical(&self.resources)
            && self.executables.iter().all(|identity| {
                is_sha256_identity(&identity.executable_digest) && is_sha256_identity(&identity.invocation_digest)
            })
            && self.descriptor_digest
                == sha256_projection_identity(
                    self.class,
                    &self.action,
                    &self.resources,
                    &self.executables,
                    self.replay_policy,
                )
    }

    /// Preserve the existing Host wire type while replacing its values with
    /// this audit projection. The returned descriptor is presentation-only.
    pub fn to_host_descriptor(&self) -> EffectDescriptor {
        let mut resources = ResourceFootprint::default();
        for resource in &self.resources {
            let marker = audit_resource_marker(resource);
            match resource.kind {
                EffectResourceKind::FileRead => {
                    resources.unrestricted_file_reads = resource.unrestricted;
                    resources.file_reads.push(marker);
                }
                EffectResourceKind::FileWrite => {
                    resources.unrestricted_file_writes = resource.unrestricted;
                    resources.file_writes.push(marker);
                }
                EffectResourceKind::NetworkDomain => {
                    resources.unrestricted_network = resource.unrestricted;
                    resources.network_domains.push(marker);
                }
                EffectResourceKind::ProcessCommand | EffectResourceKind::ProcessInvocation => {
                    resources.unrestricted_process |= resource.unrestricted;
                    resources.external_resources.push(marker);
                }
                EffectResourceKind::ExternalResource => resources.external_resources.push(marker),
                EffectResourceKind::MeshResource => resources.mesh_resources.push(marker),
            }
        }
        resources.process_invocations = self
            .executables
            .iter()
            .map(|identity| ProcessInvocation {
                executable: identity.executable_digest.clone(),
                argv: Vec::new(),
            })
            .collect();
        EffectDescriptor {
            class: self.class,
            action: format!("{} ({})", self.action.summary, self.action.digest),
            resources,
            replay_policy: self.replay_policy,
        }
    }
}

fn canonical_audit_resources(resources: &ResourceFootprint) -> ResourceFootprint {
    let mut canonical = resources.clone();
    for values in [
        &mut canonical.file_reads,
        &mut canonical.file_writes,
        &mut canonical.network_domains,
        &mut canonical.process_commands,
        &mut canonical.external_resources,
        &mut canonical.mesh_resources,
    ] {
        values.sort();
        values.dedup();
    }
    canonical
        .process_invocations
        .sort_by(|left, right| (&left.executable, &left.argv).cmp(&(&right.executable, &right.argv)));
    canonical.process_invocations.dedup();
    canonical
}

fn audit_resources(resources: &ResourceFootprint) -> Vec<EffectResourceAudit> {
    let definitions = [
        (
            EffectResourceKind::FileRead,
            resources.file_reads.as_slice(),
            resources.unrestricted_file_reads,
        ),
        (
            EffectResourceKind::FileWrite,
            resources.file_writes.as_slice(),
            resources.unrestricted_file_writes,
        ),
        (
            EffectResourceKind::NetworkDomain,
            resources.network_domains.as_slice(),
            resources.unrestricted_network,
        ),
        (
            EffectResourceKind::ProcessCommand,
            resources.process_commands.as_slice(),
            resources.unrestricted_process,
        ),
        (
            EffectResourceKind::ExternalResource,
            resources.external_resources.as_slice(),
            false,
        ),
        (
            EffectResourceKind::MeshResource,
            resources.mesh_resources.as_slice(),
            false,
        ),
    ];
    let mut audit = definitions
        .into_iter()
        .filter(|(_, values, unrestricted)| *unrestricted || !values.is_empty())
        .map(|(kind, values, unrestricted)| EffectResourceAudit {
            kind,
            count: values.len(),
            unrestricted,
            digest: (!values.is_empty()).then(|| sha256_strings(resource_digest_domain(kind), values)),
        })
        .collect::<Vec<_>>();
    if resources.unrestricted_process || !resources.process_invocations.is_empty() {
        audit.push(EffectResourceAudit {
            kind: EffectResourceKind::ProcessInvocation,
            count: resources.process_invocations.len(),
            unrestricted: resources.unrestricted_process,
            digest: (!resources.process_invocations.is_empty())
                .then(|| sha256_process_invocations(&resources.process_invocations)),
        });
    }
    audit.sort_by_key(|resource| resource_kind_rank(resource.kind));
    audit
}

fn effect_action_summary(class: EffectClass) -> &'static str {
    match class {
        EffectClass::ReadOnly => "read-only effect",
        EffectClass::AgentLifecycle => "agent lifecycle effect",
        EffectClass::MeshStateMutation => "mesh state mutation",
        EffectClass::WorkspaceMutation => "workspace mutation",
        EffectClass::Process => "process effect",
        EffectClass::Network => "network effect",
        EffectClass::ExternalSideEffect => "external side effect",
    }
}

fn is_sha256_identity(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|digest| {
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

fn resources_are_canonical(resources: &[EffectResourceAudit]) -> bool {
    resources.iter().all(|resource| {
        resource.digest.as_deref().is_none_or(is_sha256_identity)
            && ((resource.count == 0) == resource.digest.is_none())
    }) && resources
        .windows(2)
        .all(|pair| resource_kind_rank(pair[0].kind) < resource_kind_rank(pair[1].kind))
}

fn sha256_text(domain: &str, value: &str) -> String {
    let mut hasher = audit_hasher(domain);
    update_length_prefixed(&mut hasher, value.as_bytes());
    format!("sha256:{:x}", hasher.finalize())
}

fn audit_hasher(domain: &str) -> Sha256 {
    let mut hasher = Sha256::new();
    hasher.update(EffectAuditProjection::VERSION.as_bytes());
    hasher.update([0]);
    hasher.update(domain.as_bytes());
    hasher.update([0]);
    hasher
}

fn update_length_prefixed(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn sha256_strings(domain: &str, values: &[String]) -> String {
    let mut hasher = audit_hasher(domain);
    hasher.update((values.len() as u64).to_be_bytes());
    for value in values {
        update_length_prefixed(&mut hasher, value.as_bytes());
    }
    format!("sha256:{:x}", hasher.finalize())
}

fn update_process_invocation(hasher: &mut Sha256, invocation: &ProcessInvocation) {
    update_length_prefixed(hasher, invocation.executable.as_bytes());
    hasher.update((invocation.argv.len() as u64).to_be_bytes());
    for argument in &invocation.argv {
        update_length_prefixed(hasher, argument.as_bytes());
    }
}

fn sha256_process_invocation(invocation: &ProcessInvocation) -> String {
    let mut hasher = audit_hasher("process_invocation");
    update_process_invocation(&mut hasher, invocation);
    format!("sha256:{:x}", hasher.finalize())
}

fn sha256_process_invocations(invocations: &[ProcessInvocation]) -> String {
    let mut hasher = audit_hasher("process_invocations");
    hasher.update((invocations.len() as u64).to_be_bytes());
    for invocation in invocations {
        update_process_invocation(&mut hasher, invocation);
    }
    format!("sha256:{:x}", hasher.finalize())
}

fn sha256_projection_identity(
    class: EffectClass,
    action: &EffectActionAudit,
    resources: &[EffectResourceAudit],
    executables: &[ExecutableAuditIdentity],
    replay_policy: EffectReplayPolicy,
) -> String {
    let mut hasher = audit_hasher("descriptor");
    update_length_prefixed(&mut hasher, effect_class_label(class));
    update_length_prefixed(&mut hasher, action.digest.as_bytes());
    hasher.update((resources.len() as u64).to_be_bytes());
    for resource in resources {
        update_length_prefixed(&mut hasher, resource_kind_label(resource.kind));
        hasher.update((resource.count as u64).to_be_bytes());
        hasher.update([u8::from(resource.unrestricted)]);
        update_length_prefixed(&mut hasher, resource.digest.as_deref().unwrap_or("").as_bytes());
    }
    hasher.update((executables.len() as u64).to_be_bytes());
    for executable in executables {
        update_length_prefixed(&mut hasher, executable.executable_digest.as_bytes());
        hasher.update((executable.argv_count as u64).to_be_bytes());
        update_length_prefixed(&mut hasher, executable.invocation_digest.as_bytes());
    }
    update_length_prefixed(&mut hasher, replay_policy_label(replay_policy));
    format!("sha256:{:x}", hasher.finalize())
}

fn resource_digest_domain(kind: EffectResourceKind) -> &'static str {
    match kind {
        EffectResourceKind::FileRead => "resource/file_read",
        EffectResourceKind::FileWrite => "resource/file_write",
        EffectResourceKind::NetworkDomain => "resource/network_domain",
        EffectResourceKind::ProcessCommand => "resource/process_command",
        EffectResourceKind::ProcessInvocation => "resource/process_invocation",
        EffectResourceKind::ExternalResource => "resource/external_resource",
        EffectResourceKind::MeshResource => "resource/mesh_resource",
    }
}

fn resource_kind_rank(kind: EffectResourceKind) -> u8 {
    match kind {
        EffectResourceKind::FileRead => 0,
        EffectResourceKind::FileWrite => 1,
        EffectResourceKind::NetworkDomain => 2,
        EffectResourceKind::ProcessCommand => 3,
        EffectResourceKind::ProcessInvocation => 4,
        EffectResourceKind::ExternalResource => 5,
        EffectResourceKind::MeshResource => 6,
    }
}

fn effect_class_label(class: EffectClass) -> &'static [u8] {
    match class {
        EffectClass::ReadOnly => b"read_only",
        EffectClass::AgentLifecycle => b"agent_lifecycle",
        EffectClass::MeshStateMutation => b"mesh_state_mutation",
        EffectClass::WorkspaceMutation => b"workspace_mutation",
        EffectClass::Process => b"process",
        EffectClass::Network => b"network",
        EffectClass::ExternalSideEffect => b"external_side_effect",
    }
}

fn replay_policy_label(policy: EffectReplayPolicy) -> &'static [u8] {
    match policy {
        EffectReplayPolicy::Never => b"never",
        EffectReplayPolicy::Idempotent => b"idempotent",
        EffectReplayPolicy::ReplaySafe => b"replay_safe",
        EffectReplayPolicy::ReconcileRequired => b"reconcile_required",
        EffectReplayPolicy::Compensatable => b"compensatable",
    }
}

fn resource_kind_label(kind: EffectResourceKind) -> &'static [u8] {
    match kind {
        EffectResourceKind::FileRead => b"file_read",
        EffectResourceKind::FileWrite => b"file_write",
        EffectResourceKind::NetworkDomain => b"network_domain",
        EffectResourceKind::ProcessCommand => b"process_command",
        EffectResourceKind::ProcessInvocation => b"process_invocation",
        EffectResourceKind::ExternalResource => b"external_resource",
        EffectResourceKind::MeshResource => b"mesh_resource",
    }
}

fn audit_resource_marker(resource: &EffectResourceAudit) -> String {
    format!(
        "audit:{}:count={}:unrestricted={}:{}",
        String::from_utf8_lossy(resource_kind_label(resource.kind)),
        resource.count,
        resource.unrestricted,
        resource.digest.as_deref().unwrap_or("digest=none")
    )
}

#[cfg(test)]
#[path = "effect_test.rs"]
mod effect_test;
