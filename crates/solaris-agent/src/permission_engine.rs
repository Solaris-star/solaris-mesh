use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use solaris_config::file_identity::OpenedFileIdentity;
use solaris_tools::write::WorkspaceSearchPolicy;
use solaris_types::effect::{EffectClass, EffectDescriptor, EffectRequest};
use solaris_types::identity::RunId;
use solaris_types::permission::{
    AdditionalPermissions, CapabilityLease, ExecutionBoundary, LeaseScope, PermissionCeiling, PermissionDecision,
    PermissionMode, PermissionRule,
};

#[path = "permission_engine/boundary_identity.rs"]
mod boundary_identity;
#[path = "permission_engine/evaluation.rs"]
mod evaluation;
#[path = "permission_engine/process_authorization.rs"]
mod process_authorization;
#[path = "permission_engine/protected_paths.rs"]
mod protected_paths;
#[path = "permission_engine/workspace_policy.rs"]
mod workspace_policy;

use boundary_identity::BoundaryIdentityPolicy;
use evaluation::{boundary_allows, lease_matches, preset_default, replace_runtime_rules, rule_matches};
use protected_paths::{ProtectedPathPolicy, path_within, permission_path_matches};
use workspace_policy::PermissionWorkspaceSearchPolicy;

#[derive(Debug, Clone)]
struct LeaseEntry {
    lease: CapabilityLease,
    uses: u32,
}

#[derive(Default)]
struct CapabilityLeaseStore {
    leases: Mutex<Vec<LeaseEntry>>,
}

impl CapabilityLeaseStore {
    fn issue(&self, lease: CapabilityLease) {
        self.restore(lease, 0);
    }

    fn restore(&self, lease: CapabilityLease, uses: u32) {
        let mut leases = self.leases.lock().unwrap_or_else(|error| error.into_inner());
        if !lease.lease_id.is_empty() {
            leases.retain(|entry| entry.lease.lease_id != lease.lease_id);
        }
        leases.push(LeaseEntry { lease, uses });
    }

    fn snapshot(&self) -> Vec<CapabilityLease> {
        self.leases
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .map(|entry| entry.lease.clone())
            .collect()
    }

    fn entries(&self) -> Vec<LeaseEntry> {
        self.leases.lock().unwrap_or_else(|error| error.into_inner()).clone()
    }

    fn replace_entries(&self, entries: Vec<LeaseEntry>) {
        *self.leases.lock().unwrap_or_else(|error| error.into_inner()) = entries;
    }

    fn covers(&self, run_id: &RunId, request: &EffectRequest) -> bool {
        let now = chrono::Utc::now().timestamp_millis();
        self.leases
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .any(|entry| lease_matches(entry, run_id, request, now))
    }

    fn consume_with<E>(
        &self,
        run_id: &RunId,
        request: &EffectRequest,
        persist: impl FnOnce(&str, u32) -> Result<(), E>,
    ) -> Result<bool, E> {
        let now = chrono::Utc::now().timestamp_millis();
        let mut leases = self.leases.lock().unwrap_or_else(|error| error.into_inner());
        let Some(entry) = leases
            .iter_mut()
            .find(|entry| lease_matches(entry, run_id, request, now))
        else {
            return Ok(false);
        };
        let next_use = entry.uses.saturating_add(1);
        persist(&entry.lease.lease_id, next_use)?;
        entry.uses = next_use;
        Ok(true)
    }
}

pub trait PermissionReviewer: Send + Sync {
    fn review(&self, request: &EffectRequest) -> PermissionDecision;
}

#[derive(Default)]
pub struct DeterministicAutoReviewer;

impl PermissionReviewer for DeterministicAutoReviewer {
    fn review(&self, request: &EffectRequest) -> PermissionDecision {
        match request.descriptor.class {
            EffectClass::Process => {
                if request
                    .descriptor
                    .resources
                    .process_commands
                    .iter()
                    .all(|command| is_low_risk_introspection_command(command))
                    && !request.descriptor.resources.process_commands.is_empty()
                {
                    PermissionDecision::Allow
                } else {
                    PermissionDecision::Ask
                }
            }
            EffectClass::Network => PermissionDecision::Ask,
            _ => PermissionDecision::Ask,
        }
    }
}

fn is_low_risk_introspection_command(command: &str) -> bool {
    let command = command.trim().to_ascii_lowercase();
    if command.is_empty()
        || command
            .chars()
            .any(|ch| matches!(ch, '&' | '|' | ';' | '>' | '<' | '`' | '\n' | '\r'))
        || command.contains("$(")
    {
        return false;
    }
    command == "pwd"
        || command == "git status"
        || command.starts_with("git status ")
        || command == "git log"
        || command.starts_with("git log ")
        || command == "git rev-parse --show-toplevel"
        || command == "git branch --show-current"
}

#[derive(Clone)]
pub struct PermissionContext {
    policy: Arc<RwLock<PermissionPolicyState>>,
    ceiling: Arc<RwLock<PermissionCeiling>>,
    leases: Arc<CapabilityLeaseStore>,
    boundary: Arc<RwLock<ExecutionBoundary>>,
    boundary_ceilings: Vec<Arc<ExecutionBoundary>>,
    boundary_identities: Arc<RwLock<BoundaryIdentityPolicy>>,
    boundary_identity_ceilings: Vec<Arc<RwLock<BoundaryIdentityPolicy>>>,
    configured_effects: Arc<RwLock<BTreeMap<(String, String), ConfiguredEffectGrant>>>,
    reviewer: Arc<RwLock<Arc<dyn PermissionReviewer>>>,
    protected_paths: Arc<RwLock<ProtectedPathPolicy>>,
    spawn_gate: Arc<RwLock<()>>,
}

#[derive(Clone)]
struct ConfiguredEffectGrant {
    capability: String,
    descriptor: EffectDescriptor,
}

#[derive(Clone)]
struct PermissionPolicyState {
    mode: PermissionMode,
    rules: Vec<PermissionRule>,
    generated_rules: BTreeMap<String, Vec<PermissionRule>>,
}

impl PermissionContext {
    pub fn new(mode: PermissionMode, ceiling: PermissionCeiling) -> Self {
        let mut generated_rules = BTreeMap::new();
        replace_runtime_rules(&mut generated_rules);
        Self {
            policy: Arc::new(RwLock::new(PermissionPolicyState {
                mode,
                rules: Vec::new(),
                generated_rules,
            })),
            ceiling: Arc::new(RwLock::new(ceiling)),
            leases: Arc::new(CapabilityLeaseStore::default()),
            boundary: Arc::new(RwLock::new(ExecutionBoundary::unrestricted())),
            boundary_ceilings: Vec::new(),
            boundary_identities: Arc::new(RwLock::new(BoundaryIdentityPolicy::default())),
            boundary_identity_ceilings: Vec::new(),
            configured_effects: Arc::new(RwLock::new(BTreeMap::new())),
            reviewer: Arc::new(RwLock::new(Arc::new(DeterministicAutoReviewer))),
            protected_paths: Arc::new(RwLock::new(ProtectedPathPolicy::default())),
            spawn_gate: Arc::new(RwLock::new(())),
        }
    }

    pub fn from_auto_approve(_auto_approve: bool) -> Self {
        Self::new(PermissionMode::Auto, PermissionCeiling::unrestricted())
    }

    pub fn mode(&self) -> PermissionMode {
        self.policy.read().map(|policy| policy.mode).unwrap_or_default()
    }

    pub fn set_mode(&self, mode: PermissionMode) {
        let _spawn_gate = self.spawn_gate.write().unwrap_or_else(|error| error.into_inner());
        let mut policy = self.policy.write().unwrap_or_else(|error| error.into_inner());
        policy.mode = mode;
        replace_runtime_rules(&mut policy.generated_rules);
    }

    pub fn ceiling(&self) -> PermissionCeiling {
        self.ceiling.read().map(|ceiling| *ceiling).unwrap_or_default()
    }

    pub fn set_ceiling(&self, ceiling: PermissionCeiling) {
        let _spawn_gate = self.spawn_gate.write().unwrap_or_else(|error| error.into_inner());
        if let Ok(mut current) = self.ceiling.write() {
            *current = ceiling;
        }
    }

    pub fn add_rule(&self, rule: PermissionRule) {
        let _spawn_gate = self.spawn_gate.write().unwrap_or_else(|error| error.into_inner());
        self.policy
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .rules
            .push(rule);
    }

    pub fn replace_rules(&self, rules: Vec<PermissionRule>) {
        let _spawn_gate = self.spawn_gate.write().unwrap_or_else(|error| error.into_inner());
        self.policy.write().unwrap_or_else(|error| error.into_inner()).rules = rules;
    }

    /// Replace rules owned by one trusted runtime source without touching
    /// explicit user, Host, or parent-Run policy.
    pub fn set_generated_rules(&self, source: impl Into<String>, rules: Vec<PermissionRule>) {
        let _spawn_gate = self.spawn_gate.write().unwrap_or_else(|error| error.into_inner());
        self.policy
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .generated_rules
            .insert(source.into(), rules);
    }

    pub fn remove_generated_rules(&self, source: &str) {
        let _spawn_gate = self.spawn_gate.write().unwrap_or_else(|error| error.into_inner());
        self.policy
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .generated_rules
            .remove(source);
    }

    pub fn rules(&self) -> Vec<PermissionRule> {
        let policy = self.policy.read().unwrap_or_else(|error| error.into_inner());
        policy
            .rules
            .iter()
            .chain(policy.generated_rules.values().flatten())
            .cloned()
            .collect()
    }

    pub fn issue_lease(&self, lease: CapabilityLease) {
        let _spawn_gate = self.spawn_gate.write().unwrap_or_else(|error| error.into_inner());
        self.leases.issue(lease);
    }

    pub fn restore_lease(&self, lease: CapabilityLease, uses: u32) {
        let _spawn_gate = self.spawn_gate.write().unwrap_or_else(|error| error.into_inner());
        self.leases.restore(lease, uses);
    }

    pub fn leases(&self) -> Vec<CapabilityLease> {
        self.leases.snapshot()
    }

    pub fn set_boundary(&self, boundary: ExecutionBoundary) {
        let _spawn_gate = self.spawn_gate.write().unwrap_or_else(|error| error.into_inner());
        let identities = BoundaryIdentityPolicy::capture(&boundary);
        *self.boundary.write().unwrap_or_else(|error| error.into_inner()) = boundary;
        *self
            .boundary_identities
            .write()
            .unwrap_or_else(|error| error.into_inner()) = identities;
    }

    pub fn boundary(&self) -> ExecutionBoundary {
        self.boundary.read().unwrap_or_else(|error| error.into_inner()).clone()
    }

    /// Extend the hard execution boundary with resources explicitly supplied
    /// by trusted configuration or a Host command for one capability.
    pub fn allow_configured_effect_for(
        &self,
        source: impl Into<String>,
        capability: impl Into<String>,
        descriptor: &EffectDescriptor,
    ) {
        let _spawn_gate = self.spawn_gate.write().unwrap_or_else(|error| error.into_inner());
        let source = source.into();
        let capability = capability.into();
        self.configured_effects
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .insert(
                (source, capability.clone()),
                ConfiguredEffectGrant {
                    capability,
                    descriptor: descriptor.clone(),
                },
            );
    }

    pub fn revoke_configured_effect_from(&self, source: &str) {
        let _spawn_gate = self.spawn_gate.write().unwrap_or_else(|error| error.into_inner());
        self.configured_effects
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .retain(|(grant_source, _), _| grant_source != source);
    }

    pub fn has_configured_effect_source(&self, source: &str) -> bool {
        self.configured_effects
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .keys()
            .any(|(grant_source, _)| grant_source == source)
    }

    pub(crate) fn register_protected_paths(
        &self,
        runtime_root: impl AsRef<Path>,
        state_paths: Vec<PathBuf>,
    ) -> Result<(), String> {
        let _spawn_gate = self.spawn_gate.write().unwrap_or_else(|error| error.into_inner());
        self.protected_paths
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .register(runtime_root, state_paths)
    }

    pub(crate) fn protected_path_fingerprint_material(&self) -> Vec<String> {
        self.protected_paths
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .fingerprint_material()
    }

    #[cfg(test)]
    pub(crate) fn protected_paths_snapshot(&self) -> Vec<PathBuf> {
        self.protected_paths
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .snapshot()
    }

    pub(crate) fn protected_process_snapshot(
        &self,
    ) -> Result<(Vec<PathBuf>, Vec<solaris_process::ProtectedObjectIdentity>), String> {
        self.protected_paths
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .process_snapshot()
    }

    pub(crate) fn workspace_search_policy(&self) -> Arc<dyn WorkspaceSearchPolicy> {
        Arc::new(PermissionWorkspaceSearchPolicy::new(self.clone()))
    }

    fn is_protected_path(&self, path: &Path) -> bool {
        self.protected_paths
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .protects(path)
    }

    fn is_protected_traversed_file(&self, path: &Path) -> bool {
        self.protected_paths
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .protects_traversed_file(path)
    }

    fn is_protected_opened_directory(&self, path: &Path, identity: &OpenedFileIdentity) -> bool {
        self.protected_paths
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .protects_opened_directory(path, identity)
    }

    fn is_protected_file_slot(&self, path: &Path, parent_identity: &OpenedFileIdentity, file_name: &OsStr) -> bool {
        self.protected_paths
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .protects_file_slot(path, parent_identity, file_name)
    }

    fn is_protected_opened_file(
        &self,
        path: &Path,
        parent_identity: &OpenedFileIdentity,
        file_name: &OsStr,
        identity: Arc<OpenedFileIdentity>,
    ) -> bool {
        let _spawn_gate = self.spawn_gate.write().unwrap_or_else(|error| error.into_inner());
        self.protected_paths
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .protects_opened_file(path, parent_identity, file_name, identity)
    }

    pub fn evaluate_effect(&self, run_id: &RunId, capability: &str, request: &EffectRequest) -> PermissionEvaluation {
        self.evaluate_effect_with(run_id, capability, request, self.mode(), self.ceiling())
    }

    pub fn evaluate_effect_with(
        &self,
        run_id: &RunId,
        capability: &str,
        request: &EffectRequest,
        mode: PermissionMode,
        ceiling: PermissionCeiling,
    ) -> PermissionEvaluation {
        self.evaluate_effect_with_reviewer_policy(run_id, capability, request, mode, ceiling, true)
    }

    fn evaluate_effect_with_reviewer_policy(
        &self,
        run_id: &RunId,
        capability: &str,
        request: &EffectRequest,
        mode: PermissionMode,
        ceiling: PermissionCeiling,
        invoke_auto_reviewer: bool,
    ) -> PermissionEvaluation {
        if capability != request.capability {
            return PermissionEvaluation {
                decision: PermissionDecision::Deny,
                matched_rule_index: None,
                matched_lease: false,
                reason: "effect capability does not match evaluation capability".to_owned(),
            };
        }
        let capability = request.capability.as_str();
        if mode == PermissionMode::Bypass {
            let engine = PermissionEngine::with_ceilings(
                PermissionMode::Bypass,
                PermissionCeiling::unrestricted(),
                PermissionCeiling::unrestricted(),
                PermissionCeiling::unrestricted(),
            );
            engine.replace_rules(self.rules());
            return engine.evaluate(capability, &request.descriptor);
        }
        if !self.boundary_identities_valid() {
            return PermissionEvaluation {
                decision: PermissionDecision::Deny,
                matched_rule_index: None,
                matched_lease: false,
                reason: "execution boundary root identity changed".to_owned(),
            };
        }
        let mut effective_ceiling = self.ceiling().intersect(ceiling);
        if mode == PermissionMode::Plan {
            effective_ceiling = effective_ceiling.intersect(PermissionCeiling::plan());
        }
        let engine = PermissionEngine::with_ceilings(
            mode,
            PermissionCeiling::unrestricted(),
            effective_ceiling,
            PermissionCeiling::unrestricted(),
        );
        engine.replace_rules(self.rules());
        let mut evaluation = engine.evaluate(capability, &request.descriptor);
        if evaluation.decision == PermissionDecision::Deny {
            return evaluation;
        }
        if mode != PermissionMode::Bypass
            && self
                .protected_paths
                .read()
                .unwrap_or_else(|error| error.into_inner())
                .protects_descriptor(&request.descriptor)
        {
            return PermissionEvaluation {
                decision: PermissionDecision::Deny,
                matched_rule_index: evaluation.matched_rule_index,
                matched_lease: false,
                reason: "effect targets protected runtime state".to_owned(),
            };
        }
        if self
            .boundary_ceilings
            .iter()
            .any(|ceiling| !boundary_allows(ceiling, &request.descriptor))
        {
            evaluation.decision = PermissionDecision::Deny;
            evaluation.matched_lease = false;
            evaluation.reason = "effect exceeds child execution boundary".to_owned();
            return evaluation;
        }
        if !boundary_allows(&self.boundary_for_capability(capability), &request.descriptor) {
            if self.leases.covers(run_id, request) {
                evaluation.decision = PermissionDecision::Allow;
                evaluation.matched_lease = true;
                evaluation.reason = "matched capability lease for execution boundary".to_owned();
            } else if mode == PermissionMode::Auto && effective_ceiling.allows(request.descriptor.class) {
                evaluation.decision = PermissionDecision::Ask;
                evaluation.reason = "effect requires a capability lease for execution boundary".to_owned();
            } else {
                evaluation.decision = PermissionDecision::Deny;
                evaluation.reason = "effect exceeds execution boundary".to_owned();
            }
            return evaluation;
        }
        if matches!(
            evaluation.decision,
            PermissionDecision::Ask | PermissionDecision::AutoReview
        ) && self.leases.covers(run_id, request)
        {
            evaluation.decision = PermissionDecision::Allow;
            evaluation.matched_lease = true;
            evaluation.reason = "matched capability lease".to_owned();
        } else if evaluation.decision == PermissionDecision::AutoReview && invoke_auto_reviewer {
            let reviewed = self
                .reviewer
                .read()
                .unwrap_or_else(|error| error.into_inner())
                .review(request);
            evaluation.decision = match reviewed {
                PermissionDecision::Allow => PermissionDecision::Allow,
                PermissionDecision::Deny => PermissionDecision::Deny,
                PermissionDecision::Ask | PermissionDecision::AutoReview => PermissionDecision::Ask,
            };
            evaluation.reason = "deterministic auto reviewer".to_owned();
        }
        evaluation
    }

    pub fn consume_lease_with<E>(
        &self,
        run_id: &RunId,
        request: &EffectRequest,
        persist: impl FnOnce(&str, u32) -> Result<(), E>,
    ) -> Result<bool, E> {
        self.leases.consume_with(run_id, request, persist)
    }

    pub fn set_reviewer(&self, reviewer: Arc<dyn PermissionReviewer>) {
        let _spawn_gate = self.spawn_gate.write().unwrap_or_else(|error| error.into_inner());
        *self.reviewer.write().unwrap_or_else(|error| error.into_inner()) = reviewer;
    }

    pub fn narrowed(&self, requested: PermissionCeiling) -> Self {
        Self {
            policy: Arc::clone(&self.policy),
            ceiling: Arc::new(RwLock::new(self.ceiling().intersect(requested))),
            leases: Arc::clone(&self.leases),
            boundary: Arc::clone(&self.boundary),
            boundary_ceilings: self.boundary_ceilings.clone(),
            boundary_identities: Arc::clone(&self.boundary_identities),
            boundary_identity_ceilings: self.boundary_identity_ceilings.clone(),
            configured_effects: Arc::clone(&self.configured_effects),
            reviewer: Arc::clone(&self.reviewer),
            protected_paths: Arc::clone(&self.protected_paths),
            spawn_gate: Arc::clone(&self.spawn_gate),
        }
    }

    pub(crate) fn narrowed_with_boundary(&self, requested: PermissionCeiling, boundary: ExecutionBoundary) -> Self {
        let mut boundary_ceilings = self.boundary_ceilings.clone();
        boundary_ceilings.push(Arc::new(boundary.clone()));
        let mut boundary_identity_ceilings = self.boundary_identity_ceilings.clone();
        boundary_identity_ceilings.push(Arc::clone(&self.boundary_identities));
        let boundary_identities = BoundaryIdentityPolicy::capture(&boundary);
        Self {
            policy: Arc::clone(&self.policy),
            ceiling: Arc::new(RwLock::new(self.ceiling().intersect(requested))),
            leases: Arc::clone(&self.leases),
            boundary: Arc::new(RwLock::new(boundary)),
            boundary_ceilings,
            boundary_identities: Arc::new(RwLock::new(boundary_identities)),
            boundary_identity_ceilings,
            configured_effects: Arc::clone(&self.configured_effects),
            reviewer: Arc::clone(&self.reviewer),
            protected_paths: Arc::clone(&self.protected_paths),
            spawn_gate: Arc::clone(&self.spawn_gate),
        }
    }

    fn boundary_for_capability(&self, capability: &str) -> ExecutionBoundary {
        let mut boundary = self.boundary();
        for grant in self
            .configured_effects
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .filter(|grant| grant.capability == capability)
        {
            extend_boundary(&mut boundary, &grant.descriptor);
        }
        boundary
    }

    pub(crate) fn current_boundary_allows_request(&self, request: &EffectRequest) -> bool {
        self.boundary_identities_valid() && self.boundary_allows_request(&request.capability, &request.descriptor)
    }

    fn boundary_identities_valid(&self) -> bool {
        self.boundary_identities
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .validates_current_paths()
            && self.boundary_identity_ceilings.iter().all(|identities| {
                identities
                    .read()
                    .unwrap_or_else(|error| error.into_inner())
                    .validates_current_paths()
            })
    }

    fn boundary_requires_opened_identity(&self) -> bool {
        !self
            .boundary_identities
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .is_empty()
            || self
                .boundary_identity_ceilings
                .iter()
                .any(|identities| !identities.read().unwrap_or_else(|error| error.into_inner()).is_empty())
    }

    fn boundary_allows_request(&self, capability: &str, descriptor: &EffectDescriptor) -> bool {
        boundary_allows(&self.boundary_for_capability(capability), descriptor)
            && self
                .boundary_ceilings
                .iter()
                .all(|ceiling| boundary_allows(ceiling, descriptor))
    }
}

pub(crate) fn boundary_path_within(resource: &str, root: &str) -> bool {
    path_within(resource, root)
}

fn extend_boundary(boundary: &mut ExecutionBoundary, descriptor: &EffectDescriptor) {
    boundary.unrestricted_file_reads |= descriptor.resources.unrestricted_file_reads;
    boundary.unrestricted_file_writes |= descriptor.resources.unrestricted_file_writes;
    boundary.unrestricted_network |= descriptor.resources.unrestricted_network;
    boundary.unrestricted_process |= descriptor.resources.unrestricted_process;
    for path in &descriptor.resources.file_reads {
        if !boundary.readable_roots.contains(path) {
            boundary.readable_roots.push(path.clone());
        }
    }
    for path in &descriptor.resources.file_writes {
        if !boundary.writable_roots.contains(path) {
            boundary.writable_roots.push(path.clone());
        }
    }
    for domain in &descriptor.resources.network_domains {
        if !boundary.network_domains.contains(domain) {
            boundary.network_domains.push(domain.clone());
        }
    }
    for command in &descriptor.resources.process_commands {
        if !boundary.process_command_prefixes.contains(command) {
            boundary.process_command_prefixes.push(command.clone());
        }
    }
    for invocation in &descriptor.resources.process_invocations {
        if !boundary.process_invocations.contains(invocation) {
            boundary.process_invocations.push(invocation.clone());
        }
    }
    for resource in &descriptor.resources.external_resources {
        if !boundary.external_resource_prefixes.contains(resource) {
            boundary.external_resource_prefixes.push(resource.clone());
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionEvaluation {
    pub decision: PermissionDecision,
    pub matched_rule_index: Option<usize>,
    pub matched_lease: bool,
    pub reason: String,
}

struct PermissionEngine {
    mode: RwLock<PermissionMode>,
    hard_ceiling: PermissionCeiling,
    scope_ceiling: PermissionCeiling,
    role_ceiling: PermissionCeiling,
    rules: RwLock<Vec<PermissionRule>>,
}

impl PermissionEngine {
    #[cfg(test)]
    fn new(mode: PermissionMode) -> Self {
        Self {
            mode: RwLock::new(mode),
            hard_ceiling: PermissionCeiling::unrestricted(),
            scope_ceiling: PermissionCeiling::unrestricted(),
            role_ceiling: PermissionCeiling::unrestricted(),
            rules: RwLock::new(Vec::new()),
        }
    }

    fn with_ceilings(
        mode: PermissionMode,
        hard_ceiling: PermissionCeiling,
        scope_ceiling: PermissionCeiling,
        role_ceiling: PermissionCeiling,
    ) -> Self {
        Self {
            mode: RwLock::new(mode),
            hard_ceiling,
            scope_ceiling,
            role_ceiling,
            rules: RwLock::new(Vec::new()),
        }
    }

    fn mode(&self) -> PermissionMode {
        self.mode.read().map(|mode| *mode).unwrap_or_default()
    }

    #[cfg(test)]
    fn add_rule(&self, rule: PermissionRule) {
        if let Ok(mut rules) = self.rules.write() {
            rules.push(rule);
        }
    }

    fn replace_rules(&self, new_rules: Vec<PermissionRule>) {
        if let Ok(mut rules) = self.rules.write() {
            *rules = new_rules;
        }
    }

    fn effective_ceiling(&self) -> PermissionCeiling {
        self.hard_ceiling
            .intersect(self.scope_ceiling)
            .intersect(self.role_ceiling)
    }

    fn evaluate(&self, capability: &str, descriptor: &EffectDescriptor) -> PermissionEvaluation {
        let mode = self.mode();
        let effective_ceiling = if mode == PermissionMode::Plan {
            self.effective_ceiling().intersect(PermissionCeiling::plan())
        } else {
            self.effective_ceiling()
        };
        if !effective_ceiling.allows(descriptor.class) {
            return PermissionEvaluation {
                decision: PermissionDecision::Deny,
                matched_rule_index: None,
                matched_lease: false,
                reason: "effect exceeds permission ceiling".to_owned(),
            };
        }

        let matched = self.rules.read().ok().and_then(|rules| {
            let mut latest = None;
            let mut latest_deny = None;
            for (index, rule) in rules
                .iter()
                .enumerate()
                .filter(|(_, rule)| rule_matches(rule, capability, descriptor))
            {
                latest = Some((index, rule.decision));
                if rule.decision == PermissionDecision::Deny {
                    latest_deny = Some((index, rule.decision));
                }
            }
            latest_deny.or(latest)
        });

        let (matched_rule_index, rule_decision) =
            matched.map_or((None, None), |(index, decision)| (Some(index), Some(decision)));
        let base = rule_decision.unwrap_or_else(|| preset_default(mode, descriptor.class));
        let decision = match (mode, base) {
            (_, PermissionDecision::Deny) => PermissionDecision::Deny,
            (PermissionMode::Bypass, PermissionDecision::Ask | PermissionDecision::AutoReview) => {
                PermissionDecision::Allow
            }
            _ => base,
        };

        PermissionEvaluation {
            decision,
            matched_rule_index,
            matched_lease: false,
            reason: if matched_rule_index.is_some() {
                "matched ordered permission rule".to_owned()
            } else {
                format!("{:?} preset default", mode)
            },
        }
    }
}

#[cfg(test)]
#[path = "permission_engine_test.rs"]
mod permission_engine_test;

#[cfg(test)]
#[path = "permission_engine_auto_approve_test.rs"]
mod permission_engine_auto_approve_test;

#[cfg(test)]
#[path = "permission_engine_bypass_test.rs"]
mod permission_engine_bypass_test;

#[cfg(test)]
#[path = "permission_engine_process_authorization_test.rs"]
mod permission_engine_process_authorization_test;
