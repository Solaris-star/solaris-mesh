use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use sha2::{Digest, Sha256};

use solaris_types::plugin::{
    ImplementationIdentity, PluginContributionKind, PluginDefinition, PluginScope, PluginSource,
    ResolvedPluginDefinition, ResolvedPluginIdentity,
};

use crate::plugin_tool::PluginCommandContribution;

pub const PLUGIN_RUNTIME_API_VERSION: u32 = 1;
const SUPPORTED_PLUGIN_PROTOCOLS: &[&str] = &[
    "json-stream-v1",
    "native-host-v1",
    "plugin-command-v1",
    "provider-command-v1",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PluginTrustDecision {
    Trusted,
    RequiresApproval(String),
    Denied(String),
}

#[derive(Debug, Clone, Default)]
pub struct PluginTrustPolicy {
    pub trusted_local_roots: Vec<PathBuf>,
    pub trusted_git_hosts: HashSet<String>,
    pub trusted_packages: HashSet<String>,
    pub allow_host_bundled: bool,
}

impl PluginTrustPolicy {
    pub fn evaluate(&self, definition: &PluginDefinition) -> PluginTrustDecision {
        match &definition.source {
            PluginSource::HostBundled { .. } if self.allow_host_bundled => PluginTrustDecision::Trusted,
            PluginSource::HostBundled { .. } => {
                PluginTrustDecision::RequiresApproval("host-bundled plugin is not pre-trusted".into())
            }
            PluginSource::Local { path } => {
                let candidate = Path::new(path);
                if self.trusted_local_roots.iter().any(|root| candidate.starts_with(root)) {
                    PluginTrustDecision::Trusted
                } else {
                    PluginTrustDecision::RequiresApproval(format!("local plugin path {path} is outside trusted roots"))
                }
            }
            PluginSource::Git { repository, .. } => {
                let host = repository_host(repository);
                if host
                    .as_deref()
                    .is_some_and(|host| self.trusted_git_hosts.contains(host))
                {
                    PluginTrustDecision::Trusted
                } else {
                    PluginTrustDecision::RequiresApproval(format!("git plugin source {repository} is not pre-trusted"))
                }
            }
            PluginSource::Package { package, .. } => {
                if self.trusted_packages.contains(package) {
                    PluginTrustDecision::Trusted
                } else {
                    PluginTrustDecision::RequiresApproval(format!("package {package} is not pre-trusted"))
                }
            }
        }
    }
}

pub struct PluginResolver {
    workspace_root: PathBuf,
    trust: PluginTrustPolicy,
}

impl PluginResolver {
    pub fn new(workspace_root: impl Into<PathBuf>, trust: PluginTrustPolicy) -> Self {
        Self {
            workspace_root: workspace_root.into(),
            trust,
        }
    }

    pub fn resolve(&self, definition: PluginDefinition) -> Result<ResolvedPluginDefinition, String> {
        validate_plugin_definition(&definition)?;
        if !matches!(definition.source, PluginSource::Local { .. }) {
            match self.trust.evaluate(&definition) {
                PluginTrustDecision::Denied(reason) => return Err(reason),
                PluginTrustDecision::RequiresApproval(reason) => return Err(reason),
                PluginTrustDecision::Trusted => {}
            }
        }

        let (source, authority_root, implementation_id) = match &definition.source {
            PluginSource::Local { path } => {
                let canonical = self.resolve_materialized_root(path)?;
                (
                    PluginSource::Local {
                        path: canonical.to_string_lossy().into_owned(),
                    },
                    Some(canonical.to_string_lossy().into_owned()),
                    format!("local:{}", canonical.display()),
                )
            }
            PluginSource::Git { repository, reference } => {
                let root = definition
                    .materialized_path
                    .as_deref()
                    .map(|path| self.resolve_materialized_root(path))
                    .transpose()?;
                (
                    definition.source.clone(),
                    root.as_ref().map(|path| path.to_string_lossy().into_owned()),
                    format!("git:{repository}@{reference}"),
                )
            }
            PluginSource::Package { package, version } => {
                let root = definition
                    .materialized_path
                    .as_deref()
                    .map(|path| self.resolve_materialized_root(path))
                    .transpose()?;
                (
                    definition.source.clone(),
                    root.as_ref().map(|path| path.to_string_lossy().into_owned()),
                    format!("package:{package}@{version}"),
                )
            }
            PluginSource::HostBundled { id } => (definition.source.clone(), None, format!("host:{id}")),
        };

        let digest = definition_digest(&definition, &source, authority_root.as_deref())?;
        let identity = ResolvedPluginIdentity {
            plugin_id: definition.id.clone(),
            source,
            implementation: ImplementationIdentity {
                implementation_id,
                version: Some(definition.version.clone()),
                digest: Some(digest),
            },
        };
        Ok(ResolvedPluginDefinition {
            definition,
            identity,
            authority_root,
        })
    }
    fn resolve_materialized_root(&self, path: &str) -> Result<PathBuf, String> {
        let requested = PathBuf::from(path);
        let absolute = if requested.is_absolute() {
            requested
        } else {
            self.workspace_root.join(requested)
        };
        let canonical = absolute
            .canonicalize()
            .map_err(|error| format!("failed to canonicalize plugin root {}: {error}", absolute.display()))?;
        let allowed = self
            .trust
            .trusted_local_roots
            .iter()
            .filter_map(|root| root.canonicalize().ok())
            .any(|root| canonical.starts_with(root));
        if !allowed {
            return Err(format!(
                "resolved plugin root {} escapes trusted authority roots",
                canonical.display()
            ));
        }
        Ok(canonical)
    }
}

fn definition_digest(
    definition: &PluginDefinition,
    source: &PluginSource,
    authority_root: Option<&str>,
) -> Result<String, String> {
    let mut hasher = Sha256::new();
    let identity = serde_json::to_vec(&(definition, source, authority_root))
        .map_err(|error| format!("failed to serialize plugin identity: {error}"))?;
    update_digest(&mut hasher, &identity);
    if let Some(root) = authority_root {
        fingerprint_directory(Path::new(root), &mut hasher)?;
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn validate_plugin_definition(definition: &PluginDefinition) -> Result<(), String> {
    if let Some(version) = definition.compatibility.runtime_api_version
        && version != PLUGIN_RUNTIME_API_VERSION
    {
        return Err(format!(
            "plugin {} requires runtime API {version}, but this runtime provides {PLUGIN_RUNTIME_API_VERSION}",
            definition.id
        ));
    }
    for protocol in &definition.compatibility.required_protocols {
        if !SUPPORTED_PLUGIN_PROTOCOLS.contains(&protocol.as_str()) {
            return Err(format!(
                "plugin {} requires unsupported protocol {protocol}",
                definition.id
            ));
        }
    }

    for (kind, name, max_result_size) in definition
        .command_tools
        .iter()
        .map(|tool| ("tool", tool.name.as_str(), tool.max_result_size))
        .chain(
            definition
                .command_contributions
                .iter()
                .map(|contribution| ("contribution", contribution.name.as_str(), contribution.max_result_size)),
        )
    {
        if max_result_size < 2 {
            return Err(format!(
                "plugin {} {kind} {name} max_result_size must be at least 2",
                definition.id
            ));
        }
    }

    let declarations = [
        (PluginContributionKind::Provider, &definition.capabilities.providers),
        (
            PluginContributionKind::CollaborationStrategy,
            &definition.capabilities.collaboration_strategies,
        ),
        (
            PluginContributionKind::StorageBackend,
            &definition.capabilities.storage_backends,
        ),
        (PluginContributionKind::Hook, &definition.capabilities.hooks),
    ];
    for (kind, names) in declarations {
        for name in names {
            let count = definition
                .command_contributions
                .iter()
                .filter(|contribution| contribution.kind == kind && contribution.name == *name)
                .count();
            if count != 1 {
                return Err(format!(
                    "plugin {} capability {}:{name} requires exactly one callable implementation",
                    definition.id,
                    kind.capability_prefix()
                ));
            }
        }
    }
    for contribution in &definition.command_contributions {
        let declared = match contribution.kind {
            PluginContributionKind::Provider => &definition.capabilities.providers,
            PluginContributionKind::CollaborationStrategy => &definition.capabilities.collaboration_strategies,
            PluginContributionKind::StorageBackend => &definition.capabilities.storage_backends,
            PluginContributionKind::Hook => &definition.capabilities.hooks,
        };
        if !declared.contains(&contribution.name) {
            return Err(format!(
                "plugin {} implements undeclared capability {}:{}",
                definition.id,
                contribution.kind.capability_prefix(),
                contribution.name
            ));
        }
    }
    Ok(())
}

const MAX_PLUGIN_DEPTH: usize = 8;
const MAX_PLUGIN_FILES: usize = 2048;

fn update_digest(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

fn fingerprint_directory(root: &Path, hasher: &mut Sha256) -> Result<(), String> {
    let mut files = Vec::new();
    collect_files(root, root, 0, &mut files)?;
    files.sort();
    for path in files {
        let relative = path
            .strip_prefix(root)
            .map_err(|_| format!("plugin file {} escapes authority root", path.display()))?;
        update_digest(hasher, relative.to_string_lossy().as_bytes());
        let bytes =
            std::fs::read(&path).map_err(|error| format!("failed to read plugin file {}: {error}", path.display()))?;
        update_digest(hasher, &bytes);
    }
    Ok(())
}

fn collect_files(root: &Path, current: &Path, depth: usize, files: &mut Vec<PathBuf>) -> Result<(), String> {
    if depth > MAX_PLUGIN_DEPTH {
        return Err(format!(
            "plugin directory {} exceeds maximum depth {MAX_PLUGIN_DEPTH}",
            current.display()
        ));
    }
    let metadata = std::fs::symlink_metadata(current)
        .map_err(|error| format!("failed to inspect plugin path {}: {error}", current.display()))?;
    if metadata.file_type().is_symlink() {
        return Err(format!("plugin path {} must not be a symbolic link", current.display()));
    }
    let mut entries = std::fs::read_dir(current)
        .map_err(|error| format!("failed to read plugin directory {}: {error}", current.display()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("failed to enumerate plugin directory {}: {error}", current.display()))?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|error| format!("failed to inspect plugin path {}: {error}", path.display()))?;
        if file_type.is_symlink() {
            return Err(format!("plugin path {} must not be a symbolic link", path.display()));
        }
        if file_type.is_dir() {
            collect_files(root, &path, depth + 1, files)?;
        } else if file_type.is_file() {
            let canonical = path
                .canonicalize()
                .map_err(|error| format!("failed to canonicalize plugin file {}: {error}", path.display()))?;
            if !canonical.starts_with(root) {
                return Err(format!("plugin file {} escapes authority root", path.display()));
            }
            if files.len() >= MAX_PLUGIN_FILES {
                return Err(format!("plugin contains more than {MAX_PLUGIN_FILES} files"));
            }
            files.push(path);
        } else {
            return Err(format!(
                "plugin path {} is not a regular file or directory",
                path.display()
            ));
        }
    }
    Ok(())
}

fn repository_host(repository: &str) -> Option<String> {
    let without_scheme = repository
        .strip_prefix("https://")
        .or_else(|| repository.strip_prefix("http://"))
        .unwrap_or(repository);
    without_scheme
        .split(['/', ':'])
        .next()
        .filter(|value| value.contains('.'))
        .map(str::to_owned)
}

pub struct ScopedResource {
    id: String,
    dispose: Option<Box<dyn FnOnce() + Send + 'static>>,
}

impl ScopedResource {
    pub fn new(id: impl Into<String>, dispose: impl FnOnce() + Send + 'static) -> Self {
        Self {
            id: id.into(),
            dispose: Some(Box::new(dispose)),
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    fn dispose(mut self) {
        if let Some(dispose) = self.dispose.take() {
            dispose();
        }
    }
}

pub struct PluginActivation {
    pub scope: PluginScope,
    pub plugin: Arc<ResolvedPluginDefinition>,
    resources: Mutex<Vec<ScopedResource>>,
    contributions: HashMap<String, Arc<PluginCommandContribution>>,
}

impl PluginActivation {
    pub fn new(
        scope: PluginScope,
        plugin: Arc<ResolvedPluginDefinition>,
        contributions: HashMap<String, Arc<PluginCommandContribution>>,
    ) -> Self {
        Self {
            scope,
            plugin,
            resources: Mutex::new(Vec::new()),
            contributions,
        }
    }

    pub fn add_resource(&self, resource: ScopedResource) {
        self.resources
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(resource);
    }

    pub fn deactivate(&self) {
        let resources = {
            let mut current = self.resources.lock().unwrap_or_else(|error| error.into_inner());
            std::mem::take(&mut *current)
        };
        for resource in resources.into_iter().rev() {
            resource.dispose();
        }
    }

    pub fn contribution(&self, capability: &str) -> Option<Arc<PluginCommandContribution>> {
        self.contributions.get(capability).cloned()
    }
}

impl Drop for PluginActivation {
    fn drop(&mut self) {
        let resources = self.resources.get_mut().unwrap_or_else(|error| error.into_inner());
        for resource in std::mem::take(resources).into_iter().rev() {
            resource.dispose();
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilityBinding {
    pub capability: String,
    pub implementation: ImplementationIdentity,
    pub plugin_id: String,
}

#[derive(Debug, Clone)]
struct ScopeEntry {
    parent: Option<String>,
    bindings: HashMap<String, CapabilityBinding>,
}

#[derive(Default)]
pub struct CapabilityResolver {
    scopes: RwLock<HashMap<String, ScopeEntry>>,
}

impl CapabilityResolver {
    pub fn define_scope(&self, id: impl Into<String>, parent: Option<String>) {
        self.scopes
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .entry(id.into())
            .or_insert_with(|| ScopeEntry {
                parent,
                bindings: HashMap::new(),
            });
    }

    pub fn bind(&self, scope_id: &str, binding: CapabilityBinding) -> Result<(), String> {
        self.bind_batch(scope_id, vec![binding])
    }

    pub fn bind_batch(&self, scope_id: &str, bindings: Vec<CapabilityBinding>) -> Result<(), String> {
        let mut scopes = self.scopes.write().unwrap_or_else(|error| error.into_inner());
        let scope = scopes
            .get_mut(scope_id)
            .ok_or_else(|| format!("unknown plugin scope: {scope_id}"))?;
        for binding in &bindings {
            if let Some(existing) = scope.bindings.get(&binding.capability)
                && existing.plugin_id != binding.plugin_id
            {
                return Err(format!(
                    "capability {} is already bound by plugin {} in scope {scope_id}",
                    binding.capability, existing.plugin_id
                ));
            }
        }
        for binding in bindings {
            scope.bindings.insert(binding.capability.clone(), binding);
        }
        Ok(())
    }

    pub fn unbind_plugin(&self, scope_id: &str, plugin_id: &str) -> usize {
        let mut scopes = self.scopes.write().unwrap_or_else(|error| error.into_inner());
        let Some(scope) = scopes.get_mut(scope_id) else {
            return 0;
        };
        let before = scope.bindings.len();
        scope.bindings.retain(|_, binding| binding.plugin_id != plugin_id);
        before.saturating_sub(scope.bindings.len())
    }

    pub fn resolve(&self, scope_id: &str, capability: &str) -> Option<CapabilityBinding> {
        self.resolve_with_scope(scope_id, capability)
            .map(|(_, binding)| binding)
    }

    pub fn resolve_with_scope(&self, scope_id: &str, capability: &str) -> Option<(String, CapabilityBinding)> {
        let scopes = self.scopes.read().unwrap_or_else(|error| error.into_inner());
        let mut current = Some(scope_id);
        let mut seen = HashSet::new();
        while let Some(scope_id) = current {
            if !seen.insert(scope_id.to_owned()) {
                return None;
            }
            let scope = scopes.get(scope_id)?;
            if let Some(binding) = scope.bindings.get(capability) {
                return Some((scope_id.to_owned(), binding.clone()));
            }
            current = scope.parent.as_deref();
        }
        None
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PluginActivationSnapshot {
    pub activation_id: String,
    pub scope: PluginScope,
    pub plugin_id: String,
    pub implementation: ImplementationIdentity,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PluginRuntimeSnapshot {
    pub installed: Vec<ResolvedPluginIdentity>,
    pub activations: Vec<PluginActivationSnapshot>,
}

pub fn plugin_scope_id(scope: &PluginScope) -> String {
    match scope {
        PluginScope::Global => "runtime".to_owned(),
        PluginScope::Workspace { workspace_id } => format!("workspace:{workspace_id}"),
        PluginScope::Run { run_id } => format!("run:{run_id}"),
        PluginScope::Team { team_id } => format!("team:{team_id}"),
        PluginScope::Agent { agent_id } => format!("agent:{agent_id}"),
    }
}

#[derive(Default)]
pub struct PluginRuntime {
    resolved: RwLock<HashMap<String, Arc<ResolvedPluginDefinition>>>,
    activations: RwLock<HashMap<String, Arc<PluginActivation>>>,
    capabilities: Arc<CapabilityResolver>,
}

impl PluginRuntime {
    pub fn install(&self, plugin: ResolvedPluginDefinition) -> Arc<ResolvedPluginDefinition> {
        let plugin = Arc::new(plugin);
        self.resolved
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .insert(plugin.definition.id.clone(), Arc::clone(&plugin));
        plugin
    }

    pub fn install_checked(&self, plugin: ResolvedPluginDefinition) -> Result<Arc<ResolvedPluginDefinition>, String> {
        if let Some(existing) = self.installed(&plugin.definition.id) {
            if existing.identity == plugin.identity {
                return Ok(existing);
            }
            let active = self
                .activations
                .read()
                .unwrap_or_else(|error| error.into_inner())
                .values()
                .any(|activation| activation.plugin.definition.id == plugin.definition.id);
            if active {
                return Err(format!(
                    "plugin {} is active; deactivate it before installing a different implementation",
                    plugin.definition.id
                ));
            }
        }
        Ok(self.install(plugin))
    }

    pub fn activate(
        &self,
        activation_id: impl Into<String>,
        scope: PluginScope,
        plugin_id: &str,
    ) -> Result<Arc<PluginActivation>, String> {
        let plugin = self
            .resolved
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(plugin_id)
            .cloned()
            .ok_or_else(|| format!("plugin {plugin_id} is not installed"))?;
        validate_plugin_definition(&plugin.definition)?;
        let scope_id = plugin_scope_id(&scope);
        if self
            .activations
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .any(|activation| activation.plugin.definition.id == plugin_id && activation.scope == scope)
        {
            return Err(format!("plugin {plugin_id} is already active in scope {scope_id}"));
        }
        for service in &plugin.definition.requires_services {
            if self
                .capabilities
                .resolve(&scope_id, &format!("service:{service}"))
                .is_none()
            {
                return Err(format!(
                    "plugin {} requires unavailable service {service} in scope {scope_id}",
                    plugin.definition.id
                ));
            }
        }
        let mut contributions = HashMap::new();
        for definition in plugin.definition.command_contributions.clone() {
            let capability = format!("{}:{}", definition.kind.capability_prefix(), definition.name);
            let contribution = Arc::new(PluginCommandContribution::from_resolved(&plugin, definition)?);
            if contributions.insert(capability.clone(), contribution).is_some() {
                return Err(format!("duplicate plugin contribution {capability}"));
            }
        }
        let activation = Arc::new(PluginActivation::new(scope, Arc::clone(&plugin), contributions));
        self.bind_plugin_capabilities(&scope_id, &plugin)?;
        self.activations
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .insert(activation_id.into(), Arc::clone(&activation));
        Ok(activation)
    }

    fn bind_plugin_capabilities(&self, scope_id: &str, plugin: &ResolvedPluginDefinition) -> Result<(), String> {
        let implementation = plugin.identity.implementation.clone();
        let plugin_id = plugin.definition.id.clone();
        let mut bindings = Vec::new();
        for (kind, values) in [
            ("tool", &plugin.definition.capabilities.tools),
            ("skill", &plugin.definition.capabilities.skills),
            ("provider", &plugin.definition.capabilities.providers),
            ("workflow", &plugin.definition.capabilities.workflows),
            ("strategy", &plugin.definition.capabilities.collaboration_strategies),
            ("storage", &plugin.definition.capabilities.storage_backends),
            ("hook", &plugin.definition.capabilities.hooks),
            ("service", &plugin.definition.capabilities.services),
        ] {
            for value in values {
                bindings.push(CapabilityBinding {
                    capability: format!("{kind}:{value}"),
                    implementation: implementation.clone(),
                    plugin_id: plugin_id.clone(),
                });
            }
        }
        for tool in &plugin.definition.command_tools {
            bindings.push(CapabilityBinding {
                capability: format!("tool:{}", tool.name),
                implementation: implementation.clone(),
                plugin_id: plugin_id.clone(),
            });
        }
        self.capabilities.bind_batch(scope_id, bindings)
    }

    pub fn deactivate(&self, activation_id: &str) -> bool {
        let activation = self
            .activations
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .remove(activation_id);
        if let Some(activation) = activation {
            let scope_id = plugin_scope_id(&activation.scope);
            self.capabilities
                .unbind_plugin(&scope_id, &activation.plugin.definition.id);
            activation.deactivate();
            true
        } else {
            false
        }
    }

    pub fn capability_resolver(&self) -> Arc<CapabilityResolver> {
        Arc::clone(&self.capabilities)
    }

    pub fn resolve_command_contribution(
        &self,
        scope_id: &str,
        capability: &str,
    ) -> Option<Arc<PluginCommandContribution>> {
        let (binding_scope, binding) = self.capabilities.resolve_with_scope(scope_id, capability)?;
        self.activations
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .find(|activation| {
                activation.plugin.definition.id == binding.plugin_id
                    && plugin_scope_id(&activation.scope) == binding_scope
            })
            .and_then(|activation| activation.contribution(capability))
    }

    pub fn uninstall_if_inactive(&self, plugin_id: &str) -> bool {
        let is_active = self
            .activations
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .any(|activation| activation.plugin.definition.id == plugin_id);
        if is_active {
            return false;
        }
        self.resolved
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .remove(plugin_id)
            .is_some()
    }

    pub fn initialize_scope_tree(&self, workspace_id: &str, run_id: &str, root_agent_id: &str) {
        self.capabilities.define_scope("runtime", None);
        self.capabilities
            .define_scope(format!("workspace:{workspace_id}"), Some("runtime".to_owned()));
        self.capabilities
            .define_scope(format!("run:{run_id}"), Some(format!("workspace:{workspace_id}")));
        self.capabilities
            .define_scope(format!("agent:{root_agent_id}"), Some(format!("run:{run_id}")));
    }

    pub fn active_implementation_identities(&self) -> Vec<ImplementationIdentity> {
        let mut values: Vec<_> = self
            .activations
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .map(|activation| activation.plugin.identity.implementation.clone())
            .collect();
        values.sort_by(|left, right| left.implementation_id.cmp(&right.implementation_id));
        values.dedup();
        values
    }

    pub fn active_plugins(&self) -> Vec<Arc<ResolvedPluginDefinition>> {
        let mut values: Vec<_> = self
            .activations
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .map(|activation| Arc::clone(&activation.plugin))
            .collect();
        values.sort_by(|left, right| left.definition.id.cmp(&right.definition.id));
        values.dedup_by(|left, right| left.definition.id == right.definition.id);
        values
    }

    pub fn snapshot(&self) -> PluginRuntimeSnapshot {
        let mut installed: Vec<_> = self
            .resolved
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .map(|plugin| plugin.identity.clone())
            .collect();
        installed.sort_by(|left, right| left.plugin_id.cmp(&right.plugin_id));
        let mut activations: Vec<_> = self
            .activations
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .map(|(activation_id, activation)| PluginActivationSnapshot {
                activation_id: activation_id.clone(),
                scope: activation.scope.clone(),
                plugin_id: activation.plugin.definition.id.clone(),
                implementation: activation.plugin.identity.implementation.clone(),
            })
            .collect();
        activations.sort_by(|left, right| left.activation_id.cmp(&right.activation_id));
        PluginRuntimeSnapshot { installed, activations }
    }

    pub fn installed(&self, plugin_id: &str) -> Option<Arc<ResolvedPluginDefinition>> {
        self.resolved
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(plugin_id)
            .cloned()
    }
}

#[cfg(test)]
#[path = "plugin_runtime_test.rs"]
mod plugin_runtime_test;
