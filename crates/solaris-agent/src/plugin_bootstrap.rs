use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::json;
use solaris_tools::registry::ToolRegistry;
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::{AgentId, RunId};
use solaris_types::plugin::{PluginScope, ResolvedPluginDefinition, ResolvedPluginIdentity};
use solaris_types::workflow::WorkflowDefinition;

use crate::plugin_manifest::load_plugin_manifest;
use crate::plugin_runtime::{PluginResolver, PluginRuntime, PluginTrustPolicy};
use crate::plugin_tool::PluginCommandTool;
use crate::runtime_ledger::{LedgerRecord, RuntimeLedger};
use crate::workflow_controller::WorkflowController;

pub struct PluginBootstrap {
    pub runtime: Arc<PluginRuntime>,
    active: Vec<Arc<ResolvedPluginDefinition>>,
    skill_dirs: Vec<PathBuf>,
    workflows: Vec<(String, WorkflowDefinition)>,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct PluginHotContributions {
    pub skill_dirs: Vec<PathBuf>,
    pub workflows: Vec<WorkflowDefinition>,
}

impl PluginBootstrap {
    pub fn discover(
        workspace: &Path,
        run_id: &RunId,
        root_agent_id: &AgentId,
        ledger: Arc<dyn RuntimeLedger>,
    ) -> Result<Self, String> {
        let runtime = Arc::new(PluginRuntime::default());
        let workspace_id = workspace.to_string_lossy().into_owned();
        runtime.initialize_scope_tree(&workspace_id, run_id.as_str(), root_agent_id.as_str());

        let durable_records = ledger.records_for_run(run_id).map_err(|error| error.to_string())?;
        if durable_records
            .iter()
            .any(|record| is_durable_plugin_state(&record.record_type))
        {
            let restored =
                restore_durable_plugins(workspace, run_id, runtime, Arc::clone(&ledger), durable_records.clone())?;
            discover_project_plugins(workspace, run_id, ledger.as_ref(), &durable_records)?;
            return Ok(restored);
        }

        let mut warnings = Vec::new();
        warnings.extend(discover_project_plugins(
            workspace,
            run_id,
            ledger.as_ref(),
            &durable_records,
        )?);

        // Project plugins are only discovered above. Only host-global plugins
        // are eligible for automatic startup activation.
        let roots = global_plugin_roots();
        let mut manifests = Vec::new();
        for root in &roots {
            discover_manifests(root, 0, &mut manifests);
        }
        manifests.sort();
        manifests.dedup();

        let mut resolved = Vec::new();
        for manifest in manifests {
            let definition = match load_plugin_manifest(&manifest) {
                Ok(definition) => definition,
                Err(error) => {
                    record_lifecycle(
                        ledger.as_ref(),
                        run_id,
                        "plugin_lifecycle_failed",
                        json!({"phase": "manifest_load", "manifest_path": manifest, "error": error}),
                    )?;
                    warnings.push(error);
                    continue;
                }
            };
            let parent = manifest.parent().unwrap_or(workspace);
            let trust = PluginTrustPolicy {
                trusted_local_roots: roots.clone(),
                ..Default::default()
            };
            let resolver = PluginResolver::new(parent, trust);
            match resolver.resolve(definition) {
                Ok(plugin) => {
                    let payload = json!({
                        "plugin_id": plugin.definition.id,
                        "manifest_path": manifest,
                        "identity": plugin.identity,
                    });
                    record_lifecycle(ledger.as_ref(), run_id, "plugin_install_intent", payload.clone())?;
                    match runtime.install_checked(plugin.clone()) {
                        Ok(installed) => {
                            if let Err(error) = record_lifecycle(ledger.as_ref(), run_id, "plugin_installed", payload) {
                                runtime.uninstall_if_inactive(&installed.definition.id);
                                return Err(error);
                            }
                            resolved.push(plugin);
                        }
                        Err(error) => {
                            record_lifecycle(
                                ledger.as_ref(),
                                run_id,
                                "plugin_lifecycle_failed",
                                json!({
                                    "phase": "install",
                                    "manifest_path": manifest,
                                    "plugin_id": plugin.definition.id,
                                    "error": error,
                                }),
                            )?;
                            warnings.push(format!("{}: {error}", manifest.display()));
                        }
                    }
                }
                Err(error) => {
                    record_lifecycle(
                        ledger.as_ref(),
                        run_id,
                        "plugin_lifecycle_failed",
                        json!({"phase": "resolve", "manifest_path": manifest, "error": error}),
                    )?;
                    warnings.push(format!("{}: {error}", manifest.display()));
                }
            }
        }

        // Activate in dependency order. Plugins whose declared services are not
        // available remain inactive and surface a warning rather than taking
        // down the entire Mesh bootstrap.
        let mut pending = resolved;
        let mut active = Vec::new();
        while !pending.is_empty() {
            let before = pending.len();
            let mut next = Vec::new();
            for plugin in pending {
                let activation_id = format!("run:{}:{}", run_id, plugin.definition.id);
                let scope = PluginScope::Run {
                    run_id: run_id.to_string(),
                };
                let payload = json!({
                    "activation_id": activation_id,
                    "scope": scope,
                    "plugin_id": plugin.definition.id,
                    "identity": plugin.identity,
                });
                record_lifecycle(ledger.as_ref(), run_id, "plugin_activation_intent", payload.clone())?;
                match runtime.activate(activation_id.clone(), scope, &plugin.definition.id) {
                    Ok(_) => {
                        if let Err(error) = record_lifecycle(ledger.as_ref(), run_id, "plugin_activated", payload) {
                            runtime.deactivate(&activation_id);
                            return Err(error);
                        }
                        active.push(runtime.installed(&plugin.definition.id).expect("installed plugin"));
                    }
                    Err(error) => next.push((plugin, error)),
                }
            }
            if next.is_empty() {
                break;
            }
            if next.len() == before {
                for (plugin, error) in next {
                    record_lifecycle(
                        ledger.as_ref(),
                        run_id,
                        "plugin_lifecycle_failed",
                        json!({
                            "phase": "activate",
                            "plugin_id": plugin.definition.id,
                            "identity": plugin.identity,
                            "error": error,
                        }),
                    )?;
                    warnings.push(format!("plugin {} inactive: {error}", plugin.definition.id));
                }
                break;
            }
            pending = next.into_iter().map(|(plugin, _)| plugin).collect();
        }

        finish_bootstrap(runtime, active, warnings)
    }

    pub fn skill_dirs(&self) -> &[PathBuf] {
        &self.skill_dirs
    }

    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }

    pub fn register_tools(&self, registry: &mut ToolRegistry) {
        for plugin in &self.active {
            for definition in plugin.definition.command_tools.clone() {
                match PluginCommandTool::from_resolved(plugin, definition) {
                    Ok(tool) => registry.register(Box::new(tool)),
                    Err(error) => tracing::warn!(
                        target: "solaris_plugin",
                        plugin = %plugin.definition.id,
                        error = %error,
                        "plugin tool registration skipped"
                    ),
                }
            }
        }
    }

    pub fn register_workflows(&self, controller: &WorkflowController) -> Result<(), String> {
        let mut batches: std::collections::BTreeMap<String, Vec<WorkflowDefinition>> =
            std::collections::BTreeMap::new();
        for (plugin_id, workflow) in &self.workflows {
            batches.entry(plugin_id.clone()).or_default().push(workflow.clone());
        }
        for (plugin_id, workflows) in batches {
            controller.register_owned_batch(&plugin_workflow_owner(&plugin_id), workflows)?;
        }
        Ok(())
    }

    pub fn active_plugins(&self) -> &[Arc<ResolvedPluginDefinition>] {
        &self.active
    }
}

fn discover_project_plugins(
    workspace: &Path,
    run_id: &RunId,
    ledger: &dyn RuntimeLedger,
    durable_records: &[LedgerRecord],
) -> Result<Vec<String>, String> {
    let mut known = durable_records
        .iter()
        .filter(|record| record.record_type == "plugin_discovered")
        .filter_map(|record| {
            let path = record.payload.get("manifest_path")?.as_str()?;
            let identity = record.payload.get("identity")?;
            Some((path.to_owned(), serde_json::to_string(identity).ok()?))
        })
        .collect::<HashSet<_>>();
    let project_root = workspace.join(".solaris").join("plugins");
    let mut project_manifests = Vec::new();
    discover_manifests(&project_root, 0, &mut project_manifests);
    project_manifests.sort();
    project_manifests.dedup();
    let mut warnings = Vec::new();
    for manifest in project_manifests {
        let canonical_manifest = manifest.canonicalize().unwrap_or(manifest.clone());
        match resolve_manifest_for_host(workspace, &canonical_manifest) {
            Ok(plugin) => {
                let identity = serde_json::to_value(&plugin.identity).map_err(|error| error.to_string())?;
                let dedupe_key = (
                    canonical_manifest.to_string_lossy().into_owned(),
                    serde_json::to_string(&identity).map_err(|error| error.to_string())?,
                );
                if !known.insert(dedupe_key.clone()) {
                    continue;
                }
                record_lifecycle(
                    ledger,
                    run_id,
                    "plugin_discovered",
                    json!({
                        "plugin_id": plugin.definition.id,
                        "manifest_path": dedupe_key.0,
                        "identity": identity,
                        "trust": "approval_required",
                    }),
                )?;
            }
            Err(error) => {
                record_lifecycle(
                    ledger,
                    run_id,
                    "plugin_lifecycle_failed",
                    json!({"phase": "discover", "manifest_path": canonical_manifest, "error": error}),
                )?;
                warnings.push(error);
            }
        }
    }
    Ok(warnings)
}

#[derive(Clone)]
struct DurablePluginInstall {
    manifest_path: String,
    identity: ResolvedPluginIdentity,
}

fn restore_durable_plugins(
    workspace: &Path,
    run_id: &RunId,
    runtime: Arc<PluginRuntime>,
    ledger: Arc<dyn RuntimeLedger>,
    records: Vec<LedgerRecord>,
) -> Result<PluginBootstrap, String> {
    let mut manifest_intents = HashMap::<String, String>::new();
    let mut installs = HashMap::<String, DurablePluginInstall>::new();
    let mut activation_state = HashMap::<String, bool>::new();
    let mut pending = HashMap::<String, (String, String)>::new();

    for record in records {
        let plugin_id = record
            .payload
            .get("plugin_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned();
        match record.record_type.as_str() {
            "plugin_install_intent" => {
                if let Some(path) = record.payload.get("manifest_path").and_then(serde_json::Value::as_str) {
                    manifest_intents.insert(plugin_id.clone(), path.to_owned());
                }
                pending.insert(
                    operation_key("install", &plugin_id, &record.payload),
                    ("install".into(), plugin_id),
                );
            }
            "plugin_installed" => {
                let key = operation_key("install", &plugin_id, &record.payload);
                pending.remove(&key);
                let manifest_path = record
                    .payload
                    .get("manifest_path")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| manifest_intents.get(&plugin_id).cloned());
                let identity = record
                    .payload
                    .get("identity")
                    .cloned()
                    .and_then(|value| serde_json::from_value::<ResolvedPluginIdentity>(value).ok());
                if let (Some(manifest_path), Some(identity)) = (manifest_path, identity) {
                    installs.insert(
                        plugin_id,
                        DurablePluginInstall {
                            manifest_path,
                            identity,
                        },
                    );
                }
            }
            "plugin_activation_intent" => {
                pending.insert(
                    operation_key("activate", &plugin_id, &record.payload),
                    ("activate".into(), plugin_id),
                );
            }
            "plugin_activated" => {
                pending.remove(&operation_key("activate", &plugin_id, &record.payload));
                activation_state.insert(plugin_id, true);
            }
            "plugin_deactivation_intent" => {
                pending.insert(
                    operation_key("deactivate", &plugin_id, &record.payload),
                    ("deactivate".into(), plugin_id),
                );
            }
            "plugin_deactivated" => {
                pending.remove(&operation_key("deactivate", &plugin_id, &record.payload));
                activation_state.insert(plugin_id, false);
            }
            "plugin_lifecycle_failed" => {
                if let Some(request_id) = record.payload.get("request_id").and_then(serde_json::Value::as_str) {
                    pending.retain(|key, _| !key.ends_with(&format!(":{request_id}")));
                }
            }
            _ => {}
        }
    }

    let mut warnings = Vec::new();
    let mut reconcile_plugins = HashSet::new();
    for (_, (phase, plugin_id)) in pending {
        let reason = format!("plugin {plugin_id} has an unfinished {phase} intent and requires reconciliation");
        record_reconciliation(ledger.as_ref(), run_id, &plugin_id, &phase, &reason)?;
        warnings.push(reason);
        reconcile_plugins.insert(plugin_id);
    }

    let mut installed_ids: Vec<_> = installs.keys().cloned().collect();
    installed_ids.sort();
    for plugin_id in installed_ids {
        if reconcile_plugins.contains(&plugin_id) {
            continue;
        }
        let install = &installs[&plugin_id];
        let manifest = PathBuf::from(&install.manifest_path);
        let resolved = match resolve_manifest_for_host(workspace, &manifest) {
            Ok(plugin) => plugin,
            Err(error) => {
                let reason = format!("plugin {plugin_id} cannot be restored from its pinned manifest: {error}");
                record_reconciliation(ledger.as_ref(), run_id, &plugin_id, "restore", &reason)?;
                warnings.push(reason);
                reconcile_plugins.insert(plugin_id);
                continue;
            }
        };
        if resolved.identity != install.identity {
            let reason = format!("plugin {plugin_id} implementation changed since durable activation");
            record_reconciliation(ledger.as_ref(), run_id, &plugin_id, "identity", &reason)?;
            warnings.push(reason);
            reconcile_plugins.insert(plugin_id);
            continue;
        }
        runtime.install_checked(resolved)?;
    }

    let mut pending_active: Vec<_> = activation_state
        .into_iter()
        .filter_map(|(plugin_id, active)| (active && !reconcile_plugins.contains(&plugin_id)).then_some(plugin_id))
        .collect();
    pending_active.sort();
    let mut active = Vec::new();
    while !pending_active.is_empty() {
        let before = pending_active.len();
        let mut next = Vec::new();
        for plugin_id in pending_active {
            let activation_id = format!("run:{run_id}:{plugin_id}");
            let scope = PluginScope::Run {
                run_id: run_id.to_string(),
            };
            match runtime.activate(activation_id, scope, &plugin_id) {
                Ok(_) => active.push(runtime.installed(&plugin_id).expect("restored installed plugin")),
                Err(_) => next.push(plugin_id),
            }
        }
        if next.len() == before {
            for plugin_id in next {
                let reason = format!("plugin {plugin_id} durable activation dependencies cannot be restored");
                record_reconciliation(ledger.as_ref(), run_id, &plugin_id, "dependencies", &reason)?;
                warnings.push(reason);
            }
            break;
        }
        pending_active = next;
    }
    finish_bootstrap(runtime, active, warnings)
}

fn operation_key(phase: &str, plugin_id: &str, payload: &serde_json::Value) -> String {
    let operation_id = payload
        .get("request_id")
        .or_else(|| payload.get("activation_id"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or(plugin_id);
    format!("{phase}:{operation_id}")
}

fn record_reconciliation(
    ledger: &dyn RuntimeLedger,
    run_id: &RunId,
    plugin_id: &str,
    phase: &str,
    reason: &str,
) -> Result<(), String> {
    record_lifecycle(
        ledger,
        run_id,
        "plugin_reconciliation_required",
        json!({"plugin_id": plugin_id, "phase": phase, "reason": reason}),
    )
}

fn finish_bootstrap(
    runtime: Arc<PluginRuntime>,
    active: Vec<Arc<ResolvedPluginDefinition>>,
    mut warnings: Vec<String>,
) -> Result<PluginBootstrap, String> {
    let mut skill_dirs = Vec::new();
    let mut workflows = Vec::new();
    for plugin in &active {
        let Some(authority) = plugin.authority_root.as_deref() else {
            continue;
        };
        let authority = Path::new(authority);
        for relative in &plugin.definition.resources.skill_dirs {
            match resolve_resource(authority, relative) {
                Ok(path) if path.is_dir() => skill_dirs.push(path),
                Ok(path) => warnings.push(format!("plugin skill dir is not a directory: {}", path.display())),
                Err(error) => warnings.push(error),
            }
        }
        for relative in &plugin.definition.resources.workflow_files {
            match resolve_resource(authority, relative).and_then(load_workflow) {
                Ok(workflow) => workflows.push((plugin.definition.id.clone(), workflow)),
                Err(error) => warnings.push(error),
            }
        }
    }
    skill_dirs.sort();
    skill_dirs.dedup();
    Ok(PluginBootstrap {
        runtime,
        active,
        skill_dirs,
        workflows,
        warnings,
    })
}

pub fn plugin_workflow_owner(plugin_id: &str) -> String {
    format!("plugin:{plugin_id}")
}

fn record_lifecycle(
    ledger: &dyn RuntimeLedger,
    run_id: &RunId,
    record_type: &str,
    payload: serde_json::Value,
) -> Result<(), String> {
    ledger
        .append(run_id, DurabilityClass::SyncCritical, record_type, payload)
        .map(|_| ())
        .map_err(|error| format!("failed to persist {record_type}: {error}"))
}

pub fn resolve_manifest_for_host(workspace: &Path, manifest: &Path) -> Result<ResolvedPluginDefinition, String> {
    let requested_manifest = if manifest.is_absolute() {
        manifest.to_path_buf()
    } else {
        workspace.join(manifest)
    };
    let canonical_manifest = requested_manifest.canonicalize().map_err(|error| {
        format!(
            "failed to canonicalize plugin manifest {}: {error}",
            requested_manifest.display()
        )
    })?;
    let roots = plugin_roots(workspace);
    let allowed = roots
        .iter()
        .filter_map(|root| root.canonicalize().ok())
        .any(|root| canonical_manifest.starts_with(root));
    if !allowed {
        return Err(format!(
            "plugin manifest {} is outside trusted Solaris plugin roots",
            canonical_manifest.display()
        ));
    }
    let definition = load_plugin_manifest(&canonical_manifest)?;
    let trust = PluginTrustPolicy {
        trusted_local_roots: roots,
        ..Default::default()
    };
    let parent = canonical_manifest.parent().unwrap_or(workspace);
    PluginResolver::new(parent, trust).resolve(definition)
}

pub fn hot_contributions(plugin: &ResolvedPluginDefinition) -> Result<PluginHotContributions, String> {
    if plugin.definition.resources.skill_dirs.is_empty() && plugin.definition.resources.workflow_files.is_empty() {
        return Ok(PluginHotContributions::default());
    }
    let authority = plugin.authority_root.as_deref().ok_or_else(|| {
        format!(
            "plugin {} has resources but no materialized authority root",
            plugin.definition.id
        )
    })?;
    let authority = Path::new(authority);
    let mut skill_dirs = Vec::new();
    for relative in &plugin.definition.resources.skill_dirs {
        let path = resolve_resource(authority, relative)?;
        if !path.is_dir() {
            return Err(format!("plugin skill dir is not a directory: {}", path.display()));
        }
        skill_dirs.push(path);
    }
    let mut workflows = Vec::new();
    for relative in &plugin.definition.resources.workflow_files {
        workflows.push(load_workflow(resolve_resource(authority, relative)?)?);
    }
    skill_dirs.sort();
    skill_dirs.dedup();
    Ok(PluginHotContributions { skill_dirs, workflows })
}

fn plugin_roots(workspace: &Path) -> Vec<PathBuf> {
    let mut roots = vec![workspace.join(".solaris").join("plugins")];
    roots.extend(global_plugin_roots());
    roots
}

fn global_plugin_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Some(global) = solaris_config::config::app_config_dir() {
        roots.push(global.join("plugins"));
    }
    roots
}

fn is_durable_plugin_state(record_type: &str) -> bool {
    matches!(
        record_type,
        "plugin_install_intent"
            | "plugin_installed"
            | "plugin_activation_intent"
            | "plugin_activated"
            | "plugin_deactivation_intent"
            | "plugin_deactivated"
            | "plugin_reconciliation_required"
    )
}

fn discover_manifests(root: &Path, depth: usize, output: &mut Vec<PathBuf>) {
    if depth > 4 || !root.is_dir() {
        return;
    }
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            discover_manifests(&path, depth + 1, output);
        } else if path.is_file()
            && path
                .file_name()
                .and_then(|value| value.to_str())
                .is_some_and(|name| name == "plugin.json" || name.ends_with(".plugin.json"))
        {
            output.push(path);
        }
    }
}

fn resolve_resource(authority: &Path, relative: &str) -> Result<PathBuf, String> {
    let requested = PathBuf::from(relative);
    if requested.is_absolute() {
        return Err(format!(
            "plugin resource must be relative to authority root: {relative}"
        ));
    }
    let path = authority.join(requested);
    let canonical = path
        .canonicalize()
        .map_err(|error| format!("failed to resolve plugin resource {}: {error}", path.display()))?;
    let authority = authority
        .canonicalize()
        .map_err(|error| format!("invalid plugin authority root: {error}"))?;
    if !canonical.starts_with(&authority) {
        return Err(format!(
            "plugin resource {} escapes authority root",
            canonical.display()
        ));
    }
    Ok(canonical)
}

fn load_workflow(path: PathBuf) -> Result<WorkflowDefinition, String> {
    let content = std::fs::read_to_string(&path)
        .map_err(|error| format!("failed to read plugin workflow {}: {error}", path.display()))?;
    serde_json::from_str(&content).map_err(|error| format!("invalid plugin workflow {}: {error}", path.display()))
}

#[cfg(test)]
#[path = "plugin_bootstrap_test.rs"]
mod tests;
