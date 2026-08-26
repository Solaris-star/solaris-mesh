use std::collections::{BTreeMap, HashMap};

use serde_json::{Value, json};

use solaris_config::config::Config;
use solaris_tools::registry::ToolRegistry;
use solaris_types::identity::RunId;
use solaris_types::permission::CapabilityLease;
use solaris_types::plugin::ImplementationIdentity;
use solaris_types::runtime::{CompatibilityDecision, OperationEnvironmentSnapshot, ToolImplementationSnapshot};

use crate::permission_engine::PermissionContext;
use crate::runtime_ledger::RuntimeLedger;

use super::{stable_digest_bytes, stable_digest_serializable, stable_digest_value};

pub(crate) fn build_environment_snapshot(
    config: &Config,
    registry: &ToolRegistry,
    permissions: &PermissionContext,
) -> OperationEnvironmentSnapshot {
    build_environment_snapshot_with_plugins(config, registry, permissions, Vec::new())
}

pub(crate) fn build_environment_snapshot_with_plugins(
    config: &Config,
    registry: &ToolRegistry,
    permissions: &PermissionContext,
    plugins: Vec<ImplementationIdentity>,
) -> OperationEnvironmentSnapshot {
    let plugins = normalize_plugin_identities(plugins);
    let contract = config.provider_contract();
    let provider = ImplementationIdentity {
        implementation_id: format!("provider:{}:{}", config.provider_label, contract.protocol.0),
        version: None,
        digest: Some(stable_digest_serializable(&contract)),
    };
    let mut tools: Vec<_> = registry
        .tool_names()
        .into_iter()
        .filter_map(|name| {
            let tool = registry.get(&name)?;
            let default_implementation = ImplementationIdentity {
                implementation_id: format!("tool:{name}"),
                version: Some(env!("CARGO_PKG_VERSION").to_owned()),
                digest: Some(stable_digest_value(&tool.input_schema())),
            };
            Some(ToolImplementationSnapshot {
                name: name.clone(),
                implementation: tool.implementation_identity().unwrap_or(default_implementation),
                schema_digest: Some(stable_digest_value(&tool.input_schema())),
                replay_policy: tool.describe_effect(&json!({})).replay_policy,
            })
        })
        .collect();
    tools.sort_by(|left, right| left.name.cmp(&right.name));

    let permission_fingerprint = permission_fingerprint(permissions);
    let hook_digest = stable_digest_bytes(format!("{:?}", config.hooks).as_bytes());

    OperationEnvironmentSnapshot {
        runtime_generation: 1,
        config_generation: 1,
        provider: Some(provider),
        plugins,
        tools,
        hook_order: vec![ImplementationIdentity {
            implementation_id: "hook-chain".to_owned(),
            version: None,
            digest: Some(hook_digest),
        }],
        workflow: None,
        permission_fingerprint: Some(permission_fingerprint),
    }
}

pub(super) fn permission_fingerprint(permissions: &PermissionContext) -> String {
    stable_digest_value(&json!({
        "mode": permissions.mode(),
        "ceiling": permissions.ceiling(),
        "rules": permissions.rules(),
        "boundary": permissions.boundary(),
        "protected_paths": permissions.protected_path_fingerprint_material(),
    }))
}

pub(super) fn read_only_evidence_environment_digest(
    environment: &OperationEnvironmentSnapshot,
    tool_name: &str,
) -> String {
    let tool = environment.tools.iter().find(|tool| tool.name == tool_name);
    stable_digest_value(&json!({
        "runtime_generation": environment.runtime_generation,
        "config_generation": environment.config_generation,
        "plugins": environment.plugins,
        "tool": tool,
        "hook_order": environment.hook_order,
        "workflow": environment.workflow,
    }))
}

/// Digest of the execution capabilities that define a tool-call statistics scope.
///
/// Volatile state (permission fingerprint, generation counters) is excluded so
/// that duplicate detection is reset only when the provider, plugins, tool
/// implementations, hook order, or workflow actually change.
pub(super) fn tool_call_scope_environment_digest(environment: &OperationEnvironmentSnapshot) -> String {
    stable_digest_value(&json!({
        "provider": environment.provider,
        "plugins": environment.plugins,
        "tools": environment.tools,
        "hook_order": environment.hook_order,
        "workflow": environment.workflow,
    }))
}

pub(crate) fn refresh_environment_tools_and_plugins(
    current: &OperationEnvironmentSnapshot,
    registry: &ToolRegistry,
    plugins: Vec<ImplementationIdentity>,
) -> OperationEnvironmentSnapshot {
    let plugins = normalize_plugin_identities(plugins);
    let mut tools: Vec<_> = registry
        .tool_names()
        .into_iter()
        .filter_map(|name| {
            let tool = registry.get(&name)?;
            let default_implementation = ImplementationIdentity {
                implementation_id: format!("tool:{name}"),
                version: Some(env!("CARGO_PKG_VERSION").to_owned()),
                digest: Some(stable_digest_value(&tool.input_schema())),
            };
            Some(ToolImplementationSnapshot {
                name: name.clone(),
                implementation: tool.implementation_identity().unwrap_or(default_implementation),
                schema_digest: Some(stable_digest_value(&tool.input_schema())),
                replay_policy: tool.describe_effect(&json!({})).replay_policy,
            })
        })
        .collect();
    tools.sort_by(|left, right| left.name.cmp(&right.name));

    let mut next = current.clone();
    next.runtime_generation = next.runtime_generation.saturating_add(1);
    next.plugins = plugins;
    next.tools = tools;
    next
}

pub(super) fn normalize_plugin_identities(mut plugins: Vec<ImplementationIdentity>) -> Vec<ImplementationIdentity> {
    plugins.sort_by(|left, right| {
        (&left.implementation_id, &left.version, &left.digest).cmp(&(
            &right.implementation_id,
            &right.version,
            &right.digest,
        ))
    });
    plugins.dedup();
    if let Some(conflict) = plugins
        .windows(2)
        .find(|pair| pair[0].implementation_id == pair[1].implementation_id)
    {
        tracing::warn!(
            implementation_id_digest = %stable_digest_bytes(conflict[0].implementation_id.as_bytes()),
            "conflicting plugin implementation identities retained for fail-closed validation"
        );
    }
    plugins
}

pub(super) fn normalize_environment(mut environment: OperationEnvironmentSnapshot) -> OperationEnvironmentSnapshot {
    environment.plugins = normalize_plugin_identities(environment.plugins);
    environment
}

pub(super) fn validate_environment_plugin_identities(environment: &OperationEnvironmentSnapshot) -> Result<(), String> {
    let mut seen = HashMap::<&str, &ImplementationIdentity>::new();
    for identity in &environment.plugins {
        if let Some(existing) = seen.insert(&identity.implementation_id, identity)
            && existing != identity
        {
            return Err(format!(
                "conflicting plugin implementation identity sha256:{}",
                stable_digest_bytes(identity.implementation_id.as_bytes())
            ));
        }
    }
    Ok(())
}

pub(crate) fn compatibility_decision(
    recorded: &OperationEnvironmentSnapshot,
    current: &OperationEnvironmentSnapshot,
) -> CompatibilityDecision {
    if recorded == current {
        return CompatibilityDecision::Compatible;
    }
    if implementation_changed(recorded.provider.as_ref(), current.provider.as_ref()) {
        return CompatibilityDecision::Incompatible;
    }
    let recorded_tools: BTreeMap<_, _> = recorded.tools.iter().map(|tool| (tool.name.as_str(), tool)).collect();
    let current_tools: BTreeMap<_, _> = current.tools.iter().map(|tool| (tool.name.as_str(), tool)).collect();
    for (name, recorded_tool) in &recorded_tools {
        let Some(current_tool) = current_tools.get(name) else {
            return CompatibilityDecision::Incompatible;
        };
        if recorded_tool.implementation.implementation_id != current_tool.implementation.implementation_id {
            return CompatibilityDecision::Incompatible;
        }
    }
    CompatibilityDecision::ReconcileRequired
}

fn implementation_changed(recorded: Option<&ImplementationIdentity>, current: Option<&ImplementationIdentity>) -> bool {
    match (recorded, current) {
        (Some(left), Some(right)) => left.implementation_id != right.implementation_id,
        (None, None) => false,
        _ => true,
    }
}

pub(super) fn restore_permission_leases(ledger: &dyn RuntimeLedger, run_id: &RunId, permissions: &PermissionContext) {
    let records = match ledger.records_for_run(run_id) {
        Ok(records) => records,
        Err(error) => {
            tracing::warn!(
                %error,
                run_id_digest = %stable_digest_bytes(run_id.as_str().as_bytes()),
                "permission lease recovery skipped because journal loading failed"
            );
            return;
        }
    };
    let mut leases = HashMap::<String, (CapabilityLease, u32)>::new();
    for record in records {
        match record.record_type.as_str() {
            "capability_lease_issued" => {
                if record.payload.get("restorable").and_then(Value::as_bool) != Some(true) {
                    tracing::warn!(
                        sequence = record.seq,
                        "non-restorable capability lease skipped during recovery"
                    );
                    continue;
                }
                let mut lease = match serde_json::from_value::<CapabilityLease>(record.payload) {
                    Ok(lease) => lease,
                    Err(error) => {
                        tracing::warn!(
                            %error,
                            sequence = record.seq,
                            "invalid capability lease skipped during recovery"
                        );
                        continue;
                    }
                };
                if lease.lease_id.is_empty() {
                    lease.lease_id = format!("legacy:{}:{}", run_id, record.seq);
                }
                leases.insert(lease.lease_id.clone(), (lease, 0));
            }
            "capability_lease_consumed" => {
                let Some(lease_id) = record.payload.get("lease_id").and_then(Value::as_str) else {
                    continue;
                };
                let use_number = record
                    .payload
                    .get("use_number")
                    .and_then(Value::as_u64)
                    .and_then(|value| u32::try_from(value).ok())
                    .unwrap_or(1);
                if let Some((_, uses)) = leases.get_mut(lease_id) {
                    *uses = (*uses).max(use_number);
                }
            }
            "capability_lease_revoked" => {
                if let Some(lease_id) = record.payload.get("lease_id").and_then(Value::as_str) {
                    leases.remove(lease_id);
                }
            }
            _ => {}
        }
    }
    for (_, (lease, uses)) in leases {
        permissions.restore_lease(lease, uses);
    }
}
