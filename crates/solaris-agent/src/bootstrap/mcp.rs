use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use solaris_config::config::{McpServerConfig, TransportType};
use solaris_mcp::identity::McpIdentityKey;
use solaris_mcp::manager::{McpConnectionGuard, McpManager, PendingMcpServer};
use solaris_mcp::tool_proxy::{mcp_connection_resources_with_executable, mcp_server_config_identity_with_executable};
use solaris_mcp::transport::McpError;
use solaris_process::{ExecutableIdentity, PinnedExecutable, inspect_executable, pin_executable};
use solaris_types::effect::{EffectClass, EffectDescriptor, EffectReplayPolicy};
use solaris_types::permission::{PermissionDecision, PermissionRule};
use solaris_types::plugin::ImplementationIdentity;

use crate::execution_context::{
    EffectExecutionContext, EffectOutcomeGuard, EffectRecoveryDecision, stable_digest_value,
};
use crate::permission_engine::PermissionContext;
use crate::skill_tool::SharedSkillCatalog;
use crate::spawner::AgentSpawner;

#[derive(Default)]
pub(super) struct McpBootstrap {
    pub(super) manager: Option<Arc<McpManager>>,
    pub(super) managers: Vec<Arc<McpManager>>,
}

impl McpBootstrap {
    pub(super) fn has_mcp(&self) -> bool {
        self.manager.is_some()
    }
}

pub(super) fn mcp_plan_mode_disable_handler(
    spawner: Arc<AgentSpawner>,
    permissions: PermissionContext,
    skills: SharedSkillCatalog,
    configured_server_names: Vec<String>,
) -> Arc<dyn Fn() + Send + Sync> {
    Arc::new(move || {
        let mut managers = spawner.take_mcp_capability_sources();
        let mut seen = HashSet::new();
        managers.retain(|manager| seen.insert(Arc::as_ptr(manager) as usize));
        let mut server_names: HashSet<_> = configured_server_names.iter().cloned().collect();
        for manager in &managers {
            manager.begin_disable();
            server_names.extend(manager.server_names());
        }
        skills.remove_mcp();
        for server_name in server_names {
            revoke_mcp_connection_for(&permissions, &format!("config:mcp:{server_name}"));
            revoke_mcp_connection_for(&permissions, &format!("host:mcp:{server_name}"));
        }
        for manager in managers {
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    manager.shutdown().await;
                });
            } else {
                std::thread::spawn(move || {
                    let Ok(runtime) = tokio::runtime::Builder::new_current_thread().enable_all().build() else {
                        tracing::warn!(target: "solaris_mcp", "failed to create runtime for MCP shutdown");
                        return;
                    };
                    runtime.block_on(manager.shutdown());
                });
            }
        }
    })
}

pub fn mcp_connection_effect_descriptor(
    name: &str,
    config: &McpServerConfig,
    identity_key: &McpIdentityKey,
) -> EffectDescriptor {
    mcp_connection_effect_descriptor_with_executable(name, config, identity_key, None)
}

/// Stable permission capability for connecting exactly one MCP server.
pub fn mcp_connection_capability(server_name: &str) -> String {
    format!("mcp-connect:v1:{}:{server_name}", server_name.len())
}

pub fn allow_mcp_connection_for(
    permissions: &PermissionContext,
    source: impl Into<String>,
    server_name: &str,
    config: &McpServerConfig,
    identity_key: &McpIdentityKey,
) {
    let source = source.into();
    let capability = mcp_connection_capability(server_name);
    let descriptor = mcp_connection_effect_descriptor(server_name, config, identity_key);
    permissions.allow_configured_effect_for(source.clone(), capability.clone(), &descriptor);
    permissions.set_generated_rules(
        source,
        vec![PermissionRule {
            capability: Some(capability),
            action: None,
            effect_class: Some(descriptor.class),
            resource_prefixes: Vec::new(),
            decision: PermissionDecision::Allow,
        }],
    );
}

pub fn revoke_mcp_connection_for(permissions: &PermissionContext, source: &str) {
    permissions.revoke_configured_effect_from(source);
    permissions.remove_generated_rules(source);
}

fn mcp_connection_effect_descriptor_with_executable(
    name: &str,
    config: &McpServerConfig,
    identity_key: &McpIdentityKey,
    executable_identity: Option<&ExecutableIdentity>,
) -> EffectDescriptor {
    let replay_policy = if config.transport == TransportType::Stdio {
        EffectReplayPolicy::ReconcileRequired
    } else {
        EffectReplayPolicy::ReplaySafe
    };
    let mut resources = mcp_connection_resources_with_executable(config, identity_key, executable_identity);
    resources.external_resources = vec![format!("mcp:{name}")];
    if config.transport == TransportType::Stdio {
        resources.declare_sandboxed_process_access();
    }
    EffectDescriptor {
        class: if config.transport == TransportType::Stdio {
            EffectClass::Process
        } else {
            EffectClass::Network
        },
        action: format!("connect configured MCP server {name}"),
        resources,
        replay_policy,
    }
}

pub fn pin_mcp_server_config(config: &McpServerConfig) -> Result<McpServerConfig, String> {
    let mut pinned = config.clone();
    if pinned.transport != TransportType::Stdio {
        return Ok(pinned);
    }
    let command = pinned
        .command
        .as_deref()
        .ok_or_else(|| "stdio MCP server requires a command".to_owned())?;
    let resolved = which::which(command)
        .map_err(|_| "failed to resolve MCP executable".to_owned())?
        .canonicalize()
        .map_err(|_| "failed to canonicalize MCP executable".to_owned())?;
    let metadata = std::fs::symlink_metadata(&resolved).map_err(|_| "failed to inspect MCP executable".to_owned())?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err("MCP executable must be a regular file".to_owned());
    }
    pinned.command = Some(resolved.to_string_lossy().into_owned());
    Ok(pinned)
}

fn inspect_mcp_executable(config: &McpServerConfig) -> Result<Option<ExecutableIdentity>, String> {
    if config.transport != TransportType::Stdio {
        return Ok(None);
    }
    let command = config
        .command
        .as_deref()
        .ok_or_else(|| "stdio MCP server requires a command".to_owned())?;
    inspect_executable(Path::new(command))
        .map(Some)
        .map_err(|error| error.to_string())
}

pub(super) async fn prepare_mcp_server_authorized(
    name: &str,
    config: &McpServerConfig,
    context: &EffectExecutionContext,
    identity_key: &McpIdentityKey,
) -> Result<PendingMcpServer, String> {
    let pinned_config = pin_mcp_server_config(config)?;
    let config = &pinned_config;
    let approved_executable = inspect_mcp_executable(config)?;
    let input = serde_json::json!({
        "name": name,
        "config_identity": mcp_server_config_identity_with_executable(
            config,
            identity_key,
            approved_executable.as_ref(),
        ),
    });
    let effect_base = format!("mcp-connect:{name}:{}", stable_digest_value(&input));
    let call_id = context.stable_effect_attempt_id(&effect_base)?;
    let request = context.effect_request(
        &call_id,
        &mcp_connection_capability(name),
        &input,
        mcp_connection_effect_descriptor_with_executable(name, config, identity_key, approved_executable.as_ref()),
    );
    let approved_mode = context.permissions().mode();
    let approved_ceiling = context.permissions().ceiling();
    match context.recover_effect(&request)? {
        EffectRecoveryDecision::Execute => {}
        EffectRecoveryDecision::Reuse { .. } => {
            return Err("MCP connection recovery cannot reuse an ephemeral transport".into());
        }
        EffectRecoveryDecision::Reconcile { reason } => return Err(reason),
    }
    let evaluation = context.evaluate_with(&request, approved_mode, approved_ceiling);
    let approved_environment = context.environment();
    context
        .record_permission_decision(&request, &evaluation, "mcp_connection")
        .map_err(|error| error.to_string())?;
    if evaluation.decision != PermissionDecision::Allow {
        return Err(format!("MCP connection permission denied: {}", evaluation.reason));
    }
    let _permit = context.acquire_effect_permit().await?;
    let process_launch_policy = context.revalidate_configured_effect_before_execution(
        &request,
        &approved_environment,
        approved_mode,
        approved_ceiling,
        config.transport == TransportType::Stdio,
    )?;
    let process_spawn_authorization = if config.transport == TransportType::Stdio {
        let boundary = context.permissions().boundary();
        let workspace_root =
            (boundary.writable_roots.len() == 1).then(|| Path::new(&boundary.writable_roots[0]).to_path_buf());
        Some(context.process_spawn_authorization(
            request.clone(),
            approved_environment.clone(),
            approved_mode,
            approved_ceiling,
            workspace_root,
            evaluation.matched_lease,
            None,
        )?)
    } else {
        None
    };
    let pinned_executable = approved_executable
        .as_ref()
        .map(|identity| pin_executable(identity.canonical_path(), identity))
        .transpose()
        .map_err(|error| error.to_string())?;
    let actual_executable = pinned_executable.as_ref().map(|pinned| pinned.identity().clone());
    let mut prepared_command = pinned_executable
        .map(PinnedExecutable::command)
        .transpose()
        .map_err(|error| error.to_string())?;
    if let Some(policy) = process_launch_policy {
        prepared_command
            .as_mut()
            .ok_or_else(|| "stdio MCP server requires a pinned command".to_owned())?
            .launch_policy(policy);
    }
    if let Some(authorization) = process_spawn_authorization {
        prepared_command
            .as_mut()
            .ok_or_else(|| "stdio MCP server requires a pinned command".to_owned())?
            .spawn_authorizer(authorization);
    }
    let execution_config = config.clone();
    if let Some(identity) = actual_executable.as_ref() {
        let implementation = ImplementationIdentity {
            implementation_id: format!("mcp-server:{name}:{}", identity.path_digest()),
            version: None,
            digest: Some(identity.content_digest().to_owned()),
        };
        context
            .record_effect_intent_with_tool_implementation(&request, &implementation)
            .map_err(|error| error.to_string())?;
    } else {
        context
            .record_effect_intent(&request)
            .map_err(|error| error.to_string())?;
    }
    let guard = McpEffectConnectionGuard(EffectOutcomeGuard::new(
        context.clone(),
        request,
        format!("MCP server {name} connection cancelled"),
    ));
    match prepared_command {
        Some(command) => McpManager::connect_pending_authorized_with_stdio_command(
            name.to_owned(),
            &execution_config,
            command,
            guard,
        )
        .await
        .map_err(|error| error.to_string()),
        None => McpManager::connect_pending_authorized(name.to_owned(), &execution_config, guard)
            .await
            .map_err(|error| error.to_string()),
    }
}

pub async fn connect_mcp_server_authorized(
    manager: &mut McpManager,
    name: &str,
    config: &McpServerConfig,
    context: &EffectExecutionContext,
    identity_key: &McpIdentityKey,
) -> Result<Vec<String>, String> {
    let pending = prepare_mcp_server_authorized(name, config, context, identity_key).await?;
    Ok(manager.commit_pending(pending))
}

struct McpEffectConnectionGuard(EffectOutcomeGuard);

impl McpConnectionGuard for McpEffectConnectionGuard {
    fn complete(&mut self, result: &Result<Vec<String>, McpError>) -> Result<(), McpError> {
        let (is_error, output) = match result {
            Ok(tool_names) => (false, serde_json::to_string(tool_names).unwrap_or_default()),
            Err(error) => (true, error.to_string()),
        };
        self.0
            .complete(is_error, &output)
            .map_err(|error| McpError::Transport(format!("failed to persist MCP connection outcome: {error}")))
    }
}
