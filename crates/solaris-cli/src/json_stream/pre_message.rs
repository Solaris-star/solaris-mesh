//! Pre-message phase of the JSON stream protocol.
//!
//! Before the first `Message` command arrives, the host is allowed to send
//! any number of `AddMcpServer` commands to connect additional MCP servers
//! up front. This phase drains those (and only those) commands, then hands
//! off the first non-`AddMcpServer` command it sees to the main dispatch
//! loop in `session.rs`.

use std::collections::HashMap;
use std::sync::Arc;

use solaris_agent::bootstrap::{
    allow_mcp_connection_for, connect_mcp_server_authorized, pin_mcp_server_config, revoke_mcp_connection_for,
};
use solaris_agent::engine::AgentEngine;
use solaris_agent::execution_context::EffectExecutionContext;
use solaris_agent::output::OutputSink;
use solaris_agent::permission_engine::PermissionContext;
use solaris_agent::spawner::AgentSpawner;
use solaris_config::config::{McpServerConfig, TransportType};
use solaris_mcp::identity::McpIdentityKey;
use solaris_mcp::manager::McpManager;
use solaris_mcp::tool_proxy::register_single_server_tools;
use solaris_protocol::commands::ProtocolCommand;
use solaris_protocol::events::ProtocolEvent;
use solaris_protocol::reader::ProtocolInput;
use solaris_protocol::writer::ProtocolEmitter;
use solaris_types::permission::{PermissionMode, ProcessNetworkConfig};
use tokio::sync::mpsc::UnboundedReceiver;

use super::outbox::handle_acknowledgement;

fn to_mcp_server_config(
    transport: &str,
    command: Option<String>,
    args: Option<Vec<String>>,
    env: Option<HashMap<String, String>>,
    url: Option<String>,
    headers: Option<HashMap<String, String>>,
    network: ProcessNetworkConfig,
) -> Result<McpServerConfig, String> {
    let transport_type = match transport {
        "stdio" => TransportType::Stdio,
        "sse" => TransportType::Sse,
        "streamable-http" | "streamable_http" => TransportType::StreamableHttp,
        other => return Err(format!("unknown transport: {other}")),
    };
    Ok(McpServerConfig {
        transport: transport_type,
        command,
        args,
        env,
        url,
        headers,
        network,
        deferred: None,
        startup_timeout_ms: None,
    })
}

fn ensure_mcp_name_available(permissions: &PermissionContext, name: &str) -> Result<(), String> {
    let static_source = format!("config:mcp:{name}");
    let dynamic_source = format!("host:mcp:{name}");
    if permissions.has_configured_effect_source(&static_source)
        || permissions.has_configured_effect_source(&dynamic_source)
    {
        return Err(format!("MCP server name '{name}' is already registered"));
    }
    Ok(())
}

fn ensure_dynamic_mcp_allowed(mode: PermissionMode) -> Result<(), String> {
    if mode == PermissionMode::Plan {
        return Err("plan mode does not permit adding MCP servers".to_owned());
    }
    Ok(())
}

/// Outcome of draining the pre-message phase.
pub(super) enum PreMessageOutcome {
    /// A `Stop` command was received before any `Message` — the caller
    /// should shut down immediately without entering the main loop.
    Stop { dynamic_managers: Vec<Arc<McpManager>> },
    /// The phase ended because a non-`AddMcpServer` command arrived (or the
    /// channel closed). Carries any MCP managers connected during the phase
    /// plus the command that ended it (`None` if the channel closed).
    Continue {
        dynamic_managers: Vec<Arc<McpManager>>,
        next_command: Option<Box<ProtocolCommand>>,
    },
}

fn consume_delivery_acknowledgement(writer: &dyn ProtocolEmitter, command: ProtocolCommand) -> Option<ProtocolCommand> {
    match command {
        ProtocolCommand::AcknowledgeDelivery(acknowledgement) => {
            handle_acknowledgement(writer, &acknowledgement);
            None
        }
        other => Some(other),
    }
}

/// Drain `AddMcpServer` commands until a `Stop` or a different command
/// arrives.
pub(super) async fn run(
    cmd_rx: &mut UnboundedReceiver<ProtocolInput>,
    engine: &mut AgentEngine,
    output: &Arc<dyn OutputSink>,
    writer: &Arc<dyn ProtocolEmitter>,
    spawner: &Arc<AgentSpawner>,
    execution_context: &EffectExecutionContext,
    mcp_identity_key: Option<&McpIdentityKey>,
) -> PreMessageOutcome {
    let mut dynamic_managers: Vec<Arc<McpManager>> = Vec::new();

    while let Some(input) = cmd_rx.recv().await {
        let cmd = match input {
            ProtocolInput::Command(command) => *command,
            ProtocolInput::Invalid(error) => {
                let _ = writer.emit(&error.into_event());
                continue;
            }
        };
        let Some(cmd) = consume_delivery_acknowledgement(writer.as_ref(), cmd) else {
            continue;
        };
        match cmd {
            ProtocolCommand::AddMcpServer {
                name,
                transport,
                command,
                args,
                env,
                url,
                headers,
                network,
            } => {
                if let Err(error) = ensure_dynamic_mcp_allowed(execution_context.permissions().mode()) {
                    output.emit_error(&format!("AddMcpServer '{name}': {error}"));
                    continue;
                }
                let Some(mcp_identity_key) = mcp_identity_key else {
                    output.emit_error(&format!("AddMcpServer '{name}': MCP identity is unavailable"));
                    continue;
                };
                tracing::info!(target: "solaris_mcp", %name, %transport, "AddMcpServer received");
                if let Err(error) = ensure_mcp_name_available(execution_context.permissions(), &name) {
                    output.emit_error(&format!("AddMcpServer '{name}': {error}"));
                    continue;
                }
                let config = match to_mcp_server_config(&transport, command, args, env, url, headers, network)
                    .and_then(|config| pin_mcp_server_config(&config))
                {
                    Ok(config) => config,
                    Err(e) => {
                        output.emit_error(&format!("AddMcpServer '{name}': {e}"));
                        continue;
                    }
                };

                let mut single_configs = HashMap::new();
                single_configs.insert(name.clone(), config.clone());
                tracing::info!(target: "solaris_mcp", %name, "connecting to mcp server");
                allow_mcp_connection_for(
                    execution_context.permissions(),
                    format!("host:mcp:{name}"),
                    &name,
                    &config,
                    mcp_identity_key,
                );
                let mut mgr = McpManager::new();
                match connect_mcp_server_authorized(&mut mgr, &name, &config, execution_context, mcp_identity_key).await
                {
                    Ok(tool_names) => {
                        tracing::info!(target: "solaris_mcp", %name, tools = tool_names.len(), "mcp server connected");
                        let mgr_arc = Arc::new(mgr);
                        let builtin_names = engine.tool_names();
                        register_single_server_tools(
                            engine.registry_mut(),
                            &mgr_arc,
                            &name,
                            &builtin_names,
                            &config,
                            mcp_identity_key,
                        );
                        engine.refresh_execution_environment(execution_context.environment().plugins);
                        dynamic_managers.push(mgr_arc);
                        if let Err(error) = spawner.add_mcp_capability_source(
                            Arc::clone(dynamic_managers.last().expect("manager just pushed")),
                            single_configs,
                        ) {
                            tracing::warn!(target: "solaris_mcp", %name, %error, "dynamic MCP connected but child capability inheritance update failed");
                            output.emit_error(&format!(
                                "AddMcpServer '{name}': connected for root Agent but child inheritance failed: {error}"
                            ));
                        }
                        let _ = writer.emit(&ProtocolEvent::McpReady {
                            name,
                            tools: tool_names,
                        });
                    }
                    Err(e) => {
                        revoke_mcp_connection_for(execution_context.permissions(), &format!("host:mcp:{name}"));
                        tracing::warn!(target: "solaris_mcp", %name, error = %e, "mcp server connection failed");
                        output.emit_error(&format!("AddMcpServer '{name}' failed: {e}"));
                    }
                }
            }
            ProtocolCommand::HostContextReady => {
                return PreMessageOutcome::Continue {
                    dynamic_managers,
                    next_command: None,
                };
            }
            ProtocolCommand::Stop => return PreMessageOutcome::Stop { dynamic_managers },
            other => {
                return PreMessageOutcome::Continue {
                    dynamic_managers,
                    next_command: Some(Box::new(other)),
                };
            }
        }
    }

    PreMessageOutcome::Continue {
        dynamic_managers,
        next_command: None,
    }
}

#[cfg(test)]
#[path = "pre_message_test.rs"]
mod tests;
