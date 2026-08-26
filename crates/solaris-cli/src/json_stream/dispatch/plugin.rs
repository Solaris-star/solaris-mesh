use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use serde_json::json;

use solaris_agent::bootstrap::{authorize_activated_plugin, revoke_activated_plugin};
use solaris_agent::engine::AgentEngine;
use solaris_agent::plugin_bootstrap::{hot_contributions, plugin_workflow_owner, resolve_manifest_for_host};
use solaris_agent::plugin_tool::PluginCommandTool;
use solaris_protocol::events::{ErrorInfo, ProtocolEvent};
use solaris_types::effect::DurabilityClass;
use solaris_types::plugin::{PluginScope, ResolvedPluginDefinition};

use super::super::context::StreamContext;

fn emit_plugin_error(ctx: &StreamContext, request_id: &str, code: &str, message: impl Into<String>) {
    let message = message.into();
    let payload = json!({
        "request_id": request_id,
        "code": code,
        "message": message,
    });
    let _ = record_plugin_lifecycle_event(ctx, "plugin_lifecycle_failed", "plugin_failed", payload);
    let _ = ctx.writer.emit(&ProtocolEvent::Error {
        msg_id: Some(request_id.to_owned()),
        error: ErrorInfo {
            code: code.to_owned(),
            message,
            retryable: false,
        },
    });
}

fn record_plugin_lifecycle(ctx: &StreamContext, record_type: &str, payload: serde_json::Value) -> Result<(), String> {
    ctx.collaboration_runtime
        .commit_record(&ctx.run_id, DurabilityClass::SyncCritical, record_type, payload)
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn record_plugin_lifecycle_event(
    ctx: &StreamContext,
    record_type: &str,
    event_kind: &str,
    payload: serde_json::Value,
) -> Result<(), String> {
    ctx.collaboration_runtime
        .commit_runtime_event(
            &ctx.run_id,
            DurabilityClass::SyncCritical,
            record_type,
            None,
            event_kind,
            payload,
        )
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn record_plugin_lifecycle_event_within_mutation(
    ctx: &StreamContext,
    record_type: &str,
    event_kind: &str,
    payload: serde_json::Value,
) -> Result<(), String> {
    let record = ctx
        .collaboration_runtime
        .ledger()
        .append(&ctx.run_id, DurabilityClass::SyncCritical, record_type, payload.clone())
        .map_err(|error| error.to_string())?;
    ctx.collaboration_runtime
        .emit_durable_event(&record, None, event_kind, payload);
    Ok(())
}

pub(super) fn install_plugin(request_id: &str, manifest_path: &str, ctx: &StreamContext) {
    let resolved = match resolve_manifest_for_host(&ctx.workspace, Path::new(manifest_path)) {
        Ok(plugin) => plugin,
        Err(error) => {
            emit_plugin_error(ctx, request_id, "plugin_install_failed", error);
            return;
        }
    };
    if let Err(error) = record_plugin_lifecycle(
        ctx,
        "plugin_install_intent",
        json!({
            "request_id": request_id,
            "manifest_path": manifest_path,
            "plugin_id": resolved.definition.id,
            "identity": resolved.identity,
        }),
    ) {
        emit_plugin_error(ctx, request_id, "plugin_install_failed", error);
        return;
    }
    let installed = ctx.collaboration_runtime.with_run_mutation(&ctx.run_id, || {
        let previous = ctx.plugin_runtime.installed(&resolved.definition.id);
        let plugin = ctx.plugin_runtime.install_checked(resolved)?;
        let payload = json!({
            "request_id": request_id,
            "manifest_path": manifest_path,
            "plugin_id": plugin.definition.id,
            "identity": plugin.identity,
            "plugin_runtime": ctx.plugin_runtime.snapshot(),
        });
        if let Err(error) =
            record_plugin_lifecycle_event_within_mutation(ctx, "plugin_installed", "plugin_installed", payload)
        {
            let _ = ctx.plugin_runtime.uninstall_if_inactive(&plugin.definition.id);
            if let Some(previous) = previous {
                ctx.plugin_runtime.install((*previous).clone());
            }
            return Err(error);
        }
        Ok(plugin)
    });
    match installed {
        Ok(plugin) => ctx.emit_command_result(
            Some(request_id.to_owned()),
            "install_plugin",
            true,
            Some(plugin.definition.id.clone()),
        ),
        Err(error) => emit_plugin_error(ctx, request_id, "plugin_install_failed", error),
    }
}

pub(super) fn activate_plugin(request_id: &str, plugin_id: &str, engine: &mut AgentEngine, ctx: &StreamContext) {
    if ctx.workflow_controller.has_running_runs() {
        emit_plugin_error(
            ctx,
            request_id,
            "plugin_activation_busy",
            "cannot hot-activate plugins while a Workflow is running",
        );
        return;
    }
    let Some(plugin) = ctx.plugin_runtime.installed(plugin_id) else {
        emit_plugin_error(
            ctx,
            request_id,
            "plugin_not_installed",
            format!("plugin {plugin_id} is not installed"),
        );
        return;
    };
    let contributions = match hot_contributions(&plugin) {
        Ok(value) => value,
        Err(error) => {
            emit_plugin_error(ctx, request_id, "plugin_activation_failed", error);
            return;
        }
    };
    if !contributions.skill_dirs.is_empty() {
        emit_plugin_error(
            ctx,
            request_id,
            "plugin_requires_new_run",
            format!(
                "plugin {plugin_id} contributes Skill directories; Skills and system prompt are pinned at Run bootstrap, so activate this plugin in a new Run"
            ),
        );
        return;
    }

    let existing_tools: HashSet<_> = engine.tool_names().into_iter().collect();
    let mut plugin_tool_names = HashSet::new();
    let mut plugin_tools = Vec::new();
    for definition in plugin.definition.command_tools.clone() {
        if existing_tools.contains(&definition.name) || !plugin_tool_names.insert(definition.name.clone()) {
            emit_plugin_error(
                ctx,
                request_id,
                "plugin_tool_collision",
                format!("plugin tool name {} collides with an existing tool", definition.name),
            );
            return;
        }
        match PluginCommandTool::from_resolved(plugin.as_ref(), definition) {
            Ok(tool) => plugin_tools.push(tool),
            Err(error) => {
                emit_plugin_error(ctx, request_id, "plugin_activation_failed", error);
                return;
            }
        }
    }
    if let Err(error) = ctx
        .workflow_controller
        .preflight_register_batch(&contributions.workflows)
    {
        emit_plugin_error(ctx, request_id, "plugin_workflow_collision", error);
        return;
    }

    if let Err(error) = record_plugin_lifecycle(
        ctx,
        "plugin_activation_intent",
        json!({
            "request_id": request_id,
            "plugin_id": plugin_id,
            "identity": plugin.identity,
            "scope": {"kind": "run", "run_id": ctx.run_id},
        }),
    ) {
        emit_plugin_error(ctx, request_id, "plugin_activation_failed", error);
        return;
    }

    let activation_id = format!("run:{}:{}", ctx.run_id, plugin_id);
    let scope = PluginScope::Run {
        run_id: ctx.run_id.to_string(),
    };
    let activated = ctx.collaboration_runtime.with_run_mutation(&ctx.run_id, || {
        let workflow_owner = plugin_workflow_owner(plugin_id);
        ctx.plugin_runtime.activate(activation_id.clone(), scope, plugin_id)?;
        if let Err(error) = ctx.spawner.add_active_plugin(Arc::clone(&plugin)) {
            ctx.plugin_runtime.deactivate(&activation_id);
            return Err(error);
        }

        let registered_workflows: Vec<String> = contributions
            .workflows
            .iter()
            .map(|workflow| workflow.id.clone())
            .collect();
        if let Err(error) = ctx
            .workflow_controller
            .register_owned_batch(&workflow_owner, contributions.workflows.clone())
        {
            ctx.spawner.remove_active_plugin(plugin_id);
            ctx.plugin_runtime.deactivate(&activation_id);
            return Err(error);
        }
        for tool in plugin_tools {
            engine.registry_mut().register(Box::new(tool));
        }
        engine.refresh_execution_environment(ctx.plugin_runtime.active_implementation_identities());
        if let Err(error) = authorize_activated_plugin(&ctx.permission_context, plugin.as_ref()) {
            let tool_names: HashSet<_> = plugin
                .definition
                .command_tools
                .iter()
                .map(|definition| definition.name.clone())
                .collect();
            engine.registry_mut().remove_names(&tool_names);
            for workflow_id in registered_workflows {
                let _ = ctx.workflow_controller.unregister_owned(&workflow_owner, &workflow_id);
            }
            ctx.spawner.remove_active_plugin(plugin_id);
            ctx.plugin_runtime.deactivate(&activation_id);
            engine.refresh_execution_environment(ctx.plugin_runtime.active_implementation_identities());
            return Err(error);
        }
        let mut tool_names: Vec<_> = plugin_tool_names.into_iter().collect();
        tool_names.sort();
        let payload = json!({
            "request_id": request_id,
            "plugin_id": plugin_id,
            "activation_id": activation_id,
            "tool_names": tool_names,
            "plugin_runtime": ctx.plugin_runtime.snapshot(),
            "workflow_definitions": ctx.workflow_controller.definition_snapshots(),
        });
        if let Err(error) =
            record_plugin_lifecycle_event_within_mutation(ctx, "plugin_activated", "plugin_activated", payload)
        {
            let tool_names: HashSet<_> = plugin
                .definition
                .command_tools
                .iter()
                .map(|definition| definition.name.clone())
                .collect();
            engine.registry_mut().remove_names(&tool_names);
            for workflow_id in registered_workflows {
                let _ = ctx.workflow_controller.unregister_owned(&workflow_owner, &workflow_id);
            }
            ctx.spawner.remove_active_plugin(plugin_id);
            ctx.plugin_runtime.deactivate(&activation_id);
            refresh_plugin_authorizations(ctx, Some(plugin.as_ref()));
            engine.refresh_execution_environment(ctx.plugin_runtime.active_implementation_identities());
            return Err(error);
        }
        Ok(())
    });
    if let Err(error) = activated {
        emit_plugin_error(ctx, request_id, "plugin_activation_failed", error);
    } else {
        ctx.emit_command_result(
            Some(request_id.to_owned()),
            "activate_plugin",
            true,
            Some(plugin_id.to_owned()),
        );
    }
}

pub(super) fn deactivate_plugin(request_id: &str, plugin_id: &str, engine: &mut AgentEngine, ctx: &StreamContext) {
    if ctx.workflow_controller.has_running_runs() {
        emit_plugin_error(
            ctx,
            request_id,
            "plugin_deactivation_busy",
            "cannot hot-deactivate plugins while a Workflow is running",
        );
        return;
    }
    let Some(plugin) = ctx.plugin_runtime.installed(plugin_id) else {
        emit_plugin_error(
            ctx,
            request_id,
            "plugin_not_installed",
            format!("plugin {plugin_id} is not installed"),
        );
        return;
    };
    let contributions = match hot_contributions(&plugin) {
        Ok(value) => value,
        Err(error) => {
            emit_plugin_error(ctx, request_id, "plugin_deactivation_failed", error);
            return;
        }
    };
    if !contributions.skill_dirs.is_empty() {
        emit_plugin_error(
            ctx,
            request_id,
            "plugin_requires_new_run",
            format!(
                "plugin {plugin_id} contributes Skills pinned into this Run; deactivate it by starting a new Run without the plugin"
            ),
        );
        return;
    }
    for workflow in &contributions.workflows {
        if ctx.workflow_controller.definition(&workflow.id).is_none() {
            emit_plugin_error(
                ctx,
                request_id,
                "plugin_deactivation_failed",
                format!("plugin workflow {} is no longer registered", workflow.id),
            );
            return;
        }
    }
    let activation_id = format!("run:{}:{}", ctx.run_id, plugin_id);
    let active = ctx
        .plugin_runtime
        .snapshot()
        .activations
        .iter()
        .any(|activation| activation.activation_id == activation_id);
    if !active {
        emit_plugin_error(
            ctx,
            request_id,
            "plugin_not_active",
            format!("plugin {plugin_id} is not active in this Run"),
        );
        return;
    }

    if let Err(error) = record_plugin_lifecycle(
        ctx,
        "plugin_deactivation_intent",
        json!({
            "request_id": request_id,
            "plugin_id": plugin_id,
            "activation_id": activation_id,
        }),
    ) {
        emit_plugin_error(ctx, request_id, "plugin_deactivation_failed", error);
        return;
    }

    let deactivated = ctx.collaboration_runtime.with_run_mutation(&ctx.run_id, || {
        let workflow_owner = plugin_workflow_owner(plugin_id);
        if !ctx.plugin_runtime.deactivate(&activation_id) {
            return Err(format!("failed to deactivate plugin {plugin_id}"));
        }
        refresh_plugin_authorizations(ctx, Some(plugin.as_ref()));
        ctx.spawner.remove_active_plugin(plugin_id);
        let tool_names: HashSet<_> = plugin
            .definition
            .command_tools
            .iter()
            .map(|definition| definition.name.clone())
            .collect();
        engine.registry_mut().remove_names(&tool_names);
        for workflow in &contributions.workflows {
            ctx.workflow_controller
                .unregister_owned(&workflow_owner, &workflow.id)?;
        }
        engine.refresh_execution_environment(ctx.plugin_runtime.active_implementation_identities());
        let mut removed_tools: Vec<_> = tool_names.into_iter().collect();
        removed_tools.sort();
        let payload = json!({
            "request_id": request_id,
            "plugin_id": plugin_id,
            "activation_id": activation_id,
            "tool_names": removed_tools,
            "plugin_runtime": ctx.plugin_runtime.snapshot(),
            "workflow_definitions": ctx.workflow_controller.definition_snapshots(),
        });
        if let Err(error) =
            record_plugin_lifecycle_event_within_mutation(ctx, "plugin_deactivated", "plugin_deactivated", payload)
        {
            if ctx
                .plugin_runtime
                .activate(
                    activation_id.clone(),
                    PluginScope::Run {
                        run_id: ctx.run_id.to_string(),
                    },
                    plugin_id,
                )
                .is_ok()
            {
                let _ = ctx.spawner.add_active_plugin(Arc::clone(&plugin));
                for workflow in &contributions.workflows {
                    let _ = ctx
                        .workflow_controller
                        .register_owned(&workflow_owner, workflow.clone());
                }
                for definition in plugin.definition.command_tools.clone() {
                    if let Ok(tool) = PluginCommandTool::from_resolved(plugin.as_ref(), definition) {
                        engine.registry_mut().register(Box::new(tool));
                    }
                }
                let _ = authorize_activated_plugin(&ctx.permission_context, plugin.as_ref());
            }
            engine.refresh_execution_environment(ctx.plugin_runtime.active_implementation_identities());
            return Err(error);
        }
        Ok(())
    });
    if let Err(error) = deactivated {
        emit_plugin_error(ctx, request_id, "plugin_deactivation_failed", error);
    } else {
        ctx.emit_command_result(
            Some(request_id.to_owned()),
            "deactivate_plugin",
            true,
            Some(plugin_id.to_owned()),
        );
    }
}

fn refresh_plugin_authorizations(ctx: &StreamContext, removed: Option<&ResolvedPluginDefinition>) {
    if let Some(plugin) = removed {
        let _ = revoke_activated_plugin(&ctx.permission_context, plugin);
    }
    for plugin in ctx.plugin_runtime.active_plugins() {
        let _ = authorize_activated_plugin(&ctx.permission_context, plugin.as_ref());
    }
}
