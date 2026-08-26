//! Shared protocol I/O context threaded through the JSON stream main loop.
//!
//! `dispatch::handle` and `message::handle` both need the same cluster of
//! protocol plumbing (output sink, writer, approval manager, protocol sink,
//! and the current MCP availability flag). Grouping them here avoids passing
//! the same five arguments individually to every handler.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};

use solaris_agent::collaboration_runtime::{CollaborationRuntime, RuntimeProjection};
use solaris_agent::engine::RuntimeConfigurationView;
use solaris_agent::execution_context::EffectExecutionContext;
use solaris_agent::output::OutputSink;
use solaris_agent::output::protocol_sink::ProtocolSink;
use solaris_agent::permission_engine::PermissionContext;
use solaris_agent::plugin_runtime::PluginRuntime;
use solaris_agent::role_registry::AgentRoleRegistry;
use solaris_agent::spawner::AgentSpawner;
use solaris_agent::workflow_controller::WorkflowController;
use solaris_config::compat::ProviderCompat;
use solaris_protocol::ToolApprovalManager;
use solaris_protocol::events::{ProtocolEvent, RuntimeConfiguration, RuntimeSnapshotPayload};
use solaris_protocol::writer::ProtocolEmitter;
use solaris_types::config::ConfigUpdateOutcome;
use solaris_types::identity::{AgentId, RunId};
use solaris_types::permission::PermissionMode;
use solaris_types::run_preset::Intensity;
use tokio::sync::{Notify, mpsc::UnboundedSender, oneshot, watch};
use tokio::task::AbortHandle;

struct DetachedWorkflowControl {
    sender: watch::Sender<bool>,
    abort_handle: Option<AbortHandle>,
    host_msg_id: Option<String>,
}

pub(super) struct WorkflowTurnCommit {
    pub(super) host_msg_id: String,
    pub(super) user_content: String,
    pub(super) workflow_run_id: RunId,
    pub(super) workflow_id: String,
    pub(super) workflow_version: String,
    pub(super) output: Value,
    pub(super) completion: oneshot::Sender<Result<bool, String>>,
}

#[derive(Clone, Default)]
pub(super) struct DetachedWorkflowRegistry {
    controls: Arc<Mutex<HashMap<RunId, DetachedWorkflowControl>>>,
    completion: Arc<Notify>,
}

impl DetachedWorkflowRegistry {
    #[cfg(test)]
    pub(super) fn register(&self, run_id: RunId) -> Result<watch::Receiver<bool>, String> {
        self.register_inner(run_id.clone(), None)?
            .ok_or_else(|| format!("workflow run '{run_id}' is already active"))
    }

    #[cfg(test)]
    pub(super) fn register_required(
        &self,
        run_id: RunId,
        host_msg_id: String,
    ) -> Result<watch::Receiver<bool>, String> {
        self.register_inner(run_id.clone(), Some(host_msg_id))?
            .ok_or_else(|| format!("workflow run '{run_id}' is already active"))
    }

    pub(super) fn register_if_absent(&self, run_id: RunId) -> Result<Option<watch::Receiver<bool>>, String> {
        self.register_inner(run_id, None)
    }

    pub(super) fn register_required_if_absent(
        &self,
        run_id: RunId,
        host_msg_id: String,
    ) -> Result<Option<watch::Receiver<bool>>, String> {
        self.register_inner(run_id, Some(host_msg_id))
    }

    fn register_inner(
        &self,
        run_id: RunId,
        host_msg_id: Option<String>,
    ) -> Result<Option<watch::Receiver<bool>>, String> {
        let mut controls = self
            .controls
            .lock()
            .map_err(|_| "detached workflow registry lock poisoned".to_owned())?;
        if controls.contains_key(&run_id) {
            return Ok(None);
        }
        let (sender, receiver) = watch::channel(false);
        controls.insert(
            run_id,
            DetachedWorkflowControl {
                sender,
                abort_handle: None,
                host_msg_id,
            },
        );
        Ok(Some(receiver))
    }

    #[cfg(test)]
    pub(super) fn is_active(&self, run_id: &RunId) -> bool {
        self.controls.lock().is_ok_and(|controls| controls.contains_key(run_id))
    }

    pub(super) fn active_required_turn(&self) -> Option<(String, RunId)> {
        self.controls.lock().ok().and_then(|controls| {
            controls.iter().find_map(|(run_id, control)| {
                control
                    .host_msg_id
                    .as_ref()
                    .map(|msg_id| (msg_id.clone(), run_id.clone()))
            })
        })
    }

    pub(super) fn cancel_required_turn(&self, msg_id: &str) -> bool {
        let Ok(controls) = self.controls.lock() else {
            return false;
        };
        controls
            .values()
            .any(|control| control.host_msg_id.as_deref() == Some(msg_id) && control.sender.send(true).is_ok())
    }

    pub(super) fn attach_abort_handle(&self, run_id: &RunId, abort_handle: AbortHandle) -> Result<(), String> {
        let mut controls = self
            .controls
            .lock()
            .map_err(|_| "detached workflow registry lock poisoned".to_owned())?;
        let control = controls
            .get_mut(run_id)
            .ok_or_else(|| format!("workflow run '{run_id}' is no longer active"))?;
        control.abort_handle = Some(abort_handle);
        Ok(())
    }

    pub(super) fn finish(&self, run_id: &RunId) {
        if let Ok(mut controls) = self.controls.lock() {
            controls.remove(run_id);
        }
        self.completion.notify_waiters();
    }

    pub(super) fn cancel_all(&self) -> usize {
        let Ok(controls) = self.controls.lock() else {
            return 0;
        };
        for control in controls.values() {
            let _ = control.sender.send(true);
        }
        controls.len()
    }

    pub(super) fn cancel(&self, run_id: &RunId) -> bool {
        let Ok(controls) = self.controls.lock() else {
            return false;
        };
        controls
            .get(run_id)
            .is_some_and(|control| control.sender.send(true).is_ok())
    }

    pub(super) fn abort_all(&self) -> usize {
        let Ok(mut controls) = self.controls.lock() else {
            return 0;
        };
        let count = controls.len();
        for (_, control) in controls.drain() {
            if let Some(handle) = control.abort_handle {
                handle.abort();
            }
        }
        drop(controls);
        self.completion.notify_waiters();
        count
    }

    pub(super) fn active_run_ids(&self) -> Vec<RunId> {
        self.controls
            .lock()
            .map(|controls| controls.keys().cloned().collect())
            .unwrap_or_default()
    }

    pub(super) async fn cancel_and_wait(&self, timeout: std::time::Duration) -> bool {
        self.cancel_all();
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.completion.notified();
            if self.active_run_ids().is_empty() {
                return true;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return false;
            }
        }
    }
}

/// Protocol I/O handles shared across top-level command dispatch and
/// in-flight message handling.
///
/// `engine` and `cmd_rx` are deliberately excluded: they need independent
/// `&mut` borrows that would conflict with a shared `&StreamContext`.
#[derive(Clone)]
pub(super) struct StreamContext {
    pub(super) output: Arc<dyn OutputSink>,
    pub(super) writer: Arc<dyn ProtocolEmitter>,
    pub(super) approval_manager: Arc<ToolApprovalManager>,
    pub(super) permission_context: PermissionContext,
    pub(super) runtime_configuration: RuntimeConfigurationView,
    pub(super) workspace: PathBuf,
    pub(super) run_id: RunId,
    pub(super) root_agent_id: AgentId,
    pub(super) collaboration_runtime: Arc<CollaborationRuntime<()>>,
    pub(super) workflow_controller: Arc<WorkflowController>,
    pub(super) role_registry: Arc<AgentRoleRegistry>,
    pub(super) spawner: Arc<AgentSpawner>,
    pub(super) plugin_runtime: Arc<PluginRuntime>,
    pub(super) execution_context: EffectExecutionContext,
    pub(super) detached_workflows: DetachedWorkflowRegistry,
    pub(super) workflow_turn_commits: UnboundedSender<WorkflowTurnCommit>,
    pub(super) protocol_sink: Arc<ProtocolSink>,
    pub(super) has_mcp: bool,
    pub(super) host_capabilities: Value,
}

impl StreamContext {
    pub(super) fn runtime_snapshot_from(
        &self,
        runtime: RuntimeProjection,
        journal_sequence: u64,
        live_sequence: u64,
        timestamp_unix_ms: i64,
        extras: Value,
    ) -> RuntimeSnapshotPayload {
        let configuration = self.configuration();
        let intensity = configuration.selected_intensity;
        let multi_agent_policy = configuration.multi_agent_policy;
        let host_capabilities = capabilities_for_permission(&self.host_capabilities, configuration.permission);
        let extensions = json!({
            "run_id": self.run_id,
            "root_agent_id": self.root_agent_id,
            "captured_at_unix_ms": timestamp_unix_ms,
            "journal_last_sequence": journal_sequence,
            "live_last_sequence": live_sequence,
            "intensity": intensity,
            "intensity_levels": Intensity::USER_LEVELS,
            "multi_agent_policy": multi_agent_policy,
            "host_capabilities": host_capabilities,
            "runtime": runtime,
            "workflow_definitions": extras.get("workflow_definitions").cloned().unwrap_or_default(),
            "workflow_runs": extras.get("workflow_runs").cloned().unwrap_or_default(),
            "roles": extras.get("roles").cloned().unwrap_or_default(),
            "plugins": extras.get("plugins").cloned().unwrap_or_default(),
            "plugin_discoveries": extras.get("plugin_discoveries").cloned().unwrap_or_default(),
            "plugin_reconciliation": extras.get("plugin_reconciliation").cloned().unwrap_or_default(),
            "plan_artifact_refs": extras.get("plan_artifact_refs").cloned().unwrap_or_default(),
            "resources": extras.get("resources").cloned().unwrap_or_default(),
            "active_required_turn": self.detached_workflows.active_required_turn().map(|(msg_id, workflow_run_id)| {
                json!({"msg_id": msg_id, "workflow_run_id": workflow_run_id})
            }),
        });
        RuntimeSnapshotPayload::new(configuration, extensions)
    }

    pub(super) fn capture_runtime_extras(&self) -> Value {
        let plugin_records = self
            .collaboration_runtime
            .ledger()
            .records_for_run(&self.run_id)
            .unwrap_or_default();
        let plugin_reconciliation: Vec<_> = plugin_records
            .iter()
            .filter(|record| {
                matches!(
                    record.record_type.as_str(),
                    "plugin_reconciliation_required" | "plugin_lifecycle_failed"
                )
            })
            .map(|record| record.payload.clone())
            .collect();
        let plugin_discoveries: Vec<_> = plugin_records
            .iter()
            .filter(|record| record.record_type == "plugin_discovered")
            .map(|record| record.payload.clone())
            .collect();
        let plan_artifact_refs = match self.execution_context.plan_artifacts_for_run_tree(&self.run_id) {
            Ok(artifacts) => artifacts
                .into_iter()
                .map(|artifact| artifact.reference())
                .collect::<Vec<_>>(),
            Err(error) => {
                tracing::warn!(target: "solaris_protocol", error = %error, "failed to capture plan artifact references");
                Vec::new()
            }
        };
        json!({
            "workflow_definitions": self.workflow_controller.definition_snapshots(),
            "workflow_runs": self.workflow_controller.snapshots(),
            "roles": self.role_registry.snapshot(),
            "plugins": self.plugin_runtime.snapshot(),
            "plugin_discoveries": plugin_discoveries,
            "plugin_reconciliation": plugin_reconciliation,
            "plan_artifact_refs": plan_artifact_refs,
            "resources": {
                "budget": self.spawner.resource_manager().budget(),
                "usage": self.spawner.resource_manager().usage(),
                "effective_agent_limit": self.spawner.resource_manager().effective_agent_limit(),
                "provider_signals": self.spawner.resource_manager().provider_signals(),
                "elapsed_ms": self.spawner.resource_manager().elapsed_ms(),
            },
        })
    }

    pub(super) fn intensity(&self) -> Intensity {
        self.configuration().selected_intensity
    }

    pub(super) fn configuration(&self) -> RuntimeConfiguration {
        self.runtime_configuration.snapshot()
    }

    pub(super) fn emit_config_changed(&self, compat: &ProviderCompat) {
        let configuration = self.configuration();
        let current_mode = match configuration.permission {
            PermissionMode::Plan => "plan",
            PermissionMode::Auto => "auto",
            PermissionMode::Bypass => "bypass",
        };
        self.protocol_sink
            .emit_config_changed(compat, self.has_mcp, current_mode, configuration);
    }

    pub(super) fn emit_runtime_event(&self, kind: impl Into<String>, payload: Value) {
        self.collaboration_runtime
            .emit_live_event(self.run_id.clone(), None, kind, payload);
    }

    pub(super) fn emit_command_result(
        &self,
        request_id: Option<String>,
        command: &str,
        applied: bool,
        message: Option<String>,
    ) {
        if let Some(request_id) = request_id {
            let _ = self.writer.emit(&ProtocolEvent::CommandResult {
                request_id,
                command: command.to_owned(),
                applied,
                message,
                config_results: None,
            });
        }
    }

    pub(super) fn emit_config_command_result(&self, request_id: Option<String>, outcome: ConfigUpdateOutcome) {
        if let Some(request_id) = request_id {
            let _ = self.writer.emit(&ProtocolEvent::CommandResult {
                request_id,
                command: "set_config".to_owned(),
                applied: outcome.applied,
                message: Some(outcome.message),
                config_results: Some(outcome.results),
            });
        }
    }
}

fn capabilities_for_permission(template: &Value, permission: PermissionMode) -> Value {
    let mut capabilities = template.clone();
    if let Some(object) = capabilities.as_object_mut() {
        object.insert(
            "current_mode".to_owned(),
            Value::String(permission_mode_name(permission).to_owned()),
        );
    }
    capabilities
}

fn permission_mode_name(permission: PermissionMode) -> &'static str {
    match permission {
        PermissionMode::Plan => "plan",
        PermissionMode::Auto => "auto",
        PermissionMode::Bypass => "bypass",
    }
}

#[cfg(test)]
#[path = "context_test.rs"]
mod context_test;
