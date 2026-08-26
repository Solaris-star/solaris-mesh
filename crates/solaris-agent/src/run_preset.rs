use std::sync::Arc;

use serde_json::{Value, json};
use solaris_types::identity::RunId;
use solaris_types::message::TokenUsage;
use solaris_types::permission::PermissionMode;
use solaris_types::run_preset::{Intensity, RunPreset};
use solaris_types::workflow::{CollaborationSelection, MultiAgentPolicy, WorkflowRequirement};

use crate::builtin_workflows::{DEEP_RESEARCH_WORKFLOW, STANDARD_PLAN_WORKFLOW, ULTRACODE_WORKFLOW};
use crate::plugin_tool::PluginContributionDispatcher;
use crate::role_registry::AgentRoleRegistry;
use crate::spawner::AgentSpawner;
use crate::workflow_controller::{
    WorkflowController, WorkflowNodeExecutor, WorkflowRunSnapshot, WorkflowRunStatus, WorkflowRuntimeIdentity,
    workflow_input_digest,
};
use crate::workflow_executor::AgentWorkflowExecutor;

pub fn resolve_run_preset(intensity: Intensity, supported_efforts: &[String]) -> RunPreset {
    let reasoning_effort = resolve_effort(intensity, supported_efforts);
    if intensity == Intensity::Ultracode {
        RunPreset {
            intensity,
            reasoning_effort,
            workflow_requirement: WorkflowRequirement::Required {
                workflow_id: ULTRACODE_WORKFLOW.to_owned(),
            },
            multi_agent_policy: MultiAgentPolicy::Proactive,
            collaboration: CollaborationSelection::Auto,
        }
    } else {
        RunPreset {
            intensity,
            reasoning_effort,
            workflow_requirement: WorkflowRequirement::Optional,
            multi_agent_policy: MultiAgentPolicy::OnDemand,
            collaboration: CollaborationSelection::Inherit,
        }
    }
}

/// Provider-visible guidance for the currently effective multi-Agent policy.
///
/// This is read for every model request so a Host `set_config` change takes
/// effect without rebuilding the engine or relying only on the Spawn gate.
pub(crate) const fn multi_agent_policy_instructions(policy: MultiAgentPolicy) -> &'static str {
    match policy {
        MultiAgentPolicy::Disabled => {
            "Multi-agent policy: Disabled. Do not call Spawn; every Spawn request is rejected."
        }
        MultiAgentPolicy::OnDemand => {
            "Multi-agent policy: OnDemand. Call Spawn only when the user explicitly requests sub-agents or when independent parallel work is genuinely necessary for the current task. Otherwise continue in the current Agent."
        }
        MultiAgentPolicy::Proactive => {
            "Multi-agent policy: Proactive. You may proactively divide suitable independent work among sub-agents. Do not spawn for sequential work or work that requires shared mutable state."
        }
    }
}

fn resolve_effort(intensity: Intensity, supported: &[String]) -> Option<String> {
    if supported.is_empty() {
        return None;
    }
    if matches!(intensity, Intensity::Extra | Intensity::Ultracode) {
        return supported.last().cloned();
    }
    let normalized: Vec<_> = supported.iter().map(|value| value.to_ascii_lowercase()).collect();
    let preferences: &[&str] = match intensity {
        Intensity::Low => &["low"],
        Intensity::Medium => &["medium", "low"],
        Intensity::High => &["high", "medium", "low"],
        Intensity::XHigh => &["xhigh", "x_high", "high", "medium", "low"],
        Intensity::Extra | Intensity::Ultracode => unreachable!("handled above"),
    };
    for preferred in preferences {
        if let Some(index) = normalized.iter().position(|value| value == preferred) {
            return Some(supported[index].clone());
        }
    }
    // Custom providers may advertise their own ordered effort vocabulary. The
    // contract treats the last advertised value as the strongest supported tier.
    supported.last().cloned()
}

#[derive(Debug, Clone)]
pub struct WorkflowTaskResult {
    pub workflow_run_id: RunId,
    pub snapshot: WorkflowRunSnapshot,
    pub final_output: Option<Value>,
    pub turns: usize,
    pub usage: TokenUsage,
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("{message}")]
pub struct WorkflowTaskError {
    message: String,
    pub turns: usize,
    pub usage: TokenUsage,
}

pub struct RuntimeTaskRouter {
    workflow_controller: Arc<WorkflowController>,
    role_registry: Arc<AgentRoleRegistry>,
    spawner: Arc<AgentSpawner>,
    plugins: Option<Arc<PluginContributionDispatcher>>,
}

impl RuntimeTaskRouter {
    pub fn new(
        workflow_controller: Arc<WorkflowController>,
        role_registry: Arc<AgentRoleRegistry>,
        spawner: Arc<AgentSpawner>,
    ) -> Self {
        Self {
            workflow_controller,
            role_registry,
            spawner,
            plugins: None,
        }
    }

    pub fn with_plugin_contributions(mut self, plugins: Arc<PluginContributionDispatcher>) -> Self {
        self.plugins = Some(plugins);
        self
    }

    pub async fn execute_required_workflow(
        &self,
        parent_run_id: &RunId,
        request_id: &str,
        prompt: &str,
        runtime_identity: &WorkflowRuntimeIdentity,
        permission_mode: PermissionMode,
        preset: &RunPreset,
    ) -> Result<Option<WorkflowTaskResult>, WorkflowTaskError> {
        let Some(workflow_id) = required_workflow_for_task(permission_mode, prompt, preset) else {
            return Ok(None);
        };
        let parameters = required_workflow_parameters(
            request_id,
            prompt,
            &runtime_identity.provider,
            &runtime_identity.model,
            permission_mode,
            preset,
        );
        let workflow_version = self
            .workflow_controller
            .definition(workflow_id)
            .map(|definition| definition.version)
            .ok_or_else(|| workflow_task_error(format!("unknown workflow: {workflow_id}"), 0, TokenUsage::default()))?;
        let workflow_run_id = stable_workflow_run_id(
            parent_run_id,
            request_id,
            workflow_id,
            &workflow_version,
            &runtime_identity.provider,
            &runtime_identity.model,
            &parameters,
        );
        self.workflow_controller
            .start_with_runtime(
                workflow_run_id.clone(),
                workflow_id,
                parameters,
                runtime_identity.clone(),
            )
            .map_err(|message| workflow_task_error(message, 0, TokenUsage::default()))?;
        let mut executor = AgentWorkflowExecutor::new(Arc::clone(&self.spawner), Arc::clone(&self.role_registry));
        if let Some(plugins) = &self.plugins {
            executor = executor.with_plugin_contributions(Arc::clone(plugins));
        }
        let executor = Arc::new(executor);
        let node_executor: Arc<dyn WorkflowNodeExecutor> = executor.clone();
        let snapshot = match self
            .workflow_controller
            .execute_until_settled(&workflow_run_id, node_executor)
            .await
        {
            Ok(snapshot) => snapshot,
            Err(message) => {
                let (turns, usage) = executor.usage();
                return Err(workflow_task_error(message, turns, usage));
            }
        };
        let (turns, usage) = executor.usage();
        match snapshot.status {
            WorkflowRunStatus::Completed => {}
            WorkflowRunStatus::Failed => {
                return Err(workflow_task_error(
                    format!("required workflow {} failed", snapshot.workflow_id),
                    turns,
                    usage,
                ));
            }
            WorkflowRunStatus::Cancelled => {
                return Err(workflow_task_error(
                    format!("required workflow {} was cancelled", snapshot.workflow_id),
                    turns,
                    usage,
                ));
            }
            WorkflowRunStatus::Running => {
                return Err(workflow_task_error(
                    format!("required workflow {} did not settle", snapshot.workflow_id),
                    turns,
                    usage,
                ));
            }
        }
        let final_output = self.final_output(&snapshot);
        Ok(Some(WorkflowTaskResult {
            workflow_run_id,
            snapshot,
            final_output,
            turns,
            usage,
        }))
    }

    pub fn final_output(&self, snapshot: &WorkflowRunSnapshot) -> Option<Value> {
        let definition = self.workflow_controller.definition(&snapshot.workflow_id)?;
        let preferred = definition
            .outputs
            .get("result")
            .or_else(|| definition.outputs.get("report"))
            .or_else(|| definition.outputs.get("plan"))
            .or_else(|| definition.outputs.values().next())?;
        snapshot.nodes.get(preferred)?.output.clone()
    }
}

/// Builds the exact durable input used by a required workflow. Hosts use this
/// before emitting lifecycle events so identity calculation cannot drift from
/// the router's input.
pub fn required_workflow_parameters(
    request_id: &str,
    prompt: &str,
    provider: &str,
    model: &str,
    permission_mode: PermissionMode,
    preset: &RunPreset,
) -> Value {
    let default_collaboration = match preset.multi_agent_policy {
        MultiAgentPolicy::Proactive => "team",
        MultiAgentPolicy::Disabled | MultiAgentPolicy::OnDemand => "single",
    };
    json!({
        "prompt": prompt,
        "required_workflow": true,
        "host_msg_id": request_id,
        "provider": provider,
        "model": model,
        "intensity": preset.intensity.as_str(),
        "reasoning_effort": preset.reasoning_effort.clone(),
        "multi_agent_policy": preset.multi_agent_policy,
        "collaboration": default_collaboration,
        "permission_mode": match permission_mode {
            PermissionMode::Plan => "plan",
            PermissionMode::Auto => "auto",
            PermissionMode::Bypass => "bypass",
        },
    })
}

/// Builds the stable identity shared by the Host lifecycle events, cancellation,
/// and the required-workflow router. The identity changes when the workflow
/// implementation version, provider, model, or normalized input changes,
/// while retries of the same request keep the same identity.
pub fn stable_workflow_run_id(
    parent_run_id: &RunId,
    request_id: &str,
    workflow_id: &str,
    workflow_version: &str,
    provider: &str,
    model: &str,
    parameters: &Value,
) -> RunId {
    let identity_digest = workflow_input_digest(&json!({
        "workflow_id": workflow_id,
        "workflow_version": workflow_version,
        "provider": provider,
        "model": model,
        "parameters": parameters,
    }));
    RunId::new(format!(
        "{}:workflow:{}:v3:{}",
        parent_run_id, request_id, identity_digest
    ))
}

fn workflow_task_error(message: String, turns: usize, usage: TokenUsage) -> WorkflowTaskError {
    WorkflowTaskError { message, turns, usage }
}

pub fn required_workflow_for_task<'a>(
    permission_mode: PermissionMode,
    prompt: &str,
    preset: &'a RunPreset,
) -> Option<&'a str> {
    if is_research_request(prompt) {
        return Some(DEEP_RESEARCH_WORKFLOW);
    }
    if prompt.trim_start().starts_with('/') {
        return None;
    }
    if let WorkflowRequirement::Required { workflow_id } = &preset.workflow_requirement {
        return Some(workflow_id);
    }
    if permission_mode != PermissionMode::Plan {
        return None;
    }
    Some(STANDARD_PLAN_WORKFLOW)
}

fn is_research_request(prompt: &str) -> bool {
    let prompt = prompt.trim_start();
    prompt == "/research"
        || prompt
            .strip_prefix("/research")
            .is_some_and(|remainder| remainder.chars().next().is_some_and(char::is_whitespace))
}

#[cfg(test)]
#[path = "run_preset_test.rs"]
mod tests;
