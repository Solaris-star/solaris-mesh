use super::*;
use crate::collaboration_runtime::CollaborationRuntime;
use crate::execution_context::EffectExecutionContext;
use crate::permission_engine::PermissionContext;
use crate::plugin_provider::PluginLlmProvider;
use crate::plugin_tool::PluginContributionDispatcher;
use crate::resource_policy::ResourcePolicy;
use crate::role_registry::AgentRoleRegistry;
use crate::runtime_ledger::InMemoryRuntimeLedger;
use crate::scheduler::Scheduler;
use crate::spawner::AgentSpawner;
use crate::workflow_controller::{WorkflowController, WorkflowRunStatus};
use crate::workflow_executor::AgentWorkflowExecutor;
use async_trait::async_trait;
use solaris_config::config::{CliArgs, Config};
use solaris_providers::LlmProvider;
use solaris_providers::ProviderError;
use solaris_types::llm::{LlmEvent, LlmRequest};
use solaris_types::message::{StopReason, TokenUsage};
use solaris_types::permission::{PermissionCeiling, PermissionMode};
use solaris_types::plugin::{PluginProviderCommandResponse, PluginProviderEvent};
use solaris_types::resource::ResourceBudget;
use solaris_types::runtime::{AgentLifecycleState, AgentRecord, OperationEnvironmentSnapshot};
use solaris_types::workflow::{
    AgentRoleDefinition, CollaborationSelection, CollaborationStrategy, ModelPolicy, RetryPolicy, WorkflowDefinition,
    WorkflowNode,
};

struct FailingBuiltinProvider;

#[async_trait]
impl LlmProvider for FailingBuiltinProvider {
    async fn stream(&self, _request: &LlmRequest) -> Result<tokio::sync::mpsc::Receiver<LlmEvent>, ProviderError> {
        Err(ProviderError::Connection(
            "built-in provider must not run when the restored plugin provider is selected".to_owned(),
        ))
    }
}

fn plugin_workflow_config() -> Config {
    Config::resolve(&CliArgs {
        provider: Some("anthropic".into()),
        api_key: Some("unused".into()),
        base_url: None,
        model: Some("unused-model".into()),
        max_tokens: None,
        thinking: None,
        thinking_budget: None,
        max_turns: None,
        max_tool_call_malformed_turns: None,
        max_tool_call_failure_turns: None,
        system_prompt: None,
        profile: None,
        auto_approve: true,
        project_dir: None,
    })
    .unwrap()
}

#[test]
fn resource_escape_is_rejected() {
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::NamedTempFile::new().unwrap();
    let relative = pathdiff_for_test(root.path(), outside.path());
    assert!(resolve_resource(root.path(), &relative).is_err());
}

#[test]
fn discovered_project_plugin_requires_explicit_host_approval() {
    let workspace = tempfile::tempdir().unwrap();
    let plugin_dir = workspace.path().join(".solaris/plugins/recorded");
    std::fs::create_dir_all(&plugin_dir).unwrap();
    std::fs::write(
        plugin_dir.join("plugin.json"),
        serde_json::to_vec_pretty(&json!({
            "id": "recorded-startup-plugin",
            "version": "1.0.0",
            "source": {"kind": "local", "path": "."},
            "compatibility": {"runtime_api_version": 1}
        }))
        .unwrap(),
    )
    .unwrap();
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("startup-plugin-run");

    let bootstrap =
        PluginBootstrap::discover(workspace.path(), &run_id, &AgentId::from("root-agent"), ledger.clone()).unwrap();

    assert!(bootstrap.active_plugins().is_empty());
    assert!(bootstrap.runtime.installed("recorded-startup-plugin").is_none());
    let records = ledger.records_for_run(&run_id).unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|record| {
                record.payload.get("plugin_id").and_then(serde_json::Value::as_str) == Some("recorded-startup-plugin")
            })
            .map(|record| record.record_type.as_str())
            .collect::<Vec<_>>(),
        vec!["plugin_discovered"]
    );
}

#[test]
fn durable_plugin_activation_is_restored_without_new_lifecycle_records() {
    let workspace = tempfile::tempdir().unwrap();
    write_test_plugin(workspace.path(), "restored-plugin", "1.0.0");
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("restored-plugin-run");
    record_host_activation(workspace.path(), &run_id, ledger.as_ref(), "restored-plugin");
    let before = ledger.records_for_run(&run_id).unwrap().len();

    let restored =
        PluginBootstrap::discover(workspace.path(), &run_id, &AgentId::from("root"), ledger.clone()).unwrap();

    assert!(
        restored
            .active_plugins()
            .iter()
            .any(|plugin| plugin.definition.id == "restored-plugin")
    );
    let after_first_restore = ledger.records_for_run(&run_id).unwrap();
    assert_eq!(after_first_restore.len(), before + 1);
    assert_eq!(
        after_first_restore.last().map(|record| record.record_type.as_str()),
        Some("plugin_discovered")
    );

    PluginBootstrap::discover(workspace.path(), &run_id, &AgentId::from("root"), ledger.clone()).unwrap();
    assert_eq!(ledger.records_for_run(&run_id).unwrap().len(), before + 1);
}

#[tokio::test]
async fn durable_provider_command_executes_after_restart() {
    let workspace = tempfile::tempdir().unwrap();
    let response = serde_json::to_string(&PluginProviderCommandResponse {
        events: vec![
            PluginProviderEvent::TextDelta {
                text: r#"{"ok":true}"#.to_owned(),
            },
            PluginProviderEvent::Done {
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage {
                    input_tokens: 2,
                    output_tokens: 1,
                    ..Default::default()
                },
            },
        ],
    })
    .unwrap();
    write_provider_test_plugin(workspace.path(), "restored-provider", &response);
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("restored-provider-run");
    record_host_activation(workspace.path(), &run_id, ledger.as_ref(), "restored-provider");

    let first = PluginBootstrap::discover(workspace.path(), &run_id, &AgentId::from("root"), ledger.clone()).unwrap();
    drop(first);
    let restored =
        PluginBootstrap::discover(workspace.path(), &run_id, &AgentId::from("root"), ledger.clone()).unwrap();

    let plugin = restored.runtime.installed("restored-provider").unwrap();
    assert!(
        plugin
            .definition
            .compatibility
            .required_protocols
            .iter()
            .any(|protocol| protocol == solaris_types::plugin::PluginProviderCommandRequest::PROTOCOL)
    );
    assert!(
        restored
            .runtime
            .resolve_command_contribution(&format!("run:{run_id}"), "provider:fixture")
            .is_some()
    );
    let implementation = plugin.identity.implementation.clone();
    let context = EffectExecutionContext::new(
        run_id.clone(),
        AgentId::from("root"),
        ledger.clone(),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot {
            plugins: vec![implementation],
            ..Default::default()
        },
    );
    let dispatcher = Arc::new(PluginContributionDispatcher::new(
        Arc::clone(&restored.runtime),
        context,
        format!("run:{run_id}"),
    ));
    let provider = PluginLlmProvider::new("fixture", Arc::clone(&dispatcher)).unwrap();
    let request = LlmRequest {
        model: "restored-plugin-model".to_owned(),
        system: "Return JSON".to_owned(),
        messages: Vec::new(),
        tools: Vec::new(),
        max_tokens: Some(32),
        thinking: None,
        reasoning_effort: None,
    };

    let mut stream = provider.stream(&request).await.unwrap();
    assert!(matches!(
        stream.recv().await,
        Some(LlmEvent::TextDelta(text)) if text == r#"{"ok":true}"#
    ));
    assert!(matches!(
        stream.recv().await,
        Some(LlmEvent::Done { usage, .. }) if usage.input_tokens == 2 && usage.output_tokens == 1
    ));
    assert!(stream.recv().await.is_none());
    let runtime = Arc::new(CollaborationRuntime::new(Scheduler::new(ResourcePolicy::new(1))));
    let root_agent = AgentId::from("root");
    runtime.agents().upsert(AgentRecord {
        run_id: run_id.clone(),
        agent_id: root_agent.clone(),
        team_id: None,
        parent_agent_id: None,
        state: AgentLifecycleState::Active,
    });
    let spawner = Arc::new(
        AgentSpawner::new(
            Arc::new(FailingBuiltinProvider),
            plugin_workflow_config(),
            workspace.path().to_path_buf(),
        )
        .with_runtime_context(Arc::clone(&runtime), run_id.clone(), root_agent),
    );
    let roles = Arc::new(AgentRoleRegistry::default());
    roles.register(AgentRoleDefinition {
        id: "restored-provider-role".to_owned(),
        description: "Return JSON".to_owned(),
        input_schema: None,
        output_schema: Some(json!({
            "type": "object",
            "required": ["ok"],
            "properties": {"ok": {"type": "boolean"}}
        })),
        model_policy: ModelPolicy::default(),
        capability_scope: Vec::new(),
        permission_ceiling: PermissionCeiling::unrestricted(),
        context_policy: Some("isolated".to_owned()),
        recursion_policy: Some("none".to_owned()),
        budget: ResourceBudget::default(),
    });
    let controller = WorkflowController::with_runtime_and_roles(Arc::clone(&runtime), Some(Arc::clone(&roles)));
    controller
        .register(WorkflowDefinition {
            id: "restored-provider-workflow".to_owned(),
            schema_version: 1,
            version: "1".to_owned(),
            description: "Execute a restored provider contribution".to_owned(),
            roles: Vec::new(),
            parameters_schema: None,
            nodes: vec![WorkflowNode {
                id: "provider-node".to_owned(),
                depends_on: Vec::new(),
                when: None,
                role: Some("restored-provider-role".to_owned()),
                collaboration: CollaborationSelection::Fixed(CollaborationStrategy::Single),
                model_policy: ModelPolicy::default(),
                capability_scope: Vec::new(),
                permission_ceiling: PermissionCeiling::unrestricted(),
                retry: RetryPolicy { max_attempts: 1 },
                timeout_ms: None,
                output_bindings: Vec::new(),
                workflow_ref: None,
            }],
            outputs: Default::default(),
        })
        .unwrap();
    let workflow_run = RunId::from("restored-provider-run:workflow:e2e");
    controller
        .start(
            workflow_run.clone(),
            "restored-provider-workflow",
            json!({"plugin_contributions": {"provider": "fixture"}}),
        )
        .unwrap();
    let executor = AgentWorkflowExecutor::new(spawner, roles).with_plugin_contributions(dispatcher);

    let settled = controller
        .execute_until_settled(&workflow_run, Arc::new(executor))
        .await
        .unwrap();

    assert_eq!(settled.status, WorkflowRunStatus::Completed);
    assert_eq!(settled.nodes["provider-node"].output.as_ref().unwrap()["ok"], true);
    assert!(
        ledger
            .records_for_run(&run_id)
            .unwrap()
            .iter()
            .any(|record| record.record_type == "effect_outcome")
    );
}

#[test]
fn durable_deactivation_keeps_configured_plugin_inactive_after_restart() {
    let workspace = tempfile::tempdir().unwrap();
    write_test_plugin(workspace.path(), "inactive-plugin", "1.0.0");
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("inactive-plugin-run");
    record_host_activation(workspace.path(), &run_id, ledger.as_ref(), "inactive-plugin");
    let activation_id = format!("run:{run_id}:inactive-plugin");
    record_lifecycle(
        ledger.as_ref(),
        &run_id,
        "plugin_deactivation_intent",
        json!({"plugin_id": "inactive-plugin", "activation_id": activation_id}),
    )
    .unwrap();
    record_lifecycle(
        ledger.as_ref(),
        &run_id,
        "plugin_deactivated",
        json!({"plugin_id": "inactive-plugin", "activation_id": activation_id}),
    )
    .unwrap();

    let restored = PluginBootstrap::discover(workspace.path(), &run_id, &AgentId::from("root"), ledger).unwrap();
    assert!(restored.active_plugins().is_empty());
    assert!(restored.runtime.installed("inactive-plugin").is_some());
}

#[test]
fn host_installed_plugin_is_restored_from_terminal_lifecycle_records() {
    let workspace = tempfile::tempdir().unwrap();
    write_test_plugin(workspace.path(), "host-plugin", "1.0.0");
    let manifest = workspace.path().join(".solaris/plugins/host-plugin/plugin.json");
    let resolved = resolve_manifest_for_host(workspace.path(), &manifest).unwrap();
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("host-plugin-run");
    for (record_type, payload) in [
        (
            "plugin_install_intent",
            json!({
                "request_id": "install-1",
                "manifest_path": manifest.clone(),
                "plugin_id": "host-plugin",
                "identity": resolved.identity.clone(),
            }),
        ),
        (
            "plugin_installed",
            json!({
                "request_id": "install-1",
                "manifest_path": manifest,
                "plugin_id": "host-plugin",
                "identity": resolved.identity,
            }),
        ),
        (
            "plugin_activation_intent",
            json!({"request_id": "activate-1", "plugin_id": "host-plugin"}),
        ),
        (
            "plugin_activated",
            json!({
                "request_id": "activate-1",
                "plugin_id": "host-plugin",
                "activation_id": format!("run:{run_id}:host-plugin"),
            }),
        ),
    ] {
        record_lifecycle(ledger.as_ref(), &run_id, record_type, payload).unwrap();
    }

    let restored = PluginBootstrap::discover(workspace.path(), &run_id, &AgentId::from("root"), ledger).unwrap();
    assert_eq!(restored.active_plugins().len(), 1);
    assert_eq!(restored.active_plugins()[0].definition.id, "host-plugin");
}

#[test]
fn unfinished_activation_and_changed_identity_require_reconciliation() {
    let workspace = tempfile::tempdir().unwrap();
    write_test_plugin(workspace.path(), "changed-plugin", "1.0.0");
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("changed-plugin-run");
    record_host_activation(workspace.path(), &run_id, ledger.as_ref(), "changed-plugin");
    record_lifecycle(
        ledger.as_ref(),
        &run_id,
        "plugin_activation_intent",
        json!({
            "plugin_id": "changed-plugin",
            "activation_id": format!("run:{run_id}:changed-plugin"),
        }),
    )
    .unwrap();
    write_test_plugin(workspace.path(), "changed-plugin", "2.0.0");

    let restored =
        PluginBootstrap::discover(workspace.path(), &run_id, &AgentId::from("root"), ledger.clone()).unwrap();
    assert!(restored.active_plugins().is_empty());
    assert!(
        restored
            .warnings()
            .iter()
            .any(|warning| warning.contains("reconciliation"))
    );
    assert!(
        ledger
            .records_for_run(&run_id)
            .unwrap()
            .iter()
            .any(|record| record.record_type == "plugin_reconciliation_required")
    );
}

#[test]
fn changed_manifest_identity_is_not_silently_restored() {
    let workspace = tempfile::tempdir().unwrap();
    write_test_plugin(workspace.path(), "identity-plugin", "1.0.0");
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("identity-plugin-run");
    record_host_activation(workspace.path(), &run_id, ledger.as_ref(), "identity-plugin");
    write_test_plugin(workspace.path(), "identity-plugin", "2.0.0");

    let restored = PluginBootstrap::discover(workspace.path(), &run_id, &AgentId::from("root"), ledger).unwrap();
    assert!(restored.active_plugins().is_empty());
    assert!(
        restored
            .warnings()
            .iter()
            .any(|warning| warning.contains("implementation changed"))
    );
}

fn write_test_plugin(workspace: &Path, plugin_id: &str, version: &str) {
    let plugin_dir = workspace.join(".solaris/plugins").join(plugin_id);
    std::fs::create_dir_all(&plugin_dir).unwrap();
    std::fs::write(
        plugin_dir.join("plugin.json"),
        serde_json::to_vec_pretty(&json!({
            "id": plugin_id,
            "version": version,
            "source": {"kind": "local", "path": "."},
            "compatibility": {"runtime_api_version": 1}
        }))
        .unwrap(),
    )
    .unwrap();
}

fn write_provider_test_plugin(workspace: &Path, plugin_id: &str, response: &str) {
    let plugin_dir = workspace.join(".solaris/plugins").join(plugin_id);
    std::fs::create_dir_all(&plugin_dir).unwrap();
    let command = if cfg!(windows) {
        "provider-fixture.cmd"
    } else {
        "provider-fixture.sh"
    };
    let executable = plugin_dir.join(command);
    let contents = if cfg!(windows) {
        format!("@echo off\r\n@echo {response}\r\n")
    } else {
        format!("#!/bin/sh\nprintf '%s\\n' '{}'\n", response.replace('\'', "'\\''"))
    };
    std::fs::write(&executable, contents).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    std::fs::write(
        plugin_dir.join("plugin.json"),
        serde_json::to_vec_pretty(&json!({
            "id": plugin_id,
            "version": "1.0.0",
            "source": {"kind": "local", "path": "."},
            "capabilities": {"providers": ["fixture"]},
            "compatibility": {
                "runtime_api_version": 1,
                "required_protocols": ["provider-command-v1"]
            },
            "command_contributions": [{
                "kind": "provider",
                "name": "fixture",
                "command": command,
                "input_schema": {"type": "object"}
            }]
        }))
        .unwrap(),
    )
    .unwrap();
}

fn record_host_activation(workspace: &Path, run_id: &RunId, ledger: &dyn RuntimeLedger, plugin_id: &str) {
    let manifest = workspace.join(".solaris/plugins").join(plugin_id).join("plugin.json");
    let resolved = resolve_manifest_for_host(workspace, &manifest).unwrap();
    for (record_type, payload) in [
        (
            "plugin_install_intent",
            json!({
                "request_id": format!("install-{plugin_id}"),
                "manifest_path": manifest.clone(),
                "plugin_id": plugin_id,
                "identity": resolved.identity.clone(),
            }),
        ),
        (
            "plugin_installed",
            json!({
                "request_id": format!("install-{plugin_id}"),
                "manifest_path": manifest,
                "plugin_id": plugin_id,
                "identity": resolved.identity,
            }),
        ),
        (
            "plugin_activation_intent",
            json!({"request_id": format!("activate-{plugin_id}"), "plugin_id": plugin_id}),
        ),
        (
            "plugin_activated",
            json!({
                "request_id": format!("activate-{plugin_id}"),
                "plugin_id": plugin_id,
                "activation_id": format!("run:{run_id}:{plugin_id}"),
            }),
        ),
    ] {
        record_lifecycle(ledger, run_id, record_type, payload).unwrap();
    }
}

fn pathdiff_for_test(root: &Path, target: &Path) -> String {
    // An absolute path is rejected even before canonical containment, which
    // is sufficient for this security invariant and avoids another dep.
    let _ = root;
    target.to_string_lossy().into_owned()
}
