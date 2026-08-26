use std::sync::atomic::{AtomicUsize, Ordering};

use solaris_protocol::ToolApprovalManager;
use solaris_protocol::writer::{ProtocolEmitter, ProtocolWriter};
use solaris_types::identity::{AgentId, RunId};
use solaris_types::runtime::OperationEnvironmentSnapshot;

struct LaunchPolicyProbe {
    workspace: std::path::PathBuf,
    executions: Arc<AtomicUsize>,
}

#[async_trait]
impl Tool for LaunchPolicyProbe {
    fn name(&self) -> &str {
        "LaunchPolicyProbe"
    }

    fn description(&self) -> &str {
        "Tests engine-owned process launch policy"
    }

    fn input_schema(&self) -> solaris_types::tool::JsonSchema {
        json!({"type": "object"})
    }

    fn is_concurrency_safe(&self, _input: &serde_json::Value) -> bool {
        false
    }

    fn category(&self) -> ToolCategory {
        ToolCategory::Exec
    }

    async fn execute(&self, _input: serde_json::Value) -> ToolResult {
        ToolResult {
            content: "legacy execution is forbidden".to_owned(),
            is_error: true,
        }
    }

    fn prepare_effect(&self, effect_id: &str, input: &serde_json::Value) -> Result<PreparedToolEffect, String> {
        Ok(PreparedToolEffect::new(
            self.describe_effect(input),
            ToolExecutionContext::new(effect_id).with_sandbox_workspace_root(&self.workspace),
        ))
    }

    fn prepare_execution<'a>(
        &'a self,
        _input: serde_json::Value,
        context: ToolExecutionContext,
    ) -> Result<PreparedToolExecution<'a>, String> {
        let policy = context
            .process_launch_policy()
            .ok_or_else(|| "missing launch policy".to_owned())?;
        #[allow(unreachable_patterns)]
        let label = match policy {
            solaris_process::ProcessLaunchPolicy::Ambient => "ambient",
            solaris_process::ProcessLaunchPolicy::WorkspaceSandbox { .. } => "workspace-sandbox",
            _ => return Err("unexpected test-only process launch policy".to_owned()),
        };
        let executions = Arc::clone(&self.executions);
        Ok(PreparedToolExecution::new(
            None,
            Box::pin(async move {
                executions.fetch_add(1, Ordering::SeqCst);
                ToolResult {
                    content: label.to_owned(),
                    is_error: false,
                }
            }),
        ))
    }

    fn describe_effect(&self, _input: &serde_json::Value) -> EffectDescriptor {
        let mut resources = ResourceFootprint {
            file_reads: vec![self.workspace.to_string_lossy().into_owned()],
            file_writes: vec![self.workspace.to_string_lossy().into_owned()],
            process_commands: vec!["policy-probe".to_owned()],
            ..Default::default()
        };
        resources.declare_uncontained_process_access();
        EffectDescriptor {
            class: EffectClass::Process,
            action: "probe process launch policy".to_owned(),
            resources,
            replay_policy: EffectReplayPolicy::ReconcileRequired,
        }
    }
}

async fn run_launch_policy_probe(
    mode: PermissionMode,
    auto_approve: bool,
    allow_list: Vec<String>,
    approved_always: bool,
) -> (ContentBlock, usize) {
    let workspace = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let executions = Arc::new(AtomicUsize::new(0));
    let permissions = PermissionContext::new(mode, PermissionCeiling::unrestricted());
    permissions.set_boundary(ExecutionBoundary::unrestricted());
    permissions.register_protected_paths(state.path(), Vec::new()).unwrap();
    let context = EffectExecutionContext::new(
        RunId::from("launch-policy-probe-run"),
        AgentId::from("launch-policy-probe-agent"),
        Arc::new(InMemoryRuntimeLedger::default()),
        permissions,
        OperationEnvironmentSnapshot::default(),
    );
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(LaunchPolicyProbe {
        workspace: workspace.path().to_path_buf(),
        executions: Arc::clone(&executions),
    }));
    let approval_manager = Arc::new(ToolApprovalManager::new());
    approval_manager.set_mode(mode);
    if approved_always {
        approval_manager.add_auto_approve("LaunchPolicyProbe");
    }
    let writer: Arc<dyn ProtocolEmitter> = Arc::new(ProtocolWriter::new());
    let outcome = execute_tool_calls_with_approval_context(
        &registry,
        &[ContentBlock::ToolUse {
            id: "policy-probe-call".to_owned(),
            name: "LaunchPolicyProbe".to_owned(),
            input: json!({}),
            extra: None,
        }],
        &approval_manager,
        &writer,
        "policy-probe-message",
        auto_approve,
        &allow_list,
        PermissionCeiling::unrestricted(),
        &context,
        None,
        solaris_compact::CompactLevel::Off,
        false,
    )
    .await
    .unwrap();
    (
        outcome.results.into_iter().next().unwrap(),
        executions.load(Ordering::SeqCst),
    )
}

#[tokio::test]
async fn auto_approval_convenience_keeps_workspace_sandbox() {
    let cases = [
        (true, Vec::new(), false),
        (false, vec!["LaunchPolicyProbe".to_owned()], false),
        (false, Vec::new(), true),
    ];
    for (auto_approve, allow_list, approved_always) in cases {
        let (result, executions) =
            run_launch_policy_probe(PermissionMode::Auto, auto_approve, allow_list, approved_always).await;
        let ContentBlock::ToolResult { content, is_error, .. } = result else {
            panic!("probe must return a tool result")
        };
        assert!(!is_error, "{content}");
        assert_eq!(content, "workspace-sandbox");
        assert_eq!(executions, 1);
    }
}

#[tokio::test]
async fn explicit_bypass_uses_ambient_process_access() {
    let (result, executions) = run_launch_policy_probe(PermissionMode::Bypass, true, Vec::new(), false).await;
    let ContentBlock::ToolResult { content, is_error, .. } = result else {
        panic!("probe must return a tool result")
    };
    assert!(!is_error, "{content}");
    assert_eq!(content, "ambient");
    assert_eq!(executions, 1);
}

#[tokio::test]
async fn plan_mode_rejects_before_process_marker() {
    let (result, executions) = run_launch_policy_probe(PermissionMode::Plan, true, Vec::new(), false).await;
    let ContentBlock::ToolResult { is_error, .. } = result else {
        panic!("probe must return a tool result")
    };
    assert!(is_error);
    assert_eq!(executions, 0);
}
