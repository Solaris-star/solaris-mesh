use super::*;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use solaris_skills::permissions::SkillPermissionChecker;
    use solaris_skills::types::{ExecutionContext, LoadedFrom, SkillSource};

    fn make_skill(name: &str, content: &str) -> SkillMetadata {
        SkillMetadata {
            name: name.to_string(),
            display_name: None,
            description: format!("desc of {name}"),
            has_user_specified_description: true,
            allowed_tools: Vec::new(),
            argument_hint: None,
            argument_names: Vec::new(),
            when_to_use: None,
            version: None,
            model: None,
            disable_model_invocation: false,
            user_invocable: true,
            execution_context: ExecutionContext::Inline,
            agent: None,
            effort: None,
            shell: None,
            paths: Vec::new(),
            network: Default::default(),
            hooks_raw: None,
            source: SkillSource::User,
            loaded_from: LoadedFrom::Skills,
            content: content.to_string(),
            content_length: content.len(),
            skill_root: None,
        }
    }

    fn tool_with(skills: Vec<SkillMetadata>) -> SkillTool {
        SkillTool::new(
            Arc::new(skills),
            PathBuf::from("/tmp"),
            SkillPermissionChecker::new(vec![], vec![], false),
        )
    }

    #[test]
    fn implementation_identity_changes_with_hooks_and_network_policy() {
        let original = tool_with(vec![make_skill("durable", "instructions")]);
        let original_digest = original.implementation_identity().unwrap().digest.unwrap();

        let mut changed = make_skill("durable", "instructions");
        changed.network.network_domains = vec!["api.example.com".to_owned()];
        changed.hooks_raw = Some(json!({
            "PostToolUse": [{
                "matcher": "Write",
                "hooks": [{
                    "type": "command",
                    "command": "verify-output",
                    "timeout": 17,
                    "network": {"network_domains": ["hooks.example.com"]}
                }]
            }]
        }));
        let changed_digest = tool_with(vec![changed])
            .implementation_identity()
            .unwrap()
            .digest
            .unwrap();

        assert_ne!(original_digest, changed_digest);
    }

    struct TestBypassSpawnAuthorizer;

    impl solaris_process::ProcessSpawnAuthorizer for TestBypassSpawnAuthorizer {
        fn authorize_and_spawn(
            &self,
            spawn: solaris_process::ProcessSpawn,
        ) -> std::io::Result<solaris_process::ManagedChild> {
            spawn(ProcessLaunchPolicy::Ambient)
        }
    }

    fn test_prepared_shell_executor(command_count: usize) -> PreparedSkillShellExecutor {
        let identity = skill_shell_executable_identity().unwrap();
        let executables = (0..command_count)
            .map(|_| solaris_process::pin_executable(identity.canonical_path(), &identity).unwrap())
            .collect();
        PreparedSkillShellExecutor {
            executables: Mutex::new(executables),
            launch_policy: ProcessLaunchPolicy::Ambient,
            spawn_authorization: ProcessSpawnAuthorization::new(Arc::new(TestBypassSpawnAuthorizer)),
        }
    }

    #[tokio::test]
    async fn test_skill_found_returns_content() {
        let tool = tool_with(vec![make_skill("commit", "# Commit\nDo a commit.")]);
        let result = tool.execute(json!({ "skill": "commit" })).await;
        assert!(!result.is_error);
        assert!(result.content.contains("Do a commit."));
    }

    #[tokio::test]
    async fn test_skill_not_found_returns_error() {
        let tool = tool_with(vec![make_skill("commit", "content")]);
        let result = tool.execute(json!({ "skill": "nonexistent" })).await;
        assert!(result.is_error);
        assert!(result.content.contains("not found"));
        assert!(result.content.contains("commit"));
    }

    #[tokio::test]
    async fn shared_catalog_removes_only_mcp_skills() {
        let local = make_skill("local-review", "local content");
        let mut mcp = make_skill("remote-review", "remote content");
        mcp.source = SkillSource::Mcp;
        mcp.loaded_from = LoadedFrom::Mcp;
        let catalog = SharedSkillCatalog::new(Arc::new(vec![local, mcp]));
        let tool = SkillTool::with_shared_catalog_and_spawner(
            catalog.clone(),
            PathBuf::from("/tmp"),
            SkillPermissionChecker::new(vec![], vec![], false),
            None,
            None,
        );

        let before = tool.execute(json!({ "skill": "remote-review" })).await;
        assert!(!before.is_error);
        assert_eq!(catalog.remove_mcp(), 1);

        let local = tool.execute(json!({ "skill": "local-review" })).await;
        assert!(!local.is_error);
        assert_eq!(local.content, "local content");
        let mcp = tool.execute(json!({ "skill": "remote-review" })).await;
        assert!(mcp.is_error);
        assert!(mcp.content.contains("not found"));
    }

    #[tokio::test]
    async fn test_leading_slash_stripped() {
        let tool = tool_with(vec![make_skill("commit", "body")]);
        let result = tool.execute(json!({ "skill": "/commit" })).await;
        assert!(!result.is_error);
    }

    #[tokio::test]
    async fn test_missing_skill_param_returns_error() {
        let tool = tool_with(vec![]);
        let result = tool.execute(json!({})).await;
        assert!(result.is_error);
        assert!(result.content.contains("Missing required parameter"));
    }

    #[test]
    fn multiple_embedded_shell_commands_are_rejected_before_process_authorization() {
        let tool = tool_with(vec![make_skill("multi-shell", "!`echo first`\n!`echo second`")]);
        let input = json!({"skill": "multi-shell"});

        let error = match tool.prepare_effect("multi-shell-effect", &input) {
            Ok(_) => panic!("one durable Process intent must not authorize two root spawns"),
            Err(error) => error,
        };

        assert!(error.contains("contains 2 embedded shell commands"), "{error}");
        assert!(error.contains("limit is 1"), "{error}");
    }

    #[tokio::test]
    async fn test_args_substituted() {
        let tool = tool_with(vec![make_skill("greet", "Hello $ARGUMENTS!")]);
        let result = tool.execute(json!({ "skill": "greet", "args": "world" })).await;
        assert!(!result.is_error);
        assert_eq!(result.content, "Hello world!");
    }

    #[tokio::test]
    async fn embedded_shell_uses_the_outer_host_approval_effect() {
        use solaris_types::identity::{AgentId, RunId};
        use solaris_types::message::ContentBlock;
        use solaris_types::permission::{ExecutionBoundary, PermissionCeiling};
        use solaris_types::runtime::OperationEnvironmentSnapshot;

        use crate::execution_context::EffectExecutionContext;
        use crate::orchestration::execute_tool_calls_with_approval_context;
        use crate::permission_engine::PermissionContext;
        use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};
        use solaris_protocol::commands::ApprovalScope;
        use solaris_protocol::writer::{ProtocolEmitter, ProtocolWriter};
        use solaris_protocol::{ApprovalResolution, ToolApprovalManager};
        use solaris_tools::registry::ToolRegistry;

        let command = match solaris_config::shell::default_shell().kind {
            solaris_config::shell::ShellKind::PowerShell => "Write-Output skill_effect",
            solaris_config::shell::ShellKind::Cmd => "echo skill_effect",
            _ => "printf skill_effect",
        };
        let mut skill = make_skill("shell", &format!("Result: !`{command}`"));
        skill.network.network_domains.push("api.example.com".to_owned());
        let workspace = tempfile::tempdir().unwrap();
        let run_id = RunId::from("skill-shell-run");
        let ledger = Arc::new(InMemoryRuntimeLedger::default());
        let permissions = PermissionContext::from_auto_approve(false);
        permissions.set_boundary(ExecutionBoundary::workspace(workspace.path().to_string_lossy()));
        let context = EffectExecutionContext::new(
            run_id.clone(),
            AgentId::from("skill-shell-agent"),
            ledger.clone(),
            permissions,
            OperationEnvironmentSnapshot::default(),
        );
        let tool = SkillTool::new(
            Arc::new(vec![skill]),
            workspace.path().to_path_buf(),
            SkillPermissionChecker::new(vec![], vec![], false),
        )
        .with_shell_executor(Arc::new(EffectSkillShellExecutor::new(context.clone())));
        let input = json!({"skill": "shell"});
        let descriptor = tool.describe_effect(&input);
        assert!(
            descriptor
                .resources
                .external_resources
                .iter()
                .any(|resource| resource.starts_with("skill-shell-executable:sha256:"))
        );
        assert_eq!(descriptor.resources.network_domains, ["api.example.com"]);
        let serialized = serde_json::to_string(&descriptor).unwrap();
        assert!(!serialized.contains(command));
        let mut registry = ToolRegistry::new();
        registry.register(Box::new(tool));
        let approval_manager = Arc::new(ToolApprovalManager::new());
        let approvals = Arc::clone(&approval_manager);
        let approval_task = tokio::spawn(async move {
            loop {
                if approvals.approve("skill-call", ApprovalScope::Once) == ApprovalResolution::Applied {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        });
        let writer: Arc<dyn ProtocolEmitter> = Arc::new(ProtocolWriter::new());
        let outcome = execute_tool_calls_with_approval_context(
            &registry,
            &[ContentBlock::ToolUse {
                id: "skill-call".into(),
                name: "Skill".into(),
                input,
                extra: None,
            }],
            &approval_manager,
            &writer,
            "skill-message",
            false,
            &[],
            PermissionCeiling::unrestricted(),
            &context,
            None,
            solaris_compact::CompactLevel::Off,
            false,
        )
        .await
        .expect("approved Skill should execute");
        approval_task.await.unwrap();
        let ContentBlock::ToolResult { content, is_error, .. } = &outcome.results[0] else {
            panic!("Skill execution did not return a tool result");
        };
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            assert!(!is_error, "{content}");
            assert!(content.contains("skill_effect"));
        }
        #[cfg(windows)]
        {
            assert!(
                is_error,
                "Windows must reject approved-domain Auto processes without a verified proxy runner"
            );
            assert!(content.contains("approved-domain proxy"), "{content}");
        }
        let records = ledger.records_for_run(&run_id).unwrap();
        assert!(records.iter().any(|record| record.record_type == "permission_decision"));
        assert!(records.iter().any(|record| record.record_type == "effect_intent"));
        assert!(records.iter().any(|record| record.record_type == "effect_outcome"));
        assert!(!serde_json::to_string(&records).unwrap().contains("SkillShell"));
    }

    #[tokio::test]
    async fn completed_dynamic_skill_shell_call_reuses_output_without_running_again() {
        use solaris_config::shell::ShellKind;
        use solaris_types::identity::{AgentId, RunId};
        use solaris_types::message::ContentBlock;
        use solaris_types::permission::{PermissionCeiling, PermissionMode};
        use solaris_types::runtime::OperationEnvironmentSnapshot;

        use crate::confirm::ToolConfirmer;
        use crate::execution_context::EffectExecutionContext;
        use crate::orchestration::execute_tool_calls_with_policy_context;
        use crate::permission_engine::PermissionContext;
        use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};
        use solaris_tools::registry::ToolRegistry;

        let directory = tempfile::tempdir().unwrap();
        let marker = directory.path().join("skill-recovery-marker.txt");
        let command = match solaris_config::shell::default_shell().kind {
            ShellKind::PowerShell => format!(
                "Add-Content -LiteralPath '{}' -Value run; Write-Output reusable_skill",
                marker.to_string_lossy().replace('\'', "''")
            ),
            ShellKind::Cmd => format!("echo run>>\"{}\" & echo reusable_skill", marker.display()),
            ShellKind::Bash | ShellKind::Zsh | ShellKind::Sh => format!(
                "printf 'run\\n' >> '{}'; printf reusable_skill",
                marker.to_string_lossy().replace('\'', "'\\''")
            ),
        };
        let skill = make_skill("dynamic-reuse", &format!("Result: !`{command}`"));
        let run_id = RunId::from("skill-recovery-implementation-run");
        let ledger = Arc::new(InMemoryRuntimeLedger::default());
        let context = EffectExecutionContext::new(
            run_id.clone(),
            AgentId::from("skill-agent"),
            ledger.clone(),
            PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
            OperationEnvironmentSnapshot::default(),
        );
        let tool = tool_with(vec![skill]).with_shell_executor(Arc::new(EffectSkillShellExecutor::new(context.clone())));
        let mut registry = ToolRegistry::new();
        registry.register(Box::new(tool));
        let call = ContentBlock::ToolUse {
            id: "same-skill-call".into(),
            name: "Skill".into(),
            input: json!({"skill": "dynamic-reuse"}),
            extra: None,
        };
        let confirmer = Arc::new(Mutex::new(ToolConfirmer::new(true, Vec::new())));

        for _ in 0..2 {
            let outcome = execute_tool_calls_with_policy_context(
                &registry,
                std::slice::from_ref(&call),
                &confirmer,
                PermissionMode::Bypass,
                PermissionCeiling::unrestricted(),
                &context,
                None,
                solaris_compact::CompactLevel::Off,
                false,
            )
            .await
            .unwrap();
            assert!(matches!(
                &outcome.results[0],
                ContentBlock::ToolResult { is_error: false, content, .. } if content.contains("reusable_skill")
            ));
        }

        assert_eq!(std::fs::read_to_string(marker).unwrap().lines().count(), 1);
        let records = ledger.records_for_run(&run_id).unwrap();
        assert_eq!(
            records
                .iter()
                .filter(|record| record.record_type == "effect_intent")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn changed_skill_content_does_not_reuse_same_shell_outcome_across_contexts() {
        use solaris_config::shell::ShellKind;
        use solaris_types::effect::EffectReplayPolicy;
        use solaris_types::identity::{AgentId, RunId};
        use solaris_types::message::ContentBlock;
        use solaris_types::permission::{PermissionCeiling, PermissionMode};
        use solaris_types::runtime::{OperationEnvironmentSnapshot, ToolImplementationSnapshot};

        use crate::confirm::ToolConfirmer;
        use crate::execution_context::EffectExecutionContext;
        use crate::orchestration::execute_tool_calls_with_policy_context;
        use crate::permission_engine::PermissionContext;
        use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};
        use solaris_tools::registry::ToolRegistry;

        let directory = tempfile::tempdir().unwrap();
        let marker = directory.path().join("changed-skill-recovery-marker.txt");
        let command = match solaris_config::shell::default_shell().kind {
            ShellKind::PowerShell => format!(
                "Add-Content -LiteralPath '{}' -Value run; Write-Output stable_shell",
                marker.to_string_lossy().replace('\'', "''")
            ),
            ShellKind::Cmd => format!("echo run>>\"{}\" & echo stable_shell", marker.display()),
            ShellKind::Bash | ShellKind::Zsh | ShellKind::Sh => format!(
                "printf 'run\\n' >> '{}'; printf stable_shell",
                marker.to_string_lossy().replace('\'', "'\\''")
            ),
        };
        let input = json!({"skill": "versioned-shell"});
        let call = ContentBlock::ToolUse {
            id: "versioned-skill-call".into(),
            name: "Skill".into(),
            input: input.clone(),
            extra: None,
        };
        let run_id = RunId::from("changed-skill-recovery-run");
        let ledger = Arc::new(InMemoryRuntimeLedger::default());
        let confirmer = Arc::new(Mutex::new(ToolConfirmer::new(true, Vec::new())));

        let first_tool = tool_with(vec![make_skill(
            "versioned-shell",
            &format!("text A\nResult: !`{command}`"),
        )]);
        let first_implementation = first_tool.implementation_identity().unwrap();
        let first_context = EffectExecutionContext::new(
            run_id.clone(),
            AgentId::from("skill-agent"),
            ledger.clone(),
            PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
            OperationEnvironmentSnapshot {
                tools: vec![ToolImplementationSnapshot {
                    name: "Skill".into(),
                    implementation: first_implementation,
                    schema_digest: None,
                    replay_policy: EffectReplayPolicy::Never,
                }],
                ..Default::default()
            },
        );
        let mut first_registry = ToolRegistry::new();
        first_registry.register(Box::new(first_tool));
        let first = execute_tool_calls_with_policy_context(
            &first_registry,
            std::slice::from_ref(&call),
            &confirmer,
            PermissionMode::Bypass,
            PermissionCeiling::unrestricted(),
            &first_context,
            None,
            solaris_compact::CompactLevel::Off,
            false,
        )
        .await
        .unwrap();
        assert!(matches!(
            &first.results[0],
            ContentBlock::ToolResult { is_error: false, .. }
        ));

        let second_tool = tool_with(vec![make_skill(
            "versioned-shell",
            &format!("text B\nResult: !`{command}`"),
        )]);
        let second_implementation = second_tool.implementation_identity().unwrap();
        let second_context = EffectExecutionContext::new(
            run_id.clone(),
            AgentId::from("skill-agent"),
            ledger.clone(),
            PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
            OperationEnvironmentSnapshot {
                tools: vec![ToolImplementationSnapshot {
                    name: "Skill".into(),
                    implementation: second_implementation,
                    schema_digest: None,
                    replay_policy: EffectReplayPolicy::Never,
                }],
                ..Default::default()
            },
        );
        let mut second_registry = ToolRegistry::new();
        second_registry.register(Box::new(second_tool));
        let second = execute_tool_calls_with_policy_context(
            &second_registry,
            &[call],
            &confirmer,
            PermissionMode::Bypass,
            PermissionCeiling::unrestricted(),
            &second_context,
            None,
            solaris_compact::CompactLevel::Off,
            false,
        )
        .await
        .unwrap();

        assert!(matches!(
            &second.results[0],
            ContentBlock::ToolResult { is_error: true, content, .. }
                if content.contains("environment changed")
        ));
        assert_eq!(std::fs::read_to_string(marker).unwrap().lines().count(), 1);
        let records = ledger.records_for_run(&run_id).unwrap();
        let intent = records
            .iter()
            .find(|record| record.record_type == "effect_intent")
            .unwrap();
        let environment: OperationEnvironmentSnapshot =
            serde_json::from_value(intent.payload["environment"].clone()).unwrap();
        assert!(environment.tools.iter().any(|tool| tool.name == "Skill"));
        assert!(environment.tools.iter().any(|tool| tool.name.ends_with("/Skill")));
        assert_eq!(
            records
                .iter()
                .filter(|record| record.record_type == "effect_intent")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn seventeen_shell_commands_fail_before_inspection_or_execution() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use solaris_types::identity::{AgentId, RunId};
        use solaris_types::message::ContentBlock;
        use solaris_types::permission::{PermissionCeiling, PermissionMode};
        use solaris_types::runtime::OperationEnvironmentSnapshot;

        use crate::confirm::ToolConfirmer;
        use crate::execution_context::EffectExecutionContext;
        use crate::orchestration::execute_tool_calls_with_policy_context;
        use crate::permission_engine::PermissionContext;
        use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};
        use solaris_tools::registry::ToolRegistry;

        struct ObservableInvalidShell(Arc<AtomicUsize>);

        #[async_trait]
        impl SkillShellExecutor for ObservableInvalidShell {
            async fn execute(&self, _command: &str, _cwd: &Path) -> Result<String, ShellExecutionError> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Err(ShellExecutionError::CommandFailed {
                    pattern: "invalid shell".into(),
                    output: "observable pin failure".into(),
                })
            }
        }

        let content = (0..17).map(|_| "!`echo x`").collect::<Vec<_>>().join("\n");
        let skill = make_skill("too-many-shells", &content);
        let executions = Arc::new(AtomicUsize::new(0));
        let tool =
            tool_with(vec![skill]).with_shell_executor(Arc::new(ObservableInvalidShell(Arc::clone(&executions))));
        let direct_error = match tool.prepare_execution(
            json!({"skill": "too-many-shells"}),
            ToolExecutionContext::new("too-many-direct"),
        ) {
            Ok(_) => panic!("seventeen commands must fail before execution preparation"),
            Err(error) => error,
        };
        assert!(direct_error.contains("limit is 1"), "{direct_error}");
        assert!(!direct_error.contains("pin"));

        let run_id = RunId::from("skill-command-limit-run");
        let ledger = Arc::new(InMemoryRuntimeLedger::default());
        let context = EffectExecutionContext::new(
            run_id.clone(),
            AgentId::from("skill-agent"),
            ledger.clone(),
            PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
            OperationEnvironmentSnapshot::default(),
        );
        let mut registry = ToolRegistry::new();
        registry.register(Box::new(tool));
        let call = ContentBlock::ToolUse {
            id: "too-many-shell-call".into(),
            name: "Skill".into(),
            input: json!({"skill": "too-many-shells"}),
            extra: None,
        };
        let confirmer = Arc::new(Mutex::new(ToolConfirmer::new(true, Vec::new())));

        let outcome = execute_tool_calls_with_policy_context(
            &registry,
            &[call],
            &confirmer,
            PermissionMode::Bypass,
            PermissionCeiling::unrestricted(),
            &context,
            None,
            solaris_compact::CompactLevel::Off,
            false,
        )
        .await
        .unwrap();

        assert!(matches!(
            &outcome.results[0],
            ContentBlock::ToolResult { is_error: true, content, .. }
                if content.contains("limit is 1") && !content.contains("pin")
        ));
        assert_eq!(executions.load(Ordering::SeqCst), 0);
        assert!(
            ledger
                .records_for_run(&run_id)
                .unwrap()
                .iter()
                .all(|record| record.record_type != "effect_intent")
        );
    }

    #[tokio::test]
    async fn denied_outer_skill_approval_never_starts_embedded_shell() {
        use solaris_types::identity::{AgentId, RunId};
        use solaris_types::message::ContentBlock;
        use solaris_types::permission::{ExecutionBoundary, PermissionCeiling};
        use solaris_types::runtime::OperationEnvironmentSnapshot;

        use crate::execution_context::EffectExecutionContext;
        use crate::orchestration::execute_tool_calls_with_approval_context;
        use crate::permission_engine::PermissionContext;
        use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};
        use solaris_protocol::writer::{ProtocolEmitter, ProtocolWriter};
        use solaris_protocol::{ApprovalResolution, ToolApprovalManager, ToolApprovalResult};
        use solaris_tools::registry::ToolRegistry;

        let temp = tempfile::tempdir().unwrap();
        let marker = temp.path().join("denied-skill-shell.txt");
        let command = match solaris_config::shell::default_shell().kind {
            solaris_config::shell::ShellKind::PowerShell => {
                format!("Set-Content -LiteralPath '{}' -Value denied", marker.display())
            }
            solaris_config::shell::ShellKind::Cmd => format!("echo denied>\"{}\"", marker.display()),
            _ => format!("printf denied > '{}'", marker.display()),
        };
        let run_id = RunId::from("skill-shell-denied-run");
        let ledger = Arc::new(InMemoryRuntimeLedger::default());
        let permissions = PermissionContext::from_auto_approve(false);
        permissions.set_boundary(ExecutionBoundary::workspace(temp.path().to_string_lossy()));
        let context = EffectExecutionContext::new(
            run_id.clone(),
            AgentId::from("skill-shell-denied-agent"),
            ledger.clone(),
            permissions,
            OperationEnvironmentSnapshot::default(),
        );
        let input = json!({"skill": "denied-shell"});
        let tool = tool_with(vec![make_skill("denied-shell", &format!("!`{command}`"))])
            .with_shell_executor(Arc::new(EffectSkillShellExecutor::new(context.clone())));
        let mut registry = ToolRegistry::new();
        registry.register(Box::new(tool));
        let approval_manager = Arc::new(ToolApprovalManager::new());
        let approvals = Arc::clone(&approval_manager);
        let approval_task = tokio::spawn(async move {
            loop {
                if approvals.resolve(
                    "skill-denied-call",
                    ToolApprovalResult::Denied {
                        reason: "test denial".into(),
                    },
                ) == ApprovalResolution::Applied
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        });
        let writer: Arc<dyn ProtocolEmitter> = Arc::new(ProtocolWriter::new());
        let outcome = execute_tool_calls_with_approval_context(
            &registry,
            &[ContentBlock::ToolUse {
                id: "skill-denied-call".into(),
                name: "Skill".into(),
                input,
                extra: None,
            }],
            &approval_manager,
            &writer,
            "skill-denied-message",
            false,
            &[],
            PermissionCeiling::unrestricted(),
            &context,
            None,
            solaris_compact::CompactLevel::Off,
            false,
        )
        .await
        .expect("denial should return a tool result");
        approval_task.await.unwrap();

        assert!(matches!(
            &outcome.results[0],
            ContentBlock::ToolResult { is_error: true, .. }
        ));
        assert!(!marker.exists());
        assert!(
            !ledger
                .records_for_run(&run_id)
                .unwrap()
                .iter()
                .any(|record| record.record_type == "effect_intent")
        );
    }

    #[tokio::test]
    async fn plan_mode_denies_embedded_skill_shell_before_process_start() {
        use solaris_types::identity::{AgentId, RunId};
        use solaris_types::permission::{PermissionCeiling, PermissionDecision, PermissionMode};
        use solaris_types::runtime::OperationEnvironmentSnapshot;

        use crate::execution_context::EffectExecutionContext;
        use crate::permission_engine::PermissionContext;
        use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};

        let temp = tempfile::tempdir().unwrap();
        let marker = temp.path().join("must-not-exist.txt");
        let command = match solaris_config::shell::default_shell().kind {
            solaris_config::shell::ShellKind::PowerShell => {
                format!("Set-Content -LiteralPath '{}' -Value denied", marker.display())
            }
            solaris_config::shell::ShellKind::Cmd => format!("echo denied>\"{}\"", marker.display()),
            _ => format!("printf denied > '{}'", marker.display()),
        };
        let skill = make_skill("denied-shell", &format!("!`{command}`"));
        let run_id = RunId::from("skill-shell-plan-run");
        let ledger = Arc::new(InMemoryRuntimeLedger::default());
        let context = EffectExecutionContext::new(
            run_id.clone(),
            AgentId::from("skill-shell-plan-agent"),
            ledger.clone(),
            PermissionContext::new(PermissionMode::Plan, PermissionCeiling::plan()),
            OperationEnvironmentSnapshot::default(),
        );
        let tool = tool_with(vec![skill]).with_shell_executor(Arc::new(EffectSkillShellExecutor::new(context.clone())));
        let input = json!({"skill": "denied-shell"});
        let request = context.effect_request("skill-plan-call", "Skill", &input, tool.describe_effect(&input));
        let evaluation = context.evaluate(&request);

        assert_eq!(evaluation.decision, PermissionDecision::Deny);
        assert!(!marker.exists());
        assert!(
            !ledger
                .records_for_run(&run_id)
                .unwrap()
                .iter()
                .any(|record| record.record_type == "effect_intent")
        );
    }

    #[tokio::test]
    async fn embedded_skill_shell_kills_stdout_and_stderr_floods() {
        use solaris_types::identity::{AgentId, RunId};
        use solaris_types::runtime::OperationEnvironmentSnapshot;

        use crate::execution_context::EffectExecutionContext;
        use crate::permission_engine::PermissionContext;
        use crate::runtime_ledger::InMemoryRuntimeLedger;

        let context = EffectExecutionContext::new(
            RunId::from("skill-shell-output-limit"),
            AgentId::from("skill-shell-output-agent"),
            Arc::new(InMemoryRuntimeLedger::default()),
            PermissionContext::new(
                solaris_types::permission::PermissionMode::Bypass,
                solaris_types::permission::PermissionCeiling::unrestricted(),
            ),
            OperationEnvironmentSnapshot::default(),
        );
        let _ = context;
        let executor = test_prepared_shell_executor(2);
        let shell = solaris_config::shell::default_shell();
        let (stdout_command, stderr_command) = match shell.kind {
            solaris_config::shell::ShellKind::PowerShell => (
                "[Console]::Out.Write('x' * 600000)".to_owned(),
                "[Console]::Error.Write('x' * 200000)".to_owned(),
            ),
            solaris_config::shell::ShellKind::Cmd => (
                "powershell -NoProfile -Command \"[Console]::Out.Write('x' * 600000)\"".to_owned(),
                "powershell -NoProfile -Command \"[Console]::Error.Write('x' * 200000)\"".to_owned(),
            ),
            _ => (
                "yes x | head -c 600000".to_owned(),
                "yes x | head -c 200000 >&2".to_owned(),
            ),
        };

        let stdout_error = executor
            .execute(&stdout_command, &std::env::temp_dir())
            .await
            .expect_err("stdout flood must be terminated")
            .to_string();
        assert!(stdout_error.contains("combined output exceeded"), "{stdout_error}");
        let stderr_error = executor
            .execute(&stderr_command, &std::env::temp_dir())
            .await
            .expect_err("stderr flood must be terminated")
            .to_string();
        assert!(stderr_error.contains("combined output exceeded"), "{stderr_error}");
    }

    #[tokio::test]
    async fn embedded_skill_output_limit_kills_background_descendant() {
        use solaris_types::identity::{AgentId, RunId};
        use solaris_types::runtime::OperationEnvironmentSnapshot;

        use crate::execution_context::EffectExecutionContext;
        use crate::permission_engine::PermissionContext;
        use crate::runtime_ledger::InMemoryRuntimeLedger;

        let directory = tempfile::tempdir().unwrap();
        let marker = directory.path().join("descendant-must-not-survive");
        let context = EffectExecutionContext::new(
            RunId::from("skill-shell-descendant-limit"),
            AgentId::from("skill-shell-descendant-agent"),
            Arc::new(InMemoryRuntimeLedger::default()),
            PermissionContext::new(
                solaris_types::permission::PermissionMode::Bypass,
                solaris_types::permission::PermissionCeiling::unrestricted(),
            ),
            OperationEnvironmentSnapshot::default(),
        );
        let _ = context;
        let executor = test_prepared_shell_executor(1);
        let shell = solaris_config::shell::default_shell();
        let command = match shell.kind {
            solaris_config::shell::ShellKind::PowerShell => format!(
                "$p = Start-Process -FilePath (Get-Process -Id $PID).Path -ArgumentList '-NoProfile', '-Command', \"Start-Sleep -Seconds 2; Set-Content -LiteralPath '{}' -Value survived\" -PassThru -WindowStyle Hidden; [Console]::Out.Write('x' * 600000)",
                marker.to_string_lossy().replace('\'', "''")
            ),
            solaris_config::shell::ShellKind::Cmd => format!(
                "start /b cmd /c \"ping -n 3 127.0.0.1 >nul & echo survived>\\\"{}\\\"\" & powershell -NoProfile -Command \"[Console]::Out.Write('x' * 600000)\"",
                marker.display()
            ),
            _ => format!(
                "(sleep 2; printf survived > '{}') & yes x | head -c 600000",
                marker.to_string_lossy().replace('\'', "'\\''")
            ),
        };

        let error = executor
            .execute(&command, directory.path())
            .await
            .expect_err("output overflow must terminate the skill process tree")
            .to_string();
        assert!(error.contains("combined output exceeded"), "{error}");
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        assert!(!marker.exists(), "background descendant survived output termination");
    }

    #[tokio::test]
    async fn test_fork_skill_returns_error() {
        let mut skill = make_skill("fork-skill", "body");
        skill.execution_context = ExecutionContext::Fork;
        let tool = tool_with(vec![skill]);
        let result = tool.execute(json!({ "skill": "fork-skill" })).await;
        assert!(result.is_error);
        assert!(result.content.contains("fork execution context"));
    }

    #[test]
    fn test_describe_with_args() {
        let tool = tool_with(vec![]);
        let desc = tool.describe(&json!({ "skill": "commit", "args": "fix bug" }));
        assert_eq!(desc, "Skill commit fix bug");
    }

    #[test]
    fn test_describe_without_args() {
        let tool = tool_with(vec![]);
        let desc = tool.describe(&json!({ "skill": "commit" }));
        assert_eq!(desc, "Skill commit");
    }

    #[test]
    fn test_name_is_skill() {
        let tool = tool_with(vec![]);
        assert_eq!(tool.name(), "Skill");
    }

    #[test]
    fn test_not_concurrency_safe() {
        let tool = tool_with(vec![]);
        assert!(!tool.is_concurrency_safe(&json!({})));
    }
}

// ---------------------------------------------------------------------------
// Supplemental tests (tester role — covers test-plan.md cases not in impl tests)
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "skill_tool_supplemental_test.rs"]
mod supplemental_tests;

// ---------------------------------------------------------------------------
// Phase 6 supplemental tests — context_modifier_for() and session_id
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "skill_tool_context_modifier_test.rs"]
mod supplemental_tests_p6;

// ---------------------------------------------------------------------------
// Permission integration tests (P5-11, P5-12)
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "skill_tool_permission_test.rs"]
mod permission_tests;

// ---------------------------------------------------------------------------
// Phase 7 tests — SkillTool fork branch, context_modifier_for fork=None, permissions
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "skill_tool_fork_test.rs"]
mod phase7_tests;

// ---------------------------------------------------------------------------
// Phase 11 tests — skill_hooks_for() (TC-11.40 ~ TC-11.45)
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "skill_tool_hooks_test.rs"]
mod phase11_tests;

#[cfg(test)]
#[path = "skill_tool_process_policy_test.rs"]
mod process_policy_tests;
