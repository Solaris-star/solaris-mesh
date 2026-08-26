use crate::runtime_ledger::InMemoryRuntimeLedger;

struct FileExecutionCase<'case> {
    call_id: &'case str,
    tool_name: &'case str,
    input: Value,
    approved_mode: PermissionMode,
    current_mode: PermissionMode,
    mode_after_revalidation: PermissionMode,
    issue_lease: bool,
}

async fn execute_revalidated_file_tool(
    context: &EffectExecutionContext,
    permissions: &PermissionContext,
    registry: &ToolRegistry,
    case: FileExecutionCase<'_>,
) -> solaris_types::tool::ToolResult {
    execute_revalidated_file_tool_classified(context, permissions, registry, case)
        .await
        .into_legacy()
}

async fn execute_revalidated_file_tool_classified(
    context: &EffectExecutionContext,
    permissions: &PermissionContext,
    registry: &ToolRegistry,
    case: FileExecutionCase<'_>,
) -> solaris_types::tool::ClassifiedToolResult {
    let tool = registry.get(case.tool_name).expect("registered file tool");
    let (descriptor, prepared_context) = tool
        .prepare_effect(context.effect_id_for_call(case.call_id).as_str(), &case.input)
        .expect("prepare file effect")
        .into_parts();
    let request = context.effect_request(case.call_id, case.tool_name, &case.input, descriptor);
    if case.issue_lease {
        context
            .issue_approval_lease(&request, false)
            .expect("issue explicit approval lease");
    }
    let _registration = context.remember_approved_request_with_tool_context(
        request,
        prepared_context,
        case.approved_mode,
        PermissionCeiling::unrestricted(),
    );
    permissions.set_mode(case.current_mode);
    let approved = context
        .take_approved_request_for_call(case.call_id)
        .expect("consume approved request");
    let execution = context.revalidate(registry, &approved).expect("revalidate file effect");

    // This change happens after revalidation. The prepared file tool must use
    // the effective mode retained in `execution`, not this live value.
    permissions.set_mode(case.mode_after_revalidation);
    tool.prepare_execution(case.input, execution)
        .expect("prepare pinned file execution")
        .execute_classified()
        .await
}

fn file_policy_fixture(
    label: &str,
) -> (
    tempfile::TempDir,
    PathBuf,
    PathBuf,
    PermissionContext,
    EffectExecutionContext,
    ToolRegistry,
) {
    use solaris_tools::edit::EditTool;
    use solaris_tools::glob::GlobTool;
    use solaris_tools::grep::GrepTool;
    use solaris_tools::read::ReadTool;
    use solaris_tools::write::WriteTool;

    let directory = tempfile::tempdir().expect("tempdir");
    let workspace = directory.path().join("workspace");
    let outside = directory.path().join("outside");
    let runtime = workspace.join(".solaris/runtime");
    std::fs::create_dir_all(&runtime).expect("runtime directory");
    std::fs::create_dir(&outside).expect("outside directory");
    std::fs::write(workspace.join("public.txt"), "public-search-marker").expect("public file");
    let protected = runtime.join("ledger.sqlite3");
    std::fs::write(&protected, "protected-search-marker").expect("protected file");
    std::fs::write(outside.join("outside.txt"), "outside-search-marker").expect("outside file");

    let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    permissions.set_boundary(ExecutionBoundary::workspace(workspace.to_string_lossy()));
    permissions
        .register_protected_paths(&runtime, vec![protected])
        .expect("register protected runtime state");
    let policy = permissions.workspace_search_policy();
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(ReadTool::new_with_search_policy(
        None,
        &workspace,
        Arc::clone(&policy),
    )));
    registry.register(Box::new(WriteTool::new_with_search_policy(
        None,
        &workspace,
        Arc::clone(&policy),
    )));
    registry.register(Box::new(EditTool::new_with_search_policy(
        None,
        &workspace,
        Arc::clone(&policy),
    )));
    registry.register(Box::new(GlobTool::new_with_search_policy(
        workspace.clone(),
        Arc::clone(&policy),
    )));
    registry.register(Box::new(GrepTool::new_with_search_policy(workspace.clone(), policy)));
    let context = EffectExecutionContext::new(
        RunId::from(format!("file-policy-{label}")),
        AgentId::from("file-policy-agent"),
        Arc::new(InMemoryRuntimeLedger::default()),
        permissions.clone(),
        OperationEnvironmentSnapshot::default(),
    );
    (directory, workspace, outside, permissions, context, registry)
}

#[tokio::test]
async fn file_search_pins_effective_auto_across_both_mode_change_directions() {
    for (index, (approved_mode, current_mode)) in [
        (PermissionMode::Auto, PermissionMode::Bypass),
        (PermissionMode::Bypass, PermissionMode::Auto),
    ]
    .into_iter()
    .enumerate()
    {
        let (_directory, _workspace, _outside, permissions, context, registry) =
            file_policy_fixture(&format!("protected-{index}"));
        let glob = execute_revalidated_file_tool(
            &context,
            &permissions,
            &registry,
            FileExecutionCase {
                call_id: &format!("glob-{index}"),
                tool_name: "Glob",
                input: json!({"path": ".", "pattern": "**/*.txt"}),
                approved_mode,
                current_mode,
                mode_after_revalidation: PermissionMode::Bypass,
                issue_lease: false,
            },
        )
        .await;
        assert!(!glob.is_error, "{}", glob.content);
        assert!(glob.content.contains("public.txt"));
        assert!(!glob.content.contains("ledger.sqlite3"));

        let grep = execute_revalidated_file_tool(
            &context,
            &permissions,
            &registry,
            FileExecutionCase {
                call_id: &format!("grep-{index}"),
                tool_name: "Grep",
                input: json!({"path": ".", "pattern": "protected-search-marker"}),
                approved_mode,
                current_mode,
                mode_after_revalidation: PermissionMode::Bypass,
                issue_lease: false,
            },
        )
        .await;
        assert!(!grep.is_error, "{}", grep.content);
        assert!(!grep.content.contains("protected-search-marker"));
    }
}

#[tokio::test]
async fn effective_auto_and_plan_reject_ambient_search_after_live_bypass_switch() {
    for (index, (approved_mode, current_mode)) in [
        (PermissionMode::Auto, PermissionMode::Bypass),
        (PermissionMode::Bypass, PermissionMode::Auto),
        (PermissionMode::Plan, PermissionMode::Bypass),
    ]
    .into_iter()
    .enumerate()
    {
        let (_directory, _workspace, outside, permissions, context, registry) =
            file_policy_fixture(&format!("ambient-denied-{index}"));
        for tool_name in ["Glob", "Grep"] {
            let input = if tool_name == "Glob" {
                json!({"path": outside, "pattern": "*.txt"})
            } else {
                json!({"path": outside, "pattern": "outside-search-marker"})
            };
            let result = execute_revalidated_file_tool(
                &context,
                &permissions,
                &registry,
                FileExecutionCase {
                    call_id: &format!("ambient-{tool_name}-{index}"),
                    tool_name,
                    input,
                    approved_mode,
                    current_mode,
                    mode_after_revalidation: PermissionMode::Bypass,
                    issue_lease: true,
                },
            )
            .await;
            assert!(result.is_error, "{tool_name} unexpectedly accessed an ambient path");
            assert!(!result.content.contains("outside-search-marker"));
        }
    }
}

#[tokio::test]
async fn stable_bypass_allows_ambient_file_tools() {
    let (_directory, _workspace, outside, permissions, context, registry) = file_policy_fixture("ambient-bypass");
    let read = execute_revalidated_file_tool(
        &context,
        &permissions,
        &registry,
        FileExecutionCase {
            call_id: "bypass-Read",
            tool_name: "Read",
            input: json!({"file_path": outside.join("outside.txt")}),
            approved_mode: PermissionMode::Bypass,
            current_mode: PermissionMode::Bypass,
            mode_after_revalidation: PermissionMode::Bypass,
            issue_lease: false,
        },
    )
    .await;
    assert!(!read.is_error, "{}", read.content);
    assert!(read.content.contains("outside-search-marker"));

    let written = outside.join("written.txt");
    let write = execute_revalidated_file_tool(
        &context,
        &permissions,
        &registry,
        FileExecutionCase {
            call_id: "bypass-Write",
            tool_name: "Write",
            input: json!({"file_path": &written, "content": "before-edit"}),
            approved_mode: PermissionMode::Bypass,
            current_mode: PermissionMode::Bypass,
            mode_after_revalidation: PermissionMode::Bypass,
            issue_lease: false,
        },
    )
    .await;
    assert!(!write.is_error, "{}", write.content);

    let edit = execute_revalidated_file_tool(
        &context,
        &permissions,
        &registry,
        FileExecutionCase {
            call_id: "bypass-Edit",
            tool_name: "Edit",
            input: json!({"file_path": &written, "old_string": "before-edit", "new_string": "after-edit"}),
            approved_mode: PermissionMode::Bypass,
            current_mode: PermissionMode::Bypass,
            mode_after_revalidation: PermissionMode::Bypass,
            issue_lease: false,
        },
    )
    .await;
    assert!(!edit.is_error, "{}", edit.content);
    assert_eq!(std::fs::read_to_string(&written).unwrap(), "after-edit");

    for tool_name in ["Glob", "Grep"] {
        let input = if tool_name == "Glob" {
            json!({"path": outside, "pattern": "*.txt"})
        } else {
            json!({"path": outside, "pattern": "outside-search-marker"})
        };
        let result = execute_revalidated_file_tool(
            &context,
            &permissions,
            &registry,
            FileExecutionCase {
                call_id: &format!("bypass-{tool_name}"),
                tool_name,
                input,
                approved_mode: PermissionMode::Bypass,
                current_mode: PermissionMode::Bypass,
                mode_after_revalidation: PermissionMode::Bypass,
                issue_lease: false,
            },
        )
        .await;
        assert!(!result.is_error, "{}", result.content);
        assert!(result.content.contains(if tool_name == "Glob" {
            "outside.txt"
        } else {
            "outside-search-marker"
        }));
    }
}

#[tokio::test]
async fn root_and_child_contexts_share_verified_read_evidence() {
    use solaris_tools::read::ReadTool;
    use solaris_tools::read_only_evidence::ReadOnlyEvidenceIndex;
    use solaris_types::tool::ToolResultStatus;

    let workspace = tempfile::tempdir().unwrap();
    let path = workspace.path().join("shared.txt");
    std::fs::write(&path, "shared-across-agents\n").unwrap();
    let evidence = Arc::new(ReadOnlyEvidenceIndex::default());
    let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    permissions.set_boundary(ExecutionBoundary::workspace(workspace.path().to_string_lossy()));
    let policy = permissions.workspace_search_policy();
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(
        ReadTool::new_with_search_policy(None, workspace.path(), policy)
            .with_read_only_evidence_index(Arc::clone(&evidence)),
    ));
    let environment = OperationEnvironmentSnapshot::default();
    let run_id = RunId::from("shared-evidence-run");
    let root_context = EffectExecutionContext::new(
        run_id.clone(),
        AgentId::from("root-agent"),
        Arc::new(InMemoryRuntimeLedger::default()),
        permissions.clone(),
        environment.clone(),
    );
    let child_context = EffectExecutionContext::new(
        run_id,
        AgentId::from("child-agent"),
        Arc::new(InMemoryRuntimeLedger::default()),
        permissions.clone(),
        environment,
    );
    let input = json!({"file_path": path});

    let root = execute_revalidated_file_tool_classified(
        &root_context,
        &permissions,
        &registry,
        FileExecutionCase {
            call_id: "root-read",
            tool_name: "Read",
            input: input.clone(),
            approved_mode: PermissionMode::Auto,
            current_mode: PermissionMode::Auto,
            mode_after_revalidation: PermissionMode::Auto,
            issue_lease: false,
        },
    )
    .await;
    let child = execute_revalidated_file_tool_classified(
        &child_context,
        &permissions,
        &registry,
        FileExecutionCase {
            call_id: "child-read",
            tool_name: "Read",
            input,
            approved_mode: PermissionMode::Auto,
            current_mode: PermissionMode::Auto,
            mode_after_revalidation: PermissionMode::Auto,
            issue_lease: false,
        },
    )
    .await;

    assert_eq!(root.status, ToolResultStatus::Executed);
    assert_eq!(child.status, ToolResultStatus::CacheHit);
    assert_eq!(child.content, root.content);
}
