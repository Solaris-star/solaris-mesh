use super::*;

#[test]
fn builtin_workflows_validate_and_register() {
    let controller = WorkflowController::default();
    let roles = AgentRoleRegistry::default();
    register_builtin_workflows(&controller, &roles).unwrap();
    assert!(controller.definition(STANDARD_PLAN_WORKFLOW).is_some());
    assert!(controller.definition(DEEP_RESEARCH_WORKFLOW).is_some());
    assert!(controller.definition(ULTRACODE_WORKFLOW).is_some());
    assert!(roles.get("verifier").is_some());
}

#[test]
fn ultracode_has_independent_verification_and_conditional_repair() {
    let workflow = ultracode();
    assert!(workflow.nodes.iter().any(|node| node.id == "verify"));
    assert!(
        workflow
            .nodes
            .iter()
            .find(|node| node.id == "repair")
            .unwrap()
            .when
            .is_some()
    );
}

#[test]
fn ultracode_runs_plan_and_research_in_parallel_without_nested_fanout() {
    let workflow = ultracode();
    for node_id in ["plan", "research"] {
        let node = workflow.nodes.iter().find(|node| node.id == node_id).unwrap();
        assert!(
            node.depends_on.is_empty(),
            "{node_id} must remain an independent root node"
        );
        assert_eq!(
            node.collaboration,
            CollaborationSelection::Fixed(CollaborationStrategy::Single),
            "{node_id} must use one Agent instead of spawning a redundant inner fanout"
        );
    }
}

#[test]
fn ultracode_assigns_each_workspace_write_stage_to_one_agent() {
    let workflow = ultracode();
    for node_id in ["implement", "repair"] {
        let node = workflow.nodes.iter().find(|node| node.id == node_id).unwrap();
        assert_eq!(
            node.collaboration,
            CollaborationSelection::Fixed(CollaborationStrategy::Single),
            "{node_id} must not let multiple Agents write the same workspace concurrently"
        );
    }
}

#[test]
fn ultracode_uses_one_independent_verifier_per_verification_stage() {
    let workflow = ultracode();

    for node_id in ["verify", "reverify"] {
        let node = workflow
            .nodes
            .iter()
            .find(|node| node.id == node_id)
            .unwrap_or_else(|| panic!("missing {node_id} node"));
        assert_eq!(node.role.as_deref(), Some("verifier"));
        assert_eq!(
            node.collaboration,
            CollaborationSelection::Fixed(CollaborationStrategy::Single),
            "the verifier role is already independent from the implementer and must not spawn a second reviewer"
        );
        assert!(!node.permission_ceiling.workspace_mutation);
    }
}

#[test]
fn builtin_role_token_budget_can_cover_a_large_isolated_context_and_follow_up() {
    for role in builtin_roles() {
        assert!(
            role.budget.max_tokens.is_some_and(|tokens| tokens >= 128_000),
            "{} role token budget is too small for an isolated context plus follow-up",
            role.id
        );
    }
}

#[test]
fn builtin_tool_using_roles_do_not_scan_internal_runtime_state_by_default() {
    for role in builtin_roles()
        .into_iter()
        .filter(|role| !role.capability_scope.is_empty())
    {
        assert!(
            role.description.contains(".solaris") && role.description.contains("explicitly requests"),
            "{} role must keep runtime metadata out of ordinary task context",
            role.id
        );
    }
}

#[test]
fn implementer_prioritizes_the_requested_artifact_and_one_small_check() {
    let implementer = builtin_roles()
        .into_iter()
        .find(|role| role.id == "implementer")
        .expect("implementer role");
    assert!(implementer.description.contains("requested deliverable first"));
    assert!(implementer.description.contains("small data transformations"));
    assert!(implementer.description.contains("at most one smallest relevant check"));
}

#[test]
fn builtin_role_schemas_keep_stage_arrays_as_plain_strings() {
    for (role_id, fields) in [
        ("planner", &["steps", "risks", "verification"][..]),
        ("researcher", &["evidence", "constraints", "uncertainties"][..]),
        ("implementer", &["changes", "checks"][..]),
    ] {
        let schema = role_output_schema(role_id);
        for field in fields {
            assert_eq!(
                schema["properties"][field]["items"]["type"], "string",
                "{role_id}.{field} must contain plain strings"
            );
        }
    }
}

#[test]
fn builtin_roles_define_completed_as_stage_completion() {
    for role_id in ["planner", "researcher", "implementer", "synthesizer"] {
        let role = builtin_roles()
            .into_iter()
            .find(|role| role.id == role_id)
            .unwrap_or_else(|| panic!("missing {role_id} role"));
        assert!(role.description.contains("assigned stage"), "{role_id}");
        assert!(role.description.contains("completed"), "{role_id}");
        assert!(role.description.contains("plain strings"), "{role_id}");
    }
}

#[test]
fn ultracode_retries_only_non_mutating_stages() {
    let workflow = ultracode();

    for node_id in ["plan", "research", "verify", "reverify", "finalize"] {
        let node = workflow.nodes.iter().find(|node| node.id == node_id).unwrap();
        assert!(!node.permission_ceiling.workspace_mutation, "{node_id}");
        assert_eq!(node.retry.max_attempts, 2, "{node_id}");
    }

    for node_id in ["implement", "repair"] {
        let node = workflow.nodes.iter().find(|node| node.id == node_id).unwrap();
        assert!(node.permission_ceiling.workspace_mutation, "{node_id}");
        assert_eq!(node.retry.max_attempts, 1, "{node_id}");
    }
}

#[test]
fn planning_roles_stop_after_targeted_evidence_is_sufficient() {
    let roles = builtin_roles();
    let planner = roles.iter().find(|role| role.id == "planner").unwrap();
    let researcher = roles.iter().find(|role| role.id == "researcher").unwrap();

    assert!(planner.description.contains("one targeted discovery"));
    assert!(planner.description.contains("Do not inventory the workspace"));
    assert!(researcher.description.contains("one targeted Glob"));
    assert!(
        researcher
            .description
            .contains("Read each directly relevant file at most once")
    );
    assert!(researcher.description.contains("Do not inventory unrelated file types"));
}

#[test]
fn implementation_and_verification_stop_after_one_successful_check() {
    let roles = builtin_roles();
    let implementer = roles.iter().find(|role| role.id == "implementer").unwrap();
    let verifier = roles.iter().find(|role| role.id == "verifier").unwrap();

    assert!(
        implementer
            .description
            .contains("Do not create validation helper files")
    );
    assert!(implementer.description.contains("one successful verification command"));
    assert!(implementer.description.contains("return JSON immediately"));
    assert!(
        implementer
            .description
            .contains("configured shell's built-in capabilities")
    );
    assert!(implementer.description.contains("15,000 ms"));
    assert!(
        implementer
            .description
            .contains("Do not invoke an additional language runtime")
    );
    assert!(verifier.description.contains("at most one ExecCommand"));
    assert!(verifier.description.contains("return PASS immediately"));
    assert!(
        verifier
            .description
            .contains("configured shell's built-in capabilities")
    );
    assert!(verifier.description.contains("15,000 ms"));
}
