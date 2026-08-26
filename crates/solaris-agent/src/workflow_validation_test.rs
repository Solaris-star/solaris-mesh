use super::*;

use std::collections::BTreeMap;

use solaris_types::permission::PermissionCeiling;
use solaris_types::resource::ResourceBudget;
use solaris_types::workflow::{
    CollaborationRuntimeConfig, CollaborationSelection, CollaborationStrategy, ModelPolicy, RetryPolicy,
    WorkerRolePolicy,
};

fn role(id: &str) -> AgentRoleDefinition {
    AgentRoleDefinition {
        id: id.to_owned(),
        description: format!("Role {id}"),
        input_schema: None,
        output_schema: None,
        model_policy: ModelPolicy::default(),
        capability_scope: Vec::new(),
        permission_ceiling: PermissionCeiling::plan(),
        context_policy: Some("isolated".into()),
        recursion_policy: Some("none".into()),
        budget: ResourceBudget::default(),
    }
}

fn worker(role: &str) -> WorkerRolePolicy {
    WorkerRolePolicy {
        role: role.to_owned(),
        max_concurrent: 1,
        max_total: 2,
    }
}

fn config(strategy: CollaborationStrategy, worker_roles: &[&str]) -> CollaborationRuntimeConfig {
    CollaborationRuntimeConfig {
        strategy,
        worker_roles: worker_roles.iter().map(|role| worker(role)).collect(),
        ..CollaborationRuntimeConfig::default()
    }
}

fn definition(selection: CollaborationSelection, node_role: Option<&str>) -> WorkflowDefinition {
    WorkflowDefinition {
        id: "typed-collaboration".into(),
        schema_version: 2,
        version: "1".into(),
        description: "Typed collaboration validation".into(),
        roles: ["coordinator", "worker-a", "worker-b", "author", "reviewer"]
            .into_iter()
            .map(role)
            .collect(),
        parameters_schema: None,
        nodes: vec![WorkflowNode {
            id: "work".into(),
            depends_on: Vec::new(),
            when: None,
            role: node_role.map(str::to_owned),
            collaboration: selection,
            model_policy: ModelPolicy::default(),
            capability_scope: Vec::new(),
            permission_ceiling: PermissionCeiling::plan(),
            retry: RetryPolicy { max_attempts: 1 },
            timeout_ms: None,
            output_bindings: Vec::new(),
            workflow_ref: None,
        }],
        outputs: BTreeMap::new(),
    }
}

fn configured_definition(config: CollaborationRuntimeConfig, node_role: Option<&str>) -> WorkflowDefinition {
    definition(CollaborationSelection::Configured(config), node_role)
}

fn validate_inline_definition(definition: &WorkflowDefinition) -> Result<(), String> {
    validate_definition(definition)?;
    let roles: HashSet<_> = definition.roles.iter().map(|role| role.id.as_str()).collect();
    validate_definition_role_references(definition, |role| roles.contains(role))
}

fn assert_invalid(definition: &WorkflowDefinition, expected: &str) {
    let error = validate_inline_definition(definition).unwrap_err();
    assert!(error.contains(expected), "unexpected validation error: {error}");
}

#[test]
fn schema_v1_rejects_configured_collaboration_and_unknown_versions() {
    let mut configured = configured_definition(config(CollaborationStrategy::Single, &[]), None);
    configured.schema_version = 1;
    assert_invalid(&configured, "schema version 1");

    configured.schema_version = 3;
    assert_invalid(&configured, "unsupported schema version");
}

#[test]
fn schema_v2_accepts_typed_single_and_all_multi_agent_strategies() {
    let single = configured_definition(config(CollaborationStrategy::Single, &[]), None);
    validate_inline_definition(&single).unwrap();

    let supervisor = configured_definition(
        config(CollaborationStrategy::Supervisor, &["worker-a"]),
        Some("coordinator"),
    );
    validate_inline_definition(&supervisor).unwrap();

    for strategy in [CollaborationStrategy::Team, CollaborationStrategy::Fanout] {
        let definition = configured_definition(config(strategy, &["worker-a", "worker-b"]), Some("coordinator"));
        validate_inline_definition(&definition).unwrap();
    }

    let mut reviewer_config = config(CollaborationStrategy::IndependentReviewer, &["reviewer", "author"]);
    for policy in &mut reviewer_config.worker_roles {
        policy.max_total = 1;
    }
    reviewer_config.primary_role = Some("author".into());
    reviewer_config.reviewer_role = Some("reviewer".into());
    validate_inline_definition(&configured_definition(reviewer_config, Some("reviewer"))).unwrap();
}

#[test]
fn schema_v2_rejects_legacy_fixed_multi_agent_strategies() {
    validate_definition(&definition(
        CollaborationSelection::Fixed(CollaborationStrategy::Single),
        None,
    ))
    .unwrap();

    for strategy in [
        CollaborationStrategy::Supervisor,
        CollaborationStrategy::Team,
        CollaborationStrategy::Fanout,
        CollaborationStrategy::IndependentReviewer,
    ] {
        let definition = definition(CollaborationSelection::Fixed(strategy), None);
        assert_invalid(&definition, "Configured");
    }
}

#[test]
fn configured_worker_roles_must_be_non_empty_unique_and_registered() {
    for (role_name, expected) in [("", "non-empty"), ("missing", "unknown role")] {
        let definition = configured_definition(
            config(CollaborationStrategy::Supervisor, &[role_name]),
            Some("coordinator"),
        );
        assert_invalid(&definition, expected);
    }

    let duplicate = configured_definition(
        config(CollaborationStrategy::Team, &["worker-a", "worker-a"]),
        Some("coordinator"),
    );
    assert_invalid(&duplicate, "duplicate worker role");
}

#[test]
fn configured_runtime_limits_reject_zero_and_values_above_their_bounds() {
    let base = config(CollaborationStrategy::Supervisor, &["worker-a"]);
    let mut invalid = Vec::new();

    let mut value = base.clone();
    value.max_concurrent_workers = 0;
    invalid.push((value, "max_concurrent_workers"));
    let mut value = base.clone();
    value.max_concurrent_workers = 65;
    invalid.push((value, "max_concurrent_workers"));
    let mut value = base.clone();
    value.max_tasks = 0;
    invalid.push((value, "max_tasks"));
    let mut value = base.clone();
    value.max_tasks = 33;
    invalid.push((value, "max_tasks"));
    let mut value = base.clone();
    value.max_coordinator_rounds = 0;
    invalid.push((value, "max_coordinator_rounds"));
    let mut value = base.clone();
    value.max_coordinator_rounds = 9;
    invalid.push((value, "max_coordinator_rounds"));
    let mut value = base.clone();
    value.max_pending_messages = 0;
    invalid.push((value, "max_pending_messages"));
    let mut value = base.clone();
    value.max_pending_messages = 65;
    invalid.push((value, "max_pending_messages"));
    let mut value = base.clone();
    value.max_message_bytes = 0;
    invalid.push((value, "max_message_bytes"));
    let mut value = base;
    value.max_message_bytes = 65_537;
    invalid.push((value, "max_message_bytes"));

    for (config, expected) in invalid {
        assert_invalid(&configured_definition(config, Some("coordinator")), expected);
    }

    let mut team = config(CollaborationStrategy::Team, &["worker-a", "worker-b"]);
    team.max_tasks = 257;
    assert_invalid(&configured_definition(team, Some("coordinator")), "max_tasks");
}

#[test]
fn configured_runtime_limits_accept_their_inclusive_upper_bounds() {
    let mut supervisor = config(CollaborationStrategy::Supervisor, &["worker-a"]);
    supervisor.max_concurrent_workers = 64;
    validate_inline_definition(&configured_definition(supervisor, Some("coordinator"))).unwrap();

    let mut team = config(CollaborationStrategy::Team, &["worker-a", "worker-b"]);
    team.max_tasks = 256;
    for worker in &mut team.worker_roles {
        worker.max_total = 128;
    }
    validate_inline_definition(&configured_definition(team, Some("coordinator"))).unwrap();
}

#[test]
fn configured_worker_role_limits_are_consistent_with_each_other_and_max_tasks() {
    let base = config(CollaborationStrategy::Supervisor, &["worker-a"]);
    let mut invalid = Vec::new();

    let mut value = base.clone();
    value.worker_roles[0].max_concurrent = 0;
    invalid.push((value, "max_concurrent"));
    let mut value = base.clone();
    value.worker_roles[0].max_total = 0;
    invalid.push((value, "max_total"));
    let mut value = base.clone();
    value.worker_roles[0].max_concurrent = 3;
    value.worker_roles[0].max_total = 2;
    invalid.push((value, "cannot exceed max_total"));
    let mut value = base;
    value.max_tasks = 2;
    value.worker_roles[0].max_total = 3;
    invalid.push((value, "cannot exceed max_tasks"));

    for (config, expected) in invalid {
        assert_invalid(&configured_definition(config, Some("coordinator")), expected);
    }
}

#[test]
fn configured_worker_role_count_is_bounded_at_32() {
    let role_ids: Vec<_> = (0..33).map(|index| format!("worker-{index}")).collect();
    let mut definition = configured_definition(
        config(
            CollaborationStrategy::Supervisor,
            &role_ids.iter().map(String::as_str).collect::<Vec<_>>(),
        ),
        Some("coordinator"),
    );
    definition.roles.extend(role_ids.iter().map(|id| role(id)));

    assert_invalid(&definition, "worker_roles");
}

#[test]
fn configured_worker_role_count_accepts_exactly_32_registered_roles() {
    let role_ids: Vec<_> = (0..32).map(|index| format!("limit-worker-{index}")).collect();
    let mut config = config(
        CollaborationStrategy::Fanout,
        &role_ids.iter().map(String::as_str).collect::<Vec<_>>(),
    );
    config.max_tasks = 32;
    for worker in &mut config.worker_roles {
        worker.max_concurrent = 1;
        worker.max_total = 1;
    }
    let mut definition = configured_definition(config, Some("coordinator"));
    definition.roles.extend(role_ids.iter().map(|id| role(id)));

    validate_inline_definition(&definition).unwrap();
}

#[test]
fn configured_single_rejects_worker_and_reviewer_roles() {
    let with_worker = configured_definition(config(CollaborationStrategy::Single, &["worker-a"]), None);
    assert_invalid(&with_worker, "empty worker_roles");

    let mut with_primary = config(CollaborationStrategy::Single, &[]);
    with_primary.primary_role = Some("author".into());
    assert_invalid(
        &configured_definition(with_primary, None),
        "primary_role and reviewer_role",
    );

    let mut with_reviewer = config(CollaborationStrategy::Single, &[]);
    with_reviewer.reviewer_role = Some("reviewer".into());
    assert_invalid(
        &configured_definition(with_reviewer, None),
        "primary_role and reviewer_role",
    );
}

#[test]
fn configured_supervisor_requires_a_registered_coordinator_role() {
    let no_workers = configured_definition(config(CollaborationStrategy::Supervisor, &[]), Some("coordinator"));
    assert_invalid(&no_workers, "at least 1 worker_roles");

    let missing = configured_definition(config(CollaborationStrategy::Supervisor, &["worker-a"]), None);
    assert_invalid(&missing, "coordinator role");

    let unknown = configured_definition(
        config(CollaborationStrategy::Supervisor, &["worker-a"]),
        Some("missing"),
    );
    assert_invalid(&unknown, "unknown role");
}

#[test]
fn configured_team_and_fanout_require_two_workers_and_validate_optional_node_role() {
    for strategy in [CollaborationStrategy::Team, CollaborationStrategy::Fanout] {
        let too_few = configured_definition(config(strategy, &["worker-a"]), Some("coordinator"));
        assert_invalid(&too_few, "at least 2 worker_roles");

        let missing_node_role = configured_definition(config(strategy, &["worker-a", "worker-b"]), None);
        assert_invalid(&missing_node_role, "coordinator role");

        let unknown_node_role = configured_definition(config(strategy, &["worker-a", "worker-b"]), Some("missing"));
        assert_invalid(&unknown_node_role, "unknown role");
    }
}

#[test]
fn configured_independent_reviewer_requires_exact_distinct_registered_roles() {
    let mut base = config(CollaborationStrategy::IndependentReviewer, &["author", "reviewer"]);
    for policy in &mut base.worker_roles {
        policy.max_total = 1;
    }

    let mut missing_primary = base.clone();
    missing_primary.reviewer_role = Some("reviewer".into());
    assert_invalid(
        &configured_definition(missing_primary, None),
        "primary_role and reviewer_role",
    );

    let mut missing_reviewer = base.clone();
    missing_reviewer.primary_role = Some("author".into());
    assert_invalid(
        &configured_definition(missing_reviewer, None),
        "primary_role and reviewer_role",
    );

    let mut same = base.clone();
    same.primary_role = Some("author".into());
    same.reviewer_role = Some("author".into());
    assert_invalid(&configured_definition(same, None), "must be different");

    let mut unknown = base.clone();
    unknown.primary_role = Some("missing".into());
    unknown.reviewer_role = Some("reviewer".into());
    unknown.worker_roles[0].role = "missing".into();
    assert_invalid(&configured_definition(unknown, None), "unknown role");

    let mut extra = config(
        CollaborationStrategy::IndependentReviewer,
        &["author", "reviewer", "worker-a"],
    );
    for policy in &mut extra.worker_roles {
        policy.max_total = 1;
    }
    extra.primary_role = Some("author".into());
    extra.reviewer_role = Some("reviewer".into());
    assert_invalid(&configured_definition(extra, None), "exactly contain");

    let mut wrong_node_role = base.clone();
    wrong_node_role.primary_role = Some("author".into());
    wrong_node_role.reviewer_role = Some("reviewer".into());
    assert_invalid(
        &configured_definition(wrong_node_role, Some("coordinator")),
        "must match reviewer_role",
    );
}

#[test]
fn configured_independent_reviewer_accepts_multiple_instances_within_max_tasks() {
    let mut configured = config(CollaborationStrategy::IndependentReviewer, &["author", "reviewer"]);
    configured.primary_role = Some("author".into());
    configured.reviewer_role = Some("reviewer".into());
    configured.max_tasks = 5;
    configured.worker_roles[0].max_concurrent = 1;
    configured.worker_roles[0].max_total = 2;
    configured.worker_roles[1].max_concurrent = 2;
    configured.worker_roles[1].max_total = 3;

    validate_inline_definition(&configured_definition(configured.clone(), Some("reviewer"))).unwrap();

    configured.max_tasks = 4;
    assert_invalid(
        &configured_definition(configured, Some("reviewer")),
        "worker max_total sum cannot exceed max_tasks",
    );
}

#[test]
fn configured_non_reviewer_strategies_reject_reviewer_fields() {
    for strategy in [
        CollaborationStrategy::Supervisor,
        CollaborationStrategy::Team,
        CollaborationStrategy::Fanout,
    ] {
        let workers = if strategy == CollaborationStrategy::Supervisor {
            vec!["worker-a"]
        } else {
            vec!["worker-a", "worker-b"]
        };
        let mut configured = config(strategy, &workers);
        configured.primary_role = Some("author".into());
        assert_invalid(
            &configured_definition(configured, Some("coordinator")),
            "primary_role and reviewer_role are only valid",
        );
    }
}

#[test]
fn configured_team_and_fanout_bound_total_worker_instances() {
    for strategy in [CollaborationStrategy::Team, CollaborationStrategy::Fanout] {
        let mut configured = config(strategy, &["worker-a", "worker-b"]);
        configured.max_tasks = 3;
        configured.worker_roles[0].max_total = 2;
        configured.worker_roles[1].max_total = 2;
        assert_invalid(
            &configured_definition(configured, Some("coordinator")),
            "worker max_total sum cannot exceed max_tasks",
        );
    }
}

#[test]
fn structural_validation_allows_registry_only_role_references() {
    let configured = config(CollaborationStrategy::Team, &["external-a", "external-b"]);
    let definition = configured_definition(configured, Some("external-coordinator"));

    validate_definition(&definition).unwrap();
    validate_definition_role_references(&definition, |role| role.starts_with("external-")).unwrap();
    assert!(
        validate_definition_role_references(&definition, |_| false)
            .unwrap_err()
            .contains("unknown role")
    );
}
