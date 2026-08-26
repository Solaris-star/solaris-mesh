use std::collections::{HashMap, HashSet};

use serde_json::Value;
use solaris_types::workflow::{
    AgentRoleDefinition, CollaborationRuntimeConfig, CollaborationSelection, CollaborationStrategy, WorkflowDefinition,
    WorkflowNode,
};

const MAX_WORKER_ROLES: usize = 32;
const MAX_CONCURRENT_WORKERS: u32 = 64;
const MAX_SUPERVISOR_TASKS: u32 = 32;
const MAX_MULTI_AGENT_TASKS: u32 = 256;
const MAX_COORDINATOR_ROUNDS: u32 = 8;

pub(crate) fn validate_definition(definition: &WorkflowDefinition) -> Result<(), String> {
    if definition.id.trim().is_empty() {
        return Err("workflow id cannot be empty".to_owned());
    }
    if !matches!(definition.schema_version, 1 | 2) {
        return Err(format!(
            "workflow {} uses unsupported schema version {}; only versions 1 and 2 are accepted",
            definition.id, definition.schema_version
        ));
    }
    if definition.version.trim().is_empty() {
        return Err(format!("workflow {} version cannot be empty", definition.id));
    }
    if definition.description.trim().is_empty() {
        return Err(format!("workflow {} description cannot be empty", definition.id));
    }
    let ids: HashSet<_> = definition.nodes.iter().map(|node| node.id.as_str()).collect();
    if ids.len() != definition.nodes.len() {
        return Err(format!("workflow {} contains duplicate node ids", definition.id));
    }
    for node in &definition.nodes {
        if node.id.trim().is_empty() {
            return Err(format!("workflow {} contains an empty node id", definition.id));
        }
        if node
            .workflow_ref
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err(format!("workflow node {} has an empty workflow_ref", node.id));
        }
        validate_collaboration(definition.schema_version, node)?;
        for dependency in &node.depends_on {
            if !ids.contains(dependency.as_str()) {
                return Err(format!(
                    "workflow node {} depends on unknown node {dependency}",
                    node.id
                ));
            }
            if dependency == &node.id {
                return Err(format!("workflow node {} cannot depend on itself", node.id));
            }
        }
        if let Some(condition) = node.when.as_ref() {
            validate_condition(node, condition)?;
        }
        for binding in &node.output_bindings {
            if binding.to.split('.').all(|part| part.is_empty()) {
                return Err(format!(
                    "workflow node {} has an empty output binding destination",
                    node.id
                ));
            }
            let source = binding
                .from
                .split('.')
                .next()
                .filter(|value| !value.is_empty())
                .ok_or_else(|| format!("workflow node {} has an empty output binding source", node.id))?;
            if !node.depends_on.iter().any(|dependency| dependency == source) {
                return Err(format!(
                    "workflow node {} binding source {source} must be an explicit dependency",
                    node.id
                ));
            }
        }
    }
    for (output_name, node_id) in &definition.outputs {
        if output_name.trim().is_empty() || !ids.contains(node_id.as_str()) {
            return Err(format!(
                "workflow {} output {output_name:?} references unknown node {node_id}",
                definition.id
            ));
        }
    }

    let mut indegree: HashMap<&str, usize> = definition
        .nodes
        .iter()
        .map(|node| (node.id.as_str(), node.depends_on.len()))
        .collect();
    let mut children: HashMap<&str, Vec<&str>> = HashMap::new();
    for node in &definition.nodes {
        for dependency in &node.depends_on {
            children.entry(dependency.as_str()).or_default().push(node.id.as_str());
        }
    }
    let mut ready: Vec<&str> = indegree
        .iter()
        .filter_map(|(node, degree)| (*degree == 0).then_some(*node))
        .collect();
    let mut visited = 0usize;
    while let Some(node) = ready.pop() {
        visited += 1;
        for child in children.get(node).into_iter().flatten() {
            let degree = indegree.get_mut(child).expect("validated child node exists");
            *degree = degree.saturating_sub(1);
            if *degree == 0 {
                ready.push(child);
            }
        }
    }
    if visited != definition.nodes.len() {
        return Err(format!("workflow {} contains a dependency cycle", definition.id));
    }
    Ok(())
}

pub(crate) fn validate_definition_role_references(
    definition: &WorkflowDefinition,
    mut role_exists: impl FnMut(&str) -> bool,
) -> Result<(), String> {
    for node in &definition.nodes {
        if let Some(role) = node.role.as_deref()
            && !role_exists(role)
        {
            return Err(format!("workflow node {} references unknown role {role}", node.id));
        }
        let CollaborationSelection::Configured(config) = &node.collaboration else {
            continue;
        };
        for policy in &config.worker_roles {
            if !role_exists(&policy.role) {
                return Err(format!(
                    "workflow node {} collaboration references unknown role {}",
                    node.id, policy.role
                ));
            }
        }
        if let Some(primary) = config.primary_role.as_deref()
            && !role_exists(primary)
        {
            return Err(format!(
                "workflow node {} independent_reviewer references unknown primary_role {primary}",
                node.id
            ));
        }
        if let Some(reviewer) = config.reviewer_role.as_deref()
            && !role_exists(reviewer)
        {
            return Err(format!(
                "workflow node {} independent_reviewer references unknown reviewer_role {reviewer}",
                node.id
            ));
        }
    }
    Ok(())
}

fn validate_collaboration(schema_version: u32, node: &WorkflowNode) -> Result<(), String> {
    match (schema_version, &node.collaboration) {
        (1, CollaborationSelection::Configured(_)) => Err(format!(
            "workflow node {} cannot use Configured collaboration with schema version 1",
            node.id
        )),
        (1, _) => Ok(()),
        (2, CollaborationSelection::Fixed(CollaborationStrategy::Single))
        | (2, CollaborationSelection::Auto | CollaborationSelection::Inherit) => Ok(()),
        (2, CollaborationSelection::Fixed(strategy)) => Err(format!(
            "workflow node {} must use Configured collaboration for {strategy} in schema version 2",
            node.id
        )),
        (2, CollaborationSelection::Configured(config)) => validate_runtime_config(node, config),
        _ => unreachable!("schema version was validated before collaboration"),
    }
}

fn validate_runtime_config(node: &WorkflowNode, config: &CollaborationRuntimeConfig) -> Result<(), String> {
    validate_limit(
        node,
        "max_concurrent_workers",
        config.max_concurrent_workers,
        MAX_CONCURRENT_WORKERS,
    )?;
    let max_tasks = if config.strategy == CollaborationStrategy::Supervisor {
        MAX_SUPERVISOR_TASKS
    } else {
        MAX_MULTI_AGENT_TASKS
    };
    validate_limit(node, "max_tasks", config.max_tasks, max_tasks)?;
    validate_limit(
        node,
        "max_coordinator_rounds",
        config.max_coordinator_rounds,
        MAX_COORDINATOR_ROUNDS,
    )?;
    validate_limit(
        node,
        "max_pending_messages",
        config.max_pending_messages,
        CollaborationRuntimeConfig::DEFAULT_MAX_PENDING_MESSAGES,
    )?;
    validate_limit(
        node,
        "max_message_bytes",
        config.max_message_bytes,
        CollaborationRuntimeConfig::DEFAULT_MAX_MESSAGE_BYTES,
    )?;

    if config.worker_roles.len() > MAX_WORKER_ROLES {
        return Err(format!(
            "workflow node {} collaboration worker_roles must contain at most {MAX_WORKER_ROLES} entries",
            node.id
        ));
    }
    let mut worker_role_ids = HashSet::new();
    for policy in &config.worker_roles {
        if policy.role.trim().is_empty() {
            return Err(format!(
                "workflow node {} collaboration worker role must be non-empty",
                node.id
            ));
        }
        if !worker_role_ids.insert(policy.role.as_str()) {
            return Err(format!(
                "workflow node {} collaboration contains duplicate worker role {}",
                node.id, policy.role
            ));
        }
        if policy.max_concurrent == 0 {
            return Err(format!(
                "workflow node {} worker role {} max_concurrent must be greater than zero",
                node.id, policy.role
            ));
        }
        if policy.max_total == 0 {
            return Err(format!(
                "workflow node {} worker role {} max_total must be greater than zero",
                node.id, policy.role
            ));
        }
        if policy.max_concurrent > policy.max_total {
            return Err(format!(
                "workflow node {} worker role {} max_concurrent cannot exceed max_total",
                node.id, policy.role
            ));
        }
        if policy.max_total > config.max_tasks {
            return Err(format!(
                "workflow node {} worker role {} max_total cannot exceed max_tasks",
                node.id, policy.role
            ));
        }
    }

    if config.strategy != CollaborationStrategy::IndependentReviewer
        && (config.primary_role.is_some() || config.reviewer_role.is_some())
    {
        return Err(format!(
            "workflow node {} collaboration primary_role and reviewer_role are only valid for independent_reviewer",
            node.id
        ));
    }

    if matches!(
        config.strategy,
        CollaborationStrategy::Team | CollaborationStrategy::Fanout | CollaborationStrategy::IndependentReviewer
    ) {
        let total_workers: u64 = config
            .worker_roles
            .iter()
            .map(|policy| u64::from(policy.max_total))
            .sum();
        if total_workers > u64::from(config.max_tasks) {
            return Err(format!(
                "workflow node {} collaboration worker max_total sum cannot exceed max_tasks",
                node.id
            ));
        }
    }

    match config.strategy {
        CollaborationStrategy::Single => {
            if !config.worker_roles.is_empty() {
                return Err(format!(
                    "workflow node {} configured {} collaboration requires empty worker_roles",
                    node.id, config.strategy
                ));
            }
        }
        CollaborationStrategy::Supervisor => {
            if config.worker_roles.is_empty() {
                return Err(format!(
                    "workflow node {} configured {} collaboration requires at least 1 worker_roles entry",
                    node.id, config.strategy
                ));
            }
            node.role
                .as_deref()
                .filter(|role| !role.trim().is_empty())
                .ok_or_else(|| {
                    format!(
                        "workflow node {} configured {} collaboration requires a coordinator role",
                        node.id, config.strategy
                    )
                })?;
        }
        CollaborationStrategy::Team | CollaborationStrategy::Fanout => {
            if config.worker_roles.len() < 2 {
                return Err(format!(
                    "workflow node {} configured {} collaboration requires at least 2 worker_roles entries",
                    node.id, config.strategy
                ));
            }
            node.role
                .as_deref()
                .filter(|role| !role.trim().is_empty())
                .ok_or_else(|| {
                    format!(
                        "workflow node {} configured {} collaboration requires a coordinator role",
                        node.id, config.strategy
                    )
                })?;
        }
        CollaborationStrategy::IndependentReviewer => {
            validate_independent_reviewer(node, config, &worker_role_ids)?;
        }
    }
    Ok(())
}

fn validate_independent_reviewer(
    node: &WorkflowNode,
    config: &CollaborationRuntimeConfig,
    worker_role_ids: &HashSet<&str>,
) -> Result<(), String> {
    let (Some(primary), Some(reviewer)) = (
        config.primary_role.as_deref().filter(|role| !role.trim().is_empty()),
        config.reviewer_role.as_deref().filter(|role| !role.trim().is_empty()),
    ) else {
        return Err(format!(
            "workflow node {} configured independent_reviewer collaboration requires primary_role and reviewer_role",
            node.id
        ));
    };
    if primary == reviewer {
        return Err(format!(
            "workflow node {} independent_reviewer primary_role and reviewer_role must be different",
            node.id
        ));
    }
    if worker_role_ids.len() != 2 || !worker_role_ids.contains(primary) || !worker_role_ids.contains(reviewer) {
        return Err(format!(
            "workflow node {} independent_reviewer worker_roles must exactly contain primary_role and reviewer_role",
            node.id
        ));
    }
    if let Some(node_role) = node.role.as_deref()
        && node_role != reviewer
    {
        return Err(format!(
            "workflow node {} independent_reviewer role must match reviewer_role {reviewer}",
            node.id
        ));
    }
    Ok(())
}

fn validate_limit(node: &WorkflowNode, field: &str, value: u32, max: u32) -> Result<(), String> {
    if value == 0 || value > max {
        return Err(format!(
            "workflow node {} collaboration {field} must be between 1 and {max}",
            node.id
        ));
    }
    Ok(())
}

fn validate_condition(node: &WorkflowNode, condition: &Value) -> Result<(), String> {
    let condition = condition.as_object().ok_or_else(|| {
        format!(
            "workflow node {} condition must be an object with one supported operator",
            node.id
        )
    })?;
    if condition.len() != 1 {
        return Err(format!(
            "workflow node {} condition must contain exactly one supported operator",
            node.id
        ));
    }
    let (operator, rule) = condition.iter().next().expect("condition has one entry");
    match operator.as_str() {
        "always" => {
            if !rule.is_boolean() {
                return Err(format!("workflow node {} condition always must be a boolean", node.id));
            }
        }
        "all" | "any" => {
            let children = rule
                .as_array()
                .ok_or_else(|| format!("workflow node {} condition {operator} must be an array", node.id))?;
            for child in children {
                validate_condition(node, child)?;
            }
        }
        "parameter_equals" | "parameter_not_equals" => {
            validate_comparison_rule(node, operator, rule, &["field", "value"])?;
        }
        "node_output_equals" => {
            let rule = validate_comparison_rule(node, operator, rule, &["node", "field", "value"])?;
            let referenced_node = required_non_empty_string(node, operator, rule, "node")?;
            if !node.depends_on.iter().any(|dependency| dependency == referenced_node) {
                return Err(format!(
                    "workflow node {} condition source {referenced_node} must be an explicit dependency",
                    node.id
                ));
            }
        }
        _ => {
            return Err(format!(
                "workflow node {} condition uses unsupported operator {operator}",
                node.id
            ));
        }
    }
    Ok(())
}

fn validate_comparison_rule<'a>(
    node: &WorkflowNode,
    operator: &str,
    rule: &'a Value,
    expected_fields: &[&str],
) -> Result<&'a serde_json::Map<String, Value>, String> {
    let rule = rule
        .as_object()
        .ok_or_else(|| format!("workflow node {} condition {operator} must be an object", node.id))?;
    if rule.len() != expected_fields.len() || expected_fields.iter().any(|field| !rule.contains_key(*field)) {
        return Err(format!(
            "workflow node {} condition {operator} must contain exactly {}",
            node.id,
            expected_fields.join(", ")
        ));
    }
    required_non_empty_string(node, operator, rule, "field")?;
    Ok(rule)
}

fn required_non_empty_string<'a>(
    node: &WorkflowNode,
    operator: &str,
    rule: &'a serde_json::Map<String, Value>,
    field: &str,
) -> Result<&'a str, String> {
    rule.get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            format!(
                "workflow node {} condition {operator} field {field} must be a non-empty string",
                node.id
            )
        })
}

pub(crate) fn validate_role(role: &AgentRoleDefinition) -> Result<(), String> {
    if role.id.trim().is_empty() {
        return Err("workflow role id cannot be empty".to_owned());
    }
    if role.description.trim().is_empty() {
        return Err(format!("workflow role {} description cannot be empty", role.id));
    }
    if let Some(policy) = role.context_policy.as_deref()
        && !matches!(policy, "isolated" | "isolated_verification")
    {
        return Err(format!(
            "workflow role {} has unsupported context policy {policy}",
            role.id
        ));
    }
    if let Some(policy) = role.recursion_policy.as_deref()
        && !matches!(policy, "none" | "bounded")
    {
        return Err(format!(
            "workflow role {} has unsupported recursion policy {policy}",
            role.id
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "workflow_validation_test.rs"]
mod workflow_validation_test;
