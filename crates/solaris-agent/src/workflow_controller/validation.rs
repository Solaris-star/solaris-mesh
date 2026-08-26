use std::collections::{BTreeMap, HashMap, HashSet};

use serde_json::{Value, json};
use solaris_types::identity::RunId;
use solaris_types::workflow::{WorkflowDefinition, WorkflowNode, WorkflowNodeStatus};

use crate::execution_context::stable_digest_value;

use super::{
    WorkflowNodeAttempt, WorkflowRunSnapshot, WorkflowRunStatus, WorkflowRuntimeIdentity, workflow_input_digest,
};

pub(super) fn validate_workflow_run_reuse(
    existing: WorkflowRunSnapshot,
    workflow_id: &str,
    definition_digest: &str,
    input_digest: &str,
    parent_run_id: Option<&RunId>,
    runtime_identity: Option<&WorkflowRuntimeIdentity>,
) -> Result<WorkflowRunSnapshot, String> {
    if existing.workflow_id != workflow_id {
        return Err(format!(
            "workflow run {} already belongs to {}",
            existing.run_id, existing.workflow_id
        ));
    }
    if existing.parent_run_id.as_ref() != parent_run_id {
        return Err(format!(
            "workflow run {} already belongs to a different parent",
            existing.run_id
        ));
    }
    if existing.workflow_definition_digest.as_deref() != Some(definition_digest) {
        return Err(format!(
            "workflow run {} definition digest is missing or does not match the current definition",
            existing.run_id
        ));
    }
    if let Some(runtime_identity) = runtime_identity
        && existing.runtime_identity.as_ref() != Some(runtime_identity)
    {
        return Err(format!(
            "workflow run {} belongs to a different provider or model",
            existing.run_id
        ));
    }
    let persisted_digest = workflow_input_digest(&existing.parameters);
    if existing
        .input_digest
        .as_deref()
        .is_some_and(|stored_digest| stored_digest != persisted_digest)
    {
        return Err(format!(
            "workflow run {} has an invalid persisted input digest",
            existing.run_id
        ));
    }
    if persisted_digest != input_digest {
        return Err(format!(
            "workflow run {} already exists with different input",
            existing.run_id
        ));
    }
    Ok(existing)
}

pub(super) fn validate_restored_checkpoints(
    definition: &WorkflowDefinition,
    snapshot: &mut WorkflowRunSnapshot,
) -> Result<(), String> {
    if snapshot.workflow_version != definition.version {
        return Err(format!(
            "workflow {} version changed from {} to {}; explicit migration is required",
            definition.id, snapshot.workflow_version, definition.version
        ));
    }
    let current_digest = stable_digest_value(&serde_json::to_value(definition).map_err(|error| error.to_string())?);
    if snapshot.workflow_definition_digest.as_deref() != Some(current_digest.as_str()) {
        return Err(format!(
            "workflow {} definition digest changed or is missing; explicit reconciliation is required",
            definition.id
        ));
    }
    // Definitions are valid DAGs, but their serialized node order is not
    // required to be topological. Repeat until stable so invalidating an
    // upstream checkpoint also invalidates completed descendants that were
    // visited earlier in the array.
    loop {
        let mut changed = false;
        for node in &definition.nodes {
            let Some(status) = snapshot.nodes.get(&node.id).map(|attempt| attempt.status) else {
                continue;
            };
            let dependencies_settled = node.depends_on.iter().all(|dependency| {
                snapshot.nodes.get(dependency).is_some_and(|attempt| {
                    matches!(
                        attempt.status,
                        WorkflowNodeStatus::Completed | WorkflowNodeStatus::Skipped
                    )
                })
            });
            if status == WorkflowNodeStatus::Skipped {
                if !dependencies_settled {
                    reset_restored_node(snapshot, &node.id);
                    changed = true;
                }
                continue;
            }
            if status != WorkflowNodeStatus::Completed {
                continue;
            }
            let dependency_outputs: BTreeMap<_, _> = node
                .depends_on
                .iter()
                .filter_map(|dependency| {
                    snapshot
                        .nodes
                        .get(dependency)
                        .and_then(|attempt| attempt.output.clone())
                        .map(|output| (dependency.clone(), output))
                })
                .collect();
            let expected = if dependencies_settled {
                let bound_inputs = build_bound_inputs(node, &dependency_outputs)?;
                Some(stable_digest_value(&json!({
                    "parameters": snapshot.parameters,
                    "dependency_outputs": dependency_outputs,
                    "bound_inputs": bound_inputs,
                })))
            } else {
                None
            };
            let valid = snapshot
                .nodes
                .get(&node.id)
                .is_some_and(|attempt| attempt.input_digest == expected && attempt.output_ref.is_some());
            if !valid {
                reset_restored_node(snapshot, &node.id);
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    Ok(())
}

fn reset_restored_node(snapshot: &mut WorkflowRunSnapshot, node_id: &str) {
    let attempt = snapshot.nodes.get_mut(node_id).expect("definition node has state");
    attempt.status = WorkflowNodeStatus::Pending;
    attempt.output = None;
    attempt.output_ref = None;
    attempt.committed_at_unix_ms = None;
    attempt.resume_existing_attempt = false;
    snapshot.status = WorkflowRunStatus::Running;
}

pub(super) fn validate_workflow_reference_graph(graph: &HashMap<String, Vec<String>>) -> Result<(), String> {
    fn visit(
        workflow_id: &str,
        graph: &HashMap<String, Vec<String>>,
        visiting: &mut HashSet<String>,
        visited: &mut HashSet<String>,
    ) -> Result<(), String> {
        if visited.contains(workflow_id) {
            return Ok(());
        }
        if !visiting.insert(workflow_id.to_owned()) {
            return Err(format!("workflow reference graph contains a cycle at {workflow_id}"));
        }
        for referenced in graph.get(workflow_id).into_iter().flatten() {
            visit(referenced, graph, visiting, visited)?;
        }
        visiting.remove(workflow_id);
        visited.insert(workflow_id.to_owned());
        Ok(())
    }

    let mut visiting = HashSet::new();
    let mut visited = HashSet::new();
    for workflow_id in graph.keys() {
        visit(workflow_id, graph, &mut visiting, &mut visited)?;
    }
    Ok(())
}

pub(super) fn build_bound_inputs(node: &WorkflowNode, outputs: &BTreeMap<String, Value>) -> Result<Value, String> {
    let mut root = Value::Object(serde_json::Map::new());
    for binding in &node.output_bindings {
        let mut parts = binding.from.split('.');
        let source_node = parts
            .next()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| format!("workflow node {} has empty output binding source", node.id))?;
        let mut value = outputs
            .get(source_node)
            .cloned()
            .ok_or_else(|| format!("workflow node {} binding source {source_node} is unavailable", node.id))?;
        for part in parts {
            value = value
                .get(part)
                .cloned()
                .ok_or_else(|| format!("workflow node {} binding path {} is unavailable", node.id, binding.from))?;
        }
        set_json_path(&mut root, &binding.to, value)?;
    }
    Ok(root)
}

fn set_json_path(root: &mut Value, path: &str, value: Value) -> Result<(), String> {
    let parts: Vec<_> = path.split('.').filter(|part| !part.is_empty()).collect();
    if parts.is_empty() {
        return Err("output binding destination cannot be empty".to_owned());
    }
    let mut current = root;
    for part in &parts[..parts.len() - 1] {
        if !current.is_object() {
            *current = Value::Object(serde_json::Map::new());
        }
        current = current
            .as_object_mut()
            .expect("object initialized")
            .entry((*part).to_owned())
            .or_insert_with(|| Value::Object(serde_json::Map::new()));
    }
    if !current.is_object() {
        *current = Value::Object(serde_json::Map::new());
    }
    current
        .as_object_mut()
        .expect("object initialized")
        .insert(parts[parts.len() - 1].to_owned(), value);
    Ok(())
}

pub(super) fn dependencies_complete(node: &WorkflowNode, nodes: &BTreeMap<String, WorkflowNodeAttempt>) -> bool {
    node.depends_on.iter().all(|dependency| {
        nodes
            .get(dependency)
            .is_some_and(|dep| matches!(dep.status, WorkflowNodeStatus::Completed | WorkflowNodeStatus::Skipped))
    })
}

pub(super) fn condition_matches(
    condition: Option<&Value>,
    nodes: &BTreeMap<String, WorkflowNodeAttempt>,
    parameters: &Value,
) -> bool {
    let Some(condition) = condition else {
        return true;
    };
    if let Some(value) = condition.get("always").and_then(Value::as_bool) {
        return value;
    }
    if let Some(all) = condition.get("all").and_then(Value::as_array) {
        return all
            .iter()
            .all(|child| condition_matches(Some(child), nodes, parameters));
    }
    if let Some(any) = condition.get("any").and_then(Value::as_array) {
        return any
            .iter()
            .any(|child| condition_matches(Some(child), nodes, parameters));
    }
    if let Some(rule) = condition.get("parameter_equals") {
        let Some(field) = rule.get("field").and_then(Value::as_str) else {
            return false;
        };
        let Some(expected) = rule.get("value") else {
            return false;
        };
        return json_path(parameters, field).is_some_and(|value| value == expected);
    }
    if let Some(rule) = condition.get("parameter_not_equals") {
        let Some(field) = rule.get("field").and_then(Value::as_str) else {
            return false;
        };
        let Some(expected) = rule.get("value") else {
            return false;
        };
        return json_path(parameters, field).is_none_or(|value| value != expected);
    }
    let Some(rule) = condition.get("node_output_equals") else {
        return false;
    };
    let Some(node_id) = rule.get("node").and_then(Value::as_str) else {
        return false;
    };
    let Some(field) = rule.get("field").and_then(Value::as_str) else {
        return false;
    };
    let Some(expected) = rule.get("value") else {
        return false;
    };
    nodes
        .get(node_id)
        .and_then(|attempt| attempt.output.as_ref())
        .and_then(|output| json_path(output, field))
        .is_some_and(|value| value == expected)
}

fn json_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.')
        .filter(|segment| !segment.is_empty())
        .try_fold(value, |current, segment| current.get(segment))
}
