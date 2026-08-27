mod completion_source;
mod error;
mod node_state;
mod projection;
mod restore;
mod validation;

pub use self::error::WorkflowNodeError;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use solaris_types::identity::{AttemptId, OperationId, RunId, TaskId};
use solaris_types::plugin::ImplementationIdentity;
use solaris_types::runtime::{TaskFailureClass, TaskRecord, TaskState};
use solaris_types::workflow::{WorkflowDefinition, WorkflowNode, WorkflowNodeStatus, WorkflowTaskHandle};
use uuid::Uuid;

use crate::collaboration_runtime::CollaborationRuntime;
use crate::execution_context::stable_digest_value;
use crate::runtime_ledger::{InMemoryRuntimeLedger, RunMutationCoordinator, RuntimeLedger};
use crate::schema_validation::validate_value;
use crate::task_registry::TaskRegistry;
use crate::workflow_validation::{validate_definition, validate_definition_role_references, validate_role};

use self::node_state::{WorkflowTaskUpdate, workflow_task_id};
use self::validation::{
    condition_matches, dependencies_complete, validate_workflow_reference_graph, validate_workflow_run_reuse,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowRunStatus {
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowNodeAttempt {
    pub node_id: String,
    pub attempt_id: AttemptId,
    pub attempt_number: u32,
    pub status: WorkflowNodeStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_digest: Option<String>,
    #[serde(default, skip_serializing)]
    pub output: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub committed_at_unix_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_class: Option<TaskFailureClass>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub resume_existing_attempt: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deferred_task_terminal_write: Option<DeferredTaskTerminalWrite>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeferredTaskTerminalWrite {
    pub state: TaskState,
    pub clear_owner: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome_ref: Option<String>,
    pub failure_class: TaskFailureClass,
    pub operation_id: OperationId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowRunSnapshot {
    pub run_id: RunId,
    pub workflow_id: String,
    pub workflow_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_definition_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_identity: Option<WorkflowRuntimeIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_run_id: Option<RunId>,
    pub status: WorkflowRunStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconciliation_reason: Option<String>,
    pub nodes: BTreeMap<String, WorkflowNodeAttempt>,
    #[serde(default)]
    pub parameters: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowRuntimeIdentity {
    pub provider: String,
    pub model: String,
}

impl WorkflowRuntimeIdentity {
    pub fn new(provider: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            provider: provider.into(),
            model: model.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkflowStartOutcome {
    pub snapshot: WorkflowRunSnapshot,
    pub started: bool,
}

#[derive(Debug, Clone, PartialEq)]
struct WorkflowProjection {
    snapshot: WorkflowRunSnapshot,
    durable_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct WorkflowDefinitionSnapshot {
    #[serde(flatten)]
    pub definition: WorkflowDefinition,
    pub enabled: bool,
    pub generation: String,
}

#[derive(Debug, Clone)]
pub struct WorkflowExecutionContext {
    pub run_id: RunId,
    pub workflow: ImplementationIdentity,
    pub node: WorkflowNode,
    pub attempt_id: AttemptId,
    pub parameters: Value,
    pub dependency_outputs: BTreeMap<String, Value>,
    pub bound_inputs: Value,
}

pub(crate) fn workflow_input_digest(parameters: &Value) -> String {
    stable_digest_value(&normalize_workflow_parameters(parameters))
}

fn normalize_workflow_parameters(value: &Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.iter().map(normalize_workflow_parameters).collect()),
        Value::Object(object) => {
            let mut entries: Vec<_> = object.iter().collect();
            entries.sort_by_key(|(key, _)| *key);
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key.clone(), normalize_workflow_parameters(value)))
                    .collect(),
            )
        }
        _ => value.clone(),
    }
}

#[async_trait]
pub trait WorkflowNodeExecutor: Send + Sync {
    async fn execute(&self, context: WorkflowExecutionContext) -> Result<Value, WorkflowNodeError>;
}

pub struct WorkflowController {
    lifecycle: Mutex<()>,
    definitions: RwLock<HashMap<String, WorkflowDefinition>>,
    definition_owners: RwLock<HashMap<String, HashSet<String>>>,
    disabled_definitions: RwLock<HashSet<String>>,
    runs: RwLock<HashMap<RunId, WorkflowProjection>>,
    ledger: Arc<dyn RuntimeLedger>,
    mutation: Arc<RunMutationCoordinator>,
    mutation_owner_id: String,
    tasks: Arc<TaskRegistry>,
    runtime_events: Option<Arc<CollaborationRuntime<()>>>,
    roles: Option<Arc<crate::role_registry::AgentRoleRegistry>>,
}

impl Default for WorkflowController {
    fn default() -> Self {
        Self::new(Arc::new(InMemoryRuntimeLedger::default()))
    }
}

impl WorkflowController {
    pub fn new(ledger: Arc<dyn RuntimeLedger>) -> Self {
        Self::with_task_registry(ledger, Arc::new(TaskRegistry::default()))
    }

    pub fn with_task_registry(ledger: Arc<dyn RuntimeLedger>, tasks: Arc<TaskRegistry>) -> Self {
        Self {
            lifecycle: Mutex::new(()),
            definitions: RwLock::new(HashMap::new()),
            definition_owners: RwLock::new(HashMap::new()),
            disabled_definitions: RwLock::new(HashSet::new()),
            runs: RwLock::new(HashMap::new()),
            ledger,
            mutation: Arc::new(RunMutationCoordinator::default()),
            mutation_owner_id: Uuid::now_v7().to_string(),
            tasks,
            runtime_events: None,
            roles: None,
        }
    }

    pub fn with_runtime(runtime: Arc<CollaborationRuntime<()>>) -> Self {
        Self::with_runtime_and_roles(runtime, None)
    }

    pub fn with_runtime_and_roles(
        runtime: Arc<CollaborationRuntime<()>>,
        roles: Option<Arc<crate::role_registry::AgentRoleRegistry>>,
    ) -> Self {
        let mutation = runtime.mutation_coordinator();
        Self {
            lifecycle: Mutex::new(()),
            definitions: RwLock::new(HashMap::new()),
            definition_owners: RwLock::new(HashMap::new()),
            disabled_definitions: RwLock::new(HashSet::new()),
            runs: RwLock::new(HashMap::new()),
            ledger: runtime.ledger(),
            mutation,
            mutation_owner_id: Uuid::now_v7().to_string(),
            tasks: runtime.tasks(),
            runtime_events: Some(runtime),
            roles,
        }
    }

    pub fn task_registry(&self) -> Arc<TaskRegistry> {
        Arc::clone(&self.tasks)
    }

    pub fn preflight_register(&self, definition: &WorkflowDefinition) -> Result<(), String> {
        self.preflight_register_batch(std::slice::from_ref(definition))
    }

    pub fn preflight_register_batch(&self, definitions: &[WorkflowDefinition]) -> Result<(), String> {
        let _lifecycle = self.lifecycle.lock().unwrap_or_else(|error| error.into_inner());
        self.preflight_register_batch_locked(definitions)
    }

    fn preflight_register_batch_locked(&self, definitions: &[WorkflowDefinition]) -> Result<(), String> {
        let batch_ids: HashSet<_> = definitions.iter().map(|definition| definition.id.as_str()).collect();
        if batch_ids.len() != definitions.len() {
            return Err("workflow registration batch contains duplicate ids".to_owned());
        }
        let existing = self
            .definitions
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        for definition in definitions {
            validate_definition(definition)?;
            let embedded_roles: HashSet<_> = definition.roles.iter().map(|role| role.id.as_str()).collect();
            if embedded_roles.len() != definition.roles.len() {
                return Err(format!("workflow {} contains duplicate role ids", definition.id));
            }
            for role in &definition.roles {
                validate_role(role)?;
            }
            validate_definition_role_references(definition, |role| {
                embedded_roles.contains(role) || self.roles.as_ref().is_some_and(|roles| roles.get(role).is_some())
            })?;
            for node in &definition.nodes {
                if let Some(workflow_ref) = node.workflow_ref.as_deref()
                    && !batch_ids.contains(workflow_ref)
                    && !existing.contains_key(workflow_ref)
                {
                    return Err(format!(
                        "workflow node {} references unknown workflow {workflow_ref}",
                        node.id
                    ));
                }
            }
            if let Some(registered) = existing.get(&definition.id)
                && registered != definition
            {
                return Err(format!(
                    "workflow {} is already registered with a different definition",
                    definition.id
                ));
            }
        }

        let mut graph: HashMap<String, Vec<String>> = existing
            .values()
            .map(|definition| {
                (
                    definition.id.clone(),
                    definition
                        .nodes
                        .iter()
                        .filter_map(|node| node.workflow_ref.clone())
                        .collect(),
                )
            })
            .collect();
        for definition in definitions {
            graph.insert(
                definition.id.clone(),
                definition
                    .nodes
                    .iter()
                    .filter_map(|node| node.workflow_ref.clone())
                    .collect(),
            );
        }
        validate_workflow_reference_graph(&graph)
    }

    pub fn register(&self, definition: WorkflowDefinition) -> Result<(), String> {
        self.register_owned("core", definition)
    }

    pub fn register_owned(&self, owner: &str, definition: WorkflowDefinition) -> Result<(), String> {
        self.register_owned_batch(owner, vec![definition])
    }

    pub fn register_owned_batch(&self, owner: &str, batch: Vec<WorkflowDefinition>) -> Result<(), String> {
        if owner.trim().is_empty() {
            return Err("workflow definition owner must not be empty".to_owned());
        }
        let _lifecycle = self.lifecycle.lock().unwrap_or_else(|error| error.into_inner());
        self.preflight_register_batch_locked(&batch)?;
        let mut registered_roles: Vec<(String, String)> = Vec::new();
        if let Some(roles) = &self.roles {
            for definition in &batch {
                for role in &definition.roles {
                    if let Some(existing) = roles.get(&role.id)
                        && existing != *role
                    {
                        return Err(format!(
                            "agent role {} is already registered with a different definition",
                            role.id
                        ));
                    }
                }
            }
            let already_owned: HashSet<_> = self
                .definition_owners
                .read()
                .unwrap_or_else(|error| error.into_inner())
                .iter()
                .filter(|(_, owners)| owners.contains(owner))
                .map(|(workflow_id, _)| workflow_id.clone())
                .collect();
            for definition in &batch {
                if already_owned.contains(&definition.id) {
                    continue;
                }
                let role_owner = format!("workflow:{}:{owner}", definition.id);
                for role in &definition.roles {
                    if let Err(error) = roles.register_owned(&role_owner, role.clone()) {
                        for (registered_owner, role_id) in registered_roles {
                            roles.unregister_owned(&registered_owner, &role_id);
                        }
                        return Err(error);
                    }
                    registered_roles.push((role_owner.clone(), role.id.clone()));
                }
            }
        }
        let mut registered_definitions = self.definitions.write().unwrap_or_else(|error| error.into_inner());
        for definition in &batch {
            registered_definitions
                .entry(definition.id.clone())
                .or_insert_with(|| definition.clone());
        }
        drop(registered_definitions);
        let mut owners = self
            .definition_owners
            .write()
            .unwrap_or_else(|error| error.into_inner());
        let mut disabled = self
            .disabled_definitions
            .write()
            .unwrap_or_else(|error| error.into_inner());
        for definition in batch {
            owners
                .entry(definition.id.clone())
                .or_default()
                .insert(owner.to_owned());
            disabled.remove(&definition.id);
        }
        Ok(())
    }

    pub fn unregister_owned(&self, owner: &str, workflow_id: &str) -> Result<bool, String> {
        let _lifecycle = self.lifecycle.lock().unwrap_or_else(|error| error.into_inner());
        if self
            .runs
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .any(|run| run.snapshot.workflow_id == workflow_id && run.snapshot.status == WorkflowRunStatus::Running)
        {
            return Err(format!("workflow {workflow_id} still has a running Run"));
        }

        let remove_definition = {
            let owners = self.definition_owners.read().unwrap_or_else(|error| error.into_inner());
            let Some(definition_owners) = owners.get(workflow_id) else {
                return Ok(false);
            };
            if !definition_owners.contains(owner) {
                return Ok(false);
            }
            definition_owners.len() == 1
        };
        if remove_definition {
            let referenced_by = self
                .definitions
                .read()
                .unwrap_or_else(|error| error.into_inner())
                .iter()
                .filter(|(definition_id, _)| definition_id.as_str() != workflow_id)
                .find_map(|(definition_id, definition)| {
                    definition
                        .nodes
                        .iter()
                        .any(|node| node.workflow_ref.as_deref() == Some(workflow_id))
                        .then(|| definition_id.clone())
                });
            if let Some(referenced_by) = referenced_by {
                return Err(format!(
                    "workflow {workflow_id} is still referenced by workflow {referenced_by}"
                ));
            }
        }

        let removed_owner = {
            let mut owners = self
                .definition_owners
                .write()
                .unwrap_or_else(|error| error.into_inner());
            let Some(definition_owners) = owners.get_mut(workflow_id) else {
                return Ok(false);
            };
            if !definition_owners.remove(owner) {
                return Ok(false);
            }
            if definition_owners.is_empty() {
                owners.remove(workflow_id);
            }
            true
        };

        if removed_owner
            && let Some(roles) = &self.roles
            && let Some(definition) = self.definition(workflow_id)
        {
            let role_owner = format!("workflow:{workflow_id}:{owner}");
            for role in &definition.roles {
                roles.unregister_owned(&role_owner, &role.id);
            }
        }

        if remove_definition {
            self.definitions
                .write()
                .unwrap_or_else(|error| error.into_inner())
                .remove(workflow_id);
            self.disabled_definitions
                .write()
                .unwrap_or_else(|error| error.into_inner())
                .remove(workflow_id);
        }
        Ok(remove_definition)
    }

    pub fn disable(&self, workflow_id: &str) -> Result<bool, String> {
        let _lifecycle = self.lifecycle.lock().unwrap_or_else(|error| error.into_inner());
        if self.definition(workflow_id).is_none() {
            return Err(format!("unknown workflow: {workflow_id}"));
        }
        Ok(self
            .disabled_definitions
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .insert(workflow_id.to_owned()))
    }

    pub fn enable(&self, workflow_id: &str) -> Result<bool, String> {
        let _lifecycle = self.lifecycle.lock().unwrap_or_else(|error| error.into_inner());
        if self.definition(workflow_id).is_none() {
            return Err(format!("unknown workflow: {workflow_id}"));
        }
        Ok(self
            .disabled_definitions
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .remove(workflow_id))
    }

    pub fn is_enabled(&self, workflow_id: &str) -> bool {
        self.definition(workflow_id).is_some()
            && !self
                .disabled_definitions
                .read()
                .unwrap_or_else(|error| error.into_inner())
                .contains(workflow_id)
    }

    pub fn has_running_runs(&self) -> bool {
        self.runs
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .any(|projection| projection.snapshot.status == WorkflowRunStatus::Running)
    }

    pub fn definition(&self, workflow_id: &str) -> Option<WorkflowDefinition> {
        self.definitions
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(workflow_id)
            .cloned()
    }

    pub fn definitions(&self) -> Vec<WorkflowDefinition> {
        let mut definitions: Vec<_> = self
            .definitions
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .cloned()
            .collect();
        definitions.sort_by(|left, right| left.id.cmp(&right.id));
        definitions
    }

    pub fn definition_snapshots(&self) -> Vec<WorkflowDefinitionSnapshot> {
        let disabled = self
            .disabled_definitions
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        self.definitions()
            .into_iter()
            .map(|definition| WorkflowDefinitionSnapshot {
                enabled: !disabled.contains(&definition.id),
                generation: stable_digest_value(&serde_json::to_value(&definition).unwrap_or(Value::Null)),
                definition,
            })
            .collect()
    }

    pub fn snapshots(&self) -> Vec<WorkflowRunSnapshot> {
        let mut snapshots: Vec<_> = self
            .runs
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .map(|projection| projection.snapshot.clone())
            .collect();
        snapshots.sort_by(|left, right| left.run_id.as_str().cmp(right.run_id.as_str()));
        snapshots
    }

    pub fn start(&self, run_id: RunId, workflow_id: &str, parameters: Value) -> Result<WorkflowRunSnapshot, String> {
        self.start_internal(run_id, workflow_id, parameters, None, None)
            .map(|outcome| outcome.snapshot)
    }

    pub fn start_with_runtime(
        &self,
        run_id: RunId,
        workflow_id: &str,
        parameters: Value,
        runtime_identity: WorkflowRuntimeIdentity,
    ) -> Result<WorkflowStartOutcome, String> {
        self.start_internal(run_id, workflow_id, parameters, None, Some(runtime_identity))
    }

    fn start_child(
        &self,
        run_id: RunId,
        workflow_id: &str,
        parameters: Value,
        parent_run_id: RunId,
    ) -> Result<WorkflowRunSnapshot, String> {
        self.start_internal(run_id, workflow_id, parameters, Some(parent_run_id), None)
            .map(|outcome| outcome.snapshot)
    }

    fn start_internal(
        &self,
        run_id: RunId,
        workflow_id: &str,
        parameters: Value,
        parent_run_id: Option<RunId>,
        runtime_identity: Option<WorkflowRuntimeIdentity>,
    ) -> Result<WorkflowStartOutcome, String> {
        let _lifecycle = self.lifecycle.lock().unwrap_or_else(|error| error.into_inner());
        let input_digest = workflow_input_digest(&parameters);
        let definition = self
            .definition(workflow_id)
            .ok_or_else(|| format!("unknown workflow: {workflow_id}"))?;
        if self
            .disabled_definitions
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .contains(workflow_id)
        {
            return Err(format!("workflow {workflow_id} is disabled"));
        }
        if let Some(schema) = &definition.parameters_schema {
            validate_value(&parameters, schema)
                .map_err(|error| format!("workflow {workflow_id} parameters invalid: {error}"))?;
        }
        let definition_digest =
            stable_digest_value(&serde_json::to_value(&definition).map_err(|error| error.to_string())?);
        let runtime_identity = runtime_identity.or_else(|| {
            parent_run_id
                .as_ref()
                .and_then(|parent_run_id| self.snapshot(parent_run_id))
                .and_then(|snapshot| snapshot.runtime_identity)
        });
        let nodes = definition
            .nodes
            .iter()
            .map(|node| {
                (
                    node.id.clone(),
                    WorkflowNodeAttempt {
                        node_id: node.id.clone(),
                        attempt_id: AttemptId::new(format!("attempt-{}", Uuid::now_v7())),
                        attempt_number: 0,
                        status: WorkflowNodeStatus::Pending,
                        input_digest: None,
                        output: None,
                        output_ref: None,
                        committed_at_unix_ms: None,
                        error: None,
                        failure_class: None,
                        resume_existing_attempt: false,
                        deferred_task_terminal_write: None,
                    },
                )
            })
            .collect();
        let snapshot = WorkflowRunSnapshot {
            run_id: run_id.clone(),
            workflow_id: definition.id.clone(),
            workflow_version: definition.version.clone(),
            workflow_definition_digest: Some(definition_digest.clone()),
            input_digest: Some(input_digest.clone()),
            runtime_identity: runtime_identity.clone(),
            parent_run_id: parent_run_id.clone(),
            status: WorkflowRunStatus::Running,
            reconciliation_reason: None,
            nodes,
            parameters,
        };
        let task_records: Vec<_> = definition
            .nodes
            .iter()
            .map(|node| TaskRecord {
                run_id: run_id.clone(),
                task_id: workflow_task_id(&run_id, &node.id),
                revision: 0,
                task_key: Some(format!("workflow:{}:{}", definition.id, node.id)),
                team_id: None,
                workflow_id: Some(definition.id.clone()),
                node_id: Some(node.id.clone()),
                role: node.role.clone(),
                depends_on: node
                    .depends_on
                    .iter()
                    .map(|dependency| workflow_task_id(&run_id, dependency))
                    .collect(),
                content: None,
                expected_write_scope: Vec::new(),
                owner_agent_id: None,
                state: TaskState::Queued,
                outcome_ref: None,
                failure_class: None,
            })
            .collect();
        let outcome = self.mutate_projection(&run_id, |guard, prepared| {
            if let Some(parent_run_id) = parent_run_id.as_ref() {
                let parent = self
                    .runs
                    .read()
                    .unwrap_or_else(|error| error.into_inner())
                    .get(parent_run_id)
                    .map(|projection| projection.snapshot.clone())
                    .ok_or_else(|| format!("unknown parent workflow run: {parent_run_id}"))?;
                if parent.status != WorkflowRunStatus::Running {
                    return Err(format!("parent workflow run {parent_run_id} is not running"));
                }
            }
            if let Some(existing) = prepared.clone() {
                return validate_workflow_run_reuse(
                    existing,
                    workflow_id,
                    &definition_digest,
                    &input_digest,
                    parent_run_id.as_ref(),
                    runtime_identity.as_ref(),
                )
                .map(|snapshot| WorkflowStartOutcome {
                    snapshot,
                    started: false,
                });
            }
            self.append_record(
                guard,
                &run_id,
                "workflow_started",
                json!({
                    "workflow_id": definition.id,
                    "workflow_version": definition.version,
                    "snapshot": snapshot,
                }),
            )?;
            *prepared = Some(snapshot.clone());
            Ok(WorkflowStartOutcome {
                snapshot: snapshot.clone(),
                started: true,
            })
        })?;
        if outcome.started {
            for task in &task_records {
                self.tasks.upsert(task.clone());
                self.emit_task_event(&run_id, "task_created", task);
            }
        }
        Ok(outcome)
    }

    pub fn snapshot(&self, run_id: &RunId) -> Option<WorkflowRunSnapshot> {
        self.runs
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(run_id)
            .map(|projection| projection.snapshot.clone())
    }

    pub fn task_handle(&self, run_id: &RunId, node_id: &str) -> Option<WorkflowTaskHandle> {
        let snapshot = self.snapshot(run_id)?;
        let attempt = snapshot.nodes.get(node_id)?;
        Some(WorkflowTaskHandle {
            task_id: TaskId::new(format!("workflow:{}:{node_id}", run_id.as_str())),
            operation_id: OperationId::new(format!(
                "workflow:{}:{}:{}",
                run_id.as_str(),
                node_id,
                attempt.attempt_id
            )),
            node_id: node_id.to_owned(),
            state: attempt.status,
            output_ref: attempt.output_ref.clone(),
        })
    }

    pub fn ready_nodes(&self, run_id: &RunId) -> Result<Vec<WorkflowNode>, String> {
        let snapshot = self
            .snapshot(run_id)
            .ok_or_else(|| format!("unknown workflow run: {run_id}"))?;
        let definition = self
            .definition(&snapshot.workflow_id)
            .ok_or_else(|| format!("workflow definition disappeared: {}", snapshot.workflow_id))?;
        Ok(definition
            .nodes
            .iter()
            .filter(|node| {
                snapshot
                    .nodes
                    .get(&node.id)
                    .is_some_and(|attempt| attempt.status == WorkflowNodeStatus::Pending)
                    && dependencies_complete(node, &snapshot.nodes)
                    && condition_matches(node.when.as_ref(), &snapshot.nodes, &snapshot.parameters)
            })
            .cloned()
            .collect())
    }

    pub async fn execute_until_settled(
        &self,
        run_id: &RunId,
        executor: Arc<dyn WorkflowNodeExecutor>,
    ) -> Result<WorkflowRunSnapshot, String> {
        loop {
            self.skip_false_conditions(run_id)?;
            let ready = self.ready_nodes(run_id)?;
            if ready.is_empty() {
                return self.settle(run_id);
            }

            let mut contexts = Vec::with_capacity(ready.len());
            for node in &ready {
                match self.begin_attempt(run_id, node) {
                    Ok(context) => contexts.push(context),
                    Err(error) if error.starts_with("workflow input binding failed:") => {
                        self.fail_before_attempt(run_id, node, error)?;
                    }
                    Err(error) => return Err(error),
                }
            }
            let futures = contexts.into_iter().map(|context| {
                let executor = Arc::clone(&executor);
                async move {
                    let node_id = context.node.id.clone();
                    let attempt_id = context.attempt_id.clone();
                    (node_id, attempt_id, self.execute_context(context, executor).await)
                }
            });
            for (node_id, attempt_id, result) in futures::future::join_all(futures).await {
                match result {
                    Ok(output) => {
                        self.complete_node(run_id, &node_id, &attempt_id, output)?;
                    }
                    Err(error) => {
                        self.fail_node(run_id, &node_id, &attempt_id, error)?;
                    }
                }
            }
        }
    }

    fn execute_context<'a>(
        &'a self,
        context: WorkflowExecutionContext,
        executor: Arc<dyn WorkflowNodeExecutor>,
    ) -> Pin<Box<dyn Future<Output = Result<Value, WorkflowNodeError>> + Send + 'a>> {
        Box::pin(async move {
            let timeout_ms = context.node.timeout_ms;
            let Some(workflow_ref) = context.node.workflow_ref.clone() else {
                let execute = executor.execute(context);
                return if let Some(timeout_ms) = timeout_ms {
                    tokio::time::timeout(Duration::from_millis(timeout_ms.max(1)), execute)
                        .await
                        .map_err(|_| {
                            WorkflowNodeError::retryable(format!("workflow node timed out after {timeout_ms}ms"))
                        })?
                } else {
                    execute.await
                };
            };

            let child_run_id = RunId::new(format!(
                "{}:subworkflow:{}:{}",
                context.run_id, context.node.id, context.attempt_id
            ));
            let parameters = json!({
                "parent_parameters": context.parameters,
                "dependency_outputs": context.dependency_outputs,
                "bound_inputs": context.bound_inputs,
            });
            self.start_child(child_run_id.clone(), &workflow_ref, parameters, context.run_id.clone())
                .map_err(WorkflowNodeError::from)?;
            let execute_child = async {
                let settled = match Box::pin(self.execute_until_settled(&child_run_id, Arc::clone(&executor))).await {
                    Ok(settled) => settled,
                    Err(message) => {
                        let failure_class = self
                            .snapshot(&child_run_id)
                            .and_then(|snapshot| {
                                snapshot
                                    .nodes
                                    .values()
                                    .filter(|attempt| attempt.status == WorkflowNodeStatus::Failed)
                                    .filter_map(|attempt| attempt.failure_class)
                                    .next()
                            })
                            .unwrap_or(TaskFailureClass::NonRetryable);
                        return Err(WorkflowNodeError { failure_class, message });
                    }
                };
                if settled.status != WorkflowRunStatus::Completed {
                    let failure_class = match settled.status {
                        WorkflowRunStatus::Cancelled => TaskFailureClass::Cancelled,
                        WorkflowRunStatus::Failed => settled
                            .nodes
                            .values()
                            .filter(|attempt| attempt.status == WorkflowNodeStatus::Failed)
                            .filter_map(|attempt| attempt.failure_class)
                            .next()
                            .unwrap_or(TaskFailureClass::NonRetryable),
                        WorkflowRunStatus::Running | WorkflowRunStatus::Completed => TaskFailureClass::NonRetryable,
                    };
                    return Err(WorkflowNodeError {
                        failure_class,
                        message: format!("subworkflow {workflow_ref} settled as {:?}", settled.status),
                    });
                }
                let definition = self.definition(&workflow_ref).ok_or_else(|| {
                    WorkflowNodeError::non_retryable(format!("subworkflow definition disappeared: {workflow_ref}"))
                })?;
                let mut outputs = serde_json::Map::new();
                if definition.outputs.is_empty() {
                    for (node_id, attempt) in &settled.nodes {
                        if let Some(output) = &attempt.output {
                            outputs.insert(node_id.clone(), output.clone());
                        }
                    }
                } else {
                    for (name, node_id) in &definition.outputs {
                        if let Some(output) = settled.nodes.get(node_id).and_then(|attempt| attempt.output.clone()) {
                            outputs.insert(name.clone(), output);
                        }
                    }
                }
                Ok(Value::Object(outputs))
            };
            let result = if let Some(timeout_ms) = timeout_ms {
                match tokio::time::timeout(Duration::from_millis(timeout_ms.max(1)), execute_child).await {
                    Ok(result) => result,
                    Err(_) => {
                        let _ = self.cancel(&child_run_id, "parent workflow node timed out");
                        return Err(WorkflowNodeError::retryable(format!(
                            "workflow node timed out after {timeout_ms}ms"
                        )));
                    }
                }
            } else {
                execute_child.await
            };
            if result.is_err()
                && self
                    .snapshot(&child_run_id)
                    .is_some_and(|snapshot| snapshot.status == WorkflowRunStatus::Running)
            {
                let _ = self.cancel(&child_run_id, "parent workflow node did not complete");
            }
            result
        })
    }

    pub fn cancel(&self, run_id: &RunId, reason: &str) -> Result<WorkflowRunSnapshot, String> {
        let cancelled = self.mutate_projection(run_id, |guard, prepared| {
            let snapshot = prepared
                .as_mut()
                .ok_or_else(|| format!("unknown workflow run: {run_id}"))?;
            if matches!(
                snapshot.status,
                WorkflowRunStatus::Completed | WorkflowRunStatus::Failed | WorkflowRunStatus::Cancelled
            ) {
                return Ok(snapshot.clone());
            }
            let mut cancelled = snapshot.clone();
            cancelled.status = WorkflowRunStatus::Cancelled;
            for attempt in cancelled.nodes.values_mut() {
                if matches!(
                    attempt.status,
                    WorkflowNodeStatus::Pending | WorkflowNodeStatus::Running
                ) {
                    attempt.status = WorkflowNodeStatus::Cancelled;
                    attempt.error = Some(reason.to_owned());
                    attempt.failure_class = Some(TaskFailureClass::Cancelled);
                }
            }
            self.append_record(
                guard,
                run_id,
                "workflow_cancelled",
                json!({"reason": reason, "snapshot": cancelled}),
            )?;
            *snapshot = cancelled.clone();
            for (node_id, attempt) in &cancelled.nodes {
                if attempt.status == WorkflowNodeStatus::Cancelled {
                    self.update_workflow_task(
                        guard,
                        run_id,
                        node_id,
                        WorkflowTaskUpdate {
                            state: TaskState::Cancelled,
                            clear_owner: false,
                            outcome_ref: None,
                            failure_class: None,
                            operation_id: OperationId::new(format!("workflow:{run_id}:{node_id}:cancelled")),
                        },
                    )?;
                    self.emit_current_task(run_id, node_id, "task_state_changed");
                }
            }
            Ok(cancelled)
        })?;
        let child_runs: Vec<_> = self
            .runs
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .filter(|projection| {
                projection.snapshot.parent_run_id.as_ref() == Some(run_id)
                    && projection.snapshot.status == WorkflowRunStatus::Running
            })
            .map(|projection| projection.snapshot.run_id.clone())
            .collect();
        for child_run_id in child_runs {
            self.cancel(&child_run_id, reason)?;
        }
        Ok(cancelled)
    }
}

#[cfg(test)]
#[path = "workflow_controller_test.rs"]
mod workflow_controller_test;
