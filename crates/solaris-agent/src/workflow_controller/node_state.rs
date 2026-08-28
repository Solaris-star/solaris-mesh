use serde_json::{Value, json};
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::{AttemptId, OperationId, RunId, TaskId};
use solaris_types::plugin::ImplementationIdentity;
use solaris_types::runtime::{TaskFailureClass, TaskRecord, TaskState};
use solaris_types::workflow::{WorkflowNode, WorkflowNodeStatus};
use uuid::Uuid;

use crate::execution_context::{EffectOutputStore, stable_digest_bytes, stable_digest_value};
use crate::task_registry::TaskCasMutation;

use super::completion_source::WORKFLOW_COMPLETION_SCHEMA_VERSION;
use super::projection::WorkflowMutationGuard;
use super::validation::{build_bound_inputs, condition_matches, dependencies_complete};
use super::{
    DeferredTaskTerminalWrite, WorkflowController, WorkflowExecutionContext, WorkflowNodeError, WorkflowRunSnapshot,
    WorkflowRunStatus,
};

pub(super) struct WorkflowTaskUpdate {
    pub(super) state: TaskState,
    pub(super) clear_owner: bool,
    pub(super) outcome_ref: Option<String>,
    pub(super) failure_class: Option<TaskFailureClass>,
    pub(super) operation_id: OperationId,
}

impl WorkflowController {
    pub(super) fn begin_attempt(
        &self,
        run_id: &RunId,
        node: &WorkflowNode,
    ) -> Result<WorkflowExecutionContext, String> {
        let context = self.mutate_projection(run_id, |guard, prepared| {
            let snapshot = prepared
                .as_mut()
                .ok_or_else(|| format!("unknown workflow run: {run_id}"))?;
            if snapshot.status != WorkflowRunStatus::Running {
                return Err(format!("workflow run {run_id} is not running"));
            }
            let dependency_outputs = node
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
            let bound_inputs = build_bound_inputs(node, &dependency_outputs)
                .map_err(|error| format!("workflow input binding failed: {error}"))?;
            let input_digest = stable_digest_value(&json!({
                "parameters": snapshot.parameters,
                "dependency_outputs": dependency_outputs,
                "bound_inputs": bound_inputs,
            }));
            let current_attempt = snapshot
                .nodes
                .get(&node.id)
                .ok_or_else(|| format!("unknown workflow node: {}", node.id))?;
            if current_attempt.status != WorkflowNodeStatus::Pending {
                return Err(format!(
                    "workflow node {} is not pending; current state is {:?}",
                    node.id, current_attempt.status
                ));
            }
            let (attempt_id, attempt_number) = if current_attempt.resume_existing_attempt {
                (current_attempt.attempt_id.clone(), current_attempt.attempt_number)
            } else {
                (
                    AttemptId::new(format!("attempt-{}", Uuid::now_v7())),
                    current_attempt.attempt_number + 1,
                )
            };
            let context = WorkflowExecutionContext {
                run_id: run_id.clone(),
                workflow: ImplementationIdentity {
                    implementation_id: format!("workflow:{}", snapshot.workflow_id),
                    version: Some(snapshot.workflow_version.clone()),
                    digest: snapshot.workflow_definition_digest.clone(),
                },
                node: node.clone(),
                attempt_id: attempt_id.clone(),
                parameters: snapshot.parameters.clone(),
                dependency_outputs,
                bound_inputs,
            };
            self.append_record(
                guard,
                run_id,
                "workflow_node_started",
                json!({"node_id": node.id, "attempt_id": attempt_id, "input_digest": input_digest}),
            )?;
            let attempt = snapshot
                .nodes
                .get_mut(&node.id)
                .ok_or_else(|| format!("unknown workflow node: {}", node.id))?;
            attempt.resume_existing_attempt = false;
            attempt.attempt_number = attempt_number;
            attempt.attempt_id = attempt_id;
            attempt.status = WorkflowNodeStatus::Running;
            attempt.input_digest = Some(input_digest);
            attempt.output = None;
            attempt.output_ref = None;
            attempt.committed_at_unix_ms = None;
            attempt.error = None;
            attempt.failure_class = None;
            Ok(context)
        })?;
        Ok(context)
    }

    pub(super) fn fail_before_attempt(&self, run_id: &RunId, node: &WorkflowNode, error: String) -> Result<(), String> {
        self.mutate_projection(run_id, |guard, prepared| {
            let snapshot = prepared
                .as_mut()
                .ok_or_else(|| format!("unknown workflow run: {run_id}"))?;
            if snapshot.status != WorkflowRunStatus::Running {
                return Ok(());
            }
            let attempt = snapshot
                .nodes
                .get_mut(&node.id)
                .ok_or_else(|| format!("unknown workflow node: {}", node.id))?;
            if attempt.status != WorkflowNodeStatus::Pending {
                return Ok(());
            }
            self.append_record(
                guard,
                run_id,
                "workflow_node_failed",
                json!({
                    "node_id": node.id,
                    "attempt_id": attempt.attempt_id,
                    "error": error,
                    "state": WorkflowNodeStatus::Failed,
                    "pre_dispatch": true,
                }),
            )?;
            attempt.status = WorkflowNodeStatus::Failed;
            attempt.error = Some(error);
            attempt.failure_class = Some(TaskFailureClass::NonRetryable);
            attempt.resume_existing_attempt = false;
            self.update_workflow_task(
                guard,
                run_id,
                &node.id,
                WorkflowTaskUpdate {
                    state: TaskState::Failed,
                    clear_owner: false,
                    outcome_ref: None,
                    failure_class: Some(TaskFailureClass::NonRetryable),
                    operation_id: OperationId::new(format!(
                        "workflow:{run_id}:{}:{}:pre-dispatch-failed",
                        node.id, attempt.attempt_id
                    )),
                },
            )?;
            self.emit_current_task(run_id, &node.id, "task_state_changed");
            Ok(())
        })
    }

    pub(super) fn complete_node(
        &self,
        run_id: &RunId,
        node_id: &str,
        expected_attempt_id: &AttemptId,
        output: Value,
    ) -> Result<bool, String> {
        let completed = self.mutate_projection(run_id, |guard, prepared| {
            let snapshot = prepared
                .as_mut()
                .ok_or_else(|| format!("unknown workflow run: {run_id}"))?;
            if snapshot.status != WorkflowRunStatus::Running {
                return Ok(false);
            }
            let workflow_node = self
                .definition(&snapshot.workflow_id)
                .and_then(|definition| definition.nodes.into_iter().find(|node| node.id == node_id))
                .ok_or_else(|| format!("unknown workflow node definition: {node_id}"))?;
            let attempt = snapshot
                .nodes
                .get_mut(node_id)
                .ok_or_else(|| format!("unknown workflow node: {node_id}"))?;
            if attempt.status != WorkflowNodeStatus::Running || &attempt.attempt_id != expected_attempt_id {
                return Ok(false);
            }
            let serialized_output = serde_json::to_string(&output).map_err(|error| error.to_string())?;
            let output_ref = EffectOutputStore::for_run_with_ledger(run_id, self.ledger.as_ref())
                .write_named(
                    &format!("workflow:{}:{}", node_id, attempt.attempt_id),
                    &serialized_output,
                )
                .map_err(|error| format!("failed to protect workflow output: {error}"))?;
            let committed_at_unix_ms = chrono::Utc::now().timestamp_millis();
            let attempt_id = attempt.attempt_id.clone();
            let input_digest = attempt.input_digest.clone();
            let output_bytes = u64::try_from(serialized_output.len())
                .map_err(|_| "workflow output length does not fit its durable identity".to_owned())?;
            let output_digest = stable_digest_bytes(serialized_output.as_bytes());
            let source_supervisor_decision = self.build_supervisor_completion_source(
                &workflow_node,
                run_id,
                &attempt_id,
                &output_ref,
                output_bytes,
                &output_digest,
            )?;
            self.append_record(
                guard,
                run_id,
                "workflow_node_completed",
                json!({
                    "completion_schema_version": WORKFLOW_COMPLETION_SCHEMA_VERSION,
                    "node_id": node_id,
                    "attempt_id": attempt_id,
                    "input_digest": input_digest,
                    "output_ref": output_ref,
                    "output_digest": output_digest,
                    "output_bytes": output_bytes,
                    "output_status": "completed",
                    "output_redacted": true,
                    "source_supervisor_decision": source_supervisor_decision,
                    "committed_at_unix_ms": committed_at_unix_ms,
                }),
            )?;
            attempt.status = WorkflowNodeStatus::Completed;
            attempt.output = Some(output);
            attempt.output_ref = Some(output_ref);
            attempt.committed_at_unix_ms = Some(committed_at_unix_ms);
            attempt.error = None;
            attempt.failure_class = None;
            self.update_workflow_task(
                guard,
                run_id,
                node_id,
                WorkflowTaskUpdate {
                    state: TaskState::Completed,
                    clear_owner: false,
                    outcome_ref: Some(attempt.output_ref.clone().unwrap_or_default()),
                    failure_class: None,
                    operation_id: OperationId::new(format!(
                        "workflow:{run_id}:{node_id}:{expected_attempt_id}:completed"
                    )),
                },
            )?;
            self.emit_current_task(run_id, node_id, "task_state_changed");
            Ok(true)
        })?;
        Ok(completed)
    }

    pub(super) fn fail_node(
        &self,
        run_id: &RunId,
        node_id: &str,
        expected_attempt_id: &AttemptId,
        error: WorkflowNodeError,
    ) -> Result<bool, String> {
        let snapshot = self
            .snapshot(run_id)
            .ok_or_else(|| format!("unknown workflow run: {run_id}"))?;
        let definition = self
            .definition(&snapshot.workflow_id)
            .ok_or_else(|| format!("unknown workflow: {}", snapshot.workflow_id))?;
        let node = definition
            .nodes
            .iter()
            .find(|node| node.id == node_id)
            .ok_or_else(|| format!("unknown workflow node: {node_id}"))?;
        let failed = self.mutate_projection(run_id, |guard, prepared| {
            let task_id = workflow_task_id(run_id, node_id);
            let operation_id = OperationId::new(format!("workflow:{run_id}:{node_id}:{expected_attempt_id}:failed"));
            let deferred_task_terminal_write = if error.failure_class == TaskFailureClass::ReconciliationRequired {
                match self.restore_durable_task_projection(&task_id) {
                    Ok(_) => None,
                    Err(restore_error) => {
                        tracing::warn!(
                            workflow_run_id = %run_id,
                            task_id = %task_id,
                            "deferring Workflow task terminal CAS because durable projection recovery failed"
                        );
                        tracing::debug!(
                            workflow_run_id = %run_id,
                            task_id = %task_id,
                            error = %restore_error,
                            "durable Workflow task projection recovery error"
                        );
                        Some(DeferredTaskTerminalWrite {
                            state: TaskState::Failed,
                            clear_owner: false,
                            outcome_ref: None,
                            failure_class: error.failure_class,
                            operation_id: operation_id.clone(),
                        })
                    }
                }
            } else {
                None
            };
            let current = prepared
                .as_mut()
                .ok_or_else(|| format!("unknown workflow run: {run_id}"))?;
            if current.status != WorkflowRunStatus::Running {
                return Ok(false);
            }
            let attempt = current
                .nodes
                .get_mut(node_id)
                .ok_or_else(|| format!("unknown workflow node: {node_id}"))?;
            if attempt.status != WorkflowNodeStatus::Running || &attempt.attempt_id != expected_attempt_id {
                return Ok(false);
            }
            let allowed_attempts = node.retry.max_attempts.max(1);
            let may_retry = error.failure_class == TaskFailureClass::Retryable;
            let status = if may_retry && attempt.attempt_number < allowed_attempts {
                WorkflowNodeStatus::Pending
            } else {
                WorkflowNodeStatus::Failed
            };
            self.append_record(
                guard,
                run_id,
                "workflow_node_failed",
                json!({
                    "node_id": node_id,
                    "error": &error.message,
                    "failure_class": error.failure_class,
                    "state": status,
                    "task_terminal_write_deferred": deferred_task_terminal_write.is_some(),
                    "deferred_task_terminal_write": deferred_task_terminal_write,
                }),
            )?;
            attempt.error = Some(error.message.clone());
            attempt.failure_class = Some(error.failure_class);
            attempt.status = status;
            attempt
                .deferred_task_terminal_write
                .clone_from(&deferred_task_terminal_write);
            if deferred_task_terminal_write.is_none() {
                self.update_workflow_task(
                    guard,
                    run_id,
                    node_id,
                    WorkflowTaskUpdate {
                        state: TaskState::Failed,
                        clear_owner: false,
                        outcome_ref: None,
                        failure_class: Some(error.failure_class),
                        operation_id,
                    },
                )?;
                if status == WorkflowNodeStatus::Pending {
                    self.update_workflow_task(
                        guard,
                        run_id,
                        node_id,
                        WorkflowTaskUpdate {
                            state: TaskState::Queued,
                            clear_owner: true,
                            outcome_ref: None,
                            failure_class: None,
                            operation_id: OperationId::new(format!(
                                "workflow:{run_id}:{node_id}:{expected_attempt_id}:retry-queued"
                            )),
                        },
                    )?;
                }
                self.emit_current_task(run_id, node_id, "task_state_changed");
            }
            Ok(true)
        })?;
        Ok(failed)
    }

    pub(super) fn skip_false_conditions(&self, run_id: &RunId) -> Result<(), String> {
        loop {
            let snapshot = self
                .snapshot(run_id)
                .ok_or_else(|| format!("unknown workflow run: {run_id}"))?;
            let definition = self
                .definition(&snapshot.workflow_id)
                .ok_or_else(|| format!("unknown workflow: {}", snapshot.workflow_id))?;
            let skipped: Vec<_> = definition
                .nodes
                .iter()
                .filter(|node| {
                    snapshot
                        .nodes
                        .get(&node.id)
                        .is_some_and(|attempt| attempt.status == WorkflowNodeStatus::Pending)
                        && dependencies_complete(node, &snapshot.nodes)
                        && node.when.is_some()
                        && !condition_matches(node.when.as_ref(), &snapshot.nodes, &snapshot.parameters)
                })
                .map(|node| node.id.clone())
                .collect();
            if skipped.is_empty() {
                return Ok(());
            }
            for node_id in skipped {
                self.mutate_projection(run_id, |guard, prepared| {
                    self.append_record(guard, run_id, "workflow_node_skipped", json!({"node_id": node_id}))?;
                    let current = prepared
                        .as_mut()
                        .ok_or_else(|| format!("unknown workflow run: {run_id}"))?;
                    if let Some(attempt) = current.nodes.get_mut(&node_id) {
                        attempt.status = WorkflowNodeStatus::Skipped;
                    }
                    self.update_workflow_task(
                        guard,
                        run_id,
                        &node_id,
                        WorkflowTaskUpdate {
                            state: TaskState::Skipped,
                            clear_owner: false,
                            outcome_ref: None,
                            failure_class: None,
                            operation_id: OperationId::new(format!("workflow:{run_id}:{node_id}:skipped")),
                        },
                    )?;
                    self.emit_current_task(run_id, &node_id, "task_state_changed");
                    Ok(())
                })?;
            }
            // Continue until no further conditional node becomes decidable as
            // a consequence of the newly skipped dependencies.
        }
    }

    pub(super) fn settle(&self, run_id: &RunId) -> Result<WorkflowRunSnapshot, String> {
        self.mutate_projection(run_id, |guard, prepared| {
            let snapshot = prepared
                .as_mut()
                .ok_or_else(|| format!("unknown workflow run: {run_id}"))?;
            self.recover_deferred_task_terminals_locked(run_id, guard, snapshot)?;
            let status = if snapshot
                .nodes
                .values()
                .any(|node| node.status == WorkflowNodeStatus::Running)
            {
                return Ok(snapshot.clone());
            } else if snapshot
                .nodes
                .values()
                .any(|node| node.status == WorkflowNodeStatus::Failed)
            {
                WorkflowRunStatus::Failed
            } else if snapshot
                .nodes
                .values()
                .all(|node| matches!(node.status, WorkflowNodeStatus::Completed | WorkflowNodeStatus::Skipped))
            {
                WorkflowRunStatus::Completed
            } else {
                snapshot.status
            };
            let failure_summary = (status == WorkflowRunStatus::Failed)
                .then(|| super::aggregate_workflow_failures(&snapshot.nodes))
                .flatten();
            self.append_record(
                guard,
                run_id,
                "workflow_settled",
                json!({"status": status, "failure_summary": failure_summary}),
            )?;
            snapshot.status = status;
            snapshot.failure_summary = failure_summary;
            Ok(snapshot.clone())
        })
    }

    fn restore_durable_task_projection(&self, task_id: &TaskId) -> Result<bool, String> {
        self.runtime_events
            .as_ref()
            .ok_or_else(|| "Workflow task recovery requires a CollaborationRuntime".to_owned())?
            .restore_task_projection_across_lineage_locked(task_id)
            .map_err(|error| error.to_string())
    }

    pub(super) fn restore_durable_task_operation(
        &self,
        task_id: &TaskId,
        operation_id: &OperationId,
    ) -> Result<(bool, Option<TaskRecord>), String> {
        self.runtime_events
            .as_ref()
            .ok_or_else(|| "Workflow task recovery requires a CollaborationRuntime".to_owned())?
            .restore_task_operation_across_lineage_locked(task_id, operation_id)
            .map_err(|error| error.to_string())
    }

    fn recover_deferred_task_terminals_locked(
        &self,
        run_id: &RunId,
        guard: &WorkflowMutationGuard,
        snapshot: &mut WorkflowRunSnapshot,
    ) -> Result<(), String> {
        let pending: Vec<_> = snapshot
            .nodes
            .iter()
            .filter_map(|(node_id, attempt)| {
                attempt
                    .deferred_task_terminal_write
                    .clone()
                    .map(|deferred| (node_id.clone(), attempt.attempt_id.clone(), deferred))
            })
            .collect();
        for (node_id, attempt_id, deferred) in pending {
            let task_id = workflow_task_id(run_id, &node_id);
            let (found, existing_terminal) = self
                .restore_durable_task_operation(&task_id, &deferred.operation_id)
                .map_err(|error| format!("deferred Workflow task {task_id} recovery failed: {error}"))?;
            if !found {
                return Err(format!("deferred Workflow task {task_id} has no durable CAS history"));
            }
            let new_revision = if let Some(task) = existing_terminal {
                validate_deferred_terminal_task(&task, &deferred)?;
                task.revision
            } else {
                let update = WorkflowTaskUpdate {
                    state: deferred.state,
                    clear_owner: deferred.clear_owner,
                    outcome_ref: deferred.outcome_ref.clone(),
                    failure_class: Some(deferred.failure_class),
                    operation_id: deferred.operation_id.clone(),
                };
                match self.update_workflow_task(guard, run_id, &node_id, update) {
                    Ok(revision) => revision,
                    Err(append_error) => {
                        let recovered = self
                            .restore_durable_task_operation(&task_id, &deferred.operation_id)
                            .map_err(|lookup_error| {
                                format!(
                                    "deferred Workflow task {task_id} terminal CAS failed: {append_error}; durable replay failed: {lookup_error}"
                                )
                            })?
                            .1
                            .ok_or(append_error)?;
                        validate_deferred_terminal_task(&recovered, &deferred)?;
                        recovered.revision
                    }
                }
            };
            self.append_terminal_reconciled_marker(guard, run_id, &node_id, &attempt_id, &deferred, new_revision)?;
            let attempt = snapshot
                .nodes
                .get_mut(&node_id)
                .ok_or_else(|| format!("unknown workflow node: {node_id}"))?;
            if attempt.deferred_task_terminal_write.as_ref() == Some(&deferred) {
                attempt.deferred_task_terminal_write = None;
            }
            self.emit_current_task(run_id, &node_id, "task_state_changed");
        }
        Ok(())
    }

    fn append_terminal_reconciled_marker(
        &self,
        guard: &WorkflowMutationGuard,
        run_id: &RunId,
        node_id: &str,
        attempt_id: &AttemptId,
        deferred: &DeferredTaskTerminalWrite,
        new_revision: u64,
    ) -> Result<(), String> {
        let payload = terminal_reconciled_payload(node_id, attempt_id, deferred, new_revision);
        if self.terminal_reconciled_marker_exists(run_id, node_id, attempt_id, deferred, &payload)? {
            return Ok(());
        }
        match self.append_record(guard, run_id, "workflow_task_terminal_reconciled", payload.clone()) {
            Ok(_) => Ok(()),
            Err(append_error) => {
                match self.terminal_reconciled_marker_exists(run_id, node_id, attempt_id, deferred, &payload) {
                    Ok(true) => Ok(()),
                    Ok(false) => Err(append_error),
                    Err(lookup_error) => Err(format!(
                        "terminal reconciliation marker append failed: {append_error}; durable replay failed: {lookup_error}"
                    )),
                }
            }
        }
    }

    fn terminal_reconciled_marker_exists(
        &self,
        run_id: &RunId,
        node_id: &str,
        attempt_id: &AttemptId,
        deferred: &DeferredTaskTerminalWrite,
        expected_payload: &Value,
    ) -> Result<bool, String> {
        let mut found = false;
        for record in self.ledger.records_for_run(run_id).map_err(|error| error.to_string())? {
            if record.record_type != "workflow_task_terminal_reconciled" {
                continue;
            }
            let same_operation =
                record.payload.get("operation_id").and_then(Value::as_str) == Some(deferred.operation_id.as_str());
            let same_attempt = record.payload.get("node_id").and_then(Value::as_str) == Some(node_id)
                && record.payload.get("attempt_id").and_then(Value::as_str) == Some(attempt_id.as_str());
            if !same_operation && !same_attempt {
                continue;
            }
            if !same_operation || !same_attempt || record.payload != *expected_payload {
                return Err(format!(
                    "Workflow task terminal marker for node {node_id} conflicts with deferred operation"
                ));
            }
            found = true;
        }
        Ok(found)
    }

    pub(super) fn update_workflow_task(
        &self,
        guard: &WorkflowMutationGuard,
        run_id: &RunId,
        node_id: &str,
        update: WorkflowTaskUpdate,
    ) -> Result<u64, String> {
        let task_id = workflow_task_id(run_id, node_id);
        let task = self
            .tasks
            .get(&task_id)
            .ok_or_else(|| format!("unknown workflow task: {task_id}"))?;
        let expected_revision = task.revision;
        let mutation = TaskCasMutation::WorkflowState {
            state: update.state,
            clear_owner: update.clear_owner,
            outcome_ref: update.outcome_ref,
            failure_class: update.failure_class,
        };
        let mutation_digest = stable_digest_value(&serde_json::to_value(&mutation).map_err(|error| error.to_string())?);
        let updated = self
            .tasks
            .cas_durable(
                &task_id,
                expected_revision,
                &update.operation_id,
                &mutation_digest,
                &mutation,
                |next| {
                    let payload = json!({
                        "task_id": task_id,
                        "operation_id": update.operation_id,
                        "transition": mutation.name(),
                        "mutation": &mutation,
                        "mutation_digest": mutation_digest,
                        "expected_revision": expected_revision,
                        "new_revision": next.revision,
                        "expected_task": task,
                        "task": next,
                    });
                    if let Some(existing) = self.ledger.records_for_run(run_id)?.into_iter().find(|record| {
                        record.record_type == "task_cas"
                            && (record.payload.get("operation_id").and_then(Value::as_str)
                                == Some(update.operation_id.as_str())
                                || (record.payload.get("task_id").and_then(Value::as_str) == Some(task_id.as_str())
                                    && record.payload.get("expected_revision").and_then(Value::as_u64)
                                        == Some(expected_revision)))
                    }) {
                        return if existing.payload == payload {
                            Ok(())
                        } else {
                            Err(std::io::Error::other(format!(
                                "task revision {expected_revision} is already bound to a different mutation"
                            )))
                        };
                    }
                    tracing::debug!(
                        task_id = %task_id,
                        operation_id = %update.operation_id,
                        expected_revision,
                        new_revision = next.revision,
                        "persisting workflow task CAS"
                    );
                    let lease = guard
                        .lease
                        .as_ref()
                        .ok_or_else(|| std::io::Error::other("runtime ledger cannot fence Workflow task mutation"))?;
                    self.ledger.append_under_workflow_lease(
                        lease,
                        chrono::Utc::now().timestamp_millis(),
                        DurabilityClass::SyncCritical,
                        "task_cas",
                        payload,
                    )?;
                    Ok(())
                },
            )
            .map_err(|error| error.to_string())?;
        Ok(updated.revision)
    }

    pub(super) fn emit_current_task(&self, run_id: &RunId, node_id: &str, kind: &str) {
        if let Some(task) = self.tasks.get(&workflow_task_id(run_id, node_id)) {
            self.emit_task_event(run_id, kind, &task);
        }
    }

    pub(super) fn emit_task_event(&self, run_id: &RunId, kind: &str, task: &TaskRecord) {
        if let Some(runtime) = &self.runtime_events {
            runtime.emit_live_event(
                run_id.clone(),
                task.owner_agent_id.clone(),
                kind,
                serde_json::to_value(task).unwrap_or_default(),
            );
        }
    }
}

pub(super) fn validate_deferred_terminal_task(
    task: &TaskRecord,
    deferred: &DeferredTaskTerminalWrite,
) -> Result<(), String> {
    if task.state != deferred.state
        || task.outcome_ref != deferred.outcome_ref
        || task.failure_class != Some(deferred.failure_class)
        || (deferred.clear_owner && task.owner_agent_id.is_some())
    {
        return Err(format!(
            "durable task operation {} does not match the deferred terminal mutation",
            deferred.operation_id
        ));
    }
    Ok(())
}

pub(super) fn terminal_reconciled_payload(
    node_id: &str,
    attempt_id: &AttemptId,
    deferred: &DeferredTaskTerminalWrite,
    new_revision: u64,
) -> Value {
    json!({
        "node_id": node_id,
        "attempt_id": attempt_id,
        "operation_id": deferred.operation_id,
        "new_revision": new_revision,
    })
}

pub(super) fn workflow_status_to_task_state(status: WorkflowNodeStatus) -> TaskState {
    match status {
        WorkflowNodeStatus::Pending => TaskState::Queued,
        WorkflowNodeStatus::Running => TaskState::Running,
        WorkflowNodeStatus::Completed => TaskState::Completed,
        WorkflowNodeStatus::Failed => TaskState::Failed,
        WorkflowNodeStatus::Skipped => TaskState::Skipped,
        WorkflowNodeStatus::Cancelled => TaskState::Cancelled,
    }
}

pub(super) fn workflow_task_id(run_id: &RunId, node_id: &str) -> TaskId {
    TaskId::new(format!("workflow:{}:{node_id}", run_id.as_str()))
}
