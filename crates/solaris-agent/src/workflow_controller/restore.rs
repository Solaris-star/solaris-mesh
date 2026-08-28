use serde_json::{Value, json};
use solaris_types::identity::{AttemptId, OperationId, RunId};
use solaris_types::runtime::{TaskFailureClass, TaskRecord, TaskState};
use solaris_types::workflow::{WorkflowDefinition, WorkflowNodeStatus};

use crate::execution_context::{EffectOutputStore, stable_digest_bytes};

use super::node_state::{
    terminal_reconciled_payload, validate_deferred_terminal_task, workflow_status_to_task_state, workflow_task_id,
};
use super::projection::WorkflowMutationGuard;
use super::validation::validate_restored_checkpoints;
use super::{
    DeferredTaskTerminalWrite, WorkflowController, WorkflowProjection, WorkflowRunSnapshot, WorkflowRunStatus,
    workflow_input_digest,
};

const WORKFLOW_RESTORE_STALE: &str = "Workflow restore high-water changed during recovery";

impl WorkflowController {
    pub fn restore_from_ledger(&self, root_run_id: &RunId) -> Result<usize, String> {
        let prefix = format!("{}:", root_run_id.as_str());
        let run_ids = self.ledger.run_ids().map_err(|error| error.to_string())?;
        let mut restored = 0;
        for run_id in run_ids {
            if run_id == *root_run_id || !run_id.as_str().starts_with(&prefix) {
                continue;
            }
            if self.restore_workflow_run(&run_id)? {
                restored += 1;
            }
        }
        Ok(restored)
    }

    fn restore_workflow_run(&self, run_id: &RunId) -> Result<bool, String> {
        self.restore_workflow_run_with_before_projection(run_id, || {})
    }

    pub(super) fn restore_workflow_run_with_before_projection(
        &self,
        run_id: &RunId,
        before_projection: impl FnOnce(),
    ) -> Result<bool, String> {
        self.restore_workflow_run_with_observers(run_id, || {}, before_projection)
    }

    pub(super) fn restore_workflow_run_with_observers(
        &self,
        run_id: &RunId,
        before_commit: impl FnOnce(),
        before_projection: impl FnOnce(),
    ) -> Result<bool, String> {
        self.restore_workflow_run_with_guard_observer(run_id, before_commit, before_projection, |_| {})
    }

    #[cfg(test)]
    pub(super) fn restore_workflow_run_with_renew_observer(
        &self,
        run_id: &RunId,
        before_renew: impl FnOnce(&WorkflowMutationGuard),
    ) -> Result<bool, String> {
        self.restore_workflow_run_with_guard_observer(
            run_id,
            || {},
            || {},
            |guard| {
                guard.force_renewal_due();
                before_renew(guard);
            },
        )
    }

    fn restore_workflow_run_with_guard_observer<G: FnOnce(&mut WorkflowMutationGuard)>(
        &self,
        run_id: &RunId,
        before_commit: impl FnOnce(),
        before_projection: impl FnOnce(),
        before_guard_renew: G,
    ) -> Result<bool, String> {
        let line = self.mutation.line_for(run_id);
        let _mutation_guard = line.lock().unwrap_or_else(|error| error.into_inner());
        let mut guard = self.acquire_workflow_mutation_guard(run_id)?;
        let mut before_commit = Some(before_commit);
        let mut before_projection = Some(before_projection);
        let mut before_guard_renew = Some(before_guard_renew);
        let result = loop {
            match self.restore_workflow_run_locked(
                run_id,
                &mut guard,
                &mut before_commit,
                &mut before_projection,
                &mut before_guard_renew,
            ) {
                Err(error) if error == WORKFLOW_RESTORE_STALE => continue,
                result => break result,
            }
        };
        let release = self.release_workflow_mutation_guard(guard);
        match (result, release) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(release_error)) => Err(release_error),
            (Err(error), Err(release_error)) => Err(format!(
                "{error}; failed to release Workflow mutation lease: {release_error}"
            )),
        }
    }

    fn restore_workflow_run_locked<B: FnOnce(), P: FnOnce(), G: FnOnce(&mut WorkflowMutationGuard)>(
        &self,
        run_id: &RunId,
        guard: &mut WorkflowMutationGuard,
        before_commit: &mut Option<B>,
        before_projection: &mut Option<P>,
        before_guard_renew: &mut Option<G>,
    ) -> Result<bool, String> {
        let records = self.ledger.records_for_run(run_id).map_err(|error| error.to_string())?;
        let high_watermark = records.last().map_or(0, |record| record.seq);
        let Some(started) = records.iter().find(|record| record.record_type == "workflow_started") else {
            return Ok(false);
        };
        let Some(snapshot_value) = started.payload.get("snapshot").cloned() else {
            // Legacy in-memory-era ledgers did not persist a reconstructable
            // workflow snapshot. Do not guess parameters or initial node state.
            return Ok(false);
        };
        let mut snapshot: WorkflowRunSnapshot = serde_json::from_value(snapshot_value)
            .map_err(|error| format!("invalid workflow snapshot in ledger: {error}"))?;
        let Some(definition) = self.definition(&snapshot.workflow_id) else {
            return self.restore_as_reconciliation_required_locked(
                run_id,
                snapshot,
                format!(
                    "cannot restore unknown workflow definition: {}",
                    started
                        .payload
                        .get("workflow_id")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown")
                ),
                guard,
                high_watermark,
                before_commit,
            );
        };
        let input_digest = workflow_input_digest(&snapshot.parameters);
        if snapshot
            .input_digest
            .as_deref()
            .is_some_and(|stored_digest| stored_digest != input_digest)
        {
            return self.restore_as_reconciliation_required_locked(
                run_id,
                snapshot,
                "workflow snapshot input digest does not match its parameters".to_owned(),
                guard,
                high_watermark,
                before_commit,
            );
        }
        snapshot.input_digest = Some(input_digest);
        let mut terminal_reconciled_markers = Vec::new();
        let mut deferred_settled_status = None;

        for record in records.iter().skip_while(|record| record.seq <= started.seq) {
            match record.record_type.as_str() {
                "workflow_node_started" => {
                    let Some(node_id) = record.payload.get("node_id").and_then(Value::as_str) else {
                        continue;
                    };
                    let Some(attempt) = snapshot.nodes.get_mut(node_id) else {
                        continue;
                    };
                    attempt.attempt_number = attempt.attempt_number.saturating_add(1);
                    if let Some(attempt_id) = record.payload.get("attempt_id").and_then(Value::as_str) {
                        attempt.attempt_id = AttemptId::from(attempt_id);
                    }
                    attempt.status = WorkflowNodeStatus::Running;
                    attempt.input_digest = record
                        .payload
                        .get("input_digest")
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                    attempt.error = None;
                    attempt.failure_class = None;
                    attempt.resume_existing_attempt = false;
                }
                "workflow_node_completed" => {
                    let Some(node_id) = record.payload.get("node_id").and_then(Value::as_str) else {
                        continue;
                    };
                    self.validate_completion_source(&definition, run_id, record)?;
                    if record
                        .payload
                        .get("output_status")
                        .is_some_and(|status| status.as_str() != Some("completed"))
                    {
                        return self.restore_as_reconciliation_required_locked(
                            run_id,
                            snapshot,
                            format!("workflow node {node_id} protected output has an invalid status"),
                            guard,
                            high_watermark,
                            before_commit,
                        );
                    }
                    let Some(output_ref) = record.payload.get("output_ref").and_then(Value::as_str) else {
                        return self.restore_as_reconciliation_required_locked(
                            run_id,
                            snapshot,
                            format!("workflow node {node_id} completed without a protected output reference"),
                            guard,
                            high_watermark,
                            before_commit,
                        );
                    };
                    let serialized_output =
                        match EffectOutputStore::for_run_with_ledger(run_id, self.ledger.as_ref()).read(output_ref) {
                            Ok(output) => output,
                            Err(error) => {
                                return self.restore_as_reconciliation_required_locked(
                                    run_id,
                                    snapshot,
                                    format!("workflow node {node_id} protected output could not be read: {error}"),
                                    guard,
                                    high_watermark,
                                    before_commit,
                                );
                            }
                        };
                    let output_integrity_matches = record.payload.get("output_bytes").and_then(Value::as_u64)
                        == Some(serialized_output.len() as u64)
                        && record.payload.get("output_digest").and_then(Value::as_str)
                            == Some(stable_digest_bytes(serialized_output.as_bytes()).as_str());
                    if !output_integrity_matches {
                        return self.restore_as_reconciliation_required_locked(
                            run_id,
                            snapshot,
                            format!("workflow node {node_id} protected output failed integrity validation"),
                            guard,
                            high_watermark,
                            before_commit,
                        );
                    }
                    let output: Value = match serde_json::from_str(&serialized_output) {
                        Ok(output) => output,
                        Err(error) => {
                            return self.restore_as_reconciliation_required_locked(
                                run_id,
                                snapshot,
                                format!("workflow node {node_id} protected output is invalid JSON: {error}"),
                                guard,
                                high_watermark,
                                before_commit,
                            );
                        }
                    };
                    if let Some(attempt) = snapshot.nodes.get_mut(node_id) {
                        attempt.status = WorkflowNodeStatus::Completed;
                        attempt.output = Some(output);
                        attempt.output_ref = Some(output_ref.to_owned());
                        attempt.committed_at_unix_ms =
                            record.payload.get("committed_at_unix_ms").and_then(Value::as_i64);
                        attempt.error = None;
                        attempt.resume_existing_attempt = false;
                    }
                }
                "workflow_node_failed" => {
                    let Some(node_id) = record.payload.get("node_id").and_then(Value::as_str) else {
                        continue;
                    };
                    let Some(current_attempt) = snapshot.nodes.get(node_id) else {
                        continue;
                    };
                    let status = if record.payload.get("deferred_task_terminal_write").is_some() {
                        record
                            .payload
                            .get("state")
                            .cloned()
                            .ok_or_else(|| {
                                "typed deferred Workflow task terminal write is missing node state".to_owned()
                            })
                            .and_then(|value| {
                                serde_json::from_value(value)
                                    .map_err(|error| format!("invalid typed deferred Workflow node state: {error}"))
                            })?
                    } else {
                        record
                            .payload
                            .get("state")
                            .cloned()
                            .and_then(|value| serde_json::from_value(value).ok())
                            .unwrap_or(WorkflowNodeStatus::Failed)
                    };
                    let deferred_task_terminal_write = restore_deferred_task_terminal_write(
                        run_id,
                        node_id,
                        &current_attempt.attempt_id,
                        status,
                        &record.payload,
                    )?;
                    if let Some(attempt) = snapshot.nodes.get_mut(node_id) {
                        attempt.status = status;
                        attempt.error = record.payload.get("error").and_then(Value::as_str).map(str::to_owned);
                        attempt.failure_class = record
                            .payload
                            .get("failure_class")
                            .cloned()
                            .and_then(|value| serde_json::from_value(value).ok())
                            .or(Some(TaskFailureClass::NonRetryable));
                        attempt.resume_existing_attempt = false;
                        attempt.deferred_task_terminal_write = deferred_task_terminal_write;
                    }
                }
                "workflow_task_terminal_reconciled" => {
                    terminal_reconciled_markers.push(record.payload.clone());
                }
                "workflow_node_skipped" => {
                    let Some(node_id) = record.payload.get("node_id").and_then(Value::as_str) else {
                        continue;
                    };
                    if let Some(attempt) = snapshot.nodes.get_mut(node_id) {
                        attempt.status = WorkflowNodeStatus::Skipped;
                        attempt.resume_existing_attempt = false;
                    }
                }
                "workflow_cancelled" => {
                    if let Some(value) = record.payload.get("snapshot").cloned()
                        && let Ok(cancelled) = serde_json::from_value::<WorkflowRunSnapshot>(value)
                    {
                        snapshot = cancelled;
                    } else {
                        snapshot.status = WorkflowRunStatus::Cancelled;
                    }
                }
                "workflow_settled" => {
                    if let Some(status) = record
                        .payload
                        .get("status")
                        .cloned()
                        .and_then(|value| serde_json::from_value(value).ok())
                    {
                        let durable_failure_summary = record
                            .payload
                            .get("failure_summary")
                            .cloned()
                            .and_then(|value| serde_json::from_value(value).ok());
                        snapshot.failure_summary = durable_failure_summary;
                        if snapshot
                            .nodes
                            .values()
                            .any(|attempt| attempt.deferred_task_terminal_write.is_some())
                        {
                            deferred_settled_status = Some(status);
                        } else {
                            snapshot.status = status;
                        }
                    }
                }
                _ => {}
            }
        }

        if let Err(reason) = validate_restored_checkpoints(&definition, &mut snapshot) {
            return self.restore_as_reconciliation_required_locked(
                run_id,
                snapshot,
                reason,
                guard,
                high_watermark,
                before_commit,
            );
        }
        let effective_settled_status = deferred_settled_status.unwrap_or(snapshot.status);
        if effective_settled_status == WorkflowRunStatus::Failed {
            let recomputed = super::aggregate_workflow_failures(&snapshot.nodes);
            if snapshot.failure_summary.is_some() && snapshot.failure_summary != recomputed {
                return self.restore_as_reconciliation_required_locked(
                    run_id,
                    snapshot,
                    "durable Workflow failure summary does not match restored failed nodes".to_owned(),
                    guard,
                    high_watermark,
                    before_commit,
                );
            }
            snapshot.failure_summary = recomputed;
        } else {
            snapshot.failure_summary = None;
        }
        if snapshot.status == WorkflowRunStatus::Running {
            for attempt in snapshot.nodes.values_mut() {
                if attempt.status == WorkflowNodeStatus::Running {
                    attempt.status = WorkflowNodeStatus::Pending;
                    attempt.resume_existing_attempt = true;
                }
            }
        }
        if let Some(before_commit) = before_commit.take() {
            before_commit();
        }
        if let Some(before_guard_renew) = before_guard_renew.take() {
            before_guard_renew(guard);
        }
        self.commit_restore_high_water(guard, high_watermark)?;
        if let Some(before_projection) = before_projection.take() {
            before_projection();
        }
        self.validate_workflow_mutation_guard(guard)?;
        let previous_restore = self
            .runs
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(run_id)
            .map(|projection| projection.durable_sequence);
        if previous_restore.is_some_and(|previous| previous >= high_watermark) {
            return Ok(false);
        }
        self.restore_tasks_for_snapshot(&definition, &snapshot, guard)?;
        for marker in terminal_reconciled_markers {
            self.validate_terminal_reconciled_marker_locked(run_id, &mut snapshot, &marker, guard)?;
        }
        if snapshot
            .nodes
            .values()
            .any(|attempt| attempt.deferred_task_terminal_write.is_some())
        {
            if deferred_settled_status.is_some() {
                tracing::warn!(
                    workflow_run_id = %run_id,
                    "ignoring Workflow settlement recorded before deferred task terminal reconciliation"
                );
            }
        } else if let Some(status) = deferred_settled_status {
            snapshot.status = status;
        }
        self.validate_workflow_mutation_guard(guard)?;
        self.publish_restored_projection(run_id, snapshot, guard)?;
        Ok(true)
    }

    fn publish_restored_projection(
        &self,
        run_id: &RunId,
        snapshot: WorkflowRunSnapshot,
        guard: &WorkflowMutationGuard,
    ) -> Result<(), String> {
        let durable_sequence = guard
            .lease
            .as_ref()
            .ok_or_else(|| "runtime ledger cannot fence Workflow restore publication".to_owned())?
            .observed_sequence;
        let published = WorkflowProjection {
            snapshot,
            durable_sequence,
        };
        let mut runs = self.runs.write().unwrap_or_else(|error| error.into_inner());
        let previous = runs.insert(run_id.clone(), published);
        if let Err(error) = self.commit_projection_high_water(guard, durable_sequence) {
            match previous {
                Some(projection) => {
                    runs.insert(run_id.clone(), projection);
                }
                None => {
                    runs.remove(run_id);
                }
            }
            return Err(error);
        }
        Ok(())
    }

    fn commit_restore_high_water(&self, guard: &mut WorkflowMutationGuard, high_watermark: u64) -> Result<(), String> {
        self.renew_workflow_mutation_guard(guard)?;
        let Some(lease) = guard.lease.as_ref() else {
            return Ok(());
        };
        match self
            .ledger
            .commit_workflow_restore(lease, high_watermark, chrono::Utc::now().timestamp_millis())
            .map_err(|error| format!("failed to commit Workflow restore high-water: {error}"))?
        {
            crate::runtime_ledger::WorkflowRestoreCommit::Current => Ok(()),
            crate::runtime_ledger::WorkflowRestoreCommit::Stale { .. } => Err(WORKFLOW_RESTORE_STALE.to_owned()),
        }
    }

    fn validate_terminal_reconciled_marker_locked(
        &self,
        run_id: &RunId,
        snapshot: &mut WorkflowRunSnapshot,
        marker: &Value,
        guard: &WorkflowMutationGuard,
    ) -> Result<(), String> {
        if guard.run_id != *run_id || guard.owner_id != self.mutation_owner_id {
            return Err("Workflow mutation guard does not cover terminal marker recovery".to_owned());
        }
        let node_id = marker
            .get("node_id")
            .and_then(Value::as_str)
            .ok_or_else(|| "Workflow task terminal marker is missing node_id".to_owned())?;
        let attempt = snapshot
            .nodes
            .get(node_id)
            .ok_or_else(|| format!("Workflow task terminal marker references unknown node: {node_id}"))?;
        let deferred = attempt.deferred_task_terminal_write.clone().ok_or_else(|| {
            format!("Workflow task terminal marker for node {node_id} has no deferred terminal mutation")
        })?;
        let task_id = workflow_task_id(run_id, node_id);
        let (_, terminal) = self.restore_durable_task_operation(&task_id, &deferred.operation_id)?;
        let terminal = terminal.ok_or_else(|| {
            format!(
                "Workflow task terminal marker for node {node_id} has no durable operation {}",
                deferred.operation_id
            )
        })?;
        validate_deferred_terminal_task(&terminal, &deferred)?;
        let expected = terminal_reconciled_payload(node_id, &attempt.attempt_id, &deferred, terminal.revision);
        if marker != &expected {
            return Err(format!(
                "Workflow task terminal marker for node {node_id} does not match its durable operation"
            ));
        }
        snapshot
            .nodes
            .get_mut(node_id)
            .expect("Workflow node was validated above")
            .deferred_task_terminal_write = None;
        Ok(())
    }

    fn restore_as_reconciliation_required_locked<B: FnOnce()>(
        &self,
        run_id: &RunId,
        mut snapshot: WorkflowRunSnapshot,
        reason: String,
        guard: &mut WorkflowMutationGuard,
        high_watermark: u64,
        before_commit: &mut Option<B>,
    ) -> Result<bool, String> {
        snapshot.status = WorkflowRunStatus::Failed;
        snapshot.reconciliation_reason = Some(reason.clone());
        snapshot.failure_summary = None;
        for attempt in snapshot.nodes.values_mut() {
            if matches!(
                attempt.status,
                WorkflowNodeStatus::Pending | WorkflowNodeStatus::Running
            ) {
                attempt.status = WorkflowNodeStatus::Failed;
                attempt.error = Some(reason.clone());
                attempt.failure_class = Some(TaskFailureClass::ReconciliationRequired);
                attempt.resume_existing_attempt = false;
            }
        }
        snapshot.failure_summary = super::aggregate_workflow_failures(&snapshot.nodes);
        if let Some(before_commit) = before_commit.take() {
            before_commit();
        }
        self.commit_restore_high_water(guard, high_watermark)?;
        let payload = json!({
            "workflow_run_id": run_id,
            "reason": reason,
            "snapshot": snapshot,
        });
        let lease = guard
            .lease
            .as_ref()
            .ok_or_else(|| "runtime ledger cannot fence Workflow reconciliation".to_owned())?;
        let record = self
            .ledger
            .compare_and_append_under_workflow_lease(
                lease,
                chrono::Utc::now().timestamp_millis(),
                solaris_types::effect::DurabilityClass::SyncCritical,
                "workflow_reconciliation_required",
                &["workflow_run_id"],
                payload.clone(),
            )
            .map_err(|error| error.to_string())?;
        self.validate_workflow_mutation_guard(guard)?;
        self.publish_restored_projection(run_id, snapshot.clone(), guard)?;
        if let Some(runtime) = &self.runtime_events {
            runtime.emit_durable_event(&record, None, "workflow_reconciliation_required", payload);
        }
        Ok(true)
    }

    fn restore_tasks_for_snapshot(
        &self,
        definition: &WorkflowDefinition,
        snapshot: &WorkflowRunSnapshot,
        guard: &mut WorkflowMutationGuard,
    ) -> Result<(), String> {
        for node in &definition.nodes {
            self.validate_workflow_mutation_guard(guard)?;
            let state = snapshot
                .nodes
                .get(&node.id)
                .map(|attempt| {
                    if attempt.deferred_task_terminal_write.is_some() {
                        TaskState::Queued
                    } else {
                        workflow_status_to_task_state(attempt.status)
                    }
                })
                .unwrap_or(TaskState::Queued);
            let restored = TaskRecord {
                run_id: snapshot.run_id.clone(),
                task_id: workflow_task_id(&snapshot.run_id, &node.id),
                revision: 0,
                task_key: Some(format!("workflow:{}:{}", definition.id, node.id)),
                team_id: None,
                workflow_id: Some(definition.id.clone()),
                node_id: Some(node.id.clone()),
                role: node.role.clone(),
                depends_on: node
                    .depends_on
                    .iter()
                    .map(|dependency| workflow_task_id(&snapshot.run_id, dependency))
                    .collect(),
                content: None,
                expected_write_scope: Vec::new(),
                owner_agent_id: None,
                state,
                outcome_ref: None,
                failure_class: None,
            };
            if let Some(existing) = self.tasks.get(&restored.task_id) {
                if !task_immutable_metadata_matches(&existing, &restored) {
                    return Err(format!(
                        "workflow task {} has incompatible immutable metadata",
                        restored.task_id
                    ));
                }
                continue;
            }
            self.tasks.upsert(restored);
        }
        Ok(())
    }
}

fn restore_deferred_task_terminal_write(
    run_id: &RunId,
    node_id: &str,
    attempt_id: &AttemptId,
    node_status: WorkflowNodeStatus,
    payload: &Value,
) -> Result<Option<DeferredTaskTerminalWrite>, String> {
    let legacy_deferred = payload.get("task_terminal_write_deferred").and_then(Value::as_bool);
    let canonical_operation_id = OperationId::new(format!("workflow:{run_id}:{node_id}:{attempt_id}:failed"));
    let Some(typed_value) = payload
        .get("deferred_task_terminal_write")
        .filter(|value| !value.is_null())
    else {
        if legacy_deferred != Some(true) {
            return Ok(None);
        }
        let failure_class = payload
            .get("failure_class")
            .cloned()
            .and_then(|value| serde_json::from_value(value).ok())
            .unwrap_or(TaskFailureClass::ReconciliationRequired);
        return Ok(Some(DeferredTaskTerminalWrite {
            state: TaskState::Failed,
            clear_owner: false,
            outcome_ref: None,
            failure_class,
            operation_id: canonical_operation_id,
        }));
    };

    let deferred: DeferredTaskTerminalWrite = serde_json::from_value(typed_value.clone())
        .map_err(|error| format!("invalid typed deferred Workflow task terminal write: {error}"))?;
    let outer_failure_class = payload
        .get("failure_class")
        .cloned()
        .ok_or_else(|| "typed deferred Workflow task terminal write is missing failure_class".to_owned())
        .and_then(|value| {
            serde_json::from_value(value)
                .map_err(|error| format!("invalid typed deferred Workflow failure_class: {error}"))
        })?;
    if legacy_deferred != Some(true) {
        return Err("typed deferred Workflow task terminal write conflicts with its legacy flag".to_owned());
    }
    if node_status != WorkflowNodeStatus::Failed {
        return Err("typed deferred Workflow task terminal write requires a Failed node".to_owned());
    }
    if outer_failure_class != TaskFailureClass::ReconciliationRequired || deferred.failure_class != outer_failure_class
    {
        return Err("typed deferred Workflow task terminal write has inconsistent failure_class".to_owned());
    }
    if deferred.operation_id != canonical_operation_id {
        return Err("typed deferred Workflow task terminal write has a non-canonical operation_id".to_owned());
    }
    if deferred.state != TaskState::Failed || deferred.clear_owner || deferred.outcome_ref.is_some() {
        return Err("typed deferred Workflow task terminal write has invalid mutation semantics".to_owned());
    }
    Ok(Some(deferred))
}

fn task_immutable_metadata_matches(existing: &TaskRecord, restored: &TaskRecord) -> bool {
    existing.run_id == restored.run_id
        && existing.task_id == restored.task_id
        && existing.task_key == restored.task_key
        && existing.team_id == restored.team_id
        && existing.workflow_id == restored.workflow_id
        && existing.node_id == restored.node_id
        && existing.role == restored.role
        && existing.depends_on == restored.depends_on
        && existing.content == restored.content
        && existing.expected_write_scope == restored.expected_write_scope
}
