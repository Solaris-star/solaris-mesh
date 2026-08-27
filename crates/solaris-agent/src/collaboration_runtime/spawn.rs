//! Durable Agent spawn lifecycle operations.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::{AgentId, ChildAgentKey, OperationId, RunId};
use solaris_types::permission::PermissionCeiling;
use solaris_types::runtime::{AgentLifecycleState, AgentRecord};
use solaris_types::spawner::{AgentHandle, AgentOutcomeStatus, OutcomeBlobRef, SubAgentResult};

use crate::execution_context::{EffectOutputStore, stable_digest_bytes};
use crate::permission_engine::PermissionContext;
use crate::relationship_store::{AgentRelationship, AgentRelationshipStore, InMemoryAgentRelationshipStore};

use super::{AgentSpawnReservation, CollaborationRuntime};

#[derive(Serialize, Deserialize)]
struct DurableAgentOutcomeRecord {
    spawn_operation_id: OperationId,
    child_agent_id: AgentId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    status: Option<AgentOutcomeStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    outcome_ref: Option<OutcomeBlobRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    result: Option<SubAgentResult>,
}

#[derive(Serialize, Deserialize)]
struct DurableAgentOutcomeBody {
    result: SubAgentResult,
    text_source: DurableAgentOutcomeTextSource,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DurableAgentOutcomeTextSource {
    Inline,
    OutputText,
    OutputError,
}

pub(super) fn agent_state_is_terminal(state: AgentLifecycleState) -> bool {
    matches!(
        state,
        AgentLifecycleState::Completed | AgentLifecycleState::Failed | AgentLifecycleState::Cancelled
    )
}

pub(super) fn outcome_agent_state(result: &SubAgentResult) -> AgentLifecycleState {
    match result.status {
        AgentOutcomeStatus::Completed => AgentLifecycleState::Completed,
        AgentOutcomeStatus::Cancelled => AgentLifecycleState::Cancelled,
        AgentOutcomeStatus::Failed
        | AgentOutcomeStatus::OutcomeUnknown
        | AgentOutcomeStatus::ReconciliationRequired => AgentLifecycleState::Failed,
    }
}

impl<T> CollaborationRuntime<T> {
    pub fn relationships(&self) -> Arc<InMemoryAgentRelationshipStore> {
        Arc::clone(&self.relationships)
    }

    pub fn existing_child_identity(&self, key: &ChildAgentKey) -> Option<(AgentId, u8)> {
        self.relationships.relationship_for_key(key).map(|relationship| {
            let version = if relationship.child_identity_version == 0 {
                ChildAgentKey::LEGACY_IDENTITY_VERSION
            } else {
                relationship.child_identity_version
            };
            (relationship.child_agent_id, version)
        })
    }

    pub fn record_agent_handle(&self, handle: &AgentHandle) -> std::io::Result<()> {
        let line = self.mutation.line_for(&handle.run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        for record in self.ledger.records_for_run(&handle.run_id)?.into_iter().rev() {
            if record.record_type != "agent_handle_issued"
                || record.payload.get("operation_id").and_then(serde_json::Value::as_str)
                    != Some(handle.operation_id.as_str())
            {
                continue;
            }
            let recorded_digest = record
                .payload
                .get("spec_digest")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            if recorded_digest != handle.spec_digest {
                return Err(std::io::Error::other(format!(
                    "spawn operation {} was already issued for a different AgentSpawnSpec",
                    handle.operation_id
                )));
            }
            return Ok(());
        }
        self.ledger.append(
            &handle.run_id,
            DurabilityClass::SyncCritical,
            "agent_handle_issued",
            serde_json::to_value(handle).map_err(std::io::Error::other)?,
        )?;
        Ok(())
    }

    pub fn reserve_spawn(
        &self,
        run_id: RunId,
        parent_agent_id: AgentId,
        spawn_operation_id: OperationId,
        permission_context: &PermissionContext,
        requested_ceiling: PermissionCeiling,
    ) -> std::io::Result<AgentSpawnReservation> {
        let role_key = spawn_operation_id.as_str().to_owned();
        let stable_task_key = spawn_operation_id.as_str().to_owned();
        self.reserve_spawn_typed(
            run_id,
            parent_agent_id,
            role_key,
            stable_task_key,
            spawn_operation_id,
            permission_context,
            requested_ceiling,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn reserve_spawn_typed(
        &self,
        run_id: RunId,
        parent_agent_id: AgentId,
        role_key: String,
        stable_task_key: String,
        spawn_operation_id: OperationId,
        permission_context: &PermissionContext,
        requested_ceiling: PermissionCeiling,
    ) -> std::io::Result<AgentSpawnReservation> {
        let key = ChildAgentKey {
            run_id: run_id.clone(),
            parent_agent_id: parent_agent_id.clone(),
            role_key: role_key.clone(),
            stable_task_key: stable_task_key.clone(),
            spawn_operation_id: spawn_operation_id.clone(),
        };
        let line = self.mutation.line_for(&run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(child_agent_id) = self.relationships.child_for_key(&key) {
            let Some(existing) = self.agents.get(&child_agent_id) else {
                return Err(std::io::Error::other(format!(
                    "spawn relationship for {child_agent_id} has no Agent projection; reconciliation is required"
                )));
            };
            if existing.run_id != run_id || existing.parent_agent_id.as_ref() != Some(&parent_agent_id) {
                return Err(std::io::Error::other(
                    "spawn relationship points to an Agent from another run or parent",
                ));
            }
            let existing_relationship = self
                .relationships
                .snapshot()
                .into_iter()
                .find(|relationship| relationship.child_agent_id == child_agent_id)
                .ok_or_else(|| std::io::Error::other("spawn relationship projection is missing"))?;
            if existing_relationship.spawn_operation_id != spawn_operation_id {
                if existing.state != AgentLifecycleState::Failed {
                    return Err(std::io::Error::other(format!(
                        "stable child task is already bound to operation {} with state {:?}",
                        existing_relationship.spawn_operation_id, existing.state
                    )));
                }
                let retry_relationship = AgentRelationship {
                    run_id: run_id.clone(),
                    parent_agent_id: parent_agent_id.clone(),
                    child_agent_id: child_agent_id.clone(),
                    child_identity_version: if existing_relationship.child_identity_version == 0 {
                        ChildAgentKey::LEGACY_IDENTITY_VERSION
                    } else {
                        existing_relationship.child_identity_version
                    },
                    role_key: role_key.clone(),
                    stable_task_key: stable_task_key.clone(),
                    spawn_operation_id: spawn_operation_id.clone(),
                };
                let durable = self.ledger.append(
                    &run_id,
                    DurabilityClass::SyncCritical,
                    "agent_spawn_retry_started",
                    serde_json::to_value(&retry_relationship).map_err(std::io::Error::other)?,
                )?;
                self.relationships.put(retry_relationship.clone());
                self.agents.set_state(&child_agent_id, AgentLifecycleState::Active);
                self.emit_durable_event(
                    &durable,
                    Some(child_agent_id.clone()),
                    "agent_spawn_retry_started",
                    serde_json::to_value(retry_relationship).unwrap_or_default(),
                );
            }
            return Ok(AgentSpawnReservation {
                key,
                child_agent_id,
                permission_ceiling: permission_context.ceiling().intersect(requested_ceiling),
                reattached: true,
            });
        }

        let child_agent_id = key.agent_id();
        let permission_ceiling = permission_context.ceiling().intersect(requested_ceiling);
        if let Some(existing) = self.agents.get(&child_agent_id) {
            if existing.run_id != run_id || existing.parent_agent_id.as_ref() != Some(&parent_agent_id) {
                return Err(std::io::Error::other(
                    "stable child identity collides with another run or parent",
                ));
            }
            return Ok(AgentSpawnReservation {
                key,
                child_agent_id,
                permission_ceiling,
                reattached: false,
            });
        }
        let durable = self.ledger.append(
            &run_id,
            DurabilityClass::SyncCritical,
            "agent_spawn_intent",
            json!({
                "parent_agent_id": parent_agent_id,
                "child_agent_id": child_agent_id,
                "role_key": role_key,
                "stable_task_key": stable_task_key,
                "spawn_operation_id": spawn_operation_id,
            }),
        )?;
        self.agents.upsert(AgentRecord {
            run_id: run_id.clone(),
            agent_id: child_agent_id.clone(),
            team_id: None,
            parent_agent_id: Some(parent_agent_id.clone()),
            state: AgentLifecycleState::Reserved,
        });
        self.emit_durable_event(
            &durable,
            Some(child_agent_id.clone()),
            "agent_spawn_reserved",
            json!({"parent_agent_id": parent_agent_id, "child_agent_id": child_agent_id, "spawn_operation_id": spawn_operation_id}),
        );

        Ok(AgentSpawnReservation {
            key,
            child_agent_id,
            permission_ceiling,
            reattached: false,
        })
    }

    pub fn commit_spawn(&self, reservation: &AgentSpawnReservation) -> std::io::Result<()> {
        if reservation.reattached {
            return Ok(());
        }
        let line = self.mutation.line_for(&reservation.key.run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(existing_child) = self.relationships.child_for_key(&reservation.key) {
            if existing_child != reservation.child_agent_id {
                return Err(std::io::Error::other(
                    "spawn relationship is bound to a different child Agent",
                ));
            }
            let agent = self
                .agents
                .get(&reservation.child_agent_id)
                .ok_or_else(|| std::io::Error::other("committed spawn relationship has no Agent projection"))?;
            return if agent.state == AgentLifecycleState::Active {
                Ok(())
            } else {
                Err(std::io::Error::other(format!(
                    "spawn is already terminal and cannot be committed again: {:?}",
                    agent.state
                )))
            };
        }
        let agent = self.agents.get(&reservation.child_agent_id).ok_or_else(|| {
            std::io::Error::other(format!(
                "reserved child agent {} is missing",
                reservation.child_agent_id
            ))
        })?;
        if agent.state != AgentLifecycleState::Reserved {
            return Err(std::io::Error::other(format!(
                "spawn commit requires a Reserved Agent, found {:?}",
                agent.state
            )));
        }
        let relationship = AgentRelationship {
            run_id: reservation.key.run_id.clone(),
            parent_agent_id: reservation.key.parent_agent_id.clone(),
            child_agent_id: reservation.child_agent_id.clone(),
            child_identity_version: reservation
                .key
                .identity_version_for_agent_id(&reservation.child_agent_id)
                .ok_or_else(|| std::io::Error::other("spawn reservation has an unknown child identity version"))?,
            role_key: reservation.key.role_key.clone(),
            stable_task_key: reservation.key.stable_task_key.clone(),
            spawn_operation_id: reservation.key.spawn_operation_id.clone(),
        };
        let durable = self.ledger.append(
            &reservation.key.run_id,
            DurabilityClass::SyncCritical,
            "agent_spawn_committed",
            serde_json::to_value(&relationship).map_err(std::io::Error::other)?,
        )?;
        self.relationships.put(relationship.clone());
        let activated = self
            .agents
            .set_state(&reservation.child_agent_id, AgentLifecycleState::Active);
        debug_assert!(activated);
        self.emit_durable_event(
            &durable,
            Some(reservation.child_agent_id.clone()),
            "agent_spawn_committed",
            serde_json::to_value(&relationship).unwrap_or_default(),
        );
        Ok(())
    }

    pub fn record_agent_outcome(
        &self,
        reservation: &AgentSpawnReservation,
        result: &SubAgentResult,
    ) -> std::io::Result<()> {
        let line = self.mutation.line_for(&reservation.key.run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        for record in self.ledger.records_for_run(&reservation.key.run_id)?.into_iter().rev() {
            if record.record_type != "agent_outcome"
                || record
                    .payload
                    .get("spawn_operation_id")
                    .and_then(serde_json::Value::as_str)
                    != Some(reservation.key.spawn_operation_id.as_str())
            {
                continue;
            }
            let (existing, _) = self.read_agent_outcome_record(&reservation.key.run_id, record.payload)?;
            return if serde_json::to_value(&existing).ok() == serde_json::to_value(result).ok() {
                Ok(())
            } else {
                Err(std::io::Error::other(
                    "spawn already has a different terminal Agent outcome",
                ))
            };
        }
        let serialized = encode_agent_outcome_body(result)?;
        let result_bytes =
            u64::try_from(serialized.len()).map_err(|_| std::io::Error::other("Agent outcome is too large"))?;
        let result_digest = stable_digest_bytes(serialized.as_bytes());
        let store = EffectOutputStore::for_run_with_ledger(&reservation.key.run_id, self.ledger.as_ref());
        let reference = store.write_named(reservation.key.spawn_operation_id.as_str(), &serialized)?;
        let outcome_ref = OutcomeBlobRef {
            reference,
            bytes: result_bytes,
            digest: result_digest,
            run_id: Some(reservation.key.run_id.clone()),
            status: Some(result.status),
        };
        let payload = serde_json::to_value(DurableAgentOutcomeRecord {
            spawn_operation_id: reservation.key.spawn_operation_id.clone(),
            child_agent_id: reservation.child_agent_id.clone(),
            status: Some(result.status),
            outcome_ref: Some(outcome_ref),
            result: Some(agent_outcome_projection(result)),
        })
        .map_err(std::io::Error::other)?;
        let current = self
            .agents
            .get(&reservation.child_agent_id)
            .ok_or_else(|| std::io::Error::other("spawn outcome Agent projection is missing"))?;
        let target_state = outcome_agent_state(result);
        if agent_state_is_terminal(current.state) && current.state != target_state {
            return Err(std::io::Error::other(format!(
                "spawn Agent already has a different terminal state: {:?}",
                current.state
            )));
        }
        let record = self.ledger.append(
            &reservation.key.run_id,
            DurabilityClass::SyncCritical,
            "agent_outcome",
            payload.clone(),
        )?;
        self.agents.set_state(&reservation.child_agent_id, target_state);
        self.emit_durable_event(
            &record,
            Some(reservation.child_agent_id.clone()),
            "agent_outcome",
            payload,
        );
        Ok(())
    }

    pub fn agent_outcome(&self, reservation: &AgentSpawnReservation) -> std::io::Result<Option<SubAgentResult>> {
        self.agent_outcome_by_identity(
            &reservation.key.run_id,
            &reservation.key.spawn_operation_id,
            &reservation.child_agent_id,
        )
        .map(|outcome| outcome.map(|(result, _)| result))
    }

    pub(crate) fn agent_outcome_by_identity(
        &self,
        run_id: &RunId,
        operation_id: &OperationId,
        child_agent_id: &AgentId,
    ) -> std::io::Result<Option<(SubAgentResult, Option<OutcomeBlobRef>)>> {
        let records = self.ledger.records_for_run(run_id)?;
        for record in records.into_iter().rev() {
            if record.record_type != "agent_outcome" {
                continue;
            }
            let same_operation = record
                .payload
                .get("spawn_operation_id")
                .and_then(serde_json::Value::as_str)
                == Some(operation_id.as_str());
            let same_child = record.payload.get("child_agent_id").and_then(serde_json::Value::as_str)
                == Some(child_agent_id.as_str());
            if same_operation && same_child {
                return self.read_agent_outcome_record(run_id, record.payload).map(Some);
            }
        }
        Ok(None)
    }

    fn read_agent_outcome_record(
        &self,
        run_id: &RunId,
        payload: Value,
    ) -> std::io::Result<(SubAgentResult, Option<OutcomeBlobRef>)> {
        let record: DurableAgentOutcomeRecord = serde_json::from_value(payload).map_err(std::io::Error::other)?;
        if let Some(reference) = record.outcome_ref {
            let result = self.read_agent_outcome_blob(run_id, &reference)?;
            if record.status.is_some_and(|status| status != result.status) {
                return Err(std::io::Error::other(
                    "durable Agent outcome status conflicts with its blob",
                ));
            }
            return Ok((result, Some(reference)));
        }
        record
            .result
            .map(|result| (result, None))
            .ok_or_else(|| std::io::Error::other("agent_outcome record missing result or outcome_ref"))
    }

    pub(crate) fn read_agent_outcome_blob(
        &self,
        run_id: &RunId,
        reference: &OutcomeBlobRef,
    ) -> std::io::Result<SubAgentResult> {
        if reference
            .run_id
            .as_ref()
            .is_some_and(|reference_run| reference_run != run_id)
        {
            return Err(std::io::Error::other(
                "durable Agent outcome blob belongs to a different Run",
            ));
        }
        let store =
            EffectOutputStore::for_run_with_ledger(reference.run_id.as_ref().unwrap_or(run_id), self.ledger.as_ref());
        let serialized = store.read(&reference.reference)?;
        if u64::try_from(serialized.len()).ok() != Some(reference.bytes)
            || stable_digest_bytes(serialized.as_bytes()) != reference.digest
        {
            return Err(std::io::Error::other("durable Agent outcome blob identity changed"));
        }
        let result = decode_agent_outcome_body(&serialized)?;
        if reference.status.is_some_and(|status| status != result.status) {
            return Err(std::io::Error::other(
                "durable Agent outcome status conflicts with its blob reference",
            ));
        }
        Ok(result)
    }

    pub fn set_agent_state(
        &self,
        run_id: &RunId,
        agent_id: &AgentId,
        state: AgentLifecycleState,
    ) -> std::io::Result<bool> {
        let line = self.mutation.line_for(run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        let current = self
            .agents
            .get(agent_id)
            .ok_or_else(|| std::io::Error::other(format!("unknown agent: {agent_id}")))?;
        if current.run_id != *run_id {
            return Err(std::io::Error::other("agent belongs to a different run"));
        }
        if current.state == state {
            return Ok(false);
        }
        if agent_state_is_terminal(current.state) {
            return Err(std::io::Error::other(format!(
                "Agent already has terminal state {:?}",
                current.state
            )));
        }
        let durable = self.ledger.append(
            run_id,
            DurabilityClass::SyncCritical,
            "agent_state_changed",
            json!({"agent_id": agent_id, "state": state}),
        )?;
        let updated = self.agents.set_state(agent_id, state);
        debug_assert!(updated);
        self.emit_durable_event(
            &durable,
            Some(agent_id.clone()),
            "agent_state_changed",
            json!({"agent_id": agent_id, "state": state}),
        );
        Ok(true)
    }

    pub fn abort_spawn(&self, reservation: &AgentSpawnReservation, reason: &str) -> std::io::Result<()> {
        let line = self.mutation.line_for(&reservation.key.run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        if reservation.reattached {
            if self
                .ledger
                .records_for_run(&reservation.key.run_id)?
                .into_iter()
                .any(|record| {
                    record.record_type == "agent_reattach_failed"
                        && record
                            .payload
                            .get("spawn_operation_id")
                            .and_then(serde_json::Value::as_str)
                            == Some(reservation.key.spawn_operation_id.as_str())
                })
            {
                return Ok(());
            }
            let durable = self.ledger.append(
                &reservation.key.run_id,
                DurabilityClass::SyncCritical,
                "agent_reattach_failed",
                json!({
                    "child_agent_id": reservation.child_agent_id,
                    "spawn_operation_id": reservation.key.spawn_operation_id,
                    "reason": reason,
                }),
            )?;
            self.emit_durable_event(
                &durable,
                Some(reservation.child_agent_id.clone()),
                "agent_reattach_failed",
                durable.payload.clone(),
            );
            return Ok(());
        }
        if self.relationships.child_for_key(&reservation.key).is_some() {
            return self.cancel_spawn_within_mutation(reservation, reason);
        }
        if self.agents.get(&reservation.child_agent_id).is_none() {
            let already_aborted = self
                .ledger
                .records_for_run(&reservation.key.run_id)?
                .into_iter()
                .any(|record| {
                    record.record_type == "agent_spawn_aborted"
                        && record
                            .payload
                            .get("spawn_operation_id")
                            .and_then(serde_json::Value::as_str)
                            == Some(reservation.key.spawn_operation_id.as_str())
                });
            return if already_aborted {
                Ok(())
            } else {
                Err(std::io::Error::other("spawn reservation Agent is missing before abort"))
            };
        }
        let team_id = self
            .agents
            .get(&reservation.child_agent_id)
            .and_then(|agent| agent.team_id);
        let durable = self.ledger.append(
            &reservation.key.run_id,
            DurabilityClass::SyncCritical,
            "agent_spawn_aborted",
            json!({"child_agent_id": reservation.child_agent_id, "spawn_operation_id": reservation.key.spawn_operation_id, "team_id": team_id, "reason": reason, "disposition": "removed", "reattached": false}),
        )?;
        if let Some(team_id) = team_id.as_ref() {
            self.teams.leave(team_id, &reservation.child_agent_id);
        }
        self.agents.remove(&reservation.child_agent_id);
        self.emit_durable_event(
            &durable,
            Some(reservation.child_agent_id.clone()),
            "agent_spawn_aborted",
            json!({"child_agent_id": reservation.child_agent_id, "spawn_operation_id": reservation.key.spawn_operation_id, "team_id": team_id, "reason": reason, "disposition": "removed", "reattached": false}),
        );
        Ok(())
    }

    pub fn cancel_spawn(&self, reservation: &AgentSpawnReservation, reason: &str) -> std::io::Result<()> {
        let line = self.mutation.line_for(&reservation.key.run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        self.cancel_spawn_within_mutation(reservation, reason)
    }

    fn cancel_spawn_within_mutation(&self, reservation: &AgentSpawnReservation, reason: &str) -> std::io::Result<()> {
        let current = self
            .agents
            .get(&reservation.child_agent_id)
            .ok_or_else(|| std::io::Error::other("spawn cancellation Agent projection is missing"))?;
        if current.state == AgentLifecycleState::Cancelled {
            return Ok(());
        }
        if matches!(
            current.state,
            AgentLifecycleState::Completed | AgentLifecycleState::Failed
        ) {
            return Err(std::io::Error::other(format!(
                "spawn already has terminal state {:?}",
                current.state
            )));
        }
        if current.state == AgentLifecycleState::Reserved
            && self.relationships.child_for_key(&reservation.key).is_none()
        {
            let team_id = current.team_id;
            let payload = json!({
                "child_agent_id": reservation.child_agent_id,
                "spawn_operation_id": reservation.key.spawn_operation_id,
                "team_id": team_id,
                "reason": reason,
                "disposition": "cancelled",
                "reattached": reservation.reattached,
            });
            let durable = self.ledger.append(
                &reservation.key.run_id,
                DurabilityClass::SyncCritical,
                "agent_spawn_cancelled",
                payload.clone(),
            )?;
            if let Some(team_id) = team_id.as_ref() {
                self.teams.leave(team_id, &reservation.child_agent_id);
                self.agents.set_team(&reservation.child_agent_id, None);
            }
            self.agents
                .set_state(&reservation.child_agent_id, AgentLifecycleState::Cancelled);
            self.emit_durable_event(
                &durable,
                Some(reservation.child_agent_id.clone()),
                "agent_spawn_cancelled",
                payload,
            );
            return Ok(());
        }
        let team_id = current.team_id;
        let payload = json!({
            "child_agent_id": reservation.child_agent_id,
            "spawn_operation_id": reservation.key.spawn_operation_id,
            "team_id": team_id,
            "reason": reason,
        });
        let durable = self.ledger.append(
            &reservation.key.run_id,
            DurabilityClass::SyncCritical,
            "agent_spawn_cancelled",
            payload.clone(),
        )?;
        if let Some(team_id) = team_id.as_ref() {
            self.teams.leave(team_id, &reservation.child_agent_id);
            self.agents.set_team(&reservation.child_agent_id, None);
        }
        self.agents
            .set_state(&reservation.child_agent_id, AgentLifecycleState::Cancelled);
        self.emit_durable_event(
            &durable,
            Some(reservation.child_agent_id.clone()),
            "agent_spawn_cancelled",
            payload,
        );
        Ok(())
    }
}

fn encode_agent_outcome_body(result: &SubAgentResult) -> std::io::Result<String> {
    let mut stored = result.clone();
    let text_source = match result.output.as_ref() {
        Some(output) if output.get("text").and_then(Value::as_str) == Some(result.text.as_str()) => {
            stored.text.clear();
            DurableAgentOutcomeTextSource::OutputText
        }
        Some(output) if output.get("error").and_then(Value::as_str) == Some(result.text.as_str()) => {
            stored.text.clear();
            DurableAgentOutcomeTextSource::OutputError
        }
        _ => DurableAgentOutcomeTextSource::Inline,
    };
    serde_json::to_string(&DurableAgentOutcomeBody {
        result: stored,
        text_source,
    })
    .map_err(std::io::Error::other)
}

fn agent_outcome_projection(result: &SubAgentResult) -> SubAgentResult {
    SubAgentResult {
        name: result.name.clone(),
        agent_id: result.agent_id.clone(),
        task_id: result.task_id.clone(),
        status: result.status,
        failure_class: result.failure_class,
        output: None,
        text: String::new(),
        usage: Default::default(),
        turns: 0,
        is_error: result.is_error,
    }
}

fn decode_agent_outcome_body(serialized: &str) -> std::io::Result<SubAgentResult> {
    let mut body: DurableAgentOutcomeBody = serde_json::from_str(serialized).map_err(std::io::Error::other)?;
    body.result.text = match body.text_source {
        DurableAgentOutcomeTextSource::Inline => body.result.text,
        DurableAgentOutcomeTextSource::OutputText => body
            .result
            .output
            .as_ref()
            .and_then(|output| output.get("text"))
            .and_then(Value::as_str)
            .ok_or_else(|| std::io::Error::other("Agent outcome blob is missing output.text"))?
            .to_owned(),
        DurableAgentOutcomeTextSource::OutputError => body
            .result
            .output
            .as_ref()
            .and_then(|output| output.get("error"))
            .and_then(Value::as_str)
            .ok_or_else(|| std::io::Error::other("Agent outcome blob is missing output.error"))?
            .to_owned(),
    };
    Ok(body.result)
}
