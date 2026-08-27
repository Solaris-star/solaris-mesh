//! Collaboration runtime entry point for Solaris Mesh orchestration.

mod messages;
mod restore;
mod spawn;
mod tasks;

pub(crate) use tasks::TaskSettlement;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde::Serialize;

use serde_json::json;
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::{AgentId, ChildAgentKey, RunId, TeamId};
use solaris_types::permission::PermissionCeiling;
use solaris_types::runtime::{AgentRecord, TaskRecord, TaskState};
use solaris_types::spawner::{AgentCollaborationContext, AgentHandle};
use solaris_types::workflow::{CollaborationRuntimeConfig, CollaborationStrategy};
use tokio::sync::broadcast;

use crate::agent_registry::AgentRegistry;
use crate::message_bus::{AgentMessage, MessageBus};
use crate::relationship_store::{AgentRelationship, InMemoryAgentRelationshipStore};
use crate::runtime_ledger::{InMemoryRuntimeLedger, LedgerRecord, RunMutationCoordinator, RuntimeLedger};
use crate::scheduler::{ScheduledTask, Scheduler};
use crate::task_registry::TaskRegistry;
use crate::team_registry::{TeamRecord, TeamRegistry};
use crate::team_state::{ArtifactRef, TeamFact, TeamStateStore};

#[derive(Debug, Clone, Serialize)]
pub struct RuntimeLiveEvent {
    pub schema_version: u32,
    pub sequence: u64,
    pub timestamp_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub journal_sequence: Option<u64>,
    pub run_id: RunId,
    pub agent_id: Option<AgentId>,
    pub kind: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct RuntimeSubscriptionSnapshot {
    pub schema_version: u32,
    pub live_sequence: u64,
    pub journal_sequence: u64,
    pub timestamp_unix_ms: i64,
    pub projection: RuntimeProjection,
}

pub struct RuntimeSubscription {
    pub snapshot: RuntimeSubscriptionSnapshot,
    pub receiver: broadcast::Receiver<RuntimeLiveEvent>,
}

pub struct RuntimeSubscriptionWith<T> {
    pub snapshot: RuntimeSubscriptionSnapshot,
    pub extra: T,
    pub receiver: broadcast::Receiver<RuntimeLiveEvent>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RuntimeProjection {
    pub agents: Vec<AgentRecord>,
    pub tasks: Vec<TaskRecord>,
    pub teams: Vec<TeamRecord>,
    pub messages: Vec<AgentMessage>,
    pub relationships: Vec<AgentRelationship>,
    pub facts: Vec<TeamFact>,
    pub artifacts: Vec<ArtifactRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentSpawnReservation {
    pub key: ChildAgentKey,
    pub child_agent_id: AgentId,
    pub permission_ceiling: PermissionCeiling,
    pub reattached: bool,
}

pub struct CollaborationRuntime<T> {
    scheduler: Scheduler<T>,
    agents: Arc<AgentRegistry>,
    tasks: Arc<TaskRegistry>,
    teams: Arc<TeamRegistry>,
    messages: Arc<MessageBus>,
    team_state: Arc<TeamStateStore>,
    relationships: Arc<InMemoryAgentRelationshipStore>,
    ledger: Arc<dyn RuntimeLedger>,
    mutation: Arc<RunMutationCoordinator>,
    live_events: broadcast::Sender<RuntimeLiveEvent>,
    live_sequence: AtomicU64,
    live_snapshot_gate: Mutex<()>,
}

impl<T> CollaborationRuntime<T> {
    pub fn new(scheduler: Scheduler<T>) -> Self {
        Self::with_ledger(scheduler, Arc::new(InMemoryRuntimeLedger::default()))
    }

    pub fn with_ledger(scheduler: Scheduler<T>, ledger: Arc<dyn RuntimeLedger>) -> Self {
        let (live_events, _) = broadcast::channel(4096);
        let agents = Arc::new(AgentRegistry::default());
        let teams = Arc::new(TeamRegistry::default());
        let mutation = Arc::new(RunMutationCoordinator::default());
        Self {
            scheduler,
            agents: Arc::clone(&agents),
            tasks: Arc::new(TaskRegistry::default()),
            teams: Arc::clone(&teams),
            messages: Arc::new(MessageBus::new_with_mutation(
                Arc::clone(&ledger),
                agents,
                teams,
                Arc::clone(&mutation),
            )),
            team_state: Arc::new(TeamStateStore::default()),
            relationships: Arc::new(InMemoryAgentRelationshipStore::default()),
            ledger,
            mutation,
            live_events,
            live_sequence: AtomicU64::new(0),
            live_snapshot_gate: Mutex::new(()),
        }
    }

    pub fn enqueue(&mut self, task: ScheduledTask<T>) {
        self.scheduler.enqueue(task);
    }

    pub fn acquire_next(&mut self) -> Option<ScheduledTask<T>> {
        self.scheduler.acquire_next()
    }

    pub fn release(&mut self) {
        self.scheduler.release();
    }

    pub fn queued_len(&self) -> usize {
        self.scheduler.queued_len()
    }

    pub fn active(&self) -> usize {
        self.scheduler.active()
    }

    pub fn agents(&self) -> Arc<AgentRegistry> {
        Arc::clone(&self.agents)
    }

    pub fn tasks(&self) -> Arc<TaskRegistry> {
        Arc::clone(&self.tasks)
    }

    pub fn teams(&self) -> Arc<TeamRegistry> {
        Arc::clone(&self.teams)
    }

    pub fn messages(&self) -> Arc<MessageBus> {
        Arc::clone(&self.messages)
    }

    pub fn team_state(&self) -> Arc<TeamStateStore> {
        Arc::clone(&self.team_state)
    }

    pub fn ledger(&self) -> Arc<dyn RuntimeLedger> {
        Arc::clone(&self.ledger)
    }

    pub fn mutation_coordinator(&self) -> Arc<RunMutationCoordinator> {
        Arc::clone(&self.mutation)
    }

    pub fn with_run_mutation<R>(&self, run_id: &RunId, action: impl FnOnce() -> R) -> R {
        let line = self.mutation.line_for(run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        action()
    }

    pub fn subscribe_live_events(&self) -> broadcast::Receiver<RuntimeLiveEvent> {
        self.live_events.subscribe()
    }

    pub fn subscribe_with_snapshot(&self, run_id: &RunId) -> std::io::Result<RuntimeSubscription> {
        let subscription = self.subscribe_with_snapshot_and(run_id, || ())?;
        Ok(RuntimeSubscription {
            snapshot: subscription.snapshot,
            receiver: subscription.receiver,
        })
    }

    pub fn subscribe_with_snapshot_and<E>(
        &self,
        run_id: &RunId,
        capture: impl FnOnce() -> E,
    ) -> std::io::Result<RuntimeSubscriptionWith<E>> {
        let line = self.mutation.line_for(run_id);
        let _mutation_guard = line.lock().unwrap_or_else(|error| error.into_inner());
        let mut receiver = self.live_events.subscribe();
        let _live_guard = self
            .live_snapshot_gate
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let live_sequence = self.live_sequence.load(Ordering::SeqCst);
        let snapshot = RuntimeSubscriptionSnapshot {
            schema_version: 1,
            live_sequence,
            journal_sequence: self.ledger.last_sequence_tree(run_id)?,
            timestamp_unix_ms: chrono::Utc::now().timestamp_millis(),
            projection: self.projection(),
        };
        let extra = capture();
        while receiver.try_recv().is_ok() {}
        Ok(RuntimeSubscriptionWith {
            snapshot,
            extra,
            receiver,
        })
    }

    pub fn capture_snapshot_with<E>(
        &self,
        run_id: &RunId,
        capture: impl FnOnce() -> E,
    ) -> std::io::Result<(RuntimeSubscriptionSnapshot, E)> {
        let line = self.mutation.line_for(run_id);
        let _mutation_guard = line.lock().unwrap_or_else(|error| error.into_inner());
        let _live_guard = self
            .live_snapshot_gate
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let snapshot = RuntimeSubscriptionSnapshot {
            schema_version: 1,
            live_sequence: self.live_sequence.load(Ordering::SeqCst),
            journal_sequence: self.ledger.last_sequence_tree(run_id)?,
            timestamp_unix_ms: chrono::Utc::now().timestamp_millis(),
            projection: self.projection(),
        };
        Ok((snapshot, capture()))
    }

    pub fn with_projection_mutation<R>(&self, action: impl FnOnce() -> R) -> R {
        let _live_guard = self
            .live_snapshot_gate
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        action()
    }

    pub fn live_last_sequence(&self) -> u64 {
        self.live_sequence.load(Ordering::SeqCst)
    }

    pub fn emit_live_event(
        &self,
        run_id: RunId,
        agent_id: Option<AgentId>,
        kind: impl Into<String>,
        payload: serde_json::Value,
    ) -> RuntimeLiveEvent {
        self.publish_live_event(run_id, agent_id, kind, payload, None)
    }

    pub fn emit_durable_event(
        &self,
        record: &LedgerRecord,
        agent_id: Option<AgentId>,
        kind: impl Into<String>,
        payload: serde_json::Value,
    ) -> RuntimeLiveEvent {
        self.publish_live_event(record.run_id.clone(), agent_id, kind, payload, Some(record.seq))
    }

    fn publish_live_event(
        &self,
        run_id: RunId,
        agent_id: Option<AgentId>,
        kind: impl Into<String>,
        payload: serde_json::Value,
        journal_sequence: Option<u64>,
    ) -> RuntimeLiveEvent {
        let _live_guard = self
            .live_snapshot_gate
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let event = RuntimeLiveEvent {
            schema_version: 1,
            sequence: self.live_sequence.fetch_add(1, Ordering::SeqCst) + 1,
            timestamp_unix_ms: chrono::Utc::now().timestamp_millis(),
            journal_sequence,
            run_id,
            agent_id,
            kind: kind.into(),
            payload,
        };
        let _ = self.live_events.send(event.clone());
        event
    }

    pub fn commit_runtime_event(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        agent_id: Option<AgentId>,
        kind: impl Into<String>,
        payload: serde_json::Value,
    ) -> std::io::Result<LedgerRecord> {
        let line = self.mutation.line_for(run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        let record = self.ledger.append(run_id, durability, record_type, payload.clone())?;
        self.emit_durable_event(&record, agent_id, kind, payload);
        Ok(record)
    }

    pub fn commit_record(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        payload: serde_json::Value,
    ) -> std::io::Result<LedgerRecord> {
        let line = self.mutation.line_for(run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        self.ledger.append(run_id, durability, record_type, payload)
    }

    pub fn projection(&self) -> RuntimeProjection {
        RuntimeProjection {
            agents: self.agents.snapshot(),
            tasks: self.tasks.snapshot(),
            teams: self.teams.snapshot(),
            messages: self.messages.snapshot(),
            relationships: self.relationships.snapshot(),
            facts: self.team_state.all_facts(),
            artifacts: self.team_state.all_artifacts(),
        }
    }

    pub fn create_team(&self, run_id: RunId, team_id: TeamId, name: impl Into<String>) -> std::io::Result<bool> {
        self.create_collaboration_team(run_id, team_id, name, CollaborationStrategy::Team, None)
    }

    pub fn create_collaboration_team(
        &self,
        run_id: RunId,
        team_id: TeamId,
        name: impl Into<String>,
        strategy: CollaborationStrategy,
        coordinator: Option<AgentId>,
    ) -> std::io::Result<bool> {
        let record = TeamRecord {
            run_id: run_id.clone(),
            team_id: team_id.clone(),
            name: name.into(),
            strategy,
            coordinator,
            direct_peer_messaging: matches!(strategy, CollaborationStrategy::Team),
            max_pending_messages: CollaborationRuntimeConfig::DEFAULT_MAX_PENDING_MESSAGES,
            max_message_bytes: CollaborationRuntimeConfig::DEFAULT_MAX_MESSAGE_BYTES,
            members: std::collections::HashSet::new(),
        };
        let line = self.mutation.line_for(&run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        if self.teams.get(&team_id).is_some() {
            return Ok(false);
        }
        let durable = self.ledger.append(
            &run_id,
            DurabilityClass::SyncCritical,
            "team_created",
            serde_json::to_value(&record).map_err(std::io::Error::other)?,
        )?;
        let created = self.teams.create(record.clone());
        debug_assert!(created);
        self.emit_durable_event(
            &durable,
            None,
            "team_created",
            serde_json::to_value(&record).unwrap_or_default(),
        );
        Ok(true)
    }

    pub fn ensure_collaboration_team(
        &self,
        run_id: RunId,
        name: impl Into<String>,
        collaboration: &AgentCollaborationContext,
    ) -> std::io::Result<()> {
        let team_id = collaboration.team_id.clone();
        let strategy = collaboration.strategy;
        let coordinator = collaboration.coordinator_agent_id.clone();
        let max_pending_messages = collaboration.max_pending_messages;
        let max_message_bytes = collaboration.max_message_bytes;
        let record = TeamRecord {
            run_id: run_id.clone(),
            team_id: team_id.clone(),
            name: name.into(),
            strategy,
            coordinator: Some(coordinator),
            direct_peer_messaging: matches!(strategy, CollaborationStrategy::Team),
            max_pending_messages,
            max_message_bytes,
            members: std::collections::HashSet::new(),
        };
        record.validate_message_limits()?;
        if let Some(existing) = self.teams.get(&team_id) {
            if existing.run_id != run_id
                || existing.strategy != strategy
                || existing.coordinator.as_ref() != record.coordinator.as_ref()
                || existing.max_pending_messages != max_pending_messages
                || existing.max_message_bytes != max_message_bytes
            {
                return Err(std::io::Error::other(format!(
                    "team {team_id} already exists with incompatible collaboration metadata"
                )));
            }
            return Ok(());
        }
        let line = self.mutation.line_for(&run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(existing) = self.teams.get(&team_id) {
            if existing.run_id != run_id
                || existing.strategy != strategy
                || existing.coordinator.as_ref() != record.coordinator.as_ref()
                || existing.max_pending_messages != max_pending_messages
                || existing.max_message_bytes != max_message_bytes
            {
                return Err(std::io::Error::other(format!(
                    "team {team_id} already exists with incompatible collaboration metadata"
                )));
            }
            return Ok(());
        }
        let durable = self.ledger.append(
            &run_id,
            DurabilityClass::SyncCritical,
            "team_created",
            serde_json::to_value(&record).map_err(std::io::Error::other)?,
        )?;
        let created = self.teams.create(record.clone());
        debug_assert!(created);
        self.emit_durable_event(
            &durable,
            None,
            "team_created",
            serde_json::to_value(&record).unwrap_or_default(),
        );
        Ok(())
    }

    /// Atomically prepare every Team membership and collaboration Task needed
    /// by one spawn batch. Validation and the single durable record happen
    /// before any projection is changed, so partial preparation cannot leak a
    /// Team, coordinator membership, or Assigned Task.
    pub fn prepare_spawn_batch(
        &self,
        run_id: &RunId,
        handles: &[(AgentHandle, bool)],
        max_tasks_per_run: usize,
    ) -> std::io::Result<()> {
        let line = self.mutation.line_for(run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        let mut prospective_teams = std::collections::HashMap::<TeamId, TeamRecord>::new();
        let mut new_teams = Vec::new();
        let mut memberships = Vec::<(TeamId, AgentId)>::new();
        let mut tasks = Vec::new();

        for (handle, create_task) in handles {
            if &handle.run_id != run_id || handle.spec.run_id != *run_id {
                return Err(std::io::Error::other("spawn batch handle belongs to a different run"));
            }
            let child = self
                .agents
                .get(&handle.agent_id)
                .ok_or_else(|| std::io::Error::other(format!("unknown spawn batch Agent: {}", handle.agent_id)))?;
            if child.run_id != *run_id {
                return Err(std::io::Error::other("spawn batch Agent belongs to a different run"));
            }
            if let Some(collaboration) = handle.spec.overrides.collaboration.as_ref() {
                let message_limits = TeamRecord {
                    run_id: run_id.clone(),
                    team_id: collaboration.team_id.clone(),
                    name: collaboration.strategy.to_string(),
                    strategy: collaboration.strategy,
                    coordinator: Some(collaboration.coordinator_agent_id.clone()),
                    direct_peer_messaging: matches!(collaboration.strategy, CollaborationStrategy::Team),
                    max_pending_messages: collaboration.max_pending_messages,
                    max_message_bytes: collaboration.max_message_bytes,
                    members: std::collections::HashSet::new(),
                };
                message_limits.validate_message_limits()?;
                let coordinator = self.agents.get(&collaboration.coordinator_agent_id).ok_or_else(|| {
                    std::io::Error::other(format!(
                        "unknown collaboration coordinator: {}",
                        collaboration.coordinator_agent_id
                    ))
                })?;
                if coordinator.run_id != *run_id {
                    return Err(std::io::Error::other(
                        "collaboration coordinator belongs to a different run",
                    ));
                }
                let expected = message_limits;
                let existing = prospective_teams
                    .get(&collaboration.team_id)
                    .cloned()
                    .or_else(|| self.teams.get(&collaboration.team_id));
                if let Some(existing) = existing {
                    if existing.run_id != expected.run_id
                        || existing.strategy != expected.strategy
                        || existing.coordinator != expected.coordinator
                        || existing.max_pending_messages != expected.max_pending_messages
                        || existing.max_message_bytes != expected.max_message_bytes
                    {
                        return Err(std::io::Error::other(format!(
                            "team {} already exists with incompatible collaboration metadata",
                            collaboration.team_id
                        )));
                    }
                    prospective_teams.insert(collaboration.team_id.clone(), existing);
                } else {
                    prospective_teams.insert(collaboration.team_id.clone(), expected.clone());
                    new_teams.push(expected);
                }
                for agent_id in [&collaboration.coordinator_agent_id, &handle.agent_id] {
                    let already_member = prospective_teams
                        .get(&collaboration.team_id)
                        .is_some_and(|team| team.members.contains(agent_id));
                    let pending = memberships
                        .iter()
                        .any(|(team_id, member)| team_id == &collaboration.team_id && member == agent_id);
                    if !already_member && !pending {
                        memberships.push((collaboration.team_id.clone(), agent_id.clone()));
                    }
                }
            }
            if *create_task {
                let task = TaskRecord {
                    run_id: handle.run_id.clone(),
                    task_id: handle.task_id.clone(),
                    revision: 0,
                    task_key: Some(handle.stable_task_key.clone()),
                    team_id: handle
                        .spec
                        .overrides
                        .collaboration
                        .as_ref()
                        .map(|value| value.team_id.clone()),
                    workflow_id: handle
                        .spec
                        .overrides
                        .workflow
                        .as_ref()
                        .map(|workflow| workflow.implementation_id.clone()),
                    node_id: None,
                    role: Some(handle.role_key.clone()),
                    depends_on: Vec::new(),
                    content: None,
                    expected_write_scope: Vec::new(),
                    owner_agent_id: Some(handle.agent_id.clone()),
                    state: TaskState::Assigned,
                    outcome_ref: None,
                    failure_class: None,
                };
                if let Some(existing) = self.tasks.get(&task.task_id) {
                    if existing != task {
                        return Err(std::io::Error::other(format!(
                            "runtime task {} already exists with different metadata",
                            task.task_id
                        )));
                    }
                } else if tasks
                    .iter()
                    .any(|candidate: &TaskRecord| candidate.task_id == task.task_id)
                {
                    return Err(std::io::Error::other(format!(
                        "spawn batch contains duplicate task {}",
                        task.task_id
                    )));
                } else {
                    tasks.push(task);
                }
            }
        }

        if !tasks.is_empty() {
            self.admit_collaboration_tasks(run_id, max_tasks_per_run, tasks.clone())
                .map_err(std::io::Error::other)?;
        }
        if new_teams.is_empty() && memberships.is_empty() {
            return Ok(());
        }
        let payload = json!({"teams": &new_teams, "memberships": &memberships});
        let record = self.ledger.append(
            run_id,
            DurabilityClass::SyncCritical,
            "collaboration_batch_prepared",
            payload,
        )?;
        for team in &new_teams {
            self.teams.create(team.clone());
            self.emit_durable_event(
                &record,
                None,
                "team_created",
                serde_json::to_value(team).unwrap_or_default(),
            );
        }
        for (team_id, agent_id) in &memberships {
            self.teams.join(team_id, agent_id.clone());
            self.agents.set_team(agent_id, Some(team_id.clone()));
            self.emit_durable_event(
                &record,
                Some(agent_id.clone()),
                "team_member_joined",
                json!({"team_id": team_id, "agent_id": agent_id}),
            );
        }
        Ok(())
    }

    pub fn join_team(&self, run_id: &RunId, team_id: &TeamId, agent_id: AgentId) -> std::io::Result<bool> {
        let line = self.mutation.line_for(run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        let team = self
            .teams
            .get(team_id)
            .ok_or_else(|| std::io::Error::other(format!("unknown team: {team_id}")))?;
        let agent = self
            .agents
            .get(&agent_id)
            .ok_or_else(|| std::io::Error::other(format!("unknown agent: {agent_id}")))?;
        if team.run_id != *run_id || agent.run_id != *run_id {
            return Err(std::io::Error::other("team member belongs to a different run"));
        }
        if team.members.contains(&agent_id) {
            return Ok(false);
        }
        let durable = self.ledger.append(
            run_id,
            DurabilityClass::SyncCritical,
            "team_member_joined",
            json!({"team_id": team_id, "agent_id": agent_id.clone()}),
        )?;
        let joined = self.teams.join(team_id, agent_id.clone());
        let assigned = self.agents.set_team(&agent_id, Some(team_id.clone()));
        debug_assert!(joined && assigned);
        self.emit_durable_event(
            &durable,
            Some(agent_id.clone()),
            "team_member_joined",
            json!({"team_id": team_id, "agent_id": agent_id}),
        );
        Ok(true)
    }

    pub fn set_team_fact(
        &self,
        run_id: &RunId,
        team_id: &TeamId,
        updated_by: &AgentId,
        key: impl Into<String>,
        value: serde_json::Value,
    ) -> std::io::Result<TeamFact> {
        let team = self
            .teams
            .get(team_id)
            .ok_or_else(|| std::io::Error::other(format!("unknown team: {team_id}")))?;
        if &team.run_id != run_id || !team.members.contains(updated_by) {
            return Err(std::io::Error::other("fact writer must belong to the target team/run"));
        }
        let key = key.into();
        if key.trim().is_empty() {
            return Err(std::io::Error::other("team fact key cannot be empty"));
        }
        let fact = TeamFact {
            run_id: run_id.clone(),
            team_id: team_id.clone(),
            key,
            value,
            updated_by: updated_by.clone(),
            updated_at_unix_ms: chrono::Utc::now().timestamp_millis(),
        };
        let line = self.mutation.line_for(run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        let record = self.ledger.append(
            run_id,
            DurabilityClass::SyncCritical,
            "team_fact_set",
            serde_json::to_value(&fact).map_err(std::io::Error::other)?,
        )?;
        self.team_state.set_fact(fact.clone());
        self.emit_durable_event(
            &record,
            Some(updated_by.clone()),
            "team_fact_set",
            serde_json::to_value(&fact).unwrap_or_default(),
        );
        Ok(fact)
    }

    pub fn team_facts(&self, team_id: &TeamId) -> Vec<TeamFact> {
        self.team_state.facts(team_id)
    }

    pub fn register_artifact(
        &self,
        run_id: &RunId,
        team_id: Option<TeamId>,
        agent_id: &AgentId,
        uri: impl Into<String>,
        kind: impl Into<String>,
        metadata: serde_json::Value,
    ) -> std::io::Result<ArtifactRef> {
        let agent = self
            .agents
            .get(agent_id)
            .ok_or_else(|| std::io::Error::other(format!("unknown agent: {agent_id}")))?;
        if &agent.run_id != run_id {
            return Err(std::io::Error::other("artifact agent belongs to a different run"));
        }
        if let Some(team_id) = team_id.as_ref() {
            let team = self
                .teams
                .get(team_id)
                .ok_or_else(|| std::io::Error::other(format!("unknown team: {team_id}")))?;
            if &team.run_id != run_id || !team.members.contains(agent_id) {
                return Err(std::io::Error::other(
                    "artifact writer must belong to the target team/run",
                ));
            }
        }
        let artifact = ArtifactRef {
            artifact_id: format!("artifact-{}", uuid::Uuid::now_v7()),
            run_id: run_id.clone(),
            team_id,
            agent_id: agent_id.clone(),
            uri: uri.into(),
            kind: kind.into(),
            metadata,
            created_at_unix_ms: chrono::Utc::now().timestamp_millis(),
        };
        if artifact.uri.trim().is_empty() || artifact.kind.trim().is_empty() {
            return Err(std::io::Error::other("artifact uri/kind cannot be empty"));
        }
        let line = self.mutation.line_for(run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        let record = self.ledger.append(
            run_id,
            DurabilityClass::SyncCritical,
            "artifact_registered",
            serde_json::to_value(&artifact).map_err(std::io::Error::other)?,
        )?;
        self.team_state.put_artifact(artifact.clone());
        self.emit_durable_event(
            &record,
            Some(agent_id.clone()),
            "artifact_registered",
            serde_json::to_value(&artifact).unwrap_or_default(),
        );
        Ok(artifact)
    }

    pub fn artifacts(&self, team_id: Option<&TeamId>, agent_id: Option<&AgentId>) -> Vec<ArtifactRef> {
        self.team_state.artifacts(team_id, agent_id)
    }

    pub fn send_message(
        &self,
        run_id: RunId,
        team_id: Option<TeamId>,
        from: AgentId,
        to: AgentId,
        kind: impl Into<String>,
        body: serde_json::Value,
    ) -> std::io::Result<AgentMessage> {
        let from_agent = self
            .agents
            .get(&from)
            .ok_or_else(|| std::io::Error::other(format!("unknown message agent: {from}")))?;
        let to_agent = self
            .agents
            .get(&to)
            .ok_or_else(|| std::io::Error::other(format!("unknown message agent: {to}")))?;
        for agent in [&from_agent, &to_agent] {
            if agent.run_id != run_id {
                return Err(std::io::Error::other("message agents must belong to the requested run"));
            }
        }
        let effective_team_id = match (team_id, from_agent.team_id.as_ref(), to_agent.team_id.as_ref()) {
            (Some(requested), _, _) => Some(requested),
            (None, None, None) => None,
            (None, Some(from_team), Some(to_team)) if from_team == to_team => Some(from_team.clone()),
            (None, _, _) => {
                return Err(std::io::Error::other(
                    "team-scoped messages must name the shared collaboration team",
                ));
            }
        };
        if let Some(team_id) = effective_team_id.as_ref() {
            let team = self
                .teams
                .get(team_id)
                .ok_or_else(|| std::io::Error::other(format!("unknown team: {team_id}")))?;
            if team.run_id != run_id || !team.members.contains(&from) || !team.members.contains(&to) {
                return Err(std::io::Error::other(
                    "sender, receiver, and team must belong to the requested run",
                ));
            }
            if !team.direct_peer_messaging {
                let coordinator = team
                    .coordinator
                    .as_ref()
                    .ok_or_else(|| std::io::Error::other("collaboration team has no coordinator"))?;
                if &from != coordinator && &to != coordinator {
                    return Err(std::io::Error::other(
                        "direct peer messaging is disabled for this collaboration strategy",
                    ));
                }
            }
        }
        let line = self.mutation.line_for(&run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        let (message, record) =
            self.messages
                .send_within_mutation(run_id, effective_team_id, from, to.clone(), kind, body)?;
        self.emit_durable_event(
            &record,
            Some(to),
            "agent_message",
            serde_json::to_value(&message).unwrap_or_default(),
        );
        Ok(message)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn send_message_with_id(
        &self,
        message_id: String,
        run_id: RunId,
        team_id: Option<TeamId>,
        from: AgentId,
        to: AgentId,
        kind: impl Into<String>,
        body: serde_json::Value,
    ) -> std::io::Result<AgentMessage> {
        let kind = kind.into();
        let requested_team_id = team_id;
        let line = self.mutation.line_for(&run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(message) = self.messages.replay_send_with_id_within_mutation(
            &message_id,
            &run_id,
            requested_team_id.as_ref(),
            &from,
            &to,
            &kind,
            &body,
        )? {
            return Ok(message);
        }
        let from_agent = self
            .agents
            .get(&from)
            .ok_or_else(|| std::io::Error::other(format!("unknown message agent: {from}")))?;
        let to_agent = self
            .agents
            .get(&to)
            .ok_or_else(|| std::io::Error::other(format!("unknown message agent: {to}")))?;
        if from_agent.run_id != run_id || to_agent.run_id != run_id {
            return Err(std::io::Error::other("message agents must belong to the requested run"));
        }
        let effective_team_id = match (
            requested_team_id.clone(),
            from_agent.team_id.as_ref(),
            to_agent.team_id.as_ref(),
        ) {
            (Some(requested), _, _) => Some(requested),
            (None, None, None) => None,
            (None, Some(from_team), Some(to_team)) if from_team == to_team => Some(from_team.clone()),
            (None, _, _) => {
                return Err(std::io::Error::other(
                    "team-scoped messages must name the shared collaboration team",
                ));
            }
        };
        if let Some(team_id) = effective_team_id.as_ref() {
            let team = self
                .teams
                .get(team_id)
                .ok_or_else(|| std::io::Error::other(format!("unknown team: {team_id}")))?;
            if team.run_id != run_id || !team.members.contains(&from) || !team.members.contains(&to) {
                return Err(std::io::Error::other(
                    "sender, receiver, and team must belong to the requested run",
                ));
            }
            if !team.direct_peer_messaging {
                let coordinator = team
                    .coordinator
                    .as_ref()
                    .ok_or_else(|| std::io::Error::other("collaboration team has no coordinator"))?;
                if &from != coordinator && &to != coordinator {
                    return Err(std::io::Error::other(
                        "direct peer messaging is disabled for this collaboration strategy",
                    ));
                }
            }
        }
        let (message, record) = self.messages.send_with_requested_team_id_within_mutation(
            message_id,
            run_id,
            requested_team_id,
            effective_team_id,
            from,
            to.clone(),
            kind,
            body,
        )?;
        if let Some(record) = record {
            self.emit_durable_event(
                &record,
                Some(to),
                "agent_message",
                serde_json::to_value(&message).unwrap_or_default(),
            );
        }
        Ok(message)
    }

    pub fn broadcast_message(
        &self,
        run_id: RunId,
        team_id: TeamId,
        from: AgentId,
        kind: impl Into<String>,
        body: serde_json::Value,
    ) -> std::io::Result<Vec<AgentMessage>> {
        let line = self.mutation.line_for(&run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        let team = self
            .teams
            .get(&team_id)
            .ok_or_else(|| std::io::Error::other(format!("unknown team: {team_id}")))?;
        if team.run_id != run_id || !team.members.contains(&from) {
            return Err(std::io::Error::other(
                "broadcast sender must belong to the requested team/run",
            ));
        }
        if team.strategy == CollaborationStrategy::Supervisor && team.coordinator.as_ref() != Some(&from) {
            return Err(std::io::Error::other(
                "only the Supervisor coordinator may broadcast to the Team",
            ));
        }
        let kind = kind.into();
        let mut recipients: Vec<_> = team.members.into_iter().filter(|agent_id| agent_id != &from).collect();
        recipients.sort();
        let (messages, record) =
            self.messages
                .broadcast_within_mutation(run_id, team_id, from.clone(), recipients, kind, body)?;
        self.emit_durable_event(
            &record,
            Some(from),
            "agent_message_broadcast",
            serde_json::to_value(&messages).unwrap_or_default(),
        );
        Ok(messages)
    }
}

#[cfg(test)]
#[path = "collaboration_runtime_test.rs"]
mod collaboration_runtime_test;

#[cfg(test)]
#[path = "collaboration_task_cas_test.rs"]
mod collaboration_task_cas_test;

#[cfg(test)]
#[path = "collaboration_runtime_restore_test.rs"]
mod collaboration_runtime_restore_test;

#[cfg(test)]
#[path = "collaboration_task_admission_test.rs"]
mod collaboration_task_admission_test;
