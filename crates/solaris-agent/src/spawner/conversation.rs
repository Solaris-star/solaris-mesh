use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::Notify;

use solaris_types::identity::{AgentId, ChildAgentKey, OperationId, RunId, TaskId};
use solaris_types::message::{StopReason, TokenUsage};
use solaris_types::runtime::{AgentLifecycleState, OperationEnvironmentSnapshot, TaskFailureClass};
use solaris_types::spawner::{
    AgentConversationError, AgentConversationHandle, AgentConversationSpec, AgentOutcomeStatus, AgentSpawnService,
    AgentSpawnSpec, AgentTurnIdentity, AgentTurnOutcome, AgentTurnSpec, SubAgentConfig,
};

use crate::engine::AgentEngine;
use crate::error::AgentError;
use crate::execution_context::{EffectExecutionContext, build_environment_snapshot_with_plugins, stable_digest_value};
use crate::output::OutputSink;
use crate::output::runtime_sink::RuntimeOutputSink;
use crate::permission_engine::PermissionContext;
use crate::resource_manager::{AgentResourcePermit, ResourceManager};
use crate::session::SessionManager;
use crate::session::store::{
    ConversationCloseClaim, ConversationIdentity, ConversationOpenClaim, ConversationState, ConversationTurnClaim,
    ConversationTurnCompletion, ConversationTurnEnqueue, ConversationTurnIdentity, ConversationTurnState,
    DEFAULT_HEARTBEAT_SECONDS, DEFAULT_LEASE_SECONDS, SessionStore, SessionStoreError,
};

use super::{AgentSpawnReservation, AgentSpawner};

#[path = "conversation_output.rs"]
mod conversation_output;
#[path = "conversation_records.rs"]
mod conversation_records;
#[path = "conversation_recovery.rs"]
mod conversation_recovery;
#[path = "conversation_store_async.rs"]
mod conversation_store_async;
#[path = "conversation_support.rs"]
mod conversation_support;

use conversation_output::JsonTextProjection;
use conversation_store_async::{AsyncConversationStore, blocking_agent};
use conversation_support::*;

#[derive(Default)]
struct ConversationLocks {
    locks: Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurableTurnIntent {
    schema_version: u8,
    run_id: RunId,
    parent_agent_id: AgentId,
    conversation_id: String,
    task_id: TaskId,
    open_operation_id: OperationId,
    spec_digest: String,
    turn_id: String,
    agent_id: AgentId,
    session_id: String,
    identity: AgentTurnIdentity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurableClose {
    schema_version: u8,
    run_id: RunId,
    parent_agent_id: AgentId,
    conversation_id: String,
    task_id: TaskId,
    open_operation_id: OperationId,
    spec_digest: String,
    agent_id: AgentId,
    session_id: String,
    terminal_state: AgentLifecycleState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AppendDisposition {
    Appended,
    Existing,
}

/// Durable lifecycle service for a cold-resumable, multi-turn child Agent.
#[derive(Clone)]
pub struct AgentConversationService {
    spawner: Arc<AgentSpawner>,
    locks: Arc<ConversationLocks>,
    active: Arc<Mutex<HashMap<String, Arc<Notify>>>>,
    session_cleanups: Arc<SessionCleanupRegistry>,
    service_id: String,
    open_lease_duration_ms: i64,
    open_heartbeat_interval: Duration,
    #[cfg(test)]
    role_clock: Option<Arc<dyn Fn() -> i64 + Send + Sync>>,
    #[cfg(test)]
    turn_admission_hook: Arc<Mutex<Option<conversation_recovery::TurnAdmissionHook>>>,
    #[cfg(test)]
    turn_enqueued_hook: Arc<Mutex<Option<conversation_recovery::TurnEnqueuedHook>>>,
    #[cfg(test)]
    open_claim_hook: Arc<Mutex<Option<conversation_recovery::OpenClaimHook>>>,
}

impl AgentConversationService {
    /// Persist and publish an idle child Agent without calling its Provider.
    pub async fn open(&self, spec: AgentConversationSpec) -> Result<AgentConversationHandle, AgentConversationError> {
        self.validate_open_spec(&spec)?;
        let lock_key = conversation_lock_key(&spec.run_id, &spec.parent_agent_id, &spec.conversation_id);
        let _guard = self.locks.acquire(lock_key).await;
        self.restore_projection(&spec.run_id)?;
        let key = child_key(&spec);
        let agent_id = key.agent_id();
        let session_id = key.session_id();
        let runtime = self
            .prepare_open_runtime_async(&spec, agent_id.clone(), key.clone(), session_id.clone())
            .await?;
        let spec_digest = digest_serialized(&spec)?;
        if let Some(existing) = self.find_open(&spec.run_id, &spec.conversation_id)? {
            self.validate_handle_async(&existing, Some(&spec)).await?;
        }
        let durable_identity = conversation_identity(&spec, &agent_id, &session_id, &spec_digest);
        let store = AsyncConversationStore::open(self.spawner.base_config.session.directory.clone()).await?;
        let (revision, epoch) = match store
            .claim_open(&durable_identity, &self.service_id, self.open_lease_duration_ms)
            .await?
        {
            ConversationOpenClaim::Owned { revision, epoch } => (revision, epoch),
            ConversationOpenClaim::Existing(stored) if stored.state == ConversationState::Open => {
                let existing = decode_handle(stored.handle_json.as_deref())?;
                self.validate_handle_async(&existing, Some(&spec)).await?;
                self.ensure_open_projection(&existing)?;
                return Ok(existing);
            }
            ConversationOpenClaim::Existing(_) => {
                return Err(AgentConversationError::non_retryable(
                    "Agent conversation is closing or closed",
                ));
            }
            ConversationOpenClaim::Busy(_) => {
                if let Some(existing) = self.find_open(&spec.run_id, &spec.conversation_id)? {
                    let bytes = encode_handle(&existing)?;
                    store.recover_open(&durable_identity, bytes).await?;
                    self.validate_handle_async(&existing, Some(&spec)).await?;
                    self.ensure_open_projection(&existing)?;
                    return Ok(existing);
                }
                return Err(AgentConversationError::reconciliation_required(
                    "Agent conversation open is owned by another process",
                ));
            }
        };
        let opening_heartbeat = OpeningLeaseHeartbeat::start(
            store.clone(),
            durable_identity.clone(),
            self.service_id.clone(),
            epoch,
            revision,
            self.open_lease_duration_ms,
            self.open_heartbeat_interval,
        );
        #[cfg(test)]
        self.pause_after_open_claim().await;

        if let Some(existing) = self.find_open(&spec.run_id, &spec.conversation_id)? {
            if let Err(error) = opening_heartbeat.finish().await {
                tracing::debug!(
                    target: "solaris_agent",
                    run_id = %spec.run_id,
                    conversation_id = %spec.conversation_id,
                    error = %error,
                    "exact durable conversation open superseded the local opening lease"
                );
            }
            let bytes = encode_handle(&existing)?;
            store.recover_open(&durable_identity, bytes).await?;
            self.validate_handle_async(&existing, Some(&spec)).await?;
            self.ensure_open_projection(&existing)?;
            return Ok(existing);
        }

        let spawn_handle = match self.spawner.spawn(spawn_spec(&spec)).await {
            Ok(handle) => handle,
            Err(error) => {
                let lease_result = opening_heartbeat.finish().await;
                lease_result?;
                if matches!(
                    error.failure_class,
                    TaskFailureClass::Retryable
                        | TaskFailureClass::NonRetryable
                        | TaskFailureClass::PermissionDenied
                        | TaskFailureClass::MaxTurns
                        | TaskFailureClass::NonConvergent
                        | TaskFailureClass::Cancelled
                ) {
                    let abandoned = store
                        .abandon_open(&durable_identity, &self.service_id, epoch, revision)
                        .await?;
                    if !abandoned {
                        return Err(AgentConversationError::reconciliation_required(
                            "Agent conversation opening lease changed before abort",
                        ));
                    }
                }
                return Err(AgentConversationError {
                    failure_class: error.failure_class,
                    message: error.message,
                });
            }
        };
        if spawn_handle.agent_id != agent_id {
            return Err(AgentConversationError::reconciliation_required(
                "conversation spawn resolved a different child Agent identity",
            ));
        }
        let resolved_session_id = key
            .session_id_for_agent_id(&spawn_handle.agent_id)
            .ok_or_else(|| AgentConversationError::reconciliation_required("unknown child Agent identity version"))?;
        if resolved_session_id != session_id {
            return Err(AgentConversationError::reconciliation_required(
                "conversation spawn resolved a different child session identity",
            ));
        }
        let fresh_reservation = runtime.reservation.clone();
        self.spawner
            .attach_collaboration(&fresh_reservation, spec.overrides.collaboration.as_ref())
            .map_err(AgentConversationError::reconciliation_required)?;

        let handle = AgentConversationHandle {
            schema_version: CONVERSATION_SCHEMA_VERSION,
            conversation_id: spec.conversation_id.clone(),
            run_id: spec.run_id.clone(),
            parent_agent_id: spec.parent_agent_id.clone(),
            agent_id,
            identity_version: spawn_handle.identity_version,
            session_id,
            task_id: spec.task_id.clone(),
            operation_id: spec.operation_id.clone(),
            spec_digest,
            environment_digest: runtime.environment_digest,
            effective_permission_ceiling: runtime.permissions.ceiling(),
            spec,
        };
        self.record_open(&handle)?;
        opening_heartbeat.finish().await?;
        store
            .finalize_open(
                &durable_identity,
                &self.service_id,
                epoch,
                revision,
                encode_handle(&handle)?,
            )
            .await?;
        self.ensure_open_projection(&handle)?;
        tracing::info!(
            target: "solaris_agent",
            run_id = %handle.run_id,
            conversation_id = %handle.conversation_id,
            agent_id = %handle.agent_id,
            session_id = %handle.session_id,
            "durable Agent conversation opened"
        );
        Ok(handle)
    }

    /// Execute one serialized turn by cold-resuming the conversation session.
    pub async fn run_turn(
        &self,
        handle: &AgentConversationHandle,
        turn: AgentTurnSpec,
    ) -> Result<AgentTurnOutcome, AgentConversationError> {
        self.run_turn_inner(handle, turn, None).await
    }

    async fn run_turn_inner(
        &self,
        handle: &AgentConversationHandle,
        turn: AgentTurnSpec,
        projection: Option<&JsonTextProjection<'_>>,
    ) -> Result<AgentTurnOutcome, AgentConversationError> {
        if turn.turn_id.trim().is_empty() {
            return Err(AgentConversationError::non_retryable("Agent turn ID must not be empty"));
        }
        let lock_key = conversation_lock_key(&handle.run_id, &handle.parent_agent_id, &handle.conversation_id);
        let _guard = self.locks.acquire(lock_key).await;
        self.restore_projection(&handle.run_id)?;
        self.validate_handle_async(handle, None).await?;
        let identity = handle.turn_identity(&turn);
        let ledger_outcome = self.find_turn_outcome(handle, &turn.turn_id)?;
        if ledger_outcome.is_some() {
            self.require_turn_input(handle, &turn, &identity)?;
        } else {
            let _ = self.find_turn_intent(handle, &turn.turn_id)?;
        }
        let durable_identity = handle_identity(handle);
        let durable_turn = turn_store_identity(&turn, &identity);
        let handle_json = encode_handle(handle)?;
        let store = AsyncConversationStore::open(self.spawner.base_config.session.directory.clone()).await?;
        #[cfg(test)]
        self.pause_before_turn_admission().await;
        let enqueued = store
            .enqueue_turn(&durable_identity, handle_json.clone(), &durable_turn)
            .await?;
        #[cfg(test)]
        self.pause_after_turn_enqueued().await;
        let _ = store
            .load_turn(&durable_identity, &turn.turn_id)
            .await?
            .ok_or_else(|| AgentConversationError::reconciliation_required("durable queued turn is missing"))?;
        if let ConversationTurnEnqueue::Blocked { failure_class } = enqueued {
            return Err(blocked_conversation_error(&failure_class));
        }
        if let ConversationTurnEnqueue::Existing(stored) = &enqueued
            && stored.state.is_terminal()
        {
            return self
                .recover_terminal_turn_outcome(handle, &turn, &identity, stored.outcome_json.as_deref())
                .await;
        }

        let mut pending_delay = TURN_PENDING_INITIAL_DELAY;
        let mut pending_attempts = 0_u16;
        let claimed = loop {
            match store
                .claim_turn(&durable_identity, handle_json.clone(), &durable_turn, &self.service_id)
                .await?
            {
                ConversationTurnClaim::Claimed(stored) => break stored,
                ConversationTurnClaim::Pending(stored) => {
                    let _ = stored.revision;
                    pending_attempts = pending_attempts.saturating_add(1);
                    if pending_attempts >= TURN_PENDING_MAX_ATTEMPTS {
                        return Err(AgentConversationError::retryable(
                            "Agent conversation turn remained queued past the bounded wait limit",
                        ));
                    }
                    tokio::time::sleep(pending_delay).await;
                    pending_delay = pending_delay.saturating_mul(2).min(TURN_PENDING_MAX_DELAY);
                }
                ConversationTurnClaim::Blocked { failure_class } => {
                    return Err(blocked_conversation_error(&failure_class));
                }
                ConversationTurnClaim::IntentCommitted(stored) => {
                    if let Some(outcome) = self.find_turn_outcome(handle, &turn.turn_id)? {
                        let outcome_json = encode_outcome(&outcome)?;
                        store
                            .finalize_turn(
                                &durable_identity,
                                handle_json.clone(),
                                &durable_turn,
                                stored.sequence,
                                ConversationTurnCompletion {
                                    state: outcome_store_state(&outcome),
                                    outcome_json,
                                    failure_class: outcome.failure_class.map(failure_class_name).map(str::to_owned),
                                    block_following_turns: blocks_following_turns(&outcome),
                                },
                            )
                            .await?;
                        self.ensure_session_quiescent_async(handle).await?;
                        self.repair_idle_after_turn(handle)?;
                        return self.hydrate_turn_outcome(&outcome);
                    }
                    return Err(AgentConversationError::outcome_unknown(format!(
                        "Agent conversation turn '{}' has durable intent without a durable outcome",
                        turn.turn_id
                    )));
                }
                ConversationTurnClaim::Terminal(stored) => {
                    return self
                        .recover_terminal_turn_outcome(handle, &turn, &identity, stored.outcome_json.as_deref())
                        .await;
                }
                ConversationTurnClaim::ConversationNotOpen(_) => {
                    return Err(AgentConversationError::non_retryable(
                        "Agent conversation is closing or closed",
                    ));
                }
            }
        };

        let intent = DurableTurnIntent {
            schema_version: CONVERSATION_SCHEMA_VERSION,
            run_id: handle.run_id.clone(),
            parent_agent_id: handle.parent_agent_id.clone(),
            conversation_id: handle.conversation_id.clone(),
            task_id: handle.task_id.clone(),
            open_operation_id: handle.operation_id.clone(),
            spec_digest: handle.spec_digest.clone(),
            turn_id: turn.turn_id.clone(),
            agent_id: handle.agent_id.clone(),
            session_id: handle.session_id.clone(),
            identity: identity.clone(),
        };
        let prepared = self.prepare_turn_async(handle).await?;
        match self.record_turn_intent(handle, &intent) {
            Ok(AppendDisposition::Appended) => {
                if let Err(error) = store
                    .commit_turn_intent(
                        &durable_identity,
                        handle_json.clone(),
                        &durable_turn,
                        claimed.sequence,
                        &self.service_id,
                        claimed.revision,
                    )
                    .await
                {
                    release_prepared_turn(prepared).await?;
                    return Err(AgentConversationError::reconciliation_required(format!(
                        "Agent conversation turn claim changed before Provider execution: {error}"
                    )));
                }
            }
            Ok(AppendDisposition::Existing) => {
                release_prepared_turn(prepared).await?;
                return Err(AgentConversationError::outcome_unknown(format!(
                    "Agent conversation turn '{}' was concurrently admitted",
                    turn.turn_id
                )));
            }
            Err(error) => {
                let release = release_prepared_turn(prepared).await;
                return match release {
                    Ok(()) => Err(error),
                    Err(release) => Err(AgentConversationError::reconciliation_required(format!(
                        "{error}; failed to release rejected turn session lease: {release}"
                    ))),
                };
            }
        }
        if let Err(error) = self.set_agent_state(handle, AgentLifecycleState::Active) {
            let release = release_prepared_turn(prepared).await;
            return match release {
                Ok(()) => Err(error),
                Err(release) => Err(AgentConversationError::reconciliation_required(format!(
                    "{error}; failed to release turn session lease: {release}"
                ))),
            };
        }

        let notify = Arc::new(Notify::new());
        let active_key = conversation_lock_key(&handle.run_id, &handle.parent_agent_id, &handle.conversation_id);
        let active_guard = ActiveTurnGuard::register(Arc::clone(&self.active), active_key, Arc::clone(&notify));
        let outcome = self.execute_turn(handle, &turn, &identity, notify, prepared).await;
        drop(active_guard);

        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => failure_outcome(handle, &turn, &identity, &error),
        };
        let durable_outcome = self.project_turn_outcome(&outcome, projection)?;
        if let Err(error) = self.record_turn_outcome(handle, &durable_outcome) {
            let cleanup = self.set_agent_state(handle, AgentLifecycleState::Idle);
            return match cleanup {
                Ok(()) => Err(error),
                Err(cleanup) => Err(AgentConversationError::reconciliation_required(format!(
                    "{error}; failed to restore idle Agent state: {cleanup}"
                ))),
            };
        }
        store
            .finalize_turn(
                &durable_identity,
                handle_json,
                &durable_turn,
                claimed.sequence,
                ConversationTurnCompletion {
                    state: outcome_store_state(&durable_outcome),
                    outcome_json: encode_outcome(&durable_outcome)?,
                    failure_class: durable_outcome.failure_class.map(failure_class_name).map(str::to_owned),
                    block_following_turns: blocks_following_turns(&durable_outcome),
                },
            )
            .await?;
        self.set_agent_state(handle, AgentLifecycleState::Idle)?;
        tracing::debug!(
            target: "solaris_agent",
            run_id = %handle.run_id,
            conversation_id = %handle.conversation_id,
            agent_id = %handle.agent_id,
            turn_id = %turn.turn_id,
            status = ?outcome.status,
            "durable Agent conversation turn finished"
        );
        self.hydrate_turn_outcome(&durable_outcome)
    }

    /// Durably close the conversation and retain its session and Ledger history.
    pub async fn close(&self, handle: &AgentConversationHandle) -> Result<(), AgentConversationError> {
        self.validate_handle_async(handle, None).await?;
        let durable_identity = handle_identity(handle);
        let handle_json = encode_handle(handle)?;
        let store = AsyncConversationStore::open(self.spawner.base_config.session.directory.clone()).await?;
        let close_claim = store.begin_close(&durable_identity, handle_json.clone()).await?;
        match close_claim {
            ConversationCloseClaim::AlreadyClosed(stored) => {
                let terminal = parse_terminal_state(stored.terminal_state.as_deref())?;
                self.repair_terminal_state(handle, terminal)?;
                return Ok(());
            }
            ConversationCloseClaim::Started(stored) | ConversationCloseClaim::Existing(stored) => {
                let _ = stored.revision;
            }
        }
        let active_key = conversation_lock_key(&handle.run_id, &handle.parent_agent_id, &handle.conversation_id);
        if let Some(notify) = self
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&active_key)
            .cloned()
        {
            // One turn can be active for this full conversation key. `notify_one`
            // retains a permit if close wins the race before `select!` polls the
            // cancellation branch, so a validated close cannot lose cancellation.
            notify.notify_one();
        }
        let lock_key = conversation_lock_key(&handle.run_id, &handle.parent_agent_id, &handle.conversation_id);
        let _guard = self.locks.acquire(lock_key).await;
        self.restore_projection(&handle.run_id)?;
        self.validate_handle_async(handle, None).await?;
        if let Some(closed) = self.find_close(handle)? {
            store
                .finish_close(
                    &durable_identity,
                    handle_json.clone(),
                    lifecycle_state_name(closed.terminal_state),
                    None,
                )
                .await?;
            self.repair_terminal_state(handle, closed.terminal_state)?;
            return Ok(());
        }
        self.wait_for_session_cleanup(handle).await;
        self.ensure_session_quiescent_async(handle).await?;
        self.settle_turns_for_close(handle, &store, &durable_identity, &handle_json)
            .await?;
        if self
            .spawner
            .lifecycle_runtime
            .agents()
            .get(&handle.agent_id)
            .is_some_and(|agent| agent.state == AgentLifecycleState::Active)
        {
            if self.has_unresolved_turn(handle)? {
                return Err(AgentConversationError::outcome_unknown(
                    "Agent conversation has a live turn that this service could not cancel",
                ));
            }
            self.repair_idle_after_turn(handle)?;
        }
        let terminal_state = if self.has_unresolved_turn(handle)?
            || store
                .requires_failed_close(&durable_identity, handle_json.clone())
                .await?
        {
            AgentLifecycleState::Failed
        } else {
            AgentLifecycleState::Completed
        };
        let close = DurableClose {
            schema_version: CONVERSATION_SCHEMA_VERSION,
            run_id: handle.run_id.clone(),
            parent_agent_id: handle.parent_agent_id.clone(),
            conversation_id: handle.conversation_id.clone(),
            task_id: handle.task_id.clone(),
            open_operation_id: handle.operation_id.clone(),
            spec_digest: handle.spec_digest.clone(),
            agent_id: handle.agent_id.clone(),
            session_id: handle.session_id.clone(),
            terminal_state,
        };
        self.record_close(handle, &close)?;
        store
            .finish_close(
                &durable_identity,
                handle_json,
                lifecycle_state_name(terminal_state),
                (terminal_state == AgentLifecycleState::Failed).then_some("reconciliation_required"),
            )
            .await?;
        self.repair_terminal_state(handle, terminal_state)?;
        tracing::info!(
            target: "solaris_agent",
            run_id = %handle.run_id,
            conversation_id = %handle.conversation_id,
            agent_id = %handle.agent_id,
            terminal_state = ?terminal_state,
            "durable Agent conversation closed"
        );
        Ok(())
    }

    fn validate_open_spec(&self, spec: &AgentConversationSpec) -> Result<(), AgentConversationError> {
        if spec.run_id != *self.spawner.run_id() || spec.parent_agent_id != *self.spawner.parent_agent_id() {
            return Err(AgentConversationError::non_retryable(
                "AgentConversationSpec does not belong to this Run and parent",
            ));
        }
        if !self.spawner.base_config.session.enabled {
            return Err(AgentConversationError::non_retryable(
                "durable Agent conversations require session persistence",
            ));
        }
        if spec.conversation_id.trim().is_empty()
            || spec.role_key.trim().is_empty()
            || spec.stable_task_key.trim().is_empty()
            || spec.config.name.trim().is_empty()
        {
            return Err(AgentConversationError::non_retryable(
                "conversation, role, stable task, and Agent names must not be empty",
            ));
        }
        match spec.context_policy.as_deref() {
            None | Some("isolated") | Some("isolated_verification") => Ok(()),
            Some(other) => Err(AgentConversationError::non_retryable(format!(
                "unsupported role context policy: {other}"
            ))),
        }
    }

    fn build_runtime(
        &self,
        spec: &AgentConversationSpec,
        agent_id: AgentId,
        key: ChildAgentKey,
        restore_resources: bool,
    ) -> Result<ConversationRuntime, AgentConversationError> {
        let effective_ceiling = self
            .spawner
            .permission_context
            .ceiling()
            .intersect(spec.permission_ceiling);
        let permissions = self.spawner.permission_context.narrowed(effective_ceiling);
        #[cfg(test)]
        let role_resources = self.role_clock.as_ref().map_or_else(
            || ResourceManager::new(spec.resource_budget.clone()),
            |clock| ResourceManager::new_with_test_clock(spec.resource_budget.clone(), Arc::clone(clock)),
        );
        #[cfg(not(test))]
        let role_resources = ResourceManager::new(spec.resource_budget.clone());
        role_resources.set_provider_signals(self.spawner.resources.provider_signals());
        if restore_resources {
            role_resources
                .attach_ledger(
                    RunId::new(format!("{}:agent-resource:{}", spec.run_id, agent_id)),
                    self.spawner.lifecycle_runtime.ledger(),
                )
                .map_err(AgentConversationError::reconciliation_required)?;
        }
        let execution = EffectExecutionContext::new(
            spec.run_id.clone(),
            agent_id.clone(),
            self.spawner.lifecycle_runtime.ledger(),
            permissions.clone(),
            OperationEnvironmentSnapshot::default(),
        )
        .with_mutation_coordinator(self.spawner.lifecycle_runtime.mutation_coordinator())
        .with_resource_manager(Arc::clone(&self.spawner.resources))
        .with_scoped_resource_manager(Arc::clone(&role_resources));
        let tools = self.spawner.child_registry(
            &spec.overrides.allowed_tools,
            spec.overrides.inherit_capabilities,
            agent_id.clone(),
            permissions.clone(),
            spec.recursion_limit,
            execution.clone(),
        );
        let config = self.conversation_config(spec)?;
        let mut environment =
            build_environment_snapshot_with_plugins(&config, &tools, &permissions, self.spawner.plugin_identities());
        environment.workflow = spec.overrides.workflow.clone();
        let environment_digest = digest_serialized(&environment)?;
        execution.set_environment(environment);
        Ok(ConversationRuntime {
            reservation: AgentSpawnReservation {
                key,
                child_agent_id: agent_id,
                permission_ceiling: effective_ceiling,
                reattached: true,
            },
            config,
            permissions,
            execution,
            tools,
            environment_digest,
            wall_time_ms: role_resources.remaining_wall_time_ms(),
        })
    }

    fn conversation_config(
        &self,
        spec: &AgentConversationSpec,
    ) -> Result<solaris_config::config::Config, AgentConversationError> {
        let mut config = self.spawner.base_config.clone();
        config.max_turns = Some(spec.config.max_turns);
        config.max_tokens = Some(config.max_tokens.map_or(spec.config.max_tokens, |configured| {
            configured.min(spec.config.max_tokens)
        }));
        if let Some(system_prompt) = spec.config.system_prompt.clone() {
            config.system_prompt = Some(system_prompt);
        }
        if let Some(policy) = spec.context_policy.as_deref() {
            let policy_prompt = match policy {
                "isolated" => {
                    "Context policy: isolated. Use only the typed input in this task; parent transcript is unavailable."
                }
                "isolated_verification" => {
                    "Context policy: isolated verification. Verify only the supplied typed evidence; parent transcript and unstated claims are unavailable."
                }
                other => {
                    return Err(AgentConversationError::non_retryable(format!(
                        "unsupported role context policy: {other}"
                    )));
                }
            };
            config.system_prompt = Some(match config.system_prompt.take() {
                Some(existing) if !existing.is_empty() => format!("{existing}\n\n{policy_prompt}"),
                _ => policy_prompt.to_owned(),
            });
        }
        if let Some(model) = spec.overrides.model.clone() {
            config.model = model;
        }
        Ok(config)
    }

    fn ensure_session_persisted(
        &self,
        spec: &AgentConversationSpec,
        runtime: &ConversationRuntime,
        session_id: &str,
    ) -> Result<(), AgentConversationError> {
        let manager = SessionManager::new(
            runtime.config.session.directory.clone().into(),
            runtime.config.session.max_sessions,
        );
        if let Some(session) = manager
            .load_if_exists(session_id)
            .map_err(|error| AgentConversationError::reconciliation_required(error.to_string()))?
        {
            return validate_session(spec, &runtime.config, &self.spawner.cwd, &session);
        }
        manager
            .create_active_session(
                &runtime.config.provider_label,
                &runtime.config.model,
                self.spawner.cwd.to_string_lossy().as_ref(),
                Some(session_id),
                spec.run_id.as_str(),
            )
            .map_err(|error| AgentConversationError::reconciliation_required(error.to_string()))?;
        manager
            .release_active_session()
            .map_err(|error| AgentConversationError::reconciliation_required(error.to_string()))
    }

    fn validate_handle(
        &self,
        handle: &AgentConversationHandle,
        requested: Option<&AgentConversationSpec>,
    ) -> Result<(), AgentConversationError> {
        if handle.schema_version != CONVERSATION_SCHEMA_VERSION {
            return Err(AgentConversationError::reconciliation_required(
                "unsupported Agent conversation record version; no legacy wire format exists",
            ));
        }
        if handle.run_id != *self.spawner.run_id() || handle.parent_agent_id != *self.spawner.parent_agent_id() {
            return Err(AgentConversationError::non_retryable(
                "Agent conversation handle belongs to another Run or parent",
            ));
        }
        if handle.run_id != handle.spec.run_id
            || handle.parent_agent_id != handle.spec.parent_agent_id
            || handle.task_id != handle.spec.task_id
            || handle.conversation_id != handle.spec.conversation_id
            || handle.operation_id != handle.spec.operation_id
        {
            return Err(AgentConversationError::reconciliation_required(
                "Agent conversation handle identity differs from its canonical specification",
            ));
        }
        if requested.is_some_and(|spec| digest_serialized(spec).ok().as_deref() != Some(&handle.spec_digest)) {
            return Err(AgentConversationError::non_retryable(
                "conversation ID is already bound to a different specification",
            ));
        }
        if digest_serialized(&handle.spec)? != handle.spec_digest {
            return Err(AgentConversationError::reconciliation_required(
                "Agent conversation specification digest changed",
            ));
        }
        let key = child_key(&handle.spec);
        if key.identity_version_for_agent_id(&handle.agent_id) != Some(handle.identity_version)
            || key.session_id_for_agent_id(&handle.agent_id).as_deref() != Some(&handle.session_id)
        {
            return Err(AgentConversationError::reconciliation_required(
                "Agent conversation identity does not match its stable child key",
            ));
        }
        let runtime = self.build_runtime(&handle.spec, handle.agent_id.clone(), key, false)?;
        if runtime.permissions.ceiling() != handle.effective_permission_ceiling {
            return Err(AgentConversationError::reconciliation_required(
                "Agent conversation permission ceiling changed",
            ));
        }
        if runtime.environment_digest != handle.environment_digest {
            return Err(AgentConversationError::reconciliation_required(
                "Agent conversation execution environment changed",
            ));
        }
        let manager = SessionManager::new(
            runtime.config.session.directory.clone().into(),
            runtime.config.session.max_sessions,
        );
        let session = manager
            .load_if_exists(&handle.session_id)
            .map_err(|error| AgentConversationError::reconciliation_required(error.to_string()))?
            .ok_or_else(|| AgentConversationError::reconciliation_required("durable child session is missing"))?;
        validate_session(&handle.spec, &runtime.config, &self.spawner.cwd, &session)?;
        let durable = self
            .find_open(&handle.run_id, &handle.conversation_id)?
            .ok_or_else(|| {
                AgentConversationError::reconciliation_required("durable conversation open record is missing")
            })?;
        if serde_json::to_value(&durable).ok() != serde_json::to_value(handle).ok() {
            return Err(AgentConversationError::reconciliation_required(
                "Agent conversation handle differs from its exact durable open record",
            ));
        }
        Ok(())
    }

    fn ensure_open_projection(&self, handle: &AgentConversationHandle) -> Result<(), AgentConversationError> {
        let key = child_key(&handle.spec);
        let existing = self.spawner.lifecycle_runtime.agents().get(&handle.agent_id);
        if existing
            .as_ref()
            .is_some_and(|agent| matches!(agent.state, AgentLifecycleState::Idle | AgentLifecycleState::Active))
            && self
                .spawner
                .lifecycle_runtime
                .existing_child_identity(&key)
                .is_some_and(|(agent_id, version)| agent_id == handle.agent_id && version == handle.identity_version)
        {
            return Ok(());
        }
        if existing.as_ref().is_some_and(|agent| {
            matches!(
                agent.state,
                AgentLifecycleState::Completed | AgentLifecycleState::Failed | AgentLifecycleState::Cancelled
            )
        }) {
            return Err(AgentConversationError::reconciliation_required(
                "Agent conversation is already terminal",
            ));
        }
        let reservation = self
            .spawner
            .lifecycle_runtime
            .reserve_spawn_typed(
                handle.run_id.clone(),
                handle.parent_agent_id.clone(),
                handle.spec.role_key.clone(),
                conversation_stable_task_key(&handle.spec),
                conversation_spawn_operation_id(&handle.spec),
                &self.spawner.permission_context,
                handle.spec.permission_ceiling,
            )
            .map_err(|error| AgentConversationError::reconciliation_required(error.to_string()))?;
        if reservation.child_agent_id != handle.agent_id || reservation.key != key {
            return Err(AgentConversationError::reconciliation_required(
                "Agent conversation projection resolved a different identity",
            ));
        }
        self.spawner
            .attach_collaboration(&reservation, handle.spec.overrides.collaboration.as_ref())
            .map_err(AgentConversationError::reconciliation_required)?;
        let state = self
            .spawner
            .lifecycle_runtime
            .agents()
            .get(&handle.agent_id)
            .map(|agent| agent.state)
            .ok_or_else(|| AgentConversationError::reconciliation_required("Agent projection is missing"))?;
        if state == AgentLifecycleState::Reserved {
            self.spawner
                .lifecycle_runtime
                .commit_spawn(&reservation)
                .map_err(|error| AgentConversationError::reconciliation_required(error.to_string()))?;
        }
        self.set_agent_state(handle, AgentLifecycleState::Idle)
    }

    fn prepare_turn(&self, handle: &AgentConversationHandle) -> Result<PreparedTurn, AgentConversationError> {
        let key = child_key(&handle.spec);
        let runtime = self.build_runtime(&handle.spec, handle.agent_id.clone(), key, true)?;
        let output: Arc<dyn OutputSink> = Arc::new(RuntimeOutputSink::new(
            Arc::clone(&self.spawner.lifecycle_runtime),
            handle.run_id.clone(),
            handle.agent_id.clone(),
        ));
        let mut engine = self
            .spawner
            .build_child_engine(
                &runtime.reservation,
                runtime.config,
                runtime.tools,
                output,
                runtime.permissions,
                runtime.execution,
            )
            .map_err(AgentConversationError::reconciliation_required)?;
        engine.set_initial_reasoning_effort(handle.spec.overrides.effort.clone());
        Ok(PreparedTurn {
            engine: Some(engine),
            wall_time_ms: runtime.wall_time_ms,
            cleanup_registry: Arc::clone(&self.session_cleanups),
            cleanup_key: handle.session_id.clone(),
        })
    }

    async fn execute_turn(
        &self,
        handle: &AgentConversationHandle,
        turn: &AgentTurnSpec,
        identity: &AgentTurnIdentity,
        notify: Arc<Notify>,
        mut prepared: PreparedTurn,
    ) -> Result<AgentTurnOutcome, AgentConversationError> {
        let permit = match self
            .spawner
            .resources
            .acquire_reattached_agent(self.spawner.spawn_depth.saturating_add(1))
            .await
        {
            Ok(permit) => permit,
            Err(error) => {
                release_prepared_turn(prepared).await?;
                return Err(AgentConversationError::retryable(error));
            }
        };
        let wall_time_ms = prepared.wall_time_ms;
        let engine = prepared.take_engine()?;
        let cleanup = prepared.begin_cleanup();
        let prompt = turn.prompt.clone();
        let message_id = identity.message_id.clone();
        let worker_notify = Arc::clone(&notify);
        let mut cancel_on_drop = CancelOnDrop::new(notify);
        let result =
            run_agent_turn_dedicated(engine, permit, cleanup, wall_time_ms, prompt, message_id, worker_notify).await;
        cancel_on_drop.disarm();
        let result = result?;
        let failed = result.status != AgentOutcomeStatus::Completed;
        Ok(AgentTurnOutcome {
            schema_version: CONVERSATION_SCHEMA_VERSION,
            run_id: handle.run_id.clone(),
            parent_agent_id: handle.parent_agent_id.clone(),
            conversation_id: handle.conversation_id.clone(),
            task_id: handle.task_id.clone(),
            open_operation_id: handle.operation_id.clone(),
            spec_digest: handle.spec_digest.clone(),
            turn_id: turn.turn_id.clone(),
            agent_id: handle.agent_id.clone(),
            session_id: handle.session_id.clone(),
            operation_id: identity.operation_id.clone(),
            message_id: identity.message_id.clone(),
            status: result.status,
            output: json!({"text": result.text}),
            outcome_ref: None,
            output_projection: None,
            usage: result.usage,
            turns: result.turns,
            failure_class: if failed {
                Some(result.failure_class.unwrap_or(TaskFailureClass::NonRetryable))
            } else {
                None
            },
            error: failed.then(|| engine_failure_message(result.stop_reason).to_owned()),
        })
    }
}

#[cfg(test)]
#[path = "conversation_test.rs"]
mod conversation_test;
