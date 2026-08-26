use super::*;

pub(super) const CONVERSATION_SCHEMA_VERSION: u8 = 1;
pub(super) const OPEN_RECORD: &str = "agent_conversation_opened";
pub(super) const TURN_INTENT_RECORD: &str = "agent_conversation_turn_intent";
pub(super) const TURN_OUTCOME_RECORD: &str = "agent_conversation_turn_outcome";
pub(super) const CLOSE_RECORD: &str = "agent_conversation_closed";
pub(super) const TURN_PENDING_INITIAL_DELAY: Duration = Duration::from_millis(5);
pub(super) const TURN_PENDING_MAX_DELAY: Duration = Duration::from_millis(250);
pub(super) const TURN_PENDING_MAX_ATTEMPTS: u16 = 400;

pub(super) struct ConversationRuntime {
    pub(super) reservation: AgentSpawnReservation,
    pub(super) config: solaris_config::config::Config,
    pub(super) permissions: PermissionContext,
    pub(super) execution: EffectExecutionContext,
    pub(super) tools: solaris_tools::registry::ToolRegistry,
    pub(super) environment_digest: String,
    pub(super) wall_time_ms: Option<u64>,
}

pub(super) struct PreparedTurn {
    pub(super) engine: Option<AgentEngine>,
    pub(super) wall_time_ms: Option<u64>,
    pub(super) cleanup_registry: Arc<SessionCleanupRegistry>,
    pub(super) cleanup_key: String,
}

impl Drop for PreparedTurn {
    fn drop(&mut self) {
        if let Some(engine) = self.engine.take() {
            spawn_detached_session_release(engine, Arc::clone(&self.cleanup_registry), self.cleanup_key.clone());
        }
    }
}

impl PreparedTurn {
    pub(super) fn take_engine(&mut self) -> Result<AgentEngine, AgentConversationError> {
        self.engine
            .take()
            .ok_or_else(|| AgentConversationError::reconciliation_required("Agent conversation turn engine is missing"))
    }

    pub(super) fn begin_cleanup(&self) -> SessionCleanupCompletion {
        self.cleanup_registry.register(self.cleanup_key.clone())
    }
}

#[derive(Default)]
pub(super) struct SessionCleanupRegistry {
    pending: Mutex<HashMap<String, Arc<Notify>>>,
}

impl SessionCleanupRegistry {
    fn register(self: &Arc<Self>, key: String) -> SessionCleanupCompletion {
        let notify = Arc::new(Notify::new());
        self.pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(key.clone(), Arc::clone(&notify));
        SessionCleanupCompletion {
            registry: Arc::clone(self),
            key,
            notify,
        }
    }

    pub(super) async fn wait(&self, key: &str) {
        loop {
            let pending = self
                .pending
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get(key)
                .cloned();
            let Some(notify) = pending else {
                return;
            };
            notify.notified().await;
        }
    }
}

pub(super) struct SessionCleanupCompletion {
    registry: Arc<SessionCleanupRegistry>,
    key: String,
    notify: Arc<Notify>,
}

impl Drop for SessionCleanupCompletion {
    fn drop(&mut self) {
        let mut pending = self.registry.pending.lock().unwrap_or_else(|error| error.into_inner());
        if pending
            .get(&self.key)
            .is_some_and(|notify| Arc::ptr_eq(notify, &self.notify))
        {
            pending.remove(&self.key);
        }
        drop(pending);
        self.notify.notify_one();
    }
}

pub(super) struct CancelOnDrop {
    notify: Arc<Notify>,
    armed: bool,
}

struct DedicatedTurnWork {
    engine: AgentEngine,
    permit: AgentResourcePermit,
    cleanup: SessionCleanupCompletion,
    wall_time_ms: Option<u64>,
    prompt: String,
    message_id: String,
    notify: Arc<Notify>,
}

pub(super) async fn run_agent_turn_dedicated(
    engine: AgentEngine,
    permit: AgentResourcePermit,
    cleanup: SessionCleanupCompletion,
    wall_time_ms: Option<u64>,
    prompt: String,
    message_id: String,
    notify: Arc<Notify>,
) -> Result<crate::engine::AgentResult, AgentConversationError> {
    let work = Arc::new(Mutex::new(Some(DedicatedTurnWork {
        engine,
        permit,
        cleanup,
        wall_time_ms,
        prompt,
        message_id,
        notify,
    })));
    let worker_work = Arc::clone(&work);
    let (result_tx, result_rx) = tokio::sync::oneshot::channel();
    let spawn = std::thread::Builder::new()
        .name("solaris-agent-conversation-turn".to_owned())
        .spawn(move || {
            let work = worker_work
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take()
                .ok_or_else(|| {
                    AgentConversationError::reconciliation_required("Agent conversation turn worker lost its work")
                });
            let result = work.and_then(execute_dedicated_turn);
            let _ = result_tx.send(result);
        });
    if let Err(spawn_error) = spawn {
        let work = work
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
            .ok_or_else(|| {
                AgentConversationError::reconciliation_required("Agent conversation turn worker ownership is unknown")
            })?;
        let release = blocking_agent(move || {
            let _cleanup = work.cleanup;
            let _permit = work.permit;
            work.engine
                .release_session_for_cold_resume()
                .map_err(|error| AgentConversationError::reconciliation_required(error.to_string()))
        })
        .await;
        return match release {
            Ok(()) => Err(AgentConversationError::reconciliation_required(format!(
                "failed to start Agent conversation turn worker: {spawn_error}"
            ))),
            Err(release_error) => Err(AgentConversationError::reconciliation_required(format!(
                "failed to start Agent conversation turn worker: {spawn_error}; failed to release session: {release_error}"
            ))),
        };
    }
    result_rx.await.map_err(|error| {
        AgentConversationError::reconciliation_required(format!(
            "Agent conversation turn worker stopped without a result: {error}"
        ))
    })?
}

fn execute_dedicated_turn(mut work: DedicatedTurnWork) -> Result<crate::engine::AgentResult, AgentConversationError> {
    let _cleanup = work.cleanup;
    let _permit = work.permit;
    let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => {
            let release = work.engine.release_session_for_cold_resume();
            return match release {
                Ok(()) => Err(AgentConversationError::reconciliation_required(format!(
                    "failed to create Agent conversation turn runtime: {error}"
                ))),
                Err(release) => Err(AgentConversationError::reconciliation_required(format!(
                    "failed to create Agent conversation turn runtime: {error}; failed to release session: {release}"
                ))),
            };
        }
    };
    let result = runtime.block_on(async {
        let run = work.engine.run(&work.prompt, &work.message_id);
        if let Some(limit_ms) = work.wall_time_ms {
            tokio::select! {
                result = tokio::time::timeout(Duration::from_millis(limit_ms), run) => {
                    match result {
                        Ok(result) => result.map_err(agent_error),
                        Err(_) => Err(AgentConversationError {
                            failure_class: TaskFailureClass::NonRetryable,
                            message: format!("Agent role wall-time budget exhausted after {limit_ms} ms"),
                        }),
                    }
                }
                _ = work.notify.notified() => Err(AgentConversationError {
                    failure_class: TaskFailureClass::Cancelled,
                    message: "Agent conversation turn was cancelled".to_owned(),
                }),
            }
        } else {
            tokio::select! {
                result = run => result.map_err(agent_error),
                _ = work.notify.notified() => Err(AgentConversationError {
                    failure_class: TaskFailureClass::Cancelled,
                    message: "Agent conversation turn was cancelled".to_owned(),
                }),
            }
        }
    });
    work.engine
        .release_session_for_cold_resume()
        .map_err(|error| AgentConversationError::reconciliation_required(error.to_string()))?;
    result
}

impl CancelOnDrop {
    pub(super) fn new(notify: Arc<Notify>) -> Self {
        Self { notify, armed: true }
    }

    pub(super) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.notify.notify_one();
        }
    }
}

impl ConversationLocks {
    pub(super) async fn acquire(self: &Arc<Self>, key: String) -> ConversationLockGuard {
        let lock = {
            let mut locks = self.locks.lock().unwrap_or_else(|error| error.into_inner());
            locks.retain(|_, lock| lock.upgrade().is_some());
            match locks.get(&key).and_then(Weak::upgrade) {
                Some(lock) => lock,
                None => {
                    let lock = Arc::new(tokio::sync::Mutex::new(()));
                    locks.insert(key.clone(), Arc::downgrade(&lock));
                    lock
                }
            }
        };
        ConversationLockGuard {
            owner: Arc::downgrade(self),
            key,
            guard: Some(lock.lock_owned().await),
        }
    }
}

pub(super) struct ConversationLockGuard {
    owner: Weak<ConversationLocks>,
    key: String,
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl Drop for ConversationLockGuard {
    fn drop(&mut self) {
        self.guard.take();
        let Some(owner) = self.owner.upgrade() else {
            return;
        };
        let mut locks = owner.locks.lock().unwrap_or_else(|error| error.into_inner());
        if locks.get(&self.key).is_some_and(|lock| lock.upgrade().is_none()) {
            locks.remove(&self.key);
        }
        locks.retain(|_, lock| lock.upgrade().is_some());
    }
}

pub(super) struct ActiveTurnGuard {
    active: Arc<Mutex<HashMap<String, Arc<Notify>>>>,
    key: String,
    notify: Arc<Notify>,
}

impl ActiveTurnGuard {
    pub(super) fn register(active: Arc<Mutex<HashMap<String, Arc<Notify>>>>, key: String, notify: Arc<Notify>) -> Self {
        active
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(key.clone(), Arc::clone(&notify));
        Self { active, key, notify }
    }
}

impl Drop for ActiveTurnGuard {
    fn drop(&mut self) {
        let mut active = self.active.lock().unwrap_or_else(|error| error.into_inner());
        if active
            .get(&self.key)
            .is_some_and(|notify| Arc::ptr_eq(notify, &self.notify))
        {
            active.remove(&self.key);
        }
    }
}

impl AgentConversationService {
    pub fn new(spawner: Arc<AgentSpawner>) -> Self {
        Self {
            spawner,
            locks: Arc::new(ConversationLocks::default()),
            active: Arc::new(Mutex::new(HashMap::new())),
            session_cleanups: Arc::new(SessionCleanupRegistry::default()),
            service_id: uuid::Uuid::now_v7().to_string(),
            open_lease_duration_ms: DEFAULT_LEASE_SECONDS * 1_000,
            open_heartbeat_interval: Duration::from_secs(DEFAULT_HEARTBEAT_SECONDS as u64),
            #[cfg(test)]
            role_clock: None,
            #[cfg(test)]
            turn_admission_hook: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            turn_enqueued_hook: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            open_claim_hook: Arc::new(Mutex::new(None)),
        }
    }

    #[cfg(test)]
    pub(super) fn with_open_lease_timing(mut self, lease_duration: Duration, heartbeat_interval: Duration) -> Self {
        self.open_lease_duration_ms = i64::try_from(lease_duration.as_millis()).unwrap();
        self.open_heartbeat_interval = heartbeat_interval;
        self
    }

    #[cfg(test)]
    pub(super) fn with_role_clock(mut self, clock: Arc<dyn Fn() -> i64 + Send + Sync>) -> Self {
        self.role_clock = Some(clock);
        self
    }
}

pub(super) struct OpeningLeaseHeartbeat {
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<Result<(), AgentConversationError>>>,
}

impl OpeningLeaseHeartbeat {
    pub(super) fn start(
        store: AsyncConversationStore,
        identity: ConversationIdentity,
        owner_id: String,
        epoch: i64,
        revision: i64,
        lease_duration_ms: i64,
        heartbeat_interval: Duration,
    ) -> Self {
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(heartbeat_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            interval.tick().await;
            loop {
                tokio::select! {
                    _ = &mut stopped => return Ok(()),
                    _ = interval.tick() => {
                        let renewed = store
                            .heartbeat_open(
                                &identity,
                                &owner_id,
                                epoch,
                                revision,
                                lease_duration_ms,
                            )
                            .await?;
                        if !renewed {
                            return Err(AgentConversationError::reconciliation_required(
                                "Agent conversation opening lease was lost",
                            ));
                        }
                    }
                }
            }
        });
        Self {
            stop: Some(stop),
            task: Some(task),
        }
    }

    pub(super) async fn finish(mut self) -> Result<(), AgentConversationError> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let Some(task) = self.task.take() else {
            return Err(AgentConversationError::reconciliation_required(
                "Agent conversation heartbeat task is missing",
            ));
        };
        task.await.map_err(|error| {
            AgentConversationError::reconciliation_required(format!(
                "Agent conversation heartbeat task failed: {error}"
            ))
        })?
    }
}

impl Drop for OpeningLeaseHeartbeat {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

pub(super) fn spawn_spec(spec: &AgentConversationSpec) -> AgentSpawnSpec {
    AgentSpawnSpec {
        run_id: spec.run_id.clone(),
        parent_agent_id: spec.parent_agent_id.clone(),
        task_id: spec.task_id.clone(),
        role_key: spec.role_key.clone(),
        stable_task_key: conversation_stable_task_key(spec),
        operation_id: conversation_spawn_operation_id(spec),
        expected_task_revision: None,
        config: SubAgentConfig {
            name: spec.config.name.clone(),
            prompt: String::new(),
            max_turns: spec.config.max_turns,
            max_tokens: spec.config.max_tokens,
            system_prompt: spec.config.system_prompt.clone(),
        },
        overrides: spec.overrides.clone(),
        permission_ceiling: spec.permission_ceiling,
        resource_budget: spec.resource_budget.clone(),
        context_policy: spec.context_policy.clone(),
        recursion_limit: spec.recursion_limit,
    }
}

pub(super) fn child_key(spec: &AgentConversationSpec) -> ChildAgentKey {
    ChildAgentKey {
        run_id: spec.run_id.clone(),
        parent_agent_id: spec.parent_agent_id.clone(),
        role_key: spec.role_key.clone(),
        stable_task_key: conversation_stable_task_key(spec),
        spawn_operation_id: conversation_spawn_operation_id(spec),
    }
}

pub(super) fn conversation_stable_task_key(spec: &AgentConversationSpec) -> String {
    format!(
        "conversation:{}",
        stable_digest_value(&json!({
            "stable_task_key": spec.stable_task_key,
            "conversation_id": spec.conversation_id,
        }))
    )
}

pub(super) fn conversation_spawn_operation_id(spec: &AgentConversationSpec) -> OperationId {
    OperationId::new(format!(
        "conversation-open:{}",
        stable_digest_value(&json!({
            "open_operation_id": spec.operation_id,
            "conversation_id": spec.conversation_id,
        }))
    ))
}

pub(super) fn conversation_lock_key(run_id: &RunId, parent_agent_id: &AgentId, conversation_id: &str) -> String {
    stable_digest_value(&json!({
        "run_id": run_id,
        "parent_agent_id": parent_agent_id,
        "conversation_id": conversation_id,
    }))
}

pub(super) fn digest_serialized<T: Serialize>(value: &T) -> Result<String, AgentConversationError> {
    serde_json::to_value(value)
        .map(|value| stable_digest_value(&value))
        .map_err(|error| AgentConversationError::non_retryable(error.to_string()))
}

pub(super) fn conversation_identity(
    spec: &AgentConversationSpec,
    agent_id: &AgentId,
    session_id: &str,
    spec_digest: &str,
) -> ConversationIdentity {
    ConversationIdentity {
        schema_version: CONVERSATION_SCHEMA_VERSION,
        run_id: spec.run_id.to_string(),
        parent_agent_id: spec.parent_agent_id.to_string(),
        conversation_id: spec.conversation_id.clone(),
        agent_id: agent_id.to_string(),
        session_id: session_id.to_owned(),
        task_id: spec.task_id.to_string(),
        open_operation_id: spec.operation_id.to_string(),
        spec_digest: spec_digest.to_owned(),
    }
}

pub(super) fn handle_identity(handle: &AgentConversationHandle) -> ConversationIdentity {
    conversation_identity(&handle.spec, &handle.agent_id, &handle.session_id, &handle.spec_digest)
}

pub(super) fn turn_store_identity(turn: &AgentTurnSpec, identity: &AgentTurnIdentity) -> ConversationTurnIdentity {
    ConversationTurnIdentity {
        turn_id: turn.turn_id.clone(),
        operation_id: identity.operation_id.to_string(),
        message_id: identity.message_id.clone(),
        input_digest: identity.input_digest.clone(),
    }
}

pub(super) fn encode_handle(handle: &AgentConversationHandle) -> Result<Vec<u8>, AgentConversationError> {
    serde_json::to_vec(handle).map_err(|error| AgentConversationError::non_retryable(error.to_string()))
}

pub(super) fn decode_handle(bytes: Option<&[u8]>) -> Result<AgentConversationHandle, AgentConversationError> {
    let bytes =
        bytes.ok_or_else(|| AgentConversationError::reconciliation_required("durable open handle is missing"))?;
    serde_json::from_slice(bytes).map_err(|error| AgentConversationError::reconciliation_required(error.to_string()))
}

pub(super) fn encode_outcome(outcome: &AgentTurnOutcome) -> Result<Vec<u8>, AgentConversationError> {
    serde_json::to_vec(outcome).map_err(|error| AgentConversationError::non_retryable(error.to_string()))
}

pub(super) fn decode_outcome(bytes: Option<&[u8]>) -> Result<AgentTurnOutcome, AgentConversationError> {
    let bytes =
        bytes.ok_or_else(|| AgentConversationError::reconciliation_required("durable turn outcome is missing"))?;
    serde_json::from_slice(bytes).map_err(|error| AgentConversationError::reconciliation_required(error.to_string()))
}

pub(super) fn outcome_store_state(outcome: &AgentTurnOutcome) -> ConversationTurnState {
    match outcome.status {
        AgentOutcomeStatus::Completed => ConversationTurnState::Completed,
        AgentOutcomeStatus::Failed => ConversationTurnState::Failed,
        AgentOutcomeStatus::Cancelled => ConversationTurnState::Cancelled,
        AgentOutcomeStatus::OutcomeUnknown => ConversationTurnState::OutcomeUnknown,
        AgentOutcomeStatus::ReconciliationRequired => ConversationTurnState::ReconciliationRequired,
    }
}

pub(super) fn failure_class_name(class: TaskFailureClass) -> &'static str {
    match class {
        TaskFailureClass::Retryable => "retryable",
        TaskFailureClass::NonRetryable => "non_retryable",
        TaskFailureClass::PermissionDenied => "permission_denied",
        TaskFailureClass::MaxTurns => "max_turns",
        TaskFailureClass::NonConvergent => "non_convergent",
        TaskFailureClass::Cancelled => "cancelled",
        TaskFailureClass::OutcomeUnknown => "outcome_unknown",
        TaskFailureClass::ReconciliationRequired => "reconciliation_required",
        TaskFailureClass::SideEffectUnknown => "side_effect_unknown",
    }
}

pub(super) fn blocks_following_turns(outcome: &AgentTurnOutcome) -> bool {
    matches!(
        outcome.failure_class,
        Some(
            TaskFailureClass::NonRetryable
                | TaskFailureClass::PermissionDenied
                | TaskFailureClass::MaxTurns
                | TaskFailureClass::NonConvergent
                | TaskFailureClass::Cancelled
                | TaskFailureClass::OutcomeUnknown
                | TaskFailureClass::ReconciliationRequired
                | TaskFailureClass::SideEffectUnknown
        )
    )
}

pub(super) fn engine_failure_message(stop_reason: StopReason) -> &'static str {
    match stop_reason {
        StopReason::EndTurn => "Agent stopped without a valid completed result",
        StopReason::ToolUse => "Agent stopped with unresolved tool use",
        StopReason::MaxTokens => "Agent stopped after reaching its output token limit",
        StopReason::MaxTurns => "Agent stopped after reaching its turn limit",
    }
}

pub(super) fn blocked_conversation_error(class: &str) -> AgentConversationError {
    match class {
        "non_retryable" => AgentConversationError::non_retryable("Agent conversation is blocked by a prior turn"),
        "outcome_unknown" => AgentConversationError::outcome_unknown("Agent conversation is blocked by a prior turn"),
        _ => AgentConversationError::reconciliation_required("Agent conversation is blocked by a prior turn"),
    }
}

pub(super) fn lifecycle_state_name(state: AgentLifecycleState) -> &'static str {
    match state {
        AgentLifecycleState::Completed => "completed",
        AgentLifecycleState::Failed => "failed",
        AgentLifecycleState::Cancelled => "cancelled",
        _ => "invalid",
    }
}

pub(super) fn parse_terminal_state(value: Option<&str>) -> Result<AgentLifecycleState, AgentConversationError> {
    match value {
        Some("completed") => Ok(AgentLifecycleState::Completed),
        Some("failed") => Ok(AgentLifecycleState::Failed),
        _ => Err(AgentConversationError::reconciliation_required(
            "durable conversation has an invalid terminal state",
        )),
    }
}

pub(super) fn store_error(error: crate::session::store::SessionStoreError) -> AgentConversationError {
    AgentConversationError::reconciliation_required(error.to_string())
}

pub(super) fn enqueue_store_error(error: SessionStoreError) -> AgentConversationError {
    match error {
        SessionStoreError::ConversationTurnInputConflict | SessionStoreError::ConversationHandleMismatch => {
            AgentConversationError::non_retryable(error.to_string())
        }
        SessionStoreError::ConversationStateConflict { .. } => {
            AgentConversationError::non_retryable("Agent conversation is closing or closed")
        }
        _ => store_error(error),
    }
}

pub(super) fn close_store_error(error: SessionStoreError) -> AgentConversationError {
    match error {
        SessionStoreError::ConversationStateConflict { ref state } if state == "opening" => {
            AgentConversationError::reconciliation_required(error.to_string())
        }
        _ => store_error(error),
    }
}

pub(super) fn validate_session(
    spec: &AgentConversationSpec,
    config: &solaris_config::config::Config,
    cwd: &std::path::Path,
    session: &crate::session::Session,
) -> Result<(), AgentConversationError> {
    if session.run_id.as_deref() != Some(spec.run_id.as_str())
        || session.provider != config.provider_label
        || session.model != config.model
        || session.cwd != cwd.to_string_lossy()
    {
        return Err(AgentConversationError::reconciliation_required(
            "durable child session identity or configuration changed",
        ));
    }
    Ok(())
}

pub(super) async fn release_prepared_turn(mut prepared: PreparedTurn) -> Result<(), AgentConversationError> {
    let engine = prepared.take_engine()?;
    let cleanup = prepared.begin_cleanup();
    blocking_agent(move || {
        let _cleanup = cleanup;
        engine
            .release_session_for_cold_resume()
            .map_err(|error| AgentConversationError::reconciliation_required(error.to_string()))
    })
    .await
}

fn spawn_detached_session_release(engine: AgentEngine, registry: Arc<SessionCleanupRegistry>, cleanup_key: String) {
    let cleanup = registry.register(cleanup_key);
    let spawn = std::thread::Builder::new()
        .name("solaris-session-release".to_owned())
        .spawn(move || {
            let _cleanup = cleanup;
            if let Err(error) = engine.release_session_for_cold_resume() {
                tracing::warn!(
                    target: "solaris_agent",
                    error = %error,
                    "failed to release aborted Agent conversation session lease"
                );
            }
        });
    if let Err(error) = spawn {
        tracing::error!(
            target: "solaris_agent",
            error = %error,
            "failed to start Agent conversation session release worker"
        );
    }
}

pub(super) fn agent_error(error: AgentError) -> AgentConversationError {
    let failure_class = match &error {
        AgentError::ReconciliationRequired { .. } => TaskFailureClass::ReconciliationRequired,
        AgentError::UserAborted => TaskFailureClass::Cancelled,
        AgentError::ToolCallMalformed { .. } | AgentError::ToolCallFailures { .. } => TaskFailureClass::NonConvergent,
        AgentError::Provider(_) | AgentError::ApiError(_) => TaskFailureClass::OutcomeUnknown,
        AgentError::ResourceBudgetExceeded(_) | AgentError::ContextTooLong { .. } => TaskFailureClass::NonRetryable,
    };
    AgentConversationError {
        failure_class,
        message: error.to_string(),
    }
}

pub(super) fn failure_outcome(
    handle: &AgentConversationHandle,
    turn: &AgentTurnSpec,
    identity: &AgentTurnIdentity,
    error: &AgentConversationError,
) -> AgentTurnOutcome {
    let status = match error.failure_class {
        TaskFailureClass::OutcomeUnknown => AgentOutcomeStatus::OutcomeUnknown,
        TaskFailureClass::ReconciliationRequired => AgentOutcomeStatus::ReconciliationRequired,
        TaskFailureClass::Cancelled => AgentOutcomeStatus::Cancelled,
        TaskFailureClass::Retryable
        | TaskFailureClass::NonRetryable
        | TaskFailureClass::PermissionDenied
        | TaskFailureClass::MaxTurns
        | TaskFailureClass::NonConvergent
        | TaskFailureClass::SideEffectUnknown => AgentOutcomeStatus::Failed,
    };
    AgentTurnOutcome {
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
        status,
        output: json!({"error": error.message}),
        outcome_ref: None,
        output_projection: None,
        usage: TokenUsage::default(),
        turns: 0,
        failure_class: Some(error.failure_class),
        error: Some(error.message.clone()),
    }
}
