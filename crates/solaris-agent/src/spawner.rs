use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::Instant;

use futures::future::join_all;
use serde_json::json;
use tokio::sync::Notify;
use tokio::task::JoinSet;
use uuid::Uuid;

use solaris_config::config::{Config, McpServerConfig};
use solaris_mcp::manager::McpManager;
use solaris_process::filter_resource_environment;
use solaris_protocol::ToolApprovalManager;
use solaris_protocol::writer::ProtocolEmitter;
use solaris_providers::LlmProvider;
use solaris_tools::read_only_evidence::ReadOnlyEvidenceIndex;
use solaris_tools::registry::ToolRegistry;
use solaris_types::effect::{
    DurabilityClass, EffectClass, EffectDescriptor, EffectReplayPolicy, EffectRequest, ResourceFootprint,
};
use solaris_types::identity::{AgentId, ChildAgentKey, OperationId, RunId, TaskId};
use solaris_types::message::TokenUsage;
use solaris_types::permission::{PermissionCeiling, PermissionDecision};
use solaris_types::resource::ResourceBudget;
use solaris_types::runtime::{OperationEnvironmentSnapshot, TaskFailureClass, TaskRecord, TaskState};
use solaris_types::workflow::{
    CollaborationRunStatus, CollaborationRunSummary, CollaborationSelection, CollaborationTaskInput,
    CollaborationTaskSummary, MultiAgentPolicy,
};

use crate::child_capabilities::ChildCapabilityBlueprint;
use crate::collaboration_runtime::{AgentSpawnReservation, CollaborationRuntime, TaskSettlement};
use crate::engine::{AgentEngine, AgentResult};
use crate::execution_context::{
    EffectExecutionContext, EffectOutcomeGuard, EffectRecoveryDecision, SessionFenceState,
    build_environment_snapshot_with_plugins, stable_digest_value,
};
use crate::output::OutputSink;
use crate::output::runtime_sink::RuntimeOutputSink;
use crate::permission_engine::PermissionContext;
use crate::resource_manager::ResourceManager;
use crate::resource_policy::ResourcePolicy;
use crate::scheduler::Scheduler;
use crate::session::SessionManager;

use crate::spawn_tool::{DEFAULT_SUB_AGENT_MAX_TOKENS, DEFAULT_SUB_AGENT_MAX_TURNS, ParsedSpawnRequest};

mod conversation;
mod fork;
mod service;

const DIRECT_SUPERVISOR_MAX_ATTEMPTS: u32 = 2;
const INDEPENDENT_REVIEWER_TASK_ID: &str = "__independent_reviewer";

#[derive(Debug, Clone)]
struct SupervisorRetryIntent {
    task_id: String,
    attempt: u32,
    next_operation_id: OperationId,
    sequence: u64,
}

pub use conversation::AgentConversationService;

#[cfg(test)]
use service::build_tool_registry;
use service::{
    build_tool_registry_with_evidence, resource_budget_from_env, spawn_cancelled, spawn_error, spawn_failure,
    spawn_reconciliation,
};

// Re-export from solaris-types — single source of truth
pub use solaris_types::spawner::{
    AgentCollaborationContext, AgentConversationConfig, AgentConversationError, AgentConversationHandle,
    AgentConversationSpec, AgentHandle, AgentOutcome, AgentOutcomeStatus, AgentSpawnError, AgentSpawnService,
    AgentSpawnSpec, AgentTurnIdentity, AgentTurnOutcome, AgentTurnSpec, ForkOverrides, Spawner, SubAgentConfig,
    SubAgentResult,
};

fn sub_agent_result_from_engine(name: String, agent_id: AgentId, result: AgentResult) -> SubAgentResult {
    let status = result.status;
    SubAgentResult {
        name,
        agent_id: Some(agent_id),
        task_id: None,
        status,
        failure_class: result.failure_class,
        output: Some(json!({"text": result.text.clone()})),
        text: result.text,
        usage: result.usage,
        turns: result.turns,
        is_error: status != AgentOutcomeStatus::Completed,
    }
}

fn collaboration_task_id(run_id: &RunId, task_id: &str) -> TaskId {
    TaskId::new(format!("collaboration:{run_id}:{task_id}"))
}

fn collaboration_operation_id(run_id: &RunId, task_id: &str, supervisor_attempt: u32) -> OperationId {
    if supervisor_attempt == 0 {
        OperationId::new(format!("collaboration:{run_id}:{task_id}"))
    } else {
        OperationId::new(format!(
            "collaboration:{run_id}:{task_id}:supervisor-attempt:{supervisor_attempt}"
        ))
    }
}

fn resolve_collaboration_strategy(
    request: &ParsedSpawnRequest,
    configured: &CollaborationSelection,
) -> solaris_types::workflow::CollaborationStrategy {
    let selection = if matches!(request.strategy, CollaborationSelection::Auto) {
        configured
    } else {
        &request.strategy
    };
    match selection {
        CollaborationSelection::Fixed(strategy) => *strategy,
        CollaborationSelection::Configured(config) => config.strategy,
        CollaborationSelection::Inherit => solaris_types::workflow::CollaborationStrategy::Single,
        CollaborationSelection::Auto => {
            if request.tasks.len() <= 1 {
                solaris_types::workflow::CollaborationStrategy::Single
            } else if request.tasks.iter().any(|task| !task.depends_on.is_empty()) {
                solaris_types::workflow::CollaborationStrategy::Supervisor
            } else {
                solaris_types::workflow::CollaborationStrategy::Fanout
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn collaboration_task_summary(
    task: &CollaborationTaskInput,
    task_id: &TaskId,
    status: TaskState,
    agent_id: Option<AgentId>,
    error_kind: Option<TaskFailureClass>,
    retries: u32,
    duration_ms: u64,
    usage: TokenUsage,
    output: Option<serde_json::Value>,
    error: Option<String>,
) -> CollaborationTaskSummary {
    CollaborationTaskSummary {
        id: task.id.clone().unwrap_or_else(|| task_id.as_str().to_owned()),
        name: task.name.clone(),
        status,
        agent_id,
        error_kind,
        retries,
        duration_ms,
        usage,
        output,
        error,
    }
}

fn failed_collaboration_summary(tasks: Vec<CollaborationTaskInput>, error: String) -> CollaborationRunSummary {
    let summaries = tasks
        .into_iter()
        .enumerate()
        .map(|(index, task)| {
            let task_id = TaskId::new(format!("invalid:{index}"));
            collaboration_task_summary(
                &task,
                &task_id,
                TaskState::Failed,
                None,
                Some(TaskFailureClass::NonRetryable),
                0,
                0,
                TokenUsage::default(),
                None,
                Some(error.clone()),
            )
        })
        .collect::<Vec<_>>();
    CollaborationRunSummary {
        status: CollaborationRunStatus::Failed,
        summary: error.clone(),
        next_actions: vec!["修正 Spawn 输入后重新提交".to_owned()],
        tasks: summaries,
        needs_manual_verification: vec![error],
        ..CollaborationRunSummary::default()
    }
}

fn unknown_collaboration_summary(tasks: Vec<CollaborationTaskInput>, error: String) -> CollaborationRunSummary {
    let mut summary = failed_collaboration_summary(tasks, error.clone());
    summary.status = CollaborationRunStatus::OutcomeUnknown;
    summary.summary = error.clone();
    summary.next_actions = vec!["人工核验持久化协作状态后再继续 Run".to_owned()];
    summary.outcome_unknown = true;
    summary.needs_manual_verification = vec![error.clone()];
    for task in &mut summary.tasks {
        task.error_kind = Some(TaskFailureClass::ReconciliationRequired);
        task.error = Some(error.clone());
    }
    summary
}

fn single_strategy_summary(tasks: Vec<CollaborationTaskInput>) -> CollaborationRunSummary {
    let summary = "strategy=single keeps execution in the parent Agent; Spawn did not create a Child Agent".to_owned();
    let task_summaries = tasks
        .into_iter()
        .enumerate()
        .map(|(index, task)| {
            let task_id = TaskId::new(format!("single:{index}"));
            collaboration_task_summary(
                &task,
                &task_id,
                TaskState::Skipped,
                None,
                Some(TaskFailureClass::NonRetryable),
                0,
                0,
                TokenUsage::default(),
                None,
                Some(summary.clone()),
            )
        })
        .collect();
    CollaborationRunSummary {
        status: CollaborationRunStatus::Failed,
        summary: summary.clone(),
        next_actions: vec![
            "由父 Agent 直接执行这些任务，或显式选择 fanout、supervisor、team 或 independent_reviewer".to_owned(),
        ],
        tasks: task_summaries,
        ..CollaborationRunSummary::default()
    }
}

/// Spawns independent child agents that share the parent's LLM provider.
///
/// Sub-agents publish streaming/tool lifecycle events through the Mesh runtime
/// event bus. Their output never enters the parent's ordinary text stream; the
/// parent still receives the final `SubAgentResult` for compatibility.
pub struct AgentSpawner {
    provider: Arc<dyn LlmProvider>,
    base_config: Config,
    cwd: PathBuf,
    runtime_env: Vec<(String, String)>,
    permission_context: PermissionContext,
    lifecycle_runtime: Arc<CollaborationRuntime<()>>,
    run_id: RunId,
    parent_agent_id: AgentId,
    capability_blueprint: Option<Arc<ChildCapabilityBlueprint>>,
    spawn_depth: usize,
    resources: Arc<ResourceManager>,
    operation_locks: Arc<SpawnOperationLocks>,
    host_approval: Arc<Mutex<Option<HostApprovalContext>>>,
    spawn_control: Arc<SpawnControl>,
    multi_agent_policy: Arc<RwLock<MultiAgentPolicy>>,
    default_strategy: Arc<RwLock<CollaborationSelection>>,
    max_tasks_per_run: u32,
    session_fences: SessionFenceState,
    remaining_spawn_depth: Option<usize>,
    read_only_evidence_index: Arc<ReadOnlyEvidenceIndex>,
}

#[derive(Default)]
struct SpawnControl {
    cancelled: Mutex<HashSet<OperationId>>,
    active: Mutex<HashMap<OperationId, Arc<Notify>>>,
}

#[derive(Clone)]
struct HostApprovalContext {
    manager: Arc<ToolApprovalManager>,
    writer: Arc<dyn ProtocolEmitter>,
}

#[derive(Default)]
struct SpawnOperationLocks {
    locks: Mutex<HashMap<ChildAgentKey, Weak<tokio::sync::Mutex<()>>>>,
}

impl SpawnOperationLocks {
    async fn acquire(&self, key: ChildAgentKey) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = {
            let mut locks = self.locks.lock().unwrap_or_else(|error| error.into_inner());
            match locks.get(&key).and_then(Weak::upgrade) {
                Some(lock) => lock,
                None => {
                    let lock = Arc::new(tokio::sync::Mutex::new(()));
                    locks.insert(key, Arc::downgrade(&lock));
                    lock
                }
            }
        };
        lock.lock_owned().await
    }
}

struct SpawnExecutionGuard {
    runtime: Arc<CollaborationRuntime<()>>,
    reservation: AgentSpawnReservation,
    name: String,
    committed: bool,
    finished: bool,
}

impl SpawnExecutionGuard {
    fn new(runtime: Arc<CollaborationRuntime<()>>, reservation: AgentSpawnReservation, name: String) -> Self {
        Self {
            runtime,
            reservation,
            name,
            committed: false,
            finished: false,
        }
    }

    fn committed(&mut self) {
        self.committed = true;
    }

    fn abort(&mut self, reason: &str) -> Result<(), String> {
        self.runtime
            .abort_spawn(&self.reservation, reason)
            .map_err(|error| format!("spawn rollback persistence failed: {error}"))?;
        self.finished = true;
        Ok(())
    }

    fn finish(&mut self) {
        self.finished = true;
    }

    fn fail(&mut self, name: &str, reason: &str, message: String) -> SubAgentResult {
        match self.abort(reason) {
            Ok(()) => spawn_error(name, message),
            Err(cleanup) => spawn_reconciliation(name, format!("{message}; {cleanup}")),
        }
    }
}

impl Drop for SpawnExecutionGuard {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        if self.committed {
            let result = SubAgentResult {
                name: self.name.clone(),
                agent_id: Some(self.reservation.child_agent_id.clone()),
                task_id: None,
                status: AgentOutcomeStatus::Cancelled,
                failure_class: Some(TaskFailureClass::Cancelled),
                output: Some(json!({"error": "child Agent cancelled because its owning operation was dropped"})),
                text: "child Agent cancelled because its owning operation was dropped".to_owned(),
                usage: TokenUsage::default(),
                turns: 0,
                is_error: true,
            };
            if self
                .runtime
                .cancel_spawn(&self.reservation, "owning operation was dropped after spawn commit")
                .is_ok()
            {
                let _ = self.runtime.record_agent_outcome(&self.reservation, &result);
            }
        } else {
            let _ = self
                .runtime
                .abort_spawn(&self.reservation, "owning operation cancelled before spawn commit");
        }
    }
}

impl AgentSpawner {
    pub fn new(provider: Arc<dyn LlmProvider>, config: Config, cwd: PathBuf) -> Self {
        Self::new_with_env(provider, config, cwd, Vec::new())
    }

    pub fn new_with_env(
        provider: Arc<dyn LlmProvider>,
        config: Config,
        cwd: PathBuf,
        runtime_env: Vec<(String, String)>,
    ) -> Self {
        let runtime_env = filter_resource_environment(runtime_env);
        let permission_context = PermissionContext::from_auto_approve(config.tools.auto_approve);
        let lifecycle_runtime = Arc::new(CollaborationRuntime::new(Scheduler::new(ResourcePolicy::new(1))));
        let run_id = RunId::new(format!("run-{}", Uuid::now_v7()));
        let parent_agent_id = AgentId::new(format!("agent-{}", Uuid::now_v7()));
        let configured_policy = config.multi_agent.policy;
        let configured_strategy = config.multi_agent.strategy.clone();
        let configured_max_tasks = config.multi_agent.max_tasks_per_run;
        let mut resource_budget = resource_budget_from_env(&runtime_env);
        if config.multi_agent.max_active_agents.is_some() {
            resource_budget.max_active_agents = config.multi_agent.max_active_agents;
        }
        let resources = ResourceManager::new(resource_budget);
        resources.set_provider_signals(config.provider_contract().signals.clone());
        Self {
            provider,
            max_tasks_per_run: configured_max_tasks,
            base_config: config,
            cwd,
            permission_context,
            lifecycle_runtime,
            run_id,
            parent_agent_id,
            runtime_env,
            capability_blueprint: None,
            spawn_depth: 0,
            resources,
            operation_locks: Arc::new(SpawnOperationLocks::default()),
            host_approval: Arc::new(Mutex::new(None)),
            spawn_control: Arc::new(SpawnControl::default()),
            multi_agent_policy: Arc::new(RwLock::new(configured_policy)),
            default_strategy: Arc::new(RwLock::new(configured_strategy)),
            session_fences: Arc::new(RwLock::new(Vec::new())),
            remaining_spawn_depth: None,
            read_only_evidence_index: Arc::new(ReadOnlyEvidenceIndex::default()),
        }
    }

    pub fn with_resource_manager(mut self, resources: Arc<ResourceManager>) -> Self {
        self.resources = resources;
        self
    }

    pub(crate) const fn max_tasks_per_run(&self) -> usize {
        self.max_tasks_per_run as usize
    }

    pub fn resource_manager(&self) -> Arc<ResourceManager> {
        Arc::clone(&self.resources)
    }

    /// Return the strategy used when a Spawn request leaves strategy selection
    /// to configuration (`auto`).
    pub fn collaboration_strategy(&self) -> CollaborationSelection {
        self.default_strategy
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    /// Change the configured default strategy for subsequent Spawn requests.
    pub fn set_collaboration_strategy(&self, strategy: CollaborationSelection) {
        *self.default_strategy.write().unwrap_or_else(|error| error.into_inner()) = strategy;
    }

    pub fn with_permission_context(mut self, context: PermissionContext) -> Self {
        self.permission_context = context;
        self
    }

    pub(crate) fn with_read_only_evidence_index(mut self, index: Arc<ReadOnlyEvidenceIndex>) -> Self {
        self.read_only_evidence_index = index;
        self
    }

    pub(crate) fn with_session_fence_state(mut self, state: SessionFenceState) -> Self {
        self.session_fences = state;
        self
    }

    pub fn with_runtime_context(
        mut self,
        lifecycle_runtime: Arc<CollaborationRuntime<()>>,
        run_id: RunId,
        parent_agent_id: AgentId,
    ) -> Self {
        self.lifecycle_runtime = lifecycle_runtime;
        self.run_id = run_id;
        self.parent_agent_id = parent_agent_id;
        self
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn parent_agent_id(&self) -> &AgentId {
        &self.parent_agent_id
    }

    pub(crate) fn multi_agent_policy_state(&self) -> Arc<RwLock<MultiAgentPolicy>> {
        Arc::clone(&self.multi_agent_policy)
    }

    fn ensure_multi_agent_allowed(&self) -> Result<(), String> {
        let policy = *self
            .multi_agent_policy
            .read()
            .unwrap_or_else(|error| error.into_inner());
        if policy == MultiAgentPolicy::Disabled {
            Err("multi-agent policy is disabled for this Run".to_owned())
        } else {
            Ok(())
        }
    }

    pub fn lifecycle_runtime(&self) -> Arc<CollaborationRuntime<()>> {
        Arc::clone(&self.lifecycle_runtime)
    }

    pub fn prepare_collaboration_handles(&self, handles: &[(AgentHandle, bool)]) -> Result<(), String> {
        self.lifecycle_runtime
            .prepare_spawn_batch(&self.run_id, handles, self.max_tasks_per_run())
            .map_err(|error| error.to_string())
    }

    pub fn set_host_approval(&self, manager: Arc<ToolApprovalManager>, writer: Arc<dyn ProtocolEmitter>) {
        *self.host_approval.lock().unwrap_or_else(|error| error.into_inner()) =
            Some(HostApprovalContext { manager, writer });
    }

    pub fn with_capability_blueprint(mut self, blueprint: Arc<ChildCapabilityBlueprint>) -> Self {
        self.capability_blueprint = Some(blueprint);
        self
    }

    pub fn add_mcp_capability_source(
        &self,
        manager: Arc<McpManager>,
        server_configs: std::collections::HashMap<String, McpServerConfig>,
    ) -> Result<(), String> {
        let blueprint = self
            .capability_blueprint
            .as_ref()
            .ok_or_else(|| "child capability blueprint is not initialized".to_owned())?;
        blueprint.add_mcp_source(manager, server_configs);
        Ok(())
    }

    pub fn mcp_capability_source_count(&self) -> usize {
        self.capability_blueprint
            .as_ref()
            .map(|blueprint| blueprint.mcp_source_count())
            .unwrap_or(0)
    }

    pub fn take_mcp_capability_sources(&self) -> Vec<Arc<McpManager>> {
        self.capability_blueprint
            .as_ref()
            .map(|blueprint| blueprint.take_mcp_sources())
            .unwrap_or_default()
    }

    pub fn add_active_plugin(
        &self,
        plugin: Arc<solaris_types::plugin::ResolvedPluginDefinition>,
    ) -> Result<(), String> {
        let blueprint = self
            .capability_blueprint
            .as_ref()
            .ok_or_else(|| "child capability blueprint is not initialized".to_owned())?;
        blueprint.add_active_plugin(plugin)
    }

    pub fn remove_active_plugin(&self, plugin_id: &str) -> bool {
        self.capability_blueprint
            .as_ref()
            .is_some_and(|blueprint| blueprint.remove_active_plugin(plugin_id))
    }

    pub fn active_plugin_identities(&self) -> Vec<solaris_types::plugin::ImplementationIdentity> {
        self.capability_blueprint
            .as_ref()
            .map(|blueprint| blueprint.plugin_identities())
            .unwrap_or_default()
    }

    fn build_child_engine(
        &self,
        reservation: &AgentSpawnReservation,
        config: Config,
        tools: ToolRegistry,
        output: Arc<dyn OutputSink>,
        child_permissions: PermissionContext,
        child_execution_context: EffectExecutionContext,
    ) -> Result<AgentEngine, String> {
        let session_id = reservation
            .key
            .session_id_for_agent_id(&reservation.child_agent_id)
            .ok_or_else(|| "child Agent identity version is unknown; reconciliation is required".to_owned())?;
        let provider_label = config.provider_label.clone();
        let sessions_enabled = config.session.enabled;
        let existing_session = if sessions_enabled {
            let manager = SessionManager::new(config.session.directory.clone().into(), config.session.max_sessions);
            let session = manager
                .load_if_exists(&session_id)
                .map_err(|error| format!("failed to load child session: {error}"))?;
            if let Some(session) = session.as_ref()
                && session
                    .run_id
                    .as_deref()
                    .is_some_and(|run_id| run_id != reservation.key.run_id.as_str())
            {
                return Err(format!("child session '{}' belongs to another run", session.id));
            }
            session
        } else {
            None
        };
        let should_resume = existing_session.is_some();
        let should_initialize = sessions_enabled && existing_session.is_none();
        let mut engine = if let Some(session) = existing_session {
            AgentEngine::resume_with_provider_and_env(
                self.provider.clone(),
                config,
                tools,
                output,
                session,
                self.cwd.clone(),
                self.runtime_env.clone(),
            )
        } else {
            AgentEngine::new_with_provider_and_env(
                self.provider.clone(),
                config,
                tools,
                output,
                self.cwd.clone(),
                self.runtime_env.clone(),
            )
        };
        engine.set_permission_context(child_permissions);
        engine.set_execution_context(child_execution_context);
        engine.set_multi_agent_policy_state(self.multi_agent_policy_state());
        engine.set_resource_manager(self.resource_manager());
        if should_resume {
            engine
                .activate_resumed_session()
                .map_err(|error| format!("failed to acquire child session lease: {error}"))?;
        }
        engine.set_interactive_confirmation(false);
        if let Some(host) = self
            .host_approval
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
        {
            engine.set_approval_manager(host.manager);
            engine.set_protocol_writer(host.writer);
        }
        if should_initialize {
            engine
                .init_session(&provider_label, self.cwd.to_string_lossy().as_ref(), Some(&session_id))
                .map_err(|error| format!("failed to initialize child session: {error}"))?;
        }
        Ok(engine)
    }

    fn child_registry(
        &self,
        requested: &[String],
        inherit_capabilities: bool,
        child_agent_id: AgentId,
        child_permissions: PermissionContext,
        recursion_limit: Option<usize>,
        execution_context: EffectExecutionContext,
    ) -> ToolRegistry {
        if let Some(blueprint) = &self.capability_blueprint {
            let mut child_spawner = self.clone_for_child(child_agent_id.clone(), child_permissions);
            child_spawner.remaining_spawn_depth =
                recursion_limit.or_else(|| self.remaining_spawn_depth.map(|remaining| remaining.saturating_sub(1)));
            let child_spawner = Arc::new(child_spawner);
            blueprint.build_registry(
                requested,
                inherit_capabilities,
                &self.cwd,
                &self.runtime_env,
                child_spawner,
                Arc::clone(&self.lifecycle_runtime),
                self.run_id.clone(),
                child_agent_id,
                execution_context,
            )
        } else {
            build_tool_registry_with_evidence(
                requested,
                inherit_capabilities,
                &self.cwd,
                &self.runtime_env,
                &child_permissions,
                Arc::clone(&self.read_only_evidence_index),
            )
        }
    }

    fn plugin_identities(&self) -> Vec<solaris_types::plugin::ImplementationIdentity> {
        self.capability_blueprint
            .as_ref()
            .map(|blueprint| blueprint.plugin_identities())
            .unwrap_or_default()
    }

    fn attach_collaboration(
        &self,
        reservation: &AgentSpawnReservation,
        collaboration: Option<&AgentCollaborationContext>,
    ) -> Result<(), String> {
        let Some(collaboration) = collaboration else {
            return Ok(());
        };
        self.lifecycle_runtime
            .ensure_collaboration_team(self.run_id.clone(), collaboration.strategy.as_str(), collaboration)
            .map_err(|error| error.to_string())?;
        self.lifecycle_runtime
            .join_team(
                &self.run_id,
                &collaboration.team_id,
                collaboration.coordinator_agent_id.clone(),
            )
            .map_err(|error| error.to_string())?;
        self.lifecycle_runtime
            .join_team(&self.run_id, &collaboration.team_id, reservation.child_agent_id.clone())
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    fn collaboration_prompt(
        &self,
        reservation: &AgentSpawnReservation,
        collaboration: Option<&AgentCollaborationContext>,
    ) -> Option<String> {
        collaboration.map(|collaboration| {
            let peer_rule = if matches!(collaboration.strategy, solaris_types::workflow::CollaborationStrategy::Team) {
                "Peer-to-peer messaging is allowed."
            } else {
                "Communicate through the coordinator; direct worker-to-worker messaging may be denied."
            };
            let mut prompt = format!(
                "Solaris Mesh collaboration context:\nagent_id={}\nteam_id={}\nstrategy={:?}\ncoordinator_agent_id={}\n{} Use SendAgentMessage/ReadAgentMessages for directed communication and AcknowledgeAgentMessage after handling a message. Use SetTeamFact/ReadTeamFacts for durable shared Team facts, and RegisterArtifact/ListArtifacts for durable artifact references. GetTeamState reports team topology.",
                reservation.child_agent_id,
                collaboration.team_id,
                collaboration.strategy,
                collaboration.coordinator_agent_id,
                peer_rule,
            );
            let pending = self.lifecycle_runtime.messages().inbox(&reservation.child_agent_id);
            if !pending.is_empty() {
                prompt.push_str("\n\nDurable Team messages already available at Agent start:\n");
                prompt.push_str(&serde_json::to_string_pretty(&pending).unwrap_or_default());
                prompt.push_str("\nUse every worker_result message when producing the final answer.");
            }
            prompt
        })
    }

    fn reattached_outcome(
        &self,
        reservation: &AgentSpawnReservation,
        name: &str,
        context: &str,
    ) -> Option<SubAgentResult> {
        if !reservation.reattached {
            return None;
        }
        match self.lifecycle_runtime.agent_outcome(reservation) {
            Ok(Some(result)) => Some(result),
            Ok(None) => None,
            Err(error) => Some(spawn_error(
                name,
                format!("failed to recover durable child outcome for {context}: {error}"),
            )),
        }
    }

    fn persist_outcome(&self, reservation: &AgentSpawnReservation, result: SubAgentResult) -> SubAgentResult {
        if let Err(error) = self.lifecycle_runtime.record_agent_outcome(reservation, &result) {
            return spawn_reconciliation(
                &result.name,
                format!(
                    "child execution finished but durable outcome persistence failed; reconciliation required: {error}"
                ),
            );
        }
        result
    }

    fn lifecycle_effect_context(&self) -> EffectExecutionContext {
        let environment = build_environment_snapshot_with_plugins(
            &self.base_config,
            &ToolRegistry::new(),
            &self.permission_context,
            self.plugin_identities(),
        );
        EffectExecutionContext::new(
            self.run_id.clone(),
            self.parent_agent_id.clone(),
            self.lifecycle_runtime.ledger(),
            self.permission_context.clone(),
            environment,
        )
        .with_mutation_coordinator(self.lifecycle_runtime.mutation_coordinator())
        .with_resource_manager(Arc::clone(&self.resources))
        .with_session_fence_state(Arc::clone(&self.session_fences))
    }

    fn lifecycle_effect_request(&self, spec: &AgentSpawnSpec) -> EffectRequest {
        let input = serde_json::to_value(spec).unwrap_or_else(|_| {
            json!({
                "run_id": spec.run_id,
                "parent_agent_id": spec.parent_agent_id,
                "task_id": spec.task_id,
                "role_key": spec.role_key,
                "stable_task_key": spec.stable_task_key,
                "operation_id": spec.operation_id,
            })
        });
        EffectRequest {
            effect_id: solaris_types::identity::EffectId::new(format!(
                "effect:{}:{}:spawn:{}",
                self.run_id, self.parent_agent_id, spec.operation_id
            )),
            operation_id: spec.operation_id.clone(),
            capability: "SpawnAgent".to_owned(),
            descriptor: EffectDescriptor {
                class: EffectClass::AgentLifecycle,
                action: format!("Spawn child Agent {}", spec.config.name),
                resources: ResourceFootprint {
                    mesh_resources: vec![format!("mesh:run:{}/agents", self.run_id)],
                    ..ResourceFootprint::default()
                },
                replay_policy: EffectReplayPolicy::Idempotent,
            },
            input_digest: Some(stable_digest_value(&input)),
            effective_input: input,
        }
    }

    #[cfg(test)]
    pub(crate) fn record_lifecycle_effect_intent_for_test(&self, spec: &AgentSpawnSpec) -> Result<(), String> {
        let context = self.lifecycle_effect_context();
        let request = self.lifecycle_effect_request(spec);
        context
            .record_effect_intent(&request)
            .map_err(|error| error.to_string())
    }

    fn spawn_recovery_error(&self, request: &EffectRequest, reason: String) -> AgentSpawnError {
        let Ok(records) = self.lifecycle_runtime.ledger().records_for_run(&self.run_id) else {
            return AgentSpawnError::reconciliation_required(reason);
        };
        let Some(intent) = records.iter().rev().find(|record| {
            record.record_type == "effect_intent"
                && record.payload.get("effect_id").and_then(serde_json::Value::as_str)
                    == Some(request.effect_id.as_str())
        }) else {
            return AgentSpawnError::reconciliation_required(reason);
        };
        let outcome = records.iter().rev().find(|record| {
            record.record_type == "effect_outcome"
                && record.seq > intent.seq
                && (record.payload.get("effect_id").and_then(serde_json::Value::as_str)
                    == Some(request.effect_id.as_str())
                    || record.payload.get("operation_id").and_then(serde_json::Value::as_str)
                        == Some(request.operation_id.as_str()))
        });
        if outcome.is_none()
            || outcome.is_some_and(|record| {
                record.payload.get("status").and_then(serde_json::Value::as_str) == Some("outcome_unknown")
            })
        {
            AgentSpawnError::outcome_unknown(reason)
        } else {
            AgentSpawnError::reconciliation_required(reason)
        }
    }

    fn fork_spec(
        &self,
        sub_config: SubAgentConfig,
        overrides: ForkOverrides,
        operation_id: OperationId,
        requested_ceiling: PermissionCeiling,
    ) -> AgentSpawnSpec {
        let stable_task_key = operation_id.as_str().to_owned();
        AgentSpawnSpec {
            run_id: self.run_id.clone(),
            parent_agent_id: self.parent_agent_id.clone(),
            task_id: TaskId::new(format!("spawn:{operation_id}")),
            role_key: sub_config.name.clone(),
            stable_task_key,
            operation_id,
            expected_task_revision: None,
            config: sub_config,
            overrides,
            permission_ceiling: requested_ceiling,
            resource_budget: ResourceBudget::default(),
            context_policy: None,
            recursion_limit: None,
        }
    }

    /// Spawn a single sub-agent and wait for result (legacy compatibility path).
    pub async fn spawn_one(&self, sub_config: SubAgentConfig) -> SubAgentResult {
        self.spawn_one_with_operation(sub_config, OperationId::new(format!("spawn-{}", Uuid::now_v7())))
            .await
    }

    /// Spawn with a caller-provided operation id so crash/retry can reattach deterministically.
    pub async fn spawn_one_with_operation(
        &self,
        sub_config: SubAgentConfig,
        operation_id: OperationId,
    ) -> SubAgentResult {
        if let Err(error) = self.ensure_multi_agent_allowed() {
            return spawn_error(&sub_config.name, error);
        }
        if self.remaining_spawn_depth == Some(0) {
            return spawn_error(
                &sub_config.name,
                "Agent role recursion policy denies spawning another child Agent".to_owned(),
            );
        }
        let key = ChildAgentKey {
            run_id: self.run_id.clone(),
            parent_agent_id: self.parent_agent_id.clone(),
            role_key: sub_config.name.clone(),
            stable_task_key: operation_id.as_str().to_owned(),
            spawn_operation_id: operation_id.clone(),
        };
        let _operation_guard = self.operation_locks.acquire(key).await;
        let context = self.lifecycle_effect_context();
        let effect_spec = self.fork_spec(
            sub_config.clone(),
            ForkOverrides {
                inherit_capabilities: true,
                ..ForkOverrides::default()
            },
            operation_id.clone(),
            PermissionCeiling::unrestricted(),
        );
        let request = self.lifecycle_effect_request(&effect_spec);
        match context.recover_effect(&request) {
            Ok(EffectRecoveryDecision::Reuse { output, .. }) => {
                return serde_json::from_str(&output).unwrap_or_else(|error| {
                    spawn_error(&sub_config.name, format!("invalid durable spawn outcome: {error}"))
                });
            }
            Ok(EffectRecoveryDecision::Execute) => {}
            Ok(EffectRecoveryDecision::Reconcile { reason }) => {
                return spawn_failure(&sub_config.name, self.spawn_recovery_error(&request, reason));
            }
            Err(reason) => {
                return spawn_failure(&sub_config.name, AgentSpawnError::reconciliation_required(reason));
            }
        }
        let evaluation = context.evaluate(&request);
        if let Err(error) = context.record_permission_decision(&request, &evaluation, "agent_spawn_service") {
            return spawn_error(
                &sub_config.name,
                format!("failed to persist spawn permission decision: {error}"),
            );
        }
        if evaluation.decision != PermissionDecision::Allow {
            return spawn_error(
                &sub_config.name,
                format!("spawn permission denied: {}", evaluation.reason),
            );
        }
        let _effect_permit = match context.acquire_effect_permit().await {
            Ok(permit) => permit,
            Err(error) => return spawn_error(&sub_config.name, format!("spawn effect budget denied: {error}")),
        };
        let approved_environment = context.environment();
        if let Err(error) = context.revalidate_environment(&request, &approved_environment) {
            let _ = context.record_revalidation_failure(&request, &error);
            return spawn_error(&sub_config.name, error);
        }
        if let Err(error) = context.record_effect_intent(&request) {
            return spawn_error(
                &sub_config.name,
                format!("failed to persist spawn effect intent: {error}"),
            );
        }
        let cancelled_key = ChildAgentKey {
            run_id: effect_spec.run_id.clone(),
            parent_agent_id: effect_spec.parent_agent_id.clone(),
            role_key: effect_spec.role_key.clone(),
            stable_task_key: effect_spec.stable_task_key.clone(),
            spawn_operation_id: effect_spec.operation_id.clone(),
        };
        let cancelled_agent_id = self
            .lifecycle_runtime
            .existing_child_identity(&cancelled_key)
            .map_or_else(|| cancelled_key.agent_id(), |(agent_id, _)| agent_id);
        let cancellation_outcome = serde_json::to_string(&spawn_cancelled(
            &sub_config.name,
            &cancelled_agent_id,
            &effect_spec.task_id,
        ))
        .unwrap_or_else(|_| "{\"status\":\"cancelled\",\"is_error\":true}".to_owned());
        let mut outcome_guard = EffectOutcomeGuard::new(context, request, cancellation_outcome);
        let result = self.spawn_one_authorized(sub_config, operation_id).await;
        if matches!(
            result.status,
            AgentOutcomeStatus::OutcomeUnknown | AgentOutcomeStatus::ReconciliationRequired
        ) {
            outcome_guard.leave_for_reconciliation();
            return result;
        }
        let output = match serde_json::to_string(&result) {
            Ok(output) => output,
            Err(error) => return spawn_error(&result.name, format!("failed to serialize spawn outcome: {error}")),
        };
        if let Err(error) = outcome_guard.complete(result.is_error, &output) {
            return spawn_error(&result.name, format!("failed to persist spawn effect outcome: {error}"));
        }
        result
    }

    async fn spawn_one_authorized(&self, sub_config: SubAgentConfig, operation_id: OperationId) -> SubAgentResult {
        let name = sub_config.name.clone();
        let reservation = match self.lifecycle_runtime.reserve_spawn(
            self.run_id.clone(),
            self.parent_agent_id.clone(),
            operation_id,
            &self.permission_context,
            PermissionCeiling::unrestricted(),
        ) {
            Ok(value) => value,
            Err(error) => return spawn_error(&name, format!("spawn reservation failed: {error}")),
        };
        if let Some(result) = self.reattached_outcome(&reservation, &name, "spawn operation") {
            return result;
        }
        let mut execution_guard =
            SpawnExecutionGuard::new(Arc::clone(&self.lifecycle_runtime), reservation.clone(), name.clone());
        let resource_permit = if reservation.reattached {
            self.resources
                .acquire_reattached_agent(self.spawn_depth.saturating_add(1))
                .await
        } else {
            self.resources.acquire_agent(self.spawn_depth.saturating_add(1)).await
        };
        let _resource_permit = match resource_permit {
            Ok(permit) => permit,
            Err(error) => {
                return execution_guard.fail(&name, &error, format!("spawn resource budget denied: {error}"));
            }
        };

        let mut config = self.base_config.clone();
        config.max_turns = Some(sub_config.max_turns);
        config.max_tokens = Some(sub_config.max_tokens);
        if let Some(sp) = sub_config.system_prompt.clone() {
            config.system_prompt = Some(sp);
        }
        tracing::info!(target: "solaris_agent", cwd = %self.cwd.display(), child_agent_id = %reservation.child_agent_id, "sub-agent spawned with workspace cwd");
        let child_permissions = self.permission_context.narrowed(reservation.permission_ceiling);
        let child_execution_context = EffectExecutionContext::new(
            self.run_id.clone(),
            reservation.child_agent_id.clone(),
            self.lifecycle_runtime.ledger(),
            child_permissions.clone(),
            OperationEnvironmentSnapshot::default(),
        )
        .with_mutation_coordinator(self.lifecycle_runtime.mutation_coordinator())
        .with_resource_manager(Arc::clone(&self.resources))
        .with_session_fence_state(Arc::new(RwLock::new(
            self.session_fences
                .read()
                .unwrap_or_else(|error| error.into_inner())
                .clone(),
        )));
        let tools = self.child_registry(
            &[],
            true,
            reservation.child_agent_id.clone(),
            child_permissions.clone(),
            None,
            child_execution_context.clone(),
        );
        let child_environment =
            build_environment_snapshot_with_plugins(&config, &tools, &child_permissions, self.plugin_identities());
        child_execution_context.set_environment(child_environment);
        let output: Arc<dyn OutputSink> = Arc::new(RuntimeOutputSink::new(
            Arc::clone(&self.lifecycle_runtime),
            self.run_id.clone(),
            reservation.child_agent_id.clone(),
        ));
        let mut engine = match self.build_child_engine(
            &reservation,
            config,
            tools,
            output,
            child_permissions,
            child_execution_context,
        ) {
            Ok(engine) => engine,
            Err(error) => {
                return execution_guard.fail(&name, &error, error.clone());
            }
        };

        if let Err(error) = self.lifecycle_runtime.commit_spawn(&reservation) {
            return execution_guard.fail(&name, &error.to_string(), format!("spawn commit failed: {error}"));
        }
        execution_guard.committed();

        let outcome = match engine
            .run(&sub_config.prompt, reservation.child_agent_id.as_str())
            .await
        {
            Ok(result) => sub_agent_result_from_engine(name, reservation.child_agent_id.clone(), result),
            Err(error) => spawn_error(&name, format!("Sub-agent error: {error}")),
        };
        let result = self.persist_outcome(&reservation, outcome);
        execution_guard.finish();
        result
    }

    /// Execute a typed Spawn v2 request through the durable Mesh runtime.
    ///
    /// Tasks are registered before any child is started. Each dependency wave
    /// is then spawned and joined as one batch, so the scheduler and the
    /// Run-wide `ResourceManager` provide the actual concurrency limit.
    fn record_supervisor_retry(
        &self,
        task_id: &str,
        attempt: u32,
        failure_class: TaskFailureClass,
        next_operation_id: &OperationId,
        reason: &str,
    ) -> Result<(), String> {
        self.lifecycle_runtime
            .commit_runtime_event(
                &self.run_id,
                DurabilityClass::SyncCritical,
                "supervisor_retry",
                Some(self.parent_agent_id.clone()),
                "supervisor_retry",
                json!({
                    "task_id": task_id,
                    "attempt": attempt,
                    "failure_class": failure_class,
                    "next_operation_id": next_operation_id,
                    "reason": reason,
                }),
            )
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn record_supervisor_round_completed(
        &self,
        round: u32,
        task_ids: &[String],
        retried: &[String],
    ) -> Result<(), String> {
        self.lifecycle_runtime
            .commit_runtime_event(
                &self.run_id,
                DurabilityClass::SyncCritical,
                "supervisor_round_completed",
                Some(self.parent_agent_id.clone()),
                "supervisor_round_completed",
                json!({"round": round, "task_ids": task_ids, "retried": retried}),
            )
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    /// Recover retry intent before scheduling another Supervisor wave.
    ///
    /// The retry event is deliberately written before the task CAS that moves
    /// a failed task back to `Queued`. If the process stops in that small
    /// interval, the durable event is the only indication that the next
    /// operation was authorized. Replaying it here prevents a lost retry while
    /// still refusing to start work when the durable state is ambiguous.
    fn recover_supervisor_retry_intents(
        &self,
        task_ids: &HashMap<String, TaskId>,
    ) -> Result<HashMap<String, u32>, String> {
        let records = self
            .lifecycle_runtime
            .ledger()
            .records_for_run(&self.run_id)
            .map_err(|error| format!("failed to read Supervisor retry intents: {error}"))?;
        let mut intents = HashMap::<String, SupervisorRetryIntent>::new();
        for record in records.iter().filter(|record| record.record_type == "supervisor_retry") {
            let Some(task_id) = record.payload.get("task_id").and_then(serde_json::Value::as_str) else {
                return Err(format!("Supervisor retry record {} has no task_id", record.seq));
            };
            // One Run can contain multiple collaboration calls. An intent for
            // an earlier call is unrelated to the current task set.
            if !task_ids.contains_key(task_id) {
                continue;
            }
            let attempt = record
                .payload
                .get("attempt")
                .and_then(serde_json::Value::as_u64)
                .and_then(|value| u32::try_from(value).ok())
                .ok_or_else(|| format!("Supervisor retry record {} has an invalid attempt", record.seq))?;
            let next_attempt = attempt
                .checked_add(1)
                .ok_or_else(|| format!("Supervisor retry record {} overflows attempt", record.seq))?;
            if next_attempt >= DIRECT_SUPERVISOR_MAX_ATTEMPTS {
                return Err(format!(
                    "Supervisor retry record {} exceeds the retry limit",
                    record.seq
                ));
            }
            let failure_class = record
                .payload
                .get("failure_class")
                .cloned()
                .and_then(|value| serde_json::from_value::<TaskFailureClass>(value).ok());
            if failure_class != Some(TaskFailureClass::Retryable) {
                return Err(format!("Supervisor retry record {} is not Retryable", record.seq));
            }
            let next_operation_id = record
                .payload
                .get("next_operation_id")
                .and_then(serde_json::Value::as_str)
                .map(OperationId::from)
                .ok_or_else(|| format!("Supervisor retry record {} has no next_operation_id", record.seq))?;
            let expected_operation_id = collaboration_operation_id(&self.run_id, task_id, next_attempt);
            if next_operation_id != expected_operation_id {
                return Err(format!(
                    "Supervisor retry record {} has an unexpected next_operation_id",
                    record.seq
                ));
            }
            let intent = SupervisorRetryIntent {
                task_id: task_id.to_owned(),
                attempt,
                next_operation_id,
                sequence: record.seq,
            };
            if let Some(previous) = intents.get(task_id)
                && previous.sequence < intent.sequence
                && previous.next_operation_id != intent.next_operation_id
            {
                // A later intent is valid only when it advances the attempt.
                // The direct Supervisor currently permits one retry, but this
                // check keeps a corrupt or replayed record from selecting an
                // arbitrary operation.
                if intent.attempt <= previous.attempt {
                    return Err(format!(
                        "Supervisor retry records for task {task_id} do not advance monotonically"
                    ));
                }
            }
            intents.insert(task_id.to_owned(), intent);
        }

        let mut recovered_attempts = HashMap::new();
        for intent in intents.into_values() {
            let task_key = task_ids
                .get(&intent.task_id)
                .ok_or_else(|| format!("Supervisor retry task {} is not registered", intent.task_id))?;
            let task = self
                .lifecycle_runtime
                .tasks()
                .get(task_key)
                .ok_or_else(|| format!("Supervisor retry task {} is missing", intent.task_id))?;
            let retry_cas_applied = records.iter().any(|record| {
                record.seq > intent.sequence
                    && record.record_type == "task_cas"
                    && record.payload.get("task_id").and_then(serde_json::Value::as_str) == Some(task_key.as_str())
                    && record.payload.get("transition").and_then(serde_json::Value::as_str) == Some("supervisor_retry")
            });
            if matches!(
                task.state,
                TaskState::Completed | TaskState::Skipped | TaskState::Cancelled
            ) {
                continue;
            }
            if retry_cas_applied {
                // The retry CAS proves that the next operation was selected;
                // the task may still be Queued, Assigned, Running, or have
                // reached its terminal result before a restart. Keep using
                // that operation identity in every non-terminal case.
                if matches!(
                    task.state,
                    TaskState::Queued | TaskState::Assigned | TaskState::Running | TaskState::Failed
                ) {
                    recovered_attempts.insert(intent.task_id, intent.attempt + 1);
                    continue;
                }
                return Err(format!(
                    "Supervisor retry task {} has incompatible durable state {:?}",
                    intent.task_id, task.state
                ));
            }
            match task.state {
                TaskState::Queued => {
                    recovered_attempts.insert(intent.task_id, intent.attempt + 1);
                }
                TaskState::Failed if task.failure_class == Some(TaskFailureClass::Retryable) => {
                    let recovery_operation_id = OperationId::new(format!(
                        "{}:recovery-requeue:{}",
                        intent.next_operation_id,
                        intent.attempt + 1
                    ));
                    self.lifecycle_runtime
                        .requeue_supervisor_task(
                            &self.run_id,
                            task_key,
                            &self.parent_agent_id,
                            task.revision,
                            &recovery_operation_id,
                            "recovered durable Supervisor retry intent",
                        )
                        .map_err(|error| {
                            format!("failed to recover Supervisor retry for {}: {error}", intent.task_id)
                        })?;
                    recovered_attempts.insert(intent.task_id, intent.attempt + 1);
                }
                TaskState::Assigned | TaskState::Running => {
                    return Err(format!(
                        "Supervisor retry task {} has an unresolved active projection",
                        intent.task_id
                    ));
                }
                _ => {
                    return Err(format!(
                        "Supervisor retry task {} has incompatible durable state {:?}",
                        intent.task_id, task.state
                    ));
                }
            }
        }
        Ok(recovered_attempts)
    }

    pub async fn spawn_collaboration(&self, request: ParsedSpawnRequest) -> CollaborationRunSummary {
        let started = Instant::now();
        let original_tasks = request.tasks.clone();
        if let Err(error) = self.ensure_multi_agent_allowed() {
            return failed_collaboration_summary(original_tasks, error);
        }
        let strategy = resolve_collaboration_strategy(&request, &self.collaboration_strategy());
        let reviewer_required = strategy == solaris_types::workflow::CollaborationStrategy::IndependentReviewer;

        if strategy == solaris_types::workflow::CollaborationStrategy::Single {
            return single_strategy_summary(original_tasks);
        }
        let mut tasks_for_run = request.tasks.clone();
        if reviewer_required {
            if request
                .tasks
                .iter()
                .any(|task| task.id.as_deref() == Some(INDEPENDENT_REVIEWER_TASK_ID))
            {
                return failed_collaboration_summary(
                    original_tasks.clone(),
                    format!("task id {INDEPENDENT_REVIEWER_TASK_ID} is reserved for the independent reviewer"),
                );
            }
            let primary_ids = request
                .tasks
                .iter()
                .filter_map(|task| task.id.clone())
                .collect::<Vec<_>>();
            tasks_for_run.push(CollaborationTaskInput {
                id: Some(INDEPENDENT_REVIEWER_TASK_ID.to_owned()),
                name: "independent-reviewer".to_owned(),
                prompt: "Review the completed primary task results independently. Do not mutate the workspace, run processes, use the network, or create child Agents. Verify claims against the available read-only evidence and return a concise final review.".to_owned(),
                role: Some("independent_reviewer".to_owned()),
                depends_on: primary_ids,
                expected_output: None,
                resource_budget: None,
            });
        }
        let task_ids: HashMap<String, TaskId> = tasks_for_run
            .iter()
            .filter_map(|task| {
                task.id
                    .as_ref()
                    .map(|id| (id.clone(), collaboration_task_id(&self.run_id, id)))
            })
            .collect();
        if task_ids.len() != tasks_for_run.len() {
            return failed_collaboration_summary(
                original_tasks.clone(),
                "every collaboration task needs a stable id".to_owned(),
            );
        }

        let team_id = solaris_types::identity::TeamId::new(format!(
            "collaboration:{}:{}",
            self.run_id,
            stable_digest_value(&serde_json::to_value(&tasks_for_run).unwrap_or_default())
        ));
        // Standalone AgentSpawner instances used by embedders and tests do
        // not go through AgentBootstrap, so make the coordinator visible
        // before the atomic Team preparation step below.
        if self.lifecycle_runtime.agents().get(&self.parent_agent_id).is_none() {
            self.lifecycle_runtime
                .agents()
                .upsert(solaris_types::runtime::AgentRecord {
                    run_id: self.run_id.clone(),
                    agent_id: self.parent_agent_id.clone(),
                    team_id: None,
                    parent_agent_id: None,
                    state: solaris_types::runtime::AgentLifecycleState::Active,
                });
        }
        let collaboration = Some(AgentCollaborationContext {
            team_id,
            strategy,
            coordinator_agent_id: self.parent_agent_id.clone(),
            max_pending_messages: solaris_types::workflow::CollaborationRuntimeConfig::DEFAULT_MAX_PENDING_MESSAGES,
            max_message_bytes: solaris_types::workflow::CollaborationRuntimeConfig::DEFAULT_MAX_MESSAGE_BYTES,
        });

        let mut batch = Vec::with_capacity(tasks_for_run.len());
        for task in &tasks_for_run {
            let id = task.id.as_deref().unwrap_or_default();
            let task_id = task_ids.get(id).expect("task ids were validated above");
            let depends_on = task
                .depends_on
                .iter()
                .filter_map(|dependency| task_ids.get(dependency).cloned())
                .collect();
            batch.push(TaskRecord {
                run_id: self.run_id.clone(),
                task_id: task_id.clone(),
                revision: 0,
                task_key: Some(format!("collaboration:{id}")),
                team_id: collaboration.as_ref().map(|value| value.team_id.clone()),
                workflow_id: None,
                node_id: None,
                role: Some(task.role.clone().unwrap_or_else(|| task.name.clone())),
                depends_on,
                content: task.expected_output.clone(),
                expected_write_scope: Vec::new(),
                owner_agent_id: None,
                state: TaskState::Queued,
                outcome_ref: None,
                failure_class: None,
            });
        }
        // Durable Run-level admission: the quota check and the task_created
        // appends happen under the Run mutation line as one atomic step, so
        // sequential spawns, concurrent races, restarts, and idempotent
        // replays can neither bypass nor double-count the limit.
        let max_tasks = self.max_tasks_per_run.clamp(1, 256) as usize;
        if let Err(error) = self
            .lifecycle_runtime
            .admit_collaboration_tasks(&self.run_id, max_tasks, batch)
        {
            return failed_collaboration_summary(original_tasks.clone(), error.to_string());
        }

        let mut pending: HashSet<String> = task_ids.keys().cloned().collect();
        let task_by_id: HashMap<String, CollaborationTaskInput> = tasks_for_run
            .into_iter()
            .filter_map(|task| task.id.clone().map(|id| (id, task)))
            .collect();
        let mut outputs = HashMap::<String, String>::new();
        let mut summaries = HashMap::<String, CollaborationTaskSummary>::new();
        let mut created = 0_u32;
        let mut created_agent_ids = HashSet::<AgentId>::new();
        let mut reattached = 0_u32;
        let mut peak_active = 0_u32;
        let mut has_failure = false;
        let mut has_unknown = false;
        let mut needs_manual = Vec::new();
        let mut supervisor_attempts = if strategy == solaris_types::workflow::CollaborationStrategy::Supervisor {
            match self.recover_supervisor_retry_intents(&task_ids) {
                Ok(attempts) => attempts,
                Err(error) => return unknown_collaboration_summary(original_tasks.clone(), error),
            }
        } else {
            HashMap::new()
        };
        let mut supervisor_round = 0_u32;

        while !pending.is_empty() {
            // A failed prerequisite blocks the dependent task. Persist the
            // skip before removing it from the wave so recovery cannot later
            // mistake a queued dependent for work that was never considered.
            let blocked: Vec<String> = pending
                .iter()
                .filter(|id| {
                    task_by_id[*id].depends_on.iter().any(|dependency| {
                        summaries.get(dependency).is_some_and(|summary| {
                            matches!(
                                summary.status,
                                TaskState::Failed | TaskState::Skipped | TaskState::Cancelled
                            )
                        })
                    })
                })
                .cloned()
                .collect();
            for id in blocked {
                let task_id = &task_ids[&id];
                let revision = self
                    .lifecycle_runtime
                    .tasks()
                    .get(task_id)
                    .map(|task| task.revision)
                    .unwrap_or_default();
                let reason = "a collaboration prerequisite did not complete";
                match self.lifecycle_runtime.skip_collaboration_task(
                    &self.run_id,
                    task_id,
                    revision,
                    &OperationId::new(format!("collaboration:{}:{id}:skip", self.run_id)),
                    reason,
                ) {
                    Ok(_) => summaries.insert(
                        id.clone(),
                        collaboration_task_summary(
                            &task_by_id[&id],
                            task_id,
                            TaskState::Skipped,
                            None,
                            Some(TaskFailureClass::NonRetryable),
                            0,
                            0,
                            TokenUsage::default(),
                            None,
                            Some(reason.to_owned()),
                        ),
                    ),
                    Err(error) => {
                        has_unknown = true;
                        needs_manual.push(format!("{id}: failed to persist dependency skip: {error}"));
                        summaries.insert(
                            id.clone(),
                            collaboration_task_summary(
                                &task_by_id[&id],
                                task_id,
                                TaskState::Failed,
                                None,
                                Some(TaskFailureClass::ReconciliationRequired),
                                0,
                                0,
                                TokenUsage::default(),
                                None,
                                Some(error.to_string()),
                            ),
                        )
                    }
                };
                pending.remove(&id);
                has_failure = true;
            }

            let ready: Vec<String> = pending
                .iter()
                .filter(|id| {
                    task_by_id
                        .get(*id)
                        .is_some_and(|task| task.depends_on.iter().all(|dependency| !pending.contains(dependency)))
                })
                .cloned()
                .collect();
            if ready.is_empty() {
                has_failure = true;
                for id in pending.drain() {
                    summaries.insert(
                        id.clone(),
                        collaboration_task_summary(
                            &task_by_id[&id],
                            &task_ids[&id],
                            TaskState::Failed,
                            None,
                            Some(TaskFailureClass::NonRetryable),
                            0,
                            0,
                            TokenUsage::default(),
                            None,
                            Some("dependency graph did not produce an executable task wave".to_owned()),
                        ),
                    );
                }
                break;
            }
            let current_supervisor_round = supervisor_round;
            let supervisor_round_task_ids = ready.clone();
            let mut supervisor_round_retried = Vec::new();
            if strategy == solaris_types::workflow::CollaborationStrategy::Supervisor {
                let payload = json!({
                    "round": current_supervisor_round,
                    "task_ids": supervisor_round_task_ids.clone(),
                    "max_attempts": DIRECT_SUPERVISOR_MAX_ATTEMPTS,
                });
                if let Err(error) = self.lifecycle_runtime.commit_runtime_event(
                    &self.run_id,
                    DurabilityClass::SyncCritical,
                    "supervisor_round_started",
                    Some(self.parent_agent_id.clone()),
                    "supervisor_round_started",
                    payload,
                ) {
                    has_unknown = true;
                    needs_manual.push(format!("supervisor round persistence failed: {error}"));
                }
                supervisor_round = supervisor_round.saturating_add(1);
            }
            let mut specs = Vec::with_capacity(ready.len());
            let wave_started = Instant::now();
            for id in &ready {
                let task = &task_by_id[id];
                let is_independent_reviewer = reviewer_required && id == INDEPENDENT_REVIEWER_TASK_ID;
                let dependency_context = task
                    .depends_on
                    .iter()
                    .filter_map(|dependency| outputs.get(dependency).map(|value| (dependency, value)))
                    .map(|(dependency, value)| format!("Dependency {dependency} output:\n{value}"))
                    .collect::<Vec<_>>();
                let prompt = if dependency_context.is_empty() {
                    task.prompt.clone()
                } else {
                    format!("{}\n\n{}", task.prompt, dependency_context.join("\n\n"))
                };
                let budget = task.resource_budget.clone().unwrap_or_default();
                let max_turns = budget.max_turns.unwrap_or(DEFAULT_SUB_AGENT_MAX_TURNS);
                let max_tokens = budget
                    .max_tokens
                    .unwrap_or(u64::from(DEFAULT_SUB_AGENT_MAX_TOKENS))
                    .min(u64::from(u32::MAX)) as u32;
                let attempt = supervisor_attempts.get(id).copied().unwrap_or_default();
                let mut operation_id = if strategy == solaris_types::workflow::CollaborationStrategy::Supervisor {
                    collaboration_operation_id(&self.run_id, id, attempt)
                } else {
                    collaboration_operation_id(&self.run_id, id, 0)
                };
                let role_key = task.role.clone().unwrap_or_else(|| task.name.clone());
                let stable_task_key = format!("collaboration:{id}");
                // A completed durable outcome is already the answer for this
                // stable task. Reuse it before asking the spawn service for a
                // handle; otherwise `join` would correctly reject a terminal
                // TaskRecord as if it were a live task after a restart.
                let existing_key = ChildAgentKey {
                    run_id: self.run_id.clone(),
                    parent_agent_id: self.parent_agent_id.clone(),
                    role_key: role_key.clone(),
                    stable_task_key: stable_task_key.clone(),
                    spawn_operation_id: operation_id.clone(),
                };
                if let Some((existing_agent, _)) = self.lifecycle_runtime.existing_child_identity(&existing_key)
                    && let Ok(Some((outcome, _))) =
                        self.lifecycle_runtime
                            .agent_outcome_by_identity(&self.run_id, &operation_id, &existing_agent)
                {
                    let retryable_reattach = strategy == solaris_types::workflow::CollaborationStrategy::Supervisor
                        && outcome.failure_class == Some(TaskFailureClass::Retryable)
                        && self
                            .lifecycle_runtime
                            .tasks()
                            .get(&task_ids[id])
                            .is_some_and(|record| record.state == TaskState::Queued);
                    if retryable_reattach {
                        supervisor_attempts.insert(id.clone(), 1);
                        operation_id = collaboration_operation_id(&self.run_id, id, 1);
                    } else {
                        reattached = reattached.saturating_add(1);
                        pending.remove(id);
                        let (state, failure_class) = match outcome.status {
                            AgentOutcomeStatus::Completed => (TaskState::Completed, None),
                            AgentOutcomeStatus::Cancelled => (
                                TaskState::Cancelled,
                                Some(outcome.failure_class.unwrap_or(TaskFailureClass::Cancelled)),
                            ),
                            AgentOutcomeStatus::Failed => (
                                TaskState::Failed,
                                Some(outcome.failure_class.unwrap_or(TaskFailureClass::NonRetryable)),
                            ),
                            AgentOutcomeStatus::OutcomeUnknown => {
                                has_unknown = true;
                                needs_manual.push(id.clone());
                                (TaskState::Failed, Some(TaskFailureClass::OutcomeUnknown))
                            }
                            AgentOutcomeStatus::ReconciliationRequired => {
                                has_unknown = true;
                                needs_manual.push(id.clone());
                                (TaskState::Failed, Some(TaskFailureClass::ReconciliationRequired))
                            }
                        };
                        if failure_class.is_some() {
                            has_failure = true;
                        }
                        let text = outcome.text.clone();
                        if state == TaskState::Completed {
                            outputs.insert(id.clone(), text);
                        }
                        summaries.insert(
                            id.clone(),
                            collaboration_task_summary(
                                task,
                                &task_ids[id],
                                state,
                                Some(existing_agent),
                                failure_class,
                                0,
                                0,
                                outcome.usage,
                                outcome.output,
                                outcome.is_error.then_some(outcome.text),
                            ),
                        );
                        continue;
                    }
                }
                let mut spec = self.fork_spec(
                    SubAgentConfig {
                        name: task.name.clone(),
                        prompt,
                        max_turns,
                        max_tokens,
                        system_prompt: None,
                    },
                    ForkOverrides {
                        inherit_capabilities: !is_independent_reviewer,
                        allowed_tools: if is_independent_reviewer {
                            vec!["Read".to_owned(), "Grep".to_owned(), "Glob".to_owned()]
                        } else {
                            Vec::new()
                        },
                        collaboration: collaboration.clone(),
                        ..ForkOverrides::default()
                    },
                    operation_id,
                    if is_independent_reviewer {
                        PermissionCeiling::plan()
                    } else {
                        PermissionCeiling::unrestricted()
                    },
                );
                if is_independent_reviewer {
                    spec.context_policy = Some("isolated_verification".to_owned());
                    spec.recursion_limit = Some(0);
                }
                spec.task_id = task_ids[id].clone();
                spec.role_key = role_key;
                spec.stable_task_key = stable_task_key;
                spec.expected_task_revision = self
                    .lifecycle_runtime
                    .tasks()
                    .get(&spec.task_id)
                    .map(|task| task.revision);
                spec.resource_budget = budget;
                specs.push((id.clone(), spec));
            }

            let spawn_results = join_all(
                specs
                    .iter()
                    .map(|(_, spec)| AgentSpawnService::spawn(self, spec.clone())),
            )
            .await;
            let mut handles = Vec::new();
            for ((id, _), result) in specs.into_iter().zip(spawn_results) {
                match result {
                    Ok(handle) => {
                        let was_existing = self
                            .lifecycle_runtime
                            .agent_outcome_by_identity(&self.run_id, &handle.operation_id, &handle.agent_id)
                            .ok()
                            .flatten()
                            .is_some();
                        if was_existing {
                            reattached = reattached.saturating_add(1);
                        } else if created_agent_ids.insert(handle.agent_id.clone()) {
                            created = created.saturating_add(1);
                        }
                        handles.push((id, handle));
                    }
                    Err(error) => {
                        let attempt = supervisor_attempts.get(&id).copied().unwrap_or_default();
                        if strategy == solaris_types::workflow::CollaborationStrategy::Supervisor
                            && error.failure_class == TaskFailureClass::Retryable
                            && attempt + 1 < DIRECT_SUPERVISOR_MAX_ATTEMPTS
                        {
                            let next_attempt = attempt + 1;
                            let next_operation_id = collaboration_operation_id(&self.run_id, &id, next_attempt);
                            if let Err(record_error) = self.record_supervisor_retry(
                                &id,
                                attempt,
                                error.failure_class,
                                &next_operation_id,
                                &error.message,
                            ) {
                                has_unknown = true;
                                needs_manual.push(format!("{id}: failed to persist Supervisor retry: {record_error}"));
                            } else {
                                supervisor_attempts.insert(id.clone(), next_attempt);
                                supervisor_round_retried.push(id.clone());
                                continue;
                            }
                        }
                        has_failure = true;
                        summaries.insert(
                            id.clone(),
                            collaboration_task_summary(
                                &task_by_id[&id],
                                &task_ids[&id],
                                TaskState::Failed,
                                None,
                                Some(error.failure_class),
                                supervisor_attempts.get(&id).copied().unwrap_or_default(),
                                wave_started.elapsed().as_millis() as u64,
                                TokenUsage::default(),
                                None,
                                Some(error.message),
                            ),
                        );
                        pending.remove(&id);
                    }
                }
            }
            peak_active = peak_active.max(handles.len() as u32);
            if handles.is_empty() {
                if strategy == solaris_types::workflow::CollaborationStrategy::Supervisor
                    && let Err(error) = self.record_supervisor_round_completed(
                        current_supervisor_round,
                        &supervisor_round_task_ids,
                        &supervisor_round_retried,
                    )
                {
                    has_unknown = true;
                    needs_manual.push(format!("supervisor round persistence failed: {error}"));
                }
                continue;
            }
            if let Err(error) = self.prepare_collaboration_handles(
                &handles
                    .iter()
                    .map(|(_, handle)| (handle.clone(), false))
                    .collect::<Vec<_>>(),
            ) {
                has_failure = true;
                has_unknown = true;
                needs_manual.push(format!("collaboration batch preparation failed: {error}"));
                for (id, handle) in handles {
                    summaries.insert(
                        id.clone(),
                        collaboration_task_summary(
                            &task_by_id[&id],
                            &task_ids[&id],
                            TaskState::Failed,
                            Some(handle.agent_id),
                            Some(TaskFailureClass::ReconciliationRequired),
                            0,
                            wave_started.elapsed().as_millis() as u64,
                            TokenUsage::default(),
                            None,
                            Some(error.clone()),
                        ),
                    );
                    pending.remove(&id);
                }
                if strategy == solaris_types::workflow::CollaborationStrategy::Supervisor
                    && let Err(error) = self.record_supervisor_round_completed(
                        current_supervisor_round,
                        &supervisor_round_task_ids,
                        &supervisor_round_retried,
                    )
                {
                    has_unknown = true;
                    needs_manual.push(format!("supervisor round persistence failed: {error}"));
                }
                continue;
            }
            let joined = join_all(handles.iter().map(|(_, handle)| AgentSpawnService::join(self, handle))).await;
            for ((id, handle), result) in handles.into_iter().zip(joined) {
                pending.remove(&id);
                let elapsed = wave_started.elapsed().as_millis() as u64;
                let attempt = supervisor_attempts.get(&id).copied().unwrap_or_default();
                match result {
                    Ok(outcome) => {
                        let (state, failure_class) = match outcome.status {
                            AgentOutcomeStatus::Completed => (TaskState::Completed, None),
                            AgentOutcomeStatus::Cancelled => (
                                TaskState::Cancelled,
                                Some(outcome.failure_class.unwrap_or(TaskFailureClass::Cancelled)),
                            ),
                            AgentOutcomeStatus::Failed => (
                                TaskState::Failed,
                                Some(outcome.failure_class.unwrap_or(TaskFailureClass::NonRetryable)),
                            ),
                            AgentOutcomeStatus::OutcomeUnknown => {
                                has_unknown = true;
                                needs_manual.push(id.clone());
                                (TaskState::Failed, Some(TaskFailureClass::OutcomeUnknown))
                            }
                            AgentOutcomeStatus::ReconciliationRequired => {
                                has_unknown = true;
                                needs_manual.push(id.clone());
                                (TaskState::Failed, Some(TaskFailureClass::ReconciliationRequired))
                            }
                        };
                        let should_retry = strategy == solaris_types::workflow::CollaborationStrategy::Supervisor
                            && failure_class == Some(TaskFailureClass::Retryable)
                            && attempt + 1 < DIRECT_SUPERVISOR_MAX_ATTEMPTS;
                        if failure_class.is_some() && !should_retry {
                            has_failure = true;
                        }
                        let text = outcome
                            .output
                            .get("text")
                            .and_then(serde_json::Value::as_str)
                            .or(outcome.error.as_deref())
                            .unwrap_or_default()
                            .to_owned();
                        if state == TaskState::Completed {
                            outputs.insert(id.clone(), text.clone());
                        }
                        let task_revision = self
                            .lifecycle_runtime
                            .tasks()
                            .get(&handle.task_id)
                            .map(|task| task.revision);
                        let mut settled_revision = None;
                        if let Some(revision) = task_revision {
                            match self.lifecycle_runtime.settle_collaboration_task(
                                &self.run_id,
                                &handle.task_id,
                                &handle.agent_id,
                                revision,
                                &OperationId::new(format!("{}:settle", handle.operation_id)),
                                TaskSettlement {
                                    state,
                                    outcome_ref: Some(format!("agent-outcome:{}", handle.operation_id)),
                                    failure_class,
                                },
                            ) {
                                Ok(new_revision) => settled_revision = Some(new_revision),
                                Err(error) => {
                                    has_unknown = true;
                                    has_failure = true;
                                    needs_manual.push(format!("{id}: {error}"));
                                }
                            }
                        }
                        if should_retry {
                            let next_attempt = attempt + 1;
                            let next_operation_id = collaboration_operation_id(&self.run_id, &id, next_attempt);
                            let retry_reason = outcome
                                .error
                                .as_deref()
                                .unwrap_or("Supervisor received a Retryable child failure");
                            let retry_result = settled_revision
                                .ok_or_else(|| "failed to settle Retryable Supervisor task".to_owned())
                                .and_then(|revision| {
                                    self.record_supervisor_retry(
                                        &id,
                                        attempt,
                                        TaskFailureClass::Retryable,
                                        &next_operation_id,
                                        retry_reason,
                                    )?;
                                    self.lifecycle_runtime
                                        .requeue_supervisor_task(
                                            &self.run_id,
                                            &handle.task_id,
                                            &self.parent_agent_id,
                                            revision,
                                            &OperationId::new(format!(
                                                "{}:supervisor-requeue:{next_attempt}",
                                                handle.operation_id
                                            )),
                                            retry_reason,
                                        )
                                        .map(|_| ())
                                        .map_err(|error| error.to_string())
                                });
                            match retry_result {
                                Ok(()) => {
                                    supervisor_attempts.insert(id.clone(), next_attempt);
                                    supervisor_round_retried.push(id.clone());
                                    pending.insert(id);
                                    continue;
                                }
                                Err(error) => {
                                    has_unknown = true;
                                    has_failure = true;
                                    needs_manual.push(format!("{id}: Supervisor retry failed: {error}"));
                                }
                            }
                        }
                        summaries.insert(
                            id.clone(),
                            collaboration_task_summary(
                                &task_by_id[&id],
                                &handle.task_id,
                                state,
                                Some(handle.agent_id),
                                failure_class,
                                attempt,
                                elapsed,
                                outcome.usage,
                                Some(outcome.output),
                                outcome.error,
                            ),
                        );
                    }
                    Err(error) => {
                        has_failure = true;
                        has_unknown = true;
                        needs_manual.push(id.clone());
                        summaries.insert(
                            id.clone(),
                            collaboration_task_summary(
                                &task_by_id[&id],
                                &handle.task_id,
                                TaskState::Failed,
                                Some(handle.agent_id),
                                Some(TaskFailureClass::ReconciliationRequired),
                                0,
                                elapsed,
                                TokenUsage::default(),
                                None,
                                Some(error),
                            ),
                        );
                    }
                }
            }
            if strategy == solaris_types::workflow::CollaborationStrategy::Supervisor
                && let Err(error) = self.record_supervisor_round_completed(
                    current_supervisor_round,
                    &supervisor_round_task_ids,
                    &supervisor_round_retried,
                )
            {
                has_unknown = true;
                needs_manual.push(format!("supervisor round persistence failed: {error}"));
            }
        }

        let mut task_summaries: Vec<_> = task_by_id
            .iter()
            .map(|(id, task)| {
                summaries.remove(id).unwrap_or_else(|| {
                    collaboration_task_summary(
                        task,
                        &task_ids[id],
                        TaskState::Queued,
                        None,
                        None,
                        0,
                        0,
                        TokenUsage::default(),
                        None,
                        None,
                    )
                })
            })
            .collect();
        task_summaries.sort_by(|left, right| left.id.cmp(&right.id));
        let usage = self.resources.usage();
        let status = if has_unknown {
            CollaborationRunStatus::OutcomeUnknown
        } else if has_failure {
            CollaborationRunStatus::Failed
        } else {
            CollaborationRunStatus::Completed
        };
        let summary = format!(
            "collaboration {:?}: {} task(s), strategy={}, elapsed={} ms",
            status,
            task_summaries.len(),
            strategy,
            started.elapsed().as_millis()
        );
        CollaborationRunSummary {
            status,
            summary,
            next_actions: if has_unknown {
                vec!["人工核验 outcome_unknown 或协调失败的任务".to_owned()]
            } else {
                Vec::new()
            },
            artifacts: Vec::new(),
            tasks: task_summaries,
            created,
            reattached,
            queued: 0,
            peak_active,
            uncached_input_tokens: usage.uncached_input_tokens,
            cache_read_tokens: usage.cache_read_tokens,
            cache_write_tokens: usage.cache_creation_tokens,
            output_tokens: usage.output_tokens,
            tool_calls: usage.tool_calls,
            useful_call_rate: usage.useful_call_rate,
            duplicate_call_rate: usage.duplicate_call_rate,
            outcome_unknown: has_unknown,
            needs_manual_verification: needs_manual,
        }
    }

    /// Spawn multiple sub-agents. Every child shares the same Run-wide
    /// ResourceManager; calls above the active-agent limit wait on the shared
    /// permit rather than allocating a second scheduler budget.
    pub async fn spawn_parallel(&self, sub_configs: Vec<SubAgentConfig>) -> Vec<SubAgentResult> {
        let mut join_set = JoinSet::new();
        for (index, config) in sub_configs.into_iter().enumerate() {
            let spawner = self.clone_for_spawn();
            join_set.spawn(async move { (index, spawner.spawn_one(config).await) });
        }
        let mut results = Vec::new();
        while let Some(joined) = join_set.join_next().await {
            match joined {
                Ok(result) => results.push(result),
                Err(error) => results.push((usize::MAX, spawn_error("unknown", format!("Task join error: {error}")))),
            }
        }
        results.sort_by_key(|(index, _)| *index);
        results.into_iter().map(|(_, result)| result).collect()
    }
    fn clone_for_spawn(&self) -> Self {
        Self {
            provider: self.provider.clone(),
            base_config: self.base_config.clone(),
            cwd: self.cwd.clone(),
            runtime_env: self.runtime_env.clone(),
            permission_context: self.permission_context.clone(),
            lifecycle_runtime: Arc::clone(&self.lifecycle_runtime),
            run_id: self.run_id.clone(),
            parent_agent_id: self.parent_agent_id.clone(),
            capability_blueprint: self.capability_blueprint.clone(),
            spawn_depth: self.spawn_depth,
            resources: Arc::clone(&self.resources),
            operation_locks: Arc::clone(&self.operation_locks),
            host_approval: Arc::clone(&self.host_approval),
            spawn_control: Arc::clone(&self.spawn_control),
            multi_agent_policy: Arc::clone(&self.multi_agent_policy),
            default_strategy: Arc::clone(&self.default_strategy),
            max_tasks_per_run: self.max_tasks_per_run,
            session_fences: Arc::clone(&self.session_fences),
            remaining_spawn_depth: self.remaining_spawn_depth,
            read_only_evidence_index: Arc::clone(&self.read_only_evidence_index),
        }
    }

    fn clone_for_child(&self, child_agent_id: AgentId, child_permissions: PermissionContext) -> Self {
        Self {
            provider: self.provider.clone(),
            base_config: self.base_config.clone(),
            cwd: self.cwd.clone(),
            runtime_env: self.runtime_env.clone(),
            permission_context: child_permissions,
            lifecycle_runtime: Arc::clone(&self.lifecycle_runtime),
            run_id: self.run_id.clone(),
            parent_agent_id: child_agent_id,
            capability_blueprint: self.capability_blueprint.clone(),
            spawn_depth: self.spawn_depth.saturating_add(1),
            resources: Arc::clone(&self.resources),
            operation_locks: Arc::clone(&self.operation_locks),
            host_approval: Arc::clone(&self.host_approval),
            spawn_control: Arc::clone(&self.spawn_control),
            multi_agent_policy: Arc::clone(&self.multi_agent_policy),
            default_strategy: Arc::clone(&self.default_strategy),
            max_tasks_per_run: self.max_tasks_per_run,
            session_fences: Arc::clone(&self.session_fences),
            remaining_spawn_depth: self.remaining_spawn_depth,
            read_only_evidence_index: Arc::clone(&self.read_only_evidence_index),
        }
    }
}

#[cfg(test)]
#[path = "spawner_test.rs"]
mod spawner_test;
