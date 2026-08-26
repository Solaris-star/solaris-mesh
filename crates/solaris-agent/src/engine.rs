use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, RwLock};

use crate::cache_diagnostics::CacheBreakDetector;
use crate::commands::{CommandContext, CommandRegistry, CommandResult, SlashCommand, default_registry};
use crate::compact::state::CompactState;
use crate::confirm::ToolConfirmer;
use crate::error::AgentError;
use crate::execution_context::{EffectExecutionContext, build_environment_snapshot};
use crate::hook_diagnostics::log_stop_hook_output;
use crate::orchestration::EffectHookExecutor;
use crate::output::OutputSink;
use crate::permission_engine::PermissionContext;
use crate::plan::state::PlanState;
use crate::resource_manager::ResourceManager;
use crate::runtime_ledger::InMemoryRuntimeLedger;
use crate::session::{Session, SessionManager};
use crate::stream::StreamOutcome;
use crate::tool_call::{
    DEFAULT_MAX_TOOL_CALL_FAILURE, DEFAULT_MAX_TOOL_CALL_MALFORMED, ToolCallFailureFingerprint,
    ToolCallMalformedFingerprint,
};
use anyhow::Error as AnyhowError;
use serde_json::{Value, json};
use solaris_compact::CompactLevel;
use solaris_config::compact::CompactConfig;
use solaris_config::compat::ProviderCompat;
use solaris_config::config::Config;
use solaris_config::hooks::HookEngine;
use solaris_protocol::ToolApprovalManager;
use solaris_protocol::writer::ProtocolEmitter;
use solaris_providers::provider::{LlmProvider, create_provider};
use solaris_tools::registry::ToolRegistry;
use solaris_types::config::{ConfigUpdateOutcome, RuntimeConfigUpdate};
use solaris_types::identity::{AgentId, RunId};
use solaris_types::llm::ThinkingConfig;
use solaris_types::message::{ContentBlock, Message, Role, StopReason, TokenUsage};
use solaris_types::permission::{ExecutionBoundary, PermissionCeiling, PermissionMode};
use solaris_types::provider_contract::ProviderNativeMetadata;
use solaris_types::run_preset::Intensity;
use solaris_types::skill_types::ContextModifier;
use solaris_types::spawner::AgentOutcomeStatus;
use solaris_types::workflow::MultiAgentPolicy;
use tracing::{error, info};
use uuid::Uuid;

pub use self::runtime_configuration::RuntimeConfigurationView;
use self::runtime_configuration::{RuntimeConfigurationState, runtime_configuration_state, thinking_budget};

#[derive(Debug)]
pub struct AgentResult {
    pub status: AgentOutcomeStatus,
    pub text: String,
    pub stop_reason: StopReason,
    pub usage: TokenUsage,
    pub turns: usize,
}

pub struct AgentEngine {
    // Provider request configuration.
    /// Shared LLM provider used to issue model requests.
    provider: Arc<dyn LlmProvider>,
    /// Stable configured provider name or alias used in durable run identity.
    provider_label: String,
    provider_effect: solaris_types::effect::EffectDescriptor,
    /// Resolved provider compatibility and capability settings.
    compat: ProviderCompat,
    /// Optional provider-neutral thinking configuration for model requests.
    thinking: Option<ThinkingConfig>,
    /// Base system prompt sent with each model request.
    system_prompt: String,
    /// Active model identifier used for provider requests.
    model: String,
    /// Persisted reasoning effort, updated by skill context modifiers.
    /// Carried into each model turn's LlmRequest.reasoning_effort.
    reasoning_effort: Option<String>,
    /// Shared source read by host snapshots and configuration events.
    runtime_configuration: Arc<RwLock<RuntimeConfigurationState>>,
    multi_agent_policy: Arc<RwLock<MultiAgentPolicy>>,
    resources: Arc<ResourceManager>,

    // Conversation and run state.
    /// Conversation history used to build the next provider request.
    messages: Vec<Message>,
    /// Cumulative token usage across the active session/run.
    total_usage: TokenUsage,
    /// Output message ID for the currently streaming run.
    msg_id: String,
    /// Maximum output tokens requested from the provider per turn.
    max_tokens: Option<u32>,
    /// Optional cap on counted model turns within a single run.
    max_turns_per_run: Option<usize>,
    /// Consecutive malformed tool-call round limit before aborting.
    max_tool_call_malformed_turns: usize,
    /// Consecutive failed tool-call round limit before aborting.
    max_tool_call_failure_turns: usize,

    // Tool execution policy.
    /// Registry of tools available to the engine.
    tools: ToolRegistry,
    /// Shared tool confirmer used for interactive approval decisions.
    confirmer: Arc<Mutex<ToolConfirmer>>,
    /// Shared permission posture inherited by child Agent runtimes.
    permission_context: PermissionContext,
    /// Durable effect/operation context for permission leases, revalidation and ledger writes.
    execution_context: Option<EffectExecutionContext>,
    /// Tool names currently allowed without additional approval.
    allow_list: Vec<String>,
    /// Optional hook engine for lifecycle and tool hooks.
    hooks: Option<HookEngine>,

    // Session persistence.
    /// Optional session manager used when persistence is enabled.
    session_manager: Option<SessionManager>,
    /// Active session record updated as the conversation progresses.
    current_session: Option<Session>,

    // Output and host protocol integration.
    /// Sink for user-visible and host-visible output events.
    output: Arc<dyn OutputSink>,
    /// Optional host approval manager for JSON stream tool approvals.
    approval_manager: Option<Arc<ToolApprovalManager>>,
    /// Optional protocol emitter used to send structured host events.
    protocol_writer: Option<Arc<dyn ProtocolEmitter>>,

    // Compaction and plan-mode state.
    /// Static compaction thresholds, flags, and sizing configuration.
    compact_config: CompactConfig,
    /// Runtime compaction watermark and circuit-breaker state.
    compact_state: CompactState,
    /// Active compaction strategy level.
    compact_level: CompactLevel,
    /// Whether TOON-formatted compaction output is enabled.
    toon_enabled: bool,
    /// Runtime plan mode state and restoration data.
    plan_state: PlanState,
    /// Shared flag read by EnterPlanMode/ExitPlanMode tools to validate transitions.
    /// Updated by the engine when processing PlanModeTransition modifiers.
    plan_active_flag: Option<Arc<AtomicBool>>,
    plan_mode_disable_handler: Option<plan_lifecycle::PlanModeDisableHandler>,

    // Diagnostics and command handling.
    /// Prompt cache break detector for diagnostics.
    cache_detector: CacheBreakDetector,
    /// Slash command registry used before normal model execution.
    commands: CommandRegistry,
}

fn standalone_execution_context(
    config: &Config,
    tools: &ToolRegistry,
    permissions: &PermissionContext,
    cwd: &std::path::Path,
    resumed_run_id: Option<&str>,
) -> EffectExecutionContext {
    let boundary_root = cwd
        .canonicalize()
        .unwrap_or_else(|_| cwd.to_path_buf())
        .to_string_lossy()
        .into_owned();
    permissions.set_boundary(ExecutionBoundary::workspace(boundary_root));
    let provider_effect = crate::bootstrap::provider_effect_descriptor_for_config(config, cwd);
    permissions.allow_configured_effect_for("config:provider", "ProviderRequest", &provider_effect);
    permissions.allow_configured_effect_for("config:provider", "AutoCompact", &provider_effect);
    permissions.set_mode(permissions.mode());
    let run_id = resumed_run_id
        .map(RunId::from)
        .unwrap_or_else(|| RunId::new(format!("run-{}", Uuid::now_v7())));
    let agent_id = AgentId::new(format!("agent:root:{}", run_id.as_str()));
    EffectExecutionContext::new(
        run_id,
        agent_id,
        Arc::new(InMemoryRuntimeLedger::default()),
        permissions.clone(),
        build_environment_snapshot(config, tools, permissions),
    )
}

impl AgentEngine {
    pub fn new(config: Config, tools: ToolRegistry, output: Arc<dyn OutputSink>, cwd: PathBuf) -> Self {
        let provider = create_provider(&config);
        Self::new_with_provider(provider, config, tools, output, cwd)
    }

    /// Create an engine with an externally-provided provider (for sub-agent sharing)
    pub fn new_with_provider(
        provider: Arc<dyn LlmProvider>,
        config: Config,
        tools: ToolRegistry,
        output: Arc<dyn OutputSink>,
        cwd: PathBuf,
    ) -> Self {
        Self::new_with_provider_and_env(provider, config, tools, output, cwd, Vec::new())
    }

    pub fn new_with_provider_and_env(
        provider: Arc<dyn LlmProvider>,
        config: Config,
        tools: ToolRegistry,
        output: Arc<dyn OutputSink>,
        cwd: PathBuf,
        runtime_env: Vec<(String, String)>,
    ) -> Self {
        let system_prompt = config.system_prompt.clone().unwrap_or_default();
        let provider_effect = crate::bootstrap::provider_effect_descriptor_for_config(&config, &cwd);
        let permission_context = PermissionContext::from_auto_approve(config.tools.auto_approve);
        let resources = ResourceManager::new(Default::default());
        let runtime_configuration = runtime_configuration_state(
            &config.provider_label,
            &config.model,
            permission_context.mode(),
            &config.thinking,
            config.compact.compaction,
        );
        let configured_multi_agent_policy = config.multi_agent.policy;
        let configured_max_active_agents = config.multi_agent.max_active_agents;
        {
            let mut state = runtime_configuration.write().unwrap_or_else(|error| error.into_inner());
            state.configuration.multi_agent_policy = configured_multi_agent_policy;
            state.configuration.max_active_agents = configured_max_active_agents;
            state.configuration.effective_max_active_agents =
                configured_max_active_agents.unwrap_or_else(|| state.configuration.effective_max_active_agents);
        }
        let execution_context = standalone_execution_context(&config, &tools, &permission_context, &cwd, None);
        let mut hooks = HookEngine::new_with_env(config.hooks.clone(), cwd.clone(), runtime_env);
        hooks.set_executor(Arc::new(EffectHookExecutor::new(execution_context.clone())));
        let allow_list = config.tools.allow_list.clone();
        let confirmer = ToolConfirmer::new(config.tools.auto_approve, allow_list.clone());

        let session_manager = if config.session.enabled {
            Some(SessionManager::new(
                config.session.directory.clone().into(),
                config.session.max_sessions,
            ))
        } else {
            None
        };

        let compact_config = config.compact.clone();

        let engine = Self {
            provider,
            provider_label: config.provider_label.clone(),
            provider_effect,
            model: config.model,
            max_tokens: config.max_tokens,
            thinking: config.thinking,
            compat: config.compat.clone(),
            system_prompt,
            reasoning_effort: None,
            runtime_configuration,
            multi_agent_policy: Arc::new(RwLock::new(configured_multi_agent_policy)),
            resources,
            messages: Vec::new(),
            total_usage: TokenUsage::default(),
            msg_id: String::new(),
            max_turns_per_run: config.max_turns,
            max_tool_call_malformed_turns: config
                .max_tool_call_malformed_turns
                .unwrap_or(DEFAULT_MAX_TOOL_CALL_MALFORMED),
            max_tool_call_failure_turns: config
                .max_tool_call_failure_turns
                .unwrap_or(DEFAULT_MAX_TOOL_CALL_FAILURE),
            tools,
            confirmer: Arc::new(Mutex::new(confirmer)),
            permission_context,
            execution_context: Some(execution_context),
            allow_list,
            hooks: Some(hooks),
            session_manager,
            current_session: None,
            output,
            approval_manager: None,
            protocol_writer: None,
            compact_config,
            compact_state: CompactState::new(),
            compact_level: config.compact.compaction,
            toon_enabled: config.compact.toon,
            plan_state: PlanState::default(),
            plan_active_flag: None,
            plan_mode_disable_handler: None,
            cache_detector: CacheBreakDetector::new(),
            commands: default_registry(),
        };
        engine.refresh_runtime_configuration();
        engine
    }

    /// Create from a resumed session
    pub fn resume(
        config: Config,
        tools: ToolRegistry,
        output: Arc<dyn OutputSink>,
        session: Session,
        cwd: PathBuf,
    ) -> Self {
        let provider = create_provider(&config);
        Self::resume_with_provider(provider, config, tools, output, session, cwd)
    }

    /// Create from a resumed session with an externally-provided provider
    pub fn resume_with_provider(
        provider: Arc<dyn LlmProvider>,
        config: Config,
        tools: ToolRegistry,
        output: Arc<dyn OutputSink>,
        session: Session,
        cwd: PathBuf,
    ) -> Self {
        Self::resume_with_provider_and_env(provider, config, tools, output, session, cwd, Vec::new())
    }

    pub fn resume_with_provider_and_env(
        provider: Arc<dyn LlmProvider>,
        config: Config,
        tools: ToolRegistry,
        output: Arc<dyn OutputSink>,
        session: Session,
        cwd: PathBuf,
        runtime_env: Vec<(String, String)>,
    ) -> Self {
        let mut session = session;
        // A stored model is valid only for the exact provider identity that selected it.
        // Missing or changed provider identity falls back to the current configured pair.
        let same_provider = !session.provider.trim().is_empty() && session.provider == config.provider_label;
        let resumed_model = if same_provider && !session.model.trim().is_empty() {
            session.model.clone()
        } else {
            config.model.clone()
        };
        session.provider.clone_from(&config.provider_label);
        session.model.clone_from(&resumed_model);
        let restored_runtime = session.runtime_state.clone();
        let system_prompt = config.system_prompt.clone().unwrap_or_default();
        let provider_effect = crate::bootstrap::provider_effect_descriptor_for_config(&config, &cwd);
        let permission_context = PermissionContext::from_auto_approve(config.tools.auto_approve);
        let resources = ResourceManager::new(Default::default());
        let runtime_configuration = runtime_configuration_state(
            &config.provider_label,
            &resumed_model,
            permission_context.mode(),
            &config.thinking,
            config.compact.compaction,
        );
        let configured_multi_agent_policy = config.multi_agent.policy;
        let configured_max_active_agents = config.multi_agent.max_active_agents;
        {
            let mut state = runtime_configuration.write().unwrap_or_else(|error| error.into_inner());
            state.configuration.multi_agent_policy = configured_multi_agent_policy;
            state.configuration.max_active_agents = configured_max_active_agents;
            state.configuration.effective_max_active_agents =
                configured_max_active_agents.unwrap_or_else(|| state.configuration.effective_max_active_agents);
        }
        let execution_context =
            standalone_execution_context(&config, &tools, &permission_context, &cwd, session.run_id.as_deref());
        let hook_config = restored_runtime
            .as_ref()
            .map(|state| state.hooks.clone())
            .unwrap_or_else(|| config.hooks.clone());
        let mut hooks = HookEngine::new_with_env(hook_config, cwd.clone(), runtime_env);
        hooks.set_executor(Arc::new(EffectHookExecutor::new(execution_context.clone())));
        let allow_list = restored_runtime
            .as_ref()
            .map(|state| state.allow_list.clone())
            .unwrap_or_else(|| config.tools.allow_list.clone());
        let confirmer = ToolConfirmer::new(config.tools.auto_approve, allow_list.clone());

        let session_manager = if config.session.enabled {
            Some(SessionManager::new(
                config.session.directory.clone().into(),
                config.session.max_sessions,
            ))
        } else {
            None
        };

        let reasoning_effort = restored_runtime
            .as_ref()
            .and_then(|state| state.reasoning_effort.clone());
        let plan_state = restored_runtime
            .as_ref()
            .map(|state| PlanState {
                is_active: state.plan_active,
                pre_plan_allow_list: state.pre_plan_allow_list.clone(),
            })
            .unwrap_or_default();
        let compact_config = config.compact.clone();

        let engine = Self {
            provider,
            provider_label: config.provider_label.clone(),
            provider_effect,
            model: resumed_model,
            max_tokens: config.max_tokens,
            thinking: config.thinking,
            compat: config.compat.clone(),
            system_prompt,
            reasoning_effort,
            runtime_configuration,
            multi_agent_policy: Arc::new(RwLock::new(configured_multi_agent_policy)),
            resources,
            messages: session.messages.clone(),
            total_usage: session.total_usage.clone(),
            msg_id: String::new(),
            max_turns_per_run: config.max_turns,
            max_tool_call_malformed_turns: config
                .max_tool_call_malformed_turns
                .unwrap_or(DEFAULT_MAX_TOOL_CALL_MALFORMED),
            max_tool_call_failure_turns: config
                .max_tool_call_failure_turns
                .unwrap_or(DEFAULT_MAX_TOOL_CALL_FAILURE),
            tools,
            confirmer: Arc::new(Mutex::new(confirmer)),
            permission_context,
            execution_context: Some(execution_context),
            allow_list,
            hooks: Some(hooks),
            session_manager,
            current_session: Some(session),
            output,
            approval_manager: None,
            protocol_writer: None,
            compact_config,
            compact_state: CompactState::new(),
            compact_level: config.compact.compaction,
            toon_enabled: config.compact.toon,
            plan_state,
            plan_active_flag: None,
            plan_mode_disable_handler: None,
            cache_detector: CacheBreakDetector::new(),
            commands: default_registry(),
        };
        engine.refresh_runtime_configuration();
        engine
    }

    pub fn compaction_level(&self) -> CompactLevel {
        self.compact_level
    }

    /// Get a reference to the shared provider
    pub fn provider(&self) -> &Arc<dyn LlmProvider> {
        &self.provider
    }

    pub fn provider_label(&self) -> &str {
        &self.provider_label
    }

    pub fn model(&self) -> &str {
        &self.model
    }

    /// Return a read-only handle shared with first-party host integrations.
    pub fn runtime_configuration_view(&self) -> RuntimeConfigurationView {
        self.runtime_configuration
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .shared_plan_active
            .clone_from(&self.plan_active_flag);
        RuntimeConfigurationView::new(Arc::clone(&self.runtime_configuration), self.permission_context.clone())
    }

    /// Get a reference to the resolved compat settings
    pub fn compat(&self) -> &ProviderCompat {
        &self.compat
    }

    pub fn tool_names(&self) -> Vec<String> {
        self.tools.tool_names()
    }

    pub fn registry_mut(&mut self) -> &mut ToolRegistry {
        &mut self.tools
    }

    /// Get the current session ID (if sessions are enabled and initialized)
    pub fn current_session_id(&self) -> Option<String> {
        self.current_session.as_ref().map(|s| s.id.clone())
    }

    /// Import Host-owned history into a newly created, empty Mesh session.
    /// Resumed sessions reject imports so reconnects cannot duplicate context.
    pub fn import_history(&mut self, messages: Vec<Message>) -> Result<usize, String> {
        if messages.is_empty() {
            return Ok(0);
        }
        if !self.messages.is_empty() {
            return Err("session already contains history; refusing duplicate import".to_owned());
        }
        self.ensure_session_lease().map_err(|error| error.to_string())?;
        let count = messages.len();
        let previous_session_messages = self.current_session.as_ref().map(|session| session.messages.clone());
        self.messages = messages;
        if let Err(error) = self.save_session() {
            self.messages.clear();
            if let (Some(session), Some(previous_messages)) = (&mut self.current_session, previous_session_messages) {
                session.messages = previous_messages;
            }
            return Err(error.to_string());
        }
        Ok(count)
    }

    /// Commit a Host-routed Workflow turn to the same conversation history as ordinary model turns.
    ///
    /// Required Workflows run outside `AgentEngine::run`, so the Host must call this once the
    /// Workflow has a durable terminal output. The metadata keeps the typed Workflow identity
    /// available without making later providers understand Mesh internals.
    pub fn commit_workflow_turn(
        &mut self,
        user_content: &str,
        workflow_run_id: &RunId,
        workflow_id: &str,
        workflow_version: &str,
        output: &Value,
    ) -> Result<bool, String> {
        if self.messages.iter().any(|message| {
            message.role == Role::Assistant
                && message
                    .provider_metadata
                    .get("solaris.workflow")
                    .and_then(|metadata| metadata.get("run_id"))
                    .and_then(Value::as_str)
                    == Some(workflow_run_id.as_str())
        }) {
            return Ok(false);
        }
        self.ensure_session_lease().map_err(|error| error.to_string())?;
        let previous_len = self.messages.len();
        self.messages.push(Message::now(
            Role::User,
            vec![ContentBlock::Text {
                text: user_content.to_owned(),
            }],
        ));
        let text = output
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| serde_json::to_string_pretty(output).unwrap_or_else(|_| output.to_string()));
        let mut assistant = Message::now(Role::Assistant, vec![ContentBlock::Text { text }]);
        assistant.provider_metadata.insert(
            "solaris.workflow".to_owned(),
            json!({
                "run_id": workflow_run_id,
                "workflow_id": workflow_id,
                "workflow_version": workflow_version,
                "output": output,
            }),
        );
        self.messages.push(assistant);
        if let Err(error) = self.persist_session_state() {
            self.messages.truncate(previous_len);
            if let Some(session) = &mut self.current_session {
                session.messages.truncate(previous_len);
            }
            return Err(error);
        }
        Ok(true)
    }

    /// Get a reference to the output sink
    pub fn output(&self) -> &dyn OutputSink {
        self.output.as_ref()
    }

    pub fn set_approval_manager(&mut self, mgr: Arc<ToolApprovalManager>) {
        self.approval_manager = Some(mgr);
    }

    pub fn set_protocol_writer(&mut self, writer: Arc<dyn ProtocolEmitter>) {
        self.protocol_writer = Some(writer);
    }

    pub(crate) fn set_execution_context(&mut self, context: EffectExecutionContext) {
        self.permission_context = context.permissions().clone();
        if let Some(hooks) = &mut self.hooks {
            hooks.set_executor(Arc::new(EffectHookExecutor::new(context.clone())));
        }
        self.execution_context = Some(context);
        self.refresh_runtime_configuration();
    }

    pub fn execution_context(&self) -> Option<&EffectExecutionContext> {
        self.execution_context.as_ref()
    }

    pub fn refresh_execution_environment(&self, plugins: Vec<solaris_types::plugin::ImplementationIdentity>) {
        if let Some(context) = &self.execution_context {
            let current = context.environment();
            let refreshed =
                crate::execution_context::refresh_environment_tools_and_plugins(&current, &self.tools, plugins);
            context.set_environment(refreshed);
        }
    }

    pub(crate) fn set_permission_context(&mut self, context: PermissionContext) {
        if let Some(execution_context) = &mut self.execution_context {
            let shared_context = execution_context.permissions().clone();
            shared_context.replace_with(&context);
            execution_context.set_permissions(shared_context.clone());
            if let Some(hooks) = &mut self.hooks {
                hooks.set_executor(Arc::new(EffectHookExecutor::new(execution_context.clone())));
            }
            self.permission_context = shared_context;
        } else {
            self.permission_context = context;
        }
        self.refresh_runtime_configuration();
    }

    pub fn permission_context(&self) -> PermissionContext {
        self.permission_context.clone()
    }

    pub fn set_permission_mode(&mut self, mode: PermissionMode) {
        if mode == PermissionMode::Plan && self.permission_context.mode() != PermissionMode::Plan {
            self.disable_mcp_for_plan();
        }
        self.permission_context.set_mode(mode);
        self.refresh_runtime_configuration();
    }

    pub fn permission_mode(&self) -> PermissionMode {
        self.permission_context.mode()
    }

    pub fn set_permission_ceiling(&self, ceiling: PermissionCeiling) {
        self.permission_context.set_ceiling(ceiling);
    }

    pub fn permission_ceiling(&self) -> PermissionCeiling {
        self.permission_context.ceiling()
    }

    pub fn set_interactive_confirmation(&mut self, interactive: bool) {
        if let Ok(mut confirmer) = self.confirmer.lock() {
            confirmer.set_interactive(interactive);
        }
    }

    /// Set the initial reasoning effort override (used by sub-agents spawned with an effort override).
    pub fn set_initial_reasoning_effort(&mut self, effort: Option<String>) {
        self.reasoning_effort = effort;
        self.refresh_runtime_configuration();
    }

    /// Apply a user-selected intensity and its provider-supported effort.
    ///
    /// The selected preset remains visible even when the provider supports no
    /// effort parameter or requires a fallback to a lower advertised level.
    pub fn apply_intensity(&mut self, intensity: Intensity) {
        let preset = crate::run_preset::resolve_run_preset(intensity, self.compat.effort_levels());
        self.reasoning_effort = preset.reasoning_effort;
        *self
            .multi_agent_policy
            .write()
            .unwrap_or_else(|error| error.into_inner()) = preset.multi_agent_policy;
        let mut configuration = self
            .runtime_configuration
            .write()
            .unwrap_or_else(|error| error.into_inner());
        configuration.configuration.selected_intensity = intensity;
        configuration.configuration.multi_agent_policy = preset.multi_agent_policy;
        configuration.configuration.effective_effort = self.reasoning_effort.clone();
    }

    pub(crate) fn set_multi_agent_policy_state(&mut self, state: Arc<RwLock<MultiAgentPolicy>>) {
        let policy = self
            .runtime_configuration
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .configuration
            .multi_agent_policy;
        *state.write().unwrap_or_else(|error| error.into_inner()) = policy;
        self.multi_agent_policy = state;
    }

    pub(crate) fn set_resource_manager(&mut self, resources: Arc<ResourceManager>) {
        self.resources = resources;
        self.refresh_runtime_configuration();
    }

    /// Set the shared plan-mode active flag.
    ///
    /// This flag is shared with EnterPlanMode/ExitPlanMode tools so they can
    /// validate transitions (e.g. reject double-entry).  The engine updates
    /// the flag when processing `PlanModeTransition` context modifiers.
    pub fn set_plan_active_flag(&mut self, flag: Arc<AtomicBool>) {
        flag.store(self.plan_state.is_active, std::sync::atomic::Ordering::Release);
        self.runtime_configuration
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .shared_plan_active = Some(Arc::clone(&flag));
        self.plan_active_flag = Some(flag);
    }

    fn refresh_runtime_configuration(&self) {
        let mut state = self
            .runtime_configuration
            .write()
            .unwrap_or_else(|error| error.into_inner());
        let configuration = &mut state.configuration;
        configuration.provider.clone_from(&self.provider_label);
        configuration.model.clone_from(&self.model);
        configuration.permission = self.permission_context.mode();
        configuration.multi_agent_policy = *self
            .multi_agent_policy
            .read()
            .unwrap_or_else(|error| error.into_inner());
        configuration.max_active_agents = self.resources.budget().max_active_agents;
        configuration.effective_max_active_agents = self.resources.effective_agent_limit();
        configuration.effective_effort.clone_from(&self.reasoning_effort);
        configuration.thinking.clone_from(&self.thinking);
        configuration.thinking_budget = thinking_budget(&self.thinking);
        configuration.compaction = self.compact_level;
        state.plan_active = self.plan_state.is_active;
        state.shared_plan_active.clone_from(&self.plan_active_flag);
    }
}

mod abort;
mod config_update;
mod context_modifiers;
mod plan_lifecycle;
mod run;
mod run_resume;
mod runtime_configuration;
mod session_state;
mod task_phase;
impl AgentEngine {
    /// Apply a runtime config update received from the protocol layer.
    ///
    /// Validation is atomic: if one supplied field is rejected or unsupported,
    /// none of the supplied fields are written.
    pub fn apply_config_update(
        &mut self,
        model: Option<String>,
        thinking: Option<String>,
        thinking_budget: Option<u32>,
        effort: Option<String>,
        compaction: Option<String>,
    ) -> ConfigUpdateOutcome {
        self.apply_runtime_config_update(RuntimeConfigUpdate {
            model,
            thinking,
            thinking_budget,
            effort,
            compaction,
            ..Default::default()
        })
    }

    pub fn apply_config_update_with_multi_agent(
        &mut self,
        model: Option<String>,
        thinking: Option<String>,
        thinking_budget: Option<u32>,
        effort: Option<String>,
        compaction: Option<String>,
        multi_agent_policy: Option<MultiAgentPolicy>,
    ) -> ConfigUpdateOutcome {
        self.apply_runtime_config_update(RuntimeConfigUpdate {
            model,
            thinking,
            thinking_budget,
            effort,
            compaction,
            multi_agent_policy,
            max_active_agents: None,
        })
    }

    pub fn apply_runtime_config_update(&mut self, update: RuntimeConfigUpdate) -> ConfigUpdateOutcome {
        config_update::apply(self, update)
    }

    /// Handle a slash command. Plain text returns `None`; an unknown slash name
    /// returns an explicit error and is never forwarded to the provider.
    async fn handle_command(&mut self, input: &str) -> Result<Option<AgentResult>, AgentError> {
        let Some(command) = parse_command_input(input) else {
            return Ok(None);
        };
        let Some(result) = self.execute_command(command).await else {
            return Err(AgentError::ApiError(format!(
                "unknown slash command: {}",
                command.display_name
            )));
        };

        match result {
            Ok(CommandResult::Continue) => {
                info!(command = command.display_name, "Slash command executed");
                Ok(Some(AgentResult {
                    status: AgentOutcomeStatus::Completed,
                    text: String::new(),
                    stop_reason: StopReason::EndTurn,
                    usage: TokenUsage::default(),
                    turns: 0,
                }))
            }
            Ok(CommandResult::Exit) => {
                info!(command = command.display_name, "Slash command executed: exit");
                Err(AgentError::UserAborted)
            }
            Ok(CommandResult::SetModel(model)) => {
                let outcome = self.apply_config_update(Some(model), None, None, None, None);
                if !outcome.applied {
                    return Err(AgentError::ApiError(outcome.message));
                }
                self.output.emit_info(&outcome.message);
                self.persist_session_state().map_err(AgentError::ApiError)?;
                Ok(Some(AgentResult {
                    status: AgentOutcomeStatus::Completed,
                    text: String::new(),
                    stop_reason: StopReason::EndTurn,
                    usage: TokenUsage::default(),
                    turns: 0,
                }))
            }
            Err(e) => {
                error!(command = command.display_name, error = %e, "Slash command failed");
                Err(AgentError::ApiError(e.to_string()))
            }
        }
    }

    async fn execute_command(&mut self, command: ParsedSlashCommand<'_>) -> Option<Result<CommandResult, AnyhowError>> {
        let cmd = self.commands.find(command.name)?;

        // We need to borrow self mutably for CommandContext while also
        // borrowing self.commands immutably (already done above via find()).
        // Use a raw pointer to break the borrow conflict — safe because
        // the command is not modified during execution.
        let cmd_ptr = cmd as *const dyn SlashCommand;

        let mut ctx = CommandContext {
            messages: &mut self.messages,
            compact_state: &mut self.compact_state,
            compact_config: &self.compact_config,
            provider: Arc::clone(&self.provider),
            model: &self.model,
            output: self.output.as_ref(),
            registry: &self.commands,
        };

        // SAFETY: cmd_ptr points to a command inside self.commands which is only
        // borrowed immutably and not mutated during execute().
        let result = unsafe { &*cmd_ptr }.execute(&mut ctx, command.args).await;
        Some(result)
    }

    /// Return whether the input names a slash command registered by this engine.
    ///
    /// Hosts use this before routing a request into a required Workflow so that
    /// built-in commands keep their engine-defined behavior in every preset.
    pub fn recognizes_slash_command(&self, input: &str) -> bool {
        parse_command_input(input).is_some_and(|command| self.commands.find(command.name).is_some())
    }

    /// Return metadata for all registered slash commands.
    pub fn slash_command_list(&self) -> Vec<(String, String)> {
        self.commands
            .all()
            .iter()
            .map(|cmd| (cmd.name().to_string(), cmd.description().to_string()))
            .collect()
    }

    /// Run stop hooks when the agent session ends
    pub async fn run_stop_hooks(&self) {
        if let Err(error) = self.ensure_session_lease() {
            error!(target: "solaris_agent", error = %error, "skipping stop hooks after session lease loss");
            let _ = self.release_session_lease();
            return;
        }
        if let Some(hook_engine) = &self.hooks {
            let messages = hook_engine.run_stop().await;
            for msg in messages {
                log_stop_hook_output(&msg);
            }
        }
        if let Err(error) = self.release_session_lease() {
            error!(target: "solaris_agent", error = %error, "failed to release session lease after stop hooks");
        }
    }
}

/// Result of running one model turn's tool calls: the per-call results and
/// skill modifiers (aligned 1:1 with the originating `tool_calls`), plus the
/// loop-guard signals derived from this round.
struct ToolRoundOutput {
    tool_results: Vec<ContentBlock>,
    tool_modifiers: Vec<Option<ContextModifier>>,
    task_call_id: String,
    /// `Some` only when every tool call in the round was malformed; feeds the
    /// tool-call-malformed breaker.
    tool_call_malformed_fingerprint: Option<ToolCallMalformedFingerprint>,
    /// `Some` when this round produced executable (non-malformed) tool calls
    /// with the same name+input pattern, all errored, and the model emitted no
    /// visible text; feeds the consecutive-tool-call-failure breaker.
    tool_call_failure_fingerprint: Option<ToolCallFailureFingerprint>,
}

/// Assemble the assistant message content blocks (thinking, text, tool calls)
/// from a completed [`StreamOutcome`], preserving the canonical block order.
fn build_assistant_message(outcome: &StreamOutcome) -> Message {
    let mut content: Vec<ContentBlock> = Vec::new();
    if !outcome.thinking_text.is_empty() || outcome.thinking_signature.is_some() {
        content.push(ContentBlock::Thinking {
            thinking: outcome.thinking_text.clone(),
            signature: None,
        });
    }
    if !outcome.assistant_text.is_empty() {
        content.push(ContentBlock::Text {
            text: outcome.assistant_text.clone(),
        });
    }
    content.extend(outcome.tool_calls.iter().cloned());
    Message::now(Role::Assistant, content).with_provider_metadata(outcome.provider_metadata.clone())
}

fn merge_provider_metadata(metadata: &mut ProviderNativeMetadata, namespace: String, value: Value) {
    match metadata.entry(namespace) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(value);
        }
        std::collections::btree_map::Entry::Occupied(mut entry) => {
            merge_metadata_value(entry.get_mut(), value);
        }
    }
}

fn merge_metadata_value(current: &mut Value, incoming: Value) {
    match (current, incoming) {
        (Value::Object(current), Value::Object(incoming)) => {
            for (key, value) in incoming {
                if let Some(existing) = current.get_mut(&key) {
                    merge_metadata_value(existing, value);
                } else {
                    current.insert(key, value);
                }
            }
        }
        (Value::Array(current), Value::Array(mut incoming)) => current.append(&mut incoming),
        (current, incoming) => *current = incoming,
    }
}

#[derive(Debug, Clone, Copy)]
struct ParsedSlashCommand<'a> {
    display_name: &'a str,
    name: &'a str,
    args: &'a str,
}

fn parse_command_input(input: &str) -> Option<ParsedSlashCommand<'_>> {
    let input = input.trim();
    let display_name = input.split_whitespace().next().unwrap_or(input);
    let without_slash = input.strip_prefix('/')?;
    let (name, args) = match without_slash.split_once(|c: char| c.is_whitespace()) {
        Some((name, rest)) => (name, rest.trim()),
        None => (without_slash, ""),
    };

    Some(ParsedSlashCommand {
        display_name,
        name,
        args,
    })
}

#[cfg(test)]
#[path = "engine_test.rs"]
mod engine_test;

#[cfg(test)]
#[path = "engine_session_resume_test.rs"]
mod engine_session_resume_test;

#[cfg(test)]
#[path = "engine_session_lease_test.rs"]
mod engine_session_lease_test;

#[cfg(test)]
#[path = "engine_task_phase_test.rs"]
mod engine_task_phase_test;

#[cfg(test)]
#[path = "engine_user_checkpoint_test.rs"]
mod engine_user_checkpoint_test;
