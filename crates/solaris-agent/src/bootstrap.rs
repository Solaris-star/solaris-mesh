use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use anyhow::Result;
use futures::stream::{FuturesUnordered, StreamExt};
use solaris_config::config::{Config, McpServerConfig};
use solaris_config::shell::{ResolvedShell, resolve_shell_config};
use solaris_mcp::identity::McpIdentityKey;
use solaris_mcp::manager::McpManager;
use solaris_mcp::tool_proxy::register_mcp_tools;
use solaris_process::filter_resource_environment;
use solaris_providers::{LlmProvider, create_provider};
use solaris_skills::loader::load_all_skills;
use solaris_skills::permissions::SkillPermissionChecker;
use solaris_skills::types::SkillMetadata;
#[cfg(test)]
use solaris_tools::exec_command::ExecCommandTool;
use solaris_tools::read_only_evidence::ReadOnlyEvidenceIndex;
use solaris_tools::registry::ToolRegistry;
use solaris_tools::tool_search::ToolSearchTool;
use tracing::{info, warn};
use uuid::Uuid;

use solaris_types::effect::EffectClass;
#[cfg(test)]
use solaris_types::effect::{EffectDescriptor, EffectReplayPolicy, ResourceFootprint};
use solaris_types::identity::{AgentId, RunId};
use solaris_types::permission::{ExecutionBoundary, PermissionDecision, PermissionMode, PermissionRule};
use solaris_types::plugin::ResolvedPluginDefinition;
use solaris_types::runtime::{AgentLifecycleState, AgentRecord};

use crate::builtin_workflows::register_builtin_workflows;
use crate::child_capabilities::ChildCapabilityBlueprint;
use crate::collaboration_runtime::CollaborationRuntime;
use crate::collaboration_tools::{
    AcknowledgeAgentMessageTool, BroadcastTeamMessageTool, CreateTeamTaskTool, GetTeamStateTool, HandoffTaskTool,
    ListArtifactsTool, ReadAgentMessagesTool, ReadTeamFactsTool, RegisterArtifactTool, SendAgentMessageTool,
    SetTeamFactTool,
};
use crate::context::{SystemPromptCache, build_system_prompt_with_shell};
use crate::engine::AgentEngine;
use crate::execution_context::{
    EffectExecutionContext, build_environment_snapshot_with_plugins, effect_output_state_root, stable_digest_bytes,
};
use crate::memory_runtime::MemoryRuntime;
use crate::memory_tool::MemoryTool;
use crate::output::OutputSink;
use crate::permission_engine::PermissionContext;
use crate::plan::tools::{EnterPlanModeTool, ExitPlanModeTool};
use crate::plugin_bootstrap::PluginBootstrap;
use crate::plugin_runtime::PluginRuntime;
use crate::plugin_tool::{PluginCommandContribution, PluginCommandTool};
use crate::resource_manager::ResourceManager;
use crate::resource_policy::ResourcePolicy;
use crate::role_registry::AgentRoleRegistry;
use crate::runtime_ledger::{RuntimeLedger, SqliteRuntimeLedger};
use crate::scheduler::Scheduler;
use crate::session::{Session, SessionManager};
use crate::skill_tool::{EffectSkillShellExecutor, SharedSkillCatalog, SkillTool};
use crate::spawn_tool::SpawnTool;
use crate::spawner::{AgentSpawner, Spawner};
use crate::workflow_controller::WorkflowController;

mod file_tools;
mod mcp;
mod provider;
mod session;

use self::mcp::{McpBootstrap, mcp_plan_mode_disable_handler, prepare_mcp_server_authorized};
pub use self::mcp::{
    allow_mcp_connection_for, connect_mcp_server_authorized, mcp_connection_capability,
    mcp_connection_effect_descriptor, pin_mcp_server_config, revoke_mcp_connection_for,
};
pub(crate) use self::provider::{
    effect_descriptor as provider_effect_descriptor,
    effect_descriptor_for_config as provider_effect_descriptor_for_config,
};
use self::provider::{pin_credential_paths as pin_provider_credential_paths, root_agent_id_for_run};

/// Result of bootstrapping an agent engine with all features initialized.
pub struct BootstrapResult {
    // Fully initialized runtime.
    pub engine: AgentEngine,

    // Shared provider dependency created or reused during bootstrap.
    pub provider: Arc<dyn LlmProvider>,

    // MCP runtime state discovered during bootstrap.
    pub mcp_managers: Vec<Arc<McpManager>>,
    pub has_mcp: bool,

    /// Stable Mesh run identity and shared collaboration state for first-party Hosts.
    pub run_id: RunId,
    pub root_agent_id: AgentId,
    pub collaboration_runtime: Arc<CollaborationRuntime<()>>,
    pub workflow_controller: Arc<WorkflowController>,
    pub role_registry: Arc<AgentRoleRegistry>,
    pub spawner: Arc<AgentSpawner>,
    pub plugin_runtime: Arc<PluginRuntime>,
    pub execution_context: EffectExecutionContext,
    pub resource_manager: Arc<ResourceManager>,
    pub mcp_identity_key: Option<Arc<McpIdentityKey>>,
}

/// Builder for creating a fully-initialized `AgentEngine`.
///
/// Encapsulates the complete initialization pipeline so all consumers
/// (CLI, backend, sub-agents) get consistent behavior:
///
/// - System prompt always includes model identity, working directory, date
/// - Tool usage guidance is always injected
/// - AGENTS.md is loaded from the workspace hierarchy
/// - Skills, MCP, plan mode, spawn are enabled based on `Config` fields
pub struct AgentBootstrap {
    // Bootstrap configuration.
    config: Config,
    workspace: PathBuf,
    extra_skill_dirs: Vec<PathBuf>,

    // Output integration.
    output: Arc<dyn OutputSink>,

    // Optional externally supplied runtime state.
    provider: Option<Arc<dyn LlmProvider>>,
    resume_session: Option<Session>,
    runtime_env: Vec<(String, String)>,
    permission_context: PermissionContext,
    run_id: RunId,
    root_agent_id: AgentId,
    collaboration_runtime: Arc<CollaborationRuntime<()>>,
    session_manager: Option<SessionManager>,
}

struct BootstrapEnvironment {
    // Workspace context.
    workspace: PathBuf,

    // Prompt context.
    resolved_shell: ResolvedShell,
    memory: Option<Arc<MemoryRuntime>>,
}

async fn prepare_mcp_connections<T, Connector, Connection>(
    server_configs: &HashMap<String, McpServerConfig>,
    connector: Connector,
) -> Vec<(String, Result<T, String>)>
where
    Connector: Fn(String, McpServerConfig) -> Connection,
    Connection: Future<Output = Result<T, String>>,
{
    let mut pending_connections = FuturesUnordered::new();
    for (name, config) in server_configs {
        let connector = &connector;
        let name = name.clone();
        let config = config.clone();
        pending_connections.push(async move {
            let result = connector(name.clone(), config).await;
            (name, result)
        });
    }

    let mut completed = Vec::with_capacity(server_configs.len());
    while let Some(result) = pending_connections.next().await {
        completed.push(result);
    }
    completed
}

impl AgentBootstrap {
    pub fn new(mut config: Config, workspace: impl Into<String>, output: Arc<dyn OutputSink>) -> Self {
        let workspace = PathBuf::from(workspace.into());
        pin_provider_credential_paths(&mut config, &workspace);
        let session_directory = PathBuf::from(&config.session.directory);
        if !session_directory.is_absolute() {
            config.session.directory = workspace.join(session_directory).to_string_lossy().into_owned();
        }
        let boundary_root = workspace
            .canonicalize()
            .unwrap_or_else(|_| workspace.clone())
            .to_string_lossy()
            .into_owned();
        let permission_context = PermissionContext::from_auto_approve(config.tools.auto_approve);
        permission_context.set_boundary(ExecutionBoundary::workspace(boundary_root));
        let provider_effect = provider_effect_descriptor_for_config(&config, &workspace);
        permission_context.allow_configured_effect_for("config:provider", "ProviderRequest", &provider_effect);
        permission_context.allow_configured_effect_for("config:provider", "AutoCompact", &provider_effect);
        configure_runtime_permission_rules(&permission_context, permission_context.mode());
        let run_id = RunId::new(format!("run-{}", Uuid::now_v7()));
        let root_agent_id = root_agent_id_for_run(&run_id);
        let collaboration_runtime = Arc::new(CollaborationRuntime::new(Scheduler::new(ResourcePolicy::new(1))));
        collaboration_runtime.agents().upsert(AgentRecord {
            run_id: run_id.clone(),
            agent_id: root_agent_id.clone(),
            team_id: None,
            parent_agent_id: None,
            state: AgentLifecycleState::Active,
        });
        Self {
            config,
            workspace,
            extra_skill_dirs: Vec::new(),
            output,
            provider: None,
            resume_session: None,
            runtime_env: Vec::new(),
            permission_context,
            run_id,
            root_agent_id,
            collaboration_runtime,
            session_manager: None,
        }
    }

    /// Use a pre-created provider instead of creating one from config.
    pub fn provider(mut self, provider: Arc<dyn LlmProvider>) -> Self {
        self.provider = Some(provider);
        self
    }

    pub fn permission_mode(self, mode: PermissionMode) -> Self {
        self.permission_context.set_mode(mode);
        configure_runtime_permission_rules(&self.permission_context, mode);
        self
    }

    /// Resume from a previously saved session. New-format sessions preserve
    /// the Mesh Run identity; legacy sessions receive one on first resume and
    /// persist it on the next session save.
    pub fn resume(mut self, mut session: Session) -> Self {
        let run_id = session
            .run_id
            .as_deref()
            .map(RunId::from)
            .unwrap_or_else(|| self.run_id.clone());
        session.run_id = Some(run_id.to_string());
        self.run_id = run_id.clone();
        self.root_agent_id = root_agent_id_for_run(&run_id);
        self.resume_session = Some(session);
        self
    }

    /// Inject supported resource-limit variables for owned subprocesses.
    pub fn runtime_env(mut self, runtime_env: Vec<(String, String)>) -> Self {
        self.runtime_env = filter_resource_environment(runtime_env);
        self
    }

    /// Add extra directories to scan for skills.
    pub fn extra_skill_dirs(mut self, dirs: Vec<PathBuf>) -> Self {
        self.extra_skill_dirs = dirs;
        self
    }

    /// Read-only access to the config (for session management before build).
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Build the fully-initialized engine.
    pub async fn build(mut self) -> Result<BootstrapResult> {
        let session_fence = self.acquire_resumed_session_lease()?;
        let workspace = self.resolve_workspace_path();
        let plan_mode = self.permission_context.mode() == PermissionMode::Plan;
        self.initialize_persistent_runtime(&workspace)?;
        let mcp_identity_key = if plan_mode {
            None
        } else {
            for server in self.config.mcp.servers.values_mut() {
                if let Ok(pinned) = pin_mcp_server_config(server) {
                    *server = pinned;
                }
            }
            Some(Arc::new(McpIdentityKey::load_or_create(
                &self
                    .runtime_state_root(&workspace)
                    .join("runtime")
                    .join("mcp-identity-key.json"),
            )?))
        };
        if let Some(mcp_identity_key) = mcp_identity_key.as_deref() {
            for (name, server) in &self.config.mcp.servers {
                allow_mcp_connection_for(
                    &self.permission_context,
                    format!("config:mcp:{name}"),
                    name,
                    server,
                    mcp_identity_key,
                );
            }
        }
        let provider = self.resolve_provider();
        let environment = self.resolve_environment(workspace)?;
        let read_only_evidence_index = Arc::new(ReadOnlyEvidenceIndex::default());
        let mut registry =
            self.build_builtin_registry_with_evidence(&environment.workspace, Arc::clone(&read_only_evidence_index));
        let plugin_bootstrap = PluginBootstrap::discover(
            &environment.workspace,
            &self.run_id,
            &self.root_agent_id,
            self.collaboration_runtime.ledger(),
        )
        .map_err(anyhow::Error::msg)?;
        for warning in plugin_bootstrap.warnings() {
            self.output.emit_info(&format!("Plugin: {warning}"));
        }
        for plugin in plugin_bootstrap.active_plugins() {
            authorize_activated_plugin(&self.permission_context, plugin).map_err(anyhow::Error::msg)?;
        }
        plugin_bootstrap.register_tools(&mut registry);
        self.extra_skill_dirs
            .extend(plugin_bootstrap.skill_dirs().iter().cloned());

        let mcp_permission_context = self.permission_context.clone();
        let mut bootstrap_execution_context = EffectExecutionContext::new(
            self.run_id.clone(),
            self.root_agent_id.clone(),
            self.collaboration_runtime.ledger(),
            mcp_permission_context.clone(),
            build_environment_snapshot_with_plugins(
                &self.config,
                &registry,
                &mcp_permission_context,
                plugin_bootstrap.runtime.active_implementation_identities(),
            ),
        )
        .with_mutation_coordinator(self.collaboration_runtime.mutation_coordinator());
        if let Some(fence) = session_fence.clone() {
            bootstrap_execution_context = bootstrap_execution_context.with_session_fence(fence);
        }
        let builtin_names = registry.tool_names();
        let mcp = if plan_mode {
            McpBootstrap::default()
        } else {
            self.connect_mcp(
                &mut registry,
                &builtin_names,
                &bootstrap_execution_context,
                mcp_identity_key
                    .as_deref()
                    .expect("non-Plan bootstrap initializes the MCP identity key"),
            )
            .await
        };

        let skills = Arc::new(self.load_skills(&environment.workspace, mcp.manager.as_deref()).await);
        self.configure_system_prompt(&environment, skills.as_slice());
        let skill_catalog = SharedSkillCatalog::new(skills);
        let child_mcp_servers = if plan_mode {
            HashMap::new()
        } else {
            self.mcp_servers_with_runtime_env()
        };
        let child_capabilities = Arc::new(
            ChildCapabilityBlueprint::new_with_catalog(
                skill_catalog.clone(),
                self.config.tools.skills.deny.clone(),
                self.config.tools.skills.allow.clone(),
                true,
                mcp.manager.clone(),
                child_mcp_servers,
                mcp_identity_key.clone(),
                plugin_bootstrap.active_plugins().to_vec(),
            )
            .with_read_only_evidence_index(Arc::clone(&read_only_evidence_index))
            .with_memory_runtime(environment.memory.clone()),
        );

        let spawner = Arc::new(
            AgentSpawner::new_with_env(
                Arc::clone(&provider),
                self.config.clone(),
                environment.workspace.clone(),
                self.runtime_env.clone(),
            )
            .with_permission_context(self.permission_context.clone())
            .with_read_only_evidence_index(read_only_evidence_index)
            .with_session_fence_state(bootstrap_execution_context.session_fence_state())
            .with_runtime_context(
                Arc::clone(&self.collaboration_runtime),
                self.run_id.clone(),
                self.root_agent_id.clone(),
            )
            .with_capability_blueprint(child_capabilities),
        );
        let resource_manager = spawner.resource_manager();
        resource_manager
            .attach_runtime(self.run_id.clone(), Arc::clone(&self.collaboration_runtime))
            .map_err(anyhow::Error::msg)?;
        let root_execution_context = EffectExecutionContext::new(
            self.run_id.clone(),
            self.root_agent_id.clone(),
            self.collaboration_runtime.ledger(),
            self.permission_context.clone(),
            build_environment_snapshot_with_plugins(
                &self.config,
                &registry,
                &self.permission_context,
                plugin_bootstrap.runtime.active_implementation_identities(),
            ),
        )
        .with_mutation_coordinator(self.collaboration_runtime.mutation_coordinator())
        .with_resource_manager(Arc::clone(&resource_manager))
        .share_session_fence_with(&bootstrap_execution_context);
        self.register_agent_tools(
            &mut registry,
            Arc::clone(&spawner),
            skill_catalog.clone(),
            root_execution_context.clone(),
            environment.memory.clone(),
        );
        let role_registry = Arc::new(AgentRoleRegistry::default());
        let workflow_controller = Arc::new(
            WorkflowController::with_runtime_and_roles(
                Arc::clone(&self.collaboration_runtime),
                Some(Arc::clone(&role_registry)),
            )
            .with_task_admission(self.run_id.clone(), self.config.multi_agent.max_tasks_per_run as usize),
        );
        register_builtin_workflows(&workflow_controller, &role_registry).map_err(anyhow::Error::msg)?;
        plugin_bootstrap
            .register_workflows(&workflow_controller)
            .map_err(anyhow::Error::msg)?;
        let restored_workflows = workflow_controller
            .restore_from_ledger(&self.run_id)
            .map_err(anyhow::Error::msg)?;
        if restored_workflows > 0 {
            self.output.emit_info(&format!(
                "Recovered {restored_workflows} durable Mesh workflow run(s) for {}",
                self.run_id
            ));
        }
        let plan_active_flag = self.register_plan_tools(&mut registry);
        Self::register_tool_search(&mut registry);

        let root_environment = build_environment_snapshot_with_plugins(
            &self.config,
            &registry,
            &self.permission_context,
            plugin_bootstrap.runtime.active_implementation_identities(),
        );
        root_execution_context.set_environment(root_environment);
        let has_mcp = mcp.has_mcp();
        let mcp_managers = mcp.managers;
        let run_id = self.run_id.clone();
        let root_agent_id = self.root_agent_id.clone();
        let collaboration_runtime = Arc::clone(&self.collaboration_runtime);
        let workflow_controller = Arc::clone(&workflow_controller);
        let role_registry = Arc::clone(&role_registry);
        let host_spawner = Arc::clone(&spawner);
        let plugin_runtime = Arc::clone(&plugin_bootstrap.runtime);
        let configured_mcp_server_names = self.config.mcp.servers.keys().cloned().collect();
        let mut engine = self.into_engine(provider.clone(), registry, plan_active_flag, environment.workspace);
        engine.set_multi_agent_policy_state(host_spawner.multi_agent_policy_state());
        engine.set_resource_manager(host_spawner.resource_manager());
        engine.set_execution_context(root_execution_context.clone());
        engine.activate_resumed_session()?;
        engine.set_plan_mode_disable_handler(mcp_plan_mode_disable_handler(
            Arc::clone(&host_spawner),
            root_execution_context.permissions().clone(),
            skill_catalog,
            configured_mcp_server_names,
        ));

        Ok(BootstrapResult {
            engine,
            provider,
            mcp_managers,
            has_mcp,
            run_id,
            root_agent_id,
            collaboration_runtime,
            workflow_controller,
            role_registry,
            spawner: host_spawner,
            plugin_runtime,
            execution_context: root_execution_context,
            resource_manager,
            mcp_identity_key,
        })
    }

    fn initialize_persistent_runtime(&mut self, workspace: &Path) -> Result<()> {
        let state_root = self.runtime_state_root(workspace);
        let runtime_directory = state_root.join("runtime");
        let ledger_path = runtime_directory.join("ledger.sqlite3");
        let legacy_ledger_path = runtime_directory.join("ledger.jsonl");
        let ledger: Arc<dyn RuntimeLedger> = Arc::new(SqliteRuntimeLedger::open_with_jsonl_migration(
            &ledger_path,
            &legacy_ledger_path,
        )?);
        if self.config.session.enabled {
            let session_manager = self.session_manager.get_or_insert_with(|| {
                SessionManager::new(
                    PathBuf::from(&self.config.session.directory),
                    self.config.session.max_sessions,
                )
            });
            let report = session_manager.run_due_gc(ledger.as_ref(), 8)?;
            if report.examined > 0 {
                info!(
                    target: "solaris_agent",
                    examined = report.examined,
                    completed = report.completed,
                    deferred = report.deferred,
                    ledger_records_deleted = report.ledger_records_deleted,
                    blob_runs_processed = report.blob_runs_processed,
                    "processed due session GC jobs"
                );
            }
            if report.failed > 0 {
                warn!(
                    target: "solaris_agent",
                    failed = report.failed,
                    "one or more due session GC jobs remain pending after failure"
                );
            }
        }
        self.permission_context
            .register_protected_paths(&state_root, ledger.protected_state_paths())
            .map_err(anyhow::Error::msg)?;
        let effect_output_root = effect_output_state_root();
        std::fs::create_dir_all(&effect_output_root)?;
        self.permission_context
            .register_protected_paths(&effect_output_root, Vec::new())
            .map_err(anyhow::Error::msg)?;
        let runtime = Arc::new(CollaborationRuntime::with_ledger(
            Scheduler::new(ResourcePolicy::new(1)),
            ledger,
        ));
        runtime.agents().upsert(AgentRecord {
            run_id: self.run_id.clone(),
            agent_id: self.root_agent_id.clone(),
            team_id: None,
            parent_agent_id: None,
            state: AgentLifecycleState::Active,
        });
        runtime.restore_projection(&self.run_id)?;
        self.collaboration_runtime = runtime;
        info!(
            target: "solaris_agent",
            run_id = %self.run_id,
            ledger = %ledger_path.display(),
            "durable Mesh runtime ledger initialized"
        );
        Ok(())
    }

    fn runtime_state_root(&self, workspace: &Path) -> PathBuf {
        PathBuf::from(&self.config.session.directory)
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| workspace.join(".solaris"))
    }

    fn resolve_workspace_path(&self) -> PathBuf {
        info!(
            target: "solaris_agent",
            workspace = %self.workspace.display(),
            "agent bootstrap: workspace cwd resolved",
        );

        self.workspace.clone()
    }

    fn resolve_environment(&mut self, workspace_path: PathBuf) -> Result<BootstrapEnvironment> {
        let memory = if self.config.memory.enabled {
            let runtime = if self.config.session.enabled {
                let persisted = self
                    .resume_session
                    .as_ref()
                    .and_then(|session| session.runtime_state.as_ref())
                    .and_then(|state| state.memory_snapshot.clone());
                let snapshot_root = self
                    .runtime_state_root(&workspace_path)
                    .join("runtime")
                    .join("effect-outcomes")
                    .join(stable_digest_bytes(self.run_id.as_str().as_bytes()));
                let (runtime, reference, prepared) = MemoryRuntime::open_for_session(
                    &workspace_path,
                    self.config.memory.review,
                    &snapshot_root,
                    persisted.as_ref(),
                )?;
                if let Some(prepared) = prepared {
                    let manager = self.session_manager.get_or_insert_with(|| {
                        SessionManager::new(
                            PathBuf::from(&self.config.session.directory),
                            self.config.session.max_sessions,
                        )
                    });
                    if let Some(session) = self.resume_session.as_mut() {
                        manager
                            .install_active_memory_snapshot(session, &prepared)
                            .map_err(anyhow::Error::new)?;
                    } else {
                        manager
                            .set_initial_memory_snapshot(prepared)
                            .map_err(anyhow::Error::new)?;
                    }
                } else if let Some(session) = self.resume_session.as_mut() {
                    session
                        .runtime_state
                        .get_or_insert_with(Default::default)
                        .memory_snapshot = Some(reference);
                }
                Arc::new(runtime)
            } else {
                Arc::new(MemoryRuntime::open(&workspace_path, self.config.memory.review)?)
            };
            self.permission_context
                .register_protected_paths(
                    runtime.directory(),
                    vec![runtime.service().database_path().to_path_buf()],
                )
                .map_err(anyhow::Error::msg)?;
            Some(runtime)
        } else {
            None
        };
        Ok(BootstrapEnvironment {
            resolved_shell: resolve_shell_config(&self.config.shell)?,
            memory,
            workspace: workspace_path,
        })
    }

    fn resolve_provider(&mut self) -> Arc<dyn LlmProvider> {
        self.provider.take().unwrap_or_else(|| create_provider(&self.config))
    }

    #[cfg(test)]
    fn build_builtin_registry(&self, workspace_path: &Path) -> ToolRegistry {
        self.build_builtin_registry_with_evidence(workspace_path, Arc::new(ReadOnlyEvidenceIndex::default()))
    }

    fn build_builtin_registry_with_evidence(
        &self,
        workspace_path: &Path,
        read_only_evidence_index: Arc<ReadOnlyEvidenceIndex>,
    ) -> ToolRegistry {
        file_tools::build_builtin_registry(
            &self.config,
            &self.permission_context,
            workspace_path,
            &self.runtime_env,
            read_only_evidence_index,
        )
    }

    async fn connect_mcp(
        &self,
        registry: &mut ToolRegistry,
        builtin_names: &[String],
        execution_context: &EffectExecutionContext,
        identity_key: &McpIdentityKey,
    ) -> McpBootstrap {
        let server_configs = self.mcp_servers_with_runtime_env();
        if server_configs.is_empty() {
            return McpBootstrap::default();
        }

        let mut manager = McpManager::new();
        let pending_connections = prepare_mcp_connections(&server_configs, |name, config| async move {
            prepare_mcp_server_authorized(&name, &config, execution_context, identity_key).await
        })
        .await;
        for (name, result) in pending_connections {
            match result {
                Ok(pending) => {
                    manager.commit_pending(pending);
                }
                Err(error) => self
                    .output
                    .emit_error(&format!("MCP initialization error for '{name}': {error}")),
            }
        }
        if manager.server_names().is_empty() {
            return McpBootstrap::default();
        }
        let manager = Arc::new(manager);

        register_mcp_tools(registry, &manager, builtin_names, &server_configs, identity_key);

        McpBootstrap {
            manager: Some(Arc::clone(&manager)),
            managers: vec![manager],
        }
    }

    fn mcp_servers_with_runtime_env(&self) -> HashMap<String, McpServerConfig> {
        let mut servers = self.config.mcp.servers.clone();
        if self.runtime_env.is_empty() {
            return servers;
        }

        for server in servers.values_mut() {
            let mut env: HashMap<String, String> = self.runtime_env.clone().into_iter().collect();
            if let Some(server_env) = server.env.take() {
                env.extend(server_env);
            }
            server.env = Some(env);
        }

        servers
    }

    async fn load_skills(&self, workspace: &Path, mcp_manager: Option<&McpManager>) -> Vec<SkillMetadata> {
        load_all_skills(workspace, &self.extra_skill_dirs, false, mcp_manager).await
    }

    fn configure_system_prompt(&mut self, environment: &BootstrapEnvironment, skills: &[SkillMetadata]) {
        let mut prompt_cache = SystemPromptCache::new();
        if let Some(memory) = environment.memory.as_deref() {
            prompt_cache.sections.insert("memory", memory.system_prompt());
        }
        let workspace = self.workspace.to_string_lossy();
        let system_prompt = build_system_prompt_with_shell(
            &mut prompt_cache,
            self.config.system_prompt.as_deref(),
            &workspace,
            &self.config.model,
            &environment.resolved_shell,
            skills,
            None,
            environment.memory.as_deref().map(MemoryRuntime::directory),
            false,
            self.config.compact.toon,
        );
        self.config.system_prompt = Some(system_prompt);
    }

    fn register_agent_tools(
        &self,
        registry: &mut ToolRegistry,
        spawner: Arc<AgentSpawner>,
        skills: SharedSkillCatalog,
        execution_context: EffectExecutionContext,
        memory: Option<Arc<MemoryRuntime>>,
    ) {
        if let Some(memory) = memory {
            registry.register(Box::new(MemoryTool::root(memory)));
        }
        // Tool-level interactive approval is centralized in PermissionEngine.
        // SkillPermissionChecker still enforces explicit deny/allow rules but
        // must not create a second independent prompt path.
        let skill_checker = SkillPermissionChecker::new(
            self.config.tools.skills.deny.clone(),
            self.config.tools.skills.allow.clone(),
            true,
        );
        let fork_spawner: Arc<dyn Spawner> = spawner.clone();
        registry.register(Box::new(
            SkillTool::with_shared_catalog_and_spawner(
                skills,
                self.workspace.to_path_buf(),
                skill_checker,
                None,
                Some(fork_spawner),
            )
            .with_shell_executor(Arc::new(EffectSkillShellExecutor::new(execution_context))),
        ));

        registry.register(Box::new(SpawnTool::new(spawner)));
        registry.register(Box::new(SendAgentMessageTool::new(
            Arc::clone(&self.collaboration_runtime),
            self.run_id.clone(),
            self.root_agent_id.clone(),
        )));
        registry.register(Box::new(ReadAgentMessagesTool::new(
            Arc::clone(&self.collaboration_runtime),
            self.root_agent_id.clone(),
        )));
        registry.register(Box::new(AcknowledgeAgentMessageTool::new(
            Arc::clone(&self.collaboration_runtime),
            self.run_id.clone(),
            self.root_agent_id.clone(),
        )));
        registry.register(Box::new(BroadcastTeamMessageTool::new(
            Arc::clone(&self.collaboration_runtime),
            self.run_id.clone(),
            self.root_agent_id.clone(),
        )));
        registry.register(Box::new(CreateTeamTaskTool::new(
            Arc::clone(&self.collaboration_runtime),
            self.run_id.clone(),
            self.root_agent_id.clone(),
            self.config.multi_agent.max_tasks_per_run as usize,
        )));
        registry.register(Box::new(HandoffTaskTool::new(
            Arc::clone(&self.collaboration_runtime),
            self.run_id.clone(),
            self.root_agent_id.clone(),
        )));
        registry.register(Box::new(SetTeamFactTool::new(
            Arc::clone(&self.collaboration_runtime),
            self.run_id.clone(),
            self.root_agent_id.clone(),
        )));
        registry.register(Box::new(ReadTeamFactsTool::new(
            Arc::clone(&self.collaboration_runtime),
            self.root_agent_id.clone(),
        )));
        registry.register(Box::new(RegisterArtifactTool::new(
            Arc::clone(&self.collaboration_runtime),
            self.run_id.clone(),
            self.root_agent_id.clone(),
        )));
        registry.register(Box::new(ListArtifactsTool::new(
            Arc::clone(&self.collaboration_runtime),
            self.root_agent_id.clone(),
        )));
        registry.register(Box::new(GetTeamStateTool::new(Arc::clone(&self.collaboration_runtime))));
    }

    fn register_plan_tools(&self, registry: &mut ToolRegistry) -> Arc<AtomicBool> {
        let plan_active_flag = Arc::new(AtomicBool::new(false));

        if self.config.plan.enabled {
            registry.register(Box::new(EnterPlanModeTool::new(Arc::clone(&plan_active_flag))));
            registry.register(Box::new(ExitPlanModeTool::new(Arc::clone(&plan_active_flag))));
        }

        plan_active_flag
    }

    fn register_tool_search(registry: &mut ToolRegistry) {
        let tool_defs_snapshot = registry.to_tool_defs();
        registry.register(Box::new(ToolSearchTool::new(tool_defs_snapshot)));
    }

    fn into_engine(
        mut self,
        provider: Arc<dyn LlmProvider>,
        registry: ToolRegistry,
        plan_active_flag: Arc<AtomicBool>,
        workspace: PathBuf,
    ) -> AgentEngine {
        let runtime_env = self.runtime_env.clone();
        let prepared_session_manager = self.session_manager.take();
        let mut engine = if let Some(session) = self.resume_session {
            AgentEngine::resume_with_provider_and_env(
                provider,
                self.config,
                registry,
                self.output,
                session,
                workspace,
                runtime_env,
            )
        } else {
            AgentEngine::new_with_provider_and_env(provider, self.config, registry, self.output, workspace, runtime_env)
        };
        engine.set_permission_context(self.permission_context.clone());
        engine.set_plan_active_flag(plan_active_flag);
        engine.install_session_manager(prepared_session_manager);
        engine
    }
}

fn configure_runtime_permission_rules(context: &PermissionContext, mode: PermissionMode) {
    context.set_mode(mode);
}

pub fn authorize_activated_plugin(
    context: &PermissionContext,
    plugin: &ResolvedPluginDefinition,
) -> std::result::Result<(), String> {
    let mut grants = Vec::new();
    let mut capabilities = Vec::new();
    for definition in plugin.definition.command_tools.clone() {
        let capability = definition.name.clone();
        let tool = PluginCommandTool::from_resolved(plugin, definition)?;
        grants.push((capability.clone(), tool.execution_boundary_descriptor()));
        capabilities.push(capability);
    }
    for definition in plugin.definition.command_contributions.clone() {
        let contribution = PluginCommandContribution::from_resolved(plugin, definition)?;
        let capability = contribution.capability();
        grants.push((capability.clone(), contribution.execution_boundary_descriptor()));
        capabilities.push(capability);
    }
    let permission_source = format!("plugin:{}", plugin.definition.id);
    for (capability, descriptor) in &grants {
        context.allow_configured_effect_for(&permission_source, capability, descriptor);
    }
    let rules = capabilities
        .into_iter()
        .map(|capability| PermissionRule {
            capability: Some(capability),
            action: None,
            effect_class: Some(EffectClass::Process),
            resource_prefixes: Vec::new(),
            decision: PermissionDecision::Allow,
        })
        .collect();
    context.set_generated_rules(permission_source, rules);
    Ok(())
}

pub fn revoke_activated_plugin(
    context: &PermissionContext,
    plugin: &ResolvedPluginDefinition,
) -> std::result::Result<(), String> {
    let permission_source = format!("plugin:{}", plugin.definition.id);
    context.revoke_configured_effect_from(&permission_source);
    context.remove_generated_rules(&permission_source);
    Ok(())
}

#[cfg(test)]
#[path = "bootstrap_test.rs"]
mod bootstrap_test;

#[cfg(test)]
#[path = "bootstrap_auto_exec_test.rs"]
mod bootstrap_auto_exec_test;

#[cfg(test)]
#[path = "bootstrap_protected_state_test.rs"]
mod bootstrap_protected_state_test;

#[cfg(test)]
#[path = "bootstrap_memory_test.rs"]
mod bootstrap_memory_test;

#[cfg(test)]
#[path = "bootstrap_session_lease_test.rs"]
mod bootstrap_session_lease_test;
