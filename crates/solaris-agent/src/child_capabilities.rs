use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use solaris_config::config::McpServerConfig;
use solaris_mcp::identity::McpIdentityKey;
use solaris_mcp::manager::McpManager;
use solaris_mcp::tool_proxy::register_mcp_tools;
use solaris_skills::permissions::SkillPermissionChecker;
use solaris_skills::types::SkillMetadata;
use solaris_tools::edit::EditTool;
use solaris_tools::exec_command::ExecCommandTool;
use solaris_tools::glob::GlobTool;
use solaris_tools::grep::GrepTool;
use solaris_tools::read::ReadTool;
use solaris_tools::read_only_evidence::ReadOnlyEvidenceIndex;
use solaris_tools::registry::ToolRegistry;
use solaris_tools::tool_search::ToolSearchTool;
use solaris_tools::write::WriteTool;
use solaris_types::identity::{AgentId, RunId};
use solaris_types::plugin::{ImplementationIdentity, ResolvedPluginDefinition};

use crate::collaboration_runtime::CollaborationRuntime;
use crate::collaboration_tools::{
    AcknowledgeAgentMessageTool, BroadcastTeamMessageTool, CreateTeamTaskTool, GetTeamStateTool, HandoffTaskTool,
    ListArtifactsTool, ReadAgentMessagesTool, ReadTeamFactsTool, RegisterArtifactTool, SendAgentMessageTool,
    SetTeamFactTool,
};
use crate::execution_context::EffectExecutionContext;
use crate::memory_runtime::MemoryRuntime;
use crate::memory_tool::MemoryTool;
use crate::plugin_tool::PluginCommandTool;
use crate::skill_tool::{EffectSkillShellExecutor, SharedSkillCatalog, SkillTool};
use crate::spawn_tool::SpawnTool;
use crate::spawner::{AgentSpawner, Spawner};

#[derive(Clone)]
struct McpCapabilitySource {
    manager: Arc<McpManager>,
    server_configs: HashMap<String, McpServerConfig>,
}

#[cfg(test)]
#[path = "child_capabilities_test.rs"]
mod child_capabilities_test;

/// Reconstructable child capability surface captured from the parent Run.
/// Requested scopes can only retain capabilities that already exist here.
/// Inheritance must be selected explicitly by the caller.
#[derive(Clone)]
pub struct ChildCapabilityBlueprint {
    skills: SharedSkillCatalog,
    skill_deny: Vec<String>,
    skill_allow: Vec<String>,
    skill_auto_approve: bool,
    mcp_sources: Arc<RwLock<Vec<McpCapabilitySource>>>,
    mcp_identity_key: Option<Arc<McpIdentityKey>>,
    active_plugins: Arc<RwLock<Vec<Arc<ResolvedPluginDefinition>>>>,
    memory_runtime: Option<Arc<MemoryRuntime>>,
    read_only_evidence_index: Arc<ReadOnlyEvidenceIndex>,
}

impl ChildCapabilityBlueprint {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        skills: Arc<Vec<SkillMetadata>>,
        skill_deny: Vec<String>,
        skill_allow: Vec<String>,
        skill_auto_approve: bool,
        mcp_manager: Option<Arc<McpManager>>,
        mcp_server_configs: HashMap<String, McpServerConfig>,
        mcp_identity_key: Option<Arc<McpIdentityKey>>,
        active_plugins: Vec<Arc<ResolvedPluginDefinition>>,
    ) -> Self {
        Self::new_with_catalog(
            SharedSkillCatalog::new(skills),
            skill_deny,
            skill_allow,
            skill_auto_approve,
            mcp_manager,
            mcp_server_configs,
            mcp_identity_key,
            active_plugins,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_catalog(
        skills: SharedSkillCatalog,
        skill_deny: Vec<String>,
        skill_allow: Vec<String>,
        skill_auto_approve: bool,
        mcp_manager: Option<Arc<McpManager>>,
        mcp_server_configs: HashMap<String, McpServerConfig>,
        mcp_identity_key: Option<Arc<McpIdentityKey>>,
        active_plugins: Vec<Arc<ResolvedPluginDefinition>>,
    ) -> Self {
        let mcp_sources = mcp_manager
            .map(|manager| {
                vec![McpCapabilitySource {
                    manager,
                    server_configs: mcp_server_configs,
                }]
            })
            .unwrap_or_default();
        Self {
            skills,
            skill_deny,
            skill_allow,
            skill_auto_approve,
            mcp_sources: Arc::new(RwLock::new(mcp_sources)),
            mcp_identity_key,
            active_plugins: Arc::new(RwLock::new(active_plugins)),
            memory_runtime: None,
            read_only_evidence_index: Arc::new(ReadOnlyEvidenceIndex::default()),
        }
    }

    pub(crate) fn with_read_only_evidence_index(mut self, index: Arc<ReadOnlyEvidenceIndex>) -> Self {
        self.read_only_evidence_index = index;
        self
    }

    pub(crate) fn with_memory_runtime(mut self, memory_runtime: Option<Arc<MemoryRuntime>>) -> Self {
        self.memory_runtime = memory_runtime;
        self
    }

    pub fn add_mcp_source(&self, manager: Arc<McpManager>, server_configs: HashMap<String, McpServerConfig>) {
        self.mcp_sources
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .push(McpCapabilitySource {
                manager,
                server_configs,
            });
    }

    pub fn mcp_source_count(&self) -> usize {
        self.mcp_sources.read().unwrap_or_else(|error| error.into_inner()).len()
    }

    pub fn take_mcp_sources(&self) -> Vec<Arc<McpManager>> {
        let mut sources = self.mcp_sources.write().unwrap_or_else(|error| error.into_inner());
        std::mem::take(&mut *sources)
            .into_iter()
            .map(|source| source.manager)
            .collect()
    }

    fn register_mcp_tools(&self, registry: &mut ToolRegistry) {
        let sources = self
            .mcp_sources
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        for source in sources {
            let Some(identity_key) = self.mcp_identity_key.as_deref() else {
                tracing::warn!(target: "solaris_mcp", "skipping child MCP tools without an identity key");
                continue;
            };
            let reserved_names = registry.tool_names();
            register_mcp_tools(
                registry,
                &source.manager,
                &reserved_names,
                &source.server_configs,
                identity_key,
            );
        }
    }

    fn register_memory_tool(&self, registry: &mut ToolRegistry) {
        if let Some(memory_runtime) = self.memory_runtime.as_ref() {
            registry.register(Box::new(MemoryTool::child(Arc::clone(memory_runtime))));
        }
    }

    pub fn add_active_plugin(&self, plugin: Arc<ResolvedPluginDefinition>) -> Result<(), String> {
        let mut active = self.active_plugins.write().unwrap_or_else(|error| error.into_inner());
        if active
            .iter()
            .any(|existing| existing.definition.id == plugin.definition.id)
        {
            return Err(format!(
                "plugin {} is already active for child capabilities",
                plugin.definition.id
            ));
        }
        active.push(plugin);
        Ok(())
    }

    pub fn remove_active_plugin(&self, plugin_id: &str) -> bool {
        let mut active = self.active_plugins.write().unwrap_or_else(|error| error.into_inner());
        let before = active.len();
        active.retain(|plugin| plugin.definition.id != plugin_id);
        before != active.len()
    }

    pub fn plugin_identities(&self) -> Vec<ImplementationIdentity> {
        let mut values: Vec<_> = self
            .active_plugins
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .map(|plugin| plugin.identity.implementation.clone())
            .collect();
        values.sort_by(|left, right| left.implementation_id.cmp(&right.implementation_id));
        values.dedup();
        values
    }

    #[allow(clippy::too_many_arguments)]
    pub fn build_registry(
        &self,
        requested: &[String],
        inherit_capabilities: bool,
        cwd: &Path,
        runtime_env: &[(String, String)],
        spawner: Arc<AgentSpawner>,
        runtime: Arc<CollaborationRuntime<()>>,
        run_id: RunId,
        agent_id: AgentId,
        execution_context: EffectExecutionContext,
    ) -> ToolRegistry {
        let search_policy = execution_context.permissions().workspace_search_policy();
        let mut registry = ToolRegistry::new();
        registry.register(Box::new(
            ReadTool::new_with_search_policy(None, cwd, Arc::clone(&search_policy))
                .with_read_only_evidence_index(Arc::clone(&self.read_only_evidence_index)),
        ));
        registry.register(Box::new(WriteTool::new_with_search_policy(
            None,
            cwd,
            Arc::clone(&search_policy),
        )));
        registry.register(Box::new(EditTool::new_with_search_policy(
            None,
            cwd,
            Arc::clone(&search_policy),
        )));
        registry.register(Box::new(ExecCommandTool::new_with_env(
            PathBuf::from(cwd),
            runtime_env.to_vec(),
        )));
        registry.register(Box::new(
            GrepTool::new_with_search_policy(PathBuf::from(cwd), Arc::clone(&search_policy))
                .with_read_only_evidence_index(Arc::clone(&self.read_only_evidence_index)),
        ));
        registry.register(Box::new(
            GlobTool::new_with_search_policy(PathBuf::from(cwd), search_policy)
                .with_read_only_evidence_index(Arc::clone(&self.read_only_evidence_index)),
        ));

        let active_plugins = self
            .active_plugins
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        for plugin in &active_plugins {
            for definition in plugin.definition.command_tools.clone() {
                match PluginCommandTool::from_resolved(plugin, definition) {
                    Ok(tool) => registry.register(Box::new(tool)),
                    Err(error) => tracing::warn!(
                        target: "solaris_plugin",
                        plugin = %plugin.definition.id,
                        error = %error,
                        "child plugin tool registration skipped"
                    ),
                }
            }
        }

        self.register_mcp_tools(&mut registry);

        self.register_memory_tool(&mut registry);

        let checker = SkillPermissionChecker::new(
            self.skill_deny.clone(),
            self.skill_allow.clone(),
            self.skill_auto_approve,
        );
        let fork_spawner: Arc<dyn Spawner> = spawner.clone();
        registry.register(Box::new(
            SkillTool::with_shared_catalog_and_spawner(
                self.skills.clone(),
                PathBuf::from(cwd),
                checker,
                None,
                Some(fork_spawner),
            )
            .with_shell_executor(Arc::new(EffectSkillShellExecutor::new(execution_context))),
        ));
        registry.register(Box::new(SpawnTool::new(spawner)));
        registry.register(Box::new(SendAgentMessageTool::new(
            Arc::clone(&runtime),
            run_id.clone(),
            agent_id.clone(),
        )));
        registry.register(Box::new(ReadAgentMessagesTool::new(
            Arc::clone(&runtime),
            agent_id.clone(),
        )));
        registry.register(Box::new(AcknowledgeAgentMessageTool::new(
            Arc::clone(&runtime),
            run_id.clone(),
            agent_id.clone(),
        )));
        registry.register(Box::new(BroadcastTeamMessageTool::new(
            Arc::clone(&runtime),
            run_id.clone(),
            agent_id.clone(),
        )));
        registry.register(Box::new(CreateTeamTaskTool::new(
            Arc::clone(&runtime),
            run_id.clone(),
            agent_id.clone(),
        )));
        registry.register(Box::new(HandoffTaskTool::new(
            Arc::clone(&runtime),
            run_id.clone(),
            agent_id.clone(),
        )));
        registry.register(Box::new(SetTeamFactTool::new(
            Arc::clone(&runtime),
            run_id.clone(),
            agent_id.clone(),
        )));
        registry.register(Box::new(ReadTeamFactsTool::new(Arc::clone(&runtime), agent_id.clone())));
        registry.register(Box::new(RegisterArtifactTool::new(
            Arc::clone(&runtime),
            run_id,
            agent_id.clone(),
        )));
        registry.register(Box::new(ListArtifactsTool::new(Arc::clone(&runtime), agent_id)));
        registry.register(Box::new(GetTeamStateTool::new(runtime)));

        if !inherit_capabilities {
            let allowed: HashSet<String> = requested.iter().cloned().collect();
            registry.retain_names(&allowed);
        }
        register_tool_search_for_deferred_tools(&mut registry);
        registry
    }
}

fn register_tool_search_for_deferred_tools(registry: &mut ToolRegistry) {
    if !registry.to_tool_defs().iter().any(|tool| tool.deferred) {
        return;
    }
    registry.remove_names(&HashSet::from(["ToolSearch".to_owned()]));
    let snapshot = registry.to_tool_defs();
    if let Err(error) = registry.register_unique(Box::new(ToolSearchTool::new(snapshot))) {
        tracing::warn!(target: "solaris_agent", %error, "failed to register child ToolSearch");
    }
}
