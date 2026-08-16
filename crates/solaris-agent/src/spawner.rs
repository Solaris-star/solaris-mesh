use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::task::JoinSet;

use solaris_config::config::Config;
use solaris_providers::LlmProvider;
use solaris_tools::edit::EditTool;
use solaris_tools::exec_command::ExecCommandTool;
use solaris_tools::glob::GlobTool;
use solaris_tools::grep::GrepTool;
use solaris_tools::read::ReadTool;
use solaris_tools::registry::ToolRegistry;
use solaris_tools::write::WriteTool;
use solaris_types::message::TokenUsage;

use crate::engine::AgentEngine;
use crate::output::OutputSink;
use crate::output::null_sink::NullSink;
use crate::resource_policy::ResourcePolicy;
use crate::scheduler::{ScheduledTask, Scheduler};

// Re-export from solaris-types — single source of truth
pub use solaris_types::spawner::{ForkOverrides, Spawner, SubAgentConfig, SubAgentResult};

/// Spawns independent child agents that share the parent's LLM provider.
///
/// Sub-agents use a [`NullSink`] so their streaming output is silently
/// discarded.  Results are collected via `engine.run()` and returned to the
/// parent which emits them as a single `tool_result` event — matching the
/// Claude Code pattern where only the parent writes to stdout.
pub struct AgentSpawner {
    provider: Arc<dyn LlmProvider>,
    base_config: Config,
    cwd: PathBuf,
    runtime_env: Vec<(String, String)>,
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
        Self {
            provider,
            base_config: config,
            cwd,
            runtime_env,
        }
    }

    /// Spawn a single sub-agent and wait for result.
    pub async fn spawn_one(&self, sub_config: SubAgentConfig) -> SubAgentResult {
        let mut config = self.base_config.clone();
        config.max_turns = Some(sub_config.max_turns);
        config.max_tokens = Some(sub_config.max_tokens);
        if let Some(sp) = sub_config.system_prompt.clone() {
            config.system_prompt = Some(sp);
        }
        config.session.enabled = false;
        config.tools.auto_approve = true;

        tracing::info!(target: "solaris_agent", cwd = %self.cwd.display(), "sub-agent spawned with workspace cwd");

        let tools = build_tool_registry(&[], &self.cwd, &self.runtime_env);
        let output: Arc<dyn OutputSink> = Arc::new(NullSink);
        let mut engine = AgentEngine::new_with_provider_and_env(
            self.provider.clone(),
            config,
            tools,
            output,
            self.cwd.clone(),
            self.runtime_env.clone(),
        );

        match engine.run(&sub_config.prompt, "").await {
            Ok(result) => SubAgentResult {
                name: sub_config.name,
                text: result.text,
                usage: result.usage,
                turns: result.turns,
                is_error: false,
            },
            Err(e) => SubAgentResult {
                name: sub_config.name,
                text: format!("Sub-agent error: {}", e),
                usage: TokenUsage::default(),
                turns: 0,
                is_error: true,
            },
        }
    }

    /// Spawn multiple sub-agents through the Mesh scheduler.
    pub async fn spawn_parallel(&self, sub_configs: Vec<SubAgentConfig>) -> Vec<SubAgentResult> {
        let configured_max = self
            .runtime_env
            .iter()
            .find(|(k, _)| k == "SOLARIS_MAX_ACTIVE_AGENTS")
            .and_then(|(_, v)| v.parse::<usize>().ok());
        let mut scheduler = Scheduler::new(ResourcePolicy::from_system(configured_max));
        for (index, config) in sub_configs.into_iter().enumerate() {
            scheduler.enqueue(ScheduledTask {
                id: index.to_string(),
                payload: config,
            });
        }

        let mut join_set = JoinSet::new();
        let mut results = Vec::new();
        loop {
            while let Some(task) = scheduler.acquire_next() {
                let spawner = self.clone_for_spawn();
                join_set.spawn(async move {
                    (
                        task.id.parse::<usize>().unwrap_or(usize::MAX),
                        spawner.spawn_one(task.payload).await,
                    )
                });
            }
            if let Some(joined) = join_set.join_next().await {
                scheduler.release();
                match joined {
                    Ok(result) => results.push(result),
                    Err(e) => results.push((
                        usize::MAX,
                        SubAgentResult {
                            name: "unknown".to_string(),
                            text: format!("Task join error: {}", e),
                            usage: TokenUsage::default(),
                            turns: 0,
                            is_error: true,
                        },
                    )),
                }
            } else {
                break;
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
        }
    }
}

#[async_trait]
impl Spawner for AgentSpawner {
    async fn spawn_fork(&self, sub_config: SubAgentConfig, overrides: ForkOverrides) -> SubAgentResult {
        let mut config = self.base_config.clone();
        config.max_turns = Some(sub_config.max_turns);
        config.max_tokens = Some(sub_config.max_tokens);
        if let Some(sp) = sub_config.system_prompt.clone() {
            config.system_prompt = Some(sp);
        }
        config.session.enabled = false;
        config.tools.auto_approve = true;
        if let Some(model) = overrides.model.clone() {
            config.model = model;
        }

        let tools = build_tool_registry(&overrides.allowed_tools, &self.cwd, &self.runtime_env);
        let output: Arc<dyn OutputSink> = Arc::new(NullSink);
        let mut engine = AgentEngine::new_with_provider_and_env(
            self.provider.clone(),
            config,
            tools,
            output,
            self.cwd.clone(),
            self.runtime_env.clone(),
        );
        engine.set_initial_reasoning_effort(overrides.effort.clone());

        match engine.run(&sub_config.prompt, "").await {
            Ok(result) => SubAgentResult {
                name: sub_config.name,
                text: result.text,
                usage: result.usage,
                turns: result.turns,
                is_error: false,
            },
            Err(e) => SubAgentResult {
                name: sub_config.name,
                text: format!("Sub-agent error: {}", e),
                usage: TokenUsage::default(),
                turns: 0,
                is_error: true,
            },
        }
    }
}

fn build_tool_registry(allowed: &[String], cwd: &Path, runtime_env: &[(String, String)]) -> ToolRegistry {
    let all_tools: Vec<(&str, Box<dyn solaris_tools::Tool>)> = vec![
        ("Read", Box::new(ReadTool::new(None))),
        ("Write", Box::new(WriteTool::new(None))),
        ("Edit", Box::new(EditTool::new(None))),
        (
            "ExecCommand",
            Box::new(ExecCommandTool::new_with_env(cwd.to_path_buf(), runtime_env.to_vec())),
        ),
        ("Grep", Box::new(GrepTool::new(cwd.to_path_buf()))),
        ("Glob", Box::new(GlobTool::new(cwd.to_path_buf()))),
    ];

    let mut registry = ToolRegistry::new();
    for (name, tool) in all_tools {
        if allowed.is_empty() || allowed.iter().any(|a| a.as_str() == name) {
            registry.register(tool);
        }
    }
    registry
}

#[cfg(test)]
#[path = "spawner_test.rs"]
mod spawner_test;
