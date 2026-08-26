use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use solaris_process::filter_resource_environment;

use crate::shell::default_shell;

/// Hook system configuration
#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq, Eq)]
pub struct HooksConfig {
    #[serde(default)]
    pub pre_tool_use: Vec<HookDef>,
    #[serde(default)]
    pub post_tool_use: Vec<HookDef>,
    #[serde(default)]
    pub stop: Vec<HookDef>,
}

/// A single hook definition
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct HookDef {
    pub name: String,
    /// Tool name patterns to match (glob). Empty = match all.
    #[serde(default)]
    pub tool_match: Vec<String>,
    /// File path patterns to match (glob). Empty = match all.
    #[serde(default)]
    pub file_match: Vec<String>,
    /// Shell command to execute. Supports ${VAR} interpolation.
    pub command: String,
    /// Timeout in ms (default 30000)
    #[serde(default = "default_hook_timeout")]
    pub timeout_ms: u64,
    /// Exact HTTP(S) destinations required by this hook in Auto mode.
    #[serde(default)]
    pub network: solaris_types::permission::ProcessNetworkConfig,
}

fn default_hook_timeout() -> u64 {
    30_000
}

/// Event-driven hook engine
pub struct HookEngine {
    config: HooksConfig,
    cwd: PathBuf,
    runtime_env: HashMap<String, String>,
    executor: Option<Arc<dyn HookExecutor>>,
}

#[derive(Debug, Clone)]
pub struct HookInvocation {
    /// Stable durable identity supplied by the parent tool call. Legacy and
    /// stop-hook callers may omit it when no durable parent exists.
    pub identity: Option<HookInvocationIdentity>,
    /// Exact configured definition used to detect changes during recovery.
    pub definition: HookDef,
    /// Logical hook input. Executors must persist only a digest of this value.
    pub effective_input: serde_json::Value,
    /// The parent tool already has a durable outcome. Executors may only
    /// reuse a matching hook outcome and must never start the hook late.
    pub completed_parent_recovery: bool,
    pub hook_name: String,
    pub command: String,
    pub executable: PathBuf,
    pub argv: Vec<String>,
    pub cwd: PathBuf,
    pub env: HashMap<String, String>,
    pub timeout_ms: u64,
    pub network: solaris_types::permission::ProcessNetworkConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookStage {
    PreToolUse,
    PostToolUse,
}

impl HookStage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PreToolUse => "pre_tool_use",
            Self::PostToolUse => "post_tool_use",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookInvocationIdentity {
    pub parent_call_id: String,
    pub stage: HookStage,
    pub ordinal: usize,
}

#[derive(Debug, Clone)]
pub struct HookStageInvocation {
    pub parent_call_id: String,
    pub stage: HookStage,
    pub manifest: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct HookExecutionResult {
    pub success: bool,
    pub output: String,
}

#[async_trait]
pub trait HookExecutor: Send + Sync {
    async fn execute(&self, invocation: HookInvocation) -> Result<HookExecutionResult, HookError>;

    async fn recover_for_completed_parent(&self, invocation: HookInvocation) -> Result<HookExecutionResult, HookError> {
        Err(HookError::OutcomeUnknown {
            hook_name: invocation.hook_name,
            reason: "hook executor cannot prove completion for an already completed parent tool call".to_owned(),
        })
    }

    async fn validate_stage(&self, _invocation: HookStageInvocation) -> Result<(), HookError> {
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum PreHookRunMode {
    Execute,
    RecoverCompletedParent,
}

impl HookEngine {
    pub fn new(config: HooksConfig, cwd: PathBuf) -> Self {
        Self {
            config,
            cwd,
            runtime_env: HashMap::new(),
            executor: None,
        }
    }

    pub fn new_with_env(config: HooksConfig, cwd: PathBuf, runtime_env: Vec<(String, String)>) -> Self {
        Self {
            config,
            cwd,
            runtime_env: filter_resource_environment(runtime_env).into_iter().collect(),
            executor: None,
        }
    }

    pub fn set_executor(&mut self, executor: Arc<dyn HookExecutor>) {
        self.executor = Some(executor);
    }

    /// Return the serializable hook configuration without exposing runtime state.
    pub fn config_snapshot(&self) -> HooksConfig {
        self.config.clone()
    }

    /// Replace configured hooks while preserving the runtime environment and executor.
    pub fn replace_config(&mut self, config: HooksConfig) {
        self.config = config;
    }

    /// Run pre-tool-use hooks. Returns Err if any hook blocks execution.
    pub async fn run_pre_tool_use(&self, tool_name: &str, tool_input: &serde_json::Value) -> Result<(), HookError> {
        self.run_pre_tool_use_inner(None, tool_name, tool_input, PreHookRunMode::Execute)
            .await
    }

    /// Run pre-tool hooks using an identity derived from the durable parent tool call.
    pub async fn run_pre_tool_use_for_call(
        &self,
        parent_call_id: &str,
        tool_name: &str,
        tool_input: &serde_json::Value,
    ) -> Result<(), HookError> {
        self.run_pre_tool_use_inner(Some(parent_call_id), tool_name, tool_input, PreHookRunMode::Execute)
            .await
    }

    /// Verify that every matching pre-tool hook completed before an already
    /// completed parent tool call. Missing history is never executed late.
    pub async fn reconcile_pre_tool_use_for_completed_call(
        &self,
        parent_call_id: &str,
        tool_name: &str,
        tool_input: &serde_json::Value,
    ) -> Result<(), HookError> {
        self.run_pre_tool_use_inner(
            Some(parent_call_id),
            tool_name,
            tool_input,
            PreHookRunMode::RecoverCompletedParent,
        )
        .await
    }

    async fn run_pre_tool_use_inner(
        &self,
        parent_call_id: Option<&str>,
        tool_name: &str,
        tool_input: &serde_json::Value,
        mode: PreHookRunMode,
    ) -> Result<(), HookError> {
        let effective_input = serde_json::json!({
            "tool_name": tool_name,
            "tool_input": tool_input,
        });
        let matching = self
            .config
            .pre_tool_use
            .iter()
            .enumerate()
            .filter(|(_, hook)| matches_tool(hook, tool_name, tool_input))
            .collect::<Vec<_>>();
        self.validate_durable_stage(parent_call_id, HookStage::PreToolUse, &matching, &effective_input)
            .await?;

        for (ordinal, hook) in matching {
            let env = self.build_hook_env(tool_name, tool_input);
            let identity = parent_call_id.map(|parent_call_id| HookInvocationIdentity {
                parent_call_id: parent_call_id.to_owned(),
                stage: HookStage::PreToolUse,
                ordinal,
            });
            let result = self
                .execute_hook(hook, env, identity, effective_input.clone(), mode)
                .await?;
            if !result.success {
                if matches!(mode, PreHookRunMode::RecoverCompletedParent) {
                    return Err(HookError::OutcomeUnknown {
                        hook_name: hook.name.clone(),
                        reason: "pre-tool hook failed even though its parent tool has a durable outcome".to_owned(),
                    });
                }
                return Err(HookError::Blocked {
                    hook_name: hook.name.clone(),
                    output: result.output,
                });
            }
        }
        Ok(())
    }

    /// Run post-tool-use hooks. Errors are logged but don't block.
    pub async fn run_post_tool_use(
        &self,
        tool_name: &str,
        tool_input: &serde_json::Value,
        tool_output: &str,
    ) -> Vec<String> {
        match self
            .run_post_tool_use_inner(None, tool_name, tool_input, tool_output)
            .await
        {
            Ok(messages) => messages,
            Err(error) => vec![format!("[hook] error: {error}")],
        }
    }

    /// Run post-tool hooks using an identity derived from the durable parent tool call.
    /// Durable history uncertainty is returned to the caller instead of being logged and ignored.
    pub async fn run_post_tool_use_for_call(
        &self,
        parent_call_id: &str,
        tool_name: &str,
        tool_input: &serde_json::Value,
        tool_output: &str,
    ) -> Result<Vec<String>, HookError> {
        self.run_post_tool_use_inner(Some(parent_call_id), tool_name, tool_input, tool_output)
            .await
    }

    async fn run_post_tool_use_inner(
        &self,
        parent_call_id: Option<&str>,
        tool_name: &str,
        tool_input: &serde_json::Value,
        tool_output: &str,
    ) -> Result<Vec<String>, HookError> {
        let effective_input = serde_json::json!({
            "tool_name": tool_name,
            "tool_input": tool_input,
            "tool_output": tool_output,
        });
        let matching = self
            .config
            .post_tool_use
            .iter()
            .enumerate()
            .filter(|(_, hook)| matches_tool(hook, tool_name, tool_input))
            .collect::<Vec<_>>();
        self.validate_durable_stage(parent_call_id, HookStage::PostToolUse, &matching, &effective_input)
            .await?;

        let mut messages = Vec::new();
        for (ordinal, hook) in matching {
            let mut env = self.build_hook_env(tool_name, tool_input);
            env.insert("TOOL_OUTPUT".to_string(), tool_output.to_string());
            let identity = parent_call_id.map(|parent_call_id| HookInvocationIdentity {
                parent_call_id: parent_call_id.to_owned(),
                stage: HookStage::PostToolUse,
                ordinal,
            });

            match self
                .execute_hook(hook, env, identity, effective_input.clone(), PreHookRunMode::Execute)
                .await
            {
                Ok(result) => {
                    if !result.output.is_empty() {
                        messages.push(format!("[hook:{}] {}", hook.name, result.output.trim()));
                    }
                }
                Err(error @ HookError::OutcomeUnknown { .. }) => return Err(error),
                Err(e) => {
                    messages.push(format!("[hook:{}] error: {}", hook.name, e));
                }
            }
        }
        Ok(messages)
    }

    /// Run stop hooks when agent session ends.
    pub async fn run_stop(&self) -> Vec<String> {
        let mut messages = Vec::new();
        for hook in &self.config.stop {
            match self
                .execute_hook(
                    hook,
                    self.runtime_env.clone(),
                    None,
                    serde_json::json!({"stage": "stop"}),
                    PreHookRunMode::Execute,
                )
                .await
            {
                Ok(result) => {
                    if !result.output.is_empty() {
                        messages.push(format!("[hook:{}] {}", hook.name, result.output.trim()));
                    }
                }
                Err(e) => {
                    messages.push(format!("[hook:{}] error: {}", hook.name, e));
                }
            }
        }
        messages
    }

    /// Check if any hooks are configured
    pub fn has_hooks(&self) -> bool {
        !self.config.pre_tool_use.is_empty() || !self.config.post_tool_use.is_empty() || !self.config.stop.is_empty()
    }

    /// Merge additional hooks into the engine's config, skipping duplicates by name.
    /// Used by SkillTool to register skill-specific hooks at invocation time (idempotent).
    pub fn merge_hooks(&mut self, additional: HooksConfig) {
        merge_vec(&mut self.config.pre_tool_use, additional.pre_tool_use);
        merge_vec(&mut self.config.post_tool_use, additional.post_tool_use);
        merge_vec(&mut self.config.stop, additional.stop);
    }

    fn build_hook_env(&self, tool_name: &str, tool_input: &serde_json::Value) -> HashMap<String, String> {
        let mut env = self.runtime_env.clone();
        env.extend(build_env_vars(tool_name, tool_input));
        env
    }

    async fn validate_durable_stage(
        &self,
        parent_call_id: Option<&str>,
        stage: HookStage,
        matching: &[(usize, &HookDef)],
        effective_input: &serde_json::Value,
    ) -> Result<(), HookError> {
        let Some(parent_call_id) = parent_call_id else {
            return Ok(());
        };
        let executor = self.executor.as_ref().ok_or(HookError::EffectExecutorUnavailable)?;
        let hooks = matching
            .iter()
            .map(|(ordinal, hook)| {
                serde_json::json!({
                    "ordinal": ordinal,
                    "definition": hook,
                })
            })
            .collect::<Vec<_>>();
        executor
            .validate_stage(HookStageInvocation {
                parent_call_id: parent_call_id.to_owned(),
                stage,
                manifest: serde_json::json!({
                    "schema": "solaris/hook-stage-manifest/v1",
                    "stage": stage.as_str(),
                    "hooks": hooks,
                    "effective_input": effective_input,
                }),
            })
            .await
    }

    async fn execute_hook(
        &self,
        hook: &HookDef,
        env: HashMap<String, String>,
        identity: Option<HookInvocationIdentity>,
        effective_input: serde_json::Value,
        mode: PreHookRunMode,
    ) -> Result<HookExecutionResult, HookError> {
        let executor = self.executor.as_ref().ok_or(HookError::EffectExecutorUnavailable)?;
        let command = interpolate_command(&hook.command, &env);
        let shell = default_shell();
        let invocation = HookInvocation {
            identity,
            definition: hook.clone(),
            effective_input,
            completed_parent_recovery: matches!(mode, PreHookRunMode::RecoverCompletedParent),
            hook_name: hook.name.clone(),
            argv: shell.derive_exec_args(&command, false),
            executable: shell.path,
            command,
            cwd: self.cwd.clone(),
            env,
            timeout_ms: hook.timeout_ms,
            network: hook.network.clone(),
        };
        match mode {
            PreHookRunMode::Execute => executor.execute(invocation).await,
            PreHookRunMode::RecoverCompletedParent => executor.recover_for_completed_parent(invocation).await,
        }
    }
}

/// Append `incoming` hooks into `existing`, skipping any whose name already exists.
fn merge_vec(existing: &mut Vec<HookDef>, incoming: Vec<HookDef>) {
    for hook in incoming {
        if !existing.iter().any(|h| h.name == hook.name) {
            existing.push(hook);
        }
    }
}

/// Environment variables available to hook commands
fn build_env_vars(tool_name: &str, tool_input: &serde_json::Value) -> HashMap<String, String> {
    let mut env = HashMap::new();
    env.insert("TOOL_NAME".to_string(), tool_name.to_string());
    env.insert("TOOL_INPUT".to_string(), tool_input.to_string());

    // Extract common fields for convenience
    if let Some(fp) = tool_input["file_path"].as_str() {
        env.insert("TOOL_INPUT_FILE_PATH".to_string(), fp.to_string());
    }
    if let Some(cmd) = tool_input["command"].as_str() {
        env.insert("TOOL_INPUT_COMMAND".to_string(), cmd.to_string());
    }
    if let Some(pattern) = tool_input["pattern"].as_str() {
        env.insert("TOOL_INPUT_PATTERN".to_string(), pattern.to_string());
    }

    env
}

fn matches_tool(hook: &HookDef, tool_name: &str, tool_input: &serde_json::Value) -> bool {
    // Check tool_match
    if !hook.tool_match.is_empty() {
        let matches = hook.tool_match.iter().any(|pattern| glob_match(pattern, tool_name));
        if !matches {
            return false;
        }
    }

    // Check file_match (if tool has a file_path input)
    if !hook.file_match.is_empty() {
        if let Some(file_path) = tool_input["file_path"].as_str() {
            let matches = hook.file_match.iter().any(|pattern| glob_match(pattern, file_path));
            if !matches {
                return false;
            }
        } else {
            return false; // file_match specified but tool has no file_path
        }
    }

    true
}

fn glob_match(pattern: &str, value: &str) -> bool {
    glob::Pattern::new(pattern).map(|p| p.matches(value)).unwrap_or(false)
}

/// Interpolate ${VAR} in a command string with provided env vars
fn interpolate_command(command: &str, env_vars: &HashMap<String, String>) -> String {
    let mut result = command.to_string();
    for (key, value) in env_vars {
        result = result.replace(&format!("${{{}}}", key), value);
    }
    result
}

#[derive(Debug, thiserror::Error)]
pub enum HookError {
    #[error("Hook '{hook_name}' blocked execution: {output}")]
    Blocked { hook_name: String, output: String },
    #[error("Hook execution failed: {0}")]
    ExecutionFailed(String),
    #[error("Hook '{hook_name}' outcome is unknown and requires reconciliation: {reason}")]
    OutcomeUnknown { hook_name: String, reason: String },
    #[error("Hook timed out after {timeout_ms}ms\n{output}")]
    Timeout { timeout_ms: u64, output: String },
    #[error("Hook execution requires an EffectRequest executor")]
    EffectExecutorUnavailable,
}

impl HookError {
    pub fn is_outcome_unknown(&self) -> bool {
        matches!(self, Self::OutcomeUnknown { .. })
    }
}

#[cfg(test)]
#[path = "hooks_test.rs"]
mod hooks_test;
