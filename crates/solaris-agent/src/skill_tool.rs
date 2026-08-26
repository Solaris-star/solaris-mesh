use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use serde_json::{Value, json};

use crate::spawner::Spawner;
use solaris_config::hooks::HooksConfig;
use solaris_process::{
    CommandRunner, ExecutableError, ExecutableIdentity, PinnedExecutable, ProcessLaunchPolicy,
    ProcessSpawnAuthorization, inspect_executable,
};
use solaris_protocol::events::ToolCategory;
use solaris_skills::context_modifier::ContextModifier;
use solaris_skills::executor::{
    execute_fork_with_shell, prepare_inline_content_with_shell, substituted_inline_content,
};
use solaris_skills::hooks::{parse_skill_hooks, to_hook_defs};
use solaris_skills::permissions::{SkillPermission, SkillPermissionChecker};
use solaris_skills::shell::{ShellExecutionError, SkillShellExecutor, contains_shell_commands, shell_commands};
use solaris_skills::types::{ExecutionContext, LoadedFrom, SkillMetadata};
use solaris_types::effect::{EffectClass, EffectDescriptor, EffectReplayPolicy, ProcessInvocation, ResourceFootprint};
use solaris_types::plugin::ImplementationIdentity;
use solaris_types::tool::{JsonSchema, ToolResult};

use solaris_tools::{PreparedToolEffect, PreparedToolExecution, Tool, ToolExecutionContext};

use crate::execution_context::{EffectExecutionContext, stable_digest_bytes, stable_digest_value};

/// A tool that allows the LLM to invoke named skills.
///
/// Each skill is looked up by name (exact match, leading `/` stripped),
/// its content is prepared with variable substitution and shell execution,
/// and returned as a `ToolResult`.  The Skill list is injected into the
/// system prompt in Phase 9; this tool's `description()` returns a fixed string.
#[derive(Clone)]
pub struct SkillTool {
    skills: SharedSkillCatalog,
    /// Working directory for shell command execution inside skill content.
    cwd: PathBuf,
    /// Permission checker for skill-level deny/allow rules.
    checker: SkillPermissionChecker,
    /// Session ID passed to prepare_inline_content for ${SOLARIS_SESSION_ID} substitution.
    /// None if sessions are disabled or not yet initialised.
    session_id: Option<String>,
    /// Spawner for fork-mode skills. None when SkillTool is built without fork support.
    spawner: Option<Arc<dyn Spawner>>,
    /// Executes embedded shell only after a standard Process EffectRequest has been authorized.
    shell_executor: Option<Arc<dyn SkillShellExecutor>>,
}

#[derive(Clone)]
pub(crate) struct SharedSkillCatalog {
    skills: Arc<RwLock<Vec<SkillMetadata>>>,
}

impl SharedSkillCatalog {
    pub(crate) fn new(skills: Arc<Vec<SkillMetadata>>) -> Self {
        Self {
            skills: Arc::new(RwLock::new(skills.as_ref().clone())),
        }
    }

    fn find(&self, name: &str) -> Option<SkillMetadata> {
        self.skills
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .find(|skill| skill.name == name)
            .cloned()
    }

    fn snapshot(&self) -> Vec<SkillMetadata> {
        self.skills.read().unwrap_or_else(|error| error.into_inner()).clone()
    }

    pub(crate) fn remove_mcp(&self) -> usize {
        let mut skills = self.skills.write().unwrap_or_else(|error| error.into_inner());
        let before = skills.len();
        skills.retain(|skill| skill.loaded_from != LoadedFrom::Mcp);
        before.saturating_sub(skills.len())
    }
}

impl SkillTool {
    pub fn new(skills: Arc<Vec<SkillMetadata>>, cwd: PathBuf, checker: SkillPermissionChecker) -> Self {
        Self {
            skills: SharedSkillCatalog::new(skills),
            cwd,
            checker,
            session_id: None,
            spawner: None,
            shell_executor: None,
        }
    }

    /// Create a SkillTool with a known session ID.
    pub fn with_session_id(
        skills: Arc<Vec<SkillMetadata>>,
        cwd: PathBuf,
        checker: SkillPermissionChecker,
        session_id: Option<String>,
    ) -> Self {
        Self {
            skills: SharedSkillCatalog::new(skills),
            cwd,
            checker,
            session_id,
            spawner: None,
            shell_executor: None,
        }
    }

    /// Create a SkillTool with full fork-mode support.
    pub fn with_spawner(
        skills: Arc<Vec<SkillMetadata>>,
        cwd: PathBuf,
        checker: SkillPermissionChecker,
        session_id: Option<String>,
        spawner: Option<Arc<dyn Spawner>>,
    ) -> Self {
        Self {
            skills: SharedSkillCatalog::new(skills),
            cwd,
            checker,
            session_id,
            spawner,
            shell_executor: None,
        }
    }

    pub(crate) fn with_shared_catalog_and_spawner(
        skills: SharedSkillCatalog,
        cwd: PathBuf,
        checker: SkillPermissionChecker,
        session_id: Option<String>,
        spawner: Option<Arc<dyn Spawner>>,
    ) -> Self {
        Self {
            skills,
            cwd,
            checker,
            session_id,
            spawner,
            shell_executor: None,
        }
    }

    pub(crate) fn with_shell_executor(mut self, shell_executor: Arc<dyn SkillShellExecutor>) -> Self {
        self.shell_executor = Some(shell_executor);
        self
    }

    /// Find a skill by exact name (case-sensitive, leading `/` stripped).
    fn find_skill(&self, name: &str) -> Option<SkillMetadata> {
        let name = name.trim_start_matches('/');
        self.skills.find(name)
    }

    /// Build a comma-separated list of available skill names for error messages.
    fn available_names(&self) -> String {
        self.skills
            .snapshot()
            .iter()
            .map(|skill| skill.name.clone())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

#[derive(Clone)]
pub(crate) struct EffectSkillShellExecutor {
    _private: (),
}

impl EffectSkillShellExecutor {
    pub(crate) fn new(_context: EffectExecutionContext) -> Self {
        Self { _private: () }
    }
}

fn sanitized_executable_error(error: &ExecutableError) -> String {
    format!(
        "Skill shell executable rejected: {} ({})",
        error.category(),
        error.identity_digest()
    )
}

fn skill_shell_executable_identity() -> Result<ExecutableIdentity, String> {
    let shell = solaris_config::shell::default_shell();
    inspect_skill_shell(&shell.path)
}

fn inspect_skill_shell(path: &Path) -> Result<ExecutableIdentity, String> {
    inspect_executable(path).map_err(|error| sanitized_executable_error(&error))
}

fn safe_shell_descriptor(
    commands: &[String],
    identity: &ExecutableIdentity,
    network_domains: &[String],
) -> (Vec<String>, EffectDescriptor) {
    let shell = solaris_config::shell::default_shell();
    let executable = identity.canonical_path();
    let executable_digest = identity.content_digest();
    let mut invocations = Vec::with_capacity(commands.len());
    let mut resources = Vec::with_capacity(commands.len() + 1);
    resources.push(format!("skill-shell-executable:sha256:{executable_digest}"));
    let mut digests = Vec::with_capacity(commands.len());
    for command in commands {
        let digest = stable_digest_bytes(command.as_bytes());
        let mut argv = shell.derive_exec_args("<redacted>", false);
        if let Some(last) = argv.last_mut() {
            *last = format!("sha256:{digest}");
        }
        invocations.push(ProcessInvocation {
            executable: executable.to_string_lossy().into_owned(),
            argv,
        });
        resources.push(format!("skill-shell:{digest}"));
        digests.push(digest);
    }
    let mut footprint = ResourceFootprint {
        process_commands: vec![executable.to_string_lossy().into_owned()],
        process_invocations: invocations,
        external_resources: resources,
        ..Default::default()
    };
    footprint.declare_sandboxed_process_access();
    footprint.network_domains.extend(network_domains.iter().cloned());
    (
        digests,
        EffectDescriptor {
            class: EffectClass::Process,
            action: format!("Run {} embedded Skill shell command(s)", commands.len()),
            resources: footprint,
            replay_policy: EffectReplayPolicy::Never,
        },
    )
}

fn skill_shell_implementation(identity: &ExecutableIdentity) -> ImplementationIdentity {
    ImplementationIdentity {
        implementation_id: format!("skill-shell:{}", identity.path_digest()),
        version: None,
        digest: Some(identity.content_digest().to_owned()),
    }
}

struct PreparedSkillShellExecutor {
    executables: Mutex<VecDeque<PinnedExecutable>>,
    launch_policy: ProcessLaunchPolicy,
    spawn_authorization: ProcessSpawnAuthorization,
}

struct SkillShellOutput {
    exit_code: Option<i32>,
    timed_out: bool,
    output_limit_exceeded: bool,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

// One durable Process intent grants exactly one root spawn attempt. Supporting
// multiple substitutions safely requires a pinned batch helper with framed IPC;
// cloning a one-time spawn credential is not permitted.
const MAX_SKILL_SHELL_COMMANDS: usize = 1;
const MAX_SKILL_RESULT_BYTES: usize = 1024 * 1024;
const MAX_SKILL_SHELL_OUTPUT_BYTES: usize = 128 * 1024;

async fn run_skill_shell_command(
    executable: PinnedExecutable,
    args: &[String],
    cwd: &Path,
    launch_policy: ProcessLaunchPolicy,
    spawn_authorization: ProcessSpawnAuthorization,
) -> Result<SkillShellOutput, String> {
    let mut process = executable.command().map_err(|error| error.to_string())?;
    process.args(args).current_dir(cwd).kill_on_drop(true);
    let output = CommandRunner::new_pinned(process)
        .launch_policy(launch_policy)
        .spawn_authorizer(spawn_authorization)
        .timeout(std::time::Duration::from_secs(30))
        .max_output_bytes(MAX_SKILL_SHELL_OUTPUT_BYTES)
        .run()
        .await
        .map_err(|error| error.to_string())?;
    Ok(SkillShellOutput {
        exit_code: output.exit_code,
        timed_out: output.timed_out,
        output_limit_exceeded: output.output_limit_exceeded,
        stdout: output.stdout,
        stderr: output.stderr,
    })
}

#[async_trait]
impl SkillShellExecutor for EffectSkillShellExecutor {
    async fn execute(&self, _command: &str, _cwd: &std::path::Path) -> Result<String, ShellExecutionError> {
        Err(ShellExecutionError::CommandFailed {
            pattern: "skill shell".to_owned(),
            output: "Skill shell execution requires an approved prepared effect".to_owned(),
        })
    }
}

#[async_trait]
impl SkillShellExecutor for PreparedSkillShellExecutor {
    async fn execute(&self, command: &str, cwd: &Path) -> Result<String, ShellExecutionError> {
        let pinned = self
            .executables
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .pop_front()
            .ok_or_else(|| ShellExecutionError::CommandFailed {
                pattern: "skill shell executable".to_owned(),
                output: "Skill shell prepared executable unavailable".to_owned(),
            })?;
        let cwd = cwd.canonicalize().map_err(|error| ShellExecutionError::CommandFailed {
            pattern: "skill shell cwd".to_owned(),
            output: format!(
                "Skill shell cwd rejected ({})",
                stable_digest_bytes(error.to_string().as_bytes())
            ),
        })?;
        let args = solaris_config::shell::default_shell().derive_exec_args(command, false);
        let output = run_skill_shell_command(
            pinned,
            &args,
            &cwd,
            self.launch_policy.clone(),
            self.spawn_authorization.clone(),
        )
        .await
        .map_err(|output| ShellExecutionError::CommandFailed {
            pattern: "skill shell".to_owned(),
            output,
        })?;
        if output.timed_out {
            return Err(ShellExecutionError::CommandFailed {
                pattern: "skill shell".to_owned(),
                output: "Skill shell command timed out after 30000ms".to_owned(),
            });
        }
        if output.output_limit_exceeded {
            return Err(ShellExecutionError::CommandFailed {
                pattern: "skill shell".to_owned(),
                output: format!("combined output exceeded {MAX_SKILL_SHELL_OUTPUT_BYTES} bytes"),
            });
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let combined = match (stdout.is_empty(), stderr.is_empty()) {
            (false, false) => format!("{}\n[stderr]\n{}", stdout.trim_end(), stderr.trim_end()),
            (false, true) => stdout.trim_end().to_owned(),
            (true, false) => format!("[stderr]\n{}", stderr.trim_end()),
            (true, true) => String::new(),
        };
        if output.exit_code == Some(0) {
            Ok(combined)
        } else {
            Err(ShellExecutionError::CommandFailed {
                pattern: "skill shell".to_owned(),
                output: if combined.is_empty() {
                    format!("Skill shell exited with {:?}", output.exit_code)
                } else {
                    combined
                },
            })
        }
    }
}

#[async_trait]
impl Tool for SkillTool {
    fn name(&self) -> &str {
        "Skill"
    }

    fn description(&self) -> &str {
        "Invoke a named skill by name. \
         Use the skill name exactly as listed in the system prompt. \
         Optionally pass arguments as a single string."
    }

    fn input_schema(&self) -> JsonSchema {
        json!({
            "type": "object",
            "properties": {
                "skill": {
                    "type": "string",
                    "description": "The skill name. E.g., \"commit\", \"review-pr\", or \"pdf\""
                },
                "args": {
                    "type": "string",
                    "description": "Optional arguments for the skill"
                }
            },
            "required": ["skill"]
        })
    }

    fn is_concurrency_safe(&self, _input: &Value) -> bool {
        // Skills may modify context; conservatively mark as not concurrency-safe.
        false
    }

    async fn execute(&self, input: Value) -> ToolResult {
        let Some(skill_name) = input["skill"].as_str() else {
            return ToolResult {
                content: "Missing required parameter: skill".to_string(),
                is_error: true,
            };
        };

        let skill = match self.find_skill(skill_name) {
            Some(s) => s,
            None => {
                let available = self.available_names();
                return ToolResult {
                    content: format!("Skill '{}' not found. Available skills: {}", skill_name, available),
                    is_error: true,
                };
            }
        };

        // Check skill-level permissions (applies to both inline and fork modes).
        match self.checker.check(&skill) {
            SkillPermission::Deny => {
                return ToolResult {
                    content: format!("Skill '{}' is denied by configuration.", skill.name),
                    is_error: true,
                };
            }
            SkillPermission::Ask { reason } => {
                if self.shell_executor.is_none() || !contains_shell_commands(&skill.content) {
                    return ToolResult {
                        content: format!(
                            "Skill '{}' requires user approval before execution. \
                             {} \
                             Please ask the user to approve this skill in their configuration.",
                            skill.name, reason
                        ),
                        is_error: true,
                    };
                }
            }
            SkillPermission::Allow => {}
        }

        let args = input["args"].as_str();
        let prepared_content = substituted_inline_content(&skill, args, self.session_id.as_deref());
        let command_count = shell_commands(&prepared_content).len();
        if command_count > MAX_SKILL_SHELL_COMMANDS {
            return ToolResult {
                content: format!(
                    "Skill '{}' contains {command_count} embedded shell commands; the limit is {MAX_SKILL_SHELL_COMMANDS}.",
                    skill.name
                ),
                is_error: true,
            };
        }

        match skill.execution_context {
            ExecutionContext::Inline => {
                match prepare_inline_content_with_shell(
                    &skill,
                    args,
                    self.session_id.as_deref(),
                    &self.cwd,
                    self.shell_executor.as_deref(),
                )
                .await
                {
                    Ok(content) if content.len() <= MAX_SKILL_RESULT_BYTES => ToolResult {
                        content,
                        is_error: false,
                    },
                    Ok(content) => ToolResult {
                        content: format!(
                            "Skill '{}' result exceeded {MAX_SKILL_RESULT_BYTES} bytes ({} bytes).",
                            skill.name,
                            content.len()
                        ),
                        is_error: true,
                    },
                    Err(e) => ToolResult {
                        content: e.to_string(),
                        is_error: true,
                    },
                }
            }
            ExecutionContext::Fork => {
                let spawner = match self.spawner.as_ref() {
                    Some(s) => s.as_ref(),
                    None => {
                        return ToolResult {
                            content: format!(
                                "Skill '{}' requires fork execution context, \
                                 but no AgentSpawner is available. \
                                 Fork support is enabled via SkillTool::with_spawner().",
                                skill.name
                            ),
                            is_error: true,
                        };
                    }
                };
                match execute_fork_with_shell(
                    &skill,
                    args,
                    self.session_id.as_deref(),
                    &self.cwd,
                    spawner,
                    self.shell_executor.as_deref(),
                )
                .await
                {
                    Ok(content) if content.len() <= MAX_SKILL_RESULT_BYTES => ToolResult {
                        content,
                        is_error: false,
                    },
                    Ok(content) => ToolResult {
                        content: format!(
                            "Skill '{}' result exceeded {MAX_SKILL_RESULT_BYTES} bytes ({} bytes).",
                            skill.name,
                            content.len()
                        ),
                        is_error: true,
                    },
                    Err(e) => ToolResult {
                        content: e,
                        is_error: true,
                    },
                }
            }
        }
    }

    fn prepare_effect(&self, effect_id: &str, input: &Value) -> Result<PreparedToolEffect, String> {
        let Some(skill_name) = input.get("skill").and_then(Value::as_str) else {
            return Ok(PreparedToolEffect::new(
                unknown_skill_effect(),
                ToolExecutionContext::new(effect_id),
            ));
        };
        let Some(skill) = self.find_skill(skill_name) else {
            return Ok(PreparedToolEffect::new(
                unknown_skill_effect(),
                ToolExecutionContext::new(effect_id),
            ));
        };
        let content = substituted_inline_content(
            &skill,
            input.get("args").and_then(Value::as_str),
            self.session_id.as_deref(),
        );
        let commands = shell_commands(&content);
        if commands.len() > MAX_SKILL_SHELL_COMMANDS {
            return Err(format!(
                "Skill '{}' contains {} embedded shell commands; the limit is {MAX_SKILL_SHELL_COMMANDS}.",
                skill.name,
                commands.len()
            ));
        }
        if commands.is_empty() || skill.loaded_from == solaris_skills::types::LoadedFrom::Mcp {
            return Ok(PreparedToolEffect::new(
                EffectDescriptor::read_only(format!("Load Skill {}", skill.name)),
                ToolExecutionContext::new(effect_id),
            ));
        }
        let identity = skill_shell_executable_identity()?;
        let (_, descriptor) = safe_shell_descriptor(&commands, &identity, &skill.network.network_domains);
        let implementation = skill_shell_implementation(&identity);
        Ok(PreparedToolEffect::new(
            descriptor,
            ToolExecutionContext::new(effect_id)
                .with_approved_executable(identity)
                .with_prepared_implementation(implementation),
        ))
    }

    fn revalidate_effect(&self, input: &Value, context: &ToolExecutionContext) -> EffectDescriptor {
        let Some(identity) = context.approved_executable() else {
            return self.describe_effect(input);
        };
        let Some(skill_name) = input.get("skill").and_then(Value::as_str) else {
            return unknown_skill_effect();
        };
        let Some(skill) = self.find_skill(skill_name) else {
            return unknown_skill_effect();
        };
        let content = substituted_inline_content(
            &skill,
            input.get("args").and_then(Value::as_str),
            self.session_id.as_deref(),
        );
        let commands = shell_commands(&content);
        safe_shell_descriptor(&commands, identity, &skill.network.network_domains).1
    }

    fn prepare_execution<'a>(
        &'a self,
        input: Value,
        context: ToolExecutionContext,
    ) -> Result<PreparedToolExecution<'a>, String> {
        let command_count = input
            .get("skill")
            .and_then(Value::as_str)
            .and_then(|name| self.find_skill(name))
            .map(|skill| {
                let content = substituted_inline_content(
                    &skill,
                    input.get("args").and_then(Value::as_str),
                    self.session_id.as_deref(),
                );
                shell_commands(&content).len()
            })
            .unwrap_or(0);
        if command_count > MAX_SKILL_SHELL_COMMANDS {
            return Err(format!(
                "Skill contains {command_count} embedded shell commands; the limit is {MAX_SKILL_SHELL_COMMANDS}."
            ));
        }
        if command_count == 0 {
            return Ok(PreparedToolExecution::new(None, Box::pin(self.execute(input))));
        }
        let identity = context
            .approved_executable()
            .cloned()
            .ok_or_else(|| "approved Skill shell executable identity is missing".to_owned())?;
        let executables = (0..command_count)
            .map(|_| {
                solaris_process::pin_executable(identity.canonical_path(), &identity)
                    .map_err(|error| sanitized_executable_error(&error))
            })
            .collect::<Result<VecDeque<_>, _>>()?;
        let spawn_authorization = context
            .process_spawn_authorization()
            .cloned()
            .ok_or_else(|| "approved process spawn authorization is missing".to_owned())?;
        let launch_policy = context
            .process_launch_policy()
            .cloned()
            .unwrap_or(ProcessLaunchPolicy::Ambient);
        let mut prepared_tool = self.clone();
        prepared_tool.shell_executor = Some(Arc::new(PreparedSkillShellExecutor {
            executables: Mutex::new(executables),
            launch_policy,
            spawn_authorization,
        }));
        Ok(PreparedToolExecution::new(
            Some(skill_shell_implementation(&identity)),
            Box::pin(async move { prepared_tool.execute(input).await }),
        ))
    }

    fn context_modifier_for(&self, input: &serde_json::Value) -> Option<ContextModifier> {
        let skill_name = input["skill"].as_str()?;
        let skill = self.find_skill(skill_name)?;
        // Fork skills run in their own sub-agent context; modifiers must not
        // propagate back to the parent conversation.
        if skill.execution_context == ExecutionContext::Fork {
            return None;
        }
        solaris_skills::context_modifier::from_skill(&skill)
    }

    fn skill_hooks_for(&self, input: &serde_json::Value) -> Option<HooksConfig> {
        let skill_name = input["skill"].as_str()?;
        let skill = self.find_skill(skill_name)?;
        let config = parse_skill_hooks(skill.hooks_raw.as_ref(), &skill.name, skill.source)?;
        Some(to_hook_defs(&config, &skill.name))
    }

    fn category(&self) -> ToolCategory {
        // Inline mode returns skill content for the model to act on — categorised
        // as Info since it does not directly modify files or run commands.
        ToolCategory::Info
    }

    fn describe_effect(&self, input: &Value) -> EffectDescriptor {
        let Some(skill_name) = input.get("skill").and_then(Value::as_str) else {
            return unknown_skill_effect();
        };
        let Some(skill) = self.find_skill(skill_name) else {
            return unknown_skill_effect();
        };
        let content = substituted_inline_content(
            &skill,
            input.get("args").and_then(Value::as_str),
            self.session_id.as_deref(),
        );
        let commands = shell_commands(&content);
        if commands.is_empty() || skill.loaded_from == solaris_skills::types::LoadedFrom::Mcp {
            return EffectDescriptor::read_only(format!("Load Skill {}", skill.name));
        }
        skill_shell_executable_identity()
            .map(|identity| safe_shell_descriptor(&commands, &identity, &skill.network.network_domains).1)
            .unwrap_or_else(|_| unknown_skill_effect())
    }

    fn implementation_identity(&self) -> Option<ImplementationIdentity> {
        let shell = solaris_config::shell::default_shell();
        let shell_digest = inspect_executable(&shell.path)
            .ok()
            .map(|identity| identity.content_digest().to_owned());
        Some(ImplementationIdentity {
            implementation_id: "builtin:skill-tool".to_owned(),
            version: Some(env!("CARGO_PKG_VERSION").to_owned()),
            digest: Some(stable_digest_value(&json!({
                "skills": self.skills.snapshot().iter().map(|skill| json!({
                    "name": skill.name,
                    "description": skill.description,
                    "allowed_tools": skill.allowed_tools,
                    "argument_hint": skill.argument_hint,
                    "argument_names": skill.argument_names,
                    "when_to_use": skill.when_to_use,
                    "version": skill.version,
                    "model": skill.model,
                    "disable_model_invocation": skill.disable_model_invocation,
                    "user_invocable": skill.user_invocable,
                    "content": skill.content,
                    "loaded_from": format!("{:?}", skill.loaded_from),
                    "source": format!("{:?}", skill.source),
                    "execution_context": format!("{:?}", skill.execution_context),
                    "agent": skill.agent,
                    "effort": skill.effort.map(|effort| format!("{effort:?}")),
                    "skill_shell": skill.shell,
                    "paths": skill.paths,
                    "network": skill.network,
                    "hooks": skill.hooks_raw,
                    "skill_root": skill.skill_root,
                })).collect::<Vec<_>>(),
                "shell": shell.path,
                "shell_digest": shell_digest,
            }))),
        })
    }

    fn describe(&self, input: &Value) -> String {
        let name = input.get("skill").and_then(|v| v.as_str()).unwrap_or("?");
        match input.get("args").and_then(|v| v.as_str()) {
            Some(args) if !args.is_empty() => format!("Skill {name} {args}"),
            _ => format!("Skill {name}"),
        }
    }
}

fn unknown_skill_effect() -> EffectDescriptor {
    EffectDescriptor {
        class: EffectClass::ExternalSideEffect,
        action: "Invoke unknown Skill".to_owned(),
        resources: ResourceFootprint {
            external_resources: vec!["tool:Skill".to_owned()],
            ..Default::default()
        },
        replay_policy: EffectReplayPolicy::ReconcileRequired,
    }
}

#[cfg(test)]
#[path = "skill_tool_test.rs"]
mod skill_tool_test;
