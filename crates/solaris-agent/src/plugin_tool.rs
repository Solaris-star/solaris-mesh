use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;

use solaris_process::{
    CommandRunner, ExecutableIdentity, PinnedExecutable, ProcessLaunchPolicy, ProcessSpawnAuthorization,
    inspect_executable, pin_executable,
};
use solaris_protocol::events::ToolCategory;
use solaris_tools::{PreparedToolEffect, PreparedToolExecution, Tool, ToolExecutionContext};
use solaris_types::effect::{EffectClass, EffectDescriptor, EffectReplayPolicy, ProcessInvocation, ResourceFootprint};
use solaris_types::permission::PermissionDecision;
use solaris_types::plugin::{
    ImplementationIdentity, PluginCommandContributionDefinition, PluginCommandToolDefinition, PluginContributionKind,
    PluginProviderCommandRequest, ResolvedPluginDefinition,
};
use solaris_types::tool::{JsonSchema, ToolResult};

use crate::execution_context::{
    EffectExecutionContext, EffectOutcomeGuard, EffectRecoveryDecision, stable_digest_value,
};
use crate::plugin_runtime::PluginRuntime;
use crate::schema_validation::validate_value;

pub struct PluginCommandTool {
    plugin_id: String,
    definition: PluginCommandToolDefinition,
    executable: PathBuf,
    approved_executable: ExecutableIdentity,
    executable_identity: ImplementationIdentity,
}

impl PluginCommandTool {
    pub fn from_resolved(
        plugin: &ResolvedPluginDefinition,
        definition: PluginCommandToolDefinition,
    ) -> Result<Self, String> {
        let executable = resolve_plugin_executable(plugin, &definition.command)?;
        let approved_executable = inspect_executable(&executable).map_err(|error| error.to_string())?;
        let executable_identity = plugin_executable_identity(&approved_executable);
        Ok(Self {
            plugin_id: plugin.definition.id.clone(),
            definition,
            executable,
            approved_executable,
            executable_identity,
        })
    }

    fn rendered_effect(&self, input: &Value) -> EffectDescriptor {
        let mut effect = self.definition.effect.clone();
        if matches!(
            effect.class,
            EffectClass::ReadOnly | EffectClass::AgentLifecycle | EffectClass::MeshStateMutation
        ) {
            effect.class = EffectClass::Process;
        }
        effect.action = render_template(&effect.action, input);
        for value in &mut effect.resources.file_reads {
            *value = render_template(value, input);
        }
        for value in &mut effect.resources.file_writes {
            *value = render_template(value, input);
        }
        for value in &mut effect.resources.network_domains {
            *value = render_template(value, input);
        }
        for value in &mut effect.resources.process_commands {
            *value = render_template(value, input);
        }
        for value in &mut effect.resources.external_resources {
            *value = render_template(value, input);
        }
        let executable = self.executable.to_string_lossy().into_owned();
        if !effect
            .resources
            .process_commands
            .iter()
            .any(|value| value == &executable)
        {
            effect.resources.process_commands.push(executable);
        }
        effect.resources.process_invocations.push(ProcessInvocation {
            executable: self.executable.to_string_lossy().into_owned(),
            argv: self.definition.args.clone(),
        });
        effect
            .resources
            .external_resources
            .push(executable_resource(&self.executable_identity));
        effect
    }

    pub fn execution_boundary_descriptor(&self) -> EffectDescriptor {
        plugin_execution_boundary_descriptor(
            &self.plugin_id,
            &self.executable,
            &self.executable_identity,
            &self.definition.args,
        )
    }

    fn pin_for_execution(&self) -> Result<PinnedExecutable, String> {
        pin_executable(&self.executable, &self.approved_executable).map_err(|error| error.to_string())
    }

    async fn execute_pinned(
        &self,
        input: Value,
        pinned_executable: PinnedExecutable,
        launch: PluginProcessLaunch,
    ) -> ToolResult {
        let output = match run_plugin_command(
            &self.plugin_id,
            pinned_executable,
            &self.definition.args,
            input,
            self.definition.max_result_size,
            self.definition.timeout_ms,
            launch,
        )
        .await
        {
            Ok(output) => output,
            Err(error) => {
                return ToolResult {
                    content: error,
                    is_error: true,
                };
            }
        };
        if let Ok(result) = serde_json::from_str::<ToolResult>(&output.stdout) {
            return result;
        }
        let content = if output.stderr.is_empty() {
            output.stdout
        } else if output.stdout.is_empty() {
            output.stderr
        } else {
            format!("{}\nSTDERR:\n{}", output.stdout, output.stderr)
        };
        ToolResult {
            content,
            is_error: output.exit_code != Some(0),
        }
    }
}

pub struct PluginCommandContribution {
    plugin_id: String,
    implementation: ImplementationIdentity,
    definition: PluginCommandContributionDefinition,
    executable: PathBuf,
    approved_executable: ExecutableIdentity,
    executable_identity: ImplementationIdentity,
    provider_command_v1: bool,
}

impl PluginCommandContribution {
    pub fn from_resolved(
        plugin: &ResolvedPluginDefinition,
        definition: PluginCommandContributionDefinition,
    ) -> Result<Self, String> {
        let executable = resolve_plugin_executable(plugin, &definition.command)?;
        let approved_executable = inspect_executable(&executable).map_err(|error| error.to_string())?;
        let executable_identity = plugin_executable_identity(&approved_executable);
        Ok(Self {
            plugin_id: plugin.definition.id.clone(),
            implementation: plugin.identity.implementation.clone(),
            definition,
            executable,
            approved_executable,
            executable_identity,
            provider_command_v1: plugin
                .definition
                .compatibility
                .required_protocols
                .iter()
                .any(|protocol| protocol == PluginProviderCommandRequest::PROTOCOL),
        })
    }

    pub fn kind(&self) -> PluginContributionKind {
        self.definition.kind
    }

    pub fn name(&self) -> &str {
        &self.definition.name
    }

    pub fn input_schema(&self) -> &Value {
        &self.definition.input_schema
    }

    pub fn capability(&self) -> String {
        format!("{}:{}", self.definition.kind.capability_prefix(), self.definition.name)
    }

    fn supports_provider_command_v1(&self) -> bool {
        self.kind() == PluginContributionKind::Provider && self.provider_command_v1
    }

    pub fn execution_boundary_descriptor(&self) -> EffectDescriptor {
        plugin_execution_boundary_descriptor(
            &self.plugin_id,
            &self.executable,
            &self.executable_identity,
            &self.definition.args,
        )
    }

    /// Invoke a command-backed contribution through the same effect boundary
    /// used by first-party tools. No Host handles or environment secrets are
    /// injected into the child process.
    pub async fn invoke_authorized(&self, input: Value, context: &EffectExecutionContext) -> Result<Value, String> {
        validate_value(&input, &self.definition.input_schema)
            .map_err(|error| format!("plugin {} contribution input invalid: {error}", self.plugin_id))?;
        let capability = self.capability();
        let call_id = format!(
            "plugin-contribution:{}:{capability}:{}",
            self.plugin_id,
            stable_digest_value(&input)
        );
        let mut resources = ResourceFootprint {
            process_commands: vec![self.executable.to_string_lossy().into_owned()],
            process_invocations: vec![ProcessInvocation {
                executable: self.executable.to_string_lossy().into_owned(),
                argv: self.definition.args.clone(),
            }],
            external_resources: vec![
                format!("plugin:{}:{capability}", self.plugin_id),
                executable_resource(&self.executable_identity),
            ],
            ..Default::default()
        };
        resources.declare_sandboxed_process_access();
        let request = context.effect_request(
            &call_id,
            &capability,
            &input,
            EffectDescriptor {
                class: EffectClass::Process,
                action: format!("invoke plugin {} contribution {capability}", self.plugin_id),
                resources,
                replay_policy: EffectReplayPolicy::Never,
            },
        );
        let approved_mode = context.permissions().mode();
        let approved_ceiling = context.permissions().ceiling();
        match context.recover_effect(&request)? {
            EffectRecoveryDecision::Execute => {}
            EffectRecoveryDecision::Reuse {
                is_error: false,
                output,
            } => {
                return serde_json::from_str(&output)
                    .map_err(|error| format!("recovered plugin contribution output is invalid JSON: {error}"));
            }
            EffectRecoveryDecision::Reuse { is_error: true, output } => return Err(output),
            EffectRecoveryDecision::Reconcile { reason } => return Err(reason),
        }
        context.ensure_runtime_budget_available()?;
        let approved_environment = context.environment();
        let _permit = context.acquire_effect_permit().await?;
        if let Err(error) = validate_executable_identity(&self.executable, &self.executable_identity) {
            context
                .record_revalidation_failure(&request, &error)
                .map_err(|record_error| record_error.to_string())?;
            return Err(error);
        }
        let evaluation = context.evaluate(&request);
        context
            .record_permission_decision(&request, &evaluation, "plugin_contribution")
            .map_err(|error| error.to_string())?;
        if evaluation.decision != PermissionDecision::Allow {
            return Err(format!("plugin contribution permission denied: {}", evaluation.reason));
        }
        let evaluation = match context.revalidate_external(&request, &approved_environment, &self.implementation) {
            Ok(evaluation) => evaluation,
            Err(error) => {
                context
                    .record_revalidation_failure(&request, &error)
                    .map_err(|record_error| record_error.to_string())?;
                return Err(error);
            }
        };
        let launch_policy = context
            .revalidate_configured_effect_before_execution(
                &request,
                &approved_environment,
                approved_mode,
                approved_ceiling,
                true,
            )?
            .ok_or_else(|| "plugin contribution process launch policy unavailable".to_owned())?;
        let boundary = context.permissions().boundary();
        let workspace_root = match boundary.writable_roots.as_slice() {
            [workspace_root] => Some(PathBuf::from(workspace_root)),
            _ => None,
        };
        let spawn_authorization = context.process_spawn_authorization(
            request.clone(),
            approved_environment.clone(),
            approved_mode,
            approved_ceiling,
            workspace_root,
            evaluation.matched_lease,
            Some(self.implementation.clone()),
        )?;
        if let Err(error) = validate_executable_identity(&self.executable, &self.executable_identity) {
            context
                .record_revalidation_failure(&request, &error)
                .map_err(|record_error| record_error.to_string())?;
            return Err(error);
        }
        let pinned_executable =
            pin_executable(&self.executable, &self.approved_executable).map_err(|error| error.to_string())?;
        let pinned_identity = plugin_executable_identity(pinned_executable.identity());
        let mut intent_request = request.clone();
        intent_request
            .descriptor
            .resources
            .external_resources
            .retain(|resource| resource != &executable_resource(&self.executable_identity));
        intent_request
            .descriptor
            .resources
            .external_resources
            .push(executable_resource(&pinned_identity));
        context
            .record_effect_intent(&intent_request)
            .map_err(|error| error.to_string())?;
        let mut outcome_guard = EffectOutcomeGuard::new(
            context.clone(),
            intent_request,
            format!(
                "plugin {} contribution cancelled before a terminal result",
                self.plugin_id
            ),
        );

        let result = match run_plugin_command(
            &self.plugin_id,
            pinned_executable,
            &self.definition.args,
            input,
            self.definition.max_result_size,
            self.definition.timeout_ms,
            PluginProcessLaunch::new(launch_policy, spawn_authorization),
        )
        .await
        {
            Err(error) => Err(error),
            Ok(output) if output.exit_code != Some(0) => Err(if output.stderr.is_empty() {
                format!("plugin {} contribution failed", self.plugin_id)
            } else {
                output.stderr
            }),
            Ok(output) => serde_json::from_str(&output.stdout)
                .map_err(|error| format!("plugin {} returned invalid JSON: {error}", self.plugin_id)),
        };
        let outcome = match &result {
            Ok(value) => serde_json::to_string(value).unwrap_or_default(),
            Err(error) => error.clone(),
        };
        outcome_guard
            .complete(result.is_err(), &outcome)
            .map_err(|error| error.to_string())?;
        result
    }
}

#[derive(Clone)]
pub struct PluginContributionDispatcher {
    runtime: Arc<PluginRuntime>,
    context: EffectExecutionContext,
    scope_id: String,
}

impl PluginContributionDispatcher {
    pub fn new(runtime: Arc<PluginRuntime>, context: EffectExecutionContext, scope_id: impl Into<String>) -> Self {
        Self {
            runtime,
            context,
            scope_id: scope_id.into(),
        }
    }

    pub async fn invoke(&self, kind: PluginContributionKind, name: &str, input: Value) -> Result<Value, String> {
        self.invoke_scoped(kind, name, input, None).await
    }

    pub(crate) fn supports_provider_command_v1(&self, name: &str) -> bool {
        self.runtime
            .resolve_command_contribution(&self.scope_id, &format!("provider:{name}"))
            .is_some_and(|contribution| contribution.supports_provider_command_v1())
    }

    pub async fn invoke_scoped(
        &self,
        kind: PluginContributionKind,
        name: &str,
        input: Value,
        workflow: Option<ImplementationIdentity>,
    ) -> Result<Value, String> {
        let capability = format!("{}:{name}", kind.capability_prefix());
        let contribution = self
            .runtime
            .resolve_command_contribution(&self.scope_id, &capability)
            .ok_or_else(|| format!("plugin contribution {capability} is not active in {}", self.scope_id))?;
        if contribution.kind() != kind {
            return Err(format!(
                "plugin contribution {capability} resolved with a different kind"
            ));
        }
        let context = workflow.map_or_else(
            || self.context.clone(),
            |identity| self.context.scoped_to_workflow(identity),
        );
        contribution.invoke_authorized(input, &context).await
    }
}

struct PluginCommandOutput {
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
}

struct PluginProcessLaunch {
    policy: ProcessLaunchPolicy,
    authorization: ProcessSpawnAuthorization,
}

impl PluginProcessLaunch {
    fn new(policy: ProcessLaunchPolicy, authorization: ProcessSpawnAuthorization) -> Self {
        Self { policy, authorization }
    }
}

async fn run_plugin_command(
    plugin_id: &str,
    executable: PinnedExecutable,
    args: &[String],
    input: Value,
    max_stdout_bytes: usize,
    timeout_ms: u64,
    launch: PluginProcessLaunch,
) -> Result<PluginCommandOutput, String> {
    let timeout_ms = timeout_ms.max(1);
    let mut command = executable.command().map_err(|error| error.to_string())?;
    command.args(args);
    let payload = serde_json::to_vec(&input).map_err(|error| format!("plugin input serialization failed: {error}"))?;
    let output = CommandRunner::new_pinned(command)
        .launch_policy(launch.policy)
        .spawn_authorizer(launch.authorization)
        .stdin_bytes(payload)
        .timeout(Duration::from_millis(timeout_ms))
        .max_output_bytes(max_stdout_bytes.max(1))
        .run()
        .await
        .map_err(|error| format!("plugin {plugin_id} execution failed: {error}"))?;
    if output.timed_out {
        return Err(format!("plugin {plugin_id} timed out after {timeout_ms}ms"));
    }
    if output.output_limit_exceeded {
        return Err(format!("plugin {plugin_id} output exceeded {max_stdout_bytes} bytes"));
    }
    Ok(PluginCommandOutput {
        exit_code: output.exit_code,
        stdout: String::from_utf8_lossy(&output.stdout).trim().to_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
    })
}

fn resolve_plugin_executable(plugin: &ResolvedPluginDefinition, command: &str) -> Result<PathBuf, String> {
    let authority = plugin
        .authority_root
        .as_deref()
        .ok_or_else(|| format!("plugin {} has no materialized authority root", plugin.definition.id))?;
    let authority = Path::new(authority)
        .canonicalize()
        .map_err(|error| format!("invalid plugin authority root: {error}"))?;
    let requested = PathBuf::from(command);
    let executable = if requested.is_absolute() {
        requested
    } else {
        authority.join(requested)
    };
    let executable = executable
        .canonicalize()
        .map_err(|error| format!("failed to resolve plugin executable {}: {error}", executable.display()))?;
    if !executable.starts_with(&authority) {
        return Err(format!(
            "plugin executable {} escapes authority root {}",
            executable.display(),
            authority.display()
        ));
    }
    if !executable.is_file() {
        return Err(format!("plugin executable is not a file: {}", executable.display()));
    }
    Ok(executable)
}

fn plugin_executable_identity(identity: &ExecutableIdentity) -> ImplementationIdentity {
    ImplementationIdentity {
        implementation_id: format!("plugin-executable:{}", identity.path_digest()),
        version: None,
        digest: Some(identity.content_digest().to_owned()),
    }
}

#[cfg(test)]
fn executable_identity(path: &Path) -> ImplementationIdentity {
    inspect_executable(path)
        .map(|identity| plugin_executable_identity(&identity))
        .unwrap_or_else(|error| ImplementationIdentity {
            implementation_id: format!(
                "plugin-executable-error:{}",
                crate::execution_context::stable_digest_bytes(error.to_string().as_bytes())
            ),
            version: None,
            digest: None,
        })
}

fn executable_resource(identity: &ImplementationIdentity) -> String {
    format!(
        "{}:{}",
        identity.implementation_id,
        identity.digest.as_deref().unwrap_or("missing")
    )
}

fn plugin_execution_boundary_descriptor(
    plugin_id: &str,
    executable: &Path,
    identity: &ImplementationIdentity,
    args: &[String],
) -> EffectDescriptor {
    let mut resources = ResourceFootprint {
        process_commands: vec![executable.to_string_lossy().into_owned()],
        process_invocations: vec![ProcessInvocation {
            executable: executable.to_string_lossy().into_owned(),
            argv: args.to_vec(),
        }],
        external_resources: vec![format!("plugin:{plugin_id}:"), executable_resource(identity)],
        ..Default::default()
    };
    resources.declare_sandboxed_process_access();
    EffectDescriptor {
        class: EffectClass::Process,
        action: format!("execute activated plugin {plugin_id}"),
        resources,
        replay_policy: EffectReplayPolicy::Never,
    }
}

fn validate_executable_identity(path: &Path, expected: &ImplementationIdentity) -> Result<(), String> {
    let current = inspect_executable(path)
        .map(|identity| plugin_executable_identity(&identity))
        .map_err(|error| error.to_string())?;
    if current == *expected {
        Ok(())
    } else {
        Err("plugin executable implementation changed before execution".to_owned())
    }
}

#[async_trait]
impl Tool for PluginCommandTool {
    fn name(&self) -> &str {
        &self.definition.name
    }

    fn description(&self) -> &str {
        &self.definition.description
    }

    fn input_schema(&self) -> JsonSchema {
        self.definition.input_schema.clone()
    }

    fn is_concurrency_safe(&self, _input: &Value) -> bool {
        self.definition.concurrency_safe
    }

    async fn execute(&self, input: Value) -> ToolResult {
        let _ = input;
        ToolResult {
            content: "Plugin command requires an approved process spawn authorization".to_owned(),
            is_error: true,
        }
    }

    fn prepare_effect(&self, effect_id: &str, input: &Value) -> Result<PreparedToolEffect, String> {
        Ok(PreparedToolEffect::new(
            self.rendered_effect(input),
            ToolExecutionContext::new(effect_id).with_prepared_implementation(self.executable_identity.clone()),
        ))
    }

    fn prepare_execution<'a>(
        &'a self,
        input: Value,
        context: solaris_tools::ToolExecutionContext,
    ) -> Result<PreparedToolExecution<'a>, String> {
        let spawn_authorization = context
            .process_spawn_authorization()
            .cloned()
            .ok_or_else(|| "approved process spawn authorization is missing".to_owned())?;
        let launch_policy = context
            .process_launch_policy()
            .cloned()
            .unwrap_or(ProcessLaunchPolicy::Ambient);
        let pinned_executable = self.pin_for_execution()?;
        let implementation = plugin_executable_identity(pinned_executable.identity());
        Ok(PreparedToolExecution::new(
            Some(implementation),
            Box::pin(self.execute_pinned(
                input,
                pinned_executable,
                PluginProcessLaunch::new(launch_policy, spawn_authorization),
            )),
        ))
    }

    fn describe_effect(&self, input: &Value) -> EffectDescriptor {
        self.rendered_effect(input)
    }

    fn implementation_identity(&self) -> Option<ImplementationIdentity> {
        Some(self.executable_identity.clone())
    }

    fn max_result_size(&self) -> usize {
        self.definition.max_result_size
    }

    fn category(&self) -> ToolCategory {
        match self.definition.effect.class {
            EffectClass::ReadOnly | EffectClass::AgentLifecycle | EffectClass::MeshStateMutation => ToolCategory::Info,
            EffectClass::WorkspaceMutation => ToolCategory::Edit,
            EffectClass::Process | EffectClass::Network | EffectClass::ExternalSideEffect => ToolCategory::Exec,
        }
    }

    fn describe(&self, input: &Value) -> String {
        format!("Plugin {}: {}", self.plugin_id, self.rendered_effect(input).action)
    }
}

fn render_template(template: &str, input: &Value) -> String {
    let mut rendered = template.to_owned();
    let Some(object) = input.as_object() else {
        return rendered;
    };
    for (key, value) in object {
        let replacement = value.as_str().map(str::to_owned).unwrap_or_else(|| value.to_string());
        rendered = rendered.replace(&format!("${{input.{key}}}"), &replacement);
    }
    rendered
}

#[cfg(test)]
#[path = "plugin_tool_test.rs"]
mod plugin_tool_test;
