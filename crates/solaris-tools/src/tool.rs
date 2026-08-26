use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;

use async_trait::async_trait;
use serde_json::Value;

use solaris_config::hooks::HooksConfig;
use solaris_protocol::events::ToolCategory;
use solaris_types::effect::{EffectClass, EffectDescriptor, EffectReplayPolicy, ResourceFootprint};
use solaris_types::permission::PermissionMode;
use solaris_types::plugin::ImplementationIdentity;
use solaris_types::skill_types::ContextModifier;
use solaris_types::tool::{ClassifiedToolResult, JsonSchema, ToolResult, ToolResultStatus};

use solaris_process::{ExecutableIdentity, ProcessLaunchPolicy, ProcessSpawnAuthorization};

#[derive(Clone, Debug)]
pub(crate) struct ReadOnlyEvidenceScope {
    pub(crate) authorization_digest: String,
    pub(crate) environment_digest: String,
}

type LegacyToolExecutionFuture<'a> = Pin<Box<dyn Future<Output = ToolResult> + Send + 'a>>;
type ToolExecutionFuture<'a> = Pin<Box<dyn Future<Output = ClassifiedToolResult> + Send + 'a>>;

/// A tool execution prepared before durable effect intent is recorded.
///
/// Implementations may keep pinned executable handles or other revalidated
/// resources alive in the returned future until execution completes.
pub struct PreparedToolExecution<'a> {
    implementation: Option<ImplementationIdentity>,
    execution: ToolExecutionFuture<'a>,
}

/// Permission-time effect description and engine metadata derived together.
pub struct PreparedToolEffect {
    descriptor: EffectDescriptor,
    execution_context: ToolExecutionContext,
}

impl PreparedToolEffect {
    pub fn new(descriptor: EffectDescriptor, execution_context: ToolExecutionContext) -> Self {
        Self {
            descriptor,
            execution_context,
        }
    }

    pub fn into_parts(self) -> (EffectDescriptor, ToolExecutionContext) {
        (self.descriptor, self.execution_context)
    }
}

/// Engine-owned metadata for one approved tool execution.
///
/// This is kept separate from model-provided JSON input so tools cannot
/// accidentally observe internal fields through their public input schema.
#[derive(Clone, Debug)]
pub struct ToolExecutionContext {
    effect_id: String,
    approved_executable: Option<ExecutableIdentity>,
    approved_process_arguments: Option<Vec<String>>,
    approved_executable_kind: Option<String>,
    prepared_implementation: Option<ImplementationIdentity>,
    sandbox_workspace_root: Option<PathBuf>,
    process_launch_policy: Option<ProcessLaunchPolicy>,
    process_spawn_authorization: Option<ProcessSpawnAuthorization>,
    permission_mode: Option<PermissionMode>,
    read_only_evidence_scope: Option<ReadOnlyEvidenceScope>,
}

impl ToolExecutionContext {
    pub fn new(effect_id: impl Into<String>) -> Self {
        Self {
            effect_id: effect_id.into(),
            approved_executable: None,
            approved_process_arguments: None,
            approved_executable_kind: None,
            prepared_implementation: None,
            sandbox_workspace_root: None,
            process_launch_policy: None,
            process_spawn_authorization: None,
            permission_mode: None,
            read_only_evidence_scope: None,
        }
    }

    pub fn effect_id(&self) -> &str {
        &self.effect_id
    }

    pub(crate) fn with_approved_process(
        mut self,
        identity: ExecutableIdentity,
        arguments: Vec<String>,
        executable_kind: String,
    ) -> Self {
        self.prepared_implementation = Some(ImplementationIdentity {
            implementation_id: format!("exec-shell:{}", identity.path_digest()),
            version: None,
            digest: Some(identity.content_digest().to_owned()),
        });
        self.approved_executable = Some(identity);
        self.approved_process_arguments = Some(arguments);
        self.approved_executable_kind = Some(executable_kind);
        self
    }

    /// Retains a permission-time executable identity for a tool implemented in
    /// another crate.
    pub fn with_approved_executable(mut self, identity: ExecutableIdentity) -> Self {
        self.approved_executable = Some(identity);
        self
    }

    pub fn with_prepared_implementation(mut self, implementation: ImplementationIdentity) -> Self {
        self.prepared_implementation = Some(implementation);
        self
    }

    pub fn with_sandbox_workspace_root(mut self, workspace_root: impl Into<PathBuf>) -> Self {
        self.sandbox_workspace_root = Some(workspace_root.into());
        self
    }

    pub fn with_process_launch_policy(mut self, policy: ProcessLaunchPolicy) -> Self {
        self.process_launch_policy = Some(policy);
        self
    }

    pub fn with_process_spawn_authorization(mut self, authorization: ProcessSpawnAuthorization) -> Self {
        self.process_spawn_authorization = Some(authorization);
        self
    }

    /// Records the permission mode selected during execution revalidation.
    pub fn with_permission_mode(mut self, mode: PermissionMode) -> Self {
        self.permission_mode = Some(mode);
        self
    }

    /// Binds read-only evidence reuse to the effective authorization and
    /// operation environment selected during execution revalidation.
    pub fn with_read_only_evidence_scope(
        mut self,
        authorization_digest: impl Into<String>,
        environment_digest: impl Into<String>,
    ) -> Self {
        self.read_only_evidence_scope = Some(ReadOnlyEvidenceScope {
            authorization_digest: authorization_digest.into(),
            environment_digest: environment_digest.into(),
        });
        self
    }

    pub fn sandbox_workspace_root(&self) -> Option<&Path> {
        self.sandbox_workspace_root.as_deref()
    }

    pub fn prepared_implementation(&self) -> Option<&ImplementationIdentity> {
        self.prepared_implementation.as_ref()
    }

    pub fn approved_executable(&self) -> Option<&ExecutableIdentity> {
        self.approved_executable.as_ref()
    }

    pub(crate) fn approved_process_arguments(&self) -> Option<&[String]> {
        self.approved_process_arguments.as_deref()
    }

    pub(crate) fn approved_executable_kind(&self) -> Option<&str> {
        self.approved_executable_kind.as_deref()
    }

    pub fn process_launch_policy(&self) -> Option<&ProcessLaunchPolicy> {
        self.process_launch_policy.as_ref()
    }

    pub fn process_spawn_authorization(&self) -> Option<&ProcessSpawnAuthorization> {
        self.process_spawn_authorization.as_ref()
    }

    pub(crate) fn permission_mode(&self) -> Option<PermissionMode> {
        self.permission_mode
    }

    pub(crate) fn read_only_evidence_scope(&self) -> Option<&ReadOnlyEvidenceScope> {
        self.read_only_evidence_scope.as_ref()
    }
}

impl<'a> PreparedToolExecution<'a> {
    pub fn new(implementation: Option<ImplementationIdentity>, execution: LegacyToolExecutionFuture<'a>) -> Self {
        Self {
            implementation,
            execution: Box::pin(async move { execution.await.into() }),
        }
    }

    pub fn new_classified(implementation: Option<ImplementationIdentity>, execution: ToolExecutionFuture<'a>) -> Self {
        Self {
            implementation,
            execution,
        }
    }

    pub fn implementation(&self) -> Option<&ImplementationIdentity> {
        self.implementation.as_ref()
    }

    pub async fn execute(self) -> ToolResult {
        self.execution.await.into_legacy()
    }

    pub async fn execute_classified(self) -> ClassifiedToolResult {
        self.execution.await
    }
}

/// Truncate a string to at most `max_bytes`, snapping to a char boundary.
pub fn truncate_utf8(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// A tool that the agent can invoke
#[async_trait]
pub trait Tool: Send + Sync {
    /// Tool name (must match API schema)
    fn name(&self) -> &str;

    /// Stable key used by permission rules, leases, and durable recovery.
    ///
    /// Most tools have one fixed registered name, so the default preserves
    /// existing behavior. Tools whose display name may change must override
    /// this with an identity that is independent of registration collisions.
    fn permission_capability(&self) -> &str {
        self.name()
    }

    /// Human-readable description for the LLM
    fn description(&self) -> &str;

    /// JSON Schema for input parameters
    fn input_schema(&self) -> JsonSchema;

    /// Whether this tool is safe to run concurrently
    fn is_concurrency_safe(&self, input: &Value) -> bool;

    /// Execute the tool
    async fn execute(&self, input: Value) -> ToolResult;

    /// Execute while retaining the full terminal status used by the runtime.
    async fn execute_classified(&self, input: Value) -> ClassifiedToolResult {
        let result = self.execute(input.clone()).await;
        let status = self.classify_result(&input, &result);
        result.classified(status)
    }

    /// Classify a tool-specific successful result beyond the legacy boolean.
    fn classify_result(&self, _input: &Value, result: &ToolResult) -> ToolResultStatus {
        result.inferred_status()
    }

    /// Notify the tool that a result produced from `input` was removed from
    /// the model-visible conversation history.
    ///
    /// Stateful tools may override this to invalidate only cache entries that
    /// depended on the removed result. The default is intentionally a no-op.
    fn on_result_compacted(&self, _input: &Value) {}

    /// Notify the tool that the detailed conversation history was replaced by
    /// a compact summary.
    ///
    /// Stateful tools may override this to clear caches whose validity relies
    /// on model-visible tool results. The default is intentionally a no-op.
    fn on_history_compacted(&self) {}

    /// Prepare the exact execution that will run after durable intent.
    ///
    /// The default preserves existing tool behavior without performing work
    /// before intent. Executable-backed tools override this to pin and retain
    /// the approved implementation in the returned future.
    fn prepare_effect(&self, effect_id: &str, input: &Value) -> Result<PreparedToolEffect, String> {
        Ok(PreparedToolEffect::new(
            self.describe_effect(input),
            ToolExecutionContext::new(effect_id),
        ))
    }

    /// Rebuild the approved descriptor without resampling resources retained in
    /// the permission-time context.
    fn revalidate_effect(&self, input: &Value, _context: &ToolExecutionContext) -> EffectDescriptor {
        self.describe_effect(input)
    }

    fn prepare_execution<'a>(
        &'a self,
        input: Value,
        _context: ToolExecutionContext,
    ) -> Result<PreparedToolExecution<'a>, String> {
        Ok(PreparedToolExecution::new_classified(
            None,
            Box::pin(self.execute_classified(input)),
        ))
    }

    /// Return an optional context modifier based on the tool input.
    /// Called after execute() to collect any engine-level overrides.
    /// Only SkillTool overrides this; all other tools return None.
    fn context_modifier_for(&self, _input: &Value) -> Option<ContextModifier> {
        None
    }

    /// Return any hooks declared in the skill's frontmatter for dynamic registration.
    /// Called after a successful execute() so the orchestration layer can merge
    /// the returned hooks into the active HookEngine.
    /// Only SkillTool overrides this; all other tools return None.
    fn skill_hooks_for(&self, _input: &Value) -> Option<HooksConfig> {
        None
    }

    /// Describe the semantic side effect before permission checks and execution.
    ///
    /// Unknown/plugin tools default to the most conservative external-side-effect
    /// classification. Built-ins and trusted plugins should override this with a
    /// precise resource footprint.
    fn describe_effect(&self, input: &Value) -> EffectDescriptor {
        EffectDescriptor {
            class: EffectClass::ExternalSideEffect,
            action: self.describe(input),
            resources: ResourceFootprint {
                external_resources: vec![format!("tool:{}", self.name())],
                ..Default::default()
            },
            replay_policy: EffectReplayPolicy::ReconcileRequired,
        }
    }

    /// Return a stable identity for implementations that can change independently
    /// of the Solaris binary, such as command-backed plugins.
    fn implementation_identity(&self) -> Option<ImplementationIdentity> {
        None
    }

    /// Max result size in chars before truncation
    fn max_result_size(&self) -> usize {
        50_000
    }

    /// Tool category for protocol classification
    fn category(&self) -> ToolCategory;

    /// Whether this tool's schema should be deferred (sent as name-only stub).
    /// Override to `true` for tools with large schemas or infrequent use.
    fn is_deferred(&self) -> bool {
        false
    }

    /// Human-readable description of what the tool will do with the given input
    fn describe(&self, input: &Value) -> String {
        format!("{}: {}", self.name(), serde_json::to_string(input).unwrap_or_default())
    }
}

#[cfg(test)]
#[path = "tool_test.rs"]
mod tool_test;
