use async_trait::async_trait;
use solaris_config::hooks::{HookError, HookExecutionResult, HookExecutor, HookInvocation, HookStageInvocation};
use solaris_process::{
    ProcessFinalizationError, inspect_executable, process_outcome_unknown, process_recovery_required,
};
use solaris_types::effect::{EffectClass, EffectDescriptor, EffectReplayPolicy, ProcessInvocation, ResourceFootprint};
use solaris_types::permission::{PermissionDecision, PermissionRule};

use crate::execution_context::{
    EffectExecutionContext, EffectOutcomeGuard, EffectRecoveryDecision, pin_executable, stable_digest_value,
};
use crate::permission_engine::PermissionContext;

struct HookPermissionRegistration<'a> {
    permissions: &'a PermissionContext,
    source: String,
}

impl Drop for HookPermissionRegistration<'_> {
    fn drop(&mut self) {
        self.permissions.revoke_configured_effect_from(&self.source);
        self.permissions.remove_generated_rules(&self.source);
    }
}

pub(crate) struct EffectHookExecutor {
    context: EffectExecutionContext,
    permission_registration: tokio::sync::Mutex<()>,
}

impl EffectHookExecutor {
    pub(crate) fn new(context: EffectExecutionContext) -> Self {
        Self {
            context,
            permission_registration: tokio::sync::Mutex::new(()),
        }
    }

    fn call_id(&self, invocation: &HookInvocation, input: &serde_json::Value) -> Result<String, HookError> {
        if let Some(identity) = &invocation.identity {
            let parent_digest = stable_digest_value(&serde_json::json!({
                "parent_call_id": identity.parent_call_id,
            }));
            return Ok(format!(
                "hook:{}:{}:{parent_digest}",
                identity.stage.as_str(),
                identity.ordinal
            ));
        }

        let base = format!("hook:{}:{}", invocation.hook_name, stable_digest_value(input));
        self.context
            .stable_effect_attempt_id(&base)
            .map_err(HookError::ExecutionFailed)
    }

    fn outcome_unknown(invocation: &HookInvocation, reason: impl Into<String>) -> HookError {
        HookError::OutcomeUnknown {
            hook_name: invocation.hook_name.clone(),
            reason: reason.into(),
        }
    }

    fn setup_error(invocation: &HookInvocation, reason: impl Into<String>) -> HookError {
        let reason = reason.into();
        if invocation.identity.is_some() {
            Self::outcome_unknown(invocation, reason)
        } else {
            HookError::ExecutionFailed(reason)
        }
    }
}

fn process_failure_reason(error: &std::io::Error) -> String {
    let Some(finalization) = error
        .get_ref()
        .and_then(|source| source.downcast_ref::<ProcessFinalizationError>())
    else {
        return error.to_string();
    };
    let details = finalization
        .failures()
        .iter()
        .map(|failure| format!("{:?}: {}", failure.stage(), failure.error()))
        .collect::<Vec<_>>()
        .join("; ");
    format!("{error}; {details}")
}

#[async_trait]
impl HookExecutor for EffectHookExecutor {
    async fn validate_stage(&self, invocation: HookStageInvocation) -> Result<(), HookError> {
        let stage_name = invocation.stage.as_str();
        let hook_name = format!("{stage_name} stage");
        let parent_digest = stable_digest_value(&serde_json::json!({
            "parent_call_id": invocation.parent_call_id,
        }));
        let call_id = format!("hook-stage:{stage_name}:{parent_digest}");
        let request = self.context.effect_request(
            &call_id,
            "HookStageManifest",
            &invocation.manifest,
            EffectDescriptor {
                class: EffectClass::ReadOnly,
                action: format!("validate {stage_name} hook stage"),
                resources: ResourceFootprint::default(),
                replay_policy: EffectReplayPolicy::ReplaySafe,
            },
        );
        match self
            .context
            .recover_effect(&request)
            .map_err(|reason| HookError::OutcomeUnknown {
                hook_name: hook_name.clone(),
                reason,
            })? {
            EffectRecoveryDecision::Execute => {}
            EffectRecoveryDecision::Reuse { is_error: false, .. } => return Ok(()),
            EffectRecoveryDecision::Reuse { is_error: true, output } => {
                return Err(HookError::OutcomeUnknown {
                    hook_name,
                    reason: format!("durable hook stage validation failed: {output}"),
                });
            }
            EffectRecoveryDecision::Reconcile { reason } => {
                return Err(HookError::OutcomeUnknown { hook_name, reason });
            }
        }
        self.context
            .record_effect_intent(&request)
            .and_then(|()| {
                self.context
                    .record_effect_outcome(&request, false, "hook stage manifest verified")
            })
            .map_err(|error| HookError::OutcomeUnknown {
                hook_name,
                reason: format!("persist hook stage manifest: {error}"),
            })
    }

    async fn execute(&self, invocation: HookInvocation) -> Result<HookExecutionResult, HookError> {
        let executable_identity = inspect_executable(&invocation.executable)
            .map_err(|error| Self::setup_error(&invocation, error.to_string()))?;
        let executable = executable_identity.canonical_path().to_owned();
        let executable_digest = executable_identity.content_digest().to_owned();
        let stable_environment = invocation.env.iter().collect::<std::collections::BTreeMap<_, _>>();
        let definition = serde_json::to_value(&invocation.definition)
            .map_err(|error| Self::setup_error(&invocation, format!("encode hook definition: {error}")))?;
        let input = serde_json::json!({
            "schema": "solaris/hook-effect/v1",
            "stage": invocation.identity.as_ref().map(|identity| identity.stage.as_str()),
            "ordinal": invocation.identity.as_ref().map(|identity| identity.ordinal),
            "hook_name": invocation.hook_name,
            "definition_digest": stable_digest_value(&definition),
            "effective_input_digest": stable_digest_value(&invocation.effective_input),
            "command_digest": stable_digest_value(&serde_json::json!(invocation.command)),
            "argv_digest": stable_digest_value(&serde_json::json!(invocation.argv)),
            "environment_digest": stable_digest_value(&serde_json::json!(stable_environment)),
            "cwd_digest": stable_digest_value(&serde_json::json!(invocation.cwd)),
            "executable_digest": executable_digest,
        });
        let call_id = self.call_id(&invocation, &input)?;
        let mut resources = ResourceFootprint {
            process_commands: vec![executable.to_string_lossy().into_owned()],
            process_invocations: vec![ProcessInvocation {
                executable: executable.to_string_lossy().into_owned(),
                argv: invocation.argv.clone(),
            }],
            external_resources: vec![format!("hook:{}", invocation.hook_name)],
            network_domains: invocation.network.network_domains.clone(),
            ..Default::default()
        };
        resources.declare_sandboxed_process_access();
        let capability = format!("HookCommand:{}", invocation.hook_name);
        let descriptor = EffectDescriptor {
            class: EffectClass::Process,
            action: format!("run configured hook {}", invocation.hook_name),
            resources,
            replay_policy: EffectReplayPolicy::Never,
        };
        let _registration_lock = self.permission_registration.lock().await;
        let permission_source = format!(
            "config:hook:{}:{}:{}",
            invocation.hook_name,
            stable_digest_value(&serde_json::json!(call_id)),
            stable_digest_value(&input)
        );
        let permissions = self.context.permissions();
        let approved_mode = permissions.mode();
        let approved_ceiling = permissions.ceiling();
        permissions.allow_configured_effect_for(&permission_source, &capability, &descriptor);
        permissions.set_generated_rules(
            permission_source.clone(),
            vec![PermissionRule {
                capability: Some(capability.clone()),
                action: None,
                effect_class: Some(EffectClass::Process),
                resource_prefixes: Vec::new(),
                decision: PermissionDecision::Allow,
            }],
        );
        let _permission_registration = HookPermissionRegistration {
            permissions,
            source: permission_source,
        };
        let request = self.context.effect_request(&call_id, &capability, &input, descriptor);
        match self
            .context
            .recover_effect(&request)
            .map_err(|reason| Self::outcome_unknown(&invocation, reason))?
        {
            EffectRecoveryDecision::Execute if invocation.completed_parent_recovery => {
                return Err(Self::outcome_unknown(
                    &invocation,
                    "parent tool completed without a durable pre-hook outcome",
                ));
            }
            EffectRecoveryDecision::Execute => {}
            EffectRecoveryDecision::Reuse { is_error, output } => {
                return Ok(HookExecutionResult {
                    success: !is_error,
                    output,
                });
            }
            EffectRecoveryDecision::Reconcile { reason } => {
                return Err(Self::outcome_unknown(&invocation, reason));
            }
        }
        let evaluation = self.context.evaluate(&request);
        self.context
            .record_permission_decision(&request, &evaluation, "hook")
            .map_err(|error| Self::outcome_unknown(&invocation, error.to_string()))?;
        if evaluation.decision != PermissionDecision::Allow {
            return Err(HookError::ExecutionFailed(format!(
                "hook permission denied: {}",
                evaluation.reason
            )));
        }
        let _permit = self
            .context
            .acquire_effect_permit()
            .await
            .map_err(HookError::ExecutionFailed)?;
        let approved_environment = self.context.environment();
        let launch_policy = self
            .context
            .revalidate_configured_effect_before_execution(
                &request,
                &approved_environment,
                approved_mode,
                approved_ceiling,
                true,
            )
            .map_err(HookError::ExecutionFailed)?
            .ok_or_else(|| HookError::ExecutionFailed("hook process launch policy unavailable".to_owned()))?;
        let boundary = self.context.permissions().boundary();
        let workspace_root = match boundary.writable_roots.as_slice() {
            [workspace_root] => Some(std::path::PathBuf::from(workspace_root)),
            _ => None,
        };
        let spawn_authorization = self
            .context
            .process_spawn_authorization(
                request.clone(),
                approved_environment,
                approved_mode,
                approved_ceiling,
                workspace_root,
                evaluation.matched_lease,
                None,
            )
            .map_err(HookError::ExecutionFailed)?;
        let pinned_executable = pin_executable(&executable, &executable_digest).map_err(HookError::ExecutionFailed)?;
        let mut command = pinned_executable
            .command()
            .map_err(|error| HookError::ExecutionFailed(error.to_string()))?;
        command
            .args(&invocation.argv)
            .envs(&invocation.env)
            .current_dir(&invocation.cwd)
            .kill_on_drop(true);

        self.context
            .record_effect_intent(&request)
            .map_err(|error| Self::outcome_unknown(&invocation, error.to_string()))?;
        let mut outcome_guard = EffectOutcomeGuard::new(
            self.context.clone(),
            request,
            format!("hook {} cancelled before a terminal result", invocation.hook_name),
        );
        let output_limit = self
            .context
            .resource_manager()
            .and_then(|resources| resources.budget().max_process_output_bytes)
            .unwrap_or(solaris_process::DEFAULT_MAX_PROCESS_OUTPUT_BYTES)
            .max(1);
        let output = solaris_process::CommandRunner::new_pinned(command)
            .launch_policy(launch_policy)
            .spawn_authorizer(spawn_authorization)
            .timeout(std::time::Duration::from_millis(invocation.timeout_ms.max(1)))
            .max_output_bytes(output_limit)
            .run()
            .await;
        let result = match output {
            Err(error) if process_recovery_required(&error).is_some() || process_outcome_unknown(&error) => {
                // The process authorizer has already persisted this exact
                // launch as OutcomeUnknown (and, for cleanup failures, bound
                // the recovery record to the same EffectId). Do not attempt a
                // second canonical terminal write that would obscure the
                // original reason or conflict with the durable unknown state.
                outcome_guard.leave_for_reconciliation();
                return Err(Self::outcome_unknown(&invocation, process_failure_reason(&error)));
            }
            Err(error) => Err(HookError::ExecutionFailed(process_failure_reason(&error))),
            Ok(output) if output.output_limit_exceeded => Err(HookError::ExecutionFailed(format!(
                "hook output exceeded {output_limit} bytes and the process tree was terminated"
            ))),
            Ok(output) if output.timed_out => Err(HookError::Timeout {
                timeout_ms: invocation.timeout_ms,
                output: String::from_utf8_lossy(&output.stdout).into_owned(),
            }),
            Ok(output) => {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let stderr = String::from_utf8_lossy(&output.stderr);
                let combined = match (stdout.is_empty(), stderr.is_empty()) {
                    (false, false) => format!("{stdout}\n{stderr}"),
                    (false, true) => stdout.into_owned(),
                    (true, false) => stderr.into_owned(),
                    (true, true) => String::new(),
                };
                Ok(HookExecutionResult {
                    success: output.exit_code == Some(0),
                    output: combined,
                })
            }
        };
        let (is_error, outcome) = match &result {
            Ok(result) => (!result.success, result.output.clone()),
            Err(error) => (true, error.to_string()),
        };
        outcome_guard
            .complete(is_error, &outcome)
            .map_err(|error| Self::outcome_unknown(&invocation, error.to_string()))?;
        result
    }

    async fn recover_for_completed_parent(
        &self,
        mut invocation: HookInvocation,
    ) -> Result<HookExecutionResult, HookError> {
        invocation.completed_parent_recovery = true;
        self.execute(invocation).await
    }
}
