use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use serde_json::{Value, json};
use uuid::Uuid;

use solaris_process::{ProcessLaunchPolicy, ProcessRecoveryRecord};
use solaris_tools::ToolExecutionContext;
use solaris_tools::registry::ToolRegistry;
use solaris_types::effect::{DurabilityClass, EffectAuditProjection, EffectDescriptor, EffectRequest};
use solaris_types::identity::{AgentId, EffectId, OperationId, RunId};
use solaris_types::permission::{
    AdditionalPermissions, CapabilityLease, LeaseScope, PermissionCeiling, PermissionDecision, PermissionMode,
};
use solaris_types::plugin::ImplementationIdentity;
use solaris_types::runtime::OperationEnvironmentSnapshot;

use crate::permission_engine::{PermissionContext, PermissionEvaluation};
use crate::resource_manager::{EffectResourcePermit, ProviderRatePermit, ResourceManager};
use crate::runtime_ledger::{RunMutationCoordinator, RuntimeLedger};

mod environment;
mod output_store;
mod plan_artifact;
mod process_authorization;
mod recovery;
mod redaction;
mod session_fence;
mod task_phase;

pub(crate) use self::environment::{
    build_environment_snapshot, build_environment_snapshot_with_plugins, refresh_environment_tools_and_plugins,
};
use self::environment::{
    normalize_environment, permission_fingerprint, read_only_evidence_environment_digest, restore_permission_leases,
    validate_environment_plugin_identities,
};
pub(crate) use self::output_store::{
    EffectOutputStore, delete_local_run_outputs, effect_output_state_root, read_protected_blob, write_protected_blob,
};
pub use self::redaction::secret_safe_ledger_payload;
use self::redaction::{normalize_descriptor, stable_digest_serializable};
pub(crate) use self::redaction::{stable_digest_bytes, stable_digest_value};
pub(crate) use self::session_fence::SessionFenceState;
pub(crate) use crate::session::DurableTaskPhase;

const PREPARED_IMPLEMENTATION_SNAPSHOT_PREFIX: &str = "\0solaris/prepared-executable/";

#[derive(Clone)]
pub(crate) struct ApprovedEffectRequest {
    pub(crate) request: EffectRequest,
    pub(crate) registered_tool_name: String,
    pub(crate) tool_execution: ToolExecutionContext,
    mode: solaris_types::permission::PermissionMode,
    ceiling: solaris_types::permission::PermissionCeiling,
    environment: OperationEnvironmentSnapshot,
    registration_id: u64,
}

/// Removes an approved request if execution never consumes its registration.
pub(crate) struct ApprovedRequestRegistration {
    context: EffectExecutionContext,
    effect_id: EffectId,
    registration_id: u64,
    armed: bool,
}

impl ApprovedRequestRegistration {
    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ApprovedRequestRegistration {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut requests = self
            .context
            .approved_requests
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if requests
            .get(&self.effect_id)
            .is_some_and(|request| request.registration_id == self.registration_id)
        {
            requests.remove(&self.effect_id);
        }
    }
}

/// Ownership proof that an approved request was atomically consumed.
pub(crate) struct ApprovedRequestConsumption(ApprovedEffectRequest);

impl std::ops::Deref for ApprovedRequestConsumption {
    type Target = ApprovedEffectRequest;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone)]
pub struct EffectExecutionContext {
    run_id: RunId,
    agent_id: AgentId,
    ledger: Arc<dyn RuntimeLedger>,
    mutation: Arc<RunMutationCoordinator>,
    permissions: PermissionContext,
    environment: Arc<RwLock<OperationEnvironmentSnapshot>>,
    environment_permission_fingerprint: Arc<RwLock<String>>,
    approved_requests: Arc<Mutex<HashMap<EffectId, ApprovedEffectRequest>>>,
    process_recoveries: Arc<Mutex<HashMap<EffectId, ProcessRecoveryRecord>>>,
    resources: Option<Arc<ResourceManager>>,
    scoped_resources: Option<Arc<ResourceManager>>,
    output_store: Arc<EffectOutputStore>,
    session_fences: SessionFenceState,
}

pub struct ExecutionEffectPermit {
    _run: Option<EffectResourcePermit>,
    _scoped: Option<EffectResourcePermit>,
}

pub struct ExecutionProviderRatePermit {
    run: Option<ProviderRatePermit>,
    scoped: Option<ProviderRatePermit>,
}

impl ExecutionProviderRatePermit {
    pub fn mark_started(&mut self) -> Result<(), String> {
        if let Some(permit) = self.run.as_mut() {
            permit.mark_started()?;
        }
        if let Some(permit) = self.scoped.as_mut()
            && let Err(error) = permit.mark_started()
        {
            if let Some(run) = self.run.as_mut() {
                run.rollback_start().map_err(|rollback_error| {
                    format!("{error}; failed to roll back Run provider request start: {rollback_error}")
                })?;
            }
            return Err(error);
        }
        Ok(())
    }

    pub fn commit(mut self, actual_tokens: u64) -> Result<(), String> {
        self.mark_started()?;
        if let Some(permit) = self.run.take() {
            permit.commit(actual_tokens)?;
        }
        if let Some(permit) = self.scoped.take() {
            permit.commit(actual_tokens)?;
        }
        Ok(())
    }
}

/// Ensures an effect that reached its durable intent also receives a terminal
/// outcome when its async execution future is cancelled.
pub struct EffectOutcomeGuard {
    context: EffectExecutionContext,
    request: EffectRequest,
    terminal_record_attempted: bool,
    cancellation_reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EffectOutcomeCompletion {
    Committed,
    OutcomeUnknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EffectRecoveryDecision {
    Execute,
    Reuse { is_error: bool, output: String },
    Reconcile { reason: String },
}

impl EffectOutcomeGuard {
    pub fn new(
        context: EffectExecutionContext,
        request: EffectRequest,
        cancellation_reason: impl Into<String>,
    ) -> Self {
        Self {
            context,
            request,
            terminal_record_attempted: false,
            cancellation_reason: cancellation_reason.into(),
        }
    }

    pub(crate) fn complete_canonical(
        &mut self,
        is_error: bool,
        output: &str,
    ) -> std::io::Result<EffectOutcomeCompletion> {
        let result = self
            .context
            .record_effect_outcome_canonical(&self.request, is_error, output);
        self.terminal_record_attempted = true;
        if result.is_ok() {
            self.context.clear_process_recovery(&self.request.effect_id);
        }
        if result.is_err() {
            log_terminal_outcome_persistence_failure(&self.context, &self.request);
        }
        result
    }

    pub fn complete(&mut self, is_error: bool, output: &str) -> std::io::Result<()> {
        match self.complete_canonical(is_error, output)? {
            EffectOutcomeCompletion::Committed => Ok(()),
            EffectOutcomeCompletion::OutcomeUnknown => Err(std::io::Error::other(
                "the canonical effect outcome requires reconciliation",
            )),
        }
    }

    pub fn leave_for_reconciliation(&mut self) {
        self.terminal_record_attempted = true;
    }
}

impl Drop for EffectOutcomeGuard {
    fn drop(&mut self) {
        if !self.terminal_record_attempted
            && !matches!(
                self.context.recover_effect(&self.request),
                Ok(EffectRecoveryDecision::Reuse { .. })
            )
            && self
                .context
                .record_effect_outcome_unknown(&self.request, &self.cancellation_reason)
                .is_err()
        {
            log_terminal_outcome_persistence_failure(&self.context, &self.request);
        }
        self.context.clear_request(&self.request.effect_id);
        self.context.clear_process_recovery(&self.request.effect_id);
    }
}

fn log_terminal_outcome_persistence_failure(context: &EffectExecutionContext, request: &EffectRequest) {
    let run_identity =
        stable_digest_bytes(format!("solaris.effect-audit/v1/run-id\0{}", context.run_id.as_str()).as_bytes());
    let effect_identity =
        stable_digest_bytes(format!("solaris.effect-audit/v1/effect-id\0{}", request.effect_id.as_str()).as_bytes());
    tracing::error!(
        run_identity = %format!("sha256:{run_identity}"),
        effect_identity = %format!("sha256:{effect_identity}"),
        "terminal effect outcome persistence failed"
    );
}

impl EffectExecutionContext {
    pub fn new(
        run_id: RunId,
        agent_id: AgentId,
        ledger: Arc<dyn RuntimeLedger>,
        permissions: PermissionContext,
        environment: OperationEnvironmentSnapshot,
    ) -> Self {
        restore_permission_leases(ledger.as_ref(), &run_id, &permissions);
        let output_store = Arc::new(EffectOutputStore::for_run_with_ledger(&run_id, ledger.as_ref()));
        let environment = normalize_environment(environment);
        let environment_permission_fingerprint = permission_fingerprint(&permissions);
        Self {
            run_id,
            agent_id,
            ledger,
            mutation: Arc::new(RunMutationCoordinator::default()),
            permissions,
            environment: Arc::new(RwLock::new(environment)),
            environment_permission_fingerprint: Arc::new(RwLock::new(environment_permission_fingerprint)),
            approved_requests: Arc::new(Mutex::new(HashMap::new())),
            process_recoveries: Arc::new(Mutex::new(HashMap::new())),
            resources: None,
            scoped_resources: None,
            output_store,
            session_fences: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn with_resource_manager(mut self, resources: Arc<ResourceManager>) -> Self {
        self.resources = Some(resources);
        self
    }

    pub fn with_scoped_resource_manager(mut self, resources: Arc<ResourceManager>) -> Self {
        self.scoped_resources = Some(resources);
        self
    }

    pub fn with_mutation_coordinator(mut self, mutation: Arc<RunMutationCoordinator>) -> Self {
        self.mutation = mutation;
        self
    }

    pub fn resource_manager(&self) -> Option<Arc<ResourceManager>> {
        self.resources.as_ref().map(Arc::clone)
    }

    pub async fn acquire_effect_permit(&self) -> Result<Option<ExecutionEffectPermit>, String> {
        let run = match &self.resources {
            Some(resources) => Some(resources.acquire_effect().await?),
            None => None,
        };
        let scoped = match &self.scoped_resources {
            Some(resources) => Some(resources.acquire_effect().await?),
            None => None,
        };
        Ok((run.is_some() || scoped.is_some()).then_some(ExecutionEffectPermit {
            _run: run,
            _scoped: scoped,
        }))
    }

    pub fn acquire_provider_request(
        &self,
        estimated_tokens: u64,
    ) -> Result<Option<ExecutionProviderRatePermit>, String> {
        let run = match &self.resources {
            Some(resources) => Some(resources.acquire_provider_request(estimated_tokens)?),
            None => None,
        };
        let scoped = match &self.scoped_resources {
            Some(resources) => Some(resources.acquire_provider_request(estimated_tokens)?),
            None => None,
        };
        Ok((run.is_some() || scoped.is_some()).then_some(ExecutionProviderRatePermit { run, scoped }))
    }

    pub fn ensure_runtime_budget_available(&self) -> Result<(), String> {
        if let Some(resources) = &self.resources
            && let Some(reason) = resources.hard_runtime_budget_reason()
        {
            return Err(reason);
        }
        if let Some(resources) = &self.scoped_resources
            && let Some(reason) = resources.hard_runtime_budget_reason()
        {
            return Err(format!("Agent role resource budget exceeded: {reason}"));
        }
        Ok(())
    }

    pub fn record_model_usage(&self, usage: &solaris_types::message::TokenUsage) -> Result<(), String> {
        for (scope, resources) in [
            ("Run", self.resources.as_ref()),
            ("Agent role", self.scoped_resources.as_ref()),
        ] {
            let Some(resources) = resources else { continue };
            resources
                .record_model_usage_checked(usage)
                .map_err(|error| format!("{scope} resource usage persistence failed: {error}"))?;
        }
        Ok(())
    }

    pub fn record_model_usage_once(
        &self,
        effect_id: &EffectId,
        usage: &solaris_types::message::TokenUsage,
        count_turn: bool,
    ) -> Result<(), String> {
        for (scope, resources) in [
            ("Run", self.resources.as_ref()),
            ("Agent role", self.scoped_resources.as_ref()),
        ] {
            let Some(resources) = resources else { continue };
            resources
                .record_model_usage_once_checked(effect_id.as_str(), usage, count_turn)
                .map_err(|error| format!("{scope} resource usage persistence failed: {error}"))?;
        }
        Ok(())
    }

    pub(crate) fn record_tool_calls_once(
        &self,
        round_call_id: &str,
        statuses: &[solaris_types::tool::ToolResultStatus],
    ) -> Result<(), String> {
        // Child Agents can receive provider-local call IDs that match another
        // child's IDs. Include the durable Agent identity before applying the
        // Run-wide idempotency key so concurrent children cannot undercount.
        let round_identity = format!("{}:{round_call_id}", self.agent_id);
        for (scope, resources) in [
            ("Run", self.resources.as_ref()),
            ("Agent role", self.scoped_resources.as_ref()),
        ] {
            let Some(resources) = resources else { continue };
            resources
                .record_tool_calls_once_checked(&round_identity, statuses)
                .map_err(|error| format!("{scope} tool-call statistics persistence failed: {error}"))?;
        }
        Ok(())
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    pub fn agent_id(&self) -> &AgentId {
        &self.agent_id
    }

    pub fn permissions(&self) -> &PermissionContext {
        &self.permissions
    }

    pub(crate) fn set_permissions(&mut self, permissions: PermissionContext) {
        restore_permission_leases(self.ledger.as_ref(), &self.run_id, &permissions);
        self.permissions = permissions;
        self.approved_requests
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clear();
    }

    pub fn environment(&self) -> OperationEnvironmentSnapshot {
        self.environment
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    pub fn set_environment(&self, environment: OperationEnvironmentSnapshot) {
        let current_permission_fingerprint = permission_fingerprint(&self.permissions);
        let mut environment = normalize_environment(environment);
        environment.permission_fingerprint = Some(current_permission_fingerprint.clone());
        *self.environment.write().unwrap_or_else(|error| error.into_inner()) = environment;
        *self
            .environment_permission_fingerprint
            .write()
            .unwrap_or_else(|error| error.into_inner()) = current_permission_fingerprint;
    }

    pub fn scoped_to_workflow(&self, workflow: ImplementationIdentity) -> Self {
        let mut environment = self.environment();
        environment.workflow = Some(workflow);
        Self {
            run_id: self.run_id.clone(),
            agent_id: self.agent_id.clone(),
            ledger: Arc::clone(&self.ledger),
            mutation: Arc::clone(&self.mutation),
            permissions: self.permissions.clone(),
            environment: Arc::new(RwLock::new(environment)),
            environment_permission_fingerprint: Arc::new(RwLock::new(permission_fingerprint(&self.permissions))),
            approved_requests: Arc::new(Mutex::new(HashMap::new())),
            process_recoveries: Arc::new(Mutex::new(HashMap::new())),
            resources: self.resources.as_ref().map(Arc::clone),
            scoped_resources: self.scoped_resources.as_ref().map(Arc::clone),
            output_store: Arc::clone(&self.output_store),
            session_fences: Arc::clone(&self.session_fences),
        }
    }

    pub fn effect_request(
        &self,
        call_id: &str,
        capability: &str,
        input: &Value,
        descriptor: EffectDescriptor,
    ) -> EffectRequest {
        let identity = format!("{}:{}:{}", self.run_id, self.agent_id, call_id);
        EffectRequest {
            effect_id: self.effect_id_for_call(call_id),
            operation_id: OperationId::new(format!("tool:{identity}")),
            capability: capability.to_owned(),
            descriptor: normalize_descriptor(descriptor),
            effective_input: input.clone(),
            input_digest: Some(stable_digest_value(input)),
        }
    }

    pub fn remember_approved_request(
        &self,
        request: EffectRequest,
        mode: solaris_types::permission::PermissionMode,
        ceiling: solaris_types::permission::PermissionCeiling,
    ) {
        let tool_execution = ToolExecutionContext::new(request.effect_id.as_str());
        let registered_tool_name = request.capability.clone();
        self.insert_approved_request(request, registered_tool_name, tool_execution, mode, ceiling);
    }

    #[cfg(test)]
    pub(crate) fn remember_approved_request_with_tool_context(
        &self,
        request: EffectRequest,
        tool_execution: ToolExecutionContext,
        mode: solaris_types::permission::PermissionMode,
        ceiling: solaris_types::permission::PermissionCeiling,
    ) -> ApprovedRequestRegistration {
        let registered_tool_name = request.capability.clone();
        self.remember_approved_request_with_registered_tool_context(
            request,
            registered_tool_name,
            tool_execution,
            mode,
            ceiling,
        )
    }

    pub(crate) fn remember_approved_request_with_registered_tool_context(
        &self,
        request: EffectRequest,
        registered_tool_name: impl Into<String>,
        tool_execution: ToolExecutionContext,
        mode: solaris_types::permission::PermissionMode,
        ceiling: solaris_types::permission::PermissionCeiling,
    ) -> ApprovedRequestRegistration {
        let effect_id = request.effect_id.clone();
        let registration_id =
            self.insert_approved_request(request, registered_tool_name.into(), tool_execution, mode, ceiling);
        ApprovedRequestRegistration {
            context: self.clone(),
            effect_id,
            registration_id,
            armed: true,
        }
    }

    fn insert_approved_request(
        &self,
        request: EffectRequest,
        registered_tool_name: String,
        tool_execution: ToolExecutionContext,
        mode: solaris_types::permission::PermissionMode,
        ceiling: solaris_types::permission::PermissionCeiling,
    ) -> u64 {
        static NEXT_REGISTRATION_ID: AtomicU64 = AtomicU64::new(1);
        let registration_id = NEXT_REGISTRATION_ID.fetch_add(1, Ordering::Relaxed);
        self.approved_requests
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(
                request.effect_id.clone(),
                ApprovedEffectRequest {
                    request,
                    registered_tool_name,
                    tool_execution,
                    mode,
                    ceiling,
                    environment: self.environment(),
                    registration_id,
                },
            );
        registration_id
    }

    pub(crate) fn take_approved_request_for_call(&self, call_id: &str) -> Option<ApprovedRequestConsumption> {
        let effect_id = self.effect_id_for_call(call_id);
        self.approved_requests
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&effect_id)
            .map(ApprovedRequestConsumption)
    }

    pub fn clear_request(&self, effect_id: &EffectId) {
        self.approved_requests
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(effect_id);
    }

    pub(crate) fn effect_id_for_call(&self, call_id: &str) -> EffectId {
        EffectId::new(format!("effect:{}:{}:{}", self.run_id, self.agent_id, call_id))
    }

    pub fn evaluate(&self, request: &EffectRequest) -> PermissionEvaluation {
        self.permissions
            .evaluate_effect(&self.run_id, &request.capability, request)
    }

    pub fn evaluate_with(
        &self,
        request: &EffectRequest,
        mode: solaris_types::permission::PermissionMode,
        ceiling: solaris_types::permission::PermissionCeiling,
    ) -> PermissionEvaluation {
        self.permissions
            .evaluate_effect_with(&self.run_id, &request.capability, request, mode, ceiling)
    }

    pub fn issue_approval_lease(&self, request: &EffectRequest, always: bool) -> std::io::Result<CapabilityLease> {
        let can_persist_run_scope = request.descriptor.resources.external_resources.is_empty()
            && request.descriptor.resources.process_commands.len() <= 1;
        let (scope, max_uses) = if always && can_persist_run_scope {
            (
                LeaseScope::Run {
                    run_id: self.run_id.clone(),
                },
                None,
            )
        } else {
            (
                LeaseScope::Effect {
                    effect_id: request.effect_id.clone(),
                },
                Some(1),
            )
        };
        let lease = CapabilityLease {
            lease_id: Uuid::now_v7().to_string(),
            capability: request.capability.clone(),
            action: Some(request.descriptor.action.clone()),
            scope,
            grants: grants_for_descriptor(&request.descriptor),
            max_uses,
            expires_at_unix_ms: None,
        };
        let durable_payload = json!({
            "lease_id": lease.lease_id,
            "capability": lease.capability,
            "scope": lease.scope,
            "max_uses": lease.max_uses,
            "expires_at_unix_ms": lease.expires_at_unix_ms,
            "source_decision": "host_approval",
            "effect": EffectAuditProjection::from_descriptor(&request.descriptor),
            "input_digest": request.input_digest,
            "restorable": false,
        });
        self.append_record(
            &self.run_id,
            DurabilityClass::SyncCritical,
            "capability_lease_issued",
            durable_payload,
        )?;
        self.permissions.issue_lease(lease.clone());
        Ok(lease)
    }

    pub fn record_permission_decision(
        &self,
        request: &EffectRequest,
        evaluation: &PermissionEvaluation,
        source: &str,
    ) -> std::io::Result<()> {
        let effect = EffectAuditProjection::from_descriptor(&request.descriptor);
        self.append_record(
            &self.run_id,
            DurabilityClass::SyncCritical,
            "permission_decision",
            json!({
                "agent_id": self.agent_id,
                "effect_id": request.effect_id,
                "operation_id": request.operation_id,
                "capability": request.capability,
                "decision": evaluation.decision,
                "matched_rule_index": evaluation.matched_rule_index,
                "matched_lease": evaluation.matched_lease,
                "reason": "permission evaluation result",
                "reason_digest": format!("sha256:{}", stable_digest_bytes(evaluation.reason.as_bytes())),
                "source": source,
                "effect": effect,
                "input_digest": request.input_digest,
            }),
        )?;
        Ok(())
    }

    pub(crate) fn revalidate(
        &self,
        registry: &ToolRegistry,
        approved: &ApprovedEffectRequest,
    ) -> Result<ToolExecutionContext, String> {
        let request = &approved.request;
        validate_environment_plugin_identities(&approved.environment)?;
        let tool = registry
            .get(&approved.registered_tool_name)
            .ok_or_else(|| format!("tool {} disappeared before execution", approved.registered_tool_name))?;
        let current = normalize_descriptor(tool.revalidate_effect(&request.effective_input, &approved.tool_execution));
        if current != request.descriptor {
            return Err("effect footprint changed after approval; refusing stale authorization".to_owned());
        }
        let current_environment = self.environment();
        validate_environment_plugin_identities(&current_environment)?;
        if let Some(approved_tool) = approved
            .environment
            .tools
            .iter()
            .find(|tool| tool.name == approved.registered_tool_name)
        {
            let default_implementation = ImplementationIdentity {
                implementation_id: format!("tool:{}", approved.registered_tool_name),
                version: Some(env!("CARGO_PKG_VERSION").to_owned()),
                digest: Some(stable_digest_value(&tool.input_schema())),
            };
            let current_implementation = tool.implementation_identity().unwrap_or(default_implementation);
            if current_implementation != approved_tool.implementation {
                return Err("tool implementation changed after approval; refusing stale authorization".to_owned());
            }
        }
        if current_environment.plugins != approved.environment.plugins {
            return Err("plugin implementation set changed after approval; refusing stale authorization".to_owned());
        }
        if current_environment.provider != approved.environment.provider
            || current_environment.hook_order != approved.environment.hook_order
        {
            return Err("operation environment changed after approval; refusing stale authorization".to_owned());
        }
        let current_mode = self.permissions.mode();
        let execution_mode = stricter_permission_mode(approved.mode, current_mode);
        let evaluation = self.evaluate_with(request, execution_mode, approved.ceiling);
        if evaluation.decision != PermissionDecision::Allow {
            return Err(format!(
                "permission no longer allows effect before execution: {}",
                evaluation.reason
            ));
        }
        if request.descriptor.class != solaris_types::effect::EffectClass::Process
            && evaluation.matched_lease
            && !self.consume_lease_durably(request)?
        {
            return Err("capability lease expired or was consumed before execution".to_owned());
        }
        let mut tool_execution = self.tool_execution_context_for_mode(&approved.tool_execution, execution_mode)?;
        if matches!(approved.registered_tool_name.as_str(), "Read" | "Glob" | "Grep") {
            let authorization_digest = stable_digest_value(&json!({
                "mode": execution_mode,
                "ceiling": approved.ceiling,
                "permission_fingerprint": current_environment.permission_fingerprint,
                "matched_rule_index": evaluation.matched_rule_index,
                "matched_lease": evaluation.matched_lease,
                "descriptor": request.descriptor,
            }));
            let environment_digest =
                read_only_evidence_environment_digest(&current_environment, &approved.registered_tool_name);
            tool_execution = tool_execution.with_read_only_evidence_scope(authorization_digest, environment_digest);
        }
        if request.descriptor.class == solaris_types::effect::EffectClass::Process {
            let workspace_root = tool_execution.sandbox_workspace_root().map(Path::to_path_buf);
            let authorization = self.process_spawn_authorization(
                request.clone(),
                approved.environment.clone(),
                approved.mode,
                approved.ceiling,
                workspace_root,
                evaluation.matched_lease,
                None,
            )?;
            tool_execution = tool_execution.with_process_spawn_authorization(authorization);
        }
        Ok(tool_execution)
    }

    fn tool_execution_context_for_mode(
        &self,
        approved: &ToolExecutionContext,
        mode: solaris_types::permission::PermissionMode,
    ) -> Result<ToolExecutionContext, String> {
        use solaris_types::permission::PermissionMode;

        let approved = approved.clone().with_permission_mode(mode);
        let Some(workspace_root) = approved.sandbox_workspace_root() else {
            return Ok(approved);
        };
        let policy = match mode {
            PermissionMode::Plan => return Err("plan mode does not permit process execution".to_owned()),
            PermissionMode::Auto => {
                let (protected_paths, protected_object_identities) = self.permissions.protected_process_snapshot()?;
                ProcessLaunchPolicy::workspace_sandbox_with_identities(
                    workspace_root,
                    protected_paths,
                    protected_object_identities,
                )
            }
            PermissionMode::Bypass => ProcessLaunchPolicy::Ambient,
        };
        Ok(approved.with_process_launch_policy(policy))
    }

    pub fn revalidate_external(
        &self,
        request: &EffectRequest,
        approved_environment: &OperationEnvironmentSnapshot,
        implementation: &ImplementationIdentity,
    ) -> Result<PermissionEvaluation, String> {
        let current_environment = self.environment();
        validate_environment_plugin_identities(approved_environment)?;
        validate_environment_plugin_identities(&current_environment)?;
        if !current_environment.plugins.contains(implementation) {
            return Err("approved plugin implementation disappeared before execution".to_owned());
        }
        if &current_environment != approved_environment {
            return Err("operation environment changed after approval; refusing stale authorization".to_owned());
        }
        let evaluation = self.evaluate(request);
        if evaluation.decision != PermissionDecision::Allow {
            return Err(format!(
                "permission no longer allows effect before execution: {}",
                evaluation.reason
            ));
        }
        if request.descriptor.class != solaris_types::effect::EffectClass::Process
            && evaluation.matched_lease
            && !self.consume_lease_durably(request)?
        {
            return Err("capability lease expired or was consumed before execution".to_owned());
        }
        Ok(evaluation)
    }

    pub fn revalidate_environment(
        &self,
        request: &EffectRequest,
        approved_environment: &OperationEnvironmentSnapshot,
    ) -> Result<PermissionEvaluation, String> {
        let current_environment = self.environment();
        validate_environment_plugin_identities(approved_environment)?;
        validate_environment_plugin_identities(&current_environment)?;
        if &current_environment != approved_environment {
            return Err("operation environment changed after approval; refusing stale authorization".to_owned());
        }
        let evaluation = self.evaluate(request);
        if evaluation.decision != PermissionDecision::Allow {
            return Err(format!(
                "permission no longer allows effect before execution: {}",
                evaluation.reason
            ));
        }
        if request.descriptor.class != solaris_types::effect::EffectClass::Process
            && evaluation.matched_lease
            && !self.consume_lease_durably(request)?
        {
            return Err("capability lease expired or was consumed before execution".to_owned());
        }
        Ok(evaluation)
    }

    pub(crate) fn revalidate_configured_effect_before_execution(
        &self,
        request: &EffectRequest,
        approved_environment: &OperationEnvironmentSnapshot,
        approved_mode: PermissionMode,
        approved_ceiling: PermissionCeiling,
        requires_process: bool,
    ) -> Result<Option<ProcessLaunchPolicy>, String> {
        let current_environment = self.environment();
        validate_environment_plugin_identities(approved_environment)?;
        validate_environment_plugin_identities(&current_environment)?;
        if &current_environment != approved_environment {
            return Err("operation environment changed after approval; refusing stale authorization".to_owned());
        }
        let execution_mode = stricter_permission_mode(approved_mode, self.permissions.mode());
        let evaluation = self.evaluate_with(request, execution_mode, approved_ceiling);
        if evaluation.decision != PermissionDecision::Allow {
            return Err(format!(
                "permission no longer allows effect before execution: {}",
                evaluation.reason
            ));
        }
        if !requires_process && evaluation.matched_lease && !self.consume_lease_durably(request)? {
            return Err("capability lease expired or was consumed before execution".to_owned());
        }
        if !requires_process {
            return Ok(None);
        }
        match execution_mode {
            PermissionMode::Plan => Err("plan mode does not permit process execution".to_owned()),
            PermissionMode::Auto => {
                let boundary = self.permissions.boundary();
                let [workspace_root] = boundary.writable_roots.as_slice() else {
                    return Err("Auto process execution requires exactly one workspace root".to_owned());
                };
                let (protected_paths, protected_object_identities) = self.permissions.protected_process_snapshot()?;
                Ok(Some(ProcessLaunchPolicy::workspace_sandbox_with_identities(
                    PathBuf::from(workspace_root),
                    protected_paths,
                    protected_object_identities,
                )))
            }
            PermissionMode::Bypass => Ok(Some(ProcessLaunchPolicy::Ambient)),
        }
    }

    fn consume_lease_durably(&self, request: &EffectRequest) -> Result<bool, String> {
        self.permissions
            .consume_lease_with(&self.run_id, request, |lease_id, use_number| {
                self.append_record(
                    &self.run_id,
                    DurabilityClass::SyncCritical,
                    "capability_lease_consumed",
                    json!({
                        "lease_id": lease_id,
                        "effect_id": request.effect_id,
                        "operation_id": request.operation_id,
                        "use_number": use_number,
                    }),
                )
                .map(|_| ())
            })
            .map_err(|error| format!("failed to persist capability lease consumption: {error}"))
    }

    pub fn record_revalidation_failure(&self, request: &EffectRequest, reason: &str) -> std::io::Result<()> {
        let effect = EffectAuditProjection::from_descriptor(&request.descriptor);
        self.append_record(
            &self.run_id,
            DurabilityClass::SyncCritical,
            "effect_revalidation_failed",
            json!({
                "agent_id": self.agent_id,
                "effect_id": request.effect_id,
                "operation_id": request.operation_id,
                "reason": "effect revalidation failed",
                "reason_digest": format!("sha256:{}", stable_digest_bytes(reason.as_bytes())),
                "effect": effect,
                "input_digest": request.input_digest,
            }),
        )?;
        Ok(())
    }

    fn append_record(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        payload: Value,
    ) -> std::io::Result<crate::runtime_ledger::LedgerRecord> {
        self.mutation
            .append_serialized(self.ledger.as_ref(), run_id, durability, record_type, payload)
    }
}

fn stricter_permission_mode(
    approved: solaris_types::permission::PermissionMode,
    current: solaris_types::permission::PermissionMode,
) -> solaris_types::permission::PermissionMode {
    use solaris_types::permission::PermissionMode;

    match (approved, current) {
        (PermissionMode::Plan, _) | (_, PermissionMode::Plan) => PermissionMode::Plan,
        (PermissionMode::Auto, _) | (_, PermissionMode::Auto) => PermissionMode::Auto,
        (PermissionMode::Bypass, PermissionMode::Bypass) => PermissionMode::Bypass,
    }
}

fn grants_for_descriptor(descriptor: &EffectDescriptor) -> AdditionalPermissions {
    AdditionalPermissions {
        unrestricted_file_reads: descriptor.resources.unrestricted_file_reads,
        unrestricted_file_writes: descriptor.resources.unrestricted_file_writes,
        unrestricted_network: descriptor.resources.unrestricted_network,
        unrestricted_process: descriptor.resources.unrestricted_process,
        file_reads: descriptor.resources.file_reads.clone(),
        file_writes: descriptor.resources.file_writes.clone(),
        network_domains: descriptor.resources.network_domains.clone(),
        process_command_prefix: descriptor.resources.process_commands.first().cloned(),
        process_invocations: descriptor.resources.process_invocations.clone(),
    }
}

pub(crate) type PinnedExecutable = solaris_process::PinnedExecutable;

pub(crate) fn pin_executable(path: &Path, expected_digest: &str) -> Result<PinnedExecutable, String> {
    solaris_process::pin_executable_by_digest(path, expected_digest).map_err(|error| error.to_string())
}

#[cfg(test)]
#[path = "execution_context_test.rs"]
mod execution_context_test;
