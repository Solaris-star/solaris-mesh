use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use solaris_process::{
    ManagedChild, ProcessLaunchPolicy, ProcessRecoveryRecord, ProcessSpawn, ProcessSpawnAuthorization,
    ProcessSpawnAuthorizer, process_outcome_unknown, process_recovery_required,
};
use solaris_types::effect::EffectRequest;
use solaris_types::permission::{PermissionCeiling, PermissionDecision, PermissionMode};
use solaris_types::plugin::ImplementationIdentity;
use solaris_types::runtime::OperationEnvironmentSnapshot;

use super::{EffectExecutionContext, stricter_permission_mode, validate_environment_plugin_identities};

struct EffectProcessSpawnAuthorizer {
    context: EffectExecutionContext,
    request: EffectRequest,
    approved_environment: OperationEnvironmentSnapshot,
    approved_mode: PermissionMode,
    approved_ceiling: PermissionCeiling,
    workspace_root: Option<PathBuf>,
    approved_by_lease: bool,
    required_plugin_implementation: Option<ImplementationIdentity>,
    spawn_consumed: Mutex<bool>,
}

impl ProcessSpawnAuthorizer for EffectProcessSpawnAuthorizer {
    fn authorize_and_spawn(&self, spawn: ProcessSpawn) -> io::Result<ManagedChild> {
        self.context
            .permissions
            .with_process_spawn_gate(|| self.authorize_and_spawn_guarded(spawn))
    }
}

impl EffectProcessSpawnAuthorizer {
    fn authorize_and_spawn_guarded(&self, spawn: ProcessSpawn) -> io::Result<ManagedChild> {
        let mut spawn_consumed = self.spawn_consumed.lock().unwrap_or_else(|error| error.into_inner());
        if *spawn_consumed {
            return Err(permission_denied("process spawn authorization was already consumed"));
        }
        // The credential represents exactly one root process creation attempt.
        // Spend it before any sandbox preparation or operating-system spawn so
        // an unknown launch outcome can never be retried with the same intent.
        *spawn_consumed = true;

        let lease_credential = if self.approved_by_lease {
            self.context
                .consume_lease_durably(&self.request)
                .map_err(permission_denied)?
        } else {
            false
        };

        let environment = self
            .context
            .environment
            .read()
            .unwrap_or_else(|error| error.into_inner());
        validate_environment_plugin_identities(&self.approved_environment).map_err(permission_denied)?;
        validate_environment_plugin_identities(&environment).map_err(permission_denied)?;
        if *environment != self.approved_environment {
            return Err(permission_denied(
                "operation environment changed after approval; refusing stale authorization",
            ));
        }
        if self
            .required_plugin_implementation
            .as_ref()
            .is_some_and(|implementation| !environment.plugins.contains(implementation))
        {
            return Err(permission_denied(
                "approved plugin implementation disappeared before process spawn",
            ));
        }

        let execution_mode = stricter_permission_mode(self.approved_mode, self.context.permissions.mode());
        let evaluation = self.context.permissions.evaluate_effect_for_final_process_spawn(
            &self.context.run_id,
            &self.request.capability,
            &self.request,
            execution_mode,
            self.approved_ceiling,
        );
        let credential_covers_review = if self.approved_by_lease {
            // An effect-scoped Host approval exists specifically to authorize
            // this exact request when it exceeds the ordinary boundary. Its
            // one use was durably consumed above, before sandbox preparation.
            lease_credential
        } else {
            // A reviewer-approved Auto request may cover a final review result,
            // but never a request that has since moved outside its boundary.
            self.context.permissions.current_boundary_allows_request(&self.request)
        };
        let credential_allows = execution_mode == self.approved_mode
            && credential_covers_review
            && matches!(
                evaluation.decision,
                PermissionDecision::Ask | PermissionDecision::AutoReview
            );
        if evaluation.decision != PermissionDecision::Allow && !credential_allows {
            return Err(permission_denied(format!(
                "permission no longer allows effect before process spawn: {}",
                evaluation.reason
            )));
        }
        if self.approved_by_lease && !lease_credential {
            return Err(permission_denied(
                "capability lease expired or was consumed before process spawn",
            ));
        }

        let policy = match execution_mode {
            PermissionMode::Plan => return Err(permission_denied("plan mode does not permit process execution")),
            PermissionMode::Auto => {
                if self.request.descriptor.resources.unrestricted_network {
                    return Err(permission_denied(
                        "Auto process execution requires exact approved network domains",
                    ));
                }
                let workspace_root = self
                    .workspace_root
                    .as_ref()
                    .ok_or_else(|| permission_denied("Auto process execution requires exactly one workspace root"))?;
                let workspace_capability = self
                    .context
                    .permissions
                    .seal_workspace_root_for_process_spawn(workspace_root)
                    .map_err(permission_denied)?;
                let (protected_paths, protected_object_identities) = self
                    .context
                    .permissions
                    .protected_process_snapshot()
                    .map_err(permission_denied)?;
                ProcessLaunchPolicy::workspace_sandbox_with_launch_capability_and_network(
                    workspace_capability,
                    protected_paths,
                    protected_object_identities,
                    &self.request.descriptor.resources.network_domains,
                )
                .map_err(|error| permission_denied(error.to_string()))?
            }
            PermissionMode::Bypass => ProcessLaunchPolicy::Ambient,
        };
        spawn(policy).map_err(|error| self.classify_process_spawn_error(error))
    }

    fn classify_process_spawn_error(&self, error: io::Error) -> io::Error {
        if let Some(recovery) = process_recovery_required(&error) {
            self.context
                .remember_process_recovery(&self.request.effect_id, recovery);
            let reason = format!(
                "process cleanup recovery {} ({:?}) is required",
                recovery.id().get(),
                recovery.kind()
            );
            return match self
                .context
                .record_process_recovery_outcome_unknown(&self.request, &reason, recovery)
            {
                Ok(()) => error,
                Err(record_error) => io::Error::other(ProcessOutcomeRecordFailure {
                    recovery,
                    process_error: error,
                    record_error,
                }),
            };
        }
        if !process_outcome_unknown(&error) {
            return error;
        }
        match self
            .context
            .record_effect_outcome_unknown(&self.request, "released process launch outcome is unknown")
        {
            Ok(()) => error,
            Err(record_error) => io::Error::other(ProcessUnknownOutcomeRecordFailure {
                process_error: error,
                record_error,
            }),
        }
    }
}

impl EffectExecutionContext {
    fn remember_process_recovery(
        &self,
        effect_id: &solaris_types::identity::EffectId,
        recovery: ProcessRecoveryRecord,
    ) {
        self.process_recoveries
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(effect_id.clone(), recovery);
    }

    pub(super) fn process_recovery_for_effect(
        &self,
        effect_id: &solaris_types::identity::EffectId,
    ) -> Option<ProcessRecoveryRecord> {
        self.process_recoveries
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(effect_id)
            .copied()
    }

    pub(super) fn clear_process_recovery(&self, effect_id: &solaris_types::identity::EffectId) {
        self.process_recoveries
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(effect_id);
    }
}

#[derive(Debug)]
struct ProcessOutcomeRecordFailure {
    recovery: ProcessRecoveryRecord,
    process_error: io::Error,
    record_error: io::Error,
}

impl std::fmt::Display for ProcessOutcomeRecordFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "process recovery {} requires reconciliation and its outcome record failed: {}",
            self.recovery.id().get(),
            self.record_error
        )
    }
}

impl std::error::Error for ProcessOutcomeRecordFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.process_error)
    }
}

#[derive(Debug)]
struct ProcessUnknownOutcomeRecordFailure {
    process_error: io::Error,
    record_error: io::Error,
}

impl std::fmt::Display for ProcessUnknownOutcomeRecordFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "released process outcome is unknown and its outcome record failed: {}",
            self.record_error
        )
    }
}

impl std::error::Error for ProcessUnknownOutcomeRecordFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.process_error)
    }
}

impl EffectExecutionContext {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn process_spawn_authorization(
        &self,
        request: EffectRequest,
        approved_environment: OperationEnvironmentSnapshot,
        approved_mode: PermissionMode,
        approved_ceiling: PermissionCeiling,
        workspace_root: Option<PathBuf>,
        approved_by_lease: bool,
        required_plugin_implementation: Option<ImplementationIdentity>,
    ) -> Result<ProcessSpawnAuthorization, String> {
        if request.descriptor.class != solaris_types::effect::EffectClass::Process {
            return Err("process spawn authorization requires a Process effect".to_owned());
        }
        validate_environment_plugin_identities(&approved_environment)?;
        Ok(ProcessSpawnAuthorization::new(Arc::new(EffectProcessSpawnAuthorizer {
            context: self.clone(),
            request,
            approved_environment,
            approved_mode,
            approved_ceiling,
            workspace_root,
            approved_by_lease,
            required_plugin_implementation,
            spawn_consumed: Mutex::new(false),
        })))
    }
}

fn permission_denied(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message.into())
}

#[cfg(test)]
#[path = "process_authorization_test.rs"]
mod process_authorization_test;
