use super::PermissionContext;
use solaris_process::WorkspaceRootLaunchCapability;
use solaris_types::effect::EffectRequest;
use solaris_types::identity::RunId;
use solaris_types::permission::{PermissionCeiling, PermissionMode};

impl PermissionContext {
    pub(crate) fn with_process_spawn_gate<T>(&self, authorize_and_spawn: impl FnOnce() -> T) -> T {
        let _gate = self.spawn_gate.read().unwrap_or_else(|error| error.into_inner());
        authorize_and_spawn()
    }

    /// Rechecks final process-spawn restrictions without invoking a reviewer.
    ///
    /// The caller must already hold an effect-scoped approval credential and
    /// decide whether an `Ask` or `AutoReview` result is covered by it.
    pub(crate) fn evaluate_effect_for_final_process_spawn(
        &self,
        run_id: &RunId,
        capability: &str,
        request: &EffectRequest,
        mode: PermissionMode,
        ceiling: PermissionCeiling,
    ) -> super::PermissionEvaluation {
        self.evaluate_effect_with_reviewer_policy(run_id, capability, request, mode, ceiling, false)
    }

    pub(crate) fn seal_workspace_root_for_process_spawn(
        &self,
        workspace_root: &std::path::Path,
    ) -> Result<WorkspaceRootLaunchCapability, String> {
        if !self.boundary_identities_valid() {
            return Err("execution boundary root identity changed".to_owned());
        }
        let capability = self
            .boundary_identities
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .seal_workspace_root(workspace_root)
            .map_err(|error| error.to_string())?;
        if self.boundary_identity_ceilings.iter().all(|identities| {
            identities
                .read()
                .unwrap_or_else(|error| error.into_inner())
                .validates_current_paths()
        }) {
            Ok(capability)
        } else {
            Err("execution boundary ceiling identity changed".to_owned())
        }
    }

    pub(crate) fn replace_with(&self, replacement: &Self) {
        let _spawn_gate = self.spawn_gate.write().unwrap_or_else(|error| error.into_inner());
        let policy = replacement
            .policy
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let ceiling = replacement.ceiling();
        let boundary = replacement.boundary();
        let boundary_identities = replacement
            .boundary_identities
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let configured_effects = replacement
            .configured_effects
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let reviewer = replacement
            .reviewer
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let leases = replacement.leases.entries();
        let replacement_protected_paths = replacement
            .protected_paths
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone();

        *self.policy.write().unwrap_or_else(|error| error.into_inner()) = policy;
        *self.ceiling.write().unwrap_or_else(|error| error.into_inner()) = ceiling;
        *self.boundary.write().unwrap_or_else(|error| error.into_inner()) = boundary;
        *self
            .boundary_identities
            .write()
            .unwrap_or_else(|error| error.into_inner()) = boundary_identities;
        *self
            .configured_effects
            .write()
            .unwrap_or_else(|error| error.into_inner()) = configured_effects;
        *self.reviewer.write().unwrap_or_else(|error| error.into_inner()) = reviewer;
        self.leases.replace_entries(leases);
        self.protected_paths
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .merge(&replacement_protected_paths);
    }
}
