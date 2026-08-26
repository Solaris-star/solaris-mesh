use serde::{Deserialize, Serialize};

use crate::effect::{EffectClass, ProcessInvocation};
use crate::identity::{EffectId, OperationId, RunId};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionMode {
    Plan,
    #[default]
    #[serde(alias = "default", alias = "auto_edit")]
    Auto,
    #[serde(alias = "yolo")]
    Bypass,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionDecision {
    Allow,
    AutoReview,
    Ask,
    Deny,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionCeiling {
    pub agent_lifecycle: bool,
    pub mesh_state_mutation: bool,
    pub workspace_mutation: bool,
    pub process: bool,
    pub network: bool,
    pub external_side_effect: bool,
}

impl PermissionCeiling {
    pub const fn plan() -> Self {
        Self {
            workspace_mutation: false,
            agent_lifecycle: true,
            mesh_state_mutation: true,
            process: false,
            network: true,
            external_side_effect: false,
        }
    }

    pub const fn unrestricted() -> Self {
        Self {
            workspace_mutation: true,
            agent_lifecycle: true,
            mesh_state_mutation: true,
            process: true,
            network: true,
            external_side_effect: true,
        }
    }

    pub fn allows(self, class: EffectClass) -> bool {
        match class {
            EffectClass::ReadOnly => true,
            EffectClass::AgentLifecycle => self.agent_lifecycle,
            EffectClass::MeshStateMutation => self.mesh_state_mutation,
            EffectClass::WorkspaceMutation => self.workspace_mutation,
            EffectClass::Process => self.process,
            EffectClass::Network => self.network,
            EffectClass::ExternalSideEffect => self.external_side_effect,
        }
    }

    pub fn intersect(self, other: Self) -> Self {
        Self {
            workspace_mutation: self.workspace_mutation && other.workspace_mutation,
            agent_lifecycle: self.agent_lifecycle && other.agent_lifecycle,
            mesh_state_mutation: self.mesh_state_mutation && other.mesh_state_mutation,
            process: self.process && other.process,
            network: self.network && other.network,
            external_side_effect: self.external_side_effect && other.external_side_effect,
        }
    }
}

impl Default for PermissionCeiling {
    fn default() -> Self {
        Self::unrestricted()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionRule {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capability: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effect_class: Option<EffectClass>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resource_prefixes: Vec<String>,
    pub decision: PermissionDecision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "scope", rename_all = "snake_case")]
pub enum LeaseScope {
    Effect { effect_id: EffectId },
    Operation { operation_id: OperationId },
    Run { run_id: RunId },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdditionalPermissions {
    #[serde(default)]
    pub unrestricted_file_reads: bool,
    #[serde(default)]
    pub unrestricted_file_writes: bool,
    #[serde(default)]
    pub unrestricted_network: bool,
    #[serde(default)]
    pub unrestricted_process: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub file_reads: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub file_writes: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub network_domains: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_command_prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub process_invocations: Vec<ProcessInvocation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityLease {
    #[serde(default)]
    pub lease_id: String,
    pub capability: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action: Option<String>,
    pub scope: LeaseScope,
    pub grants: AdditionalPermissions,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_uses: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionBoundary {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub readable_roots: Vec<String>,
    #[serde(default)]
    pub unrestricted_file_reads: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub writable_roots: Vec<String>,
    #[serde(default)]
    pub unrestricted_file_writes: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub network_domains: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub process_command_prefixes: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub process_invocations: Vec<ProcessInvocation>,
    #[serde(default)]
    pub unrestricted_process: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub external_resource_prefixes: Vec<String>,
    #[serde(default)]
    pub unrestricted_external_side_effects: bool,
    #[serde(default)]
    pub unrestricted_network: bool,
}

impl ExecutionBoundary {
    pub fn workspace(root: impl Into<String>) -> Self {
        let root = root.into();
        Self {
            readable_roots: vec![root.clone()],
            unrestricted_file_reads: false,
            writable_roots: vec![root],
            unrestricted_file_writes: false,
            network_domains: Vec::new(),
            process_command_prefixes: Vec::new(),
            process_invocations: Vec::new(),
            unrestricted_process: false,
            external_resource_prefixes: Vec::new(),
            unrestricted_external_side_effects: false,
            unrestricted_network: false,
        }
    }

    pub fn unrestricted() -> Self {
        Self {
            readable_roots: Vec::new(),
            unrestricted_file_reads: true,
            writable_roots: Vec::new(),
            unrestricted_file_writes: true,
            network_domains: Vec::new(),
            process_command_prefixes: Vec::new(),
            process_invocations: Vec::new(),
            unrestricted_process: true,
            external_resource_prefixes: Vec::new(),
            unrestricted_external_side_effects: true,
            unrestricted_network: true,
        }
    }
}

#[cfg(test)]
#[path = "permission_test.rs"]
mod permission_test;
/// Exact destinations requested by a sandboxed process contribution.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessNetworkConfig {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub network_domains: Vec<String>,
}
