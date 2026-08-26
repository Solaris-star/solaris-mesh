use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::{Value, json};
use solaris_types::identity::{RunId, TaskId};
use solaris_types::permission::PermissionCeiling;
use solaris_types::resource::ResourceBudget;
use solaris_types::runtime::TaskState;
use solaris_types::workflow::{
    AgentRoleDefinition, CollaborationSelection, ModelPolicy, OutputBinding, RetryPolicy, WorkflowDefinition,
    WorkflowNode, WorkflowNodeStatus,
};

use super::*;
use crate::builtin_workflows::ultracode;
use crate::execution_context::{EffectOutputStore, stable_digest_bytes};

fn node(id: &str, depends_on: &[&str]) -> WorkflowNode {
    WorkflowNode {
        id: id.into(),
        depends_on: depends_on.iter().map(|value| (*value).to_owned()).collect(),
        when: None,
        role: None,
        collaboration: CollaborationSelection::Inherit,
        model_policy: ModelPolicy::default(),
        capability_scope: Vec::new(),
        permission_ceiling: PermissionCeiling::unrestricted(),
        retry: RetryPolicy { max_attempts: 1 },
        timeout_ms: None,
        output_bindings: Vec::new(),
        workflow_ref: None,
    }
}

include!("workflow_controller_execution_test.rs");
include!("workflow_controller_registration_test.rs");
include!("workflow_controller_restore_test.rs");
include!("workflow_controller_deferred_restore_validation_test.rs");
include!("workflow_controller_restore_race_test.rs");
