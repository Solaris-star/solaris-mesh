use std::sync::Arc;

use serde_json::json;
use solaris_types::effect::DurabilityClass;
use solaris_types::permission::PermissionCeiling;
use solaris_types::workflow::{
    CollaborationRuntimeConfig, CollaborationSelection, ModelPolicy, RetryPolicy, WorkerRolePolicy, WorkflowNode,
};

use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};

use super::*;

#[test]
fn legacy_v1_supervisor_completion_reads_its_unique_v1_finalize_decision() {
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let runtime_ledger: Arc<dyn RuntimeLedger> = ledger.clone();
    let controller = WorkflowController::new(runtime_ledger.clone());
    let root_run_id = RunId::from("legacy-completion-root");
    let workflow_run_id = RunId::from("legacy-completion-root:workflow:one");
    let attempt_id = AttemptId::from("legacy-completion-attempt");
    let output = json!({"legacy": true});
    let serialized = serde_json::to_string(&output).unwrap();
    let output_reference = EffectOutputStore::for_run_with_ledger(&workflow_run_id, runtime_ledger.as_ref())
        .write_named("legacy-supervisor-final", &serialized)
        .unwrap();
    let output_digest = stable_digest_bytes(serialized.as_bytes());
    let final_output_ref = OutcomeBlobRef {
        reference: output_reference.clone(),
        bytes: serialized.len() as u64,
        digest: output_digest.clone(),
        run_id: Some(workflow_run_id.clone()),
        status: Some(AgentOutcomeStatus::Completed),
    };
    let hydrated_decision = json!({"decision": "finalize", "output": output});
    let decision_digest = stable_digest_value(&json!({
        "schema_version": 1,
        "workflow_run_id": workflow_run_id,
        "workflow_id": "workflow:legacy-completion",
        "node_id": "work",
        "attempt_id": attempt_id,
        "round": 0,
        "input_message_ids": [],
        "input_deliveries": [],
        "turns": 1,
        "usage": {"input_tokens": 0, "output_tokens": 0, "cache_creation_tokens": 0, "cache_read_tokens": 0},
        "final_output_ref": final_output_ref,
        "decision": hydrated_decision,
    }));
    ledger
        .append(
            &root_run_id,
            DurabilityClass::SyncCritical,
            "workflow_supervisor_decision",
            json!({
                "schema_version": 1,
                "workflow_run_id": workflow_run_id,
                "workflow_id": "workflow:legacy-completion",
                "node_id": "work",
                "attempt_id": attempt_id,
                "round": 0,
                "input_message_ids": [],
                "input_deliveries": [],
                "decision_digest": decision_digest,
                "turns": 1,
                "usage": {"input_tokens": 0, "output_tokens": 0, "cache_creation_tokens": 0, "cache_read_tokens": 0},
                "final_output_ref": final_output_ref,
                "decision": {"decision": "finalize", "output": null},
            }),
        )
        .unwrap();
    let completion = ledger
        .append(
            &workflow_run_id,
            DurabilityClass::SyncCritical,
            "workflow_node_completed",
            json!({
                "node_id": "work",
                "attempt_id": attempt_id,
                "output_ref": output_reference,
                "output_digest": output_digest,
                "output_bytes": serialized.len(),
                "output_status": "completed",
            }),
        )
        .unwrap();
    let definition = WorkflowDefinition {
        id: "legacy-completion".to_owned(),
        schema_version: 2,
        version: "1".to_owned(),
        description: String::new(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![WorkflowNode {
            id: "work".to_owned(),
            depends_on: Vec::new(),
            when: None,
            role: Some("coordinator".to_owned()),
            collaboration: CollaborationSelection::Configured(CollaborationRuntimeConfig {
                strategy: CollaborationStrategy::Supervisor,
                worker_roles: vec![WorkerRolePolicy {
                    role: "worker".to_owned(),
                    max_concurrent: 1,
                    max_total: 1,
                }],
                ..CollaborationRuntimeConfig::default()
            }),
            model_policy: ModelPolicy::default(),
            capability_scope: Vec::new(),
            permission_ceiling: PermissionCeiling::unrestricted(),
            retry: RetryPolicy { max_attempts: 1 },
            timeout_ms: None,
            output_bindings: Vec::new(),
            workflow_ref: None,
        }],
        outputs: Default::default(),
    };

    controller
        .validate_completion_source(&definition, &workflow_run_id, &completion)
        .unwrap();
}
