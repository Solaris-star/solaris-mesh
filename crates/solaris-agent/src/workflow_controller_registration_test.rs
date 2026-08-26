#[test]
fn workflow_disable_reenable_and_definition_collision() {
    let controller = WorkflowController::default();
    let definition = WorkflowDefinition {
        id: "hot-workflow".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Test workflow".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("work", &[])],
        outputs: BTreeMap::new(),
    };
    controller.register(definition.clone()).unwrap();
    assert!(controller.is_enabled("hot-workflow"));
    assert!(controller.disable("hot-workflow").unwrap());
    assert!(!controller.is_enabled("hot-workflow"));
    assert!(
        controller
            .start(RunId::from("disabled-run"), "hot-workflow", json!({}))
            .unwrap_err()
            .contains("disabled")
    );
    assert!(controller.enable("hot-workflow").unwrap());
    assert!(
        controller
            .start(RunId::from("enabled-run"), "hot-workflow", json!({}))
            .is_ok()
    );

    let mut incompatible = definition;
    incompatible.version = "2".into();
    assert!(
        controller
            .preflight_register(&incompatible)
            .unwrap_err()
            .contains("different definition")
    );
}

#[test]
fn workflow_run_id_cannot_reuse_different_parameters() {
    let controller = WorkflowController::default();
    controller
        .register(WorkflowDefinition {
            id: "input-bound-workflow".into(),
            schema_version: 1,
            version: "1".into(),
            description: "Test workflow".into(),
            roles: Vec::new(),
            parameters_schema: None,
            nodes: vec![node("work", &[])],
            outputs: BTreeMap::new(),
        })
        .unwrap();
    let run_id = RunId::from("input-bound-run");
    controller
        .start(run_id.clone(), "input-bound-workflow", json!({"query":"first"}))
        .unwrap();

    let error = controller
        .start(run_id, "input-bound-workflow", json!({"query":"second"}))
        .unwrap_err();

    assert!(error.contains("different input"), "unexpected error: {error}");
}

#[test]
fn workflow_run_id_reuses_equivalent_normalized_parameters() {
    let controller = WorkflowController::default();
    controller
        .register(WorkflowDefinition {
            id: "normalized-input-workflow".into(),
            schema_version: 1,
            version: "1".into(),
            description: "Test workflow".into(),
            roles: Vec::new(),
            parameters_schema: None,
            nodes: vec![node("work", &[])],
            outputs: BTreeMap::new(),
        })
        .unwrap();
    let run_id = RunId::from("normalized-input-run");
    let first = controller
        .start(
            run_id.clone(),
            "normalized-input-workflow",
            json!({"nested":{"z":2,"a":1},"query":"same"}),
        )
        .unwrap();
    let reused = controller
        .start(
            run_id,
            "normalized-input-workflow",
            json!({"query":"same","nested":{"a":1,"z":2}}),
        )
        .unwrap();

    assert!(first.input_digest.is_some());
    assert_eq!(reused, first);
}

#[test]
fn workflow_run_reuse_rejects_tampered_definition_digest() {
    let controller = WorkflowController::default();
    controller
        .register(WorkflowDefinition {
            id: "definition-bound-workflow".into(),
            schema_version: 1,
            version: "1".into(),
            description: "Test workflow".into(),
            roles: Vec::new(),
            parameters_schema: None,
            nodes: vec![node("work", &[])],
            outputs: BTreeMap::new(),
        })
        .unwrap();
    let run_id = RunId::from("definition-bound-run");
    controller
        .start(run_id.clone(), "definition-bound-workflow", json!({"query":"same"}))
        .unwrap();
    controller
        .runs
        .write()
        .unwrap()
        .get_mut(&run_id)
        .unwrap()
        .snapshot
        .workflow_definition_digest = Some("tampered".to_owned());

    let error = controller
        .start(run_id, "definition-bound-workflow", json!({"query":"same"}))
        .unwrap_err();

    assert!(error.contains("definition digest"), "unexpected error: {error}");
}

#[test]
fn workflow_runtime_identity_controls_reuse_and_reports_start_state() {
    let controller = WorkflowController::default();
    controller
        .register(WorkflowDefinition {
            id: "runtime-bound-workflow".into(),
            schema_version: 1,
            version: "1".into(),
            description: "Test workflow".into(),
            roles: Vec::new(),
            parameters_schema: None,
            nodes: vec![node("work", &[])],
            outputs: BTreeMap::new(),
        })
        .unwrap();
    let run_id = RunId::from("runtime-bound-run");
    let identity = WorkflowRuntimeIdentity::new("provider-a", "model-a");
    let first = controller
        .start_with_runtime(
            run_id.clone(),
            "runtime-bound-workflow",
            json!({"query":"same"}),
            identity.clone(),
        )
        .unwrap();
    let reused = controller
        .start_with_runtime(
            run_id.clone(),
            "runtime-bound-workflow",
            json!({"query":"same"}),
            identity.clone(),
        )
        .unwrap();

    assert!(first.started);
    assert!(!reused.started);
    assert_eq!(first.snapshot, reused.snapshot);
    assert_eq!(first.snapshot.runtime_identity, Some(identity));

    let error = controller
        .start_with_runtime(
            run_id,
            "runtime-bound-workflow",
            json!({"query":"same"}),
            WorkflowRuntimeIdentity::new("provider-a", "model-b"),
        )
        .unwrap_err();
    assert!(error.contains("provider or model"), "unexpected error: {error}");
}

#[test]
fn runtime_bound_reuse_safely_rejects_legacy_snapshot_without_identity() {
    let controller = WorkflowController::default();
    controller
        .register(WorkflowDefinition {
            id: "legacy-runtime-workflow".into(),
            schema_version: 1,
            version: "1".into(),
            description: "Test workflow".into(),
            roles: Vec::new(),
            parameters_schema: None,
            nodes: vec![node("work", &[])],
            outputs: BTreeMap::new(),
        })
        .unwrap();
    let run_id = RunId::from("legacy-runtime-run");
    controller
        .start(run_id.clone(), "legacy-runtime-workflow", json!({"query":"same"}))
        .unwrap();

    let error = controller
        .start_with_runtime(
            run_id,
            "legacy-runtime-workflow",
            json!({"query":"same"}),
            WorkflowRuntimeIdentity::new("provider-a", "model-a"),
        )
        .unwrap_err();

    assert!(error.contains("provider or model"), "unexpected error: {error}");
}
#[test]
fn workflow_definition_owners_are_reference_counted_and_reversible() {
    use crate::collaboration_runtime::CollaborationRuntime;
    use crate::resource_policy::ResourcePolicy;
    use crate::role_registry::AgentRoleRegistry;
    use crate::scheduler::Scheduler;

    let runtime = Arc::new(CollaborationRuntime::new(Scheduler::new(ResourcePolicy::new(1))));
    let roles = Arc::new(AgentRoleRegistry::default());
    let controller = WorkflowController::with_runtime_and_roles(runtime, Some(Arc::clone(&roles)));
    let role = AgentRoleDefinition {
        id: "shared-role".into(),
        description: "Shared role".into(),
        input_schema: None,
        output_schema: None,
        model_policy: ModelPolicy::default(),
        capability_scope: Vec::new(),
        permission_ceiling: PermissionCeiling::plan(),
        context_policy: Some("isolated".into()),
        recursion_policy: Some("none".into()),
        budget: ResourceBudget::default(),
    };
    let mut workflow_node = node("work", &[]);
    workflow_node.role = Some(role.id.clone());
    let definition = WorkflowDefinition {
        id: "shared-workflow".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Shared definition".into(),
        roles: vec![role],
        parameters_schema: None,
        nodes: vec![workflow_node],
        outputs: BTreeMap::new(),
    };

    controller.register_owned("plugin:a", definition.clone()).unwrap();
    controller.register_owned("plugin:b", definition).unwrap();
    assert!(!controller.unregister_owned("plugin:a", "shared-workflow").unwrap());
    assert!(controller.definition("shared-workflow").is_some());
    assert!(roles.get("shared-role").is_some());
    assert!(controller.unregister_owned("plugin:b", "shared-workflow").unwrap());
    assert!(controller.definition("shared-workflow").is_none());
    assert!(roles.get("shared-role").is_none());
}

#[test]
fn failed_batch_registration_keeps_preexisting_role_owner() {
    use crate::collaboration_runtime::CollaborationRuntime;
    use crate::resource_policy::ResourcePolicy;
    use crate::role_registry::AgentRoleRegistry;
    use crate::scheduler::Scheduler;

    let runtime = Arc::new(CollaborationRuntime::new(Scheduler::new(ResourcePolicy::new(1))));
    let roles = Arc::new(AgentRoleRegistry::default());
    let controller = WorkflowController::with_runtime_and_roles(runtime, Some(Arc::clone(&roles)));
    let existing_role = AgentRoleDefinition {
        id: "rollback-role".into(),
        description: "Existing role".into(),
        input_schema: None,
        output_schema: None,
        model_policy: ModelPolicy::default(),
        capability_scope: Vec::new(),
        permission_ceiling: PermissionCeiling::plan(),
        context_policy: Some("isolated".into()),
        recursion_policy: Some("none".into()),
        budget: ResourceBudget::default(),
    };
    let mut existing_node = node("work", &[]);
    existing_node.role = Some(existing_role.id.clone());
    let existing_definition = WorkflowDefinition {
        id: "existing-role-owner".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Existing workflow".into(),
        roles: vec![existing_role.clone()],
        parameters_schema: None,
        nodes: vec![existing_node],
        outputs: BTreeMap::new(),
    };
    controller
        .register_owned("plugin:owner", existing_definition.clone())
        .unwrap();

    let mut conflicting_role = existing_role.clone();
    conflicting_role.description = "Conflicting role".into();
    let mut conflicting_node = node("work", &[]);
    conflicting_node.role = Some(conflicting_role.id.clone());
    let conflicting_definition = WorkflowDefinition {
        id: "conflicting-role-owner".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Conflicting workflow".into(),
        roles: vec![conflicting_role],
        parameters_schema: None,
        nodes: vec![conflicting_node],
        outputs: BTreeMap::new(),
    };

    let error = controller
        .register_owned_batch(
            "plugin:owner",
            vec![existing_definition.clone(), conflicting_definition],
        )
        .unwrap_err();

    assert!(error.contains("different definition"));
    assert_eq!(roles.get(&existing_role.id), Some(existing_role));
    assert_eq!(
        controller.definition(&existing_definition.id),
        Some(existing_definition)
    );
}
#[test]
fn workflow_v1_preflight_rejects_unknown_contract_references() {
    use crate::collaboration_runtime::CollaborationRuntime;
    use crate::resource_policy::ResourcePolicy;
    use crate::role_registry::AgentRoleRegistry;
    use crate::scheduler::Scheduler;

    let runtime = Arc::new(CollaborationRuntime::new(Scheduler::new(ResourcePolicy::new(1))));
    let roles = Arc::new(AgentRoleRegistry::default());
    let controller = WorkflowController::with_runtime_and_roles(runtime, Some(roles));
    let mut unknown_role = WorkflowDefinition {
        id: "invalid-v1".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Invalid Workflow".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("work", &[])],
        outputs: BTreeMap::new(),
    };
    unknown_role.nodes[0].role = Some("missing-role".into());
    assert!(
        controller
            .preflight_register(&unknown_role)
            .unwrap_err()
            .contains("unknown role")
    );

    let mut unknown_workflow = unknown_role.clone();
    unknown_workflow.nodes[0].role = None;
    unknown_workflow.nodes[0].workflow_ref = Some("missing-workflow".into());
    assert!(
        controller
            .preflight_register(&unknown_workflow)
            .unwrap_err()
            .contains("unknown workflow")
    );

    let mut unsupported_schema = unknown_workflow;
    unsupported_schema.nodes[0].workflow_ref = None;
    unsupported_schema.schema_version = 3;
    assert!(
        controller
            .preflight_register(&unsupported_schema)
            .unwrap_err()
            .contains("unsupported schema version")
    );

    let no_registry = WorkflowController::default();
    assert!(
        no_registry
            .preflight_register(&unknown_role)
            .unwrap_err()
            .contains("unknown role")
    );
}

#[test]
fn workflow_registration_accepts_v1_and_typed_v2_but_rejects_v1_configured() {
    let controller = WorkflowController::default();
    let v1 = WorkflowDefinition {
        id: "legacy-v1".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Legacy workflow".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("work", &[])],
        outputs: BTreeMap::new(),
    };
    controller.register(v1).unwrap();

    let mut v2_node = node("work", &[]);
    v2_node.collaboration = CollaborationSelection::Configured(solaris_types::workflow::CollaborationRuntimeConfig {
        strategy: solaris_types::workflow::CollaborationStrategy::Single,
        ..solaris_types::workflow::CollaborationRuntimeConfig::default()
    });
    let v2 = WorkflowDefinition {
        id: "typed-v2".into(),
        schema_version: 2,
        version: "1".into(),
        description: "Typed workflow".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![v2_node.clone()],
        outputs: BTreeMap::new(),
    };
    controller.register(v2).unwrap();

    let invalid_v1 = WorkflowDefinition {
        id: "configured-v1".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Invalid legacy workflow".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![v2_node],
        outputs: BTreeMap::new(),
    };
    assert!(
        controller
            .preflight_register(&invalid_v1)
            .unwrap_err()
            .contains("schema version 1")
    );
}

#[test]
fn workflow_v2_registration_accepts_roles_from_the_external_registry() {
    use solaris_types::workflow::{CollaborationRuntimeConfig, CollaborationStrategy, WorkerRolePolicy};

    use crate::collaboration_runtime::CollaborationRuntime;
    use crate::resource_policy::ResourcePolicy;
    use crate::role_registry::AgentRoleRegistry;
    use crate::scheduler::Scheduler;

    let runtime = Arc::new(CollaborationRuntime::new(Scheduler::new(ResourcePolicy::new(1))));
    let roles = Arc::new(AgentRoleRegistry::default());
    for role_id in ["external-coordinator", "external-worker-a", "external-worker-b"] {
        roles.register(AgentRoleDefinition {
            id: role_id.into(),
            description: format!("External role {role_id}"),
            input_schema: None,
            output_schema: None,
            model_policy: ModelPolicy::default(),
            capability_scope: Vec::new(),
            permission_ceiling: PermissionCeiling::plan(),
            context_policy: Some("isolated".into()),
            recursion_policy: Some("none".into()),
            budget: ResourceBudget::default(),
        });
    }
    let controller = WorkflowController::with_runtime_and_roles(runtime, Some(roles));
    let mut work = node("work", &[]);
    work.role = Some("external-coordinator".into());
    work.collaboration = CollaborationSelection::Configured(CollaborationRuntimeConfig {
        strategy: CollaborationStrategy::Team,
        worker_roles: ["external-worker-a", "external-worker-b"]
            .into_iter()
            .map(|role| WorkerRolePolicy {
                role: role.into(),
                max_concurrent: 1,
                max_total: 1,
            })
            .collect(),
        ..CollaborationRuntimeConfig::default()
    });
    let definition = WorkflowDefinition {
        id: "external-role-v2".into(),
        schema_version: 2,
        version: "1".into(),
        description: "Registry-only roles".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![work],
        outputs: BTreeMap::new(),
    };

    controller.register(definition).unwrap();
    assert!(controller.definition("external-role-v2").is_some());
}

#[test]
fn workflow_preflight_rejects_malformed_or_implicit_condition_references() {
    let controller = WorkflowController::default();
    let source = node("source", &[]);
    let mut implicit_reference = node("conditional", &[]);
    implicit_reference.when = Some(json!({
        "node_output_equals": {
            "node": "source",
            "field": "ok",
            "value": true,
        }
    }));
    let implicit_definition = WorkflowDefinition {
        id: "implicit-condition-reference".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Invalid condition".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![source.clone(), implicit_reference],
        outputs: BTreeMap::new(),
    };

    let implicit_error = controller.preflight_register(&implicit_definition).unwrap_err();
    assert!(implicit_error.contains("explicit dependency"));

    let mut malformed = node("conditional", &["source"]);
    malformed.when = Some(json!({"node_output_equals": {"node": "source"}}));
    let malformed_definition = WorkflowDefinition {
        id: "malformed-condition".into(),
        nodes: vec![source.clone(), malformed],
        ..implicit_definition.clone()
    };
    let malformed_error = controller.preflight_register(&malformed_definition).unwrap_err();
    assert!(malformed_error.contains("condition"));

    let mut unknown = node("conditional", &["source"]);
    unknown.when = Some(json!({"unknown_operator": true}));
    let unknown_definition = WorkflowDefinition {
        id: "unknown-condition".into(),
        nodes: vec![source, unknown],
        ..implicit_definition
    };
    let unknown_error = controller.preflight_register(&unknown_definition).unwrap_err();
    assert!(unknown_error.contains("condition"));
}

#[test]
fn workflow_batch_registration_supports_subworkflows_and_rejects_reference_cycles_atomically() {
    let controller = WorkflowController::default();
    let child = WorkflowDefinition {
        id: "batch-child".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Child".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("work", &[])],
        outputs: BTreeMap::new(),
    };
    let mut parent_node = node("delegate", &[]);
    parent_node.workflow_ref = Some(child.id.clone());
    let parent = WorkflowDefinition {
        id: "batch-parent".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Parent".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![parent_node],
        outputs: BTreeMap::new(),
    };
    controller
        .register_owned_batch("plugin:batch", vec![parent.clone(), child.clone()])
        .unwrap();
    assert!(controller.definition("batch-parent").is_some());
    assert!(controller.definition("batch-child").is_some());

    let isolated = WorkflowController::default();
    let mut a = parent;
    a.id = "cycle-a".into();
    a.nodes[0].workflow_ref = Some("cycle-b".into());
    let mut b = child;
    b.id = "cycle-b".into();
    b.nodes[0].workflow_ref = Some("cycle-a".into());
    assert!(
        isolated
            .register_owned_batch("plugin:cycle", vec![a, b])
            .unwrap_err()
            .contains("cycle")
    );
    assert!(isolated.definitions().is_empty());
}

#[test]
fn unregister_last_owner_rejects_a_still_referenced_workflow() {
    let controller = WorkflowController::default();
    let child = WorkflowDefinition {
        id: "referenced-child".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Child".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("work", &[])],
        outputs: BTreeMap::new(),
    };
    let mut parent_node = node("delegate", &[]);
    parent_node.workflow_ref = Some(child.id.clone());
    let parent = WorkflowDefinition {
        id: "referencing-parent".into(),
        schema_version: 1,
        version: "1".into(),
        description: "Parent".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![parent_node],
        outputs: BTreeMap::new(),
    };
    controller
        .register_owned_batch("plugin:owner", vec![parent.clone(), child.clone()])
        .unwrap();

    let error = controller.unregister_owned("plugin:owner", &child.id).unwrap_err();

    assert!(error.contains("referenced"));
    assert_eq!(controller.definition(&child.id), Some(child.clone()));
    assert!(controller.unregister_owned("plugin:owner", &parent.id).unwrap());
    assert!(controller.unregister_owned("plugin:owner", &child.id).unwrap());
}

#[test]
fn concurrent_conflicting_registrations_keep_definition_and_owner_consistent() {
    use std::sync::Barrier;

    let controller = Arc::new(WorkflowController::default());
    let first = WorkflowDefinition {
        id: "concurrent-registration".into(),
        schema_version: 1,
        version: "1".into(),
        description: "First definition".into(),
        roles: Vec::new(),
        parameters_schema: None,
        nodes: vec![node("first", &[])],
        outputs: BTreeMap::new(),
    };
    let mut second = first.clone();
    second.version = "2".into();
    second.description = "Second definition".into();
    second.nodes[0].id = "second".into();

    let gate = Arc::new(Barrier::new(3));
    let first_controller = Arc::clone(&controller);
    let first_gate = Arc::clone(&gate);
    let first_thread = std::thread::spawn(move || {
        first_gate.wait();
        first_controller.register_owned("plugin:first", first)
    });
    let second_controller = Arc::clone(&controller);
    let second_gate = Arc::clone(&gate);
    let second_thread = std::thread::spawn(move || {
        second_gate.wait();
        second_controller.register_owned("plugin:second", second)
    });

    gate.wait();
    let first_result = first_thread.join().unwrap();
    let second_result = second_thread.join().unwrap();

    assert_ne!(first_result.is_ok(), second_result.is_ok());
    let definition = controller.definition("concurrent-registration").unwrap();
    let owners = controller.definition_owners.read().unwrap();
    let owners = owners.get("concurrent-registration").unwrap();
    if definition.version == "1" {
        assert_eq!(owners, &HashSet::from(["plugin:first".to_owned()]));
    } else {
        assert_eq!(definition.version, "2");
        assert_eq!(owners, &HashSet::from(["plugin:second".to_owned()]));
    }
}

#[test]
fn concurrent_start_and_unregister_have_a_single_valid_order() {
    use std::sync::Barrier;

    let controller = Arc::new(WorkflowController::default());
    controller
        .register_owned(
            "plugin:owner",
            WorkflowDefinition {
                id: "start-or-unregister".into(),
                schema_version: 1,
                version: "1".into(),
                description: "Lifecycle test".into(),
                roles: Vec::new(),
                parameters_schema: None,
                nodes: vec![node("work", &[])],
                outputs: BTreeMap::new(),
            },
        )
        .unwrap();

    let gate = Arc::new(Barrier::new(3));
    let start_controller = Arc::clone(&controller);
    let start_gate = Arc::clone(&gate);
    let start_thread = std::thread::spawn(move || {
        start_gate.wait();
        start_controller.start(RunId::from("start-or-unregister-run"), "start-or-unregister", json!({}))
    });
    let unregister_controller = Arc::clone(&controller);
    let unregister_gate = Arc::clone(&gate);
    let unregister_thread = std::thread::spawn(move || {
        unregister_gate.wait();
        unregister_controller.unregister_owned("plugin:owner", "start-or-unregister")
    });

    gate.wait();
    let start_result = start_thread.join().unwrap();
    let unregister_result = unregister_thread.join().unwrap();

    match (start_result, unregister_result) {
        (Ok(snapshot), Err(error)) => {
            assert_eq!(snapshot.status, WorkflowRunStatus::Running);
            assert!(error.contains("running Run"));
            assert!(controller.definition("start-or-unregister").is_some());
        }
        (Err(error), Ok(true)) => {
            assert!(error.contains("unknown workflow"));
            assert!(controller.definition("start-or-unregister").is_none());
        }
        (start, unregister) => panic!("invalid lifecycle results: {start:?}, {unregister:?}"),
    }
}

#[test]
fn concurrent_start_and_disable_have_a_single_valid_order() {
    use std::sync::Barrier;

    let controller = Arc::new(WorkflowController::default());
    controller
        .register(WorkflowDefinition {
            id: "start-or-disable".into(),
            schema_version: 1,
            version: "1".into(),
            description: "Lifecycle test".into(),
            roles: Vec::new(),
            parameters_schema: None,
            nodes: vec![node("work", &[])],
            outputs: BTreeMap::new(),
        })
        .unwrap();

    let gate = Arc::new(Barrier::new(3));
    let start_controller = Arc::clone(&controller);
    let start_gate = Arc::clone(&gate);
    let start_thread = std::thread::spawn(move || {
        start_gate.wait();
        start_controller.start(RunId::from("start-or-disable-run"), "start-or-disable", json!({}))
    });
    let disable_controller = Arc::clone(&controller);
    let disable_gate = Arc::clone(&gate);
    let disable_thread = std::thread::spawn(move || {
        disable_gate.wait();
        disable_controller.disable("start-or-disable")
    });

    gate.wait();
    let start_result = start_thread.join().unwrap();
    assert_eq!(disable_thread.join().unwrap(), Ok(true));

    match start_result {
        Ok(snapshot) => {
            assert_eq!(snapshot.status, WorkflowRunStatus::Running);
            assert_eq!(controller.snapshot(&snapshot.run_id), Some(snapshot));
        }
        Err(error) => assert!(error.contains("disabled")),
    }
    assert!(!controller.is_enabled("start-or-disable"));
}
