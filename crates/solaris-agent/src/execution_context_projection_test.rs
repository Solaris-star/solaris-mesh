#[test]
fn process_effect_records_use_stable_secret_safe_projection() {
    use solaris_types::permission::PermissionDecision;

    let ledger: Arc<dyn RuntimeLedger> = Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default());
    let run_id = RunId::from("secret-process-run");
    let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    permissions.set_boundary(solaris_types::permission::ExecutionBoundary::workspace("workspace"));
    let context = EffectExecutionContext::new(
        run_id.clone(),
        AgentId::from("agent"),
        Arc::clone(&ledger),
        permissions,
        OperationEnvironmentSnapshot::default(),
    );
    let secret_command = "deploy --token mesh-secret-token-9812";
    let input = json!({"cmd": secret_command, "argv": ["deploy", "--token", "mesh-secret-token-9812"]});
    let tool = solaris_tools::exec_command::ExecCommandTool::new(std::env::temp_dir());
    let prepared_effect = tool
        .prepare_effect(context.effect_id_for_call("secret-process").as_str(), &input)
        .unwrap();
    let (descriptor, tool_execution) = prepared_effect.into_parts();
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(tool));
    let request = context.effect_request("secret-process", "ExecCommand", &input, descriptor);

    let evaluation = context.evaluate(&request);
    context
        .record_permission_decision(&request, &evaluation, "sentinel_test")
        .unwrap();
    let _registration = context.remember_approved_request_with_tool_context(
        request.clone(),
        tool_execution,
        PermissionMode::Auto,
        PermissionCeiling::unrestricted(),
    );
    let approved = context.take_approved_request_for_call("secret-process").unwrap();
    let revalidation_error = context.revalidate(&registry, &approved).unwrap_err();
    assert!(revalidation_error.contains("permission no longer allows effect"));
    context
        .record_revalidation_failure(&request, &revalidation_error)
        .unwrap();
    context.issue_approval_lease(&request, false).unwrap();
    context.record_effect_intent(&request).unwrap();

    let records = ledger.records_for_run(&run_id).unwrap();
    let journal = serde_json::to_string(&records).unwrap();
    assert!(!journal.contains(secret_command));
    assert!(!journal.contains("mesh-secret-token-9812"));
    assert!(journal.contains("sha256:"));
    let effect_records = records
        .iter()
        .filter(|record| {
            matches!(
                record.record_type.as_str(),
                "permission_decision" | "effect_intent" | "effect_revalidation_failed"
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(effect_records.len(), 3);
    let descriptor_digest = effect_records[0].payload["effect"]["descriptor_digest"]
        .as_str()
        .unwrap();
    assert!(descriptor_digest.starts_with("sha256:"));
    assert!(effect_records.iter().all(|record| {
        record.payload.get("descriptor").is_none()
            && record.payload["effect"]["descriptor_digest"].as_str() == Some(descriptor_digest)
    }));

    let restored_permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    restored_permissions.set_boundary(solaris_types::permission::ExecutionBoundary::workspace("workspace"));
    let restored = EffectExecutionContext::new(
        run_id,
        AgentId::from("agent"),
        ledger,
        restored_permissions,
        OperationEnvironmentSnapshot::default(),
    );
    assert_ne!(restored.evaluate(&request).decision, PermissionDecision::Allow);
    assert!(matches!(
        restored.recover_effect(&request).unwrap(),
        EffectRecoveryDecision::Reconcile { reason } if !reason.contains("identity conflicts")
    ));
}

#[test]
fn host_ledger_payload_redacts_legacy_process_and_credential_fields() {
    let secret = "legacy-secret-token-4419";
    let payload = json!({
        "descriptor": {
            "class": "process",
            "action": format!("execute {secret}"),
            "replay_policy": "reconcile_required",
            "resources": {
                "process_commands": [format!("deploy --token {secret}")],
                "process_invocations": [{"executable": "shell", "argv": ["-c", secret]}]
            }
        },
        "effective_input": {
            "cmd": format!("deploy --token {secret}"),
            "headers": {"Authorization": secret}
        },
        "client_secret": secret
    });

    let redacted = secret_safe_ledger_payload(payload);
    let serialized = serde_json::to_string(&redacted).unwrap();

    assert!(!serialized.contains(secret));
    assert!(serialized.contains("sha256:"));
    assert_eq!(redacted["effect"]["executables"][0]["argv_count"], 2);
    assert!(redacted.get("descriptor").is_none());
    assert_eq!(secret_safe_ledger_payload(redacted.clone()), redacted);
}

#[test]
fn host_ledger_filter_preserves_versioned_process_and_non_process_projections() {
    let descriptors = [
        EffectDescriptor {
            class: solaris_types::effect::EffectClass::Process,
            action: "run secret command".into(),
            resources: ResourceFootprint {
                process_commands: vec!["secret command".into()],
                process_invocations: vec![ProcessInvocation {
                    executable: "secret executable".into(),
                    argv: vec!["--secret".into()],
                }],
                ..Default::default()
            },
            replay_policy: solaris_types::effect::EffectReplayPolicy::Never,
        },
        EffectDescriptor {
            class: solaris_types::effect::EffectClass::WorkspaceMutation,
            action: "write secret path".into(),
            resources: ResourceFootprint {
                file_writes: vec!["secret/path".into()],
                ..Default::default()
            },
            replay_policy: solaris_types::effect::EffectReplayPolicy::ReconcileRequired,
        },
    ];

    for descriptor in descriptors {
        let projection = EffectAuditProjection::from_descriptor(&descriptor);
        let payload = json!({
            "effect": projection,
            "effective_input": {"secret": "sentinel"},
        });

        let once = secret_safe_ledger_payload(payload);
        let twice = secret_safe_ledger_payload(once.clone());

        assert_eq!(twice, once);
        let restored: EffectAuditProjection = serde_json::from_value(once["effect"].clone()).unwrap();
        assert!(restored.has_supported_version());
        assert_eq!(restored, EffectAuditProjection::from_descriptor(&descriptor));
    }
}

#[test]
fn host_ledger_filter_rejects_unversioned_or_unknown_effect_projections() {
    for version in [None, Some("solaris.effect-audit/v999")] {
        let mut projection = serde_json::to_value(EffectAuditProjection::from_descriptor(
            &EffectDescriptor::read_only("secret action"),
        ))
        .unwrap();
        match version {
            Some(version) => projection["version"] = json!(version),
            None => {
                projection.as_object_mut().unwrap().remove("version");
            }
        }

        let once = secret_safe_ledger_payload(json!({"effect": projection}));
        let twice = secret_safe_ledger_payload(once.clone());

        assert_eq!(twice, once);
        assert_eq!(once["effect"]["redacted"], true);
        assert_eq!(once["effect"]["version"], EffectAuditProjection::VERSION);
        assert!(
            once["effect"]["descriptor_digest"]
                .as_str()
                .unwrap()
                .starts_with("sha256:")
        );
    }
}

#[test]
fn host_ledger_filter_rejects_a_projection_with_user_derived_display_text() {
    let sentinel = "super-secret-token-projection-summary";
    let mut projection = serde_json::to_value(EffectAuditProjection::from_descriptor(&EffectDescriptor::read_only(
        "safe input",
    )))
    .unwrap();
    projection["action"]["summary"] = json!(sentinel);

    let filtered = secret_safe_ledger_payload(json!({"effect": projection}));
    let serialized = serde_json::to_string(&filtered).unwrap();

    assert!(!serialized.contains(sentinel));
    assert_eq!(filtered["effect"]["redacted"], true);
}

#[test]
fn host_ledger_filter_rejects_fallback_projection_with_extra_fields() {
    let sentinel = "super-secret-token-fallback-extra";
    let fallback = json!({
        "version": EffectAuditProjection::VERSION,
        "redacted": true,
        "descriptor_digest": format!("sha256:{}", "a".repeat(64)),
        "extra": sentinel,
    });

    let once = secret_safe_ledger_payload(json!({"effect": fallback}));
    let twice = secret_safe_ledger_payload(once.clone());
    let serialized = serde_json::to_string(&once).unwrap();

    assert_eq!(twice, once);
    assert!(!serialized.contains(sentinel));
    assert!(is_exact_fallback_projection(&once["effect"]));
}

#[test]
fn host_ledger_filter_rejects_forged_markers_and_extended_argv() {
    let sentinel = "super-secret-token-forged-marker";
    let uppercase_digest = format!("sha256:{}", "A".repeat(64));
    let arbitrary_label = format!("attacker:sha256:{}:{sentinel}", "a".repeat(64));
    let payload = json!({
        "command": arbitrary_label,
        "client_secret": uppercase_digest,
        "argv": [format!("sha256:{}", "b".repeat(64)), "argc:2", sentinel],
    });

    let once = secret_safe_ledger_payload(payload);
    let twice = secret_safe_ledger_payload(once.clone());
    let serialized = serde_json::to_string(&once).unwrap();

    assert_eq!(twice, once);
    assert!(!serialized.contains(sentinel));
    assert!(once["command"].as_str().is_some_and(is_redaction_marker));
    assert!(once["client_secret"].as_str().is_some_and(is_redaction_marker));
    assert_eq!(once["argv"].as_array().unwrap().len(), 2);
    assert!(once["argv"][0].as_str().is_some_and(is_bare_sha256_marker));
    assert!(once["argv"][1].as_str().is_some_and(is_argc_marker));
}

#[test]
fn host_ledger_filter_rejects_noncanonical_or_unbound_full_projections() {
    let descriptor = EffectDescriptor {
        class: solaris_types::effect::EffectClass::Network,
        action: "network request".into(),
        resources: ResourceFootprint {
            file_reads: vec!["input".into()],
            network_domains: vec!["example.test".into()],
            ..Default::default()
        },
        replay_policy: solaris_types::effect::EffectReplayPolicy::Never,
    };
    let projection = serde_json::to_value(EffectAuditProjection::from_descriptor(&descriptor)).unwrap();
    let mut forged = Vec::new();

    let mut duplicate_kind = projection.clone();
    let duplicate = duplicate_kind["resources"][0].clone();
    duplicate_kind["resources"].as_array_mut().unwrap().insert(1, duplicate);
    forged.push(duplicate_kind);

    let mut out_of_order = projection.clone();
    out_of_order["resources"].as_array_mut().unwrap().reverse();
    forged.push(out_of_order);

    let mut stale_descriptor_digest = projection;
    stale_descriptor_digest["resources"][0]["count"] = json!(2);
    forged.push(stale_descriptor_digest);

    for projection in forged {
        let once = secret_safe_ledger_payload(json!({"effect": projection}));
        let twice = secret_safe_ledger_payload(once.clone());

        assert_eq!(twice, once);
        assert!(is_exact_fallback_projection(&once["effect"]));
    }
}

#[test]
fn pinned_executable_cannot_be_swapped_to_unapproved_content() {
    let temp = tempfile::tempdir().unwrap();
    let executable = temp.path().join("approved-program");
    std::fs::write(&executable, b"approved-content").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let digest = stable_digest_bytes(b"approved-content");
    let pinned = pin_executable(&executable, &digest).unwrap();

    #[cfg(windows)]
    {
        let command = pinned.command().unwrap();
        assert!(std::fs::write(&executable, b"replacement-content").is_err());
        assert!(std::fs::remove_file(&executable).is_err());
        drop(command);
        std::fs::write(&executable, b"replacement-content").unwrap();
    }

    #[cfg(unix)]
    {
        let replacement = temp.path().join("replacement-program");
        std::fs::write(&replacement, b"replacement-content").unwrap();
        std::fs::remove_file(&executable).unwrap();
        std::fs::rename(&replacement, &executable).unwrap();
        assert_eq!(std::fs::read(&executable).unwrap(), b"replacement-content");
        assert!(pinned.command().is_ok());
    }
}

#[test]
fn completed_effect_requires_matching_output_integrity_fields() {
    for (index, field) in ["output_ref", "output_bytes", "output_digest"].into_iter().enumerate() {
        let ledger = Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default());
        let run_id = RunId::new(format!("integrity-run-{index}"));
        let context = EffectExecutionContext::new(
            run_id.clone(),
            AgentId::from("agent"),
            ledger.clone(),
            PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
            OperationEnvironmentSnapshot::default(),
        );
        let request = context.effect_request(
            "stable-call",
            "Effect",
            &json!({}),
            EffectDescriptor {
                class: solaris_types::effect::EffectClass::ExternalSideEffect,
                action: "test output integrity".into(),
                resources: ResourceFootprint::default(),
                replay_policy: solaris_types::effect::EffectReplayPolicy::ReplaySafe,
            },
        );
        context.record_effect_intent(&request).unwrap();
        context.record_effect_outcome(&request, false, "stored").unwrap();
        assert!(matches!(
            context.recover_effect(&request).unwrap(),
            EffectRecoveryDecision::Reuse { output, .. } if output == "stored"
        ));
        let mut payload = ledger
            .records_for_run(&run_id)
            .unwrap()
            .into_iter()
            .find(|record| record.record_type == "effect_outcome")
            .unwrap()
            .payload;
        match field {
            "output_ref" => payload["output_ref"] = json!("../forged-output"),
            "output_bytes" => payload["output_bytes"] = json!(7),
            "output_digest" => payload["output_digest"] = json!("sha256:mismatch"),
            _ => unreachable!(),
        }
        ledger
            .append(&run_id, DurabilityClass::SyncCritical, "effect_outcome", payload)
            .unwrap();

        let EffectRecoveryDecision::Reconcile { reason } = context.recover_effect(&request).unwrap() else {
            panic!("corrupt {field} must require reconciliation");
        };
        if field == "output_ref" {
            assert!(reason.contains("protected output failed integrity validation"));
        } else {
            assert!(reason.contains("durable output failed integrity validation"));
        }
    }
}
