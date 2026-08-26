use std::sync::atomic::{AtomicBool, Ordering};

use crate::runtime_ledger::LedgerRecord;

struct EffectOutcomeAppendResultUnknownLedger {
    inner: crate::runtime_ledger::InMemoryRuntimeLedger,
    failed: AtomicBool,
}

impl Default for EffectOutcomeAppendResultUnknownLedger {
    fn default() -> Self {
        Self {
            inner: crate::runtime_ledger::InMemoryRuntimeLedger::default(),
            failed: AtomicBool::new(false),
        }
    }
}

impl RuntimeLedger for EffectOutcomeAppendResultUnknownLedger {
    fn logical_append_capability(&self) -> crate::runtime_ledger::LogicalAppendCapability {
        self.inner.logical_append_capability()
    }

    fn acquire_workflow_mutation_lease(
        &self,
        run_id: &RunId,
        owner_id: &str,
        now_unix_ms: i64,
    ) -> std::io::Result<crate::runtime_ledger::WorkflowMutationLease> {
        self.inner.acquire_workflow_mutation_lease(run_id, owner_id, now_unix_ms)
    }

    fn renew_workflow_mutation_lease(
        &self,
        lease: &crate::runtime_ledger::WorkflowMutationLease,
        now_unix_ms: i64,
    ) -> std::io::Result<crate::runtime_ledger::WorkflowMutationLease> {
        self.inner.renew_workflow_mutation_lease(lease, now_unix_ms)
    }

    fn commit_workflow_restore(
        &self,
        lease: &crate::runtime_ledger::WorkflowMutationLease,
        expected_sequence: u64,
        now_unix_ms: i64,
    ) -> std::io::Result<crate::runtime_ledger::WorkflowRestoreCommit> {
        self.inner.commit_workflow_restore(lease, expected_sequence, now_unix_ms)
    }

    fn release_workflow_mutation_lease(
        &self,
        lease: &crate::runtime_ledger::WorkflowMutationLease,
    ) -> std::io::Result<()> {
        self.inner.release_workflow_mutation_lease(lease)
    }

    fn append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        payload: serde_json::Value,
    ) -> std::io::Result<LedgerRecord> {
        self.inner.append(run_id, durability, record_type, payload)
    }

    fn append_under_workflow_lease(
        &self,
        lease: &crate::runtime_ledger::WorkflowMutationLease,
        now_unix_ms: i64,
        durability: DurabilityClass,
        record_type: &str,
        payload: serde_json::Value,
    ) -> std::io::Result<LedgerRecord> {
        self.inner
            .append_under_workflow_lease(lease, now_unix_ms, durability, record_type, payload)
    }

    fn compare_and_append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        identity_fields: &[&str],
        payload: serde_json::Value,
    ) -> std::io::Result<LedgerRecord> {
        let record = self
            .inner
            .compare_and_append(run_id, durability, record_type, identity_fields, payload)?;
        if record_type == "effect_outcome" && !self.failed.swap(true, Ordering::SeqCst) {
            return Err(std::io::Error::other(
                "injected failure after durable effect outcome append",
            ));
        }
        Ok(record)
    }

    fn compare_and_append_under_workflow_lease(
        &self,
        lease: &crate::runtime_ledger::WorkflowMutationLease,
        now_unix_ms: i64,
        durability: DurabilityClass,
        record_type: &str,
        identity_fields: &[&str],
        payload: serde_json::Value,
    ) -> std::io::Result<LedgerRecord> {
        self.inner.compare_and_append_under_workflow_lease(
            lease,
            now_unix_ms,
            durability,
            record_type,
            identity_fields,
            payload,
        )
    }

    fn run_ids(&self) -> std::io::Result<Vec<RunId>> {
        self.inner.run_ids()
    }

    fn records_for_run(&self, run_id: &RunId) -> std::io::Result<Vec<LedgerRecord>> {
        self.inner.records_for_run(run_id)
    }

    fn effect_output_root(&self) -> Option<std::path::PathBuf> {
        self.inner.effect_output_root()
    }
}

#[test]
fn effect_outcome_append_result_unknown_reuses_the_single_durable_record() {
    let ledger = Arc::new(EffectOutcomeAppendResultUnknownLedger::default());
    let run_id = RunId::from("effect-outcome-append-result-unknown");
    let context = EffectExecutionContext::new(
        run_id.clone(),
        AgentId::from("agent"),
        ledger.clone(),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let request = context.effect_request(
        "stable-call",
        "ExternalCommand",
        &json!({"command": "side-effect"}),
        EffectDescriptor {
            class: solaris_types::effect::EffectClass::Process,
            action: "run external command".into(),
            resources: ResourceFootprint::default(),
            replay_policy: EffectReplayPolicy::Never,
        },
    );
    context.record_effect_intent(&request).unwrap();

    context.record_effect_outcome(&request, false, "completed once").unwrap();
    context.record_effect_outcome(&request, false, "completed once").unwrap();

    let outcomes = ledger
        .records_for_run(&run_id)
        .unwrap()
        .into_iter()
        .filter(|record| record.record_type == "effect_outcome")
        .collect::<Vec<_>>();
    assert_eq!(outcomes.len(), 1);
    assert_eq!(
        context.recover_effect(&request).unwrap(),
        EffectRecoveryDecision::Reuse {
            is_error: false,
            output: "completed once".into(),
        }
    );
}

#[test]
fn durable_effect_intent_is_fenced_after_session_lease_takeover() {
    use rusqlite::Connection;
    use tempfile::tempdir;

    use crate::session::SessionManager;

    let directory = tempdir().unwrap();
    let first = SessionManager::new(directory.path().to_path_buf(), 20);
    first
        .create_active_session("provider", "model", "workspace", Some("effect-fence"), "fenced-run")
        .unwrap();
    let fence = first.active_fence("effect-fence").unwrap();
    let ledger = Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default());
    let context = EffectExecutionContext::new(
        RunId::from("fenced-run"),
        AgentId::from("agent"),
        ledger.clone(),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    )
    .with_session_fence(fence);
    let connection = Connection::open(directory.path().join("session.sqlite3")).unwrap();
    connection
        .execute(
            "UPDATE session_leases SET heartbeat_at_ms = 0, expires_at_ms = 0
             WHERE session_id = 'effect-fence'",
            [],
        )
        .unwrap();
    drop(connection);
    let second = SessionManager::new(directory.path().to_path_buf(), 20);
    second.load_active_session("effect-fence").unwrap();
    let request = context.effect_request(
        "must-not-start",
        "Write",
        &json!({}),
        EffectDescriptor {
            class: solaris_types::effect::EffectClass::ExternalSideEffect,
            action: "must not execute".into(),
            resources: ResourceFootprint::default(),
            replay_policy: EffectReplayPolicy::Never,
        },
    );

    let error = context.record_effect_intent(&request).unwrap_err();

    assert!(error.to_string().contains("session lease"));
    assert!(ledger.records_for_run(&RunId::from("fenced-run")).unwrap().is_empty());
    second.release_active_session().unwrap();
}

#[test]
fn non_replayable_effect_with_prior_intent_requires_reconciliation() {
    let ledger = Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default());
    let context = EffectExecutionContext::new(
        RunId::from("replay-effect-run"),
        AgentId::from("agent"),
        ledger,
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let request = context.effect_request(
        "stable-call",
        "ExternalCommand",
        &json!({"value": 1}),
        EffectDescriptor {
            class: solaris_types::effect::EffectClass::Process,
            action: "run external command".into(),
            resources: ResourceFootprint::default(),
            replay_policy: solaris_types::effect::EffectReplayPolicy::Never,
        },
    );
    context.record_effect_intent(&request).unwrap();

    let decision = context.recover_effect(&request).unwrap();

    assert!(matches!(
        decision,
        EffectRecoveryDecision::Reconcile { reason } if reason.contains("requires reconciliation")
    ));
}

#[test]
fn recovery_recomputes_the_live_permission_fingerprint() {
    use solaris_config::config::{CliArgs, Config};
    use solaris_types::effect::{EffectClass, EffectReplayPolicy};

    let workspace = tempfile::tempdir().unwrap();
    let runtime = workspace.path().join(".solaris").join("runtime");
    std::fs::create_dir_all(&runtime).unwrap();
    let ledger_path = runtime.join("ledger.sqlite3");
    std::fs::write(&ledger_path, b"state").unwrap();
    let config = Config::resolve(&CliArgs {
        provider: Some("anthropic".into()),
        api_key: Some("test".into()),
        base_url: Some("https://provider.example.test".into()),
        model: Some("test-model".into()),
        max_tokens: None,
        thinking: None,
        thinking_budget: None,
        max_turns: None,
        max_tool_call_malformed_turns: None,
        max_tool_call_failure_turns: None,
        system_prompt: None,
        profile: None,
        auto_approve: false,
        project_dir: Some(workspace.path().to_path_buf()),
    })
    .unwrap();
    let tools = ToolRegistry::new();
    for (index, change) in ["auto", "plan", "ceiling", "protected"].into_iter().enumerate() {
        let permissions = PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted());
        permissions.set_boundary(ExecutionBoundary::unrestricted());
        let environment = build_environment_snapshot(&config, &tools, &permissions);
        let context = EffectExecutionContext::new(
            RunId::new(format!("live-permission-recovery-{index}")),
            AgentId::from("agent"),
            Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default()),
            permissions.clone(),
            environment,
        );
        let descriptor = EffectDescriptor {
            class: EffectClass::ReadOnly,
            action: "read runtime state".into(),
            resources: ResourceFootprint {
                file_reads: vec![ledger_path.to_string_lossy().into_owned()],
                ..Default::default()
            },
            replay_policy: EffectReplayPolicy::ReplaySafe,
        };
        let request = context.effect_request("stable-read", "Read", &json!({}), descriptor);
        context.record_effect_intent(&request).unwrap();
        context.record_effect_outcome(&request, false, "old state").unwrap();

        match change {
            "auto" => permissions.set_mode(PermissionMode::Auto),
            "plan" => permissions.set_mode(PermissionMode::Plan),
            "ceiling" => permissions.set_ceiling(PermissionCeiling::plan()),
            "protected" => permissions
                .register_protected_paths(&runtime, vec![ledger_path.clone()])
                .unwrap(),
            _ => unreachable!(),
        }

        assert!(
            matches!(
                context.recover_effect(&request).unwrap(),
                EffectRecoveryDecision::Reconcile { reason } if reason.contains("environment changed")
            ),
            "live permission change {change} reused an older outcome"
        );
    }
}

#[test]
fn set_environment_stamps_the_current_permission_fingerprint_before_recording() {
    let permissions = PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted());
    let environment = OperationEnvironmentSnapshot {
        permission_fingerprint: Some(permission_fingerprint(&permissions)),
        ..Default::default()
    };
    let context = EffectExecutionContext::new(
        RunId::from("set-environment-permission-run"),
        AgentId::from("agent"),
        Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default()),
        permissions.clone(),
        environment,
    );
    permissions.set_mode(PermissionMode::Auto);
    context.set_environment(context.environment());
    let request = context.effect_request(
        "stable-read",
        "Read",
        &json!({}),
        EffectDescriptor::read_only("read stable value"),
    );
    context.record_effect_intent(&request).unwrap();
    context
        .record_effect_outcome(&request, false, "recorded under Auto")
        .unwrap();

    permissions.set_mode(PermissionMode::Bypass);
    context.set_environment(context.environment());

    assert!(matches!(
        context.recover_effect(&request).unwrap(),
        EffectRecoveryDecision::Reconcile { reason } if reason.contains("environment changed")
    ));
}

#[test]
fn recovery_rechecks_configured_effect_authorization() {
    use solaris_types::permission::{ExecutionBoundary, PermissionRule};

    let workspace = tempfile::tempdir().unwrap();
    let external = tempfile::tempdir().unwrap();
    let protected_file = external.path().join("configured.txt");
    let run_id = RunId::from("configured-effect-recovery-run");
    let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    permissions.set_boundary(ExecutionBoundary::workspace(
        workspace.path().to_string_lossy().into_owned(),
    ));
    permissions.add_rule(PermissionRule {
        capability: Some("Read".into()),
        action: None,
        effect_class: None,
        resource_prefixes: vec![protected_file.to_string_lossy().into_owned()],
        decision: PermissionDecision::Allow,
    });
    let descriptor = EffectDescriptor {
        class: solaris_types::effect::EffectClass::ReadOnly,
        action: "read configured file".into(),
        resources: ResourceFootprint {
            file_reads: vec![protected_file.to_string_lossy().into_owned()],
            ..Default::default()
        },
        replay_policy: solaris_types::effect::EffectReplayPolicy::ReplaySafe,
    };
    permissions.allow_configured_effect_for("test:configured-read", "Read", &descriptor);
    let context = EffectExecutionContext::new(
        run_id,
        AgentId::from("agent"),
        Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default()),
        permissions.clone(),
        OperationEnvironmentSnapshot::default(),
    );
    let request = context.effect_request("configured-read", "Read", &json!({}), descriptor);
    assert_eq!(context.evaluate(&request).decision, PermissionDecision::Allow);
    context.record_effect_intent(&request).unwrap();
    context
        .record_effect_outcome(&request, false, "configured output")
        .unwrap();

    permissions.revoke_configured_effect_from("test:configured-read");
    assert_ne!(context.evaluate(&request).decision, PermissionDecision::Allow);

    assert!(matches!(
        context.recover_effect(&request).unwrap(),
        EffectRecoveryDecision::Reconcile { reason } if reason.contains("environment changed")
    ));
}

#[test]
fn recovery_rechecks_a_consumed_one_time_lease() {
    use solaris_types::permission::{ExecutionBoundary, PermissionRule};

    let workspace = tempfile::tempdir().unwrap();
    let external = tempfile::tempdir().unwrap();
    let protected_file = external.path().join("leased.txt");
    let run_id = RunId::from("consumed-lease-recovery-run");
    let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    permissions.set_boundary(ExecutionBoundary::workspace(
        workspace.path().to_string_lossy().into_owned(),
    ));
    permissions.add_rule(PermissionRule {
        capability: Some("Read".into()),
        action: None,
        effect_class: None,
        resource_prefixes: vec![protected_file.to_string_lossy().into_owned()],
        decision: PermissionDecision::Allow,
    });
    let descriptor = EffectDescriptor {
        class: solaris_types::effect::EffectClass::ReadOnly,
        action: "read leased file".into(),
        resources: ResourceFootprint {
            file_reads: vec![protected_file.to_string_lossy().into_owned()],
            ..Default::default()
        },
        replay_policy: solaris_types::effect::EffectReplayPolicy::ReplaySafe,
    };
    let context = EffectExecutionContext::new(
        run_id.clone(),
        AgentId::from("agent"),
        Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default()),
        permissions.clone(),
        OperationEnvironmentSnapshot::default(),
    );
    let request = context.effect_request("leased-read", "Read", &json!({}), descriptor.clone());
    permissions.issue_lease(CapabilityLease {
        lease_id: "one-time-lease".into(),
        capability: "Read".into(),
        action: Some(descriptor.action.clone()),
        scope: LeaseScope::Effect {
            effect_id: request.effect_id.clone(),
        },
        grants: AdditionalPermissions::default(),
        max_uses: Some(1),
        expires_at_unix_ms: None,
    });
    let initial = context.evaluate(&request);
    assert_eq!(initial.decision, PermissionDecision::Allow);
    assert!(initial.matched_lease);
    context.record_effect_intent(&request).unwrap();
    context.record_effect_outcome(&request, false, "leased output").unwrap();
    assert!(
        permissions
            .consume_lease_with(&run_id, &request, |_, _| Ok::<_, ()>(()))
            .unwrap()
    );
    assert_ne!(context.evaluate(&request).decision, PermissionDecision::Allow);

    assert!(matches!(
        context.recover_effect(&request).unwrap(),
        EffectRecoveryDecision::Reconcile { reason } if reason.contains("environment changed")
    ));

    permissions.issue_lease(CapabilityLease {
        lease_id: "replacement-one-time-lease".into(),
        capability: "Read".into(),
        action: Some(descriptor.action),
        scope: LeaseScope::Effect {
            effect_id: request.effect_id.clone(),
        },
        grants: AdditionalPermissions::default(),
        max_uses: Some(1),
        expires_at_unix_ms: None,
    });
    assert_eq!(
        context.recover_effect(&request).unwrap(),
        EffectRecoveryDecision::Reuse {
            is_error: false,
            output: "leased output".into(),
        }
    );
}

#[test]
fn recovery_rechecks_an_expired_lease() {
    use solaris_types::permission::{ExecutionBoundary, PermissionRule};

    let workspace = tempfile::tempdir().unwrap();
    let external = tempfile::tempdir().unwrap();
    let protected_file = external.path().join("expiring-lease.txt");
    let run_id = RunId::from("expired-lease-recovery-run");
    let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    permissions.set_boundary(ExecutionBoundary::workspace(
        workspace.path().to_string_lossy().into_owned(),
    ));
    permissions.add_rule(PermissionRule {
        capability: Some("Read".into()),
        action: None,
        effect_class: None,
        resource_prefixes: vec![protected_file.to_string_lossy().into_owned()],
        decision: PermissionDecision::Allow,
    });
    let descriptor = EffectDescriptor {
        class: solaris_types::effect::EffectClass::ReadOnly,
        action: "read with expiring lease".into(),
        resources: ResourceFootprint {
            file_reads: vec![protected_file.to_string_lossy().into_owned()],
            ..Default::default()
        },
        replay_policy: solaris_types::effect::EffectReplayPolicy::ReplaySafe,
    };
    let context = EffectExecutionContext::new(
        run_id,
        AgentId::from("agent"),
        Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default()),
        permissions.clone(),
        OperationEnvironmentSnapshot::default(),
    );
    let request = context.effect_request("expiring-read", "Read", &json!({}), descriptor.clone());
    let lease = CapabilityLease {
        lease_id: "expiring-lease".into(),
        capability: "Read".into(),
        action: Some(descriptor.action.clone()),
        scope: LeaseScope::Effect {
            effect_id: request.effect_id.clone(),
        },
        grants: AdditionalPermissions::default(),
        max_uses: None,
        expires_at_unix_ms: Some(chrono::Utc::now().timestamp_millis() + 60_000),
    };
    permissions.issue_lease(lease.clone());
    let initial = context.evaluate(&request);
    assert_eq!(initial.decision, PermissionDecision::Allow);
    assert!(initial.matched_lease);
    context.record_effect_intent(&request).unwrap();
    context.record_effect_outcome(&request, false, "leased output").unwrap();
    permissions.restore_lease(
        CapabilityLease {
            expires_at_unix_ms: Some(chrono::Utc::now().timestamp_millis() - 1),
            ..lease
        },
        0,
    );
    assert_ne!(context.evaluate(&request).decision, PermissionDecision::Allow);

    assert!(matches!(
        context.recover_effect(&request).unwrap(),
        EffectRecoveryDecision::Reconcile { reason } if reason.contains("environment changed")
    ));
}

#[test]
fn only_pending_read_only_replay_safe_effects_execute_again() {
    use solaris_types::effect::{EffectClass, EffectReplayPolicy};

    let classes = [
        EffectClass::ReadOnly,
        EffectClass::AgentLifecycle,
        EffectClass::MeshStateMutation,
        EffectClass::WorkspaceMutation,
        EffectClass::Process,
        EffectClass::Network,
        EffectClass::ExternalSideEffect,
    ];
    let policies = [
        EffectReplayPolicy::Never,
        EffectReplayPolicy::Idempotent,
        EffectReplayPolicy::ReplaySafe,
        EffectReplayPolicy::ReconcileRequired,
        EffectReplayPolicy::Compensatable,
    ];
    for (index, (class, policy)) in classes
        .into_iter()
        .flat_map(|class| policies.into_iter().map(move |policy| (class, policy)))
        .enumerate()
    {
        let ledger = Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default());
        let context = EffectExecutionContext::new(
            RunId::new(format!("policy-run-{index}")),
            AgentId::from("agent"),
            ledger,
            PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
            OperationEnvironmentSnapshot::default(),
        );
        let request = context.effect_request(
            "stable-call",
            "Effect",
            &json!({"index": index}),
            EffectDescriptor {
                class,
                action: "test policy".into(),
                resources: ResourceFootprint::default(),
                replay_policy: policy,
            },
        );
        context.record_effect_intent(&request).unwrap();
        let pending = context.recover_effect(&request).unwrap();
        let may_retry_pending = class == EffectClass::ReadOnly && policy == EffectReplayPolicy::ReplaySafe;
        if may_retry_pending {
            assert_eq!(
                pending,
                EffectRecoveryDecision::Execute,
                "class={class:?}, policy={policy:?}"
            );
        } else {
            assert!(
                matches!(pending, EffectRecoveryDecision::Reconcile { reason } if reason.contains("outcome is unknown")),
                "class={class:?}, policy={policy:?}"
            );
        }

        context.record_effect_outcome(&request, false, "stored result").unwrap();
        assert_eq!(
            context.recover_effect(&request).unwrap(),
            EffectRecoveryDecision::Reuse {
                is_error: false,
                output: "stored result".into(),
            }
        );
    }
}

#[test]
fn effect_output_is_reusable_without_appearing_in_the_ledger() {
    let ledger = Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default());
    let run_id = RunId::from("protected-output-run");
    let context = EffectExecutionContext::new(
        run_id.clone(),
        AgentId::from("agent"),
        ledger.clone(),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let request = context.effect_request(
        "secret-read",
        "Read",
        &json!({}),
        EffectDescriptor {
            class: solaris_types::effect::EffectClass::ReadOnly,
            action: "read secret".into(),
            resources: ResourceFootprint::default(),
            replay_policy: solaris_types::effect::EffectReplayPolicy::ReplaySafe,
        },
    );
    let secret = "mesh-secret-output-7f30";
    context.record_effect_intent(&request).unwrap();
    context.record_effect_outcome(&request, false, secret).unwrap();

    let journal = serde_json::to_string(&ledger.records_for_run(&run_id).unwrap()).unwrap();
    assert!(!journal.contains(secret));
    assert!(journal.contains("output_ref"));
    assert_eq!(
        context.recover_effect(&request).unwrap(),
        EffectRecoveryDecision::Reuse {
            is_error: false,
            output: secret.into(),
        }
    );
}

#[test]
fn sqlite_effect_output_is_written_once_beside_the_workspace_ledger() {
    let workspace = tempfile::tempdir().unwrap();
    let runtime_root = workspace.path().join(".solaris").join("runtime");
    let ledger =
        Arc::new(crate::runtime_ledger::SqliteRuntimeLedger::open(runtime_root.join("ledger.sqlite3")).unwrap());
    let run_id = RunId::new(format!("workspace-output-{}", uuid::Uuid::now_v7()));
    let context = EffectExecutionContext::new(
        run_id.clone(),
        AgentId::from("agent"),
        ledger.clone(),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let request = context.effect_request("call", "Read", &json!({}), EffectDescriptor::read_only("read"));
    context.record_effect_intent(&request).unwrap();
    context
        .record_effect_outcome(&request, false, "one protected body")
        .unwrap();
    context
        .record_effect_outcome(&request, false, "one protected body")
        .unwrap();
    let output_ref = ledger
        .records_for_run(&run_id)
        .unwrap()
        .into_iter()
        .find(|record| record.record_type == "effect_outcome")
        .unwrap()
        .payload["output_ref"]
        .as_str()
        .unwrap()
        .to_owned();
    let run_directory = runtime_root
        .join("effect-outcomes")
        .join(stable_digest_bytes(run_id.as_str().as_bytes()));

    assert_eq!(
        std::fs::read_to_string(run_directory.join(output_ref)).unwrap(),
        "one protected body"
    );
    assert_eq!(std::fs::read_dir(run_directory).unwrap().count(), 1);
}

#[test]
fn ledger_local_output_store_reads_legacy_global_blob_without_writing_a_new_copy() {
    let workspace = tempfile::tempdir().unwrap();
    let runtime_root = workspace.path().join(".solaris").join("runtime");
    let ledger = crate::runtime_ledger::SqliteRuntimeLedger::open(runtime_root.join("ledger.sqlite3")).unwrap();
    let run_id = RunId::new(format!("legacy-output-{}", uuid::Uuid::now_v7()));
    let legacy_store = EffectOutputStore::for_legacy_run(&run_id);
    let output_ref = legacy_store.write_legacy_fixture("legacy protected body").unwrap();
    let local_store = EffectOutputStore::for_run_with_ledger(&run_id, &ledger);

    assert_eq!(local_store.read(&output_ref).unwrap(), "legacy protected body");
    assert!(!runtime_root.join("effect-outcomes").exists());

    std::fs::remove_dir_all(effect_output_state_root().join(stable_digest_bytes(run_id.as_str().as_bytes()))).unwrap();
}

#[test]
fn effect_output_reference_requires_one_normal_path_component() {
    let store = EffectOutputStore::for_legacy_run(&RunId::from("output-reference-run"));
    for invalid in [".", "..", "../secret", "/absolute", "nested/path"] {
        assert_eq!(store.read(invalid).unwrap_err().kind(), std::io::ErrorKind::Other);
    }
}

#[test]
fn recovery_uses_versioned_descriptor_identity_not_projection_display_fields() {
    let ledger = Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default());
    let run_id = RunId::from("projection-display-run");
    let context = EffectExecutionContext::new(
        run_id.clone(),
        AgentId::from("agent"),
        ledger.clone(),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let request = context.effect_request(
        "projection-display",
        "Read",
        &json!({"path": "value"}),
        EffectDescriptor::read_only("read value"),
    );
    let mut projection = EffectAuditProjection::from_descriptor(&request.descriptor);
    projection.action.summary = "new presentation text".into();
    context.record_effect_intent(&request).unwrap();
    let mut intent_payload = ledger
        .records_for_run(&run_id)
        .unwrap()
        .into_iter()
        .find(|record| record.record_type == "effect_intent")
        .unwrap()
        .payload;
    intent_payload["effect"] = json!(projection);
    ledger
        .append(&run_id, DurabilityClass::SyncCritical, "effect_intent", intent_payload)
        .unwrap();
    context.record_effect_outcome(&request, false, "stored").unwrap();

    assert_eq!(
        context.recover_effect(&request).unwrap(),
        EffectRecoveryDecision::Reuse {
            is_error: false,
            output: "stored".into(),
        }
    );
}

#[test]
fn recovery_accepts_legacy_descriptors_when_all_identity_bindings_match() {
    let ledger = Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default());
    let run_id = RunId::from("legacy-descriptor-recovery-run");
    let context = EffectExecutionContext::new(
        run_id.clone(),
        AgentId::from("agent"),
        ledger.clone(),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let request = context.effect_request(
        "legacy-descriptor",
        "Read",
        &json!({"path": "value"}),
        EffectDescriptor::read_only("read value"),
    );
    context.record_effect_intent(&request).unwrap();
    context.record_effect_outcome(&request, false, "stored").unwrap();
    let records = ledger.records_for_run(&run_id).unwrap();
    let current_intent = records
        .iter()
        .find(|record| record.record_type == "effect_intent")
        .unwrap();
    let current_outcome = records
        .iter()
        .find(|record| record.record_type == "effect_outcome")
        .unwrap();
    let mut legacy_intent = current_intent.payload.clone();
    legacy_intent.as_object_mut().unwrap().remove("effect");
    legacy_intent["descriptor"] = json!(request.descriptor);
    ledger
        .append(&run_id, DurabilityClass::SyncCritical, "effect_intent", legacy_intent)
        .unwrap();
    let mut legacy_outcome = current_outcome.payload.clone();
    legacy_outcome.as_object_mut().unwrap().remove("effect");
    legacy_outcome["descriptor"] = json!(request.descriptor);
    ledger
        .append(&run_id, DurabilityClass::SyncCritical, "effect_outcome", legacy_outcome)
        .unwrap();

    assert_eq!(
        context.recover_effect(&request).unwrap(),
        EffectRecoveryDecision::Reuse {
            is_error: false,
            output: "stored".into(),
        }
    );
}

#[test]
fn recovery_rejects_outcome_identity_binding_mismatches() {
    for (index, field) in ["effect_id", "input_digest", "operation_id", "effect"]
        .into_iter()
        .enumerate()
    {
        let ledger = Arc::new(crate::runtime_ledger::InMemoryRuntimeLedger::default());
        let run_id = RunId::new(format!("outcome-binding-run-{index}"));
        let context = EffectExecutionContext::new(
            run_id.clone(),
            AgentId::from("agent"),
            ledger.clone(),
            PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
            OperationEnvironmentSnapshot::default(),
        );
        let request = context.effect_request(
            "binding-call",
            "Read",
            &json!({"index": index}),
            EffectDescriptor::read_only("read value"),
        );
        context.record_effect_intent(&request).unwrap();
        context.record_effect_outcome(&request, false, "stored").unwrap();
        let mut payload = ledger
            .records_for_run(&run_id)
            .unwrap()
            .into_iter()
            .find(|record| record.record_type == "effect_outcome")
            .unwrap()
            .payload;
        match field {
            "effect_id" => payload["effect_id"] = json!("effect:mismatch"),
            "input_digest" => payload["input_digest"] = json!("sha256:mismatch"),
            "operation_id" => payload["operation_id"] = json!("operation:mismatch"),
            "effect" => payload["effect"]["descriptor_digest"] = json!("sha256:mismatch"),
            _ => unreachable!(),
        }
        ledger
            .append(&run_id, DurabilityClass::SyncCritical, "effect_outcome", payload)
            .unwrap();

        assert!(matches!(
            context.recover_effect(&request).unwrap(),
            EffectRecoveryDecision::Reconcile { reason } if reason.contains("outcome identity")
        ));
    }
}
