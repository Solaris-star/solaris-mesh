use serde_json::{Value, json};

use solaris_process::ProcessRecoveryRecord;

use solaris_types::effect::{DurabilityClass, EffectAuditProjection, EffectClass, EffectReplayPolicy, EffectRequest};
use solaris_types::plugin::ImplementationIdentity;
use solaris_types::runtime::{CompatibilityDecision, OperationEnvironmentSnapshot, ToolImplementationSnapshot};

use super::environment::{compatibility_decision, permission_fingerprint, validate_environment_plugin_identities};
use super::redaction::{payload_effect_descriptor_digest, secret_safe_descriptor};
use super::{
    EffectExecutionContext, EffectOutcomeCompletion, EffectRecoveryDecision, PREPARED_IMPLEMENTATION_SNAPSHOT_PREFIX,
    stable_digest_bytes,
};

const PROCESS_RECOVERY_SCHEMA_V1: &str = "solaris/process-recovery/v1";

impl EffectExecutionContext {
    pub(crate) fn has_unresolved_effect_intent(&self) -> std::io::Result<bool> {
        let records = self.ledger.records_for_run(&self.run_id)?;
        let completed = records
            .iter()
            .filter(|record| record.record_type == "effect_outcome")
            .filter_map(|record| record.payload.get("effect_id").and_then(Value::as_str))
            .collect::<std::collections::HashSet<_>>();
        Ok(records.iter().any(|record| {
            record.record_type == "effect_intent"
                && record
                    .payload
                    .get("effect_id")
                    .and_then(Value::as_str)
                    .is_some_and(|effect_id| !completed.contains(effect_id))
        }))
    }

    pub(crate) fn unresolved_other_effect_for_capability(
        &self,
        capability: &str,
        current_effect_id: &str,
    ) -> Result<Option<String>, String> {
        let records = self
            .ledger
            .records_for_run(&self.run_id)
            .map_err(|error| format!("failed to inspect pending {capability} history: {error}"))?;
        for intent in records.iter().rev().filter(|record| {
            record.record_type == "effect_intent"
                && record.payload.get("capability").and_then(Value::as_str) == Some(capability)
                && record.payload.get("agent_id").and_then(Value::as_str) == Some(self.agent_id.as_str())
        }) {
            let Some(effect_id) = intent.payload.get("effect_id").and_then(Value::as_str) else {
                return Err(format!("pending {capability} intent is missing its effect identity"));
            };
            if effect_id == current_effect_id {
                continue;
            }
            let outcome = records.iter().rev().find(|record| {
                record.record_type == "effect_outcome"
                    && record.seq > intent.seq
                    && record.payload.get("effect_id").and_then(Value::as_str) == Some(effect_id)
            });
            if outcome.is_none()
                || outcome.is_some_and(|record| {
                    record.payload.get("status").and_then(Value::as_str) == Some("outcome_unknown")
                })
            {
                return Ok(Some(effect_id.to_owned()));
            }
        }
        Ok(None)
    }

    pub(crate) fn exact_effect_intent_exists(&self, effect_id: &str) -> Result<bool, String> {
        let records = self
            .ledger
            .records_for_run(&self.run_id)
            .map_err(|error| format!("failed to inspect exact effect history: {error}"))?;
        Ok(records.iter().any(|record| {
            record.record_type == "effect_intent"
                && record.payload.get("effect_id").and_then(Value::as_str) == Some(effect_id)
                && record.payload.get("agent_id").and_then(Value::as_str) == Some(self.agent_id.as_str())
        }))
    }

    pub fn record_effect_intent(&self, request: &EffectRequest) -> std::io::Result<()> {
        self.record_effect_intent_with_environment(request, self.environment_with_current_permissions(request))
    }

    pub(crate) fn record_effect_intent_with_tool_implementation(
        &self,
        request: &EffectRequest,
        implementation: &ImplementationIdentity,
    ) -> std::io::Result<()> {
        let environment = self.environment_with_tool_implementation(request, implementation);
        self.record_effect_intent_with_environment(request, environment)
    }

    fn environment_with_tool_implementation(
        &self,
        request: &EffectRequest,
        implementation: &ImplementationIdentity,
    ) -> OperationEnvironmentSnapshot {
        let mut environment = self.environment_with_current_permissions(request);
        let prepared_name = format!("{PREPARED_IMPLEMENTATION_SNAPSHOT_PREFIX}{}", request.capability);
        if let Some(tool) = environment.tools.iter_mut().find(|tool| tool.name == prepared_name) {
            tool.implementation = implementation.clone();
        } else {
            environment.tools.push(ToolImplementationSnapshot {
                name: prepared_name,
                implementation: implementation.clone(),
                schema_digest: None,
                replay_policy: request.descriptor.replay_policy,
            });
            environment.tools.sort_by(|left, right| left.name.cmp(&right.name));
        }
        environment
    }

    fn environment_with_current_permissions(&self, request: &EffectRequest) -> OperationEnvironmentSnapshot {
        let mut environment = self.environment();
        let current = permission_fingerprint(&self.permissions);
        let baseline = self
            .environment_permission_fingerprint
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let evaluation = self.evaluate(request);
        environment.permission_fingerprint = Some(super::stable_digest_value(&json!({
            "schema": "solaris/permission-authorization/v1",
            "environment": environment.permission_fingerprint,
            "baseline": baseline,
            "current": current,
            "decision": evaluation.decision,
            "matched_lease": evaluation.matched_lease,
        })));
        environment
    }

    fn record_effect_intent_with_environment(
        &self,
        request: &EffectRequest,
        environment: OperationEnvironmentSnapshot,
    ) -> std::io::Result<()> {
        self.ensure_session_fence()?;
        let effect = EffectAuditProjection::from_descriptor(&request.descriptor);
        self.append_record(
            &self.run_id,
            DurabilityClass::SyncCritical,
            "effect_intent",
            json!({
                "agent_id": self.agent_id,
                "effect_id": request.effect_id,
                "operation_id": request.operation_id,
                "capability": request.capability,
                "effect": effect,
                "input_digest": request.input_digest,
                "environment": environment,
            }),
        )?;
        Ok(())
    }

    /// Resolve a stable effect identity against durable history before any
    /// external work is repeated.
    pub fn recover_effect(&self, request: &EffectRequest) -> Result<EffectRecoveryDecision, String> {
        self.recover_effect_with_tool_implementation(request, None)
    }

    pub(crate) fn recover_effect_with_tool_implementation(
        &self,
        request: &EffectRequest,
        implementation: Option<&ImplementationIdentity>,
    ) -> Result<EffectRecoveryDecision, String> {
        let records = self
            .ledger
            .records_for_run(&self.run_id)
            .map_err(|error| format!("failed to inspect effect history: {error}"))?;
        let Some(intent) = records.iter().rev().find(|record| {
            record.record_type == "effect_intent"
                && record.payload.get("effect_id").and_then(Value::as_str) == Some(request.effect_id.as_str())
        }) else {
            return Ok(EffectRecoveryDecision::Execute);
        };
        let audit_projection = EffectAuditProjection::from_descriptor(&request.descriptor);
        let recorded_effect_digest = payload_effect_descriptor_digest(&intent.payload);
        let recorded_descriptor = intent.payload.get("descriptor");
        let legacy_descriptor = secret_safe_descriptor(&request.descriptor);
        let effect_matches = recorded_effect_digest.as_deref() == Some(audit_projection.descriptor_digest.as_str())
            || recorded_descriptor == Some(&json!(legacy_descriptor))
            || recorded_descriptor == Some(&json!(request.descriptor));
        if intent.payload.get("operation_id") != Some(&json!(request.operation_id))
            || intent.payload.get("input_digest") != Some(&json!(request.input_digest))
            || !effect_matches
        {
            return Ok(EffectRecoveryDecision::Reconcile {
                reason: format!(
                    "effect {} durable identity conflicts with current input or descriptor",
                    request.effect_id
                ),
            });
        }
        let recorded_environment = intent
            .payload
            .get("environment")
            .cloned()
            .ok_or_else(|| "effect intent is missing its operation environment".to_owned())
            .and_then(|value| {
                serde_json::from_value::<OperationEnvironmentSnapshot>(value)
                    .map_err(|error| format!("effect intent has invalid operation environment: {error}"))
            })?;
        let current_environment = implementation.map_or_else(
            || self.environment_with_current_permissions(request),
            |implementation| self.environment_with_tool_implementation(request, implementation),
        );
        if validate_environment_plugin_identities(&recorded_environment).is_err()
            || validate_environment_plugin_identities(&current_environment).is_err()
        {
            return Ok(EffectRecoveryDecision::Reconcile {
                reason: format!(
                    "effect {} operation environment contains conflicting plugin identities",
                    request.effect_id
                ),
            });
        }
        match compatibility_decision(&recorded_environment, &current_environment) {
            CompatibilityDecision::Compatible => {}
            CompatibilityDecision::ReconcileRequired => {
                return Ok(EffectRecoveryDecision::Reconcile {
                    reason: format!(
                        "effect {} operation environment changed and requires reconciliation",
                        request.effect_id
                    ),
                });
            }
            CompatibilityDecision::Incompatible => {
                return Ok(EffectRecoveryDecision::Reconcile {
                    reason: format!("effect {} operation environment is incompatible", request.effect_id),
                });
            }
        }
        let outcomes = records
            .iter()
            .filter(|record| {
                record.record_type == "effect_outcome"
                    && record.seq > intent.seq
                    && (record.payload.get("effect_id").and_then(Value::as_str) == Some(request.effect_id.as_str())
                        || record.payload.get("operation_id").and_then(Value::as_str)
                            == Some(request.operation_id.as_str()))
            })
            .collect::<Vec<_>>();
        if let Some(outcome) = outcomes.first() {
            let mut first_output = None;
            for (index, candidate) in outcomes.iter().enumerate() {
                let outcome_descriptor_digest = payload_effect_descriptor_digest(&candidate.payload);
                if candidate.payload.get("effect_id") != Some(&json!(request.effect_id))
                    || candidate.payload.get("operation_id") != Some(&json!(request.operation_id))
                    || candidate.payload.get("input_digest") != Some(&json!(request.input_digest))
                    || outcome_descriptor_digest.as_deref() != Some(audit_projection.descriptor_digest.as_str())
                    || recorded_effect_digest.is_some() && outcome_descriptor_digest != recorded_effect_digest
                {
                    return Ok(EffectRecoveryDecision::Reconcile {
                        reason: format!(
                            "effect {} durable outcome identity failed integrity validation",
                            request.effect_id
                        ),
                    });
                }
                if candidate.payload.get("status").and_then(Value::as_str) == Some("outcome_unknown") {
                    continue;
                }
                let Some(output_ref) = candidate.payload.get("output_ref").and_then(Value::as_str) else {
                    return Ok(EffectRecoveryDecision::Reconcile {
                        reason: format!("effect {} completed without a reusable output", request.effect_id),
                    });
                };
                let output = match self.output_store.read(output_ref) {
                    Ok(output) => output,
                    Err(error) => {
                        return Ok(EffectRecoveryDecision::Reconcile {
                            reason: format!(
                                "effect {} protected output failed integrity validation: {error}",
                                request.effect_id
                            ),
                        });
                    }
                };
                let expected_bytes = output.len() as u64;
                let recorded_bytes = candidate.payload.get("output_bytes").and_then(Value::as_u64);
                let expected_digest = stable_digest_bytes(output.as_bytes());
                let recorded_digest = candidate.payload.get("output_digest").and_then(Value::as_str);
                if recorded_bytes != Some(expected_bytes) || recorded_digest != Some(expected_digest.as_str()) {
                    return Ok(EffectRecoveryDecision::Reconcile {
                        reason: format!(
                            "effect {} durable output failed integrity validation",
                            request.effect_id
                        ),
                    });
                }
                if index == 0 {
                    first_output = Some(output);
                }
            }
            if outcomes
                .iter()
                .skip(1)
                .any(|candidate| candidate.payload != outcome.payload)
            {
                return Ok(EffectRecoveryDecision::Reconcile {
                    reason: format!(
                        "effect {} has conflicting durable outcome identity records",
                        request.effect_id
                    ),
                });
            }
            if outcome.payload.get("status").and_then(Value::as_str) == Some("outcome_unknown") {
                return Ok(EffectRecoveryDecision::Reconcile {
                    reason: format!("effect {} started but its outcome is unknown", request.effect_id),
                });
            }
            let Some(output) = first_output else {
                return Ok(EffectRecoveryDecision::Reconcile {
                    reason: format!("effect {} completed without a reusable output", request.effect_id),
                });
            };
            return Ok(EffectRecoveryDecision::Reuse {
                is_error: outcome.payload.get("is_error").and_then(Value::as_bool).unwrap_or(true),
                output,
            });
        }
        if request.descriptor.class == EffectClass::ReadOnly
            && request.descriptor.replay_policy == EffectReplayPolicy::ReplaySafe
        {
            Ok(EffectRecoveryDecision::Execute)
        } else {
            Ok(EffectRecoveryDecision::Reconcile {
                reason: format!(
                    "effect {} has a durable intent without an outcome; its outcome is unknown and class {:?} with replay policy {:?} requires reconciliation",
                    request.effect_id, request.descriptor.class, request.descriptor.replay_policy
                ),
            })
        }
    }

    pub fn stable_effect_attempt_id(&self, base: &str) -> Result<String, String> {
        let records = self
            .ledger
            .records_for_run(&self.run_id)
            .map_err(|error| format!("failed to inspect effect attempts: {error}"))?;
        for attempt in 1..=records.len().saturating_add(1) {
            let call_id = format!("{base}:attempt:{attempt}");
            let effect_id = self.effect_id_for_call(&call_id);
            let Some(intent) = records.iter().rev().find(|record| {
                record.record_type == "effect_intent"
                    && record.payload.get("effect_id").and_then(Value::as_str) == Some(effect_id.as_str())
            }) else {
                return Ok(call_id);
            };
            let completed = records.iter().any(|record| {
                record.record_type == "effect_outcome"
                    && record.seq > intent.seq
                    && record.payload.get("effect_id").and_then(Value::as_str) == Some(effect_id.as_str())
            });
            if !completed {
                return Ok(call_id);
            }
        }
        Err("effect attempt history exceeded its durable record bound".into())
    }

    pub fn record_effect_outcome(&self, request: &EffectRequest, is_error: bool, output: &str) -> std::io::Result<()> {
        match self.record_effect_outcome_canonical(request, is_error, output)? {
            EffectOutcomeCompletion::Committed => Ok(()),
            EffectOutcomeCompletion::OutcomeUnknown => Err(std::io::Error::other(
                "the canonical effect outcome requires reconciliation",
            )),
        }
    }

    pub(crate) fn record_effect_outcome_canonical(
        &self,
        request: &EffectRequest,
        is_error: bool,
        output: &str,
    ) -> std::io::Result<EffectOutcomeCompletion> {
        let output_ref = self.output_store.write(&request.effect_id, output)?;
        let effect = EffectAuditProjection::from_descriptor(&request.descriptor);
        let process_recovery = self.process_recovery_for_effect(&request.effect_id);
        let status = if process_recovery.is_some() {
            "outcome_unknown"
        } else if is_error {
            "failed"
        } else {
            "executed"
        };
        let mut payload = json!({
            "agent_id": self.agent_id,
            "effect_id": request.effect_id,
            "operation_id": request.operation_id,
            "effect": effect,
            "input_digest": request.input_digest,
            "status": status,
            "is_error": is_error || process_recovery.is_some(),
            "output_digest": stable_digest_bytes(output.as_bytes()),
            "output_bytes": output.len(),
            "output_ref": output_ref,
            "output_redacted": true,
        });
        if let Some(recovery) = process_recovery {
            payload["process_recovery"] = process_recovery_metadata(recovery);
        }
        self.persist_canonical_effect_outcome(request, payload)
    }

    pub(crate) fn record_effect_outcome_unknown(&self, request: &EffectRequest, reason: &str) -> std::io::Result<()> {
        let process_recovery = self
            .process_recovery_for_effect(&request.effect_id)
            .map(process_recovery_metadata);
        self.record_effect_outcome_unknown_with_metadata(request, reason, process_recovery)
    }

    pub(crate) fn record_process_recovery_outcome_unknown(
        &self,
        request: &EffectRequest,
        reason: &str,
        recovery: ProcessRecoveryRecord,
    ) -> std::io::Result<()> {
        self.record_effect_outcome_unknown_with_metadata(request, reason, Some(process_recovery_metadata(recovery)))
    }

    fn record_effect_outcome_unknown_with_metadata(
        &self,
        request: &EffectRequest,
        reason: &str,
        process_recovery: Option<Value>,
    ) -> std::io::Result<()> {
        let output_ref = self.output_store.write(&request.effect_id, reason)?;
        let effect = EffectAuditProjection::from_descriptor(&request.descriptor);
        let mut payload = json!({
            "agent_id": self.agent_id,
            "effect_id": request.effect_id,
            "operation_id": request.operation_id,
            "effect": effect,
            "input_digest": request.input_digest,
            "status": "outcome_unknown",
            "is_error": true,
            "output_digest": stable_digest_bytes(reason.as_bytes()),
            "output_bytes": reason.len(),
            "output_ref": output_ref,
            "output_redacted": true,
        });
        if let Some(process_recovery) = process_recovery {
            payload["process_recovery"] = process_recovery;
        }
        match self.persist_canonical_effect_outcome(request, payload)? {
            EffectOutcomeCompletion::Committed | EffectOutcomeCompletion::OutcomeUnknown => Ok(()),
        }
    }

    fn persist_canonical_effect_outcome(
        &self,
        request: &EffectRequest,
        payload: Value,
    ) -> std::io::Result<EffectOutcomeCompletion> {
        let requested_completion = if payload.get("status").and_then(Value::as_str) == Some("outcome_unknown") {
            EffectOutcomeCompletion::OutcomeUnknown
        } else {
            EffectOutcomeCompletion::Committed
        };
        let append = self.mutation.compare_and_append_serialized(
            self.ledger.as_ref(),
            &self.run_id,
            DurabilityClass::SyncCritical,
            "effect_outcome",
            &["effect_id"],
            payload.clone(),
        );
        match append {
            Ok(_) => Ok(requested_completion),
            Err(append_error) => {
                let records = self.ledger.records_for_run(&self.run_id)?;
                let outcomes = records
                    .iter()
                    .filter(|record| {
                        record.record_type == "effect_outcome"
                            && record.payload.get("effect_id").and_then(Value::as_str)
                                == Some(request.effect_id.as_str())
                    })
                    .collect::<Vec<_>>();
                let Some(canonical) = outcomes.first() else {
                    return Err(append_error);
                };
                if outcomes
                    .iter()
                    .skip(1)
                    .any(|candidate| candidate.payload != canonical.payload)
                {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "effect has conflicting durable outcome identity records",
                    ));
                }
                if canonical.payload == payload {
                    return Ok(requested_completion);
                }
                if canonical.payload.get("status").and_then(Value::as_str) == Some("outcome_unknown") {
                    return Ok(EffectOutcomeCompletion::OutcomeUnknown);
                }
                Err(append_error)
            }
        }
    }
}

fn process_recovery_metadata(recovery: ProcessRecoveryRecord) -> Value {
    json!({
        "schema": PROCESS_RECOVERY_SCHEMA_V1,
        "id": recovery.id().get(),
        "kind": recovery.kind().as_str(),
        "ref": recovery.reference(),
    })
}
