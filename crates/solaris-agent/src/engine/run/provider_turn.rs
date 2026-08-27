use solaris_types::permission::PermissionDecision;
use tracing::warn;

use super::super::AgentEngine;
use super::super::task_phase::{provider_call_id, require_resume_call_id, turn_kind_key};
use crate::compact::estimate::estimate_tokens_from_messages;
use crate::error::AgentError;
use crate::execution_context::{DurableTaskPhase, EffectOutcomeGuard, EffectRecoveryDecision, stable_digest_value};
use crate::session::StoredDurableTask;
use crate::stream::StreamOutcome;
use crate::turn::TurnKind;

impl AgentEngine {
    pub(super) async fn run_turn_with_text_emission(
        &mut self,
        kind: TurnKind,
        emit_assistant_text: bool,
    ) -> Result<StreamOutcome, AgentError> {
        self.run_turn_with_durable_resume(kind, emit_assistant_text, None).await
    }

    pub(super) async fn run_turn_with_durable_resume(
        &mut self,
        kind: TurnKind,
        emit_assistant_text: bool,
        durable_resume: Option<&StoredDurableTask>,
    ) -> Result<StreamOutcome, AgentError> {
        self.ensure_session_lease()?;
        let execution_context = self.execution_context.clone().ok_or_else(|| {
            AgentError::ApiError("effect execution context is required before provider execution".to_owned())
        })?;
        // A durable Provider phase is created only after compaction and its
        // resulting session state were saved. Re-running compaction here could
        // invoke the Provider or change the request before its identity check.
        if durable_resume.is_none() {
            self.run_compaction().await?;
            self.save_session()?;
        }
        let request = self.build_request(kind);
        let input = serde_json::to_value(&request)
            .map_err(|error| AgentError::ApiError(format!("provider request serialization failed: {error}")))?;
        let call_id = provider_call_id(turn_kind_key(kind), &stable_digest_value(&input));
        if let Some(task) = durable_resume {
            require_resume_call_id(task, &call_id)?;
        } else {
            self.record_durable_task_phase(DurableTaskPhase::AwaitingProvider, Some(&call_id))?;
        }
        let effect_request =
            execution_context.effect_request(&call_id, "ProviderRequest", &input, self.provider_effect.clone());
        let effect_id = effect_request.effect_id.clone();
        match execution_context
            .recover_effect(&effect_request)
            .map_err(AgentError::ApiError)?
        {
            EffectRecoveryDecision::Execute => {}
            EffectRecoveryDecision::Reuse { is_error: true, output } => {
                self.record_durable_task_phase(DurableTaskPhase::ProviderCompleted, Some(&call_id))?;
                return Err(AgentError::ApiError(output));
            }
            EffectRecoveryDecision::Reuse {
                is_error: false,
                output,
            } => {
                self.record_durable_task_phase(DurableTaskPhase::ProviderCompleted, Some(&call_id))?;
                let outcome: StreamOutcome = serde_json::from_str(&output)
                    .map_err(|error| AgentError::ApiError(format!("recovered provider outcome is invalid: {error}")))?;
                self.emit_recovered_stream(&outcome, emit_assistant_text);
                self.record_turn_usage(&outcome.usage, &effect_id)?;
                return Ok(outcome);
            }
            EffectRecoveryDecision::Reconcile { reason } => {
                if let Some(task) = durable_resume {
                    warn!(
                        target: "solaris_agent",
                        task_key = %task.task_key,
                        reason,
                        "durable provider call requires reconciliation"
                    );
                    return Err(AgentError::ReconciliationRequired {
                        task_key: task.task_key.clone(),
                        call_id: task.call_id.clone(),
                    });
                }
                return Err(AgentError::ReconciliationRequired {
                    task_key: durable_resume
                        .map(|task| task.task_key.clone())
                        .unwrap_or_else(|| call_id.clone()),
                    call_id: Some(call_id.clone()),
                });
            }
        }
        execution_context
            .ensure_runtime_budget_available()
            .map_err(AgentError::ResourceBudgetExceeded)?;
        let evaluation = execution_context.evaluate(&effect_request);
        let approved_environment = execution_context.environment();
        execution_context
            .record_permission_decision(&effect_request, &evaluation, "provider_request")
            .map_err(|error| AgentError::ApiError(format!("provider permission persistence failed: {error}")))?;
        if evaluation.decision != PermissionDecision::Allow {
            return Err(AgentError::PermissionDenied(format!(
                "provider request: {}",
                evaluation.reason
            )));
        }
        let estimated_provider_tokens =
            estimate_tokens_from_messages(&request.messages).saturating_add(u64::from(request.max_tokens.unwrap_or(0)));
        let mut provider_rate_permit = execution_context
            .acquire_provider_request(estimated_provider_tokens)
            .map_err(AgentError::ResourceBudgetExceeded)?;
        let _effect_permit = execution_context
            .acquire_effect_permit()
            .await
            .map_err(AgentError::ResourceBudgetExceeded)?;
        execution_context
            .revalidate_environment(&effect_request, &approved_environment)
            .map_err(AgentError::ApiError)?;
        execution_context
            .record_effect_intent(&effect_request)
            .map_err(|error| AgentError::ApiError(format!("provider effect intent persistence failed: {error}")))?;
        let mut task_phase = self.begin_durable_task_phase(DurableTaskPhase::ProviderInFlight, &call_id)?;
        let provider_signals = execution_context
            .resource_manager()
            .map(|resources| resources.provider_signals());
        let mut outcome_guard = EffectOutcomeGuard::new(
            execution_context,
            effect_request,
            "provider request cancelled before a terminal result",
        );
        let single_attempt = provider_signals
            .as_ref()
            .is_some_and(|signals| signals.requests_per_minute.is_some() || signals.tokens_per_minute.is_some());
        if let Some(permit) = provider_rate_permit.as_mut() {
            permit.mark_started().map_err(AgentError::ResourceBudgetExceeded)?;
        }
        let provider_stream = if single_attempt {
            self.provider.stream_once(&request).await
        } else {
            self.provider.stream(&request).await
        };
        let mut rx = match provider_stream {
            Ok(rx) => rx,
            Err(error) => {
                if let Err(persistence_error) = outcome_guard.complete(true, &error.to_string()) {
                    return Err(AgentError::ApiError(format!(
                        "provider request failed: {error}; provider outcome persistence failed: {persistence_error}"
                    )));
                }
                task_phase.complete(DurableTaskPhase::ProviderCompleted)?;
                return Err(AgentError::from_dispatched_provider(error));
            }
        };
        let outcome = match self.consume_stream(&mut rx, emit_assistant_text).await {
            Ok(outcome) => outcome,
            Err(error) => {
                if let Err(persistence_error) = outcome_guard.complete(true, &error.to_string()) {
                    return Err(AgentError::ApiError(format!(
                        "provider stream failed: {error}; provider outcome persistence failed: {persistence_error}"
                    )));
                }
                task_phase.complete(DurableTaskPhase::ProviderCompleted)?;
                return Err(error);
            }
        };
        let output = serde_json::to_string(&outcome)
            .map_err(|error| AgentError::ApiError(format!("provider outcome serialization failed: {error}")))?;
        outcome_guard
            .complete(false, &output)
            .map_err(|error| AgentError::ApiError(format!("provider outcome persistence failed: {error}")))?;
        task_phase.complete(DurableTaskPhase::ProviderCompleted)?;
        if let Some(permit) = provider_rate_permit.take() {
            permit
                .commit(outcome.usage.input_tokens.saturating_add(outcome.usage.output_tokens))
                .map_err(AgentError::ResourceBudgetExceeded)?;
        }
        self.record_turn_usage(&outcome.usage, &effect_id)?;
        Ok(outcome)
    }
}
