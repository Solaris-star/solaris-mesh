use super::*;

#[cfg(test)]
#[derive(Clone)]
pub(super) struct TurnAdmissionHook {
    reached: Arc<tokio::sync::Barrier>,
    resume: Arc<tokio::sync::Barrier>,
}

#[cfg(test)]
#[derive(Clone)]
pub(super) struct TurnEnqueuedHook {
    reached: Arc<tokio::sync::Barrier>,
    resume: Arc<tokio::sync::Barrier>,
}

#[cfg(test)]
#[derive(Clone)]
pub(super) struct OpenClaimHook {
    reached: Arc<tokio::sync::Barrier>,
    resume: Arc<tokio::sync::Barrier>,
}

impl AgentConversationService {
    pub(super) async fn recover_terminal_turn_outcome(
        &self,
        handle: &AgentConversationHandle,
        turn: &AgentTurnSpec,
        identity: &AgentTurnIdentity,
        outcome_json: Option<&[u8]>,
    ) -> Result<AgentTurnOutcome, AgentConversationError> {
        let outcome = decode_outcome(outcome_json)?;
        conversation_records::validate_outcome(handle, &turn.turn_id, identity, &outcome)?;
        self.ensure_session_quiescent_async(handle).await?;
        self.repair_idle_after_turn(handle)?;
        self.hydrate_turn_outcome(&outcome)
    }

    #[cfg(test)]
    pub(super) fn install_open_claim_hook(&self) -> (Arc<tokio::sync::Barrier>, Arc<tokio::sync::Barrier>) {
        let hook = OpenClaimHook {
            reached: Arc::new(tokio::sync::Barrier::new(2)),
            resume: Arc::new(tokio::sync::Barrier::new(2)),
        };
        *self.open_claim_hook.lock().unwrap_or_else(|error| error.into_inner()) = Some(hook.clone());
        (hook.reached, hook.resume)
    }

    #[cfg(test)]
    pub(super) async fn pause_after_open_claim(&self) {
        let hook = self
            .open_claim_hook
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(hook) = hook {
            hook.reached.wait().await;
            hook.resume.wait().await;
        }
    }

    #[cfg(test)]
    pub(super) fn install_turn_admission_hook(&self) -> (Arc<tokio::sync::Barrier>, Arc<tokio::sync::Barrier>) {
        let hook = TurnAdmissionHook {
            reached: Arc::new(tokio::sync::Barrier::new(2)),
            resume: Arc::new(tokio::sync::Barrier::new(2)),
        };
        *self
            .turn_admission_hook
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(hook.clone());
        (hook.reached, hook.resume)
    }

    #[cfg(test)]
    pub(super) async fn pause_before_turn_admission(&self) {
        let hook = self
            .turn_admission_hook
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(hook) = hook {
            hook.reached.wait().await;
            hook.resume.wait().await;
        }
    }

    #[cfg(test)]
    pub(super) fn install_turn_enqueued_hook(&self) -> (Arc<tokio::sync::Barrier>, Arc<tokio::sync::Barrier>) {
        let hook = TurnEnqueuedHook {
            reached: Arc::new(tokio::sync::Barrier::new(2)),
            resume: Arc::new(tokio::sync::Barrier::new(2)),
        };
        *self
            .turn_enqueued_hook
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(hook.clone());
        (hook.reached, hook.resume)
    }

    #[cfg(test)]
    pub(super) async fn pause_after_turn_enqueued(&self) {
        let hook = self
            .turn_enqueued_hook
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(hook) = hook {
            hook.reached.wait().await;
            hook.resume.wait().await;
        }
    }

    pub(super) fn restore_projection(&self, run_id: &RunId) -> Result<(), AgentConversationError> {
        self.spawner
            .lifecycle_runtime
            .restore_projection(run_id)
            .map_err(|error| AgentConversationError::reconciliation_required(error.to_string()))
    }

    pub(super) async fn settle_turns_for_close(
        &self,
        handle: &AgentConversationHandle,
        store: &AsyncConversationStore,
        durable_identity: &ConversationIdentity,
        handle_json: &[u8],
    ) -> Result<(), AgentConversationError> {
        for stored in store.list_turns(durable_identity).await? {
            let identity = AgentTurnIdentity {
                operation_id: OperationId::from(stored.identity.operation_id.clone()),
                message_id: stored.identity.message_id.clone(),
                input_digest: stored.identity.input_digest.clone(),
            };
            let turn = AgentTurnSpec {
                turn_id: stored.identity.turn_id.clone(),
                prompt: String::new(),
            };
            if stored.state.is_terminal() {
                let outcome = decode_outcome(stored.outcome_json.as_deref())?;
                conversation_records::validate_outcome(handle, &turn.turn_id, &identity, &outcome)?;
                let ledger_outcome = self.find_turn_outcome(handle, &turn.turn_id)?.ok_or_else(|| {
                    AgentConversationError::reconciliation_required(
                        "terminal conversation turn has no durable Ledger outcome",
                    )
                })?;
                let expected_failure = outcome.failure_class.map(failure_class_name);
                if outcome_store_state(&outcome) != stored.state
                    || stored.failure_class.as_deref() != expected_failure
                    || encode_outcome(&ledger_outcome)? != encode_outcome(&outcome)?
                {
                    return Err(AgentConversationError::reconciliation_required(
                        "terminal conversation turn state conflicts with its durable outcome",
                    ));
                }
                continue;
            }
            let existing_intent = self.find_turn_intent(handle, &turn.turn_id)?;
            let possibly_executed = stored.state == ConversationTurnState::IntentCommitted || existing_intent.is_some();
            if existing_intent.is_none() {
                self.record_turn_intent(
                    handle,
                    &DurableTurnIntent {
                        schema_version: CONVERSATION_SCHEMA_VERSION,
                        run_id: handle.run_id.clone(),
                        parent_agent_id: handle.parent_agent_id.clone(),
                        conversation_id: handle.conversation_id.clone(),
                        task_id: handle.task_id.clone(),
                        open_operation_id: handle.operation_id.clone(),
                        spec_digest: handle.spec_digest.clone(),
                        turn_id: turn.turn_id.clone(),
                        agent_id: handle.agent_id.clone(),
                        session_id: handle.session_id.clone(),
                        identity: identity.clone(),
                    },
                )?;
            }
            let error = if possibly_executed {
                AgentConversationError::outcome_unknown("Provider outcome is unknown during conversation close")
            } else {
                AgentConversationError {
                    failure_class: TaskFailureClass::Cancelled,
                    message: "Agent conversation closed before Provider execution".to_owned(),
                }
            };
            let outcome = failure_outcome(handle, &turn, &identity, &error);
            self.record_turn_outcome(handle, &outcome)?;
            store
                .finalize_turn(
                    durable_identity,
                    handle_json.to_vec(),
                    &stored.identity,
                    stored.sequence,
                    ConversationTurnCompletion {
                        state: outcome_store_state(&outcome),
                        outcome_json: encode_outcome(&outcome)?,
                        failure_class: outcome.failure_class.map(failure_class_name).map(str::to_owned),
                        block_following_turns: possibly_executed,
                    },
                )
                .await?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn conversation_store(&self) -> Result<SessionStore, AgentConversationError> {
        SessionStore::open(&self.spawner.base_config.session.directory).map_err(store_error)
    }

    pub(super) async fn validate_handle_async(
        &self,
        handle: &AgentConversationHandle,
        requested: Option<&AgentConversationSpec>,
    ) -> Result<(), AgentConversationError> {
        let service = self.clone();
        let handle = handle.clone();
        let requested = requested.cloned();
        blocking_agent(move || service.validate_handle(&handle, requested.as_ref())).await
    }

    pub(super) async fn prepare_open_runtime_async(
        &self,
        spec: &AgentConversationSpec,
        agent_id: AgentId,
        key: ChildAgentKey,
        session_id: String,
    ) -> Result<ConversationRuntime, AgentConversationError> {
        let service = self.clone();
        let spec = spec.clone();
        blocking_agent(move || {
            let runtime = service.build_runtime(&spec, agent_id, key, true)?;
            service.ensure_session_persisted(&spec, &runtime, &session_id)?;
            Ok(runtime)
        })
        .await
    }

    pub(super) async fn prepare_turn_async(
        &self,
        handle: &AgentConversationHandle,
    ) -> Result<PreparedTurn, AgentConversationError> {
        let service = self.clone();
        let handle = handle.clone();
        blocking_agent(move || service.prepare_turn(&handle)).await
    }

    pub(super) async fn ensure_session_quiescent_async(
        &self,
        handle: &AgentConversationHandle,
    ) -> Result<(), AgentConversationError> {
        let service = self.clone();
        let handle = handle.clone();
        blocking_agent(move || service.ensure_session_quiescent(&handle)).await
    }

    pub(super) async fn wait_for_session_cleanup(&self, handle: &AgentConversationHandle) {
        self.session_cleanups.wait(&handle.session_id).await;
    }

    pub(super) fn ensure_session_quiescent(
        &self,
        handle: &AgentConversationHandle,
    ) -> Result<(), AgentConversationError> {
        let manager = SessionManager::new(
            self.spawner.base_config.session.directory.clone().into(),
            self.spawner.base_config.session.max_sessions,
        );
        manager
            .load_active_session(&handle.session_id)
            .map_err(|error| AgentConversationError::outcome_unknown(error.to_string()))?;
        manager
            .release_active_session()
            .map_err(|error| AgentConversationError::reconciliation_required(error.to_string()))
    }

    pub(super) fn set_agent_state(
        &self,
        handle: &AgentConversationHandle,
        state: AgentLifecycleState,
    ) -> Result<(), AgentConversationError> {
        match self
            .spawner
            .lifecycle_runtime
            .set_agent_state(&handle.run_id, &handle.agent_id, state)
        {
            Ok(_) => Ok(()),
            Err(error) => {
                self.restore_projection(&handle.run_id)?;
                if self
                    .spawner
                    .lifecycle_runtime
                    .agents()
                    .get(&handle.agent_id)
                    .is_some_and(|agent| agent.state == state)
                {
                    Ok(())
                } else {
                    Err(AgentConversationError::reconciliation_required(error.to_string()))
                }
            }
        }
    }

    pub(super) fn repair_idle_after_turn(
        &self,
        handle: &AgentConversationHandle,
    ) -> Result<(), AgentConversationError> {
        let state = self
            .spawner
            .lifecycle_runtime
            .agents()
            .get(&handle.agent_id)
            .map(|agent| agent.state)
            .ok_or_else(|| AgentConversationError::reconciliation_required("Agent projection is missing"))?;
        if state == AgentLifecycleState::Idle {
            Ok(())
        } else if matches!(state, AgentLifecycleState::Active | AgentLifecycleState::Initializing) {
            self.set_agent_state(handle, AgentLifecycleState::Idle)
        } else {
            Err(AgentConversationError::reconciliation_required(
                "Agent conversation has an unexpected terminal state",
            ))
        }
    }

    pub(super) fn repair_terminal_state(
        &self,
        handle: &AgentConversationHandle,
        terminal: AgentLifecycleState,
    ) -> Result<(), AgentConversationError> {
        let state = self
            .spawner
            .lifecycle_runtime
            .agents()
            .get(&handle.agent_id)
            .map(|agent| agent.state)
            .ok_or_else(|| AgentConversationError::reconciliation_required("Agent projection is missing"))?;
        if state == terminal {
            Ok(())
        } else if state == AgentLifecycleState::Idle {
            self.set_agent_state(handle, terminal)
        } else {
            Err(AgentConversationError::reconciliation_required(
                "Agent conversation terminal state conflicts with its close record",
            ))
        }
    }
}
