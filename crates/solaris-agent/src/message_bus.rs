use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use serde_json::json;
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::{AgentId, RunId, TeamId};
use solaris_types::workflow::CollaborationRuntimeConfig;
use uuid::Uuid;

use crate::runtime_ledger::{InMemoryRuntimeLedger, LedgerRecord, RunMutationCoordinator, RuntimeLedger};
use crate::{agent_registry::AgentRegistry, team_registry::TeamRegistry};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentMessage {
    pub message_id: String,
    pub run_id: RunId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_id: Option<TeamId>,
    pub from: AgentId,
    pub to: AgentId,
    pub kind: String,
    pub body: serde_json::Value,
    pub created_at_unix_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acknowledged_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct DurableDirectMessage {
    #[serde(flatten)]
    message: AgentMessage,
    #[serde(default)]
    requested_team_id_known: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    requested_team_id: Option<TeamId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct DurableBroadcast {
    broadcast_id: String,
    run_id: RunId,
    team_id: TeamId,
    from: AgentId,
    recipients: Vec<AgentId>,
    kind: String,
    body: serde_json::Value,
    messages: Vec<AgentMessage>,
}

pub struct MessageBus {
    inboxes: RwLock<HashMap<AgentId, Vec<AgentMessage>>>,
    claims: RwLock<HashMap<String, String>>,
    ledger: Arc<dyn RuntimeLedger>,
    mutation: Arc<RunMutationCoordinator>,
    agents: Arc<AgentRegistry>,
    teams: Arc<TeamRegistry>,
}

impl Default for MessageBus {
    fn default() -> Self {
        Self::new(
            Arc::new(InMemoryRuntimeLedger::default()),
            Arc::new(AgentRegistry::default()),
            Arc::new(TeamRegistry::default()),
        )
    }
}

impl MessageBus {
    pub fn new(ledger: Arc<dyn RuntimeLedger>, agents: Arc<AgentRegistry>, teams: Arc<TeamRegistry>) -> Self {
        Self::new_with_mutation(ledger, agents, teams, Arc::new(RunMutationCoordinator::default()))
    }

    pub fn new_with_mutation(
        ledger: Arc<dyn RuntimeLedger>,
        agents: Arc<AgentRegistry>,
        teams: Arc<TeamRegistry>,
        mutation: Arc<RunMutationCoordinator>,
    ) -> Self {
        Self {
            inboxes: RwLock::new(HashMap::new()),
            claims: RwLock::new(HashMap::new()),
            ledger,
            mutation,
            agents,
            teams,
        }
    }

    pub fn send(
        &self,
        run_id: RunId,
        team_id: Option<TeamId>,
        from: AgentId,
        to: AgentId,
        kind: impl Into<String>,
        body: serde_json::Value,
    ) -> std::io::Result<AgentMessage> {
        let line = self.mutation.line_for(&run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        self.send_within_mutation(run_id, team_id, from, to, kind, body)
            .map(|(message, _)| message)
    }

    pub(crate) fn send_within_mutation(
        &self,
        run_id: RunId,
        team_id: Option<TeamId>,
        from: AgentId,
        to: AgentId,
        kind: impl Into<String>,
        body: serde_json::Value,
    ) -> std::io::Result<(AgentMessage, LedgerRecord)> {
        let message_id = format!("msg-{}", Uuid::now_v7());
        let (message, record) = self.send_with_id_within_mutation(message_id, run_id, team_id, from, to, kind, body)?;
        let record = record.ok_or_else(|| std::io::Error::other("random message id unexpectedly reused"))?;
        Ok((message, record))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn send_with_id_within_mutation(
        &self,
        message_id: String,
        run_id: RunId,
        team_id: Option<TeamId>,
        from: AgentId,
        to: AgentId,
        kind: impl Into<String>,
        body: serde_json::Value,
    ) -> std::io::Result<(AgentMessage, Option<LedgerRecord>)> {
        self.send_with_requested_team_id_within_mutation(
            message_id,
            run_id,
            team_id.clone(),
            team_id,
            from,
            to,
            kind,
            body,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn send_with_requested_team_id_within_mutation(
        &self,
        message_id: String,
        run_id: RunId,
        requested_team_id: Option<TeamId>,
        effective_team_id: Option<TeamId>,
        from: AgentId,
        to: AgentId,
        kind: impl Into<String>,
        body: serde_json::Value,
    ) -> std::io::Result<(AgentMessage, Option<LedgerRecord>)> {
        let kind = kind.into();
        if let Some(existing) = self.replay_send_with_id_within_mutation(
            &message_id,
            &run_id,
            requested_team_id.as_ref(),
            &from,
            &to,
            &kind,
            &body,
        )? {
            return Ok((existing, None));
        }
        self.validate_agent_run(&run_id, &from)?;
        self.validate_agent_run(&run_id, &to)?;
        let (max_pending_messages, max_message_bytes) = if let Some(team_id) = effective_team_id.as_ref() {
            let team = self
                .teams
                .get(team_id)
                .ok_or_else(|| std::io::Error::other(format!("unknown message team: {team_id}")))?;
            team.validate_message_limits()?;
            if team.run_id != run_id || !team.members.contains(&from) || !team.members.contains(&to) {
                return Err(std::io::Error::other(
                    "message team and both agents must belong to the requested run",
                ));
            }
            (team.max_pending_messages, team.max_message_bytes)
        } else {
            (
                CollaborationRuntimeConfig::DEFAULT_MAX_PENDING_MESSAGES,
                CollaborationRuntimeConfig::DEFAULT_MAX_MESSAGE_BYTES,
            )
        };
        Self::validate_body_size(&body, max_message_bytes)?;
        let pending_messages = self
            .inboxes
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(&to)
            .map(|messages| {
                messages
                    .iter()
                    .filter(|message| message.acknowledged_at_unix_ms.is_none())
                    .count()
            })
            .unwrap_or_default();
        if pending_messages >= max_pending_messages as usize {
            return Err(std::io::Error::other(format!(
                "message recipient {to} has reached max_pending_messages {max_pending_messages}"
            )));
        }
        let message = AgentMessage {
            message_id,
            run_id: run_id.clone(),
            team_id: effective_team_id,
            from,
            to: to.clone(),
            kind,
            body,
            created_at_unix_ms: chrono::Utc::now().timestamp_millis(),
            acknowledged_at_unix_ms: None,
        };
        let record = self.ledger.append(
            &run_id,
            DurabilityClass::SyncCritical,
            "message_delivered",
            serde_json::to_value(DurableDirectMessage {
                message: message.clone(),
                requested_team_id_known: true,
                requested_team_id,
            })
            .map_err(std::io::Error::other)?,
        )?;
        self.inboxes
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .entry(to)
            .or_default()
            .push(message.clone());
        Ok((message, Some(record)))
    }

    pub(crate) fn broadcast_within_mutation(
        &self,
        run_id: RunId,
        team_id: TeamId,
        from: AgentId,
        recipients: Vec<AgentId>,
        kind: String,
        body: serde_json::Value,
    ) -> std::io::Result<(Vec<AgentMessage>, LedgerRecord)> {
        let broadcast_id = format!("broadcast-{}", Uuid::now_v7());
        let (messages, record) =
            self.broadcast_with_id_within_mutation(broadcast_id, run_id, team_id, from, recipients, kind, body)?;
        let record = record.ok_or_else(|| std::io::Error::other("random broadcast id unexpectedly reused"))?;
        Ok((messages, record))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn broadcast_with_id_within_mutation(
        &self,
        broadcast_id: String,
        run_id: RunId,
        team_id: TeamId,
        from: AgentId,
        recipients: Vec<AgentId>,
        kind: String,
        body: serde_json::Value,
    ) -> std::io::Result<(Vec<AgentMessage>, Option<LedgerRecord>)> {
        if let Some(existing) = self.replay_broadcast_with_recipients_within_mutation(
            &broadcast_id,
            &run_id,
            &team_id,
            &from,
            Some(&recipients),
            &kind,
            &body,
        )? {
            return Ok((existing, None));
        }
        self.validate_agent_run(&run_id, &from)?;
        let team = self
            .teams
            .get(&team_id)
            .ok_or_else(|| std::io::Error::other(format!("unknown message team: {team_id}")))?;
        team.validate_message_limits()?;
        if team.run_id != run_id || !team.members.contains(&from) {
            return Err(std::io::Error::other(
                "broadcast sender and team must belong to the requested run",
            ));
        }
        for recipient in &recipients {
            self.validate_agent_run(&run_id, recipient)?;
            if !team.members.contains(recipient) {
                return Err(std::io::Error::other(
                    "every broadcast recipient must belong to the requested team/run",
                ));
            }
        }
        Self::validate_body_size(&body, team.max_message_bytes)?;
        let mut additions = HashMap::<AgentId, usize>::new();
        for recipient in &recipients {
            *additions.entry(recipient.clone()).or_default() += 1;
        }
        let inboxes = self.inboxes.read().unwrap_or_else(|error| error.into_inner());
        for (recipient, additional) in additions {
            let pending = inboxes
                .get(&recipient)
                .map(|messages| {
                    messages
                        .iter()
                        .filter(|message| message.acknowledged_at_unix_ms.is_none())
                        .count()
                })
                .unwrap_or_default();
            if pending.saturating_add(additional) > team.max_pending_messages as usize {
                return Err(std::io::Error::other(format!(
                    "broadcast recipient {recipient} would exceed max_pending_messages {}",
                    team.max_pending_messages
                )));
            }
        }
        drop(inboxes);
        let created_at_unix_ms = chrono::Utc::now().timestamp_millis();
        let messages: Vec<_> = recipients
            .iter()
            .enumerate()
            .map(|(index, to)| AgentMessage {
                message_id: format!("{broadcast_id}:{index}"),
                run_id: run_id.clone(),
                team_id: Some(team_id.clone()),
                from: from.clone(),
                to: to.clone(),
                kind: kind.clone(),
                body: body.clone(),
                created_at_unix_ms,
                acknowledged_at_unix_ms: None,
            })
            .collect();
        let durable = DurableBroadcast {
            broadcast_id,
            run_id: run_id.clone(),
            team_id,
            from,
            recipients,
            kind,
            body,
            messages: messages.clone(),
        };
        let record = self.ledger.append(
            &run_id,
            DurabilityClass::SyncCritical,
            "messages_broadcast",
            serde_json::to_value(durable).map_err(std::io::Error::other)?,
        )?;
        let mut inboxes = self.inboxes.write().unwrap_or_else(|error| error.into_inner());
        for message in &messages {
            inboxes.entry(message.to.clone()).or_default().push(message.clone());
        }
        Ok((messages, Some(record)))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn replay_send_with_id_within_mutation(
        &self,
        message_id: &str,
        run_id: &RunId,
        requested_team_id: Option<&TeamId>,
        from: &AgentId,
        to: &AgentId,
        kind: &str,
        body: &serde_json::Value,
    ) -> std::io::Result<Option<AgentMessage>> {
        let records = self.ledger.records_for_run(run_id)?;
        let Some(record) = records.iter().find(|record| {
            record.record_type == "message_delivered"
                && record.payload.get("message_id").and_then(serde_json::Value::as_str) == Some(message_id)
        }) else {
            return Ok(None);
        };
        let durable: DurableDirectMessage = serde_json::from_value(record.payload.clone())
            .map_err(|error| std::io::Error::other(format!("invalid durable message: {error}")))?;
        let existing = durable.message;
        if &existing.run_id != run_id
            || (durable.requested_team_id_known && durable.requested_team_id.as_ref() != requested_team_id)
            || (!durable.requested_team_id_known
                && requested_team_id.is_some()
                && existing.team_id.as_ref() != requested_team_id)
            || &existing.from != from
            || &existing.to != to
            || existing.kind != kind
            || &existing.body != body
        {
            return Err(std::io::Error::other(format!(
                "message id {message_id} is already bound to different content"
            )));
        }
        self.repair_durable_messages(std::slice::from_ref(&existing), record.seq, &records)?;
        Ok(Some(existing))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn replay_broadcast_with_id_within_mutation(
        &self,
        broadcast_id: &str,
        run_id: &RunId,
        team_id: &TeamId,
        from: &AgentId,
        kind: &str,
        body: &serde_json::Value,
    ) -> std::io::Result<Option<Vec<AgentMessage>>> {
        self.replay_broadcast_with_recipients_within_mutation(broadcast_id, run_id, team_id, from, None, kind, body)
    }

    #[allow(clippy::too_many_arguments)]
    fn replay_broadcast_with_recipients_within_mutation(
        &self,
        broadcast_id: &str,
        run_id: &RunId,
        team_id: &TeamId,
        from: &AgentId,
        recipients: Option<&[AgentId]>,
        kind: &str,
        body: &serde_json::Value,
    ) -> std::io::Result<Option<Vec<AgentMessage>>> {
        let records = self.ledger.records_for_run(run_id)?;
        let Some(record) = records.iter().find(|record| {
            record.record_type == "messages_broadcast"
                && record.payload.get("broadcast_id").and_then(serde_json::Value::as_str) == Some(broadcast_id)
        }) else {
            return Ok(None);
        };
        let existing: DurableBroadcast = serde_json::from_value(record.payload.clone())
            .map_err(|error| std::io::Error::other(format!("invalid durable broadcast: {error}")))?;
        if &existing.run_id != run_id
            || &existing.team_id != team_id
            || &existing.from != from
            || recipients.is_some_and(|recipients| existing.recipients != recipients)
            || existing.kind != kind
            || &existing.body != body
        {
            return Err(std::io::Error::other(format!(
                "broadcast id {broadcast_id} is already bound to different content"
            )));
        }
        self.repair_durable_messages(&existing.messages, record.seq, &records)?;
        Ok(Some(existing.messages))
    }

    fn repair_durable_messages(
        &self,
        durable_messages: &[AgentMessage],
        delivered_sequence: u64,
        records: &[LedgerRecord],
    ) -> std::io::Result<()> {
        for durable in durable_messages {
            let mut projected = durable.clone();
            let mut dequeued = false;
            let mut claim_id = None;
            for record in records.iter().filter(|record| record.seq > delivered_sequence) {
                match record.record_type.as_str() {
                    "message_acknowledged"
                        if record.payload.get("message_id").and_then(serde_json::Value::as_str)
                            == Some(durable.message_id.as_str()) =>
                    {
                        projected.acknowledged_at_unix_ms = record
                            .payload
                            .get("acknowledged_at_unix_ms")
                            .and_then(serde_json::Value::as_i64);
                    }
                    "messages_claimed" if Self::record_contains_message_id(record, &durable.message_id) => {
                        claim_id = record
                            .payload
                            .get("claim_id")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_owned);
                    }
                    "messages_dequeued" if Self::record_contains_message_id(record, &durable.message_id) => {
                        dequeued = true;
                    }
                    _ => {}
                }
            }
            if let Some(claim_id) = claim_id {
                self.claims
                    .write()
                    .unwrap_or_else(|error| error.into_inner())
                    .insert(durable.message_id.clone(), claim_id);
            }
            let mut inboxes = self.inboxes.write().unwrap_or_else(|error| error.into_inner());
            let messages = inboxes.entry(durable.to.clone()).or_default();
            if dequeued {
                messages.retain(|message| message.message_id != durable.message_id);
                if messages.is_empty() {
                    inboxes.remove(&durable.to);
                }
                continue;
            }
            if let Some(existing) = messages
                .iter_mut()
                .find(|message| message.message_id == durable.message_id)
            {
                if !Self::same_message_content(existing, durable) {
                    return Err(std::io::Error::other(format!(
                        "message projection {} conflicts with its durable content",
                        durable.message_id
                    )));
                }
                existing.acknowledged_at_unix_ms = projected.acknowledged_at_unix_ms;
            } else {
                messages.push(projected);
            }
        }
        Ok(())
    }

    fn record_contains_message_id(record: &LedgerRecord, message_id: &str) -> bool {
        record
            .payload
            .get("message_ids")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|ids| ids.iter().any(|id| id.as_str() == Some(message_id)))
    }

    fn same_message_content(left: &AgentMessage, right: &AgentMessage) -> bool {
        left.message_id == right.message_id
            && left.run_id == right.run_id
            && left.team_id == right.team_id
            && left.from == right.from
            && left.to == right.to
            && left.kind == right.kind
            && left.body == right.body
            && left.created_at_unix_ms == right.created_at_unix_ms
    }

    fn validate_body_size(body: &serde_json::Value, max_message_bytes: u32) -> std::io::Result<()> {
        let bytes = serde_json::to_vec(body).map_err(std::io::Error::other)?.len();
        if bytes > max_message_bytes as usize {
            return Err(std::io::Error::other(format!(
                "message body is {bytes} bytes and exceeds max_message_bytes {max_message_bytes}"
            )));
        }
        Ok(())
    }

    pub fn restore_message(&self, message: AgentMessage) {
        let mut inboxes = self.inboxes.write().unwrap_or_else(|error| error.into_inner());
        let messages = inboxes.entry(message.to.clone()).or_default();
        if !messages
            .iter()
            .any(|existing| existing.message_id == message.message_id)
        {
            messages.push(message);
        }
    }

    pub(crate) fn restore_broadcast(&self, payload: serde_json::Value) -> std::io::Result<()> {
        let messages = if payload.is_array() {
            serde_json::from_value::<Vec<AgentMessage>>(payload)
                .map_err(|error| std::io::Error::other(format!("invalid legacy durable broadcast: {error}")))?
        } else {
            serde_json::from_value::<DurableBroadcast>(payload)
                .map_err(|error| std::io::Error::other(format!("invalid durable broadcast: {error}")))?
                .messages
        };
        for message in messages {
            self.restore_message(message);
        }
        Ok(())
    }

    pub fn inbox(&self, agent_id: &AgentId) -> Vec<AgentMessage> {
        self.inboxes
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(agent_id)
            .map(|messages| {
                messages
                    .iter()
                    .filter(|message| message.acknowledged_at_unix_ms.is_none())
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn claim(&self, run_id: &RunId, agent_id: &AgentId, claim_id: &str) -> std::io::Result<Vec<AgentMessage>> {
        self.validate_agent_run(run_id, agent_id)?;
        let line = self.mutation.line_for(run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(record) = self.ledger.records_for_run(run_id)?.into_iter().find(|record| {
            record.record_type == "messages_claimed"
                && record.payload.get("claim_id").and_then(serde_json::Value::as_str) == Some(claim_id)
                && record.payload.get("agent_id").and_then(serde_json::Value::as_str) == Some(agent_id.as_str())
        }) {
            let ids: Vec<_> = record
                .payload
                .get("message_ids")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(serde_json::Value::as_str)
                .collect();
            return Ok(self
                .inboxes
                .read()
                .unwrap_or_else(|error| error.into_inner())
                .get(agent_id)
                .into_iter()
                .flat_map(|messages| messages.iter())
                .filter(|message| {
                    message.acknowledged_at_unix_ms.is_none() && ids.contains(&message.message_id.as_str())
                })
                .cloned()
                .collect());
        }
        let claims = self.claims.read().unwrap_or_else(|error| error.into_inner());
        let messages: Vec<_> = self
            .inboxes
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(agent_id)
            .into_iter()
            .flat_map(|messages| messages.iter())
            .filter(|message| {
                &message.run_id == run_id
                    && message.acknowledged_at_unix_ms.is_none()
                    && !claims.contains_key(&message.message_id)
            })
            .cloned()
            .collect();
        drop(claims);
        if messages.is_empty() {
            return Ok(Vec::new());
        }
        let message_ids: Vec<_> = messages.iter().map(|message| message.message_id.clone()).collect();
        self.ledger.append(
            run_id,
            DurabilityClass::SyncCritical,
            "messages_claimed",
            json!({"agent_id": agent_id, "claim_id": claim_id, "message_ids": message_ids}),
        )?;
        let mut claims = self.claims.write().unwrap_or_else(|error| error.into_inner());
        for message_id in message_ids {
            claims.insert(message_id, claim_id.to_owned());
        }
        Ok(messages)
    }

    pub fn restore_claim(&self, claim_id: &str, message_ids: &[String]) {
        let mut claims = self.claims.write().unwrap_or_else(|error| error.into_inner());
        for message_id in message_ids {
            claims.insert(message_id.clone(), claim_id.to_owned());
        }
    }

    pub fn acknowledge(&self, run_id: &RunId, agent_id: &AgentId, message_id: &str) -> std::io::Result<bool> {
        self.validate_agent_run(run_id, agent_id)?;
        let line = self.mutation.line_for(run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        self.acknowledge_within_mutation(run_id, agent_id, message_id)
            .map(|(acknowledged, _)| acknowledged)
    }

    pub(crate) fn acknowledge_within_mutation(
        &self,
        run_id: &RunId,
        agent_id: &AgentId,
        message_id: &str,
    ) -> std::io::Result<(bool, Option<LedgerRecord>)> {
        self.validate_agent_run(run_id, agent_id)?;
        let mut inboxes = self.inboxes.write().unwrap_or_else(|error| error.into_inner());
        let Some(message) = inboxes
            .get_mut(agent_id)
            .and_then(|messages| messages.iter_mut().find(|message| message.message_id == message_id))
        else {
            return Ok((false, None));
        };
        if &message.run_id != run_id || &message.to != agent_id {
            return Err(std::io::Error::other(
                "message acknowledgement does not belong to the requested run/agent",
            ));
        }
        if message.acknowledged_at_unix_ms.is_some() {
            return Ok((true, None));
        }
        let acknowledged_at_unix_ms = chrono::Utc::now().timestamp_millis();
        let record = self.ledger.append(
            run_id,
            DurabilityClass::SyncCritical,
            "message_acknowledged",
            json!({
                "message_id": message_id,
                "agent_id": agent_id,
                "acknowledged_at_unix_ms": acknowledged_at_unix_ms,
            }),
        )?;
        message.acknowledged_at_unix_ms = Some(acknowledged_at_unix_ms);
        Ok((true, Some(record)))
    }

    pub fn restore_acknowledgement(&self, agent_id: &AgentId, message_id: &str, acknowledged_at_unix_ms: i64) -> bool {
        self.inboxes
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .get_mut(agent_id)
            .and_then(|messages| messages.iter_mut().find(|message| message.message_id == message_id))
            .is_some_and(|message| {
                message.acknowledged_at_unix_ms = Some(acknowledged_at_unix_ms);
                true
            })
    }

    pub fn drain(&self, run_id: &RunId, agent_id: &AgentId) -> std::io::Result<Vec<AgentMessage>> {
        self.validate_agent_run(run_id, agent_id)?;
        let line = self.mutation.line_for(run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        self.drain_within_mutation(run_id, agent_id)
            .map(|(messages, _)| messages)
    }

    pub(crate) fn drain_within_mutation(
        &self,
        run_id: &RunId,
        agent_id: &AgentId,
    ) -> std::io::Result<(Vec<AgentMessage>, Option<LedgerRecord>)> {
        self.validate_agent_run(run_id, agent_id)?;
        let mut inboxes = self.inboxes.write().unwrap_or_else(|error| error.into_inner());
        let drained: Vec<_> = inboxes
            .get(agent_id)
            .into_iter()
            .flat_map(|messages| messages.iter())
            .filter(|message| &message.run_id == run_id)
            .cloned()
            .collect();
        if drained.is_empty() {
            return Ok((Vec::new(), None));
        }
        let message_ids: Vec<_> = drained.iter().map(|message| message.message_id.clone()).collect();
        let record = self.ledger.append(
            run_id,
            DurabilityClass::SyncCritical,
            "messages_dequeued",
            json!({"agent_id": agent_id, "message_ids": message_ids}),
        )?;
        if let Some(messages) = inboxes.get_mut(agent_id) {
            messages.retain(|message| !message_ids.contains(&message.message_id));
            if messages.is_empty() {
                inboxes.remove(agent_id);
            }
        }
        Ok((drained, Some(record)))
    }

    pub fn restore_dequeue(&self, agent_id: &AgentId, message_ids: &[String]) -> usize {
        let mut inboxes = self.inboxes.write().unwrap_or_else(|error| error.into_inner());
        let Some(messages) = inboxes.get_mut(agent_id) else {
            return 0;
        };
        let before = messages.len();
        messages.retain(|message| !message_ids.contains(&message.message_id));
        let removed = before.saturating_sub(messages.len());
        if messages.is_empty() {
            inboxes.remove(agent_id);
        }
        removed
    }

    pub fn snapshot(&self) -> Vec<AgentMessage> {
        let mut messages: Vec<_> = self
            .inboxes
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .flat_map(|messages| messages.iter().cloned())
            .collect();
        messages.sort_by_key(|message| (message.created_at_unix_ms, message.message_id.clone()));
        messages
    }

    fn validate_agent_run(&self, run_id: &RunId, agent_id: &AgentId) -> std::io::Result<()> {
        let agent = self
            .agents
            .get(agent_id)
            .ok_or_else(|| std::io::Error::other(format!("unknown message agent: {agent_id}")))?;
        if &agent.run_id != run_id {
            return Err(std::io::Error::other(
                "message agent does not belong to the requested run",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "message_bus_test.rs"]
mod message_bus_test;
