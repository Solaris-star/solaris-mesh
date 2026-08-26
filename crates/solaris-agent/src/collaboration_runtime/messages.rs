use solaris_types::identity::{AgentId, RunId, TeamId};
use solaris_types::workflow::CollaborationStrategy;

use crate::message_bus::AgentMessage;

use super::CollaborationRuntime;

impl<T> CollaborationRuntime<T> {
    pub fn broadcast_message_with_id(
        &self,
        broadcast_id: String,
        run_id: RunId,
        team_id: TeamId,
        from: AgentId,
        kind: impl Into<String>,
        body: serde_json::Value,
    ) -> std::io::Result<Vec<AgentMessage>> {
        let kind = kind.into();
        let line = self.mutation.line_for(&run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(messages) = self.messages.replay_broadcast_with_id_within_mutation(
            &broadcast_id,
            &run_id,
            &team_id,
            &from,
            &kind,
            &body,
        )? {
            return Ok(messages);
        }
        let team = self
            .teams
            .get(&team_id)
            .ok_or_else(|| std::io::Error::other(format!("unknown team: {team_id}")))?;
        if team.run_id != run_id || !team.members.contains(&from) {
            return Err(std::io::Error::other(
                "broadcast sender must belong to the requested team/run",
            ));
        }
        if team.strategy == CollaborationStrategy::Supervisor && team.coordinator.as_ref() != Some(&from) {
            return Err(std::io::Error::other(
                "only the Supervisor coordinator may broadcast to the Team",
            ));
        }
        let mut recipients: Vec<_> = team.members.into_iter().filter(|agent_id| agent_id != &from).collect();
        recipients.sort();
        let (messages, record) = self.messages.broadcast_with_id_within_mutation(
            broadcast_id,
            run_id,
            team_id,
            from.clone(),
            recipients,
            kind,
            body,
        )?;
        if let Some(record) = record {
            self.emit_durable_event(
                &record,
                Some(from),
                "agent_message_broadcast",
                serde_json::to_value(&messages).unwrap_or_default(),
            );
        }
        Ok(messages)
    }
}

#[cfg(test)]
#[path = "messages_test.rs"]
mod messages_test;
