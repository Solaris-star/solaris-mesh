use serde_json::json;

use solaris_types::effect::DurabilityClass;

use super::{EffectExecutionContext, stable_digest_bytes};
use crate::session::DurableTaskPhase;

impl EffectExecutionContext {
    pub(crate) fn record_task_phase(
        &self,
        message_id: &str,
        phase: DurableTaskPhase,
        call_id: Option<&str>,
    ) -> std::io::Result<()> {
        self.ensure_session_fence()?;
        let task_id = stable_digest_bytes(format!("solaris.agent-task/v1\0{message_id}").as_bytes());
        self.append_record(
            &self.run_id,
            DurabilityClass::SyncCritical,
            "agent_task_phase",
            json!({
                "agent_id": self.agent_id,
                "task_id": format!("sha256:{task_id}"),
                "phase": phase,
                "call_id": call_id,
            }),
        )?;
        Ok(())
    }
}
