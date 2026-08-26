use solaris_protocol::events::ProtocolEvent;
use solaris_types::identity::RunId;

use super::StreamContext;

pub(super) fn emit_plan_artifacts(request_id: String, run_id: Option<String>, ctx: &StreamContext) {
    let artifact_run_id = match resolve_artifact_run_id(&ctx.run_id, run_id.as_deref()) {
        Ok(run_id) => run_id,
        Err(()) => {
            let _ = ctx.writer.emit(&ProtocolEvent::Error {
                msg_id: Some(request_id),
                error: solaris_protocol::events::ErrorInfo {
                    code: "plan_artifacts_forbidden".into(),
                    message: "requested plan artifacts are outside this Host session".into(),
                    retryable: false,
                },
            });
            return;
        }
    };
    match ctx.execution_context.plan_artifacts_for_run_tree(&artifact_run_id) {
        Ok(artifacts) => {
            let _ = ctx.writer.emit(&ProtocolEvent::PlanArtifacts {
                request_id,
                run_id: artifact_run_id.to_string(),
                artifacts,
            });
        }
        Err(error) => {
            tracing::warn!(target: "solaris_protocol", error = %error, "failed to query plan artifacts");
            let _ = ctx.writer.emit(&ProtocolEvent::Error {
                msg_id: Some(request_id),
                error: solaris_protocol::events::ErrorInfo {
                    code: "plan_artifacts_failed".into(),
                    message: "failed to read plan artifacts".into(),
                    retryable: true,
                },
            });
        }
    }
}

fn resolve_artifact_run_id(root_run_id: &RunId, requested: Option<&str>) -> Result<RunId, ()> {
    match requested {
        Some(run_id) if run_id == root_run_id.as_str() || run_id.starts_with(&format!("{}:", root_run_id.as_str())) => {
            Ok(RunId::from(run_id))
        }
        Some(_) => Err(()),
        None => Ok(root_run_id.clone()),
    }
}

#[cfg(test)]
#[path = "plan_test.rs"]
mod plan_test;
