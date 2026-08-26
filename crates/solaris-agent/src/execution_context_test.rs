use super::environment::{compatibility_decision, normalize_plugin_identities};
use super::redaction::{is_argc_marker, is_bare_sha256_marker, is_exact_fallback_projection, is_redaction_marker};
use super::*;
use solaris_tools::Tool;
use solaris_tools::registry::ToolRegistry;
use solaris_tools::write::WriteTool;
use solaris_types::effect::{EffectReplayPolicy, ProcessInvocation, ResourceFootprint};
use solaris_types::permission::{ExecutionBoundary, PermissionCeiling, PermissionMode};
use solaris_types::runtime::{CompatibilityDecision, ToolImplementationSnapshot};

impl EffectExecutionContext {
    pub(crate) fn has_approved_request_for_call(&self, call_id: &str) -> bool {
        let effect_id = self.effect_id_for_call(call_id);
        self.approved_requests
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .contains_key(&effect_id)
    }
}

#[derive(Clone, Default)]
struct TestLogBuffer(Arc<Mutex<String>>);

struct TestSubscriber(TestLogBuffer);

struct TestVisitor<'buffer>(&'buffer TestLogBuffer);

impl tracing::field::Visit for TestVisitor<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write;

        let _ = write!(self.0.0.lock().unwrap(), "{}={value:?} ", field.name());
    }
}

impl tracing::Subscriber for TestSubscriber {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        *metadata.level() <= tracing::Level::ERROR
    }

    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        event.record(&mut TestVisitor(&self.0));
    }

    fn enter(&self, _span: &tracing::span::Id) {}

    fn exit(&self, _span: &tracing::span::Id) {}
}

struct RejectingRuntimeLedger;

impl RuntimeLedger for RejectingRuntimeLedger {
    crate::runtime_ledger::unsupported_compare_and_append!();

    fn append(
        &self,
        _run_id: &RunId,
        _durability: DurabilityClass,
        _record_type: &str,
        _payload: Value,
    ) -> std::io::Result<crate::runtime_ledger::LedgerRecord> {
        Err(std::io::Error::other("super-secret-token-ledger-error"))
    }

    fn run_ids(&self) -> std::io::Result<Vec<RunId>> {
        Ok(Vec::new())
    }

    fn records_for_run(&self, _run_id: &RunId) -> std::io::Result<Vec<crate::runtime_ledger::LedgerRecord>> {
        Ok(Vec::new())
    }
}

include!("execution_context_approval_test.rs");
include!("execution_context_recovery_test.rs");
include!("execution_context_projection_test.rs");
include!("execution_context_launch_policy_test.rs");
include!("execution_context_file_policy_test.rs");
include!("execution_context_task_phase_test.rs");
