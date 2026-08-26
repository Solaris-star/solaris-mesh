use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use solaris_agent::engine::{AgentEngine, AgentResult};
use solaris_agent::output::OutputSink;
use solaris_agent::plan::tools::EnterPlanModeTool;
use solaris_agent::runtime_ledger::{InMemoryRuntimeLedger, LedgerRecord, RuntimeLedger};
use solaris_agent::workflow_controller::{WorkflowController, WorkflowRunStatus};
use solaris_config::config::{CliArgs, Config};
use solaris_protocol::commands::ProtocolCommand;
use solaris_protocol::reader::ProtocolInput;
use solaris_tools::Tool;
use solaris_tools::registry::ToolRegistry;
use solaris_types::config::{ConfigField, ConfigFieldStatus};
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::RunId;
use solaris_types::message::{StopReason, TokenUsage};
use solaris_types::run_preset::Intensity;
use solaris_types::skill_types::PlanModeTransition;
use solaris_types::spawner::AgentOutcomeStatus;

use super::{
    ENGINE_FAILURE_CODE, ENGINE_FAILURE_MESSAGE, OperationTerminal, PendingConfiguration, PendingControls,
    REQUIRED_WORKFLOW_FAILURE_CODE, REQUIRED_WORKFLOW_FAILURE_MESSAGE, RequiredCancellationDisposition, WorkflowWait,
    apply_queued_intensity, cancel_required_workflow, classify_required_cancellation, emit_turn_failure,
    operation_terminal_for_agent_result, pending_control_rejection, rejected_config_outcome,
    required_workflow_for_message, wait_for_engine_or_control, wait_for_workflow_or_control,
};
use crate::json_stream::dispatch::ConfigCommand;

struct NullOutput;

impl OutputSink for NullOutput {
    fn emit_text_delta(&self, _: &str, _: &str) {}
    fn emit_thinking(&self, _: &str, _: &str) {}
    fn emit_tool_call(&self, _: &str, _: &str, _: &str) {}
    fn emit_tool_result(&self, _: &str, _: &str, _: bool, _: &str) {}
    fn emit_stream_start(&self, _: &str) {}
    fn emit_stream_end(&self, _: &str, _: usize, _: u64, _: u64, _: u64, _: u64) {}
    fn emit_error(&self, _: &str) {}
    fn emit_info(&self, _: &str) {}
}

#[derive(Debug, PartialEq, Eq)]
enum RecordedEvent {
    Error {
        msg_id: String,
        code: String,
        message: String,
        retryable: bool,
    },
    End(String),
}

#[derive(Default)]
struct RecordingOutput {
    events: Mutex<Vec<RecordedEvent>>,
}

impl OutputSink for RecordingOutput {
    fn emit_text_delta(&self, _: &str, _: &str) {}
    fn emit_thinking(&self, _: &str, _: &str) {}
    fn emit_tool_call(&self, _: &str, _: &str, _: &str) {}
    fn emit_tool_result(&self, _: &str, _: &str, _: bool, _: &str) {}
    fn emit_stream_start(&self, _: &str) {}
    fn emit_stream_end(&self, msg_id: &str, _: usize, _: u64, _: u64, _: u64, _: u64) {
        self.events.lock().unwrap().push(RecordedEvent::End(msg_id.to_owned()));
    }
    fn emit_error(&self, _: &str) {}
    fn emit_protocol_error(&self, msg_id: &str, code: &str, message: &str, retryable: bool) {
        self.events.lock().unwrap().push(RecordedEvent::Error {
            msg_id: msg_id.to_owned(),
            code: code.to_owned(),
            message: message.to_owned(),
            retryable,
        });
    }
    fn emit_info(&self, _: &str) {}
}

struct CancelFailLedger {
    records: InMemoryRuntimeLedger,
    fail_cancel: AtomicBool,
}

impl CancelFailLedger {
    fn new() -> Self {
        Self {
            records: InMemoryRuntimeLedger::default(),
            fail_cancel: AtomicBool::new(false),
        }
    }
}

impl RuntimeLedger for CancelFailLedger {
    fn logical_append_capability(&self) -> solaris_agent::runtime_ledger::LogicalAppendCapability {
        self.records.logical_append_capability()
    }

    fn acquire_workflow_mutation_lease(
        &self,
        run_id: &RunId,
        owner_id: &str,
        now_unix_ms: i64,
    ) -> io::Result<solaris_agent::runtime_ledger::WorkflowMutationLease> {
        self.records
            .acquire_workflow_mutation_lease(run_id, owner_id, now_unix_ms)
    }

    fn renew_workflow_mutation_lease(
        &self,
        lease: &solaris_agent::runtime_ledger::WorkflowMutationLease,
        now_unix_ms: i64,
    ) -> io::Result<solaris_agent::runtime_ledger::WorkflowMutationLease> {
        self.records.renew_workflow_mutation_lease(lease, now_unix_ms)
    }

    fn commit_workflow_restore(
        &self,
        lease: &solaris_agent::runtime_ledger::WorkflowMutationLease,
        expected_sequence: u64,
        now_unix_ms: i64,
    ) -> io::Result<solaris_agent::runtime_ledger::WorkflowRestoreCommit> {
        self.records
            .commit_workflow_restore(lease, expected_sequence, now_unix_ms)
    }

    fn release_workflow_mutation_lease(
        &self,
        lease: &solaris_agent::runtime_ledger::WorkflowMutationLease,
    ) -> io::Result<()> {
        self.records.release_workflow_mutation_lease(lease)
    }

    fn append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        payload: serde_json::Value,
    ) -> io::Result<LedgerRecord> {
        if record_type == "workflow_cancelled" && self.fail_cancel.load(Ordering::SeqCst) {
            return Err(io::Error::other("injected cancellation persistence failure"));
        }
        self.records.append(run_id, durability, record_type, payload)
    }

    fn append_under_workflow_lease(
        &self,
        lease: &solaris_agent::runtime_ledger::WorkflowMutationLease,
        now_unix_ms: i64,
        durability: DurabilityClass,
        record_type: &str,
        payload: serde_json::Value,
    ) -> io::Result<LedgerRecord> {
        if record_type == "workflow_cancelled" && self.fail_cancel.load(Ordering::SeqCst) {
            return Err(io::Error::other("injected cancellation persistence failure"));
        }
        self.records
            .append_under_workflow_lease(lease, now_unix_ms, durability, record_type, payload)
    }

    fn compare_and_append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        identity_fields: &[&str],
        payload: serde_json::Value,
    ) -> io::Result<LedgerRecord> {
        self.records
            .compare_and_append(run_id, durability, record_type, identity_fields, payload)
    }

    fn compare_and_append_under_workflow_lease(
        &self,
        lease: &solaris_agent::runtime_ledger::WorkflowMutationLease,
        now_unix_ms: i64,
        durability: DurabilityClass,
        record_type: &str,
        identity_fields: &[&str],
        payload: serde_json::Value,
    ) -> io::Result<LedgerRecord> {
        self.records.compare_and_append_under_workflow_lease(
            lease,
            now_unix_ms,
            durability,
            record_type,
            identity_fields,
            payload,
        )
    }

    fn run_ids(&self) -> io::Result<Vec<RunId>> {
        self.records.run_ids()
    }

    fn records_for_run(&self, run_id: &RunId) -> io::Result<Vec<LedgerRecord>> {
        self.records.records_for_run(run_id)
    }
}

fn make_engine() -> AgentEngine {
    let config = Config::resolve(&CliArgs {
        provider: Some("openai".into()),
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
        auto_approve: true,
        project_dir: None,
    })
    .unwrap();
    AgentEngine::new(
        config,
        ToolRegistry::new(),
        Arc::new(NullOutput),
        std::env::current_dir().unwrap(),
    )
}

#[test]
fn active_turn_intensity_keeps_request_id_and_does_not_apply_while_queued() {
    let mut engine = make_engine();
    let mut pending = PendingControls::default();
    pending.configurations.push_back(PendingConfiguration::Intensity {
        request_id: Some("intensity-1".into()),
        intensity: Intensity::Extra,
    });

    assert_eq!(
        engine.runtime_configuration_view().snapshot().selected_intensity,
        Intensity::High
    );
    let Some(PendingConfiguration::Intensity { request_id, intensity }) = pending.configurations.pop_front() else {
        panic!("queued intensity command missing");
    };
    let application = apply_queued_intensity(&mut engine, request_id, intensity, None);

    assert_eq!(application.request_id.as_deref(), Some("intensity-1"));
    assert!(application.applied);
    assert_eq!(
        engine.runtime_configuration_view().snapshot().selected_intensity,
        Intensity::Extra
    );
}

#[test]
fn cancelled_turn_rejects_queued_intensity_with_a_terminal_result() {
    let mut engine = make_engine();

    let application = apply_queued_intensity(
        &mut engine,
        Some("intensity-after-cancel".into()),
        Intensity::Ultracode,
        Some("turn was cancelled before the queued intensity was applied"),
    );

    assert!(!application.applied);
    assert!(application.message.as_deref().unwrap().contains("cancelled"));
    assert_eq!(application.request_id.as_deref(), Some("intensity-after-cancel"));
    assert_eq!(
        engine.runtime_configuration_view().snapshot().selected_intensity,
        Intensity::High
    );
}

#[test]
fn failed_turn_rejects_queued_intensity_with_a_terminal_result() {
    let mut engine = make_engine();

    let application = apply_queued_intensity(
        &mut engine,
        Some("intensity-after-failure".into()),
        Intensity::Low,
        Some("turn failed before the queued intensity was applied"),
    );

    assert!(!application.applied);
    assert!(application.message.as_deref().unwrap().contains("failed"));
    assert_eq!(application.request_id.as_deref(), Some("intensity-after-failure"));
    assert_eq!(
        engine.runtime_configuration_view().snapshot().selected_intensity,
        Intensity::High
    );
}

#[test]
fn stopped_session_rejects_queued_intensity_without_mutating_configuration() {
    let mut engine = make_engine();

    let application = apply_queued_intensity(
        &mut engine,
        Some("intensity-before-stop".into()),
        Intensity::Low,
        Some("session stopped before the queued intensity was applied"),
    );

    assert!(!application.applied);
    assert_eq!(application.request_id.as_deref(), Some("intensity-before-stop"));
    assert!(application.message.is_some());
    assert_eq!(
        engine.runtime_configuration_view().snapshot().selected_intensity,
        Intensity::High
    );
}

#[test]
fn active_turn_terminal_states_select_safe_intensity_results() {
    assert_eq!(pending_control_rejection(OperationTerminal::Succeeded), None);
    assert!(
        pending_control_rejection(OperationTerminal::Cancelled)
            .unwrap()
            .contains("cancelled")
    );
    assert!(
        pending_control_rejection(OperationTerminal::Failed)
            .unwrap()
            .contains("failed")
    );
    assert!(
        pending_control_rejection(OperationTerminal::Stopped)
            .unwrap()
            .contains("stopped")
    );
}

#[test]
fn failed_turn_rejects_every_supplied_config_field_without_mutating_engine() {
    let engine = make_engine();
    let before = engine.runtime_configuration_view().snapshot();
    let command = ConfigCommand {
        request_id: Some("config-after-failure".into()),
        update: solaris_types::config::RuntimeConfigUpdate {
            model: Some("other-model".into()),
            thinking: Some("enabled".into()),
            thinking_budget: Some(2048),
            effort: Some("low".into()),
            compaction: Some("auto".into()),
            multi_agent_policy: Some(solaris_types::workflow::MultiAgentPolicy::Disabled),
            max_active_agents: Some(4),
        },
    };

    let outcome = rejected_config_outcome(&command, pending_control_rejection(OperationTerminal::Failed).unwrap());

    assert!(!outcome.applied);
    assert!(!outcome.changed);
    assert_eq!(outcome.results.len(), 7);
    assert_eq!(
        outcome.results.iter().map(|result| result.field).collect::<Vec<_>>(),
        vec![
            ConfigField::Model,
            ConfigField::Thinking,
            ConfigField::ThinkingBudget,
            ConfigField::Effort,
            ConfigField::Compaction,
            ConfigField::MultiAgentPolicy,
            ConfigField::MaxActiveAgents,
        ]
    );
    assert!(
        outcome
            .results
            .iter()
            .all(|result| result.status == ConfigFieldStatus::Rejected)
    );
    assert_eq!(engine.runtime_configuration_view().snapshot(), before);
}

#[test]
fn engine_and_workflow_failures_emit_safe_scoped_error_before_stream_end() {
    let output = RecordingOutput::default();

    emit_turn_failure(
        &output,
        "engine-message",
        ENGINE_FAILURE_CODE,
        ENGINE_FAILURE_MESSAGE,
        false,
    );
    output.emit_stream_end("engine-message", 0, 0, 0, 0, 0);
    emit_turn_failure(
        &output,
        "workflow-message",
        REQUIRED_WORKFLOW_FAILURE_CODE,
        REQUIRED_WORKFLOW_FAILURE_MESSAGE,
        false,
    );
    output.emit_stream_end("workflow-message", 0, 0, 0, 0, 0);

    assert_eq!(
        *output.events.lock().unwrap(),
        vec![
            RecordedEvent::Error {
                msg_id: "engine-message".into(),
                code: "engine_error".into(),
                message: "agent turn failed".into(),
                retryable: false,
            },
            RecordedEvent::End("engine-message".into()),
            RecordedEvent::Error {
                msg_id: "workflow-message".into(),
                code: "required_workflow_failed".into(),
                message: "required workflow failed".into(),
                retryable: false,
            },
            RecordedEvent::End("workflow-message".into()),
        ]
    );
}

#[test]
fn required_workflow_cancel_failure_keeps_running_projection() {
    let ledger = Arc::new(CancelFailLedger::new());
    let controller = WorkflowController::new(ledger.clone());
    let definition = solaris_agent::builtin_workflows::ultracode();
    let workflow_id = definition.id.clone();
    controller.register(definition).unwrap();
    let run_id = RunId::from("cancel-persistence-failure");
    controller
        .start(
            run_id.clone(),
            &workflow_id,
            serde_json::json!({"prompt": "test", "permission_mode": "auto"}),
        )
        .unwrap();
    ledger.fail_cancel.store(true, Ordering::SeqCst);

    let error = cancel_required_workflow(&controller, &run_id, "host_cancel").unwrap_err();

    assert!(error.contains("injected cancellation persistence failure"));
    assert_eq!(controller.snapshot(&run_id).unwrap().status, WorkflowRunStatus::Running);
    assert!(
        ledger
            .records_for_run(&run_id)
            .unwrap()
            .iter()
            .all(|record| record.record_type != "workflow_cancelled")
    );
}

#[tokio::test]
async fn ready_workflow_result_wins_over_simultaneously_ready_cancel() {
    for index in 0..64 {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        tx.send(ProtocolInput::Command(Box::new(ProtocolCommand::Cancel {
            request_id: Some(format!("cancel-{index}")),
            msg_id: "workflow-message".into(),
        })))
        .unwrap();
        let workflow = std::future::ready(index);
        tokio::pin!(workflow);

        let selected = wait_for_workflow_or_control(workflow.as_mut(), &mut rx).await;

        assert!(matches!(selected, WorkflowWait::Settled(value) if value == index));
        let ProtocolInput::Command(command) = rx.try_recv().unwrap() else {
            panic!("cancel command must remain queued");
        };
        assert!(matches!(*command, ProtocolCommand::Cancel { msg_id, .. } if msg_id == "workflow-message"));
    }
}

#[tokio::test]
async fn ready_engine_result_wins_over_simultaneously_ready_stop() {
    for index in 0..64 {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        tx.send(ProtocolInput::Command(Box::new(ProtocolCommand::Stop)))
            .unwrap();
        let engine = std::future::ready(index);
        tokio::pin!(engine);

        let selected = wait_for_engine_or_control(engine.as_mut(), &mut rx).await;

        assert!(matches!(selected, WorkflowWait::Settled(value) if value == index));
        assert!(matches!(
            rx.try_recv(),
            Ok(ProtocolInput::Command(command)) if *command == ProtocolCommand::Stop
        ));
    }
}

#[tokio::test]
async fn closed_command_channel_stops_pending_engine_immediately() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    drop(tx);
    let engine = std::future::pending::<()>();
    tokio::pin!(engine);

    let selected = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        wait_for_engine_or_control(engine.as_mut(), &mut rx),
    )
    .await
    .expect("a closed host command channel must stop the pending Engine");

    assert!(matches!(
        selected,
        WorkflowWait::Control(input)
            if matches!(input.as_ref(), ProtocolInput::Command(command) if command.as_ref() == &ProtocolCommand::Stop)
    ));
}

#[tokio::test]
async fn closed_command_channel_stops_pending_required_workflow_immediately() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    drop(tx);
    let workflow = std::future::pending::<()>();
    tokio::pin!(workflow);

    let selected = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        wait_for_workflow_or_control(workflow.as_mut(), &mut rx),
    )
    .await
    .expect("a closed host command channel must stop the pending required Workflow");

    assert!(matches!(
        selected,
        WorkflowWait::Control(input)
            if matches!(input.as_ref(), ProtocolInput::Command(command) if command.as_ref() == &ProtocolCommand::Stop)
    ));
}

#[test]
fn failed_agent_result_is_not_a_successful_operation_terminal() {
    let result = AgentResult {
        status: AgentOutcomeStatus::Failed,
        failure_class: Some(solaris_types::runtime::TaskFailureClass::MaxTurns),
        text: "fallback".into(),
        stop_reason: StopReason::MaxTurns,
        usage: TokenUsage::default(),
        turns: 1,
        failure_class: Some(solaris_types::runtime::TaskFailureClass::MaxTurns),
    };

    assert_eq!(operation_terminal_for_agent_result(&result), OperationTerminal::Failed);
    assert!(pending_control_rejection(operation_terminal_for_agent_result(&result)).is_some());
}

#[tokio::test]
async fn enter_plan_mode_shared_state_routes_next_message_to_standard_plan_workflow() {
    let mut engine = make_engine();
    engine.set_permission_mode(solaris_types::permission::PermissionMode::Auto);
    let plan_active = Arc::new(AtomicBool::new(false));
    engine.set_plan_active_flag(Arc::clone(&plan_active));
    let configuration = engine.runtime_configuration_view();
    let preset = solaris_agent::run_preset::resolve_run_preset(Intensity::High, engine.compat().effort_levels());
    let enter_plan_mode = EnterPlanModeTool::new(Arc::clone(&plan_active));

    assert!(required_workflow_for_message(&engine, &configuration, "inspect the project", &preset).is_none());
    assert!(!enter_plan_mode.execute(serde_json::json!({})).await.is_error);
    assert!(matches!(
        enter_plan_mode
            .context_modifier_for(&serde_json::json!({}))
            .and_then(|modifier| modifier.plan_mode_transition),
        Some(PlanModeTransition::Enter)
    ));
    plan_active.store(true, Ordering::Release);

    let route = required_workflow_for_message(&engine, &configuration, "inspect the project", &preset);

    assert_eq!(
        route.map(|(permission, workflow)| (permission, workflow.to_owned())),
        Some((
            solaris_types::permission::PermissionMode::Plan,
            solaris_agent::builtin_workflows::STANDARD_PLAN_WORKFLOW.to_owned(),
        ))
    );
    assert_eq!(
        engine.permission_mode(),
        solaris_types::permission::PermissionMode::Auto,
        "the legacy permission reader intentionally remains Auto"
    );
}

#[test]
fn required_workflow_cancellation_statuses_remain_distinct() {
    assert_eq!(
        classify_required_cancellation(Ok(WorkflowRunStatus::Cancelled)),
        RequiredCancellationDisposition::Cancelled
    );
    assert_eq!(
        classify_required_cancellation(Ok(WorkflowRunStatus::Completed)),
        RequiredCancellationDisposition::Completed
    );
    assert_eq!(
        classify_required_cancellation(Ok(WorkflowRunStatus::Failed)),
        RequiredCancellationDisposition::Failed
    );
    assert_eq!(
        classify_required_cancellation(Err("ledger failed".into())),
        RequiredCancellationDisposition::PersistenceFailed
    );
}
