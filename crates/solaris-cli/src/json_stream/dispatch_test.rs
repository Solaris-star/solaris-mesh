use std::collections::BTreeMap;
use std::sync::Mutex;

use serde_json::{Value, json};
use solaris_agent::output::OutputSink;
use solaris_agent::runtime_ledger::{InMemoryRuntimeLedger, LedgerRecord, RuntimeLedger};

use super::*;

#[derive(Default)]
struct SequenceSink {
    events: Mutex<Vec<String>>,
}

impl SequenceSink {
    fn events(&self) -> Vec<String> {
        self.events.lock().unwrap().clone()
    }
}

impl OutputSink for SequenceSink {
    fn emit_text_delta(&self, text: &str, msg_id: &str) {
        self.events.lock().unwrap().push(format!("delta:{msg_id}:{text}"));
    }

    fn emit_thinking(&self, _text: &str, _msg_id: &str) {}

    fn emit_tool_call(&self, _tool_use_id: &str, _name: &str, _input: &str) {}

    fn emit_tool_result(&self, _tool_use_id: &str, _name: &str, _is_error: bool, _content: &str) {}

    fn emit_stream_start(&self, msg_id: &str) {
        self.events.lock().unwrap().push(format!("start:{msg_id}"));
    }

    fn emit_stream_end(
        &self,
        msg_id: &str,
        _turns: usize,
        _input_tokens: u64,
        _output_tokens: u64,
        _cache_creation_tokens: u64,
        _cache_read_tokens: u64,
    ) {
        self.events.lock().unwrap().push(format!("end:{msg_id}"));
    }

    fn emit_error(&self, _msg: &str) {}

    fn emit_info(&self, _msg: &str) {}
}

#[test]
fn restored_running_required_workflow_emits_one_stream_start() {
    let sink = SequenceSink::default();

    ensure_required_workflow_stream_started(&sink, "message-1", false);
    emit_required_workflow_text(&sink, "message-1", &json!("done"), true);
    sink.emit_stream_end("message-1", 0, 0, 0, 0, 0);

    assert_eq!(
        sink.events(),
        vec!["start:message-1", "delta:message-1:done", "end:message-1"]
    );
}

#[test]
fn restored_workflow_requires_the_same_durable_provider_and_model() {
    let mut snapshot = WorkflowRunSnapshot {
        run_id: RunId::from("restored-runtime-identity"),
        workflow_id: "workflow-a".to_owned(),
        workflow_version: "1".to_owned(),
        workflow_definition_digest: Some("definition-digest".to_owned()),
        input_digest: Some("input-digest".to_owned()),
        runtime_identity: Some(WorkflowRuntimeIdentity::new("provider-a", "model-a")),
        parent_run_id: None,
        status: WorkflowRunStatus::Running,
        reconciliation_reason: None,
        nodes: BTreeMap::new(),
        parameters: json!({}),
    };

    assert!(validate_workflow_runtime_identity(&snapshot, "provider-a", "model-a").is_ok());
    assert!(
        validate_workflow_runtime_identity(&snapshot, "provider-a", "model-b")
            .unwrap_err()
            .contains("different provider or model")
    );

    snapshot.runtime_identity = None;
    assert!(
        validate_workflow_runtime_identity(&snapshot, "provider-a", "model-a")
            .unwrap_err()
            .contains("no durable provider or model identity")
    );
}

#[test]
fn run_workflow_failure_includes_negative_command_result() {
    let events = run_workflow_failure_events("request-1", "safe failure");

    assert!(events.iter().any(|event| {
        matches!(
            event,
            ProtocolEvent::CommandResult {
                request_id,
                command,
                applied: false,
                message: Some(message),
                ..
            } if request_id == "request-1" && command == "run_workflow" && message == "safe failure"
        )
    }));
}

#[tokio::test]
async fn fast_workflow_cannot_finish_before_abort_handle_is_installed() {
    let registry = DetachedWorkflowRegistry::default();
    let run_id = RunId::from("fast-workflow");
    let _cancel_rx = registry.register(run_id.clone()).unwrap();
    let (start_tx, start_rx) = tokio::sync::oneshot::channel();
    let task_registry = registry.clone();
    let task_run_id = run_id.clone();
    let handle = tokio::spawn(async move {
        start_rx.await.unwrap();
        task_registry.finish(&task_run_id);
    });

    let result = activate_workflow_execution(&registry, &run_id, &handle, start_tx);

    assert!(result.is_ok(), "fast workflow was reported as failed: {result:?}");
    handle.await.unwrap();
    assert!(!registry.is_active(&run_id));
}

#[test]
fn required_workflow_delivery_marker_is_idempotent() {
    let ledger = InMemoryRuntimeLedger::default();
    let run_id = RunId::from("required-marker-run");

    record_required_workflow_turn_emitted_in(&ledger, &run_id, "message-1").unwrap();
    record_required_workflow_turn_emitted_in(&ledger, &run_id, "message-1").unwrap();

    assert!(required_workflow_turn_was_emitted_in(&ledger, &run_id, "message-1").unwrap());
    assert!(!required_workflow_turn_was_emitted_in(&ledger, &run_id, "message-2").unwrap());
    assert_eq!(ledger.records_for_run(&run_id).unwrap().len(), 1);
}

#[test]
fn committed_required_workflow_output_always_records_delivery_marker() {
    let sink = SequenceSink::default();
    let ledger = InMemoryRuntimeLedger::default();
    let run_id = RunId::from("normal-required-workflow");

    emit_and_record_required_workflow_turn_in(
        &sink,
        &ledger,
        &run_id,
        "message-1",
        &json!({"status": "completed"}),
        true,
    )
    .unwrap();

    assert_eq!(sink.events(), vec!["delta:message-1:{\n  \"status\": \"completed\"\n}"]);
    assert!(required_workflow_turn_was_emitted_in(&ledger, &run_id, "message-1").unwrap());
}

struct FailingReadLedger;

impl RuntimeLedger for FailingReadLedger {
    fn logical_append_capability(&self) -> solaris_agent::runtime_ledger::LogicalAppendCapability {
        solaris_agent::runtime_ledger::LogicalAppendCapability::Unsupported
    }

    fn acquire_workflow_mutation_lease(
        &self,
        _run_id: &RunId,
        _owner_id: &str,
        _now_unix_ms: i64,
    ) -> std::io::Result<solaris_agent::runtime_ledger::WorkflowMutationLease> {
        Err(std::io::Error::other("lease unavailable"))
    }

    fn renew_workflow_mutation_lease(
        &self,
        _lease: &solaris_agent::runtime_ledger::WorkflowMutationLease,
        _now_unix_ms: i64,
    ) -> std::io::Result<solaris_agent::runtime_ledger::WorkflowMutationLease> {
        Err(std::io::Error::other("lease unavailable"))
    }

    fn commit_workflow_restore(
        &self,
        _lease: &solaris_agent::runtime_ledger::WorkflowMutationLease,
        _expected_sequence: u64,
        _now_unix_ms: i64,
    ) -> std::io::Result<solaris_agent::runtime_ledger::WorkflowRestoreCommit> {
        Err(std::io::Error::other("restore unavailable"))
    }

    fn release_workflow_mutation_lease(
        &self,
        _lease: &solaris_agent::runtime_ledger::WorkflowMutationLease,
    ) -> std::io::Result<()> {
        Err(std::io::Error::other("lease unavailable"))
    }

    fn append(
        &self,
        _run_id: &RunId,
        _durability: DurabilityClass,
        _record_type: &str,
        _payload: Value,
    ) -> std::io::Result<LedgerRecord> {
        Err(std::io::Error::other("append unavailable"))
    }

    fn append_under_workflow_lease(
        &self,
        _lease: &solaris_agent::runtime_ledger::WorkflowMutationLease,
        _now_unix_ms: i64,
        _durability: DurabilityClass,
        _record_type: &str,
        _payload: Value,
    ) -> std::io::Result<LedgerRecord> {
        Err(std::io::Error::other("append unavailable"))
    }

    fn compare_and_append(
        &self,
        _run_id: &RunId,
        _durability: DurabilityClass,
        _record_type: &str,
        _identity_fields: &[&str],
        _payload: Value,
    ) -> std::io::Result<LedgerRecord> {
        Err(std::io::Error::other("append unavailable"))
    }

    fn compare_and_append_under_workflow_lease(
        &self,
        _lease: &solaris_agent::runtime_ledger::WorkflowMutationLease,
        _now_unix_ms: i64,
        _durability: DurabilityClass,
        _record_type: &str,
        _identity_fields: &[&str],
        _payload: Value,
    ) -> std::io::Result<LedgerRecord> {
        Err(std::io::Error::other("append unavailable"))
    }

    fn run_ids(&self) -> std::io::Result<Vec<RunId>> {
        Err(std::io::Error::other("read unavailable"))
    }

    fn records_for_run(&self, _run_id: &RunId) -> std::io::Result<Vec<LedgerRecord>> {
        Err(std::io::Error::other("read unavailable"))
    }
}

#[test]
fn delivery_marker_read_error_is_not_treated_as_not_emitted() {
    let run_id = RunId::from("required-marker-read-error");

    let read_error = required_workflow_turn_was_emitted_in(&FailingReadLedger, &run_id, "message-1")
        .expect_err("ledger failure must be reported");
    let record_error = record_required_workflow_turn_emitted_in(&FailingReadLedger, &run_id, "message-1")
        .expect_err("marker write must not proceed after an uncertain read");

    assert!(read_error.contains("failed to inspect"));
    assert!(record_error.contains("failed to inspect"));
}

fn runtime_journal_page(
    ledger: &dyn RuntimeLedger,
    run_id: &RunId,
    after_sequence: u64,
    limit: usize,
) -> (u64, Vec<u64>, bool) {
    let event = build_runtime_journal_event(
        "journal-request".to_owned(),
        run_id.clone(),
        after_sequence,
        Some(limit),
        ledger,
    )
    .unwrap();

    match event {
        ProtocolEvent::RuntimeJournal {
            last_sequence,
            records,
            truncated,
            ..
        } => (
            last_sequence,
            records.into_iter().map(|record| record.sequence).collect(),
            truncated,
        ),
        event => panic!("expected runtime journal event, got {event:?}"),
    }
}

fn append_journal_record(ledger: &dyn RuntimeLedger, run_id: &RunId, record_type: &str) {
    ledger
        .append(run_id, DurabilityClass::AsyncDurable, record_type, json!({}))
        .unwrap();
}

#[test]
fn runtime_journal_cursor_follows_the_last_record_in_each_page() {
    let ledger = InMemoryRuntimeLedger::default();
    let run_id = RunId::from("runtime-journal-pagination");
    for index in 1..=5 {
        append_journal_record(&ledger, &run_id, &format!("record-{index}"));
    }

    let first = runtime_journal_page(&ledger, &run_id, 0, 2);
    assert_eq!(first, (2, vec![1, 2], true));

    // A record appended after the first read must be available from that page's cursor.
    append_journal_record(&ledger, &run_id, "record-6");
    let second = runtime_journal_page(&ledger, &run_id, first.0, 2);
    assert_eq!(second, (4, vec![3, 4], true));

    let third = runtime_journal_page(&ledger, &run_id, second.0, 2);
    assert_eq!(third, (6, vec![5, 6], false));
}

#[test]
fn empty_runtime_journal_page_keeps_the_callers_cursor() {
    let ledger = InMemoryRuntimeLedger::default();
    let run_id = RunId::from("empty-runtime-journal");

    assert_eq!(runtime_journal_page(&ledger, &run_id, 42, 2), (42, vec![], false));
}
