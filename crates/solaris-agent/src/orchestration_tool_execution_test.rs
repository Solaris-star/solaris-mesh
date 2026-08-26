// -- truncate_result ------------------------------------------------------

#[test]
fn truncate_result_short_unchanged() {
    let s = "short content";
    assert_eq!(truncate_result(s, 1000), s);
}

#[test]
fn truncate_result_cjk_does_not_panic() {
    let cjk: String = "这是一段较长的中文内容用于测试截断功能".repeat(50);
    let result = truncate_result(&cjk, 100);
    assert!(result.contains("truncated"));
}

#[test]
fn truncate_result_mixed_cjk_ascii_does_not_panic() {
    let mixed = "Hello你好World世界Test测试".repeat(100);
    let result = truncate_result(&mixed, 200);
    assert!(result.contains("truncated"));
}

#[test]
fn truncate_result_handles_zero_and_one_character_limits() {
    assert_eq!(truncate_result("secret", 0), "");
    assert_eq!(truncate_result("secret", 1), "s");
}

// -- maybe_append_deferred_hint -------------------------------------------

#[test]
fn deferred_hint_appended_when_required_field_missing() {
    let schema = json!({
        "type": "object",
        "properties": { "tasks": { "type": "array" } },
        "required": ["tasks"]
    });
    let input = json!({});
    let result = maybe_append_deferred_hint("Missing or invalid 'tasks' array", schema, &input);
    assert!(result.contains("Missing or invalid 'tasks' array"));
    assert!(result.contains("ToolSearch"));
}

#[test]
fn deferred_hint_not_appended_when_required_fields_present() {
    let schema = json!({
        "type": "object",
        "properties": { "tasks": { "type": "array" } },
        "required": ["tasks"]
    });
    let input = json!({"tasks": [{"name": "t1", "prompt": "do x"}]});
    let result = maybe_append_deferred_hint("Some runtime error", schema, &input);
    assert_eq!(result, "Some runtime error");
    assert!(!result.contains("ToolSearch"));
}

#[test]
fn deferred_hint_not_appended_when_no_required_field() {
    let schema = json!({
        "type": "object",
        "properties": {}
    });
    let input = json!({});
    let result = maybe_append_deferred_hint("some error", schema, &input);
    assert_eq!(result, "some error");
}

#[test]
fn deferred_hint_not_appended_when_required_is_empty() {
    let schema = json!({
        "type": "object",
        "properties": {},
        "required": []
    });
    let input = json!({});
    let result = maybe_append_deferred_hint("some error", schema, &input);
    assert_eq!(result, "some error");
}

#[test]
fn deferred_hint_appended_for_partial_missing_fields() {
    let schema = json!({
        "type": "object",
        "properties": {
            "a": { "type": "string" },
            "b": { "type": "string" }
        },
        "required": ["a", "b"]
    });
    let input = json!({"a": "present"});
    let result = maybe_append_deferred_hint("validation failed", schema, &input);
    assert!(result.contains("ToolSearch"));
}

// -- execute_single integration tests (deferred tool hint) ----------------

use solaris_tools::Tool;
use solaris_tools::registry::ToolRegistry;

struct MockDeferredTool {
    schema: serde_json::Value,
}

#[async_trait::async_trait]
impl Tool for MockDeferredTool {
    fn name(&self) -> &str {
        "MockDeferred"
    }
    fn description(&self) -> &str {
        "A mock deferred tool for testing"
    }
    fn input_schema(&self) -> serde_json::Value {
        self.schema.clone()
    }
    fn is_concurrency_safe(&self, _input: &serde_json::Value) -> bool {
        true
    }
    fn is_deferred(&self) -> bool {
        true
    }
    async fn execute(&self, input: serde_json::Value) -> solaris_types::tool::ToolResult {
        if input.get("tasks").is_none() {
            return solaris_types::tool::ToolResult {
                content: "Missing or invalid 'tasks' array".to_string(),
                is_error: true,
            };
        }
        solaris_types::tool::ToolResult {
            content: "ok".to_string(),
            is_error: false,
        }
    }
    fn category(&self) -> solaris_protocol::events::ToolCategory {
        solaris_protocol::events::ToolCategory::Exec
    }
}

struct MockNonDeferredTool;

#[async_trait::async_trait]
impl Tool for MockNonDeferredTool {
    fn name(&self) -> &str {
        "MockNonDeferred"
    }
    fn description(&self) -> &str {
        "A mock non-deferred tool"
    }
    fn input_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": { "cmd": { "type": "string" } },
            "required": ["cmd"]
        })
    }
    fn is_concurrency_safe(&self, _input: &serde_json::Value) -> bool {
        true
    }
    async fn execute(&self, input: serde_json::Value) -> solaris_types::tool::ToolResult {
        if input.get("cmd").is_none() {
            return solaris_types::tool::ToolResult {
                content: "Missing cmd".to_string(),
                is_error: true,
            };
        }
        solaris_types::tool::ToolResult {
            content: "ok".to_string(),
            is_error: false,
        }
    }
    fn category(&self) -> solaris_protocol::events::ToolCategory {
        solaris_protocol::events::ToolCategory::Exec
    }
}

fn make_registry_with_deferred() -> ToolRegistry {
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(MockDeferredTool {
        schema: json!({
            "type": "object",
            "properties": { "tasks": { "type": "array" } },
            "required": ["tasks"]
        }),
    }));
    registry.register(Box::new(MockNonDeferredTool));
    registry
}

fn prepare_test_execution<'a>(registry: &'a ToolRegistry, call: &ContentBlock) -> PreparedToolExecution<'a> {
    let ContentBlock::ToolUse { id, name, input, .. } = call else {
        panic!("test call must be a tool use")
    };
    registry
        .get(name)
        .expect("test tool should be registered")
        .prepare_execution(input.clone(), solaris_tools::ToolExecutionContext::new(id))
        .expect("test tool preparation should succeed")
}

#[tokio::test]
async fn execute_single_deferred_tool_error_missing_required_appends_hint() {
    let registry = make_registry_with_deferred();
    let call = ContentBlock::ToolUse {
        id: "call_1".into(),
        name: "MockDeferred".into(),
        input: json!({}),
        extra: None,
    };
    let prepared = prepare_test_execution(&registry, &call);
    let (result, _, _, _) = execute_single(
        &registry,
        &call,
        prepared,
        None,
        solaris_compact::CompactLevel::Off,
        false,
    )
    .await;
    if let ContentBlock::ToolResult { content, is_error, .. } = &result {
        assert!(is_error);
        assert!(content.contains("Missing or invalid 'tasks' array"));
        assert!(content.contains("ToolSearch"));
    } else {
        panic!("expected ToolResult");
    }
}

#[tokio::test]
async fn execute_single_deferred_tool_error_with_required_present_no_hint() {
    let registry = make_registry_with_deferred();
    // tasks is present but wrong type — tool still fails, but required field exists
    let call = ContentBlock::ToolUse {
        id: "call_2".into(),
        name: "MockDeferred".into(),
        input: json!({"tasks": "not_an_array"}),
        extra: None,
    };
    let prepared = prepare_test_execution(&registry, &call);
    let (result, _, _, _) = execute_single(
        &registry,
        &call,
        prepared,
        None,
        solaris_compact::CompactLevel::Off,
        false,
    )
    .await;
    if let ContentBlock::ToolResult { content, is_error, .. } = &result {
        // Tool succeeds because input.get("tasks") is Some
        assert!(!is_error);
        assert!(!content.contains("ToolSearch"));
    } else {
        panic!("expected ToolResult");
    }
}

#[tokio::test]
async fn execute_single_deferred_tool_success_no_hint() {
    let registry = make_registry_with_deferred();
    let call = ContentBlock::ToolUse {
        id: "call_3".into(),
        name: "MockDeferred".into(),
        input: json!({"tasks": [{"name": "t1", "prompt": "do x"}]}),
        extra: None,
    };
    let prepared = prepare_test_execution(&registry, &call);
    let (result, _, _, _) = execute_single(
        &registry,
        &call,
        prepared,
        None,
        solaris_compact::CompactLevel::Off,
        false,
    )
    .await;
    if let ContentBlock::ToolResult { content, is_error, .. } = &result {
        assert!(!is_error);
        assert_eq!(content, "ok");
    } else {
        panic!("expected ToolResult");
    }
}

#[tokio::test]
async fn execute_single_non_deferred_tool_error_no_hint() {
    let registry = make_registry_with_deferred();
    let call = ContentBlock::ToolUse {
        id: "call_4".into(),
        name: "MockNonDeferred".into(),
        input: json!({}),
        extra: None,
    };
    let prepared = prepare_test_execution(&registry, &call);
    let (result, _, _, _) = execute_single(
        &registry,
        &call,
        prepared,
        None,
        solaris_compact::CompactLevel::Off,
        false,
    )
    .await;
    if let ContentBlock::ToolResult { content, is_error, .. } = &result {
        assert!(is_error);
        assert!(content.contains("Missing cmd"));
        assert!(!content.contains("ToolSearch"));
    } else {
        panic!("expected ToolResult");
    }
}

struct TimedSafeTool {
    active: Arc<std::sync::atomic::AtomicUsize>,
    maximum: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl Tool for TimedSafeTool {
    fn name(&self) -> &str {
        "TimedSafe"
    }

    fn description(&self) -> &str {
        "Test concurrency scheduling"
    }

    fn input_schema(&self) -> serde_json::Value {
        json!({"type": "object"})
    }

    fn is_concurrency_safe(&self, _input: &serde_json::Value) -> bool {
        true
    }

    async fn execute(&self, input: serde_json::Value) -> solaris_types::tool::ToolResult {
        use std::sync::atomic::Ordering;

        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.maximum.fetch_max(active, Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        self.active.fetch_sub(1, Ordering::SeqCst);
        let is_error = input.get("fail").and_then(serde_json::Value::as_bool).unwrap_or(false);
        solaris_types::tool::ToolResult {
            content: if is_error { "failed".into() } else { "ok".into() },
            is_error,
        }
    }

    fn describe_effect(&self, _input: &serde_json::Value) -> EffectDescriptor {
        EffectDescriptor {
            class: EffectClass::ReadOnly,
            action: "timed safe read".into(),
            resources: ResourceFootprint::default(),
            replay_policy: EffectReplayPolicy::ReplaySafe,
        }
    }

    fn category(&self) -> solaris_protocol::events::ToolCategory {
        solaris_protocol::events::ToolCategory::Exec
    }
}

#[tokio::test]
async fn policy_path_runs_safe_calls_concurrently_and_preserves_order() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(TimedSafeTool {
        active,
        maximum: Arc::clone(&maximum),
    }));
    let context = EffectExecutionContext::new(
        RunId::from("safe-concurrency-run"),
        AgentId::from("agent"),
        Arc::new(InMemoryRuntimeLedger::default()),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let calls = vec![
        ContentBlock::ToolUse {
            id: "first".into(),
            name: "TimedSafe".into(),
            input: json!({"fail": true}),
            extra: None,
        },
        ContentBlock::ToolUse {
            id: "second".into(),
            name: "TimedSafe".into(),
            input: json!({}),
            extra: None,
        },
    ];
    let confirmer = Arc::new(Mutex::new(ToolConfirmer::new(true, Vec::new())));

    let outcome = execute_tool_calls_with_policy_context(
        &registry,
        &calls,
        &confirmer,
        PermissionMode::Bypass,
        PermissionCeiling::unrestricted(),
        &context,
        None,
        solaris_compact::CompactLevel::Off,
        false,
    )
    .await
    .unwrap();

    assert_eq!(maximum.load(Ordering::SeqCst), 2);
    assert!(
        matches!(&outcome.results[0], ContentBlock::ToolResult { tool_use_id, is_error: true, .. } if tool_use_id == "first")
    );
    assert!(
        matches!(&outcome.results[1], ContentBlock::ToolResult { tool_use_id, is_error: false, .. } if tool_use_id == "second")
    );
}

struct BlockingSecondPreHook {
    calls: std::sync::atomic::AtomicUsize,
    second_entered: tokio::sync::Semaphore,
}

#[async_trait::async_trait]
impl HookExecutor for BlockingSecondPreHook {
    async fn execute(&self, _invocation: HookInvocation) -> Result<HookExecutionResult, HookError> {
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1 {
            self.second_entered.add_permits(1);
            std::future::pending::<()>().await;
        }
        Ok(HookExecutionResult {
            success: true,
            output: String::new(),
        })
    }
}

#[tokio::test]
async fn cancellation_during_second_safe_pre_hook_clears_first_registration() {
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(TimedSafeTool {
        active: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        maximum: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
    }));
    let run_id = RunId::from("safe-pre-hook-cancel-run");
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let context = EffectExecutionContext::new(
        run_id.clone(),
        AgentId::from("agent"),
        ledger.clone(),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    );
    let calls = vec![
        ContentBlock::ToolUse {
            id: "first-safe".into(),
            name: "TimedSafe".into(),
            input: json!({}),
            extra: None,
        },
        ContentBlock::ToolUse {
            id: "second-safe".into(),
            name: "TimedSafe".into(),
            input: json!({}),
            extra: None,
        },
    ];
    let blocker = Arc::new(BlockingSecondPreHook {
        calls: std::sync::atomic::AtomicUsize::new(0),
        second_entered: tokio::sync::Semaphore::new(0),
    });
    let mut hooks = HookEngine::new(
        HooksConfig {
            pre_tool_use: vec![HookDef {
                name: "blocking-second".into(),
                tool_match: vec!["TimedSafe".into()],
                file_match: Vec::new(),
                command: "ignored".into(),
                timeout_ms: 30_000,
                network: Default::default(),
            }],
            ..Default::default()
        },
        std::env::temp_dir(),
    );
    hooks.set_executor(blocker.clone());
    let task_context = context.clone();
    let confirmer = Arc::new(Mutex::new(ToolConfirmer::new(true, Vec::new())));
    let task = tokio::spawn(async move {
        execute_tool_calls_with_policy_context(
            &registry,
            &calls,
            &confirmer,
            PermissionMode::Bypass,
            PermissionCeiling::unrestricted(),
            &task_context,
            Some(&mut hooks),
            solaris_compact::CompactLevel::Off,
            false,
        )
        .await
    });

    let permit = tokio::time::timeout(std::time::Duration::from_secs(2), blocker.second_entered.acquire())
        .await
        .unwrap()
        .unwrap();
    permit.forget();
    assert!(context.has_approved_request_for_call("first-safe"));
    task.abort();
    let _ = task.await;

    assert!(!context.has_approved_request_for_call("first-safe"));
    assert!(!context.has_approved_request_for_call("second-safe"));
    assert!(
        ledger
            .records_for_run(&run_id)
            .unwrap()
            .iter()
            .all(|record| record.record_type != "effect_intent")
    );
}

#[tokio::test]
async fn cancellation_while_waiting_for_effect_permit_leaves_no_approved_request() {
    use solaris_types::resource::ResourceBudget;

    use crate::resource_manager::ResourceManager;

    let resources = ResourceManager::new(ResourceBudget {
        max_concurrent_effects: Some(1),
        ..Default::default()
    });
    let held = resources.acquire_effect().await.unwrap();
    let ledger = Arc::new(InMemoryRuntimeLedger::default());
    let run_id = RunId::from("cancelled-permit-wait-run");
    let context = EffectExecutionContext::new(
        run_id.clone(),
        AgentId::from("agent"),
        ledger.clone(),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot::default(),
    )
    .with_resource_manager(Arc::clone(&resources));
    let mut registry = ToolRegistry::new();
    registry.register(Box::new(MockNonDeferredTool));
    let call = ContentBlock::ToolUse {
        id: "waiting-call".into(),
        name: "MockNonDeferred".into(),
        input: json!({"cmd": "ok"}),
        extra: None,
    };
    let ContentBlock::ToolUse { id, name, input, .. } = &call else {
        unreachable!()
    };
    let prepared_effect = registry
        .get(name)
        .unwrap()
        .prepare_effect(context.effect_id_for_call(id).as_str(), input)
        .unwrap();
    let (descriptor, tool_execution) = prepared_effect.into_parts();
    let request = context.effect_request(id, name, input, descriptor);
    let registration = context.remember_approved_request_with_tool_context(
        request,
        tool_execution,
        PermissionMode::Bypass,
        PermissionCeiling::unrestricted(),
    );
    let task_context = context.clone();
    let task = tokio::spawn(async move {
        execute_single_with_effect_context(
            &registry,
            &call,
            None,
            &task_context,
            solaris_compact::CompactLevel::Off,
            false,
            Some(registration),
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while context.has_approved_request_for_call("waiting-call") {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    task.abort();
    let _ = task.await;
    drop(held);

    assert!(!context.has_approved_request_for_call("waiting-call"));
    assert!(context.take_approved_request_for_call("waiting-call").is_none());
    assert!(
        !ledger
            .records_for_run(&run_id)
            .unwrap()
            .iter()
            .any(|record| record.record_type == "effect_intent")
    );
}
