use super::*;
use crate::execution_context::EffectExecutionContext;
use crate::permission_engine::PermissionContext;
use crate::plugin_runtime::PluginRuntime;
use crate::plugin_tool::PluginContributionDispatcher;
use crate::runtime_ledger::InMemoryRuntimeLedger;
use solaris_types::identity::{AgentId, RunId};
use solaris_types::message::{StopReason, TokenUsage};
use solaris_types::permission::{PermissionCeiling, PermissionMode};
use solaris_types::plugin::{
    ImplementationIdentity, PluginCapabilities, PluginCommandContributionDefinition, PluginCompatibility,
    PluginDefinition, PluginProviderCommandResponse, PluginProviderEvent, PluginResources, PluginScope, PluginSource,
    ResolvedPluginDefinition, ResolvedPluginIdentity,
};
use solaris_types::runtime::OperationEnvironmentSnapshot;
use solaris_types::tool::ToolDef;
use std::sync::Mutex;

struct RecordingInvoker {
    input: Mutex<Option<Value>>,
    output: Mutex<Option<Result<Value, String>>>,
}

#[async_trait]
impl PluginProviderInvoker for RecordingInvoker {
    async fn invoke(&self, provider_name: &str, input: Value) -> Result<Value, String> {
        assert_eq!(provider_name, "fixture");
        *self.input.lock().unwrap() = Some(input);
        self.output.lock().unwrap().take().unwrap()
    }
}

fn request() -> LlmRequest {
    LlmRequest {
        model: "plugin-model".to_owned(),
        system: "system".to_owned(),
        messages: Vec::new(),
        tools: vec![ToolDef {
            name: "Read".to_owned(),
            description: "Read a file".to_owned(),
            input_schema: serde_json::json!({"type": "object"}),
            deferred: false,
        }],
        max_tokens: Some(32),
        thinking: None,
        reasoning_effort: None,
    }
}

#[tokio::test]
async fn adapts_provider_command_events_to_llm_stream() {
    let response = PluginProviderCommandResponse {
        events: vec![
            PluginProviderEvent::TextDelta {
                text: "hello".to_owned(),
            },
            PluginProviderEvent::Done {
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage {
                    input_tokens: 4,
                    output_tokens: 1,
                    ..Default::default()
                },
            },
        ],
    };
    let invoker = Arc::new(RecordingInvoker {
        input: Mutex::new(None),
        output: Mutex::new(Some(Ok(serde_json::to_value(response).unwrap()))),
    });
    let provider = PluginLlmProvider::with_invoker("fixture", invoker.clone());

    let mut stream = provider.stream(&request()).await.unwrap();
    assert!(matches!(stream.recv().await, Some(LlmEvent::TextDelta(text)) if text == "hello"));
    assert!(matches!(
        stream.recv().await,
        Some(LlmEvent::Done {
            stop_reason: StopReason::EndTurn,
            usage,
        }) if usage.input_tokens == 4 && usage.output_tokens == 1
    ));
    assert!(stream.recv().await.is_none());

    let input = invoker.input.lock().unwrap().clone().unwrap();
    assert_eq!(input["protocol"], PluginProviderCommandRequest::PROTOCOL);
    assert_eq!(input["request"]["model"], "plugin-model");
    assert_eq!(input["request"]["tools"][0]["name"], "Read");
}

#[tokio::test]
async fn rejects_non_terminal_or_misordered_provider_events() {
    for response in [
        PluginProviderCommandResponse {
            events: vec![PluginProviderEvent::TextDelta {
                text: "unfinished".to_owned(),
            }],
        },
        PluginProviderCommandResponse {
            events: vec![
                PluginProviderEvent::Done {
                    stop_reason: StopReason::EndTurn,
                    usage: TokenUsage::default(),
                },
                PluginProviderEvent::TextDelta {
                    text: "late".to_owned(),
                },
            ],
        },
    ] {
        let invoker = Arc::new(RecordingInvoker {
            input: Mutex::new(None),
            output: Mutex::new(Some(Ok(serde_json::to_value(response).unwrap()))),
        });
        let provider = PluginLlmProvider::with_invoker("fixture", invoker);
        let error = provider.stream(&request()).await.err().unwrap();
        assert!(matches!(error, ProviderError::Parse(_)));
    }
}

#[tokio::test]
async fn invocation_failure_does_not_expose_plugin_stderr() {
    let secret = "provider-secret-do-not-expose";
    let invoker = Arc::new(RecordingInvoker {
        input: Mutex::new(None),
        output: Mutex::new(Some(Err(secret.to_owned()))),
    });
    let provider = PluginLlmProvider::with_invoker("fixture", invoker);
    let error = provider.stream(&request()).await.err().unwrap();
    assert!(!error.to_string().contains(secret));
}

#[tokio::test]
async fn activated_command_provider_executes_through_llm_provider_trait() {
    let shell = solaris_config::shell::default_shell();
    let executable = shell.path.canonicalize().unwrap();
    let authority = executable.parent().unwrap().to_path_buf();
    let response = serde_json::to_string(&PluginProviderCommandResponse {
        events: vec![
            PluginProviderEvent::TextDelta {
                text: r#"{"ok":true}"#.to_owned(),
            },
            PluginProviderEvent::Done {
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage {
                    input_tokens: 2,
                    output_tokens: 1,
                    ..Default::default()
                },
            },
        ],
    })
    .unwrap();
    #[cfg(windows)]
    let script = format!("Write-Output '{}'", response.replace('\'', "''"));
    #[cfg(not(windows))]
    let script = format!("printf '%s\\n' '{}'", response.replace('\'', "'\\''"));
    let args = shell.derive_exec_args(&script, false);
    let implementation = ImplementationIdentity {
        implementation_id: "plugin:provider-fixture".to_owned(),
        version: Some("1".to_owned()),
        digest: Some("fixture-digest".to_owned()),
    };
    let resolved = ResolvedPluginDefinition {
        definition: PluginDefinition {
            id: "provider-fixture".to_owned(),
            version: "1".to_owned(),
            source: PluginSource::HostBundled {
                id: "provider-fixture".to_owned(),
            },
            materialized_path: None,
            capabilities: PluginCapabilities {
                providers: vec!["fixture".to_owned()],
                ..Default::default()
            },
            compatibility: PluginCompatibility {
                runtime_api_version: Some(1),
                required_protocols: vec![PluginProviderCommandRequest::PROTOCOL.to_owned()],
            },
            requested_paths: Vec::new(),
            requires_services: Vec::new(),
            resources: PluginResources::default(),
            command_tools: Vec::new(),
            command_contributions: vec![PluginCommandContributionDefinition {
                kind: PluginContributionKind::Provider,
                name: "fixture".to_owned(),
                command: executable.file_name().unwrap().to_string_lossy().into_owned(),
                args,
                input_schema: serde_json::json!({"type": "object"}),
                max_result_size: 16 * 1024,
                timeout_ms: 5_000,
            }],
        },
        identity: ResolvedPluginIdentity {
            plugin_id: "provider-fixture".to_owned(),
            source: PluginSource::HostBundled {
                id: "provider-fixture".to_owned(),
            },
            implementation: implementation.clone(),
        },
        authority_root: Some(authority.to_string_lossy().into_owned()),
    };
    let runtime = Arc::new(PluginRuntime::default());
    let run_id = RunId::from("plugin-provider-e2e");
    runtime.initialize_scope_tree("workspace", run_id.as_str(), "root");
    runtime.install_checked(resolved).unwrap();
    runtime
        .activate(
            "provider-activation",
            PluginScope::Run {
                run_id: run_id.to_string(),
            },
            "provider-fixture",
        )
        .unwrap();
    let context = EffectExecutionContext::new(
        run_id.clone(),
        AgentId::from("root"),
        Arc::new(InMemoryRuntimeLedger::default()),
        PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted()),
        OperationEnvironmentSnapshot {
            plugins: vec![implementation],
            ..Default::default()
        },
    );
    let dispatcher = Arc::new(PluginContributionDispatcher::new(
        runtime,
        context,
        format!("run:{run_id}"),
    ));
    let provider = PluginLlmProvider::new("fixture", dispatcher).unwrap();

    let mut stream = provider.stream(&request()).await.unwrap();
    assert!(matches!(stream.recv().await, Some(LlmEvent::TextDelta(text)) if text == r#"{"ok":true}"#));
    assert!(matches!(stream.recv().await, Some(LlmEvent::Done { .. })));
}
