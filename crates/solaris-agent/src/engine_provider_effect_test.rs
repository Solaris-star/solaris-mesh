use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use solaris_config::config::{CliArgs, Config};
use solaris_providers::error::ProviderError;
use solaris_providers::provider::LlmProvider;
use solaris_tools::registry::ToolRegistry;
use solaris_types::identity::{AgentId, RunId};
use solaris_types::llm::{LlmEvent, LlmRequest};
use solaris_types::permission::{ExecutionBoundary, PermissionCeiling, PermissionMode};
use solaris_types::runtime::OperationEnvironmentSnapshot;

use crate::engine::AgentEngine;
use crate::execution_context::EffectExecutionContext;
use crate::output::null_sink::NullSink;
use crate::permission_engine::PermissionContext;
use crate::runtime_ledger::InMemoryRuntimeLedger;
use crate::turn::TurnKind;

struct CountingProvider(AtomicUsize);

#[async_trait]
impl LlmProvider for CountingProvider {
    async fn stream(&self, _request: &LlmRequest) -> Result<tokio::sync::mpsc::Receiver<LlmEvent>, ProviderError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        Ok(rx)
    }
}

#[tokio::test]
async fn bypass_allows_provider_network_call_outside_boundary() {
    let config = Config::resolve(&CliArgs {
        provider: Some("anthropic".into()),
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
    let provider = Arc::new(CountingProvider(AtomicUsize::new(0)));
    let mut engine = AgentEngine::new_with_provider(
        provider.clone(),
        config,
        ToolRegistry::new(),
        Arc::new(NullSink),
        std::path::PathBuf::from("workspace"),
    );
    let permissions = PermissionContext::new(PermissionMode::Bypass, PermissionCeiling::unrestricted());
    permissions.set_boundary(ExecutionBoundary::workspace("workspace"));
    engine.set_execution_context(EffectExecutionContext::new(
        RunId::from("provider-denied-run"),
        AgentId::from("root"),
        Arc::new(InMemoryRuntimeLedger::default()),
        permissions,
        OperationEnvironmentSnapshot::default(),
    ));

    let result = engine.run_turn(TurnKind::Normal).await;

    assert!(result.is_ok());
    assert_eq!(provider.0.load(Ordering::SeqCst), 1);
}
