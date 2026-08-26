use std::sync::Arc;

use solaris_agent::bootstrap::AgentBootstrap;
use solaris_agent::output::null_sink::NullSink;
use solaris_config::compat::ProviderCompat;
use solaris_config::config::{Config, ProviderType};

fn minimal_config() -> Config {
    Config {
        provider_label: "openai".into(),
        provider: ProviderType::OpenAI,
        api_key: "sk-test".into(),
        base_url: "http://localhost:0".into(),
        model: "gpt-test-model".into(),
        max_tokens: Some(1024),
        max_turns: Some(5),
        max_tool_call_malformed_turns: Some(3),
        max_tool_call_failure_turns: Some(3),
        system_prompt: None,
        thinking: None,
        prompt_caching: false,
        compat: ProviderCompat::openai_defaults(),
        tools: Default::default(),
        session: Default::default(),
        memory: Default::default(),
        compact: Default::default(),
        plan: Default::default(),
        shell: Default::default(),
        file_cache: Default::default(),
        hooks: Default::default(),
        bedrock: None,
        vertex: None,
        mcp: Default::default(),
        logging: Default::default(),
        multi_agent: Default::default(),
    }
}

struct IsolatedRuntime {
    _root: tempfile::TempDir,
    workspace: String,
    config: Config,
}

fn isolated_runtime() -> IsolatedRuntime {
    let root = tempfile::tempdir().expect("create isolated bootstrap root");
    let workspace = root.path().join("workspace");
    std::fs::create_dir_all(&workspace).expect("create isolated workspace");

    let mut config = minimal_config();
    config.session.directory = root
        .path()
        .join("state")
        .join("sessions")
        .to_string_lossy()
        .into_owned();

    IsolatedRuntime {
        _root: root,
        workspace: workspace.to_string_lossy().into_owned(),
        config,
    }
}

fn null_output() -> Arc<dyn solaris_agent::output::OutputSink> {
    Arc::new(NullSink)
}

#[tokio::test]
async fn bootstrap_builds_engine_with_model_in_prompt() {
    let runtime = isolated_runtime();
    let result = AgentBootstrap::new(runtime.config, &runtime.workspace, null_output())
        .build()
        .await
        .expect("bootstrap should succeed");

    assert!(!result.engine.tool_names().is_empty());
    assert!(!result.has_mcp);
    assert!(result.mcp_managers.is_empty());
}

#[tokio::test]
async fn bootstrap_registers_all_expected_tools() {
    let runtime = isolated_runtime();
    let result = AgentBootstrap::new(runtime.config, &runtime.workspace, null_output())
        .build()
        .await
        .unwrap();

    let names = result.engine.tool_names();

    for expected in &["Read", "Write", "Edit", "ExecCommand", "Grep", "Glob"] {
        assert!(names.iter().any(|n| n == expected), "missing built-in tool: {expected}");
    }

    assert!(names.iter().any(|n| n == "Skill"), "SkillTool should be registered");
    assert!(names.iter().any(|n| n == "Spawn"), "SpawnTool should be registered");
    assert!(
        names.iter().any(|n| n == "ToolSearch"),
        "ToolSearchTool should be registered"
    );
}

#[tokio::test]
async fn bootstrap_plan_tools_when_enabled() {
    let mut runtime = isolated_runtime();
    runtime.config.plan.enabled = true;

    let result = AgentBootstrap::new(runtime.config, &runtime.workspace, null_output())
        .build()
        .await
        .unwrap();

    let names = result.engine.tool_names();
    assert!(
        names.iter().any(|n| n == "EnterPlanMode"),
        "EnterPlanMode should be registered when plan.enabled"
    );
    assert!(
        names.iter().any(|n| n == "ExitPlanMode"),
        "ExitPlanMode should be registered when plan.enabled"
    );
}

#[tokio::test]
async fn bootstrap_no_plan_tools_when_disabled() {
    let mut runtime = isolated_runtime();
    runtime.config.plan.enabled = false;

    let result = AgentBootstrap::new(runtime.config, &runtime.workspace, null_output())
        .build()
        .await
        .unwrap();

    let names = result.engine.tool_names();
    assert!(
        !names.iter().any(|n| n == "EnterPlanMode"),
        "EnterPlanMode should NOT be registered when plan.disabled"
    );
}

#[tokio::test]
async fn bootstrap_no_mcp_when_no_servers() {
    let runtime = isolated_runtime();
    let result = AgentBootstrap::new(runtime.config, &runtime.workspace, null_output())
        .build()
        .await
        .unwrap();

    assert!(!result.has_mcp);
    assert!(result.mcp_managers.is_empty());
}

#[tokio::test]
async fn bootstrap_with_custom_system_prompt() {
    let mut runtime = isolated_runtime();
    runtime.config.system_prompt = Some("You are a pirate assistant.".into());

    let _result = AgentBootstrap::new(runtime.config, &runtime.workspace, null_output())
        .build()
        .await
        .unwrap();
}

#[tokio::test]
async fn bootstrap_with_agents_md_in_workspace() {
    let tmp = tempfile::TempDir::new().unwrap();
    let workspace = tmp.path();
    std::fs::write(workspace.join("AGENTS.md"), "PROJECT_RULES_MARKER").unwrap();

    let mut config = minimal_config();
    config.session.directory = tmp.path().join("state").join("sessions").to_string_lossy().into_owned();
    let _result = AgentBootstrap::new(config, workspace.to_string_lossy().as_ref(), null_output())
        .build()
        .await
        .unwrap();
}

#[tokio::test]
async fn bootstrap_config_accessor_returns_config() {
    let config = minimal_config();
    let bootstrap = AgentBootstrap::new(config, "/tmp/ws", null_output());
    assert_eq!(bootstrap.config().model, "gpt-test-model");
    assert_eq!(bootstrap.config().max_tokens, Some(1024));
}

#[tokio::test]
async fn bootstrap_with_external_provider() {
    let runtime = isolated_runtime();
    let provider = solaris_providers::create_provider(&runtime.config);

    let result = AgentBootstrap::new(runtime.config, &runtime.workspace, null_output())
        .provider(provider)
        .build()
        .await
        .unwrap();

    assert!(!result.engine.tool_names().is_empty());
}
