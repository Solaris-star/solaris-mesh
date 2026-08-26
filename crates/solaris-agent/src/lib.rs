// Core agent infrastructure: engine, session, orchestration, output sinks.

pub mod agent_registry;
pub mod agents_md;
pub mod bootstrap;
pub mod builtin_workflows;
pub mod cache_diagnostics;
pub mod child_capabilities;
pub mod collaboration_runtime;
pub mod collaboration_strategy;
pub mod collaboration_tools;
pub mod commands;
pub mod compact;
pub mod confirm;
pub mod context;
pub mod engine;
pub mod error;
pub mod execution_context;
mod hook_diagnostics;
mod memory_runtime;
mod memory_tool;
pub mod message_bus;
pub mod orchestration;
pub mod output;
pub mod permission_engine;
pub mod plan;
pub mod plugin_bootstrap;
pub mod plugin_manifest;
mod plugin_provider;
pub mod plugin_runtime;
pub mod plugin_tool;
pub mod relationship_store;
pub mod resource_manager;
pub mod resource_policy;
pub mod role_registry;
pub mod run_preset;
pub mod runtime_ledger;
pub mod scheduler;
pub mod schema_validation;
pub mod session;
pub mod skill_tool;
pub mod spawn_tool;
pub mod spawner;
mod stream;
pub mod supervisor;
pub mod task_registry;
pub mod team_registry;
pub mod team_state;
mod tool_call;
mod turn;
pub mod vcr;
pub mod workflow_controller;
pub mod workflow_executor;
#[path = "workflow_supervisor_turn_chain.rs"]
mod workflow_supervisor_turn_chain;
mod workflow_validation;

// Re-export the skills crate so existing callers (solaris-cli, tests) can use
// `solaris_agent::skills::` without changing their import paths.
pub use solaris_skills as skills;
