use solaris_config::hooks::HookEngine;
use solaris_protocol::events::ToolStatus;
use solaris_tools::PreparedToolExecution;
use solaris_tools::registry::ToolRegistry;
use solaris_types::message::ContentBlock;
use solaris_types::skill_types::ContextModifier;
use solaris_types::tool::{ToolResult, ToolResultMetadata};

use crate::execution_context::stable_digest_bytes;

use super::{maybe_append_deferred_hint, truncate_result};

pub(super) async fn execute_single(
    registry: &ToolRegistry,
    call: &ContentBlock,
    prepared_execution: PreparedToolExecution<'_>,
    hooks: Option<&HookEngine>,
    compaction_level: solaris_compact::CompactLevel,
    toon_enabled: bool,
) -> (
    ContentBlock,
    Option<ContextModifier>,
    ToolStatus,
    Option<ToolResultMetadata>,
) {
    let ContentBlock::ToolUse { id, name, input, .. } = call else {
        unreachable!("execute_single called with non-ToolUse block")
    };
    let start = std::time::Instant::now();
    tracing::info!(target: "solaris_agent", tool = %name, call_id = %id, "tool execution started");
    let (result, modifier, status, metadata) = match registry.get(name) {
        Some(tool) => {
            let r = prepared_execution.execute_classified().await;
            let modifier = (!r.is_error).then(|| tool.context_modifier_for(input)).flatten();
            let content = if r.is_error && tool.is_deferred() {
                maybe_append_deferred_hint(&r.content, tool.input_schema(), input)
            } else {
                r.content.clone()
            };
            let content = truncate_result(&content, tool.max_result_size());
            let content = solaris_compact::compact_output(&content, compaction_level);
            let content = if toon_enabled {
                solaris_compact::compact_output_toon(&content)
            } else {
                content
            };
            (
                ToolResult {
                    content,
                    is_error: r.is_error,
                },
                modifier,
                r.status,
                r.metadata,
            )
        }
        None => (
            ToolResult {
                content: format!("Unknown tool: {name}"),
                is_error: true,
            },
            None,
            ToolStatus::Failed,
            None,
        ),
    };
    if let Some(hook_engine) = hooks {
        for message in hook_engine.run_post_tool_use(name, input, &result.content).await {
            tracing::info!(
                target: "solaris_agent",
                hook_output_digest = %stable_digest_bytes(message.as_bytes()),
                hook_output_bytes = message.len(),
                "post-tool-use hook completed"
            );
        }
    }
    tracing::info!(target: "solaris_agent", duration_ms = start.elapsed().as_millis() as u64, ?status, "tool execution completed");
    (
        ContentBlock::ToolResult {
            tool_use_id: id.clone(),
            content: result.content,
            is_error: result.is_error,
        },
        modifier,
        status,
        metadata,
    )
}
