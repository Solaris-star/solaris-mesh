use solaris_types::message::ContentBlock;
use solaris_types::tool::{ToolResultMetadata, ToolResultStatus};
use tracing::{debug, error};

use crate::output::OutputSink;

pub(super) fn emit_tool_results_to_sink(
    output: &dyn OutputSink,
    tool_calls: &[ContentBlock],
    tool_results: &[ContentBlock],
    tool_statuses: &[ToolResultStatus],
    tool_metadata: &std::collections::BTreeMap<String, ToolResultMetadata>,
) {
    debug_assert_eq!(tool_results.len(), tool_statuses.len());
    for (result, status) in tool_results.iter().zip(tool_statuses) {
        let ContentBlock::ToolResult {
            tool_use_id,
            content,
            is_error,
        } = result
        else {
            continue;
        };
        let tool_name = tool_calls
            .iter()
            .find_map(|call| {
                if let ContentBlock::ToolUse { id, name, .. } = call
                    && id == tool_use_id
                {
                    return Some(name.as_str());
                }
                None
            })
            .unwrap_or("unknown");
        debug_assert_eq!(*is_error, status.is_error());
        if tool_use_id.trim().is_empty() {
            error!(
                target: "solaris_agent",
                tool = %tool_name,
                status = ?status,
                "tool result has empty tool_use_id"
            );
        } else {
            debug!(
                target: "solaris_agent",
                tool_use_id = %tool_use_id,
                tool = %tool_name,
                status = ?status,
                "tool result emitted"
            );
        }
        output.emit_tool_result_with_metadata(tool_use_id, tool_name, *status, content, tool_metadata.get(tool_use_id));
    }
}
