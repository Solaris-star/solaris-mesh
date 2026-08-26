use solaris_tools::ToolExecutionContext;
use solaris_types::effect::EffectClass;

use crate::execution_context::EffectExecutionContext;

pub(super) fn with_process_workspace_root(
    class: EffectClass,
    context: ToolExecutionContext,
    execution: &EffectExecutionContext,
) -> ToolExecutionContext {
    if class != EffectClass::Process || context.sandbox_workspace_root().is_some() {
        return context;
    }
    let boundary = execution.permissions().boundary();
    let [workspace_root] = boundary.writable_roots.as_slice() else {
        return context;
    };
    context.with_sandbox_workspace_root(workspace_root)
}
