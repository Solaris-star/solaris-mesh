use async_trait::async_trait;

use super::{CommandContext, CommandResult, SlashCommand};

pub struct ModelCommand;

#[async_trait]
impl SlashCommand for ModelCommand {
    fn name(&self) -> &str {
        "model"
    }

    fn description(&self) -> &str {
        "Show or change the active model"
    }

    async fn execute(&self, ctx: &mut CommandContext<'_>, args: &str) -> anyhow::Result<CommandResult> {
        let requested = args.trim();
        if requested.is_empty() {
            ctx.output.emit_info(&format!("Current model: {}", ctx.model));
            return Ok(CommandResult::Continue);
        }
        if requested.chars().any(char::is_whitespace) {
            anyhow::bail!("model must be a single non-empty identifier");
        }
        Ok(CommandResult::SetModel(requested.to_owned()))
    }
}

#[cfg(test)]
#[path = "model_test.rs"]
mod model_test;
