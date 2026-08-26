pub mod clear;
pub mod compact;
pub mod help;
pub mod model;
pub mod quit;
mod registry;

pub use registry::{CommandContext, CommandRegistry, CommandResult, SlashCommand, default_registry};
