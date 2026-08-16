# Solaris Mesh Architecture

## Current implementation

Solaris Mesh is a plugin-first, multi-agent-native Agent Runtime.

Implemented components:

- Agent Core: agent execution, tools, sessions and runtime flow.
- Scheduler: task queue and resource-aware active agent scheduling.
- Collaboration Runtime: runtime entry point for scheduled agent collaboration.
- Plugin System: extensible tool and integration model.
- CLI: command-line runtime interface.

Planned integration areas:

- TUI: a dedicated host-facing terminal interface beyond the current CLI surfaces.
- ACP Adapter: an integration boundary for external agent protocols.

## Host boundary

Solaris Studio is the first-party Mesh Host. External clients such as Claude Code, Codex CLI and Gemini CLI are ACP Adapter integration targets; support depends on the adapter implementation available in the deployed version.

## Resource scheduling

SOLARIS_MAX_ACTIVE_AGENTS optionally limits active agents. Without this setting, ResourcePolicy derives the default capacity from system available parallelism. Tasks beyond available capacity remain queued until resources are released.

## Planned areas

TUI improvements, broader host integrations and additional client adapters remain planned extensions unless provided by the current build.
