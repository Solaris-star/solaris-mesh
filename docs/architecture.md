# Solaris Mesh Architecture

## Current implementation

Solaris Mesh is a plugin-first, multi-agent-native Agent Runtime.

Implemented components:

- Runtime kernel: typed identity, scope, permission, effects, ledger, replay, and provider contracts.
- Lifecycle runtime: durable spawn reservations, live registries, relationships, sessions, and child results.
- Collaboration runtime: teams, typed tasks and roles, durable mailbox acknowledgements, supervisor decisions, and scheduling.
- Workflow runtime: validated definitions, node checkpoints, resume, and built-in standard-plan, deep-research-v1, and ultracode-v1 workflows.
- Native Host boundary: commands, queries, authoritative snapshots, ordered live events, and durable journal access.
- Plugin runtime: manifests, trust checks, transactional registration, authority-bound capabilities, lifecycle events, and storage declarations.
- CLI: interactive and JSON Stream Host transports.

## Host boundary

Solaris Studio is the first-party Mesh Host. It can launch Mesh through an explicit executable setting and preserves the legacy Pi runtime as a fallback. External clients such as Claude Code, Codex CLI, and Gemini CLI remain adapter integration targets.

## Resource scheduling

SOLARIS_MAX_ACTIVE_AGENTS optionally sets the active-agent budget. Without it, ResourcePolicy uses system parallelism as a default hint. An explicit configured budget is not capped by CPU count. Tasks beyond the budget remain queued until a lease is released.

## Contract documents

The [Mesh Runtime Contract](mesh/README.md) contains acceptance criteria, state models, Host behavior, decisions, and the four v1 RFCs.

## Planned areas

TUI improvements, broader Host adapters, the long-term ledger backend, future workflow schema versions, richer team shared-state consistency, signed remote plugin distribution, and mandatory plugin process isolation remain future work.
