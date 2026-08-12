# Solaris Mesh boundaries

Solaris Mesh is a headless multi-agent collaboration runtime. It can run on
its own or be embedded by a host such as Solaris Studio. The dependency
direction is one way:

```text
Solaris Studio
    -> solaris-team
        -> solaris-mesh
```

Solaris Mesh must never depend on Solaris Studio, Electron, a Studio database
schema, Studio HTTP types, Studio WebSocket events, or UI models.

## Solaris Mesh owns

- Provider-neutral agent identity and lifecycle state.
- Supervisor/worker and peer-to-peer team topology.
- Task graph validation, assignment, delegation, and execution state.
- Mailbox and message-bus contracts.
- Shared-workspace references and runtime ports. A host decides how a
  workspace reference maps to files, containers, or remote storage.
- Runtime commands and events for local or distributed implementations.
- Failure signals, recovery commands, and runtime correlation identifiers.
- Agent execution, tool registry, MCP clients, skills, hooks, sessions,
  memory, and context compression.

The runtime API uses provider-neutral and host-neutral data. It does not expose
database rows, HTTP responses, UI view models, or process-specific handles.

## Solaris Studio owns

- HTTP routes and request authentication.
- User, organization, and product authorization.
- Database persistence and migration policy.
- WebSocket delivery and UI data projection.
- Team creation screens, settings, notifications, and product copy.
- Product workflows, quotas, billing rules, and audit presentation.
- Adapters between Studio repositories or agent sessions and Mesh ports.

Studio may persist Mesh identifiers and events, but its database types are not
part of the Mesh API.

## Current Studio code

The existing `solaris-team` implementation remains canonical until each
generic part is moved in a dedicated PR and the Studio caller is changed to use
Mesh in that same PR.

| Studio module | Current decision |
| --- | --- |
| `mailbox.rs` | Keep the database adapter in Studio. Move only host-neutral mailbox rules after a repository port exists in Mesh. |
| `member_runtime.rs` | Candidate for Mesh lifecycle state once its Studio session inputs are represented by runtime ports. |
| `event_loop.rs` | Split later. Generic scheduling can move to Mesh; Studio agent execution and API payload adapters stay in Studio. |
| `task_board.rs` | Mesh owns graph rules. Studio keeps persistence and authorization. |
| `scheduler/` | Move generic delegation, wake, and recovery rules incrementally. Studio keeps product policy and repository adapters. |
| `message_projection.rs` | Keep in Studio because it writes product messages and emits Studio WebSocket events. |
| `team_run/` | Keep in Studio until the runtime command/event adapter replaces direct product dependencies. |

No implementation may be copied into Mesh while an independent implementation
of the same rule remains active in Studio. During extraction, Studio either
delegates to Mesh or deletes the superseded rule in the same change.

## Initial public API

The `solaris-mesh` crate starts with stable host-facing contracts:

- Typed identifiers and agent identities.
- Validated team topology.
- Validated acyclic task graphs.
- Mailbox messages with agent or broadcast targets.
- A single async `Runtime` command/event boundary.

The first API is deliberately implementation-free. Local, process-isolated,
and distributed runtimes can implement the same trait without changing host
code. New fields should be optional or introduced through new enum variants;
breaking changes require a version update and a Studio compatibility change.

## Extraction order

1. Land these host-neutral contracts.
2. Add a Studio adapter pinned to a reviewed Mesh revision.
3. Move task-graph validation and replace the Studio implementation.
4. Move mailbox protocol rules while retaining Studio persistence adapters.
5. Move member lifecycle, delegation, wake, and recovery state machines.
6. Add remote execution transports only after local behavior is stable.

Every extraction PR must run Mesh tests without Studio and Studio tests with
the pinned Mesh revision.
