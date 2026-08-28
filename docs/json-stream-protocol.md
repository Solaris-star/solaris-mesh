# Solaris Mesh JSON Stream Host Protocol

This document describes the JSON Lines transport used by a Host such as Solaris Studio. The semantic rules are defined in [Native Host Contract](mesh/host-protocol.md).

## Transport

- stdin/stdout, one UTF-8 JSON object per line;
- start with solaris --json-stream;
- stderr is diagnostic output and is not part of the protocol;
- one process can serve multiple turns in one session.

The runtime emits ready after initialization. A client waits for ready, sends any history and MCP configuration, then sends host_context_ready. Mesh resumes durable workflows and emits the initial runtime_snapshot without waiting for a message.

## Durable delivery and acknowledgements

Every application-visible event, including `ready`, is first committed to the session SQLite outbox, then sent with its original top-level fields plus a `delivery` object:

    {"type":"text_delta","text":"hello","msg_id":"m1","delivery":{"delivery_id":"019...","msg_id":"m1","run_epoch":4,"sequence":12,"digest":"sha256:..."}}

The digest is `sha256:` followed by the lowercase SHA-256 of canonical event-map bytes. A Host may pass the received top-level object directly to canonicalization: only the top-level `delivery` field is omitted. Object keys are sorted recursively by their UTF-8 bytes, array order is preserved, and the output is compact JSON without insignificant whitespace. Strings remain JSON strings with deterministic JSON escaping; numbers remain JSON numbers in their normalized `serde_json::Number` representation. The digest therefore does not depend on object insertion order and does not require an additional raw-payload field.

Interoperability fixture: the canonical bytes `{"items":[{"a":true,"z":2}],"payload":{"count":1,"label":"1"},"type":"info"}` have digest `sha256:2d21fdbf242f6b48866e618320a9ba4b915202b3dab28d3e2fde45bb6c439984`. A received top-level `delivery` object does not change this digest.

`sequence` orders deliveries within a session run epoch. A Host must durably record `delivery_id` before displaying an event, suppress an already-recorded delivery, and acknowledge either case:

    {"type":"acknowledge_delivery","session_id":"s1","run_epoch":4,"delivery_id":"019...","digest":"sha256:..."}

Acknowledgements are idempotent and may be sent as soon as an event is durably recorded, including before `host_context_ready`. Mesh accepts an acknowledgement only when its session, current run epoch, delivery ID, and digest all match. Unknown, stale, or mismatched acknowledgements are rejected without copying event content into the error. If Mesh exits after committing or sending an event but before committing its acknowledgement, the next owner replays that pending event with the same `delivery_id`; the new owner adopts it into the current run epoch and preserves its payload and digest. An old Host may ignore the optional `delivery` object for one protocol version, but only Hosts that persist delivery IDs and acknowledge them receive duplicate-display protection across restarts.

For startup compatibility, Mesh activates the outbox and sends one transport-only `ready` copy without delivery metadata so an existing Host can finish its handshake before opening its durable inbox. Mesh then replays pending envelopes in committed `sequence` order and commits the application-visible `ready` as the next sequence. The bootstrap copy is not an application event and must not be displayed; the later enveloped copy is durably recorded, displayed once, and acknowledged like every other event. No envelope is sent ahead of an earlier sequence.

## Ready and capabilities

    {"type":"ready","version":"0.3.0","session_id":"s1","capabilities":{"tool_approval":true,"thinking":true,"effort":true,"effort_levels":["low","medium","high"],"modes":["plan","auto","bypass"],"current_mode":"auto","mcp":true}}

Clients feature-detect from capabilities. Native permission modes are plan, auto, and bypass. Auto is one permission mode shared by file operations, network access, MCP, plugins, hooks, skills, and child processes. A platform process sandbox is only the executor used when Auto starts process-backed work; it is not another permission mode. Bypass runs with the complete host access available to the current OS user. An explicit deny rule or a blocking hook may still refuse an operation. Bypass does not promise to prevent destructive host actions. Legacy input aliases default, auto_edit, and yolo normalize to auto, auto, and bypass respectively.

The product contract requires a Full sandbox backend for process execution in strict Auto. A Partial or Unavailable backend denies that process operation without disabling Auto file tools, and it must never fall back to Ambient execution. Auto child processes never receive direct network access. Linux and macOS use the approved-domain Host proxy with per-connection DNS/IP validation. Windows uses a packaged AppContainer proxy peer plus a PSEC Security Environment; Full is emitted only after a pre-target proof reaches an approved destination through that peer and proves direct egress to the proxy's actual upstream address is unavailable. If PSEC, package registration/identity, proxy activation, DNS validation, or the isolation proof is unavailable, the operation is emitted as `status: "denied"` with `metadata.sandbox_report` set to backend `windows_app_container`, enforcement `unavailable`, and reason `network_proxy_unavailable`. Hosts must preserve those typed fields rather than replace them with output text.

## Conversation events

| Event | Required fields | Purpose |
| --- | --- | --- |
| stream_start | msg_id | Turn started. |
| text_delta | msg_id, text | Incremental assistant text. |
| thinking | msg_id, text | Provider thinking output when supported. |
| tool_request | msg_id, call_id, tool | Bounded effect awaiting approval. Optional run_id, agent_id, operation_id, and effect_id identify child operations. |
| tool_running | msg_id, call_id, tool_name | Tool execution started. |
| tool_result | msg_id, call_id, tool_name, status, output, output_type | Tool execution finished. |
| tool_cancelled | msg_id, call_id, reason | Tool was denied or cancelled. |
| stream_end | msg_id, usage | Turn ended. |
| error | error | Typed protocol, provider, tool, configuration, or internal error. msg_id is included when the error belongs to a turn. |
| info | msg_id, message | Non-critical information. |
| config_changed | capabilities, configuration | Effective configuration changed. |
| command_result | request_id, command, applied | Receipt for an evaluated control command. message and config_results are optional. |
| mcp_ready | name, tools | Dynamically added MCP server is ready. |
| pong | none | Ping response. |

`stream_end.usage` keeps the provider-reported `input_tokens` and `output_tokens` fields for compatibility. When the Provider contract declares cache accounting, it also reports `uncached_input_tokens`, `cache_read_tokens`, `cache_write_tokens`, and, when every non-zero category has a configured price, `cost_usd`. For protocols whose cached input is already included in `input_tokens`, Solaris subtracts the cache categories before calculating uncached input and cost. Hosts must not add raw `input_tokens` to the cache categories unless the declared Provider accounting says they are separate. An absent `cost_usd` means Unknown; it must not be displayed or aggregated as zero.

tool_request.tool includes name, category, args, description, and an optional effect descriptor. Categories are info, edit, exec, and mcp. The Host resolves approval with the emitted call_id. effect_id is a separate durable effect identity used for audit and recovery. In Plan and Auto, approval never replaces permission-ceiling or resource-boundary validation. Bypass uses current-user host access, but explicit deny rules and blocking hooks still apply.

`config_changed.configuration` reports the provider, model, permission, selected_intensity, effective_effort, thinking, thinking_budget, and compaction currently effective for new turns. `selected_intensity` preserves the requested preset, while `effective_effort` reports the provider-supported value actually used. Hosts must tolerate additional optional configuration fields in future minor versions.

## Runtime state events

Initial subscription:

    {"type":"runtime_snapshot","request_id":"subscription","schema_version":1,"timestamp_unix_ms":1,"live_sequence":12,"journal_sequence":8,"run_id":"r1","snapshot":{}}

Live event:

    {"type":"runtime_event","schema_version":1,"kind":"agent_state_changed","sequence":13,"timestamp_unix_ms":2,"journal_sequence":9,"run_id":"r1","payload":{}}

The kind namespace is open. Clients retain unknown kinds and continue. Apply only events whose sequence is greater than the last accepted live sequence. On runtime_event_gap, request a journal and then a fresh snapshot.

Journal response:

    {"type":"runtime_journal","request_id":"q1","run_id":"r1","after_sequence":8,"last_sequence":10,"records":[],"truncated":false}

Each journal record contains schema_version, sequence, run_id, timestamp_unix_ms, durability, record_type, and payload. Journal sequence numbers are global across the root Run and all descendant Runs. A root journal query therefore returns interleaved root and descendant records exactly once.

## Client commands

### Message and lifecycle

    {"type":"message","msg_id":"m1","content":"Inspect the project","files":[]}
    {"type":"cancel","msg_id":"m1"}
    {"type":"cancel_workflow","run_id":"r1:workflow:w1"}
    {"type":"stop"}

cancel ends only the turn named by msg_id, emits stream_end for that turn, and leaves the session ready for another message. A stale or mismatched msg_id is ignored. cancel_workflow signals only the named detached Workflow. stop cancels all active work and closes the session. Closing stdin also shuts down the process.

### Approval

    {"type":"tool_approve","call_id":"c1","scope":"once"}
    {"type":"tool_deny","call_id":"c1","reason":"Target is outside the project"}

scope is once or always. In Plan and Auto, always creates only a bounded session lease for the matching capability; it does not widen the permission ceiling. For a once-approved process operation, Mesh first commits the durable process intent, then consumes the approval before attempting sandbox preparation or spawn. A sandbox or spawn failure does not restore that approval. Bypass normally needs no ordinary approval.

### Session configuration

    {"type":"init_history","text":"Prior conversation summary"}
    {"type":"host_context_ready"}
    {"type":"set_mode","mode":"plan"}
    {"type":"set_intensity","intensity":"high"}
    {"type":"set_config","request_id":"cfg-1","model":"model-id","thinking":"enabled","thinking_budget":16000,"effort":"high","compaction":"safe","multi_agent_policy":"proactive","max_active_agents":4}
    {"type":"command_result","request_id":"cfg-1","command":"set_config","applied":false,"config_results":[{"field":"effort","status":"unsupported","message":"..."}]}

init_history is accepted before the first message. host_context_ready ends Host initialization and causes the initial snapshot and workflow recovery to start. set_config fields are optional and the update is atomic: if any supplied field is unsupported or rejected, `applied` is false and no supplied field changes runtime state. `max_active_agents` accepts an integer from 1 through 64; `configuration.max_active_agents` reports the explicit value and `configuration.effective_max_active_agents` reports the limit currently enforced by admission control. An invalid set_intensity value is a protocol error.

### Workflow

    {"type":"run_workflow","request_id":"w1","workflow":"deep-research-v1","parameters":{"question":"..."}}

The workflow must be registered and its input must satisfy WorkflowDefinition v1 validation.

### Runtime queries

    {"type":"get_runtime_snapshot","request_id":"s1"}
    {"type":"get_runtime_journal","request_id":"j1","run_id":"r1:workflow:w1","after_sequence":20,"limit":100}
    {"type":"ping"}

limit is optional. A truncated journal response tells the client to continue from last_sequence.

### Plugins

    {"type":"install_plugin","request_id":"p1","manifest_path":"/authority/plugin.json"}
    {"type":"activate_plugin","request_id":"p2","plugin_id":"example"}
    {"type":"deactivate_plugin","request_id":"p3","plugin_id":"example"}

Plugin commands produce open runtime events for lifecycle changes and typed errors for rejected manifests, trust failures, or partial activation attempts. Successful install, activation, and deactivation records are Run-durable. Resuming the same Run re-resolves the pinned manifest and identity; incomplete intents or changed implementations require explicit reconciliation and are not silently activated.

Project-local manifests found under .solaris/plugins are discovery-only. They are reported with trust=approval_required and remain inactive until the Host sends install_plugin and activate_plugin. Plugin lifecycle events carry the current workflow_definitions projection so a Host can update enabled and disabled definitions without restarting.

### MCP injection

    {"type":"add_mcp_server","name":"tools","transport":"stdio","command":"node","args":["bridge.js"],"env":{},"network":{"network_domains":["api.example.com:443"]}}

Transport is stdio, sse, or streamable-http. MCP injection is accepted after ready and before the first message.
After the last add_mcp_server command, the Host sends host_context_ready even when no MCP servers are configured.
For a stdio server in Auto mode, `network.network_domains` lists exact approved `host:port` destinations. It defaults to an empty list, which leaves direct network access disabled.
Connection and tool EffectRequests pin a versioned, secret-safe configuration identity. Environment and header values contribute SHA-256 digests and are not copied in plaintext into effective_input or error output.

## Error behavior

    {"type":"error","error":{"code":"protocol_error","message":"...","retryable":false}}

Stable error codes are provider_error, tool_error, config_error, protocol_error, internal_error, engine_error, required_workflow_failed, required_workflow_cancel_failed, turn_busy, history_import_failed, workflow_start_failed, runtime_journal_forbidden, runtime_journal_failed, runtime_snapshot_failed, required_workflow_commit_failed, plugin_install_failed, plugin_activation_busy, plugin_not_installed, plugin_activation_failed, plugin_requires_new_run, plugin_tool_collision, plugin_workflow_collision, plugin_deactivation_busy, plugin_deactivation_failed, and plugin_not_active. Terminal turn errors include msg_id and are followed by stream_end. Messages must not expose credentials or secret environment values.

## Startup and versioning

    solaris --json-stream --provider openai --model model-id --max-tokens 8192 --max-turns 30 --workspace /project

Use --session-id ID for a new named session or --resume ID for a durable session. ready includes the protocol semantic version. New optional fields and open event kinds are minor-version compatible. Required-field or semantic changes require a major version. Runtime records independently include schema_version.
