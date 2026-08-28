# Solaris Mesh

A plugin-first, multi-agent-native Agent Runtime written in Rust. Solaris Mesh owns agent execution, collaboration, permissions, workflows, provider/protocol integration, and host-facing runtime state; the `solaris` binary is its lightweight CLI entry point, while Solaris Studio is the first-party native Host.

## Features

- **Multi-provider** — Anthropic, OpenAI (and compatibles like DeepSeek/Ollama/Gemini), AWS Bedrock, Google Vertex AI
- **ProviderCompat layer** — Configuration-driven compatibility for provider quirks (no hardcoded conditionals)
- **Reasoning model support** — OpenAI `o1`/`o3` reasoning models with `reasoning_effort` control
- **7 built-in tools** — Read, Write, Edit, Bash, Grep, Glob, Spawn (Child Agents)
- **MCP client** — Connect to any [Model Context Protocol](https://modelcontextprotocol.io/) server (stdio / SSE / streamable-http)
- **Dynamic MCP injection** — Host clients can inject MCP servers at runtime via the [JSON stream protocol](docs/json-stream-protocol.md)
- **Skills** — Named prompt snippets with variable substitution, shell expansion, conditional activation, and per-skill model/permission overrides (see [docs/skills.md](docs/skills.md))
- **Hook system** — Event-driven automation on tool lifecycle (auto-format, lint, audit)
- **Multi-agent collaboration** — Configurable Child Agent policy and strategy, durable task identities, dependency waves, recovery, and typed run summaries
- **Session persistence** — Save and resume conversation history
- **Persistent memory** — Project-specific memory with auto-indexing across sessions (see [docs/advanced.md](docs/advanced.md#memory-system))
- **Plan mode** — No-workspace-mutation execution for planning, research, and analysis (see [docs/advanced.md](docs/advanced.md#plan-mode))
- **Context compression** — Three-tier automatic compaction: microcompact, autocompact, emergency (see [docs/advanced.md](docs/advanced.md#context-compression))
- **Output compaction** — Configurable output compression (off/safe/full) with TOON encoding (see [docs/advanced.md](docs/advanced.md#output-compaction))
- **File state cache** — LRU cache with read deduplication and write tracking
- **Prompt caching** — Anthropic cache_control for up to 90% cost reduction
- **Profile inheritance** — Named profiles with `extends` for quick provider/model switching
- **OAuth login** — Use Claude.ai subscription directly, no API key needed
- **AGENTS.md injection** — Hierarchical loading of project instructions with @include support

## Quick Start

```bash
# Build from source
cargo build --release

# Generate default config, then add your API key
./target/release/solaris config init
# Edit the generated config (run `solaris config path` to find it)

# Single-shot mode
solaris "Read Cargo.toml and explain the dependencies"

# Interactive REPL
solaris

# Agent Client Protocol (ACP) stdio agent
solaris acp

# Release archives also include solaris-extension.json. Solaris Studio reads
# its acpAdapters contribution and launches the same `solaris acp` entry point.

# Full CLI reference
solaris --help
```

## Runtime Limits

`max_turns` is the broad model-turn limit per run. It is unset by default,
so runs have no broad model-turn limit unless you configure one. Set it to
`0` to explicitly disable the broad limit. `max_tool_call_malformed_turns`
stops repeated same tool-call-malformed rounds earlier; it defaults to `3`.
`max_tool_call_failure_turns` stops zero-text turns where every executable
tool result failed; it also defaults to `3`. Set either guard to `0` to
disable that breaker and rely on `max_turns` if a broad turn limit is
configured.

See [Core Concepts](docs/core-concepts.md) for the distinction between runs,
turns, tool rounds, and tool calls.

```toml
[default]
max_turns = 20  # optional broad model-turn limit
max_tool_call_malformed_turns = 3
max_tool_call_failure_turns = 3

# Profile names are user-defined; this is not a built-in profile.
[profiles.my-weak-provider]
max_turns = 10
max_tool_call_malformed_turns = 2
max_tool_call_failure_turns = 2
```

CLI override:

```bash
solaris --max-turns 10 "Run the task"
solaris --max-tool-call-malformed-turns 2 "Run the task"
solaris --max-tool-call-failure-turns 2 "Run the task"
```

## Multi-agent collaboration

`Spawn` accepts the legacy `{ "tasks": [{ "name", "prompt" }] }` shape and
the durable v2 shape. v2 tasks have stable `id` values, optional `role`,
`depends_on`, `expected_output`, and a per-task `budget`.

```json
{
  "strategy": "supervisor",
  "tasks": [
    {"id": "inspect", "name": "Inspect", "prompt": "Read the relevant modules."},
    {"id": "verify", "name": "Verify", "prompt": "Check the proposed result.", "depends_on": ["inspect"]}
  ]
}
```

The policy is `disabled`, `on_demand` (the default), or `proactive`.
`auto`, `single`, `supervisor`, `team`, `fanout`, and
`independent_reviewer` are supported strategies. Roles are free-form; the
runtime still limits resources: automatic parallelism is 2–8 active Child
Agents, an explicit `--max-active-agents` value is 1–64, and durable atomic
admission limits the entire Run to 32 logical collaboration tasks by default
or at most 256 when configured. Replays and retries reuse their logical task
identity; `single` creates no Child Agent task, while an automatic independent
reviewer consumes one task slot.
For collaboration Spawn batches, the Task records and the complete Team/member
batch marker are committed together before the in-memory Task, Team, or
membership projection changes. Conversation/fork/configured-Supervisor paths
prepare the exact Team shell before a Child Agent can be durably reserved, then
commit the child membership as one atomic batch before the conversation becomes
usable. Backends that cannot provide these atomic metadata commits reject the
operation before the corresponding projection changes; exact replay reuses the
same durable state.

Workflow retries are failure-class based. Only an explicit `Retryable` failure
is retried automatically. When a child Workflow has multiple failed branches,
its durable failure summary keeps the complete failure set and the parent can
retry only when every failed branch is `Retryable`; reconciliation/unknown
outcomes take priority as the deterministic primary failure. A bare `Failed`
result defaults to `NonRetryable`; permission, cancellation, convergence,
turn-budget, `SideEffectUnknown`, `OutcomeUnknown`, and
`ReconciliationRequired` keep their terminal or reconciliation semantics and
are not silently replayed.

On Windows, approved-domain Auto networking is implemented as a separate
capability boundary: the Target remains an AppContainer with no network
capabilities, while an installed packaged proxy peer owns network access. PSEC
binds the Target to that peer, and the helper returns Full only after a real
pre-target proof reaches an approved endpoint through the proxy and fails a
direct connection to the exact upstream address. Unsupported PSEC/package
registration stays typed fail-closed; it never falls back to Ambient. The
currently validated host does not export the required V2 PSEC create/query/close
API from `processmodel.dll`, so approved-domain networking remains unavailable
here even though the proxy, DNS, codec, package, and fail-closed paths are
implemented and tested under both the service identity and an interactive
Administrator session.

`CollaborationRunSummary` reports `duplicate_call_rate` for terminal tool calls.
A duplicate is a later call in the same Agent execution scope with the same
stable tool name, canonical JSON input, and execution-environment snapshot.
All terminal statuses, including cache hits, denials, failures, aborts, and
unknown outcomes, enter the denominator; status is not part of the fingerprint.

The same settings can be supplied in the project configuration or through
`SOLARIS_MULTI_AGENT_POLICY`, `SOLARIS_COLLABORATION_STRATEGY`,
`SOLARIS_MAX_ACTIVE_AGENTS`, and `SOLARIS_MAX_AGENT_TASKS`.

## Architecture

```text
Solaris Studio / CLI / Server Hosts
              │
              │ Native Mesh API / Host Protocol
              ▼
        Solaris Mesh Runtime
  ┌───────────────────────────────────────┐
  │ Mesh Kernel                           │
  │ Identity / Scope / Permission /       │
  │ Runtime Ledger / Effect Boundary      │
  ├───────────────────────────────────────┤
  │ Collaboration Runtime / Workflow      │
  │ Scheduler / Agent & Task Registry     │
  ├───────────────────────────────────────┤
  │ Agent Core / Sessions / Context       │
  ├───────────────────────────────────────┤
  │ Protocol Adapters / Providers         │
  │ Tools / Skills / MCP / Memory         │
  └───────────────────────────────────────┘
```

The current codebase contains the Agent Core, provider adapters, tools,
sessions, MCP/skills/memory, Spawn execution, and the durable collaboration
runtime. Durable identity, permission/effect policy, workflow ownership, and
multi-agent state live in the Mesh Runtime.

## Documentation

| Document | Description |
|----------|-------------|
| [Getting Started](docs/getting-started.md) | Installation, CLI reference, configuration, usage examples |
| [Built-in Tools](docs/tools.md) | Detailed reference for all 7 tools |
| [MCP Integration](docs/mcp.md) | Model Context Protocol client setup and usage |
| [Providers & Auth](docs/providers.md) | Multi-provider config, profiles, Bedrock, Vertex, OAuth |
| [Advanced Features](docs/advanced.md) | Multi-agent collaboration, hooks, prompt caching, VCR, AGENTS.md |
| [Troubleshooting](docs/troubleshooting.md) | Common errors and solutions |
| [JSON Stream Protocol](docs/json-stream-protocol.md) | Host integration protocol (`--json-stream` mode) |
| [Mesh Runtime Contract](docs/mesh/README.md) | Acceptance criteria, state models, Host contract, decisions, and v1 RFCs |

## Supported Providers

| Provider | Auth | Notes |
|----------|------|-------|
| Anthropic | API Key / OAuth | Prompt caching, streaming, vision |
| OpenAI | API Key | Reasoning models (`o1`/`o3`), compatible with DeepSeek, Qwen, Ollama, Gemini, vLLM |
| AWS Bedrock | SigV4 | Regional endpoints, AWS credential chain, schema sanitization, actionable error hints |
| Google Vertex AI | GCP OAuth2 / Service Account | Metadata server auto-detection |

## ProviderCompat

All provider-specific behaviors are driven by the `ProviderCompat` configuration layer — no hardcoded URL or model-name checks. Each provider type has sensible defaults; override any field via config:

```toml
[providers.my-openai.compat]
max_tokens_field = "max_completion_tokens"   # Field name for max tokens
merge_assistant_messages = true              # Merge consecutive assistant messages
clean_orphan_tool_calls = true               # Remove tool_use without tool_result
dedup_tool_results = true                    # Deduplicate same tool_call_id results
ensure_alternation = false                   # Insert filler for user/assistant alternation
merge_same_role = false                      # Merge consecutive same-role messages
sanitize_schema = false                      # Bedrock-style schema sanitization
strip_patterns = ["<think>", "</think>"]     # Strip text patterns from history
auto_tool_id = false                         # Auto-generate missing tool IDs
api_path = "/v1/chat/completions"            # Custom chat completions endpoint path
```

Provider defaults: **Anthropic/Vertex** — alternation, merge, auto tool ID; **Bedrock** — same + schema sanitization; **OpenAI** — assistant merge, orphan cleanup, dedup.

## License

Apache-2.0

## Solaris Mesh Runtime Architecture

Solaris Mesh is the runtime and source of truth for agent, workflow, task, permission, and host-visible state.

- **Agent Core**: current agent execution loop, tools, sessions, and provider integration.
- **Scheduler**: task queue and resource-aware scheduling for spawned agent tasks. `SOLARIS_MAX_ACTIVE_AGENTS` sets an explicit active-agent budget; when unset, system parallelism supplies a bounded 2–8 default. Explicit values are limited to 1–64. Queued tasks wait until resources are available.
- **Collaboration Runtime**: current scheduling entry point for coordinating queued agent work.
- **Plugin System**: tools, skills, and MCP-related extension points provide plugin-style capabilities.
- **CLI**: the `solaris` command-line and stdio host entry point.
- **TUI**: intentionally not a current-stage priority.
- **ACP Agent**: `solaris acp` serves the standard Agent Client Protocol over stdio and reuses the same runtime as the CLI.

Solaris Mesh also exposes a standard ACP agent through `solaris acp`. The
first-party Studio integration may continue using the native Mesh Host API,
but the ACP endpoint uses the same Engine, permission checks, session store,
and collaboration runtime.
