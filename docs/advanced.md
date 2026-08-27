# Advanced Features

## Multi-agent collaboration

The LLM can use the Spawn tool to create durable Child Agent tasks. Each Child
Agent has its own conversation context and inherits the parent Agent's current
provider, permission state, workspace boundary, and tool policy. The scheduler
shares one Run-wide resource budget, while task identities and outcomes are
kept in the collaboration runtime for restart recovery.

### Use Cases

- "Search these 3 files simultaneously and summarize each"
- "Run tests and lint in parallel"
- "Search for X in the codebase while reading Y"

### Limits

| Setting | Default | Description |
|---------|---------|-------------|
| Automatic active Child Agents | 2–8 | CPU-based default, bounded to avoid resource exhaustion |
| Explicit active Child Agents | unset | `--max-active-agents` accepts 1–64 |
| Tasks per Run | 32 | `--max-agent-tasks` accepts 1–256 |
| Child Agent max turns | 200 | Per task unless its budget supplies a lower value |
| Child Agent max tokens | 4096 | Per response unless its budget supplies a lower value |

### Behavior

- `Disabled` rejects every Spawn request, including explicit requests.
- `OnDemand` is the default and allows a child only for an explicit request or a concrete need in the current task. Older `explicit` and `adaptive` values migrate to `OnDemand`.
- `Proactive` also allows the agent to create children on its own when independent work benefits from parallel execution.
- Child Agents use the current Run permission state and cannot independently elevate it; Spawn is itself permission-checked.
- Root and child sessions use fenced SQLite ownership and keep their shared Run reference
- Child Agents run through the durable task path and return a typed
  `CollaborationRunSummary` in tool metadata. The summary includes task
  status, stable Agent IDs, durations, four token counters, useful-call rate,
  duplicate-call rate, and any `outcome_unknown` work that needs manual
  verification.
- A failed prerequisite is durably marked `Skipped`; it is never silently
  executed during a later recovery.

### Strategies and task flow

`single`, `fanout`, `supervisor`, `team`, and `independent_reviewer` are
available in addition to `auto`. `auto` selects `single` for one task, fanout
for independent tasks, and supervisor ordering when dependencies are present.
For a Spawn request, `single` means the parent Agent keeps the work and no
Child Agent is created; workflow nodes may still use their ordinary single
executor. Task IDs are checked before execution; duplicate IDs, missing
dependencies, self-dependencies, and cycles are rejected as one request.
`max_tasks_per_run` is a durable historical quota for the entire Run: direct
Spawn batches, Workflow tasks, `CreateTeamTask`, configured Supervisor dispatch,
and the independent reviewer all use the same atomic ledger admission. Replays
of the same logical task do not consume another slot, while completed, failed,
and cancelled tasks continue to occupy their original slot. `single` creates no
Child task and therefore consumes no slot. Across SQLite Runtime instances, the
quota check and all `task_created` records commit in one immediate transaction.

Workflow retry policy is type-based. Only an explicit `Retryable` failure class
authorizes automatic retry; a bare `Failed` result is `NonRetryable`, and
permission, cancellation, convergence, turn-budget, unknown side-effect, unknown
outcome, and reconciliation failures retain their typed terminal or recovery
semantics.

The CLI also exposes the settings directly:

```text
solaris --multi-agent-policy on_demand \
  --collaboration-strategy supervisor \
  --max-active-agents 4 --max-agent-tasks 64
```

`solaris acp` exposes the same policy, strategy, and intensity options as ACP
session configuration values. ACP `session/new`, `session/load`, `session/prompt`,
`session/cancel`, `session/close`, and configuration updates are handled by the
same Engine and durable session store.

---

## Permission and process isolation

Auto is a permission mode shared by files, network access, MCP, plugins, hooks, skills, and child processes. A process sandbox is only the platform executor used by Auto. Strict Auto accepts a Full executor only; Partial or Unavailable denies the process operation without disabling Auto file tools, and there is no Ambient fallback.

The Linux Full runner requires Bubblewrap at `/usr/bin/bwrap` or `/bin/bwrap`, the packaged `solaris-process-sandbox-helper` beside the Solaris executable, Landlock ABI v3 or newer, and a successful real startup handshake. Missing prerequisites are reported as `BubblewrapUnavailable`, `HelperUnavailable`, or `LandlockUnavailable`; preparation and handshake failures are reported as `SetupFailed` or `StartConfirmationFailed`. Any of these results denies the process operation before Solaris returns a running child.

The Linux sandbox exposes the workspace and private `HOME`, temporary, and runtime-state directories as writable. Its supported read-only host toolchain roots are `/bin`, `/usr`, `/lib`, `/lib64`, `/sbin`, and `/nix/store` when present, plus a small fixed set of `/etc` loader, identity, certificate, resolver, host, and timezone files. Toolchains or runtime dependencies stored under `/opt`, the real user home, or another host path are not advertised as supported and may fail inside the sandbox. Solaris does not mount the complete host root because a read-only mount would still expose pathname Unix sockets.

The Windows Full runner uses the packaged helper to create an AppContainer target with no network capabilities. Temporary ACL leases grant only the launch profile access to the workspace and private directories, keep Runtime state unavailable, and remain correct for concurrent launches in one Solaris process. The outer helper and all descendants remain in a kill-on-close Job Object, which is terminated immediately when the sandbox target completes so background descendants cannot outlive the operation. A real probe launches and waits for an AppContainer target before Full is cached; if the current account cannot initialize that child, the probe returns Partial or Unavailable before the requested target starts. The native Windows suite currently passes 39 tests under an interactive Administrator account; this is not a claim that every Windows service account supports AppContainer startup.

The macOS Full runner uses the fixed `/usr/bin/sandbox-exec`, a restrictive Seatbelt profile, and the packaged helper. It retains the opened workspace directory identity through managed-child cleanup, checks that the configured path still names that object immediately before spawn, and rejects protected aliases, external hardlinks, sockets, and FIFOs before target execution. A guardian owns the target process group and verifies descendant termination; Seatbelt denies `setsid`, `setpgid`, and `posix_spawn` so a managed descendant cannot detach. Auto processes have no direct network access. Exact approved HTTP(S) destinations are available only through the per-launch Host proxy, which revalidates DNS and destination IPs. Full is cached only after a real probe verifies workspace writes, external file and non-proxy loopback denial, target startup, and the helper handshake. The implementation and native CI suite are present, but the current change still has no macOS runtime evidence.

Other Unix platforms may use the [external strict-Auto runner contract](external-sandbox-runner.md) with an explicitly configured absolute binary path and exact SHA-256. Solaris pins that binary and requires versioned request, capability, digest, and single-use-token confirmation before and after target startup. This verifies which trusted implementation answered; it does not independently attest the implementation's OCI runtime or VM. Missing, malformed, Partial, or Unavailable results deny the process and never fall back to Ambient. Other non-Unix platforms remain Unavailable.

Auto child processes have no direct network access. On Linux and macOS, a process may reach an exact approved HTTP(S) host and port only through the per-launch Host proxy. The proxy rejects IP literals, wildcards, user information, metadata names, private or special-use DNS results, and malformed or oversized proxy requests; it resolves the name again before connecting. An empty domain declaration starts no proxy and exposes no proxy environment variables. Windows rejects Auto process launches that declare network domains until a verified process-isolated proxy runner is available; it does not grant `internetClient`, install a system loopback exemption, or rely on `HTTP_PROXY`. The denial is a request-level typed `SandboxReport` with backend `WindowsAppContainer`, enforcement `Unavailable`, and reason `NetworkProxyUnavailable`; it is preserved through ToolResult and Host protocol events. The private PSEC probe is based on Microsoft `mxc` commit [`b497fd48653e01c846c6ef225e0af1c859b70122`](https://github.com/microsoft/mxc/commit/b497fd48653e01c846c6ef225e0af1c859b70122), but version or export checks alone never produce Full.

Bypass uses the complete host access available to the current OS user. Explicit deny rules and blocking hooks can still refuse an operation. Bypass does not claim to prevent destructive host actions.

---

## Hook System

Event-driven hooks execute shell commands at specific points in the tool lifecycle, enabling auto-formatting, linting, auditing, and more.

Hooks read the same permission mode as the triggering tool. A process-backed hook in strict Auto requires a Full executor. A non-zero `pre_tool_use` hook remains a blocking decision in every mode, including Bypass.

A Hook that needs network access declares exact destinations under `network.network_domains`. Skill frontmatter, Skill-owned command hooks, executable plugins, and stdio MCP use the same declaration and final process-spawn authorization. The declaration never enables direct child network access.

### Hook Types

| Type | Trigger | Behavior |
|------|---------|----------|
| `pre_tool_use` | Before tool execution | Non-zero exit blocks the tool |
| `post_tool_use` | After tool execution | Non-blocking; errors are logged |
| `stop` | When agent session ends | Non-blocking |

### Configuration

```toml
# Auto-format Rust files after modification
[[hooks.post_tool_use]]
name = "rustfmt"
tool_match = ["Write", "Edit"]
file_match = ["*.rs"]
command = "rustfmt ${TOOL_INPUT_FILE_PATH}"

# Auto-format TypeScript files after modification
[[hooks.post_tool_use]]
name = "prettier"
tool_match = ["Write", "Edit"]
file_match = ["*.ts", "*.tsx"]
command = "npx prettier --write ${TOOL_INPUT_FILE_PATH}"

# Audit ExecCommand commands
[[hooks.post_tool_use]]
name = "audit-log"
tool_match = ["ExecCommand"]
command = "echo \"$(date): ${TOOL_INPUT_COMMAND}\" >> .solaris/audit.log"

# Run lint on session end
[[hooks.stop]]
name = "final-lint"
command = "cargo clippy --quiet 2>&1 | tail -5"
```

### Environment Variables

Hook commands can reference these variables via `${VAR}` syntax:

| Variable | Description |
|----------|-------------|
| `TOOL_NAME` | Tool name |
| `TOOL_INPUT` | Full tool input JSON |
| `TOOL_INPUT_FILE_PATH` | File path (if the tool has a file_path parameter) |
| `TOOL_INPUT_COMMAND` | Command (if the tool has a command parameter) |
| `TOOL_INPUT_PATTERN` | Search pattern (if the tool has a pattern parameter) |
| `TOOL_OUTPUT` | Tool output (post_tool_use only) |

### Matching Rules

- `tool_match`: glob patterns matching tool names; empty = match all
- `file_match`: glob patterns matching file paths; empty = match all
- Default timeout: 30 seconds, configurable via `timeout_ms`

---

## Prompt Caching (Anthropic)

Prompt caching stores system prompts and tool definitions on Anthropic's servers, so subsequent requests only process the changed parts.

- **First request**: full input token cost + 25% write premium
- **Subsequent requests**: cached portion costs only 10%
- **Cache TTL**: 5 minutes (auto-renewed on each hit)

### Configuration

```toml
[providers.anthropic]
api_key = "sk-ant-xxx"
prompt_caching = true   # default true (Anthropic only)
```

### Token Stats

With caching enabled, stats show cache data:

```
[turns: 3 | tokens: 100 in (5000 cached) / 200 out | cache: 5000 created, 5000 read]
```

---

## VCR Recording & Replay

Record real API interactions and replay them in tests — no API key or network needed.

### Usage

```bash
# Record mode
VCR_MODE=record VCR_CASSETTE=tests/cassettes/my_test.json \
  solaris -k sk-ant-xxx "Read Cargo.toml"

# Replay mode (in tests)
VCR_MODE=replay VCR_CASSETTE=tests/cassettes/my_test.json \
  solaris "Read Cargo.toml"
```

### Features

- Auto-sanitization: sensitive headers (api-key, auth, token) are replaced with `[REDACTED]` during recording
- JSON-formatted cassette files, editable by hand
- Supports recording/replay of SSE streaming responses

---

## Logging

Structured JSON file logging with daily rotation, powered by the `tracing` crate. All internal events (LLM requests/responses, tool execution, MCP connections, compaction) are captured with structured fields.

### Enabling

Three ways to enable logging, from highest to lowest priority:

1. **CLI parameter**: `--log-dir /path/to/logs` (automatically enables logging)
2. **Config file**: add a `[logging]` section (global or project-level)
3. **Default**: logging is disabled unless explicitly configured

```bash
# CLI — logs to /tmp/solaris-logs at debug level
solaris --log-dir /tmp/solaris-logs --log-level debug "Read Cargo.toml"
```

### Configuration

```toml
[logging]
enabled = true       # enable file logging (default: false; auto-enabled when dir is set)
level = "info"       # tracing filter directives (default: "info")
dir = "/path/to/logs"  # log directory (default: platform-specific, see below)
```

The `level` field accepts standard tracing filter directives:

| Value | Effect |
|-------|--------|
| `"info"` | Info and above for all targets |
| `"debug"` | Debug and above for all targets |
| `"solaris_providers=debug,info"` | Debug for providers, info for everything else |

### Default Log Directory

When `dir` is not set, logs go to the platform-specific location:

| Platform | Path |
|----------|------|
| macOS | `~/Library/Logs/solaris/` |
| Linux | `$XDG_STATE_HOME/solaris/logs/` or `~/.local/state/solaris/logs/` |
| Windows | `{data_local_dir}/solaris/logs/` |

### Log Format

Each line is a JSON object with structured fields:

```json
{"timestamp":"2026-05-13T12:12:52.431Z","level":"INFO","fields":{"message":"mcp server connected","server":"sentry","tools":20},"target":"solaris_mcp","spans":[{"name":"agent_run","session_id":"abc-123","msg_id":"msg-456"}]}
```

Key fields:

| Field | Description |
|-------|-------------|
| `target` | Source crate (`solaris_agent`, `solaris_providers`, `solaris_mcp`, etc.) |
| `spans[].session_id` | Session ID for correlating events within a conversation |
| `spans[].msg_id` | Message ID for correlating events within a single turn |

### Session Correlation

All events during `engine.run()` — LLM streaming, tool execution, compaction — are wrapped in an `agent_run` span carrying `session_id` and `msg_id`. This allows filtering all logs for a specific conversation:

```bash
# Find all events for a specific session
grep '"session_id":"abc-123"' 2026-05-13.solaris.log | jq .
```

### Library Integration

When Solaris Mesh is embedded in a native Host or backend server, the `create_file_layer()` API provides a composable tracing layer:

```rust
use solaris_config::logging::{ResolvedLogging, create_file_layer};

let resolved = ResolvedLogging {
    enabled: true,
    level: "solaris_agent=debug,solaris_providers=debug".to_string(),
    dir: log_dir.to_path_buf(),
};
let (layer, guard) = create_file_layer(&resolved)?;

// Compose with your existing subscriber
tracing_subscriber::registry()
    .with(your_app_layer)
    .with(layer)  // Solaris Mesh logs → separate solaris.log file
    .init();
```

The host application owns the global subscriber; Solaris Mesh library crates only emit tracing events and never initialize a subscriber themselves.

---

## AGENTS.md Hierarchical Loading

AGENTS.md files provide project-specific instructions that are automatically injected into the system prompt. Files are discovered hierarchically and merged from remote to near:

1. **Global**: `<config_dir>/solaris/AGENTS.md` — user-level instructions for all projects
2. **Project hierarchy**: Walk up from cwd to the git root (or home directory), collecting every `AGENTS.md` found along the way

Files closer to the working directory appear later in the prompt and take precedence (via LLM recency bias). Each file is annotated with its absolute path for traceability.

### @include Directive

AGENTS.md files can include other files using `@` syntax:

- `@FILENAME` or `@./relative/path` — relative to the AGENTS.md file's directory
- `@~/path` — relative to home directory
- `@/absolute/path` — absolute path

Paths inside fenced code blocks are ignored. Includes are recursive (up to depth 5) with circular reference detection. Non-existent files and non-text files are silently skipped.

### Example

Given this structure:

```
my-workspace/
├── .git/
├── AGENTS.md          ← workspace rules
└── packages/
    └── server/
        └── AGENTS.md  ← server-specific rules
```

Running the `solaris` CLI in `packages/server/` produces a system prompt containing both files, workspace first, then server.

---

## Memory System

Persistent memory is optional and is disabled by default. When enabled, Solaris freezes a read-only snapshot at session start. Changes made during the session become visible to the next session, so a running conversation cannot silently change the context it started with.

### Memory Types

| Type | Purpose |
|------|---------|
| `user` | User's role, goals, preferences, knowledge |
| `feedback` | Corrections and confirmations on work approach |
| `project` | Ongoing work context not derivable from code/git |
| `reference` | Pointers to external systems and resources |

### Storage and migration

Memory is stored in a per-project SQLite database under the global config:

```
<config_dir>/solaris/projects/<sanitized-project-path>/memory/
└── memory.sqlite3
```

The database uses WAL mode, full synchronous writes, versioned records, proposals, and FTS5 search. Search returns at most 8 records and at most 32 KiB of content.

Older `MEMORY.md` and Markdown record files are kept in place. On the first enabled start they are imported once through a bounded, identity-checked migration. Solaris does not delete or rewrite those files, and it does not read them while Memory is disabled.

Legacy files may use YAML frontmatter such as:

```markdown
---
name: auth rewrite
description: Auth middleware rewrite driven by compliance
type: project
---

Auth middleware rewrite is driven by legal/compliance requirements.
```

### Configuration

Enable Memory explicitly in the user or project configuration:

```toml
[memory]
enabled = true
review = false
```

`review = true` routes root-agent changes through proposals. Review is off by default. Child agents can only submit proposals and cannot approve, edit, or delete records directly.

Override the base directory via environment variable:

```bash
export SOLARIS_MEMORY_DIR=/custom/path
```

### How It Works

1. When enabled, Solaris opens the project database and imports eligible legacy files once.
2. Solaris freezes the current record metadata for the session prompt. Record bodies are not copied into the prompt.
3. The dedicated `Memory` tool provides bounded search, list, create, edit, delete, proposal, and review operations.
4. Root-agent writes apply directly only when review is disabled. Child-agent writes always create proposals.
5. New or changed records are visible to sessions started afterward.

---

## Plan Mode

A read-only exploration mode where the agent focuses on understanding the codebase and producing an implementation plan before making any changes.

### How It Works

1. Agent calls `EnterPlanMode` → tool access restricted to read-only (Read, Grep, Glob)
2. Agent explores code, designs the approach, and prepares complete Markdown beginning with an H1 heading
3. Agent calls `ExitPlanMode` with that Markdown → Solaris durably records a versioned `PlanArtifact` before leaving Plan mode

Entering Plan mode permanently disables every MCP manager attached to the
current Run. `ExitPlanMode` does not reconnect or re-enable MCP. Start a new
Run and authorize the connections again when MCP access is needed.

### Configuration

```toml
[plan]
enabled = true                    # Register Plan Mode tools (default: true)
plan_directory = ".solaris/plans"  # Where plan files are saved
```

`plan_directory` remains readable for legacy plan files, but new plans are stored in the Runtime Ledger. Each artifact has a stable ID, revision, Markdown digest, Run/message references, and timestamps. JSON stream Hosts can query artifacts with `get_plan_artifacts`; `runtime_snapshot.plan_artifact_refs` provides the latest references without copying full Markdown into every snapshot.

### Workflow Phases

When in plan mode, the agent follows a structured 4-phase process:

1. **Understand** — Explore the codebase with read-only tools
2. **Design** — Identify files to modify, code to reuse
3. **Write the plan** — Compose a clear, actionable implementation plan
4. **Submit** — Call `ExitPlanMode` with the complete Markdown plan; persistence must succeed before the mode changes, and MCP remains disabled for the current Run

---

## Context Compression

A three-tier automatic compaction strategy that prevents context window overflow during long conversations.

### Tiers

| Tier | Trigger | Method | LLM Call |
|------|---------|--------|----------|
| **Microcompact** | Tool result count exceeds threshold or time gap | Clears old tool result content, keeping the N most recent | No |
| **Autocompact** | Input tokens approach context limit | LLM summarizes the conversation | Yes |
| **Emergency** | Input tokens near absolute limit | Blocks further API calls, asks user to start fresh | No |

### How It Works

- **Microcompact** runs automatically: replaces old Read/ExecCommand/Grep/Glob/Write/Edit results with `[Tool result cleared]`, keeping the 5 most recent results intact. Triggered by count (>10 compactable results) or time (>1 hour since last assistant message).

- **Autocompact** triggers when input tokens reach a threshold. By default this is `context_window - output_reserve - autocompact_buffer` (200,000 - 20,000 - 13,000 = 167,000 tokens). Alternatively, set `autocompact_threshold_pct` to trigger at a percentage of the context window (e.g. `50` = 50% of 200k = 100k tokens). The agent calls the LLM to produce a conversation summary, then replaces history with a compact boundary marker. A circuit breaker stops retrying after 3 consecutive failures.

- **Emergency** is the last safety net at `context_window - emergency_buffer` (default: 197,000 tokens). Always active regardless of config. Blocks API calls and prompts the user to compact or start a new conversation.

### Configuration

```toml
[compact]
enabled = true              # Enable compaction system (default: true)
context_window = 200000     # Context window in tokens
output_reserve = 20000      # Reserved for output generation
autocompact_buffer = 13000  # Buffer before autocompact triggers
emergency_buffer = 3000     # Buffer before emergency block
max_failures = 3            # Circuit breaker threshold
micro_keep_recent = 5       # Keep N most recent tool results
# autocompact_threshold_pct = 50  # Override: trigger at N% of context_window
```

---

## File State Cache

An LRU cache that tracks files the agent has recently accessed, enabling read deduplication and automatic cache updates on writes.

- **Read dedup**: A repeated Read reopens the file and verifies both the opened object identity and the selected-content digest. When both match, the tool returns a short unchanged marker instead of repeating the content in the model context.
- **Edit guard**: Edit requires a prior full-file Read of the same opened object and content. A partial Read does not authorize a whole-file edit.
- **Write/Edit auto-update**: After an atomic Write or Edit, the cache is bound to the newly opened object and content digest. Millisecond modification time is retained only for compatibility and is not used as the validity check.
- **Dual eviction**: Entries are evicted when either the entry count limit or the total byte size limit is reached.
- **Oversized entries**: A single entry larger than `max_size_bytes` is not inserted and does not evict unrelated cached files.

### Configuration

```toml
[file_cache]
enabled = true                # Enable file state caching (default: true)
max_entries = 100             # Maximum cached files
max_size_bytes = 26214400     # Max total cache size (25 MB)
```

---

## Output Compaction

Post-processes tool output to reduce token usage. Three levels from lightest to heaviest:

| Level | Transformations |
|-------|----------------|
| `off` | No transformation |
| `safe` (default) | Strip ANSI escape codes, merge consecutive blank lines, collapse carriage-return progress bars |
| `full` | Everything in `safe`, plus: fold repeated lines, compact JSON indentation |

### TOON Encoding

When enabled alongside `full` compaction, TOON (Token-Oriented Object Notation) encodes uniform JSON arrays as compact tables:

```
[2]{id,name,role}:
  1,Alice,admin
  2,Bob,user
```

This is equivalent to:

```json
[{"id":1,"name":"Alice","role":"admin"},{"id":2,"name":"Bob","role":"user"}]
```

TOON instructions are injected into the system prompt so the LLM understands the format.

### Configuration

```toml
[compact]
compaction = "safe"   # off | safe | full (default: safe)
toon = false          # Enable TOON encoding (default: false)
```

### Runtime Control

In `--json-stream` mode, the compaction level can be changed at runtime via `set_config`:

```json
{"type": "set_config", "compaction": "full"}
```
