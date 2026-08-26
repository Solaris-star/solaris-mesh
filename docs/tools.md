# Built-in Tools

The agent has 7 always-available built-in tools. ToolSearch is a deferred
schema loader, and enabling persistent Memory adds the optional `Memory` tool.
The LLM automatically selects and invokes them based on the task.

| Tool | Function | Concurrent |
|------|----------|------------|
| **Read** | Read file contents (with line numbers) | Yes |
| **Write** | Write files (auto-creates directories) | No |
| **Edit** | Precise string replacement | No |
| **ExecCommand** | Execute shell commands | No |
| **Grep** | Regex search file contents (via ripgrep) | Yes |
| **Glob** | Find files by pattern matching | Yes |
| **Spawn** | Spawn sub-agents for parallel tasks | No |
| **ToolSearch** | Load schemas for deferred tools | Yes |
| **Memory** | Search and change versioned persistent memory when enabled | No |

---

## Permission modes

Auto is one permission mode shared by file tools, network operations, MCP, plugins, hooks, skills, and process launches. Process sandboxing is only Auto's platform executor for process-backed work. Strict Auto starts a process only when the selected backend reports Full; Partial or Unavailable denies that process operation, keeps Auto file tools available, and never falls back to Ambient.

On Linux, Full requires Bubblewrap at `/usr/bin/bwrap` or `/bin/bwrap`, the packaged `solaris-process-sandbox-helper`, Landlock ABI v3 or newer, and a successful real startup handshake. The structured reason reports a missing prerequisite or a setup/handshake failure, and strict Auto denies the process before exposing a running child. Supported read-only host toolchain roots are `/bin`, `/usr`, `/lib`, `/lib64`, `/sbin`, and `/nix/store` when present, plus selected loader, certificate, resolver, host, identity, and timezone files under `/etc`. Toolchains or runtime dependencies under `/opt`, the real user home, or other host paths are not guaranteed to run. The complete host root is not mounted because a read-only mount would still expose pathname Unix sockets.

On Windows, Full requires the packaged helper, AppContainer profile support, temporary workspace and protected-state ACLs, Job Object containment, and a successful real startup handshake. Each launch uses a private home and temporary directory and no network capability. Junctions, protected-object aliases, and workspace files with hardlinks outside the workspace are rejected before the target starts. A failure to create or verify the AppContainer, ACLs, Job, or handshake is reported as Partial or Unavailable and the target is not returned as a running child. On Windows 11 build 26200, the native suite passes under an interactive Administrator account; the earlier `NT SERVICE\WebCodexRunner` result remains a service-account incompatibility until that account also passes the startup probe.

On macOS, Full requires the fixed `/usr/bin/sandbox-exec`, the packaged helper, a captured and sealed workspace directory authority, a restrictive Seatbelt profile, guardian process-group containment, and a successful real startup handshake over a helper-only `CLOEXEC` pipe. The capability probe launches the trusted helper target in a private workspace through the normal strict policy; the helper must write inside that workspace, observe denied external file reads and writes, observe denied non-proxy loopback access, start the pinned target, and complete the handshake before Full is reported. The profile permits writes only in the workspace and per-launch private home, temporary, and control directories. Runtime-state roots are denied even when nested under the workspace; direct network access and host Unix sockets remain unavailable. Read-only host tooling is limited to `/System`, `/usr`, `/bin`, `/sbin`, `/dev/fd`, selected safe device files, `/Library/Apple`, selected dynamic-loader and timezone roots, and selected identity, resolver, host, and certificate files under `/private/etc`. The real TTY, the complete device tree, privileged task ports, general host IPC, and process-argument sysctls remain closed. Tools that require the real user home or another host path are rejected. Workspace sockets, FIFOs, protected-object aliases, and workspace files with hardlinks outside the workspace are rejected before the target starts. Seatbelt denies `setsid`, `setpgid`, and the kernel `posix_spawn` path so managed descendants cannot detach from cleanup. The trusted helper starts the pinned target with explicit `fork` and `execve`; programs that require `posix_spawn` receive a permission error, while fork/exec-based shells and tools remain supported. If Seatbelt, the helper, workspace identity checks, behavior probes, containment, or the handshake cannot be verified, strict Auto reports Unavailable and does not return the target as a running child. The workspace directory fd stays open through child cleanup, and the configured path is checked against its retained identity immediately before spawn. A same-user process could still replace the name in the short interval between that final check and `sandbox-exec` consuming the path; an already-running malicious same-user process is outside the stated threat scope.

On other Unix platforms, a digest-pinned OCI or VM runner may implement the [versioned external strict-Auto contract](external-sandbox-runner.md). It is disabled unless both its absolute path and SHA-256 are explicitly configured. Solaris accepts `Full` only when the pinned runner proves the exact request, process intent, isolation capabilities, and single-use launch token again at target startup. Configuring the binary means trusting its platform isolation; native escape tests are still required. Missing or weaker runners deny process execution without an Ambient fallback. Other non-Unix platforms remain Unavailable.

Auto child processes have no direct network access. On Linux and macOS, exact approved HTTP(S) destinations are exposed only through a per-launch Host proxy. DNS is resolved again at connection time and every result must be public; IP literals, wildcards, metadata names, user information, malformed headers, and unapproved host/port pairs are rejected. No domains means no proxy process and no inherited or injected `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`, or `NO_PROXY`. Windows fails closed for Auto processes that declare network destinations until a verified process-isolated proxy runner is available; Solaris does not grant `internetClient`, create a system loopback exemption, or treat proxy environment variables as enforcement. The request returns `denied` with an `Unavailable` `WindowsAppContainer` report and reason `NetworkProxyUnavailable` before target spawn. Empty network policy requests continue to use the ordinary AppContainer capability probe and can still report Full.

The Windows network probe follows the PSEC ABI described by Microsoft `mxc` commit [`b497fd48653e01c846c6ef225e0af1c859b70122`](https://github.com/microsoft/mxc/commit/b497fd48653e01c846c6ef225e0af1c859b70122). It loads `processmodel.dll` from an absolute System32 path and distinguishes missing DLLs or exports, unsupported Query results, `E_NOTIMPL`, `HRESULT_FROM_WIN32(ERROR_CALL_NOT_IMPLEMENTED)`, other Query failures, and unfinished functional proof. Export presence, Query success, and the Windows build number are diagnostics only. Full additionally requires a tested PSEC create/close lifecycle, `PROC_THREAD_ATTRIBUTE_SECURITY_ENVIRONMENT`, a packaged proxy AppContainer peer, and WFP behavior. Solaris does not copy Microsoft's PSEC FlatBuffer schema in this stage, so the probe cannot make a network-bearing request Full.

Bypass runs tools with the complete host access available to the current OS user. Explicit deny rules and blocking hooks can still refuse an operation. Bypass does not promise to prevent destructive host actions.

Linux, Windows, and macOS each have a native strict-Auto test suite. A platform is reported as Full only after its actual backend passes the startup, file-boundary, direct-network, and cleanup probes; compiling another platform's code is not a substitute for that runtime proof. Linux and macOS additionally require native approved-domain proxy tests before that capability is release evidence.

---

## Read

Read file contents with line numbers, similar to `cat -n`.

- Supports `offset` and `limit` parameters for reading file slices
- Auto-detects binary files
- Output format: line-numbered text

## Write

Write content to a file atomically.

- Atomic write: writes to a temp file first, then renames
- Auto-creates parent directories

## Edit

Find and replace exact strings in a file.

- Matches `old_string` exactly and replaces with `new_string`
- Requires a unique match by default; errors on multiple matches
- Use `replace_all` to replace all occurrences
- Revalidates the opened target identity before atomic replacement. The narrow interval between the final identity check and rename is a known limitation and does not by itself block release.

## ExecCommand

Execute a shell command and return the result.

- Default timeout: 120 seconds, max 600 seconds
- Returns exit code, stdout, and stderr

## Grep

Search file contents with regular expressions.

- Uses `rg` (ripgrep) when available, falls back to `grep -rn`
- Supports glob filtering and case-insensitive search
- Results limited to 250 lines

## Glob

Find files matching a glob pattern.

- Standard glob patterns (e.g., `**/*.rs`)
- Results sorted by modification time (newest first)
- Returns up to 100 files

## Spawn

See [Multi-agent collaboration](advanced.md#multi-agent-collaboration) in the Advanced Features guide.

## ToolSearch

Load full schemas for deferred tools so the LLM can invoke them. Deferred tools (from MCP servers with `deferred = true`) are registered by name only — their parameter schemas are not loaded until the LLM calls ToolSearch.

- Query by exact name: `"select:Read,Edit,Grep"`
- Keyword search: `"slack send"` returns best matches
- Returns up to 5 results by default

## Memory

Memory is disabled by default. When `[memory].enabled = true`, the tool supports bounded `search` and `list` operations plus versioned `create`, `edit`, and `delete` changes.

- A session searches its frozen startup snapshot; writes appear in later sessions.
- With `[memory].review = true`, root-agent writes create proposals instead of applying immediately.
- Child agents can submit proposals but cannot directly change records or review proposals.
- Search returns at most 8 records and at most 32 KiB of record content.

---

## How It Works

```
User input → Build request (system prompt + history + tool definitions)
           → Stream LLM API response
           → Output text to stdout in real-time
           → If LLM returns tool_use → authorize under the current permission mode → execute → send result back
           → Loop until LLM stops calling tools
           → Output final reply → save session
```

- Concurrent-safe tools (Read, Grep, Glob) execute in parallel
- Non-concurrent tools (Write, Edit, ExecCommand) execute sequentially
- Tool output is auto-truncated to prevent context window overflow
- Tool output can be compacted (see [Output Compaction](advanced.md#output-compaction))

## Tool Descriptions

Each built-in tool includes a detailed description and usage guidance that is injected into the system prompt. These descriptions help the LLM select the right tool and use it effectively — for example, preferring Grep over ExecCommand for content search, or using Edit instead of Write for modifications.
