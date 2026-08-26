# MCP (Model Context Protocol) Integration

## Overview

MCP allows the agent to connect to external tool servers, extending beyond the 7 built-in tools to the entire MCP server ecosystem.

## Configuring MCP Servers

Declare MCP servers in the config file:

```toml
# Stdio transport: launch a local subprocess
[mcp.servers.filesystem]
transport = "stdio"
command = "npx"
args = ["-y", "@modelcontextprotocol/server-filesystem", "/Users/me/project"]

[mcp.servers.github]
transport = "stdio"
command = "npx"
args = ["-y", "@modelcontextprotocol/server-github"]
env = { GITHUB_TOKEN = "ghp_xxx" }
network = { network_domains = ["https://api.github.com"] }
startup_timeout_ms = 30000

# SSE transport: connect to a remote SSE server
[mcp.servers.database]
transport = "sse"
url = "http://localhost:3001/sse"

# Streamable HTTP transport: HTTP POST communication
[mcp.servers.remote-tools]
transport = "streamable-http"
url = "https://tools.example.com/mcp"
headers = { Authorization = "Bearer xxx" }
```

## Transport Types

| Transport | Description | Use Case |
|-----------|-------------|----------|
| `stdio` | Launch local subprocess, communicate via stdin/stdout | Local MCP servers (npx, uvx) |
| `sse` | GET for SSE event stream, POST for requests | Remote MCP servers |
| `streamable-http` | HTTP POST, supports SSE streaming responses | Remote MCP servers |

A stdio server has no direct network access in Auto. If it needs outbound HTTP(S), declare exact origins in `network.network_domains`. The server process receives only the per-launch Host proxy for those origins on supported platforms. Explicit proxy variables in `env` cannot replace this policy. Remote SSE and Streamable HTTP transports derive their network destination from `url` and do not use the child-process proxy.

## Startup Timeout

Configured MCP servers are connected concurrently during startup. Each server
has a startup timeout covering transport connection, `initialize`, and
`tools/list`. The default is `30000` milliseconds.

```toml
[mcp.servers.slow-tools]
transport = "stdio"
command = "npx"
args = ["-y", "slow-mcp-server"]
startup_timeout_ms = 60000
```

Increase `startup_timeout_ms` for servers that need extra time for first-run
setup, package downloads, remote authentication, or slow network handshakes.

## Deferred Loading

MCP tools can be registered as "deferred" — their full schema is not loaded into the system prompt at startup, reducing initial token usage. The LLM discovers deferred tools via the `ToolSearch` tool when needed.

```toml
[mcp.servers.large-toolset]
transport = "stdio"
command = "npx"
args = ["-y", "my-mcp-server"]
deferred = true    # Don't load tool schemas at startup
```

| `deferred` | Behavior |
|------------|----------|
| omitted or `true` (default) | Tools registered but schemas loaded on-demand via `ToolSearch` |
| `false` | Tool schemas included in the system prompt at startup |

Set `deferred = false` only when the schemas should always be present in the
initial system prompt. Child Agents that inherit at least one deferred MCP tool
also receive `ToolSearch`; a child created after MCP sources are removed for
Plan mode does not inherit either the proxies or their search metadata.
Host-added MCP servers use the same deferred-by-default behavior. A successful
dynamic connection refreshes the root Agent's `ToolSearch` snapshot; a failed
connection does not change the tool registry.

## Tool Naming

- Every MCP tool uses a deterministic `mcp_v3_{digest}` model-visible alias.
- The digest is the first 28 bytes (224 bits) of SHA-256 over a versioned,
  length-delimited `(server UTF-8, tool UTF-8)` tuple.
- The alias is always 63 ASCII characters and contains only letters, digits,
  and underscores, so it remains within the common 64-character provider
  limit regardless of the original names.
- The alias is a one-way identifier, not a mathematically reversible encoding.
  Each proxy retains the exact server and tool names for the actual MCP call.
- Registry insertion still checks for an existing alias. A collision is
  rejected without replacing the first registered tool.

This v3 namespace intentionally replaces both the ambiguous legacy
`mcp__{server}__{tool}` display name and the unbounded hexadecimal v2 name.
Pending calls and allow-lists that store an older display name must select the
tool again after upgrade; old aliases are not registered. Permission
capabilities are keyed separately by the exact server and tool names, so the
display-name change does not widen an existing grant.

## Plan Mode Lifecycle

Entering Plan mode permanently disables the MCP managers attached to the
current Run, cancels admitted requests, removes MCP proxies and MCP-derived
child capabilities, and refreshes `ToolSearch` without the removed schemas.
Leaving Plan mode does not re-enable those managers. Start a new Run and repeat
connection authorization to use MCP again.

## Startup Flow

1. Connect to all configured MCP servers
2. Perform MCP protocol handshake (`initialize`) for each server
3. Discover available tools (`tools/list`)
4. Register tools in the tool registry — the agent uses them like built-in tools
5. On shutdown, reject new requests, cancel admitted requests, close transports
   concurrently, and drain within one overall deadline shared by every server
   and Host-owned manager
