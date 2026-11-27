# Provider ACP tool frame capture

`acp-stdio-capture.ts` runs a provider command as a subprocess. Its stdin and
stdout are the caller's ACP connection. It relays the original bytes and writes
one `acp-<pid>.jsonl` file per connection, with a shared sequence across both
directions. It records provider permission requests and the caller's replies as
well as tool announcements and updates. Provider stderr is outside this ACP
capture and is suppressed by the relay.

The relay requires a canonical, owner-private directory with mode 0700.
The JSONL files are exclusive mode 0600. They can contain sensitive payloads.
Keep them in the run's private evidence directory. An incomplete or malformed
line leaves an explicit record and prevents report generation.

## Commands for a later authorized live run

Run these only after the owner has authorized the provider, model and account
for this particular run. Declare the frozen source and signed executable
hashes, a private provider-specific debug root, free loopback ports, an
authorized debug controller, a deadline, the synthetic work folder and the
owned process inventory. Use an existing pre-authorized account and stop on
any sign-in or unexpected permission request. Read back the exact model before
input. Use a fresh owner-private capture directory for each provider.

### Claude ACP

The product qualifies the official Claude adapter by its package entry and
requires empty configured arguments. Use the repository's existing Node
preload inside that adapter. The following command instruments the provider-only
debug Host against an already-started, separately qualified debug proxy:

```sh
umask 077
NODE_OPTIONS="--import=\"${PWD}/verification/observations/providers/acp/acp-stdio-recorder.mjs\"" \
CODEX_ROUTER_D0_ACP_RECORDING_DIR="$CLAUDE_CAPTURE" \
scripts/cargo_debug_signing_runner.sh target/debug/provider-only-debug-host \
  "$CLAUDE_DEBUG_RUN" "$CLAUDE_PROXY_PORT" "$CLAUDE_MCP_PORT" "$CLAUDE_ADAPTER"
```

`CLAUDE_DEBUG_RUN` is the declared canonical private direct child of the system
temporary directory. Its `router/` holds the debug proxy state, with an eligible
pre-authorized Claude account for `claude-haiku-5-5`. `CLAUDE_ADAPTER` is the
qualified absolute official package `dist/index.js`. The provider-only Host
does not start native Codex. Verify the signed debug proxy first and retain
only categorical evidence of the inference route and account eligibility.

### Cursor ACP

The pinned Host launches Cursor as a stdio subprocess. Cursor owns its model
connection and uses its own configured service and login. The Router injects
proxy configuration only into Claude's launch. The Cursor service's literal
network destination is outside the Router source.
[Provider bindings](../../../../crates/codex-router-host/src/collaboration_runtime.rs#L84-L111)
[Proxy launch configuration](../../../../crates/codex-router-host/src/claude_provider_launch_environment.rs#L35-L66)
[Subprocess transport](../../../../crates/acp-client-runtime/src/agent_session_client/provider_transport.rs#L27-L41)

The capture command below therefore requires explicit authority for Cursor's
own inference route. Stop before launch if the commission still requires Grok
traffic through the Router proxy. Declare `CURSOR_DEBUG_ROOT` with only Cursor
enabled, the existing debug native-profile permissions required by this Host
entry, the existing isolated Cursor account, an advertised Grok model and an
absolute `CURSOR_EXECUTABLE`. The original Step B limits did not grant native
profile access. If those limits still apply, the Host invocation below remains
blocked until an eligible launch route is declared.

```sh
umask 077
scripts/cargo_debug_signing_runner.sh target/debug/codex-router host \
  --router-root "$CURSOR_DEBUG_ROOT" --port "$CURSOR_ROUTER_PORT" \
  --mcp-bind "127.0.0.1:$CURSOR_MCP_PORT" --require-debug-isolation \
  --cursor-acp-executable "$NODE_EXECUTABLE" \
  --cursor-acp-arguments "$PWD/verification/observations/providers/acp/acp-stdio-capture.ts" \
  --cursor-acp-arguments "$CURSOR_CAPTURE" \
  --cursor-acp-arguments=-- \
  --cursor-acp-arguments "$CURSOR_EXECUTABLE" \
  --cursor-acp-arguments=--disable-auto-update \
  --cursor-acp-arguments=--disable-project-configs \
  --cursor-acp-arguments=--workspace \
  --cursor-acp-arguments "$CURSOR_WORK" \
  --cursor-acp-arguments acp
```

The equivalent provider launch is
`node verification/observations/providers/acp/acp-stdio-capture.ts "$CURSOR_CAPTURE" -- "$CURSOR_EXECUTABLE" --disable-auto-update --disable-project-configs --workspace "$CURSOR_WORK" acp`.
Starting it by itself supplies no Router, model or prompt evidence.

## Report

After the owned connection closes, select the JSONL file that contains its
initialize and the three intended turns. Repeat for the other provider. A
metadata-only connection's file is insufficient. Generate the private report:

```sh
umask 077
node verification/observations/providers/acp/acp-tool-frame-report.ts \
  --claude "$CLAUDE_JSONL" --cursor "$CURSOR_JSONL" > "$REPORT"
```

Omit the switch for any provider that stopped. The report compares exact paths
recognized from structured fields and includes every key of each recorded tool
frame for review. It also reports keys that recur with changed or unchanged
values, measured compact JSON sizes, and ID form and overlap counts. Raw payload
values stay in the private capture. Review provider-specific key names and
initialize metadata for sensitive content before sharing the report.

These captures establish the ACP exchange. Provider identity, model selection,
upstream routing, file effects and terminal exit are separate observations
when the ACP frames omit the relevant evidence. Account usability and the live
launch commands remain unverified by the canned-fixture suite.
