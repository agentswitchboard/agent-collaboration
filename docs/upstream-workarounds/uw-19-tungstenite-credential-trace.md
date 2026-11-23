# UW-19 · Tungstenite traces credential bytes

Tungstenite 0.29.0 formats the serialized upgrade request in `handshake/client.rs:197`.
The serialized Authorization header has the bearer bytes even when the header is marked sensitive.
`protocol/mod.rs:768` and `protocol/frame/mod.rs:228,257` trace whole frames. Machine token
renewal sends a fresh credential in a text frame. `protocol/mod.rs:719` logs the received Close
reason at Debug and `protocol/mod.rs:736` logs the reply including that reason. The peer can put
credential text in a Close reason. The Router can forward `log` records to tracing.

The Mac link uses Hyper for the authenticated HTTP/1.1 upgrade and hands the upgraded stream to
Tokio-Tungstenite as a client socket. The driver logs only its typed Close classification. The response still
has to be an HTTP/1.1 101 with the expected WebSocket handshake headers. Hyper limits the inbound header
block to 124 headers. The driver enforces a cumulative budget of 64 KiB and 512 successful,
nonempty reads across every response head, including informational heads. The budget stops when
the handshake succeeds. Hyper replays any bytes it read alongside the 101 before reading from
the underlying stream again.
The driver limits the bearer header to 64 KiB and each WebSocket frame, assembled message, and write
buffer to 1 MiB. The outbound text budget leaves room for the client frame's longest masked header.

The driver's Cargo edge leaves logging level features unset because Cargo unifies those features
across the build. Host integration owns the runtime filter. It must drop records below Info for
`tungstenite`, `tokio_tungstenite`, and `hyper`, including their module targets, before exporting
diagnostics. The Host's integration test must drive the credential-bearing upgrade, renewal and
peer Close with verbose diagnostics enabled and verify marker absence through the configured sinks.

## Recreate and check removal

In an isolated comparison build, add a driver TRACE record formatting the raw peer Close before
classifying it. Run the credential regression with the repository's configured signing runner.

```sh
CARGO_TARGET_DIR="$PWD/target" cargo nextest run --locked -p switchboard-central-link \
  --features test-support --test integration --test-threads 4 --no-tests fail \
  -E 'test(=renewal_extends_the_same_socket_without_disclosing_credentials)'
```

The test installs both a Trace-level `log` logger and a TRACE tracing subscriber. Its loopback peer
checks the Authorization header, observes the typed renewal on the same socket, and returns a
credential marker in a Close reason. It observes the final typed Close and a driver-owned TRACE
record for its classification before asserting that credentials stayed out of driver diagnostics.
The comparison fails that assertion, which withholds the markers. The test also identifies which
dependencies logged synthetic markers. Host runtime filtering has its own integration check.

When considering an upgrade, confirm from the replacement library's source that its request and
frame formatting omit credential bytes, payloads, and Close reasons. Before dropping the workaround,
run the Host diagnostic regression with the library filter disabled and the original client
handshake restored in a comparison build. Both the source check and the full-sink diagnostic assertion
must pass at TRACE.
