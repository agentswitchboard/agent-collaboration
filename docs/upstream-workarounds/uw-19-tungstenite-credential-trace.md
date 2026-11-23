# UW-19 · Tungstenite traces credential bytes

Tungstenite 0.29.0 formats the serialized upgrade request in `handshake/client.rs:197`.
The serialized Authorization header has the bearer bytes even when the header is marked sensitive.
`protocol/mod.rs:768` and `protocol/frame/mod.rs:228,257` trace whole frames. Machine token
renewal sends a fresh credential in a text frame. The Router can forward `log` records to tracing.

The Mac link uses Hyper for the authenticated HTTP/1.1 upgrade and hands the upgraded stream to
Tokio-Tungstenite as a client socket. Its normal dependency on `log` enables `max_level_debug`,
which compiles out the frame traces in every profile. The response still has to be HTTP 101 with
the expected WebSocket handshake headers. Hyper limits the inbound header block to 124 headers
and its HTTP/1 read buffer to 64 KiB. Tungstenite's former handshake used 124 headers, a 64 KiB
packet-size cap, and 512 reads. The socket wrapper keeps bytes Hyper read alongside the response.
The driver limits the bearer header to 64 KiB and each WebSocket frame, assembled message, and write
buffer to 1 MiB. The outbound text budget leaves room for the client frame's longest masked header.

## Recreate and check removal

In an isolated comparison build, remove `max_level_debug` from the driver dependency and run the
credential regression with the repository's configured signing runner.

```sh
CARGO_TARGET_DIR="$PWD/target" cargo nextest run --locked -p switchboard-central-link \
  --features test-support --test integration --test-threads 4 --no-tests fail \
  -E 'test(=renewal_extends_the_same_socket_without_disclosing_credentials)'
```

The test installs both a Trace-level `log` logger and a TRACE tracing subscriber. Its loopback peer
checks the Authorization header and observes the typed renewal on the same socket. Removing the
cap on 0.29 fails the assertion that both markers stay out of diagnostics. The original Tungstenite
handshake with no cap also fails that assertion. The assertion withholds both marker values.

When considering an upgrade, confirm from the replacement library's source that its request and
frame formatting omit credential and payload bytes. Re-run the test with the cap removed and the
original client handshake restored in a comparison build. Remove this workaround when both the
source check and the real-wire diagnostic assertion pass with trace compiled in.
