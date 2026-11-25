# Person link client tests

- The colocated connection machine tests check both directions of the transition table and generate histories with reconnects, stale replies, and typed refusals.
- The colocated macOS Keychain tests use disposable named Keychains to exercise the native compare and the Rust-only item namespace.
- `integration` checks the real OAuth loopback listener and DPoP exchange with a local TLS issuer. It also forces a lost refresh reply, refused successor save and overlapping saves with deterministic gates. The workspace journey forwards to real Tokio Central and checks pagination and reconnect on one manual clock.
