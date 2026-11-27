# Native Central link integration

The integration target uses Central’s public fixture and the Mac driver’s loopback constructor to prove Welcome, on-socket renewal, expiry, revocation, and replacement against the real Tokio server.

The custody journey joins the real admitted driver to in-process Tokio Central and a real Mac SQLite store on one manual clock.

The first-message journey uses two message systems, one native Tokio Central, and one manual clock. Each Mac's enrollment is written directly into its store with a fixture bearer. Tests enter at the real message-system port with fixture-admitted sessions, below the service and MCP/CLI. They check acceptance, keep, a scripted managed notice, Seen, and acknowledgement. The other cases check restart during a notice, reads after the delivery window, and acknowledgement before notice. The recipient directory and notice writer are fixtures. PR 411's Host fixtures and the owner's watched run cover the ACP Claude step separately.

The simulated link cases run the same Mac driver, custody and SQLite stores against
Central's real Axum router over Turmoil TCP. They inspect the simulator's queued
TCP segments to delay or drop a selected WebSocket frame and model connections
that reset or outlive their requests. The existing fixture supplies the signed
enrollment and JWKS service.
The receipt resend case drops the answer to a committed delivery and Seen batch,
then checks replay, reply correlation and length, one handover, and final
confirmation on the replacement connection.
It replays an earlier request's answer through the replacement connection and
checks that its old correlation is refused. The stale-connection completion guard
is exercised separately at the [custody inbox boundary](../../switchboard-message-custody/tests/integration/inbox_custody.rs#L453).
The reopen case drops the custody worker and driver while the receipt is owed
and checks the same receipt after opening the persisted store and a fresh link.

Registration cases also decode frames from the native Central socket's byte
stream. They hold the acknowledgement independently of later heartbeat and
renewal traffic and check custody across minute boundaries, registration
failures, reconnects, and a full inbox intake.

The C5 transcript case observes one empty capability announcement before an
ordinary request and reply on both the original and replacement sockets.

The enrolled lease journey drives the real refresh owner, lease adapter, Mac enrollment SQLite store and Central's signed revocation route. Its sign-in grant and loopback address are supplied at the fixture boundary. The adjacent enrollment cases run lease mapping and settings publication against the same real store with an injected Keychain.

The message-system journey starts unenrolled, writes enrollment through a second store on the same SQLite database, connects to real Central and sends through the outbox and inbox. It uses one manual clock to drive polling, autonomous driver recovery and enrollment ending. It holds the driver's join to prove the drain order. Two close cases check that retiring the initial read or retry leaves the database unopened.
