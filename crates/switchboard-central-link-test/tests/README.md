# Native Central link integration

The integration target uses Central’s public fixture and the Mac driver’s loopback constructor to prove Welcome, on-socket renewal, expiry, revocation, and replacement against the real Tokio server.

The custody journey joins the real admitted driver to in-process Tokio Central and a real Mac SQLite store on one manual clock.

The first-message journey uses two independently enrolled message systems and one local Central on one manual clock. It checks the real tool results through acceptance, keep, one scripted managed notice, Seen, and a read acknowledgement. A second case holds the notice result across receiver shutdown and proves the reopened inbox suppresses a second notice. The journey stops at the managed notice port. PR 411's Host tests and the owner's watched run cover the ACP Claude step separately.

The simulated link cases run the same Mac driver, custody and SQLite stores against
Central's real Axum router over Turmoil TCP. They inspect the simulator's queued
TCP segments to delay or drop a selected WebSocket frame and model connections
that reset or outlive their requests. The existing fixture supplies the signed
enrollment and JWKS service.

The C5 transcript case observes one empty capability announcement before an
ordinary request and reply on both the original and replacement sockets.

The enrolled lease journey drives the real refresh owner, lease adapter, Mac enrollment SQLite store and Central's signed revocation route. Its sign-in grant and loopback address are supplied at the fixture boundary. The adjacent enrollment cases run lease mapping and settings publication against the same real store with an injected Keychain.

The message-system journey starts unenrolled, writes enrollment through a second store on the same SQLite database, connects to real Central and sends through the outbox and inbox. It uses one manual clock to drive polling, autonomous driver recovery and enrollment ending. It holds the driver's join to prove the drain order. Two close cases check that retiring the initial read or retry leaves the database unopened.
