# Native Central link integration

The integration target uses Central’s public fixture and the Mac driver’s loopback constructor to prove Welcome, on-socket renewal, expiry, revocation, and replacement against the real Tokio server.

The custody journey joins the real admitted driver to in-process Tokio Central and a real Mac SQLite store on one manual clock.

The simulated link cases run the same Mac driver, custody and SQLite stores against
Central's real Axum router over Turmoil TCP. They inspect the simulator's queued
TCP segments to delay or drop a selected WebSocket frame and model connections
that reset or outlive their requests. The existing fixture supplies the signed
enrollment and JWKS service.

The enrolled lease journey drives the real refresh owner, lease adapter, Mac enrollment SQLite store and Central's signed revocation route. Its sign-in grant and loopback address are supplied at the fixture boundary. The adjacent enrollment cases run lease mapping and settings publication against the same real store with an injected Keychain.
