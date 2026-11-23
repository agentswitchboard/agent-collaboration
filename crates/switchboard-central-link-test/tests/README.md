# Native Central link integration

The integration target uses Central’s public fixture and the Mac driver’s loopback constructor to prove Welcome, on-socket renewal, expiry, revocation, and replacement against the real Tokio server.

The custody journey joins the real admitted driver to in-process Tokio Central and a real Mac SQLite store on one manual clock.

The simulated link cases run the same Mac driver, custody and SQLite stores against
Central's real Axum router over Turmoil TCP. They inspect the simulator's queued
TCP segments to delay or drop a selected WebSocket frame and model connections
that reset or outlive their requests. The existing fixture supplies the signed
enrollment and JWKS service.
