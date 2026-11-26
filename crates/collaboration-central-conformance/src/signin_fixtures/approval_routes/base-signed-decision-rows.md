# Stored rows from a signed person decision

Captured at `c253c97b4b9d41c26feb6b320eb474e940bddb1b` by the Tokio Central integration test `organization_signin::person_approval_routes::socket_replays_the_exact_signed_answer_after_permission_loss`. It seeded one waiting approval and made the first Decide over the private Unix listener. Before the authorization floor changed, a test-only readback observed the same approval ended and one operation-log entry, up from zero. The actor, person mapping and people destination were read in the same fixture. The test then recovered the same answer over the socket with the grant removed.

The row readback was the only harness change. Its private receipt is `tmp/2026-11-25-people-piece-2/base-capture-only.patch` (SHA-256 `70dde8b87d8fb9472f680eb2324d3b17aacd8468c810518a39a6a9171b5c539b`). The production writer was unchanged. Cargo nextest, through the repository signing runner, reported 1 passed, 0 failed, 621 skipped, exit 0. The fixture uses synthetic identities and a controlled clock.

`base-signed-decision-rows.json` SHA-256 `3354298b740d814c416b60cd40e5c245b331a834ef5f5baf447041be1c6fa6e1`. Nested JSON strings are the raw SQLite TEXT values. They retain their original bytes when decoded from the outer fixture.
