# Person approval results

The person WebSocket serves `listPersonApprovals`, `readPersonApproval` and
`decidePersonApproval`. List, read and a new decision require a current
`approval:decide` grant. Central verifies the person bearer only during the
upgrade. A well-formed upgrade with a refused bearer or authorization standing
returns an empty HTTP 401. An HTTP/1.1 GET without upgrade headers receives
HTTP 426 on Cloudflare and HTTP 400 on Tokio.

Each approval transaction checks the admitted session expiry, the authorization
snapshot expiry, and the current organization and person authorization floors.
Expired or superseded authority on an open socket requests a reconnect. A current
connection without the grant receives `notPermitted` unless it asks for its own
exact retained Decide result.

[Reply vectors](approval-person-reply-vectors.json) pin the closed JSON body
shapes, including whole-second and fractional timestamps.

| Call and reply | Approval-domain effect |
| --- | --- |
| List `page { items, next }` | Settle visible timeouts discovered on the page and advance the approval time floor for a settlement. |
| Read `request { view }` | Return the destination's permitted view. Settle a discovered timeout and advance the floor. |
| Read or Decide `notVisible` | Missing and hidden requests share a reply. These requests create no operation result. |
| Decide `savedDecision { decision }` | Save the ending and delivery debt, advance the approval time floor, resolve the person's actor and retain the first result. |
| Decide `alreadyDecided { by, at }` | Retain the result for the new operation ID and preserve the original winner and time. |
| Decide `tooLate` | Retain the result. A timeout discovered by this call settles the approval and advances the floor. |
| Decide `invalid { reason }` | Retain the result and preserve the approval. |
| Decide `operationConflict` | Preserve the original result when the same operation ID names a different intent. |

List scans up to 100 records in descending request ID order. The continuation
`next` is required and null at the end. A waiting page can omit settled timeouts
and still supply a continuation. Read projects the destination's own answer or
another destination's standing. Approval rows preserve millisecond precision.

An exact retry recovers the original body within the 24-hour operation retention
window. The caller must be the same person and supply the same operation ID and
answer. The stored intent digest binds that answer to the organization and the
request. Central validates the retained decision against the stored ending before
returning it. If the grant was removed, the person reconnects with current
authority and can still recover their own exact result.

Native SQLite can commit a decision before alarm publication fails. The person
socket closes with code 1011 while the committed ending reaches the holder. The
native conformance case observes the holder ending before retrying the decision
and checks that the recovered bytes match the retained answer.
