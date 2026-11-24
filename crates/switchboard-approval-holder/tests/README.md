# Approval holder checks

Module-local tests check the approval-wait and run-gate reducers with fixed and generated orders. The integration target composes those reducers for selected core model traces and transfers typed registration and Stop packets across a simulated link with Turmoil.

The fixed review traces cover exact dial and call attempts, link retirement,
authority-first refusal, admitted endings, provider loss, Stop visibility and
local capacity. The generated history compares both reducers with independent
gate and wait facts. Turmoil drives typed registration, link refusal and Stop
loss over simulated TCP with the exact request bytes retained across retries.
The agent withdrawal cases exercise exact take-back before publication, after a
possible Raise, after claim or permit, and after the original writer starts.
The unproven-call cases exercise the exact one-shot handover, withdrawal before
and after publication, Stop replay across a reconnect, and cause-family refusal.
The capacity cases exercise typed withdrawal before and after the final permit.
The exposure cases distinguish a frame withdrawn before the writer started from
an answer lost after possible admission. The generated history checks both traces
for each request family.
The capacity-retry histories cover an older possibly admitted Raise, selection
of a final local negative, and exact Stop reconciliation across writer loss
and reconnect. They also exercise loss or withdrawal before selection.
The confirmation cases exercise retired inputs against the current gate, queued
candidate rejection and exact retry, and the four ways a capacity-released wait
can dispose of its last call.
