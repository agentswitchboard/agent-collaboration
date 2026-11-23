# Approval holder checks

Module-local tests check the provider-wait and run-gate reducers with fixed and generated orders. The integration target composes those reducers for selected core model traces and transfers typed registration and Stop packets across a simulated link with Turmoil.

The fixed review traces cover exact dial and call attempts, link retirement,
authority-first refusal, admitted endings, provider loss, Stop visibility and
local capacity. The generated history compares both reducers with independent
gate and wait facts. Turmoil drives typed registration, link refusal and Stop
loss over simulated TCP with the exact request bytes retained across retries.
The agent withdrawal cases exercise exact take-back before publication, after a
possible Raise, after claim or permit, and after the original writer starts.
