-- Add future waiting neighbors under separate synthetic holders, one per run.
WITH RECURSIVE neighbors(ordinal) AS (
    SELECT 1 WHERE ?1 > 1
    UNION ALL SELECT ordinal + 1 FROM neighbors WHERE ordinal < ?1 - 1
)
INSERT INTO approval_requests (
    approval_row_id, organization_id, machine_id, enrollment_id, router_run_id,
    request_id, contents_digest, record_state, request_kind, holder_session_id,
    offer_json, waiting_status, first_saved_at, deadline_ms, created_at, updated_at
)
SELECT
    printf('019b0000-0000-7000-8000-%012d', neighbors.ordinal),
    source.organization_id, source.machine_id, source.enrollment_id,
    printf('019c0000-0000-7000-8000-%012d', neighbors.ordinal),
    printf('019d0000-0000-7000-8000-%012d', neighbors.ordinal),
    source.contents_digest, source.record_state, source.request_kind, source.holder_session_id,
    source.offer_json, source.waiting_status, source.first_saved_at,
    source.deadline_ms + 3600000, source.created_at, source.updated_at
FROM neighbors CROSS JOIN (
    SELECT * FROM approval_requests WHERE record_state = 'waiting' ORDER BY approval_row_id LIMIT 1
) AS source
