INSERT INTO approval_erased_requests (
    approval_row_id, organization_id, request_id, router_run_id,
    ending_json, created_at, updated_at
)
SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?6
FROM approval_router_runs
WHERE organization_id = ?2 AND router_run_id = ?4 AND retired_at IS NULL
