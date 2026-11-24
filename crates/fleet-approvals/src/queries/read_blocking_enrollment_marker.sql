SELECT marker.approval_row_id
FROM approval_router_runs AS run
JOIN approval_erased_requests AS marker ON marker.router_run_id = run.router_run_id
WHERE run.organization_id = ?1 AND run.enrollment_id = ?2 AND run.machine_id = ?3
LIMIT 1
