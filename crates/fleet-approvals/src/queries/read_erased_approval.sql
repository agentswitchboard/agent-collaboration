SELECT marker.request_id, marker.router_run_id, run.machine_id, run.enrollment_id,
       marker.ending_json
FROM approval_erased_requests AS marker
JOIN approval_router_runs AS run ON run.router_run_id = marker.router_run_id
  AND run.organization_id = marker.organization_id AND run.retired_at IS NULL
WHERE marker.organization_id = ?1 AND marker.request_id = ?2
