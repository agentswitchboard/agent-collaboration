SELECT requests.approval_row_id, requests.organization_id, requests.machine_id, requests.enrollment_id,
       requests.router_run_id, requests.request_id, requests.record_state, requests.ended_at
FROM approval_requests AS requests
WHERE organization_id = ?1 AND request_id = ?2
