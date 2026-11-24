SELECT requests.approval_row_id, requests.organization_id, requests.machine_id, requests.enrollment_id,
       requests.router_run_id, requests.request_id, requests.record_state, requests.ended_at, requests.erase_after_ms
FROM approval_request_destinations AS destination INDEXED BY approval_destinations_people_lookup
CROSS JOIN approval_requests AS requests ON requests.approval_row_id = destination.approval_row_id
WHERE destination.organization_id = ?1
  AND destination.request_id = ?2
  AND destination.destination_kind = 'people'
  AND requests.organization_id = destination.organization_id
  AND requests.request_id = destination.request_id
