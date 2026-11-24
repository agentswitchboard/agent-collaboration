SELECT requests.approval_row_id, requests.organization_id, requests.machine_id, requests.enrollment_id,
       requests.router_run_id, requests.request_id, requests.record_state, requests.ended_at, requests.erase_after_ms
FROM approval_request_destinations AS destination
JOIN approval_requests AS requests ON requests.approval_row_id = destination.approval_row_id
WHERE destination.organization_id = ?1 AND destination.destination_kind = 'people'
  AND destination.approval_row_id > ?2 AND requests.organization_id = ?1
  AND CASE WHEN ?4 = 1
      THEN requests.record_state = 'ended' AND requests.erase_after_ms <= ?3
      ELSE requests.record_state <> 'ended' OR requests.erase_after_ms IS NULL OR requests.erase_after_ms > ?3
  END
ORDER BY CASE WHEN ?4 = 1 THEN requests.erase_after_ms END DESC, destination.approval_row_id
LIMIT ?5
