SELECT requests.approval_row_id, requests.organization_id, requests.machine_id, requests.enrollment_id,
       requests.router_run_id, requests.request_id, requests.record_state, requests.ended_at
FROM approval_request_destinations AS destination
JOIN approval_requests AS requests ON requests.approval_row_id = destination.approval_row_id
WHERE destination.organization_id = ?1 AND destination.destination_kind = 'people'
  AND destination.approval_row_id > ?2 AND requests.organization_id = ?1
  AND CASE WHEN ?4 = 1
      THEN requests.record_state = 'ended' AND requests.ended_at IS NOT NULL
           AND (?3 IS NULL OR requests.ended_at <= ?3)
      ELSE requests.record_state <> 'ended' OR requests.ended_at IS NULL
           OR (?3 IS NOT NULL AND requests.ended_at > ?3)
  END
ORDER BY CASE WHEN ?4 = 1 THEN requests.ended_at END DESC, destination.approval_row_id
LIMIT ?5
