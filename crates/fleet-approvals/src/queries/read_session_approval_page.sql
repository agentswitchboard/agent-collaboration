SELECT requests.approval_row_id, requests.organization_id, requests.machine_id, requests.enrollment_id,
       requests.router_run_id, requests.request_id, requests.record_state, requests.ended_at, requests.erase_after_ms
FROM approval_request_destinations AS destination
JOIN approval_requests AS requests ON requests.approval_row_id = destination.approval_row_id
WHERE destination.organization_id = ?1 AND requests.organization_id = ?1
  AND destination.destination_kind = 'parent_session' AND destination.session_machine_id = ?2
  AND destination.session_enrollment_id = ?3 AND destination.session_id = ?4
  AND (?5 IS NULL OR requests.request_id < ?5)
  AND CASE WHEN ?7 = 1
      THEN requests.record_state = 'ended' AND requests.erase_after_ms <= ?6
      ELSE requests.record_state <> 'ended' OR requests.erase_after_ms IS NULL OR requests.erase_after_ms > ?6
  END
ORDER BY CASE WHEN ?7 = 1 THEN requests.erase_after_ms END DESC, requests.request_id DESC
LIMIT ?8
