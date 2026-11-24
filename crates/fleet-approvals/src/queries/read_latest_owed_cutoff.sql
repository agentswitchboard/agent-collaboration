SELECT approval_row_id, organization_id, machine_id, enrollment_id, router_run_id,
       request_id, record_state, ended_at
FROM approval_requests
WHERE organization_id=?1 AND machine_id=?2 AND enrollment_id=?3 AND router_run_id=?4
  AND record_state='ended' AND delivery_state='owed' AND ended_at IS NOT NULL
  AND (?5 IS NULL OR ended_at <= ?5)
ORDER BY ended_at DESC LIMIT 1
