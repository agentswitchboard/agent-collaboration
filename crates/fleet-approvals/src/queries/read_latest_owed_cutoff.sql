SELECT approval_row_id, organization_id, machine_id, enrollment_id, router_run_id,
       request_id, record_state, ended_at, erase_after_ms
FROM approval_requests
WHERE organization_id=?1 AND machine_id=?2 AND enrollment_id=?3 AND router_run_id=?4
  AND record_state='ended' AND delivery_state='owed' AND erase_after_ms<=?5
ORDER BY erase_after_ms DESC LIMIT 1
