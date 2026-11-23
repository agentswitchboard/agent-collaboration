SELECT requests.*
FROM approval_requests AS requests
WHERE organization_id = ?1 AND machine_id = ?2 AND enrollment_id = ?3 AND router_run_id = ?4
  AND ((record_state = 'waiting' AND deadline_ms <= ?5)
       OR (record_state = 'ended' AND delivery_state = 'owed'))
ORDER BY approval_row_id
LIMIT ?6
