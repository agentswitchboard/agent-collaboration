SELECT requests.*
FROM approval_requests AS requests
WHERE organization_id = ?1 AND machine_id = ?2 AND enrollment_id = ?3
  AND holder_session_id = ?4 AND record_state = 'waiting'
ORDER BY approval_row_id
LIMIT ?5
