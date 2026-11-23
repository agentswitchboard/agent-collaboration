UPDATE approval_requests
SET waiting_status = 'passed_on', updated_at = MAX(updated_at, ?1)
WHERE approval_row_id = ?2 AND organization_id = ?3 AND contents_digest = ?4
  AND record_state = 'waiting' AND waiting_status = ?6 AND deadline_ms = ?5
