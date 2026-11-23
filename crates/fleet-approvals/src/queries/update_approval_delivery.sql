UPDATE approval_requests
SET delivery_state = ?1, updated_at = MAX(updated_at, ?2)
WHERE approval_row_id = ?3 AND organization_id = ?4 AND contents_digest = ?5
  AND record_state = 'ended' AND ending_json = ?6 AND delivery_state = ?7
