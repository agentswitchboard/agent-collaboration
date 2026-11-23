UPDATE approval_requests
SET record_state = 'ended', ended_at = ?1, ending_json = ?2, decided_operation_id = ?3,
    delivery_state = ?4, updated_at = MAX(updated_at, ?1)
WHERE approval_row_id = ?5 AND organization_id = ?6 AND contents_digest = ?7
  AND record_state = 'waiting' AND waiting_status = ?8 AND deadline_ms = ?9
