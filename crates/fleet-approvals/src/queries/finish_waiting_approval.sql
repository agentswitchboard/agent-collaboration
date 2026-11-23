UPDATE approval_requests
SET record_state = 'ended', ended_at = ?1, erase_after_ms = ?2,
    ending_json = ?3, decided_operation_id = ?4, delivery_state = ?5,
    updated_at = MAX(updated_at, ?1)
WHERE approval_row_id = ?6 AND organization_id = ?7 AND contents_digest = ?8
  AND record_state = 'waiting' AND waiting_status = ?9 AND deadline_ms = ?10
