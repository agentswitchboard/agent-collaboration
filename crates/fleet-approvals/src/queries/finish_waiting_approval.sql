UPDATE approval_requests
SET record_state = 'ended', ended_at = ?1, erase_after_ms = ?2,
    ending_json = ?3, compact_ending_json = ?4, decided_operation_id = ?5, delivery_state = ?6,
    updated_at = MAX(updated_at, ?1)
WHERE approval_row_id = ?7 AND organization_id = ?8 AND contents_digest = ?9
  AND record_state = 'waiting' AND waiting_status = ?10 AND deadline_ms = ?11
