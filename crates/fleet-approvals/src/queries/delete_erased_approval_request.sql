DELETE FROM approval_requests
WHERE organization_id = ?1 AND approval_row_id = ?2
  AND record_state = 'ended' AND erase_after_ms = ?3
