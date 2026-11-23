SELECT approval_row_id
FROM approval_requests
WHERE organization_id=?1 AND approval_row_id=?2 AND record_state='waiting' AND deadline_ms=?3
