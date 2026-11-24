UPDATE runtime_request_log
SET approval_row_id = ?2
WHERE request_id = ?1 AND approval_row_id IS NULL
