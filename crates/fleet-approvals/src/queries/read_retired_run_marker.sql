SELECT approval_row_id FROM approval_erased_requests
WHERE router_run_id = ?1 ORDER BY approval_row_id LIMIT 1
