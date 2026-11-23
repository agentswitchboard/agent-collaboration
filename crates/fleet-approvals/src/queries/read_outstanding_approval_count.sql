SELECT count(*) AS outstanding
FROM (
    SELECT 1 FROM approval_requests
    WHERE organization_id = ?1 AND machine_id = ?2 AND enrollment_id = ?3
      AND router_run_id = ?4 AND record_state = 'waiting'
    UNION ALL
    SELECT 1 FROM approval_requests
    WHERE organization_id = ?1 AND machine_id = ?2 AND enrollment_id = ?3
      AND router_run_id = ?4 AND record_state = 'ended' AND delivery_state = 'owed'
      AND (erase_after_ms IS NULL OR erase_after_ms > ?5)
    LIMIT ?6
)
