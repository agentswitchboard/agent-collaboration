DELETE FROM approval_router_runs
WHERE router_run_id = ?1 AND organization_id = ?2 AND enrollment_cleanup_ms = ?3
  AND NOT EXISTS (
      SELECT 1 FROM approval_erased_requests WHERE router_run_id = ?1
  )
