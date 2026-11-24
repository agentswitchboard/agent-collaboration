SELECT router_run_id FROM approval_router_runs
WHERE organization_id = ?1 AND enrollment_id = ?2 AND machine_id = ?3
  AND enrollment_cleanup_ms IS NOT NULL AND enrollment_cleanup_due_ms IS NULL
LIMIT 1
