UPDATE approval_router_runs
SET enrollment_cleanup_ms = ?1, enrollment_cleanup_due_ms = ?2
WHERE router_run_id = ?3 AND organization_id = ?4 AND retired_at IS NOT NULL
    AND enrollment_cleanup_ms IS NULL
