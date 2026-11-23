UPDATE approval_router_runs
SET enrollment_cleanup_ms = ?1
WHERE router_run_id = ?2 AND organization_id = ?3 AND retired_at IS NOT NULL
    AND enrollment_cleanup_ms IS NULL
