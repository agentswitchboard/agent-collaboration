UPDATE approval_router_runs SET marker_cleanup_due_ms = ?1
WHERE router_run_id = ?2 AND retired_at IS NOT NULL
