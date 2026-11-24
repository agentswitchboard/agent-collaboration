UPDATE approval_router_runs SET enrollment_cleanup_due_ms = ?1
WHERE router_run_id = ?2 AND enrollment_cleanup_ms = ?3
