UPDATE approval_router_runs
SET retired_at = MAX(registered_at, ?1), updated_at = MAX(registered_at, ?1)
WHERE router_run_id = ?2 AND organization_id = ?3
  AND machine_id = ?4 AND enrollment_id = ?5 AND retired_at IS NULL
