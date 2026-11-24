DELETE FROM approval_router_runs
WHERE router_run_id IN (
    SELECT run.router_run_id
    FROM approval_router_runs AS run
    WHERE organization_id = ?1 AND enrollment_id = ?2
      AND router_run_id <> ?3 AND retired_at IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM approval_erased_requests AS marker
          WHERE marker.router_run_id = run.router_run_id
      )
    ORDER BY registration_order LIMIT ?4
)
