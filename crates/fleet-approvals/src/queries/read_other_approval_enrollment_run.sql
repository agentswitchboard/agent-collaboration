SELECT router_run_id FROM approval_router_runs
WHERE organization_id = ?1 AND enrollment_id = ?2 AND router_run_id <> ?3
ORDER BY registration_order LIMIT 1
