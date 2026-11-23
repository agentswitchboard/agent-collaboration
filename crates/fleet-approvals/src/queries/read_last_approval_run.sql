SELECT router_run_id, organization_id, machine_id, enrollment_id,
       registration_order, registered_at, retired_at, created_at, updated_at
FROM approval_router_runs
WHERE organization_id = ?1 AND machine_id = ?2 AND enrollment_id = ?3
ORDER BY registration_order DESC
LIMIT 1
