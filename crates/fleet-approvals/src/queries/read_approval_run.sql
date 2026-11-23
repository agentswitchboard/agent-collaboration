SELECT router_run_id, organization_id, machine_id, enrollment_id,
       registration_order, registered_at, retired_at, created_at, updated_at
FROM approval_router_runs
WHERE router_run_id = ?1
