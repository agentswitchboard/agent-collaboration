SELECT allowance_id, workspace_id, state, change_request_id, change_origin, created_at, updated_at
FROM login_machine_allowances
WHERE machine_id = ? AND state = 'active'
ORDER BY workspace_id
