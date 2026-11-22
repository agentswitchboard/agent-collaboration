INSERT INTO login_machine_allowances
    (allowance_id, machine_id, workspace_id, state, change_request_id, change_origin, created_at, updated_at)
VALUES (?1, ?2, ?3, 'active', ?4, ?5, ?6, ?6)
ON CONFLICT (machine_id, workspace_id) DO UPDATE
SET state = excluded.state,
    change_request_id = excluded.change_request_id,
    change_origin = excluded.change_origin,
    updated_at = excluded.updated_at
WHERE login_machine_allowances.state = 'withdrawn'
