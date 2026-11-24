INSERT INTO signin_change_records
    (id, request_id, kind, origin, actor_person_id, permission_checked, target_id,
     created_at, updated_at, workspace_id, allowance_state)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
